from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import delete, desc, select
from sqlalchemy.orm import selectinload

from .models import (
    FlowAuditLogRecord,
    FlowDeadJobRecord,
    FlowRecord,
    FlowRunRecord,
    FlowStepRecord,
    FlowVersionRecord,
)
from ..schemas.workflow import (
    StepRun,
    StepStatus,
    TriggerType,
    WorkflowDefinition,
    WorkflowRun,
    WorkflowStatus,
)

logger = logging.getLogger("xflow.store")

WORKFLOW_CACHE_TTL = 3600
RUN_CACHE_TTL = 86400


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class WorkflowStore:
    def __init__(self, db: Any, cache: Any | None) -> None:
        self._db = db
        self._cache = cache

    # ------------------------------------------------------------------
    # Cache Keys
    # ------------------------------------------------------------------

    def _workflow_cache_key(self, tenant_id: str, name: str) -> str:
        return f"xflow:{tenant_id}:workflow:{name}"

    def _workflow_list_cache_key(self, tenant_id: str) -> str:
        return f"xflow:{tenant_id}:workflow:list"

    def _run_cache_key(self, run_id: str) -> str:
        return f"xflow:run:{run_id}"

    def _serialize_definition(self, d: WorkflowDefinition) -> Dict[str, Any]:
        return d.model_dump(mode="json")

    # ------------------------------------------------------------------
    # Workflow Definitions
    # ------------------------------------------------------------------

    async def save_definition(self, tenant_id: str, definition: WorkflowDefinition) -> WorkflowDefinition:
        payload = self._serialize_definition(definition)

        async with self._db.session() as session:
            result = await session.execute(
                select(FlowRecord).where(
                    FlowRecord.tenant_id == tenant_id,
                    FlowRecord.name == definition.name,
                )
            )
            flow = result.scalar_one_or_none()

            if flow is None:
                flow = FlowRecord(
                    tenant_id=tenant_id,
                    name=definition.name,
                    description=definition.description,
                )
                session.add(flow)
                await session.flush()
            else:
                flow.description = definition.description
                flow.updated_at = _utcnow()

            version_record = FlowVersionRecord(
                flow_id=flow.id,
                version_tag=definition.version,
                definition=payload,
            )
            session.add(version_record)
            await session.commit()

        await self._cache_definition(tenant_id, definition)
        await self._invalidate_workflow_lists(tenant_id, definition.trigger.event_name)
        return definition

    async def get_definition(self, tenant_id: str, workflow_name: str) -> Optional[WorkflowDefinition]:
        if self._cache:
            cached = await self._cache.get(self._workflow_cache_key(tenant_id, workflow_name))
            if cached:
                return WorkflowDefinition.model_validate(cached)

        async with self._db.session() as session:
            result = await session.execute(
                select(FlowVersionRecord)
                .join(FlowRecord)
                .where(
                    FlowRecord.tenant_id == tenant_id,
                    FlowRecord.name == workflow_name,
                )
                .order_by(desc(FlowVersionRecord.created_at))
            )
            record = result.scalars().first()
            if record is None:
                return None
            definition = WorkflowDefinition.model_validate(record.definition)

        await self._cache_definition(tenant_id, definition)
        return definition

    async def delete_definition(self, tenant_id: str, workflow_name: str) -> bool:
        async with self._db.session() as session:
            result = await session.execute(
                select(FlowRecord).where(
                    FlowRecord.tenant_id == tenant_id,
                    FlowRecord.name == workflow_name,
                )
            )
            flow = result.scalar_one_or_none()
            if flow is None:
                return False
            await session.delete(flow)
            await session.commit()

        if self._cache:
            await self._cache.delete(self._workflow_cache_key(tenant_id, workflow_name))
            await self._cache.delete(self._workflow_list_cache_key(tenant_id))
        return True

    async def list_definitions(self, tenant_id: str) -> List[WorkflowDefinition]:
        if self._cache:
            cached = await self._cache.get(self._workflow_list_cache_key(tenant_id))
            if cached:
                return [WorkflowDefinition.model_validate(item) for item in cached]

        async with self._db.session() as session:
            stmt = (
                select(FlowVersionRecord)
                .options(selectinload(FlowVersionRecord.flow))
                .join(FlowRecord)
                .where(FlowRecord.tenant_id == tenant_id)
                .order_by(desc(FlowVersionRecord.created_at))
            )
            result = await session.execute(stmt)
            records = result.scalars().all()

            seen: set[str] = set()
            definitions: List[WorkflowDefinition] = []
            for r in records:
                if r.flow.name not in seen:
                    definitions.append(WorkflowDefinition.model_validate(r.definition))
                    seen.add(r.flow.name)

        if self._cache:
            await self._cache.set(
                self._workflow_list_cache_key(tenant_id),
                [self._serialize_definition(d) for d in definitions],
                ttl=WORKFLOW_CACHE_TTL,
            )
        return definitions

    async def list_event_definitions(self, tenant_id: str, event_name: str) -> List[WorkflowDefinition]:
        all_defs = await self.list_definitions(tenant_id)
        return [
            d
            for d in all_defs
            if d.trigger.type == TriggerType.EVENT
            and d.trigger.event_name == event_name
        ]

    # ------------------------------------------------------------------
    # Runs
    # ------------------------------------------------------------------

    async def save_run(self, run: WorkflowRun) -> WorkflowRun:
        async with self._db.session() as session:
            v_res = await session.execute(
                select(FlowVersionRecord)
                .join(FlowRecord)
                .where(
                    FlowRecord.tenant_id == run.tenant_id,
                    FlowRecord.name == run.workflow_name,
                    FlowVersionRecord.version_tag == run.workflow_version,
                )
            )
            version = v_res.scalars().first()

            record = await session.get(FlowRunRecord, run.run_id)
            if record is None:
                record = FlowRunRecord(
                    run_id=run.run_id,
                    tenant_id=run.tenant_id,
                    version_id=version.id if version else None,
                    status=run.status.value,
                    trigger_type=run.context.get("_triggered_by", "manual"),
                    trigger_data=run.trigger_payload,
                    context=run.context,
                    started_at=run.started_at,
                )
                session.add(record)
            else:
                record.status = run.status.value
                record.context = run.context
                record.error = run.error
                record.finished_at = run.finished_at

            for sid, srun in run.steps.items():
                s_res = await session.execute(
                    select(FlowStepRecord).where(
                        FlowStepRecord.run_id == run.run_id,
                        FlowStepRecord.step_id == sid,
                    )
                )
                s_record = s_res.scalar_one_or_none()
                if s_record is None:
                    s_record = FlowStepRecord(
                        run_id=run.run_id,
                        step_id=sid,
                        step_type="action",
                        status=srun.status.value,
                        started_at=srun.started_at,
                        finished_at=srun.finished_at,
                        output_data=srun.result,
                        error=srun.error,
                    )
                    session.add(s_record)
                else:
                    s_record.status = srun.status.value
                    s_record.finished_at = srun.finished_at
                    s_record.output_data = srun.result
                    s_record.error = srun.error

            await session.commit()

        if self._cache:
            await self._cache.set(
                self._run_cache_key(run.run_id),
                run.model_dump(mode="json"),
                ttl=RUN_CACHE_TTL,
            )
        return run

    async def get_run(self, run_id: str) -> Optional[WorkflowRun]:
        if self._cache:
            cached = await self._cache.get(self._run_cache_key(run_id))
            if cached:
                return WorkflowRun.model_validate(cached)

        async with self._db.session() as session:
            stmt = (
                select(FlowRunRecord, FlowRecord.name, FlowVersionRecord.version_tag)
                .join(
                    FlowVersionRecord,
                    FlowRunRecord.version_id == FlowVersionRecord.id,
                )
                .join(FlowRecord, FlowVersionRecord.flow_id == FlowRecord.id)
                .where(FlowRunRecord.run_id == run_id)
            )
            result = await session.execute(stmt)
            row = result.first()
            if not row:
                return None

            record, flow_name, version_tag = row

            step_res = await session.execute(
                select(FlowStepRecord).where(FlowStepRecord.run_id == run_id)
            )
            steps = {
                s.step_id: StepRun(
                    step_id=s.step_id,
                    status=StepStatus(s.status),
                    started_at=s.started_at,
                    finished_at=s.finished_at,
                    result=s.output_data,
                    error=s.error,
                )
                for s in step_res.scalars().all()
            }

            run = WorkflowRun(
                run_id=record.run_id,
                tenant_id=record.tenant_id,
                workflow_name=flow_name,
                workflow_version=version_tag,
                status=WorkflowStatus(record.status),
                trigger_payload=record.trigger_data,
                context=record.context,
                steps=steps,
                started_at=record.started_at,
                finished_at=record.finished_at,
                error=record.error,
            )

        if self._cache:
            await self._cache.set(
                self._run_cache_key(run.run_id),
                run.model_dump(mode="json"),
                ttl=RUN_CACHE_TTL,
            )
        return run

    async def list_runs(
        self, tenant_id: str, workflow_name: Optional[str] = None, limit: int = 50
    ) -> List[WorkflowRun]:
        async with self._db.session() as session:
            stmt = (
                select(FlowRunRecord.run_id)
                .where(FlowRunRecord.tenant_id == tenant_id)
                .order_by(desc(FlowRunRecord.created_at))
                .limit(limit)
            )
            if workflow_name:
                stmt = (
                    stmt.join(FlowVersionRecord)
                    .join(FlowRecord)
                    .where(FlowRecord.name == workflow_name)
                )
            result = await session.execute(stmt)
            run_ids = result.scalars().all()

        runs = []
        for rid in run_ids:
            run = await self.get_run(rid)
            if run:
                runs.append(run)
        return runs

    async def list_crashed_runs(self) -> List[tuple[str, str]]:
        """Retourne les (run_id, tenant_id) des runs RUNNING au dernier arrêt."""
        async with self._db.session() as session:
            stmt = select(FlowRunRecord.run_id, FlowRunRecord.tenant_id).where(
                FlowRunRecord.status == WorkflowStatus.RUNNING.value
            )
            result = await session.execute(stmt)
            return result.all()

    # ------------------------------------------------------------------
    # Dead-letter queue
    # ------------------------------------------------------------------

    async def add_dead_job(self, run_id: str, payload: Dict[str, Any], error: str) -> None:
        async with self._db.session() as session:
            record = FlowDeadJobRecord(run_id=run_id, payload=payload, error=error)
            session.add(record)
            await session.commit()

    # ------------------------------------------------------------------
    # Audit log
    # ------------------------------------------------------------------

    async def add_audit_log(
        self,
        run_id: str,
        message: str,
        step_id: Optional[str] = None,
        level: str = "INFO",
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        async with self._db.session() as session:
            record = FlowAuditLogRecord(
                run_id=run_id,
                level=level,
                message=f"[{step_id}] {message}" if step_id else message,
                extra_data=data,
            )
            session.add(record)
            await session.commit()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _cache_definition(self, tenant_id: str, definition: WorkflowDefinition) -> None:
        if not self._cache:
            return
        await self._cache.set(
            self._workflow_cache_key(tenant_id, definition.name),
            self._serialize_definition(definition),
            ttl=WORKFLOW_CACHE_TTL,
        )

    async def _invalidate_workflow_lists(self, tenant_id: str, event_name: Optional[str]) -> None:
        if not self._cache:
            return
        await self._cache.delete(self._workflow_list_cache_key(tenant_id))
        if event_name:
            await self._cache.delete(f"xflow:{tenant_id}:event:{event_name}")
