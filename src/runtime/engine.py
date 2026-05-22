"""
Moteur d'exécution des workflows XFlow V2.
Stateless, résilient, supportant tous les types de nodes.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Protocol

from .condition import evaluate_condition, render_payload, render_value
from .retry import RetryExhausted, execute_with_retry
from ..repositories.workflow import WorkflowStore
from ..schemas.workflow import (
    ActionStep,
    AIStep,
    AnyStep,
    ConditionStep,
    ForeachStep,
    ParallelStep,
    StepRun,
    StepStatus,
    StepType,
    SwitchStep,
    TemplateStep,
    TransformStep,
    WaitStep,
    WorkflowDefinition,
    WorkflowRun,
    WorkflowStatus,
)
from ..services.webhooks import dispatch_webhooks

if TYPE_CHECKING:
    class _PluginLike(Protocol):
        ctx: Any
        def get_service(self, name: str) -> Any: ...
        async def call_plugin(self,
        plugin_name: str,
        action: str,
        payload: dict | None = None,) -> Dict[str, Any]: ...

logger = logging.getLogger("xflow.engine")

END = "END"


class WorkflowEngine:
    """Moteur d'exécution XFlow V2 — sans état propre par run."""

    def __init__(self, plugin: "_PluginLike") -> None:
        self.plugin = plugin
        self.ctx = plugin.ctx
        self._store: Optional[WorkflowStore] = None
        # Sera injecté par le plugin parent pour le scheduler
        self._enqueue_run_id: Optional[Callable] = None

    async def setup(self, store: WorkflowStore) -> None:
        self._store = store

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def init_run(
        self,
        definition: WorkflowDefinition,
        trigger_payload: Optional[Dict[str, Any]] = None,
        trigger_type: str = "manual",
    ) -> WorkflowRun:
        """Crée et persiste un run en statut PENDING."""
        run = WorkflowRun(
            run_id=str(uuid.uuid4()),
            workflow_name=definition.name,
            workflow_version=definition.version,
            status=WorkflowStatus.PENDING,
            trigger_payload=trigger_payload or {},
            context={
                "trigger": trigger_payload or {},
                "_visited": [],
                "_triggered_by": trigger_type,
            },
            started_at=datetime.now(timezone.utc),
        )
        await self._save_run(run)
        return run

    async def execute_existing(self, definition: WorkflowDefinition, run: WorkflowRun) -> None:
        """Exécute un run déjà initialisé (appelé depuis le worker)."""
        run.status = WorkflowStatus.RUNNING
        await self._save_run(run)
        await self._emit("workflow.started", run)

        if definition.webhooks:
            asyncio.create_task(dispatch_webhooks(definition.webhooks, "start", run))

        await self._execute(definition, run)

    async def cancel_run(self, run_id: str) -> bool:
        """Marque un run comme annulé."""
        if not self._store:
            return False
        run = await self._store.get_run(run_id)
        if not run:
            return False
        run.status = WorkflowStatus.CANCELLED
        await self._store.save_run(run)
        return True

    # ------------------------------------------------------------------
    # Internal execution
    # ------------------------------------------------------------------

    async def _execute(self, definition: WorkflowDefinition, run: WorkflowRun) -> None:
        timeout = definition.timeout_seconds
        try:
            start_id = run.current_step_id or definition.start_step_id
            coro = self._run_from_step(definition, run, start_id)
            if timeout:
                await asyncio.wait_for(coro, timeout=timeout)
            else:
                await coro

            run.status = WorkflowStatus.SUCCESS
            run.finished_at = datetime.now(timezone.utc)
            await self._save_run(run)
            await self._emit("workflow.success", run)
            asyncio.create_task(dispatch_webhooks(definition.webhooks, "success", run))

        except asyncio.TimeoutError:
            run.status = WorkflowStatus.FAILED
            run.error = f"Timeout global dépassé ({timeout}s)"
            run.finished_at = datetime.now(timezone.utc)
            await self._save_run(run)
            await self._emit("workflow.failed", run)
            asyncio.create_task(dispatch_webhooks(definition.webhooks, "failure", run))

        except asyncio.CancelledError:
            run.status = WorkflowStatus.CANCELLED
            run.finished_at = datetime.now(timezone.utc)
            await self._save_run(run)
            raise

        except Exception as exc:
            run.status = WorkflowStatus.FAILED
            run.error = str(exc)
            run.finished_at = datetime.now(timezone.utc)
            await self._save_run(run)
            logger.exception("Erreur fatale — workflow %s run %s", run.workflow_name, run.run_id)
            asyncio.create_task(dispatch_webhooks(definition.webhooks, "failure", run))

    async def _run_from_step(
        self,
        definition: WorkflowDefinition,
        run: WorkflowRun,
        step_id: str,
    ) -> None:
        visited: List[str] = run.context.get("_visited", [])
        current_id: Optional[str] = step_id

        while current_id and current_id != END:
            # Détection de cycle
            if current_id in visited:
                raise RuntimeError(f"Cycle détecté au step '{current_id}'")
            visited.append(current_id)
            run.context["_visited"] = visited

            if run.status == WorkflowStatus.CANCELLED:
                logger.info("Run %s annulé à l'étape '%s'", run.run_id, current_id)
                return

            step = definition.get_step(current_id)
            if step is None:
                raise ValueError(f"Step introuvable : '{current_id}'")

            run.current_step_id = current_id
            await self._save_run(run)

            current_id = await self._dispatch_step(definition, run, step)

    # ------------------------------------------------------------------
    # Step dispatcher
    # ------------------------------------------------------------------

    async def _dispatch_step(
        self,
        definition: WorkflowDefinition,
        run: WorkflowRun,
        step: AnyStep,
    ) -> Optional[str]:
        # Crash recovery : si le step est déjà SUCCESS, passer au suivant
        if (
            step.id in run.steps
            and run.steps[step.id].status == StepStatus.SUCCESS
            and hasattr(step, "on_success")
        ):
            return getattr(step, "on_success", None) or END

        if isinstance(step, ActionStep):
            return await self._run_action(definition, run, step)
        if isinstance(step, ConditionStep):
            return await self._run_condition(run, step)
        if isinstance(step, ParallelStep):
            return await self._run_parallel(definition, run, step)
        if isinstance(step, SwitchStep):
            return await self._run_switch(run, step)
        if isinstance(step, ForeachStep):
            return await self._run_foreach(definition, run, step)
        if isinstance(step, WaitStep):
            return await self._run_wait(run, step)
        if isinstance(step, TransformStep):
            return await self._run_transform(run, step)
        if isinstance(step, TemplateStep):
            return await self._run_template(run, step)
        if isinstance(step, AIStep):
            return await self._run_ai(run, step)
        return END

    # ------------------------------------------------------------------
    # Step implementations
    # ------------------------------------------------------------------

    async def _run_action(self, definition: WorkflowDefinition, run: WorkflowRun, step: ActionStep) -> str:
        step_run = self._get_or_create_step_run(run, step.id)
        step_run.status = StepStatus.RUNNING
        step_run.started_at = datetime.now(timezone.utc)
        await self._save_run(run)

        context = self._build_context(run)
        payload = render_payload(step.payload, context)
        async def call_action() -> dict:
            reponse =  await self.plugin.call_plugin(
                plugin_name=step.plugin,
                action=step.action,
                payload=payload
            )
            return reponse

        try:
            result = await execute_with_retry(call_action, step.retry)
            step_run.status = StepStatus.SUCCESS

            print('data:', result)
            step_run.result = result.get("result") if isinstance(result, dict) else result
            run.context.setdefault("steps", {})[step.id] = {"result": step_run.result}
            await self._emit("workflow.step.success", run, {"step_id": step.id})
            return step.on_success or END

        except (RetryExhausted, Exception) as e:
            step_run.status = StepStatus.FAILED
            step_run.error = str(e)
            await self._emit("workflow.step.failed", run, {"step_id": step.id, "error": str(e)})
            if step.on_failure:
                return step.on_failure
            raise

        finally:
            step_run.finished_at = datetime.now(timezone.utc)
            await self._save_run(run)

    async def _run_condition(self, run: WorkflowRun, step: ConditionStep) -> str:
        context = self._build_context(run)
        met = evaluate_condition(step.condition, context)
        run.context.setdefault("steps", {})[step.id] = {"condition_met": met}
        await self._save_run(run)
        return (step.if_true if met else step.if_false) or END

    async def _run_parallel(self, definition: WorkflowDefinition, run: WorkflowRun, step: ParallelStep) -> str:
        tasks = [
            self._run_from_step(definition, run, branch[0])
            for branch in step.branches
            if branch
        ]
        if step.wait_all:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            errors = [r for r in results if isinstance(r, Exception)]
            if errors and step.on_failure:
                return step.on_failure
            if errors:
                raise errors[0]
        else:
            for task in tasks:
                asyncio.create_task(task)
        return step.on_success or END

    async def _run_switch(self, run: WorkflowRun, step: SwitchStep) -> str:
        context = self._build_context(run)
        val = str(render_value(step.expression, context))
        run.context.setdefault("steps", {})[step.id] = {"switched_on": val}
        await self._save_run(run)
        return step.cases.get(val, step.default) or END

    async def _run_foreach(self, definition: WorkflowDefinition, run: WorkflowRun, step: ForeachStep) -> str:
        context = self._build_context(run)
        items = render_value(step.items, context)
        if not isinstance(items, list):
            items = []

        if step.parallel and step.steps:
            sem = asyncio.Semaphore(step.max_parallel)

            async def run_item(item: Any) -> None:
                async with sem:
                    run.context["loop_item"] = item
                    await self._run_from_step(definition, run, step.steps[0])

            await asyncio.gather(*[run_item(item) for item in items], return_exceptions=True)
        else:
            for item in items:
                run.context["loop_item"] = item
                if step.steps:
                    await self._run_from_step(definition, run, step.steps[0])

        run.context.pop("loop_item", None)
        return step.on_success or END

    async def _run_wait(self, run: WorkflowRun, step: WaitStep) -> str:
        if step.delay_seconds:
            await asyncio.sleep(step.delay_seconds)
        return step.on_success or END

    async def _run_transform(self, run: WorkflowRun, step: TransformStep) -> str:
        context = self._build_context(run)
        result = render_value(step.query, context)
        run.context.setdefault("steps", {})[step.id] = {"result": result}
        await self._save_run(run)
        return step.on_success or END

    async def _run_template(self, run: WorkflowRun, step: TemplateStep) -> str:
        context = self._build_context(run)
        rendered = render_value(step.template, context)
        run.context.setdefault("steps", {})[step.id] = {step.output_key: rendered}
        await self._save_run(run)
        return step.on_success or END

    async def _run_ai(self, run: WorkflowRun, step: AIStep) -> str:
        context = self._build_context(run)
        prompt = render_value(step.prompt, context)
        try:
            res = await self.plugin.call_plugin('ai',"generate", {
                "prompt": prompt,
                "service": step.service.value,
                **step.options,
            })
            result = res.get("data", {}) if isinstance(res, dict) else {}
            run.context.setdefault("steps", {})[step.id] = {"result": result}
            await self._save_run(run)
            return step.on_success or END
        except Exception as e:
            if step.on_failure:
                return step.on_failure
            raise

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_context(self, run: WorkflowRun) -> Dict[str, Any]:
        return {
            "trigger": run.trigger_payload,
            "steps": run.context.get("steps", {}),
            "context": run.context,
            "run": {"id": run.run_id, "workflow": run.workflow_name},
            "loop_item": run.context.get("loop_item"),
        }

    def _get_or_create_step_run(self, run: WorkflowRun, step_id: str) -> StepRun:
        if step_id not in run.steps:
            run.steps[step_id] = StepRun(step_id=step_id)
        return run.steps[step_id]

    async def _save_run(self, run: WorkflowRun) -> None:
        if self._store:
            await self._store.save_run(run)

    async def _emit(self, event_name: str, run: WorkflowRun, extra: Optional[dict] = None) -> None:
        data = {
            "run_id": run.run_id,
            "workflow": run.workflow_name,
            "status": run.status.value,
            **(extra or {}),
        }
        try:
            await self.ctx.events.emit(event_name, data)
        except Exception:
            pass  # L'EventBus ne doit jamais faire planter un run
