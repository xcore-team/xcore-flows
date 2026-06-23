"""
Service de registre des composite nodes — XFlow V2.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import select

from ..repositories import Base
from ..repositories.models import CompositeRecord
from ..schemas.composite import (
    CompositeInput,
    CompositeNodeDefinition,
    CompositeOutput,
    CompositeRegistryEntry,
)

logger = logging.getLogger("xflow.composites")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class CompositeService:
    def __init__(self, db: Any) -> None:
        self._db = db

    async def create(self, tenant_id: str, definition: CompositeNodeDefinition) -> CompositeNodeDefinition:
        payload = definition.model_dump(mode="json")

        async with self._db.session() as session:
            result = await session.execute(
                select(CompositeRecord).where(
                    CompositeRecord.tenant_id == tenant_id,
                    CompositeRecord.name == definition.name,
                )
            )
            existing = result.scalar_one_or_none()

            if existing:
                definition.version = self._bump_version(existing.version)
                payload["version"] = definition.version
                existing.version = definition.version
                existing.description = definition.description
                existing.icon = definition.icon
                existing.category = definition.category
                existing.definition = payload
                existing.updated_at = _utcnow()
                logger.info("Composite '%s' mis à jour vers v%s (tenant=%s)", definition.name, definition.version, tenant_id)
            else:
                record = CompositeRecord(
                    tenant_id=tenant_id,
                    name=definition.name,
                    version=definition.version,
                    description=definition.description,
                    icon=definition.icon,
                    category=definition.category,
                    definition=payload,
                    is_active=True,
                    created_at=_utcnow(),
                    updated_at=_utcnow(),
                )
                session.add(record)
                logger.info("Composite '%s' v%s enregistré (tenant=%s).", definition.name, definition.version, tenant_id)

            await session.commit()

        return definition

    async def get(self, tenant_id: str, name: str) -> Optional[CompositeNodeDefinition]:
        async with self._db.session() as session:
            result = await session.execute(
                select(CompositeRecord).where(
                    CompositeRecord.tenant_id == tenant_id,
                    CompositeRecord.name == name,
                    CompositeRecord.is_active == True,
                )
            )
            record = result.scalar_one_or_none()
            if record is None:
                return None
            return CompositeNodeDefinition.model_validate(record.definition)

    async def list_all(self, tenant_id: str) -> List[CompositeRegistryEntry]:
        async with self._db.session() as session:
            result = await session.execute(
                select(CompositeRecord).where(
                    CompositeRecord.tenant_id == tenant_id,
                    CompositeRecord.is_active == True,
                ).order_by(CompositeRecord.name)
            )
            records = result.scalars().all()

        entries = []
        for r in records:
            definition = CompositeNodeDefinition.model_validate(r.definition)
            entries.append(
                CompositeRegistryEntry(
                    name=r.name,
                    version=r.version,
                    description=r.description,
                    icon=r.icon,
                    category=r.category,
                    inputs=definition.inputs,
                    outputs=definition.outputs,
                    step_count=len(definition.steps),
                    tags=definition.tags,
                )
            )
        return entries

    async def delete(self, tenant_id: str, name: str) -> bool:
        async with self._db.session() as session:
            result = await session.execute(
                select(CompositeRecord).where(
                    CompositeRecord.tenant_id == tenant_id,
                    CompositeRecord.name == name,
                )
            )
            record = result.scalar_one_or_none()
            if record is None:
                return False
            record.is_active = False
            record.updated_at = _utcnow()
            await session.commit()

        logger.info("Composite '%s' désactivé (tenant=%s).", name, tenant_id)
        return True

    async def expand_composite(
        self,
        tenant_id: str,
        composite_name: str,
        instance_id: str,
        inputs: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        composite = await self.get(tenant_id, composite_name)
        if composite is None:
            return None

        expanded_steps = []
        step_remap: Dict[str, str] = {}

        for step in composite.steps:
            new_id = f"{instance_id}_{step.id}"
            step_remap[step.id] = new_id
            step_data = step.model_dump(mode="json")
            step_data["id"] = new_id

            if step.id in composite.input_mappings:
                mapping = composite.input_mappings[step.id]
                for input_name, target_field in mapping.items():
                    if input_name in inputs:
                        if "payload" not in step_data:
                            step_data["payload"] = {}
                        step_data["payload"][target_field] = inputs[input_name]

            if step_data.get("on_success"):
                step_data["on_success"] = f"{instance_id}_{step_data['on_success']}"
            if step_data.get("on_failure"):
                step_data["on_failure"] = f"{instance_id}_{step_data['on_failure']}"

            expanded_steps.append(step_data)

        entry_step = f"{instance_id}_{composite.steps[0].id}" if composite.steps else None
        output_mapping = {
            out.name: {"step_id": f"{instance_id}_{out.source_step}", "field": out.source_field}
            for out in composite.outputs
        }

        return {"steps": expanded_steps, "entry_step": entry_step, "output_mapping": output_mapping}

    def _bump_version(self, version: str) -> str:
        parts = version.split(".")
        if len(parts) >= 2:
            try:
                parts[1] = str(int(parts[1]) + 1)
                return ".".join(parts)
            except ValueError:
                pass
        return version + ".1"
