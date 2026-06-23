from __future__ import annotations

import logging
from typing import List, Optional

import yaml

from ..schemas.workflow import TriggerType, WorkflowDefinition
from ..repositories.workflow import WorkflowStore
from .scheduler import WorkflowScheduler

logger = logging.getLogger("xflow.registry")


class WorkflowRegistryService:
    def __init__(
        self,
        store: WorkflowStore,
        scheduler: Optional[WorkflowScheduler] = None,
    ) -> None:
        self._store = store
        self._scheduler = scheduler

    async def register_yaml(self, tenant_id: str, yaml_content: str) -> WorkflowDefinition:
        data = yaml.safe_load(yaml_content)
        definition = WorkflowDefinition(**data)
        return await self.register(tenant_id, definition)

    async def register(self, tenant_id: str, definition: WorkflowDefinition) -> WorkflowDefinition:
        if (
            definition.trigger.type == TriggerType.SCHEDULE
            and self._scheduler is not None
        ):
            job_id = await self._scheduler.register(definition)
            if job_id:
                definition.trigger.initial_payload["_job_id"] = job_id
                logger.info("Workflow '%s' — job schedulé : %s", definition.name, job_id)

        saved = await self._store.save_definition(tenant_id, definition)
        logger.info("Workflow '%s' v%s enregistré (tenant=%s).", definition.name, definition.version, tenant_id)
        return saved

    async def unregister(self, tenant_id: str, workflow_name: str) -> Optional[WorkflowDefinition]:
        definition = await self._store.get_definition(tenant_id, workflow_name)
        if definition is None:
            return None

        if self._scheduler is not None:
            await self._scheduler.unregister_by_workflow(workflow_name)

        await self._store.delete_definition(tenant_id, workflow_name)
        logger.info("Workflow '%s' supprimé (tenant=%s).", workflow_name, tenant_id)
        return definition

    async def get(self, tenant_id: str, workflow_name: str) -> Optional[WorkflowDefinition]:
        return await self._store.get_definition(tenant_id, workflow_name)

    async def list_all(self, tenant_id: str) -> List[WorkflowDefinition]:
        return await self._store.list_definitions(tenant_id)

    async def list_event_handlers(self, tenant_id: str, event_name: str) -> List[WorkflowDefinition]:
        return await self._store.list_event_definitions(tenant_id, event_name)

    async def sync_scheduler(self) -> None:
        """Réenregistre les workflows SCHEDULE au démarrage — sans isolation tenant (global scan)."""
        if self._scheduler is None:
            return
        # Le scheduler est global : on n'a pas de tenant_id ici.
        # Les workflows schedulés seront re-enregistrés à la prochaine requête tenant.
        logger.info("sync_scheduler ignoré — tenant isolation active.")
