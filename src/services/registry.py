"""
Service de registre des workflows — XFlow V2.
Gère le CRUD des définitions et la synchronisation avec le scheduler.
"""
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

    async def register_yaml(self, yaml_content: str) -> WorkflowDefinition:
        """Enregistre un workflow depuis un contenu YAML."""
        data = yaml.safe_load(yaml_content)
        definition = WorkflowDefinition(**data)
        return await self.register(definition)

    async def register(self, definition: WorkflowDefinition) -> WorkflowDefinition:
        """Persiste un workflow. Enregistre aussi le job cron si nécessaire."""
        if (
            definition.trigger.type == TriggerType.SCHEDULE
            and self._scheduler is not None
        ):
            job_id = await self._scheduler.register(definition)
            if job_id:
                definition.trigger.initial_payload["_job_id"] = job_id
                logger.info("Workflow '%s' — job schedulé : %s", definition.name, job_id)

        saved = await self._store.save_definition(definition)
        logger.info("Workflow '%s' v%s enregistré.", definition.name, definition.version)
        return saved

    async def unregister(self, workflow_name: str) -> Optional[WorkflowDefinition]:
        """Supprime un workflow et son job schedulé associé."""
        definition = await self._store.get_definition(workflow_name)
        if definition is None:
            return None

        if self._scheduler is not None:
            await self._scheduler.unregister_by_workflow(workflow_name)

        await self._store.delete_definition(workflow_name)
        logger.info("Workflow '%s' supprimé.", workflow_name)
        return definition

    async def get(self, workflow_name: str) -> Optional[WorkflowDefinition]:
        return await self._store.get_definition(workflow_name)

    async def list_all(self) -> List[WorkflowDefinition]:
        return await self._store.list_definitions()

    async def list_event_handlers(self, event_name: str) -> List[WorkflowDefinition]:
        """Retourne les workflows déclenchés par un événement donné."""
        return await self._store.list_event_definitions(event_name)

    async def sync_scheduler(self) -> None:
        """Réenregistre tous les workflows SCHEDULE au démarrage."""
        if self._scheduler is None:
            return
        definitions = await self._store.list_definitions()
        count = 0
        for definition in definitions:
            if definition.trigger.type == TriggerType.SCHEDULE:
                await self._scheduler.register(definition)
                count += 1
        if count:
            logger.info("%d workflow(s) schedulé(s) rechargés.", count)
