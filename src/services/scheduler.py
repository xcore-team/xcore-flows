"""
Intégration du scheduler XCore (APScheduler) pour XFlow V2.
"""
from __future__ import annotations

import logging
from functools import partial
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ..schemas.workflow import TriggerType, WorkflowDefinition

if TYPE_CHECKING:
    from ..runtime.engine import WorkflowEngine

logger = logging.getLogger("xflow.scheduler")


class WorkflowScheduler:
    def __init__(self, scheduler_service: Any, engine: "WorkflowEngine") -> None:
        self._sched = scheduler_service
        self._engine = engine
        self._jobs: Dict[str, str] = {}  # job_id -> workflow_name

    async def register(self, tenant_id: str, definition: WorkflowDefinition) -> Optional[str]:
        trigger = definition.trigger
        if trigger.type != TriggerType.SCHEDULE:
            return None

        # Le tenant fait partie de l'identifiant du job : deux organisations qui déploient le même workflow ne doivent pas
        # s'écraser l'une l'autre (`replace_existing=True` ne garderait que la dernière).
        job_id = f"wf_sched_{tenant_id}_{definition.name}"
        await self.unregister(job_id)

        # `SchedulerService` nomme son paramètre `job_id` (il pose lui-même `id=` pour APScheduler) : passer `id` ici provoquait
        # « multiple values for keyword argument 'id' ». `misfire_grace_time`, lui, va bien jusqu'au déclencheur.
        base_kwargs: Dict[str, Any] = {
            "job_id": job_id,
            "replace_existing": True,
            "misfire_grace_time": 60,
        }

        if trigger.cron:
            parts = trigger.cron.strip().split()
            if len(parts) == 5:
                minute, hour, day, month, dow = parts
            elif len(parts) == 6:
                _, minute, hour, day, month, dow = parts
            else:
                logger.error("Cron invalide pour '%s': %s", definition.name, trigger.cron)
                return None

            self._sched.add_job(
                func=partial(self._fire, tenant_id, definition),
                trigger="cron",
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=dow,
                **base_kwargs,
            )
            logger.info("Workflow '%s' schedulé (cron: %s)", definition.name, trigger.cron)

        elif trigger.interval_seconds:
            self._sched.add_job(
                func=partial(self._fire, tenant_id, definition),
                trigger="interval",
                seconds=trigger.interval_seconds,
                **base_kwargs,
            )
            logger.info(
                "Workflow '%s' schedulé (interval: %ds)",
                definition.name,
                trigger.interval_seconds,
            )
        else:
            logger.warning("Trigger SCHEDULE sans cron ni interval pour '%s'", definition.name)
            return None

        self._jobs[job_id] = definition.name
        return job_id

    async def unregister(self, job_id: str) -> bool:
        try:
            self._sched.remove_job(job_id)
            self._jobs.pop(job_id, None)
            return True
        except Exception:
            return False

    async def unregister_by_workflow(self, tenant_id: str, workflow_name: str) -> bool:
        # Le MÊME identifiant que `register` : sans l'organisation, supprimer un flux planifié laissait son job tourner.
        return await self.unregister(f"wf_sched_{tenant_id}_{workflow_name}")

    def list_scheduled(self) -> List[Dict[str, str]]:
        return [{"job_id": jid, "workflow_name": wn} for jid, wn in self._jobs.items()]

    async def _fire(self, tenant_id: str, definition: WorkflowDefinition) -> None:
        """Callback déclenché par APScheduler — initialise et enqueue le run.

        Il n'y a personne derrière un cron : le run s'exécute avec les droits de celui qui a DÉPLOYÉ le workflow, relus à
        chaque exécution (`WorkflowEngine._principal`). Et il tourne dans l'organisation du workflow, pas dans « default ».
        """
        logger.info("Déclenchement schedulé du workflow '%s' (tenant=%s)", definition.name, tenant_id)
        run = await self._engine.init_run(
            definition,
            tenant_id=tenant_id,
            trigger_payload={
                **definition.trigger.initial_payload,
                "_triggered_by": "scheduler",
            },
            trigger_type="scheduler",
        )
        if definition.deployed_by:
            run.context[self._engine.ACTOR_KEY] = definition.deployed_by
            await self._engine._save_run(run)
        # On enqueue via le mécanisme standard du plugin parent
        # Le plugin expose _enqueue_run_id via l'engine
        if hasattr(self._engine, "_enqueue_run_id"):
            await self._engine._enqueue_run_id(run.run_id)
        else:
            # Fallback direct si pas de queue
            await self._engine.execute_existing(definition, run)
