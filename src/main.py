"""
Plugin : xflow (V2)
===================
Moteur d'automatisation enterprise natif pour XCore.

Actions IPC exposées :
  register / deploy   — Enregistrer un workflow
  unregister          — Supprimer un workflow
  run / trigger       — Déclencher un workflow
  executions          — Lister les runs
  cancel_run / pause  — Annuler un run
  registry            — Catalogue IPC découvert
  ai_generate         — Générer un workflow via IA
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional

from pydantic import BaseModel
from sqlalchemy import select

from .repositories import FlowRunRecord
from .runtime import WorkflowEngine
from .schemas import TriggerType, WorkflowDefinition, WorkflowStatus
from .services import (
    AIWorkflowGenerator,
    DiscoveryService,
    EventCatalogService,
    WorkflowRegistryService,
    WorkflowScheduler,
)
from .services.composites import CompositeService
from .schemas.composite import CompositeNodeDefinition
from .workers import LocalQueue

logger = logging.getLogger("xflow")

XFLOW_QUEUE = "xflow:queue:tasks"

# ---------------------------------------------------------------------------
# SDK imports — adaptés à la structure réelle de XCore
# ---------------------------------------------------------------------------
from xcore.sdk import AutoDispatchMixin, RoutedPlugin, TrustedBase, action, route, validate_payload

# ---------------------------------------------------------------------------
# Payload schemas
# ---------------------------------------------------------------------------

class RegisterPayload(BaseModel):
    definition: Dict[str, Any]


class TriggerPayload(BaseModel):
    workflow_name: str
    payload: Dict[str, Any] = {}


class UnregisterPayload(BaseModel):
    workflow_name: str


class GetRunPayload(BaseModel):
    run_id: str


class CancelRunPayload(BaseModel):
    run_id: str


class ListRunsPayload(BaseModel):
    workflow_name: Optional[str] = None
    limit: int = 50


class AIGeneratePayload(BaseModel):
    prompt: str


# ---------------------------------------------------------------------------
# Plugin principal XFlow V2
# ---------------------------------------------------------------------------

class Plugin(RoutedPlugin, AutoDispatchMixin, TrustedBase):
    """
    Plugin XFlow V2 — moteur d'orchestration central de XCore.

    Lifecycle :
        on_load  → initialise DB, engine, discovery, scheduler, workers
        on_unload → arrête proprement worker task et jobs schedulés
    """
    

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def on_load(self) -> None:
        logger.info("Initialisation de XFlow V2...")
        
        self.db = self.get_service("db")
        self._queue_client = self._init_queue_client()

        await self._create_tables()

        from .repositories import WorkflowStore  # local import pour éviter cycle
        self._store = WorkflowStore(self.db, self._queue_client if self._has_redis else None)
        self._engine = WorkflowEngine(self)
        # Expose _enqueue_run_id sur l'engine pour le scheduler
        self._engine._enqueue_run_id = self._enqueue_run_id  # type: ignore[attr-defined]
        await self._engine.setup(self._store)

        self._discovery = DiscoveryService(self)

        try:
            sched_svc = self.get_service("scheduler")
            self._scheduler = WorkflowScheduler(sched_svc, self._engine)
        except Exception:
            self._scheduler = None
            logger.warning("Scheduler XCore non disponible — cron désactivé.")

        self._registry = WorkflowRegistryService(self._store, self._scheduler)
        await self._registry.sync_scheduler()

        self._ai_gen = AIWorkflowGenerator(self, self._discovery)

        # Initialiser les services composites et events
        self._composite_service = CompositeService(self.db)
        self._event_catalog = EventCatalogService(self.ctx)
        asyncio.create_task(self._event_catalog.discover_events())

        # Abonnement global EventBus
        self.ctx.events.on("*", self._on_any_event)

        # Scan initial des plugins actifs
        asyncio.create_task(self._discovery.scan_all_plugins())

        # Crash recovery
        await self._resume_crashed_runs()

        # Démarrage du worker loop
        self._worker_task = asyncio.create_task(self._worker_loop())

        logger.info("XFlow V2 prêt.")

    async def on_unload(self) -> None:
        if hasattr(self, "_worker_task"):
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

        if self._scheduler:
            for job in self._scheduler.list_scheduled():
                await self._scheduler.unregister(job["job_id"])

        logger.info("XFlow V2 arrêté proprement.")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _init_queue_client(self) -> Any:
        """Retourne le client Redis si disponible, sinon une queue locale."""
        try:
            cache = self.get_service("cache")
            # Vérification rapide que c'est bien un vrai client Redis
            if hasattr(cache, "lpush") and hasattr(cache, "rpop"):
                self._has_redis = True
                return cache
        except Exception:
            pass
        self._has_redis = False
        logger.warning("Cache Redis non disponible — utilisation de la queue locale.")
        return LocalQueue()

    async def _create_tables(self) -> None:
        from .repositories.models import Base
        async with self.db.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def _resume_crashed_runs(self) -> None:
        """Requeue les runs qui étaient RUNNING lors du dernier arrêt."""
        async with self.db.session() as session:
            stmt = select(FlowRunRecord.run_id).where(
                FlowRunRecord.status == WorkflowStatus.RUNNING.value
            )
            result = await session.execute(stmt)
            run_ids = result.scalars().all()

        for rid in run_ids:
            logger.info("Reprise du run crashé: %s", rid)
            await self._enqueue_run_id(rid)

    async def _enqueue_run_id(self, run_id: str) -> None:
        await self._queue_client.lpush(XFLOW_QUEUE, json.dumps({"run_id": run_id}))

    async def _worker_loop(self) -> None:
        """Consommateur principal de la queue de runs."""
        logger.info("Worker XFlow démarré (mode: %s).", "redis" if self._has_redis else "local")
        while True:
            try:
                task_data = await self._queue_client.rpop(XFLOW_QUEUE)
                if not task_data:
                    await asyncio.sleep(0.5)
                    continue

                payload = json.loads(task_data)
                run_id = payload.get("run_id")
                if not run_id:
                    continue

                run = await self._store.get_run(run_id)
                if not run:
                    logger.warning("Run introuvable: %s", run_id)
                    continue

                definition = await self._registry.get(run.workflow_name)
                if not definition:
                    logger.error("Définition introuvable pour le run %s (%s)", run_id, run.workflow_name)
                    continue

                await self._engine.execute_existing(definition, run)

            except asyncio.CancelledError:
                logger.info("Worker XFlow arrêté.")
                break
            except Exception as e:
                logger.exception("Erreur critique dans le worker XFlow: %s", e)
                await asyncio.sleep(2)

    def _ok(self, data: Dict[str, Any] | None = None, **kwargs: Any) -> dict:
        try:
            from xcore.kernel.api.contract import ok
            return ok(data, **kwargs)
        except ImportError:
            return {"status": "success", "data": {**(data or {}), **kwargs}}

    def _error(self, msg: str, code: str | None = None, **kwargs: Any) -> dict:
        try:
            from xcore.kernel.api.contract import error
            return error(msg, code, **kwargs)
        except ImportError:
            return {"status": "error", "message": msg, "code": code, **kwargs}

    # ------------------------------------------------------------------
    # IPC Actions
    # ------------------------------------------------------------------

    @action("register")
    @action("deploy")
    @validate_payload(RegisterPayload)
    async def ipc_register(self, payload: Dict[str, Any]) -> dict:
        try:
            definition = WorkflowDefinition(**payload["definition"])
            definition = await self._registry.register(definition)
            return self._ok(workflow_name=definition.name, message="Workflow déployé avec succès.")
        except Exception as e:
            logger.exception("Erreur lors de l'enregistrement du workflow")
            return self._error(str(e), code="register_error")

    @action("unregister")
    @validate_payload(UnregisterPayload)
    async def ipc_unregister(self, payload: Dict[str, Any]) -> dict:
        try:
            definition = await self._registry.unregister(payload["workflow_name"])
            if not definition:
                return self._error(f"Workflow '{payload['workflow_name']}' introuvable.", code="not_found")
            return self._ok(message=f"Workflow '{definition.name}' supprimé.")
        except Exception as e:
            return self._error(str(e), code="unregister_error")

    @action("run")
    @action("trigger")
    @validate_payload(TriggerPayload)
    async def ipc_trigger(self, payload: Dict[str, Any]) -> dict:
        name = payload["workflow_name"]
        definition = await self._registry.get(name)
        if not definition:
            return self._error(f"Workflow '{name}' inconnu.", code="not_found")

        run = await self._engine.init_run(
            definition,
            trigger_payload=payload.get("payload", {}),
            trigger_type="ipc",
        )
        await self._enqueue_run_id(run.run_id)
        return self._ok(run_id=run.run_id, status=run.status.value)

    @action("get_run")
    @validate_payload(GetRunPayload)
    async def ipc_get_run(self, payload: Dict[str, Any]) -> dict:
        run = await self._store.get_run(payload["run_id"])
        if not run:
            return self._error("Run introuvable.", code="not_found")
        return self._ok(run=run.model_dump(mode="json"))

    @action("executions")
    @action("list_runs")
    async def ipc_list_runs(self, payload: Dict[str, Any]) -> dict:
        runs = await self._store.list_runs(
            workflow_name=payload.get("workflow_name"),
            limit=int(payload.get("limit", 50)),
        )
        return self._ok(runs=[r.model_dump(mode="json") for r in runs])

    @action("cancel_run")
    @action("pause")
    @validate_payload(CancelRunPayload)
    async def ipc_cancel_run(self, payload: Dict[str, Any]) -> dict:
        success = await self._engine.cancel_run(payload["run_id"])
        return self._ok(success=success)

    @action("list_workflows")
    async def ipc_list_workflows(self, payload: Dict[str, Any]) -> dict:
        defs = await self._registry.list_all()
        return self._ok(
            workflows=[
                {
                    "name": d.name,
                    "version": d.version,
                    "trigger": d.trigger.type.value,
                    "description": d.description,
                    "tags": d.tags,
                }
                for d in defs
            ]
        )

    @action("registry")
    async def ipc_registry(self, payload: Dict[str, Any]) -> dict:
        return self._ok(actions=self._discovery.list_available_actions())

    @action("ai_generate")
    @validate_payload(AIGeneratePayload)
    async def ipc_ai_generate(self, payload: Dict[str, Any]) -> dict:
        try:
            workflow = await self._ai_gen.generate_from_prompt(payload["prompt"])
            return self._ok(workflow=workflow)
        except Exception as e:
            return self._error(str(e), code="ai_error")

    @action("xflow_integration")
    async def ipc_xflow_integration(self, payload: Dict[str, Any]) -> dict:
        """Contrat d'intégration XFlow — permet à XFlow de se découvrir lui-même."""
        return self._ok(
            ipc_actions=[
                {"name": "register", "description": "Enregistrer un workflow"},
                {"name": "run", "description": "Déclencher un workflow"},
                {"name": "get_run", "description": "État d'un run"},
                {"name": "list_runs", "description": "Lister les runs"},
                {"name": "cancel_run", "description": "Annuler un run"},
                {"name": "list_workflows", "description": "Lister les workflows"},
                {"name": "registry", "description": "Catalogue des actions IPC"},
                {"name": "ai_generate", "description": "Générer un workflow via IA"},
                {"name": "composite.register", "description": "Enregistrer un composite node"},
                {"name": "composite.list", "description": "Lister les composites"},
                {"name": "composite.expand", "description": "Étendre un composite en steps"},
                {"name": "events.list", "description": "Lister les événements disponibles"},
            ]
        )

    # ------------------------------------------------------------------
    # Composite Nodes IPC Actions
    # ------------------------------------------------------------------

    @action("composite.register")
    async def ipc_composite_register(self, payload: Dict[str, Any]) -> dict:
        """Enregistre un composite node."""
        try:
            definition = CompositeNodeDefinition(**payload)
            saved = await self._composite_service.create(definition)
            return self._ok(
                name=saved.name,
                version=saved.version,
                message="Composite node enregistré."
            )
        except Exception as e:
            logger.exception("Erreur enregistrement composite")
            return self._error(str(e), code="composite_register_error")

    @action("composite.list")
    async def ipc_composite_list(self, payload: Dict[str, Any]) -> dict:
        """Liste tous les composites disponibles."""
        composites = await self._composite_service.list_all()
        return self._ok(composites=[c.model_dump(mode="json") for c in composites])

    @action("composite.expand")
    async def ipc_composite_expand(self, payload: Dict[str, Any]) -> dict:
        """Étend un composite en ses steps internes pour exécution."""
        result = await self._composite_service.expand_composite(
            composite_name=payload.get("composite_name"),
            instance_id=payload.get("instance_id", "instance"),
            inputs=payload.get("inputs", {}),
        )
        if result is None:
            return self._error("Composite introuvable.", code="not_found")
        return self._ok(expansion=result)

    # ------------------------------------------------------------------
    # Events IPC Actions
    # ------------------------------------------------------------------

    @action("events.list")
    async def ipc_events_list(self, payload: Dict[str, Any]) -> dict:
        """Liste tous les événements disponibles dans le catalogue."""
        events = self._event_catalog.list_events()
        return self._ok(events=[e.model_dump(mode="json") for e in events])

    # ------------------------------------------------------------------
    # HTTP Routes
    # ------------------------------------------------------------------

    @route("/flows", method="GET", tags=["xflow"])
    async def http_list_flows(self) -> dict:
        return await self.ipc_list_workflows({})

    @route("/flows", method="POST", tags=["xflow"])
    async def http_deploy(self, body: Dict[str, Any]) -> dict:
        return await self.ipc_register({"definition": body})

    @route("/flows/{workflow_name}", method="GET", tags=["xflow"])
    async def http_get_flow(self, workflow_name: str) -> dict:
        d = await self._registry.get(workflow_name)
        if not d:
            return self._error("Workflow introuvable.", code="not_found")
        return self._ok(workflow=d.model_dump(mode="json"))

    @route("/flows/{workflow_name}", method="DELETE", tags=["xflow"])
    async def http_delete_flow(self, workflow_name: str) -> dict:
        return await self.ipc_unregister({"workflow_name": workflow_name})

    @route("/run/{workflow_name}", method="POST", tags=["xflow"])
    async def http_run(self, workflow_name: str, body: Dict[str, Any] = {}) -> dict:
        return await self.ipc_trigger({"workflow_name": workflow_name, "payload": body})

    @route("/executions", method="GET", tags=["xflow"])
    async def http_executions(self, workflow_name: Optional[str] = None, limit: int = 50) -> dict:
        return await self.ipc_list_runs({"workflow_name": workflow_name, "limit": limit})

    @route("/executions/{run_id}", method="GET", tags=["xflow"])
    async def http_get_execution(self, run_id: str) -> dict:
        return await self.ipc_get_run({"run_id": run_id})

    @route("/executions/{run_id}/cancel", method="POST", tags=["xflow"])
    async def http_cancel(self, run_id: str) -> dict:
        return await self.ipc_cancel_run({"run_id": run_id})

    @route("/registry", method="GET", tags=["xflow"])
    async def http_registry(self) -> dict:
        return await self.ipc_registry({})

    @route("/webhook/{workflow_name}", method="POST", tags=["xflow"])
    async def http_webhook(self, workflow_name: str, body: Dict[str, Any] = {}) -> dict:
        """Endpoint webhook public — déclenche un workflow par nom."""
        definition = await self._registry.get(workflow_name)
        if not definition:
            return self._error(f"Workflow '{workflow_name}' introuvable.", code="not_found")
        if definition.trigger.type not in (TriggerType.WEBHOOK, TriggerType.MANUAL):
            return self._error("Ce workflow n'accepte pas les webhooks.", code="forbidden")
        run = await self._engine.init_run(definition, trigger_payload=body, trigger_type="webhook")
        await self._enqueue_run_id(run.run_id)
        return self._ok(run_id=run.run_id, status=run.status.value)

    @route("/flows/{workflow_name}/graph", method="GET", tags=["xflow"])
    async def http_flow_graph(self, workflow_name: str) -> dict:
        d = await self._registry.get(workflow_name)
        if not d:
            return self._error("Workflow introuvable.", code="not_found")
        return self._ok(graph=d.export_graph())

    # ------------------------------------------------------------------
    # Composite Nodes HTTP Routes
    # ------------------------------------------------------------------

    @route("/composites", method="GET", tags=["composites"])
    async def http_list_composites(self) -> dict:
        """Liste tous les composites disponibles."""
        return await self.ipc_composite_list({})

    @route("/composites", method="POST", tags=["composites"])
    async def http_create_composite(self, body: Dict[str, Any]) -> dict:
        """Crée un nouveau composite node."""
        return await self.ipc_composite_register(body)

    @route("/composites/{name}", method="GET", tags=["composites"])
    async def http_get_composite(self, name: str) -> dict:
        """Récupère un composite par son nom."""
        composite = await self._composite_service.get(name)
        if not composite:
            return self._error("Composite introuvable.", code="not_found")
        return self._ok(composite=composite.model_dump(mode="json"))

    @route("/composites/{name}", method="DELETE", tags=["composites"])
    async def http_delete_composite(self, name: str) -> dict:
        """Supprime un composite."""
        success = await self._composite_service.delete(name)
        if not success:
            return self._error("Composite introuvable.", code="not_found")
        return self._ok(message=f"Composite '{name}' supprimé.")

    @route("/composites/{name}/expand", method="POST", tags=["composites"])
    async def http_expand_composite(self, name: str, body: Dict[str, Any]) -> dict:
        """Étend un composite en ses steps internes."""
        return await self.ipc_composite_expand({
            "composite_name": name,
            "instance_id": body.get("instance_id", "instance"),
            "inputs": body.get("inputs", {}),
        })

    # ------------------------------------------------------------------
    # Events HTTP Routes
    # ------------------------------------------------------------------

    @route("/events", method="GET", tags=["events"])
    async def http_list_events(self) -> dict:
        """Liste tous les événements disponibles."""
        return await self.ipc_events_list({})

    @route("/events/refresh", method="POST", tags=["events"])
    async def http_refresh_events(self) -> dict:
        """Rafraîchit le catalogue d'événements."""
        await self._event_catalog.refresh()
        return self._ok(message="Catalogue événements rafraîchi.")

    def get_router(self) -> Any | None:
        return self.RouterIn()

    # ------------------------------------------------------------------
    # Event Bus handlers
    # ------------------------------------------------------------------

    async def _on_any_event(self, event: Any) -> None:
        event_name = getattr(event, "name", None) or (
            event.get("name") if isinstance(event, dict) else None
        )
        if not event_name:
            return

        definitions = await self._registry.list_event_handlers(event_name)
        for d in definitions:
            trigger_data = (
                getattr(event, "data", {})
                if not isinstance(event, dict)
                else event.get("data", {})
            )
            run = await self._engine.init_run(d, trigger_payload=trigger_data, trigger_type="event")
            await self._enqueue_run_id(run.run_id)
