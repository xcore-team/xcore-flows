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
import os
from typing import Any, Dict, List, Optional
from pathlib import Path
from sqlalchemy import select
from .repositories.models import Base
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

from fastapi import Depends, Request
from fastapi.responses import JSONResponse
from .services.webhook_auth import (
    MAX_BODY_BYTES,
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    WebhookGuard,
    WebhookSecretStore,
    verify,
    verify_dummy,
)
from xcore.sdk import AutoDispatchMixin, RoutedPlugin, TrustedBase, action, route, schema, get_logger
from xcore.kernel.api.rbac import get_current_user, require_permission, AuthPayload

logger = get_logger(__name__)


def _tenant_of(user: Any) -> str:
    """L'organisation d'un utilisateur authentifié : `user.tenant_id` (xauth) ou `tenant_id` à la racine du jeton (autres backends).
    Sans organisation, refus (403) plutôt qu'un repli sur « default » qui mêlerait les flux de tout le monde."""
    from fastapi import HTTPException

    tenant = ((user or {}).get("user") or {}).get("tenant_id") or (user or {}).get("tenant_id")
    if not tenant:
        raise HTTPException(403, "Organisation inconnue pour cet utilisateur")
    return str(tenant)


def _merged_actions(from_file: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Le catalogue des actions qu'une étape peut appeler. Celui d'xflow vient d'un FICHIER (`schemas.json`, écrit par
    `xcli plugin security validate --save`) presque toujours absent : l'éditeur du Desk n'avait alors rien à proposer. On y ajoute le
    registre VIVANT d'xcore, où chaque plugin chargé inscrit ses actions (`AutoDispatchMixin`), le fichier gagnant sur un doublon."""
    out = {a.get("qualified_name"): a for a in from_file if a.get("qualified_name")}
    try:
        from xcore.kernel.schema.registry import schema_registry
        live = schema_registry.all()
    except Exception:  # noqa: BLE001 - un registre illisible : le catalogue du fichier seul
        live = []
    for sch in live:
        key = f"{sch.plugin}.{sch.action}"
        out.setdefault(key, {
            "qualified_name": key, "plugin": sch.plugin, "action": sch.action, "version": sch.version or "",
            "description": sch.description or "", "input_schema": {k: str(v) for k, v in (sch.input or {}).items()},
            "output_schema": {k: str(v) for k, v in (sch.output or {}).items()},
        })
    return sorted(out.values(), key=lambda a: a["qualified_name"])


def _context_tenant() -> Optional[str]:
    """L'organisation de l'appel en cours (posée par xcore pour un appel de plugin ou une requête), ou None."""
    try:
        from xcore.kernel.tenancy import _current_tenant_id
    except Exception:  # noqa: BLE001
        return None
    tenant = _current_tenant_id.get(None)
    return None if tenant in (None, "", "default") else str(tenant)
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
    async def _initialize_tables(self, db) -> None:
        import logging as _log

        from xcore.services.database.migrations import MigrationRunner
        logger.info("Tables créées / vérifiées")
        migrations_dir = Path(__file__).parent.parent / "migrations"
        # str(url) masque le mot de passe (« *** ») : le runner, qui ouvre sa propre connexion,
        # échouait alors silencieusement sur toute base protégée par mot de passe.
        runner = MigrationRunner(
            db_url=db.engine.url.render_as_string(hide_password=False),
            migrations_dir=migrations_dir,
        )
        try:
            await runner.init(autogenerate=False, message="first_initialisation")
            await runner.upgrade()
        except Exception as exc:
            # Une chaîne Alembic en échec laisse le schéma à un état inconnu : en production on refuse
            # de démarrer (même règle que xauth) ; hors production on journalise et on poursuit,
            # create_all ayant déjà convergé.
            logger.exception("[xflows] Échec de la migration Alembic : %s", exc, error=str(exc))
            if os.getenv("MODE", "production") == "production":
                raise RuntimeError(
                    f"[xflows] migration Alembic en échec, démarrage refusé en production : {exc}"
                ) from exc
            logger.warning("[xflows] Migration en échec ignorée (MODE != production), démarrage poursuivi")
    
    async def on_load(self) -> None:
        logger.info("Initialisation de XFlow V2...")

        self.db = self.get_service("db")
        self._queue_client = self._init_queue_client()

        await self._initialize_tables(self.db)

        await self._create_tables()

        # Service WebSocket temps réel (optionnel — absent en tests/déploiements sans WS).
        # Même convention que xcompany/tasks : canal "xflow" déclaré dans integration.yaml.
        # `get_service` LÈVE quand le service manque (il ne rend pas None) : un hôte sans extension WebSocket
        # — c'est le cas de xcore-doctype — faisait donc échouer tout le chargement du plugin.
        try:
            self._ws = self.get_service("ext.websocket")
        except Exception:  # noqa: BLE001 - optionnel par contrat, voir le commentaire ci-dessus
            self._ws = None
        if self._ws is None:
            logger.info("Service WebSocket absent — diffusion temps réel désactivée.")

        from .repositories import WorkflowStore
        self._store = WorkflowStore(self.db, self._queue_client if self._has_redis else None)
        self._engine = WorkflowEngine(self)
        self._engine._enqueue_run_id = self._enqueue_run_id  # type: ignore[attr-defined]
        await self._engine.setup(self._store)

        self._discovery = DiscoveryService()
        self._discovery.load()

        try:
            sched_svc = self.get_service("scheduler")
            self._scheduler = WorkflowScheduler(sched_svc, self._engine)
        except Exception:
            self._scheduler = None
            logger.warning("Scheduler XCore non disponible — cron désactivé.")

        self._webhook_secrets = WebhookSecretStore(self.db)
        self._webhook_guard = WebhookGuard()

        self._registry = WorkflowRegistryService(self._store, self._scheduler)
        await self._registry.sync_scheduler()

        self._ai_gen = AIWorkflowGenerator(self, self._discovery)

        self._composite_service = CompositeService(self.db)
        self._event_catalog = EventCatalogService(self._discovery, ctx=self.ctx)
        await self._declare_rbac()

        @self.ctx.events.on("*",)
        async def _(event):
            # Découplé de la requête émettrice : le déclenchement (écriture DB du run)
            # ne doit pas s'exécuter dans la transaction de l'appelant (ex. login),
            # sous peine de contention de verrou ("database is locked" en SQLite).
            asyncio.create_task(self._on_any_event(event=event))

        await self._resume_crashed_runs()

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
        try:
            cache = self.get_service("cache")
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
        crashed = await self._store.list_crashed_runs()
        for run_id, _tenant_id in crashed:
            logger.info("Reprise du run crashé: %s (tenant=%s)", run_id, _tenant_id)
            await self._enqueue_run_id(run_id)

    async def _enqueue_run_id(self, run_id: str) -> None:
        await self._queue_client.lpush(XFLOW_QUEUE, json.dumps({"run_id": run_id}))

    async def _worker_loop(self) -> None:
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

                definition = await self._registry.get(run.tenant_id, run.workflow_name)
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

    async def handle(self, action: str, payload: Dict[str, Any]) -> dict:
        """Toute action IPC passe ici d'abord. Les actions prennent `tenant_id` dans la CHARGE UTILE ; un appel de plugin porte
        aussi l'organisation de son contexte (`ctx.caller(..., tenant_id=...)`, posée par xcore dans `_current_tenant_id`).
        Les deux doivent concorder : sinon n'importe quel plugin déploierait, lancerait ou lirait les flux d'une AUTRE
        organisation en écrivant son identifiant dans la charge utile. Un appel sans organisation de contexte (opérateur,
        « default ») n'est pas lié."""
        if isinstance(payload, dict) and payload.get("tenant_id"):
            bound = _context_tenant()
            if bound and payload["tenant_id"] != bound:
                return self._error("L'organisation de la charge utile n'est pas celle de l'appel.", code="tenant_mismatch")
        return await super().handle(action, payload)

    @action("register")
    @schema(
        version="1.0",
        input={"tenant_id": (str, ...), "definition": (dict, ...)},
        output={"workflow_name": str, "message": str, "webhook_secret": Optional[str]},
        type_response="dict",
        unset=False,
    )
    async def ipc_register(self, payload: Dict[str, Any]) -> dict:
        # Identité de l'appelant IPC : la ContextVar du superviseur, jamais la charge utile (elle serait forgeable, et de
        # toute façon `@schema` jette les clés non déclarées — c'est ainsi que `deployed_by` disparaissait).
        try:
            from xcore.kernel.api.auth import get_current_principal
            who = (get_current_principal() or {}).get("sub")
        except Exception:  # noqa: BLE001
            who = None
        return await self._deploy(payload["tenant_id"], payload["definition"], who)

    async def _deploy(self, tenant_id: str, body: Dict[str, Any], deployed_by: Optional[str]) -> dict:
        """Enregistre un workflow en retenant QUI le déploie : c'est son identité que porteront ses exécutions planifiées."""
        try:
            definition = WorkflowDefinition(**body)
            # TOUJOURS celui de l'appel, jamais celui du corps : un `deployed_by` écrit dans la définition, gardé faute
            # d'identité, faisait tourner les runs planifiés et d'événement AU NOM de qui l'on voulait.
            definition.deployed_by = deployed_by
            definition = await self._registry.register(tenant_id, definition)
            extra: Dict[str, Any] = {}
            if definition.trigger.type == TriggerType.WEBHOOK:
                # Généré à la première activation ; exposé UNE seule fois (None ensuite).
                new_secret = await self._webhook_secrets.ensure(tenant_id, definition.name)
                if new_secret:
                    extra["webhook_secret"] = new_secret
                    extra["webhook_secret_notice"] = (
                        "Conservez ce secret : il ne sera plus affiché. "
                        "Régénérable via POST /flows/{name}/webhook/secret."
                    )
            return self._ok(workflow_name=definition.name, message="Workflow déployé avec succès.", **extra)
        except Exception as e:
            logger.exception("Erreur lors de l'enregistrement du workflow")
            return self._error(str(e), code="register_error")

    # Deux `@action` empilés sur une méthode n'en enregistrent qu'UNE (le décorateur du dessus écrase `_xcore_action`) : `deploy`,
    # `trigger`, `list_runs` et `pause`, documentés, n'existaient donc pas (« Action 'trigger' inconnue », mesuré). De vrais alias.
    @action("deploy")
    async def ipc_deploy(self, payload: Dict[str, Any]) -> dict:
        return await self.ipc_register(payload)

    @action("trigger")
    async def ipc_trigger_alias(self, payload: Dict[str, Any]) -> dict:
        return await self.ipc_trigger(payload)

    @action("list_runs")
    async def ipc_list_runs_alias(self, payload: Dict[str, Any]) -> dict:
        return await self.ipc_list_runs(payload)

    @action("pause")
    async def ipc_pause(self, payload: Dict[str, Any]) -> dict:
        return await self.ipc_cancel_run(payload)

    @action("unregister")
    @schema(
        version="1.0",
        input={"tenant_id": (str, ...), "workflow_name": (str, ...)},
        output={"message": str},
        type_response="dict",
        unset=False,
    )
    async def ipc_unregister(self, payload: Dict[str, Any]) -> dict:
        try:
            tenant_id = payload["tenant_id"]
            definition = await self._registry.unregister(tenant_id, payload["workflow_name"])
            if not definition:
                return self._error(f"Workflow '{payload['workflow_name']}' introuvable.", code="not_found")
            await self._webhook_secrets.delete(tenant_id, definition.name)
            return self._ok(message=f"Workflow '{definition.name}' supprimé.")
        except Exception as e:
            return self._error(str(e), code="unregister_error")

    @action("run")
    @schema(
        version="1.0",
        input={"tenant_id": (str, ...), "workflow_name": (str, ...), "payload": (dict, {})},
        output={"run_id": str, "status": str},
        type_response="dict",
        unset=False,
    )
    async def ipc_trigger(self, payload: Dict[str, Any]) -> dict:
        tenant_id = payload["tenant_id"]
        name = payload["workflow_name"]
        definition = await self._registry.get(tenant_id, name)
        if not definition:
            return self._error(f"Workflow '{name}' inconnu.", code="not_found")

        # L'identité d'un appel IPC vient de la ContextVar posée par le superviseur — JAMAIS de la charge utile, qui serait
        # forgeable par l'appelant. Une route HTTP, elle, passe son `AuthPayload` authentifié à `_trigger` (voir `http_run`).
        try:
            from xcore.kernel.api.auth import get_current_principal
            principal = get_current_principal()
        except Exception:  # noqa: BLE001 - hors requête : personne à retenir
            principal = None
        return await self._trigger(tenant_id, name, payload.get("payload", {}), principal, "ipc")

    async def _trigger(
        self,
        tenant_id: str,
        name: str,
        trigger_payload: Dict[str, Any],
        principal: Optional[Dict[str, Any]],
        trigger_type: str,
    ) -> dict:
        """Crée le run, retient l'identité de son déclencheur, puis le met en file.

        Le run s'exécute EN TÂCHE DE FOND : la réponse part en « pending » et il n'y a plus aucune requête quand les steps
        tournent. C'est ici, et seulement ici, qu'on peut encore savoir qui a déclenché — d'où la capture. Elle reste en
        mémoire (voir `WorkflowEngine.remember_actor`) et disparaît avec le run.
        """
        definition = await self._registry.get(tenant_id, name)
        if not definition:
            return self._error(f"Workflow \'{name}\' inconnu.", code="not_found")

        run = await self._engine.init_run(
            definition,
            tenant_id=tenant_id,
            trigger_payload=trigger_payload,
            trigger_type=trigger_type,
        )
        # On ne garde que l'IDENTIFIANT : ses droits seront redemandés à xauth à chaque exécution (voir
        # `WorkflowEngine._principal`). Sans déclencheur humain — un cron —, on retient celui qui a déployé le workflow.
        actor = (principal or {}).get("sub") or getattr(definition, "deployed_by", None)
        if actor:
            run.context[self._engine.ACTOR_KEY] = actor
            await self._engine._save_run(run)
        await self._enqueue_run_id(run.run_id)
        return self._ok(run_id=run.run_id, status=run.status.value)

    @action("webhook.rotate_secret")
    @schema(
        version="1.0",
        input={"tenant_id": (str, ...), "workflow_name": (str, ...)},
        output={"webhook_secret": str},
        type_response="dict",
        unset=False,
    )
    async def ipc_webhook_rotate(self, payload: Dict[str, Any]) -> dict:
        """(Re)génère le secret de signature du webhook ; l'ancien est invalidé. Affiché une seule fois."""
        tenant_id, name = payload["tenant_id"], payload["workflow_name"]
        definition = await self._registry.get(tenant_id, name)
        if not definition:
            return self._error("Workflow introuvable.", code="not_found")
        if definition.trigger.type not in (TriggerType.WEBHOOK, TriggerType.MANUAL):
            return self._error("Ce workflow n'accepte pas les webhooks.", code="forbidden")
        secret = await self._webhook_secrets.rotate(tenant_id, name)
        return self._ok(
            webhook_secret=secret,
            notice="Conservez ce secret : il ne sera plus affiché.",
        )

    @action("webhook.status")
    @schema(
        version="1.0",
        input={"tenant_id": (str, ...), "workflow_name": (str, ...)},
        output={"configured": bool},
        type_response="dict",
        unset=False,
    )
    async def ipc_webhook_status(self, payload: Dict[str, Any]) -> dict:
        tenant_id, name = payload["tenant_id"], payload["workflow_name"]
        if not await self._registry.get(tenant_id, name):
            return self._error("Workflow introuvable.", code="not_found")
        meta = await self._webhook_secrets.status(tenant_id, name)
        if meta is None:
            return self._ok(
                configured=False,
                message=(
                    "Webhook désactivé : aucun secret de signature. "
                    "Générez-en un via POST /flows/{name}/webhook/secret."
                ),
            )
        return self._ok(configured=True, **meta)

    @action("get_run")
    @schema(
        version="1.0",
        input={"tenant_id": (str, ...), "run_id": (str, ...)},
        output={"run": dict},
        type_response="dict",
        unset=False,
    )
    async def ipc_get_run(self, payload: Dict[str, Any]) -> dict:
        run = await self._store.get_run(payload["run_id"])
        if not run:
            return self._error("Run introuvable.", code="not_found")
        if run.tenant_id != payload["tenant_id"]:
            return self._error("Run introuvable.", code="not_found")
        return self._ok(run=run.model_dump(mode="json"))

    @action("executions")
    @schema(
        version="1.0",
        input={"tenant_id": (str, ...), "workflow_name": (Optional[str], None), "limit": (int, 50)},
        output={"runs": list},
        type_response="dict",
        unset=False,
    )
    async def ipc_list_runs(self, payload: Dict[str, Any]) -> dict:
        tenant_id = payload["tenant_id"]
        runs = await self._store.list_runs(
            tenant_id=tenant_id,
            workflow_name=payload.get("workflow_name"),
            limit=int(payload.get("limit", 50)),
        )
        return self._ok(runs=[r.model_dump(mode="json") for r in runs])

    @action("cancel_run")
    @schema(
        version="1.0",
        input={"tenant_id": (str, ...), "run_id": (str, ...)},
        output={"success": bool},
        type_response="dict",
        unset=False,
    )
    async def ipc_cancel_run(self, payload: Dict[str, Any]) -> dict:
        run = await self._store.get_run(payload["run_id"])
        if not run or run.tenant_id != payload["tenant_id"]:
            return self._error("Run introuvable.", code="not_found")
        success = await self._engine.cancel_run(payload["run_id"])
        return self._ok(success=success)

    @action("list_workflows")
    @schema(
        version="1.0",
        input={"tenant_id": (str, ...)},
        output={"workflows": list},
        type_response="dict",
        unset=False,
    )
    async def ipc_list_workflows(self, payload: Dict[str, Any]) -> dict:
        tenant_id = payload["tenant_id"]
        defs = await self._registry.list_all(tenant_id)
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
    @schema(version="1.0", output={"actions": list})
    async def ipc_registry(self, payload: Dict[str, Any]) -> dict:
        return self._ok(actions=_merged_actions(self._discovery.list_available_actions()))

    @action("ai_generate")
    @schema(
        version="1.0",
        input={"prompt": (str, ...)},
        output={"workflow": dict},
        type_response="dict",
        unset=False,
    )
    async def ipc_ai_generate(self, payload: Dict[str, Any]) -> dict:
        try:
            workflow = await self._ai_gen.generate_from_prompt(payload["prompt"])
            return self._ok(workflow=workflow)
        except Exception as e:
            return self._error(str(e), code="ai_error")

    @action("xflow_integration")
    @schema(version="1.0", output={"ipc_actions": list})
    async def ipc_xflow_integration(self, payload: Dict[str, Any]) -> dict:
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
    @schema(version="1.0", input={"tenant_id": (str, ...), "name": (str, ...), "steps": (list, [])}, output={"name": str, "version": str, "message": str})
    async def ipc_composite_register(self, payload: Dict[str, Any]) -> dict:
        try:
            tenant_id = payload.get("tenant_id")
            if not tenant_id:
                return self._error("tenant_id requis.", code="missing_tenant")
            definition = CompositeNodeDefinition(**{k: v for k, v in payload.items() if k != "tenant_id"})
            saved = await self._composite_service.create(tenant_id, definition)
            return self._ok(name=saved.name, version=saved.version, message="Composite node enregistré.")
        except Exception as e:
            logger.exception("Erreur enregistrement composite")
            return self._error(str(e), code="composite_register_error")

    @action("composite.list")
    @schema(version="1.0", input={"tenant_id": (str, ...)}, output={"composites": list})
    async def ipc_composite_list(self, payload: Dict[str, Any]) -> dict:
        tenant_id = payload.get("tenant_id")
        if not tenant_id:
            return self._error("tenant_id requis.", code="missing_tenant")
        composites = await self._composite_service.list_all(tenant_id)
        return self._ok(composites=[c.model_dump(mode="json") for c in composites])

    @action("composite.expand")
    @schema(version="1.0", input={"tenant_id": (str, ...), "composite_name": (str, ...), "instance_id": (str, "instance"), "inputs": (dict, {})}, output={"expansion": dict})
    async def ipc_composite_expand(self, payload: Dict[str, Any]) -> dict:
        tenant_id = payload.get("tenant_id")
        if not tenant_id:
            return self._error("tenant_id requis.", code="missing_tenant")
        result = await self._composite_service.expand_composite(
            tenant_id=tenant_id,
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
    @schema(version="1.0", output={"events": list})
    async def ipc_events_list(self, payload: Dict[str, Any]) -> dict:
        events = self._event_catalog.list_events()
        return self._ok(events=[e.model_dump(mode="json") for e in events])

    # ------------------------------------------------------------------
    # HTTP Routes  (tenant_id = query param obligatoire)
    # ------------------------------------------------------------------

    @route("/flows", method="GET", tags=["xflow"])
    async def http_list_flows(self, users:AuthPayload=Depends(require_permission("xflow:workflows:read"))) -> dict:
        return await self.ipc_list_workflows({"tenant_id": _tenant_of(users)})

    @route("/flows", method="POST", tags=["xflow"])
    async def http_deploy(self,body: Dict[str, Any], users:AuthPayload=Depends(require_permission("xflow:workflows:write"))) -> dict:
        # Qui déploie signe : un déclenchement planifié s'exécutera avec SES droits du moment (jamais figés).
        return await self._deploy(_tenant_of(users), body, users.get("sub"))

    @route("/flows/{workflow_name}", method="GET", tags=["xflow"])
    async def http_get_flow(self,workflow_name: str, users:AuthPayload=Depends(require_permission("xflow:workflows:read"))) -> dict:
        d = await self._registry.get( _tenant_of(users), workflow_name)
        if not d:
            return self._error("Workflow introuvable.", code="not_found")
        return self._ok(workflow=d.model_dump(mode="json"))

    @route("/flows/{workflow_name}", method="DELETE", tags=["xflow"])
    async def http_delete_flow(self, workflow_name: str, users:AuthPayload=Depends(require_permission("xflow:workflows:write"))) -> dict:
        return await self.ipc_unregister({"tenant_id": _tenant_of(users), "workflow_name": workflow_name})

    @route("/run/{workflow_name}", method="POST", tags=["xflow"])
    async def http_run(self, workflow_name: str, body: Dict[str, Any] = {}, users:AuthPayload=Depends(require_permission("xflow:runs:write"))) -> dict:
        # `users` EST le principal authentifié (sub, permissions, user) : on le passe tel quel, sans repasser par l'action IPC,
        # dont la charge utile ne doit jamais porter d'identité.
        return await self._trigger(_tenant_of(users), workflow_name, body, dict(users), "manual")

    @route("/executions", method="GET", tags=["xflow"])
    async def http_executions(self, workflow_name: Optional[str] = None, limit: int = 50, users:AuthPayload=Depends(require_permission("xflow:runs:read"))) -> dict:
        return await self.ipc_list_runs({"tenant_id": _tenant_of(users), "workflow_name": workflow_name, "limit": limit})

    @route("/executions/{run_id}", method="GET", tags=["xflow"])
    async def http_get_execution(self, run_id: str, users:AuthPayload=Depends(require_permission("xflow:runs:read"))) -> dict:
        return await self.ipc_get_run({"tenant_id": _tenant_of(users), "run_id": run_id})

    @route("/executions/{run_id}/cancel", method="POST", tags=["xflow"])
    async def http_cancel(self, run_id: str, users:AuthPayload=Depends(require_permission("xflow:runs:write"))) -> dict:
        return await self.ipc_cancel_run({"tenant_id": _tenant_of(users), "run_id": run_id})

    @route("/registry", method="GET", tags=["xflow"])
    async def http_registry(self, users: AuthPayload = Depends(require_permission("xflow:workflows:read"))) -> dict:
        # Le catalogue des actions appelables : réservé à qui lit les automatisations (il était servi sans identité).
        return await self.ipc_registry({})

    @route("/flows/{workflow_name}/webhook", method="GET", tags=["xflow"])
    async def http_webhook_status(self, workflow_name: str, users:AuthPayload=Depends(require_permission("xflow:workflows:read"))) -> dict:
        return await self.ipc_webhook_status({"tenant_id": _tenant_of(users), "workflow_name": workflow_name})

    @route("/flows/{workflow_name}/webhook/secret", method="POST", tags=["xflow"])
    async def http_webhook_rotate(self, workflow_name: str, users:AuthPayload=Depends(require_permission("xflow:workflows:write"))) -> dict:
        return await self.ipc_webhook_rotate({"tenant_id": _tenant_of(users), "workflow_name": workflow_name})

    @route("/webhook/{workflow_name}", method="POST", tags=["xflow"])
    async def http_webhook(self, workflow_name: str, request: Request, tenant_id: Optional[str] = None):
        """Webhook entrant signé (HMAC-SHA256). Tout échec d'authentification = même 404.

        Le `tenant_id` de l'URL ne sert que de clé de recherche : le secret est propre à
        (tenant, workflow) et le run s'exécute dans le tenant de l'enregistrement du secret.
        """
        denied = JSONResponse({"detail": "Not Found"}, status_code=404)
        client = request.client.host if request.client else "?"
        if not self._webhook_guard.allow(client):
            return JSONResponse({"detail": "Too Many Requests"}, status_code=429)

        declared = request.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > MAX_BODY_BYTES:
            return JSONResponse({"detail": "Payload Too Large"}, status_code=413)
        chunks, size = [], 0
        async for chunk in request.stream():
            size += len(chunk)
            if size > MAX_BODY_BYTES:
                return JSONResponse({"detail": "Payload Too Large"}, status_code=413)
            chunks.append(chunk)
        raw = b"".join(chunks)

        ts = request.headers.get(TIMESTAMP_HEADER)
        sig = request.headers.get(SIGNATURE_HEADER)

        secret = await self._webhook_secrets.get(tenant_id, workflow_name) if tenant_id else None
        if secret is None:
            verify_dummy(ts, sig, raw)
            if tenant_id and await self._registry.get(tenant_id, workflow_name):
                logger.warning(
                    "Webhook refusé: workflow '%s' (tenant=%s) sans secret de signature "
                    "(webhook désactivé jusqu'à génération d'un secret).", workflow_name, tenant_id,
                )
            return denied
        if not verify(secret, ts, sig, raw):
            return denied
        if not self._webhook_guard.first_use(f"{tenant_id}:{workflow_name}:{sig}"):
            return denied

        definition = await self._registry.get(tenant_id, workflow_name)
        if not definition or definition.trigger.type not in (TriggerType.WEBHOOK, TriggerType.MANUAL):
            return denied
        try:
            body = json.loads(raw) if raw else {}
        except ValueError:
            return JSONResponse({"detail": "Invalid JSON"}, status_code=400)
        if not isinstance(body, dict):
            return JSONResponse({"detail": "Invalid JSON"}, status_code=400)

        run = await self._engine.init_run(
            definition, tenant_id=tenant_id, trigger_payload=body, trigger_type="webhook"
        )
        await self._enqueue_run_id(run.run_id)
        return self._ok(run_id=run.run_id, status=run.status.value)

    @route("/flows/{workflow_name}/graph", method="GET", tags=["xflow"])
    async def http_flow_graph(self, workflow_name: str, users:AuthPayload=Depends(require_permission("xflow:workflows:read"))) -> dict:
        d = await self._registry.get(_tenant_of(users), workflow_name)
        if not d:
            return self._error("Workflow introuvable.", code="not_found")
        return self._ok(graph=d.export_graph())

    # ------------------------------------------------------------------
    # Composite Nodes HTTP Routes
    # ------------------------------------------------------------------

    @route("/composites", method="GET", tags=["composites"])
    async def http_list_composites(self, users:AuthPayload=Depends(require_permission("xflow:composites:read"))) -> dict:
        return await self.ipc_composite_list({"tenant_id": _tenant_of(users)})

    @route("/composites", method="POST", tags=["composites"])
    async def http_create_composite(self, body: Dict[str, Any], users:AuthPayload=Depends(require_permission("xflow:composites:write"))) -> dict:
        return await self.ipc_composite_register({"tenant_id": _tenant_of(users), **body})

    @route("/composites/{name}", method="GET", tags=["composites"])
    async def http_get_composite(self, name: str, users:AuthPayload=Depends(require_permission("xflow:composites:read"))) -> dict:
        composite = await self._composite_service.get(_tenant_of(users), name)
        if not composite:
            return self._error("Composite introuvable.", code="not_found")
        return self._ok(composite=composite.model_dump(mode="json"))

    @route("/composites/{name}", method="DELETE", tags=["composites"])
    async def http_delete_composite(self, name: str, users:AuthPayload=Depends(require_permission("xflow:composites:write"))) -> dict:
        success = await self._composite_service.delete(_tenant_of(users), name)
        if not success:
            return self._error("Composite introuvable.", code="not_found")
        return self._ok(message=f"Composite '{name}' supprimé.")

    @route("/composites/{name}/expand", method="POST", tags=["composites"])
    async def http_expand_composite(self, name: str, body: Dict[str, Any], users:AuthPayload=Depends(require_permission("xflow:composites:read"))) -> dict:
        return await self.ipc_composite_expand({
            "tenant_id": _tenant_of(users),
            "composite_name": name,
            "instance_id": body.get("instance_id", "instance"),
            "inputs": body.get("inputs", {}),
        })

    # ------------------------------------------------------------------
    # Events HTTP Routes
    # ------------------------------------------------------------------

    @route("/events", method="GET", tags=["events"])
    async def http_list_events(self, users:AuthPayload=Depends(require_permission("xflow:workflows:read"))) -> dict:
        return await self.ipc_events_list({})

    @route("/events/refresh", method="POST", tags=["events"])
    async def http_refresh_events(self, users:AuthPayload=Depends(require_permission("xflow:admin"))) -> dict:
        await self._event_catalog.refresh()
        return self._ok(message="Catalogue événements rafraîchi.")

    async def _declare_rbac(self) -> None:
        rbac = (self.ctx.config or {}).get("rbac") or {}
        grants = rbac.get("grants") or []
        if not grants:
            return
        try:
            await self.ctx.events.emit(
                "rbac.declare",
                {"plugin": "xflow", "grants": grants},
                source="xflow",
            )
            logger.info("[xflow] rbac.declare émis (%d grant(s))", len(grants))
        except Exception as exc:
            logger.warning("[xflow] rbac.declare ignoré : %s", exc)

    def get_router(self) -> Any | None:
        return super().get_router()

    # ------------------------------------------------------------------
    # Event Bus handlers
    # ------------------------------------------------------------------

    async def _on_any_event(self, event: Any) -> None:
        event_name = getattr(event, "name", None) or (
            event.get("name") if isinstance(event, dict) else None
        )
        if not event_name:
            return

        # Ignorer les events internes xflow pour éviter le traitement inutile
        if event_name.startswith("workflow."):
            return

        event_data = (
            getattr(event, "data", {})
            if not isinstance(event, dict)
            else event.get("data", {})
        ) or {}

        tenant_id = event_data.get("tenant_id")

        if tenant_id:
            # Event tenant-scopé : on ne déclenche que les workflows de ce tenant.
            handlers = [
                (tenant_id, d)
                for d in await self._registry.list_event_handlers(tenant_id, event_name)
            ]
        else:
            # La plupart des events métier ne portent pas de tenant_id : on cherche
            # alors les handlers de TOUS les tenants et on déclenche chacun dans le sien.
            handlers = await self._registry.list_event_handlers_all_tenants(event_name)

        for handler_tenant, d in handlers:
            # Injecte le tenant résolu dans le payload pour que les steps y aient accès.
            payload = {**event_data, "tenant_id": handler_tenant}
            run = await self._engine.init_run(
                d, tenant_id=handler_tenant, trigger_payload=payload, trigger_type="event"
            )
            # Un événement n'a pas de déclencheur AUTHENTIFIÉ : le `user` d'une charge utile du bus est une donnée qu'un plugin
            # écrit, pas une identité. Comme un cron, le run agit donc au nom de qui a DÉPLOYÉ le flux (ses droits du moment,
            # redemandés à xauth). Avant, il partait sans identité et chaque étape `doc.*` répondait `unauthenticated`.
            if getattr(d, "deployed_by", None):
                run.context[self._engine.ACTOR_KEY] = d.deployed_by
                await self._engine._save_run(run)
            await self._enqueue_run_id(run.run_id)
            logger.info(
                "Workflow '%s' déclenché par l'événement '%s' (tenant=%s, run=%s).",
                d.name, event_name, handler_tenant, run.run_id,
            )
