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

import jmespath
from jmespath.exceptions import JMESPathError

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
        # Droits RÉSOLUS d'un run, le temps de ce run seulement. On ne stocke jamais de permissions (ni ici de façon durable,
        # ni en base) : le run ne retient que l'IDENTIFIANT de son déclencheur (`context._actor_sub`), et ses droits sont
        # redemandés à xauth à chaque exécution. Un compte parti ou un grant retiré arrête donc le travail, au lieu de le
        # laisser courir avec les droits d'hier — et un run repris après un redémarrage, ou exécuté dans un autre processus,
        # retrouve son identité tout seul.
        self._resolved: Dict[str, Dict[str, Any]] = {}
        # Sera injecté par le plugin parent pour le scheduler
        self._enqueue_run_id: Optional[Callable] = None

    AUTH_PLUGIN = "auth"
    ACTOR_KEY = "_actor_sub"

    async def _principal(self, run: WorkflowRun) -> Optional[Dict[str, Any]]:
        """Les droits du run : ceux que son déclencheur a MAINTENANT, demandés à xauth une fois par run.

        `None` quand le run n'a pas d'identité (aucun déclencheur connu) ou que xauth ne la reconnaît plus : le plugin appelé
        refusera alors de lui-même, ce qui est le bon sens de l'échec.
        """
        cached = self._resolved.get(run.run_id)
        if cached is not None:
            return cached or None
        sub = (run.context or {}).get(self.ACTOR_KEY)
        caller = getattr(self.ctx, "caller", None)
        if not sub or caller is None:
            self._resolved[run.run_id] = {}
            return None
        try:
            answer = await caller(
                self.AUTH_PLUGIN,
                "xauth.user.permissions",
                {"user_id": sub, "tenant_id": run.tenant_id},
                caller=self.ctx.name,
                tenant_id=run.tenant_id,
                principal=None,
            )
        except Exception as exc:  # noqa: BLE001 - xauth absent ou injoignable : le run échouera sur le refus du plugin appelé
            logger.warning("xflow: identité non résolue pour le run %s : %s", run.run_id, exc)
            answer = None
        perms = (answer or {}).get("permissions") if isinstance(answer, dict) else None
        if not isinstance(perms, list):
            logger.warning("xflow: xauth ne reconnaît plus %s (run %s)", sub, run.run_id)
            self._resolved[run.run_id] = {}
            return None
        principal = {"sub": sub, "permissions": perms, "user": {"id": sub, "tenant_id": run.tenant_id}}
        self._resolved[run.run_id] = principal
        return principal

    def forget_actor(self, run_id: str) -> None:
        self._resolved.pop(run_id, None)

    async def setup(self, store: WorkflowStore) -> None:
        self._store = store

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def init_run(
        self,
        definition: WorkflowDefinition,
        tenant_id: str = "default",
        trigger_payload: Optional[Dict[str, Any]] = None,
        trigger_type: str = "manual",
    ) -> WorkflowRun:
        """Crée et persiste un run en statut PENDING."""
        run = WorkflowRun(
            run_id=str(uuid.uuid4()),
            tenant_id=tenant_id,
            workflow_name=definition.name,
            workflow_version=definition.version,
            status=WorkflowStatus.PENDING,
            trigger_payload=trigger_payload or {},
            context={
                "trigger": trigger_payload or {},
                "_triggered_by": trigger_type,
            },
            started_at=datetime.now(timezone.utc),
        )
        await self._save_run(run)
        return run

    async def execute_existing(self, definition: WorkflowDefinition, run: WorkflowRun) -> None:
        """Exécute un run déjà initialisé (appelé depuis le worker)."""
        try:
            await self._execute_existing(definition, run)
        finally:
            self.forget_actor(run.run_id)  # l'identité ne survit pas au run

    async def _execute_existing(self, definition: WorkflowDefinition, run: WorkflowRun) -> None:
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
        # Snapshot figé AVANT de commencer à exécuter : seuls les steps déjà
        # SUCCESS suite à un crash d'un précédent appel (reprise) doivent être
        # sautés. Sans ce gel, un step relancé plusieurs fois dans le MÊME
        # appel (boucle foreach, branches parallel) passerait SUCCESS dès la
        # 1ʳᵉ occurrence et toutes les suivantes seraient silencieusement
        # sautées — la boucle n'agirait alors qu'une seule fois.
        resumed_success_ids = frozenset(
            sid for sid, step_run in run.steps.items() if step_run.status == StepStatus.SUCCESS
        )
        try:
            start_id = run.current_step_id or definition.start_step_id
            coro = self._run_from_step(definition, run, start_id, resumed_success_ids=resumed_success_ids)
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
        visited: Optional[List[str]] = None,
        resumed_success_ids: "frozenset[str]" = frozenset(),
    ) -> None:
        # `visited` est propre à ce chemin d'exécution (copié, jamais partagé
        # avec l'appelant) : une branche de parallel ou une itération de
        # foreach démarre sa propre traçabilité de cycle à partir de l'état
        # au moment où elle est lancée, sans hériter des passages faits par
        # les autres branches/itérations. Ça permet de revisiter légitimement
        # le même step_id (boucle, branches convergentes) tout en détectant
        # toujours un vrai cycle A→B→A à l'intérieur d'un même chemin.
        visited = list(visited) if visited is not None else []
        current_id: Optional[str] = step_id

        while current_id and current_id != END:
            # Détection de cycle
            if current_id in visited:
                raise RuntimeError(f"Cycle détecté au step '{current_id}'")
            visited.append(current_id)

            if run.status == WorkflowStatus.CANCELLED:
                logger.info("Run %s annulé à l'étape '%s'", run.run_id, current_id)
                return

            step = definition.get_step(current_id)
            if step is None:
                raise ValueError(f"Step introuvable : '{current_id}'")

            run.current_step_id = current_id
            await self._save_run(run)

            current_id = await self._dispatch_step(definition, run, step, visited, resumed_success_ids)

    # ------------------------------------------------------------------
    # Step dispatcher
    # ------------------------------------------------------------------

    async def _dispatch_step(
        self,
        definition: WorkflowDefinition,
        run: WorkflowRun,
        step: AnyStep,
        visited: List[str],
        resumed_success_ids: "frozenset[str]",
    ) -> Optional[str]:
        # Crash recovery : un step déjà SUCCESS avant le début de CET appel
        # (reprise après crash) est sauté. Un step re-SUCCESS pendant le
        # déroulement de cet appel (itération de foreach, branche parallel)
        # n'est PAS dans ce snapshot figé — il est donc bien ré-exécuté.
        if step.id in resumed_success_ids and hasattr(step, "on_success"):
            return getattr(step, "on_success", None) or END

        if isinstance(step, ActionStep):
            return await self._run_action(definition, run, step)
        if isinstance(step, ConditionStep):
            return await self._run_condition(run, step)
        if isinstance(step, ParallelStep):
            return await self._run_parallel(definition, run, step, visited, resumed_success_ids)
        if isinstance(step, SwitchStep):
            return await self._run_switch(run, step)
        if isinstance(step, ForeachStep):
            return await self._run_foreach(definition, run, step, visited, resumed_success_ids)
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

    async def _call(self, plugin_name: str, action: str, payload: dict, run: WorkflowRun) -> Dict[str, Any]:
        """Appelle l'action d'un autre plugin AU NOM du run : son organisation, et l'identité de son déclencheur.

        `call_plugin` du SDK déduit le tenant et le principal des ContextVar de la requête en cours. C'est juste quand le step
        tourne pendant une requête (déclenchement manuel), et faux partout ailleurs : un run repris de la file, relancé après un
        crash ou déclenché par cron n'a plus de requête, donc plus de tenant (repli silencieux sur « default ») ni d'identité.
        Or le run, lui, SAIT dans quelle organisation il tourne. On passe donc les deux explicitement.

        L'identité n'est jamais stockée dans le run (ce serait stocker une capacité : un run rejoué agirait avec les droits de
        quelqu'un parti depuis). Tant qu'il n'y a pas de compte de service, un run hors requête part sans principal, et le plugin
        appelé refuse lui-même s'il exige une identité — c'est le bon sens de l'échec, pas un contournement à inventer ici.
        """
        caller = getattr(self.ctx, "caller", None)
        if caller is None:  # plugin sandboxé ou test sans caller : on garde le chemin d'origine
            return await self.plugin.call_plugin(plugin_name=plugin_name, action=action, payload=payload)
        principal = await self._principal(run)
        return await caller(
            plugin_name,
            action,
            payload or {},
            caller=self.ctx.name,
            tenant_id=run.tenant_id,
            principal=principal,
        )

    async def _run_action(self, definition: WorkflowDefinition, run: WorkflowRun, step: ActionStep) -> str:
        step_run = self._get_or_create_step_run(run, step.id)
        step_run.status = StepStatus.RUNNING
        step_run.started_at = datetime.now(timezone.utc)
        await self._save_run(run)
        await self._emit("workflow.step.running", run, {"step_id": step.id, "status": "running"})

        context = self._build_context(run)
        payload = render_payload(step.payload, context)
        async def call_action() -> dict:
            return await self._call(step.plugin, step.action, payload, run)

        try:
            result = await execute_with_retry(call_action, step.retry)
            step_run.status = StepStatus.SUCCESS

            if isinstance(result, dict):
                step_run.result = {k: v for k, v in result.items() if k != "status"}
            else:
                step_run.result = result
            run.context.setdefault("steps", {})[step.id] = {"result": step_run.result}
            await self._emit("workflow.step.success", run, {"step_id": step.id, "status": "success"})
            return step.on_success or END

        except (RetryExhausted, Exception) as e:
            step_run.status = StepStatus.FAILED
            step_run.error = str(e)
            await self._emit("workflow.step.failed", run, {"step_id": step.id, "status": "failed", "error": str(e)})
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

    async def _run_parallel(
        self,
        definition: WorkflowDefinition,
        run: WorkflowRun,
        step: ParallelStep,
        visited: List[str],
        resumed_success_ids: "frozenset[str]",
    ) -> str:
        tasks = [
            self._run_from_step(definition, run, branch[0], visited, resumed_success_ids)
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

    async def _run_foreach(
        self,
        definition: WorkflowDefinition,
        run: WorkflowRun,
        step: ForeachStep,
        visited: List[str],
        resumed_success_ids: "frozenset[str]",
    ) -> str:
        context = self._build_context(run)
        items = render_value(step.items, context)
        if not isinstance(items, list):
            items = []

        if step.parallel and step.steps:
            sem = asyncio.Semaphore(step.max_parallel)

            async def run_item(item: Any) -> None:
                async with sem:
                    run.context["loop_item"] = item
                    # Chaque item repart de l'état `visited` figé à l'entrée
                    # de la boucle : la ré-exécution de step.steps[0] à
                    # chaque itération n'est pas un cycle.
                    await self._run_from_step(definition, run, step.steps[0], visited, resumed_success_ids)

            await asyncio.gather(*[run_item(item) for item in items], return_exceptions=True)
        else:
            for item in items:
                run.context["loop_item"] = item
                if step.steps:
                    await self._run_from_step(definition, run, step.steps[0], visited, resumed_success_ids)

        run.context.pop("loop_item", None)
        return step.on_success or END

    async def _run_wait(self, run: WorkflowRun, step: WaitStep) -> str:
        if step.delay_seconds:
            await asyncio.sleep(step.delay_seconds)
        return step.on_success or END

    async def _run_transform(self, run: WorkflowRun, step: TransformStep) -> str:
        context = self._build_context(run)
        query = step.query.strip()

        # Compat : une query purement "{{ ... }}" reste résolue par le
        # même moteur de templates que les autres steps (aucun changement
        # de comportement pour les workflows existants qui l'utilisaient
        # ainsi, sans vraie syntaxe JMESPath).
        if query.startswith("{{") and query.endswith("}}"):
            result = render_value(step.query, context)
        else:
            source = render_value(step.input, context) if step.input is not None else context
            expression = query[1:] if query.startswith(".") else query
            try:
                result = jmespath.search(expression, source) if expression else source
            except JMESPathError as exc:
                raise ValueError(
                    f"Requête JMESPath invalide dans le step '{step.id}' ({query!r}) : {exc}"
                ) from exc

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

        # Diffusion temps réel vers les clients connectés (canal "xflow").
        # Fire-and-forget : une erreur WS ne doit jamais faire planter un run.
        ws = getattr(self.plugin, "_ws", None)
        if ws is not None:
            try:
                await ws.broadcast(channel="xflow", event=event_name, data=data)
            except Exception:
                pass
