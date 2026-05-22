"""
Dispatcher de webhooks sortants — XFlow V2.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal

import httpx

from ..runtime.condition import render_payload
from ..schemas.workflow import WebhookNotification, WorkflowRun

logger = logging.getLogger("xflow.webhooks")

WebhookEvent = Literal["start", "success", "failure", "step_success", "step_failure"]


async def dispatch_webhooks(
    webhooks: List[WebhookNotification],
    event: WebhookEvent,
    run: WorkflowRun,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    targets = [wh for wh in webhooks if event in wh.on_events]
    if not targets:
        return

    context = _build_context(run, event, extra)
    await asyncio.gather(
        *[_send_one(wh, context, event) for wh in targets],
        return_exceptions=True,
    )


async def _send_one(
    wh: WebhookNotification,
    context: Dict[str, Any],
    event: str,
) -> None:
    body = render_payload(wh.body_template or _default_body(event), context)
    headers = {"Content-Type": "application/json", **wh.headers}

    try:
        async with httpx.AsyncClient(timeout=wh.timeout_seconds) as client:
            resp = await client.request(
                method=wh.method,
                url=wh.url,
                json=body,
                headers=headers,
            )
        if resp.is_success:
            logger.info("Webhook [%s] → %s [HTTP %s]", event, wh.url, resp.status_code)
        else:
            logger.warning(
                "Webhook [%s] → %s réponse %s : %s",
                event, wh.url, resp.status_code, resp.text[:300],
            )
    except Exception as exc:
        logger.error("Webhook [%s] → %s ERREUR : %s", event, wh.url, exc)


def _build_context(run: WorkflowRun, event: str, extra: Any) -> Dict[str, Any]:
    return {
        "run": {
            "id": run.run_id,
            "workflow": run.workflow_name,
            "status": run.status.value,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        },
        "event": event,
        "trigger": run.trigger_payload,
        "context": run.context,
        **(extra or {}),
    }


def _default_body(event: str) -> Dict[str, Any]:
    return {
        "event": event,
        "run_id": "{{ run.id }}",
        "workflow": "{{ run.workflow }}",
        "status": "{{ run.status }}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# Fix: Optional import
from typing import Optional
