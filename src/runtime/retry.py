"""
Logique de retry avec stratégies de backoff pour les steps d'action XFlow V2.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Coroutine, Optional

from ..schemas.workflow import RetryConfig

logger = logging.getLogger("xflow.retry")


class RetryExhausted(Exception):
    def __init__(self, last_error: str, attempts: int) -> None:
        super().__init__(f"Retry épuisé après {attempts} tentatives : {last_error}")
        self.last_error = last_error
        self.attempts = attempts


class _SoftError(Exception):
    """Erreur logique (code d'erreur IPC) traitée comme un échec retryable."""
    pass


async def execute_with_retry(
    coro_factory: Callable[[], Coroutine[Any, Any, dict]],
    retry_cfg: Optional[RetryConfig],
    on_retry: Optional[Callable[[int, float, str], None]] = None,
) -> dict:
    """
    Exécute une coroutine avec retry automatique selon la configuration fournie.

    Args:
        coro_factory: Callable qui retourne une nouvelle coroutine à chaque appel.
        retry_cfg:    Configuration retry. Si None, exécution unique sans retry.
        on_retry:     Callback optionnel appelé avant chaque retry (attempt, delay, error).
    """
    if retry_cfg is None:
        return await coro_factory()

    max_attempts = retry_cfg.max_attempts
    last_error = ""

    for attempt in range(1, max_attempts + 1):
        try:
            result = await coro_factory()

            # Traitement des erreurs "soft" (réponse IPC avec status="error")
            if isinstance(result, dict) and result.get("status") == "error":
                code = result.get("code", "")
                if retry_cfg.retry_on_codes and code not in retry_cfg.retry_on_codes:
                    # Code non retryable → retourner l'erreur directement
                    return result
                last_error = result.get("message", str(result))
                raise _SoftError(last_error)

            return result

        except _SoftError as exc:
            last_error = str(exc)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_error = str(exc)

        if attempt >= max_attempts:
            break

        delay = retry_cfg.compute_delay(attempt)
        logger.warning(
            "Retry %d/%d dans %.1fs — %s", attempt, max_attempts, delay, last_error
        )
        if on_retry:
            on_retry(attempt, delay, last_error)
        await asyncio.sleep(delay)

    raise RetryExhausted(last_error, max_attempts)
