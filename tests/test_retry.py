"""Tests unitaires pour la logique de retry."""
from __future__ import annotations

import asyncio
import pytest
from app.xflow.src.runtime.retry import RetryExhausted, execute_with_retry
from app.xflow.src.schemas.workflow import RetryBackoff, RetryConfig


@pytest.mark.asyncio
async def test_success_on_first_attempt():
    async def ok(): return {"status": "success", "data": {"result": 42}}
    result = await execute_with_retry(ok, RetryConfig(max_attempts=3))
    assert result["data"]["result"] == 42


@pytest.mark.asyncio
async def test_retry_then_success():
    calls = []

    async def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError("transient error")
        return {"status": "success"}

    cfg = RetryConfig(max_attempts=3, delay_seconds=0.01, backoff=RetryBackoff.CONSTANT)
    result = await execute_with_retry(flaky, cfg)
    assert result["status"] == "success"
    assert len(calls) == 3


@pytest.mark.asyncio
async def test_retry_exhausted():
    async def always_fail():
        raise RuntimeError("always fails")

    cfg = RetryConfig(max_attempts=2, delay_seconds=0.01, backoff=RetryBackoff.CONSTANT)
    with pytest.raises(RetryExhausted) as exc_info:
        await execute_with_retry(always_fail, cfg)
    assert exc_info.value.attempts == 2


@pytest.mark.asyncio
async def test_no_retry_config():
    async def ok(): return {"ok": True}
    result = await execute_with_retry(ok, None)
    assert result["ok"] is True


@pytest.mark.asyncio
async def test_soft_error_retried():
    calls = []

    async def soft_fail():
        calls.append(1)
        if len(calls) == 1:
            return {"status": "error", "code": "timeout", "message": "timed out"}
        return {"status": "success"}

    cfg = RetryConfig(max_attempts=3, delay_seconds=0.01, backoff=RetryBackoff.CONSTANT, retry_on_codes=["timeout"])
    result = await execute_with_retry(soft_fail, cfg)
    assert result["status"] == "success"


def test_compute_delay_exponential():
    cfg = RetryConfig(delay_seconds=2.0, backoff=RetryBackoff.EXPONENTIAL, max_delay_seconds=30.0)
    assert cfg.compute_delay(1) == 2.0
    assert cfg.compute_delay(2) == 4.0
    assert cfg.compute_delay(3) == 8.0
    assert cfg.compute_delay(10) == 30.0  # capped
