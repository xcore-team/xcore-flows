"""
Queue locale en mémoire — fallback si Redis n'est pas disponible.
"""
from __future__ import annotations

import asyncio
import logging
from collections import deque
from typing import Any, Deque, Dict, Optional

logger = logging.getLogger("xflow.local_queue")


class LocalQueue:
    """File FIFO async-safe en mémoire."""

    def __init__(self) -> None:
        self._queues: Dict[str, Deque[str]] = {}
        self._lock = asyncio.Lock()

    async def lpush(self, key: str, value: str) -> None:
        async with self._lock:
            if key not in self._queues:
                self._queues[key] = deque()
            self._queues[key].appendleft(value)

    async def rpop(self, key: str) -> Optional[str]:
        async with self._lock:
            q = self._queues.get(key)
            if q:
                try:
                    return q.pop()
                except IndexError:
                    pass
        return None

    async def llen(self, key: str) -> int:
        async with self._lock:
            return len(self._queues.get(key, []))

    async def get(self, key: str) -> Any:  # noqa: D401
        return None

    async def set(self, key: str, value: Any, ttl: int = 0) -> None:
        pass

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._queues.pop(key, None)
