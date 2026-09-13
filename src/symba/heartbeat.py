"""Heartbeat / timeout / cancellation shell (spec 12).

One shell per running job, always an asyncio task in the parent process. It:

* beats every ``heartbeat_interval_s`` (unary ``Heartbeat`` RPC), refreshing the
  lease and reading the cooperative-cancel flag;
* tolerates up to ``lease_ttl/interval - 1`` consecutive missed beats before
  giving up (the job keeps running; a lost lease surfaces later as a swallowed
  ``StaleLease`` on Complete, spec 9.4);
* enforces ``timeout_s`` worker-side (the engine lease reclaim is the backstop);
* cancels the handler task on cancel-flag or timeout.

For io handlers the cancellation path injects ``CancelledError`` into the handler
task. cpu/gpu subprocess signalling lands in M5 (the shell calls ``on_cancel``).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from enum import Enum, auto
from typing import Any

import grpc

from ._proto import data_plane_pb2, data_plane_pb2_grpc
from .logging import get_logger

_log = get_logger(component="heartbeat")


class CancelReason(Enum):
    CANCELLED = auto()  # engine cooperative cancel
    TIMEOUT = auto()  # worker-side timeout_s expiry


class HeartbeatShell:
    """Owns liveness for a single running job (spec 12)."""

    def __init__(
        self,
        stub: data_plane_pb2_grpc.WorkerServiceStub,
        *,
        job_id: str,
        lease_token: str,
        interval_s: float,
        lease_ttl_s: float,
        timeout_s: float | None,
        on_cancel: Callable[[CancelReason], Awaitable[None]],
    ) -> None:
        self._stub = stub
        self._job_id = job_id
        self._lease_token = lease_token
        self._interval_s = interval_s
        self._timeout_s = timeout_s
        self._on_cancel = on_cancel
        self._max_misses = max(1, int(lease_ttl_s / interval_s) - 1) if interval_s > 0 else 1
        self._task: asyncio.Task[None] | None = None
        self._beat_now = asyncio.Event()
        self.cancel_reason: CancelReason | None = None

    def start(self) -> None:
        self._task = asyncio.ensure_future(self._run())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def beat_now(self) -> None:
        """Trigger an immediate beat (ctx.heartbeat / tight loops)."""
        self._beat_now.set()

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        deadline = None if self._timeout_s is None else loop.time() + self._timeout_s
        misses = 0
        while True:
            wait_for = self._interval_s
            if deadline is not None:
                wait_for = min(wait_for, max(0.0, deadline - loop.time()))
            try:
                await asyncio.wait_for(self._beat_now.wait(), timeout=wait_for)
                self._beat_now.clear()
            except TimeoutError:
                pass

            if deadline is not None and loop.time() >= deadline:
                self.cancel_reason = CancelReason.TIMEOUT
                _log.warning("job_timeout", job_id=self._job_id, timeout_s=self._timeout_s)
                await self._on_cancel(CancelReason.TIMEOUT)
                return

            cancelled, ok = await self._beat_once()
            if cancelled:
                self.cancel_reason = CancelReason.CANCELLED
                _log.info("job_cancelled_by_engine", job_id=self._job_id)
                await self._on_cancel(CancelReason.CANCELLED)
                return
            if not ok:
                misses += 1
                _log.warning(
                    "heartbeat_missed", job_id=self._job_id, misses=misses, cap=self._max_misses
                )
                if misses > self._max_misses:
                    _log.warning("heartbeat_giving_up", job_id=self._job_id)
                    return
            else:
                misses = 0

    async def _beat_once(self) -> tuple[bool, bool]:
        """Return ``(cancelled, ok)``."""
        req = data_plane_pb2.HeartbeatRequest(job_id=self._job_id, lease_token=self._lease_token)
        try:
            resp: Any = await self._stub.Heartbeat(req)
        except grpc.aio.AioRpcError:
            return False, False
        return bool(resp.cancelled), True


__all__ = ["HeartbeatShell", "CancelReason"]
