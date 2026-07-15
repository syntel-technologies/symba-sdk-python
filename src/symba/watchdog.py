"""Event-loop-lag watchdog (spec 11.5).

A 100ms ticker measures how late it actually fires; sustained drift over the
threshold means something is blocking the loop — almost always a sync call inside an
io handler that should be ``profile="cpu"``. The watchdog logs a WARNING naming the
currently running ``task_name``s, the actionable signal for that fix. The measured
lag is exposed as ``symba_worker_event_loop_lag_ms`` when metrics middleware is on.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

from .logging import get_logger

_log = get_logger(component="watchdog")

_TICK_S = 0.1
_LAG_THRESHOLD_S = 0.25
#: consecutive over-threshold ticks before a WARNING (avoids one-off GC blips).
_SUSTAINED_TICKS = 3


class EventLoopWatchdog:
    """Background ticker that flags sustained event-loop lag (spec 11.5)."""

    def __init__(
        self,
        running_task_names: Callable[[], list[str]],
        *,
        on_lag: Callable[[float], None] | None = None,
    ) -> None:
        self._running_task_names = running_task_names
        self._on_lag = on_lag
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    def start(self) -> None:
        if self._task is None:
            self._stop.clear()
            self._task = asyncio.ensure_future(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        over = 0
        while not self._stop.is_set():
            before = loop.time()
            try:
                await asyncio.sleep(_TICK_S)
            except asyncio.CancelledError:
                return
            lag = (loop.time() - before) - _TICK_S
            if self._on_lag is not None:
                self._on_lag(max(0.0, lag) * 1000.0)
            if lag > _LAG_THRESHOLD_S:
                over += 1
                if over >= _SUSTAINED_TICKS:
                    _log.warning(
                        "event_loop_lag",
                        lag_ms=round(lag * 1000, 1),
                        running_tasks=self._running_task_names(),
                        hint="a blocking call in an io handler — consider profile='cpu'",
                    )
                    over = 0
            else:
                over = 0

    @staticmethod
    def now_ms() -> float:
        return time.monotonic() * 1000.0


__all__ = ["EventLoopWatchdog"]
