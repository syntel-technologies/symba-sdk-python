"""io-profile executor (spec 11.1).

Runs ``async def`` handlers as coroutines directly on the worker loop, up to
``slots`` interleaved. Cancellation is cooperative: the dispatch pipeline cancels
the awaiting task, which raises ``CancelledError`` inside the handler.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from symba.context import Ctx
    from symba.task_registry import RegisteredTask


class AsyncioExecutor:
    """The default executor — no setup, no teardown, just await the coroutine."""

    async def start(self) -> None:
        return None

    async def run(self, task: RegisteredTask, ctx: Ctx, payload: Any) -> Any:
        return await task.handler(ctx, payload)

    async def stop(self, drain_s: float) -> None:
        return None
