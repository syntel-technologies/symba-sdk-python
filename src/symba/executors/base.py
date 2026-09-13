"""Executor protocol (spec 11.2)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from symba.context import Ctx
    from symba.task_registry import RegisteredTask


@runtime_checkable
class Executor(Protocol):
    """Runs a handler to completion or raises. Must be cancellable (spec 12)."""

    async def start(self) -> None: ...

    async def run(self, task: RegisteredTask, ctx: Ctx, payload: Any) -> Any:
        """Run ``task.handler(ctx, payload)`` under this profile's mechanism."""
        ...

    async def stop(self, drain_s: float) -> None: ...
