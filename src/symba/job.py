"""JobHandle and Gate (spec 7).

Neither object holds state beyond identity — both are thin views over engine
state, reconstructable from a bare id. ``result()`` uses the engine's
server-side long-poll (``AwaitJob``) sliced into <=60s requests, never client
polling (spec 7.1).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

from .types import CancelOutcome, GateStatus, JobEvent, JobStatus

if TYPE_CHECKING:
    from pydantic import BaseModel


class _EngineOps(Protocol):
    """The subset of Engine used by handles — avoids an import cycle."""

    async def _await_job(self, job_id: str, timeout: float | None) -> Any: ...
    async def _get_status(self, job_id: str) -> JobStatus: ...
    async def _cancel(self, job_id: str, cascade: bool) -> CancelOutcome: ...
    async def _job_events(self, job_id: str) -> list[JobEvent]: ...
    async def _gate_status(self, gate_id: str) -> GateStatus: ...
    async def _gate_result(
        self,
        gate_id: str,
        ctx_id: str | None,
        continuation_task: str | None,
        timeout: float | None,
    ) -> Any: ...


class JobHandle:
    """Handle to one submitted job (spec 7.1)."""

    def __init__(
        self,
        engine: _EngineOps,
        job_id: str,
        *,
        task_name: str = "",
        ctx_id: str | None = None,
        deduplicated: bool = False,
    ) -> None:
        self._engine = engine
        self.id = job_id
        self.task_name = task_name
        self.ctx_id = ctx_id
        self.deduplicated = deduplicated

    async def result(self, timeout: float | None = None) -> dict[str, Any] | BaseModel:
        """Block until terminal via ``AwaitJob`` timeout slices (spec 7.1).

        Returns the deserialized result on SUCCEEDED; raises ``JobFailed`` on
        DEAD (carrying ``error_history``) or ``JobCancelled`` on CANCELLED.
        """
        return await self._engine._await_job(self.id, timeout)

    async def status(self) -> JobStatus:
        """One atomic ``GetJob`` read (spec 7.1)."""
        return await self._engine._get_status(self.id)

    async def cancel(self, cascade: bool = True) -> CancelOutcome:
        return await self._engine._cancel(self.id, cascade)

    async def events(self) -> list[JobEvent]:
        return await self._engine._job_events(self.id)

    def __repr__(self) -> str:
        dedup = " deduplicated" if self.deduplicated else ""
        return f"<JobHandle {self.id} task={self.task_name!r}{dedup}>"


class Gate:
    """View over a fan-out gate (spec 7.2). The gate itself is engine state."""

    def __init__(
        self,
        engine: _EngineOps,
        gate_id: str,
        children: list[JobHandle],
        *,
        ctx_id: str | None = None,
        continuation_task: str | None = None,
    ) -> None:
        self._engine = engine
        self.id = gate_id
        self.children = children
        self.ctx_id = ctx_id
        self.continuation_task = continuation_task

    async def result(self, timeout: float | None = None) -> dict[str, Any]:
        """Await the continuation job the gate fires exactly once (spec 7.2)."""
        return await self._engine._gate_result(
            self.id, self.ctx_id, self.continuation_task, timeout
        )

    async def status(self) -> GateStatus:
        return await self._engine._gate_status(self.id)

    def __repr__(self) -> str:
        return f"<Gate {self.id} children={len(self.children)}>"


__all__ = ["JobHandle", "Gate"]
