"""Synchronous facade over the async client (spec 21).

For scripts, notebooks and Django views — call sites that are not async. Every
method is a thin blocking wrapper around the real :class:`~symba.engine.Engine`,
so there is zero logic duplication. A dedicated background event-loop thread per
:class:`SyncEngine` (started lazily, closed at interpreter exit) hosts the one
persistent gRPC channel; each call is ``run_coroutine_threadsafe(...).result()``.

Calling a :class:`SyncEngine` method from inside a running event loop raises
``RuntimeError`` — you are already async, use :class:`~symba.engine.Engine`.

There is deliberately no sync ``Worker``: workers own their own loop and run
long; a blocking facade there would be a footgun.
"""

from __future__ import annotations

import asyncio
import atexit
import threading
from collections.abc import Coroutine
from datetime import datetime
from typing import TYPE_CHECKING, Any, TypeVar

from .engine import Engine
from .transport import TlsConfig
from .types import CancelOutcome, GateStatus, JobEvent, JobState, JobStatus

if TYPE_CHECKING:
    from .config import SdkSettings

_T = TypeVar("_T")


class _LoopThread:
    """A background thread hosting one asyncio loop for blocking round-trips."""

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, name="symba-sync-loop", daemon=True)
        self._started = False
        self._lock = threading.Lock()

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def ensure_started(self) -> None:
        with self._lock:
            if not self._started:
                self._thread.start()
                self._started = True

    def run(self, coro: Coroutine[Any, Any, _T], timeout: float | None = None) -> _T:
        self.ensure_started()
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout)

    def close(self) -> None:
        if not self._started:
            return
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)


def _guard_not_in_loop() -> None:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    raise RuntimeError(
        "SyncEngine called from inside a running event loop; you are already async — "
        "use symba.Engine (the async client) directly instead of the sync facade"
    )


class SyncJobHandle:
    """Blocking view over one job (spec 21)."""

    def __init__(self, loop: _LoopThread, handle: Any) -> None:
        self._loop = loop
        self._h = handle
        self.id = handle.id
        self.task_name = handle.task_name
        self.ctx_id = handle.ctx_id
        self.deduplicated = handle.deduplicated

    def result(self, timeout: float | None = None) -> Any:
        _guard_not_in_loop()
        return self._loop.run(self._h.result(timeout), timeout)

    def status(self) -> JobStatus:
        _guard_not_in_loop()
        return self._loop.run(self._h.status())

    def cancel(self, cascade: bool = True) -> CancelOutcome:
        _guard_not_in_loop()
        return self._loop.run(self._h.cancel(cascade))

    def events(self) -> list[JobEvent]:
        _guard_not_in_loop()
        return self._loop.run(self._h.events())

    def __repr__(self) -> str:
        return f"<SyncJobHandle {self.id} task={self.task_name!r}>"


class SyncGate:
    """Blocking view over a fan-out gate (spec 21)."""

    def __init__(self, loop: _LoopThread, gate: Any) -> None:
        self._loop = loop
        self._g = gate
        self.id = gate.id
        self.children = [SyncJobHandle(loop, c) for c in gate.children]

    def result(self, timeout: float | None = None) -> Any:
        _guard_not_in_loop()
        return self._loop.run(self._g.result(timeout), timeout)

    def status(self) -> GateStatus:
        _guard_not_in_loop()
        return self._loop.run(self._g.status())

    def __repr__(self) -> str:
        return f"<SyncGate {self.id} children={len(self.children)}>"


class SyncAdmin:
    """Blocking facade over :class:`~symba.admin.AdminClient` (spec 6.7, 21).

    The RFC's iKnowledge worker registers its ``graph-reconcile-tick`` schedule at
    startup from sync ``run_sync`` task bodies; this wrapper lets that happen without
    hand-rolling an event loop. Thin: every method is ``self._loop.run(...)`` over the
    real async admin surface, zero logic duplication.
    """

    def __init__(self, loop: _LoopThread, admin: Any) -> None:
        self._loop = loop
        self._admin = admin

    def upsert_cron(
        self,
        schedule_id: str,
        cron_expr: str,
        task_name: str,
        payload: dict[str, Any] | None = None,
        enabled: bool = True,
    ) -> Any:
        _guard_not_in_loop()
        return self._loop.run(
            self._admin.upsert_cron(schedule_id, cron_expr, task_name, payload, enabled)
        )

    def delete_cron(self, schedule_id: str) -> bool:
        _guard_not_in_loop()
        return self._loop.run(self._admin.delete_cron(schedule_id))

    def list_cron(self) -> list[Any]:
        _guard_not_in_loop()
        return self._loop.run(self._admin.list_cron())

    def set_cron_enabled(self, schedule_id: str, enabled: bool) -> Any:
        _guard_not_in_loop()
        return self._loop.run(self._admin.set_cron_enabled(schedule_id, enabled))

    def upsert_rate_class(self, name: str, capacity: float, refill_per_s: float) -> Any:
        _guard_not_in_loop()
        return self._loop.run(self._admin.upsert_rate_class(name, capacity, refill_per_s))

    def list_rate_classes(self) -> list[Any]:
        _guard_not_in_loop()
        return self._loop.run(self._admin.list_rate_classes())

    def list_workers(self) -> list[Any]:
        _guard_not_in_loop()
        return self._loop.run(self._admin.list_workers())


class SyncEngine:
    """Blocking wrapper around :class:`~symba.engine.Engine` (spec 21)."""

    def __init__(
        self,
        target: str | None = None,
        *,
        tenant: str = "default",
        token: str | None = None,
        tls: TlsConfig | None = None,
        default_pipeline: str | None = None,
        settings: SdkSettings | None = None,
        load_dotenv: bool = False,
    ) -> None:
        self._loop = _LoopThread()
        self._engine = Engine(
            target,
            tenant=tenant,
            token=token,
            tls=tls,
            default_pipeline=default_pipeline,
            settings=settings,
            load_dotenv=load_dotenv,
        )
        self.tenant = tenant
        self._admin: SyncAdmin | None = None
        atexit.register(self.close)

    @property
    def admin(self) -> SyncAdmin:
        """Blocking ``AdminService`` facade (cron + rate-class + fleet ops)."""
        if self._admin is None:
            self._admin = SyncAdmin(self._loop, self._engine.admin)
        return self._admin

    def submit(self, task: str, payload: Any = None, **kwargs: Any) -> SyncJobHandle:
        _guard_not_in_loop()
        handle = self._loop.run(self._engine.submit(task, payload, **kwargs))
        return SyncJobHandle(self._loop, handle)

    def submit_many(self, specs: list[dict[str, Any]]) -> list[SyncJobHandle]:
        _guard_not_in_loop()
        handles = self._loop.run(self._engine.submit_many(specs))
        return [SyncJobHandle(self._loop, h) for h in handles]

    def fan_out(
        self,
        children: list[dict[str, Any]],
        *,
        on_complete: dict[str, Any],
        gate_policy: str = "all_success",
        ctx_id: str | None = None,
    ) -> tuple[list[SyncJobHandle], SyncGate]:
        _guard_not_in_loop()
        handles, gate = self._loop.run(
            self._engine.fan_out(
                children, on_complete=on_complete, gate_policy=gate_policy, ctx_id=ctx_id
            )
        )
        return [SyncJobHandle(self._loop, h) for h in handles], SyncGate(self._loop, gate)

    def get_job(self, job_id: str) -> JobStatus:
        _guard_not_in_loop()
        return self._loop.run(self._engine.get_job(job_id))

    def probe(self, *, timeout_s: float = 8.0) -> Any:
        """Bounded control-plane probe for ``doctor`` (ENG-1 SDK half)."""
        _guard_not_in_loop()
        return self._loop.run(self._engine.probe(timeout_s=timeout_s), timeout=timeout_s + 5)

    def query(
        self,
        *,
        ctx_id: str | None = None,
        state: JobState | str | None = None,
        task_name: str | None = None,
        pipeline: str | None = None,
        stage: str | None = None,
        group_key: str | None = None,
        created_after: datetime | None = None,
        page_size: int = 100,
        limit: int | None = 1000,
    ) -> list[JobStatus]:
        _guard_not_in_loop()

        async def _collect() -> list[JobStatus]:
            return await self._engine.query(
                ctx_id=ctx_id,
                state=state,
                task_name=task_name,
                pipeline=pipeline,
                stage=stage,
                group_key=group_key,
                created_after=created_after,
                page_size=page_size,
                limit=limit,
            )

        return self._loop.run(_collect())

    def cancel(self, job_id: str, cascade: bool = True) -> CancelOutcome:
        _guard_not_in_loop()
        return self._loop.run(self._engine.cancel(job_id, cascade))

    def resubmit(self, job_id: str) -> SyncJobHandle:
        _guard_not_in_loop()
        handle = self._loop.run(self._engine.resubmit(job_id))
        return SyncJobHandle(self._loop, handle)

    def signal(self, wait_key: str, payload: Any = None, *, signaled_by: str = "") -> int:
        _guard_not_in_loop()
        return self._loop.run(self._engine.signal(wait_key, payload, signaled_by=signaled_by))

    def close(self) -> None:
        """Close the channel and stop the background loop thread."""
        try:
            self._loop.run(self._engine.aclose())
        except Exception:  # best-effort close at interpreter shutdown
            pass
        self._loop.close()

    def __enter__(self) -> SyncEngine:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


__all__ = ["SyncEngine", "SyncJobHandle", "SyncGate", "SyncAdmin"]
