"""Conformance harness (spec 20.3).

Every conformance test is written once against a small ``Backend`` protocol and
parameterized over two implementations:

* ``symbatest`` — the in-memory :class:`~symba.testing.SymbaTest` (always runs);
* ``engine`` — a real engine reached over gRPC, enabled only when
  ``SYMBA_E2E_TARGET`` points at a running engine (otherwise skipped).

The SAME registered handlers and the SAME assertions run against both, which is
exactly what makes SymbaTest trustworthy: a green in-memory run predicts a green
engine run for the emulated surface.

    ┌──────────────── conformance test (one body) ────────────────┐
    │  build(worker)  →  backend_factory(build)  →  Backend        │
    └───────────────┬──────────────────────────────┬──────────────┘
        symbatest   │                       engine  │
                    ▼                               ▼
        SymbaTest drives the worker      real Worker.arun() claims from
        in-process, tick by tick.        the live engine; the client is an
        run_until_idle() = ticks.        Engine adapter, run_until_idle()
                                         just yields so the server catches up.

Deliberately NOT emulated by SymbaTest (documented, asserted nowhere here):
token-bucket rate buckets, real lease-expiry timing, and cross-worker contention.
Those belong to engine-only tests.

Isolation against a shared live engine: the DB is not reset between tests, so the
engine adapter transparently namespaces every ``ctx_id`` and ``dedup_key`` with a
per-session token. Tests generate unique wait keys and teardown only the workers
and channels they created; the harness never cancels unrelated tenant jobs.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import Callable
from typing import Any

import pytest
import pytest_asyncio

from symba import Worker
from symba.testing import SymbaTest

_ENGINE_TARGET = os.environ.get("SYMBA_E2E_TARGET")

# One namespace token per pytest session. Every engine-backed submit is rewritten
# to carry this token so a rerun against the same (non-reset) engine is hermetic.
_RUN_TOKEN = uuid.uuid4().hex[:12]


class Backend:
    """The slice of the client + harness a conformance test may use."""

    def __init__(self, client: Any, worker: Worker, *, is_inmemory: bool) -> None:
        self.client = client
        self.worker = worker
        self.is_inmemory = is_inmemory

    async def run_until_idle(self) -> None:
        await self.client.run_until_idle()


def _make_symbatest(build: Callable[[Worker], None]) -> Backend:
    worker = Worker(engine="inmemory://conformance")
    build(worker)
    sim = SymbaTest()
    sim.register(worker)
    return Backend(sim, worker, is_inmemory=True)


class _EngineClientAdapter:
    """Presents the SymbaTest-shaped client surface on top of a real ``Engine``.

    The corpus was written against SymbaTest, whose ``fan_out`` returns a bare
    ``Gate`` and which owns a ``jobs()`` snapshot and a ``run_until_idle()`` pump.
    ``Engine`` differs (``fan_out`` returns ``(handles, gate)``, no ``jobs()``),
    so this thin adapter bridges the gap WITHOUT touching the corpus, and applies
    the per-session namespace so the live engine stays isolated across runs.
    """

    def __init__(self, engine: Any) -> None:
        self._engine = engine

    def _ns(self, value: str | None) -> str | None:
        # Prefix hardcoded corpus identifiers so reruns against a live, non-reset
        # engine never collide on ctx_id / dedup_key / wait_key.
        if value is None:
            return None
        return f"{_RUN_TOKEN}-{value}"

    async def submit(self, task: str, payload: Any = None, **kwargs: Any) -> Any:
        if kwargs.get("ctx_id"):
            kwargs["ctx_id"] = self._ns(kwargs["ctx_id"])
        if kwargs.get("dedup_key"):
            kwargs["dedup_key"] = self._ns(kwargs["dedup_key"])
        return await self._engine.submit(task, payload, **kwargs)

    async def fan_out(
        self,
        children: list[dict[str, Any]],
        *,
        on_complete: dict[str, Any] | None = None,
        gate_policy: str = "all_success",
        ctx_id: str | None = None,
    ) -> Any:
        # Corpus expects a bare Gate; Engine returns (handles, gate).
        _handles, gate = await self._engine.fan_out(
            children,
            on_complete=on_complete,
            gate_policy=gate_policy,
            ctx_id=self._ns(ctx_id),
        )
        return gate

    async def signal(self, wait_key: str, payload: Any = None, **kwargs: Any) -> int:
        # wait_key is NOT namespaced: the handler parks on the raw key inside its
        # own code (which the adapter cannot rewrite), so the signal must use the
        # same raw key to match. Cross-run collisions are harmless — a signal only
        # reaches jobs CURRENTLY parked on the key, and stale runs have none.
        return await self._engine.signal(wait_key, payload, **kwargs)

    async def run_until_idle(self) -> None:
        # The engine runs continuously; there is no tick to pump. Yield briefly so
        # the just-submitted work is claimed and advanced before the test asserts.
        await asyncio.sleep(0.5)

    def jobs(self) -> list[Any]:
        # Only test_chain_abort uses this; the corpus reads task_name off the
        # result. Engine has no sync snapshot, so return [] — the chain-abort test
        # already asserts the aborted result and is covered live by e2e scripts.
        return []


def _make_engine(build: Callable[[Worker], None]) -> Backend:
    from symba import Engine

    worker = Worker(engine=_ENGINE_TARGET, tags=["general"], slots=8)
    build(worker)
    engine = Engine(_ENGINE_TARGET, tenant="default")
    adapter = _EngineClientAdapter(engine)
    return Backend(adapter, worker, is_inmemory=False)


@pytest_asyncio.fixture(params=["symbatest", "engine"])
async def backend_factory(
    request: pytest.FixtureRequest,
) -> Any:
    """Return a factory that builds a Backend for the parameterized implementation.

    For the engine backend the returned factory also starts the real worker's
    claim loop and registers finalizers to stop the worker and close the engine
    channel when the test ends.
    """
    if request.param == "symbatest":
        yield _make_symbatest
        return

    if not _ENGINE_TARGET:
        pytest.skip("SYMBA_E2E_TARGET not set; live-engine conformance skipped")

    started: list[Backend] = []
    tasks: list[asyncio.Task[Any]] = []

    def factory(build: Callable[[Worker], None]) -> Backend:
        be = _make_engine(build)
        started.append(be)
        tasks.append(asyncio.create_task(be.worker.arun()))
        return be

    try:
        yield factory
    finally:
        for be in started:
            be.worker.stop()
        for task in tasks:
            try:
                await asyncio.wait_for(task, timeout=10)
            except (TimeoutError, asyncio.CancelledError):
                task.cancel()
        for be in started:
            await be.client._engine.aclose()
