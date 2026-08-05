"""Worker runtime tests: registration, boot validation, slot accounting (spec 8)."""

from __future__ import annotations

import asyncio

import pytest

from symba.context import Ctx
from symba.dispatch import DispatchDeps, Dispatcher
from symba.errors import ConfigError
from symba.executors.asyncio_executor import AsyncioExecutor
from symba.middleware import MiddlewareChain
from symba.profiles import Profile
from symba.worker import Worker

from ._fakes import FakeWorkerStub, make_assignment

pytestmark = pytest.mark.asyncio


def _worker(**kw) -> Worker:
    return Worker("grpc://localhost:7233", **kw)


def _fake_dispatcher(w: Worker, stub: FakeWorkerStub) -> Dispatcher:
    return Dispatcher(
        DispatchDeps(
            stub=stub,  # type: ignore[arg-type]
            registry=w.registry,
            middleware=MiddlewareChain([]),
            executors={Profile.IO: AsyncioExecutor()},
            tenant="default",
            heartbeat_interval_s=15.0,
            classify_overrides=[],
        )
    )


async def test_task_decorator_registers():
    w = _worker()

    @w.task("echo", profile=Profile.IO)
    async def echo(ctx: Ctx, payload: dict) -> dict:
        return payload

    assert "echo" in w.registry.names()
    # decorator returns the function unchanged
    assert await echo(None, {"a": 1}) == {"a": 1}  # type: ignore[arg-type]


async def test_checkpoint_redis_url_kwarg_flows_to_settings():
    """SDK-5: an explicit kwarg configures the fast path without an env var."""
    w = _worker(checkpoint_redis_url="redis://cache:6379/0")
    assert w._settings.redis.url == "redis://cache:6379/0"


async def test_checkpoint_redis_url_defaults_to_none():
    """SDK-5: absent kwarg + absent env leaves the durable-only path (url None)."""
    w = _worker()
    assert w._settings.redis.url is None


async def test_boot_rejects_sync_io_handler():
    w = _worker()

    @w.task("bad", profile=Profile.IO)
    def bad(ctx: Ctx, payload: dict) -> dict:  # sync io handler is illegal
        return payload

    with pytest.raises(ConfigError, match="io-profile"):
        w.registry.validate(strict_schemas=False, heartbeat_interval_s=15.0)


async def test_boot_rejects_zero_tasks():
    w = _worker()
    with pytest.raises(ConfigError, match="no tasks registered"):
        w.registry.validate(strict_schemas=False, heartbeat_interval_s=15.0)


async def test_boot_rejects_long_io_task_without_explicit_lease():
    """SDK-7: a long io timeout under the engine-default (~60s) lease fails fast."""
    w = _worker()

    @w.task("parse", profile=Profile.IO, timeout_s=1800)
    async def parse(ctx: Ctx, payload: dict) -> dict:
        return payload

    with pytest.raises(ConfigError, match="lease"):
        w.registry.validate(strict_schemas=False, heartbeat_interval_s=15.0)


async def test_long_io_task_with_explicit_lease_is_ok():
    """SDK-7: setting lease_ttl_s >= timeout_s clears the boot check."""
    w = _worker()

    @w.task("parse", profile=Profile.IO, timeout_s=1800, lease_ttl_s=1800)
    async def parse(ctx: Ctx, payload: dict) -> dict:
        return payload

    w.registry.validate(strict_schemas=False, heartbeat_interval_s=15.0)


async def test_short_io_task_without_lease_is_ok():
    """SDK-7: a normal io task (no timeout_s) is not a false positive."""
    w = _worker()

    @w.task("quick", profile=Profile.IO)
    async def quick(ctx: Ctx, payload: dict) -> dict:
        return payload

    w.registry.validate(strict_schemas=False, heartbeat_interval_s=15.0)


async def test_explicit_slots_win():
    w = _worker(slots=7)

    @w.task("t", profile=Profile.IO)
    async def t(ctx: Ctx, payload: dict) -> dict:
        return {}

    assert w._resolve_slots() == 7


async def test_explicit_cpu_slots_win():
    """cpu_slots is honoured verbatim for the forkserver pool size."""
    w = _worker(slots=200, cpu_slots=6)
    assert w._settings.worker.cpu_slots == 6
    assert w._resolve_cpu_slots() == 6


async def test_cpu_slots_default_is_bounded_by_cores_not_slots(monkeypatch):
    """A high io ``slots`` budget must NOT size the cpu pool -- that OOM-kills the
    host by forking hundreds of model-loading subprocesses. The default caps at
    the core count."""
    import symba.worker as worker_mod

    monkeypatch.setattr(worker_mod.os, "cpu_count", lambda: 8)
    w = _worker(slots=200)  # no explicit cpu_slots
    w._slots = w._resolve_slots()
    assert w._resolve_cpu_slots() == 8  # min(200, 8), NOT 200


async def test_cpu_slots_default_never_exceeds_slots(monkeypatch):
    """When there are more cores than io slots, the cpu pool follows slots."""
    import symba.worker as worker_mod

    monkeypatch.setattr(worker_mod.os, "cpu_count", lambda: 64)
    w = _worker(slots=4)
    w._slots = w._resolve_slots()
    assert w._resolve_cpu_slots() == 4  # min(4, 64)


async def test_cpu_executor_sized_by_cpu_slots_not_io_slots(monkeypatch):
    """End-to-end: booting a worker with a cpu task builds a ProcessExecutor
    bounded by cpu_slots, never by the (large) io slot budget."""
    import symba.worker as worker_mod

    monkeypatch.setattr(worker_mod.os, "cpu_count", lambda: 8)
    w = _worker(slots=200)

    @w.task("chunk", profile=Profile.CPU)
    def chunk(ctx: Ctx, payload: dict) -> dict:  # sync, cpu profile
        return {}

    w._boot()
    cpu_executor = w._executors[Profile.CPU]
    assert cpu_executor._max_workers == 8  # NOT 200
    # cpu pool is lazy: nothing forked at boot.
    assert cpu_executor._spawned == 0


async def test_cpu_max_jobs_flows_to_executor():
    """The recycle cap is threaded from the kwarg into the ProcessExecutor."""
    w = _worker(slots=200, cpu_slots=4, cpu_max_jobs_per_process=50)

    @w.task("chunk", profile=Profile.CPU)
    def chunk(ctx: Ctx, payload: dict) -> dict:
        return {}

    w._boot()
    assert w._executors[Profile.CPU]._max_jobs_per_process == 50


async def test_announce_free_slots_zeroed_by_admission():
    """The announced slot count is honest local capacity: zeroed while draining or
    when the admission gate is closed, else the real free-slot count."""
    w = _worker(slots=5)
    w._free_slots = 5

    assert w._announce_free_slots() == 5
    w._admission_ok = False
    assert w._announce_free_slots() == 0  # admission gate closed
    w._admission_ok = True
    w._accepting = False
    assert w._announce_free_slots() == 0  # draining


async def test_admission_loop_transitions_and_forces_reannounce():
    """The poller flips _admission_ok on transition and pings _slot_changed so the
    claim stream re-announces the new free_slots."""
    state = {"ok": False}
    w = _worker(admission_control=lambda: state["ok"])
    w._admission_poll_s = 0.01

    w._start_admission_loop()
    try:
        await asyncio.sleep(0.05)
        assert w._admission_ok is False
        assert w._slot_changed.is_set()

        w._slot_changed.clear()
        state["ok"] = True
        await asyncio.sleep(0.05)
        assert w._admission_ok is True
        assert w._slot_changed.is_set()  # re-announce forced on recovery too
    finally:
        w._stopped.set()
        await w._stop_background_loops()


async def test_admission_loop_fails_open_on_hook_exception():
    """A raising probe must never wedge the worker: treat as 'accept'."""
    def boom() -> bool:
        raise RuntimeError("probe blew up")

    w = _worker(admission_control=boom)
    w._admission_poll_s = 0.01

    w._start_admission_loop()
    try:
        await asyncio.sleep(0.05)
        assert w._admission_ok is True  # stayed open despite the exception
    finally:
        w._stopped.set()
        await w._stop_background_loops()


async def test_admission_loop_not_started_when_hook_absent():
    """No hook -> no poller task at all (feature entirely inert)."""
    w = _worker()  # no admission_control
    w._start_admission_loop()
    assert w._admission_task is None


async def test_idle_claim_stream_periodically_refreshes_registration():
    """An idle worker must not become stale while its claim stream is healthy."""
    w = _worker(heartbeat_interval_s=0.01)
    w._slots = 7
    w._free_slots = 7
    requests = w._claim_requests(["io"])

    first = await anext(requests)
    second = await asyncio.wait_for(anext(requests), timeout=0.1)

    assert first.worker_id == w.worker_id
    assert first.free_slots == 7
    assert second.worker_id == w.worker_id
    assert second.free_slots == 7
    assert list(second.tags) == ["io"]

    w._stopped.set()
    await requests.aclose()


async def test_claim_stream_reannounces_immediately_on_slot_change():
    """Capacity changes should not wait for the periodic idle heartbeat."""
    w = _worker(heartbeat_interval_s=10)
    w._slots = 7
    w._free_slots = 7
    requests = w._claim_requests(["io"])

    first = await anext(requests)
    next_request = asyncio.create_task(anext(requests))
    await asyncio.sleep(0)
    w._free_slots = 6
    w._slot_changed.set()
    second = await asyncio.wait_for(next_request, timeout=0.1)

    assert first.free_slots == 7
    assert second.free_slots == 6

    w._stopped.set()
    await requests.aclose()


async def test_liveness_loop_touches_file(tmp_path):
    """The liveness writer creates + refreshes the file from the event loop."""
    live = tmp_path / "worker.live"
    w = _worker(liveness_file=str(live))
    w._heartbeat_interval_s = 0.01

    assert not live.exists()
    w._start_liveness_loop()
    try:
        await asyncio.sleep(0.05)
        assert live.exists()
        first = live.stat().st_mtime_ns
        await asyncio.sleep(0.05)
        assert live.stat().st_mtime_ns >= first  # kept fresh
    finally:
        w._stopped.set()
        await w._stop_background_loops()


async def test_liveness_loop_not_started_when_unset():
    """No path -> no writer task (feature inert; healthcheck falls back to pgrep)."""
    w = _worker()
    w._start_liveness_loop()
    assert w._liveness_task is None


async def test_slot_release_is_symmetric():
    """Every spawned task releases exactly one slot via the done-callback."""
    w = _worker(slots=2)

    started = asyncio.Event()
    release = asyncio.Event()

    @w.task("slow", profile=Profile.IO)
    async def slow(ctx: Ctx, payload: dict) -> dict:
        started.set()
        await release.wait()
        return {}

    w._boot()
    stub = FakeWorkerStub()
    w._stub = stub  # type: ignore[assignment]
    w._dispatcher = _fake_dispatcher(w, stub)

    assert w._free_slots == 2
    w._handle_assignment(make_assignment("j1", task_name="slow"))
    assert w._free_slots == 1
    await started.wait()

    release.set()
    # allow the spawned task + done-callback to run
    await asyncio.gather(*w._running, return_exceptions=True)
    await asyncio.sleep(0)

    assert w._free_slots == 2
    assert not w._running
    assert len(stub.completes) == 1


async def test_over_assignment_is_rejected_retryable():
    w = _worker(slots=1)

    @w.task("t", profile=Profile.IO)
    async def t(ctx: Ctx, payload: dict) -> dict:
        return {}

    w._boot()
    stub = FakeWorkerStub()
    w._stub = stub  # type: ignore[assignment]
    w._free_slots = 0  # simulate no free slot

    w._handle_assignment(make_assignment("over", task_name="t"))
    await asyncio.gather(*w._running, return_exceptions=True)

    assert len(stub.fails) == 1
    assert stub.fails[0].error_type == "NoFreeSlot"
    assert stub.fails[0].retryable is True


async def test_drain_waits_for_running_tasks():
    w = _worker(slots=2, shutdown_drain_s=1.0)

    done = asyncio.Event()

    @w.task("t", profile=Profile.IO)
    async def t(ctx: Ctx, payload: dict) -> dict:
        await asyncio.sleep(0.05)
        done.set()
        return {}

    w._boot()
    stub = FakeWorkerStub()
    w._stub = stub  # type: ignore[assignment]
    w._dispatcher = _fake_dispatcher(w, stub)

    w._handle_assignment(make_assignment("j1", task_name="t"))
    await w._drain()

    assert done.is_set()
    assert not w._running
