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


async def test_explicit_slots_win():
    w = _worker(slots=7)

    @w.task("t", profile=Profile.IO)
    async def t(ctx: Ctx, payload: dict) -> dict:
        return {}

    assert w._resolve_slots() == 7


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
