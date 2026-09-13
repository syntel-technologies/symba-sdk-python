"""Unit tests for the sync facade (spec 21).

The facade's contract is mechanical: a background loop thread runs coroutines to
completion, and calling it from inside a running loop raises. We exercise both
without needing a live engine by driving the private ``_LoopThread`` directly and
by asserting the in-loop guard.
"""

from __future__ import annotations

import asyncio
import atexit
import threading
import time
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from symba.sync import SyncEngine, _guard_not_in_loop, _LoopThread


def test_loop_thread_runs_coroutine_to_completion():
    loop = _LoopThread()
    try:

        async def add(a: int, b: int) -> int:
            await asyncio.sleep(0)
            return a + b

        assert loop.run(add(2, 3)) == 5
        assert loop.run(add(10, 20)) == 30
    finally:
        loop.close()


def test_loop_thread_close_is_idempotent_before_and_after_start() -> None:
    never_started = _LoopThread()
    assert never_started.close()
    assert never_started.close()

    started = _LoopThread()
    assert started.run(asyncio.sleep(0, result="ok")) == "ok"
    assert started.close()
    assert started.close()
    coroutine = asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="closed"):
        started.run(coroutine)


def test_loop_thread_close_is_bounded_with_in_flight_work() -> None:
    loop = _LoopThread()
    started = threading.Event()

    async def blocked() -> None:
        started.set()
        await asyncio.Event().wait()

    outcome: list[BaseException] = []

    def invoke() -> None:
        try:
            loop.run(blocked())
        except BaseException as exc:
            outcome.append(exc)

    caller = threading.Thread(target=invoke)
    caller.start()
    assert started.wait(timeout=1)
    before = time.monotonic()
    loop.close(timeout=0.2)
    elapsed = time.monotonic() - before
    caller.join(timeout=1)

    assert elapsed < 1.0
    assert not caller.is_alive()
    assert outcome
    assert loop.close(timeout=1.0)


def test_guard_allows_sync_context():
    # No running loop here — must not raise.
    _guard_not_in_loop()


@pytest.mark.asyncio
async def test_guard_rejects_async_context():
    with pytest.raises(RuntimeError, match="already async"):
        _guard_not_in_loop()


@pytest.mark.asyncio
async def test_sync_engine_methods_reject_async_context():
    # Constructing is fine (no I/O yet); calling a verb from a loop must raise.
    eng = SyncEngine("grpc://localhost:1")
    try:
        with pytest.raises(RuntimeError, match="already async"):
            eng.submit("noop", {})
    finally:
        # close() runs on the background loop thread; safe from here.
        eng._loop.close()


def test_sync_engine_close_is_idempotent_and_unregisters_atexit(monkeypatch) -> None:
    engine = SyncEngine("grpc://localhost:1")
    close = AsyncMock()
    cast(Any, engine)._engine = SimpleNamespace(aclose=close)
    unregistered: list[object] = []
    monkeypatch.setattr(atexit, "unregister", unregistered.append)

    engine.close()
    engine.close()

    close.assert_awaited_once()
    assert len(unregistered) == 1
