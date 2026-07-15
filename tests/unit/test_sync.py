"""Unit tests for the sync facade (spec 21).

The facade's contract is mechanical: a background loop thread runs coroutines to
completion, and calling it from inside a running loop raises. We exercise both
without needing a live engine by driving the private ``_LoopThread`` directly and
by asserting the in-loop guard.
"""

from __future__ import annotations

import asyncio

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
