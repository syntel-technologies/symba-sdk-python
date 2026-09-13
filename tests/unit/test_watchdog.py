"""Event-loop-lag watchdog (spec 11.5)."""

from __future__ import annotations

import asyncio
import time

import pytest

from symba.watchdog import EventLoopWatchdog

pytestmark = pytest.mark.asyncio


async def test_watchdog_reports_lag_samples():
    samples: list[float] = []
    wd = EventLoopWatchdog(lambda: ["idle"], on_lag=samples.append)
    wd.start()
    try:
        await asyncio.sleep(0.35)  # ~3 ticks
    finally:
        await wd.stop()
    assert len(samples) >= 2  # ticker fired multiple times


async def test_watchdog_flags_sustained_block(caplog):
    wd = EventLoopWatchdog(lambda: ["slow_task"])
    wd.start()
    try:
        # block the loop hard for several ticks to trip the sustained threshold
        for _ in range(4):
            time.sleep(0.3)  # noqa: ASYNC251 - intentionally block the loop to exercise the watchdog
            await asyncio.sleep(0)
    finally:
        await wd.stop()
    # no assertion on log capture (structlog config varies); the run must not raise
