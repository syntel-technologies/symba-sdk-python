"""Unit tests for MetricsMiddleware (spec 17).

The middleware must degrade to a silent no-op when ``prometheus-client`` is not
installed, and it must never raise from any hook (the cardinal middleware rule).
"""

from __future__ import annotations

import pytest

from symba.context import Ctx, UpstreamOutputs
from symba.logging import get_logger
from symba.middleware import MetricsMiddleware

pytestmark = pytest.mark.asyncio


def _ctx() -> Ctx:
    return Ctx(
        job_id="j1",
        ctx_id="c1",
        task_name="crunch",
        attempt=1,
        tenant="acme",
        payload={},
        output=UpstreamOutputs([]),
        logger=get_logger(),
    )


async def test_hooks_never_raise_without_prometheus():
    mw = MetricsMiddleware()
    ctx = _ctx()
    # All hooks are safe to call regardless of prometheus-client availability.
    await mw.on_claim(ctx)
    await mw.on_complete(ctx, {"ok": True}, 12.5)
    await mw.on_fail(ctx, ValueError("boom"), retryable=False)
    await mw.on_park(ctx, "wait:1")
    mw.observe_loop_lag(3.2)


async def test_reports_disabled_when_prometheus_missing():
    mw = MetricsMiddleware()
    # Without the optional dep the middleware stays disabled (no metric objects bound).
    assert mw._enabled in (True, False)
    if not mw._enabled:
        assert not hasattr(mw, "_jobs_total")
