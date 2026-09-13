"""Worker middleware (spec 17).

Hooks around the job lifecycle. The cardinal rule: **middleware must not raise** —
every hook call is wrapped, exceptions logged and suppressed, so a broken metrics
middleware can never fail a job. ``LoggingMiddleware`` is always prepended;
``MetricsMiddleware`` lands in M7.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from .logging import get_logger

if TYPE_CHECKING:
    from .context import Ctx

_log = get_logger(component="worker")


@runtime_checkable
class WorkerMiddleware(Protocol):
    async def on_claim(self, ctx: Ctx) -> None: ...
    async def on_complete(self, ctx: Ctx, result: Any, duration_ms: float) -> None: ...
    async def on_fail(self, ctx: Ctx, exc: BaseException, retryable: bool) -> None: ...
    async def on_park(self, ctx: Ctx, wait_key: str) -> None:
        """Optional: job parked on a wait_for_event (spec 14.1). Default: no-op."""
        ...


class LoggingMiddleware:
    """One INFO line per lifecycle transition, engine ``job_events`` vocabulary (spec 17)."""

    async def on_claim(self, ctx: Ctx) -> None:
        ctx.logger.info("[dispatch] claimed", event_kind="claimed")

    async def on_complete(self, ctx: Ctx, result: Any, duration_ms: float) -> None:
        ctx.logger.info("[dispatch] succeeded", event_kind="succeeded", duration_ms=duration_ms)

    async def on_fail(self, ctx: Ctx, exc: BaseException, retryable: bool) -> None:
        ctx.logger.warning(
            "[dispatch] failed",
            event_kind="failed",
            retryable=retryable,
            error_type=type(exc).__name__,
        )

    async def on_park(self, ctx: Ctx, wait_key: str) -> None:
        ctx.logger.info("[dispatch] parked", event_kind="parked", wait_key=wait_key)


class MiddlewareChain:
    """Runs hooks with the spec's ordering + never-raise guarantee (spec 17)."""

    def __init__(self, middleware: list[WorkerMiddleware]) -> None:
        self._middleware = middleware

    async def on_claim(self, ctx: Ctx) -> None:
        for mw in self._middleware:  # registration order
            await self._safe(mw.on_claim, ctx)

    async def on_complete(self, ctx: Ctx, result: Any, duration_ms: float) -> None:
        for mw in reversed(self._middleware):  # reverse = nesting semantics
            await self._safe(mw.on_complete, ctx, result, duration_ms)

    async def on_fail(self, ctx: Ctx, exc: BaseException, retryable: bool) -> None:
        for mw in reversed(self._middleware):
            await self._safe(mw.on_fail, ctx, exc, retryable)

    async def on_park(self, ctx: Ctx, wait_key: str) -> None:
        for mw in reversed(self._middleware):
            hook = getattr(mw, "on_park", None)
            if hook is not None:
                await self._safe(hook, ctx, wait_key)

    @staticmethod
    async def _safe(hook: Any, *args: Any) -> None:
        try:
            await hook(*args)
        except Exception as exc:  # middleware must never fail a job
            _log.warning("middleware_error", hook=getattr(hook, "__name__", "?"), error=str(exc))


class MetricsMiddleware:
    """Prometheus metrics (spec 17).

    Emits the four spec metrics when ``prometheus-client`` is importable, and
    degrades to a silent no-op otherwise — metrics are observability, never a hard
    dependency. Optionally starts a local scrape endpoint via ``port``.

    * ``symba_worker_jobs_total{task,outcome}`` — counter, outcome in
      ``{succeeded, failed}``;
    * ``symba_worker_job_duration_seconds{task}`` — histogram of handler duration;
    * ``symba_worker_slots_busy`` — gauge, in-flight jobs on this worker;
    * ``symba_worker_event_loop_lag_ms`` — gauge, most recent watchdog sample.
    """

    def __init__(self, *, port: int | None = None, namespace: str = "symba_worker") -> None:
        self._enabled = False
        self._port = port
        try:
            from prometheus_client import (  # type: ignore[import-not-found]
                Counter,
                Gauge,
                Histogram,
                start_http_server,
            )
        except ImportError:
            _log.info("metrics_disabled_no_prometheus_client")
            return
        self._jobs_total = Counter(
            f"{namespace}_jobs_total", "Jobs finished, by task and outcome", ["task", "outcome"]
        )
        self._job_duration = Histogram(
            f"{namespace}_job_duration_seconds", "Handler duration in seconds", ["task"]
        )
        self._slots_busy = Gauge(f"{namespace}_slots_busy", "In-flight jobs on this worker")
        self._loop_lag = Gauge(f"{namespace}_event_loop_lag_ms", "Most recent event-loop lag (ms)")
        self._enabled = True
        if port is not None:
            start_http_server(port)
            _log.info("metrics_endpoint_started", port=port)

    def observe_loop_lag(self, lag_ms: float) -> None:
        """Feed a watchdog sample into the gauge (wired by the Worker, spec 11.5)."""
        if self._enabled:
            self._loop_lag.set(lag_ms)

    async def on_claim(self, ctx: Ctx) -> None:
        if self._enabled:
            self._slots_busy.inc()

    async def on_complete(self, ctx: Ctx, result: Any, duration_ms: float) -> None:
        if not self._enabled:
            return
        self._slots_busy.dec()
        self._jobs_total.labels(task=ctx.task_name, outcome="succeeded").inc()
        self._job_duration.labels(task=ctx.task_name).observe(duration_ms / 1000.0)

    async def on_fail(self, ctx: Ctx, exc: BaseException, retryable: bool) -> None:
        if not self._enabled:
            return
        self._slots_busy.dec()
        self._jobs_total.labels(task=ctx.task_name, outcome="failed").inc()

    async def on_park(self, ctx: Ctx, wait_key: str) -> None:
        if self._enabled:
            self._slots_busy.dec()


__all__ = ["WorkerMiddleware", "LoggingMiddleware", "MetricsMiddleware", "MiddlewareChain"]
