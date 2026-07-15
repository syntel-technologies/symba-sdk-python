"""Logging (spec 18).

Prime directive: the SDK does **not** configure structlog if the host app already
has (detected via ``structlog.is_configured()``). It only binds its context keys,
so ``ctx.logger`` is a child of whatever pipeline the process owns.

Standalone workers (no prior config) get the engine-identical pipeline.

Hard rules (ruff-enforced elsewhere): no emojis, no ``print()``, no unstructured
interpolated strings, and ``import logging`` is forbidden outside this module.
"""

from __future__ import annotations

import logging  # the one module allowed to touch stdlib logging (spec 18)
import os
import socket
import uuid
from typing import Any, cast

import structlog
from structlog.typing import EventDict, WrappedLogger

_configured_by_sdk = False


def resolve_worker_name(explicit: str | None = None) -> str:
    """Worker-name precedence (spec 18, reused from engine 7.3).

    ``explicit`` > ``SYMBA_WORKER_NAME`` > ``WORKER_NAME`` > ``HOSTNAME`` >
    ``<hostname>-<uuid8>``. The same stable name flows to ``ClaimRequest.worker_id``,
    ``jobs.claimed_by``, the fleet UI, and every log line.
    """
    for candidate in (
        explicit,
        os.environ.get("SYMBA_WORKER_NAME"),
        os.environ.get("WORKER_NAME"),
        os.environ.get("HOSTNAME"),
    ):
        if candidate:
            return candidate
    return f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"


def _current_trace_id() -> str:
    """Return the active otel trace id as 32 hex chars, or "" when unavailable."""
    import importlib

    try:
        trace = cast(Any, importlib.import_module("opentelemetry.trace"))
    except ImportError:
        return ""
    try:
        raw = trace.get_current_span().get_span_context().trace_id
        value = int(raw) if isinstance(raw, int) else 0
        return format(value, "032x") if value else ""
    except Exception:
        return ""


def _trace_id(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
    """Attach an otel trace id if one is present, else an empty string."""
    event_dict.setdefault("trace_id", _current_trace_id())
    return event_dict


def configure(
    *,
    level: str = "INFO",
    fmt: str = "json",
    worker_name: str | None = None,
    version: str | None = None,
) -> None:
    """Configure the engine-identical default pipeline — ONLY if the host app has
    not already configured structlog (spec 18)."""
    global _configured_by_sdk
    if structlog.is_configured():
        return

    renderer: Any = (
        structlog.dev.ConsoleRenderer() if fmt == "console" else structlog.processors.JSONRenderer()
    )

    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.contextvars.merge_contextvars,
            _app_context(worker_name=worker_name, version=version),
            _trace_id,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping().get(level.upper(), logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    _configured_by_sdk = True


def _app_context(*, worker_name: str | None, version: str | None):
    ctx = {"app_name": "symba", "version": version or "", "worker_name": worker_name or ""}

    def processor(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
        for key, value in ctx.items():
            event_dict.setdefault(key, value)
        return event_dict

    return processor


def get_logger(**bound: Any) -> structlog.stdlib.BoundLogger:
    """Return a bound logger; a child of the host pipeline when one exists."""
    return structlog.get_logger().bind(**bound)


__all__ = ["configure", "get_logger", "resolve_worker_name"]
