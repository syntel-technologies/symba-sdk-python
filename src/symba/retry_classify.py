"""Retryable-vs-fatal classification of unhandled handler exceptions (spec 15.2).

When a handler raises something that is not a :class:`SymbaError`, this module
decides ``FailRequest.retryable``. The engine trusts the SDK — it is closest to
the exception. The rules are *data* (:data:`CLASSIFICATION_RULES`), unit-tested
rule by rule, and per-worker extensible via ``Worker(classify_overrides=[...])``.

HTTP clients (httpx / httpcore / aiohttp) are matched by **duck-typing**
(attribute probe + module-name prefix) so the SDK stays free of HTTP-client
dependencies while still classifying the three big clients correctly.

Timeouts are likewise matched by name: any exception whose class is named
``TimeoutError`` is treated as retryable regardless of its module, so library
timeouts that do NOT subclass the builtin (e.g. ``sqlalchemy.exc.TimeoutError``)
are recognised without importing those libraries.
"""

from __future__ import annotations

import asyncio
import errno
from collections.abc import Callable
from dataclasses import dataclass

#: errnos that indicate a transient infrastructure blip (spec 15.2).
_TRANSIENT_ERRNOS = frozenset(
    {
        errno.ECONNRESET,
        errno.ETIMEDOUT,
        errno.EPIPE,
        errno.ECONNREFUSED,
        errno.ECONNABORTED,
        errno.EHOSTUNREACH,
        errno.ENETUNREACH,
        errno.EAGAIN,
    }
)

_RETRYABLE_STATUS = frozenset({408, 429, 500, 502, 503, 504})
_FATAL_STATUS = frozenset({400, 401, 403, 404, 422})

#: HTTP-client module prefixes we recognise by name, never by import (spec 15.2).
_HTTP_MODULE_PREFIXES = ("httpx", "httpcore", "aiohttp", "urllib3", "requests")


@dataclass(slots=True, frozen=True)
class ClassificationRule:
    """One classification rule. ``predicate`` returns True to claim an exception."""

    name: str
    predicate: Callable[[BaseException], bool]
    retryable: bool


def _status_code(exc: BaseException) -> int | None:
    for attr in ("status_code", "status"):
        value = getattr(exc, attr, None)
        if isinstance(value, int):
            return value
        resp = getattr(exc, "response", None)
        if resp is not None:
            rv = getattr(resp, attr, None)
            if isinstance(rv, int):
                return rv
    return None


def _is_http_transport_error(exc: BaseException) -> bool:
    module = type(exc).__module__ or ""
    if not module.startswith(_HTTP_MODULE_PREFIXES):
        return False
    name = type(exc).__name__.lower()
    return "timeout" in name or "connect" in name or "transport" in name or "network" in name


def _transient_oserror(exc: BaseException) -> bool:
    return isinstance(exc, OSError) and exc.errno in _TRANSIENT_ERRNOS


#: Ordered rules — first match wins (spec 15.2). Per-worker overrides prepend.
CLASSIFICATION_RULES: list[ClassificationRule] = [
    ClassificationRule(
        "http_retryable_status",
        lambda e: _status_code(e) in _RETRYABLE_STATUS,
        retryable=True,
    ),
    ClassificationRule(
        "http_fatal_status",
        lambda e: _status_code(e) in _FATAL_STATUS,
        retryable=False,
    ),
    ClassificationRule(
        "timeout",
        lambda e: isinstance(e, (TimeoutError, asyncio.TimeoutError)),
        retryable=True,
    ),
    ClassificationRule(
        # Any library that names its exception ``TimeoutError`` but does NOT
        # subclass the builtin (e.g. ``sqlalchemy.exc.TimeoutError``, some HTTP
        # pools) is a transient timeout. Match by name, never by import, so the
        # SDK stays dependency-light (SDK-6).
        "named_timeout",
        lambda e: type(e).__name__ == "TimeoutError",
        retryable=True,
    ),
    ClassificationRule(
        "connection_error",
        lambda e: isinstance(e, ConnectionError),
        retryable=True,
    ),
    ClassificationRule(
        "transient_oserror",
        _transient_oserror,
        retryable=True,
    ),
    ClassificationRule(
        "http_transport_error",
        _is_http_transport_error,
        retryable=True,
    ),
    ClassificationRule(
        # A urllib3/http.client body cut off mid-transfer (ProtocolError /
        # IncompleteRead). Idempotent GETs are safe to re-issue, so this is a
        # transient transport failure, not permanent. Match by class name so the
        # SDK does not import urllib3 (mirrors _HTTP_MODULE_PREFIXES policy).
        "incomplete_body_read",
        lambda e: type(e).__name__ in {"ProtocolError", "IncompleteRead"},
        retryable=True,
    ),
    ClassificationRule(
        "programming_or_data_error",
        lambda e: (
            isinstance(e, (KeyError, TypeError, ValueError, AttributeError))
            or type(e).__name__ == "ValidationError"
        ),
        retryable=False,
    ),
]

#: Exceptions that are process-level, not job-level — re-raised, never classified.
_PROCESS_LEVEL = (MemoryError, SystemExit, KeyboardInterrupt)


def classify(
    exc: BaseException,
    *,
    overrides: list[ClassificationRule] | None = None,
) -> bool:
    """Return whether ``exc`` should be retried (spec 15.2).

    Re-raises process-level exceptions (``MemoryError`` etc.) — those are the
    worker's concern, not a job outcome. The conservative default for anything
    unmatched is **not retryable**; handlers opt in via
    ``raise RetryableError(...) from exc``.
    """
    if isinstance(exc, _PROCESS_LEVEL):
        raise exc
    rules = (overrides or []) + CLASSIFICATION_RULES
    for rule in rules:
        try:
            if rule.predicate(exc):
                return rule.retryable
        except Exception:
            continue
    return False


__all__ = ["classify", "ClassificationRule", "CLASSIFICATION_RULES"]
