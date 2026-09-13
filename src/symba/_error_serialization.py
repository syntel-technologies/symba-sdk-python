"""Safe serialization for handler exceptions crossing into the engine.

Third-party provider exceptions frequently render response bodies, headers,
prompts, or credentials from ``str(exc)``. Only a bounded public message and a
small allow-list of scalar diagnostics may cross the worker RPC boundary.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any

from .errors import RateLimitedError, SymbaError

_MESSAGE_CAP = 2048
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9._:/-]{1,128}$")
_SAFE_MODULE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]{0,127}$")
_SECRET_TOKEN_RE = re.compile(r"(?i)\b(?:sk|pk|api)-[A-Za-z0-9_-]{8,}\b")
_BEARER_RE = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{6,}")
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(api[_-]?key|authorization|access[_-]?token|refresh[_-]?token|secret|password)"
    r"(\s*[:=]\s*)([^\s,;]+)"
)
_STRUCTURED_BODY_MARKERS = (
    "response body",
    "response_body",
    "response content",
    "response_content",
    "'headers':",
    '"headers":',
    "headers=",
    "'body':",
    '"body":',
    "body=",
    "{'error':",
    '{"error":',
    "<html",
    "<!doctype",
)
_REQUEST_ID_HEADERS = (
    "x-request-id",
    "request-id",
    "x-amzn-requestid",
    "x-goog-request-id",
    "x-openrouter-request-id",
    "cf-ray",
)


@dataclass(frozen=True, slots=True)
class SerializedFailure:
    error_type: str
    message: str
    metadata: dict[str, object]
    rate_limited: bool
    retry_after_s: float | None


def serialize_exception(exc: BaseException) -> SerializedFailure:
    """Convert a live exception to a provider-body-free wire representation."""

    already_safe = getattr(exc, "_symba_serialized_failure", None)
    if isinstance(already_safe, SerializedFailure):
        return already_safe

    error_type = str(getattr(exc, "original_type", None) or type(exc).__name__)[:128]
    module = type(exc).__module__ or ""
    response = _read_attr(exc, "response")
    status_code = _status_code(exc, response)
    error_code = _safe_identifier(_read_attr(exc, "code"))
    if error_code is None and isinstance(exc, SymbaError):
        error_code = _safe_identifier(exc.error_code)
    request_id = _request_id(exc, response)
    retry_after_s = _retry_after(exc, response)
    rate_limited = _is_rate_limited(exc, status_code, error_code)

    metadata: dict[str, object] = {}
    if _SAFE_MODULE_RE.fullmatch(module):
        metadata["module"] = module
    if status_code is not None:
        metadata["status_code"] = status_code
    if error_code is not None:
        metadata["error_code"] = error_code
    if request_id is not None:
        metadata["request_id"] = request_id
    if retry_after_s is not None:
        metadata["retry_after_s"] = retry_after_s

    external = (
        response is not None
        or _read_attr(exc, "body") is not None
        or _read_attr(exc, "headers") is not None
        or (status_code is not None and module not in {"builtins", "__main__"})
    )
    message = _public_message(exc, external=external, rate_limited=rate_limited, status_code=status_code)
    return SerializedFailure(
        error_type=error_type,
        message=message,
        metadata=metadata,
        rate_limited=rate_limited,
        retry_after_s=retry_after_s,
    )


def _public_message(
    exc: BaseException,
    *,
    external: bool,
    rate_limited: bool,
    status_code: int | None,
) -> str:
    if rate_limited:
        return "External service rate limited the request"
    if external:
        suffix = f" (status {status_code})" if status_code is not None else ""
        return f"External service request failed{suffix}"

    module = type(exc).__module__ or ""
    if not isinstance(exc, SymbaError) and module not in {"builtins", "__main__"}:
        return "Task handler failed"
    try:
        raw = str(exc)
    except Exception:
        return "Task handler failed"
    return _sanitize_freeform(raw)


def _sanitize_freeform(message: str) -> str:
    compact = " ".join(message.split())
    compact = _BEARER_RE.sub("Bearer [redacted]", compact)
    compact = _SECRET_TOKEN_RE.sub("[redacted]", compact)
    compact = _SECRET_ASSIGNMENT_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}[redacted]", compact)
    lowered = compact.casefold()
    if (
        not compact
        or any(marker in lowered for marker in _STRUCTURED_BODY_MARKERS)
        or compact.startswith(("{", "["))
        or "http://" in lowered
        or "https://" in lowered
    ):
        return "Task handler failed"
    return compact[:_MESSAGE_CAP]


def _read_attr(value: object, name: str) -> Any:
    try:
        return getattr(value, name, None)
    except Exception:
        return None


def _status_code(exc: BaseException, response: object | None) -> int | None:
    for source in (exc, response):
        if source is None:
            continue
        for name in ("status_code", "status"):
            value = _read_attr(source, name)
            if isinstance(value, int) and not isinstance(value, bool) and 100 <= value <= 599:
                return value
    return None


def _safe_identifier(value: object) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    text = str(value)
    return text if _SAFE_IDENTIFIER_RE.fullmatch(text) else None


def _request_id(exc: BaseException, response: object | None) -> str | None:
    for source in (exc, response):
        if source is None:
            continue
        for name in ("request_id", "requestId"):
            value = _safe_identifier(_read_attr(source, name))
            if value is not None:
                return value

    headers = _read_attr(response, "headers") if response is not None else None
    getter = _read_attr(headers, "get") if headers is not None else None
    if not callable(getter):
        return None
    for name in _REQUEST_ID_HEADERS:
        try:
            value = _safe_identifier(getter(name))
        except Exception:
            continue
        if value is not None:
            return value
    return None


def _retry_after(exc: BaseException, response: object | None) -> float | None:
    for name in ("retry_after_s", "retry_after"):
        delay = _finite_delay(_read_attr(exc, name))
        if delay is not None:
            return delay

    headers = _read_attr(response, "headers") if response is not None else None
    getter = _read_attr(headers, "get") if headers is not None else None
    if callable(getter):
        try:
            return _finite_delay(getter("retry-after"))
        except Exception:
            return None
    return None


def _finite_delay(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        delay = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not math.isfinite(delay) or delay < 0 or delay > 86_400:
        return None
    return delay


def _is_rate_limited(exc: BaseException, status_code: int | None, error_code: str | None) -> bool:
    if isinstance(exc, RateLimitedError) or status_code == 429:
        return True
    normalized_code = (error_code or "").replace("-", "_").casefold()
    if normalized_code in {"429", "rate_limit", "rate_limited", "rate_limit_exceeded", "too_many_requests"}:
        return True
    normalized_type = type(exc).__name__.replace("_", "").casefold()
    return "ratelimit" in normalized_type or "toomanyrequests" in normalized_type


__all__ = ["SerializedFailure", "serialize_exception"]
