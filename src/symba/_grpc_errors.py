"""gRPC status -> SDK error mapping (spec 15.4).

Central translation so every call site raises the same typed errors with the
engine's message text preserved verbatim where the spec requires it.
"""

from __future__ import annotations

import grpc

from .errors import (
    AuthError,
    EngineUnavailable,
    JobNotFound,
    PayloadValidationError,
    ResultTooLarge,
    StaleLease,
    SymbaError,
)


def translate(exc: grpc.aio.AioRpcError) -> SymbaError:
    """Map a grpc.aio error to the appropriate :class:`SymbaError` (spec 15.4)."""
    code = exc.code()
    detail = exc.details() or ""

    if code == grpc.StatusCode.FAILED_PRECONDITION:
        low = detail.lower()
        if "lease" in low:
            return StaleLease(detail)
        return SymbaError(detail or "failed precondition")
    if code == grpc.StatusCode.NOT_FOUND:
        return JobNotFound(detail or "job not found")
    if code == grpc.StatusCode.INVALID_ARGUMENT:
        low = detail.lower()
        if "result" in low and "large" in low:
            return ResultTooLarge(detail)
        return PayloadValidationError(detail or "invalid argument")
    if code == grpc.StatusCode.RESOURCE_EXHAUSTED:
        return EngineUnavailable(detail or "resource exhausted", retry_after_s=None)
    if code in (grpc.StatusCode.UNAUTHENTICATED, grpc.StatusCode.PERMISSION_DENIED):
        return AuthError(detail or "authentication failed")
    if code == grpc.StatusCode.UNAVAILABLE:
        return EngineUnavailable(detail or "engine unavailable")
    return SymbaError(f"{code.name}: {detail}" if detail else code.name)


__all__ = ["translate"]
