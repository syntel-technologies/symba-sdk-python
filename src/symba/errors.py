"""Exception taxonomy (spec 15.1).

Every SDK error carries a class-level ``error_code``, a ``retryable`` default,
and a free-form ``context`` dict that lands in the engine's ``error_history``
JSONB. Two directions:

* raised **by handlers** (app -> engine): ``RetryableError``, ``FatalError``,
  ``RateLimitedError`` — explicit overrides of the automatic classification.
* raised **by the SDK** (engine -> app): everything else, surfaced to callers of
  the client/worker verbs.
"""

from __future__ import annotations

from typing import Any


class SymbaError(Exception):
    """Base for every SDK error.

    Carries ``error_code``, a ``retryable`` default, and structured ``context``
    that is forwarded into the engine's ``error_history``.
    """

    error_code: str = "symba_error"
    retryable: bool = False
    #: Default message used when an instance is constructed without one.
    message: str = "Symba error"

    def __init__(self, message: str | None = None, **context: Any) -> None:
        self.message = message or type(self).message
        self.context: dict[str, Any] = context
        super().__init__(self.message)


# --------------------------------------------------------------------------- #
# Raised BY handlers (app -> engine direction)                                 #
# --------------------------------------------------------------------------- #
class RetryableError(SymbaError):
    """Explicitly request a retry, overriding automatic classification."""

    error_code = "retryable"
    retryable = True
    message = "Retryable error"


class FatalError(SymbaError):
    """Explicitly refuse a retry: the job goes DEAD immediately."""

    error_code = "fatal"
    retryable = False
    message = "Fatal error"


class RateLimitedError(RetryableError):
    """Retryable AND drains the job's ``rate_class`` bucket engine-wide.

    One worker discovering a 429 backs the whole fleet off that rate class, not
    just itself (engine 6.3).
    """

    error_code = "rate_limited"
    message = "Rate limited"

    def __init__(
        self, message: str | None = None, retry_after_s: float | None = None, **context: Any
    ) -> None:
        self.retry_after_s = retry_after_s
        super().__init__(message, retry_after_s=retry_after_s, **context)


# --------------------------------------------------------------------------- #
# Raised BY the SDK (engine -> app direction)                                  #
# --------------------------------------------------------------------------- #
class ConfigError(FatalError):
    """Invalid ``Engine``/``Worker`` configuration; raised at construction (spec 19)."""

    error_code = "config_error"
    message = "Invalid Symba configuration"


class JobFailed(SymbaError):
    """``JobHandle.result()`` on a DEAD job. Carries the engine ``error_history``."""

    error_code = "job_failed"
    message = "Job failed"

    def __init__(
        self,
        message: str | None = None,
        *,
        job_id: str | None = None,
        error_history: list[dict[str, Any]] | None = None,
        **context: Any,
    ) -> None:
        self.job_id = job_id
        self.error_history = error_history or []
        super().__init__(message, job_id=job_id, error_history=self.error_history, **context)


class JobCancelled(SymbaError):
    """``JobHandle.result()`` on a CANCELLED job."""

    error_code = "job_cancelled"
    message = "Job was cancelled"

    def __init__(self, message: str | None = None, *, job_id: str | None = None, **context: Any):
        self.job_id = job_id
        super().__init__(message, job_id=job_id, **context)


class JobNotFound(SymbaError, KeyError):
    """Query verbs when the job id does not exist (engine ``NOT_FOUND``)."""

    error_code = "job_not_found"
    message = "Job not found"


class StaleLease(SymbaError):
    """Complete/Fail/checkpoint rejected because the lease was lost (engine
    ``FAILED_PRECONDITION``). Swallowed + WARNed in the dispatch pipeline;
    raised to callers of explicit verbs (spec 9.4)."""

    error_code = "stale_lease"
    message = "Lease is stale; another attempt won"


class ResultTooLarge(SymbaError):
    """Result exceeded the 64KB cap. The message names the fix (store a reference)."""

    error_code = "result_too_large"
    message = "Result exceeds 64KB cap; store a reference and return the pointer"


class PayloadValidationError(FatalError):
    """``input_schema`` rejected the payload (or the payload exceeded 256KB)."""

    error_code = "payload_validation_error"
    message = "Payload failed validation"


class OutputValidationError(FatalError):
    """``output_schema`` rejected the handler's return value."""

    error_code = "output_validation_error"
    message = "Output failed validation"


class AmbiguousResultKey(SymbaError):
    """``ctx.output[key]`` matched multiple upstream task names without an alias."""

    error_code = "ambiguous_result_key"
    message = "Ambiguous upstream result key; declare an alias in depends_on"


class UnsupportedInProfile(SymbaError):
    """A verb (e.g. ``wait_for_event``) was used outside the ``io`` profile."""

    error_code = "unsupported_in_profile"
    message = "Operation is only supported in the io profile"


class WaitKeyAlreadyConsumed(SymbaError):
    """A repeat ``wait_for_event`` reused a key whose signal was already consumed (spec 14.2)."""

    error_code = "wait_key_already_consumed"
    message = "wait_for_event key was already consumed; use a distinct key per wait"


class EngineUnavailable(RetryableError):
    """Transport-level failure after channel retries were exhausted."""

    error_code = "engine_unavailable"
    message = "Symba engine is unavailable"

    def __init__(
        self, message: str | None = None, *, retry_after_s: float | None = None, **context: Any
    ) -> None:
        self.retry_after_s = retry_after_s
        super().__init__(message, retry_after_s=retry_after_s, **context)


class AuthError(FatalError):
    """Engine ``UNAUTHENTICATED`` / ``PERMISSION_DENIED``; never retried."""

    error_code = "auth_error"
    message = "Authentication or authorization failed"


class ProtocolMismatch(FatalError):
    """SDK/engine version-handshake rejection (spec 4.3)."""

    error_code = "protocol_mismatch"
    message = "SDK/engine protocol version mismatch"


class WrongEventLoop(SymbaError):
    """A channel bound to one event loop was used from another (spec 5.3).

    The message points at the ``.sync`` facade as the fix.
    """

    error_code = "wrong_event_loop"
    message = (
        "This Symba client is bound to a different event loop. "
        "Use symba.sync.SyncEngine from synchronous/threaded contexts."
    )


__all__ = [
    "SymbaError",
    "RetryableError",
    "FatalError",
    "RateLimitedError",
    "ConfigError",
    "JobFailed",
    "JobCancelled",
    "JobNotFound",
    "StaleLease",
    "ResultTooLarge",
    "PayloadValidationError",
    "OutputValidationError",
    "AmbiguousResultKey",
    "UnsupportedInProfile",
    "WaitKeyAlreadyConsumed",
    "EngineUnavailable",
    "AuthError",
    "ProtocolMismatch",
    "WrongEventLoop",
]
