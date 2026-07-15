"""Symba — the Python SDK for the Symba durable job engine.

This module is THE public API surface (spec 3, rule 1). Anything not exported
here is private; ``_``-prefixed modules are hard-private. SemVer applies to these
exports only.

The package ships two SDKs in one import root:

* the **client SDK** (:class:`Engine`) — submit / query / await / signal, what
  application code calls;
* the **worker SDK** (:class:`Worker`) — register handlers, claim, execute, what
  runs on the fleet.
"""

from __future__ import annotations

from ._version import SDK_VERSION_STRING, __engine_protocol__, __version__
from .context import Ctx, Skip, StopChain
from .engine import Engine
from .errors import (
    AmbiguousResultKey,
    AuthError,
    ConfigError,
    EngineUnavailable,
    FatalError,
    JobCancelled,
    JobFailed,
    JobNotFound,
    OutputValidationError,
    PayloadValidationError,
    ProtocolMismatch,
    RateLimitedError,
    ResultTooLarge,
    RetryableError,
    StaleLease,
    SymbaError,
    UnsupportedInProfile,
    WaitKeyAlreadyConsumed,
    WrongEventLoop,
)
from .job import Gate, JobHandle
from .middleware import LoggingMiddleware, MetricsMiddleware, WorkerMiddleware
from .profiles import Profile
from .sync import SyncEngine
from .types import (
    CancelOutcome,
    GateStatus,
    JobEvent,
    JobState,
    JobStatus,
    RetryPolicy,
)
from .worker import Worker

__all__ = [
    "__version__",
    "__engine_protocol__",
    "SDK_VERSION_STRING",
    # client SDK (spec 6, 7)
    "Engine",
    "SyncEngine",
    "JobHandle",
    "Gate",
    "RetryPolicy",
    "JobState",
    "JobStatus",
    "JobEvent",
    "CancelOutcome",
    "GateStatus",
    # worker SDK (spec 8, 10, 11)
    "Worker",
    "Ctx",
    "StopChain",
    "Skip",
    "Profile",
    "WorkerMiddleware",
    "LoggingMiddleware",
    "MetricsMiddleware",
    # errors (spec 15.1)
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
