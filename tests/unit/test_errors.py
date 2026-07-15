"""Exception taxonomy tests (spec 15.1)."""

from __future__ import annotations

import symba
from symba.errors import (
    EngineUnavailable,
    FatalError,
    JobFailed,
    RateLimitedError,
    RetryableError,
    SymbaError,
)


def test_base_carries_code_and_context():
    err = SymbaError("boom", job_id="j1", attempt=3)
    assert err.error_code == "symba_error"
    assert err.retryable is False
    assert err.message == "boom"
    assert err.context == {"job_id": "j1", "attempt": 3}


def test_default_message_used_when_none():
    err = RetryableError()
    assert err.message == "Retryable error"


def test_retryable_and_fatal_flags():
    assert RetryableError().retryable is True
    assert FatalError().retryable is False


def test_rate_limited_is_retryable_and_carries_retry_after():
    err = RateLimitedError("429 from vendor", retry_after_s=12.5)
    assert isinstance(err, RetryableError)
    assert err.retryable is True
    assert err.retry_after_s == 12.5
    assert err.context["retry_after_s"] == 12.5


def test_engine_unavailable_is_retryable():
    assert EngineUnavailable().retryable is True


def test_job_failed_carries_history():
    history = [{"error_type": "ValueError", "attempt": 1}]
    err = JobFailed("dead", job_id="j2", error_history=history)
    assert err.job_id == "j2"
    assert err.error_history == history


def test_all_public_errors_are_symba_errors():
    for name in symba.__all__:
        obj = getattr(symba, name)
        if isinstance(obj, type) and issubclass(obj, Exception):
            assert issubclass(obj, SymbaError), name
