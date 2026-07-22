"""Retryable-vs-fatal classification tests (spec 15.2, SDK-6)."""

from __future__ import annotations

import asyncio

from symba.retry_classify import classify


class _LibTimeoutError(Exception):
    """Mimics ``sqlalchemy.exc.TimeoutError``: named ``TimeoutError`` but NOT a
    subclass of the builtin ``TimeoutError`` (its class name is what matters)."""


# Named exactly ``TimeoutError`` in this (non-builtins) module, extending a
# non-timeout base — the exact shape of ``sqlalchemy.exc.TimeoutError``.
_LibTimeoutError.__name__ = "TimeoutError"
_LibTimeoutError.__qualname__ = "TimeoutError"


def test_named_timeout_from_other_module_is_retryable():
    """SDK-6: a lib exception NAMED TimeoutError that does not subclass the
    builtin is still classified retryable (matched by class name, no import)."""
    exc = _LibTimeoutError("QueuePool limit reached")
    assert not isinstance(exc, TimeoutError)  # precondition: not the builtin
    assert classify(exc) is True


def test_builtin_timeout_still_retryable():
    assert classify(TimeoutError("boom")) is True


def test_asyncio_timeout_still_retryable():
    # asyncio.TimeoutError aliases the builtin on 3.11+; assert the alias path too.
    assert classify(asyncio.TimeoutError()) is True  # noqa: UP041


def test_value_error_stays_fatal():
    """The name-based rule must not widen the net: a genuine data error is fatal."""
    assert classify(ValueError("bad input")) is False


class _HttpBadRequest(Exception):
    status_code = 400


def test_http_fatal_status_stays_fatal():
    """A 4xx fatal HTTP status is still non-retryable (precedence preserved)."""
    assert classify(_HttpBadRequest()) is False
