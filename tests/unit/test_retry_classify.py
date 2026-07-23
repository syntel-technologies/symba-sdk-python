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


# Mimic ``urllib3.exceptions.ProtocolError`` / ``IncompleteRead``: matched by
# class name so the SDK never imports urllib3 (same policy as _LibTimeoutError).
class _ProtocolError(Exception):
    """A body cut off mid-transfer (urllib3.exceptions.ProtocolError shape)."""


_ProtocolError.__name__ = "ProtocolError"
_ProtocolError.__qualname__ = "ProtocolError"


class _IncompleteRead(Exception):
    """A short read of an idempotent GET (urllib3.exceptions.IncompleteRead)."""


_IncompleteRead.__name__ = "IncompleteRead"
_IncompleteRead.__qualname__ = "IncompleteRead"


def test_protocol_error_is_retryable():
    """A urllib3 ProtocolError (mid-body truncation of an idempotent GET) is a
    transient transport failure and must be retryable (matched by name)."""
    exc = _ProtocolError("Connection broken: IncompleteRead(8152536 bytes read)")
    assert classify(exc) is True


def test_incomplete_read_is_retryable():
    """A urllib3 IncompleteRead (short body) is retryable (matched by name)."""
    assert classify(_IncompleteRead("8152536 bytes read, 100 more expected")) is True
