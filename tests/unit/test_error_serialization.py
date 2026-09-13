from __future__ import annotations

import pytest

from symba import _json
from symba._error_serialization import serialize_exception
from symba.context import Ctx
from symba.dispatch import DispatchDeps, Dispatcher
from symba.errors import RateLimitedError, RetryableError
from symba.executors.asyncio_executor import AsyncioExecutor
from symba.middleware import MiddlewareChain
from symba.profiles import Profile
from symba.task_registry import TaskRegistry

from ._fakes import FakeWorkerStub, make_assignment


class _Response:
    def __init__(self) -> None:
        self.status_code = 429
        self.headers = {"x-request-id": "req-123", "retry-after": "17", "authorization": "secret"}


class _ProviderError(Exception):
    __module__ = "provider_sdk.errors"

    def __init__(self) -> None:
        self.response = _Response()
        self.body = {"prompt": "private prompt", "api_key": "sk-provider-secret"}
        self.code = "rate_limit_exceeded"

    def __str__(self) -> str:
        return f"provider failed: body={self.body!r}; headers={self.response.headers!r}"


def test_provider_response_is_replaced_by_safe_typed_details() -> None:
    failure = serialize_exception(_ProviderError())

    assert failure.message == "External service rate limited the request"
    assert failure.rate_limited is True
    assert failure.retry_after_s == 17.0
    assert failure.metadata == {
        "module": "provider_sdk.errors",
        "status_code": 429,
        "error_code": "rate_limit_exceeded",
        "request_id": "req-123",
        "retry_after_s": 17.0,
    }
    assert "private prompt" not in str(failure)
    assert "sk-provider-secret" not in str(failure)
    assert "authorization" not in str(failure)


def test_builtin_and_explicit_symba_messages_remain_useful() -> None:
    assert serialize_exception(ValueError("invalid board date")).message == "invalid board date"
    assert serialize_exception(RetryableError("database warming up")).message == "database warming up"


def test_explicit_message_still_redacts_secrets() -> None:
    failure = serialize_exception(RetryableError("api_key=sk-sensitive-value retry later"))

    assert "sk-sensitive-value" not in failure.message
    assert "[redacted]" in failure.message


def test_rate_limited_error_carries_retry_hint_without_freeform_message() -> None:
    failure = serialize_exception(RateLimitedError("raw downstream text", retry_after_s=3.5))

    assert failure.message == "External service rate limited the request"
    assert failure.rate_limited is True
    assert failure.retry_after_s == 3.5


@pytest.mark.asyncio
async def test_dispatch_sends_no_provider_body_and_typed_rate_feedback() -> None:
    registry = TaskRegistry()

    async def handler(ctx: Ctx, payload: dict) -> dict:
        raise _ProviderError()

    registry.register("provider", handler, profile=Profile.IO)
    stub = FakeWorkerStub()
    dispatcher = Dispatcher(
        DispatchDeps(
            stub=stub,  # type: ignore[arg-type]
            registry=registry,
            middleware=MiddlewareChain([]),
            executors={Profile.IO: AsyncioExecutor()},
            tenant="default",
            heartbeat_interval_s=15.0,
            classify_overrides=[],
        )
    )

    await dispatcher.dispatch(make_assignment("provider-job", task_name="provider"))

    assert len(stub.fails) == 1
    request = stub.fails[0]
    assert request.error_message == "External service rate limited the request"
    assert request.error_message_safe is True
    assert request.rate_limited is True
    assert request.retry_after_s == 17.0
    assert _json.loads(request.error_metadata_json)["request_id"] == "req-123"
    assert "private prompt" not in str(request)
    assert "sk-provider-secret" not in str(request)
