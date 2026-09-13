"""AdminClient unit tests (spec 6.7).

Drives the async wrappers with a fake stub (no channel / no engine): asserts the
request messages are built correctly (tenant threaded, payload JSON-encoded) and
responses are unwrapped. gRPC error translation is covered separately; here we only
verify the happy-path request/response shaping.
"""

from __future__ import annotations

from typing import Any

import pytest

from symba._proto import admin_pb2
from symba.admin import AdminClient


class _FakeStub:
    """Records the last request per RPC and returns a canned response."""

    def __init__(self) -> None:
        self.calls: dict[str, Any] = {}

    def _record(self, name: str, response: Any):
        async def _rpc(request: Any) -> Any:
            self.calls[name] = request
            return response

        return _rpc

    def bind(self, name: str, response: Any) -> None:
        setattr(self, name, self._record(name, response))


def _client_with_stub(stub: _FakeStub, tenant: str = "acme") -> AdminClient:
    # Transport is never touched because we inject the stub directly.
    client = AdminClient(transport=None, tenant=tenant)  # type: ignore[arg-type]
    client._stub = stub  # type: ignore[assignment]
    return client


@pytest.mark.asyncio
async def test_upsert_cron_builds_request_and_returns_schedule() -> None:
    stub = _FakeStub()
    echoed = admin_pb2.CronSchedule(schedule_id="recon", cron_expr="*/30 * * * *", task_name="t")
    stub.bind("UpsertCronSchedule", echoed)
    client = _client_with_stub(stub)

    result = await client.upsert_cron(
        schedule_id="recon",
        cron_expr="*/30 * * * *",
        task_name="graph.reconcile_dispatch",
        payload={"k": "v"},
    )

    req = stub.calls["UpsertCronSchedule"]
    assert req.schedule_id == "recon"
    assert req.cron_expr == "*/30 * * * *"
    assert req.task_name == "graph.reconcile_dispatch"
    assert req.tenant == "acme"
    assert req.enabled is True
    assert req.payload_json == b'{"k": "v"}'
    assert result is echoed


@pytest.mark.asyncio
async def test_upsert_cron_defaults_payload_to_empty_object() -> None:
    stub = _FakeStub()
    stub.bind("UpsertCronSchedule", admin_pb2.CronSchedule())
    client = _client_with_stub(stub)

    await client.upsert_cron(schedule_id="s", cron_expr="* * * * *", task_name="t")

    assert stub.calls["UpsertCronSchedule"].payload_json == b"{}"


@pytest.mark.asyncio
async def test_delete_cron_returns_bool_and_threads_tenant() -> None:
    stub = _FakeStub()
    stub.bind("DeleteCronSchedule", admin_pb2.DeleteCronResponse(deleted=True))
    client = _client_with_stub(stub)

    ok = await client.delete_cron("recon")

    assert ok is True
    req = stub.calls["DeleteCronSchedule"]
    assert req.schedule_id == "recon" and req.tenant == "acme"


@pytest.mark.asyncio
async def test_list_cron_threads_tenant_and_unwraps() -> None:
    stub = _FakeStub()
    resp = admin_pb2.ListCronResponse(schedules=[admin_pb2.CronSchedule(schedule_id="a")])
    stub.bind("ListCronSchedules", resp)
    client = _client_with_stub(stub)

    schedules = await client.list_cron()

    assert stub.calls["ListCronSchedules"].tenant == "acme"
    assert [s.schedule_id for s in schedules] == ["a"]


@pytest.mark.asyncio
async def test_set_cron_enabled_threads_tenant() -> None:
    stub = _FakeStub()
    stub.bind("SetCronEnabled", admin_pb2.CronSchedule(schedule_id="a", enabled=False))
    client = _client_with_stub(stub)

    result = await client.set_cron_enabled("a", False)

    req = stub.calls["SetCronEnabled"]
    assert req.schedule_id == "a" and req.enabled is False and req.tenant == "acme"
    assert result.enabled is False
