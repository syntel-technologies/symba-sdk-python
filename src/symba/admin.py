"""``engine.admin`` — thin 1:1 wrappers over ``AdminService`` (spec 6.7).

Deliberately unpolished: an ops surface, not an app surface. Fully fleshed out
in M7; the class exists here so ``Engine.admin`` resolves.
"""

from __future__ import annotations

import json
from typing import Any

import grpc

from ._grpc_errors import translate
from ._proto import admin_pb2, admin_pb2_grpc
from .transport import Transport


class AdminClient:
    def __init__(self, transport: Transport, tenant: str) -> None:
        self._transport = transport
        self._tenant = tenant
        self._stub: admin_pb2_grpc.AdminServiceStub | None = None

    def _client(self) -> admin_pb2_grpc.AdminServiceStub:
        if self._stub is None:
            self._stub = admin_pb2_grpc.AdminServiceStub(self._transport.channel())
        return self._stub

    async def _call(self, method: Any, request: Any) -> Any:
        try:
            return await method(request)
        except grpc.aio.AioRpcError as exc:
            raise translate(exc) from exc

    async def list_rate_classes(self) -> list[admin_pb2.RateClass]:
        resp = await self._call(self._client().ListRateClasses, admin_pb2.ListRateClassesRequest())
        return list(resp.classes)

    async def upsert_rate_class(
        self, name: str, capacity: float, refill_per_s: float
    ) -> admin_pb2.RateClass:
        req = admin_pb2.RateClass(name=name, capacity=capacity, refill_per_s=refill_per_s)
        return await self._call(self._client().UpsertRateClass, req)

    async def list_cron(self) -> list[admin_pb2.CronSchedule]:
        req = admin_pb2.ListCronRequest(tenant=self._tenant)
        resp = await self._call(self._client().ListCronSchedules, req)
        return list(resp.schedules)

    async def set_cron_enabled(self, schedule_id: str, enabled: bool) -> admin_pb2.CronSchedule:
        req = admin_pb2.SetCronEnabledRequest(schedule_id=schedule_id, enabled=enabled, tenant=self._tenant)
        return await self._call(self._client().SetCronEnabled, req)

    async def upsert_cron(
        self,
        schedule_id: str,
        cron_expr: str,
        task_name: str,
        payload: dict[str, Any] | None = None,
        enabled: bool = True,
    ) -> admin_pb2.CronSchedule:
        """Create or update a cron schedule (idempotent). Returns the persisted row.

        Typical use is an at-startup, idempotent registration of an application's own
        schedule (e.g. a reconcile tick). A changed ``cron_expr`` resets the fire
        window engine-side; an unrelated edit leaves timing untouched.
        """
        req = admin_pb2.CronSchedule(
            schedule_id=schedule_id,
            cron_expr=cron_expr,
            task_name=task_name,
            payload_json=json.dumps(payload or {}).encode(),
            tenant=self._tenant,
            enabled=enabled,
        )
        return await self._call(self._client().UpsertCronSchedule, req)

    async def delete_cron(self, schedule_id: str) -> bool:
        """Delete a cron schedule. Returns True; raises NotFound for an unknown id."""
        req = admin_pb2.DeleteCronRequest(schedule_id=schedule_id, tenant=self._tenant)
        resp = await self._call(self._client().DeleteCronSchedule, req)
        return resp.deleted

    async def list_workers(self) -> list[admin_pb2.Worker]:
        resp = await self._call(self._client().ListWorkers, admin_pb2.ListWorkersRequest())
        return list(resp.workers)


__all__ = ["AdminClient"]
