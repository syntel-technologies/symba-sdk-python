"""CtxProxy frame marshaling + parent-side verb servicing (spec 11.4)."""

from __future__ import annotations

import pytest

from symba.context import Ctx, UpstreamOutputs
from symba.errors import UnsupportedInProfile
from symba.executors._pump import build_snapshot, service_ctx_call
from symba.executors.ctx_proxy import CtxCall, CtxProxy, CtxReply, CtxSnapshot
from symba.logging import get_logger

from ._fakes import FakeControlStub, FakeWorkerStub

pytestmark = pytest.mark.asyncio


class _FakeConn:
    """In-process stand-in for a duplex Connection: verbs are answered by a callback."""

    def __init__(self, answer) -> None:
        self._answer = answer
        self.sent: list = []
        self._reply: CtxReply | None = None

    def send(self, frame) -> None:
        self.sent.append(frame)
        if isinstance(frame, CtxCall):
            self._reply = self._answer(frame)

    def recv(self) -> CtxReply:
        assert self._reply is not None
        return self._reply


def _snapshot() -> CtxSnapshot:
    return CtxSnapshot(
        job_id="j1",
        ctx_id="c1",
        task_name="crunch",
        attempt=1,
        tenant="acme",
        pipeline="p",
        stage=None,
        group_key=None,
        payload={"n": 3},
        inline_output={"parse": {"tokens": 9}},
        checkpoint_data=None,
        event_payload=None,
        idempotency_key="idem",
        idempotency_key_attempt="idem#1",
        profile="cpu",
    )


async def test_proxy_data_fields_are_local():
    proxy = CtxProxy(_snapshot(), _FakeConn(lambda c: CtxReply(ok=True)))  # type: ignore[arg-type]
    assert proxy.payload == {"n": 3}
    assert proxy.output["parse"] == {"tokens": 9}
    assert proxy.attempt == 1
    assert proxy.tenant == "acme"


async def test_proxy_checkpoint_marshals_ctxcall():
    conn = _FakeConn(lambda c: CtxReply(ok=True, value=None))
    proxy = CtxProxy(_snapshot(), conn)  # type: ignore[arg-type]

    proxy.checkpoint({"partial": 1})

    call = conn.sent[0]
    assert isinstance(call, CtxCall)
    assert call.verb == "checkpoint"
    assert call.args["data"] == {"partial": 1}


async def test_proxy_submit_returns_parent_value():
    conn = _FakeConn(lambda c: CtxReply(ok=True, value={"job_id": "spawned"}))
    proxy = CtxProxy(_snapshot(), conn)  # type: ignore[arg-type]

    result = proxy.submit(task="child")

    assert result == {"job_id": "spawned"}


async def test_proxy_wait_for_event_unsupported():
    proxy = CtxProxy(_snapshot(), _FakeConn(lambda c: CtxReply(ok=True)))  # type: ignore[arg-type]
    with pytest.raises(UnsupportedInProfile):
        proxy.wait_for_event("k", 5)


async def test_service_ctx_call_checkpoint_hits_backend():
    from symba.checkpoint import CheckpointStore
    from symba.ctx_backend import IoCtxBackend

    control, worker = FakeControlStub(), FakeWorkerStub()
    store = CheckpointStore(
        worker_stub=worker,  # type: ignore[arg-type]
        job_id="j1",
        lease_token="lease",
        idempotency_key="idem",
    )
    backend = IoCtxBackend(
        client_stub=control,  # type: ignore[arg-type]
        worker_stub=worker,  # type: ignore[arg-type]
        tenant="acme",
        ctx_id="c1",
        pipeline="p",
        job_id="j1",
        lease_token="lease",
        checkpoint_store=store,
    )
    ctx = Ctx(
        job_id="j1",
        ctx_id="c1",
        task_name="crunch",
        attempt=1,
        tenant="acme",
        payload={},
        output=UpstreamOutputs([]),
        logger=get_logger(),
        backend=backend,
    )

    reply = await service_ctx_call(CtxCall("checkpoint", {"data": {"x": 1}}), ctx)

    assert reply.ok
    assert len(worker.checkpoints) == 1


async def test_build_snapshot_captures_inline_output():
    from symba import _json
    from symba._proto import common_pb2

    upstream = [common_pb2.UpstreamResult(key="a", job_id="j", result_json=_json.dumps({"v": 1}))]
    ctx = Ctx(
        job_id="j1",
        ctx_id="c1",
        task_name="crunch",
        attempt=2,
        tenant="acme",
        payload={"input": True},
        output=UpstreamOutputs(upstream),
        logger=get_logger(),
        profile="cpu",
    )

    snap = build_snapshot(ctx, {"input": True})

    assert snap.payload == {"input": True}
    assert snap.inline_output == {"a": {"v": 1}}
    assert snap.attempt == 2
    assert snap.profile == "cpu"
