"""Proto stub sanity + version surface (spec 4)."""

from __future__ import annotations

import symba
from symba._json import dumps, loads
from symba._proto import (
    admin_pb2_grpc,
    common_pb2,
    control_plane_pb2_grpc,
    data_plane_pb2_grpc,
)


def test_version_surface():
    assert isinstance(symba.__version__, str)
    assert isinstance(symba.__engine_protocol__, str)
    assert (
        symba.SDK_VERSION_STRING == f"symba/{symba.__version__} proto/{symba.__engine_protocol__}"
    )


def test_proto_messages_roundtrip():
    spec = common_pb2.JobSpec(task_name="parse", payload_json=b'{"x":1}')
    raw = spec.SerializeToString()
    parsed = common_pb2.JobSpec()
    parsed.ParseFromString(raw)
    assert parsed.task_name == "parse"
    assert parsed.payload_json == b'{"x":1}'


def test_job_state_enum():
    assert common_pb2.JobState.Value("QUEUED") == 2
    assert common_pb2.JobState.Value("SUCCEEDED") == 5


def test_service_stubs_exist():
    assert hasattr(data_plane_pb2_grpc, "WorkerServiceStub")
    assert hasattr(control_plane_pb2_grpc, "ClientServiceStub")
    assert hasattr(admin_pb2_grpc, "AdminServiceStub")


def test_json_seam_roundtrip():
    payload = {"a": 1, "b": ["x", "y"], "c": None}
    assert loads(dumps(payload)) == payload
    assert loads(b"") is None
