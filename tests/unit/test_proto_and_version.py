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


def test_runtime_meets_gencode_floors():
    """The installed runtime and published requirements cover the actual gencode."""
    import ast
    from importlib.metadata import requires, version
    from pathlib import Path

    from packaging.requirements import Requirement
    from packaging.version import Version

    from symba._proto import common_pb2, common_pb2_grpc

    module = ast.parse(Path(common_pb2.__file__).read_text())
    validation = next(
        node
        for node in ast.walk(module)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "ValidateProtobufRuntimeVersion"
    )
    protobuf_floor = ".".join(str(ast.literal_eval(arg)) for arg in validation.args[1:4])
    floors = {"protobuf": protobuf_floor, "grpcio": common_pb2_grpc.GRPC_GENERATED_VERSION}
    declared = {
        requirement.name: requirement
        for entry in requires("syntel-symba") or []
        if (requirement := Requirement(entry)).name in floors
    }
    for name, floor in floors.items():
        assert Version(version(name)) >= Version(floor), f"{name} below the gencode floor"
        # A newer lockfile alone cannot protect consumers resolving our public ranges.
        lower_bounds = [
            Version(spec.version)
            for spec in declared[name].specifier
            if spec.operator in {">=", "=="}
        ]
        assert lower_bounds and max(lower_bounds) >= Version(floor), (
            f"{name} requirement allows a runtime older than generated code {floor}"
        )
