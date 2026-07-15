"""JobSpec builder + client-side validation (spec 6.3).

kwargs -> ``JobSpec`` proto, with fail-fast validation that mirrors the engine's
wording (spec 6.3 step 2). The SDK validates *shape* only — never *semantics*
(dependency existence is the engine's transactional check).

Anything left unset is sent as the proto zero value so the engine ``[defaults]``
apply (spec 4.2).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from google.protobuf.timestamp_pb2 import Timestamp

from . import _json
from ._proto import common_pb2
from .errors import PayloadValidationError, SymbaError
from .types import RetryPolicy

#: Engine-enforced caps, pre-validated client-side to fail fast (spec 4.2).
PAYLOAD_CAP_BYTES = 256 * 1024
RESULT_CAP_BYTES = 64 * 1024
MAX_CHAIN_ENTRIES = 50
MAX_FANOUT_CHILDREN = 100_000

_KNOWN_SUBMIT_KWARGS = frozenset(
    {
        "task",
        "payload",
        "ctx_id",
        "chain",
        "depends_on",
        "on_failure",
        "pipeline",
        "stage",
        "group_key",
        "dedup_key",
        "priority",
        "run_at",
        "runs_on",
        "rate_class",
        "max_concurrent_per_group",
        "timeout_s",
        "lease_ttl_s",
        "retry",
    }
)


def encode_payload(payload: Any) -> bytes:
    """Serialize a submit payload (dict | pydantic model | None) to JSON bytes.

    Raises :class:`PayloadValidationError` on non-serializable input or a payload
    exceeding the 256KB cap — with the same store-a-reference wording the engine
    uses.
    """
    if payload is None:
        return b""
    if hasattr(payload, "model_dump"):  # pydantic BaseModel (duck-typed, no import)
        payload = payload.model_dump(mode="json")
    try:
        raw = _json.dumps(payload)
    except (TypeError, ValueError) as exc:
        raise PayloadValidationError(f"payload is not JSON-serializable: {exc}") from exc
    if len(raw) > PAYLOAD_CAP_BYTES:
        raise PayloadValidationError(
            f"payload is {len(raw)} bytes, exceeds {PAYLOAD_CAP_BYTES} cap; "
            "store a reference (blob id) and pass the pointer instead"
        )
    return raw


def _retry_proto(retry: RetryPolicy | None) -> common_pb2.RetryPolicy | None:
    if retry is None:
        return None
    msg = common_pb2.RetryPolicy()
    if retry.max_attempts is not None:
        msg.max_attempts = retry.max_attempts
    if retry.backoff_base_s is not None:
        msg.backoff_base_s = retry.backoff_base_s
    if retry.backoff_factor is not None:
        msg.backoff_factor = retry.backoff_factor
    if retry.backoff_max_s is not None:
        msg.backoff_max_s = retry.backoff_max_s
    if retry.jitter is not None:
        msg.jitter = retry.jitter
    return msg


def _unwrap_job_id(value: Any) -> str:
    """Accept a bare id or a JobHandle-like object (ergonomics, spec 6.3 step 2)."""
    if isinstance(value, str):
        return value
    job_id = getattr(value, "id", None)
    if isinstance(job_id, str):
        return job_id
    raise PayloadValidationError(f"expected a job id or JobHandle, got {type(value).__name__}")


def _build_dependencies(
    depends_on: list[Any] | dict[str, Any] | None,
) -> list[common_pb2.Dependency]:
    if depends_on is None:
        return []
    deps: list[common_pb2.Dependency] = []
    if isinstance(depends_on, dict):
        for alias, ref in depends_on.items():
            deps.append(common_pb2.Dependency(job_id=_unwrap_job_id(ref), alias=alias))
    else:
        for ref in depends_on:
            deps.append(common_pb2.Dependency(job_id=_unwrap_job_id(ref)))
    return deps


def build_job_spec(
    *,
    task: str,
    payload: Any = None,
    default_pipeline: str | None = None,
    ctx_id: str | None = None,
    chain: list[str] | None = None,
    depends_on: list[Any] | dict[str, Any] | None = None,
    on_failure: dict[str, Any] | None = None,
    pipeline: str | None = None,
    stage: str | None = None,
    group_key: str | None = None,
    dedup_key: str | None = None,
    priority: int | None = None,
    run_at: datetime | None = None,
    runs_on: list[str] | None = None,
    rate_class: str | None = None,
    max_concurrent_per_group: int | None = None,
    timeout_s: int | None = None,
    lease_ttl_s: int | None = None,
    retry: RetryPolicy | None = None,
) -> common_pb2.JobSpec:
    """Build one ``JobSpec`` proto from submit kwargs, validating shape."""
    if not task or not isinstance(task, str):
        raise SymbaError("submit requires a non-empty task name")

    chain = chain or []
    if len(chain) > MAX_CHAIN_ENTRIES:
        raise SymbaError(f"chain has {len(chain)} entries, exceeds the {MAX_CHAIN_ENTRIES} limit")

    spec = common_pb2.JobSpec(
        task_name=task,
        payload_json=encode_payload(payload),
        pipeline=pipeline or default_pipeline or "",
        stage=stage or "",
        ctx_id=ctx_id or "",
        group_key=group_key or "",
        dedup_key=dedup_key or "",
        rate_class=rate_class or "",
    )
    spec.runs_on.extend(runs_on or [])
    spec.chain.extend(chain)
    spec.depends_on.extend(_build_dependencies(depends_on))

    if priority is not None:
        spec.priority = priority
    if timeout_s is not None:
        spec.timeout_s = timeout_s
    if lease_ttl_s is not None:
        spec.lease_ttl_s = lease_ttl_s
    if max_concurrent_per_group is not None:
        spec.max_concurrent_per_group = max_concurrent_per_group

    retry_msg = _retry_proto(retry)
    if retry_msg is not None:
        spec.retry.CopyFrom(retry_msg)

    if run_at is not None:
        ts = Timestamp()
        ts.FromDatetime(run_at)
        spec.run_at.CopyFrom(ts)

    if on_failure is not None:
        spec.on_failure.CopyFrom(spec_from_dict(on_failure, default_pipeline=default_pipeline))

    return spec


def spec_from_dict(d: dict[str, Any], *, default_pipeline: str | None = None) -> common_pb2.JobSpec:
    """Build a JobSpec from a dict (used for ``on_failure`` and fan-out children)."""
    unknown = set(d) - _KNOWN_SUBMIT_KWARGS
    if unknown:
        raise SymbaError(f"unknown submit keys: {sorted(unknown)}")
    kwargs = dict(d)
    retry = kwargs.get("retry")
    if isinstance(retry, dict):
        kwargs["retry"] = RetryPolicy(**retry)
    return build_job_spec(default_pipeline=default_pipeline, **kwargs)


__all__ = [
    "build_job_spec",
    "spec_from_dict",
    "encode_payload",
    "PAYLOAD_CAP_BYTES",
    "RESULT_CAP_BYTES",
    "MAX_CHAIN_ENTRIES",
    "MAX_FANOUT_CHILDREN",
]
