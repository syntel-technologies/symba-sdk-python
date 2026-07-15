"""JobSpec builder + validation tests (spec 6.3)."""

from __future__ import annotations

import pytest

from symba import _json
from symba.errors import PayloadValidationError, SymbaError
from symba.specs import (
    MAX_CHAIN_ENTRIES,
    build_job_spec,
    encode_payload,
    spec_from_dict,
)
from symba.types import RetryPolicy


def test_minimal_spec():
    spec = build_job_spec(task="download")
    assert spec.task_name == "download"
    assert spec.payload_json == b""
    # unset numeric knobs stay zero so engine defaults apply (spec 4.2)
    assert spec.timeout_s == 0
    assert not spec.HasField("retry")


def test_payload_serialized():
    spec = build_job_spec(task="t", payload={"a": 1})
    assert _json.loads(spec.payload_json) == {"a": 1}


def test_pydantic_payload_serialized():
    from pydantic import BaseModel

    class P(BaseModel):
        doc_id: str
        n: int

    spec = build_job_spec(task="t", payload=P(doc_id="d1", n=3))
    assert _json.loads(spec.payload_json) == {"doc_id": "d1", "n": 3}


def test_default_pipeline_applied_when_unset():
    spec = build_job_spec(task="t", default_pipeline="ingestion")
    assert spec.pipeline == "ingestion"
    spec2 = build_job_spec(task="t", pipeline="explicit", default_pipeline="ingestion")
    assert spec2.pipeline == "explicit"


def test_depends_on_list_defaults_alias_empty():
    spec = build_job_spec(task="t", depends_on=["job-1", "job-2"])
    assert [d.job_id for d in spec.depends_on] == ["job-1", "job-2"]
    assert all(d.alias == "" for d in spec.depends_on)


def test_depends_on_dict_uses_alias():
    spec = build_job_spec(task="t", depends_on={"first": "job-1"})
    assert spec.depends_on[0].alias == "first"
    assert spec.depends_on[0].job_id == "job-1"


def test_depends_on_unwraps_handle_like():
    class FakeHandle:
        id = "job-x"

    spec = build_job_spec(task="t", depends_on=[FakeHandle()])
    assert spec.depends_on[0].job_id == "job-x"


def test_retry_policy_partial_only_sets_given_fields():
    spec = build_job_spec(task="t", retry=RetryPolicy(max_attempts=3))
    assert spec.retry.max_attempts == 3
    assert spec.retry.backoff_base_s == 0.0  # unset -> engine default


def test_on_failure_recursive():
    spec = build_job_spec(task="t", on_failure={"task": "cleanup", "payload": {"x": 1}})
    assert spec.on_failure.task_name == "cleanup"
    assert _json.loads(spec.on_failure.payload_json) == {"x": 1}


def test_payload_too_large_rejected():
    big = {"blob": "x" * (256 * 1024)}
    with pytest.raises(PayloadValidationError) as exc:
        encode_payload(big)
    assert "store a reference" in str(exc.value)


def test_non_serializable_payload_rejected():
    with pytest.raises(PayloadValidationError):
        encode_payload({"bad": object()})


def test_chain_too_long_rejected():
    with pytest.raises(SymbaError):
        build_job_spec(task="t", chain=[f"s{i}" for i in range(MAX_CHAIN_ENTRIES + 1)])


def test_unknown_submit_key_rejected():
    with pytest.raises(SymbaError):
        spec_from_dict({"task": "t", "bogus": 1})


def test_empty_task_rejected():
    with pytest.raises(SymbaError):
        build_job_spec(task="")
