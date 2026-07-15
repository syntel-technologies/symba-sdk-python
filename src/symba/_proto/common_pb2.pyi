import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class JobState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    JOB_STATE_UNSPECIFIED: _ClassVar[JobState]
    SUBMITTED: _ClassVar[JobState]
    QUEUED: _ClassVar[JobState]
    RUNNING: _ClassVar[JobState]
    WAITING: _ClassVar[JobState]
    SUCCEEDED: _ClassVar[JobState]
    DEAD: _ClassVar[JobState]
    CANCELLED: _ClassVar[JobState]
JOB_STATE_UNSPECIFIED: JobState
SUBMITTED: JobState
QUEUED: JobState
RUNNING: JobState
WAITING: JobState
SUCCEEDED: JobState
DEAD: JobState
CANCELLED: JobState

class RetryPolicy(_message.Message):
    __slots__ = ("max_attempts", "backoff_base_s", "backoff_factor", "backoff_max_s", "jitter")
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    BACKOFF_BASE_S_FIELD_NUMBER: _ClassVar[int]
    BACKOFF_FACTOR_FIELD_NUMBER: _ClassVar[int]
    BACKOFF_MAX_S_FIELD_NUMBER: _ClassVar[int]
    JITTER_FIELD_NUMBER: _ClassVar[int]
    max_attempts: int
    backoff_base_s: float
    backoff_factor: float
    backoff_max_s: float
    jitter: bool
    def __init__(self, max_attempts: _Optional[int] = ..., backoff_base_s: _Optional[float] = ..., backoff_factor: _Optional[float] = ..., backoff_max_s: _Optional[float] = ..., jitter: _Optional[bool] = ...) -> None: ...

class Dependency(_message.Message):
    __slots__ = ("job_id", "alias")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    ALIAS_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    alias: str
    def __init__(self, job_id: _Optional[str] = ..., alias: _Optional[str] = ...) -> None: ...

class JobSpec(_message.Message):
    __slots__ = ("task_name", "payload_json", "pipeline", "stage", "ctx_id", "group_key", "dedup_key", "runs_on", "rate_class", "priority", "timeout_s", "lease_ttl_s", "retry", "run_at", "chain", "depends_on", "on_failure", "max_concurrent_per_group")
    TASK_NAME_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_FIELD_NUMBER: _ClassVar[int]
    STAGE_FIELD_NUMBER: _ClassVar[int]
    CTX_ID_FIELD_NUMBER: _ClassVar[int]
    GROUP_KEY_FIELD_NUMBER: _ClassVar[int]
    DEDUP_KEY_FIELD_NUMBER: _ClassVar[int]
    RUNS_ON_FIELD_NUMBER: _ClassVar[int]
    RATE_CLASS_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_S_FIELD_NUMBER: _ClassVar[int]
    LEASE_TTL_S_FIELD_NUMBER: _ClassVar[int]
    RETRY_FIELD_NUMBER: _ClassVar[int]
    RUN_AT_FIELD_NUMBER: _ClassVar[int]
    CHAIN_FIELD_NUMBER: _ClassVar[int]
    DEPENDS_ON_FIELD_NUMBER: _ClassVar[int]
    ON_FAILURE_FIELD_NUMBER: _ClassVar[int]
    MAX_CONCURRENT_PER_GROUP_FIELD_NUMBER: _ClassVar[int]
    task_name: str
    payload_json: bytes
    pipeline: str
    stage: str
    ctx_id: str
    group_key: str
    dedup_key: str
    runs_on: _containers.RepeatedScalarFieldContainer[str]
    rate_class: str
    priority: int
    timeout_s: int
    lease_ttl_s: int
    retry: RetryPolicy
    run_at: _timestamp_pb2.Timestamp
    chain: _containers.RepeatedScalarFieldContainer[str]
    depends_on: _containers.RepeatedCompositeFieldContainer[Dependency]
    on_failure: JobSpec
    max_concurrent_per_group: int
    def __init__(self, task_name: _Optional[str] = ..., payload_json: _Optional[bytes] = ..., pipeline: _Optional[str] = ..., stage: _Optional[str] = ..., ctx_id: _Optional[str] = ..., group_key: _Optional[str] = ..., dedup_key: _Optional[str] = ..., runs_on: _Optional[_Iterable[str]] = ..., rate_class: _Optional[str] = ..., priority: _Optional[int] = ..., timeout_s: _Optional[int] = ..., lease_ttl_s: _Optional[int] = ..., retry: _Optional[_Union[RetryPolicy, _Mapping]] = ..., run_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., chain: _Optional[_Iterable[str]] = ..., depends_on: _Optional[_Iterable[_Union[Dependency, _Mapping]]] = ..., on_failure: _Optional[_Union[JobSpec, _Mapping]] = ..., max_concurrent_per_group: _Optional[int] = ...) -> None: ...

class UpstreamResult(_message.Message):
    __slots__ = ("key", "job_id", "result_json")
    KEY_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    RESULT_JSON_FIELD_NUMBER: _ClassVar[int]
    key: str
    job_id: str
    result_json: bytes
    def __init__(self, key: _Optional[str] = ..., job_id: _Optional[str] = ..., result_json: _Optional[bytes] = ...) -> None: ...

class Job(_message.Message):
    __slots__ = ("id", "tenant", "spec", "state", "attempt", "result_json", "claimed_by", "last_error", "created_at", "started_at", "finished_at", "upstream")
    ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    SPEC_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    RESULT_JSON_FIELD_NUMBER: _ClassVar[int]
    CLAIMED_BY_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    FINISHED_AT_FIELD_NUMBER: _ClassVar[int]
    UPSTREAM_FIELD_NUMBER: _ClassVar[int]
    id: str
    tenant: str
    spec: JobSpec
    state: JobState
    attempt: int
    result_json: bytes
    claimed_by: str
    last_error: str
    created_at: _timestamp_pb2.Timestamp
    started_at: _timestamp_pb2.Timestamp
    finished_at: _timestamp_pb2.Timestamp
    upstream: _containers.RepeatedCompositeFieldContainer[UpstreamResult]
    def __init__(self, id: _Optional[str] = ..., tenant: _Optional[str] = ..., spec: _Optional[_Union[JobSpec, _Mapping]] = ..., state: _Optional[_Union[JobState, str]] = ..., attempt: _Optional[int] = ..., result_json: _Optional[bytes] = ..., claimed_by: _Optional[str] = ..., last_error: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., finished_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., upstream: _Optional[_Iterable[_Union[UpstreamResult, _Mapping]]] = ...) -> None: ...

class JobEvent(_message.Message):
    __slots__ = ("job_id", "event", "at", "detail_json")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_FIELD_NUMBER: _ClassVar[int]
    AT_FIELD_NUMBER: _ClassVar[int]
    DETAIL_JSON_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    event: str
    at: _timestamp_pb2.Timestamp
    detail_json: bytes
    def __init__(self, job_id: _Optional[str] = ..., event: _Optional[str] = ..., at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., detail_json: _Optional[bytes] = ...) -> None: ...
