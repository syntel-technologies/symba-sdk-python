import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from symba.v1 import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GetGateRequest(_message.Message):
    __slots__ = ("tenant", "gate_id")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    GATE_ID_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    gate_id: str
    def __init__(self, tenant: _Optional[str] = ..., gate_id: _Optional[str] = ...) -> None: ...

class GateStatus(_message.Message):
    __slots__ = ("gate_id", "expected", "terminal", "succeeded", "fired_at")
    GATE_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_FIELD_NUMBER: _ClassVar[int]
    SUCCEEDED_FIELD_NUMBER: _ClassVar[int]
    FIRED_AT_FIELD_NUMBER: _ClassVar[int]
    gate_id: str
    expected: int
    terminal: int
    succeeded: int
    fired_at: _timestamp_pb2.Timestamp
    def __init__(self, gate_id: _Optional[str] = ..., expected: _Optional[int] = ..., terminal: _Optional[int] = ..., succeeded: _Optional[int] = ..., fired_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class SubmitRequest(_message.Message):
    __slots__ = ("tenant", "specs")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    SPECS_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    specs: _containers.RepeatedCompositeFieldContainer[_common_pb2.JobSpec]
    def __init__(self, tenant: _Optional[str] = ..., specs: _Optional[_Iterable[_Union[_common_pb2.JobSpec, _Mapping]]] = ...) -> None: ...

class SubmitResponse(_message.Message):
    __slots__ = ("job_ids", "deduplicated")
    JOB_IDS_FIELD_NUMBER: _ClassVar[int]
    DEDUPLICATED_FIELD_NUMBER: _ClassVar[int]
    job_ids: _containers.RepeatedScalarFieldContainer[str]
    deduplicated: _containers.RepeatedScalarFieldContainer[bool]
    def __init__(self, job_ids: _Optional[_Iterable[str]] = ..., deduplicated: _Optional[_Iterable[bool]] = ...) -> None: ...

class FanOutRequest(_message.Message):
    __slots__ = ("tenant", "children", "on_complete", "gate_policy", "ctx_id")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    CHILDREN_FIELD_NUMBER: _ClassVar[int]
    ON_COMPLETE_FIELD_NUMBER: _ClassVar[int]
    GATE_POLICY_FIELD_NUMBER: _ClassVar[int]
    CTX_ID_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    children: _containers.RepeatedCompositeFieldContainer[_common_pb2.JobSpec]
    on_complete: _common_pb2.JobSpec
    gate_policy: str
    ctx_id: str
    def __init__(self, tenant: _Optional[str] = ..., children: _Optional[_Iterable[_Union[_common_pb2.JobSpec, _Mapping]]] = ..., on_complete: _Optional[_Union[_common_pb2.JobSpec, _Mapping]] = ..., gate_policy: _Optional[str] = ..., ctx_id: _Optional[str] = ...) -> None: ...

class FanOutResponse(_message.Message):
    __slots__ = ("child_job_ids", "gate_id")
    CHILD_JOB_IDS_FIELD_NUMBER: _ClassVar[int]
    GATE_ID_FIELD_NUMBER: _ClassVar[int]
    child_job_ids: _containers.RepeatedScalarFieldContainer[str]
    gate_id: str
    def __init__(self, child_job_ids: _Optional[_Iterable[str]] = ..., gate_id: _Optional[str] = ...) -> None: ...

class QueryRequest(_message.Message):
    __slots__ = ("tenant", "ctx_id", "state", "task_name", "pipeline", "stage", "group_key", "created_after", "page_size", "page_token", "parent_gate_id")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    CTX_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    TASK_NAME_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_FIELD_NUMBER: _ClassVar[int]
    STAGE_FIELD_NUMBER: _ClassVar[int]
    GROUP_KEY_FIELD_NUMBER: _ClassVar[int]
    CREATED_AFTER_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    PARENT_GATE_ID_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    ctx_id: str
    state: _common_pb2.JobState
    task_name: str
    pipeline: str
    stage: str
    group_key: str
    created_after: _timestamp_pb2.Timestamp
    page_size: int
    page_token: str
    parent_gate_id: str
    def __init__(self, tenant: _Optional[str] = ..., ctx_id: _Optional[str] = ..., state: _Optional[_Union[_common_pb2.JobState, str]] = ..., task_name: _Optional[str] = ..., pipeline: _Optional[str] = ..., stage: _Optional[str] = ..., group_key: _Optional[str] = ..., created_after: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ..., parent_gate_id: _Optional[str] = ...) -> None: ...

class QueryResponse(_message.Message):
    __slots__ = ("jobs", "next_page_token")
    JOBS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    jobs: _containers.RepeatedCompositeFieldContainer[_common_pb2.Job]
    next_page_token: str
    def __init__(self, jobs: _Optional[_Iterable[_Union[_common_pb2.Job, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class GetJobRequest(_message.Message):
    __slots__ = ("tenant", "job_id")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    job_id: str
    def __init__(self, tenant: _Optional[str] = ..., job_id: _Optional[str] = ...) -> None: ...

class AwaitJobRequest(_message.Message):
    __slots__ = ("tenant", "job_id", "timeout_s")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_S_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    job_id: str
    timeout_s: int
    def __init__(self, tenant: _Optional[str] = ..., job_id: _Optional[str] = ..., timeout_s: _Optional[int] = ...) -> None: ...

class CancelRequest(_message.Message):
    __slots__ = ("tenant", "job_id", "cascade")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    CASCADE_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    job_id: str
    cascade: bool
    def __init__(self, tenant: _Optional[str] = ..., job_id: _Optional[str] = ..., cascade: _Optional[bool] = ...) -> None: ...

class CancelResponse(_message.Message):
    __slots__ = ("previous_state", "cancelled", "note")
    PREVIOUS_STATE_FIELD_NUMBER: _ClassVar[int]
    CANCELLED_FIELD_NUMBER: _ClassVar[int]
    NOTE_FIELD_NUMBER: _ClassVar[int]
    previous_state: _common_pb2.JobState
    cancelled: bool
    note: str
    def __init__(self, previous_state: _Optional[_Union[_common_pb2.JobState, str]] = ..., cancelled: _Optional[bool] = ..., note: _Optional[str] = ...) -> None: ...

class SignalRequest(_message.Message):
    __slots__ = ("tenant", "wait_key", "payload_json", "signaled_by")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    WAIT_KEY_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    SIGNALED_BY_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    wait_key: str
    payload_json: bytes
    signaled_by: str
    def __init__(self, tenant: _Optional[str] = ..., wait_key: _Optional[str] = ..., payload_json: _Optional[bytes] = ..., signaled_by: _Optional[str] = ...) -> None: ...

class SignalResponse(_message.Message):
    __slots__ = ("delivered",)
    DELIVERED_FIELD_NUMBER: _ClassVar[int]
    delivered: int
    def __init__(self, delivered: _Optional[int] = ...) -> None: ...

class ResubmitRequest(_message.Message):
    __slots__ = ("tenant", "job_ids")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    JOB_IDS_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    job_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, tenant: _Optional[str] = ..., job_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class StreamEventsRequest(_message.Message):
    __slots__ = ("tenant", "ctx_id", "snapshot")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    CTX_ID_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    ctx_id: str
    snapshot: bool
    def __init__(self, tenant: _Optional[str] = ..., ctx_id: _Optional[str] = ..., snapshot: _Optional[bool] = ...) -> None: ...
