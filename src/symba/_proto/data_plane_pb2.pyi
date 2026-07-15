import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from simba.v1 import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GetResultRequest(_message.Message):
    __slots__ = ("job_id", "lease_token", "task_name")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    TASK_NAME_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    lease_token: str
    task_name: str
    def __init__(self, job_id: _Optional[str] = ..., lease_token: _Optional[str] = ..., task_name: _Optional[str] = ...) -> None: ...

class GetResultResponse(_message.Message):
    __slots__ = ("result_json", "found")
    RESULT_JSON_FIELD_NUMBER: _ClassVar[int]
    FOUND_FIELD_NUMBER: _ClassVar[int]
    result_json: bytes
    found: bool
    def __init__(self, result_json: _Optional[bytes] = ..., found: _Optional[bool] = ...) -> None: ...

class ClaimRequest(_message.Message):
    __slots__ = ("worker_id", "tags", "free_slots", "sdk_version", "labels")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    FREE_SLOTS_FIELD_NUMBER: _ClassVar[int]
    SDK_VERSION_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    worker_id: str
    tags: _containers.RepeatedScalarFieldContainer[str]
    free_slots: int
    sdk_version: str
    labels: _containers.ScalarMap[str, str]
    def __init__(self, worker_id: _Optional[str] = ..., tags: _Optional[_Iterable[str]] = ..., free_slots: _Optional[int] = ..., sdk_version: _Optional[str] = ..., labels: _Optional[_Mapping[str, str]] = ...) -> None: ...

class JobAssignment(_message.Message):
    __slots__ = ("job", "lease_token", "lease_expires_at", "checkpoint_json", "event_payload_json")
    JOB_FIELD_NUMBER: _ClassVar[int]
    LEASE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    LEASE_EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_JSON_FIELD_NUMBER: _ClassVar[int]
    EVENT_PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    job: _common_pb2.Job
    lease_token: str
    lease_expires_at: _timestamp_pb2.Timestamp
    checkpoint_json: bytes
    event_payload_json: bytes
    def __init__(self, job: _Optional[_Union[_common_pb2.Job, _Mapping]] = ..., lease_token: _Optional[str] = ..., lease_expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., checkpoint_json: _Optional[bytes] = ..., event_payload_json: _Optional[bytes] = ...) -> None: ...

class HeartbeatRequest(_message.Message):
    __slots__ = ("job_id", "lease_token")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    lease_token: str
    def __init__(self, job_id: _Optional[str] = ..., lease_token: _Optional[str] = ...) -> None: ...

class HeartbeatResponse(_message.Message):
    __slots__ = ("lease_expires_at", "cancelled")
    LEASE_EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    CANCELLED_FIELD_NUMBER: _ClassVar[int]
    lease_expires_at: _timestamp_pb2.Timestamp
    cancelled: bool
    def __init__(self, lease_expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., cancelled: _Optional[bool] = ...) -> None: ...

class CompleteRequest(_message.Message):
    __slots__ = ("job_id", "lease_token", "result_json", "drop_chain_tail", "skipped")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    RESULT_JSON_FIELD_NUMBER: _ClassVar[int]
    DROP_CHAIN_TAIL_FIELD_NUMBER: _ClassVar[int]
    SKIPPED_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    lease_token: str
    result_json: bytes
    drop_chain_tail: bool
    skipped: bool
    def __init__(self, job_id: _Optional[str] = ..., lease_token: _Optional[str] = ..., result_json: _Optional[bytes] = ..., drop_chain_tail: _Optional[bool] = ..., skipped: _Optional[bool] = ...) -> None: ...

class CompleteResponse(_message.Message):
    __slots__ = ("accepted",)
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    accepted: bool
    def __init__(self, accepted: _Optional[bool] = ...) -> None: ...

class FailRequest(_message.Message):
    __slots__ = ("job_id", "lease_token", "error_type", "error_message", "stack_hash", "retryable")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    ERROR_TYPE_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    STACK_HASH_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    lease_token: str
    error_type: str
    error_message: str
    stack_hash: str
    retryable: bool
    def __init__(self, job_id: _Optional[str] = ..., lease_token: _Optional[str] = ..., error_type: _Optional[str] = ..., error_message: _Optional[str] = ..., stack_hash: _Optional[str] = ..., retryable: _Optional[bool] = ...) -> None: ...

class FailResponse(_message.Message):
    __slots__ = ("accepted", "will_retry")
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    WILL_RETRY_FIELD_NUMBER: _ClassVar[int]
    accepted: bool
    will_retry: bool
    def __init__(self, accepted: _Optional[bool] = ..., will_retry: _Optional[bool] = ...) -> None: ...

class WaitRequest(_message.Message):
    __slots__ = ("job_id", "lease_token", "wait_key", "timeout_s")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    WAIT_KEY_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_S_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    lease_token: str
    wait_key: str
    timeout_s: int
    def __init__(self, job_id: _Optional[str] = ..., lease_token: _Optional[str] = ..., wait_key: _Optional[str] = ..., timeout_s: _Optional[int] = ...) -> None: ...

class WaitResponse(_message.Message):
    __slots__ = ("parked", "event_payload_json")
    PARKED_FIELD_NUMBER: _ClassVar[int]
    EVENT_PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    parked: bool
    event_payload_json: bytes
    def __init__(self, parked: _Optional[bool] = ..., event_payload_json: _Optional[bytes] = ...) -> None: ...

class PutCheckpointRequest(_message.Message):
    __slots__ = ("job_id", "lease_token", "checkpoint_json")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_JSON_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    lease_token: str
    checkpoint_json: bytes
    def __init__(self, job_id: _Optional[str] = ..., lease_token: _Optional[str] = ..., checkpoint_json: _Optional[bytes] = ...) -> None: ...

class PutCheckpointResponse(_message.Message):
    __slots__ = ("accepted",)
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    accepted: bool
    def __init__(self, accepted: _Optional[bool] = ...) -> None: ...

class GetCheckpointRequest(_message.Message):
    __slots__ = ("job_id", "lease_token")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    lease_token: str
    def __init__(self, job_id: _Optional[str] = ..., lease_token: _Optional[str] = ...) -> None: ...

class GetCheckpointResponse(_message.Message):
    __slots__ = ("checkpoint_json", "found")
    CHECKPOINT_JSON_FIELD_NUMBER: _ClassVar[int]
    FOUND_FIELD_NUMBER: _ClassVar[int]
    checkpoint_json: bytes
    found: bool
    def __init__(self, checkpoint_json: _Optional[bytes] = ..., found: _Optional[bool] = ...) -> None: ...
