import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RateClass(_message.Message):
    __slots__ = ("name", "capacity", "refill_per_s")
    NAME_FIELD_NUMBER: _ClassVar[int]
    CAPACITY_FIELD_NUMBER: _ClassVar[int]
    REFILL_PER_S_FIELD_NUMBER: _ClassVar[int]
    name: str
    capacity: float
    refill_per_s: float
    def __init__(self, name: _Optional[str] = ..., capacity: _Optional[float] = ..., refill_per_s: _Optional[float] = ...) -> None: ...

class ListRateClassesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListRateClassesResponse(_message.Message):
    __slots__ = ("classes",)
    CLASSES_FIELD_NUMBER: _ClassVar[int]
    classes: _containers.RepeatedCompositeFieldContainer[RateClass]
    def __init__(self, classes: _Optional[_Iterable[_Union[RateClass, _Mapping]]] = ...) -> None: ...

class CronSchedule(_message.Message):
    __slots__ = ("schedule_id", "cron_expr", "task_name", "payload_json", "tenant", "enabled", "last_fire", "next_fire")
    SCHEDULE_ID_FIELD_NUMBER: _ClassVar[int]
    CRON_EXPR_FIELD_NUMBER: _ClassVar[int]
    TASK_NAME_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    LAST_FIRE_FIELD_NUMBER: _ClassVar[int]
    NEXT_FIRE_FIELD_NUMBER: _ClassVar[int]
    schedule_id: str
    cron_expr: str
    task_name: str
    payload_json: bytes
    tenant: str
    enabled: bool
    last_fire: _timestamp_pb2.Timestamp
    next_fire: _timestamp_pb2.Timestamp
    def __init__(self, schedule_id: _Optional[str] = ..., cron_expr: _Optional[str] = ..., task_name: _Optional[str] = ..., payload_json: _Optional[bytes] = ..., tenant: _Optional[str] = ..., enabled: _Optional[bool] = ..., last_fire: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., next_fire: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ListCronRequest(_message.Message):
    __slots__ = ("tenant",)
    TENANT_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    def __init__(self, tenant: _Optional[str] = ...) -> None: ...

class ListCronResponse(_message.Message):
    __slots__ = ("schedules",)
    SCHEDULES_FIELD_NUMBER: _ClassVar[int]
    schedules: _containers.RepeatedCompositeFieldContainer[CronSchedule]
    def __init__(self, schedules: _Optional[_Iterable[_Union[CronSchedule, _Mapping]]] = ...) -> None: ...

class SetCronEnabledRequest(_message.Message):
    __slots__ = ("schedule_id", "enabled", "tenant")
    SCHEDULE_ID_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    schedule_id: str
    enabled: bool
    tenant: str
    def __init__(self, schedule_id: _Optional[str] = ..., enabled: _Optional[bool] = ..., tenant: _Optional[str] = ...) -> None: ...

class DeleteCronRequest(_message.Message):
    __slots__ = ("schedule_id", "tenant")
    SCHEDULE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    schedule_id: str
    tenant: str
    def __init__(self, schedule_id: _Optional[str] = ..., tenant: _Optional[str] = ...) -> None: ...

class DeleteCronResponse(_message.Message):
    __slots__ = ("deleted",)
    DELETED_FIELD_NUMBER: _ClassVar[int]
    deleted: bool
    def __init__(self, deleted: _Optional[bool] = ...) -> None: ...

class Worker(_message.Message):
    __slots__ = ("worker_id", "tags", "labels", "slots", "slots_busy", "last_seen", "stale", "registered_tasks")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    SLOTS_FIELD_NUMBER: _ClassVar[int]
    SLOTS_BUSY_FIELD_NUMBER: _ClassVar[int]
    LAST_SEEN_FIELD_NUMBER: _ClassVar[int]
    STALE_FIELD_NUMBER: _ClassVar[int]
    REGISTERED_TASKS_FIELD_NUMBER: _ClassVar[int]
    worker_id: str
    tags: _containers.RepeatedScalarFieldContainer[str]
    labels: _containers.ScalarMap[str, str]
    slots: int
    slots_busy: int
    last_seen: _timestamp_pb2.Timestamp
    stale: bool
    registered_tasks: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, worker_id: _Optional[str] = ..., tags: _Optional[_Iterable[str]] = ..., labels: _Optional[_Mapping[str, str]] = ..., slots: _Optional[int] = ..., slots_busy: _Optional[int] = ..., last_seen: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., stale: _Optional[bool] = ..., registered_tasks: _Optional[_Iterable[str]] = ...) -> None: ...

class ListWorkersRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListWorkersResponse(_message.Message):
    __slots__ = ("workers",)
    WORKERS_FIELD_NUMBER: _ClassVar[int]
    workers: _containers.RepeatedCompositeFieldContainer[Worker]
    def __init__(self, workers: _Optional[_Iterable[_Union[Worker, _Mapping]]] = ...) -> None: ...
