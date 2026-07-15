"""Task registration bookkeeping + boot-time validation (spec 8.2, 8.3).

The decorator returns the function unchanged (handlers stay plain-callable in
unit tests). Boot validation turns whole classes of runtime surprises into named
errors *at boot* — never warnings (spec 8.3).
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .errors import ConfigError
from .profiles import PROFILE_DEFAULTS, Profile, coerce_profile
from .types import RetryPolicy

if TYPE_CHECKING:
    from pydantic import BaseModel


@dataclass(slots=True)
class RegisteredTask:
    """Everything the dispatch pipeline needs to run one task (spec 8.2)."""

    name: str
    handler: Callable[..., Any]
    profile: Profile = Profile.IO
    runs_on: list[str] = field(default_factory=list)
    rate_class: str | None = None
    timeout_s: int | None = None
    lease_ttl_s: int | None = None
    max_attempts: int | None = None
    backoff: RetryPolicy | None = None
    max_concurrent_per_group: int | None = None
    input_schema: type[BaseModel] | None = None
    output_schema: type[BaseModel] | None = None
    is_coroutine: bool = False

    @property
    def effective_timeout_s(self) -> int | None:
        if self.timeout_s is not None:
            return self.timeout_s
        return PROFILE_DEFAULTS[self.profile].timeout_s

    @property
    def effective_lease_ttl_s(self) -> int | None:
        if self.lease_ttl_s is not None:
            return self.lease_ttl_s
        return PROFILE_DEFAULTS[self.profile].lease_ttl_s


class TaskRegistry:
    """Holds registrations and validates them at boot (spec 8.3)."""

    def __init__(self) -> None:
        self._tasks: dict[str, RegisteredTask] = {}

    def register(
        self,
        name: str,
        handler: Callable[..., Any],
        *,
        profile: str | Profile = Profile.IO,
        runs_on: list[str] | None = None,
        rate_class: str | None = None,
        timeout_s: int | None = None,
        lease_ttl_s: int | None = None,
        max_attempts: int | None = None,
        backoff: RetryPolicy | None = None,
        max_concurrent_per_group: int | None = None,
        input_schema: type[BaseModel] | None = None,
        output_schema: type[BaseModel] | None = None,
    ) -> RegisteredTask:
        if name in self._tasks:
            raise ConfigError(
                f"duplicate task_name {name!r}: a task identity is registered once per fleet"
            )
        task = RegisteredTask(
            name=name,
            handler=handler,
            profile=coerce_profile(profile),
            runs_on=runs_on or [],
            rate_class=rate_class,
            timeout_s=timeout_s,
            lease_ttl_s=lease_ttl_s,
            max_attempts=max_attempts,
            backoff=backoff,
            max_concurrent_per_group=max_concurrent_per_group,
            input_schema=input_schema,
            output_schema=output_schema,
            is_coroutine=inspect.iscoroutinefunction(handler),
        )
        self._tasks[name] = task
        return task

    def get(self, name: str) -> RegisteredTask | None:
        return self._tasks.get(name)

    def names(self) -> list[str]:
        return list(self._tasks)

    def all_runs_on(self) -> set[str]:
        tags: set[str] = set()
        for task in self._tasks.values():
            tags.update(task.runs_on)
        return tags

    def profiles(self) -> set[Profile]:
        return {task.profile for task in self._tasks.values()}

    def validate(self, *, strict_schemas: bool, heartbeat_interval_s: float) -> None:
        """Run every boot check; raise :class:`ConfigError` on the first violation."""
        if not self._tasks:
            raise ConfigError("no tasks registered: a worker with zero @worker.task handlers")

        for task in self._tasks.values():
            self._validate_signature(task)
            self._validate_profile_kind(task)
            self._validate_schemas(task, strict_schemas)
            self._validate_timeout_lease(task, heartbeat_interval_s)

    @staticmethod
    def _validate_signature(task: RegisteredTask) -> None:
        sig = inspect.signature(task.handler)
        params = [
            p
            for p in sig.parameters.values()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        ]
        names = [p.name for p in params]
        if len(names) < 2:
            raise ConfigError(
                f"task {task.name!r} handler must accept (ctx, payload); got {names or 'no params'}"
            )
        if names[0] in ("payload", "data") and names[1] in ("ctx", "context"):
            raise ConfigError(
                f"task {task.name!r} handler params look reversed: expected (ctx, payload), "
                f"got ({names[0]}, {names[1]})"
            )

    @staticmethod
    def _validate_profile_kind(task: RegisteredTask) -> None:
        wants_async = PROFILE_DEFAULTS[task.profile].wants_async
        if task.profile == Profile.IO and not task.is_coroutine:
            raise ConfigError(
                f"task {task.name!r} is io-profile but its handler is sync — a sync handler on "
                f"the event loop is the #1 silent-latency bug; make it `async def`, or declare "
                f"profile='cpu'"
            )
        if not wants_async and task.is_coroutine:
            raise ConfigError(
                f"task {task.name!r} is {task.profile.value}-profile but its handler is async — "
                f"a coroutine cannot cross a process boundary; make it a plain `def`"
            )

    @staticmethod
    def _validate_schemas(task: RegisteredTask, strict_schemas: bool) -> None:
        if strict_schemas and (task.input_schema is None or task.output_schema is None):
            missing = []
            if task.input_schema is None:
                missing.append("input_schema")
            if task.output_schema is None:
                missing.append("output_schema")
            raise ConfigError(
                f"task {task.name!r} is missing {', '.join(missing)} "
                f"(worker.strict_schemas=True requires both)"
            )

    @staticmethod
    def _validate_timeout_lease(task: RegisteredTask, heartbeat_interval_s: float) -> None:
        timeout = task.effective_timeout_s
        lease = task.effective_lease_ttl_s
        if timeout is not None and lease is not None and timeout >= lease * 10:
            raise ConfigError(
                f"task {task.name!r} has timeout_s={timeout} >= lease_ttl_s*10 ({lease * 10}); "
                f"this is almost always a unit mistake"
            )
        if lease is not None and heartbeat_interval_s > lease / 3:
            raise ConfigError(
                f"task {task.name!r}: heartbeat_interval_s={heartbeat_interval_s} is too large "
                f"for lease_ttl_s={lease} (must be <= ttl/3 to survive missed beats)"
            )


__all__ = ["TaskRegistry", "RegisteredTask"]
