"""SymbaTest — an in-process engine that runs the SDK's real dispatch pipeline (spec 20).

``SymbaTest`` wires the production :class:`~symba.dispatch.Dispatcher` to an
in-memory job table via fake gRPC stubs. Everything the SDK owns runs unchanged:
schema validation, middleware, error classification, sentinels, chains, fan-out
gates, retries (with a compressed clock), wait/signal, checkpoints, dedup and
cancellation. Only transport and persistence are faked.

Deliberately NOT emulated (documented in spec 20.3): token-bucket rate limiting,
real lease expiry timing, and cross-worker contention. Tests needing those run
against the dockerized engine via the conformance corpus.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from symba import _json
from symba._proto import common_pb2, data_plane_pb2
from symba.dispatch import DispatchDeps, Dispatcher
from symba.errors import JobCancelled, JobFailed
from symba.executors.asyncio_executor import AsyncioExecutor
from symba.job import Gate, JobHandle
from symba.logging import get_logger
from symba.middleware import LoggingMiddleware, MiddlewareChain
from symba.profiles import Profile
from symba.schemas import register_output_schema
from symba.specs import build_job_spec, spec_from_dict
from symba.testing._store import GateRecord, JobRecord, MemStore
from symba.testing._stubs import MemClientStub, MemWorkerStub
from symba.types import CancelOutcome, GateStatus, JobEvent, JobState, JobStatus

if TYPE_CHECKING:
    from symba.executors.base import Executor
    from symba.worker import Worker

_log = get_logger(component="symbatest")

#: Default retry ceiling when neither the task nor the spec sets one (mirrors the
#: engine's conservative default; kept local so SymbaTest never imports engine config).
_DEFAULT_MAX_ATTEMPTS = 3


@dataclass(slots=True)
class _ForcedFailure:
    """A harness-injected failure for a task (spec 20.2).

    ``retryable=True`` + ``remaining=1`` reproduces the classic one-shot
    ``fail_next``. ``retryable=False`` drives a task to DEAD in one attempt.
    ``remaining=None`` (via ``fail_always``) fails every completion until cleared.
    """

    reason: str
    retryable: bool
    remaining: int | None  # None => unbounded

    def consume(self) -> None:
        if self.remaining is not None:
            self.remaining -= 1

    @property
    def exhausted(self) -> bool:
        return self.remaining is not None and self.remaining <= 0


class SymbaTest:
    """In-memory engine + client facade for tests (spec 20.1, 20.2)."""

    def __init__(self, *, default_pipeline: str | None = None, tenant: str = "default") -> None:
        self.tenant = tenant
        self.default_pipeline = default_pipeline
        self._store = MemStore()
        self._worker_stub = MemWorkerStub(self)
        self._client_stub = MemClientStub(self)
        self._dispatcher: Dispatcher | None = None
        self._registry_worker: Worker | None = None
        self._executors: dict[Profile, Executor] = {Profile.IO: AsyncioExecutor()}
        self._middleware: list[Any] = [LoggingMiddleware()]
        #: job_ids for which a cancel was requested (cooperative, surfaced via Heartbeat).
        self._cancel_requested: set[str] = set()
        #: forced failures keyed by task name (spec 20.2 fail_next / fail_always).
        #: value = (reason, retryable, remaining) where remaining is None for an
        #: unbounded fail_always and a countdown otherwise.
        self._forced_failures: dict[str, _ForcedFailure] = {}
        #: id of the job the current dispatch is running, so Wait/GetResult resolve locally.
        self._current_job_id: str | None = None
        self._lease_seq = 0
        self._idle = asyncio.Event()
        self._idle.set()

    # ----------------------------------------------------------- registration
    def register(self, worker: Worker) -> None:
        """Mount a Worker's registry + middleware onto this in-memory engine (spec 20.2)."""
        self._registry_worker = worker
        for name in worker.registry.names():
            task = worker.registry.get(name)
            assert task is not None
            register_output_schema(name, task.output_schema)
        self._middleware = [LoggingMiddleware(), *worker._middleware[1:]]  # keep user middleware
        self._executors = {Profile.IO: AsyncioExecutor()}
        self._dispatcher = Dispatcher(
            DispatchDeps(
                stub=self._worker_stub,  # type: ignore[arg-type]
                registry=worker.registry,
                middleware=MiddlewareChain(self._middleware),
                executors=self._executors,
                tenant=self.tenant,
                heartbeat_interval_s=worker._heartbeat_interval_s,
                classify_overrides=worker._classify_overrides,
                client_stub=self._client_stub,  # type: ignore[arg-type]
                default_pipeline=self.default_pipeline,
                redis_client=None,
            )
        )

    def _next_lease(self) -> str:
        self._lease_seq += 1
        return f"lease-{self._lease_seq}"

    def _max_attempts(self, spec: common_pb2.JobSpec) -> int:
        if spec.HasField("retry") and spec.retry.max_attempts:
            return spec.retry.max_attempts
        if self._registry_worker is not None:
            task = self._registry_worker.registry.get(spec.task_name)
            if task is not None and task.max_attempts:
                return task.max_attempts
        return _DEFAULT_MAX_ATTEMPTS

    # ------------------------------------------------------ enqueue / fan-out
    def _enqueue(
        self, spec: common_pb2.JobSpec, *, tenant: str, gate_id: str | None = None
    ) -> tuple[str, bool]:
        """Insert one job (or return the deduped id). Chains split into a head + tail."""
        identity = self._store.dedup_identity(tenant, spec)
        if identity is not None and identity in self._store.dedup:
            return self._store.dedup[identity], True

        job_id = self._store.new_job_id()
        chain = list(spec.chain)
        head_spec = spec
        chain_tail: list[str] = []
        if chain:
            head_spec = common_pb2.JobSpec()
            head_spec.CopyFrom(spec)
            head_spec.task_name = chain[0]
            del head_spec.chain[:]
            head_spec.ctx_id = spec.ctx_id or job_id
            chain_tail = chain[1:]

        record = JobRecord(
            id=job_id,
            spec=head_spec,
            tenant=tenant,
            state=JobState.QUEUED,
            chain_tail=chain_tail,
            gate_id=gate_id,
        )
        if not head_spec.ctx_id:
            head_spec.ctx_id = job_id
        record.log("submitted", task=head_spec.task_name)
        self._store.jobs[job_id] = record
        if identity is not None:
            self._store.dedup[identity] = job_id
        self._idle.clear()
        return job_id, False

    def _fan_out(
        self,
        children: list[common_pb2.JobSpec],
        *,
        on_complete: common_pb2.JobSpec | None,
        gate_policy: str,
        ctx_id: str,
        tenant: str,
    ) -> tuple[str, list[str]]:
        gate_id = self._store.new_gate_id()
        child_ids: list[str] = []
        for child in children:
            child.ctx_id = child.ctx_id or ctx_id
            jid, _ = self._enqueue(child, tenant=tenant, gate_id=gate_id)
            child_ids.append(jid)
        continuation_task = on_complete.task_name if on_complete is not None else None
        self._store.gates[gate_id] = GateRecord(
            id=gate_id,
            ctx_id=ctx_id,
            child_ids=child_ids,
            gate_policy=gate_policy or "all_success",
            on_complete=on_complete,
            continuation_task=continuation_task,
        )
        return gate_id, child_ids

    # ------------------------------------------------------------- scheduler
    def _runnable(self) -> JobRecord | None:
        """Pick the next QUEUED job whose backoff window has elapsed (spec 20.2)."""
        now = self._store.clock.now()
        for record in self._store.jobs.values():
            if record.state == JobState.QUEUED and record.run_after <= now:
                return record
        return None

    async def tick(self) -> bool:
        """Run exactly one runnable job through the real pipeline. Returns False when idle."""
        record = self._runnable()
        if record is None:
            return False
        await self._run_one(record)
        return True

    async def run_until_idle(self, *, max_ticks: int = 10_000) -> None:
        """Drive the pipeline until no job is runnable (spec 20.2).

        Fast-forwards the fake clock to the earliest pending backoff when nothing is
        immediately runnable but retries are still scheduled.
        """
        for _ in range(max_ticks):
            if await self.tick():
                continue
            if self._advance_to_next_backoff():
                continue
            self._idle.set()
            return
        raise RuntimeError(
            "run_until_idle exceeded max_ticks; a task may be self-scheduling without terminating"
        )

    def _advance_to_next_backoff(self) -> bool:
        """Jump the clock to the soonest scheduled retry, if any QUEUED job is waiting."""
        now = self._store.clock.now()
        pending = [
            r.run_after
            for r in self._store.jobs.values()
            if r.state == JobState.QUEUED and r.run_after > now
        ]
        if not pending:
            return False
        self._store.clock.advance(min(pending) - now)
        return True

    async def _run_one(self, record: JobRecord) -> None:
        assert self._dispatcher is not None, "register(worker) before running jobs"
        record.state = JobState.RUNNING
        record.log("running", attempt=record.attempt)
        assignment = self._build_assignment(record)
        self._current_job_id = record.id
        try:
            await self._dispatcher.dispatch(assignment)
        finally:
            self._current_job_id = None

    def _build_assignment(self, record: JobRecord) -> data_plane_pb2.JobAssignment:
        job = common_pb2.Job(
            id=record.id,
            tenant=record.tenant,
            spec=record.spec,
            state=cast("common_pb2.JobState", JobState.RUNNING.value),
            attempt=record.attempt,
            upstream=record.upstream,
        )
        checkpoint_json = self._store.checkpoints.get(record.id, b"")
        return data_plane_pb2.JobAssignment(
            job=job,
            lease_token=self._next_lease(),
            checkpoint_json=checkpoint_json,
            event_payload_json=record.event_payload,
        )

    # -------------------------------------------------- terminal transitions
    def _on_complete(
        self, job_id: str, result_json: bytes, *, drop_chain_tail: bool, skipped: bool
    ) -> None:
        record = self._store.jobs.get(job_id)
        if record is None:
            return
        # fail_next / fail_always: the handler ran fine, but the harness forces a
        # failure. retryable controls whether the task can recover; remaining bounds
        # how many completions are forced (None => every one).
        forced = self._forced_failures.get(record.spec.task_name)
        if forced is not None:
            forced.consume()
            if forced.exhausted:
                del self._forced_failures[record.spec.task_name]
            self._on_fail(
                job_id,
                error_type="ForcedFailure",
                error_message=forced.reason,
                retryable=forced.retryable,
            )
            return
        record.result = result_json
        record.skipped = skipped
        record.state = JobState.SUCCEEDED
        record.log("succeeded", skipped=skipped)
        self._advance_chain(record, drop_chain_tail=drop_chain_tail)
        if record.gate_id is not None:
            self._maybe_fire_gate(record.gate_id)

    def _on_fail(
        self, job_id: str, *, error_type: str, error_message: str, retryable: bool
    ) -> bool:
        record = self._store.jobs.get(job_id)
        if record is None:
            return False
        record.last_error = f"{error_type}: {error_message}"
        record.error_history.append(
            {"type": error_type, "message": error_message, "attempt": record.attempt}
        )
        will_retry = retryable and record.attempt < self._max_attempts(record.spec)
        if will_retry:
            record.attempt += 1
            record.state = JobState.QUEUED
            record.run_after = self._store.clock.now() + self._backoff_s(record)
            record.log("retry_scheduled", attempt=record.attempt)
            self._idle.clear()
            return True
        record.state = JobState.DEAD
        record.log("dead", error=record.last_error)
        self._run_on_failure(record)
        if record.gate_id is not None:
            self._maybe_fire_gate(record.gate_id)
        return False

    def _backoff_s(self, record: JobRecord) -> float:
        """Compressed exponential backoff (spec 20.2). Values elapse on the fake clock."""
        base, factor = 1.0, 2.0
        if record.spec.HasField("retry"):
            base = record.spec.retry.backoff_base_s or base
            factor = record.spec.retry.backoff_factor or factor
        return base * (factor ** (record.attempt - 1))

    def _advance_chain(self, record: JobRecord, *, drop_chain_tail: bool) -> None:
        """Fire the next chain link, threading this job's result as upstream (spec 10.3)."""
        if drop_chain_tail or not record.chain_tail:
            return
        next_task = record.chain_tail[0]
        remaining = record.chain_tail[1:]
        next_spec = common_pb2.JobSpec()
        next_spec.CopyFrom(record.spec)
        next_spec.task_name = next_task
        del next_spec.chain[:]
        del next_spec.depends_on[:]
        next_spec.payload_json = b""
        next_id = self._store.new_job_id()
        next_spec.ctx_id = record.spec.ctx_id or record.id
        upstream = [
            common_pb2.UpstreamResult(
                key=record.spec.task_name, job_id=record.id, result_json=record.result
            )
        ]
        self._store.jobs[next_id] = JobRecord(
            id=next_id,
            spec=next_spec,
            tenant=record.tenant,
            state=JobState.QUEUED,
            chain_tail=remaining,
            upstream=upstream,
            gate_id=record.gate_id,
        )
        record.chain_tail = []
        self._idle.clear()

    def _run_on_failure(self, record: JobRecord) -> None:
        """Enqueue the on_failure spec when a job dies (spec 6.3)."""
        if not record.spec.HasField("on_failure"):
            return
        failure_spec = common_pb2.JobSpec()
        failure_spec.CopyFrom(record.spec.on_failure)
        failure_spec.ctx_id = record.spec.ctx_id or record.id
        self._enqueue(failure_spec, tenant=record.tenant)

    # ------------------------------------------------------------ fan-out gate
    def _maybe_fire_gate(self, gate_id: str) -> None:
        """Settle the gate; fire the continuation once the policy is satisfied (spec 7.2).

        Policy fidelity matches the engine's ``bump_gate.sql`` thresholds (FE-1):

            all_success   -> succeeded == expected
            all_terminal  -> every child terminal
            quorum(n)     -> succeeded >= min(n, expected)

        A gate whose policy can never be met (e.g. ``all_success`` with a DEAD
        child) does NOT fire ``on_complete``; instead it enqueues the
        continuation's ``on_failure`` if one is declared, and always exposes the
        outcome via :meth:`_gate_status`.
        """
        gate = self._store.gates.get(gate_id)
        if gate is None or gate.fired:
            return
        children = [self._store.jobs[c] for c in gate.child_ids if c in self._store.jobs]
        if any(not c.state.is_terminal for c in children):
            return  # not every child is terminal yet
        succeeded = [c for c in children if c.state == JobState.SUCCEEDED and not c.skipped]
        # A SKIP is a non-failure: it neither succeeds nor blocks a gate (spec 7.2).
        # The engine's bump_gate.sql must exclude skips from succeeded_children to
        # match this — see docs/engine_fixes.md SDK-2 "Skip counting". Only a DEAD
        # child can fail all_success.
        failed = [c for c in children if c.state == JobState.DEAD]

        satisfied = self._gate_policy_satisfied(
            gate.gate_policy, len(children), len(succeeded), len(failed)
        )
        if not satisfied:
            # Policy can no longer be met (all children terminal, threshold unmet).
            gate.fired = True
            gate.failed = True
            self._fire_gate_on_failure(gate)
            return

        gate.fired = True
        if gate.on_complete is None:
            return
        results = [
            {"job_id": c.id, "task": c.spec.task_name, "result": self._decode(c.result)}
            for c in succeeded
        ]
        cont_spec = common_pb2.JobSpec()
        cont_spec.CopyFrom(gate.on_complete)
        cont_spec.ctx_id = gate.ctx_id
        cont_spec.group_key = gate.id
        # SDK-2: preserve the caller's on_complete payload; deliver the gate
        # manifest under the reserved ``__gate__`` key (the shape the real engine
        # must also ship — see docs/engine_fixes.md).
        caller_payload = self._decode(gate.on_complete.payload_json)
        if not isinstance(caller_payload, dict):
            caller_payload = {}
        cont_spec.payload_json = _json.dumps(
            {
                **caller_payload,
                "__gate__": {
                    "gate_id": gate.id,
                    "results": results,
                    "expected": len(children),
                    "succeeded": len(succeeded),
                },
            }
        )
        cont_id, _ = self._enqueue(cont_spec, tenant=self.tenant)
        gate.continuation_job_id = cont_id

    @staticmethod
    def _gate_policy_satisfied(policy: str, expected: int, succeeded: int, failed: int) -> bool:
        """Evaluate a gate policy over terminal child counts (mirrors bump_gate.sql).

        A SKIPPED child counts as neither succeeded nor failed, so ``all_success``
        is satisfied as long as NO child failed (all-skipped still fires); a
        ``quorum(n)`` needs ``n`` genuine successes.
        """
        if policy == "all_success":
            return failed == 0
        if policy == "all_terminal":
            return True  # caller only invokes this once every child is terminal
        if policy.startswith("quorum(") and policy.endswith(")"):
            n = int(policy[len("quorum(") : -1])
            return succeeded >= min(n, expected)
        raise ValueError(f"unknown gate policy {policy!r}")

    def _fire_gate_on_failure(self, gate: GateRecord) -> None:
        """Enqueue the continuation's ``on_failure`` when a gate cannot satisfy its policy."""
        if gate.on_complete is None or not gate.on_complete.HasField("on_failure"):
            return
        failure_spec = common_pb2.JobSpec()
        failure_spec.CopyFrom(gate.on_complete.on_failure)
        failure_spec.ctx_id = gate.ctx_id
        failure_spec.group_key = gate.id
        self._enqueue(failure_spec, tenant=self.tenant)

    def _gate_math(self, gate: GateRecord) -> tuple[int, int, int]:
        children = [self._store.jobs[c] for c in gate.child_ids if c in self._store.jobs]
        expected = len(children)
        terminal = sum(1 for c in children if c.state.is_terminal)
        succeeded = sum(1 for c in children if c.state == JobState.SUCCEEDED and not c.skipped)
        return expected, terminal, succeeded

    # ---------------------------------------------------------- wait / signal
    def _try_consume_signal(self, wait_key: str) -> bytes | None:
        """Signal-first rendezvous: a pending signal flows straight through (spec 14.1)."""
        if wait_key in self._store.pending_signals:
            return self._store.pending_signals.pop(wait_key)
        return None

    def _park(self, job_id: str, wait_key: str) -> None:
        record = self._store.jobs.get(job_id)
        if record is None:
            return
        record.state = JobState.WAITING
        record.wait_key = wait_key
        record.log("waiting", wait_key=wait_key)

    def _signal(self, wait_key: str, payload_json: bytes) -> int:
        """Resume every job parked on wait_key; buffer for signal-first if none (spec 14.2)."""
        delivered = 0
        for record in self._store.jobs.values():
            if record.state == JobState.WAITING and record.wait_key == wait_key:
                record.state = JobState.QUEUED
                record.wait_key = None
                record.event_payload = payload_json
                record.run_after = self._store.clock.now()
                record.log("resumed", wait_key=wait_key)
                delivered += 1
                self._idle.clear()
        if delivered == 0:
            self._store.pending_signals[wait_key] = payload_json
        return delivered

    # ---------------------------------------------------------- result lookup
    def _resolve_result(self, task_name: str) -> bytes | None:
        """Serve ctx.output.fetch(): the newest SUCCEEDED job for task in the same ctx."""
        current = self._store.jobs.get(self._current_job_id or "")
        ctx_id = current.spec.ctx_id if current is not None else None
        best: JobRecord | None = None
        for record in self._store.jobs.values():
            if record.spec.task_name != task_name or record.state != JobState.SUCCEEDED:
                continue
            if ctx_id is not None and record.spec.ctx_id != ctx_id:
                continue
            best = record
        return best.result if best is not None else None

    def _decode(self, raw: bytes) -> Any:
        return _json.loads(raw) if raw else {}

    # ---------------------------------------------------------------- cancel
    def _do_cancel(self, job_id: str, *, cascade: bool) -> tuple[JobState, bool]:
        record = self._store.jobs.get(job_id)
        if record is None:
            return JobState.UNSPECIFIED, False
        prev = record.state
        if prev.is_terminal:
            return prev, False
        if prev in (JobState.QUEUED, JobState.WAITING):
            record.state = JobState.CANCELLED
            record.wait_key = None
            record.log("cancelled", from_state=prev.name)
        else:  # RUNNING: cooperative — flag it; the running handler observes via Heartbeat.
            self._cancel_requested.add(job_id)
            record.log("cancel_requested")
        if cascade:
            for child in self._store.jobs.values():
                if child.spec.ctx_id == record.spec.ctx_id and child.id != job_id:
                    if not child.state.is_terminal:
                        self._do_cancel(child.id, cascade=False)
        return prev, True

    # ----------------------------------------------------------------- query
    def _query(
        self,
        *,
        ctx_id: str | None,
        task_name: str | None,
        state: JobState | None,
        group_key: str | None,
    ) -> list[common_pb2.Job]:
        out: list[common_pb2.Job] = []
        for record in self._store.jobs.values():
            if ctx_id is not None and record.spec.ctx_id != ctx_id:
                continue
            if task_name is not None and record.spec.task_name != task_name:
                continue
            if state is not None and record.state != state:
                continue
            if group_key is not None and record.spec.group_key != group_key:
                continue
            out.append(self._job_proto(record.id))
        return out

    def _job_proto(self, job_id: str) -> common_pb2.Job:
        record = self._store.jobs.get(job_id)
        if record is None:
            return common_pb2.Job(id=job_id, state=common_pb2.JOB_STATE_UNSPECIFIED)
        return common_pb2.Job(
            id=record.id,
            tenant=record.tenant,
            spec=record.spec,
            state=cast("common_pb2.JobState", record.state.value),
            attempt=record.attempt,
            result_json=record.result,
            last_error=record.last_error,
            upstream=record.upstream,
        )

    async def _run_until_job_terminal(self, job_id: str, timeout_s: int) -> None:
        """Drive the scheduler until the awaited job reaches a terminal state (spec 20.2)."""
        for _ in range(100_000):
            record = self._store.jobs.get(job_id)
            if record is not None and record.state.is_terminal:
                return
            if await self.tick():
                continue
            if self._advance_to_next_backoff():
                continue
            return  # nothing left to run; caller inspects the (possibly non-terminal) state

    # =================================================== public client facade
    async def submit(self, task: str, payload: Any = None, **kwargs: Any) -> JobHandle:
        """Mirror of :meth:`Engine.submit` against the in-memory table (spec 20.2)."""
        spec = build_job_spec(
            task=task, payload=payload, default_pipeline=self.default_pipeline, **kwargs
        )
        job_id, deduped = self._enqueue(spec, tenant=self.tenant)
        record = self._store.jobs[job_id]
        return JobHandle(
            self,
            job_id,
            task_name=record.spec.task_name,
            ctx_id=record.spec.ctx_id or None,
            deduplicated=deduped,
        )

    async def fan_out(
        self,
        children: list[dict[str, Any]],
        *,
        on_complete: dict[str, Any] | None = None,
        gate_policy: str = "all_success",
        ctx_id: str | None = None,
    ) -> Gate:
        """Mirror of :meth:`Engine.fan_out` (spec 20.2)."""
        gate_ctx = ctx_id or self._store.new_gate_id()
        child_specs = [spec_from_dict(c, default_pipeline=self.default_pipeline) for c in children]
        cont_spec = (
            spec_from_dict(on_complete, default_pipeline=self.default_pipeline)
            if on_complete is not None
            else None
        )
        gate_id, child_ids = self._fan_out(
            child_specs,
            on_complete=cont_spec,
            gate_policy=gate_policy,
            ctx_id=gate_ctx,
            tenant=self.tenant,
        )
        handles = [
            JobHandle(self, cid, task_name=self._store.jobs[cid].spec.task_name, ctx_id=gate_ctx)
            for cid in child_ids
        ]
        return Gate(
            self,
            gate_id,
            handles,
            ctx_id=gate_ctx,
            continuation_task=cont_spec.task_name if cont_spec is not None else None,
        )

    async def signal(self, wait_key: str, payload: Any = None) -> int:
        """Deliver a signal to jobs parked on ``wait_key`` (spec 14.2)."""
        return self._signal(wait_key, _json.dumps(payload) if payload is not None else b"")

    async def get(self, job_id: str) -> JobHandle:
        record = self._store.jobs.get(job_id)
        task_name = record.spec.task_name if record is not None else ""
        return JobHandle(self, job_id, task_name=task_name)

    # ------------------------------------------------- _EngineOps for handles
    async def _await_job(self, job_id: str, timeout: float | None) -> Any:
        await self._run_until_job_terminal(job_id, int(timeout or 0))
        record = self._store.jobs.get(job_id)
        if record is None or not record.state.is_terminal:
            raise TimeoutError(f"job {job_id} did not reach a terminal state")
        if record.state == JobState.SUCCEEDED:
            return self._decode(record.result)
        if record.state == JobState.CANCELLED:
            raise JobCancelled(f"job {job_id} was cancelled")
        raise JobFailed(
            f"job {job_id} died: {record.last_error}",
            error_history=record.error_history,
        )

    async def _get_status(self, job_id: str) -> JobStatus:
        record = self._store.jobs.get(job_id)
        if record is None:
            return JobStatus(id=job_id, task_name="", state=JobState.UNSPECIFIED, attempt=0)
        return JobStatus(
            id=record.id,
            task_name=record.spec.task_name,
            state=record.state,
            attempt=record.attempt,
            ctx_id=record.spec.ctx_id or None,
            tenant=record.tenant or None,
            last_error=record.last_error or None,
        )

    async def _cancel(self, job_id: str, cascade: bool) -> CancelOutcome:
        prev, cancelled = self._do_cancel(job_id, cascade=cascade)
        return CancelOutcome(previous_state=prev, cancelled=cancelled)

    async def _job_events(self, job_id: str) -> list[JobEvent]:
        record = self._store.jobs.get(job_id)
        if record is None:
            return []
        return [
            JobEvent(job_id=job_id, event=e["event"], detail=e["detail"]) for e in record.events
        ]

    async def _gate_status(self, gate_id: str) -> GateStatus:
        gate = self._store.gates.get(gate_id)
        if gate is None:
            return GateStatus(gate_id=gate_id, expected=0, terminal=0, succeeded=0)
        expected, terminal, succeeded = self._gate_math(gate)
        return GateStatus(
            gate_id=gate_id, expected=expected, terminal=terminal, succeeded=succeeded
        )

    async def _gate_result(
        self,
        gate_id: str,
        ctx_id: str | None,
        continuation_task: str | None,
        timeout: float | None,
    ) -> Any:
        gate = self._store.gates.get(gate_id)
        if gate is None:
            raise JobFailed(f"unknown gate {gate_id}")
        while gate.continuation_job_id is None and not gate.failed:
            if not await self.tick() and not self._advance_to_next_backoff():
                break
        if gate.failed:
            raise JobFailed(
                f"gate {gate_id} did not satisfy policy {gate.gate_policy!r}; "
                f"the continuation was blocked (a child did not succeed)"
            )
        if gate.continuation_job_id is None:
            raise JobFailed(f"gate {gate_id} never fired its continuation")
        return await self._await_job(gate.continuation_job_id, timeout)

    # ------------------------------------------------------------ inspection
    def jobs(self) -> list[JobStatus]:
        """Snapshot every job in the table (spec 20.2)."""
        return [
            JobStatus(
                id=r.id,
                task_name=r.spec.task_name,
                state=r.state,
                attempt=r.attempt,
                ctx_id=r.spec.ctx_id or None,
                tenant=r.tenant or None,
                last_error=r.last_error or None,
            )
            for r in self._store.jobs.values()
        ]

    @property
    def checkpoints(self) -> dict[str, Any]:
        """Decoded checkpoint payloads keyed by job_id (spec 20.2)."""
        return {jid: self._decode(raw) for jid, raw in self._store.checkpoints.items()}

    @property
    def clock(self) -> Any:
        """The fake clock; ``clock.advance(seconds)`` fast-forwards backoff (spec 20.2)."""
        return self._store.clock

    def fail_next(
        self, task_name: str, *, reason: str = "forced failure", retryable: bool = True
    ) -> None:
        """Force the NEXT completion of ``task_name`` to fail once (spec 20.2).

        ``retryable=True`` (default) recovers on the next attempt when
        ``max_attempts > 1`` — the classic one-shot. ``retryable=False`` drives the
        task straight to DEAD in a single attempt, which is the only way to reach a
        DEAD child through a gate deterministically (FE-2).
        """
        self._forced_failures[task_name] = _ForcedFailure(
            reason=reason, retryable=retryable, remaining=1
        )

    def fail_always(
        self, task_name: str, *, times: int | None = None, reason: str = "forced failure"
    ) -> None:
        """Force ``task_name`` to fail retryably on every completion (spec 20.2).

        With the task's ``max_attempts`` exhausted the job reaches DEAD. ``times``
        bounds how many completions are forced; ``None`` forces every completion
        until cleared. This is the deterministic path to a DEAD job for tasks whose
        ``max_attempts > 1`` (FE-2).
        """
        self._forced_failures[task_name] = _ForcedFailure(
            reason=reason, retryable=True, remaining=times
        )

    # ------------------------------------------------------- context manager
    async def __aenter__(self) -> SymbaTest:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None
