"""Local spawn-process task invocation using the existing DAG scheduler and actors.

Only trusted Python registrations cross the private actor pipe. This is neither
a distributed runtime nor a sandbox for untrusted functions or pickle payloads.
"""

from __future__ import annotations

import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from threading import Event, Lock, get_ident
from types import MappingProxyType
from typing import Literal

from graph_sail.actors import (
    ActorCall,
    ActorConfig,
    ActorDiedError,
    ProcessActor,
    _CancellationConnection,
    _pack,
    _preserve_failure,
)
from graph_sail.errors import GraphSailError, ValidationError
from graph_sail.execution import (
    ExecutionConfig,
    ExecutionResult,
    TaskCancelled,
    TaskContext,
    TaskDefinition,
    TaskRegistry,
    _attempt,
    _count,
    _duration,
    _invoke,
    _Outcome,
    _prepare_execution,
    _Runner,
)
from graph_sail.models import GraphSpec


class ProcessTaskTimeout(GraphSailError):
    """The parent invocation budget expired; side effects may already exist."""


@dataclass(frozen=True, slots=True)
class ProcessTaskConfig:
    """Per-worker wire, startup, invocation and cancellation cleanup budgets.

    Invocation time includes dispatch/serialization, but excludes startup. The
    cancellation grace begins when the driver observes a stop. OS/native calls,
    trusted pickle hooks and interpreter teardown are not hard-real-time bounded.
    """

    max_message_bytes: int = 1024 * 1024
    startup_timeout_seconds: float = 30.0
    attempt_timeout_seconds: float | None = None
    cancellation_grace_seconds: float = 0.25
    shutdown_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        _count(self.max_message_bytes, "max_message_bytes", minimum=1024, maximum=16 * 1024**2)
        _duration(
            self.startup_timeout_seconds, "startup_timeout_seconds", minimum=0.001, maximum=60
        )
        if self.attempt_timeout_seconds is not None:
            _duration(
                self.attempt_timeout_seconds, "attempt_timeout_seconds", minimum=1e-6, maximum=86400
            )
        _duration(
            self.cancellation_grace_seconds, "cancellation_grace_seconds", minimum=0, maximum=60
        )
        _duration(self.shutdown_timeout_seconds, "shutdown_timeout_seconds", minimum=0, maximum=60)


@dataclass(frozen=True, slots=True)
class ProcessAttempt:
    """Output-only invocation diagnostics, without task values or callable bodies."""

    node_id: str
    attempt: int
    worker_id: int | None
    pid: int | None
    timing_source: Literal["worker", "driver"]
    round_trip_ms: float | None
    cancellation_requested: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "node_id": self.node_id,
            "attempt": self.attempt,
            "worker_id": self.worker_id,
            "pid": self.pid,
            "timing_source": self.timing_source,
            "round_trip_ms": self.round_trip_ms,
            "cancellation_requested": self.cancellation_requested,
        }


@dataclass(frozen=True, slots=True)
class ProcessWorker:
    """Output-only metadata for a successfully started and subsequently joined worker."""

    worker_id: int
    pid: int
    exitcode: int | None
    cancellation_requested: bool
    termination_requested: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "worker_id": self.worker_id,
            "pid": self.pid,
            "exitcode": self.exitcode,
            "cancellation_requested": self.cancellation_requested,
            "termination_requested": self.termination_requested,
        }


@dataclass(frozen=True, slots=True)
class ProcessExecutionResult:
    """Output-only process diagnostics composed with the shared scheduler result.

    Constructors are ordinary data containers, not verification certificates.
    Results returned by execute_process_graph include only joined workers.
    """

    execution: ExecutionResult
    attempts: tuple[ProcessAttempt, ...]
    workers: tuple[ProcessWorker, ...]
    elapsed_ms: float

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": "graph-sail-process-execution",
            "schema_version": 1,
            "execution": self.execution.to_dict(),
            "attempts": [item.to_dict() for item in self.attempts],
            "workers": [item.to_dict() for item in self.workers],
            "elapsed_ms": self.elapsed_ms,
        }


class _PipeCancellation:
    """Child-only, single-thread task observation of irreversible sender closure."""

    def __init__(self, receiver: _CancellationConnection) -> None:
        self.receiver = receiver
        self._cancelled = False

    @property
    def cancelled(self) -> bool:
        if not self._cancelled:
            try:
                # No sender ever writes data. Readability is EOF; a malformed
                # readable channel is also fail-closed cancellation, not input.
                self._cancelled = self.receiver.poll(0)
            except (OSError, ValueError):
                self._cancelled = True
        return self._cancelled

    def raise_if_cancelled(self) -> None:
        if self.cancelled:
            raise TaskCancelled("process task cancellation requested")


@dataclass(frozen=True, slots=True)
class _TaskReply:
    outcome: _Outcome
    pid: int


class _TaskWorker:
    """The sole factory permitted to receive the private bootstrap OS endpoint."""

    def __init__(self, cancellation: _CancellationConnection) -> None:
        self.cancellation = _PipeCancellation(cancellation)

    def invoke(
        self,
        definition: TaskDefinition,
        node_id: str,
        device: str,
        attempt: int,
        dependencies: dict[str, object],
        epoch: float,
        submitted_ms: float,
    ) -> _TaskReply:
        context = TaskContext(
            node_id, device, attempt, MappingProxyType(dependencies), self.cancellation
        )
        return _TaskReply(_invoke(definition, context, epoch, submitted_ms), os.getpid())


@dataclass(slots=True)
class _Lease:
    worker_id: int
    actor: ProcessActor
    sender: _CancellationConnection
    cancelled: bool = False
    retired: bool = False


class _ProcessInvoker:
    """One healthy serial actor per driver thread; no second scheduler or retry loop."""

    def __init__(self, config: ProcessTaskConfig) -> None:
        self.config = config
        self.lock = Lock()
        self.leases: dict[int, _Lease] = {}
        self.attempts: list[ProcessAttempt] = []
        self.workers: list[ProcessWorker] = []
        self.next_id = 1

    def _worker(self) -> _Lease:
        owner = get_ident()
        with self.lock:
            lease = self.leases.get(owner)
        if lease is not None:
            return lease
        actor, sender = ProcessActor._for_task_worker(
            ActorConfig(
                max_pending=1,
                max_message_bytes=self.config.max_message_bytes,
                startup_timeout_seconds=self.config.startup_timeout_seconds,
            )
        )
        try:
            with self.lock:
                lease = _Lease(self.next_id, actor, sender)
                self.next_id += 1
                self.leases[owner] = lease
        except BaseException as error:
            primary = error
            for cleanup_operation in (sender.close, actor.terminate):
                try:
                    cleanup_operation()
                except BaseException as cleanup:
                    primary = _preserve_failure(primary, cleanup)
            if primary is not error:
                raise primary from error
            raise
        return lease

    @staticmethod
    def _signal(lease: _Lease) -> None:
        # Each lease belongs to exactly one driver until that driver is joined.
        # No lock held across close/join, no bytes sent, no reusable reset state.
        lease.cancelled = True
        lease.sender.close()

    def _retire(self, lease: _Lease, *, force: bool) -> None:
        if lease.retired:
            return
        problem: BaseException | None = None
        try:
            lease.sender.close()
        except BaseException as error:
            problem = error
        try:
            if force:
                lease.actor.terminate()
            else:
                lease.actor.close(self.config.shutdown_timeout_seconds)
        except BaseException as error:
            problem = error if problem is None else _preserve_failure(problem, error)
        if problem is not None:
            raise problem
        # close/terminate return only after the broker's actual child cleanup.
        pid = lease.actor.pid
        if pid is None:
            raise ActorDiedError("started task worker has no process ID")
        with self.lock:
            remaining = {
                owner: active for owner, active in self.leases.items() if active is not lease
            }
            self.workers.append(
                ProcessWorker(
                    lease.worker_id,
                    pid,
                    lease.actor.exitcode,
                    lease.cancelled,
                    force,
                )
            )
            self.leases = remaining
            lease.retired = True

    def __call__(
        self, definition: TaskDefinition, context: TaskContext, epoch: float, submitted_ms: float
    ) -> _Outcome:
        started = (time.monotonic() - epoch) * 1000
        lease: _Lease | None = None
        call: ActorCall | None = None
        timing_source: Literal["worker", "driver"] = "driver"
        try:
            context.cancellation.raise_if_cancelled()
            lease = self._worker()
            context.cancellation.raise_if_cancelled()
            dispatched = time.monotonic()
            call = lease.actor.submit(
                "invoke",
                args=(
                    definition,
                    context.node_id,
                    context.device,
                    context.attempt,
                    dict(context.dependencies),
                    epoch,
                    submitted_ms,
                ),
            )
            value = self._wait(lease, call, context, dispatched)
            if type(value) is not _TaskReply or value.pid != lease.actor.pid:
                raise ActorDiedError("invalid process task reply")
            timing_source = "worker"
            outcome = value.outcome
            if outcome.attempt.status == "cancelled":
                self._signal(lease)
            if lease.cancelled:
                self._retire(lease, force=False)
            return outcome
        except BaseException as error:
            if lease is not None:
                try:
                    if isinstance(error, (TaskCancelled, ProcessTaskTimeout)) or not isinstance(
                        error, Exception
                    ):
                        lease.cancelled = True
                    self._retire(lease, force=call is not None and not call.done())
                except BaseException as cleanup:
                    primary = _preserve_failure(error, cleanup)
                    if primary is cleanup:
                        raise
                    raise primary from cleanup
            if not isinstance(error, Exception):
                raise
            return _Outcome(
                _attempt(
                    context.attempt,
                    "cancelled" if isinstance(error, TaskCancelled) else "failed",
                    submitted_ms,
                    started,
                    epoch,
                    error,
                ),
                retryable=False,
            )
        finally:
            with self.lock:
                self.attempts.append(
                    ProcessAttempt(
                        context.node_id,
                        context.attempt,
                        None if lease is None else lease.worker_id,
                        None if lease is None else lease.actor.pid,
                        timing_source,
                        None
                        if call is None or call.elapsed_seconds is None
                        else call.elapsed_seconds * 1000,
                        lease is not None and lease.cancelled,
                    )
                )

    def _wait(
        self, lease: _Lease, call: ActorCall, context: TaskContext, dispatched: float
    ) -> object:
        timeout = self.config.attempt_timeout_seconds
        stop: TaskCancelled | ProcessTaskTimeout | None = None
        grace_end: float | None = None
        while True:
            if stop is None:
                if context.cancellation.cancelled:
                    stop = TaskCancelled("execution cancellation requested")
                elif timeout is not None and time.monotonic() - dispatched >= timeout:
                    stop = ProcessTaskTimeout("process task invocation budget expired")
                if stop is not None:
                    try:
                        self._signal(lease)
                    except Exception as cleanup:
                        stop.add_note(
                            f"cancellation channel close failed: {type(cleanup).__name__}"
                        )
                        raise stop from cleanup
                    grace_end = time.monotonic() + self.config.cancellation_grace_seconds
            if call.done():
                # Once stop is observed, a late success or broken-pipe response
                # cannot supersede it. Retirement still joins the actual child.
                if stop is not None:
                    raise stop
                return call.result()
            if grace_end is not None and time.monotonic() >= grace_end and stop is not None:
                raise stop
            try:
                call.result(0.02)
            except TimeoutError:
                pass
            except Exception as error:
                # Classify only after rechecking cancellation/deadline above.
                if not call.done():
                    raise ActorDiedError("unsettled actor handle returned an exception") from error

    def close(self) -> None:
        # Called only after the shared scheduler has joined every driver thread.
        problem: BaseException | None = None
        for lease in tuple(self.leases.values()):
            try:
                self._retire(lease, force=False)
            except BaseException as error:
                problem = error if problem is None else _preserve_failure(problem, error)
        if problem is not None:
            raise problem


def execute_process_graph(
    graph: GraphSpec,
    registry: TaskRegistry,
    placements: Mapping[str, str],
    *,
    config: ExecutionConfig | None = None,
    process_config: ProcessTaskConfig | None = None,
    cancel_event: Event | None = None,
) -> ProcessExecutionResult:
    """Execute the same admitted DAG using trusted local spawned task workers.

    Only child-returned application errors use TaskDefinition's retry policy.
    Infrastructure failures/timeouts are never replayed automatically. Values
    are serialized snapshots; ObjectRefs remain refs until task code explicitly
    reads them with a LocalObjectClient. No callable names come from graph JSON.
    """
    began = time.monotonic()
    tasks, assigned, memory, options = _prepare_execution(
        graph, registry, placements, config, cancel_event
    )
    invoker = _prepare_process_invoker(tasks, options, process_config)
    runner = _Runner(graph, tasks, assigned, memory, options, cancel_event, invocation=invoker)
    return _run_process_runner(runner, invoker, began)


def _prepare_process_invoker(
    tasks: TaskRegistry, options: ExecutionConfig, process_config: ProcessTaskConfig | None
) -> _ProcessInvoker:
    """Preflight only: no child, pipe or driver starts during registration checks."""
    if process_config is None:
        process_config = ProcessTaskConfig()
    if not isinstance(process_config, ProcessTaskConfig):
        raise ValidationError("process_config must be ProcessTaskConfig")
    process_config = ProcessTaskConfig(
        process_config.max_message_bytes,
        process_config.startup_timeout_seconds,
        process_config.attempt_timeout_seconds,
        process_config.cancellation_grace_seconds,
        process_config.shutdown_timeout_seconds,
    )
    if options.max_workers > 16:
        raise ValidationError("process execution supports at most 16 workers")
    if options.max_workers * process_config.max_message_bytes > 64 * 1024**2:
        raise ValidationError("aggregate configured process message capacity exceeds 64 MiB")
    for node in tasks.tasks:
        # No worker starts if even a single registered callable is unpicklable.
        _pack(tasks.definition(node), process_config.max_message_bytes)
    return _ProcessInvoker(process_config)


def _run_process_runner(
    runner: _Runner, invoker: _ProcessInvoker, began: float
) -> ProcessExecutionResult:
    """Shared blocking/handle cleanup; results are published only after worker joins."""
    try:
        result = runner.run()
    except BaseException as error:
        try:
            invoker.close()
        except BaseException as cleanup:
            primary = _preserve_failure(error, cleanup)
            if primary is not error:
                raise primary from error
        raise
    invoker.close()
    return ProcessExecutionResult(
        result,
        tuple(sorted(invoker.attempts, key=lambda item: (item.node_id, item.attempt))),
        tuple(sorted(invoker.workers, key=lambda item: item.worker_id)),
        (time.monotonic() - began) * 1000,
    )


__all__ = [
    "ProcessAttempt",
    "ProcessExecutionResult",
    "ProcessTaskConfig",
    "ProcessTaskTimeout",
    "ProcessWorker",
    "execute_process_graph",
]
