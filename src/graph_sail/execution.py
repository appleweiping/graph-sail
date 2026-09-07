"""Execute a validated DAG through explicitly registered, trusted local callables."""

from __future__ import annotations

import heapq
import inspect
import math
import time
from collections import deque
from collections.abc import Callable, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from threading import Event
from types import MappingProxyType
from typing import Literal

from graph_sail.errors import GraphSailError, ValidationError
from graph_sail.graph import predecessor_edges, successor_edges, topological_order
from graph_sail.limits import MAX_DEVICES, MAX_NODES, MAX_TEXT_LENGTH
from graph_sail.models import GraphSpec

TaskStatus = Literal["succeeded", "failed", "skipped", "cancelled"]
AttemptStatus = Literal["succeeded", "failed", "cancelled"]


class TaskCancelled(GraphSailError):
    """A registered task cooperatively acknowledged a cancellation request."""


@dataclass(frozen=True, slots=True)
class CancellationToken:
    """Read-only cancellation observation shared by the scheduler and tasks."""

    _internal: Event
    _external: Event | None = None

    @property
    def cancelled(self) -> bool:
        return self._internal.is_set() or (self._external is not None and self._external.is_set())

    def raise_if_cancelled(self) -> None:
        if self.cancelled:
            raise TaskCancelled("execution cancellation requested")


@dataclass(frozen=True, slots=True)
class TaskContext:
    """Invocation metadata and immediate predecessor results.

    The mapping is read-only; its values are shared in-process objects. Callers
    own their immutability or synchronization. ``device`` is a logical placement,
    not an acquired GPU handle or an operating-system affinity guarantee.
    """

    node_id: str
    device: str
    attempt: int
    dependencies: Mapping[str, object]
    cancellation: CancellationToken


TaskCallable = Callable[[TaskContext], object]


@dataclass(frozen=True, slots=True)
class TaskDefinition:
    """Trusted synchronous task plus a bounded, explicit application retry policy."""

    function: TaskCallable
    max_retries: int = 0
    retry_on: tuple[type[Exception], ...] = (Exception,)
    retry_delay_seconds: float = 0.0

    def __post_init__(self) -> None:
        if (
            not callable(self.function)
            or inspect.iscoroutinefunction(self.function)
            # Inspect callable objects' async entry point after callable() above.
            or inspect.iscoroutinefunction(getattr(self.function, "__call__", None))  # noqa: B004
        ):
            raise ValidationError("task function must be a synchronous callable")
        _count(self.max_retries, "max_retries", minimum=0, maximum=20)
        _duration(self.retry_delay_seconds, "retry_delay_seconds", minimum=0, maximum=60)
        if (
            not isinstance(self.retry_on, tuple)
            or len(self.retry_on) > 16
            or any(
                not isinstance(kind, type) or not issubclass(kind, Exception)
                for kind in self.retry_on
            )
        ):
            raise ValidationError("retry_on must be a tuple of at most 16 Exception classes")


@dataclass(frozen=True, slots=True)
class TaskRegistry:
    """Immutable snapshot of explicitly registered node IDs and Python callables."""

    tasks: Mapping[str, TaskDefinition | TaskCallable]

    def __post_init__(self) -> None:
        if not isinstance(self.tasks, Mapping) or not 1 <= len(self.tasks) <= MAX_NODES:
            raise ValidationError(f"registry must contain 1 to {MAX_NODES} tasks")
        snapshot: dict[str, TaskDefinition] = {}
        for node_id, task in self.tasks.items():
            _name(node_id, "registry node ID")
            snapshot[node_id] = (
                TaskDefinition(task)
                if not isinstance(task, TaskDefinition)
                else TaskDefinition(
                    task.function, task.max_retries, task.retry_on, task.retry_delay_seconds
                )
            )
        object.__setattr__(self, "tasks", MappingProxyType(snapshot))

    def definition(self, node_id: str) -> TaskDefinition:
        value = self.tasks[node_id]
        # The constructor normalizes every entry, including bare callables.
        if not isinstance(value, TaskDefinition):
            raise ValidationError("registry entry is not a TaskDefinition")
        return value


@dataclass(frozen=True, slots=True)
class ExecutionConfig:
    """Local thread/slot limits and cooperative stop policy.

    ``timeout_seconds`` requests cancellation and stops new work. Already-running
    callables are joined before return, so it is not a hard execution deadline.
    """

    max_workers: int = 4
    device_workers: Mapping[str, int] = field(default_factory=dict)
    fail_fast: bool = False
    timeout_seconds: float | None = None

    def __post_init__(self) -> None:
        _count(self.max_workers, "max_workers", minimum=1, maximum=64)
        if not isinstance(self.device_workers, Mapping) or len(self.device_workers) > MAX_DEVICES:
            raise ValidationError("device_workers must be a bounded mapping")
        slots = {}
        for device, count in self.device_workers.items():
            _name(device, "device_workers key")
            _count(count, "device worker count", minimum=1, maximum=64)
            slots[device] = count
        if type(self.fail_fast) is not bool:
            raise ValidationError("fail_fast must be a boolean")
        if self.timeout_seconds is not None:
            _duration(self.timeout_seconds, "timeout_seconds", minimum=1e-6, maximum=86_400)
        object.__setattr__(self, "device_workers", MappingProxyType(slots))


@dataclass(frozen=True, slots=True)
class TaskAttempt:
    attempt: int
    status: AttemptStatus
    submitted_ms: float
    started_ms: float
    finished_ms: float
    error_type: str | None = None
    error_message: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "attempt": self.attempt,
            "status": self.status,
            "submitted_ms": self.submitted_ms,
            "started_ms": self.started_ms,
            "finished_ms": self.finished_ms,
            "duration_ms": self.finished_ms - self.started_ms,
            "error_type": self.error_type,
            "error_message": self.error_message,
        }


@dataclass(frozen=True, slots=True)
class TaskExecution:
    node_id: str
    device: str
    status: TaskStatus
    attempts: tuple[TaskAttempt, ...]
    reason: str

    def to_dict(self) -> dict[str, object]:
        return {
            "node_id": self.node_id,
            "device": self.device,
            "status": self.status,
            "attempts": [attempt.to_dict() for attempt in self.attempts],
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    """Actual callable outcomes and timings, separate from simulated schedule data.

    ``outputs`` holds successful return objects without copying their values.
    JSON telemetry deliberately excludes those arbitrary objects.
    """

    graph_name: str
    status: Literal["succeeded", "failed", "cancelled"]
    elapsed_ms: float
    tasks: tuple[TaskExecution, ...]
    outputs: Mapping[str, object]
    cancellation_reason: str | None
    peak_in_flight_by_device: Mapping[str, int]
    reserved_memory_mb: Mapping[str, float]

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": "graph-sail-local-execution",
            "schema_version": 1,
            "graph_name": self.graph_name,
            "status": self.status,
            "elapsed_ms": self.elapsed_ms,
            "tasks": [task.to_dict() for task in self.tasks],
            "output_nodes": list(self.outputs),
            "cancellation_reason": self.cancellation_reason,
            "peak_in_flight_by_device": dict(self.peak_in_flight_by_device),
            "reserved_memory_mb": dict(self.reserved_memory_mb),
        }


def execute_graph(
    graph: GraphSpec,
    registry: TaskRegistry,
    placements: Mapping[str, str],
    *,
    config: ExecutionConfig | None = None,
    cancel_event: Event | None = None,
) -> ExecutionResult:
    """Run registered local functions after all their immediate predecessors succeed.

    Placement compatibility, memory admission and complete registry coverage are
    checked before any task starts. No latency/transfer estimate causes a sleep;
    results travel as shared Python objects. Failed descendants are skipped;
    unrelated branches continue unless fail_fast requests cancellation.
    """

    if not isinstance(graph, GraphSpec) or not isinstance(registry, TaskRegistry):
        raise ValidationError("graph and registry must be GraphSpec and TaskRegistry")
    graph.validate()
    options = ExecutionConfig() if config is None else config
    if not isinstance(options, ExecutionConfig):
        raise ValidationError("config must be ExecutionConfig")
    options = ExecutionConfig(
        options.max_workers, options.device_workers, options.fail_fast, options.timeout_seconds
    )
    if cancel_event is not None and not isinstance(cancel_event, Event):
        raise ValidationError("cancel_event must be threading.Event")
    tasks = TaskRegistry(registry.tasks)
    assigned, memory = _validate_placement(graph, tasks, placements, options)
    return _Runner(graph, tasks, assigned, memory, options, cancel_event).run()


def _validate_placement(
    graph: GraphSpec,
    registry: TaskRegistry,
    placements: Mapping[str, str],
    config: ExecutionConfig,
) -> tuple[dict[str, str], dict[str, float]]:
    nodes = graph.node_map
    devices = {device.name: device for device in graph.devices}
    if set(registry.tasks) != set(nodes):
        raise ValidationError("registry must name exactly the graph's nodes")
    if (
        not isinstance(placements, Mapping)
        or len(placements) != len(nodes)
        or set(placements) != set(nodes)
    ):
        raise ValidationError("placements must name exactly the graph's nodes")
    if set(config.device_workers) - set(devices):
        raise ValidationError("device_workers names an unknown graph device")
    assigned: dict[str, str] = {}
    memory = dict.fromkeys(sorted(devices), 0.0)
    for node_id in sorted(nodes):
        device = placements[node_id]
        if (
            not isinstance(device, str)
            or device not in devices
            or not nodes[node_id].can_run_on(devices[device])
        ):
            raise ValidationError(f"invalid device placement for node {node_id!r}")
        assigned[node_id] = device
        memory[device] += nodes[node_id].memory_mb
        if not math.isfinite(memory[device]) or memory[device] > devices[device].memory_mb + 1e-9:
            raise ValidationError(f"persistent memory admission exceeded on device {device!r}")
    return assigned, memory


@dataclass(frozen=True, slots=True)
class _Outcome:
    attempt: TaskAttempt
    value: object = None
    retryable: bool = False


def _invoke(
    definition: TaskDefinition, context: TaskContext, epoch: float, submitted_ms: float
) -> _Outcome:
    started = (time.monotonic() - epoch) * 1000
    try:
        context.cancellation.raise_if_cancelled()
        value = definition.function(context)
        if inspect.isawaitable(value):
            if inspect.iscoroutine(value):
                value.close()
            raise TypeError(
                "task returned an awaitable; local execution requires synchronous results"
            )
    except TaskCancelled as error:
        return _Outcome(_attempt(context.attempt, "cancelled", submitted_ms, started, epoch, error))
    except Exception as error:
        return _Outcome(
            _attempt(context.attempt, "failed", submitted_ms, started, epoch, error),
            retryable=isinstance(error, definition.retry_on),
        )
    return _Outcome(_attempt(context.attempt, "succeeded", submitted_ms, started, epoch), value)


def _attempt(
    number: int,
    status: AttemptStatus,
    submitted: float,
    started: float,
    epoch: float,
    error: Exception | None = None,
) -> TaskAttempt:
    finished = (time.monotonic() - epoch) * 1000
    message = None
    if error is not None:
        try:
            message = str(error)[:MAX_TEXT_LENGTH]
        except Exception:
            message = "<exception could not be formatted>"
    return TaskAttempt(
        number,
        status,
        submitted,
        started,
        finished,
        type(error).__name__[:256] if error is not None else None,
        message,
    )


class _Runner:
    def __init__(
        self,
        graph: GraphSpec,
        registry: TaskRegistry,
        assigned: dict[str, str],
        memory: dict[str, float],
        config: ExecutionConfig,
        external: Event | None,
    ) -> None:
        self.graph, self.registry, self.assigned, self.memory, self.config = (
            graph,
            registry,
            assigned,
            memory,
            config,
        )
        self.order = topological_order(graph)
        self.incoming = predecessor_edges(graph)
        self.outgoing = successor_edges(graph)
        self.remaining = {node: len(self.incoming[node]) for node in self.order}
        self.ready: dict[str, list[str]] = {device: [] for device in memory}
        for node in self.order:
            if self.remaining[node] == 0:
                heapq.heappush(self.ready[assigned[node]], node)
        self.attempts: dict[str, list[TaskAttempt]] = {node: [] for node in self.order}
        self.completed: dict[str, TaskExecution] = {}
        self.outputs: dict[str, object] = {}
        self.running: dict[Future[_Outcome], str] = {}
        self.delayed: list[tuple[float, str]] = []
        self.active = dict.fromkeys(memory, 0)
        self.peak = dict(self.active)
        self.internal = Event()
        self.token = CancellationToken(self.internal, external)
        self.stop_reason: str | None = None
        self.epoch = time.monotonic()

    def run(self) -> ExecutionResult:
        pool = ThreadPoolExecutor(
            max_workers=self.config.max_workers, thread_name_prefix="graph-sail"
        )
        try:
            while len(self.completed) < len(self.order):
                self._check_stop()
                now = time.monotonic()
                while self.delayed and self.delayed[0][0] <= now:
                    _, node = heapq.heappop(self.delayed)
                    heapq.heappush(self.ready[self.assigned[node]], node)
                self._submit_ready(pool)
                if self.running:
                    finished, _ = wait(self.running, timeout=0.02, return_when=FIRST_COMPLETED)
                    # A task can observe the external event before this thread.
                    # Record the stop before propagating its completed outcome.
                    self._check_stop()
                    for future in sorted(finished, key=lambda item: self.running[item]):
                        node = self.running.pop(future)
                        self.active[self.assigned[node]] -= 1
                        self._finish(node, future.result())
                elif self.delayed:
                    self.internal.wait(min(0.02, max(0, self.delayed[0][0] - time.monotonic())))
        except BaseException:
            self.internal.set()
            raise
        finally:
            pool.shutdown(wait=True, cancel_futures=True)
        statuses = {task.status for task in self.completed.values()}
        status: Literal["succeeded", "failed", "cancelled"] = "succeeded"
        if "failed" in statuses:
            status = "failed"
        elif self.stop_reason or "cancelled" in statuses:
            status = "cancelled"
        return ExecutionResult(
            self.graph.name,
            status,
            (time.monotonic() - self.epoch) * 1000,
            tuple(self.completed[node] for node in self.order),
            MappingProxyType(
                {node: self.outputs[node] for node in self.order if node in self.outputs}
            ),
            self.stop_reason,
            MappingProxyType(dict(self.peak)),
            MappingProxyType(dict(self.memory)),
        )

    def _check_stop(self) -> None:
        if self.stop_reason is None and self.token.cancelled:
            self._stop("external_cancellation")
        timeout = self.config.timeout_seconds
        if (
            self.stop_reason is None
            and timeout is not None
            and time.monotonic() - self.epoch >= timeout
        ):
            self._stop("timeout")

    def _stop(self, reason: str) -> None:
        self.stop_reason = reason
        self.internal.set()
        running = set(self.running.values())
        for node in self.order:
            if node not in self.completed and node not in running:
                self._record(node, "cancelled", reason)
        for queue in self.ready.values():
            queue.clear()
        self.delayed.clear()

    def _submit_ready(self, pool: ThreadPoolExecutor) -> None:
        while len(self.running) < self.config.max_workers:
            self._check_stop()
            if self.stop_reason:
                return
            candidates = [
                queue[0]
                for device, queue in self.ready.items()
                if queue and self.active[device] < self.config.device_workers.get(device, 1)
            ]
            if not candidates:
                return
            node = min(candidates)
            device = self.assigned[node]
            heapq.heappop(self.ready[device])
            context = TaskContext(
                node,
                device,
                len(self.attempts[node]) + 1,
                MappingProxyType(
                    {edge.source: self.outputs[edge.source] for edge in self.incoming[node]}
                ),
                self.token,
            )
            future = pool.submit(
                _invoke,
                self.registry.definition(node),
                context,
                self.epoch,
                (time.monotonic() - self.epoch) * 1000,
            )
            self.running[future] = node
            self.active[device] += 1
            self.peak[device] = max(self.peak[device], self.active[device])

    def _finish(self, node: str, outcome: _Outcome) -> None:
        self.attempts[node].append(outcome.attempt)
        if outcome.attempt.status == "succeeded":
            self.outputs[node] = outcome.value
            self._record(node, "succeeded", "returned")
            if self.stop_reason is None:
                for edge in self.outgoing[node]:
                    if edge.target not in self.completed:
                        self.remaining[edge.target] -= 1
                        if self.remaining[edge.target] == 0:
                            heapq.heappush(self.ready[self.assigned[edge.target]], edge.target)
            return
        definition = self.registry.definition(node)
        if (
            outcome.attempt.status == "failed"
            and outcome.retryable
            and len(self.attempts[node]) <= definition.max_retries
            and self.stop_reason is None
            and not self.token.cancelled
        ):
            heapq.heappush(self.delayed, (time.monotonic() + definition.retry_delay_seconds, node))
            return
        self._record(
            node,
            outcome.attempt.status,
            "task_cancelled" if outcome.attempt.status == "cancelled" else "task_failed",
        )
        descendants = deque(edge.target for edge in self.outgoing[node])
        while descendants:
            child = descendants.popleft()
            if child not in self.completed:
                cause = (
                    "dependency_cancelled"
                    if outcome.attempt.status == "cancelled"
                    else "dependency_failed"
                )
                self._record(child, "skipped", f"{cause}:{node}")
                descendants.extend(edge.target for edge in self.outgoing[child])
        if self.config.fail_fast and self.stop_reason is None:
            self._stop("fail_fast")

    def _record(self, node: str, status: TaskStatus, reason: str) -> None:
        self.completed[node] = TaskExecution(
            node, self.assigned[node], status, tuple(self.attempts[node]), reason
        )


def _name(value: object, label: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > MAX_TEXT_LENGTH
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ValidationError(f"{label} must be a non-empty bounded text identifier")


def _count(value: object, name: str, *, minimum: int, maximum: int) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValidationError(f"{name} must be an integer between {minimum} and {maximum}")


def _duration(value: object, name: str, *, minimum: float, maximum: float) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not minimum <= value <= maximum
    ):
        raise ValidationError(f"{name} must be finite and between {minimum} and {maximum}")


__all__ = [
    "CancellationToken",
    "ExecutionConfig",
    "ExecutionResult",
    "TaskAttempt",
    "TaskCallable",
    "TaskCancelled",
    "TaskContext",
    "TaskDefinition",
    "TaskExecution",
    "TaskRegistry",
    "TaskStatus",
    "execute_graph",
]
