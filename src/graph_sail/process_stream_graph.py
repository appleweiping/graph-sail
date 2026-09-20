"""Run one local spawned-process DAG for each bounded native stream yield.

The source is local and thread-owned. Each accepted item owns one independent
process-graph invocation through the existing shared DAG scheduler.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from functools import partial
from importlib.machinery import ModuleSpec
from threading import Lock
from types import BuiltinFunctionType, FunctionType, MethodType, ModuleType, TracebackType
from typing import Protocol, cast

from graph_sail.actors import ActorSerializationError, _preserve_failure
from graph_sail.errors import GraphSailError, ValidationError
from graph_sail.execution import (
    CancellationToken,
    ExecutionConfig,
    TaskContext,
    TaskDefinition,
    TaskRegistry,
)
from graph_sail.models import GraphSpec
from graph_sail.process_execution import (
    ProcessExecutionResult,
    ProcessTaskConfig,
    _prepare_process_invoker,
    execute_process_graph,
)
from graph_sail.stream_graph import StreamGraphExecutionError, _validate_template
from graph_sail.stream_map import (
    StreamMap,
    StreamMapConfig,
    StreamMapContext,
    StreamMapProducer,
    StreamMapResult,
    start_stream_map,
)
from graph_sail.task_streams import TaskStreamItem

_MAX_PROCESS_WORKERS = 16
_MAX_PROCESS_FRAMES = 64 * 1024 * 1024
_MAX_PENDING = 16
_MAX_TEMPLATE_NODES = 256


@dataclass(frozen=True, slots=True)
class _InputValueTask:
    """A module-level callable so spawn can serialize the per-item input root."""

    value: object

    def __call__(self, context: TaskContext) -> object:
        context.cancellation.raise_if_cancelled()
        return self.value


class ProcessStreamGraphExecutionError(StreamGraphExecutionError):
    """An item graph settled without a selected successful process output."""

    def __init__(self, sequence: int, process_result: ProcessExecutionResult) -> None:
        self.process_result = process_result
        super().__init__(sequence, process_result.execution)


class ProcessStreamGraphInfrastructureError(GraphSailError):
    """A process invocation could not establish a trustworthy item result."""

    def __init__(
        self,
        sequence: int,
        cause: Exception | None = None,
        *,
        process_result: ProcessExecutionResult | None = None,
    ) -> None:
        self.sequence = sequence
        self.cause = cause
        self.process_result = process_result
        kind = type(cause).__name__ if cause is not None else "failed driver attempt"
        super().__init__(f"stream item {sequence} process graph infrastructure failed: {kind}")


class _CleanupOwner(Protocol):
    @property
    def closed(self) -> bool: ...

    def close(self) -> None: ...


class _CleanupRegistry:
    """Keep failed child cleanup owners until explicit retry settles them."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._pending: list[_CleanupOwner] = []

    def retain_from(self, error: BaseException) -> None:
        owner = getattr(error, "process_graph_cleanup", None)
        if owner is not None and not owner.closed:
            with self._lock:
                self._pending.append(cast(_CleanupOwner, owner))

    @property
    def closed(self) -> bool:
        with self._lock:
            return all(owner.closed for owner in self._pending)

    def close(self) -> None:
        with self._lock:
            pending = tuple(self._pending)
        problem: BaseException | None = None
        for owner in pending:
            if owner.closed:
                continue
            try:
                owner.close()
            except BaseException as error:
                problem = error if problem is None else _preserve_failure(problem, error)
        with self._lock:
            self._pending = [owner for owner in self._pending if not owner.closed]
        if problem is not None:
            raise problem


class ProcessStreamGraph(Iterator[TaskStreamItem]):
    """Compose stream ownership with retryable failed child cleanup ownership."""

    def __init__(self, stream: StreamMap, cleanup: _CleanupRegistry) -> None:
        self._stream = stream
        self._cleanup = cleanup
        self._close_lock = Lock()

    def __iter__(self) -> ProcessStreamGraph:
        return self

    def __next__(self) -> TaskStreamItem:
        return self.next()

    def next(self, timeout: float | None = None) -> TaskStreamItem:
        return self._stream.next(timeout)

    def completion(self, timeout: float | None = None) -> StreamMapResult:
        return self._stream.completion(timeout)

    def done(self) -> bool:
        return self._stream.done()

    @property
    def closed(self) -> bool:
        return self._stream.closed and self._cleanup.closed

    def cancel(self) -> bool:
        return self._stream.cancel()

    def close(self, timeout: float | None = None) -> None:
        with self._close_lock:
            problem: BaseException | None = None
            try:
                self._stream.close(timeout)
            except BaseException as error:
                problem = error
            try:
                self._cleanup.close()
            except BaseException as error:
                problem = error if problem is None else _preserve_failure(problem, error)
            if problem is not None:
                raise problem

    def __enter__(self) -> ProcessStreamGraph:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            self.close()
        except BaseException as cleanup:
            if exc is None:
                raise
            primary = _preserve_failure(exc, cleanup)
            if primary is cleanup:
                raise
            raise primary from cleanup


def _driver_failure(result: ProcessExecutionResult) -> bool:
    """The reused process executor records transport failures as driver attempts."""
    failed = {
        (task.node_id, attempt.attempt)
        for task in result.execution.tasks
        for attempt in task.attempts
        if attempt.status == "failed"
    }
    return any(
        attempt.timing_source == "driver" and (attempt.node_id, attempt.attempt) in failed
        for attempt in result.attempts
    )


def _reject_known_unimportable_module(module_name: str) -> None:
    """Inspect only current import metadata; never import or execute application code."""
    if module_name in {"__main__", "__mp_main__", "builtins"}:
        # Spawn has special main-module handling; its success needs runtime proof.
        return
    parts = module_name.split(".")
    for length in range(1, len(parts) + 1):
        name = ".".join(parts[:length])
        module = sys.modules.get(name)
        if not isinstance(module, ModuleType):
            continue  # Unknown: the later pickle/spawn boundary remains authoritative.
        metadata = vars(module)
        spec = metadata.get("__spec__")
        has_file = metadata.get("__file__") is not None
        has_path = metadata.get("__path__") is not None
        if (spec is None and not has_file and not has_path) or (
            isinstance(spec, ModuleSpec)
            and spec.loader is None
            and spec.submodule_search_locations is None
            and not has_file
        ):
            raise ActorSerializationError(f"task module {name!r} has no spawn-importable origin")


def _preflight_known_spawn_origins(function: object) -> None:
    """Reject known driver-only callable owners, not claim child importability."""
    while isinstance(function, partial):
        function = function.func
    if isinstance(function, MethodType):
        _preflight_known_spawn_origins(function.__func__)
        function = function.__self__
    owner = (
        function
        if isinstance(function, (FunctionType, BuiltinFunctionType, type))
        else type(function)
    )
    _reject_known_unimportable_module(owner.__module__)


def start_process_stream_graph(
    producer: StreamMapProducer,
    graph: GraphSpec,
    registry: TaskRegistry,
    placements: Mapping[str, str],
    *,
    input_node: str,
    output_node: str,
    config: StreamMapConfig | None = None,
    execution_config: ExecutionConfig | None = None,
    process_config: ProcessTaskConfig | None = None,
) -> ProcessStreamGraph:
    """Return an owned stream of per-yield selected process-DAG outputs.

    The local native producer is never moved to a subprocess. Every graph
    invocation uses actual local spawn workers, but no process pool or resource
    reservation is shared with a different stream or execution.
    """
    if config is not None and type(config) is not StreamMapConfig:
        raise ValidationError("config must be StreamMapConfig")
    stream_options = StreamMapConfig() if config is None else config
    stream_options.__post_init__()
    tasks, assigned, graph_options = _validate_template(
        graph, registry, placements, input_node, output_node, stream_options, execution_config
    )
    if len(graph.nodes) > _MAX_TEMPLATE_NODES:
        raise ValidationError("process stream graph template exceeds 256 nodes")
    if stream_options.max_pending > _MAX_PENDING:
        raise ValidationError("process stream graph max_pending exceeds 16")

    # These bounds are conservative for independent per-item process pools.
    total_workers = stream_options.max_workers * graph_options.max_workers
    if total_workers > _MAX_PROCESS_WORKERS:
        raise ValidationError("aggregate process stream graph worker budget exceeds 16")
    preflight = TaskRegistry({**tasks.tasks, input_node: TaskDefinition(_InputValueTask(None))})
    process_options = _prepare_process_invoker(preflight, graph_options, process_config).config
    for node in preflight.tasks:
        _preflight_known_spawn_origins(preflight.definition(node).function)
    if total_workers * process_options.max_message_bytes > _MAX_PROCESS_FRAMES:
        raise ValidationError("aggregate process stream graph message budget exceeds 64 MiB")
    cleanup = _CleanupRegistry()

    def run_item(context: StreamMapContext, value: object) -> object:
        # The source value was already snapshotted by StreamMap. This private
        # top-level callable is pickled again for its child process invocation.
        if not isinstance(context.cancellation, CancellationToken):
            raise RuntimeError("stream map did not supply its cancellation token")
        invocation = TaskRegistry(
            {**tasks.tasks, input_node: TaskDefinition(_InputValueTask(value))}
        )
        try:
            result = execute_process_graph(
                graph,
                invocation,
                assigned,
                config=graph_options,
                process_config=process_options,
                cancel_event=context.cancellation._internal,
            )
        except BaseException as error:
            cleanup.retain_from(error)
            if not isinstance(error, Exception):
                raise
            raise ProcessStreamGraphInfrastructureError(context.sequence, error) from error
        if _driver_failure(result):
            raise ProcessStreamGraphInfrastructureError(context.sequence, process_result=result)
        if result.execution.status != "succeeded":
            raise ProcessStreamGraphExecutionError(context.sequence, result)
        try:
            return result.execution.outputs[output_node]
        except KeyError as error:
            raise ProcessStreamGraphInfrastructureError(context.sequence, error) from error

    return ProcessStreamGraph(start_stream_map(producer, run_item, config=stream_options), cleanup)


__all__ = [
    "ProcessStreamGraph",
    "ProcessStreamGraphExecutionError",
    "ProcessStreamGraphInfrastructureError",
    "start_process_stream_graph",
]
