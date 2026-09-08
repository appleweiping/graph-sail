"""Owned nonblocking execution and selective terminal results for local DAGs.

The same scheduler drives blocking and handle APIs. Handles are local thread-safe
coordination objects, not portable object-store references or durable jobs.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from threading import Condition, Event, Thread, current_thread
from types import TracebackType
from typing import Generic, TypeVar

from graph_sail.actors import _preserve_failure
from graph_sail.errors import GraphSailError, ValidationError
from graph_sail.execution import (
    ExecutionConfig,
    ExecutionResult,
    TaskExecution,
    TaskRegistry,
    _count,
    _duration,
    _prepare_execution,
    _Runner,
)
from graph_sail.models import GraphSpec
from graph_sail.process_execution import (
    ProcessExecutionResult,
    ProcessTaskConfig,
    _prepare_process_invoker,
    _run_process_runner,
)

_Result = TypeVar("_Result", ExecutionResult, ProcessExecutionResult)


class TaskNotSuccessful(GraphSailError):
    """A terminal node failed, was skipped, or acknowledged cancellation."""

    def __init__(self, execution: TaskExecution) -> None:
        self.execution = execution
        super().__init__(f"node {execution.node_id!r} is {execution.status}: {execution.reason}")


@dataclass(frozen=True, slots=True)
class WaitResult:
    """All observed ready/pending IDs in requested order, not completion order."""

    ready: tuple[str, ...]
    pending: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class NodeHandle(Generic[_Result]):
    """One node in an owned execution; successful values are borrowed objects."""

    _owner: ExecutionHandle[_Result]
    node_id: str

    def done(self) -> bool:
        """Whether terminal metadata was published, including non-success statuses."""
        with self._owner._condition:
            return self.node_id in self._owner._terminal

    def execution(self, timeout: float | None = None) -> TaskExecution:
        """Wait for immutable terminal metadata; a wait timeout does not cancel work."""
        return self._owner._node_result(self.node_id, timeout)[0]

    def result(self, timeout: float | None = None) -> object:
        """Return a successful value, or raise TaskNotSuccessful with its metadata."""
        execution, value = self._owner._node_result(self.node_id, timeout)
        if execution.status != "succeeded":
            raise TaskNotSuccessful(execution)
        return value


class ExecutionHandle(Generic[_Result]):
    """Own a scheduler controller until explicitly closed/joined.

    Use start_graph/start_process_graph, not this internal constructor. Calls may
    come from multiple application threads. Do not wait from a task for its own
    graph to finish. No destructor, daemon thread or implicit interpreter cleanup
    substitutes for close(); context exit requests cancellation and joins.
    """

    def __init__(self, _runner: _Runner, _run: Callable[[], _Result], _stop: Event) -> None:
        self._condition = Condition()
        self._terminal: dict[str, tuple[TaskExecution, object]] = {}
        self._nodes = tuple(_runner.order)
        self._node_set = frozenset(self._nodes)
        self._stop = _stop
        self._finished = False
        self._closed = False
        self._result: _Result | None = None
        self._error: BaseException | None = None
        _runner.terminal_observer = self._publish
        self._thread = Thread(
            target=self._drive, args=(_run,), name="graph-sail-controller", daemon=False
        )

    @property
    def nodes(self) -> tuple[str, ...]:
        """The admitted graph's bounded topological node order."""
        return self._nodes

    @property
    def closed(self) -> bool:
        """True only after an explicit close successfully joined the controller."""
        with self._condition:
            return self._closed

    def node(self, node_id: str) -> NodeHandle[_Result]:
        self._validate_node(node_id)
        return NodeHandle(self, node_id)

    def done(self) -> bool:
        """Whether backend work and its cleanup completed, successfully or otherwise."""
        with self._condition:
            return self._finished

    def cancel(self) -> bool:
        """Request whole-graph cancellation; return False if already requested/done."""
        with self._condition:
            if self._finished or self._stop.is_set():
                return False
            self._stop.set()
            return True

    def wait(
        self,
        node_ids: tuple[str, ...] | None = None,
        *,
        count: int = 1,
        timeout: float | None = None,
    ) -> WaitResult:
        """Wait for at least count selected terminals; timeout returns those available.

        Selection must be a bounded unique tuple of this graph's IDs. A count of
        zero is a snapshot. Failure, skip and cancellation are ready statuses.
        Waiting does not consume results, cancel work or infer a task's success.
        """
        selected = self._selection(node_ids)
        _count(count, "count", minimum=0, maximum=len(selected))
        _timeout(timeout)
        with self._condition:
            self._condition.wait_for(
                lambda: self._finished or sum(node in self._terminal for node in selected) >= count,
                timeout,
            )
            ready = tuple(node for node in selected if node in self._terminal)
            pending = tuple(node for node in selected if node not in self._terminal)
            if self._finished and len(ready) < count and self._error is not None:
                raise self._error
            return WaitResult(ready, pending)

    def result(self, timeout: float | None = None) -> _Result:
        """Wait for the final backend result after worker cleanup; timeout is wait-only.

        Application task failures are recorded in that result. Scheduler/control
        failures instead re-raise the original exception, as with a local Future.
        """
        _timeout(timeout)
        with self._condition:
            if not self._condition.wait_for(lambda: self._finished, timeout):
                raise TimeoutError("execution result is not ready")
            if self._error is not None:
                raise self._error
            if self._result is None:
                raise RuntimeError("finished execution has no result")
            return self._result

    def close(self, timeout: float | None = None) -> None:
        """Request cancellation and join; timeout retains ownership for a later retry.

        Cleanup does not raise a stored execution failure: use result() to observe
        it. A trusted non-cooperating thread task may prevent an unbounded close
        from returning. This does not force-stop thread callbacks.
        """
        _timeout(timeout)
        if current_thread() is self._thread:
            raise RuntimeError("controller cannot join itself")
        self.cancel()
        self._join(timeout)

    def _join(self, timeout: float | None) -> None:
        deadline = None if timeout is None else time.monotonic() + timeout
        # Backend completion is authoritative. In particular, Thread.is_alive()
        # alone must not certify worker cleanup after an interrupted native join.
        with self._condition:
            if not self._condition.wait_for(lambda: self._finished, timeout):
                raise TimeoutError("execution still owns a live controller; retry close")
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        self._thread.join(remaining)
        if self._thread.is_alive():
            raise TimeoutError("execution still owns a live controller; retry close")
        with self._condition:
            self._closed = True

    def __enter__(self) -> ExecutionHandle[_Result]:
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

    def _start(self) -> ExecutionHandle[_Result]:
        try:
            self._thread.start()
        except BaseException as error:
            self._stop.set()
            # Normal failed starts create no thread. If a custom Thread.start
            # started and then raised, retain cleanup responsibility here.
            if self._thread.ident is not None:
                try:
                    self._join(None)
                except BaseException as cleanup:
                    primary = _preserve_failure(error, cleanup)
                    if primary is not error:
                        raise primary from error
            raise
        return self

    def _drive(self, run: Callable[[], _Result]) -> None:
        try:
            result = run()
        except BaseException as error:
            with self._condition:
                self._error = error
                self._finished = True
                self._condition.notify_all()
        else:
            with self._condition:
                self._result = result
                self._finished = True
                self._condition.notify_all()

    def _publish(self, execution: TaskExecution, value: object) -> None:
        with self._condition:
            self._terminal[execution.node_id] = (execution, value)
            self._condition.notify_all()

    def _validate_node(self, node_id: str) -> None:
        if not isinstance(node_id, str) or node_id not in self._node_set:
            raise ValidationError("node ID must belong to this execution")

    def _selection(self, node_ids: tuple[str, ...] | None) -> tuple[str, ...]:
        if node_ids is None:
            return self._nodes
        if type(node_ids) is not tuple or len(node_ids) > len(self._nodes):
            raise ValidationError("node_ids must be a bounded tuple of unique graph IDs")
        for node in node_ids:
            self._validate_node(node)
        if len(set(node_ids)) != len(node_ids):
            raise ValidationError("node_ids must not contain duplicates")
        return node_ids

    def _node_result(self, node_id: str, timeout: float | None) -> tuple[TaskExecution, object]:
        self._validate_node(node_id)
        _timeout(timeout)
        with self._condition:
            if not self._condition.wait_for(
                lambda: node_id in self._terminal or self._finished, timeout
            ):
                raise TimeoutError(f"node {node_id!r} is not ready")
            if node_id in self._terminal:
                return self._terminal[node_id]
            if self._error is not None:
                raise self._error
            raise RuntimeError("finished execution has no terminal node result")


def _timeout(value: float | None) -> None:
    if value is not None:
        _duration(value, "wait timeout", minimum=0, maximum=86400)


def start_graph(
    graph: GraphSpec,
    registry: TaskRegistry,
    placements: Mapping[str, str],
    *,
    config: ExecutionConfig | None = None,
) -> ExecutionHandle[ExecutionResult]:
    """Preflight synchronously, then start the shared thread DAG scheduler.

    Return values are shared with live dependents and the final result; reading
    does not copy/freeze them. Use immutable values or application synchronization.
    """
    stop = Event()
    tasks, assigned, memory, options = _prepare_execution(graph, registry, placements, config, stop)
    runner = _Runner(graph, tasks, assigned, memory, options, stop)
    return ExecutionHandle(runner, runner.run, stop)._start()


def start_process_graph(
    graph: GraphSpec,
    registry: TaskRegistry,
    placements: Mapping[str, str],
    *,
    config: ExecutionConfig | None = None,
    process_config: ProcessTaskConfig | None = None,
) -> ExecutionHandle[ProcessExecutionResult]:
    """Preflight/serialize registrations synchronously; spawn workers lazily.

    Received values are parent-side objects also used for later dispatch. Early
    retrieval is borrowed, not an isolated copy: mutation can affect a dependent's
    not-yet-serialized arguments. Process worker cleanup precedes final result().
    """
    began = time.monotonic()
    stop = Event()
    tasks, assigned, memory, options = _prepare_execution(graph, registry, placements, config, stop)
    invoker = _prepare_process_invoker(tasks, options, process_config)
    runner = _Runner(graph, tasks, assigned, memory, options, stop, invocation=invoker)
    return ExecutionHandle(
        runner, lambda: _run_process_runner(runner, invoker, began), stop
    )._start()


__all__ = [
    "ExecutionHandle",
    "NodeHandle",
    "TaskNotSuccessful",
    "WaitResult",
    "start_graph",
    "start_process_graph",
]
