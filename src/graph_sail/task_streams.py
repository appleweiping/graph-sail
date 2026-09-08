"""Owned native-generator tasks with ordered, explicitly bounded backpressure."""

from __future__ import annotations

import inspect
import time
from collections import deque
from collections.abc import Callable, Generator, Iterator
from dataclasses import dataclass
from threading import Condition, Event, Thread, current_thread
from types import TracebackType
from typing import Literal, cast

from graph_sail.actors import _preserve_failure
from graph_sail.errors import ValidationError
from graph_sail.execution import CancellationSignal, CancellationToken, TaskCancelled, _count
from graph_sail.handles import _timeout

StreamStatus = Literal["succeeded", "limited", "cancelled"]


@dataclass(frozen=True, slots=True)
class TaskStreamConfig:
    """Item-count bounds, not callback/value memory or execution-time limits."""

    max_buffered: int = 16
    max_yields: int = 100_000

    def __post_init__(self) -> None:
        _count(self.max_buffered, "max_buffered", minimum=1, maximum=1024)
        _count(self.max_yields, "max_yields", minimum=1, maximum=10_000_000)


@dataclass(frozen=True, slots=True)
class TaskStreamContext:
    """Read-only cooperative cancellation for trusted producer code."""

    cancellation: CancellationSignal


@dataclass(frozen=True, slots=True)
class TaskStreamItem:
    """An accepted zero-based yield and a borrowed, unmodified application value."""

    sequence: int
    value: object

    def __post_init__(self) -> None:
        _count(self.sequence, "stream sequence", minimum=0, maximum=9_999_999)


@dataclass(frozen=True, slots=True)
class TaskStreamResult:
    """Settled producer metadata; accepted yields need not have been consumed."""

    status: StreamStatus
    produced: int

    def __post_init__(self) -> None:
        if type(self.status) is not str or self.status not in ("succeeded", "limited", "cancelled"):
            raise ValidationError("invalid stream completion status")
        _count(self.produced, "produced", minimum=0, maximum=10_000_000)


StreamCallable = Callable[[TaskStreamContext], Generator[object, None, object]]


class TaskStream(Iterator[TaskStreamItem]):
    """One consuming mailbox and one owned producer. Use start_task_stream.

    Concurrent readers compete for items, never receive a broadcast. Explicit
    close joins the producer, discards queued references without closing values,
    and does not raise a stored task failure; completion/next observe failures.
    """

    def __init__(self, function: StreamCallable, config: TaskStreamConfig) -> None:
        self._condition = Condition()
        self._stop = Event()
        self._config = config
        self._queue: deque[TaskStreamItem] = deque()
        self._finished = False
        self._closed = False
        self._discard = False
        self._error: BaseException | None = None
        self._error_traceback: TracebackType | None = None
        self._result: TaskStreamResult | None = None
        self._thread = Thread(
            target=self._drive, args=(function,), name="graph-sail-stream", daemon=False
        )

    def _not_producer(self) -> None:
        if current_thread() is self._thread:
            raise RuntimeError("producer cannot wait on or close its own stream")

    def __iter__(self) -> TaskStream:
        return self

    def __next__(self) -> TaskStreamItem:
        return self.next()

    def next(self, timeout: float | None = None) -> TaskStreamItem:
        """Consume an item or observe terminal failure/EOF; timeout never cancels."""
        _timeout(timeout)
        self._not_producer()
        with self._condition:
            if not self._condition.wait_for(
                lambda: bool(self._queue) or self._finished or self._discard, timeout
            ):
                raise TimeoutError("stream item is not ready")
            if self._discard:
                raise StopIteration
            if self._queue:
                item = self._queue.popleft()
                self._condition.notify_all()
                return item
            if self._error is not None:
                raise self._error.with_traceback(self._error_traceback)
            raise StopIteration

    def wait_ready(self, timeout: float | None = None) -> bool:
        """Non-consuming wait for a queued item, settled termination, or close."""
        _timeout(timeout)
        self._not_producer()
        with self._condition:
            return self._condition.wait_for(
                lambda: bool(self._queue) or self._finished or self._discard, timeout
            )

    def done(self) -> bool:
        """True only after generator execution and owned cleanup have settled."""
        with self._condition:
            return self._finished

    @property
    def closed(self) -> bool:
        with self._condition:
            return self._closed

    def completion(self, timeout: float | None = None) -> TaskStreamResult:
        """Wait for cleanup, not consumption. Backpressure can delay completion."""
        _timeout(timeout)
        self._not_producer()
        with self._condition:
            if not self._condition.wait_for(lambda: self._finished, timeout):
                raise TimeoutError("stream completion is not ready")
            if self._error is not None:
                raise self._error.with_traceback(self._error_traceback)
            if self._result is None:
                raise RuntimeError("finished stream has no result")
            return self._result

    def cancel(self) -> bool:
        """Request cooperative stopping, preserving already accepted items."""
        with self._condition:
            if self._finished or self._stop.is_set():
                return False
            self._stop.set()
            self._condition.notify_all()
            return True

    def close(self, timeout: float | None = None) -> None:
        """Discard queued values and join; timeout retains ownership for retry."""
        _timeout(timeout)
        self._not_producer()
        # Allocate the replacement first; do not dispose arbitrary user objects
        # while holding the coordination lock (their destructors can run code).
        replacement: deque[TaskStreamItem] = deque()
        with self._condition:
            abandoned, self._queue = self._queue, replacement
            self._discard = True
            self._stop.set()
            self._condition.notify_all()
        del abandoned
        self._join(timeout)

    def _join(self, timeout: float | None) -> None:
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            if not self._condition.wait_for(lambda: self._finished, timeout):
                raise TimeoutError("stream still owns a live producer; retry close")
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        self._thread.join(remaining)
        if self._thread.is_alive():
            raise TimeoutError("stream still owns a live producer; retry close")
        with self._condition:
            self._closed = True

    def __enter__(self) -> TaskStream:
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

    def _start(self) -> TaskStream:
        try:
            self._thread.start()
        except BaseException as error:
            self.cancel()
            if self._thread.ident is not None:
                try:
                    self._join(None)
                except BaseException as cleanup:
                    primary = _preserve_failure(error, cleanup)
                    if primary is not error:
                        raise primary from error
            raise
        return self

    def _produce(self, generator: Generator[object, None, object]) -> TaskStreamResult:
        produced = 0
        status: StreamStatus = "cancelled"
        while produced < self._config.max_yields:
            with self._condition:
                self._condition.wait_for(
                    lambda: self._stop.is_set() or len(self._queue) < self._config.max_buffered
                )
                if self._stop.is_set():
                    break
            try:
                value = next(generator)
            except StopIteration:
                status = "cancelled" if self._stop.is_set() else "succeeded"
                break
            except TaskCancelled:
                if not self._stop.is_set():
                    raise
                break
            item = TaskStreamItem(produced, value)
            with self._condition:
                if self._stop.is_set():
                    break
                self._queue.append(item)
                produced += 1
                self._condition.notify_all()
            # Drop this loop's references before waiting for the next free slot.
            del value, item
        else:
            status = "limited"
        return TaskStreamResult(status, produced)

    def _drive(self, function: StreamCallable) -> None:
        result: TaskStreamResult | None = None
        problem: BaseException | None = None
        try:
            result = self._run(function)
        except BaseException as error:
            problem = error
        with self._condition:
            self._error, self._result = problem, result
            self._error_traceback = problem.__traceback__ if problem is not None else None
            self._finished = True
            self._condition.notify_all()

    def _run(self, function: StreamCallable) -> TaskStreamResult:
        owned: Generator[object, None, object] | None = None
        problem: BaseException | None = None
        result: TaskStreamResult | None = None
        try:
            generator = function(TaskStreamContext(CancellationToken(self._stop)))
            if not inspect.isgenerator(generator) or inspect.getgeneratorstate(generator) != (
                inspect.GEN_CREATED
            ):
                raise ValidationError("task stream requires a fresh native synchronous generator")
            owned = cast(Generator[object, None, object], generator)
            result = self._produce(owned)
        except BaseException as error:
            problem = error
        if owned is not None:
            try:
                owned.close()
            except BaseException as cleanup:
                problem = cleanup if problem is None else _preserve_failure(problem, cleanup)
        if problem is not None:
            raise problem
        if result is None:
            raise RuntimeError("producer has no completion result")
        return result


def start_task_stream(
    function: StreamCallable, *, config: TaskStreamConfig | None = None
) -> TaskStream:
    """Preflight a native generator function, then start its owned local producer.

    No iterator is borrowed from the caller. Arbitrary iterators, coroutine and
    async-generator functions are rejected before any thread is started.
    """
    if not inspect.isgeneratorfunction(function):
        raise ValidationError("stream function must be a native synchronous generator function")
    if config is not None and type(config) is not TaskStreamConfig:
        raise ValidationError("stream config must be TaskStreamConfig")
    options = config if config is not None else TaskStreamConfig()
    options.__post_init__()
    return TaskStream(function, options)._start()


__all__ = [
    "TaskStream",
    "TaskStreamConfig",
    "TaskStreamContext",
    "TaskStreamItem",
    "TaskStreamResult",
    "start_task_stream",
]
