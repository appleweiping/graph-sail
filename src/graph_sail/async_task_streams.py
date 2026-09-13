"""Explicitly owned native async-generator producers on a trusted stdlib loop.

One lazy Task bypasses the application's task factory intentionally. All public
operations are owner-loop affine; notification waits never await that Task.
"""

from __future__ import annotations

import asyncio
import inspect
from asyncio import Task as _Task
from collections import deque
from collections.abc import AsyncGenerator, Callable, Coroutine
from contextvars import Context, copy_context
from dataclasses import dataclass
from types import AsyncGeneratorType, FunctionType, TracebackType
from typing import Any, Literal, cast

from graph_sail._completion import _await_snapshot, _CompletionHub, _Snapshot
from graph_sail.actors import _preserve_failure
from graph_sail.errors import GraphSailError, ValidationError
from graph_sail.execution import TaskCancelled
from graph_sail.handles import _timeout
from graph_sail.task_streams import (
    StreamStatus,
    TaskStreamConfig,
    TaskStreamContext,
    TaskStreamItem,
    TaskStreamResult,
)

AsyncStreamCallable = Callable[[TaskStreamContext], AsyncGenerator[object, None]]
_OwnershipPhase = Literal["startup", "cleanup"]
_EOF = object()
_ACTIVE_OWNERS: set[AsyncTaskStream] = set()


class AsyncTaskStreamOwnershipError(GraphSailError):
    """Fresh ordinary failure with an explicit retained native stream owner."""

    def __init__(self, stream: AsyncTaskStream, phase: _OwnershipPhase) -> None:
        super().__init__("native async stream ownership needs attention; inspect stream and cause")
        self.stream = stream
        self.phase = phase


class AsyncTaskStreamOwnershipControlError(BaseException):
    """Fresh ownership control, deliberately outside the Exception hierarchy."""

    def __init__(self, stream: AsyncTaskStream, phase: _OwnershipPhase) -> None:
        super().__init__("native async stream ownership raised a control; inspect stream and cause")
        self.stream = stream
        self.phase = phase


@dataclass(frozen=True, slots=True)
class _StopToken:
    owner: AsyncTaskStream

    @property
    def cancelled(self) -> bool:
        return self.owner._stop

    def raise_if_cancelled(self) -> None:
        if self.cancelled:
            raise TaskCancelled("native async stream cancellation requested")


def _validate_function(function: AsyncStreamCallable) -> None:
    # inspect alone also accepts wrappers/partials on some supported versions.
    if type(function) is not FunctionType or not inspect.isasyncgenfunction(function):
        raise ValidationError("stream function must be an exact native async generator function")


def _next_count(produced: int) -> int:
    """Prepare the allocating integer increment before the acceptance commit."""
    return produced + 1


class AsyncTaskStream:
    """One loop-affine consuming mailbox and one explicitly owned producer Task.

    Construct through start_async_task_stream. cancel preserves the prefix;
    aclose discards it and acknowledges actual Task/native-generator settlement.
    Timeout or cancellation of a close wait leaves the owner available for retry.
    """

    def __init__(
        self,
        function: AsyncStreamCallable,
        config: TaskStreamConfig,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._loop = loop
        self._config = config
        self._queue: deque[TaskStreamItem] = deque()
        self._completion_hub = _CompletionHub()
        self._task: asyncio.Task[None] | None = None
        self._owned: AsyncGeneratorType[object, None] | None = None
        self._capacity: asyncio.Future[None] | None = None
        self._phase: Literal["startup", "production", "cleanup", "settled"] = "startup"
        self._stop = False
        self._discard = False
        self._entered = False
        self._start_committed = False
        self._aborted = False
        self._native_cancel_issued = False
        self._finished = False
        self._closed = False
        self._incomplete = False
        self._produced = 0
        self._error: BaseException | None = None
        self._result: TaskStreamResult | None = None
        self._cancel_marker = object()
        self._context = TaskStreamContext(_StopToken(self))
        self._task_context = copy_context()
        self._notification_context = Context()
        self._done_callback = self._on_done
        self._abort_callback = self._acknowledge_abort
        self._driver_coroutine: Coroutine[Any, Any, None] = self._drive(function)

    def _check_loop(self, *, waiting: bool = False) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError("native async stream requires its running owner loop") from None
        if loop is not self._loop:
            raise RuntimeError("native async stream requires its running owner loop")
        if waiting and asyncio.current_task() is self._task:
            raise RuntimeError("producer cannot wait on or close its own stream")

    def _raise_ownership(self, phase: _OwnershipPhase) -> None:
        if isinstance(self._error, Exception):
            raise AsyncTaskStreamOwnershipError(self, phase) from self._error
        raise AsyncTaskStreamOwnershipControlError(self, phase) from self._error

    def _record_failure(self, error: BaseException) -> None:
        self._error = error if self._error is None else _preserve_failure(self._error, error)

    def _start(self) -> AsyncTaskStream:
        try:
            _ACTIVE_OWNERS.add(self)
            # Intentional factory bypass; default Task construction is lazy on
            # supported versions, including when the loop factory is eager.
            self._task = _Task(
                self._driver_coroutine,
                loop=self._loop,
                context=self._task_context,
                name="graph-sail-native-async-stream",
            )
            self._task.add_done_callback(self._done_callback, context=self._notification_context)
            self._start_committed = True
        except BaseException as error:
            self._aborted = self._stop = True
            self._record_failure(error)
            try:
                # FIFO on the functioning stdlib loop: an adopted lazy driver
                # records itself and sees abort before this callback runs.
                self._loop.call_soon(self._abort_callback, context=self._notification_context)
            except BaseException as cleanup:
                self._record_failure(cleanup)
            self._raise_ownership("startup")
        return self

    def _acknowledge_abort(self) -> None:
        if self._finished:
            return
        if self._task is not None:
            if self._task.done():
                self._on_done(self._task)
            return
        try:
            self._driver_coroutine.close()
            self._result = TaskStreamResult("cancelled", 0)
        except BaseException as error:
            self._record_failure(error)
        self._settle()

    def _on_done(self, task: asyncio.Task[None]) -> None:
        if self._finished:
            return
        try:
            error = task.exception()
        except BaseException as error_from_task:
            self._record_failure(error_from_task)
        else:
            if error is not None:
                self._record_failure(error)
        self._settle()

    def _settle(self) -> None:
        self._incomplete = self._owned is not None and self._owned.ag_frame is not None
        if self._incomplete and self._error is None:
            self._error = RuntimeError("native async generator cleanup retained its frame")
        self._phase = "settled"
        self._finished = True
        _ACTIVE_OWNERS.discard(self)
        try:
            self._completion_hub.notify(terminal=True)
        except BaseException as error:
            # State is authoritative even when a failing runtime notification
            # cannot wake existing waiters. Do not fabricate a successful result.
            self._record_failure(error)

    async def _drive(self, function: AsyncStreamCallable) -> None:
        try:
            self._task = cast(asyncio.Task[None], asyncio.current_task())
            self._entered = True
            if self._stop or self._aborted or not self._start_committed:
                self._result = TaskStreamResult("cancelled", 0)
            else:
                self._phase = "production"
                # A caller may have replaced a native function's __code__ since
                # admission. Recheck before invoking it; never adopt its return
                # if it is no longer a native async-generator function.
                _validate_function(function)
                generator = function(self._context)
                if type(generator) is not AsyncGeneratorType:
                    raise ValidationError("stream requires a fresh native async generator")
                self._owned = generator
                self._result = await self._produce(generator)
        except BaseException as error:
            self._record_failure(error)
        finally:
            self._phase = "cleanup"
            if self._owned is not None:
                try:
                    await self._owned.aclose()
                except BaseException as cleanup:
                    self._record_failure(cleanup)

    def _owner_acknowledged(self, error: BaseException) -> bool:
        if isinstance(error, TaskCancelled):
            return self._stop
        return (
            type(error) is asyncio.CancelledError
            and len(error.args) == 1
            and error.args[0] is self._cancel_marker
        )

    async def _credit(self) -> None:
        while not self._stop and len(self._queue) >= self._config.max_buffered:
            future: asyncio.Future[None] = self._loop.create_future()
            self._capacity = future
            try:
                await future
            finally:
                if self._capacity is future:
                    self._capacity = None

    def _wake_capacity(self) -> None:
        future = self._capacity
        if future is not None and not future.done():
            future.set_result(None)

    async def _produce(self, generator: AsyncGenerator[object, None]) -> TaskStreamResult:
        status: StreamStatus = "cancelled"
        try:
            while self._produced < self._config.max_yields:
                await self._credit()
                if self._stop:
                    break
                try:
                    value = await anext(generator)
                except StopAsyncIteration:
                    status = "cancelled" if self._stop else "succeeded"
                    break
                item = TaskStreamItem(self._produced, value)
                next_count = _next_count(self._produced)
                if self._stop:
                    break
                self._queue.append(item)
                self._produced = next_count
                self._completion_hub.notify()
                del value, item
            else:
                status = "limited"
        except (TaskCancelled, asyncio.CancelledError) as error:
            if not self._owner_acknowledged(error):
                raise
        return TaskStreamResult(status, self._produced)

    def __aiter__(self) -> AsyncTaskStream:
        self._check_loop(waiting=True)
        return self

    async def __anext__(self) -> TaskStreamItem:
        return await self.next_async()

    def _next_snapshot(self) -> _Snapshot:
        if self._discard:
            return _Snapshot(True, _EOF)
        if self._queue:
            observed = _Snapshot(True, self._queue[0])
            # Resolve before consuming: scheduling failure cannot lose an item.
            # Standard callbacks defer execution, so the producer rechecks credit.
            self._wake_capacity()
            self._queue.popleft()
            return observed
        return _Snapshot(self._finished, _EOF, failure=self._error if self._finished else None)

    async def next_async(self, timeout: float | None = None) -> TaskStreamItem:
        """Consume once; caller cancellation is checked before any ready dequeue."""
        _timeout(timeout)
        self._check_loop(waiting=True)
        await asyncio.sleep(0)
        value = await _await_snapshot(self._completion_hub, self._next_snapshot, timeout)
        if value is _EOF:
            raise StopAsyncIteration
        return cast(TaskStreamItem, value)

    def _ready_snapshot(self) -> _Snapshot:
        ready = bool(self._queue) or self._finished or self._discard
        return _Snapshot(ready, ready)

    async def wait_ready_async(self, timeout: float | None = None) -> bool:
        """Observe readiness without consuming, cancelling or reserving an item."""
        _timeout(timeout)
        self._check_loop(waiting=True)
        return cast(
            bool,
            await _await_snapshot(
                self._completion_hub,
                self._ready_snapshot,
                timeout,
                partial=True,
            ),
        )

    def _completion_snapshot(self) -> _Snapshot:
        if self._finished and self._result is None and self._error is None:
            raise RuntimeError("finished native async stream has no result")
        return _Snapshot(self._finished, self._result, failure=self._error)

    async def completion_async(self, timeout: float | None = None) -> TaskStreamResult:
        """Wait for actual producer settlement without draining or closing."""
        _timeout(timeout)
        self._check_loop(waiting=True)
        return cast(
            TaskStreamResult,
            await _await_snapshot(
                self._completion_hub,
                self._completion_snapshot,
                timeout,
            ),
        )

    def cancel(self) -> bool:
        """Preserve accepted items and request stopping once, never recancel cleanup."""
        self._check_loop()
        if self._stop or self._phase not in ("startup", "production"):
            return False
        self._stop = True
        try:
            self._wake_capacity()
        finally:
            task = self._task
            if (
                self._entered
                and task is not None
                and asyncio.current_task() is not task
                and not self._native_cancel_issued
            ):
                self._native_cancel_issued = True
                task.cancel(self._cancel_marker)
        return True

    def _close_snapshot(self) -> _Snapshot:
        return _Snapshot(self._finished)

    async def aclose(self, timeout: float | None = None) -> None:
        """Discard and request stop; wait interruption retains ownership for retry."""
        _timeout(timeout)
        self._check_loop(waiting=True)
        await asyncio.sleep(0)
        if self._closed:
            return
        replacement: deque[TaskStreamItem] = deque()
        abandoned, self._queue = self._queue, replacement
        first_discard = not self._discard
        self._discard = True
        try:
            self.cancel()
        finally:
            self._stop = True
            try:
                if first_discard:
                    self._completion_hub.notify()
            finally:
                del abandoned
        await _await_snapshot(self._completion_hub, self._close_snapshot, timeout)
        if self._incomplete:
            self._raise_ownership("cleanup")
        self._closed = True

    def done(self) -> bool:
        """True only after actual Task settlement and terminal publication."""
        self._check_loop()
        return self._finished

    @property
    def closed(self) -> bool:
        self._check_loop()
        return self._closed

    @property
    def cleanup_incomplete(self) -> bool:
        self._check_loop()
        return self._finished and self._incomplete

    async def __aenter__(self) -> AsyncTaskStream:
        self._check_loop(waiting=True)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._check_loop(waiting=True)
        try:
            await self.aclose()
        except BaseException as cleanup:
            if exc is None:
                raise
            primary = _preserve_failure(exc, cleanup)
            if primary is cleanup:
                raise
            raise primary from cleanup


def start_async_task_stream(
    function: AsyncStreamCallable,
    *,
    config: TaskStreamConfig | None = None,
) -> AsyncTaskStream:
    """Start one native async generator Task inside its running owner loop.

    Exact native functions only; no borrowed iterators or custom task factories.
    Supported runtime providers are functioning standard-library loops and Tasks.
    """
    _validate_function(function)
    if config is not None and type(config) is not TaskStreamConfig:
        raise ValidationError("stream config must be TaskStreamConfig")
    options = config if config is not None else TaskStreamConfig()
    options.__post_init__()
    loop = asyncio.get_running_loop()
    return AsyncTaskStream(function, options, loop)._start()


__all__ = [
    "AsyncTaskStream",
    "AsyncTaskStreamOwnershipControlError",
    "AsyncTaskStreamOwnershipError",
    "start_async_task_stream",
]
