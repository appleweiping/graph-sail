"""Owned local generator processes using the existing TaskStream mailbox.

All functions, arguments and pickle reconstruction hooks are trusted local
Python code. Bounded serialized frames are not a decoded-heap or OS sandbox.
"""

from __future__ import annotations

import inspect
import os
import time
from collections.abc import Callable, Generator, Iterator, Mapping
from dataclasses import dataclass
from threading import Lock
from types import TracebackType
from typing import cast

from graph_sail.actors import (
    ActorCall,
    ActorConfig,
    ActorDiedError,
    ActorTimeoutError,
    ProcessActor,
    _CancellationConnection,
    _integer,
    _preserve_failure,
    _seconds,
    _StreamStartupCleanup,
)
from graph_sail.errors import ValidationError
from graph_sail.execution import CancellationSignal, TaskCancelled
from graph_sail.handles import _timeout
from graph_sail.process_execution import _PipeCancellation
from graph_sail.task_streams import (
    StreamCallable,
    StreamStatus,
    TaskStream,
    TaskStreamConfig,
    TaskStreamContext,
    TaskStreamItem,
    TaskStreamResult,
)

ProcessStreamCallable = Callable[..., Generator[object, None, object]]


@dataclass(frozen=True, slots=True)
class ProcessStreamConfig:
    """Private full-frame bounds and controlled wait budgets, not hard real time."""

    max_message_bytes: int = 1_048_576
    startup_timeout_seconds: float = 30.0
    advance_timeout_seconds: float = 30.0
    cancellation_grace_seconds: float = 1.0
    shutdown_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        _integer(self.max_message_bytes, "max_message_bytes", 16_777_216, 1024)
        _seconds(self.startup_timeout_seconds, "startup_timeout_seconds", 0.001)
        _seconds(self.advance_timeout_seconds, "advance_timeout_seconds", 0.001)
        _seconds(self.cancellation_grace_seconds, "cancellation_grace_seconds")
        _seconds(self.shutdown_timeout_seconds, "shutdown_timeout_seconds")


@dataclass(frozen=True, slots=True)
class ProcessStreamWorker:
    """Observed ownership state; generator_closed requires an explicit close ACK.

    termination_requested counts explicit backend terminate calls only; actor
    close can itself escalate. No field proves application finally side effects.
    """

    pid: int
    exitcode: int | None
    cancellation_requested: bool
    termination_requested: bool
    generator_closed: bool
    resources_closed: bool

    def __post_init__(self) -> None:
        _integer(self.pid, "worker pid", 2**63 - 1)
        if self.exitcode is not None and (
            type(self.exitcode) is not int or not -(2**63) <= self.exitcode < 2**63
        ):
            raise ValidationError("worker exitcode must be a bounded integer or None")
        for value in (
            self.cancellation_requested,
            self.termination_requested,
            self.generator_closed,
            self.resources_closed,
        ):
            if type(value) is not bool:
                raise ValidationError("worker lifecycle flags must be booleans")
        if self.resources_closed and self.exitcode is None:
            raise ValidationError("closed native worker requires an observed exitcode")


@dataclass(frozen=True, slots=True)
class ProcessStreamResult:
    """Successful mailbox completion plus independently observed native cleanup."""

    status: StreamStatus
    produced: int
    worker: ProcessStreamWorker

    def __post_init__(self) -> None:
        TaskStreamResult(self.status, self.produced)
        if type(self.worker) is not ProcessStreamWorker:
            raise ValidationError("worker must be ProcessStreamWorker")
        self.worker.__post_init__()
        if not self.worker.resources_closed:
            raise ValidationError("process completion requires closed native resources")
        if self.status != "cancelled" and not self.worker.generator_closed:
            raise ValidationError("successful or limited completion requires generator close ACK")


def _function(value: object) -> ProcessStreamCallable:
    if (
        not inspect.isgeneratorfunction(value)
        or not inspect.isfunction(value)
        or ("<" in value.__qualname__)
    ):
        raise ValidationError("process stream requires an importable native generator function")
    return cast(ProcessStreamCallable, value)


class _StreamWorker:
    """One fresh native generator, touched only on the actor's worker thread."""

    def __init__(
        self,
        receiver: _CancellationConnection,
        function: object,
        args: object,
        kwargs: object,
    ) -> None:
        producer = _function(function)
        if type(args) is not tuple or type(kwargs) is not dict:
            raise ValidationError("invalid internal stream arguments")
        checked = ProcessActor._arguments(args, kwargs)
        self._signal = _PipeCancellation(receiver)
        self._generator = producer(TaskStreamContext(self._signal), *args, **checked)
        # Native generator functions produce a fresh generator without entering
        # their bodies; caller-owned iterators and factories are never admitted.
        self._sequence = 0
        self._ended = False
        self._pid = os.getpid()

    def advance(self, sequence: int) -> tuple[str, int, int, object]:
        if type(sequence) is not int or sequence != self._sequence or self._ended:
            raise ActorDiedError("invalid stream advance sequence or terminal state")
        self._signal.raise_if_cancelled()
        try:
            value = next(self._generator)
        except StopIteration:
            self._ended = True
            return "eof", self._pid, sequence, None
        self._sequence += 1
        return "yield", self._pid, sequence, value

    def finish(self) -> tuple[str, int, None]:
        self._ended = True
        self._generator.close()
        if inspect.getgeneratorstate(self._generator) != inspect.GEN_CLOSED:
            raise ActorDiedError("generator close did not reach a closed state")
        return "closed", self._pid, None


class _StreamDriver:
    """Single producer-thread RPC owner; external threads may only signal EOF."""

    def __init__(
        self, actor: ProcessActor, sender: _CancellationConnection, config: ProcessStreamConfig
    ) -> None:
        if actor.pid is None:
            raise ActorDiedError("stream worker has no process identity")
        self._actor = actor
        self._sender = sender
        self._config = config
        self._pid = actor.pid
        self._lock = Lock()
        self._sender_closed = False
        self._actor_closed = False
        self._cancelled = False
        self._terminated = False
        self._generator_closed = False
        self._finish_attempted = False

    def snapshot(self) -> ProcessStreamWorker:
        with self._lock:
            return ProcessStreamWorker(
                self._pid,
                self._actor.exitcode,
                self._cancelled,
                self._terminated,
                self._generator_closed,
                self._sender_closed and self._actor_closed,
            )

    def signal(self) -> None:
        with self._lock:
            self._cancelled = True
            self._close_sender()

    def _close_sender(self) -> None:
        if not self._sender_closed:
            self._sender.close()
            self._sender_closed = True

    def _retire(self, *, force: bool) -> None:
        problem: BaseException | None = None
        try:
            with self._lock:
                self._close_sender()
        except BaseException as error:
            problem = error
        try:
            if not self._actor_closed:
                if force:
                    with self._lock:
                        self._terminated = True
                    self._actor.terminate()
                else:
                    self._actor.close(self._config.shutdown_timeout_seconds)
                with self._lock:
                    self._actor_closed = True
        except BaseException as cleanup:
            problem = cleanup if problem is None else _preserve_failure(problem, cleanup)
        if problem is not None:
            raise problem

    def _wait(self, call: ActorCall, signal: CancellationSignal) -> object:
        deadline = time.monotonic() + self._config.advance_timeout_seconds
        stopping: BaseException | None = None
        grace_end = 0.0
        while True:
            now = time.monotonic()
            if stopping is None:
                if signal.cancelled:
                    stopping = TaskCancelled("process stream cancellation requested")
                elif now >= deadline:
                    stopping = ActorTimeoutError("process stream advance budget expired")
                if stopping is not None:
                    grace_end = now + self._config.cancellation_grace_seconds
                    try:
                        self.signal()
                    except BaseException as cleanup:
                        primary = _preserve_failure(stopping, cleanup)
                        raise primary from (stopping if primary is cleanup else cleanup)
            if stopping is not None and now >= grace_end:
                try:
                    self._retire(force=True)
                except BaseException as cleanup:
                    primary = _preserve_failure(stopping, cleanup)
                    raise primary from (stopping if primary is cleanup else cleanup)
                raise stopping
            try:
                result = call.result(
                    min(0.02, max(0.0, (grace_end if stopping else deadline) - now))
                )
            except TimeoutError:
                continue
            except BaseException as error:
                if stopping is not None:
                    primary = _preserve_failure(stopping, error)
                    raise primary from (stopping if primary is error else error)
                if isinstance(error, Exception) and (
                    signal.cancelled or time.monotonic() >= deadline
                ):
                    # Failure delivery can race the same stop boundary as a
                    # success response. Re-enter the loop to latch it before
                    # inspecting the already-settled call again; no RPC retry.
                    continue
                raise
            # A response racing with cancellation cannot turn it into success.
            if stopping is not None:
                raise stopping
            if signal.cancelled or time.monotonic() >= deadline:
                continue
            return result

    def proxy(self, context: TaskStreamContext) -> Generator[object, None, object]:
        sequence = 0
        while True:
            response = self._wait(
                self._actor.submit("advance", args=(sequence,)),
                context.cancellation,
            )
            if (
                type(response) is not tuple
                or len(response) != 4
                or type(response[0]) is not str
                or response[0] not in ("yield", "eof")
                or type(response[1]) is not int
                or response[1] != self._pid
                or type(response[2]) is not int
                or response[2] != sequence
                or (response[0] == "eof" and response[3] is not None)
            ):
                raise ActorDiedError("invalid process stream response binding")
            if response[0] == "eof":
                return None
            sequence += 1
            yield response[3]
            del response

    def finish(self) -> None:
        if self.snapshot().resources_closed:
            return
        problem: BaseException | None = None
        if not self._finish_attempted and not self._actor_closed:
            self._finish_attempted = True
            try:
                response = self._actor.submit("finish").result(
                    self._config.shutdown_timeout_seconds
                )
                if (
                    type(response) is not tuple
                    or len(response) != 3
                    or type(response[0]) is not str
                    or response[0] != "closed"
                    or type(response[1]) is not int
                    or response[1] != self._pid
                    or response[2] is not None
                ):
                    raise ActorDiedError("invalid process stream close acknowledgement")
                with self._lock:
                    self._generator_closed = True
            except BaseException as error:
                problem = error
        try:
            self._retire(
                force=problem is not None or (self._finish_attempted and not self._generator_closed)
            )
        except BaseException as cleanup:
            problem = cleanup if problem is None else _preserve_failure(problem, cleanup)
        if problem is not None:
            raise problem


class _ProcessMailbox(TaskStream):
    """Keep the sole mailbox state machine; extend only owned-resource cleanup."""

    def __init__(self, driver: _StreamDriver, config: TaskStreamConfig) -> None:
        self._driver = driver
        super().__init__(driver.proxy, config)

    def _run(self, function: StreamCallable) -> TaskStreamResult:
        result: TaskStreamResult | None = None
        problem: BaseException | None = None
        try:
            result = super()._run(function)
        except BaseException as error:
            problem = error
        # This also covers a cancellation before the proxy's first next():
        # closing a never-started generator does not enter its finally block.
        if self._stop.is_set():
            try:
                self._driver.signal()
            except BaseException as cleanup:
                problem = cleanup if problem is None else _preserve_failure(problem, cleanup)
        try:
            self._driver.finish()
        except BaseException as cleanup:
            problem = cleanup if problem is None else _preserve_failure(problem, cleanup)
        if problem is not None:
            raise problem
        if result is None:
            raise RuntimeError("process mailbox has no completion result")
        return result


class ProcessTaskStream(Iterator[TaskStreamItem]):
    """One consuming TaskStream mailbox and one explicitly owned native worker.

    Values are trusted serialized snapshots, not shared references. Close joins
    the local driver before inspecting/retrying native cleanup. Stored producer
    and generator errors are observed through next/completion, not close.
    """

    def __init__(self, stream: TaskStream, driver: _StreamDriver) -> None:
        self._stream = stream
        self._driver = driver
        self._close_lock = Lock()

    def __iter__(self) -> ProcessTaskStream:
        return self

    def __next__(self) -> TaskStreamItem:
        return self.next()

    def next(self, timeout: float | None = None) -> TaskStreamItem:
        return self._stream.next(timeout)

    def wait_ready(self, timeout: float | None = None) -> bool:
        return self._stream.wait_ready(timeout)

    def done(self) -> bool:
        """Driver settled (possibly with cleanup failure); not a native-close claim."""
        return self._stream.done()

    @property
    def worker(self) -> ProcessStreamWorker:
        return self._driver.snapshot()

    @property
    def closed(self) -> bool:
        return self._stream.closed and self.worker.resources_closed

    def completion(self, timeout: float | None = None) -> ProcessStreamResult:
        result = self._stream.completion(timeout)
        return ProcessStreamResult(result.status, result.produced, self.worker)

    def cancel(self) -> bool:
        changed = self._stream.cancel()
        if changed:
            self._driver.signal()
        return changed

    def close(self, timeout: float | None = None) -> None:
        """Wait budget applies to the driver; native cleanup has its own budgets."""
        _timeout(timeout)
        self._stream.close(timeout)
        # Only a joined producer releases exclusive ownership of its driver.
        with self._close_lock:
            self._driver.finish()

    def __enter__(self) -> ProcessTaskStream:
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


def start_process_task_stream(
    function: ProcessStreamCallable,
    *,
    args: tuple[object, ...] = (),
    kwargs: Mapping[str, object] | None = None,
    config: TaskStreamConfig | None = None,
    process_config: ProcessStreamConfig | None = None,
) -> ProcessTaskStream:
    """Admit trusted importable code/data, spawn synchronously, then start pulling.

    Startup uses its own budget before the returned handle exists. Full-frame
    pickle admission and OS spawn/join operations are not hard-interruptible.
    """
    producer = _function(function)
    checked = ProcessActor._arguments(args, kwargs)
    if config is not None and type(config) is not TaskStreamConfig:
        raise ValidationError("stream config must be TaskStreamConfig")
    options = config if config is not None else TaskStreamConfig()
    options.__post_init__()
    if process_config is not None and type(process_config) is not ProcessStreamConfig:
        raise ValidationError("process config must be ProcessStreamConfig")
    process = process_config if process_config is not None else ProcessStreamConfig()
    process.__post_init__()
    if options.max_buffered * process.max_message_bytes > 67_108_864:
        raise ValidationError("max_buffered * max_message_bytes must not exceed 64 MiB")
    actor, sender = ProcessActor._for_stream_worker(
        producer,
        args,
        checked,
        ActorConfig(
            max_pending=1,
            max_message_bytes=process.max_message_bytes,
            startup_timeout_seconds=process.startup_timeout_seconds,
        ),
    )
    driver: _StreamDriver | None = None
    try:
        driver = _StreamDriver(actor, sender, process)
        stream = _ProcessMailbox(driver, options)
        handle = ProcessTaskStream(stream, driver)
        stream._start()
        return handle
    except BaseException as error:
        primary = _StreamStartupCleanup(actor, (sender,)).retain(error)
        if primary is not error:
            raise primary from error
        raise


__all__ = [
    "ProcessStreamConfig",
    "ProcessStreamResult",
    "ProcessStreamWorker",
    "ProcessTaskStream",
    "start_process_task_stream",
]
