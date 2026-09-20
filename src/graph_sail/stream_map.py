"""A bounded local source-to-map edge with per-yield downstream execution.

The producer and mapper are trusted application code. This is not Ray's
distributed object-reference protocol, scheduler, or worker-failure recovery.
"""

from __future__ import annotations

import inspect
import time
from collections.abc import Callable, Generator, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from threading import Condition, Event, Thread, current_thread
from types import TracebackType
from typing import Literal

from graph_sail.actors import ActorSerializationError, _pack, _preserve_failure, _unpack
from graph_sail.errors import GraphSailError, ValidationError
from graph_sail.execution import CancellationSignal, CancellationToken, TaskCancelled, _count
from graph_sail.handles import _timeout
from graph_sail.task_streams import TaskStreamContext, TaskStreamItem

_MAX_OUTSTANDING_BYTES = 64 * 1024 * 1024


class StreamMapSerializationError(GraphSailError):
    """An accepted source or mapped value cannot fit the snapshot profile."""


@dataclass(frozen=True, slots=True)
class StreamMapConfig:
    """One source, a bounded map pool, and one shared accepted-item credit."""

    max_pending: int = 8
    max_workers: int = 2
    max_yields: int = 100_000
    max_item_bytes: int = 1_048_576
    max_result_bytes: int = 1_048_576

    def __post_init__(self) -> None:
        _count(self.max_pending, "max_pending", minimum=1, maximum=1_024)
        _count(self.max_workers, "max_workers", minimum=1, maximum=64)
        _count(self.max_yields, "max_yields", minimum=1, maximum=10_000_000)
        _count(self.max_item_bytes, "max_item_bytes", minimum=1, maximum=8_388_608)
        _count(self.max_result_bytes, "max_result_bytes", minimum=1, maximum=8_388_608)
        if self.max_workers > self.max_pending:
            raise ValidationError("max_workers cannot exceed max_pending")
        if self.max_pending * (self.max_item_bytes + self.max_result_bytes) > (
            _MAX_OUTSTANDING_BYTES
        ):
            raise ValidationError("stream map outstanding snapshot budget exceeds 64 MiB")


@dataclass(frozen=True, slots=True)
class StreamMapContext:
    sequence: int
    cancellation: CancellationSignal


@dataclass(frozen=True, slots=True)
class StreamMapResult:
    status: Literal["succeeded", "limited", "cancelled"]
    accepted: int
    published: int

    def __post_init__(self) -> None:
        if type(self.status) is not str or self.status not in ("succeeded", "limited", "cancelled"):
            raise ValidationError("invalid stream map status")
        _count(self.accepted, "accepted", minimum=0, maximum=10_000_000)
        _count(self.published, "published", minimum=0, maximum=self.accepted)


StreamMapProducer = Callable[[TaskStreamContext], Generator[object, None, object]]
StreamMapper = Callable[[StreamMapContext, object], object]


def _snapshot(value: object, limit: int) -> bytes:
    try:
        return _pack(value, limit)
    except ActorSerializationError as error:
        raise StreamMapSerializationError(
            "stream map value exceeds the snapshot profile"
        ) from error


def _not_eof_failure(error: BaseException) -> BaseException:
    if isinstance(error, StopIteration):
        failure = ValidationError("stream map callback raised StopIteration")
        failure.__cause__ = error
        return failure
    return error


class StreamMap(Iterator[TaskStreamItem]):
    """Owned local source and map pool; one public ordered consuming reader.

    `max_pending` covers every accepted but not yet consumed source item,
    including running maps and out-of-order completed results. A credit is
    acquired *before* the next source advancement and released only when an
    ordered mapped result is consumed. There is no hidden source prefetch.
    """

    def __init__(
        self,
        producer: StreamMapProducer,
        mapper: StreamMapper,
        config: StreamMapConfig | None = None,
    ) -> None:
        if not inspect.isgeneratorfunction(producer):
            raise ValidationError("stream map source must be a native generator function")
        if not callable(mapper) or inspect.iscoroutinefunction(mapper):
            raise ValidationError("stream mapper must be a synchronous callable")
        if config is not None and type(config) is not StreamMapConfig:
            raise ValidationError("stream map config must be StreamMapConfig")
        options = config if config is not None else StreamMapConfig()
        options.__post_init__()
        self._producer = producer
        self._mapper = mapper
        self._config = options
        self._condition = Condition()
        self._stop = Event()
        self._discard = False
        self._finished = False
        self._closed = False
        self._reading = False
        self._accepted = 0
        self._published = 0
        self._running = 0
        self._worker_threads: set[Thread] = set()
        self._completed: dict[int, bytes | BaseException] = {}
        self._failure_sequence: int | None = None
        self._failure: BaseException | None = None
        self._status: Literal["succeeded", "limited", "cancelled"] = "cancelled"
        self._thread = Thread(target=self._drive, name="graph-sail-stream-map", daemon=False)
        self._thread.start()

    def _not_coordinator(self) -> None:
        with self._condition:
            if current_thread() is self._thread or current_thread() in self._worker_threads:
                raise RuntimeError("stream map owned worker cannot wait on or close itself")

    def _fail(self, sequence: int, error: BaseException) -> None:
        failure = _not_eof_failure(error)
        with self._condition:
            if self._failure_sequence is None or sequence < self._failure_sequence:
                self._failure_sequence = sequence
                self._failure = failure
            self._stop.set()
            self._condition.notify_all()

    def _map_one(self, sequence: int, source_bytes: bytes) -> bytes:
        worker = current_thread()
        with self._condition:
            self._worker_threads.add(worker)
        try:
            value = _unpack(source_bytes)
            result = self._mapper(StreamMapContext(sequence, CancellationToken(self._stop)), value)
            if inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()
                raise ValidationError("stream mapper must finish synchronously")
            return _snapshot(result, self._config.max_result_bytes)
        finally:
            with self._condition:
                self._worker_threads.discard(worker)

    def _settle(self, sequence: int, future: Future[bytes]) -> None:
        try:
            settled: bytes | BaseException = future.result()
        except BaseException as error:
            settled = _not_eof_failure(error)
        with self._condition:
            self._running -= 1
            if not self._discard:
                self._completed[sequence] = settled
                if isinstance(settled, BaseException) and (
                    self._failure_sequence is None or sequence < self._failure_sequence
                ):
                    self._failure_sequence = sequence
                    self._failure = settled
                    self._stop.set()
            self._condition.notify_all()

    def _drive(self) -> None:
        generator: Generator[object, None, object] | None = None
        executor: ThreadPoolExecutor | None = None
        status: Literal["succeeded", "limited", "cancelled"] = "cancelled"
        try:
            generator = self._producer(TaskStreamContext(CancellationToken(self._stop)))
            if not inspect.isgenerator(generator) or inspect.getgeneratorstate(generator) != (
                inspect.GEN_CREATED
            ):
                raise ValidationError("stream map source must be a fresh native generator")
            executor = ThreadPoolExecutor(
                max_workers=self._config.max_workers, thread_name_prefix="graph-sail-map"
            )
            while self._accepted < self._config.max_yields:
                with self._condition:
                    self._condition.wait_for(
                        lambda: (
                            self._stop.is_set()
                            or (
                                self._accepted - self._published < self._config.max_pending
                                and self._running < self._config.max_workers
                            )
                        )
                    )
                    if self._stop.is_set():
                        break
                try:
                    value = next(generator)
                except StopIteration:
                    status = "succeeded"
                    break
                except TaskCancelled:
                    if not self._stop.is_set():
                        raise
                    break
                # Snapshot before the next advancement, never borrow a mutable
                # value across producer/map threads.
                source_bytes = _snapshot(value, self._config.max_item_bytes)
                del value
                with self._condition:
                    if self._stop.is_set():
                        break
                    sequence = self._accepted
                    future = executor.submit(self._map_one, sequence, source_bytes)
                    self._accepted += 1
                    self._running += 1

                    def on_done(settled: Future[bytes], *, seq: int = sequence) -> None:
                        self._settle(seq, settled)

                    future.add_done_callback(on_done)
                    self._condition.notify_all()
            else:
                status = "limited"
        except BaseException as error:
            self._fail(self._accepted, error)
        finally:
            if generator is not None:
                try:
                    generator.close()
                except BaseException as error:
                    self._fail(self._accepted, error)
            if executor is not None:
                try:
                    executor.shutdown(wait=True, cancel_futures=self._discard)
                except BaseException as error:
                    self._fail(self._accepted, error)
            with self._condition:
                self._status = "cancelled" if self._discard else status
                self._finished = True
                self._condition.notify_all()

    def __iter__(self) -> StreamMap:
        return self

    def __next__(self) -> TaskStreamItem:
        return self.next()

    def next(self, timeout: float | None = None) -> TaskStreamItem:
        _timeout(timeout)
        self._not_coordinator()
        with self._condition:
            if self._reading:
                raise RuntimeError("stream map already has a consuming reader")
            self._reading = True
            try:
                if not self._condition.wait_for(
                    lambda: self._discard or self._published in self._completed or self._finished,
                    timeout,
                ):
                    raise TimeoutError("mapped item is not ready")
                if self._discard:
                    raise StopIteration
                sequence = self._published
                entry = self._completed.get(sequence)
                if entry is None:
                    if self._failure is not None:
                        raise self._failure
                    raise StopIteration
                if isinstance(entry, BaseException):
                    raise entry
            except BaseException:
                self._reading = False
                self._condition.notify_all()
                raise
        try:
            value = _unpack(entry)
        except BaseException as error:
            failure = _not_eof_failure(error)
            self._fail(sequence, failure)
            with self._condition:
                if not self._discard and self._completed.get(sequence) is entry:
                    self._completed[sequence] = failure
                self._reading = False
                self._condition.notify_all()
            if failure is error:
                raise
            raise failure from error
        with self._condition:
            self._reading = False
            if self._discard:
                self._condition.notify_all()
                raise StopIteration
            del self._completed[sequence]
            self._published += 1
            self._condition.notify_all()
        return TaskStreamItem(sequence, value)

    def done(self) -> bool:
        with self._condition:
            return self._finished

    @property
    def closed(self) -> bool:
        with self._condition:
            return self._closed

    def completion(self, timeout: float | None = None) -> StreamMapResult:
        """Wait for source and maps; capacity may require consuming items first."""
        _timeout(timeout)
        self._not_coordinator()
        with self._condition:
            if not self._condition.wait_for(lambda: self._finished, timeout):
                raise TimeoutError("stream map completion is not ready")
            if self._failure is not None:
                raise self._failure
            return StreamMapResult(self._status, self._accepted, self._published)

    def cancel(self) -> bool:
        """Discard future publications and request cooperative source/map stop."""
        with self._condition:
            if self._discard:
                return False
            active = not self._finished
            self._discard = True
            self._stop.set()
            self._completed.clear()
            self._condition.notify_all()
            return active

    def close(self, timeout: float | None = None) -> None:
        """Join owned workers or retain an unfinished owner for a later retry."""
        _timeout(timeout)
        self._not_coordinator()
        deadline = None if timeout is None else time.monotonic() + timeout
        self.cancel()
        with self._condition:
            if not self._condition.wait_for(lambda: self._finished, timeout):
                raise TimeoutError("stream map still owns live work; retry close")
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        self._thread.join(remaining)
        if self._thread.is_alive():
            raise TimeoutError("stream map still owns live work; retry close")
        with self._condition:
            self._closed = True

    def __enter__(self) -> StreamMap:
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


def start_stream_map(
    producer: StreamMapProducer,
    mapper: StreamMapper,
    *,
    config: StreamMapConfig | None = None,
) -> StreamMap:
    """Start a local source-to-map edge without producer prefetch or retries."""
    return StreamMap(producer, mapper, config)


__all__ = [
    "StreamMap",
    "StreamMapConfig",
    "StreamMapContext",
    "StreamMapResult",
    "StreamMapSerializationError",
    "start_stream_map",
]
