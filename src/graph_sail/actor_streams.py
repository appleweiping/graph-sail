"""Exclusive, stateful native generator methods on an existing local actor.

The actor broker owns transport/reaping; one mailbox driver owns advancement.
Running cancellation irreversibly retires the actor, never resets its EOF pipe.
"""

from __future__ import annotations

import inspect
import multiprocessing
import os
import time
from collections.abc import Callable, Generator, Iterator, Mapping
from dataclasses import dataclass, replace
from threading import Lock, current_thread
from types import MethodType, TracebackType
from typing import Protocol, cast

from graph_sail.actors import (
    ActorCall,
    ActorClosedError,
    ActorConfig,
    ActorDefinition,
    ActorDiedError,
    ActorError,
    ActorRegistry,
    ActorRemoteError,
    ActorSerializationError,
    ProcessActor,
    _CancellationConnection,
    _diagnostic,
    _identifier,
    _integer,
    _pack,
    _preserve_failure,
    _seconds,
    _StreamStartupCleanup,
)
from graph_sail.errors import ValidationError
from graph_sail.execution import TaskCancelled
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

_PROFILE = "graph-sail.actor-stream.v1"
_MAX_ID = (1 << 63) - 1


class _OwnedEndpoint(_CancellationConnection, Protocol):
    @property
    def closed(self) -> bool: ...


class _ActorCancellationEndpoint:
    """Parent-only close ownership; the child receives the original raw pipe."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._connection: _OwnedEndpoint | None = None

    def bind(self, connection: _OwnedEndpoint) -> None:
        with self._lock:
            if self._connection is not None:
                raise RuntimeError("actor cancellation endpoint is already bound")
            self._connection = connection

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._connection is None or self._connection.closed

    def close(self) -> None:
        with self._lock:
            if self._connection is not None and not self._connection.closed:
                self._connection.close()

    def poll(self, timeout: float = 0.0) -> bool:
        with self._lock:
            if self._connection is None:
                raise RuntimeError("actor cancellation endpoint is unbound")
            return self._connection.poll(timeout)


class ActorStreamBusyError(ActorError):
    """Actor work/lease or a not-yet-joined previous stream occupies admission."""


@dataclass(frozen=True, slots=True)
class ActorStreamConfig:
    open_timeout_seconds: float = 30.0
    advance_timeout_seconds: float = 30.0
    finish_timeout_seconds: float = 5.0
    cancellation_grace_seconds: float = 1.0
    shutdown_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        for name in ("open_timeout_seconds", "advance_timeout_seconds", "finish_timeout_seconds"):
            _seconds(getattr(self, name), name, 0.001)
        for name in ("cancellation_grace_seconds", "shutdown_timeout_seconds"):
            _seconds(getattr(self, name), name)


@dataclass(frozen=True, slots=True)
class ActorStreamState:
    actor_pid: int
    stream_id: int
    generator_opened: bool
    generator_closed: bool
    lease_released: bool
    actor_reusable_at_release: bool
    retirement_requested: bool
    actor_resources_closed: bool

    def __post_init__(self) -> None:
        _integer(self.actor_pid, "actor_pid", _MAX_ID)
        _integer(self.stream_id, "stream_id", _MAX_ID)
        for name in (
            "generator_opened",
            "generator_closed",
            "lease_released",
            "actor_reusable_at_release",
            "retirement_requested",
            "actor_resources_closed",
        ):
            if type(getattr(self, name)) is not bool:
                raise ValidationError("actor stream state flags must be booleans")
        if self.actor_reusable_at_release and (
            not self.lease_released or self.retirement_requested
        ):
            raise ValidationError("reusable actor state requires a clean lease release")


@dataclass(frozen=True, slots=True)
class ActorStreamResult:
    status: StreamStatus
    produced: int
    state: ActorStreamState

    def __post_init__(self) -> None:
        TaskStreamResult(self.status, self.produced)
        if type(self.state) is not ActorStreamState:
            raise ValidationError("result requires ActorStreamState")
        self.state.__post_init__()
        if not self.state.lease_released:
            raise ValidationError("successful result requires a settled lease")


def _profile_startup(
    value: object,
) -> tuple[
    Callable[..., object], tuple[str, ...], tuple[str, ...], tuple[object, ...], dict[str, object]
]:
    if (
        type(value) is not tuple
        or len(value) != 6
        or type(value[0]) is not str
        or value[0] != _PROFILE
    ):
        raise ActorDiedError("invalid actor stream startup")
    _, factory, names, streams, args, kwargs = value
    definition = ActorDefinition(factory, names, streams)
    if not definition.stream_methods:
        raise ActorDiedError("actor stream profile requires stream methods")
    return factory, names, streams, args, ProcessActor._arguments(args, kwargs)


class _ActorSession:
    """One child-thread native generator and one bounded closed receipt."""

    def __init__(
        self,
        instance: object,
        names: tuple[str, ...],
        receiver: _CancellationConnection,
        limit: int,
    ) -> None:
        self._methods: dict[str, Callable[..., object]] = {}
        for name in names:
            method = getattr(instance, name)
            if (
                type(method) is not MethodType
                or method.__self__ is not instance
                or not inspect.isgeneratorfunction(method.__func__)
            ):
                raise TypeError("stream method must be a native bound synchronous generator")
            self._methods[name] = method
        self._context = TaskStreamContext(_PipeCancellation(receiver))
        self._limit = limit
        self._pid = os.getpid()
        self._id = self._sequence = 0
        self._max_yields = 0
        self._generator: Generator[object, None, object] | None = None
        self._state = "idle"
        self._close_attempted = False

    @property
    def active(self) -> bool:
        return self._state not in ("idle", "closed")

    def _reply(self, request_id: int, kind: str, *tail: object) -> bytes:
        return _pack(
            (request_id, "ok", (kind, self._pid, self._id, self._sequence, *tail)),
            self._limit,
        )

    def _error(self, request_id: int, phase: str, error: Exception) -> bytes:
        self._state = "failed"
        kind, message = _diagnostic(error)
        kind = kind.encode("utf-8", "replace")[:256].decode("utf-8", "ignore")
        message = message.encode("utf-8", "replace")[: min(4096, self._limit // 8)].decode(
            "utf-8", "ignore"
        )
        return self._reply(request_id, "error", phase, kind, message)

    def dispatch(self, request: object) -> bytes:
        if (
            type(request) is not tuple
            or len(request) != 5
            or type(request[0]) is not int
            or request[0] < 1
            or type(request[1]) is not str
            or request[1] != _PROFILE
            or type(request[2]) is not int
            or not 1 <= request[2] <= _MAX_ID
            or type(request[3]) is not str
            or request[3] not in ("open", "advance", "finish")
        ):
            raise ActorDiedError("invalid actor stream request")
        request_id, _, stream_id, operation, payload = request
        if operation == "open":
            if self.active or stream_id <= self._id:
                raise ActorDiedError("actor stream open conflicts with its generation")
            if type(payload) is not tuple or len(payload) != 4:
                raise ActorDiedError("invalid actor stream open payload")
            name, args, kwargs, maximum = payload
            if type(name) is not str or name not in self._methods:
                raise ActorDiedError("unregistered stream method")
            _integer(maximum, "max_yields", 10_000_000)
            call_kwargs = ProcessActor._arguments(args, kwargs)
            self._id, self._sequence, self._max_yields = stream_id, 0, maximum
            self._generator, self._close_attempted, self._state = None, False, "open"
            try:
                generator = self._methods[name](self._context, *args, **call_kwargs)
                if (
                    not inspect.isgenerator(generator)
                    or inspect.getgeneratorstate(generator) != inspect.GEN_CREATED
                ):
                    raise TypeError("stream method must return a fresh native generator")
                self._generator = cast(Generator[object, None, object], generator)
            except Exception as error:
                return self._error(request_id, "open", error)
            return self._reply(request_id, "opened")
        if stream_id != self._id or type(payload) is not int or payload != self._sequence:
            raise ActorDiedError("actor stream generation/sequence mismatch")
        if operation == "finish":
            if self._state == "closed":
                return self._reply(request_id, "closed")
            if self._state == "idle" or self._close_attempted:
                raise ActorDiedError("actor stream closure is unknown")
            try:
                self.shutdown()
            except Exception as error:
                return self._error(request_id, "finish", error)
            self._state = "closed"
            return self._reply(request_id, "closed")
        if self._state != "open" or self._sequence >= self._max_yields or self._generator is None:
            raise ActorDiedError("actor stream cannot advance in this state")
        try:
            value = next(self._generator)
        except StopIteration:
            self._state = "ended"
            return self._reply(request_id, "eof")
        except Exception as error:
            return self._error(request_id, "advance", error)
        try:
            packed = self._reply(request_id, "yield", value)
        except Exception as error:
            return self._error(request_id, "serialization", error)
        self._sequence += 1  # Native advancement alone never spends a wire sequence.
        return packed

    def shutdown(self) -> None:
        if self._close_attempted:
            return
        self._close_attempted = True
        if self._generator is not None:
            self._generator.close()
            if inspect.getgeneratorstate(self._generator) != inspect.GEN_CLOSED:
                raise RuntimeError("actor generator did not close")
            self._generator = None


def _initialize_actor(
    actor: ProcessActor,
    registry: ActorRegistry,
    name: str,
    args: tuple[object, ...],
    kwargs: Mapping[str, object] | None,
    config: ActorConfig | None,
) -> None:
    options = ActorConfig() if config is None else config
    if not isinstance(options, ActorConfig):
        raise ValidationError("config must be ActorConfig")
    options = replace(options)
    definition = registry.actors[name]
    definition = ActorDefinition(definition.factory, definition.methods, definition.stream_methods)
    _pack(
        (
            _PROFILE,
            definition.factory,
            definition.methods,
            definition.stream_methods,
            args,
            ProcessActor._arguments(args, kwargs),
        ),
        options.max_message_bytes,
    )
    receiver_owner, sender_owner = _ActorCancellationEndpoint(), _ActorCancellationEndpoint()
    actor._actor_stream_endpoints = (receiver_owner, sender_owner)
    actor._actor_stream_sender = sender_owner
    actor._actor_stream_cleanup_lock = Lock()
    ownership = _StreamStartupCleanup(actor, actor._actor_stream_endpoints)
    try:
        raw_pair = multiprocessing.get_context("spawn").Pipe(duplex=False)
        # Wrappers, locks and the retry owner exist before native acquisition.
        # Until both binds succeed the same owner directly retains the raw pair.
        ownership._endpoints = raw_pair
        receiver_owner.bind(raw_pair[0])
        sender_owner.bind(raw_pair[1])
        ownership._endpoints = actor._actor_stream_endpoints
        actor._initialize(
            registry,
            name,
            args=args,
            kwargs=kwargs,
            config=options,
            task_cancellation=raw_pair[0],
            _stream_ownership=ownership,
        )
        receiver_owner.close()
    except BaseException as error:
        primary = ownership.retain(error)
        primary.actor_stream_cleanup = ownership  # type: ignore[attr-defined]
        if primary is not error:
            raise primary from error
        raise


class _ActorDriver:
    def __init__(self, actor: ProcessActor, stream_id: int, options: ActorStreamConfig) -> None:
        self.actor, self.id, self.options = actor, stream_id, options
        self.capability = object()
        self.open_payload = b""
        self.open_call: ActorCall | None = None
        self.sequence = 0
        self.opened = self.closed = self.released = self.reusable = self.retiring = False
        self.finish_attempted = False
        self.poisoned = False
        self._retire_attempted = False
        self._cleanup_lock = Lock()
        self.stream: _ActorMailbox

    def snapshot(self) -> ActorStreamState:
        with self.actor._condition:
            return ActorStreamState(
                cast(int, self.actor.pid),
                self.id,
                self.opened,
                self.closed,
                self.released,
                self.reusable,
                self.retiring,
                self.actor._process_closed
                and all(e.closed for e in self.actor._actor_stream_endpoints),
            )

    def _submit(self, operation: str) -> ActorCall:
        actor = self.actor
        with actor._condition:
            if actor._stream_lease is not self.capability or actor._closed.is_set() or actor._abort:
                raise ActorClosedError("actor stream lease is unavailable")
            if actor._closing and operation != "finish":
                raise ActorClosedError("actor no longer accepts stream advancement")
            if actor._pending:
                raise ActorDiedError("actor stream already has an in-flight credit")
            request_id = actor._next_id
            payload = (
                self.open_payload
                if operation == "open"
                else _pack(
                    (request_id, _PROFILE, self.id, operation, self.sequence),
                    actor.config.max_message_bytes,
                )
            )
            call = ActorCall(request_id, operation)
            call._operation_timeout = getattr(self.options, operation + "_timeout_seconds")
            actor._pending[request_id] = call
            try:
                actor._queue.append((call, payload))
            except BaseException:
                del actor._pending[request_id]
                raise
            actor._next_id += 1
            if operation == "open":
                self.open_call = call
            actor._condition.notify_all()
            return call

    def signal(self) -> None:
        actor = self.actor
        with actor._condition:
            if actor._stream_lease is not self.capability or self.released:
                return
            # Cancelled/not-yet-dispatched open is a positively unacquired source.
            if self.open_call is None or self.open_call._future.cancel():
                actor._condition.notify_all()
                return
            self.retiring = True
            if not actor._actor_stream_sender.closed:
                actor._actor_stream_sender.close()

    def _record(self, value: object, operation: str) -> tuple[object, ...]:
        if (
            type(value) is not tuple
            or len(value) < 4
            or type(value[0]) is not str
            or type(value[1]) is not int
            or value[1] != self.actor.pid
            or type(value[2]) is not int
            or value[2] != self.id
            or type(value[3]) is not int
            or value[3] != self.sequence
        ):
            self.poisoned = True
            raise ActorDiedError("invalid actor stream response binding")
        kind = value[0]
        if kind == "error":
            if (
                len(value) != 7
                or any(type(v) is not str for v in value[4:])
                or (
                    value[4] != operation
                    and not (operation == "advance" and value[4] == "serialization")
                )
            ):
                self.poisoned = True
                raise ActorDiedError("invalid actor stream error acknowledgement")
            try:
                bounded = len(value[5].encode("utf-8")) <= 256 and len(
                    value[6].encode("utf-8")
                ) <= min(4096, self.actor.config.max_message_bytes // 8)
            except UnicodeEncodeError as error:
                self.poisoned = True
                raise ActorDiedError("invalid actor stream diagnostic UTF-8") from error
            if not bounded:
                self.poisoned = True
                raise ActorDiedError("invalid actor stream error acknowledgement")
            if operation == "finish":
                self.poisoned = True
            if value[4] == "serialization":
                raise ActorSerializationError(value[6])
            raise ActorRemoteError(value[4], value[5], value[6])
        expected = {"open": {"opened"}, "advance": {"yield", "eof"}, "finish": {"closed"}}
        if kind not in expected[operation] or len(value) != (5 if kind == "yield" else 4):
            self.poisoned = True
            raise ActorDiedError("invalid actor stream response shape")
        if kind == "opened":
            self.opened = True
        elif kind == "yield":
            self.sequence += 1
        elif kind == "closed":
            self.closed = True
        return value

    def _wait(self, call: ActorCall, operation: str) -> tuple[object, ...]:
        grace_end: float | None = None
        while True:
            if operation != "finish" and self.stream._stop.is_set():
                self.signal()
                if call.cancelled():
                    raise TaskCancelled("actor stream cancelled before open dispatch")
                if grace_end is None:
                    grace_end = time.monotonic() + self.options.cancellation_grace_seconds
                if not call.done() and time.monotonic() >= grace_end:
                    self._retire()
                    raise TaskCancelled("actor stream cancellation grace expired")
            try:
                value = call.result(0.02)
            except TimeoutError:
                continue
            except BaseException:
                self.poisoned = True
                raise
            try:
                record = self._record(value, operation)
            except (ActorRemoteError, ActorSerializationError):
                if operation != "finish" and self.stream._stop.is_set():
                    self.signal()
                    raise TaskCancelled("actor stream cancelled") from None
                raise
            if operation != "finish" and self.stream._stop.is_set():
                self.signal()
                raise TaskCancelled("actor stream cancelled; late value discarded")
            return record

    def proxy(self, context: TaskStreamContext) -> Generator[object, None, object]:
        if context.cancellation.cancelled:
            return None
        self._wait(self._submit("open"), "open")
        while True:
            record = self._wait(self._submit("advance"), "advance")
            if record[0] == "eof":
                return None
            yield record[4]
            del record

    def _release(self, reusable: bool) -> None:
        with self.actor._condition:
            if self.actor._stream_lease is self.capability or (
                self.retiring
                and self.actor._stream_owner is not None
                and self.actor._stream_owner._driver is self
            ):
                self.actor._stream_lease = None
                self.released, self.reusable = True, reusable
                self.actor._condition.notify_all()

    def _retire(self) -> None:
        actor = self.actor
        with actor._condition:
            if self.released:
                return
            self.retiring = True
        problem: BaseException | None = None
        try:
            if not actor._actor_stream_sender.closed:
                actor._actor_stream_sender.close()
        except BaseException as error:
            problem = error
        try:
            retry = self._retire_attempted
            self._retire_attempted = True
            if self.closed and not self.poisoned:
                # The source is positively closed; let the same worker exit on
                # ordinary EOF, but keep this owner until native cleanup joins.
                with actor._condition:
                    actor._closing = True
                    if actor._stream_lease is self.capability:
                        actor._stream_lease = None
                    actor._condition.notify_all()
                if not actor._closed.wait(self.options.shutdown_timeout_seconds):
                    actor._request_abort(ActorClosedError("actor stream shutdown budget expired"))
            else:
                actor._request_abort(ActorClosedError("actor stream retired; state is lost"))
            actor._thread.join()
            if retry and actor._cleanup_error is not None:
                _retry_cleanup(actor)
            if actor._cleanup_error is not None:
                raise actor._cleanup_error
            if not actor._process_closed or any(
                not e.closed for e in actor._actor_stream_endpoints
            ):
                raise ActorDiedError("actor stream native cleanup is unresolved")
            self._release(False)
        except BaseException as cleanup:
            problem = cleanup if problem is None else _preserve_failure(problem, cleanup)
        if problem is not None:
            raise problem

    def finish(self) -> None:
        with self._cleanup_lock:
            if self.released:
                return
            actor = self.actor
            if self.open_call is None or self.open_call.cancelled():
                # Let the broker remove a positively cancelled queued request.
                with actor._condition:
                    actor._condition.wait_for(lambda: not actor._pending or actor._closed.is_set())
                self._release(not actor._closing and actor.failure is None and actor.alive)
                return
            problem: BaseException | None = None
            if not self.finish_attempted and not self.poisoned and not actor._closed.is_set():
                self.finish_attempted = True
                try:
                    self._wait(self._submit("finish"), "finish")
                except BaseException as error:
                    problem = error
                    self.poisoned = True
            with actor._condition:
                reusable = (
                    self.closed
                    and not self.poisoned
                    and not self.retiring
                    and not actor._closing
                    and actor.failure is None
                    and actor.alive
                    and not self.stream._stop.is_set()
                )
                if reusable:
                    self._release(True)
                    return
            try:
                self._retire()
            except BaseException as cleanup:
                problem = cleanup if problem is None else _preserve_failure(problem, cleanup)
            if problem is not None:
                raise problem


class _ActorMailbox(TaskStream):
    def __init__(self, driver: _ActorDriver, config: TaskStreamConfig) -> None:
        self._driver = driver
        super().__init__(driver.proxy, config)
        driver.stream = self

    def _start(self) -> _ActorMailbox:
        actor = self._driver.actor
        try:
            with actor._condition:
                if (
                    actor._closing
                    or actor._closed.is_set()
                    or actor._stream_lease is not self._driver.capability
                ):
                    raise ActorClosedError("actor closed before its admitted stream started")
                self._thread.start()
        except BaseException as error:
            # A real adopted driver may need the actor condition to finish.
            # Never cancel/join while holding that admission/start lock.
            primary = error
            try:
                self.cancel()
            except BaseException as cleanup:
                primary = _preserve_failure(primary, cleanup)
            if self._thread.ident is not None:
                try:
                    TaskStream.close(self, self._driver.options.shutdown_timeout_seconds)
                except BaseException as cleanup:
                    primary = _preserve_failure(primary, cleanup)
            if primary is not error:
                raise primary from error
            raise
        return self

    def _run(self, function: StreamCallable) -> TaskStreamResult:
        result = None
        problem: BaseException | None = None
        try:
            result = super()._run(function)
        except BaseException as error:
            problem = error
            if not isinstance(error, (ActorRemoteError, ActorSerializationError, TaskCancelled)):
                self._driver.poisoned = True
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
            raise RuntimeError("actor stream has no completion result")
        return result


class ActorMethodStream(Iterator[TaskStreamItem]):
    """One consuming mailbox and retained lease; close is explicit, not GC-based."""

    def __init__(self, stream: _ActorMailbox, driver: _ActorDriver) -> None:
        self._stream, self._driver = stream, driver

    @property
    def state(self) -> ActorStreamState:
        return self._driver.snapshot()

    @property
    def closed(self) -> bool:
        return self._stream.closed and self.state.lease_released

    def done(self) -> bool:
        return self._stream.done()

    def _not_transport(self) -> None:
        if current_thread() is self._driver.actor._thread:
            raise ActorError("actor transport cannot wait on its own stream")

    def __iter__(self) -> ActorMethodStream:
        return self

    def __next__(self) -> TaskStreamItem:
        return self.next()

    def next(self, timeout: float | None = None) -> TaskStreamItem:
        self._not_transport()
        return self._stream.next(timeout)

    def wait_ready(self, timeout: float | None = None) -> bool:
        self._not_transport()
        return self._stream.wait_ready(timeout)

    def completion(self, timeout: float | None = None) -> ActorStreamResult:
        self._not_transport()
        result = self._stream.completion(timeout)
        return ActorStreamResult(result.status, result.produced, self.state)

    def __aiter__(self) -> ActorMethodStream:
        return self

    async def __anext__(self) -> TaskStreamItem:
        return await self.next_async()

    async def next_async(self, timeout: float | None = None) -> TaskStreamItem:
        self._not_transport()
        return await self._stream.next_async(timeout)

    async def wait_ready_async(self, timeout: float | None = None) -> bool:
        self._not_transport()
        return await self._stream.wait_ready_async(timeout)

    async def completion_async(self, timeout: float | None = None) -> ActorStreamResult:
        self._not_transport()
        result = await self._stream.completion_async(timeout)
        return ActorStreamResult(result.status, result.produced, self.state)

    def cancel(self) -> bool:
        changed = self._stream.cancel()
        if changed:
            self._driver.signal()
        return changed

    def close(self, timeout: float | None = None) -> None:
        _timeout(timeout)
        with self._driver.actor._condition:
            self._driver.actor._check_lifecycle_reentry()
        if self._stream._thread.ident is None:
            self._stream.cancel()
            self._driver.finish()
            self._stream._finished = self._stream._closed = True
            return
        self._stream.close(timeout)
        self._driver.finish()

    def __enter__(self) -> ActorMethodStream:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
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


def _start_actor_stream(
    actor: ProcessActor,
    method: str,
    args: tuple[object, ...],
    kwargs: Mapping[str, object] | None,
    config: TaskStreamConfig | None,
    stream_config: ActorStreamConfig | None,
) -> ActorMethodStream:
    _identifier(method, "stream method")
    if method not in actor._stream_methods:
        raise ValidationError("method is not registered for actor streaming")
    if config is not None and type(config) is not TaskStreamConfig:
        raise ValidationError("config must be TaskStreamConfig")
    if stream_config is not None and type(stream_config) is not ActorStreamConfig:
        raise ValidationError("stream_config must be ActorStreamConfig")
    options = TaskStreamConfig() if config is None else replace(config)
    process = ActorStreamConfig() if stream_config is None else replace(stream_config)
    if options.max_buffered * actor.config.max_message_bytes > 67_108_864:
        raise ValidationError("actor stream buffer times message bytes exceeds 64 MiB")
    call_kwargs = actor._arguments(args, kwargs)
    with actor._condition:
        actor._check_lifecycle_reentry()
        if actor._closing or actor._closed.is_set():
            raise ActorClosedError("actor is closing or closed")
        previous = actor._stream_owner
        if (
            actor._stream_lease is not None
            or actor._pending
            or (previous is not None and previous._stream._thread.is_alive())
        ):
            raise ActorStreamBusyError("actor is not idle for streaming")
        if previous is not None and previous._stream._thread.ident is not None:
            previous._stream._thread.join(0)
        _integer(actor._next_stream_id, "stream_id", _MAX_ID)
        driver = _ActorDriver(actor, actor._next_stream_id, process)
        actor._serializing = True
        try:
            driver.open_payload = _pack(
                (
                    actor._next_id,
                    _PROFILE,
                    driver.id,
                    "open",
                    (method, args, call_kwargs, options.max_yields),
                ),
                actor.config.max_message_bytes,
            )
        finally:
            actor._serializing = False
        if actor._closing or actor._closed.is_set():
            raise ActorClosedError("actor closed during stream serialization")
        stream = _ActorMailbox(driver, options)
        handle = ActorMethodStream(stream, driver)
        actor._stream_owner, actor._stream_lease = handle, driver.capability
        actor._next_stream_id += 1
    try:
        stream._start()
    except BaseException as error:
        primary = error
        try:
            handle.close(process.shutdown_timeout_seconds)
        except BaseException as cleanup:
            primary = _preserve_failure(primary, cleanup)
        primary.actor_stream_cleanup = handle  # type: ignore[attr-defined]
        if primary is not error:
            raise primary from error
        raise
    return handle


def _shutdown_actor(actor: ProcessActor, timeout: float, *, force: bool) -> None:
    with actor._condition:
        actor._check_lifecycle_reentry()
        owner = actor._stream_owner
        retry = actor._closed.is_set()
        if owner is not None:
            owner._stream._not_producer()
        actor._closing = True
        actor._condition.notify_all()
    problem: BaseException | None = None
    try:
        if owner is not None:
            owner.cancel()
    except BaseException as error:
        problem = error
        force = True
    try:
        if force or not actor._closed.wait(timeout):
            actor._request_abort(
                ActorClosedError("actor shutdown requested; accepted work abandoned")
            )
        actor._thread.join()
        if retry and actor._cleanup_error is not None:
            _retry_cleanup(actor)
        if actor._cleanup_error is not None:
            raise actor._cleanup_error
    except BaseException as cleanup:
        problem = cleanup if problem is None else _preserve_failure(problem, cleanup)
    try:
        if owner is not None:
            owner.close()
    except BaseException as cleanup:
        problem = cleanup if problem is None else _preserve_failure(problem, cleanup)
    if problem is not None:
        raise problem


def _retry_cleanup(actor: ProcessActor) -> None:
    """Only the opted-in profile retries, after its sole broker has joined."""
    with actor._actor_stream_cleanup_lock:
        if actor._thread.is_alive():
            raise ActorDiedError("cannot retry native cleanup beside the broker")
        if not actor._process_closed:
            actor._cleanup_error = None
            actor._cleanup_process(graceful=False)
        else:
            problem: BaseException | None = None
            for endpoint in (*actor._actor_stream_endpoints, actor._connection):
                try:
                    endpoint.close()
                except BaseException as cleanup:
                    problem = cleanup if problem is None else _preserve_failure(problem, cleanup)
            if problem is not None:
                raise problem
            actor._cleanup_error = None


__all__ = [
    "ActorMethodStream",
    "ActorStreamBusyError",
    "ActorStreamConfig",
    "ActorStreamResult",
    "ActorStreamState",
]
