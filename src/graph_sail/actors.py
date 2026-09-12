"""Trusted, local, spawn-process actors with a bounded serial mailbox.

Pickle is restricted to the private pipe of a locally spawned trusted worker.
Process isolation is not a security sandbox; do not register untrusted code.
"""

from __future__ import annotations

import inspect
import io
import math
import multiprocessing

# Explicit trusted local Python object boundary, never network input.
import pickle  # nosec B403
import time
from collections import deque
from collections.abc import Callable, Mapping
from concurrent.futures import Future
from dataclasses import dataclass
from multiprocessing.connection import Connection, wait
from threading import Condition, Event, Lock, Thread, current_thread
from types import MappingProxyType
from typing import Protocol

from graph_sail._completion import _await_snapshot, _CompletionHub, _Snapshot
from graph_sail.errors import GraphSailError, ValidationError


class ActorError(GraphSailError):
    """Base for actor lifecycle, transport and remote invocation failures."""


class ActorClosedError(ActorError):
    """The actor no longer accepts work, or shutdown abandoned accepted work."""


class ActorDiedError(ActorError):
    """The worker exited or its private protocol failed; state is not recovered."""


class ActorTimeoutError(ActorError):
    """The startup or invocation budget expired and the worker was stopped."""


class ActorQueueFullError(ActorError):
    """The bounded mailbox already contains the maximum pending requests."""


class ActorSerializationError(ActorError):
    """An argument/result is not serializable or exceeds the byte limit."""


class ActorRemoteError(ActorError):
    """A remote ordinary exception, represented without unpickling its class."""

    def __init__(self, phase: str, remote_type: str, remote_message: str) -> None:
        self.phase = phase
        self.remote_type = remote_type
        self.remote_message = remote_message
        super().__init__(f"{phase}: {remote_type}: {remote_message}")


class ActorStartupError(ActorError):
    """A registered factory could not produce a usable actor."""


class _CancellationConnection(Protocol):
    """Common spawn pipe subset on Windows and POSIX (internal task bootstrap)."""

    def close(self) -> None: ...

    def poll(self, timeout: float = 0.0) -> bool: ...


class _StreamStartupCleanup:
    """Retained failed-start ownership, exposed on the original stream error.

    This is deliberately not a factory or an actor lifecycle extension. Only
    acquired resources from the internal stream bootstrap are admitted here.
    """

    def __init__(self, actor: ProcessActor, endpoints: tuple[_CancellationConnection, ...]) -> None:
        self._actor = actor
        self._endpoints = endpoints
        self._endpoints_closed = [False] * len(endpoints)
        self._actor_closed = False
        self._child_endpoint: _CancellationConnection | None = None
        self._child_closed = False
        self._lock = Lock()
        self._cleanup_lock = Lock()

    @property
    def closed(self) -> bool:
        with self._lock:
            return (
                self._actor_closed
                and all(self._endpoints_closed)
                and (self._child_endpoint is None or self._child_closed)
            )

    def close(self) -> None:
        """Retry acquired-resource cleanup; failure retains this same owner."""
        with self._cleanup_lock:
            problem: BaseException | None = None
            for index, endpoint in enumerate(self._endpoints):
                if not self._endpoints_closed[index]:
                    try:
                        endpoint.close()
                        with self._lock:
                            self._endpoints_closed[index] = True
                    except BaseException as error:
                        problem = error if problem is None else _preserve_failure(problem, error)
            if self._child_endpoint is not None and not self._child_closed:
                try:
                    self._child_endpoint.close()
                    with self._lock:
                        self._child_closed = True
                except BaseException as cleanup:
                    problem = cleanup if problem is None else _preserve_failure(problem, cleanup)
            try:
                if not self._actor_closed:
                    self._close_actor()
                    with self._lock:
                        self._actor_closed = True
            except BaseException as cleanup:
                problem = cleanup if problem is None else _preserve_failure(problem, cleanup)
            if problem is not None:
                raise problem

    def _close_actor(self) -> None:
        actor = self._actor
        broker = getattr(actor, "_thread", None)
        if broker is not None and broker.ident is not None:
            # Includes an initializer wrapper that raised *after* starting the
            # actual broker. Never reap concurrently with that broker.
            actor.terminate()
        elif hasattr(actor, "_process") and not actor._process_closed:
            # No broker ever started: this owner may retry partial bootstrap
            # cleanup. Clear only its old diagnostic, not resource identities.
            actor._cleanup_error = None
            actor._cleanup_process(graceful=False)
            if actor._cleanup_error is not None:
                raise actor._cleanup_error
        elif hasattr(actor, "_connection"):
            # Process construction may fail after allocating the actor pipe,
            # or earlier cleanup may have closed the process handle already.
            actor._connection.close()

    def retain(self, primary: BaseException) -> BaseException:
        """Attempt all resources, attaching retry ownership without hiding control."""
        try:
            self.close()
        except BaseException as cleanup:
            primary = _preserve_failure(primary, cleanup)
        # These are trusted local exceptions, not a user-facing wire document.
        # Preserve the original KeyboardInterrupt/SystemExit object and type.
        primary.process_stream_cleanup = self  # type: ignore[attr-defined]
        return primary


def _preserve_failure(primary: BaseException, secondary: BaseException) -> BaseException:
    """Keep a primary control exception, without hiding a new cleanup interrupt."""
    if primary is secondary:
        return primary
    if isinstance(primary, Exception) and not isinstance(secondary, Exception):
        secondary.add_note(f"earlier failure: {type(primary).__name__}")
        return secondary
    primary.add_note(f"secondary cleanup failure: {type(secondary).__name__}")
    return primary


def _integer(value: int, name: str, maximum: int, minimum: int = 1) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValidationError(f"{name} must be an integer in [{minimum}, {maximum}]")


def _seconds(value: float, name: str, minimum: float = 0) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not minimum <= value <= 86_400
        or (isinstance(value, float) and not math.isfinite(value))
    ):
        raise ValidationError(f"{name} must be finite and in [{minimum}, 86400] seconds")


def _identifier(value: str, name: str) -> None:
    if (
        not isinstance(value, str)
        or not value.isascii()
        or not value.isidentifier()
        or value.startswith("_")
        or len(value) > 128
    ):
        raise ValidationError(f"{name} must be a public ASCII identifier of at most 128 characters")


@dataclass(frozen=True, slots=True)
class ActorDefinition:
    """An importable trusted factory/class and explicit public method allowlist."""

    factory: Callable[..., object]
    methods: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            not (inspect.isfunction(self.factory) or inspect.isclass(self.factory))
            or inspect.iscoroutinefunction(self.factory)
            or "<" in self.factory.__qualname__
        ):
            raise ValidationError("actor factory must be an importable synchronous function/class")
        if not isinstance(self.methods, tuple) or not 1 <= len(self.methods) <= 64:
            raise ValidationError("methods must be a tuple containing 1 to 64 public method names")
        for method in self.methods:
            _identifier(method, "method")
        if len(set(self.methods)) != len(self.methods):
            raise ValidationError("method names must be unique")


@dataclass(frozen=True, slots=True)
class ActorRegistry:
    """Immutable named definitions; no import paths are resolved from documents."""

    actors: Mapping[str, ActorDefinition]

    def __post_init__(self) -> None:
        if not isinstance(self.actors, Mapping) or not 1 <= len(self.actors) <= 256:
            raise ValidationError("actor registry must contain 1 to 256 definitions")
        snapshot = {}
        for name, definition in self.actors.items():
            _identifier(name, "actor name")
            if not isinstance(definition, ActorDefinition):
                raise ValidationError("registry entries must be ActorDefinition values")
            snapshot[name] = ActorDefinition(definition.factory, definition.methods)
        object.__setattr__(self, "actors", MappingProxyType(snapshot))


@dataclass(frozen=True, slots=True)
class ActorConfig:
    """Bounded mailbox, serialized-message sizes and process lifecycle budgets."""

    max_pending: int = 32
    max_message_bytes: int = 1_048_576
    startup_timeout_seconds: float = 30.0
    method_timeout_seconds: float | None = None

    def __post_init__(self) -> None:
        _integer(self.max_pending, "max_pending", 1024)
        _integer(self.max_message_bytes, "max_message_bytes", 16_777_216, 1024)
        if self.max_pending * self.max_message_bytes > 67_108_864:
            raise ValidationError("max_pending * max_message_bytes must not exceed 64 MiB")
        _seconds(self.startup_timeout_seconds, "startup_timeout_seconds", 0.001)
        if self.method_timeout_seconds is not None:
            _seconds(self.method_timeout_seconds, "method_timeout_seconds", 0.001)


class _LimitedBuffer(io.BytesIO):
    def __init__(self, limit: int) -> None:
        super().__init__()
        self.limit = limit

    def write(self, data: bytes, /) -> int:  # type: ignore[override]
        if self.tell() + len(data) > self.limit:
            raise ActorSerializationError(f"serialized message exceeds {self.limit} bytes")
        return super().write(data)


def _pack(value: object, limit: int) -> bytes:
    try:
        with _LimitedBuffer(limit) as buffer:
            pickle.Pickler(buffer, protocol=5).dump(value)
            return buffer.getvalue()
    except Exception as error:
        if isinstance(error, ActorSerializationError):
            raise
        raise ActorSerializationError(f"cannot serialize {type(error).__name__}") from error


def _unpack(data: bytes) -> object:
    # Only our private child/parent pipe and explicitly trusted registered code.
    return pickle.loads(data)  # nosec B301


def _diagnostic(error: Exception) -> tuple[str, str]:
    try:
        message = str(error)[:4096]
    except Exception:
        message = "exception message unavailable"
    return type(error).__name__[:256], message


def _synchronous(value: object) -> object:
    if inspect.isawaitable(value):
        if inspect.iscoroutine(value):
            value.close()
        raise TypeError("actor factories/methods must not return awaitables")
    return value


def _response(request_id: int, phase: str, error: Exception, limit: int) -> bytes:
    kind, message = _diagnostic(error)
    # Diagnostics must themselves fit even the smallest configured message size.
    # Character truncation alone does not bound Unicode's serialized byte size.
    kind = kind.encode("utf-8", "replace")[:256].decode("utf-8", "ignore")
    message = message.encode("utf-8", "replace")[: min(4096, limit // 8)].decode("utf-8", "ignore")
    return _pack((request_id, "error", (phase, kind, message)), limit)


def _startup_fields(
    data: object,
) -> tuple[Callable[..., object], tuple[str, ...], tuple[object, ...], dict[str, object]]:
    if (
        not isinstance(data, tuple)
        or len(data) != 4
        or not callable(data[0])
        or not isinstance(data[1], tuple)
        or not all(isinstance(name, str) for name in data[1])
        or not isinstance(data[2], tuple)
    ):
        raise ActorDiedError("invalid actor startup protocol")
    factory, names, args, kwargs = data
    return factory, names, args, ProcessActor._arguments(args, kwargs)


def _request_fields(
    data: object, methods: Mapping[str, Callable[..., object]]
) -> tuple[int, str, tuple[object, ...], dict[str, object]]:
    if (
        not isinstance(data, tuple)
        or len(data) != 4
        or type(data[0]) is not int
        or data[0] < 1
        or not isinstance(data[1], str)
        or data[1] not in methods
        or not isinstance(data[2], tuple)
    ):
        raise ActorDiedError("invalid actor request protocol")
    request_id, name, args, kwargs = data
    return request_id, name, args, ProcessActor._arguments(args, kwargs)


def _worker(
    connection: Connection,
    startup: bytes,
    limit: int,
    task_cancellation: _CancellationConnection | None = None,
) -> None:
    """Top-level spawn target; registered methods execute sequentially here."""
    stream_shutdown: Callable[[], object] | None = None
    problem: BaseException | None = None
    try:
        try:
            factory, names, args, kwargs = _startup_fields(_unpack(startup))
            if task_cancellation is None:
                instance = _synchronous(factory(*args, **kwargs))
            else:
                # An OS handle is passed only by the internal task bootstrap,
                # never through the serialized actor request/startup protocol.
                from graph_sail.process_execution import _TaskWorker
                from graph_sail.process_task_streams import _StreamWorker

                if factory is _TaskWorker and names == ("invoke",) and not args and not kwargs:
                    instance = _TaskWorker(task_cancellation)
                elif (
                    factory is _StreamWorker
                    and names == ("advance", "finish")
                    and len(args) == 3
                    and not kwargs
                ):
                    instance = _StreamWorker(task_cancellation, *args)
                    stream_shutdown = instance.finish
                else:
                    raise TypeError("cancellation bootstrap is restricted to the task worker")
            methods: dict[str, Callable[..., object]] = {}
            for name in names:
                method = getattr(instance, name)
                if not callable(method) or inspect.iscoroutinefunction(method):
                    raise TypeError(f"registered method {name} is not a synchronous callable")
                methods[name] = method
        except Exception as error:
            connection.send_bytes(_response(0, "startup", error, limit))
            return
        connection.send_bytes(_pack((0, "ready", None), limit))
        while True:
            request = _unpack(connection.recv_bytes(limit))
            if request is None:
                return
            request_id, name, args, kwargs = _request_fields(request, methods)
            try:
                result = _synchronous(methods[name](*args, **kwargs))
            except Exception as error:
                response = _response(request_id, "method", error, limit)
            else:
                try:
                    response = _pack((request_id, "ok", result), limit)
                except ActorSerializationError as error:
                    response = _response(request_id, "serialization", error, limit)
            connection.send_bytes(response)
    except (EOFError, OSError):
        # Parent close/death makes the private pipe unusable; no replay is safe.
        return
    except BaseException as error:
        problem = error
    finally:
        # Only our exact internal stream worker owns a generator. Ordinary
        # actor instances do not gain an implicit user-controlled close hook.
        # Shutdown runs on this same worker thread, never beside next().
        operations: list[Callable[[], object]] = []
        if stream_shutdown is not None:
            operations.append(stream_shutdown)
        operations.append(connection.close)
        if task_cancellation is not None:
            operations.append(task_cancellation.close)
        for operation in operations:
            try:
                operation()
            except BaseException as cleanup:
                problem = cleanup if problem is None else _preserve_failure(problem, cleanup)
        if problem is not None:
            raise problem


class ActorCall:
    """An asynchronous result handle; a wait timeout does not cancel execution.

    No callbacks execute on the transport thread. Completed handles are owned by
    the caller and are not retained in actor history.
    """

    def __init__(self, request_id: int, method: str) -> None:
        self._request_id = request_id
        self._method = method
        self._future: Future[object] = Future()
        self._completion = _CompletionHub()
        self._elapsed_seconds: float | None = None

    @property
    def request_id(self) -> int:
        return self._request_id

    @property
    def method(self) -> str:
        return self._method

    def result(self, timeout: float | None = None) -> object:
        if timeout is not None:
            _seconds(timeout, "timeout")
        return self._future.result(timeout)

    async def result_async(self, timeout: float | None = None) -> object:
        """Await a borrowed result; cancelling this wait never cancels the request.

        Source failures get fresh AsyncSourceError/AsyncSourceControlError
        wrappers. Explicit request cancellation is AsyncSourceCancelledError.
        Actor ownership and shutdown remain with the original ProcessActor.
        """
        if timeout is not None:
            _seconds(timeout, "timeout")
        return await _await_snapshot(self._completion, self._async_snapshot, timeout)

    def _async_snapshot(self) -> _Snapshot:
        if not self._future.done():
            return _Snapshot(False)
        if self._future.cancelled():
            return _Snapshot(True, cancelled=True)
        failure = self._future.exception(timeout=0)
        if failure is not None:
            return _Snapshot(True, failure=failure)
        return _Snapshot(True, value=self._future.result(timeout=0))

    def _set_result(self, value: object) -> None:
        self._future.set_result(value)
        self._completion.notify(terminal=True)

    def _set_exception(self, error: BaseException) -> None:
        self._future.set_exception(error)
        self._completion.notify(terminal=True)

    def exception(self, timeout: float | None = None) -> BaseException | None:
        if timeout is not None:
            _seconds(timeout, "timeout")
        return self._future.exception(timeout)

    def cancel(self) -> bool:
        """Cancel only if the broker has not dispatched the request yet."""
        cancelled = self._future.cancel()
        if cancelled:
            self._completion.notify(terminal=True)
        return cancelled

    def cancelled(self) -> bool:
        return self._future.cancelled()

    def done(self) -> bool:
        return self._future.done()

    def running(self) -> bool:
        return self._future.running()

    @property
    def elapsed_seconds(self) -> float | None:
        """Observed dispatch-to-response time, including transport/serialization."""
        return self._elapsed_seconds


class ProcessActor:
    """One local stateful spawn process, explicitly closed by its owner.

    ``close(timeout)`` drains accepted calls within the budget, then terminates
    the worker if necessary. ``terminate()`` abandons pending work immediately.
    Both join the child; forced shutdown can leave application side effects.
    """

    def __init__(
        self,
        registry: ActorRegistry,
        name: str,
        *,
        args: tuple[object, ...] = (),
        kwargs: Mapping[str, object] | None = None,
        config: ActorConfig | None = None,
    ) -> None:
        self._initialize(registry, name, args=args, kwargs=kwargs, config=config)

    @classmethod
    def _for_task_worker(cls, config: ActorConfig) -> tuple[ProcessActor, _CancellationConnection]:
        """Private bootstrap: child owns receive-only EOF cancellation observation.

        The returned sender has one owner, the process task backend. Closing it
        is irrevocable cancellation; a cancelled worker must never be reused.
        """
        from graph_sail.process_execution import _TaskWorker

        registry = ActorRegistry({"task_worker": ActorDefinition(_TaskWorker, ("invoke",))})
        receiver, sender = multiprocessing.get_context("spawn").Pipe(duplex=False)
        ready = False
        try:
            actor = cls.__new__(cls)
            actor._initialize(registry, "task_worker", config=config, task_cancellation=receiver)
            ready = True
            receiver.close()
        except BaseException as error:
            primary = error
            for endpoint in (receiver, sender):
                try:
                    endpoint.close()
                except BaseException as cleanup:
                    primary = _preserve_failure(primary, cleanup)
            if ready:
                try:
                    actor.terminate()
                except BaseException as cleanup:
                    primary = _preserve_failure(primary, cleanup)
            if primary is not error:
                raise primary from error
            raise
        return actor, sender

    @classmethod
    def _for_stream_worker(
        cls,
        function: object,
        args: tuple[object, ...],
        kwargs: dict[str, object],
        config: ActorConfig,
    ) -> tuple[ProcessActor, _CancellationConnection]:
        """Closed internal generator bootstrap; no public arbitrary factory hook."""
        from graph_sail.process_task_streams import _StreamWorker

        registry = ActorRegistry(
            {"stream_worker": ActorDefinition(_StreamWorker, ("advance", "finish"))}
        )
        # Admit the entire startup frame before allocating OS handles. The
        # initializer also serializes it independently at the spawn boundary.
        _pack(
            (_StreamWorker, ("advance", "finish"), (function, args, kwargs), {}),
            config.max_message_bytes,
        )
        actor = cls.__new__(cls)
        receiver, sender = multiprocessing.get_context("spawn").Pipe(duplex=False)
        ownership = _StreamStartupCleanup(actor, (receiver, sender))
        try:
            actor._initialize(
                registry,
                "stream_worker",
                args=(function, args, kwargs),
                config=config,
                task_cancellation=receiver,
                _stream_ownership=ownership,
            )
            receiver.close()
        except BaseException as error:
            primary = ownership.retain(error)
            if primary is not error:
                raise primary from error
            raise
        return actor, sender

    def _initialize(
        self,
        registry: ActorRegistry,
        name: str,
        *,
        args: tuple[object, ...] = (),
        kwargs: Mapping[str, object] | None = None,
        config: ActorConfig | None = None,
        task_cancellation: _CancellationConnection | None = None,
        _stream_ownership: _StreamStartupCleanup | None = None,
    ) -> None:
        if not isinstance(registry, ActorRegistry):
            raise ValidationError("registry must be an ActorRegistry")
        _identifier(name, "actor name")
        if name not in registry.actors:
            raise ValidationError(f"actor {name} is not registered")
        definition = registry.actors[name]
        definition = ActorDefinition(definition.factory, definition.methods)
        if config is None:
            config = ActorConfig()
        if not isinstance(config, ActorConfig):
            raise ValidationError("config must be an ActorConfig")
        config = ActorConfig(
            config.max_pending,
            config.max_message_bytes,
            config.startup_timeout_seconds,
            config.method_timeout_seconds,
        )
        constructor_kwargs = self._arguments(args, kwargs)
        startup = _pack(
            (definition.factory, definition.methods, args, constructor_kwargs),
            config.max_message_bytes,
        )
        self._config = config
        self._name = name
        self._methods = definition.methods
        self._condition = Condition()
        self._queue: deque[tuple[ActorCall, bytes]] = deque()
        self._pending: dict[int, ActorCall] = {}
        self._next_id = 1
        self._serializing = False
        self._closing = False
        self._abort: ActorError | None = None
        self._failure: ActorError | None = None
        self._closed = Event()
        self._exitcode: int | None = None
        self._process_closed = False
        self._cleanup_error: ActorDiedError | None = None
        context = multiprocessing.get_context("spawn")
        self._connection, child = context.Pipe()
        if _stream_ownership is not None:
            # The stream owner must retain this otherwise-local endpoint even
            # when Process construction fails before the ordinary startup try.
            _stream_ownership._child_endpoint = child
        self._process = context.Process(
            target=_worker,
            args=(child, startup, config.max_message_bytes)
            if task_cancellation is None
            else (child, startup, config.max_message_bytes, task_cancellation),
            name=f"graph-sail-actor-{name}",
            daemon=True,
        )
        try:
            self._process.start()
            child.close()
            if _stream_ownership is not None:
                _stream_ownership._child_closed = True
            self._pid = self._process.pid
            reply = self._receive(time.monotonic() + config.startup_timeout_seconds)
            kind, value = self._reply(reply, 0)
            if kind != "ready":
                raise ActorStartupError(str(self._remote_error(value)))
        except BaseException as error:
            if _stream_ownership is not None:
                # Its outer owner has both actor-pipe ends plus cancellation
                # endpoints. Preserve the original control exception and let
                # that owner attempt all resources exactly at one boundary.
                if isinstance(error, Exception) and not isinstance(error, ActorError):
                    raise ActorStartupError(
                        f"actor startup failed: {type(error).__name__}"
                    ) from error
                raise
            try:
                child.close()
            finally:
                self._cleanup_process(graceful=not isinstance(error, ActorTimeoutError))
            if self._cleanup_error is not None:
                raise self._cleanup_error from error
            if isinstance(error, Exception) and not isinstance(error, ActorError):
                raise ActorStartupError(f"actor startup failed: {type(error).__name__}") from error
            raise
        self._thread = Thread(target=self._serve, name=f"actor-mailbox-{name}", daemon=True)
        try:
            self._thread.start()
        except BaseException as error:
            if _stream_ownership is not None:
                raise
            self._cleanup_process(graceful=False)
            if self._cleanup_error is not None:
                raise self._cleanup_error from error
            raise

    @staticmethod
    def _arguments(
        args: tuple[object, ...], kwargs: Mapping[str, object] | None
    ) -> dict[str, object]:
        if not isinstance(args, tuple) or len(args) > 256:
            raise ValidationError("args must be a tuple containing at most 256 values")
        if kwargs is None:
            return {}
        if (
            not isinstance(kwargs, Mapping)
            or len(kwargs) > 256
            or any(not isinstance(key, str) or len(key) > 128 for key in kwargs)
        ):
            raise ValidationError("kwargs must be a mapping with at most 256 bounded string keys")
        return dict(kwargs)

    @property
    def config(self) -> ActorConfig:
        return self._config

    @property
    def name(self) -> str:
        return self._name

    @property
    def pid(self) -> int | None:
        return self._pid

    @property
    def pending_count(self) -> int:
        with self._condition:
            return len(self._pending)

    @property
    def alive(self) -> bool:
        with self._condition:
            # Only the broker reaps the worker. Process.is_alive() can consume
            # waitpid's exit status on POSIX and race with the broker's join().
            return not self._process_closed and not wait([self._process.sentinel], timeout=0)

    @property
    def failure(self) -> ActorError | None:
        with self._condition:
            return self._failure

    @property
    def exitcode(self) -> int | None:
        with self._condition:
            return self._exitcode

    def submit(
        self,
        method: str,
        *,
        args: tuple[object, ...] = (),
        kwargs: Mapping[str, object] | None = None,
    ) -> ActorCall:
        _identifier(method, "method")
        if method not in self._methods:
            raise ValidationError(f"method {method} is not registered for actor {self.name}")
        call_kwargs = self._arguments(args, kwargs)
        with self._condition:
            if self._serializing:
                raise ActorError("reentrant actor submission during serialization is not supported")
            if self._closing or self._closed.is_set():
                raise ActorClosedError("actor is closing or closed")
            if len(self._pending) >= self.config.max_pending:
                raise ActorQueueFullError(
                    "actor mailbox is full; wait for accepted calls to complete"
                )
            request_id = self._next_id
            self._serializing = True
            try:
                payload = _pack(
                    (request_id, method, args, call_kwargs), self.config.max_message_bytes
                )
            finally:
                self._serializing = False
            if self._closing or self._closed.is_set():
                raise ActorClosedError("actor closed while serializing arguments")
            handle = ActorCall(request_id, method)
            self._next_id += 1
            self._pending[request_id] = handle
            self._queue.append((handle, payload))
            self._condition.notify_all()
            return handle

    def close(self, timeout: float = 5.0) -> None:
        _seconds(timeout, "timeout")
        with self._condition:
            self._check_lifecycle_reentry()
            self._closing = True
            self._condition.notify_all()
        if not self._closed.wait(timeout):
            self._request_abort(
                ActorClosedError("actor close budget expired; accepted work abandoned")
            )
        self._thread.join()
        if self._cleanup_error is not None:
            raise self._cleanup_error

    def terminate(self) -> None:
        self._request_abort(
            ActorClosedError("actor explicitly terminated; accepted work abandoned")
        )
        self._thread.join()
        if self._cleanup_error is not None:
            raise self._cleanup_error

    def __enter__(self) -> ProcessActor:
        return self

    def __exit__(self, *exception_info: object) -> None:
        self.close()

    def _request_abort(self, error: ActorError) -> None:
        with self._condition:
            self._check_lifecycle_reentry()
            self._closing = True
            if self._abort is None:
                self._abort = error
            self._condition.notify_all()

    def _check_lifecycle_reentry(self) -> None:
        if self._serializing or current_thread() is self._thread:
            raise ActorError(
                "actor lifecycle cannot be reentered from serialization/transport hooks"
            )

    def _receive(self, deadline: float | None) -> object:
        while True:
            self._check_wait(deadline)
            if self._connection.poll(0.02):
                data = self._connection.recv_bytes(self.config.max_message_bytes)
                self._check_wait(deadline)
                value = _unpack(data)
                self._check_wait(deadline)
                return value
            if not self._process.is_alive():
                raise ActorDiedError(f"actor worker exited with code {self._process.exitcode}")

    def _check_wait(self, deadline: float | None) -> None:
        with self._condition:
            if self._abort is not None:
                raise self._abort
        if deadline is not None and time.monotonic() >= deadline:
            raise ActorTimeoutError("actor startup/invocation budget expired; actor state is lost")

    @staticmethod
    def _reply(reply: object, request_id: int) -> tuple[str, object]:
        if (
            not isinstance(reply, tuple)
            or len(reply) != 3
            or type(reply[0]) is not int
            or reply[0] != request_id
            or reply[1] not in ("ready", "ok", "error")
        ):
            raise ActorDiedError("invalid actor response protocol")
        return reply[1], reply[2]

    @staticmethod
    def _remote_error(value: object) -> ActorError:
        if (
            not isinstance(value, tuple)
            or len(value) != 3
            or not all(isinstance(part, str) for part in value)
            or value[0] not in ("startup", "method", "serialization")
        ):
            raise ActorDiedError("invalid actor error response")
        phase, kind, message = value
        if phase == "serialization":
            return ActorSerializationError(message)
        return ActorRemoteError(phase, kind, message)

    def _serve(self) -> None:
        failure: ActorError | None = None
        try:
            while True:
                with self._condition:
                    if self._abort is not None:
                        raise self._abort
                    if not self._queue:
                        if self._closing:
                            break
                        if not self._process.is_alive():
                            raise ActorDiedError("idle actor worker exited")
                        self._condition.wait(0.02)
                        continue
                    handle, payload = self._queue.popleft()
                    if not handle._future.set_running_or_notify_cancel():
                        del self._pending[handle.request_id]
                        continue
                    started = time.monotonic()
                    timeout = self.config.method_timeout_seconds
                    deadline = None if timeout is None else started + timeout
                    # Dispatch and stop requests share one admission lock. The
                    # worker has acknowledged startup/the previous response and
                    # is ready to read; no other parent thread writes this pipe.
                    self._connection.send_bytes(payload)
                kind, value = self._reply(self._receive(deadline), handle.request_id)
                if kind == "ready":
                    raise ActorDiedError("unexpected actor startup response")
                remote_error = self._remote_error(value) if kind == "error" else None
                handle._elapsed_seconds = time.monotonic() - started
                with self._condition:
                    del self._pending[handle.request_id]
                if remote_error is not None:
                    handle._set_exception(remote_error)
                else:
                    handle._set_result(value)
            self._connection.send_bytes(_pack(None, self.config.max_message_bytes))
            self._process.join(0.5)
        except ActorError as error:
            failure = error
        except BaseException as error:
            # A trusted reconstruction hook can raise SystemExit in this broker
            # thread. It is worker-transport loss, not a healthy explicit close.
            failure = ActorDiedError(f"actor transport failed: {type(error).__name__}")
        finally:
            with self._condition:
                self._closing = True
                self._failure = failure
                pending = list(self._pending.values())
                self._pending.clear()
                self._queue.clear()
            try:
                for handle in pending:
                    # Atomically prevent cancellation racing with failure delivery.
                    if handle._future.running() or handle._future.set_running_or_notify_cancel():
                        handle._set_exception(failure or ActorClosedError("actor closed"))
            finally:
                self._cleanup_process(
                    graceful=failure is None or isinstance(failure, ActorDiedError)
                )

    def _cleanup_process(self, *, graceful: bool) -> None:
        problems: list[str] = []

        def attempt(label: str, operation: Callable[[], object]) -> bool:
            try:
                operation()
                return True
            except Exception as error:
                problems.append(f"{label}: {type(error).__name__[:128]}")
                return False

        def alive() -> bool:
            try:
                return self._process.is_alive()
            except Exception as error:
                problems.append(f"inspect worker: {type(error).__name__[:128]}")
                # An unreadable process state is not evidence of termination.
                return True

        # Closing our pipe first lets an idle worker exit normally on EOF before
        # escalation; waiting with that pipe open would force-kill an idle actor.
        attempt("close pipe", self._connection.close)
        try:
            if self._process.pid is not None:
                # EOF precedes complete interpreter teardown, especially when
                # multiprocessing coverage/finalizers still need to flush.
                attempt("initial join", lambda: self._process.join(5.0 if graceful else 0.0))
                if alive():
                    attempt("terminate", self._process.terminate)
                attempt("termination join", lambda: self._process.join(1.0))
                if alive():
                    attempt("kill", self._process.kill)
                    attempt("kill join", lambda: self._process.join(1.0))
                if alive():
                    problems.append("worker may still be alive; operating-system cleanup required")
                self._exitcode = self._process.exitcode
        except Exception as error:
            problems.append(f"inspect process metadata: {type(error).__name__[:128]}")
        finally:
            with self._condition:
                if not alive():
                    self._process_closed = attempt("close process handle", self._process.close)
                if problems:
                    self._cleanup_error = ActorDiedError(
                        "actor cleanup failed: " + "; ".join(problems)
                    )
                    self._failure = self._cleanup_error
                self._closed.set()
