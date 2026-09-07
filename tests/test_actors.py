"""Real spawn-process tests; factories must remain importable module-level names."""

from __future__ import annotations

import multiprocessing
import os
import threading
import time
from concurrent.futures import CancelledError
from pathlib import Path

import pytest

from graph_sail.actors import (
    ActorCall,
    ActorClosedError,
    ActorConfig,
    ActorDefinition,
    ActorDiedError,
    ActorError,
    ActorQueueFullError,
    ActorRegistry,
    ActorRemoteError,
    ActorSerializationError,
    ActorStartupError,
    ActorTimeoutError,
    ProcessActor,
)
from graph_sail.errors import ValidationError


class Counter:
    def __init__(self, initial=0):
        self.value = initial

    def add(self, amount=1):
        self.value += amount
        return self.value

    def pid(self):
        return os.getpid()

    def mutate(self, values):
        values.append(self.value)
        return values

    def fail(self):
        self.value += 1
        raise ValueError("mutation is not rolled back")

    def block(self, started, release):
        Path(started).touch()
        deadline = time.monotonic() + 10
        while not Path(release).exists():
            if time.monotonic() >= deadline:
                raise RuntimeError("test release marker did not arrive")
            time.sleep(0.01)
        return self.value

    def crash(self):
        os._exit(23)

    def stop(self):
        raise SystemExit(7)

    def unpicklable(self):
        self.value += 1
        return lambda: self.value

    def large(self):
        return "x" * 100_000

    def long_error(self):
        raise ValueError("x" * 10_000)

    def broken_error(self):
        raise BrokenError()

    def unicode_error(self):
        error_type = type("\U0001f600" * 256, (Exception,), {})
        raise error_type("\U0001f600" * 1000)

    def awaitable(self):
        return coroutine()


class BrokenError(Exception):
    def __str__(self):
        raise ValueError("message conversion failed")


class FailedFactory:
    def __init__(self):
        raise ValueError("constructor failed")


class SlowFactory:
    def __init__(self):
        time.sleep(10)

    def run(self):
        return None


class MissingMethod:
    value = 1

    async def asynchronous(self):
        return 1


async def coroutine():
    return 1


def awaitable_factory():
    return coroutine()


def crashing_factory():
    os._exit(17)


def counter_factory(initial=0):
    return Counter(initial)


METHODS = (
    "add",
    "pid",
    "mutate",
    "fail",
    "block",
    "crash",
    "stop",
    "unpicklable",
    "large",
    "long_error",
    "broken_error",
    "unicode_error",
    "awaitable",
)


def registry(factory=Counter, methods=METHODS):
    return ActorRegistry({"counter": ActorDefinition(factory, methods)})


def wait_until(predicate):
    deadline = time.monotonic() + 5
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail("condition was not observed within five seconds")
        time.sleep(0.005)


def assert_reaped(actor):
    assert not actor.alive
    assert actor.exitcode is not None
    assert actor.pending_count == 0
    assert all(child.pid != actor.pid for child in multiprocessing.active_children())
    assert not actor._thread.is_alive()


def test_state_process_fifo_result_handles_and_clean_drain():
    with ProcessActor(registry(), "counter", args=(10,)) as actor:
        handles = [actor.submit("add", args=(amount,)) for amount in (2, 5, -3)]
        assert [handle.result(5) for handle in handles] == [12, 17, 14]
        assert [handle.request_id for handle in handles] == [1, 2, 3]
        assert all(handle.method == "add" and handle.done() for handle in handles)
        assert all(handle.elapsed_seconds >= 0 for handle in handles)
        assert all(handle.exception() is None for handle in handles)
        assert actor.submit("pid").result(5) == actor.pid != os.getpid()
        assert actor.config == ActorConfig()
        assert actor.name == "counter"
        last = actor.submit("add", kwargs={"amount": 6})
    assert last.result() == 20
    assert actor.failure is None
    assert actor.exitcode == 0
    assert_reaped(actor)
    actor.close()
    actor.terminate()
    with pytest.raises(ActorClosedError):
        actor.submit("add")


def test_independent_processes_and_serialization_copy_boundary(tmp_path):
    with (
        ProcessActor(registry(counter_factory), "counter", kwargs={"initial": 7}) as left,
        ProcessActor(registry(), "counter", args=(50,)) as right,
    ):
        assert left.pid != right.pid
        started = tmp_path / "started"
        release = tmp_path / "release"
        blocked = left.submit("block", args=(str(started), str(release)))
        wait_until(started.exists)
        # An independent process completes while the first process is blocked.
        assert right.submit("add", args=(2,)).result(2) == 52
        values = [1]
        copied = left.submit("mutate", args=(values,))
        values.append(99)
        release.touch()
        assert blocked.result(5) == 7
        result = copied.result(5)
        assert result == [1, 7]
        assert values == [1, 99]
        result.append(100)
        assert left.submit("add", args=(0,)).result(5) == 7


def test_bounded_pending_cancellation_and_wait_timeout(tmp_path):
    with ProcessActor(registry(), "counter", config=ActorConfig(max_pending=2)) as actor:
        started, release = tmp_path / "started", tmp_path / "release"
        running = actor.submit("block", args=(str(started), str(release)))
        wait_until(started.exists)
        queued = actor.submit("add", args=(100,))
        assert running.running() and not running.cancel()
        assert running.elapsed_seconds is None
        assert actor.pending_count == 2
        with pytest.raises(ActorQueueFullError):
            actor.submit("add")
        with pytest.raises(TimeoutError):
            running.result(0.01)
        with pytest.raises(TimeoutError):
            running.exception(0)
        assert queued.cancel() and queued.cancelled()
        with pytest.raises(CancelledError):
            queued.result(0)
        release.touch()
        assert running.result(5) == 0
        wait_until(lambda: actor.pending_count == 0)
        assert actor.submit("add").result(5) == 1


def test_concurrent_submitters_fifo_is_admission_order(tmp_path):
    with ProcessActor(registry(), "counter") as actor:
        started, release = tmp_path / "started", tmp_path / "release"
        block = actor.submit("block", args=(str(started), str(release)))
        wait_until(started.exists)
        gate = threading.Barrier(3)
        handles = []

        def submit(amount):
            gate.wait(5)
            handles.append((actor.submit("add", args=(amount,)), amount))

        threads = [threading.Thread(target=submit, args=(amount,)) for amount in (3, 8)]
        for thread in threads:
            thread.start()
        gate.wait(5)
        for thread in threads:
            thread.join(5)
            assert not thread.is_alive()
        release.touch()
        assert block.result(5) == 0
        ordered = sorted(handles, key=lambda pair: pair[0].request_id)
        assert ordered[0][0].result(5) == ordered[0][1]
        assert ordered[1][0].result(5) == 11


def test_method_errors_continue_and_diagnostics_are_bounded():
    with ProcessActor(registry(), "counter", config=ActorConfig(max_message_bytes=1024)) as actor:
        failure = actor.submit("fail")
        with pytest.raises(ActorRemoteError) as caught:
            failure.result(5)
        assert caught.value.phase == "method"
        assert caught.value.remote_type == "ValueError"
        assert caught.value.remote_message == "mutation is not rolled back"
        assert failure.exception(0) is caught.value
        assert actor.submit("add").result(5) == 2
        with pytest.raises(ActorRemoteError) as long_error:
            actor.submit("long_error").result(5)
        assert len(long_error.value.remote_message) == 128
        with pytest.raises(ActorRemoteError, match="message unavailable"):
            actor.submit("broken_error").result(5)
        with pytest.raises(ActorRemoteError) as unicode_error:
            actor.submit("unicode_error").result(5)
        assert len(unicode_error.value.remote_type.encode("utf-8")) == 256
        assert len(unicode_error.value.remote_message.encode("utf-8")) == 128
        assert actor.submit("add").result(5) == 3
        with pytest.raises(ActorRemoteError, match="must not return awaitables"):
            actor.submit("awaitable").result(5)


def test_result_serialization_errors_do_not_restart_or_replay():
    with ProcessActor(registry(), "counter", config=ActorConfig(max_message_bytes=1024)) as actor:
        with pytest.raises(ActorSerializationError, match="cannot serialize"):
            actor.submit("unpicklable").result(5)
        with pytest.raises(ActorSerializationError, match="exceeds"):
            actor.submit("large").result(5)
        assert actor.submit("add").result(5) == 2
        with pytest.raises(ActorSerializationError):
            actor.submit("mutate", args=(lambda: None,))
        with pytest.raises(ActorSerializationError, match="exceeds"):
            actor.submit("mutate", args=(b"x" * 2000,))
        assert actor.pending_count == 0
        assert actor.submit("add").result(5) == 3


@pytest.mark.parametrize("method,code", [("crash", 23), ("stop", 7)])
def test_worker_death_fails_all_pending_and_reaps(method, code, tmp_path):
    actor = ProcessActor(registry(), "counter")
    try:
        started, release = tmp_path / "started", tmp_path / "release"
        block = actor.submit("block", args=(str(started), str(release)))
        wait_until(started.exists)
        death = actor.submit(method)
        after = actor.submit("add")
        release.touch()
        assert block.result(5) == 0
        for handle in (death, after):
            with pytest.raises(ActorDiedError):
                handle.result(5)
        wait_until(lambda: not actor.alive)
        assert isinstance(actor.failure, ActorDiedError)
    finally:
        actor.close()
    assert actor.exitcode == code
    assert_reaped(actor)


def test_idle_worker_death_is_detected():
    actor = ProcessActor(registry(), "counter")
    actor._process.terminate()
    wait_until(lambda: actor.failure is not None)
    actor.close()
    assert isinstance(actor.failure, ActorDiedError)
    assert_reaped(actor)


@pytest.mark.parametrize("action", ["terminate", "close"])
def test_forced_shutdown_stops_running_work_without_launching_next(action, tmp_path):
    actor = ProcessActor(registry(), "counter")
    started, release = tmp_path / "started", tmp_path / "release"
    running = actor.submit("block", args=(str(started), str(release)))
    wait_until(started.exists)
    next_started = tmp_path / "next-started"
    queued = actor.submit("block", args=(str(next_started), str(release)))
    before = time.monotonic()
    if action == "terminate":
        actor.terminate()
    else:
        actor.close(timeout=0.01)
    assert time.monotonic() - before < 4
    for handle in (running, queued):
        with pytest.raises(ActorClosedError):
            handle.result(0)
    assert not next_started.exists()
    assert_reaped(actor)


def test_method_budget_terminates_actor_and_pending_work(tmp_path):
    actor = ProcessActor(registry(), "counter", config=ActorConfig(method_timeout_seconds=0.1))
    try:
        call = actor.submit("block", args=(str(tmp_path / "started"), str(tmp_path / "never")))
        after = actor.submit("add")
        for handle in (call, after):
            with pytest.raises(ActorTimeoutError):
                handle.result(5)
    finally:
        actor.close()
    assert isinstance(actor.failure, ActorTimeoutError)
    assert_reaped(actor)


@pytest.mark.parametrize(
    "factory,methods,error,match",
    [
        (FailedFactory, ("run",), ActorStartupError, "constructor failed"),
        (MissingMethod, ("missing",), ActorStartupError, "AttributeError"),
        (MissingMethod, ("value",), ActorStartupError, "not a synchronous callable"),
        (MissingMethod, ("asynchronous",), ActorStartupError, "not a synchronous callable"),
        (awaitable_factory, ("run",), ActorStartupError, "must not return awaitables"),
        (crashing_factory, ("run",), (ActorStartupError, ActorDiedError), "actor"),
        (SlowFactory, ("run",), ActorTimeoutError, "budget expired"),
    ],
)
def test_startup_failure_cleans_up(factory, methods, error, match):
    before = {child.pid for child in multiprocessing.active_children()}
    config = ActorConfig(startup_timeout_seconds=0.15 if factory is SlowFactory else 30)
    with pytest.raises(error, match=match):
        ProcessActor(registry(factory, methods), "counter", config=config)
    assert {child.pid for child in multiprocessing.active_children()} == before


@pytest.mark.parametrize(
    "options",
    [
        {"max_pending": 0},
        {"max_pending": 1025},
        {"max_pending": True},
        {"max_message_bytes": 1023},
        {"max_message_bytes": 16_777_217},
        {"max_pending": 100, "max_message_bytes": 1_048_576},
        {"startup_timeout_seconds": 0},
        {"method_timeout_seconds": 0},
        {"startup_timeout_seconds": float("inf")},
        {"startup_timeout_seconds": float("nan")},
        {"method_timeout_seconds": float("nan")},
        {"method_timeout_seconds": float("inf")},
        {"startup_timeout_seconds": True},
        {"method_timeout_seconds": True},
        {"method_timeout_seconds": "1"},
        {"startup_timeout_seconds": 10**1000},
        {"method_timeout_seconds": 10**1000},
    ],
)
def test_config_rejects_invalid_options(options):
    with pytest.raises(ValidationError):
        ActorConfig(**options)


@pytest.mark.parametrize("factory", [None, 3, Counter(), lambda: Counter(), coroutine])
def test_factory_registration_is_explicit_importable_and_synchronous(factory):
    with pytest.raises(ValidationError):
        ActorDefinition(factory, ("add",))


@pytest.mark.parametrize(
    "methods",
    [(), [], ("add", "add"), ("_secret",), ("__dict__",), ("a.b",), ("é",), ("a" * 129,), (3,)],
)
def test_method_allowlist_rejects_malformed_values(methods):
    with pytest.raises(ValidationError):
        ActorDefinition(Counter, methods)


def test_registry_is_snapshot_and_rejects_bad_entries():
    source = {"counter": ActorDefinition(Counter, ("add",))}
    snapshot = ActorRegistry(source)
    source.clear()
    assert "counter" in snapshot.actors
    with pytest.raises(TypeError):
        snapshot.actors["other"] = ActorDefinition(Counter, ("add",))
    for bad in ({}, [], {"_hidden": ActorDefinition(Counter, ("add",))}, {"counter": Counter}):
        with pytest.raises(ValidationError):
            ActorRegistry(bad)


def test_constructor_preflight_before_child_start(monkeypatch):
    import graph_sail.actors as module

    def forbidden(*args):
        pytest.fail("invalid constructor must not create a process")

    monkeypatch.setattr(module.multiprocessing, "get_context", forbidden)
    for target, name, options in (
        (None, "counter", {}),
        (registry(), "unknown", {}),
        (registry(), "_bad", {}),
        (registry(), "counter", {"config": {}}),
        (registry(), "counter", {"args": []}),
        (registry(), "counter", {"kwargs": {1: 2}}),
    ):
        with pytest.raises(ValidationError):
            ProcessActor(target, name, **options)
    config = ActorConfig()
    object.__setattr__(config, "max_pending", -1)
    with pytest.raises(ValidationError):
        ProcessActor(registry(), "counter", config=config)
    with pytest.raises(ActorSerializationError):
        ProcessActor(registry(), "counter", args=(lambda: None,))


def test_submit_and_handle_validation_without_side_effects():
    with ProcessActor(registry(), "counter") as actor:
        for method, args, kwargs in (
            ("missing", (), None),
            ("__dict__", (), None),
            (3, (), None),
            ("add", [], None),
            ("add", tuple(range(257)), None),
            ("add", (), {1: 2}),
            ("add", (), {"a" * 129: 1}),
            ("add", (), {str(n): n for n in range(257)}),
            ("add", (), []),
        ):
            with pytest.raises(ValidationError):
                actor.submit(method, args=args, kwargs=kwargs)
        handle = actor.submit("add")
        for timeout in (-1, float("inf"), float("nan"), True, "1", 86_401, 10**1000):
            with pytest.raises(ValidationError):
                handle.result(timeout)
            with pytest.raises(ValidationError):
                handle.exception(timeout)
            with pytest.raises(ValidationError):
                actor.close(timeout)
        assert handle.result(5) == 1


@pytest.mark.parametrize(
    "value", [None, [], (1, "ok"), (True, "ok", 3), (2, "ok", 3), (1, "unknown", 3)]
)
def test_malformed_response_is_terminal(value):
    with pytest.raises(ActorDiedError):
        ProcessActor._reply(value, 1)


@pytest.mark.parametrize("value", [None, ("method",), ("method", 3, "x"), ("bad", "E", "x")])
def test_malformed_remote_exception_is_terminal(value):
    with pytest.raises(ActorDiedError):
        ProcessActor._remote_error(value)


def test_handle_identity_is_read_only():
    handle = ActorCall(1, "add")
    with pytest.raises(AttributeError):
        handle.request_id = 7
    with pytest.raises(AttributeError):
        handle.method = "other"


@pytest.mark.parametrize("reply", [(1, "ready", None), (1, "error", ("bad", "E", "x"))])
def test_bad_reply_fails_current_handle_too(monkeypatch, reply):
    with ProcessActor(registry(), "counter") as actor:
        monkeypatch.setattr(actor, "_receive", lambda deadline: reply)
        handle = actor.submit("add")
        with pytest.raises(ActorDiedError):
            handle.result(5)
    assert_reaped(actor)


@pytest.mark.parametrize(
    "data",
    [
        None,
        (),
        (Counter, [], (), {}),
        (Counter, (3,), (), {}),
        (3, ("add",), (), {}),
        (Counter, ("add",), [], {}),
    ],
)
def test_startup_protocol_rejects_malformed_payload(data):
    from graph_sail.actors import _startup_fields

    with pytest.raises(ActorDiedError):
        _startup_fields(data)


@pytest.mark.parametrize(
    "data",
    [
        None,
        (),
        (True, "add", (), {}),
        (0, "add", (), {}),
        (1, 3, (), {}),
        (1, "other", (), {}),
        (1, "add", [], {}),
    ],
)
def test_request_protocol_rejects_malformed_payload(data):
    from graph_sail.actors import _request_fields

    with pytest.raises(ActorDiedError):
        _request_fields(data, {"add": Counter().add})


def test_non_coroutine_awaitable_is_rejected():
    from graph_sail.actors import _synchronous

    class Awaitable:
        def __await__(self):
            yield

    with pytest.raises(TypeError, match="awaitables"):
        _synchronous(Awaitable())


def test_worker_parent_eof_exits_cleanly_without_shutdown_message():
    from graph_sail.actors import _pack, _unpack, _worker

    parent, child = multiprocessing.get_context("spawn").Pipe()
    worker = threading.Thread(
        target=_worker, args=(child, _pack((Counter, ("add",), (), {}), 1024), 1024)
    )
    worker.start()
    assert parent.poll(5)
    assert _unpack(parent.recv_bytes(1024)) == (0, "ready", None)
    parent.close()
    worker.join(5)
    assert not worker.is_alive()


def test_mailbox_thread_start_failure_cleans_up(monkeypatch):
    import graph_sail.actors as module

    before = {child.pid for child in multiprocessing.active_children()}

    def failed_start(self):
        raise RuntimeError("thread creation failed")

    monkeypatch.setattr(module.Thread, "start", failed_start)
    with pytest.raises(RuntimeError, match="thread creation failed"):
        ProcessActor(registry(), "counter")
    assert {child.pid for child in multiprocessing.active_children()} == before


def test_cancellation_racing_failure_settlement_still_joins_worker(tmp_path, monkeypatch):
    actor = ProcessActor(registry(), "counter")
    started, release = tmp_path / "started", tmp_path / "release"
    running = actor.submit("block", args=(str(started), str(release)))
    wait_until(started.exists)
    queued = actor.submit("add")
    original = queued._future.set_running_or_notify_cancel

    def cancel_at_settlement():
        assert queued.cancel()
        return original()

    monkeypatch.setattr(queued._future, "set_running_or_notify_cancel", cancel_at_settlement)
    actor.terminate()
    assert queued.cancelled()
    with pytest.raises(ActorClosedError):
        running.result(0)
    assert_reaped(actor)


@pytest.mark.parametrize("action", ["submit", "close", "terminate"])
def test_serialization_reentry_cannot_reuse_ids_or_close_actor(action):
    with ProcessActor(registry(), "counter") as actor:

        class Reentrant:
            def __reduce__(self):
                if action == "submit":
                    actor.submit("add")
                else:
                    getattr(actor, action)()
                return int, (1,)

        with pytest.raises(ActorSerializationError) as caught:
            actor.submit("add", args=(Reentrant(),))
        assert isinstance(caught.value.__cause__, ActorError)
        assert actor.pending_count == 0
        assert actor.alive
        accepted = actor.submit("add", args=(2,))
        assert accepted.request_id == 1
        assert accepted.result(5) == 2


@pytest.mark.parametrize("operation", ["terminate", "join", "close", "pipe_close"])
def test_cleanup_os_errors_are_reported_and_best_effort_reaps(operation, tmp_path, monkeypatch):
    actor = ProcessActor(registry(), "counter")
    process = actor._process
    original_kill, original_join = process.kill, process.join
    original_close, original_pipe_close = process.close, actor._connection.close
    started, release = tmp_path / "started", tmp_path / "release"
    call = actor.submit("block", args=(str(started), str(release)))
    wait_until(started.exists)

    def os_error(*args):
        raise OSError("injected OS cleanup failure")

    target = actor._connection if operation == "pipe_close" else process
    monkeypatch.setattr(target, "close" if operation == "pipe_close" else operation, os_error)
    try:
        with pytest.raises(ActorDiedError, match="cleanup failed"):
            actor.terminate()
        assert actor.failure is actor._cleanup_error
        assert actor._closed.is_set()
        assert not actor._thread.is_alive()
        with pytest.raises(ActorClosedError):
            call.result(0)
        with pytest.raises(ActorDiedError, match="cleanup failed"):
            actor.close()
    finally:
        monkeypatch.undo()
        if not actor._process_closed:
            if process.is_alive():
                original_kill()
            original_join(5)
            original_close()
            actor._process_closed = True
        original_pipe_close()
    assert not actor.alive
    assert all(child.pid != actor.pid for child in multiprocessing.active_children())


def test_failed_kill_does_not_claim_worker_terminated(tmp_path, monkeypatch):
    actor = ProcessActor(registry(), "counter")
    process = actor._process
    original_kill = process.kill
    started, release = tmp_path / "started", tmp_path / "release"
    actor.submit("block", args=(str(started), str(release)))
    wait_until(started.exists)

    def denied():
        raise OSError("injected permission failure")

    monkeypatch.setattr(process, "terminate", denied)
    monkeypatch.setattr(process, "kill", denied)
    try:
        with pytest.raises(ActorDiedError, match="may still be alive"):
            actor.terminate()
        assert actor.alive
        assert actor.exitcode is None
        assert actor._closed.is_set()
    finally:
        monkeypatch.undo()
        original_kill()
        process.join(5)
        process.close()
        actor._process_closed = True
    assert not actor.alive
    assert all(child.pid != actor.pid for child in multiprocessing.active_children())


def test_budget_expiring_during_parent_deserialization_is_observed(monkeypatch):
    import graph_sail.actors as module

    with ProcessActor(
        registry(), "counter", config=ActorConfig(method_timeout_seconds=0.1)
    ) as actor:
        original = module._unpack

        def slow_unpack(data):
            value = original(data)
            time.sleep(0.2)
            return value

        monkeypatch.setattr(module, "_unpack", slow_unpack)
        call = actor.submit("add")
        with pytest.raises(ActorTimeoutError):
            call.result(5)
        assert call.elapsed_seconds is None
    assert_reaped(actor)


def test_shutdown_requested_during_parent_deserialization_is_observed(monkeypatch):
    import graph_sail.actors as module

    actor = ProcessActor(registry(), "counter")
    entered, release = threading.Event(), threading.Event()
    original = module._unpack

    def paused_unpack(data):
        value = original(data)
        entered.set()
        assert release.wait(5)
        return value

    monkeypatch.setattr(module, "_unpack", paused_unpack)
    call = actor.submit("add")
    assert entered.wait(5)
    closer = threading.Thread(target=actor.terminate)
    closer.start()
    wait_until(lambda: actor._abort is not None)
    release.set()
    closer.join(5)
    assert not closer.is_alive()
    with pytest.raises(ActorClosedError):
        call.result(0)
    assert_reaped(actor)


def test_transport_control_exception_is_recorded_as_failure(monkeypatch):
    import graph_sail.actors as module

    with ProcessActor(registry(), "counter") as actor:

        def terminate_broker(data):
            raise SystemExit(4)

        monkeypatch.setattr(module, "_unpack", terminate_broker)
        with pytest.raises(ActorDiedError, match="SystemExit"):
            actor.submit("add").result(5)
    assert isinstance(actor.failure, ActorDiedError)
    assert_reaped(actor)
