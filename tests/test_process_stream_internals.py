from __future__ import annotations

from threading import Event
from types import SimpleNamespace

import pytest
from process_stream_functions import counted, empty

from graph_sail import (
    ActorDiedError,
    ActorTimeoutError,
    CancellationToken,
    TaskCancelled,
    TaskStreamConfig,
    TaskStreamContext,
    ValidationError,
)
from graph_sail import process_task_streams as implementation
from graph_sail.actors import (
    ActorCall,
    ActorConfig,
    ActorRemoteError,
    ProcessActor,
    _StreamStartupCleanup,
)


class Endpoint:
    def __init__(self, failures=()):
        self.failures = list(failures)
        self.closed = False

    def close(self):
        if self.failures:
            raise self.failures.pop(0)
        self.closed = True

    def poll(self, timeout=0):
        return self.closed


class StartupActor:
    pid = 123

    def __init__(self):
        self.failures = [OSError("terminate temporarily failed")]
        self.terminated = False
        self._thread = SimpleNamespace(ident=1)

    def terminate(self):
        if self.failures:
            raise self.failures.pop(0)
        self.terminated = True


class Actor:
    pid = 123
    exitcode = None

    def __init__(self, responses=()):
        self.responses = list(responses)
        self.closes = 0
        self.terminations = 0
        self.requests = []
        self.close_failures = []

    def submit(self, method, *, args=()):
        self.requests.append((method, args))
        call = ActorCall(len(self.requests), method)
        value = self.responses.pop(0) if self.responses else ("closed", self.pid, None)
        if isinstance(value, BaseException):
            call._set_exception(value)
        else:
            call._set_result(value)
        return call

    def close(self, timeout=None):
        self.closes += 1
        if self.close_failures:
            raise self.close_failures.pop(0)
        self.exitcode = 0

    def terminate(self):
        self.terminations += 1
        self.close()


def test_cancel_set_during_remote_failure_delivery_is_not_misclassified():
    event = Event()
    actor = Actor()
    driver = implementation._StreamDriver(actor, Endpoint(), implementation.ProcessStreamConfig())

    class ReturningFailure:
        def result(self, timeout):
            event.set()
            raise ActorRemoteError("method", "TaskCancelled", "stopped")

    with pytest.raises(TaskCancelled):
        driver._wait(ReturningFailure(), CancellationToken(event))


@pytest.mark.parametrize(
    "primary", [RuntimeError("setup failed"), KeyboardInterrupt(), SystemExit(7)]
)
def test_failed_handle_setup_retains_explicit_retryable_resources(monkeypatch, primary):
    actor, endpoint = StartupActor(), Endpoint([OSError("close temporarily failed")])
    monkeypatch.setattr(ProcessActor, "_for_stream_worker", lambda *a: (actor, endpoint))

    def fail(*args):
        raise primary

    monkeypatch.setattr(implementation, "_StreamDriver", fail)
    with pytest.raises(type(primary)) as caught:
        implementation.start_process_task_stream(empty)
    assert caught.value is primary
    cleanup = primary.process_stream_cleanup
    assert not cleanup.closed
    cleanup.close()
    assert cleanup.closed and actor.terminated and endpoint.closed


@pytest.mark.parametrize(
    "field,value",
    [
        ("max_message_bytes", True),
        ("max_message_bytes", 1023),
        ("max_message_bytes", 16_777_217),
        ("max_message_bytes", 1024.0),
        ("startup_timeout_seconds", 0),
        ("startup_timeout_seconds", 0.0009),
        ("advance_timeout_seconds", 0),
        ("cancellation_grace_seconds", -1),
        ("shutdown_timeout_seconds", -1),
        *[
            (field, value)
            for field in (
                "startup_timeout_seconds",
                "advance_timeout_seconds",
                "cancellation_grace_seconds",
                "shutdown_timeout_seconds",
            )
            for value in (True, float("nan"), float("inf"), 10**400, "1")
        ],
    ],
)
def test_invalid_process_bounds_rejected(field, value):
    with pytest.raises(ValidationError):
        implementation.ProcessStreamConfig(**{field: value})


@pytest.mark.parametrize("bad", [None, [], iter(()), lambda ctx: (), 1])
def test_invalid_function_never_allocates_worker(monkeypatch, bad):
    monkeypatch.setattr(ProcessActor, "_for_stream_worker", lambda *a: pytest.fail("spawned"))
    with pytest.raises(ValidationError):
        implementation.start_process_task_stream(bad)


def test_local_generator_async_generator_and_coroutine_never_allocate_worker(monkeypatch):
    def local(context):
        yield 1

    async def coroutine(context):
        return 1

    async def async_generator(context):
        yield 1

    monkeypatch.setattr(ProcessActor, "_for_stream_worker", lambda *a: pytest.fail("spawned"))
    for function in (local, coroutine, async_generator):
        with pytest.raises(ValidationError):
            implementation.start_process_task_stream(function)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"args": []},
        {"kwargs": {1: "value"}},
        {"config": object()},
        {"process_config": object()},
        {
            "config": TaskStreamConfig(max_buffered=65),
            "process_config": implementation.ProcessStreamConfig(max_message_bytes=1_048_576),
        },
    ],
)
def test_invalid_admission_never_allocates_worker(monkeypatch, kwargs):
    monkeypatch.setattr(ProcessActor, "_for_stream_worker", lambda *a: pytest.fail("spawned"))
    with pytest.raises(ValidationError):
        implementation.start_process_task_stream(empty, **kwargs)


def test_full_startup_frame_is_checked_before_pipe_creation(monkeypatch):
    import graph_sail.actors as actors

    monkeypatch.setattr(actors.multiprocessing, "get_context", lambda *a: pytest.fail("allocated"))
    with pytest.raises(actors.ActorSerializationError):
        ProcessActor._for_stream_worker(
            empty, (bytes(2048),), {}, ActorConfig(max_message_bytes=1024)
        )


def worker(**changes):
    fields = {
        "pid": 123,
        "exitcode": 0,
        "cancellation_requested": False,
        "termination_requested": False,
        "generator_closed": True,
        "resources_closed": True,
    }
    fields.update(changes)
    return implementation.ProcessStreamWorker(**fields)


@pytest.mark.parametrize(
    "field,value",
    [
        ("pid", True),
        ("pid", 0),
        ("pid", 2**63),
        ("exitcode", True),
        ("exitcode", 2**63),
        ("exitcode", "0"),
        ("cancellation_requested", 0),
        ("termination_requested", 1),
        ("generator_closed", []),
        ("resources_closed", None),
        ("exitcode", None),
    ],
)
def test_strict_worker_snapshot_fields(field, value):
    with pytest.raises(ValidationError):
        worker(**{field: value})


@pytest.mark.parametrize(
    "status,produced,metadata",
    [
        ("unknown", 0, worker()),
        ("succeeded", True, worker()),
        ("limited", 10_000_001, worker()),
        ("succeeded", 1, object()),
        ("cancelled", 0, worker(resources_closed=False)),
        ("succeeded", 0, worker(generator_closed=False)),
    ],
)
def test_completion_metadata_does_not_admit_invalid_or_unsettled_state(status, produced, metadata):
    with pytest.raises(ValidationError):
        implementation.ProcessStreamResult(status, produced, metadata)


def test_native_worker_does_not_execute_body_for_never_started_generator_close(tmp_path):
    marker = tmp_path / "never-entered-finally"
    # The importable producer writes from both its body and its finally. Neither
    # executes when close is applied to a never-started native generator.
    native = implementation._StreamWorker(Endpoint(), counted, (str(marker),), {})
    assert native.finish() == ("closed", implementation.os.getpid(), None)
    assert not marker.exists()
    assert native.finish() == ("closed", implementation.os.getpid(), None)
    with pytest.raises(ActorDiedError):
        native.advance(0)


@pytest.mark.parametrize("args,kwargs", [([], {}), ((), []), ((), {1: 2})])
def test_native_worker_rejects_malformed_argument_envelope(args, kwargs):
    with pytest.raises(ValidationError):
        implementation._StreamWorker(Endpoint(), empty, args, kwargs)


def test_native_worker_exact_eof_sequence_and_cancellation():
    native = implementation._StreamWorker(Endpoint(), empty, (), {})
    for sequence in (False, 1, -1):
        with pytest.raises(ActorDiedError):
            native.advance(sequence)
    assert native.advance(0) == ("eof", implementation.os.getpid(), 0, None)
    with pytest.raises(ActorDiedError):
        native.advance(0)
    assert native.finish()[0] == "closed"
    endpoint = Endpoint()
    native = implementation._StreamWorker(endpoint, empty, (), {})
    endpoint.close()
    with pytest.raises(TaskCancelled):
        native.advance(0)
    native.finish()


@pytest.mark.parametrize(
    "response",
    [
        None,
        ["yield", 123, 0, 1],
        ("yield",),
        ("bad", 123, 0, 1),
        (1, 123, 0, 1),
        ("yield", True, 0, 1),
        ("yield", 124, 0, 1),
        ("yield", 123, False, 1),
        ("yield", 123, 1, 1),
        ("eof", 123, 0, 1),
    ],
)
def test_yield_protocol_binds_exact_kind_pid_sequence_and_eof_payload(response):
    actor = Actor([response])
    driver = implementation._StreamDriver(actor, Endpoint(), implementation.ProcessStreamConfig())
    proxy = driver.proxy(TaskStreamContext(CancellationToken(Event())))
    with pytest.raises(ActorDiedError):
        next(proxy)
    driver.finish()
    assert driver.snapshot().resources_closed


@pytest.mark.parametrize(
    "response",
    [
        None,
        ["closed", 123, None],
        ("closed",),
        (False, 123, None),
        ("eof", 123, None),
        ("closed", True, None),
        ("closed", 124, None),
        ("closed", 123, 0),
    ],
)
def test_cleanup_ack_requires_exact_binding_and_failure_retires_without_replay(response):
    actor = Actor([response])
    driver = implementation._StreamDriver(actor, Endpoint(), implementation.ProcessStreamConfig())
    with pytest.raises(ActorDiedError):
        driver.finish()
    assert not driver.snapshot().generator_closed
    assert driver.snapshot().termination_requested and driver.snapshot().resources_closed
    driver.finish()
    assert actor.requests == [("finish", ())]


@pytest.mark.parametrize("failure", [OSError("sender"), KeyboardInterrupt(), SystemExit(8)])
def test_retire_attempts_actor_even_if_endpoint_cleanup_raises_and_can_retry(failure):
    actor, endpoint = Actor(), Endpoint([failure])
    driver = implementation._StreamDriver(actor, endpoint, implementation.ProcessStreamConfig())
    with pytest.raises(type(failure)) as caught:
        driver._retire(force=True)
    assert caught.value is failure
    assert actor.terminations == 1 and not driver.snapshot().resources_closed
    driver._retire(force=True)
    assert driver.snapshot().resources_closed and actor.terminations == 1


def test_failed_native_actor_close_is_not_hidden_by_joined_local_mailbox_and_retries():
    actor = Actor([("eof", 123, 0, None)])
    failure = OSError("actor native cleanup not yet settled")
    actor.close_failures.append(failure)
    driver = implementation._StreamDriver(actor, Endpoint(), implementation.ProcessStreamConfig())
    mailbox = implementation._ProcessMailbox(driver, TaskStreamConfig())
    handle = implementation.ProcessTaskStream(mailbox, driver)
    mailbox._start()
    try:
        with pytest.raises(OSError) as caught:
            handle.completion(5)
        assert caught.value is failure and handle.done()
        assert not handle.closed and not handle.worker.resources_closed
        handle.close(5)
        assert handle.closed and actor.closes == 2
        assert actor.requests.count(("finish", ())) == 1
    finally:
        handle.close(5)


def test_cancel_before_proxy_first_next_still_closes_native_owner():
    actor = Actor()
    driver = implementation._StreamDriver(actor, Endpoint(), implementation.ProcessStreamConfig())
    mailbox = implementation._ProcessMailbox(driver, TaskStreamConfig())
    handle = implementation.ProcessTaskStream(mailbox, driver)
    assert handle.cancel()
    mailbox._start()
    try:
        result = handle.completion(5)
        assert result.status == "cancelled" and result.produced == 0
        assert result.worker.resources_closed and result.worker.generator_closed
        assert actor.requests == [("finish", ())]
    finally:
        handle.close(5)


@pytest.mark.parametrize("delivered", ["result", "error", "control"])
def test_cancellation_race_rechecks_delivery_but_never_swallows_control(delivered):
    event = Event()
    driver = implementation._StreamDriver(Actor(), Endpoint(), implementation.ProcessStreamConfig())
    interrupt = KeyboardInterrupt()

    class Reply:
        def result(self, timeout):
            event.set()
            if delivered == "error":
                raise ValueError("remote failure")
            if delivered == "control":
                raise interrupt
            return 1

    with pytest.raises(KeyboardInterrupt if delivered == "control" else TaskCancelled):
        driver._wait(Reply(), CancellationToken(event))
    driver.finish()


@pytest.mark.parametrize("delivered", ["result", "error"])
def test_deadline_race_rechecks_both_success_and_failure_delivery(monkeypatch, delivered):
    clock = [0.0]
    monkeypatch.setattr(implementation.time, "monotonic", lambda: clock[0])
    driver = implementation._StreamDriver(Actor(), Endpoint(), implementation.ProcessStreamConfig())

    class Reply:
        def result(self, timeout):
            clock[0] = 31
            if delivered == "error":
                raise ValueError("late error")
            return 1

    with pytest.raises(ActorTimeoutError):
        driver._wait(Reply(), CancellationToken(Event()))
    driver.finish()


def test_startup_capability_preserves_new_cleanup_control_and_observes_all_resources():
    actor = StartupActor()
    actor.failures = []
    control = KeyboardInterrupt()
    first, second = Endpoint([control]), Endpoint()
    cleanup = _StreamStartupCleanup(actor, (first, second))
    error = cleanup.retain(ValueError("ordinary setup"))
    assert error is control and error.process_stream_cleanup is cleanup
    assert second.closed and actor.terminated and not cleanup.closed
    cleanup.close()
    assert cleanup.closed


def test_bootstrap_receiver_close_and_post_start_error_keep_real_owner(monkeypatch):
    import graph_sail.actors as actors

    receiver, sender = Endpoint([OSError("receiver close"), OSError("retry close")]), Endpoint()
    monkeypatch.setattr(
        actors.multiprocessing,
        "get_context",
        lambda *a: SimpleNamespace(Pipe=lambda **k: (receiver, sender)),
    )

    def initialize(self, *args, **kwargs):
        self._thread = SimpleNamespace(ident=1)
        self.failures = [OSError("actor close")]
        self.terminated = False

    def terminate(self):
        if self.failures:
            raise self.failures.pop(0)
        self.terminated = True

    monkeypatch.setattr(ProcessActor, "_initialize", initialize)
    monkeypatch.setattr(ProcessActor, "terminate", terminate)
    with pytest.raises(OSError, match="receiver close") as caught:
        ProcessActor._for_stream_worker(empty, (), {}, ActorConfig())
    cleanup = caught.value.process_stream_cleanup
    assert not cleanup.closed and sender.closed
    cleanup.close()
    assert cleanup.closed and cleanup._actor.terminated


def test_initializer_can_raise_after_broker_start_without_losing_actor(monkeypatch):
    import graph_sail.actors as actors

    receiver, sender = Endpoint(), Endpoint()
    monkeypatch.setattr(
        actors.multiprocessing,
        "get_context",
        lambda *a: SimpleNamespace(Pipe=lambda **k: (receiver, sender)),
    )

    def initialize(self, *args, **kwargs):
        self._thread = SimpleNamespace(ident=1)
        raise SystemExit(6)

    terminated = []
    monkeypatch.setattr(ProcessActor, "_initialize", initialize)
    monkeypatch.setattr(ProcessActor, "terminate", lambda self: terminated.append(self))
    with pytest.raises(SystemExit) as caught:
        ProcessActor._for_stream_worker(empty, (), {}, ActorConfig())
    assert caught.value.process_stream_cleanup.closed
    assert len(terminated) == 1 and receiver.closed and sender.closed


def test_process_construction_failure_retains_and_retries_local_child_pipe(monkeypatch):
    import graph_sail.actors as actors

    receiver, sender, parent, child = (
        Endpoint(),
        Endpoint(),
        Endpoint(),
        Endpoint([OSError("child")]),
    )

    class Context:
        def Pipe(self, duplex=True):
            return (parent, child) if duplex else (receiver, sender)

        def Process(self, **kwargs):
            raise RuntimeError("process construction failed")

    monkeypatch.setattr(actors.multiprocessing, "get_context", lambda *a: Context())
    with pytest.raises(RuntimeError, match="process construction failed") as caught:
        ProcessActor._for_stream_worker(empty, (), {}, ActorConfig())
    cleanup = caught.value.process_stream_cleanup
    assert receiver.closed and sender.closed and parent.closed
    assert not cleanup.closed and not child.closed
    cleanup.close()
    assert child.closed and cleanup.closed


def test_worker_value_sequence_and_terminal_close_state_are_checked(tmp_path, monkeypatch):
    marker = tmp_path / "single-next"
    native = implementation._StreamWorker(Endpoint(), counted, (str(marker),), {})
    assert native.advance(0) == (
        "yield",
        implementation.os.getpid(),
        0,
        (implementation.os.getpid(), 0),
    )
    assert marker.read_text() == "next:0\n"
    original = implementation.inspect.getgeneratorstate
    monkeypatch.setattr(implementation.inspect, "getgeneratorstate", lambda value: "GEN_SUSPENDED")
    with pytest.raises(ActorDiedError, match="closed state"):
        native.finish()
    monkeypatch.setattr(implementation.inspect, "getgeneratorstate", original)
    assert native.finish()[0] == "closed"
    assert marker.read_text() == "next:0\nfinally\n"


def test_unidentified_worker_cannot_be_described_as_owned():
    actor = Actor()
    actor.pid = None
    with pytest.raises(ActorDiedError, match="identity"):
        implementation._StreamDriver(actor, Endpoint(), implementation.ProcessStreamConfig())


@pytest.mark.parametrize("failure", [OSError("signal"), KeyboardInterrupt(), SystemExit(5)])
def test_wait_signal_failure_preserves_control_and_keeps_retryable_endpoint(failure):
    event = Event()
    event.set()
    endpoint = Endpoint([failure])
    driver = implementation._StreamDriver(Actor(), endpoint, implementation.ProcessStreamConfig())
    expected = type(failure) if not isinstance(failure, Exception) else TaskCancelled
    with pytest.raises(expected) as caught:
        driver._wait(ActorCall(1, "advance"), CancellationToken(event))
    if expected is not TaskCancelled:
        assert caught.value is failure and failure.__cause__ is not failure
    driver.finish()
    assert driver.snapshot().resources_closed


@pytest.mark.parametrize("failure", [OSError("terminate"), KeyboardInterrupt(), SystemExit(5)])
def test_wait_forced_retirement_failure_does_not_swallow_control(failure):
    event = Event()
    event.set()
    actor = Actor()
    actor.close_failures = [failure]
    driver = implementation._StreamDriver(
        actor, Endpoint(), implementation.ProcessStreamConfig(cancellation_grace_seconds=0)
    )
    expected = type(failure) if not isinstance(failure, Exception) else TaskCancelled
    with pytest.raises(expected) as caught:
        driver._wait(ActorCall(1, "advance"), CancellationToken(event))
    if expected is not TaskCancelled:
        assert caught.value is failure and failure.__cause__ is not failure
    assert not driver.snapshot().resources_closed
    driver.finish()
    assert driver.snapshot().resources_closed


def test_mailbox_signal_cleanup_error_still_attempts_finish_and_can_retry_endpoint():
    actor, endpoint = Actor(), Endpoint([OSError("signal"), OSError("retire")])
    driver = implementation._StreamDriver(actor, endpoint, implementation.ProcessStreamConfig())
    mailbox = implementation._ProcessMailbox(driver, TaskStreamConfig())
    mailbox.cancel()  # Keep the external signal failure inside owned _run.
    mailbox._start()
    try:
        with pytest.raises(OSError, match="signal"):
            mailbox.completion(5)
        assert actor.closes == 1 and not driver.snapshot().resources_closed
        mailbox.close(5)
        driver.finish()
        assert driver.snapshot().resources_closed
    finally:
        mailbox.close(5)
        driver.finish()


def test_inconsistent_underlying_mailbox_result_still_cleans_native_owner(monkeypatch):
    monkeypatch.setattr(implementation.TaskStream, "_run", lambda *a: None)
    driver = implementation._StreamDriver(Actor(), Endpoint(), implementation.ProcessStreamConfig())
    mailbox = implementation._ProcessMailbox(driver, TaskStreamConfig())
    with pytest.raises(RuntimeError, match="no completion"):
        mailbox._run(driver.proxy)
    assert driver.snapshot().resources_closed


@pytest.mark.parametrize(
    "primary,cleanup",
    [
        (None, OSError("cleanup")),
        (KeyboardInterrupt(), OSError("cleanup")),
        (ValueError("body"), SystemExit(3)),
    ],
)
def test_context_exit_cleanup_keeps_primary_control_and_never_self_causes(
    monkeypatch, primary, cleanup
):
    driver = implementation._StreamDriver(Actor(), Endpoint(), implementation.ProcessStreamConfig())
    mailbox = implementation._ProcessMailbox(driver, TaskStreamConfig())
    handle = implementation.ProcessTaskStream(mailbox, driver)

    def fail(*args):
        raise cleanup

    monkeypatch.setattr(handle, "close", fail)
    winner = primary if primary is not None and not isinstance(primary, Exception) else cleanup
    with pytest.raises(type(winner)) as caught:
        handle.__exit__(type(primary) if primary else None, primary, None)
    assert caught.value is winner and winner.__cause__ is not winner


def test_failed_setup_promotes_new_cleanup_interrupt_and_retains_owner(monkeypatch):
    actor = StartupActor()
    actor.failures = []
    interrupt = KeyboardInterrupt()
    endpoint = Endpoint([interrupt])
    monkeypatch.setattr(ProcessActor, "_for_stream_worker", lambda *a: (actor, endpoint))

    def fail(*args):
        raise ValueError("ordinary setup")

    monkeypatch.setattr(implementation, "_StreamDriver", fail)
    with pytest.raises(KeyboardInterrupt) as caught:
        implementation.start_process_task_stream(empty)
    assert caught.value is interrupt and not interrupt.process_stream_cleanup.closed
    interrupt.process_stream_cleanup.close()
    assert interrupt.process_stream_cleanup.closed


class ScriptedConnection(Endpoint):
    def __init__(self, requests, failures=()):
        super().__init__(failures)
        self.requests = list(requests)
        self.sent = []

    def recv_bytes(self, limit):
        from graph_sail.actors import _pack

        if not self.requests:
            raise EOFError
        return _pack(self.requests.pop(0), limit)

    def send_bytes(self, value):
        from graph_sail.actors import _unpack

        self.sent.append(_unpack(value))


def test_closed_internal_bootstrap_protocol_and_same_thread_shutdown(tmp_path):
    import graph_sail.actors as actors

    marker = tmp_path / "protocol"
    channel, signal = ScriptedConnection([(1, "advance", (0,), {})]), Endpoint()
    startup = actors._pack(
        (implementation._StreamWorker, ("advance", "finish"), (counted, (str(marker),), {}), {}),
        1024,
    )
    actors._worker(channel, startup, 1024, signal)
    assert channel.sent == [
        (0, "ready", None),
        (1, "ok", ("yield", implementation.os.getpid(), 0, (implementation.os.getpid(), 0))),
    ]
    assert marker.read_text() == "next:0\nfinally\n"
    assert channel.closed and signal.closed


def test_internal_stream_cleanup_keeps_original_worker_control_and_closes_remaining_endpoints(
    tmp_path,
):
    from process_stream_functions import control

    import graph_sail.actors as actors

    marker = tmp_path / "control"
    channel = ScriptedConnection(
        [(1, "advance", (0,), {}), (2, "advance", (1,), {})], [OSError("transport close")]
    )
    signal = Endpoint()
    startup = actors._pack(
        (
            implementation._StreamWorker,
            ("advance", "finish"),
            (control, ("exit", str(marker)), {}),
            {},
        ),
        1024,
    )
    with pytest.raises(SystemExit) as caught:
        actors._worker(channel, startup, 1024, signal)
    assert caught.value.code == 7 and signal.closed and not channel.closed
    assert marker.read_text() == "finally"
    channel.close()


@pytest.mark.parametrize(
    "fields",
    [
        (empty, ("advance", "finish"), (), {}),
        (implementation._StreamWorker, ("advance",), (empty, (), {}), {}),
        (implementation._StreamWorker, ("advance", "finish"), (), {}),
    ],
)
def test_cancellation_bootstrap_cannot_admit_arbitrary_factories_or_method_sets(fields):
    import graph_sail.actors as actors

    channel, signal = ScriptedConnection([]), Endpoint()
    actors._worker(channel, actors._pack(fields, 1024), 1024, signal)
    assert channel.sent[0][1] == "error"
    assert channel.sent[0][2][:2] == ("startup", "TypeError")
    assert channel.closed and signal.closed


def test_child_endpoint_cleanup_control_does_not_skip_partial_actor_cleanup(monkeypatch):
    import graph_sail.actors as actors

    receiver, sender, parent = Endpoint(), Endpoint(), Endpoint()
    control = KeyboardInterrupt()
    child = Endpoint([OSError("child close")])

    class Context:
        def Pipe(self, duplex=True):
            return (parent, child) if duplex else (receiver, sender)

        def Process(self, **kwargs):
            raise control

    monkeypatch.setattr(actors.multiprocessing, "get_context", lambda *a: Context())
    with pytest.raises(KeyboardInterrupt) as caught:
        ProcessActor._for_stream_worker(empty, (), {}, ActorConfig())
    assert caught.value is control
    assert receiver.closed and sender.closed and parent.closed
    assert not control.process_stream_cleanup.closed
    control.process_stream_cleanup.close()
    assert child.closed and control.process_stream_cleanup.closed


def test_partial_bootstrap_native_failure_retains_identity_and_retries():
    actor = SimpleNamespace(_process=object(), _process_closed=False, _cleanup_error=None)
    original_process = actor._process
    failure = ActorDiedError("native handle cleanup failed")
    attempts = []

    def cleanup_process(*, graceful):
        attempts.append(graceful)
        assert actor._process is original_process
        if len(attempts) == 1:
            actor._cleanup_error = failure
        else:
            assert actor._cleanup_error is None
            actor._process_closed = True

    actor._cleanup_process = cleanup_process
    cleanup = _StreamStartupCleanup(actor, ())
    with pytest.raises(ActorDiedError) as caught:
        cleanup.close()
    assert caught.value is failure and not cleanup.closed
    cleanup.close()
    assert cleanup.closed and attempts == [False, False]
    _StreamStartupCleanup(SimpleNamespace(), ()).close()  # Failure before any actor allocation.


@pytest.mark.parametrize("failure", [OSError("start"), KeyboardInterrupt(), SystemExit(6)])
def test_stream_process_start_failure_preserves_controls_and_all_pipe_ownership(
    monkeypatch, failure
):
    import graph_sail.actors as actors

    receiver, sender, parent, child = Endpoint(), Endpoint(), Endpoint(), Endpoint()

    class Process:
        pid = None
        closed = False

        def start(self):
            raise failure

        def is_alive(self):
            return False

        def close(self):
            self.closed = True

    process = Process()

    class Context:
        def Pipe(self, duplex=True):
            return (parent, child) if duplex else (receiver, sender)

        def Process(self, **kwargs):
            return process

    monkeypatch.setattr(actors.multiprocessing, "get_context", lambda *a: Context())
    expected = actors.ActorStartupError if isinstance(failure, Exception) else type(failure)
    with pytest.raises(expected) as caught:
        ProcessActor._for_stream_worker(empty, (), {}, ActorConfig())
    if not isinstance(failure, Exception):
        assert caught.value is failure
    assert caught.value.process_stream_cleanup.closed and process.closed
    assert all(endpoint.closed for endpoint in (receiver, sender, parent, child))


def test_bootstrap_cleanup_promotes_secondary_control_without_losing_retry_capability(monkeypatch):
    import graph_sail.actors as actors

    control = KeyboardInterrupt()
    receiver, sender = Endpoint([control]), Endpoint()
    monkeypatch.setattr(
        actors.multiprocessing,
        "get_context",
        lambda *a: SimpleNamespace(Pipe=lambda **k: (receiver, sender)),
    )

    def initialize(*args, **kwargs):
        raise OSError("failed before actor pipe")

    monkeypatch.setattr(ProcessActor, "_initialize", initialize)
    with pytest.raises(KeyboardInterrupt) as caught:
        ProcessActor._for_stream_worker(empty, (), {}, ActorConfig())
    assert caught.value is control and not control.process_stream_cleanup.closed
    assert sender.closed
    control.process_stream_cleanup.close()
    assert receiver.closed and control.process_stream_cleanup.closed
