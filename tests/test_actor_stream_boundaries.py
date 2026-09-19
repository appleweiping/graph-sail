"""Bounded ingress and failure-injection ownership, with actual parent owners."""

from collections import deque
from contextlib import suppress
from dataclasses import replace
from threading import Condition, Event, current_thread
from types import SimpleNamespace

import pytest

import graph_sail as gs
import graph_sail.actor_streams as implementation
from graph_sail.actors import ActorCall
from tests.actor_stream_functions import Counter
from tests.test_actor_method_streams import actor, until


@pytest.mark.parametrize(
    "name",
    [
        "open_timeout_seconds",
        "advance_timeout_seconds",
        "finish_timeout_seconds",
        "cancellation_grace_seconds",
        "shutdown_timeout_seconds",
    ],
)
@pytest.mark.parametrize("value", [-1, True, float("nan"), float("inf"), 86401, "1"])
def test_config_invalid_budgets(name, value):
    with pytest.raises(gs.ValidationError):
        gs.ActorStreamConfig(**{name: value})


@pytest.mark.parametrize(
    "names",
    [
        None,
        [],
        ("_private",),
        ("dup", "dup"),
        ("state",),
        ("x" * 129,),
        (True,),
        tuple(f"s{i}" for i in range(65)),
    ],
)
def test_bad_stream_allowlist_is_preflight(names):
    with pytest.raises(gs.ValidationError):
        gs.ActorDefinition(Counter, ("state",), names)


def test_exact_config_revalidation_and_no_admission_on_bad_values():
    with actor() as owner:
        bad = gs.TaskStreamConfig()
        object.__setattr__(bad, "max_yields", True)
        for options in (
            {"config": object()},
            {"stream_config": object()},
            {"config": bad},
            {"config": gs.TaskStreamConfig(max_buffered=65)},
            {"args": []},
            {"args": tuple(range(257))},
            {"kwargs": {1: 2}},
        ):
            with pytest.raises(gs.ValidationError):
                owner.stream("values", **options)
        with pytest.raises(gs.ActorSerializationError):
            owner.stream("values", args=(lambda: None,))
        assert owner._next_id == owner._next_stream_id == 1
        assert owner._stream_lease is None
        assert owner.submit("state").result(5)[:3] == (0, 0, 1)


def test_registry_snapshot_and_state_result_validation():
    definition = gs.ActorDefinition(Counter, ("state",), ("values",))
    registry = gs.ActorRegistry({"counter": definition})
    object.__setattr__(definition, "stream_methods", ())
    assert registry.actors["counter"].stream_methods == ("values",)
    state = gs.ActorStreamState(1, 1, True, True, True, True, False, False)
    for name, value in (
        ("actor_pid", True),
        ("stream_id", 0),
        ("generator_closed", 1),
        ("retirement_requested", True),
    ):
        with pytest.raises(gs.ValidationError):
            replace(state, **{name: value})
    with pytest.raises(gs.ValidationError):
        gs.ActorStreamResult("succeeded", 1, object())
    with pytest.raises(gs.ValidationError):
        gs.ActorStreamResult(
            "succeeded", 1, replace(state, lease_released=False, actor_reusable_at_release=False)
        )


@pytest.mark.parametrize(
    "record",
    [
        None,
        (),
        ("yield", True, 1, 0, 1),
        ("yield", 1, True, 0, 1),
        ("yield", 1, 1, True, 1),
        ("yield", 1, 1, 1, 1),
        ("yield", 1, 1, 0),
        ("eof", 1, 1, 0, 1),
        ("bogus", 1, 1, 0),
        ("error", 1, 1, 0, "bad", "E", "message"),
        ("error", 1, 1, 0, "advance", "E" * 257, "message"),
        ("error", 1, 1, 0, "advance", "E", "m" * 129),
    ],
)
def test_strict_bound_reply_records(record):
    owner = SimpleNamespace(pid=1, config=gs.ActorConfig(max_message_bytes=1024))
    driver = implementation._ActorDriver(owner, 1, gs.ActorStreamConfig())
    with pytest.raises(gs.ActorDiedError):
        driver._record(record, "advance")
    assert driver.poisoned


@pytest.mark.parametrize("operation", ["open", "finish"])
def test_serialization_phase_belongs_only_to_advance(operation):
    owner = SimpleNamespace(pid=1, config=gs.ActorConfig())
    driver = implementation._ActorDriver(owner, 1, gs.ActorStreamConfig())
    with pytest.raises(gs.ActorDiedError):
        driver._record(("error", 1, 1, 0, "serialization", "E", "message"), operation)
    assert driver.poisoned


def test_private_submission_cannot_spend_another_lease():
    with actor() as owner:
        driver = implementation._ActorDriver(owner, 1, gs.ActorStreamConfig())
        with pytest.raises(gs.ActorClosedError):
            driver._submit("advance")
        driver.signal()
        assert not owner._actor_stream_sender.closed


def test_cancel_before_first_proxy_step_releases_without_generator(monkeypatch):
    original = implementation._ActorMailbox._run

    def pre_cancel(mailbox, function):
        mailbox.cancel()
        return original(mailbox, function)

    monkeypatch.setattr(implementation._ActorMailbox, "_run", pre_cancel)
    with actor() as owner:
        with owner.stream("values") as stream:
            assert list(stream) == []
            assert stream.completion(5).status == "cancelled"
        assert not stream.state.generator_opened
        assert stream.state.actor_reusable_at_release
        assert owner.submit("state").result(5)[:3] == (0, 0, 1)


def test_actual_thread_adopted_then_start_raises_keeps_original_owner(monkeypatch):
    import graph_sail.task_streams as mailbox

    original = mailbox.Thread.start
    control = SystemExit("after actual start")

    def start(thread):
        original(thread)
        if thread.name == "graph-sail-stream":
            raise control

    with actor() as owner:
        monkeypatch.setattr(mailbox.Thread, "start", start)
        with pytest.raises(SystemExit) as caught:
            owner.stream("values")
        assert caught.value is control
        retained = control.actor_stream_cleanup
        retained.close()
        assert retained.closed and not retained._stream._thread.is_alive()


def test_stream_handle_allocation_failure_does_not_commit_lease(monkeypatch):
    with actor() as owner:
        control = KeyboardInterrupt("handle allocation")

        def fail(*args):
            raise control

        monkeypatch.setattr(implementation, "ActorMethodStream", fail)
        with pytest.raises(KeyboardInterrupt) as caught:
            owner.stream("values")
        assert caught.value is control
        assert owner._stream_lease is None and owner._next_id == 1


def test_open_queue_allocation_failure_releases_without_dispatch(monkeypatch):
    class FailingQueue(deque):
        def append(self, value):
            raise MemoryError("queue allocation")

    with actor() as owner:
        original = owner._queue
        owner._queue = FailingQueue()
        with owner.stream("values") as stream, pytest.raises(MemoryError):
            stream.completion(5)
        owner._queue = original
        assert owner._next_id == 1 and owner.pending_count == 0
        assert owner.submit("add").result(5) == 0


def test_lost_close_ack_poison_even_when_native_closed(monkeypatch):
    original = implementation._ActorDriver._record

    def lose(driver, value, operation):
        if operation == "finish":
            driver.poisoned = True
            raise gs.ActorDiedError("lost close acknowledgement")
        return original(driver, value, operation)

    monkeypatch.setattr(implementation._ActorDriver, "_record", lose)
    owner = actor()
    stream = owner.stream("values", args=(1,))
    try:
        assert stream.next(5).value == 1
        with pytest.raises(gs.ActorDiedError):
            stream.completion(10)
    finally:
        stream.close()
        owner.close()
    assert not stream.state.generator_closed
    assert not owner.alive and not stream.state.actor_reusable_at_release


def test_close_timeout_retains_live_owner_and_cancellation_signals_once(tmp_path):
    owner = actor()
    stream = owner.stream(
        "wait",
        args=(str(tmp_path / "entered"), False),
        stream_config=gs.ActorStreamConfig(cancellation_grace_seconds=0.1),
    )
    underlying = owner._actor_stream_sender
    calls = []

    class Sender:
        @property
        def closed(self):
            return underlying.closed

        def close(self):
            calls.append(1)
            underlying.close()

    owner._actor_stream_sender = Sender()
    try:
        until((tmp_path / "entered").exists)
        with pytest.raises(TimeoutError):
            stream.close(0)
        assert not stream.closed
        stream.close(10)
        assert calls == [1]
    finally:
        stream.close()
        owner.close()


def test_internal_cancelled_call_does_not_invoke_future_callbacks():
    # Private queued cancellation is ownership bookkeeping, not public callbacks.
    call = ActorCall(1, "open")
    assert call.cancel()
    assert call.cancelled() and call.done()


def test_signal_control_still_attempts_native_retirement_before_terminal(tmp_path):
    owner = actor()
    stream = owner.stream("wait", args=(str(tmp_path / "entered"), False))
    underlying = owner._actor_stream_sender
    control = KeyboardInterrupt("signal close failed")

    class Sender:
        @property
        def closed(self):
            return underlying.closed

        def close(self):
            raise control

    try:
        until((tmp_path / "entered").exists)
        owner._actor_stream_sender = Sender()
        with pytest.raises(KeyboardInterrupt) as cancellation:
            stream.cancel()
        assert cancellation.value is control
        with pytest.raises(KeyboardInterrupt) as completion:
            stream.completion(10)
        assert completion.value is control
        assert not owner.alive
        assert stream.state.actor_resources_closed
    finally:
        with suppress(KeyboardInterrupt):
            stream.close()
        owner.close()


def test_close_reentry_rejects_before_discarding_or_signalling():
    owner = actor()
    stream = owner.stream("values", args=(100,), config=gs.TaskStreamConfig(max_buffered=1))
    try:
        assert stream.wait_ready(5)
        owner._serializing = True
        with pytest.raises(gs.ActorError):
            stream.close(0)
        assert not stream._stream._stop.is_set()
    finally:
        owner._serializing = False
        stream.close()
        owner.close()


@pytest.mark.parametrize("operation", ["next", "wait_ready", "completion", "close"])
def test_transport_wait_reentry_is_rejected_without_consumption(operation):
    with (
        actor() as owner,
        owner.stream("values", args=(2,), config=gs.TaskStreamConfig(max_buffered=1)) as stream,
    ):
        assert stream.wait_ready(5)
        broker = owner._thread
        try:
            owner._thread = current_thread()
            with pytest.raises(gs.ActorError):
                getattr(stream, operation)(0)
        finally:
            owner._thread = broker
        assert stream.next(5).sequence == 0


def test_bootstrap_failure_after_actual_broker_adoption_retains_owner(monkeypatch):
    original = gs.ProcessActor._initialize
    observed = []
    control = KeyboardInterrupt("after broker adopted")

    def initialize(owner, *args, **kwargs):
        original(owner, *args, **kwargs)
        observed.append(owner)
        raise control

    monkeypatch.setattr(gs.ProcessActor, "_initialize", initialize)
    with pytest.raises(KeyboardInterrupt) as caught:
        actor()
    assert caught.value is control
    assert control.actor_stream_cleanup.closed
    control.actor_stream_cleanup.close()
    assert len(observed) == 1 and not observed[0].alive


@pytest.mark.parametrize(
    "primary,cleanup",
    [
        (None, ValueError("close")),
        (ValueError("body"), SystemExit("close")),
        (KeyboardInterrupt("body"), ValueError("close")),
    ],
)
def test_context_failure_priority_preserves_control_identity(monkeypatch, primary, cleanup):
    handle = gs.ActorMethodStream.__new__(gs.ActorMethodStream)

    def fail(self):
        raise cleanup

    monkeypatch.setattr(gs.ActorMethodStream, "close", fail)
    expected = cleanup if primary is None or isinstance(primary, Exception) else primary
    with pytest.raises(type(expected)) as caught:
        handle.__exit__(type(primary) if primary else None, primary, None)
    assert caught.value is expected


class Endpoint:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def controlled_driver():
    owner = SimpleNamespace(
        pid=42,
        config=gs.ActorConfig(),
        _condition=Condition(),
        _closed=Event(),
        _closing=False,
        _abort=None,
        _pending={},
        _queue=deque(),
        _next_id=1,
        _actor_stream_sender=Endpoint(),
    )
    driver = implementation._ActorDriver(owner, 1, gs.ActorStreamConfig())
    owner._stream_lease = driver.capability
    driver.stream = SimpleNamespace(_stop=Event())
    return driver


def test_unopened_release_does_not_claim_dead_worker_is_reusable():
    driver = controlled_driver()
    driver.actor.failure = None
    driver.actor.alive = False
    driver.finish()
    assert driver.released
    assert not driver.reusable


@pytest.mark.parametrize("kind", ["yield", "error", "control"])
def test_delivery_race_never_accepts_late_value_or_swallows_control(kind):
    driver = controlled_driver()
    real_call = ActorCall(1, "open")
    real_call._future.set_running_or_notify_cancel()
    driver.open_call = real_call
    control = KeyboardInterrupt("delivery control")

    class Call:
        def result(self, timeout):
            driver.stream._stop.set()
            if kind == "control":
                raise control
            if kind == "error":
                return ("error", 42, 1, 0, "advance", "ValueError", "failed")
            return ("yield", 42, 1, 0, "late")

    expected = KeyboardInterrupt if kind == "control" else gs.TaskCancelled
    with pytest.raises(expected) as caught:
        driver._wait(Call(), "advance")
    if kind == "control":
        assert caught.value is control
    else:
        assert driver.retiring and driver.actor._actor_stream_sender.closed
    assert driver.sequence == (1 if kind == "yield" else 0)


def test_cancelled_private_open_and_credit_guards():
    driver = controlled_driver()
    call = ActorCall(1, "open")
    driver.open_call = call
    driver.stream._stop.set()
    with pytest.raises(gs.TaskCancelled):
        driver._wait(call, "open")
    assert call.cancelled() and not driver.actor._actor_stream_sender.closed
    driver.actor._closing = True
    with pytest.raises(gs.ActorClosedError):
        driver._submit("advance")
    driver.actor._closing = False
    driver.actor._pending[1] = call
    with pytest.raises(gs.ActorDiedError):
        driver._submit("finish")


def test_retry_cleanup_refuses_live_broker_then_repairs_owned_closed_actor():
    owner = actor()
    try:
        with pytest.raises(gs.ActorDiedError):
            implementation._retry_cleanup(owner)
    finally:
        owner.close()
    owner._cleanup_error = gs.ActorDiedError("retained cleanup diagnostic")
    implementation._retry_cleanup(owner)
    assert owner._cleanup_error is None and not owner.alive


def test_completed_closed_actor_rejects_new_stream_and_invalid_methods():
    owner = actor()
    try:
        with pytest.raises(gs.ValidationError):
            owner.stream("state")
        with pytest.raises(gs.ValidationError):
            owner.submit("values")
    finally:
        owner.close()
    with pytest.raises(gs.ActorClosedError):
        owner.stream("values")


@pytest.mark.parametrize("abort", [False, True])
def test_cancelled_queued_open_wakes_lease_release_waiter(monkeypatch, abort):
    original_run = implementation._ActorMailbox._run
    original_submit = implementation._ActorDriver._submit

    def under_admission(mailbox, function):
        # Keep the broker behind admission until finish enters its real
        # Condition.wait_for(), which releases all recursive acquisitions.
        with mailbox._driver.actor._condition:
            return original_run(mailbox, function)

    def cancelled_open(driver, operation):
        call = original_submit(driver, operation)
        if operation == "open":
            driver.stream._stop.set()
            assert call.cancel()
            if abort:
                driver.actor._request_abort(gs.ActorClosedError("simultaneous actor retirement"))
        return call

    monkeypatch.setattr(implementation._ActorMailbox, "_run", under_admission)
    monkeypatch.setattr(implementation._ActorDriver, "_submit", cancelled_open)
    with actor() as owner:
        stream = owner.stream("values")
        try:
            until(lambda: owner.pending_count == 0)
            if abort:
                until(owner._closed.is_set)
            assert stream.completion(2).status == "cancelled"
            assert stream.state.actor_reusable_at_release is not abort
            assert not stream.state.generator_opened
        finally:
            with owner._condition:
                owner._condition.notify_all()
            stream.close()


def test_native_cleanup_failure_retains_handle_until_explicit_retry(monkeypatch, tmp_path):
    owner = actor()
    process_close = owner._process.close
    denied = [True]

    def close_process():
        if denied[0]:
            raise OSError("process handle close temporarily denied")
        process_close()

    monkeypatch.setattr(owner._process, "close", close_process)
    stream = owner.stream(
        "wait",
        args=(str(tmp_path / "entered"), False),
        stream_config=gs.ActorStreamConfig(cancellation_grace_seconds=0),
    )
    try:
        until((tmp_path / "entered").exists)
        stream.cancel()
        with pytest.raises(gs.ActorDiedError):
            stream.completion(10)
        assert stream.done() and not stream.closed
        assert not stream.state.actor_resources_closed
        assert not stream.state.lease_released
        denied[0] = False
        stream.close(10)
        assert stream.closed and stream.state.actor_resources_closed
    finally:
        denied[0] = False
        stream.close()
        owner.close()


def test_endpoint_retry_attempts_all_retained_resources_on_control_failure():
    owner = actor()
    owner.close()
    actual = owner._actor_stream_endpoints
    control = KeyboardInterrupt("first retained endpoint")
    calls = []

    class First(Endpoint):
        def close(self):
            calls.append("first")
            raise control

    class Second(Endpoint):
        def close(self):
            calls.append("second")
            self.closed = True

    try:
        owner._actor_stream_endpoints = (First(), Second())
        with pytest.raises(KeyboardInterrupt) as caught:
            implementation._retry_cleanup(owner)
        assert caught.value is control
        assert calls == ["first", "second"]
    finally:
        owner._actor_stream_endpoints = actual
