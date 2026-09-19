"""Startup/shutdown ordering and failure visibility; local fakes are labeled."""

from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from threading import Condition, Event, current_thread
from types import SimpleNamespace

import pytest

import graph_sail as gs
import graph_sail.actor_streams as implementation
from graph_sail.actors import ActorCall
from tests.test_actor_method_streams import actor, until
from tests.test_actor_stream_boundaries import controlled_driver


@pytest.mark.parametrize("adopted", [False, True])
def test_primary_start_control_survives_secondary_cancel_failure(monkeypatch, adopted):
    import threading

    owner = actor()
    original_start = threading.Thread.start
    original_cancel = implementation._ActorMailbox.cancel
    primary = KeyboardInterrupt("original actual start failure")
    creator = current_thread()
    calls = []

    def start(thread):
        if thread.name != "graph-sail-stream":
            return original_start(thread)
        if adopted:
            original_start(thread)
        raise primary

    def cancel(mailbox):
        if current_thread() is creator and not calls:
            calls.append(True)
            raise MemoryError("secondary stop allocation failure")
        return original_cancel(mailbox)

    monkeypatch.setattr(threading.Thread, "start", start)
    monkeypatch.setattr(implementation._ActorMailbox, "cancel", cancel)
    try:
        with pytest.raises(KeyboardInterrupt) as caught:
            owner.stream("values", args=(0,))
        assert caught.value is primary
        retained = primary.actor_stream_cleanup
        retained.close(10)
        assert retained.closed
        assert retained._stream._thread.ident is None or not retained._stream._thread.is_alive()
    finally:
        if owner._stream_owner is not None:
            owner._stream_owner.close(10)
        owner.close()


@pytest.mark.parametrize("close_fails", [False, True])
def test_failed_start_cancel_at_full_buffer_preserves_control_and_retry(monkeypatch, close_fails):
    import threading

    owner = actor()
    original_start = threading.Thread.start
    original_cancel = implementation._ActorMailbox.cancel
    original_close = implementation.TaskStream.close
    creator = current_thread()
    primary = KeyboardInterrupt("start failed after adoption")
    attempted = []
    broken_close = [close_fails]

    def start(thread):
        original_start(thread)
        if thread.name == "graph-sail-stream":
            raise primary

    def cancel(mailbox):
        if current_thread() is creator and not attempted:
            until(lambda: len(mailbox._queue) == 1)
            assert not mailbox._stop.is_set()
            attempted.append(True)
            raise MemoryError("cancel failed before stop")
        return original_cancel(mailbox)

    def close(mailbox, timeout=None):
        if current_thread() is creator and broken_close[0]:
            raise TimeoutError("close failed before stop")
        return original_close(mailbox, timeout)

    monkeypatch.setattr(threading.Thread, "start", start)
    monkeypatch.setattr(implementation._ActorMailbox, "cancel", cancel)
    monkeypatch.setattr(implementation.TaskStream, "close", close)
    try:
        with pytest.raises(KeyboardInterrupt) as caught:
            owner.stream("values", args=(3,), config=gs.TaskStreamConfig(max_buffered=1))
        assert caught.value is primary and attempted
        retained = primary.actor_stream_cleanup
        assert any("MemoryError" in note for note in primary.__notes__)
        if close_fails:
            assert retained._stream._thread.is_alive() and not retained.closed
            assert not retained._stream._stop.is_set()
            assert not retained.state.lease_released
        broken_close[0] = False
        retained.close(10)
        assert retained.closed and not retained._stream._thread.is_alive()
    finally:
        broken_close[0] = False
        owner._stream_owner.close(10)
        owner.close()


def test_failed_start_has_two_finite_cleanup_attempts_and_retains_on_timeout(monkeypatch):
    import threading

    owner = actor()
    original_start = threading.Thread.start
    original_run = implementation._ActorMailbox._run
    original_close = implementation.TaskStream.close
    entered, release = Event(), Event()
    attempts = []
    primary = SystemExit("started but caller interrupted")

    def start(thread):
        original_start(thread)
        if thread.name == "graph-sail-stream":
            raise primary

    def run(mailbox, function):
        entered.set()
        assert release.wait(10)
        return original_run(mailbox, function)

    def close(mailbox, timeout=None):
        attempts.append(timeout)
        return original_close(mailbox, timeout)

    monkeypatch.setattr(threading.Thread, "start", start)
    monkeypatch.setattr(implementation._ActorMailbox, "_run", run)
    monkeypatch.setattr(implementation.TaskStream, "close", close)
    try:
        with pytest.raises(SystemExit) as caught:
            owner.stream(
                "values", stream_config=gs.ActorStreamConfig(shutdown_timeout_seconds=0.01)
            )
        assert caught.value is primary and entered.is_set()
        assert attempts == [0.01, 0.01]
        retained = primary.actor_stream_cleanup
        assert not retained.closed and retained._stream._thread.is_alive()
        assert retained._stream._stop.is_set() and not retained.state.lease_released
        release.set()
        retained.close(10)
        assert retained.closed and not retained._stream._thread.is_alive()
    finally:
        release.set()
        owner._stream_owner.close(10)
        owner.close()


@pytest.mark.parametrize("field", [5, 6])
def test_invalid_diagnostic_utf8_is_typed_protocol_loss(field):
    driver = controlled_driver()
    record = ["error", 42, 1, 0, "advance", "ValueError", "message"]
    record[field] = "\ud800"
    with pytest.raises(gs.ActorDiedError):
        driver._record(tuple(record), "advance")
    assert driver.poisoned


@pytest.mark.parametrize("action", ["close", "terminate"])
def test_shutdown_wins_before_driver_start_without_resurrection(monkeypatch, action):
    owner = actor()
    admitted, resume_start, running = Event(), Event(), Event()
    original_start = implementation._ActorMailbox._start
    original_run = implementation._ActorMailbox._run

    def delayed_start(mailbox):
        admitted.set()
        assert resume_start.wait(10)
        return original_start(mailbox)

    def observe_run(mailbox, function):
        running.set()
        return original_run(mailbox, function)

    monkeypatch.setattr(implementation._ActorMailbox, "_start", delayed_start)
    monkeypatch.setattr(implementation._ActorMailbox, "_run", observe_run)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(owner.stream, "values")
        try:
            assert admitted.wait(10)
            retained = owner._stream_owner
            if action == "close":
                owner.close(0)
            else:
                owner.terminate()
            assert owner._closed.is_set()
            resume_start.set()
            with pytest.raises(gs.ActorClosedError) as caught:
                future.result(10)
            assert caught.value.actor_stream_cleanup is retained
            assert retained.closed and not running.is_set()
            assert retained._stream._thread.ident is None
            assert retained.state.lease_released and not retained.state.generator_opened
        finally:
            resume_start.set()
            with suppress(gs.ActorClosedError):
                future.result(10)
            owner._stream_owner.close(10)
            owner.close()


@pytest.mark.parametrize("action", ["close", "terminate"])
def test_actual_driver_start_wins_and_shutdown_waits_for_join(monkeypatch, action):
    entered, resume = Event(), Event()
    original_run = implementation._ActorMailbox._run

    def held_run(mailbox, function):
        entered.set()
        assert resume.wait(10)
        return original_run(mailbox, function)

    monkeypatch.setattr(implementation._ActorMailbox, "_run", held_run)
    with actor() as owner, ThreadPoolExecutor(max_workers=1) as pool:
        stream = owner.stream("values")
        closing = None
        try:
            assert entered.wait(10)
            closing = (
                pool.submit(owner.close, 0) if action == "close" else pool.submit(owner.terminate)
            )
            until(owner._closed.is_set)
            assert not closing.done()
            assert stream._stream._thread.is_alive() and not stream.closed
            resume.set()
            closing.result(10)
            assert stream.closed and not stream._stream._thread.is_alive()
            assert stream.state.lease_released and stream.state.actor_resources_closed
        finally:
            resume.set()
            if closing is not None:
                closing.result(10)
            stream.close(10)


@pytest.mark.parametrize("has_failure", [False, True])
@pytest.mark.parametrize("control", [False, True])
def test_broker_notification_failure_priority_and_pending_settlement(has_failure, control):
    # A local broker seam: fake transport selects its terminal notification.
    # Native process ownership is separately tested below with actual spawn.
    owner = object.__new__(gs.ProcessActor)
    earlier = gs.ActorClosedError("earlier transport stop") if has_failure else None
    notification = KeyboardInterrupt("notification") if control else OSError("notification")
    pending = ActorCall(1, "advance")
    cleaned = []

    def cancel():
        raise notification

    def joined(timeout):
        owner._stream_lease = object()

    owner._condition = Condition()
    owner._abort = earlier
    owner._queue = deque()
    owner._pending = {1: pending}
    owner._closing = True
    owner._stream_lease = object() if has_failure else None
    owner._stream_owner = SimpleNamespace(_stream=SimpleNamespace(cancel=cancel))
    owner._config = gs.ActorConfig()
    owner._connection = SimpleNamespace(send_bytes=lambda data: None)
    owner._process = SimpleNamespace(join=joined)
    owner._cleanup_process = lambda *, graceful: cleaned.append(graceful)
    escaped = None
    try:
        owner._serve()
    except BaseException as error:
        escaped = error
    assert escaped is None, f"broker propagated {type(escaped).__name__}"
    assert cleaned and pending.done() and owner.pending_count == 0
    failure = pending.exception()
    assert failure is owner.failure
    if has_failure and not control:
        assert failure is earlier
        assert any("OSError" in note for note in failure.__notes__)
    else:
        assert isinstance(failure, gs.ActorDiedError)
        assert failure.__cause__ is notification
        if has_failure:
            assert any("ActorClosedError" in note for note in notification.__notes__)


@pytest.mark.parametrize("control", [False, True])
def test_broker_notification_failure_always_closes_actual_child(monkeypatch, tmp_path, control):
    import threading

    owner = actor()
    marker = tmp_path / "entered"
    stream = owner.stream("wait", args=(str(marker),))
    original_cancel = stream._stream.cancel
    notification = (
        KeyboardInterrupt("broker notification") if control else OSError("broker notification")
    )
    earlier = gs.ActorClosedError("explicit stop")
    observed = []

    def cancel():
        if current_thread() is owner._thread:
            raise notification
        return original_cancel()

    try:
        until(marker.exists)
        with owner._condition:
            pending = next(iter(owner._pending.values()))
        monkeypatch.setattr(stream._stream, "cancel", cancel)
        monkeypatch.setattr(threading, "excepthook", lambda args: observed.append(args.exc_value))
        owner._request_abort(earlier)
        owner._thread.join(10)
        assert not owner._thread.is_alive()
        assert owner._process_closed and not owner.alive
        assert owner._connection.closed and all(e.closed for e in owner._actor_stream_endpoints)
        assert pending.done() and not observed
        failure = pending.exception()
        assert failure is owner.failure
        if control:
            assert isinstance(failure, gs.ActorDiedError) and failure.__cause__ is notification
        else:
            assert failure is earlier
        with pytest.raises(gs.ActorError):
            stream.completion(10)
    finally:
        monkeypatch.setattr(stream._stream, "cancel", original_cancel)
        original_cancel()
        owner._thread.join(10)
        if not owner._process_closed:
            implementation._retry_cleanup(owner)
        stream.close(10)
        owner.close()
