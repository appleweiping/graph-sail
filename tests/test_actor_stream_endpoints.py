"""Parent-only endpoint ownership; raw child-wire and allocation failure bounds."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from threading import Event, Lock, current_thread
from types import SimpleNamespace

import pytest

import graph_sail as gs
import graph_sail.actor_streams as implementation
from tests.test_actor_method_streams import actor


@pytest.mark.parametrize("seam", ["wrapper-1", "wrapper-2", "lock-1", "lock-2", "lock-3", "owner"])
def test_python_ownership_allocation_precedes_native_pipe(monkeypatch, seam):
    context = implementation.multiprocessing.get_context("spawn")
    original_pipe = context.Pipe
    original_wrapper = implementation._ActorCancellationEndpoint
    original_lock = implementation.Lock
    original_owner = implementation._StreamStartupCleanup
    pipes = []
    counts = {"wrapper": 0, "lock": 0}
    control = KeyboardInterrupt("pre-native ownership allocation")

    def pipe(*args, **kwargs):
        pipes.append(True)
        return original_pipe(*args, **kwargs)

    def wrapper():
        counts["wrapper"] += 1
        if seam == f"wrapper-{counts['wrapper']}":
            raise control
        return original_wrapper()

    def lock():
        counts["lock"] += 1
        if seam == f"lock-{counts['lock']}":
            raise control
        return original_lock()

    def cleanup_owner(*args, **kwargs):
        if seam == "owner":
            raise control
        return original_owner(*args, **kwargs)

    monkeypatch.setattr(context, "Pipe", pipe)
    monkeypatch.setattr(implementation, "_ActorCancellationEndpoint", wrapper)
    monkeypatch.setattr(implementation, "Lock", lock)
    monkeypatch.setattr(implementation, "_StreamStartupCleanup", cleanup_owner)
    with pytest.raises(KeyboardInterrupt) as caught:
        actor()
    assert caught.value is control and pipes == []


@pytest.mark.parametrize("index", [0, 1])
@pytest.mark.parametrize("after_bind", [False, True])
def test_bind_failure_retains_and_closes_both_real_raw_endpoints(monkeypatch, index, after_bind):
    context = implementation.multiprocessing.get_context("spawn")
    original_pipe = context.Pipe
    original_bind = implementation._ActorCancellationEndpoint.bind
    raw_pairs = []
    count = []
    control = SystemExit("endpoint binding failure")

    def pipe(*args, **kwargs):
        result = original_pipe(*args, **kwargs)
        raw_pairs.append(result)
        return result

    def bind(endpoint, raw):
        position = len(count)
        count.append(position)
        if position == index and not after_bind:
            raise control
        original_bind(endpoint, raw)
        if position == index:
            raise control

    monkeypatch.setattr(context, "Pipe", pipe)
    monkeypatch.setattr(implementation._ActorCancellationEndpoint, "bind", bind)
    with pytest.raises(SystemExit) as caught:
        actor()
    assert caught.value is control
    retained = control.actor_stream_cleanup
    assert retained.closed and len(raw_pairs) == 1
    assert all(endpoint.closed for endpoint in raw_pairs[0])
    assert not hasattr(retained._actor, "_process")
    retained.close()


@pytest.mark.parametrize("closed_before_error", [False, True])
def test_native_close_failure_preserves_closed_truth_and_retry(closed_before_error):
    endpoint = implementation._ActorCancellationEndpoint()
    assert endpoint.closed
    endpoint.close()
    with pytest.raises(RuntimeError):
        endpoint.poll()
    raw = SimpleNamespace(closed=False, poll=lambda timeout=0: False)
    calls = []
    control = KeyboardInterrupt("native close interrupted")

    def close():
        calls.append(True)
        if len(calls) == 1:
            raw.closed = closed_before_error
            raise control
        raw.closed = True

    raw.close = close
    endpoint.bind(raw)
    with pytest.raises(RuntimeError):
        endpoint.bind(raw)
    assert not endpoint.poll()
    with pytest.raises(KeyboardInterrupt) as caught:
        endpoint.close()
    assert caught.value is control
    assert endpoint.closed is closed_before_error
    endpoint.close()
    assert endpoint.closed and len(calls) == (1 if closed_before_error else 2)
    endpoint.close()


def test_actual_native_cancellation_endpoint_close_is_serialized(monkeypatch):
    owner = actor()
    stream = owner.stream("values", args=(3,), config=gs.TaskStreamConfig(max_buffered=1))
    native = owner._actor_stream_sender._connection
    original_native_close = native._close
    original_cancel = stream._stream.cancel
    entered, release, attempted, overlap = Event(), Event(), Event(), Event()
    lock = Lock()
    active = [0]

    def native_close():
        with lock:
            active[0] += 1
            if active[0] > 1:
                overlap.set()
        try:
            if current_thread() is owner._thread:
                entered.set()
                assert release.wait(10)
            original_native_close()
        finally:
            with lock:
                active[0] -= 1

    def cancel():
        if current_thread() is owner._thread:
            raise OSError("stop notification failed")
        return original_cancel()

    def cancel_from_caller():
        attempted.set()
        return stream.cancel()

    try:
        assert stream.wait_ready(10)
        monkeypatch.setattr(native, "_close", native_close)
        monkeypatch.setattr(stream._stream, "cancel", cancel)
        owner._request_abort(gs.ActorClosedError("endpoint overlap probe"))
        assert entered.wait(10)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(cancel_from_caller)
            try:
                assert attempted.wait(10)
                assert not overlap.wait(0.25), "two callers entered the same native close"
            finally:
                release.set()
                with suppress(OSError, TypeError):
                    future.result(10)
        owner._thread.join(10)
        assert owner._process_closed and all(e.closed for e in owner._actor_stream_endpoints)
        assert owner._cleanup_error is None
    finally:
        release.set()
        monkeypatch.setattr(stream._stream, "cancel", original_cancel)
        owner._thread.join(10)
        if owner._cleanup_error is not None:
            implementation._retry_cleanup(owner)
        stream.close(10)
        owner.close()
