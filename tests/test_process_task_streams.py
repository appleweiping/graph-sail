from __future__ import annotations

import multiprocessing
import os
import time

import pytest
from process_stream_functions import (
    abandon_stream_owner,
    bad_close,
    bad_yield,
    control,
    cooperative,
    counted,
    empty,
    failing,
    snapshots,
    stubborn,
)

import graph_sail
from graph_sail import (
    ActorDiedError,
    ActorRemoteError,
    ActorSerializationError,
    ActorStartupError,
    ActorTimeoutError,
    ProcessStreamConfig,
    TaskStreamConfig,
)


@pytest.fixture(autouse=True)
def no_owned_children_left():
    before = {child.pid for child in multiprocessing.active_children()}
    yield
    assert {child.pid for child in multiprocessing.active_children()} == before


def wait_marker(path, expected="entered"):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if path.exists() and path.read_text(encoding="ascii") == expected:
            return
        time.sleep(0.005)
    pytest.fail(f"expected marker did not appear: {expected}")


def test_real_process_backpressure_and_exact_cap(tmp_path):
    marker = tmp_path / "advances.txt"
    with graph_sail.start_process_task_stream(
        counted, args=(str(marker),), config=TaskStreamConfig(max_buffered=1, max_yields=2)
    ) as stream:
        assert stream.wait_ready(30)
        assert marker.read_text() == "next:0\n"
        first = stream.next(30)
        assert first.sequence == 0 and first.value[1] == 0
        assert first.value[0] != os.getpid()
        result = stream.completion(30)
        assert result.status == "limited" and result.produced == 2
        assert stream.next(30).value == (first.value[0], 1)
        assert list(stream) == []
        assert marker.read_text() == "next:0\nnext:1\nfinally\n"
        assert result.worker.pid == first.value[0]
        assert result.worker.generator_closed and result.worker.resources_closed
    assert stream.closed


def test_true_empty_stream_and_none_prefix_before_remote_failure():
    with graph_sail.start_process_task_stream(empty) as stream:
        assert list(stream) == []
        assert stream.completion().status == "succeeded"
    with graph_sail.start_process_task_stream(failing) as stream:
        assert stream.next(30).value is None
        with pytest.raises(ActorRemoteError, match="expected remote failure"):
            stream.next(30)
        with pytest.raises(ActorRemoteError):
            stream.completion()
    assert stream.closed


def test_each_yield_is_an_independent_serialized_snapshot_and_return_is_not_a_yield():
    with graph_sail.start_process_task_stream(snapshots, kwargs={"count": 3}) as stream:
        items = list(stream)
        assert [(i.sequence, i.value) for i in items] == [(0, [0]), (1, [0, 1]), (2, [0, 1, 2])]
        items[0].value.append("parent mutation")
        assert items[1].value == [0, 1]
        assert stream.completion().produced == 3


@pytest.mark.parametrize("kind", ["large", "unpicklable"])
def test_complete_response_frame_is_bounded_and_accepted_prefix_precedes_error(kind):
    import pickle

    assert len(bytes(1000)) < 1024
    assert len(pickle.dumps((1, "ok", ("yield", 123, 0, bytes(1000))), protocol=5)) > 1024
    with graph_sail.start_process_task_stream(
        bad_yield, args=(kind,), process_config=ProcessStreamConfig(max_message_bytes=1024)
    ) as stream:
        assert stream.next(30).value == "prefix"
        with pytest.raises(ActorSerializationError):
            stream.next(30)
        assert stream.worker.generator_closed and stream.worker.resources_closed


@pytest.mark.parametrize("kind", ["exit", "interrupt"])
def test_remote_control_is_process_loss_not_original_control_object(tmp_path, kind):
    marker = tmp_path / "control-finally"
    with graph_sail.start_process_task_stream(control, args=(kind, str(marker))) as stream:
        pid = stream.next(30).value
        with pytest.raises(ActorDiedError):
            stream.next(30)
        assert stream.worker.pid == pid and stream.worker.resources_closed
        # The marker independently witnesses this fixture's actual effect.
        # An absent ACK remains unknown despite that effect being observed.
        assert marker.read_text() == "finally"
        assert not stream.worker.generator_closed


def test_cooperative_running_cancel_preserves_prefix_and_confirms_close(tmp_path):
    marker = tmp_path / "cancel"
    with graph_sail.start_process_task_stream(cooperative, args=(str(marker),)) as stream:
        assert stream.wait_ready(30)
        wait_marker(marker)
        assert stream.cancel()
        result = stream.completion(30)
        assert result.status == "cancelled" and result.produced == 1
        assert stream.next(0).value == result.worker.pid
        assert list(stream) == []
        assert result.worker.cancellation_requested and result.worker.generator_closed
        assert not result.worker.termination_requested
        assert marker.read_text() == "finally"
        assert not stream.cancel()


def test_cancel_noncooperative_next_reaps_without_claiming_finally(tmp_path):
    marker = tmp_path / "stubborn"
    with graph_sail.start_process_task_stream(
        stubborn,
        args=(str(marker),),
        process_config=ProcessStreamConfig(cancellation_grace_seconds=0.05),
    ) as stream:
        assert stream.wait_ready(30)
        wait_marker(marker)
        stream.cancel()
        result = stream.completion(30)
        assert result.status == "cancelled"
        assert result.worker.termination_requested and result.worker.resources_closed
        assert not result.worker.generator_closed and marker.read_text() == "entered"
        assert stream.next(0).value == result.worker.pid


def test_advance_deadline_is_failure_not_success_or_eof(tmp_path):
    marker = tmp_path / "deadline"
    with graph_sail.start_process_task_stream(
        stubborn,
        args=(str(marker),),
        process_config=ProcessStreamConfig(advance_timeout_seconds=1, cancellation_grace_seconds=0),
    ) as stream:
        assert stream.next(30).value != os.getpid()
        with pytest.raises(ActorTimeoutError, match="advance budget"):
            stream.next(30)
        assert stream.worker.termination_requested and stream.worker.resources_closed


@pytest.mark.parametrize("kind", ["raise", "block"])
def test_generator_cleanup_failure_is_observed_even_at_exact_cap(tmp_path, kind):
    marker = tmp_path / "close"
    with graph_sail.start_process_task_stream(
        bad_close,
        args=(str(marker), kind),
        config=TaskStreamConfig(max_yields=1),
        process_config=ProcessStreamConfig(shutdown_timeout_seconds=0.2),
    ) as stream:
        assert stream.next(30).value == "prefix"
        with pytest.raises(ActorRemoteError if kind == "raise" else TimeoutError):
            stream.completion(30)
        assert stream.worker.resources_closed and not stream.worker.generator_closed
        assert marker.read_text() == "entered"


def test_wait_only_timeout_then_explicit_close_timeout_retains_owner(tmp_path):
    marker = tmp_path / "retry"
    stream = graph_sail.start_process_task_stream(
        stubborn,
        args=(str(marker),),
        process_config=ProcessStreamConfig(cancellation_grace_seconds=0.1),
    )
    try:
        assert stream.next(30).value != os.getpid()
        wait_marker(marker)
        assert not stream.wait_ready(0)
        with pytest.raises(TimeoutError):
            stream.next(0)
        assert not stream.worker.cancellation_requested
        with pytest.raises(TimeoutError):
            stream.close(0)
        assert not stream.closed
        stream.close(30)
        assert stream.closed and stream.worker.resources_closed
    finally:
        stream.close(30)


@pytest.mark.parametrize("blocked", [False, True])
def test_actual_communication_owner_death_closes_idle_or_cooperative_generator(tmp_path, blocked):
    from graph_sail.actors import _pack, _worker
    from graph_sail.process_task_streams import _StreamWorker

    context = multiprocessing.get_context("spawn")
    parent_end, worker_end = context.Pipe()
    receiver, sender = context.Pipe(duplex=False)
    marker = tmp_path / "dead-owner"
    function = cooperative if blocked else counted
    startup = _pack(
        (_StreamWorker, ("advance", "finish"), (function, (str(marker),), {}), {}), 1024
    )
    worker = context.Process(target=_worker, args=(worker_end, startup, 1024, receiver))
    owner = context.Process(
        target=abandon_stream_owner, args=(parent_end, sender, str(marker), blocked)
    )
    try:
        worker.start()
        owner.start()
        # The dying owner now holds the only parent endpoints. The supervisor
        # retains process handles solely to verify/reap even a failing fixture.
        for endpoint in (parent_end, worker_end, receiver, sender):
            endpoint.close()
        owner.join(30)
        assert not owner.is_alive() and owner.exitcode == 0
        worker.join(30)
        assert not worker.is_alive() and worker.exitcode == 0
        assert "finally" in marker.read_text()
        assert worker.pid != owner.pid and worker.pid != os.getpid()
    finally:
        for endpoint in (parent_end, worker_end, receiver, sender):
            endpoint.close()
        for process in (owner, worker):
            if process.pid is not None:
                if process.is_alive():
                    process.terminate()
                process.join(10)
            process.close()


def test_real_remote_argument_binding_failure_returns_closed_startup_owner():
    with pytest.raises(ActorStartupError) as caught:
        graph_sail.start_process_task_stream(empty, args=("unexpected",))
    assert caught.value.process_stream_cleanup.closed


@pytest.mark.parametrize("after_start", [False, True])
def test_real_parent_driver_thread_start_failure_still_reaps_native_worker(
    monkeypatch, after_start
):
    import graph_sail.task_streams as thread_implementation

    original = thread_implementation.Thread.start
    primary = KeyboardInterrupt()

    def start(thread):
        if thread.name != "graph-sail-stream":
            return original(thread)
        if after_start:
            original(thread)
        raise primary

    monkeypatch.setattr(thread_implementation.Thread, "start", start)
    with pytest.raises(KeyboardInterrupt) as caught:
        graph_sail.start_process_task_stream(empty)
    assert caught.value is primary and primary.process_stream_cleanup.closed
