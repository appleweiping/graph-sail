from __future__ import annotations

import hashlib
import json
import multiprocessing
import operator
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

import pytest
from process_execution_tasks import (
    FailAfterOtherEntered,
    ReadObject,
    Rendezvous,
    StopTask,
    Value,
    abandon_endpoint,
    exit_worker,
    multiply,
    mutate_dependency,
    pid_task,
    retry_value_error,
    spontaneously_cancel,
    unpicklable_result,
    wait_for,
)

from graph_sail import (
    ActorSerializationError,
    ExecutionConfig,
    LocalObjectStore,
    ProcessTaskConfig,
    TaskCancelled,
    TaskContext,
    TaskDefinition,
    TaskRegistry,
    ValidationError,
    execute_process_graph,
    graph_from_dict,
)


def graph_for(nodes=("a", "b", "join"), edges=(("a", "join"), ("b", "join"))):
    return graph_from_dict(
        {
            "name": "process-oracle",
            "devices": [{"name": "cpu", "memory_mb": 100}],
            "nodes": [
                {"id": node, "kind": "test", "memory_mb": 1, "latency_ms": {"cpu": 999999}}
                for node in nodes
            ],
            "edges": [{"source": a, "target": b} for a, b in edges],
        }
    )


def run(tasks, *, edges=(), config=None, process_config=None, cancel_event=None):
    graph = graph_for(tuple(tasks), edges)
    return execute_process_graph(
        graph,
        TaskRegistry(tasks),
        dict.fromkeys(tasks, "cpu"),
        config=config or ExecutionConfig(max_workers=1),
        process_config=process_config,
        cancel_event=cancel_event,
    )


@pytest.fixture(autouse=True)
def no_owned_children_left():
    before = {child.pid for child in multiprocessing.active_children()}
    yield
    assert {child.pid for child in multiprocessing.active_children()} == before


def test_real_spawn_parallel_diamond_uses_independent_oracle_and_reports_actual_timings(tmp_path):
    result = run(
        {"a": Rendezvous(str(tmp_path)), "b": Rendezvous(str(tmp_path)), "join": multiply},
        edges=(("a", "join"), ("b", "join")),
        config=ExecutionConfig(max_workers=2, device_workers={"cpu": 2}),
    )
    assert result.execution.outputs == {"a": 6, "b": 7, "join": 42}
    pids = {int((tmp_path / name).read_text()) for name in ("a", "b")}
    assert len(pids) == 2 and os.getpid() not in pids
    assert {worker.pid for worker in result.workers} == pids
    assert len(result.workers) == 2  # the join reuses a healthy worker
    assert result.execution.peak_in_flight_by_device == {"cpu": 2}
    attempts = {task.node_id: task.attempts[0] for task in result.execution.tasks}
    assert attempts["join"].started_ms >= max(attempts[n].finished_ms for n in ("a", "b"))
    for attempt in attempts.values():
        assert 0 <= attempt.submitted_ms <= attempt.started_ms <= attempt.finished_ms
    assert all(item.timing_source == "worker" for item in result.attempts)
    assert all(item.round_trip_ms >= 0 for item in result.attempts)
    assert all(worker.exitcode == 0 for worker in result.workers)
    telemetry = result.to_dict()
    assert telemetry["kind"] == "graph-sail-process-execution"
    assert "outputs" not in telemetry["execution"]
    assert result.elapsed_ms >= result.execution.elapsed_ms
    json.dumps(telemetry, allow_nan=False)


def test_healthy_worker_reuse_and_process_dependency_copy_isolation():
    result = run({"a": Value([1, 2]), "b": mutate_dependency}, edges=(("a", "b"),))
    assert result.execution.outputs == {"a": [1, 2], "b": [1, 2, 9]}
    assert len(result.workers) == 1
    assert {attempt.pid for attempt in result.attempts} == {result.workers[0].pid}


@pytest.mark.parametrize(
    "retry_on, status, count", [((ValueError,), "succeeded", 2), ((KeyError,), "failed", 1)]
)
def test_retry_class_is_evaluated_in_child_without_remote_exception_reconstruction(
    retry_on, status, count
):
    result = run({"a": TaskDefinition(retry_value_error, 1, retry_on)})
    assert result.execution.status == status
    assert len(result.execution.tasks[0].attempts) == count
    assert result.execution.tasks[0].attempts[0].error_type == "ValueError"
    assert len(result.workers) == 1
    if status == "succeeded":
        assert result.execution.outputs["a"] == (42, result.workers[0].pid)


@pytest.mark.parametrize(
    "task, error",
    [(exit_worker, "ActorDiedError"), (unpicklable_result, "ActorSerializationError")],
)
def test_ambiguous_failures_never_retry_and_independent_work_uses_joined_replacement(task, error):
    result = run({"a": TaskDefinition(task, max_retries=2), "b": pid_task})
    tasks = {item.node_id: item for item in result.execution.tasks}
    assert tasks["a"].status == "failed" and len(tasks["a"].attempts) == 1
    assert tasks["a"].attempts[0].error_type == error
    assert tasks["b"].status == "succeeded"
    assert len(result.workers) == 2
    assert result.attempts[0].timing_source == "driver"
    assert result.attempts[0].pid != result.attempts[1].pid


@pytest.mark.parametrize("cooperative, translate", [(True, False), (True, True), (False, False)])
def test_external_cancellation_eof_is_irrevocable_and_every_worker_is_joined(
    tmp_path, cooperative, translate
):
    event = Event()
    entered, observed = tmp_path / "entered", tmp_path / "observed"
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(
            run,
            {"a": StopTask(str(entered), str(observed), cooperative, translate)},
            cancel_event=event,
            process_config=ProcessTaskConfig(cancellation_grace_seconds=0.15),
        )
        try:
            wait_for(entered)
        finally:
            event.set()
        result = pending.result(15)
    assert result.execution.status == "cancelled"
    assert result.execution.cancellation_reason == "external_cancellation"
    assert result.execution.outputs == {}
    assert result.attempts[0].cancellation_requested
    if not cooperative:
        assert result.workers[0].termination_requested and not observed.exists()
    elif not result.workers[0].termination_requested:
        # Cooperation only avoids termination if its response arrives within
        # the configured grace. OS/coverage delays can legitimately exceed it.
        assert observed.exists() and result.workers[0].exitcode == 0


def test_attempt_timeout_does_not_accept_late_success_or_retry_ambiguous_attempt(tmp_path):
    result = run(
        {
            "a": TaskDefinition(
                StopTask(str(tmp_path / "entered"), str(tmp_path / "observed"), translate=True),
                max_retries=3,
            ),
            "b": operator.attrgetter("node_id"),
        },
        process_config=ProcessTaskConfig(attempt_timeout_seconds=3, cancellation_grace_seconds=1),
    )
    first = result.execution.tasks[0]
    assert first.status == "failed" and len(first.attempts) == 1
    assert first.attempts[0].error_type == "ProcessTaskTimeout"
    assert (tmp_path / "observed").exists()
    assert result.execution.outputs.keys() == {"b"}
    assert len(result.workers) == 2
    assert result.workers[0].cancellation_requested


def test_spontaneously_cancelled_worker_is_retired_before_independent_task():
    result = run({"a": spontaneously_cancel, "b": pid_task})
    assert result.execution.tasks[0].status == "cancelled"
    assert result.execution.tasks[1].status == "succeeded"
    assert len(result.workers) == 2


def test_object_ref_moves_two_mib_dependency_over_four_kib_actor_transport(tmp_path):
    data = bytes(range(256)) * 8192
    with LocalObjectStore(tmp_path) as store:
        ref = store.put(data)
        result = run(
            {"a": Value(ref), "b": ReadObject(store.client())},
            edges=(("a", "b"),),
            process_config=ProcessTaskConfig(max_message_bytes=4096),
        )
        output = result.execution.outputs["b"]
        assert output[:3] == (2097152, 267386880, hashlib.sha256(data).hexdigest())
        assert output[3] != os.getpid()
        with pytest.raises(ActorSerializationError):
            run({"a": Value(data)}, process_config=ProcessTaskConfig(max_message_bytes=4096))


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_message_bytes": True},
        {"max_message_bytes": 1023},
        {"max_message_bytes": 16 * 1024**2 + 1},
        {"startup_timeout_seconds": 0},
        {"startup_timeout_seconds": 1e-6},
        {"startup_timeout_seconds": float("nan")},
        {"startup_timeout_seconds": 10**1000},
        {"attempt_timeout_seconds": False},
        {"attempt_timeout_seconds": -1},
        {"cancellation_grace_seconds": float("inf")},
        {"cancellation_grace_seconds": -1},
        {"shutdown_timeout_seconds": 61},
    ],
)
def test_process_config_bounds(kwargs):
    with pytest.raises(ValidationError):
        ProcessTaskConfig(**kwargs)


@pytest.mark.parametrize(
    "kind", ["callable", "placements", "registry", "workers", "capacity", "config"]
)
def test_preflight_rejects_before_any_worker_is_created(kind, monkeypatch):
    from graph_sail.actors import ProcessActor

    def forbidden(*args, **kwargs):
        raise AssertionError("preflight started a worker")

    monkeypatch.setattr(ProcessActor, "_for_task_worker", forbidden)
    graph = graph_for(("a",), ())
    registry = TaskRegistry({"a": (lambda context: 1) if kind == "callable" else pid_task})
    config = ExecutionConfig(
        max_workers=17 if kind == "workers" else 16 if kind == "capacity" else 1
    )
    with pytest.raises((ValidationError, ActorSerializationError)):
        execute_process_graph(
            graph,
            TaskRegistry({"other": pid_task}) if kind == "registry" else registry,
            {"a": "wrong" if kind == "placements" else "cpu"},
            config=config,
            process_config=object()
            if kind == "config"
            else ProcessTaskConfig(max_message_bytes=16 * 1024**2 if kind == "capacity" else 4096),
        )


def test_precancelled_execution_starts_no_process(monkeypatch):
    from graph_sail.actors import ProcessActor

    event = Event()
    event.set()
    monkeypatch.setattr(ProcessActor, "_for_task_worker", lambda *args: pytest.fail("spawned"))
    result = run({"a": pid_task}, cancel_event=event)
    assert result.execution.status == "cancelled"
    assert result.workers == result.attempts == ()


def test_native_eof_channel_can_cancel_without_any_byte_message():
    from graph_sail.actors import ActorConfig, ProcessActor

    actor, sender = ProcessActor._for_task_worker(ActorConfig(max_pending=1))
    try:
        sender.close()
        sender.close()  # idempotent signal; no send call exists
        response = actor.submit(
            "invoke", args=(TaskDefinition(pid_task), "a", "cpu", 1, {}, time.monotonic(), 0.0)
        ).result(10)
        assert response.outcome.attempt.status == "cancelled"
        assert response.outcome.value is None
    finally:
        sender.close()
        actor.close()


def test_fail_fast_uses_shared_scheduler_and_closes_other_running_native_channel(tmp_path):
    entered, observed = tmp_path / "entered", tmp_path / "observed"
    result = run(
        {
            "a": FailAfterOtherEntered(str(entered)),
            "b": StopTask(str(entered), str(observed)),
            "descendant": pid_task,
        },
        edges=(("a", "descendant"),),
        config=ExecutionConfig(max_workers=2, device_workers={"cpu": 2}, fail_fast=True),
        process_config=ProcessTaskConfig(cancellation_grace_seconds=1),
    )
    tasks = {task.node_id: task for task in result.execution.tasks}
    assert tasks["a"].status == "failed"
    assert tasks["b"].status == "cancelled"
    assert tasks["descendant"].status == "skipped"
    assert result.execution.cancellation_reason == "fail_fast"
    assert observed.exists()
    assert len(result.workers) == 2


def test_total_execution_timeout_stops_admission_and_joins_noncooperative_worker(tmp_path):
    result = run(
        {
            "a": StopTask(str(tmp_path / "entered"), str(tmp_path / "observed"), False),
            "b": pid_task,
        },
        config=ExecutionConfig(max_workers=1, timeout_seconds=5),
        process_config=ProcessTaskConfig(cancellation_grace_seconds=0),
    )
    assert result.execution.status == "cancelled"
    assert result.execution.cancellation_reason == "timeout"
    assert result.execution.outputs == {}
    assert result.execution.tasks[1].attempts == ()
    assert len(result.workers) == 1


def test_native_cancellation_latches_when_sole_sender_process_disappears(tmp_path):
    from graph_sail.process_execution import _PipeCancellation

    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    entered, release = tmp_path / "entered", tmp_path / "release"
    producer = context.Process(target=abandon_endpoint, args=(sender, str(entered), str(release)))
    try:
        producer.start()
        sender.close()
        wait_for(entered)
        signal = _PipeCancellation(receiver)
        assert not signal.cancelled
        release.write_text("go", encoding="ascii")
        producer.join(10)
        assert not producer.is_alive() and producer.exitcode == 0
        assert signal.cancelled
        receiver.close()
        assert signal.cancelled  # no further OS reads after the irreversible latch
        with pytest.raises(TaskCancelled):
            signal.raise_if_cancelled()
    finally:
        sender.close()
        receiver.close()
        if producer.is_alive():
            producer.terminate()
            producer.join(5)
        producer.close()


def test_parallel_independent_executions_cancel_result_race_leaves_no_child(tmp_path):
    events = [Event() for _ in range(4)]
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [
            pool.submit(
                run,
                {
                    "a": StopTask(
                        str(tmp_path / f"entered-{i}"),
                        str(tmp_path / f"observed-{i}"),
                        translate=True,
                    )
                },
                cancel_event=event,
                process_config=ProcessTaskConfig(cancellation_grace_seconds=1),
            )
            for i, event in enumerate(events)
        ]
        try:
            for i in range(4):
                wait_for(tmp_path / f"entered-{i}")
        finally:
            for event in events:
                event.set()
        results = [future.result(20) for future in futures]
    assert len({result.workers[0].pid for result in results}) == 4
    assert all(result.execution.status == "cancelled" for result in results)
    assert all(result.execution.outputs == {} for result in results)


class FakeEndpoint:
    def __init__(self, *, close_error=None, poll_error=None):
        self.close_error, self.poll_error = close_error, poll_error
        self.closes = 0

    def close(self):
        self.closes += 1
        if self.close_error is not None:
            raise self.close_error

    def poll(self, timeout=0):
        if self.poll_error is not None:
            raise self.poll_error
        return False


class FakeActor:
    pid = 123
    exitcode = 0

    def __init__(self, call=None, *, close_error=None):
        self.call, self.close_error = call, close_error
        self.closes = self.terminations = 0

    def submit(self, *args, **kwargs):
        return self.call

    def close(self, timeout=5):
        self.closes += 1
        if self.close_error is not None:
            raise self.close_error

    def terminate(self):
        self.terminations += 1
        if self.close_error is not None:
            raise self.close_error


class FakeCall:
    elapsed_seconds = 0.1

    def __init__(self, value=None, *, error=None, done=True):
        self.value, self.error, self.completed = value, error, done

    def done(self):
        return self.completed

    def result(self, timeout=None):
        if self.error is not None:
            raise self.error
        return self.value


def context_for(event=None):
    from graph_sail.execution import CancellationToken

    return TaskContext("a", "cpu", 1, {}, CancellationToken(event or Event()))


@pytest.mark.parametrize("error", [OSError("gone"), ValueError("closed")])
def test_native_observation_fails_closed_after_handle_error(error):
    from graph_sail.process_execution import _PipeCancellation

    endpoint = FakeEndpoint(poll_error=error)
    signal = _PipeCancellation(endpoint)
    assert signal.cancelled
    endpoint.poll_error = RuntimeError("must not read after latch")
    assert signal.cancelled


@pytest.mark.parametrize("cause", ["external", "timeout"])
@pytest.mark.parametrize("reply", ["success", "broken"])
def test_stop_has_precedence_over_already_completed_or_broken_response(cause, reply):
    from graph_sail.actors import ActorDiedError
    from graph_sail.process_execution import ProcessTaskTimeout, _Lease, _ProcessInvoker

    event = Event()
    if cause == "external":
        event.set()
    invoker = _ProcessInvoker(ProcessTaskConfig(attempt_timeout_seconds=0.001))
    call = FakeCall("late success", error=ActorDiedError("broken") if reply == "broken" else None)
    lease = _Lease(1, FakeActor(call), FakeEndpoint())
    with pytest.raises(TaskCancelled if cause == "external" else ProcessTaskTimeout):
        invoker._wait(lease, call, context_for(event), time.monotonic() - 1)
    assert lease.cancelled and lease.sender.closes == 1


def test_cancel_channel_close_error_preserves_stop_and_retirement_still_attempts_join(monkeypatch):
    from graph_sail.process_execution import _Lease, _ProcessInvoker

    event = Event()
    invoker = _ProcessInvoker(ProcessTaskConfig())
    endpoint = FakeEndpoint(close_error=OSError("close error"))
    actor = FakeActor(FakeCall(done=False))
    lease = _Lease(1, actor, endpoint)
    monkeypatch.setattr(invoker, "_worker", lambda: lease)

    def submit(*args, **kwargs):
        event.set()
        return actor.call

    monkeypatch.setattr(actor, "submit", submit)
    with pytest.raises(TaskCancelled) as failure:
        invoker(TaskDefinition(pid_task), context_for(event), time.monotonic(), 0)
    assert "cancellation channel close failed" in str(failure.value.__notes__)
    assert "secondary cleanup failure" in str(failure.value.__notes__)
    assert actor.terminations == 1
    assert invoker.attempts[0].cancellation_requested


def test_invalid_process_reply_is_driver_failure_and_not_retryable(monkeypatch):
    from graph_sail.process_execution import _Lease, _ProcessInvoker

    invoker = _ProcessInvoker(ProcessTaskConfig())
    lease = _Lease(1, FakeActor(FakeCall("not a worker reply")), FakeEndpoint())
    monkeypatch.setattr(invoker, "_worker", lambda: lease)
    outcome = invoker(TaskDefinition(pid_task, max_retries=2), context_for(), time.monotonic(), 0)
    assert outcome.attempt.error_type == "ActorDiedError"
    assert not outcome.retryable
    assert lease.retired and lease.actor.closes == 1


def test_retirement_aggregates_close_errors_and_does_not_claim_worker_joined():
    from graph_sail.process_execution import _Lease, _ProcessInvoker

    invoker = _ProcessInvoker(ProcessTaskConfig())
    first = _Lease(
        1,
        FakeActor(close_error=RuntimeError("join failed")),
        FakeEndpoint(close_error=OSError("channel failed")),
    )
    second = _Lease(2, FakeActor(close_error=RuntimeError("second join failed")), FakeEndpoint())
    invoker.leases = {1: first, 2: second}
    with pytest.raises(OSError, match="channel failed") as failure:
        invoker.close()
    assert first.actor.closes == second.actor.closes == 1
    assert not first.retired and not second.retired and invoker.workers == []
    assert len(failure.value.__notes__) == 2
    first.sender.close_error = first.actor.close_error = second.actor.close_error = None
    invoker.close()
    invoker.close()
    assert len(invoker.workers) == 2


@pytest.mark.parametrize("primary", [RuntimeError("startup"), KeyboardInterrupt(), SystemExit(9)])
def test_bootstrap_failure_closes_both_endpoints_without_masking_primary(primary, monkeypatch):
    from graph_sail import actors

    receiver = FakeEndpoint(close_error=OSError("receiver close"))
    sender = FakeEndpoint(close_error=OSError("sender close"))

    class Context:
        def Pipe(self, duplex):
            assert duplex is False
            return receiver, sender

    monkeypatch.setattr(actors.multiprocessing, "get_context", lambda _: Context())

    def failed(*args, **kwargs):
        raise primary

    monkeypatch.setattr(actors.ProcessActor, "_initialize", failed)
    with pytest.raises(type(primary)) as failure:
        actors.ProcessActor._for_task_worker(actors.ActorConfig())
    assert failure.value is primary
    assert receiver.closes == sender.closes == 1
    assert len(primary.__notes__) == 2


def test_bootstrap_receiver_close_failure_after_start_terminates_started_actor(monkeypatch):
    from graph_sail import actors

    receiver = FakeEndpoint(close_error=OSError("receiver close"))
    sender = FakeEndpoint()
    terminated = []

    class Context:
        def Pipe(self, duplex):
            return receiver, sender

    monkeypatch.setattr(actors.multiprocessing, "get_context", lambda _: Context())
    monkeypatch.setattr(actors.ProcessActor, "_initialize", lambda *args, **kwargs: None)
    monkeypatch.setattr(actors.ProcessActor, "terminate", lambda self: terminated.append(self))
    with pytest.raises(OSError, match="receiver close"):
        actors.ProcessActor._for_task_worker(actors.ActorConfig())
    assert receiver.closes == 2 and sender.closes == 1 and len(terminated) == 1


@pytest.mark.parametrize("control", [KeyboardInterrupt(), SystemExit(9)])
def test_control_exception_during_endpoint_cleanup_still_joins_all_owned_workers(control):
    from graph_sail.process_execution import _Lease, _ProcessInvoker

    invoker = _ProcessInvoker(ProcessTaskConfig())
    first = _Lease(
        1, FakeActor(close_error=RuntimeError("join failure")), FakeEndpoint(close_error=control)
    )
    second = _Lease(2, FakeActor(), FakeEndpoint())
    invoker.leases = {1: first, 2: second}
    with pytest.raises(type(control)) as failure:
        invoker.close()
    assert failure.value is control
    assert first.actor.closes == second.actor.closes == 1
    assert not first.retired and second.retired
    assert len(invoker.workers) == 1
    first.sender.close_error = first.actor.close_error = None
    invoker.close()
    assert len(invoker.workers) == 2


@pytest.mark.parametrize("control", [KeyboardInterrupt(), SystemExit(9)])
def test_control_exception_during_actor_cleanup_does_not_skip_other_workers(control):
    from graph_sail.process_execution import _Lease, _ProcessInvoker

    invoker = _ProcessInvoker(ProcessTaskConfig())
    first = _Lease(1, FakeActor(close_error=control), FakeEndpoint(close_error=OSError("channel")))
    second = _Lease(2, FakeActor(), FakeEndpoint())
    invoker.leases = {1: first, 2: second}
    with pytest.raises(type(control)) as failure:
        invoker.close()
    assert failure.value is control
    assert first.actor.closes == second.actor.closes == 1
    assert "earlier failure: OSError" in str(control.__notes__)
    first.actor.close_error = first.sender.close_error = None
    invoker.close()


def test_cancellation_during_startup_wait_is_observed_before_task_dispatch(monkeypatch):
    from graph_sail.actors import ProcessActor

    original_receive = ProcessActor._receive
    startup_received, release_startup, cancelled = Event(), Event(), Event()

    def delayed_startup(self, deadline):
        response = original_receive(self, deadline)
        if isinstance(response, tuple) and response[:2] == (0, "ready"):
            startup_received.set()
            assert release_startup.wait(10)
        return response

    monkeypatch.setattr(ProcessActor, "_receive", delayed_startup)
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(run, {"a": pid_task}, cancel_event=cancelled)
        try:
            assert startup_received.wait(15)
            cancelled.set()
            assert not pending.done()  # cancellation does not pretend startup was preempted
        finally:
            release_startup.set()
        result = pending.result(15)
    assert result.execution.status == "cancelled" and result.execution.outputs == {}
    assert result.attempts[0].round_trip_ms is None
    assert result.attempts[0].timing_source == "driver"
    assert len(result.workers) == 1 and result.workers[0].exitcode == 0


def test_startup_timeout_never_retries_or_leaves_spawned_child():
    result = run(
        {"a": TaskDefinition(pid_task, max_retries=2)},
        process_config=ProcessTaskConfig(startup_timeout_seconds=0.001),
    )
    assert result.execution.status == "failed"
    assert result.execution.tasks[0].attempts[0].error_type == "ActorTimeoutError"
    assert len(result.execution.tasks[0].attempts) == 1
    assert result.attempts[0].pid is None and result.workers == ()


@pytest.mark.parametrize(
    "primary, cleanup",
    [
        (KeyboardInterrupt(), OSError("cleanup")),
        (SystemExit(9), KeyboardInterrupt()),
        (RuntimeError("primary"), KeyboardInterrupt()),
    ],
)
def test_scheduler_failure_preserves_control_precedence_through_final_cleanup(
    primary, cleanup, monkeypatch
):
    from graph_sail import process_execution as module

    def broken_run(self):
        raise primary

    def broken_close(self):
        raise cleanup

    monkeypatch.setattr(module._Runner, "run", broken_run)
    monkeypatch.setattr(module._ProcessInvoker, "close", broken_close)
    expected = cleanup if isinstance(primary, Exception) else primary
    with pytest.raises(type(expected)) as failure:
        run({"a": pid_task})
    assert failure.value is expected


def test_worker_admission_bookkeeping_failure_closes_unadmitted_native_resources(monkeypatch):
    from graph_sail.actors import ProcessActor
    from graph_sail.process_execution import _ProcessInvoker

    actor, sender = FakeActor(), FakeEndpoint()
    monkeypatch.setattr(ProcessActor, "_for_task_worker", lambda _: (actor, sender))

    class NoAdmission(dict):
        def __setitem__(self, key, value):
            raise MemoryError("injected reservation failure")

    invoker = _ProcessInvoker(ProcessTaskConfig())
    invoker.leases = NoAdmission()
    with pytest.raises(MemoryError):
        invoker._worker()
    assert actor.terminations == sender.closes == 1
    assert not invoker.leases


def test_executable_offline_process_graph_has_independent_byte_oracle(tmp_path):
    example = Path(__file__).resolve().parents[1] / "examples" / "process_graph.py"
    completed = subprocess.run(
        [sys.executable, str(example)],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    decoder = json.JSONDecoder()
    output, consumed = decoder.raw_decode(completed.stdout)
    telemetry = json.loads(completed.stdout[consumed:])
    assert output["bytes"] == 2097152 and output["sum"] == 267386880
    assert output["sha256"] == "91d3beb88a9b2f778a6c44a1c53b63d3c79931845a9aef84b3fb414610bd1938"
    assert telemetry["execution"]["status"] == "succeeded"
    assert len(telemetry["workers"]) == 2
    assert all(worker["exitcode"] == 0 for worker in telemetry["workers"])


@pytest.mark.parametrize("control", [KeyboardInterrupt(), SystemExit(9)])
def test_invocation_cleanup_preserves_repeated_control_identity_without_self_cause(
    control, monkeypatch
):
    from graph_sail.process_execution import _Lease, _ProcessInvoker

    event = Event()
    invoker = _ProcessInvoker(ProcessTaskConfig())
    endpoint = FakeEndpoint(close_error=control)
    actor = FakeActor(FakeCall(done=False))
    lease = _Lease(1, actor, endpoint)
    monkeypatch.setattr(invoker, "_worker", lambda: lease)

    def submit(*args, **kwargs):
        event.set()
        return actor.call

    monkeypatch.setattr(actor, "submit", submit)
    with pytest.raises(type(control)) as failure:
        invoker(TaskDefinition(pid_task), context_for(event), time.monotonic(), 0)
    assert failure.value is control and control.__cause__ is not control
    assert actor.terminations == 1 and endpoint.closes == 2


def test_process_admission_revalidates_underlying_actor_startup_minimum(monkeypatch):
    from graph_sail.actors import ProcessActor

    options = ProcessTaskConfig()
    object.__setattr__(options, "startup_timeout_seconds", 1e-6)
    monkeypatch.setattr(ProcessActor, "_for_task_worker", lambda _: pytest.fail("spawned"))
    with pytest.raises(ValidationError, match="startup_timeout_seconds"):
        run({"a": pid_task}, process_config=options)


def test_cooperative_response_during_grace_retires_without_forced_termination(monkeypatch):
    from graph_sail.process_execution import _Lease, _ProcessInvoker

    class SettlesDuringGrace(FakeCall):
        def result(self, timeout=None):
            self.completed = True
            return "late success cannot override the already-observed cancellation"

    event = Event()
    invoker = _ProcessInvoker(ProcessTaskConfig(cancellation_grace_seconds=0.15))
    actor = FakeActor(SettlesDuringGrace(done=False))
    lease = _Lease(1, actor, FakeEndpoint())
    monkeypatch.setattr(invoker, "_worker", lambda: lease)

    def submit(*args, **kwargs):
        event.set()
        return actor.call

    monkeypatch.setattr(actor, "submit", submit)
    outcome = invoker(TaskDefinition(pid_task), context_for(event), time.monotonic(), 0)
    assert outcome.attempt.status == "cancelled" and outcome.value is None
    assert lease.retired and actor.closes == 1 and actor.terminations == 0
    assert not invoker.workers[0].termination_requested


def test_running_native_task_observes_eof_and_acknowledges_cancellation(tmp_path):
    from graph_sail.actors import ActorConfig, ProcessActor

    actor, sender = ProcessActor._for_task_worker(ActorConfig(max_pending=1))
    entered, observed = tmp_path / "entered", tmp_path / "observed"
    try:
        call = actor.submit(
            "invoke",
            args=(
                TaskDefinition(StopTask(str(entered), str(observed))),
                "a",
                "cpu",
                1,
                {},
                time.monotonic(),
                0.0,
            ),
        )
        wait_for(entered)
        sender.close()
        reply = call.result(15)
        assert reply.outcome.attempt.status == "cancelled"
        assert observed.read_text(encoding="ascii") == "stopped"
    finally:
        sender.close()
        actor.close()
