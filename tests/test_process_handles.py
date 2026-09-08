from __future__ import annotations

import multiprocessing
import os
from pathlib import Path
from threading import Thread

import pytest
from process_execution_tasks import StopTask, Value, pid_task, retry_value_error, wait_for
from test_process_execution import graph_for

from graph_sail import (
    ActorSerializationError,
    ExecutionConfig,
    ProcessTaskConfig,
    TaskDefinition,
    TaskNotSuccessful,
    TaskRegistry,
    WaitResult,
    start_process_graph,
)


@pytest.fixture(autouse=True)
def no_owned_children_left():
    before = {child.pid for child in multiprocessing.active_children()}
    yield
    assert {child.pid for child in multiprocessing.active_children()} == before


@pytest.mark.parametrize("cooperative", [True, False])
def test_real_partial_process_results_and_cancel_join_owned_workers(tmp_path, cooperative):
    entered, observed = tmp_path / "entered", tmp_path / "observed"
    graph = graph_for(("a", "b", "c"), (("a", "c"),))
    with start_process_graph(
        graph,
        TaskRegistry(
            {
                "a": Value(6),
                "b": StopTask(str(entered), str(observed), cooperative=cooperative),
                "c": pid_task,
            }
        ),
        dict.fromkeys(("a", "b", "c"), "cpu"),
        config=ExecutionConfig(max_workers=2, device_workers={"cpu": 2}),
        process_config=ProcessTaskConfig(cancellation_grace_seconds=0.25),
    ) as handle:
        wait_for(entered, 35)
        value = handle.node("c").result(35)
        assert value[0] != os.getpid()
        assert value[1:] == ("c", 1, {"a": 6})
        assert handle.wait(("c", "b", "a"), count=2) == WaitResult(("c", "a"), ("b",))
        assert not handle.done()
        with pytest.raises(TimeoutError):
            handle.result(0)
        assert handle.cancel()
        result = handle.result(35)
        assert result.execution.status == "cancelled"
        assert result.execution.outputs["c"] == value
        assert len(result.workers) == 2
        assert {worker.pid for worker in result.workers} == {
            value[0],
            int(entered.read_text(encoding="ascii")),
        }
        assert all(worker.exitcode is not None for worker in result.workers)
        assert any(worker.cancellation_requested for worker in result.workers)
        stopped_worker = next(
            worker
            for worker in result.workers
            if worker.pid == int(entered.read_text(encoding="ascii"))
        )
        if observed.exists():
            assert cooperative
        else:
            # A declared-cooperative child may not be scheduled within grace.
            # Missing observation requires actual forced retirement, not an
            # assumption about OS scheduling latency.
            assert stopped_worker.termination_requested
        if not cooperative:
            assert not observed.exists() and stopped_worker.termination_requested
        with pytest.raises(TaskNotSuccessful) as error:
            handle.node("b").result()
        assert error.value.execution.status == "cancelled"
    assert handle.closed


def test_process_retry_has_one_terminal_handle_and_shared_attempt_history():
    graph = graph_for(("a",), ())
    with start_process_graph(
        graph,
        TaskRegistry({"a": TaskDefinition(retry_value_error, max_retries=1)}),
        {"a": "cpu"},
        config=ExecutionConfig(max_workers=1),
    ) as handle:
        value = handle.node("a").result(35)
        assert value[0] == 42 and value[1] != os.getpid()
        assert [attempt.status for attempt in handle.node("a").execution().attempts] == [
            "failed",
            "succeeded",
        ]
        result = handle.result(35)
        assert result.execution.status == "succeeded"
        assert len(result.attempts) == 2 and len(result.workers) == 1
        assert result.workers[0].exitcode == 0


def test_unpicklable_registration_fails_before_controller_start(monkeypatch):
    def forbidden(_):
        pytest.fail("unpicklable registration started work")

    monkeypatch.setattr(Thread, "start", forbidden)
    with pytest.raises(ActorSerializationError):
        start_process_graph(
            graph_for(("a",), ()), TaskRegistry({"a": lambda _: Path.cwd()}), {"a": "cpu"}
        )
