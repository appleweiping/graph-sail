"""A bounded one-node process-DAG cancellation contract."""

import multiprocessing
import os
import time
from pathlib import Path

import pytest
from process_execution_tasks import (
    GateFailure,
    GateValue,
    MarkerValue,
    RetryMarker,
    StopTask,
    Value,
    pid_task,
    wait_for,
)
from test_process_execution import graph_for

import graph_sail.process_execution as process_execution
from graph_sail import (
    ExecutionConfig,
    LogicalResources,
    ProcessTaskConfig,
    TaskDefinition,
    TaskNotSuccessful,
    TaskRegistry,
    ValidationError,
    start_graph,
    start_process_graph,
)


@pytest.fixture(autouse=True)
def no_owned_children_left():
    before = {child.pid for child in multiprocessing.active_children()}
    yield
    assert {child.pid for child in multiprocessing.active_children()} == before


def _read_child_pid(path: Path) -> int:
    deadline = time.monotonic() + 35
    while time.monotonic() < deadline:
        if path.exists():
            value = path.read_text(encoding="ascii")
            if value.isdecimal():
                return int(value)
        time.sleep(0.005)
    raise AssertionError(f"child did not finish writing its PID to {path}")


def test_process_handle_exposes_selective_node_cancellation() -> None:
    graph = graph_for(("a",), ())
    with start_process_graph(graph, TaskRegistry({"a": pid_task}), {"a": "cpu"}) as handle:
        assert callable(handle.cancel_node)


def test_pending_node_never_enters_child_and_independent_branch_completes(tmp_path) -> None:
    entered = tmp_path / "a-entered"
    release = tmp_path / "a-release"
    b_marker = tmp_path / "b-entered"
    c_marker = tmp_path / "c-entered"
    graph = graph_for(("a", "b", "c", "d"), (("a", "d"), ("b", "c")))
    registry = TaskRegistry(
        {
            "a": GateValue(str(entered), str(release), 6),
            "b": MarkerValue(str(b_marker), 7),
            "c": MarkerValue(str(c_marker), 42),
            "d": pid_task,
        }
    )
    with start_process_graph(
        graph,
        registry,
        dict.fromkeys(("a", "b", "c", "d"), "cpu"),
        config=ExecutionConfig(
            max_workers=1,
            resources=LogicalResources(
                capacities={"CPU": 1},
                requests={node: {"CPU": 1} for node in ("a", "b", "c", "d")},
            ),
        ),
    ) as handle:
        try:
            wait_for(entered, 35)
            assert handle.cancel_node("b")
            assert not handle.cancel_node("b")
            with pytest.raises(TaskNotSuccessful) as cancelled:
                handle.node("b").result(10)
            assert cancelled.value.execution.status == "cancelled"
            assert cancelled.value.execution.reason == "target_cancellation"
            assert cancelled.value.execution.attempts == ()
        finally:
            release.write_text("go", encoding="ascii")
        result = handle.result(35)
        assert result.execution.status == "cancelled"
        assert result.execution.cancellation_reason is None
        assert result.execution.outputs["a"] == 6
        child_pid, node, attempt, dependencies = result.execution.outputs["d"]
        assert child_pid != os.getpid()
        assert (node, attempt, dependencies) == ("d", 1, {"a": 6})
        assert not b_marker.exists() and not c_marker.exists()
        assert next(task for task in result.execution.tasks if task.node_id == "c").reason == (
            "dependency_cancelled:b"
        )
        assert not any(item.node_id in {"b", "c"} for item in result.attempts)
        assert result.execution.resource_usage is not None
        assert result.execution.resource_usage.reservations == 2
        assert result.execution.resource_usage.releases == 2
        assert all(worker.exitcode is not None for worker in result.workers)


@pytest.mark.parametrize("cooperative", [True, False])
def test_running_node_only_retires_its_worker_and_sibling_survives(tmp_path, cooperative) -> None:
    entered, observed = tmp_path / "b-entered", tmp_path / "b-observed"
    c_marker = tmp_path / "c-entered"
    graph = graph_for(("a", "b", "c", "d"), (("a", "d"), ("b", "c")))
    registry = TaskRegistry(
        {
            "a": Value(6),
            "b": StopTask(str(entered), str(observed), cooperative=cooperative),
            "c": MarkerValue(str(c_marker), 42),
            "d": pid_task,
        }
    )
    with start_process_graph(
        graph,
        registry,
        dict.fromkeys(("a", "b", "c", "d"), "cpu"),
        config=ExecutionConfig(max_workers=2, device_workers={"cpu": 2}),
        process_config=ProcessTaskConfig(cancellation_grace_seconds=0.25),
    ) as handle:
        wait_for(entered, 35)
        stopped_pid = _read_child_pid(entered)
        assert handle.cancel_node("b")
        value = handle.node("d").result(35)
        assert value[0] not in (os.getpid(), stopped_pid)
        assert value[1:] == ("d", 1, {"a": 6})
        result = handle.result(35)
        assert result.execution.status == "cancelled"
        assert not c_marker.exists()
        stopped = next(worker for worker in result.workers if worker.pid == stopped_pid)
        assert stopped.cancellation_requested
        if not cooperative:
            assert stopped.termination_requested and not observed.exists()
        assert all(worker.exitcode is not None for worker in result.workers)
        assert all(
            not worker.cancellation_requested for worker in result.workers if worker is not stopped
        )
        assert any(item.node_id == "b" and item.cancellation_requested for item in result.attempts)


def test_cancelled_worker_pipe_does_not_leak_into_later_task(tmp_path) -> None:
    entered = tmp_path / "b-entered"
    graph = graph_for(("b", "c"), ())
    with start_process_graph(
        graph,
        TaskRegistry({"b": StopTask(str(entered), str(tmp_path / "observed")), "c": pid_task}),
        {"b": "cpu", "c": "cpu"},
        config=ExecutionConfig(max_workers=1),
    ) as handle:
        wait_for(entered, 35)
        stopped_pid = _read_child_pid(entered)
        assert handle.cancel_node("b")
        successor = handle.node("c").result(35)
        assert successor[0] not in (os.getpid(), stopped_pid)
        result = handle.result(35)
        assert len(result.workers) == 2
        assert {worker.pid for worker in result.workers} == {stopped_pid, successor[0]}
        assert sum(worker.cancellation_requested for worker in result.workers) == 1


def test_cancel_during_retry_delay_preserves_first_attempt_without_replay(tmp_path) -> None:
    marker = tmp_path / "attempt"
    graph = graph_for(("a",), ())
    with start_process_graph(
        graph,
        TaskRegistry(
            {
                "a": TaskDefinition(
                    RetryMarker(str(marker)),
                    max_retries=1,
                    retry_on=(ValueError,),
                    retry_delay_seconds=5,
                )
            }
        ),
        {"a": "cpu"},
        config=ExecutionConfig(max_workers=1),
    ) as handle:
        wait_for(marker, 35)
        deadline = time.monotonic() + 15
        while not handle._process_runner.attempts["a"] and time.monotonic() < deadline:
            time.sleep(0.005)
        assert len(handle._process_runner.attempts["a"]) == 1
        assert handle.cancel_node("a")
        result = handle.result(35)
        task = result.execution.tasks[0]
        assert task.status == "cancelled" and task.reason == "target_cancellation"
        assert len(task.attempts) == 1 and task.attempts[0].error_type == "ValueError"
        assert marker.read_text(encoding="ascii") == "1"
        assert len(result.attempts) == 1


def test_cancelled_parent_skips_a_multi_parent_join(tmp_path) -> None:
    entered, release = tmp_path / "a-entered", tmp_path / "a-release"
    b_marker, join_marker = tmp_path / "b-entered", tmp_path / "join-entered"
    graph = graph_for(("a", "b", "join"), (("a", "join"), ("b", "join")))
    with start_process_graph(
        graph,
        TaskRegistry(
            {
                "a": GateValue(str(entered), str(release), 6),
                "b": MarkerValue(str(b_marker), 7),
                "join": MarkerValue(str(join_marker), 42),
            }
        ),
        {"a": "cpu", "b": "cpu", "join": "cpu"},
        config=ExecutionConfig(max_workers=1),
    ) as handle:
        try:
            wait_for(entered, 35)
            assert handle.cancel_node("b")
        finally:
            release.write_text("go", encoding="ascii")
        result = handle.result(35)
        assert result.execution.outputs == {"a": 6}
        assert not b_marker.exists() and not join_marker.exists()
        join = next(task for task in result.execution.tasks if task.node_id == "join")
        assert join.status == "skipped" and join.reason == "dependency_cancelled:b"


def test_application_failure_racing_with_target_cancel_never_retries(tmp_path) -> None:
    entered, release = tmp_path / "entered", tmp_path / "release"
    graph = graph_for(("a",), ())
    with start_process_graph(
        graph,
        TaskRegistry(
            {
                "a": TaskDefinition(
                    GateFailure(str(entered), str(release)),
                    max_retries=1,
                    retry_on=(ValueError,),
                )
            }
        ),
        {"a": "cpu"},
    ) as handle:
        try:
            wait_for(entered, 35)
            assert handle.cancel_node("a")
        finally:
            release.write_text("go", encoding="ascii")
        result = handle.result(35)
        task = result.execution.tasks[0]
        assert task.status in {"cancelled", "failed"}
        assert len(task.attempts) == len(result.attempts) == 1
        assert entered.read_text(encoding="ascii") == "1"


def test_target_cancel_retains_child_owner_after_injected_retirement_failure(
    tmp_path, monkeypatch
) -> None:
    entered = tmp_path / "entered"
    graph = graph_for(("a",), ())
    original = process_execution._ProcessInvoker._retire
    failures = 0

    def fail_twice(self, lease, *, force):
        nonlocal failures
        if lease.cancelled and failures < 2:
            failures += 1
            raise RuntimeError("injected selective retirement failure")
        return original(self, lease, force=force)

    monkeypatch.setattr(process_execution._ProcessInvoker, "_retire", fail_twice)
    handle = start_process_graph(
        graph,
        TaskRegistry({"a": StopTask(str(entered), str(tmp_path / "observed"))}),
        {"a": "cpu"},
        process_config=ProcessTaskConfig(cancellation_grace_seconds=0.05),
    )
    try:
        wait_for(entered, 35)
        assert handle.cancel_node("a")
        with pytest.raises(BaseException) as raised:
            handle.result(35)
        cleanup = getattr(raised.value, "process_graph_cleanup", None)
        assert cleanup is not None and not cleanup.closed
        assert failures == 2 and not handle.closed
    finally:
        monkeypatch.setattr(process_execution._ProcessInvoker, "_retire", original)
        handle.close(35)
    assert handle.closed


def test_request_validation_and_thread_handle_remain_distinct() -> None:
    graph = graph_for(("a",), ())
    with start_graph(graph, TaskRegistry({"a": Value(1)}), {"a": "cpu"}) as thread_handle:
        assert not hasattr(thread_handle, "cancel_node")
    with start_process_graph(graph, TaskRegistry({"a": Value(1)}), {"a": "cpu"}) as handle:
        with pytest.raises(ValidationError):
            handle.cancel_node("missing")
        with pytest.raises(ValidationError):
            handle.cancel_node(7)

        class Spoof:
            @property
            def __class__(self) -> type[str]:
                return str

        spoof = Spoof()
        assert isinstance(spoof, str)
        with pytest.raises(ValidationError):
            handle.cancel_node(spoof)
        assert handle.node("a").result(35) == 1
        assert not handle.cancel_node("a")


def test_str_subclass_hooks_cannot_reenter_node_cancel_lock(tmp_path) -> None:
    entered, release = tmp_path / "entered", tmp_path / "release"
    graph = graph_for(("a", "b"), ())
    with start_process_graph(
        graph,
        TaskRegistry(
            {
                "a": GateValue(str(entered), str(release), 6),
                "b": MarkerValue(str(tmp_path / "b"), 7),
            }
        ),
        {"a": "cpu", "b": "cpu"},
        config=ExecutionConfig(max_workers=1),
    ) as handle:

        class ReentrantNode(str):
            hash_calls = 0
            equality_calls = 0

            def __str__(self) -> str:
                raise AssertionError("subclass string conversion must not run")

            def __hash__(self) -> int:
                self.hash_calls += 1
                lock = handle._process_runner._selective_lock
                if not lock.acquire(blocking=False):
                    raise AssertionError("hash hook would reenter the cancellation lock")
                lock.release()
                return str.__hash__(self)

            def __eq__(self, other: object) -> bool:
                self.equality_calls += 1
                return str.__eq__(self, other)

        hostile = ReentrantNode("b")
        try:
            wait_for(entered, 35)
            assert handle.cancel_node(hostile)
            assert hostile.hash_calls == hostile.equality_calls == 0
        finally:
            release.write_text("go", encoding="ascii")
        result = handle.result(35)
        b = next(task for task in result.execution.tasks if task.node_id == "b")
        assert b.status == "cancelled" and b.reason == "target_cancellation"
        assert result.execution.outputs["a"] == 6


def test_global_cancel_takes_priority_over_later_node_request(tmp_path) -> None:
    entered, release = tmp_path / "entered", tmp_path / "release"
    graph = graph_for(("a",), ())
    with start_process_graph(
        graph, TaskRegistry({"a": GateValue(str(entered), str(release), 6)}), {"a": "cpu"}
    ) as handle:
        try:
            wait_for(entered, 35)
            assert handle.cancel()
            assert not handle.cancel_node("a")
        finally:
            release.write_text("go", encoding="ascii")
        result = handle.result(35)
        assert result.execution.cancellation_reason == "external_cancellation"


def test_fail_fast_escalates_a_targeted_pending_cancel(tmp_path) -> None:
    entered, release = tmp_path / "entered", tmp_path / "release"
    b_marker = tmp_path / "b-entered"
    graph = graph_for(("a", "b", "d"), (("a", "d"),))
    with start_process_graph(
        graph,
        TaskRegistry(
            {
                "a": GateValue(str(entered), str(release), 6),
                "b": MarkerValue(str(b_marker), 7),
                "d": pid_task,
            }
        ),
        {"a": "cpu", "b": "cpu", "d": "cpu"},
        config=ExecutionConfig(max_workers=1, fail_fast=True),
    ) as handle:
        try:
            wait_for(entered, 35)
            assert handle.cancel_node("b")
        finally:
            release.write_text("go", encoding="ascii")
        result = handle.result(35)
        assert result.execution.cancellation_reason == "fail_fast"
        assert not b_marker.exists() and "d" not in result.execution.outputs
