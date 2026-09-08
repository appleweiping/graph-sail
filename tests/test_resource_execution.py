from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event, Lock
from threading import enumerate as threads

import pytest
from test_execution import graph_for

from graph_sail import (
    ExecutionConfig,
    TaskCancelled,
    TaskDefinition,
    TaskRegistry,
    ValidationError,
    execute_graph,
    execute_process_graph,
    start_graph,
)
from graph_sail.resources import LogicalResources, _ResourcePool


@pytest.fixture(autouse=True)
def no_owned_threads_left():
    before = {thread.ident for thread in threads() if thread.name.startswith("graph-sail")}
    yield
    assert {thread.ident for thread in threads() if thread.name.startswith("graph-sail")} == before


def options(requests, capacities=None, *, workers=3, **kwargs):
    return ExecutionConfig(
        max_workers=workers,
        device_workers={"cpu": workers},
        resources=LogicalResources({"CPU": 1} if capacities is None else capacities, requests),
        **kwargs,
    )


def run(tasks, config, edges=()):
    return execute_graph(
        graph_for(tuple(tasks), edges),
        TaskRegistry(tasks),
        dict.fromkeys(tasks, "cpu"),
        config=config,
    )


def test_fractional_requests_really_overlap_and_account_for_each_terminal_attempt():
    barrier = Barrier(2, timeout=10)

    def task(context):
        barrier.wait()
        return context.node_id

    result = run({"a": task, "b": task}, options({"a": {"CPU": 0.5}, "b": {"CPU": 0.5}}))
    assert result.status == "succeeded"
    assert result.outputs == {"a": "a", "b": "b"}
    assert result.resource_usage.peak_units == {"CPU": 10_000}
    assert result.resource_usage.reservations == result.resource_usage.releases == 2
    assert result.to_dict()["resource_usage"] == result.resource_usage.to_dict()


def test_infeasible_ready_head_does_not_hide_a_feasible_tail():
    running, tail_started, release = Event(), Event(), Event()

    def first(_):
        running.set()
        assert tail_started.wait(10)
        assert release.wait(10)
        return "first"

    def large(_):
        assert release.is_set()
        return "large"

    def small(_):
        assert running.wait(10)
        tail_started.set()
        return "small"

    tasks = {"a": first, "b": large, "c": small}
    handle = start_graph(
        graph_for(tuple(tasks), ()),
        TaskRegistry(tasks),
        dict.fromkeys(tasks, "cpu"),
        config=options({"a": {"CPU": 0.5}, "b": {"CPU": 1}, "c": {"CPU": 0.5}}),
    )
    try:
        assert handle.node("c").result(10) == "small"
        assert not handle.node("b").done()
        release.set()
        assert handle.result(10).status == "succeeded"
    finally:
        release.set()
        handle.close(10)


def test_retry_releases_resources_during_backoff():
    middle = Event()

    def retry(context):
        if context.attempt == 1:
            raise ValueError("retry")
        assert middle.is_set()
        return 7

    result = run(
        {
            "a": TaskDefinition(retry, max_retries=1, retry_delay_seconds=0.05),
            "b": lambda _: middle.set(),
        },
        options({"a": {"CPU": 1}, "b": {"CPU": 1}}),
    )
    assert result.status == "succeeded"
    assert result.resource_usage.reservations == result.resource_usage.releases == 3
    assert result.resource_usage.peak_units == {"CPU": 10_000}


def test_no_policy_keeps_previous_result_wire_exactly():
    result = run({"a": lambda _: 7}, ExecutionConfig())
    assert result.resource_usage is None
    assert set(result.to_dict()) == {
        "kind",
        "schema_version",
        "graph_name",
        "status",
        "elapsed_ms",
        "tasks",
        "output_nodes",
        "cancellation_reason",
        "peak_in_flight_by_device",
        "reserved_memory_mb",
    }


def test_unknown_node_is_rejected_before_any_controller_or_task():
    import graph_sail.handles as handles

    def forbidden(*args, **kwargs):
        pytest.fail("controller must not start before complete admission")

    from unittest.mock import patch

    with (
        patch.object(handles, "Thread", forbidden),
        pytest.raises(ValidationError, match="unknown graph node"),
    ):
        tasks = {"a": lambda _: 7}
        start_graph(
            graph_for(tuple(tasks), ()),
            TaskRegistry(tasks),
            {"a": "cpu"},
            config=options({"missing": {"CPU": 1}}),
        )


@pytest.mark.parametrize("bad", [{}, True, 0, "CPU", object()])
def test_invalid_resource_policy_is_rejected(bad):
    with pytest.raises(ValidationError, match="LogicalResources"):
        ExecutionConfig(resources=bad)


def test_config_snapshots_input_and_zero_requests_still_count_attempts():
    capacities, demands = {"CPU": 1}, {"a": {"CPU": 0}}
    policy = LogicalResources(capacities, demands)
    config = ExecutionConfig(resources=policy)
    capacities["CPU"] = 10
    demands["a"]["CPU"] = 10
    assert config.resources is not policy
    assert config.resources.digest == policy.digest
    result = run({"a": lambda _: 1, "b": lambda _: 2}, config)
    assert result.resource_usage.peak_units == {"CPU": 0}
    assert result.resource_usage.reservations == result.resource_usage.releases == 2


def test_empty_policy_and_positional_config_compatibility():
    old = ExecutionConfig(2, {"cpu": 2}, False, None)
    assert old.resources is None
    result = run({"a": lambda _: 1}, options({}, {}))
    assert result.resource_usage.capacity_units == {}
    assert result.resource_usage.reservations == 1


def test_resources_are_global_across_distinct_placement_devices():
    order, active = [], 0
    lock = Lock()

    def task(context):
        nonlocal active
        with lock:
            active += 1
            assert active == 1
            order.append(context.node_id)
            active -= 1
        return context.device

    graph = graph_for(("a", "b"), (), devices=("left", "right"))
    result = execute_graph(
        graph,
        TaskRegistry({"a": task, "b": task}),
        {"a": "left", "b": "right"},
        config=ExecutionConfig(
            resources=LogicalResources(
                {"license": 1},
                {
                    "a": {"license": 1},
                    "b": {"license": 1},
                },
            )
        ),
    )
    assert order == ["a", "b"]
    assert result.outputs == {"a": "left", "b": "right"}
    assert result.resource_usage.peak_units == {"license": 10_000}


def test_device_slots_still_apply_when_logical_resources_fit():
    entered, release, second = Event(), Event(), Event()

    def first(_):
        entered.set()
        assert release.wait(10)

    config = ExecutionConfig(
        max_workers=2, device_workers={"cpu": 1}, resources=LogicalResources({})
    )
    tasks = {"a": first, "b": lambda _: second.set()}
    handle = start_graph(
        graph_for(tuple(tasks), ()), TaskRegistry(tasks), dict.fromkeys(tasks, "cpu"), config=config
    )
    try:
        assert entered.wait(10)
        assert not second.wait(0.02)
        release.set()
        assert handle.result(10).status == "succeeded"
        assert second.is_set()
    finally:
        release.set()
        handle.close(10)


@pytest.mark.parametrize("kind", ["failed", "cancelled", "retry_exhausted"])
def test_failed_and_cancelled_descendants_never_reserve(kind):
    def fail(_):
        if kind == "cancelled":
            raise TaskCancelled("stop")
        raise ValueError("application failure")

    result = run(
        {
            "a": TaskDefinition(fail, max_retries=2 if kind == "retry_exhausted" else 0),
            "b": lambda _: 7,
        },
        options({"a": {"CPU": 1}, "b": {"CPU": 1}}),
        (("a", "b"),),
    )
    assert result.tasks[1].status == "skipped"
    assert (
        result.resource_usage.reservations
        == result.resource_usage.releases
        == (3 if kind == "retry_exhausted" else 1)
    )


def test_pre_cancelled_graph_never_acquires_even_zero_demand():
    stop = Event()
    stop.set()
    result = execute_graph(
        graph_for(("a",), ()),
        TaskRegistry({"a": lambda _: pytest.fail("started")}),
        {"a": "cpu"},
        config=options({}),
        cancel_event=stop,
    )
    assert result.status == "cancelled"
    assert result.resource_usage.reservations == result.resource_usage.releases == 0


def recording_pool(monkeypatch):
    import graph_sail.execution as execution

    captured = []

    class ObservedPool(_ResourcePool):
        def __init__(self, policy):
            super().__init__(policy)
            captured.append(self)

    monkeypatch.setattr(execution, "_ResourcePool", ObservedPool)
    return captured


def test_cancel_does_not_release_a_still_running_callable(monkeypatch):
    captured = recording_pool(monkeypatch)
    entered, release = Event(), Event()

    def task(_):
        entered.set()
        assert release.wait(10)
        return "late success"

    handle = start_graph(
        graph_for(("a", "b"), ()),
        TaskRegistry({"a": task, "b": lambda _: 9}),
        {"a": "cpu", "b": "cpu"},
        config=options({"a": {"CPU": 1}, "b": {"CPU": 1}}),
    )
    try:
        assert entered.wait(10)
        handle.cancel()
        with pytest.raises(TimeoutError):
            handle.close(0.02)
        assert not captured[0].fits("b")
        with pytest.raises(ValidationError, match="leases remain"):
            captured[0].snapshot()
        release.set()
        result = handle.result(10)
        assert result.status == "cancelled"
        assert result.outputs == {"a": "late success"}
        assert result.resource_usage.reservations == result.resource_usage.releases == 1
    finally:
        release.set()
        handle.close(10)


def test_submit_can_raise_after_enqueuing_and_must_not_release_early(monkeypatch):
    import graph_sail.execution as execution

    captured = recording_pool(monkeypatch)
    entered, release, submit_failed = Event(), Event(), Event()

    class UnacknowledgedExecutor(ThreadPoolExecutor):
        def submit(self, fn, /, *args, **kwargs):
            super().submit(fn, *args, **kwargs)
            assert entered.wait(10)
            submit_failed.set()
            raise RuntimeError("submission acknowledgment lost")

    monkeypatch.setattr(execution, "ThreadPoolExecutor", UnacknowledgedExecutor)

    def task(_):
        entered.set()
        assert release.wait(10)

    handle = start_graph(
        graph_for(("a",), ()),
        TaskRegistry({"a": task}),
        {"a": "cpu"},
        config=options({"a": {"CPU": 1}}),
    )
    try:
        assert submit_failed.wait(10)
        with pytest.raises(TimeoutError):
            handle.result(0.02)
        with pytest.raises(ValidationError, match="leases remain"):
            captured[0].snapshot()
        release.set()
        with pytest.raises(RuntimeError, match="acknowledgment lost"):
            handle.result(10)
        assert captured[0].snapshot().releases == 1
    finally:
        release.set()
        handle.close(10)


@pytest.mark.parametrize(
    "primary,cleanup,expected",
    [
        (KeyboardInterrupt, RuntimeError, KeyboardInterrupt),
        (RuntimeError, KeyboardInterrupt, KeyboardInterrupt),
        (RuntimeError, ValueError, RuntimeError),
    ],
)
def test_shutdown_failure_preserves_control_priority_and_unsettled_accounting(
    monkeypatch, primary, cleanup, expected
):
    import graph_sail.execution as execution

    captured = recording_pool(monkeypatch)
    first, second = primary("primary"), cleanup("cleanup")

    class BrokenExecutor(ThreadPoolExecutor):
        def submit(self, *args, **kwargs):
            raise first

        def shutdown(self, *args, **kwargs):
            super().shutdown(*args, **kwargs)
            raise second

    monkeypatch.setattr(execution, "ThreadPoolExecutor", BrokenExecutor)
    with pytest.raises(expected) as raised:
        run({"a": lambda _: 1}, options({"a": {"CPU": 1}}))
    assert raised.value is (first if expected is primary else second)
    # shutdown failed to acknowledge completion: do not publish success even
    # though this particular test executor happened not to start any work.
    with pytest.raises(ValidationError, match="leases remain"):
        captured[0].snapshot()


def test_control_failure_joins_other_running_work_before_final_resource_release(monkeypatch):
    captured = recording_pool(monkeypatch)
    entered, release, failed = Event(), Event(), Event()

    def failing(_):
        assert entered.wait(10)
        failed.set()
        raise KeyboardInterrupt("application control")

    def blocked(_):
        entered.set()
        assert release.wait(10)

    tasks = {"a": failing, "b": blocked}
    handle = start_graph(
        graph_for(tuple(tasks), ()),
        TaskRegistry(tasks),
        dict.fromkeys(tasks, "cpu"),
        config=options({"a": {"CPU": 0.5}, "b": {"CPU": 0.5}}),
    )
    try:
        assert failed.wait(10)
        with pytest.raises(TimeoutError):
            handle.result(0.02)
        with pytest.raises(ValidationError, match="leases remain"):
            captured[0].snapshot()
        release.set()
        with pytest.raises(KeyboardInterrupt, match="application control"):
            handle.result(10)
        assert captured[0].snapshot().reservations == captured[0].snapshot().releases == 2
    finally:
        release.set()
        handle.close(10)


def test_real_spawn_workers_share_fractional_resource_admission(tmp_path):
    import os

    from process_execution_tasks import Rendezvous

    task = Rendezvous(str(tmp_path))
    result = execute_process_graph(
        graph_for(("a", "b"), ()),
        TaskRegistry({"a": task, "b": task}),
        {"a": "cpu", "b": "cpu"},
        config=options({"a": {"CPU": 0.5}, "b": {"CPU": 0.5}}, workers=2),
    )
    assert result.execution.status == "succeeded"
    assert result.execution.outputs == {"a": 6, "b": 7}
    assert len(result.workers) == 2
    assert all(worker.pid != os.getpid() and worker.exitcode == 0 for worker in result.workers)
    assert (
        result.execution.resource_usage.reservations
        == result.execution.resource_usage.releases
        == 2
    )
    assert result.execution.resource_usage.peak_units == {"CPU": 10_000}


@pytest.mark.parametrize("separate_devices", [False, True])
def test_blocked_multi_resource_request_cannot_hoard_a_second_resource(separate_devices):
    entered, small_done, release = Event(), Event(), Event()

    def first(_):
        entered.set()
        assert small_done.wait(10)
        assert release.wait(10)

    def small(_):
        assert entered.wait(10)
        small_done.set()
        return "license remained available"

    tasks = {"a": first, "b": lambda _: "both", "c": small}
    devices = ("left", "right") if separate_devices else ("cpu",)
    assigned = {"a": devices[0], "b": devices[-1], "c": devices[-1]}
    config = ExecutionConfig(
        max_workers=3,
        device_workers=dict.fromkeys(devices, 3),
        resources=LogicalResources(
            {"CPU": 1, "license": 1},
            {
                "a": {"CPU": 1},
                "b": {"CPU": 1, "license": 1},
                "c": {"license": 1},
            },
        ),
    )
    handle = start_graph(
        graph_for(tuple(tasks), (), devices=devices), TaskRegistry(tasks), assigned, config=config
    )
    try:
        assert handle.node("c").result(10) == "license remained available"
        assert not handle.node("b").done()
        release.set()
        result = handle.result(10)
        assert result.status == "succeeded"
        assert result.resource_usage.peak_units == {"CPU": 10_000, "license": 10_000}
    finally:
        release.set()
        handle.close(10)


def test_successful_work_does_not_hide_shutdown_failure(monkeypatch):
    import graph_sail.execution as execution

    class FailedShutdown(ThreadPoolExecutor):
        def shutdown(self, *args, **kwargs):
            super().shutdown(*args, **kwargs)
            raise RuntimeError("shutdown did not acknowledge")

    monkeypatch.setattr(execution, "ThreadPoolExecutor", FailedShutdown)
    with pytest.raises(RuntimeError, match="shutdown did not acknowledge"):
        run({"a": lambda _: 7}, options({"a": {"CPU": 1}}))


@pytest.mark.parametrize("fail_fast", [False, True])
def test_failure_resource_release_preserves_fail_fast_semantics(fail_fast):
    def failing(_):
        raise RuntimeError("failure")

    result = run(
        {"a": failing, "b": lambda _: 9},
        options({"a": {"CPU": 1}, "b": {"CPU": 1}}, fail_fast=fail_fast),
    )
    assert result.status == "failed"
    assert result.tasks[1].status == ("cancelled" if fail_fast else "succeeded")
    assert result.resource_usage.reservations == (1 if fail_fast else 2)


def test_timeout_keeps_live_allocation_until_callable_observes_stop(monkeypatch):
    captured = recording_pool(monkeypatch)
    observed, release = Event(), Event()

    def task(context):
        while not context.cancellation.cancelled:
            release.wait(0.001)
        observed.set()
        assert release.wait(10)
        context.cancellation.raise_if_cancelled()

    handle = start_graph(
        graph_for(("a",), ()),
        TaskRegistry({"a": task}),
        {"a": "cpu"},
        config=options({"a": {"CPU": 1}}, timeout_seconds=0.05),
    )
    try:
        assert observed.wait(10)
        with pytest.raises(ValidationError, match="leases remain"):
            captured[0].snapshot()
        release.set()
        result = handle.result(10)
        assert result.cancellation_reason == "timeout"
        assert result.tasks[0].status == "cancelled"
        assert result.resource_usage.reservations == result.resource_usage.releases == 1
    finally:
        release.set()
        handle.close(10)


def test_offline_resource_example_executes_real_graph(capsys):
    import json
    import runpy
    from pathlib import Path

    runpy.run_path(
        str(Path(__file__).parents[1] / "examples" / "logical_resources.py"), run_name="__main__"
    )
    result = json.loads(capsys.readouterr().out)
    assert result["result"] == 42
    assert result["resources"]["reservations"] == result["resources"]["releases"] == 3
