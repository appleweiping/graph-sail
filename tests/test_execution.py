from __future__ import annotations

import json
import runpy
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Event

import pytest

from graph_sail import (
    ExecutionConfig,
    GreedyPlanner,
    TaskCancelled,
    TaskContext,
    TaskDefinition,
    TaskRegistry,
    ValidationError,
    execute_graph,
    graph_from_dict,
)


def graph_for(
    nodes=("a", "b", "join"),
    edges=(("a", "join"), ("b", "join")),
    *,
    devices=("cpu",),
    memory=10,
    node_memory=1,
):
    return graph_from_dict(
        {
            "name": "execution-oracle",
            "devices": [{"name": device, "memory_mb": memory} for device in devices],
            "nodes": [
                {
                    "id": node,
                    "kind": "test",
                    "memory_mb": node_memory,
                    "latency_ms": dict.fromkeys(devices, 999_999),
                }
                for node in nodes
            ],
            "edges": [{"source": left, "target": right} for left, right in edges],
        }
    )


def test_real_parallel_callables_feed_dependency_results_and_measured_telemetry():
    graph = graph_for()
    barrier = Barrier(2, timeout=5)
    observed = []

    def source(context: TaskContext):
        assert context.dependencies == {}
        barrier.wait()  # would fail in a sequential or simulation-only implementation
        return 6 if context.node_id == "a" else 7

    def join(context: TaskContext):
        observed.append(tuple(context.dependencies))
        with pytest.raises(TypeError):
            context.dependencies["a"] = 0
        return context.dependencies["a"] * context.dependencies["b"]

    registry = TaskRegistry({"a": source, "b": source, "join": join})
    result = execute_graph(
        graph,
        registry,
        GreedyPlanner().plan(graph).placements,
        config=ExecutionConfig(max_workers=2, device_workers={"cpu": 2}),
    )
    assert result.status == "succeeded"
    assert result.outputs == {"a": 6, "b": 7, "join": 42}
    assert observed == [("a", "b")]
    assert result.peak_in_flight_by_device == {"cpu": 2}
    assert result.reserved_memory_mb == {"cpu": 3}
    tasks = {task.node_id: task for task in result.tasks}
    parent_finish = max(tasks[node].attempts[0].finished_ms for node in ("a", "b"))
    assert tasks["join"].attempts[0].started_ms >= parent_finish
    for task in result.tasks:
        attempt = task.attempts[0]
        assert (
            0
            <= attempt.submitted_ms
            <= attempt.started_ms
            <= attempt.finished_ms
            <= result.elapsed_ms
        )
    telemetry = result.to_dict()
    assert telemetry["kind"] == "graph-sail-local-execution"
    assert telemetry["output_nodes"] == ["a", "b", "join"]
    assert "outputs" not in telemetry
    json.dumps(telemetry, allow_nan=False)


def test_distinct_logical_devices_can_run_at_once_with_default_single_slot():
    graph = graph_for(nodes=("a", "b"), edges=(), devices=("left", "right"))
    barrier = Barrier(2, timeout=5)

    def task(context):
        barrier.wait()
        return context.device

    result = execute_graph(graph, TaskRegistry({"a": task, "b": task}), {"a": "left", "b": "right"})
    assert result.outputs == {"a": "left", "b": "right"}
    assert result.peak_in_flight_by_device == {"left": 1, "right": 1}


@pytest.mark.parametrize(
    "config", [ExecutionConfig(), ExecutionConfig(max_workers=1, device_workers={"cpu": 2})]
)
def test_global_and_device_worker_limits_prevent_concurrent_entry(config):
    graph = graph_for(nodes=("a", "b"), edges=())
    started, release, second_started = Event(), Event(), Event()

    def first(_):
        started.set()
        assert release.wait(5)
        return 1

    def second(_):
        second_started.set()
        return 2

    with ThreadPoolExecutor(max_workers=1) as harness:
        future = harness.submit(
            execute_graph,
            graph,
            TaskRegistry({"a": first, "b": second}),
            {"a": "cpu", "b": "cpu"},
            config=config,
        )
        try:
            assert started.wait(5)
            assert not second_started.wait(0.03)
        finally:
            release.set()
        result = future.result(5)
    assert result.outputs == {"a": 1, "b": 2}
    assert result.peak_in_flight_by_device == {"cpu": 1}


def test_failure_skips_only_descendants_and_preserves_independent_branch():
    graph = graph_for(
        nodes=("bad", "child", "grandchild", "good", "good-child"),
        edges=(("bad", "child"), ("child", "grandchild"), ("good", "good-child")),
    )

    def bad(_):
        raise ValueError("bad input")

    def forbidden(_):
        pytest.fail("descendant of failed node must not execute")

    tasks = {
        "bad": bad,
        "child": forbidden,
        "grandchild": forbidden,
        "good": lambda _: 21,
        "good-child": lambda ctx: ctx.dependencies["good"] * 2,
    }
    result = execute_graph(graph, TaskRegistry(tasks), dict.fromkeys(tasks, "cpu"))
    statuses = {task.node_id: task.status for task in result.tasks}
    assert statuses == {
        "bad": "failed",
        "child": "skipped",
        "grandchild": "skipped",
        "good": "succeeded",
        "good-child": "succeeded",
    }
    assert result.outputs == {"good": 21, "good-child": 42}
    assert result.status == "failed"
    failure = next(task for task in result.tasks if task.node_id == "bad").attempts[0]
    assert failure.error_type == "ValueError" and failure.error_message == "bad input"


def test_retry_yields_device_slot_then_publishes_only_successful_return():
    graph = graph_for(nodes=("a", "b", "child"), edges=(("a", "child"),))
    calls = []

    def flaky(ctx):
        calls.append((ctx.node_id, ctx.attempt))
        if ctx.attempt == 1:
            raise TimeoutError("temporary")
        return 14

    def other(ctx):
        calls.append((ctx.node_id, ctx.attempt))
        return "independent"

    tasks = {
        "a": TaskDefinition(
            flaky, max_retries=1, retry_on=(TimeoutError,), retry_delay_seconds=0.03
        ),
        "b": other,
        "child": lambda ctx: ctx.dependencies["a"] * 3,
    }
    result = execute_graph(graph, TaskRegistry(tasks), dict.fromkeys(tasks, "cpu"))
    assert calls == [("a", 1), ("b", 1), ("a", 2)]
    assert result.outputs["child"] == 42
    attempts = result.tasks[0].attempts
    assert [attempt.status for attempt in attempts] == ["failed", "succeeded"]
    assert attempts[1].submitted_ms >= attempts[0].finished_ms


@pytest.mark.parametrize(
    ("retry_on", "expected"), [((ValueError,), 3), ((TimeoutError,), 1), ((), 1)]
)
def test_retry_budget_and_exception_filter_are_enforced(retry_on, expected):
    graph = graph_for(nodes=("a",), edges=())
    calls = []

    def always_fails(ctx):
        calls.append(ctx.attempt)
        raise ValueError("still bad")

    result = execute_graph(
        graph,
        TaskRegistry({"a": TaskDefinition(always_fails, max_retries=2, retry_on=retry_on)}),
        {"a": "cpu"},
    )
    assert result.status == "failed"
    assert calls == list(range(1, expected + 1))


def test_fail_fast_cancels_unstarted_independent_nodes_after_failure():
    graph = graph_for(nodes=("a", "child", "z"), edges=(("a", "child"),))

    def failure(_):
        raise RuntimeError("stop")

    def forbidden(_):
        pytest.fail("task should not be called")

    result = execute_graph(
        graph,
        TaskRegistry({"a": failure, "child": forbidden, "z": forbidden}),
        {"a": "cpu", "child": "cpu", "z": "cpu"},
        config=ExecutionConfig(max_workers=1, fail_fast=True),
    )
    assert result.status == "failed"
    assert result.cancellation_reason == "fail_fast"
    assert {task.node_id: task.status for task in result.tasks} == {
        "a": "failed",
        "child": "skipped",
        "z": "cancelled",
    }


def test_pre_cancelled_run_has_no_callable_side_effects():
    graph = graph_for(nodes=("a",), edges=())
    cancelled = Event()
    cancelled.set()
    result = execute_graph(
        graph,
        TaskRegistry({"a": lambda _: pytest.fail("called")}),
        {"a": "cpu"},
        cancel_event=cancelled,
    )
    assert result.status == "cancelled"
    assert result.tasks[0].attempts == ()
    assert result.peak_in_flight_by_device == {"cpu": 0}


@pytest.mark.parametrize("mode", ["external", "timeout"])
def test_cooperative_cancellation_stops_running_work_and_blocks_later_tasks(mode):
    graph = graph_for(nodes=("a", "child"), edges=(("a", "child"),))
    external, started, tick = Event(), Event(), Event()

    def cooperate(ctx):
        started.set()
        while not ctx.cancellation.cancelled:
            tick.wait(0.002)
        ctx.cancellation.raise_if_cancelled()

    registry = TaskRegistry({"a": cooperate, "child": lambda _: pytest.fail("child ran")})
    config = ExecutionConfig(timeout_seconds=0.04 if mode == "timeout" else None)
    with ThreadPoolExecutor(max_workers=1) as harness:
        future = harness.submit(
            execute_graph,
            graph,
            registry,
            {"a": "cpu", "child": "cpu"},
            config=config,
            cancel_event=external,
        )
        assert started.wait(5)
        if mode == "external":
            external.set()
        result = future.result(5)
    assert result.status == "cancelled"
    assert result.cancellation_reason == (
        "timeout" if mode == "timeout" else "external_cancellation"
    )
    assert all(task.status == "cancelled" for task in result.tasks)
    assert result.tasks[0].attempts[0].error_type == "TaskCancelled"


def test_timeout_joins_noncooperating_task_and_never_claims_forced_termination():
    graph = graph_for(nodes=("a", "b"), edges=())
    started, release, context_ready = Event(), Event(), []

    def finishes_late(ctx):
        context_ready.append(ctx)
        started.set()
        assert release.wait(5)
        return 42

    with ThreadPoolExecutor(max_workers=1) as harness:
        future = harness.submit(
            execute_graph,
            graph,
            TaskRegistry({"a": finishes_late, "b": lambda _: pytest.fail("late task started")}),
            {"a": "cpu", "b": "cpu"},
            config=ExecutionConfig(timeout_seconds=0.02),
        )
        try:
            assert started.wait(5)
            deadline_seen = Event()
            for _ in range(200):
                if context_ready[0].cancellation.cancelled:
                    deadline_seen.set()
                    break
                deadline_seen.wait(0.005)
            assert deadline_seen.is_set()
            assert not future.done()
        finally:
            release.set()
        result = future.result(5)
    assert result.status == "cancelled"
    assert result.outputs == {"a": 42}
    assert result.tasks[0].status == "succeeded"
    assert result.tasks[1].status == "cancelled"


def test_registry_snapshots_bindings_and_output_values_are_explicitly_shared():
    graph = graph_for(nodes=("a", "child"), edges=(("a", "child"),))
    value = {"answer": 42}
    definitions = {"a": lambda _: value, "child": lambda ctx: ctx.dependencies["a"]}
    registry = TaskRegistry(definitions)
    definitions["a"] = lambda _: "changed"
    result = execute_graph(graph, registry, {"a": "cpu", "child": "cpu"})
    assert result.outputs["a"] is value
    assert result.outputs["child"] is value
    with pytest.raises(TypeError):
        result.outputs["new"] = 5
    with pytest.raises(TypeError):
        registry.tasks["new"] = lambda _: 1


@pytest.mark.parametrize(
    "config",
    [
        {"max_workers": 0},
        {"max_workers": 65},
        {"max_workers": True},
        {"device_workers": {"cpu": 0}},
        {"device_workers": {"cpu": 65}},
        {"device_workers": []},
        {"device_workers": {"": 1}},
        {"fail_fast": 1},
        {"timeout_seconds": float("nan")},
        {"timeout_seconds": float("inf")},
        {"timeout_seconds": True},
        {"timeout_seconds": 10**1000},
    ],
)
def test_execution_config_rejects_invalid_limits(config):
    with pytest.raises(ValidationError):
        ExecutionConfig(**config)


@pytest.mark.parametrize(
    "settings",
    [
        {"max_retries": -1},
        {"max_retries": 21},
        {"max_retries": True},
        {"retry_delay_seconds": float("nan")},
        {"retry_delay_seconds": 61},
        {"retry_on": [ValueError]},
        {"retry_on": (BaseException,)},
        {"retry_on": (None,)},
    ],
)
def test_task_definition_rejects_invalid_retry_policy(settings):
    with pytest.raises(ValidationError):
        TaskDefinition(lambda _: None, **settings)


@pytest.mark.parametrize(
    "tasks", [{}, [], {"a": None}, {"bad\nname": lambda _: 1}, {"x" * 1025: lambda _: 1}]
)
def test_registry_rejects_invalid_definitions(tasks):
    with pytest.raises(ValidationError):
        TaskRegistry(tasks)


def test_registry_rejects_coroutine_functions_and_runtime_rejects_hidden_awaitable():
    graph = graph_for(nodes=("a",), edges=())

    async def async_function(_):
        return 42

    class AsyncCallable:
        async def __call__(self, _):
            return 42

    for function in (async_function, AsyncCallable()):
        with pytest.raises(ValidationError):
            TaskRegistry({"a": function})
    result = execute_graph(
        graph, TaskRegistry({"a": lambda ctx: async_function(ctx)}), {"a": "cpu"}
    )
    assert result.status == "failed"
    assert result.tasks[0].attempts[0].error_type == "TypeError"


def test_every_preflight_error_happens_before_callable_entry():
    graph = graph_for(nodes=("a",), edges=())
    registry = TaskRegistry({"a": lambda _: pytest.fail("side effect during invalid run")})
    for placements in ({}, {"x": "cpu"}, {"a": "missing"}, {"a": []}, []):
        with pytest.raises(ValidationError):
            execute_graph(graph, registry, placements)
    for keyword in (
        {"config": object()},
        {"cancel_event": 1},
        {"config": ExecutionConfig(device_workers={"missing": 1})},
    ):
        with pytest.raises(ValidationError):
            execute_graph(graph, registry, {"a": "cpu"}, **keyword)
    with pytest.raises(ValidationError):
        execute_graph(graph, TaskRegistry({"wrong": lambda _: 1}), {"a": "cpu"})
    with pytest.raises(ValidationError):
        execute_graph(object(), registry, {"a": "cpu"})
    overloaded = graph_for(nodes=("a", "b"), edges=(), memory=1, node_memory=1)
    with pytest.raises(ValidationError, match="memory admission"):
        execute_graph(
            overloaded,
            TaskRegistry(
                {"a": lambda _: pytest.fail("called"), "b": lambda _: pytest.fail("called")}
            ),
            {"a": "cpu", "b": "cpu"},
        )


def test_execution_example_runs_without_importing_external_code_or_network():
    runpy.run_path(
        str(Path(__file__).parents[1] / "examples" / "execute_graph.py"), run_name="__main__"
    )


def test_memory_admission_uses_same_floating_tolerance_as_planner():
    graph = graph_from_dict(
        {
            "devices": [{"name": "cpu", "memory_mb": 0.3}],
            "nodes": [
                {"id": node, "kind": "test", "memory_mb": memory, "latency_ms": {"cpu": 1}}
                for node, memory in (("a", 0.1), ("b", 0.2))
            ],
            "edges": [],
        }
    )
    plan = GreedyPlanner().plan(graph)
    result = execute_graph(
        graph, TaskRegistry({"a": lambda _: 1, "b": lambda _: 2}), plan.placements
    )
    assert result.status == "succeeded"
    assert result.reserved_memory_mb == plan.memory_used_mb


def test_explicit_task_cancellation_is_never_retried_and_blocks_descendants():
    graph = graph_for(nodes=("a", "child"), edges=(("a", "child"),))

    def cancel(_):
        raise TaskCancelled("task declined work")

    registry = TaskRegistry(
        {"a": TaskDefinition(cancel, max_retries=20), "child": lambda _: pytest.fail("called")}
    )
    result = execute_graph(graph, registry, {"a": "cpu", "child": "cpu"})
    assert result.status == "cancelled"
    assert len(result.tasks[0].attempts) == 1
    assert result.tasks[1].reason == "dependency_cancelled:a"


def test_process_control_exception_propagates_after_worker_cleanup():
    graph = graph_for(nodes=("a",), edges=())

    def terminate(_):
        raise SystemExit(17)

    with pytest.raises(SystemExit) as failure:
        execute_graph(graph, TaskRegistry({"a": terminate}), {"a": "cpu"})
    assert failure.value.code == 17


def test_exception_message_bound_and_formatting_failure_are_reported():
    graph = graph_for(nodes=("a",), edges=())

    class UnprintableError(Exception):
        def __str__(self):
            raise RuntimeError("broken formatter")

    for error, expected in (
        (ValueError("x" * 2000), "x" * 1024),
        (UnprintableError(), "<exception could not be formatted>"),
    ):

        def fail(_, exception=error):
            raise exception

        result = execute_graph(graph, TaskRegistry({"a": fail}), {"a": "cpu"})
        assert result.tasks[0].attempts[0].error_message == expected
