"""A bounded accepted yield runs a real registered fork/join DAG."""

from __future__ import annotations

from threading import Event
from threading import enumerate as threads

import pytest

from graph_sail import (
    ExecutionConfig,
    LogicalResources,
    StreamGraphExecutionError,
    StreamMapConfig,
    TaskDefinition,
    TaskRegistry,
    ValidationError,
    start_stream_graph,
)
from graph_sail.models import DeviceSpec, EdgeSpec, GraphSpec, NodeSpec


def _graph(*, device_memory: float = 64) -> GraphSpec:
    return GraphSpec(
        "per-yield-fork-join",
        (DeviceSpec("cpu", device_memory),),
        tuple(NodeSpec(node, "work", 1, {"cpu": 1}) for node in ("input", "left", "right", "join")),
        (
            EdgeSpec("input", "left"),
            EdgeSpec("input", "right"),
            EdgeSpec("left", "join"),
            EdgeSpec("right", "join"),
        ),
    )


@pytest.fixture(autouse=True)
def no_owned_threads_left():
    before = {thread.ident for thread in threads() if thread.name.startswith("graph-sail")}
    yield
    assert {thread.ident for thread in threads() if thread.name.startswith("graph-sail")} == before


def _registry(
    *, fail_on: int | None = None, call_log: list[tuple[str, int]] | None = None
) -> TaskRegistry:
    def left(context):
        value = context.dependencies["input"]
        if call_log is not None:
            call_log.append(("left", value))
        if value == fail_on:
            raise ValueError("left-failed")
        return value * 2

    def right(context):
        value = context.dependencies["input"]
        if call_log is not None:
            call_log.append(("right", value))
        return value + 1

    def join(context):
        return context.dependencies["left"] + context.dependencies["right"]

    return TaskRegistry({"left": left, "right": right, "join": join})


def _start(source, registry=None, *, graph=None, config=None, execution_config=None, **kwargs):
    return start_stream_graph(
        source,
        _graph() if graph is None else graph,
        _registry() if registry is None else registry,
        dict.fromkeys(("input", "left", "right", "join"), "cpu"),
        input_node="input",
        output_node="join",
        config=StreamMapConfig(1, 1) if config is None else config,
        execution_config=ExecutionConfig(max_workers=3, device_workers={"cpu": 3})
        if execution_config is None
        else execution_config,
        **kwargs,
    )


def test_fork_join_finishes_first_item_before_source_eof() -> None:
    release_source = Event()
    source_at_eof = Event()

    def source(context):
        yield 4
        release_source.wait(5)
        context.cancellation.raise_if_cancelled()
        source_at_eof.set()

    handle = _start(source)
    try:
        item = handle.next(5)
        assert (item.sequence, item.value) == (0, 13)
        assert not source_at_eof.is_set()
        release_source.set()
        assert handle.completion(5).status == "succeeded"
    finally:
        release_source.set()
        handle.close(5)


def test_public_consumption_releases_one_global_source_credit() -> None:
    advanced = Event()

    def source(_context):
        yield 1
        advanced.set()
        yield 2

    handle = _start(source)
    try:
        assert not advanced.wait(0.15)
        assert handle.next(5).value == 4
        assert advanced.wait(5)
        assert handle.next(5).value == 7
        assert handle.completion(5).accepted == 2
    finally:
        handle.close(5)


def test_graph_failure_keeps_ordered_prefix_without_task_retry() -> None:
    log: list[tuple[str, int]] = []

    def source(_context):
        yield 1
        yield 2
        yield 3

    handle = _start(source, _registry(fail_on=2, call_log=log))
    try:
        assert handle.next(5).value == 4
        with pytest.raises(StreamGraphExecutionError) as raised:
            handle.next(5)
        assert raised.value.sequence == 1
        assert raised.value.failed_nodes == ("left",)
        failed = next(task for task in raised.value.result.tasks if task.node_id == "left")
        assert failed.attempts[0].error_type == "ValueError"
        assert failed.attempts[0].error_message == "left-failed"
        with pytest.raises(StreamGraphExecutionError):
            handle.completion(5)
        assert log.count(("left", 2)) == 1
        assert ("left", 3) not in log
    finally:
        handle.close(5)


def test_later_graph_failure_does_not_cancel_earlier_item_prefix() -> None:
    first_entered = Event()
    second_failed = Event()
    release_first = Event()

    def source(_context):
        yield 1
        yield 2

    ordinary = _registry()

    def left(context):
        value = context.dependencies["input"]
        if value == 1:
            first_entered.set()
            release_first.wait(5)
            context.cancellation.raise_if_cancelled()
            return value * 2
        assert first_entered.wait(3)
        second_failed.set()
        raise ValueError("later-item-failed")

    registry = TaskRegistry({**ordinary.tasks, "left": left})
    handle = _start(source, registry, config=StreamMapConfig(2, 2))
    try:
        assert first_entered.wait(3)
        assert second_failed.wait(3)
        with handle._condition:
            assert handle._condition.wait_for(lambda: handle._failure_sequence == 1, 3)
        release_first.set()
        assert handle.next(5).value == 4
        with pytest.raises(StreamGraphExecutionError) as raised:
            handle.next(5)
        assert raised.value.sequence == 1
        assert raised.value.failed_nodes == ("left",)
    finally:
        release_first.set()
        handle.close(5)


def test_source_failure_does_not_cancel_already_accepted_dag() -> None:
    graph_entered = Event()
    release_graph = Event()

    def source(_context):
        yield 1
        assert graph_entered.wait(3)
        raise ValueError("source-after-accepted-yield")

    ordinary = _registry()

    def left(context):
        graph_entered.set()
        release_graph.wait(5)
        context.cancellation.raise_if_cancelled()
        return context.dependencies["input"] * 2

    registry = TaskRegistry({**ordinary.tasks, "left": left})
    handle = _start(source, registry, config=StreamMapConfig(2, 2))
    try:
        assert graph_entered.wait(3)
        with handle._condition:
            assert handle._condition.wait_for(lambda: handle._failure_sequence == 1, 3)
        release_graph.set()
        assert handle.next(5).value == 4
        with pytest.raises(ValueError, match="source-after-accepted-yield"):
            handle.next(5)
    finally:
        release_graph.set()
        handle.close(5)


def test_invalid_template_is_rejected_before_source_enters() -> None:
    entered = Event()

    def source(_context):
        entered.set()
        yield 1

    with pytest.raises(ValidationError, match="registry"):
        _start(source, TaskRegistry({"left": lambda ctx: 1}))
    assert not entered.is_set()


def test_aggregate_memory_is_admitted_before_source_enters() -> None:
    entered = Event()

    def source(_context):
        entered.set()
        yield 1

    with pytest.raises(ValidationError, match=r"aggregate.*memory"):
        _start(source, graph=_graph(device_memory=4), config=StreamMapConfig(2, 2))
    assert not entered.is_set()


def test_out_of_order_item_dags_still_publish_in_source_order() -> None:
    second_completed = Event()
    release_first = Event()

    def source(_context):
        yield 1
        yield 2

    def left(context):
        value = context.dependencies["input"]
        if value == 1:
            release_first.wait(5)
        else:
            second_completed.set()
        return value * 2

    other = _registry()
    registry = TaskRegistry({**other.tasks, "left": left})
    handle = _start(source, registry, config=StreamMapConfig(2, 2))
    try:
        assert second_completed.wait(3)
        with pytest.raises(TimeoutError, match="not ready"):
            handle.next(0.05)
        release_first.set()
        assert [(item.sequence, item.value) for item in (handle.next(5), handle.next(5))] == [
            (0, 4),
            (1, 7),
        ]
        assert handle.completion(5).status == "succeeded"
    finally:
        release_first.set()
        handle.close(5)


def test_retry_and_aggregate_resource_overcommit_are_preadmission_errors() -> None:
    entered = Event()

    def source(_context):
        entered.set()
        yield 1

    ordinary = _registry()
    retried = TaskRegistry(
        {
            **ordinary.tasks,
            "left": TaskDefinition(ordinary.definition("left").function, max_retries=1),
        }
    )
    with pytest.raises(ValidationError, match="automatic retries"):
        _start(source, retried)
    resources = LogicalResources({"CPU": 1}, {"left": {"CPU": 0.5}, "right": {"CPU": 0.5}})
    with pytest.raises(ValidationError, match="aggregate logical resource"):
        _start(
            source,
            config=StreamMapConfig(2, 2),
            execution_config=ExecutionConfig(
                max_workers=3, device_workers={"cpu": 3}, resources=resources
            ),
        )
    assert not entered.is_set()


def test_unrelated_sink_and_aggregate_thread_overcommit_are_preadmission_errors() -> None:
    entered = Event()

    def source(_context):
        entered.set()
        yield 1

    fork_without_join = GraphSpec(
        "unrelated-sink",
        _graph().devices,
        _graph().nodes,
        (EdgeSpec("input", "left"), EdgeSpec("input", "right"), EdgeSpec("left", "join")),
    )
    with pytest.raises(ValidationError, match="input-to-output path"):
        _start(source, graph=fork_without_join)
    with pytest.raises(ValidationError, match="worker budget"):
        _start(
            source,
            config=StreamMapConfig(max_pending=16, max_workers=16),
            execution_config=ExecutionConfig(max_workers=5, device_workers={"cpu": 5}),
        )
    assert not entered.is_set()


def test_exact_yield_limit_does_not_probe_extra_source_item() -> None:
    advanced: list[int] = []

    def source(_context):
        for value in range(5):
            advanced.append(value)
            yield value

    handle = _start(source, config=StreamMapConfig(1, 1, max_yields=2))
    try:
        assert [handle.next(5).value, handle.next(5).value] == [1, 4]
        assert handle.completion(5).status == "limited"
        assert advanced == [0, 1]
    finally:
        handle.close(5)


def test_cancel_retains_owner_until_noncooperating_graph_task_exits() -> None:
    entered = Event()
    release = Event()

    def source(_context):
        yield 1

    ordinary = _registry()

    def left(context):
        entered.set()
        release.wait(5)
        return context.dependencies["input"] * 2

    registry = TaskRegistry({**ordinary.tasks, "left": left})
    handle = _start(source, registry)
    try:
        assert entered.wait(3)
        assert handle.cancel()
        with pytest.raises(TimeoutError, match="live work"):
            handle.close(0.05)
        assert not handle.closed
    finally:
        release.set()
        handle.close(5)
    assert handle.closed
