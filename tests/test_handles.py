from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Event, Thread
from threading import enumerate as threads

import pytest
from test_execution import graph_for

from graph_sail import (
    ExecutionConfig,
    TaskCancelled,
    TaskDefinition,
    TaskNotSuccessful,
    TaskRegistry,
    ValidationError,
    WaitResult,
    start_graph,
    start_process_graph,
)


def start(tasks, edges=(), *, config=None):
    graph = graph_for(tuple(tasks), edges)
    return start_graph(
        graph,
        TaskRegistry(tasks),
        dict.fromkeys(tasks, "cpu"),
        config=config or ExecutionConfig(max_workers=2, device_workers={"cpu": 2}),
    )


@pytest.fixture(autouse=True)
def no_owned_threads_left():
    before = {thread.ident for thread in threads() if thread.name.startswith("graph-sail")}
    yield
    assert {thread.ident for thread in threads() if thread.name.startswith("graph-sail")} == before


def test_partial_dependency_result_arrives_while_unrelated_branch_is_still_running():
    entered, release = Event(), Event()

    def blocked(_):
        entered.set()
        assert release.wait(10)
        return 7

    handle = start(
        {"a": lambda _: 6, "b": blocked, "c": lambda ctx: ctx.dependencies["a"] * 2},
        (("a", "c"),),
    )
    try:
        assert entered.wait(5)
        assert handle.node("c").result(5) == 12
        assert handle.node("a").done()
        assert not handle.node("b").done()
        assert not handle.done() and not handle.closed
        assert handle.wait(("c", "b", "a"), count=2) == WaitResult(("c", "a"), ("b",))
        assert handle.wait(("b",), timeout=0) == WaitResult((), ("b",))
        with pytest.raises(TimeoutError, match="execution result"):
            handle.result(0)
        with pytest.raises(TimeoutError, match="node 'b'"):
            handle.node("b").execution(0)
        release.set()
        result = handle.result(5)
        assert result.status == "succeeded"  # read timeouts did not request a stop
        assert result.outputs == {"a": 6, "b": 7, "c": 12}
        assert handle.done() and not handle.closed
        assert not handle.cancel()
    finally:
        release.set()
        handle.close()
    assert handle.closed
    handle.close(0)


def test_none_is_a_successful_value_and_read_does_not_copy_shared_results():
    value = [1]
    with start({"a": lambda _: None, "b": lambda _: value}) as handle:
        assert handle.node("a").result(5) is None
        assert handle.node("a").execution().status == "succeeded"
        assert handle.node("b").result(5) is value
        assert handle.result(5).outputs["b"] is value
        value.append(2)
        assert handle.node("b").result() == [1, 2]


def test_retries_publish_only_the_final_terminal_with_all_attempts():
    first, release = Event(), Event()

    def retry(ctx):
        if ctx.attempt == 1:
            first.set()
            raise ValueError("retry me")
        assert release.wait(10)
        return 4

    handle = start({"a": TaskDefinition(retry, max_retries=1)})
    try:
        assert first.wait(5)
        assert not handle.node("a").done()
        assert handle.wait(count=0) == WaitResult((), ("a",))
        release.set()
        assert handle.node("a").result(5) == 4
        execution = handle.node("a").execution()
        assert [attempt.status for attempt in execution.attempts] == ["failed", "succeeded"]
    finally:
        release.set()
        handle.close()


def test_failed_and_skipped_are_ready_but_cannot_be_read_as_success():
    def fail(_):
        raise ValueError("bad source")

    with start({"a": fail, "b": lambda _: 1, "c": lambda _: 2}, (("a", "c"),)) as handle:
        result = handle.result(5)
        assert result.status == "failed"
        assert handle.wait(("c", "b", "a"), count=3) == WaitResult(("c", "b", "a"), ())
        for node, status in (("a", "failed"), ("c", "skipped")):
            with pytest.raises(TaskNotSuccessful) as failure:
                handle.node(node).result()
            assert failure.value.execution == handle.node(node).execution()
            assert failure.value.execution.status == status
        assert handle.node("b").result() == 1


def test_close_timeout_keeps_ownership_and_cancel_is_idempotent():
    entered, release = Event(), Event()

    def blocked(_):
        entered.set()
        assert release.wait(10)
        return "late-success"

    handle = start({"a": blocked, "b": lambda _: "never"}, (("a", "b"),))
    try:
        assert entered.wait(5)
        assert handle.cancel()
        assert not handle.cancel()
        with pytest.raises(TimeoutError, match="retry close"):
            handle.close(0)
        assert not handle.closed
        release.set()
        result = handle.result(5)
        assert result.status == "cancelled"
        assert result.outputs == {"a": "late-success"}
        assert handle.node("b").execution().status == "cancelled"
        with pytest.raises(TaskNotSuccessful):
            handle.node("b").result()
    finally:
        release.set()
        handle.close()
    assert handle.closed


def test_context_exit_signals_cooperative_task_and_preserves_body_exception():
    entered, pause = Event(), Event()

    def cooperative(ctx):
        entered.set()
        while not ctx.cancellation.cancelled:
            pause.wait(0.001)
        raise TaskCancelled("stopped")

    with pytest.raises(LookupError, match="body"), start({"a": cooperative}) as handle:
        assert entered.wait(5)
        raise LookupError("body")
    assert handle.closed and handle.done()
    assert handle.result().status == "cancelled"


@pytest.mark.parametrize("error", [KeyboardInterrupt("stop"), SystemExit(4), RuntimeError("pool")])
def test_controller_errors_wake_waiters_without_fabricating_node_success(monkeypatch, error):
    from graph_sail.execution import _Runner

    entered, release = Event(), Event()

    def explode(_):
        entered.set()
        assert release.wait(10)
        raise error

    monkeypatch.setattr(_Runner, "run", explode)
    handle = start({"a": lambda _: 1})
    try:
        assert entered.wait(5)
        release.set()
        with pytest.raises(type(error)) as observed:
            handle.result(5)
        assert observed.value is error
        assert handle.done() and not handle.node("a").done()
        with pytest.raises(type(error)):
            handle.node("a").result()
        with pytest.raises(type(error)):
            handle.wait(count=1)
        assert handle.wait(count=0) == WaitResult((), ("a",))
    finally:
        release.set()
        handle.close()


def test_multiple_waiters_observe_same_non_consuming_result():
    release = Event()

    def task(_):
        assert release.wait(10)
        return 42

    handle = start({"a": task})
    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(handle.node("a").result, 5) for _ in range(4)]
            release.set()
            assert [future.result(5) for future in futures] == [42] * 4
        assert handle.result(5).outputs == {"a": 42}
    finally:
        release.set()
        handle.close()


@pytest.mark.parametrize("timeout", [-1, True, "1", float("nan"), float("inf"), 86401])
def test_all_public_waits_reject_invalid_timeouts_without_cancelling(timeout):
    with start({"a": lambda _: 1}) as handle:
        for call in (
            handle.result,
            handle.close,
            handle.node("a").result,
            handle.node("a").execution,
        ):
            with pytest.raises(ValidationError):
                call(timeout)
        with pytest.raises(ValidationError):
            handle.wait(timeout=timeout)
        assert handle.result(5).status == "succeeded"


@pytest.mark.parametrize("selected", [["a"], "a", ("b",), ([],), ("a", "a"), (1,)])
def test_invalid_node_selection(selected):
    with start({"a": lambda _: 1}) as handle:
        with pytest.raises(ValidationError):
            handle.wait(selected)
        assert handle.result(5).status == "succeeded"


@pytest.mark.parametrize("count", [-1, 2, True, 0.5, None])
def test_invalid_wait_count(count):
    with start({"a": lambda _: 1}) as handle:
        with pytest.raises(ValidationError):
            handle.wait(count=count)
        assert handle.wait((), count=0) == WaitResult((), ())
        assert handle.result(5).status == "succeeded"


def test_unknown_node_is_rejected_before_wait():
    with start({"a": lambda _: 1}) as handle:
        for value in ("missing", [], None):
            with pytest.raises(ValidationError):
                handle.node(value)
        handle.result(5)


@pytest.mark.parametrize("factory", [start_graph, start_process_graph])
def test_bad_registration_does_not_start_a_controller(monkeypatch, factory):
    def forbidden(_):
        pytest.fail("admission started a controller")

    monkeypatch.setattr(Thread, "start", forbidden)
    with pytest.raises(ValidationError):
        factory(graph_for(("a",), ()), TaskRegistry({"wrong": lambda _: 1}), {"a": "cpu"})


def test_failed_thread_start_leaves_no_owned_work(monkeypatch):
    def fail(_):
        raise RuntimeError("cannot start")

    monkeypatch.setattr(Thread, "start", fail)
    with pytest.raises(RuntimeError, match="cannot start"):
        start({"a": lambda _: 1})


def test_custom_start_that_started_then_raised_still_joins_controller(monkeypatch):
    original = Thread.start
    created = []

    def fail_after_start(thread):
        original(thread)
        if thread.name == "graph-sail-controller":
            created.append(thread)
            raise RuntimeError("post-start failure")

    monkeypatch.setattr(Thread, "start", fail_after_start)
    with pytest.raises(RuntimeError, match="post-start failure"):
        start({"a": lambda _: 1})
    assert len(created) == 1 and not created[0].is_alive()


def test_nodes_duplicate_subset_and_terminal_metadata_are_stable():
    with start({"b": lambda _: 2, "a": lambda _: 1}) as handle:
        assert handle.nodes == ("a", "b")
        with pytest.raises(ValidationError, match="duplicates"):
            handle.wait(("a", "a"))
        handle.result(5)
        assert handle.node("a").execution() is handle.node("a").execution()


@pytest.mark.parametrize(
    ("body", "cleanup", "winner"),
    [
        (ValueError("body"), RuntimeError("cleanup"), ValueError),
        (ValueError("body"), KeyboardInterrupt("cleanup"), KeyboardInterrupt),
        (KeyboardInterrupt("body"), RuntimeError("cleanup"), KeyboardInterrupt),
        (None, RuntimeError("cleanup"), RuntimeError),
    ],
)
def test_context_cleanup_preserves_control_priority(monkeypatch, body, cleanup, winner):
    from graph_sail import ExecutionHandle

    handle = start({"a": lambda _: 1})
    handle.result(5)
    handle.close()

    def fail(*_):
        raise cleanup

    monkeypatch.setattr(ExecutionHandle, "close", fail)
    with pytest.raises(winner):
        handle.__exit__(type(body) if body else None, body, None)


def test_real_task_control_error_settles_execution_after_other_worker_cleanup():
    entered, stopped = Event(), Event()

    def control(_):
        assert entered.wait(5)
        raise KeyboardInterrupt("task control")

    def cooperative(ctx):
        entered.set()
        pause = Event()
        while not ctx.cancellation.cancelled:
            pause.wait(0.001)
        stopped.set()
        ctx.cancellation.raise_if_cancelled()

    with start({"a": control, "b": cooperative}) as handle:
        with pytest.raises(KeyboardInterrupt, match="task control"):
            handle.result(5)
        assert stopped.is_set()


def test_close_requires_backend_completion_not_only_native_thread_liveness(monkeypatch):
    entered, release = Event(), Event()

    def blocked(_):
        entered.set()
        assert release.wait(10)

    handle = start({"a": blocked})
    try:
        assert entered.wait(5)
        with monkeypatch.context() as patch:
            original = Thread.is_alive
            patch.setattr(
                Thread,
                "is_alive",
                lambda thread: False if thread is handle._thread else original(thread),
            )
            with pytest.raises(TimeoutError):
                handle.close(0)
            assert not handle.closed
    finally:
        release.set()
        handle.close()


def test_close_waits_for_controller_exit_after_backend_settlement(monkeypatch):
    original = Thread.run
    held, release = Event(), Event()

    def lingering(thread):
        original(thread)
        if thread.name == "graph-sail-controller":
            held.set()
            assert release.wait(10)

    monkeypatch.setattr(Thread, "run", lingering)
    handle = start({"a": lambda _: 1})
    try:
        assert handle.result(5).status == "succeeded"
        assert held.wait(5)
        with pytest.raises(TimeoutError, match="retry close"):
            handle.close(0)
        assert handle.done() and not handle.closed
    finally:
        release.set()
        handle.close()


def test_controller_self_join_is_explicitly_rejected(monkeypatch):
    from graph_sail.execution import _Runner

    ready = Event()
    box = []

    def attempt_self_join(_):
        assert ready.wait(5)
        box[0].close()

    monkeypatch.setattr(_Runner, "run", attempt_self_join)
    handle = start({"a": lambda _: 1})
    box.append(handle)
    ready.set()
    with handle, pytest.raises(RuntimeError, match="cannot join itself"):
        handle.result(5)


def test_internal_missing_result_invariant_is_not_silently_success(monkeypatch):
    from graph_sail.execution import _Runner

    monkeypatch.setattr(_Runner, "run", lambda _: None)
    with start({"a": lambda _: 1}) as handle:
        with pytest.raises(RuntimeError, match="has no result"):
            handle.result(5)
        with pytest.raises(RuntimeError, match="no terminal node result"):
            handle.node("a").result(5)


@pytest.mark.parametrize(
    "cleanup", [RuntimeError("join failure"), KeyboardInterrupt("join control")]
)
def test_post_start_cleanup_error_keeps_control_priority_after_real_join(monkeypatch, cleanup):
    original_start, original_join = Thread.start, Thread.join
    created = []

    def failed_start(thread):
        original_start(thread)
        if thread.name == "graph-sail-controller":
            created.append(thread)
            raise ValueError("start failure")

    def failed_join(thread, timeout=None):
        original_join(thread, timeout)
        if thread.name == "graph-sail-controller":
            raise cleanup

    monkeypatch.setattr(Thread, "start", failed_start)
    monkeypatch.setattr(Thread, "join", failed_join)
    with pytest.raises(ValueError if isinstance(cleanup, Exception) else KeyboardInterrupt):
        start({"a": lambda _: 1})
    assert len(created) == 1 and not created[0].is_alive()


def test_offline_partial_result_example(capsys):
    import runpy
    from pathlib import Path

    runpy.run_path(
        str(Path(__file__).parents[1] / "examples" / "execution_handles.py"), run_name="__main__"
    )
    assert "pending=('b',)" in capsys.readouterr().out
