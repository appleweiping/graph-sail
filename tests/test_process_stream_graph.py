"""A stream yield drives an owned local spawned-process DAG before source EOF."""

from __future__ import annotations

import os
import sys
import time
from importlib.machinery import ModuleSpec
from multiprocessing import active_children
from pathlib import Path
from threading import Event
from threading import enumerate as threads
from types import FunctionType, ModuleType, SimpleNamespace

import pytest
from process_execution_tasks import StopTask
from process_stream_graph_tasks import BlockFirst, die_on_two, fail_on_two, join, left, right

import graph_sail.process_stream_graph as process_stream_graph_module
from graph_sail import (
    ActorConfig,
    ActorSerializationError,
    ExecutionConfig,
    ExecutionHandle,
    LogicalResources,
    ProcessStreamGraphExecutionError,
    ProcessStreamGraphInfrastructureError,
    ProcessTaskConfig,
    StreamMapConfig,
    TaskDefinition,
    TaskRegistry,
    ValidationError,
    start_process_stream_graph,
)
from graph_sail.actors import ProcessActor
from graph_sail.models import DeviceSpec, EdgeSpec, GraphSpec, NodeSpec
from graph_sail.process_execution import _ProcessInvoker, _run_process_runner


def _graph() -> GraphSpec:
    return GraphSpec(
        "per-yield-process-fork-join",
        (DeviceSpec("cpu", 64),),
        tuple(NodeSpec(node, "work", 1, {"cpu": 1}) for node in ("input", "left", "right", "join")),
        (
            EdgeSpec("input", "left"),
            EdgeSpec("input", "right"),
            EdgeSpec("left", "join"),
            EdgeSpec("right", "join"),
        ),
    )


def _start(source, registry=None, *, config=None, execution_config=None, process_config=None):
    return start_process_stream_graph(
        source,
        _graph(),
        TaskRegistry({"left": left, "right": right, "join": join})
        if registry is None
        else registry,
        dict.fromkeys(("input", "left", "right", "join"), "cpu"),
        input_node="input",
        output_node="join",
        config=StreamMapConfig(1, 1) if config is None else config,
        execution_config=ExecutionConfig(max_workers=3, device_workers={"cpu": 3})
        if execution_config is None
        else execution_config,
        process_config=process_config,
    )


def _await_file(path: Path, seconds: float = 20) -> None:
    end = time.monotonic() + seconds
    while not path.exists():
        if time.monotonic() >= end:
            raise AssertionError(f"missing process marker: {path}")
        time.sleep(0.01)


@pytest.fixture(autouse=True)
def no_owned_threads_left():
    before = {thread.ident for thread in threads() if thread.name.startswith("graph-sail")}
    yield
    assert {thread.ident for thread in threads() if thread.name.startswith("graph-sail")} == before


def test_process_stream_graph_api_is_importable() -> None:
    assert callable(start_process_stream_graph)


def test_process_preflight_rejects_unpicklable_task_before_source_entry() -> None:
    entered = Event()

    def source(_context):
        entered.set()
        yield 1

    registry = TaskRegistry({"left": lambda context: 1, "right": right, "join": join})
    with pytest.raises(ActorSerializationError):
        _start(source, registry)
    assert not entered.is_set()


def test_process_preflight_rejects_driver_only_module_before_source_entry(monkeypatch) -> None:
    module_name = "graph_sail_ephemeral_task_for_preflight"
    module = ModuleType(module_name)
    transient = FunctionType(left.__code__, left.__globals__, name="transient")
    transient.__module__ = module_name
    transient.__qualname__ = "transient"
    module.transient = transient
    monkeypatch.setitem(sys.modules, module_name, module)
    entered = Event()

    def source(_context):
        entered.set()
        if False:
            yield None

    handle = None
    try:
        with pytest.raises(ActorSerializationError, match="importable"):
            handle = _start(source, TaskRegistry({"left": transient, "right": right, "join": join}))
    finally:
        if handle is not None:
            handle.close(30)
    assert not entered.is_set()


def test_unproven_spawn_import_reports_sequence_and_driver_diagnostic(monkeypatch) -> None:
    module_name = "graph_sail_unproven_namespace_task"
    module = ModuleType(module_name)
    module.__spec__ = ModuleSpec(module_name, loader=None, is_package=True)
    module.__path__ = []
    transient = FunctionType(left.__code__, left.__globals__, name="transient")
    transient.__module__ = module_name
    transient.__qualname__ = "transient"
    module.transient = transient
    monkeypatch.setitem(sys.modules, module_name, module)

    def source(_context):
        yield 1

    handle = _start(source, TaskRegistry({"left": transient, "right": right, "join": join}))
    try:
        with pytest.raises(ProcessStreamGraphInfrastructureError) as raised:
            handle.next(30)
        error = raised.value
        assert error.sequence == 0
        assert error.process_result is not None
        assert error.cause is None
        failures = (
            attempt
            for task in error.process_result.execution.tasks
            for attempt in task.attempts
            if attempt.status == "failed"
        )
        assert any(attempt.error_type and attempt.error_message for attempt in failures)
    finally:
        handle.close(30)


@pytest.mark.parametrize(
    ("config", "execution_config", "process_config", "match"),
    [
        (StreamMapConfig(max_pending=17, max_workers=1), None, None, "max_pending"),
        (
            StreamMapConfig(max_pending=6, max_workers=6),
            ExecutionConfig(max_workers=3, device_workers={"cpu": 3}),
            None,
            "worker budget",
        ),
        (
            StreamMapConfig(max_pending=4, max_workers=4),
            ExecutionConfig(max_workers=4, device_workers={"cpu": 4}),
            ProcessTaskConfig(max_message_bytes=8 * 1024 * 1024),
            "message budget",
        ),
    ],
)
def test_process_aggregate_bounds_are_preadmission(
    config, execution_config, process_config, match
) -> None:
    entered = Event()

    def source(_context):
        entered.set()
        yield 1

    with pytest.raises(ValidationError, match=match):
        _start(
            source,
            config=config,
            execution_config=execution_config,
            process_config=process_config,
        )
    assert not entered.is_set()


def test_process_resource_overcommit_and_retries_fail_before_source_entry() -> None:
    entered = Event()

    def source(_context):
        entered.set()
        yield 1

    retried = TaskRegistry(
        {"left": TaskDefinition(left, max_retries=1), "right": right, "join": join}
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


def test_oversized_dynamic_item_fails_at_its_sequence_without_spawning() -> None:
    def source(_context):
        yield b"x" * 2048

    handle = _start(source, process_config=ProcessTaskConfig(max_message_bytes=1024))
    try:
        with pytest.raises(ProcessStreamGraphInfrastructureError) as raised:
            handle.next(10)
        assert raised.value.sequence == 0
        assert isinstance(raised.value.cause, ActorSerializationError)
        assert raised.value.process_result is None
    finally:
        handle.close(10)


def test_failed_process_cleanup_retains_retryable_core_owner_without_spawning() -> None:
    class FakeRunner:
        def run(self):
            return object()

    class FakeInvoker:
        def __init__(self):
            self.close_calls = 0
            self.leases = {1: object()}
            self.startup_cleanup = []

        def close(self):
            self.close_calls += 1
            if self.close_calls == 1:
                raise RuntimeError("injected process cleanup failure")
            self.leases.clear()

    invoker = FakeInvoker()
    with pytest.raises(RuntimeError, match="injected process cleanup failure") as raised:
        _run_process_runner(FakeRunner(), invoker, time.monotonic())
    owner = getattr(raised.value, "process_graph_cleanup", None)
    assert owner is not None
    assert not owner.closed
    owner.close()
    assert owner.closed
    assert invoker.close_calls == 2


def test_stream_owner_does_not_claim_closed_after_injected_child_cleanup_failure(
    monkeypatch,
) -> None:
    class FakeOwner:
        closed = False
        close_calls = 0

        def close(self):
            self.close_calls += 1
            if self.close_calls == 1:
                raise RuntimeError("injected retry cleanup failure")
            self.closed = True

    owner = FakeOwner()
    upstream = RuntimeError("injected process cleanup failure")

    def fake_process_graph(*_args, **_kwargs):
        upstream.process_graph_cleanup = owner
        raise upstream

    monkeypatch.setattr(process_stream_graph_module, "execute_process_graph", fake_process_graph)

    def source(_context):
        yield 1

    handle = _start(source)
    with pytest.raises(ProcessStreamGraphInfrastructureError) as raised:
        handle.next(10)
    assert raised.value.cause is upstream
    assert raised.value.__cause__ is upstream
    with pytest.raises(RuntimeError, match="injected retry cleanup failure"):
        handle.close(10)
    assert not handle.closed
    handle.close(10)
    assert handle.closed
    assert owner.close_calls == 2


def test_stream_owner_retries_child_cleanup_even_if_stream_close_fails() -> None:
    class FakeStream:
        closed = False
        close_calls = 0

        def close(self, _timeout):
            self.close_calls += 1
            if self.close_calls == 1:
                raise TimeoutError("injected stream join timeout")
            self.closed = True

    class FakeCleanup:
        closed = False
        close_calls = 0

        def close(self):
            self.close_calls += 1
            self.closed = True

    stream = FakeStream()
    cleanup = FakeCleanup()
    handle = process_stream_graph_module.ProcessStreamGraph(stream, cleanup)
    with pytest.raises(TimeoutError, match="injected stream join timeout"):
        handle.close(0)
    assert cleanup.closed
    assert cleanup.close_calls == 1
    assert not handle.closed
    handle.close(0)
    assert handle.closed


def test_existing_process_handle_retains_injected_cleanup_owner_without_spawning() -> None:
    class FakeRunner:
        order = ("node",)
        terminal_observer = None

    class FakeOwner:
        closed = False
        close_calls = 0

        def close(self):
            self.close_calls += 1
            if self.close_calls == 1:
                raise RuntimeError("injected handle cleanup retry failed")
            self.closed = True

    owner = FakeOwner()

    def run():
        error = RuntimeError("injected process close failed")
        error.process_graph_cleanup = owner
        raise error

    handle = ExecutionHandle(FakeRunner(), run, Event())._start()
    with pytest.raises(RuntimeError, match="injected process close failed"):
        handle.result(10)
    with pytest.raises(RuntimeError, match="injected handle cleanup retry failed"):
        handle.close(10)
    assert not handle.closed
    handle.close(10)
    assert handle.closed


def test_task_worker_partial_bootstrap_retains_retryable_owner_without_spawning(
    monkeypatch,
) -> None:
    calls = 0

    def fake_initialize(self, *_args, **_kwargs):
        self._thread = SimpleNamespace(ident=1)
        raise RuntimeError("injected after broker start")

    def fake_terminate(self):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("injected failed first terminate")

    monkeypatch.setattr(ProcessActor, "_initialize", fake_initialize)
    monkeypatch.setattr(ProcessActor, "terminate", fake_terminate)
    with pytest.raises(RuntimeError, match="injected after broker start") as raised:
        ProcessActor._for_task_worker(ActorConfig(max_pending=1))
    owner = getattr(raised.value, "process_stream_cleanup", None)
    assert owner is not None
    assert not owner.closed
    owner.close()
    assert owner.closed
    assert calls == 2


def test_invoker_retains_partial_startup_owner_after_failed_worker_factory(monkeypatch) -> None:
    class FakeOwner:
        closed = False
        close_calls = 0

        def close(self):
            self.close_calls += 1
            if self.close_calls == 1:
                raise RuntimeError("injected first startup cleanup failed")
            self.closed = True

    owner = FakeOwner()

    def fake_factory(_cls, _config):
        error = RuntimeError("injected worker startup failed")
        error.process_stream_cleanup = owner
        raise error

    monkeypatch.setattr(ProcessActor, "_for_task_worker", classmethod(fake_factory))
    invoker = _ProcessInvoker(ProcessTaskConfig())
    with pytest.raises(RuntimeError, match="injected worker startup failed"):
        invoker._worker()
    with pytest.raises(RuntimeError, match="injected first startup cleanup failed"):
        invoker.close()
    assert not owner.closed
    invoker.close()
    assert owner.closed
    assert owner.close_calls == 2


def test_invoker_retains_worker_if_lease_registration_and_first_cleanup_fail(
    monkeypatch,
) -> None:
    class FailingLeaseMap(dict):
        def __setitem__(self, _key, _value):
            raise RuntimeError("injected lease registration failed")

    class FakeSender:
        def close(self):
            return None

    class FakeActor:
        _thread = SimpleNamespace(ident=1)

        def __init__(self):
            self.terminate_calls = 0

        def terminate(self):
            self.terminate_calls += 1
            if self.terminate_calls == 1:
                raise RuntimeError("injected first actor retirement failed")

    actor = FakeActor()
    monkeypatch.setattr(
        ProcessActor,
        "_for_task_worker",
        classmethod(lambda _cls, _config: (actor, FakeSender())),
    )
    invoker = _ProcessInvoker(ProcessTaskConfig())
    invoker.leases = FailingLeaseMap()
    with pytest.raises(RuntimeError, match="injected lease registration failed"):
        invoker._worker()
    assert len(invoker.startup_cleanup) == 1
    assert not invoker.startup_cleanup[0].closed
    invoker.close()
    assert not invoker.startup_cleanup
    assert actor.terminate_calls == 2


def test_invoker_keeps_single_owner_if_lease_insert_succeeds_then_raises(monkeypatch) -> None:
    class InsertThenFailLeaseMap(dict):
        def __setitem__(self, key, value):
            super().__setitem__(key, value)
            raise RuntimeError("injected after lease insertion")

    class FakeSender:
        def close(self):
            return None

    class FakeActor:
        pid = 4242
        exitcode = 0
        _thread = SimpleNamespace(ident=1)

        def __init__(self):
            self.close_calls = 0
            self.terminate_calls = 0

        def close(self, _timeout):
            self.close_calls += 1

        def terminate(self):
            self.terminate_calls += 1
            raise RuntimeError("injected competing actor cleanup")

    actor = FakeActor()
    monkeypatch.setattr(
        ProcessActor,
        "_for_task_worker",
        classmethod(lambda _cls, _config: (actor, FakeSender())),
    )
    invoker = _ProcessInvoker(ProcessTaskConfig())
    invoker.leases = InsertThenFailLeaseMap()
    with pytest.raises(RuntimeError, match="injected after lease insertion"):
        invoker._worker()
    assert len(invoker.leases) == 1
    assert not invoker.startup_cleanup
    assert actor.terminate_calls == 0
    invoker.close()
    assert not invoker.leases
    assert actor.close_calls == 1


def test_process_graph_keeps_pre_advance_credit_until_public_consumption() -> None:
    advanced = Event()

    def source(_context):
        yield 1
        advanced.set()
        yield 2

    handle = _start(source)
    try:
        assert not advanced.wait(0.1)
        assert handle.next(30).value[0] == 4
        assert advanced.wait(10)
        assert handle.next(30).value[0] == 7
        assert handle.completion(30).accepted == 2
    finally:
        handle.close(30)


def test_process_graph_exact_yield_cap_does_not_probe_next_source_item() -> None:
    advanced: list[int] = []

    def source(_context):
        for value in (4, 5):
            advanced.append(value)
            yield value

    handle = _start(source, config=StreamMapConfig(1, 1, max_yields=1))
    try:
        assert handle.next(30).value[0] == 13
        assert handle.completion(30).status == "limited"
        assert advanced == [4]
    finally:
        handle.close(30)


def test_source_error_after_accepted_process_item_keeps_ordered_prefix() -> None:
    def source(_context):
        yield 4
        raise ValueError("source failed after process yield")

    handle = _start(source)
    try:
        assert handle.next(30).value[0] == 13
        with pytest.raises(ValueError, match="source failed after process yield"):
            handle.next(30)
        with pytest.raises(ValueError, match="source failed after process yield"):
            handle.completion(30)
    finally:
        handle.close(30)


def test_spawned_fork_join_output_is_ready_before_producer_eof() -> None:
    release_source = Event()
    source_eof = Event()

    def source(context):
        yield 4
        release_source.wait(20)
        context.cancellation.raise_if_cancelled()
        source_eof.set()

    handle = _start(source)
    try:
        item = handle.next(30)
        value, pids = item.value
        assert item.sequence == 0
        assert value == 13  # Independent arithmetic oracle: 2*4 + (4+1).
        assert all(pid != os.getpid() for pid in pids)
        assert not source_eof.is_set()
        release_source.set()
        assert handle.completion(30).status == "succeeded"
    finally:
        release_source.set()
        handle.close(30)


def test_process_dag_failure_keeps_prefix_and_actual_worker_observations() -> None:
    def source(_context):
        yield 1
        yield 2

    registry = TaskRegistry({"left": fail_on_two, "right": right, "join": join})
    handle = _start(source, registry)
    try:
        assert handle.next(30).value[0] == 4
        with pytest.raises(ProcessStreamGraphExecutionError) as raised:
            handle.next(30)
        error = raised.value
        assert error.sequence == 1
        assert error.failed_nodes == ("left",)
        assert error.process_result.execution.status == "failed"
        assert error.process_result.workers
        assert all(worker.pid != os.getpid() for worker in error.process_result.workers)
        with pytest.raises(ProcessStreamGraphExecutionError):
            handle.completion(30)
    finally:
        handle.close(30)


def test_out_of_order_process_item_dags_publish_in_source_order(tmp_path: Path) -> None:
    def source(_context):
        yield 1
        yield 2

    registry = TaskRegistry({"left": BlockFirst(str(tmp_path)), "right": right, "join": join})
    handle = _start(source, registry, config=StreamMapConfig(2, 2))
    try:
        _await_file(tmp_path / "entered-2")
        with pytest.raises(TimeoutError, match="not ready"):
            handle.next(0.01)
        (tmp_path / "release-first").write_text("go", encoding="ascii")
        assert [(item.sequence, item.value[0]) for item in (handle.next(30), handle.next(30))] == [
            (0, 4),
            (1, 7),
        ]
        assert handle.completion(30).status == "succeeded"
    finally:
        (tmp_path / "release-first").write_text("go", encoding="ascii")
        handle.close(30)


def test_child_exit_is_not_replayed_and_prior_item_is_preserved() -> None:
    def source(_context):
        yield 1
        yield 2

    registry = TaskRegistry({"left": die_on_two, "right": right, "join": join})
    handle = _start(source, registry)
    try:
        assert handle.next(30).value[0] == 4
        with pytest.raises(ProcessStreamGraphInfrastructureError) as raised:
            handle.next(30)
        assert raised.value.sequence == 1
        assert raised.value.process_result is not None
        assert raised.value.cause is None
        assert any(
            attempt.error_type and attempt.error_message
            for task in raised.value.process_result.execution.tasks
            for attempt in task.attempts
            if attempt.status == "failed"
        )
        assert any(
            attempt.timing_source == "driver" for attempt in raised.value.process_result.attempts
        )
        with pytest.raises(ProcessStreamGraphInfrastructureError):
            handle.completion(30)
    finally:
        handle.close(30)


def test_cancel_running_child_joins_owner_and_observes_cooperative_eof(tmp_path: Path) -> None:
    entered = tmp_path / "entered"
    observed = tmp_path / "observed"
    before = {child.pid for child in active_children()}

    def source(_context):
        yield 1

    registry = TaskRegistry(
        {"left": StopTask(str(entered), str(observed)), "right": right, "join": join}
    )
    handle = _start(
        source,
        registry,
        process_config=ProcessTaskConfig(cancellation_grace_seconds=5),
    )
    try:
        _await_file(entered)
        assert handle.cancel()
        handle.close(30)
        assert handle.closed
        _await_file(observed)
        assert {child.pid for child in active_children()} <= before
    finally:
        handle.close(30)
