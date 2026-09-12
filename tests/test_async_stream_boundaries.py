"""Stream integration at deadlines, loop lifetime and producer ownership boundaries."""

import asyncio
import gc
import json
import subprocess
import sys
import weakref
from pathlib import Path
from threading import Event, Thread
from threading import enumerate as threads

import pytest

from graph_sail import (
    ActorRemoteError,
    AsyncSourceError,
    ProcessStreamConfig,
    TaskStream,
    TaskStreamConfig,
    ValidationError,
    start_process_task_stream,
    start_task_stream,
)
from tests.process_stream_functions import cooperative, failing


@pytest.fixture(autouse=True)
def no_owned_threads_left():
    before = {thread.ident for thread in threads() if thread.name.startswith("graph-sail")}
    yield
    assert {thread.ident for thread in threads() if thread.name.startswith("graph-sail")} == before


@pytest.mark.parametrize("name", ["next_async", "wait_ready_async", "completion_async"])
@pytest.mark.parametrize(
    "timeout", [True, False, -1, 86_401, float("nan"), float("inf"), 10**400, "1"]
)
def test_invalid_timeout_precedes_ready_observation_and_consumption(name, timeout):
    def produce(context):
        yield "untouched"

    with start_task_stream(produce) as stream:
        stream.completion(5)
        with pytest.raises(ValidationError):
            asyncio.run(getattr(stream, name)(timeout))
        assert stream._completion_hub.waiter_count == 0
        assert not stream._stop.is_set()
        assert stream.next(0).value == "untouched"


def test_producer_cannot_await_its_own_stream_even_when_immediately_ready():
    gate = Event()
    holder = []

    async def attempt():
        stream = holder[0]
        for name in ("next_async", "wait_ready_async", "completion_async"):
            with pytest.raises(RuntimeError, match="own stream"):
                await getattr(stream, name)(0)
        assert stream._completion_hub.waiter_count == 0

    def produce(context):
        assert gate.wait(5)
        yield 31
        asyncio.run(attempt())

    stream = start_task_stream(produce)
    try:
        holder.append(stream)
        gate.set()
        stream.completion(5)
        assert asyncio.run(stream.next_async(5)).value == 31
        assert asyncio.run(stream.completion_async(5)).status == "succeeded"
    finally:
        gate.set()
        stream.close(5)


@pytest.mark.parametrize("name", ["next_async", "wait_ready_async", "completion_async"])
def test_real_publication_at_expiring_deadline_is_observed(name, monkeypatch):
    gate, published = Event(), Event()

    def produce(context):
        assert gate.wait(5)
        yield 42

    stream = start_task_stream(produce)
    original_notify = stream._completion_hub.notify
    original_timeout = asyncio.timeout_at

    def observed_notify(*, terminal=False):
        original_notify(terminal=terminal)
        if terminal:
            published.set()

    class PublishOnTimeoutExit:
        async def __aenter__(self):
            self.context = original_timeout(asyncio.get_running_loop().time())
            return await self.context.__aenter__()

        async def __aexit__(self, *args):
            # Deterministic test-only gate: producer publishes after the private
            # Future was cancelled for timeout but before the final snapshot.
            gate.set()
            assert published.wait(5)
            return await self.context.__aexit__(*args)

    monkeypatch.setattr(stream._completion_hub, "notify", observed_notify)
    monkeypatch.setattr(asyncio, "timeout_at", lambda _: PublishOnTimeoutExit())
    try:
        result = asyncio.run(getattr(stream, name)(1))
        if name == "next_async":
            assert (result.sequence, result.value) == (0, 42)
        else:
            assert result is True if name == "wait_ready_async" else result.produced == 1
            assert stream.next(0).value == 42
        assert stream._completion_hub.waiter_count == 0
        assert not stream._stop.is_set()
    finally:
        gate.set()
        stream.close(5)


def test_cancel_and_deadline_churn_owns_no_tasks_threads_executor_or_values():
    gate = Event()
    value = []

    def produce(context):
        assert gate.wait(10)
        yield value

    stream = start_task_stream(produce)

    async def exercise():
        loop = asyncio.get_running_loop()
        current = asyncio.current_task()
        methods = [stream.next_async, stream.wait_ready_async, stream.completion_async]
        before = {thread.ident for thread in threads()}
        for index in range(90):
            operation = methods[index % 3]
            if index % 3 == 1:
                assert await operation(0.001) is False
            else:
                with pytest.raises(TimeoutError):
                    await operation(0.001)
        references = []
        for index in range(300):
            pending = asyncio.create_task(methods[index % 3]())
            references.append(weakref.ref(pending))
            await asyncio.sleep(0)
            pending.cancel("consumer only")
            with pytest.raises(asyncio.CancelledError, match="consumer only"):
                await pending
            assert stream._completion_hub.waiter_count == stream._completion_hub.loop_count == 0
        del pending
        await asyncio.sleep(0)
        gc.collect()
        assert all(reference() is None for reference in references)
        assert asyncio.all_tasks() == {current} and loop._default_executor is None
        assert {thread.ident for thread in threads()} == before
        assert not stream._stop.is_set()
        gate.set()
        assert (await stream.next_async(5)).value is value

    try:
        asyncio.run(exercise())
    finally:
        gate.set()
        stream.close(5)


def test_closed_real_loop_is_pruned_after_nonterminal_publication_without_losing_item(monkeypatch):
    gate, notified, second = Event(), Event(), Event()

    def produce(context):
        assert gate.wait(5)
        yield 29
        assert second.wait(5)
        yield 30

    stream = start_task_stream(produce, config=TaskStreamConfig(max_buffered=1))
    loop = asyncio.new_event_loop()
    hub = stream._completion_hub
    notify = hub.notify

    def observed_notify(*, terminal=False):
        notify(terminal=terminal)
        notified.set()

    monkeypatch.setattr(hub, "notify", observed_notify)
    try:
        lease = hub.acquire(loop)
        future = loop.create_future()
        assert lease.arm(hub.epoch, future)
        gate.set()
        assert stream.wait_ready(5)
        assert notified.wait(5)
        assert not stream.done() and hub.pending_pumps == 1
        loop.close()
        assert hub.waiter_count == hub.loop_count == 0
        lease.close()
        future.cancel()
        reference = weakref.ref(loop)
        del future
        loop = None
        gc.collect()
        assert reference() is None
        assert not stream._stop.is_set()
        assert asyncio.run(stream.next_async(5)).value == 29
        second.set()
        assert asyncio.run(stream.next_async(5)).value == 30
        assert asyncio.run(stream.completion_async(5)).produced == 2
    finally:
        gate.set()
        second.set()
        if loop is not None and not loop.is_closed():
            loop.close()
        stream.close(5)


def test_invalid_settled_state_and_snapshot_failure_are_not_fabricated_success(monkeypatch):
    def produce(context):
        yield from ()

    unstarted = TaskStream(produce, TaskStreamConfig())
    unstarted._finished = True
    with pytest.raises(RuntimeError, match="no result"):
        asyncio.run(unstarted.completion_async(0))
    assert unstarted._completion_hub.waiter_count == 0

    with start_task_stream(produce) as stream:
        stream.completion(5)

        def snapshot_failed():
            raise MemoryError("injected snapshot allocation failure")

        monkeypatch.setattr(stream, "_next_snapshot", snapshot_failed)
        with pytest.raises(MemoryError, match="snapshot allocation"):
            asyncio.run(stream.next_async())
        assert stream._completion_hub.waiter_count == 0


def test_snapshot_allocation_failure_does_not_remove_accepted_item(monkeypatch):
    import graph_sail.task_streams as implementation

    def produce(context):
        yield 99

    with start_task_stream(produce) as stream:
        stream.completion(5)

        def no_snapshot(*args, **kwargs):
            raise MemoryError("injected item snapshot allocation failure")

        monkeypatch.setattr(implementation, "_Snapshot", no_snapshot)
        with pytest.raises(MemoryError, match="item snapshot allocation"):
            asyncio.run(stream.next_async(0))
        assert stream._completion_hub.waiter_count == 0
        assert not stream._stop.is_set()
        assert stream.next(0).value == 99


def test_available_items_and_eof_never_subscribe_or_copy_borrowed_values(monkeypatch):
    value = []

    def produce(context):
        yield value
        value.append("later producer mutation")
        yield value

    with start_task_stream(produce) as stream:
        stream.completion(5)

        def forbidden(*args):
            raise AssertionError("immediate observation subscribed")

        monkeypatch.setattr(stream._completion_hub, "acquire", forbidden)

        async def exercise():
            assert aiter(stream) is stream
            assert await stream.wait_ready_async(0)
            assert (await stream.completion_async(0)).produced == 2
            items = [item async for item in stream]
            assert [item.sequence for item in items] == [0, 1]
            assert all(item.value is value for item in items)
            assert value == ["later producer mutation"]
            assert await stream.wait_ready_async(0)
            with pytest.raises(StopAsyncIteration):
                await stream.next_async(0)

        asyncio.run(exercise())


def test_pending_cancellation_before_call_cannot_remove_already_queued_item():
    def produce(context):
        yield 7

    with start_task_stream(produce) as stream:
        stream.completion(5)

        async def exercise():
            async def cancelled_consumer():
                asyncio.current_task().cancel("before dequeue")
                return await stream.next_async()

            waiting = asyncio.create_task(cancelled_consumer())
            with pytest.raises(asyncio.CancelledError, match="before dequeue"):
                await waiting
            assert stream._completion_hub.waiter_count == 0
            assert not stream._stop.is_set()
            assert stream.next(0).value == 7

        asyncio.run(exercise())


def test_caught_prior_cancellation_does_not_poison_later_valid_consumption():
    def produce(context):
        yield 8

    with start_task_stream(produce) as stream:
        stream.completion(5)

        async def exercise():
            task = asyncio.current_task()
            task.cancel("already caught")
            with pytest.raises(asyncio.CancelledError, match="already caught"):
                await asyncio.sleep(0)
            # asyncio's count is not a statement that another cancellation must
            # still be delivered. Do not use Task.cancelling() as a rejection flag.
            assert task.cancelling() == 1
            assert (await stream.next_async(0)).value == 8

        asyncio.run(exercise())


def test_real_process_waiter_cancellation_timeout_and_validation_leave_worker_owned(
    tmp_path, monkeypatch
):
    marker = tmp_path / "cooperative.txt"
    stream = start_process_task_stream(
        cooperative,
        args=(str(marker),),
        config=TaskStreamConfig(max_buffered=1),
        process_config=ProcessStreamConfig(startup_timeout_seconds=20),
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("process async wait used a blocking wrapper or executor")

    async def exercise():
        loop = asyncio.get_running_loop()
        monkeypatch.setattr(type(loop), "run_in_executor", forbidden)
        monkeypatch.setattr(asyncio, "to_thread", forbidden)
        monkeypatch.setattr(Thread, "start", forbidden)
        for name in ("next", "wait_ready", "completion"):
            monkeypatch.setattr(stream, name, forbidden)
            monkeypatch.setattr(stream._stream, name, forbidden)
        for name in ("next_async", "wait_ready_async", "completion_async"):
            for timeout in (True, -1, float("nan"), float("inf"), "1"):
                with pytest.raises(ValidationError):
                    await getattr(stream, name)(timeout)
        assert await stream.wait_ready_async(10)
        item = await stream.next_async(0)
        assert item.value == stream.worker.pid
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(stream.next_async(), 0.01)
        assert await stream.wait_ready_async(0.01) is False
        pending = asyncio.create_task(stream.completion_async())
        await asyncio.sleep(0)
        pending.cancel("only the process waiter")
        with pytest.raises(asyncio.CancelledError, match="only the process waiter"):
            await pending
        assert not stream.worker.cancellation_requested
        assert not stream._stream._stop.is_set() and not stream.done()
        assert stream._stream._completion_hub.waiter_count == 0
        assert stream.cancel()
        result = await stream.completion_async(10)
        assert (result.status, result.produced) == ("cancelled", 1)
        assert result.worker.cancellation_requested
        assert result.worker.generator_closed and result.worker.resources_closed
        assert marker.read_text(encoding="ascii") == "finally"
        with pytest.raises(StopAsyncIteration):
            await anext(stream)

    try:
        asyncio.run(exercise())
    finally:
        stream.close(10)
    assert stream.closed


def test_real_process_failure_is_wrapped_without_losing_prefix_or_cleanup():
    with start_process_task_stream(failing) as stream:

        async def exercise():
            with pytest.raises(AsyncSourceError) as completion:
                await stream.completion_async(10)
            source = completion.value.__cause__
            assert isinstance(source, ActorRemoteError)
            trace = source.__traceback__
            assert (await stream.next_async(0)).value is None
            for _ in range(3):
                with pytest.raises(AsyncSourceError) as later:
                    await anext(stream)
                assert later.value is not completion.value
                assert later.value.__cause__ is source and source.__traceback__ is trace
            assert stream.worker.generator_closed and stream.worker.resources_closed
            assert stream._stream._completion_hub.waiter_count == 0

        asyncio.run(exercise())


def test_offline_example_runs_real_thread_and_process_consumers_together():
    example = Path(__file__).resolve().parents[1] / "examples" / "async_task_streams.py"
    result = subprocess.run(
        [sys.executable, str(example)],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert not result.stderr
    assert json.loads(result.stdout) == {
        "thread": [0, 1, 4, 9],
        "process": [0, 1, 4, 9],
        "owners_closed": True,
    }
