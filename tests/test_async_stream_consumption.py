"""Independent async-consumption contracts over the existing single mailbox."""

import asyncio
import traceback
from threading import Barrier, Event, Lock, Thread

import pytest

from graph_sail import (
    AsyncSourceControlError,
    AsyncSourceError,
    AsyncWaitLimitError,
    ProcessStreamConfig,
    ProcessTaskStream,
    TaskStream,
    TaskStreamConfig,
    start_process_task_stream,
    start_task_stream,
)
from tests.process_stream_functions import counted, snapshots


@pytest.mark.parametrize("stream_type", [TaskStream, ProcessTaskStream])
@pytest.mark.parametrize(
    "name", ["__aiter__", "__anext__", "next_async", "wait_ready_async", "completion_async"]
)
def test_public_async_consumption_api_exists(stream_type, name):
    assert callable(getattr(stream_type, name, None)), f"{stream_type.__name__}.{name} missing"


def test_async_consumer_progress_uses_no_blocking_methods_executor_or_extra_thread(monkeypatch):
    gate = Event()

    def produce(context):
        assert gate.wait(5)
        yield 49

    stream = start_task_stream(produce)

    def forbidden(*args, **kwargs):
        raise AssertionError("async consumption attempted a blocking delegation")

    async def exercise():
        loop = asyncio.get_running_loop()
        monkeypatch.setattr(type(loop), "run_in_executor", forbidden)
        monkeypatch.setattr(asyncio, "to_thread", forbidden)
        monkeypatch.setattr(Thread, "start", forbidden)
        for name in ("next", "wait_ready", "completion"):
            monkeypatch.setattr(stream, name, forbidden)
        pending = asyncio.create_task(stream.next_async())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert stream._completion_hub.waiter_count == 1 and not pending.done()
        # Independent loop work progresses while the real producer is blocked.
        for _ in range(20):
            await asyncio.sleep(0)
        gate.set()
        item = await asyncio.wait_for(pending, 5)
        assert (item.sequence, item.value) == (0, 49)
        result = await stream.completion_async(5)
        assert (result.status, result.produced) == ("succeeded", 1)
        with pytest.raises(StopAsyncIteration):
            await anext(stream)
        assert stream._completion_hub.waiter_count == 0

    try:
        asyncio.run(exercise())
    finally:
        gate.set()
        stream.close(5)


def test_timeout_and_cancelled_wait_do_not_consume_or_cancel_producer():
    gate = Event()

    def produce(context):
        assert gate.wait(5)
        yield 17

    stream = start_task_stream(produce)

    async def exercise():
        assert await stream.wait_ready_async(0) is False
        with pytest.raises(TimeoutError):
            await stream.next_async(0)
        with pytest.raises(TimeoutError):
            await stream.completion_async(0)
        pending = asyncio.create_task(stream.next_async())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert stream._completion_hub.waiter_count == 1
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert not stream._stop.is_set() and stream._completion_hub.waiter_count == 0
        gate.set()
        assert await stream.wait_ready_async(5) is True
        item = await stream.next_async(5)
        assert (item.sequence, item.value) == (0, 17)
        assert (await stream.completion_async(5)).produced == 1

    try:
        asyncio.run(exercise())
    finally:
        gate.set()
        stream.close(5)


def test_readiness_and_completion_do_not_drain_or_break_backpressure():
    entered_second = Event()

    def produce(context):
        yield 0
        entered_second.set()
        yield 1

    stream = start_task_stream(produce, config=TaskStreamConfig(max_buffered=1, max_yields=2))

    async def exercise():
        assert await stream.wait_ready_async(5)
        assert not entered_second.is_set()
        assert await stream.wait_ready_async(0)
        with pytest.raises(TimeoutError):
            await stream.completion_async(0)
        assert not entered_second.is_set()
        assert (await stream.next_async(0)).value == 0
        result = await stream.completion_async(5)
        assert (result.status, result.produced) == ("limited", 2)
        assert [(item.sequence, item.value) async for item in stream] == [(1, 1)]

    try:
        asyncio.run(exercise())
    finally:
        stream.close(5)


@pytest.mark.parametrize(
    "error",
    [
        ValueError("expected"),
        KeyboardInterrupt("expected"),
        SystemExit(7),
        asyncio.CancelledError("source cancellation"),
    ],
)
def test_async_failure_wrappers_are_fresh_and_preserve_accepted_prefix(error):
    def produce(context):
        yield "prefix"
        raise error

    stream = start_task_stream(produce)
    wrapper = AsyncSourceError if isinstance(error, Exception) else AsyncSourceControlError

    async def exercise():
        with pytest.raises(wrapper) as first:
            await stream.completion_async(5)
        original_trace = traceback.extract_tb(error.__traceback__)
        assert first.value.__cause__ is error
        assert await stream.wait_ready_async(0)
        assert (await stream.next_async(0)).value == "prefix"
        for _ in range(3):
            with pytest.raises(wrapper) as later:
                await stream.next_async(0)
            assert later.value is not first.value and later.value.__cause__ is error
            assert traceback.extract_tb(error.__traceback__) == original_trace

    try:
        asyncio.run(exercise())
    finally:
        stream.close(5)


def test_mixed_async_method_waiters_share_one_256_lease_budget():
    gate = Event()

    def produce(context):
        assert gate.wait(10)
        yield 1

    stream = start_task_stream(produce)

    async def exercise():
        methods = [stream.next_async, stream.wait_ready_async, stream.completion_async]
        pending = [asyncio.create_task(methods[index % 3]()) for index in range(256)]
        await asyncio.sleep(0)
        await asyncio.sleep(0)  # Item waiters pass their pre-consumption checkpoint.
        assert stream._completion_hub.waiter_count == 256
        with pytest.raises(AsyncWaitLimitError):
            await stream.next_async()
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        assert stream._completion_hub.waiter_count == 0
        assert not stream._stop.is_set()

    try:
        asyncio.run(exercise())
    finally:
        gate.set()
        stream.close(5)


def test_close_notifies_discard_once_but_does_not_pretend_live_producer_settled():
    gate = Event()
    started = Event()

    def produce(context):
        started.set()
        assert gate.wait(10)
        yield 3

    stream = start_task_stream(produce)
    assert started.wait(5)

    async def exercise():
        waiting = asyncio.create_task(stream.next_async())
        completion = asyncio.create_task(stream.completion_async())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert stream._completion_hub.waiter_count == 2
        before = stream._completion_hub.epoch
        for _ in range(3):
            with pytest.raises(TimeoutError):
                stream.close(0)
        assert stream._completion_hub.epoch == before + 1
        with pytest.raises(StopAsyncIteration):
            await waiting
        assert not completion.done() and not stream.done()
        gate.set()
        assert (await asyncio.wait_for(completion, 5)).status == "cancelled"

    try:
        asyncio.run(exercise())
    finally:
        gate.set()
        stream.close(5)


@pytest.mark.parametrize("mode", ["counted", "snapshots"])
def test_real_process_async_consumption_preserves_caps_snapshots_and_native_cleanup(tmp_path, mode):
    marker = tmp_path / "advances.txt"
    stream = start_process_task_stream(
        counted if mode == "counted" else snapshots,
        args=(str(marker), 8) if mode == "counted" else (),
        config=TaskStreamConfig(max_buffered=1, max_yields=3),
        process_config=ProcessStreamConfig(startup_timeout_seconds=20),
    )

    async def exercise():
        assert await stream.wait_ready_async(10)
        if mode == "counted":
            assert marker.read_text().splitlines() == ["next:0"]
        values = [item async for item in stream]
        assert [item.sequence for item in values] == [0, 1, 2]
        if mode == "counted":
            assert [item.value for item in values] == [(stream.worker.pid, i) for i in range(3)]
            assert marker.read_text().splitlines() == ["next:0", "next:1", "next:2", "finally"]
        else:
            assert [item.value for item in values] == [[0], [0, 1], [0, 1, 2]]
            assert len({id(item.value) for item in values}) == 3
        result = await stream.completion_async(5)
        assert result.status == "limited" and result.produced == 3
        assert result.worker.resources_closed and result.worker.generator_closed
        assert stream._stream._completion_hub.waiter_count == 0

    try:
        asyncio.run(exercise())
    finally:
        stream.close(10)


def test_cancel_after_notification_before_resumption_leaves_item_for_another_reader(monkeypatch):
    gate = Event()

    def produce(context):
        assert gate.wait(5)
        yield 61

    stream = start_task_stream(produce, config=TaskStreamConfig(max_buffered=1))

    async def exercise():
        pending = asyncio.create_task(stream.next_async())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert stream._completion_hub.waiter_count == 1
        deliver = stream._completion_hub.deliver

        def deliver_then_cancel(slot):
            deliver(slot)
            # Future resolution has posted the task's resumption, not executed it.
            pending.cancel()

        monkeypatch.setattr(stream._completion_hub, "deliver", deliver_then_cancel)
        gate.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(pending, 5)
        assert not stream._stop.is_set()
        item = stream.next(0)
        assert (item.sequence, item.value) == (0, 61)
        assert stream._completion_hub.waiter_count == 0

    try:
        asyncio.run(exercise())
    finally:
        gate.set()
        stream.close(5)


def test_publication_between_snapshot_and_subscription_cannot_lose_wakeup(monkeypatch):
    gate = Event()
    published = Event()

    def produce(context):
        assert gate.wait(5)
        yield 73

    stream = start_task_stream(produce, config=TaskStreamConfig(max_buffered=1))
    hub = stream._completion_hub
    acquire, notify = hub.acquire, hub.notify

    def publish_before_acquire(loop):
        # A test-only event gate forces publication into the subscription gap.
        gate.set()
        assert published.wait(5)
        return acquire(loop)

    def observed_notify(*, terminal=False):
        notify(terminal=terminal)
        published.set()

    monkeypatch.setattr(hub, "acquire", publish_before_acquire)
    monkeypatch.setattr(hub, "notify", observed_notify)

    async def exercise():
        item = await stream.next_async(5)
        assert (item.sequence, item.value) == (0, 73)
        assert hub.waiter_count == 0

    try:
        asyncio.run(exercise())
    finally:
        gate.set()
        stream.close(5)


def test_notified_item_stolen_by_sync_reader_does_not_reserve_or_end_async_wait(monkeypatch):
    first = Event()
    second = Event()
    stolen = []

    def produce(context):
        assert first.wait(5)
        yield "first"
        assert second.wait(5)
        yield "second"

    stream = start_task_stream(produce, config=TaskStreamConfig(max_buffered=1))
    deliver = stream._completion_hub.deliver

    def deliver_then_steal(slot):
        deliver(slot)
        if not stolen:
            stolen.append(stream.next(0))

    monkeypatch.setattr(stream._completion_hub, "deliver", deliver_then_steal)

    async def exercise():
        pending = asyncio.create_task(stream.next_async())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert stream._completion_hub.waiter_count == 1
        first.set()
        # Event-loop turns are test scheduling only, never production polling.
        for _ in range(10000):
            if stolen:
                break
            await asyncio.sleep(0)
        assert [item.value for item in stolen] == ["first"]
        await asyncio.sleep(0)
        assert not pending.done() and stream._completion_hub.waiter_count == 1
        second.set()
        item = await asyncio.wait_for(pending, 5)
        assert (item.sequence, item.value) == (1, "second")

    try:
        asyncio.run(exercise())
    finally:
        first.set()
        second.set()
        stream.close(5)


def test_two_real_event_loops_and_sync_reader_consume_each_sequence_exactly_once():
    start = Barrier(4)
    gate = Event()
    lock = Lock()
    collected = []
    failures = []

    def produce(context):
        assert gate.wait(5)
        for index in range(300):
            yield index * index

    stream = start_task_stream(produce, config=TaskStreamConfig(max_buffered=4))

    async def consume_async():
        return [(item.sequence, item.value) async for item in stream]

    def reader(async_reader):
        try:
            start.wait(5)
            rows = (
                asyncio.run(consume_async())
                if async_reader
                else [(item.sequence, item.value) for item in stream]
            )
            with lock:
                collected.extend(rows)
        except BaseException as error:
            with lock:
                failures.append(error)

    readers = [Thread(target=reader, args=(mode,)) for mode in (True, True, False)]
    try:
        for thread in readers:
            thread.start()
        start.wait(5)
        gate.set()
        for thread in readers:
            thread.join(10)
        assert all(not thread.is_alive() for thread in readers)
        assert not failures
        assert sorted(collected) == [(index, index * index) for index in range(300)]
        assert stream._completion_hub.waiter_count == 0
    finally:
        gate.set()
        stream.close(5)
        for thread in readers:
            if thread.ident is not None:
                thread.join(5)
