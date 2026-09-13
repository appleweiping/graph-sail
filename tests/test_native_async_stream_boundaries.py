"""Fault, admission, cancellation and ownership boundaries for native producers."""

import asyncio
import gc
import json
import subprocess
import sys
import weakref
from pathlib import Path

import pytest

import graph_sail.async_task_streams as implementation
from graph_sail import (
    AsyncSourceControlError,
    AsyncSourceError,
    AsyncTaskStreamOwnershipControlError,
    AsyncTaskStreamOwnershipError,
    AsyncWaitLimitError,
    TaskStreamConfig,
    ValidationError,
    start_async_task_stream,
)


@pytest.fixture(autouse=True)
def no_active_native_owners():
    before = set(implementation._ACTIVE_OWNERS)
    yield
    assert before == implementation._ACTIVE_OWNERS


@pytest.mark.parametrize("phase", ["before_adoption", "after_adoption", "callback_registration"])
@pytest.mark.parametrize(
    "problem", [MemoryError("birth"), KeyboardInterrupt("birth"), asyncio.CancelledError("birth")]
)
def test_startup_failure_retains_owner_and_aborts_without_invoking_source(
    phase, problem, monkeypatch
):
    async def exercise():
        calls = []
        original = implementation._Task

        class CallbackFailure(original):
            def add_done_callback(self, callback, *, context=None):
                raise problem

        def constructor(coroutine, **options):
            if phase == "before_adoption":
                raise problem
            task = (CallbackFailure if phase == "callback_registration" else original)(
                coroutine,
                **options,
            )
            if phase == "after_adoption":
                raise problem
            return task

        async def source(context):
            calls.append(True)
            yield 1

        monkeypatch.setattr(implementation, "_Task", constructor)
        wrapper = (
            AsyncTaskStreamOwnershipError
            if isinstance(problem, Exception)
            else AsyncTaskStreamOwnershipControlError
        )
        with pytest.raises(wrapper) as failure:
            start_async_task_stream(source)
        stream = failure.value.stream
        assert failure.value.phase == "startup" and failure.value.__cause__ is problem
        assert not stream.closed and stream in implementation._ACTIVE_OWNERS
        await stream.aclose(1)
        assert stream.done() and stream.closed and stream._owned is None and not calls
        assert stream._task is None or stream._task.done()
        assert asyncio.all_tasks() == {asyncio.current_task()}
        with pytest.raises(
            AsyncSourceError if isinstance(problem, Exception) else AsyncSourceControlError
        ) as source_failure:
            await stream.completion_async(0)
        assert source_failure.value.__cause__ is problem
        # Both real callbacks can acknowledge; only one terminal publication.
        epoch = stream._completion_hub.epoch
        stream._acknowledge_abort()
        if stream._task is not None:
            stream._on_done(stream._task)
        assert stream._completion_hub.epoch == epoch

    asyncio.run(exercise())


def test_external_task_cancel_before_entry_is_observed_and_owner_settles():
    async def exercise():
        calls = []

        async def source(context):
            calls.append(True)
            yield 1

        stream = start_async_task_stream(source)
        # Test-only introspection models application/loop shutdown interference.
        # The normal public cancellation API never does this before first entry.
        stream._task.cancel("external before entry")
        with pytest.raises(AsyncSourceControlError) as observed:
            await stream.completion_async(1)
        assert isinstance(observed.value.__cause__, asyncio.CancelledError)
        assert not calls and not stream._entered and stream.done()
        await stream.aclose(1)
        assert stream.closed

    asyncio.run(exercise())


def test_function_code_replacement_is_rejected_before_running_new_body():
    async def exercise():
        calls = []

        async def source(context):
            yield calls

        def replacement(context):
            calls.append("not a native generator now")
            return iter(())

        stream = start_async_task_stream(source)
        source.__code__ = replacement.__code__
        with pytest.raises(AsyncSourceError) as observed:
            await stream.completion_async(1)
        assert isinstance(observed.value.__cause__, ValidationError)
        assert not calls and stream._owned is None
        await stream.aclose(1)

    asyncio.run(exercise())


@pytest.mark.parametrize("name", ["next_async", "wait_ready_async", "completion_async", "aclose"])
@pytest.mark.parametrize(
    "timeout", [True, False, -1, 86_401, float("inf"), float("nan"), 10**400, "1"]
)
def test_invalid_timeout_precedes_observation_discard_and_subscription(name, timeout):
    async def exercise():
        async def source(context):
            yield "prefix"

        stream = start_async_task_stream(source)
        await stream.completion_async(1)
        with pytest.raises(ValidationError):
            await getattr(stream, name)(timeout)
        assert stream._completion_hub.waiter_count == 0
        assert not stream._discard and not stream._stop
        assert (await stream.next_async(0)).value == "prefix"
        await stream.aclose(1)

    asyncio.run(exercise())


@pytest.mark.parametrize("name", ["next_async", "aclose"])
def test_already_pending_caller_cancel_precedes_consume_or_close_mutation(name):
    async def exercise():
        async def source(context):
            yield "prefix"
            await asyncio.get_running_loop().create_future()

        stream = start_async_task_stream(source)
        assert await stream.wait_ready_async(1)

        async def operation():
            asyncio.current_task().cancel("before mutation")
            await getattr(stream, name)()

        waiter = asyncio.create_task(operation())
        with pytest.raises(asyncio.CancelledError, match="before mutation"):
            await waiter
        assert not stream._stop and not stream._discard
        assert stream._completion_hub.waiter_count == 0 and stream._task.cancelling() == 0
        assert (await stream.next_async(0)).value == "prefix"
        await stream.aclose(1)

    asyncio.run(exercise())


def test_caught_nonzero_cancellation_count_does_not_poison_read_or_close():
    async def exercise():
        async def source(context):
            yield "prefix"

        stream = start_async_task_stream(source)
        await stream.completion_async(1)
        current = asyncio.current_task()
        current.cancel("already delivered")
        with pytest.raises(asyncio.CancelledError):
            await asyncio.sleep(0)
        assert current.cancelling() == 1
        assert (await stream.next_async(0)).value == "prefix"
        await stream.aclose(1)
        assert stream.closed

    asyncio.run(exercise())


def test_256_mixed_waiters_bound_close_wait_after_committed_stop_and_discard():
    async def exercise():
        started, cleanup_gate = asyncio.Event(), asyncio.Event()

        async def source(context):
            started.set()
            try:
                await asyncio.get_running_loop().create_future()
                yield 1
            finally:
                await cleanup_gate.wait()

        stream = start_async_task_stream(source)
        await started.wait()
        methods = [stream.next_async, stream.wait_ready_async, stream.completion_async]
        waiting = [asyncio.create_task(methods[index % 3]()) for index in range(256)]
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert stream._completion_hub.waiter_count == 256
        with pytest.raises(AsyncWaitLimitError):
            await stream.aclose()
        assert stream._discard and stream._stop and not stream.closed
        assert stream._task.cancelling() == 1
        for task in waiting:
            task.cancel("release application waiter")
        await asyncio.gather(*waiting, return_exceptions=True)
        assert stream._completion_hub.waiter_count == 0
        cleanup_gate.set()
        await stream.aclose(1)
        assert stream.closed

    asyncio.run(exercise())


def test_256_close_waiters_share_bound_and_repeated_close_only_notifies_first_discard(monkeypatch):
    async def exercise():
        cleaning, release = asyncio.Event(), asyncio.Event()

        async def source(context):
            try:
                yield 1
            finally:
                cleaning.set()
                await release.wait()

        stream = start_async_task_stream(source, config=TaskStreamConfig(max_yields=1))
        await cleaning.wait()
        original = stream._completion_hub.notify
        notified = []

        def notify(*, terminal=False):
            notified.append(terminal)
            original(terminal=terminal)

        monkeypatch.setattr(stream._completion_hub, "notify", notify)
        waiting = [asyncio.create_task(stream.aclose()) for _ in range(256)]
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert stream._completion_hub.waiter_count == 256 and notified == [False]
        with pytest.raises(AsyncWaitLimitError):
            await stream.aclose()
        assert notified == [False] and stream._task.cancelling() == 0
        for task in waiting:
            task.cancel()
        await asyncio.gather(*waiting, return_exceptions=True)
        assert stream._completion_hub.waiter_count == 0 and not stream.closed
        release.set()
        await stream.aclose(1)
        assert stream.closed and notified == [False, True]

    asyncio.run(exercise())


def test_competing_consumers_receive_each_sequence_once():
    async def exercise():
        async def source(context):
            for index in range(100):
                yield index

        stream = start_async_task_stream(source, config=TaskStreamConfig(max_buffered=2))

        async def consume():
            return [(item.sequence, item.value) async for item in stream]

        batches = await asyncio.gather(*(consume() for _ in range(20)))
        assert sorted(item for batch in batches for item in batch) == [(i, i) for i in range(100)]
        assert (await stream.completion_async(1)).produced == 100
        await stream.aclose(1)

    asyncio.run(exercise())


def test_wait_cancellation_churn_leaves_no_subscription_or_helper_task():
    async def exercise():
        started = asyncio.Event()

        async def source(context):
            started.set()
            await asyncio.get_running_loop().create_future()
            yield 1

        stream = start_async_task_stream(source)
        await started.wait()
        methods = [stream.next_async, stream.wait_ready_async, stream.completion_async]
        references = []
        for index in range(60):
            waiting = asyncio.create_task(methods[index % 3]())
            references.append(weakref.ref(waiting))
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            waiting.cancel("only waiter")
            with pytest.raises(asyncio.CancelledError, match="only waiter"):
                await waiting
            assert stream._completion_hub.waiter_count == 0
            assert stream._completion_hub.loop_count == 0
        del waiting
        await asyncio.sleep(0)
        gc.collect()
        assert all(reference() is None for reference in references)
        assert asyncio.all_tasks() == {asyncio.current_task(), stream._task}
        assert asyncio.get_running_loop()._default_executor is None
        assert not stream._stop and stream._task.cancelling() == 0
        await stream.aclose(1)

    asyncio.run(exercise())


@pytest.mark.parametrize("boundary", ["_Snapshot", "capacity"])
def test_failed_observation_or_capacity_wake_cannot_remove_item(boundary, monkeypatch):
    async def exercise():
        advances = []

        async def source(context):
            for index in range(2):
                advances.append(index)
                yield index

        stream = start_async_task_stream(source, config=TaskStreamConfig(max_buffered=1))
        assert await stream.wait_ready_async(1)
        accepted = stream._queue[0]
        target = implementation if boundary == "_Snapshot" else stream
        name = "_Snapshot" if boundary == "_Snapshot" else "_wake_capacity"

        def failing(*args, **kwargs):
            raise MemoryError("observation boundary")

        with monkeypatch.context() as scoped:
            scoped.setattr(target, name, failing)
            with pytest.raises(MemoryError, match="observation boundary"):
                await stream.next_async(0)
        assert list(stream._queue) == [accepted] and stream._produced == 1 and advances == [0]
        assert (await stream.next_async(0)) is accepted
        assert (await stream.next_async(1)).value == 1
        await stream.aclose(1)

    asyncio.run(exercise())


@pytest.mark.parametrize("boundary", ["TaskStreamItem", "_next_count", "TaskStreamResult"])
def test_acceptance_and_result_allocation_failures_preserve_prefix_and_cleanup(
    boundary, monkeypatch
):
    async def exercise():
        cleaned, calls = [], []
        original = getattr(implementation, boundary)
        failure = MemoryError(boundary)

        def allocator(*args, **kwargs):
            calls.append(True)
            if boundary == "TaskStreamResult" or len(calls) == 2:
                raise failure
            return original(*args, **kwargs)

        async def source(context):
            try:
                for index in range(2):
                    yield index
            finally:
                cleaned.append(True)

        with monkeypatch.context() as scoped:
            scoped.setattr(implementation, boundary, allocator)
            stream = start_async_task_stream(source)
            with pytest.raises(AsyncSourceError) as observed:
                await stream.completion_async(1)
        assert observed.value.__cause__ is failure and cleaned == [True]
        count = 2 if boundary == "TaskStreamResult" else 1
        assert stream._produced == count and [item.sequence for item in stream._queue] == list(
            range(count)
        )
        assert [item.value for item in stream._queue] == list(range(count))
        await stream.aclose(1)
        assert stream.closed

    asyncio.run(exercise())


@pytest.mark.parametrize("terminal", [False, True])
def test_publication_notification_failure_never_rolls_back_accepted_prefix(terminal, monkeypatch):
    async def exercise():
        cleaned = []
        failure = MemoryError("notification")

        async def source(context):
            try:
                yield 1
                yield 2
            finally:
                cleaned.append(True)

        stream = start_async_task_stream(source)
        original = stream._completion_hub.notify
        injected = []

        def notify(*, terminal=False):
            if terminal == should_fail and not injected:
                injected.append(True)
                raise failure
            original(terminal=terminal)

        should_fail = terminal
        monkeypatch.setattr(stream._completion_hub, "notify", notify)
        # A callback failure cannot promise an existing waiter wakes. Observe
        # through the independently scheduled done callback, then a ready read.
        acknowledged = asyncio.get_running_loop().create_future()
        stream._task.add_done_callback(lambda _: acknowledged.set_result(None))
        await acknowledged
        assert stream.done() and cleaned == [True] and injected == [True]
        with pytest.raises(AsyncSourceError) as observed:
            await stream.completion_async(0)
        assert observed.value.__cause__ is failure
        expected = [1, 2] if terminal else [1]
        assert [item.value for item in stream._queue] == expected
        assert stream._produced == len(expected)
        await stream.aclose(1)

    asyncio.run(exercise())


def test_replacement_deque_failure_precedes_any_close_effect(monkeypatch):
    async def exercise():
        async def source(context):
            yield 1
            await asyncio.get_running_loop().create_future()

        stream = start_async_task_stream(source)
        assert await stream.wait_ready_async(1)
        accepted = stream._queue

        def failed_deque():
            raise MemoryError("close replacement")

        with monkeypatch.context() as scoped:
            scoped.setattr(implementation, "deque", failed_deque)
            with pytest.raises(MemoryError, match="close replacement"):
                await stream.aclose(0)
        assert stream._queue is accepted and not stream._stop and not stream._discard
        assert stream._task.cancelling() == 0
        await stream.aclose(1)

    asyncio.run(exercise())


@pytest.mark.parametrize("primary_control", [False, True])
def test_cleanup_control_priority_and_incomplete_control_ownership(primary_control, monkeypatch):
    async def exercise():
        primary = (
            KeyboardInterrupt("allocation control") if primary_control else MemoryError("item")
        )
        cleanup = SystemExit("cleanup control")

        async def source(context):
            try:
                yield 1
            finally:
                if primary_control:
                    yield "illegal cleanup yield"
                else:
                    raise cleanup

        def failed_item(*args):
            raise primary

        with monkeypatch.context() as scoped:
            scoped.setattr(implementation, "TaskStreamItem", failed_item)
            stream = start_async_task_stream(source)
            with pytest.raises(AsyncSourceControlError) as observed:
                await stream.completion_async(1)
        assert observed.value.__cause__ is (primary if primary_control else cleanup)
        assert stream._task.exception() is None and stream.done()
        if primary_control:
            with pytest.raises(AsyncTaskStreamOwnershipControlError) as ownership:
                await stream.aclose(1)
            assert ownership.value.stream is stream and ownership.value.phase == "cleanup"
            assert ownership.value.__cause__ is primary
            assert not stream.closed and stream.cleanup_incomplete
            with pytest.raises(GeneratorExit):
                await anext(stream._owned)  # Fixture-only malformed-generator release.
        else:
            await stream.aclose(1)
            assert stream.closed and not stream.cleanup_incomplete

    asyncio.run(exercise())


def test_discard_does_not_close_values_and_destructors_see_coherent_state():
    async def exercise():
        observations = []
        holder = []

        class Value:
            def close(self):
                raise AssertionError("borrowed value close invoked")

            def __del__(self):
                stream = holder[0]
                observations.append((stream._discard, stream._stop, len(stream._queue)))

        async def source(context):
            yield Value()

        stream = start_async_task_stream(source)
        holder.append(stream)
        await stream.completion_async(1)
        await stream.aclose(1)
        assert observations == [(True, True, 0)] and stream.closed

    asyncio.run(exercise())


@pytest.mark.parametrize("body", [None, ValueError("body"), KeyboardInterrupt("body")])
@pytest.mark.parametrize("closing", [ValueError("close"), SystemExit("close")])
def test_context_exit_retains_existing_primary_control_priority(body, closing, monkeypatch):
    async def exercise():
        async def source(context):
            yield 1

        stream = start_async_task_stream(source)
        await stream.aclose(1)

        async def failed_close(timeout=None):
            raise closing

        monkeypatch.setattr(stream, "aclose", failed_close)
        expected = (
            closing
            if body is None or (isinstance(body, Exception) and not isinstance(closing, Exception))
            else body
        )
        with pytest.raises(type(expected)) as observed:
            await stream.__aexit__(type(body) if body is not None else None, body, None)
        assert observed.value is expected and stream.closed

    asyncio.run(exercise())


def test_foreign_context_exit_rejects_before_touching_body_exception():
    holder = []

    async def source(context):
        yield 1

    async def create():
        stream = start_async_task_stream(source)
        holder.append(stream)
        await stream.aclose(1)

    async def foreign():
        problem = KeyboardInterrupt("body")
        with pytest.raises(RuntimeError, match="owner loop"):
            await holder[0].__aexit__(type(problem), problem, None)
        assert not hasattr(problem, "__notes__") and problem.__cause__ is None

    asyncio.run(create())
    asyncio.run(foreign())


def test_source_can_close_via_context_after_already_delivered_body_cancellation():
    async def exercise():
        async def source(context):
            yield 1
            await asyncio.get_running_loop().create_future()

        stream = start_async_task_stream(source)
        with pytest.raises(asyncio.CancelledError, match="body cancellation"):
            async with stream:
                assert (await stream.next_async(1)).value == 1
                asyncio.current_task().cancel("body cancellation")
                await asyncio.sleep(0)
        assert stream.closed and stream.done()

    asyncio.run(exercise())


@pytest.mark.parametrize("optimized", [False, True])
def test_offline_native_async_example_has_real_order_cap_cleanup_and_closed_owner(optimized):
    example = Path(__file__).resolve().parents[1] / "examples" / "native_async_task_streams.py"
    completed = subprocess.run(
        [sys.executable, "-I", "-B", *(["-O"] if optimized else []), str(example)],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert not completed.stderr
    assert json.loads(completed.stdout) == {
        "native_async": [0, 1, 4, 9],
        "status": "limited",
        "awaited_cleanup": True,
        "owner_closed": True,
    }


def test_acknowledged_close_is_idempotent_and_discarded_reads_are_eof():
    async def exercise():
        async def source(context):
            yield 1

        stream = start_async_task_stream(source)
        await stream.completion_async(1)
        await stream.aclose(1)
        epoch = stream._completion_hub.epoch
        await stream.aclose(0)
        with pytest.raises(StopAsyncIteration):
            await stream.next_async(0)
        assert await stream.wait_ready_async(0)
        assert stream.closed and stream._completion_hub.epoch == epoch
        assert (await stream.completion_async(0)).produced == 1

    asyncio.run(exercise())


def test_unexpected_driver_task_exception_is_retrieved_by_settlement_backstop(monkeypatch):
    async def exercise():
        failure = RuntimeError("internal driver fault")

        async def source(context):
            raise AssertionError("source invoked")
            yield

        async def failed_driver(self, function):
            raise failure

        monkeypatch.setattr(implementation.AsyncTaskStream, "_drive", failed_driver)
        stream = start_async_task_stream(source)
        with pytest.raises(AsyncSourceError) as observed:
            await stream.completion_async(1)
        assert observed.value.__cause__ is failure and stream._task.done()
        assert not stream._task._log_traceback
        await stream.aclose(1)
        assert stream.closed

    asyncio.run(exercise())


def test_abort_result_allocation_failure_still_acknowledges_source_free_owner(monkeypatch):
    async def exercise():
        birth_failure = MemoryError("before adoption")
        result_failure = KeyboardInterrupt("abort result allocation")

        async def source(context):
            yield 1

        def failed_birth(*args, **kwargs):
            raise birth_failure

        def failed_result(*args, **kwargs):
            raise result_failure

        monkeypatch.setattr(implementation, "_Task", failed_birth)
        monkeypatch.setattr(implementation, "TaskStreamResult", failed_result)
        with pytest.raises(AsyncTaskStreamOwnershipError) as failure:
            start_async_task_stream(source)
        stream = failure.value.stream
        await stream.aclose(1)
        assert stream.closed and stream._task is None and stream._owned is None
        with pytest.raises(AsyncSourceControlError) as observed:
            await stream.completion_async(0)
        assert observed.value.__cause__ is result_failure

    asyncio.run(exercise())


def test_invalid_terminal_record_is_not_fabricated_as_success():
    async def exercise():
        async def source(context):
            yield 1

        stream = start_async_task_stream(source)
        await stream.completion_async(1)
        stream._result = None  # Deliberate defensive-state fault, not public mutation.
        with pytest.raises(RuntimeError, match="no result"):
            await stream.completion_async(0)
        await stream.aclose(1)

    asyncio.run(exercise())


@pytest.mark.parametrize("shape", ["subclass", "extra_argument"])
def test_only_exact_private_marker_cancellation_is_owner_acknowledgement(shape):
    async def exercise():
        holder = []

        class SourceCancelled(asyncio.CancelledError):
            pass

        async def source(context):
            stream = holder[0]
            assert stream.cancel()
            if shape == "subclass":
                raise SourceCancelled(stream._cancel_marker)
            raise asyncio.CancelledError(stream._cancel_marker, "extra")
            yield

        stream = start_async_task_stream(source)
        holder.append(stream)
        with pytest.raises(AsyncSourceControlError) as failure:
            await stream.completion_async(1)
        assert isinstance(failure.value.__cause__, asyncio.CancelledError)
        assert stream._task.cancelling() == 0
        await stream.aclose(1)

    asyncio.run(exercise())


@pytest.mark.parametrize("method", ["next_async", "wait_ready_async", "completion_async"])
def test_in_loop_publication_at_expired_deadline_uses_final_authoritative_snapshot(
    method, monkeypatch
):
    async def exercise():
        loop = asyncio.get_running_loop()
        gate, started = asyncio.Event(), asyncio.Event()
        published = loop.create_future()

        async def source(context):
            started.set()
            await gate.wait()
            yield 19

        stream = start_async_task_stream(source)
        stream._task.add_done_callback(lambda _: published.set_result(None))
        await started.wait()
        original = asyncio.timeout_at

        class PublishDuringTimeoutExit:
            async def __aenter__(self):
                self.context = original(loop.time())
                return await self.context.__aenter__()

            async def __aexit__(self, *args):
                gate.set()
                await published
                return await self.context.__aexit__(*args)

        monkeypatch.setattr(asyncio, "timeout_at", lambda _: PublishDuringTimeoutExit())
        observed = await getattr(stream, method)(1)
        if method == "next_async":
            assert observed.value == 19 and observed.sequence == 0
        else:
            assert observed is True if method == "wait_ready_async" else observed.produced == 1
            assert (await stream.next_async(0)).value == 19
        assert stream._completion_hub.waiter_count == 0 and not stream._stop
        await stream.aclose(1)

    asyncio.run(exercise())


def test_waiter_cancel_after_ready_notification_before_dequeue_preserves_item(monkeypatch):
    async def exercise():
        loop = asyncio.get_running_loop()
        gate = asyncio.Event()

        async def source(context):
            await gate.wait()
            yield 23

        stream = start_async_task_stream(source)
        waiting = asyncio.create_task(stream.next_async())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert stream._completion_hub.waiter_count == 1
        original = stream._completion_hub.notify

        def notify(*, terminal=False):
            original(terminal=terminal)
            if not terminal:
                # Pump runs first, but this cancel is queued before the waiter
                # continuation which that pump will schedule. No timing guess.
                loop.call_soon(waiting.cancel, "after ready notification")

        monkeypatch.setattr(stream._completion_hub, "notify", notify)
        gate.set()
        with pytest.raises(asyncio.CancelledError, match="after ready notification"):
            await waiting
        assert not stream._stop and stream._produced == 1
        assert (await stream.next_async(1)).value == 23
        await stream.aclose(1)

    asyncio.run(exercise())


@pytest.mark.parametrize("method", ["next_async", "wait_ready_async", "completion_async"])
def test_elapsed_timeout_and_wait_for_leave_native_producer_uncancelled(method):
    async def exercise():
        async def source(context):
            await asyncio.get_running_loop().create_future()
            yield 1

        stream = start_async_task_stream(source)
        if method == "wait_ready_async":
            assert await stream.wait_ready_async(0.001) is False
        else:
            with pytest.raises(TimeoutError):
                await getattr(stream, method)(0.001)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(getattr(stream, method)(), 0.001)
        assert not stream._stop and stream._task.cancelling() == 0
        assert stream._completion_hub.waiter_count == 0
        await stream.aclose(1)

    asyncio.run(exercise())
