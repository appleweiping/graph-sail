"""Native async producer contracts, independent of the older stream engines."""

import asyncio
import functools
import gc
import weakref
from contextvars import ContextVar
from threading import Thread

import pytest

import graph_sail
import graph_sail.async_task_streams as implementation
from graph_sail import (
    AsyncSourceControlError,
    AsyncSourceError,
    AsyncTaskStream,
    AsyncTaskStreamOwnershipError,
    TaskCancelled,
    TaskStreamConfig,
    TaskStreamItem,
    ValidationError,
    start_async_task_stream,
)


@pytest.fixture(autouse=True)
def no_active_native_owners():
    before = set(implementation._ACTIVE_OWNERS)
    yield
    assert before == implementation._ACTIVE_OWNERS


@pytest.mark.parametrize(
    "name",
    [
        "AsyncTaskStream",
        "AsyncTaskStreamOwnershipError",
        "AsyncTaskStreamOwnershipControlError",
        "start_async_task_stream",
    ],
)
def test_native_async_producer_public_api_exists(name):
    assert callable(getattr(graph_sail, name, None)), f"missing public API: {name}"
    assert name in graph_sail.__all__


@pytest.mark.parametrize(
    "method",
    [
        "__aiter__",
        "__anext__",
        "next_async",
        "wait_ready_async",
        "completion_async",
        "cancel",
        "aclose",
        "done",
        "__aenter__",
        "__aexit__",
    ],
)
def test_native_async_methods(method):
    assert callable(getattr(AsyncTaskStream, method, None))


def test_admission_has_no_callable_iterator_signature_or_annotation_hooks():
    calls = []

    class Hostile:
        def __getattribute__(self, name):
            calls.append(name)
            raise AssertionError("admission invoked a user hook")

    class Bound:
        async def source(self, context):
            yield 1

    async def source(context):
        yield 1

    async def coroutine(context):
        return 1

    async def exercise():
        borrowed = source(None)
        try:
            for candidate in [
                Hostile(),
                Bound().source,
                borrowed,
                coroutine,
                functools.partial(source),
                None,
                2,
                lambda _: borrowed,
            ]:
                with pytest.raises(ValidationError, match="exact native"):
                    start_async_task_stream(candidate)
            assert not calls
            assert borrowed.ag_frame is not None
            assert asyncio.all_tasks() == {asyncio.current_task()}
        finally:
            await borrowed.aclose()

    asyncio.run(exercise())


@pytest.mark.parametrize("option", [False, {}, 1, object()])
def test_invalid_config_never_starts_source(option):
    async def source(context):
        raise AssertionError("source invoked")
        yield

    async def exercise():
        with pytest.raises(ValidationError, match="config"):
            start_async_task_stream(source, config=option)
        assert asyncio.all_tasks() == {asyncio.current_task()}

    asyncio.run(exercise())


def test_mutated_config_revalidated_and_running_loop_required():
    async def source(context):
        yield 1

    config = TaskStreamConfig()
    object.__setattr__(config, "max_buffered", True)
    with pytest.raises(ValidationError):
        start_async_task_stream(source, config=config)
    with pytest.raises(RuntimeError, match="running event loop"):
        start_async_task_stream(source)


def test_source_call_arity_failure_is_observed_not_borrowed():
    async def wrong_arity():
        yield 1

    async def exercise():
        stream = start_async_task_stream(wrong_arity)
        with pytest.raises(AsyncSourceError) as failure:
            await stream.completion_async(1)
        assert isinstance(failure.value.__cause__, TypeError)
        assert stream._owned is None and stream.done()
        await stream.aclose(1)
        assert stream.closed

    asyncio.run(exercise())


@pytest.mark.parametrize("count", [0, 1, 7])
def test_real_await_interleaving_identity_and_order(count):
    async def exercise():
        started, release, cleaned = asyncio.Event(), asyncio.Event(), asyncio.Event()
        borrowed = []

        async def source(context):
            started.set()
            try:
                await release.wait()
                for _ in range(count):
                    context.cancellation.raise_if_cancelled()
                    yield borrowed
            finally:
                await asyncio.sleep(0)
                cleaned.set()

        stream = start_async_task_stream(source, config=TaskStreamConfig(max_buffered=1))
        await started.wait()
        assert not stream.done() and not cleaned.is_set()
        assert await stream.wait_ready_async(0) is False
        with pytest.raises(TimeoutError):
            await stream.completion_async(0)
        release.set()
        items = [item async for item in stream]
        assert [item.sequence for item in items] == list(range(count))
        assert all(type(item) is TaskStreamItem and item.value is borrowed for item in items)
        result = await stream.completion_async(1)
        assert (result.status, result.produced) == ("succeeded", count)
        assert cleaned.is_set() and stream._task.done() and stream.done()
        assert not stream.closed and not stream.cleanup_incomplete
        await stream.aclose(1)
        assert stream.closed

    asyncio.run(exercise())


def test_pre_advance_credit_and_exact_limit_never_peek_eof():
    async def exercise():
        advances, cleanup = [], []

        async def source(context):
            try:
                for index in range(3):
                    advances.append(index)
                    yield index
            finally:
                cleanup.append(True)

        stream = start_async_task_stream(
            source,
            config=TaskStreamConfig(max_buffered=1, max_yields=2),
        )
        assert await stream.wait_ready_async(1)
        assert advances == [0] and not cleanup
        with pytest.raises(TimeoutError):
            await stream.completion_async(0)
        assert (await stream.next_async(0)).value == 0
        assert advances == [0]  # Capacity wake is deferred, never inline user code.
        assert (await stream.next_async(1)).value == 1
        result = await stream.completion_async(1)
        assert (result.status, result.produced) == ("limited", 2)
        assert advances == [0, 1] and cleanup == [True]
        await stream.aclose(1)

    asyncio.run(exercise())


def test_custom_factory_bypassed_and_caller_context_copied(monkeypatch):
    async def exercise():
        loop = asyncio.get_running_loop()
        calls, seen = [], []
        local = ContextVar("native-stream-example", default="unset")

        def forbidden_factory(*args, **kwargs):
            calls.append(True)
            raise AssertionError("application factory adopted source")

        async def source(context):
            seen.append(local.get())
            yield None

        local.set("captured")
        original = loop.get_task_factory()
        loop.set_task_factory(forbidden_factory)
        try:
            stream = start_async_task_stream(source)
            assert not seen and not stream._entered and not stream._task.done()
            local.set("later")
            assert (await stream.next_async(1)).value is None
            await stream.aclose(1)
            assert seen == ["captured"] and not calls
            assert loop.get_task_factory() is forbidden_factory
        finally:
            loop.set_task_factory(original)

    asyncio.run(exercise())


@pytest.mark.skipif(not hasattr(asyncio, "eager_task_factory"), reason="Python 3.12+ factory")
def test_real_eager_factory_does_not_start_owned_task_early():
    async def exercise():
        loop = asyncio.get_running_loop()
        started = []

        async def source(context):
            started.append(True)
            yield 1

        original = loop.get_task_factory()
        loop.set_task_factory(asyncio.eager_task_factory)
        try:
            stream = start_async_task_stream(source)
            assert not started and not stream._task.done()
            assert (await stream.next_async(1)).value == 1
            await stream.aclose(1)
            assert started == [True]
        finally:
            loop.set_task_factory(original)

    asyncio.run(exercise())


def test_cancel_before_first_step_runs_no_source_body_or_finally():
    async def exercise():
        effects = []

        async def source(context):
            try:
                effects.append("body")
                yield 1
            finally:
                effects.append("finally")

        stream = start_async_task_stream(source)
        assert stream.cancel() and not stream.cancel()
        assert stream._task.cancelling() == 0 and not stream._entered
        result = await stream.completion_async(1)
        assert (result.status, result.produced) == ("cancelled", 0)
        assert not effects and stream._owned is None
        await stream.aclose(1)
        assert stream.closed

    asyncio.run(exercise())


def test_one_cancel_waiter_cancellation_and_timeout_preserve_awaited_finally():
    async def exercise():
        started, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        cleaned = []

        async def source(context):
            started.set()
            try:
                yield "prefix"
                await asyncio.get_running_loop().create_future()
            finally:
                cleaning.set()
                await release.wait()
                cleaned.append(True)

        stream = start_async_task_stream(source)
        await started.wait()
        assert stream.cancel()
        await cleaning.wait()
        assert stream._task.cancelling() == 1
        assert (await stream.next_async(0)).value == "prefix"
        for _ in range(3):
            assert not stream.cancel()
            with pytest.raises(TimeoutError):
                await stream.aclose(0)
        waiter = asyncio.create_task(stream.aclose())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        waiter.cancel("close waiter only")
        with pytest.raises(asyncio.CancelledError, match="close waiter only"):
            await waiter
        assert not stream.closed and not stream.done()
        assert stream._task.cancelling() == 1 and not cleaned
        assert stream._completion_hub.waiter_count == 0
        release.set()
        await stream.aclose(1)
        assert cleaned == [True] and stream.closed
        assert (await stream.completion_async(0)).status == "cancelled"

    asyncio.run(exercise())


def test_cancel_during_known_outer_cleanup_never_injects_or_rewrites_limit():
    async def exercise():
        cleaning, release = asyncio.Event(), asyncio.Event()

        async def source(context):
            try:
                yield 4
                raise AssertionError("peeked past limit")
            finally:
                cleaning.set()
                await release.wait()

        stream = start_async_task_stream(source, config=TaskStreamConfig(max_yields=1))
        await cleaning.wait()
        assert stream._phase == "cleanup" and not stream.cancel()
        with pytest.raises(TimeoutError):
            await stream.aclose(0)
        assert stream._task.cancelling() == 0 and not stream.closed
        release.set()
        await stream.aclose(1)
        result = await stream.completion_async(0)
        assert (result.status, result.produced) == ("limited", 1)

    asyncio.run(exercise())


def test_cancel_suppressed_by_source_rejects_its_late_yield():
    async def exercise():
        started = asyncio.Event()

        async def source(context):
            yield 1
            started.set()
            try:
                await asyncio.get_running_loop().create_future()
            except asyncio.CancelledError:
                yield "must not be accepted"

        stream = start_async_task_stream(source)
        await started.wait()
        assert stream.cancel()
        result = await stream.completion_async(1)
        assert (result.status, result.produced) == ("cancelled", 1)
        assert (await stream.next_async(0)).value == 1
        with pytest.raises(StopAsyncIteration):
            await stream.next_async(0)
        await stream.aclose(1)

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "failure",
    [
        RuntimeError("ordinary"),
        asyncio.CancelledError("source"),
        KeyboardInterrupt("source"),
        SystemExit(23),
        BaseException("control"),
        BaseExceptionGroup("mixed", [KeyboardInterrupt()]),
        TaskCancelled("without stop"),
    ],
)
def test_source_failures_are_fresh_wrapped_after_prefix_without_trace_mutation(failure):
    async def exercise():
        async def source(context):
            yield 9
            raise failure

        stream = start_async_task_stream(source)
        wrapper = AsyncSourceError if isinstance(failure, Exception) else AsyncSourceControlError
        with pytest.raises(wrapper) as first:
            await stream.completion_async(1)
        trace = failure.__traceback__
        assert first.value.__cause__ is failure and stream._task.exception() is None
        assert await stream.wait_ready_async(0)
        assert (await stream.next_async(0)).value == 9
        for _ in range(3):
            with pytest.raises(wrapper) as later:
                await stream.next_async(0)
            assert later.value is not first.value and later.value.__cause__ is failure
            assert failure.__traceback__ is trace
        await stream.aclose(1)
        assert stream.closed

    asyncio.run(exercise())


@pytest.mark.parametrize("kind", ["independent", "cooperative", "self_stop"])
def test_source_self_stop_does_not_queue_native_cancel_and_controls_stay_distinct(kind):
    async def exercise():
        failure = asyncio.CancelledError("independent after stop")
        holder = []

        async def source(context):
            assert holder[0].cancel()
            assert context.cancellation.cancelled
            assert holder[0]._task.cancelling() == 0
            if kind == "independent":
                raise failure
            if kind == "cooperative":
                context.cancellation.raise_if_cancelled()
            yield "after stop"

        stream = start_async_task_stream(source)
        holder.append(stream)
        if kind == "independent":
            with pytest.raises(AsyncSourceControlError) as observed:
                await stream.completion_async(1)
            assert observed.value.__cause__ is failure
        else:
            result = await stream.completion_async(1)
            assert (result.status, result.produced) == ("cancelled", 0)
        await stream.aclose(1)

    asyncio.run(exercise())


def test_illegal_cleanup_retains_owner_frame_without_retrying_generator():
    async def exercise():
        effects = []

        async def source(context):
            try:
                yield 1
            finally:
                effects.append("entered")
                yield 2
                effects.append("resumed")

        stream = start_async_task_stream(source, config=TaskStreamConfig(max_yields=1))
        with pytest.raises(AsyncSourceError) as failed:
            await stream.completion_async(1)
        assert isinstance(failed.value.__cause__, RuntimeError)
        assert stream.done() and stream._task.done() and not stream.closed
        assert stream.cleanup_incomplete and stream._owned.ag_frame is not None
        errors = []
        for _ in range(3):
            with pytest.raises(AsyncTaskStreamOwnershipError) as ownership:
                await stream.aclose(1)
            assert ownership.value.stream is stream and ownership.value.phase == "cleanup"
            assert ownership.value.__cause__ is failed.value.__cause__
            errors.append(ownership.value)
        assert len({id(error) for error in errors}) == 3
        assert effects == ["entered"] and not stream.closed
        # Fixture-only release after verifying the public API never resumes it.
        # There is deliberately no public recovery/force-resume API.
        with pytest.raises(GeneratorExit):
            await anext(stream._owned)

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "name",
    [
        "__aiter__",
        "next_async",
        "wait_ready_async",
        "completion_async",
        "aclose",
        "__aenter__",
    ],
)
def test_producer_self_wait_and_self_cleanup_wait_are_rejected(name):
    async def exercise():
        holder, rejected = [], []

        async def attempt():
            before = holder[0]._completion_hub.waiter_count
            with pytest.raises(RuntimeError, match="own stream"):
                result = getattr(holder[0], name)()
                if name != "__aiter__":
                    await result
            rejected.append(True)
            assert holder[0]._completion_hub.waiter_count == before
            assert not holder[0]._discard and not holder[0]._stop

        async def source(context):
            try:
                await attempt()
                yield 1
            finally:
                await attempt()

        stream = start_async_task_stream(source, config=TaskStreamConfig(max_yields=1))
        holder.append(stream)
        await stream.completion_async(1)
        assert rejected == [True, True]
        await stream.aclose(1)

    asyncio.run(exercise())


def test_active_owner_survives_lost_application_handle_until_real_settlement():
    async def exercise():
        gate, started = asyncio.Event(), asyncio.Event()

        async def source(context):
            started.set()
            await gate.wait()
            yield 1

        stream = start_async_task_stream(source)
        await started.wait()
        reference = weakref.ref(stream)
        task_reference = weakref.ref(stream._task)
        del stream
        gc.collect()
        assert reference() in implementation._ACTIVE_OWNERS and task_reference() is not None
        gate.set()
        recovered = reference()
        await recovered.aclose(1)
        assert recovered.closed and recovered not in implementation._ACTIVE_OWNERS
        assert asyncio.all_tasks() == {asyncio.current_task()}

    asyncio.run(exercise())


def test_foreign_loop_and_thread_operations_fail_even_when_closed():
    holder, failures = [], []

    async def source(context):
        yield 1

    def no_running_loop():
        stream = holder[0]
        for read in [
            stream.done,
            stream.cancel,
            stream.__aiter__,
            lambda: stream.closed,
            lambda: stream.cleanup_incomplete,
        ]:
            with pytest.raises(RuntimeError, match="owner loop"):
                read()
            failures.append(True)

    async def create_and_close():
        stream = start_async_task_stream(source)
        holder.append(stream)
        await stream.aclose(1)
        thread = Thread(target=no_running_loop)
        thread.start()
        thread.join(2)
        assert not thread.is_alive()

    async def wrong_loop():
        stream = holder[0]
        for name in ["next_async", "wait_ready_async", "completion_async", "aclose", "__aenter__"]:
            with pytest.raises(RuntimeError, match="owner loop"):
                await getattr(stream, name)()
        no_running_loop()  # Running, but a different loop.

    asyncio.run(create_and_close())
    asyncio.run(wrong_loop())
    assert len(failures) == 10


def test_context_manager_discards_and_settles_after_body_failure():
    async def exercise():
        cleaned = []

        async def source(context):
            try:
                yield 1
                await asyncio.get_running_loop().create_future()
            finally:
                await asyncio.sleep(0)
                cleaned.append(True)

        problem = ValueError("body")
        stream = start_async_task_stream(source)
        with pytest.raises(ValueError) as observed:
            async with stream as entered:
                assert entered is stream
                assert (await stream.next_async(1)).value == 1
                raise problem
        assert observed.value is problem and stream.closed and cleaned == [True]

    asyncio.run(exercise())
