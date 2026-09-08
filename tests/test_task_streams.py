from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Event, current_thread
from threading import enumerate as threads

import pytest

from graph_sail import (
    TaskCancelled,
    TaskStreamConfig,
    TaskStreamItem,
    TaskStreamResult,
    ValidationError,
    start_task_stream,
)
from graph_sail import task_streams as implementation


@pytest.fixture(autouse=True)
def no_producer_threads_left():
    before = {t.ident for t in threads() if t.name.startswith("graph-sail")}
    yield
    assert {t.ident for t in threads() if t.name.startswith("graph-sail")} == before


def test_first_yield_is_visible_before_later_work_finishes():
    entered, release = Event(), Event()

    def producer(ctx):
        yield 17
        entered.set()
        assert release.wait(10)
        yield 25

    stream = start_task_stream(producer)
    try:
        first = stream.next(5)
        assert (first.sequence, first.value) == (0, 17)
        assert entered.wait(5)
        assert not stream.done()
        assert not stream.wait_ready(0)
        with pytest.raises(TimeoutError):
            stream.next(0)
        release.set()
        assert stream.next(5).value == 25
        assert stream.completion(5).status == "succeeded"
        assert list(stream) == []
    finally:
        release.set()
        stream.close(5)


def test_backpressure_precedes_generator_advancement_and_cap_does_not_peek():
    second = Event()
    calls, finalized = [], []

    def producer(ctx):
        try:
            for index in range(3):
                calls.append(index)
                if index == 1:
                    second.set()
                yield index
        finally:
            finalized.append(current_thread().ident)

    with start_task_stream(
        producer, config=TaskStreamConfig(max_buffered=1, max_yields=2)
    ) as stream:
        assert stream.wait_ready(5)
        assert calls == [0] and not second.is_set()
        with pytest.raises(TimeoutError):
            stream.completion(0)
        assert stream.next(5).value == 0
        assert second.wait(5)
        assert stream.completion(5).status == "limited"
        assert stream.completion().produced == 2
        assert stream.next(5).value == 1
        assert list(stream) == []
        assert calls == [0, 1]
    assert len(finalized) == 1 and finalized[0] != current_thread().ident


def test_accepted_items_precede_original_failure():
    failure = ValueError("application failure")

    def producer(ctx):
        yield None
        raise failure

    with start_task_stream(producer) as stream:
        assert stream.next(5).value is None
        with pytest.raises(ValueError) as caught:
            stream.next(5)
        assert caught.value is failure

        with pytest.raises(ValueError) as caught:
            stream.completion(5)
        assert caught.value is failure


def test_cancel_keeps_queue_and_acknowledged_count_after_consumption():
    entered, release = Event(), Event()

    def producer(ctx):
        yield 1
        entered.set()
        assert release.wait(10)
        ctx.cancellation.raise_if_cancelled()
        yield 2

    stream = start_task_stream(producer)
    try:
        assert stream.next(5).value == 1
        assert entered.wait(5)
        assert stream.cancel() and not stream.cancel()
        release.set()
        assert stream.completion(5) == TaskStreamResult("cancelled", 1)
        assert list(stream) == []
    finally:
        release.set()
        stream.close(5)


def test_cancel_unblocks_full_mailbox_without_discarding_accepted_item():
    finalized = Event()

    def producer(ctx):
        try:
            yield 3
            pytest.fail("backpressure should stop before the next pull")
        finally:
            finalized.set()

    with start_task_stream(producer, config=TaskStreamConfig(max_buffered=1)) as stream:
        assert stream.wait_ready(5)
        assert stream.cancel()
        assert stream.completion(5) == TaskStreamResult("cancelled", 1)
        assert finalized.is_set()
        assert not stream.cancel()
        assert stream.next(5).value == 3
        assert list(stream) == []


def test_close_timeout_retains_live_ownership_and_discards_late_yield():
    entered, release = Event(), Event()

    def producer(ctx):
        entered.set()
        assert release.wait(10)
        yield 1

    stream = start_task_stream(producer)
    try:
        assert entered.wait(5)
        with pytest.raises(TimeoutError, match="owns a live producer"):
            stream.close(0)
        assert not stream.closed and not stream.done()
        assert stream.wait_ready(0)
        assert list(stream) == []
        release.set()
        stream.close(5)
        assert stream.closed and stream.done()
        assert stream.completion() == TaskStreamResult("cancelled", 0)
        stream.close(0)
    finally:
        release.set()
        stream.close(5)


@pytest.mark.parametrize("error", [ValueError("ordinary"), KeyboardInterrupt(), SystemExit(3)])
def test_empty_failure_is_original_and_wait_ready_observes_settlement(error):
    def producer(ctx):
        raise error
        yield

    with start_task_stream(producer) as stream:
        assert stream.wait_ready(5)
        for operation in (stream.next, stream.completion):
            with pytest.raises(type(error)) as caught:
                operation(5)
            assert caught.value is error


def test_unsolicited_task_cancelled_is_failure():
    failure = TaskCancelled("not requested")

    def producer(ctx):
        raise failure
        yield

    with start_task_stream(producer) as stream:
        with pytest.raises(TaskCancelled) as caught:
            stream.completion(5)
        assert caught.value is failure


@pytest.mark.parametrize("count", [0, 1, 15, 16, 17, 129])
def test_ordered_finite_stream_and_none_values(count):
    def producer(ctx):
        yield from (None for _ in range(count))
        return "ignored result"

    with start_task_stream(producer, config=TaskStreamConfig(max_buffered=3)) as stream:
        assert [(i.sequence, i.value) for i in stream] == list(enumerate([None] * count))
        assert stream.completion(5) == TaskStreamResult("succeeded", count)
        assert stream.wait_ready(0) and stream.done()
        assert not stream.closed


def test_multiple_consumers_get_each_item_once_without_broadcast():
    def producer(ctx):
        yield from range(200)

    with start_task_stream(producer, config=TaskStreamConfig(max_buffered=2)) as stream:
        with ThreadPoolExecutor(max_workers=4) as pool:
            consumers = [pool.submit(list, stream) for _ in range(4)]
            result = [item for future in consumers for item in future.result(10)]
        assert sorted((i.sequence, i.value) for i in result) == list(enumerate(range(200)))
        assert stream.completion(5).produced == 200


def test_values_are_borrowed_and_not_closed_by_stream():
    class Value:
        def close(self):
            pytest.fail("borrowed values must not be closed")

    value = Value()

    def producer(ctx):
        yield value
        yield value

    stream = start_task_stream(producer)
    try:
        assert stream.next(5).value is value
        assert stream.completion(5).produced == 2
        stream.close(5)
        assert list(stream) == []
    finally:
        stream.close(5)


@pytest.mark.parametrize("name", ["max_buffered", "max_yields"])
@pytest.mark.parametrize("value", [0, -1, True, 1.0, None, "1", 10_000_001])
def test_config_rejects_invalid_bounded_counts(name, value):
    with pytest.raises(ValidationError):
        TaskStreamConfig(**{name: value})


@pytest.mark.parametrize("timeout", [-1, True, float("inf"), float("nan"), "1", 86401])
def test_invalid_timeouts_do_not_cancel_or_consume(timeout):
    def producer(ctx):
        yield 7

    with start_task_stream(producer) as stream:
        for method in (stream.next, stream.wait_ready, stream.completion, stream.close):
            with pytest.raises(ValidationError):
                method(timeout)
        assert stream.next(5).value == 7
        assert stream.completion(5).status == "succeeded"


def test_preflight_rejects_ordinary_callable_coroutine_async_generator_and_iterator():
    async def coroutine(ctx):
        return 1

    async def asynchronous(ctx):
        yield 1

    for value in (None, lambda ctx: iter([1]), iter([1]), coroutine, asynchronous):
        with pytest.raises(ValidationError):
            start_task_stream(value)

    def producer(ctx):
        yield 1

    with pytest.raises(ValidationError):
        start_task_stream(producer, config={})


@pytest.mark.parametrize("sequence", [-1, True, 1.5, 10_000_000])
def test_item_rejects_invalid_sequence(sequence):
    with pytest.raises(ValidationError):
        TaskStreamItem(sequence, None)


@pytest.mark.parametrize("status", [None, "failed", "closed", True])
def test_result_rejects_invalid_status(status):
    with pytest.raises(ValidationError):
        TaskStreamResult(status, 0)


def test_producer_cannot_wait_on_or_close_its_own_stream():
    released = Event()
    holder = []

    def producer(ctx):
        assert released.wait(10)
        stream = holder[0]
        for method in (stream.next, stream.wait_ready, stream.completion, stream.close):
            with pytest.raises(RuntimeError, match="own stream"):
                method(0)
        yield 1

    stream = start_task_stream(producer)
    try:
        holder.append(stream)
        released.set()
        assert stream.next(5).value == 1
        assert stream.completion(5).status == "succeeded"
    finally:
        released.set()
        stream.close(5)


@pytest.mark.parametrize(
    ("primary", "cleanup", "winner"),
    [
        (ValueError("primary"), RuntimeError("cleanup"), "primary"),
        (KeyboardInterrupt(), ValueError("cleanup"), "primary"),
        (ValueError("primary"), SystemExit(5), "cleanup"),
        (KeyboardInterrupt(), SystemExit(5), "primary"),
    ],
)
def test_owned_generator_cleanup_preserves_failure_priority(monkeypatch, primary, cleanup, winner):
    finalized = Event()

    def producer(ctx):
        try:
            yield 1
        finally:
            finalized.set()
            raise cleanup

    def failing_production(self, generator):
        assert next(generator) == 1
        raise primary

    monkeypatch.setattr(implementation.TaskStream, "_produce", failing_production)
    expected = primary if winner == "primary" else cleanup
    with start_task_stream(producer) as stream:
        with pytest.raises(type(expected)) as caught:
            stream.completion(5)
        assert caught.value is expected and finalized.is_set()
        assert expected.__notes__


def test_cap_reports_generator_cleanup_failure_after_buffered_value():
    error = RuntimeError("cleanup failed")

    def producer(ctx):
        try:
            yield 9
        finally:
            raise error

    with start_task_stream(producer, config=TaskStreamConfig(max_yields=1)) as stream:
        assert stream.next(5).value == 9
        with pytest.raises(RuntimeError) as caught:
            stream.next(5)
        assert caught.value is error


def test_factory_result_is_defensively_checked_without_closing_borrowed_generator():
    def producer(ctx):
        yield 1
        yield 2

    borrowed = producer(None)
    assert next(borrowed) == 1
    # Internal construction permits directly exercising a forged factory result;
    # public preflight never accepts this ordinary callable.
    stream = implementation.TaskStream(lambda ctx: borrowed, TaskStreamConfig())._start()
    try:
        with pytest.raises(ValidationError, match="fresh native"):
            stream.completion(5)
        assert next(borrowed) == 2
    finally:
        borrowed.close()
        stream.close(5)


def test_start_failure_before_thread_creation_does_not_run_generator(monkeypatch):
    error = RuntimeError("cannot start")

    def producer(ctx):
        pytest.fail("must not run")
        yield

    def failed_start(self):
        raise error

    monkeypatch.setattr(implementation.Thread, "start", failed_start)
    with pytest.raises(RuntimeError) as caught:
        start_task_stream(producer)
    assert caught.value is error


def test_start_raised_after_start_retains_cleanup_responsibility(monkeypatch):
    real_start = implementation.Thread.start
    error = RuntimeError("start acknowledgement lost")

    def producer(ctx):
        while True:
            ctx.cancellation.raise_if_cancelled()
            yield 1

    def failed_start(self):
        real_start(self)
        raise error

    monkeypatch.setattr(implementation.Thread, "start", failed_start)
    with pytest.raises(RuntimeError) as caught:
        start_task_stream(producer, config=TaskStreamConfig(max_buffered=1))
    assert caught.value is error


def test_interrupted_join_never_claims_closed_before_authoritative_completion(monkeypatch):
    entered, release = Event(), Event()

    def producer(ctx):
        entered.set()
        assert release.wait(10)
        yield 1

    stream = start_task_stream(producer)
    try:
        assert entered.wait(5)
        called = []
        real_join = stream._thread.join

        def misleading_join(timeout):
            called.append(True)

        with monkeypatch.context() as patch:
            patch.setattr(stream._thread, "join", misleading_join)
            patch.setattr(stream._thread, "is_alive", lambda: False)
            with pytest.raises(TimeoutError):
                stream.close(0)
            assert not stream.closed and not called
        release.set()
        stream.close(5)
        real_join(5)
        assert stream.closed
    finally:
        release.set()
        stream.close(5)


def test_native_join_interrupt_keeps_handle_for_later_retry(monkeypatch):
    def producer(ctx):
        yield 1

    stream = start_task_stream(producer)
    try:
        assert stream.completion(5).status == "succeeded"
        interrupt = KeyboardInterrupt()

        def interrupted_join(timeout):
            raise interrupt

        with monkeypatch.context() as patch:
            patch.setattr(stream._thread, "join", interrupted_join)
            with pytest.raises(KeyboardInterrupt) as caught:
                stream.close(5)
            assert caught.value is interrupt and not stream.closed
        stream.close(5)
    finally:
        stream.close(5)


@pytest.mark.parametrize("control", [False, True])
def test_context_cleanup_failure_does_not_hide_body_control(monkeypatch, control):
    def producer(ctx):
        yield 1

    stream = start_task_stream(producer)
    body = KeyboardInterrupt() if control else ValueError("body")
    cleanup = RuntimeError("close")

    def failed_close():
        raise cleanup

    try:
        with monkeypatch.context() as patch:
            patch.setattr(stream, "close", failed_close)
            with pytest.raises(type(body)) as caught, stream:
                raise body
            assert caught.value is body and body.__notes__
    finally:
        stream.close(5)


def test_cancel_during_generator_natural_return_records_cancelled():
    entered, release = Event(), Event()

    def producer(ctx):
        entered.set()
        assert release.wait(10)
        return
        yield

    stream = start_task_stream(producer)
    try:
        assert entered.wait(5)
        stream.cancel()
        release.set()
        assert stream.completion(5) == TaskStreamResult("cancelled", 0)
    finally:
        release.set()
        stream.close(5)


def test_repeated_failure_reads_do_not_accumulate_prior_reader_tracebacks():
    error = ValueError("one failure")

    def producer(ctx):
        raise error
        yield

    with start_task_stream(producer) as stream:
        assert stream.wait_ready(5)
        sizes = []
        for index in range(500):
            operation = stream.next if index % 2 else stream.completion
            with pytest.raises(ValueError) as caught:
                operation(5)
            assert caught.value is error
            traceback = caught.value.__traceback__
            depth = 0
            while traceback is not None:
                depth += 1
                traceback = traceback.tb_next
            sizes.append(depth)
        assert min(sizes) == max(sizes)


def test_liveness_after_backend_completion_still_requires_native_join(monkeypatch):
    def producer(ctx):
        yield 1

    stream = start_task_stream(producer)
    try:
        stream.completion(5)
        with monkeypatch.context() as patch:
            patch.setattr(stream._thread, "is_alive", lambda: True)
            with pytest.raises(TimeoutError, match="live producer"):
                stream.close(5)
            assert not stream.closed
        stream.close(5)
    finally:
        stream.close(5)


@pytest.mark.parametrize("body", [None, ValueError("body")])
def test_context_new_cleanup_interrupt_takes_priority(monkeypatch, body):
    def producer(ctx):
        yield 1

    stream = start_task_stream(producer)
    interrupt = KeyboardInterrupt()

    def failed_close():
        raise interrupt

    try:
        with monkeypatch.context() as patch:
            patch.setattr(stream, "close", failed_close)
            with pytest.raises(KeyboardInterrupt) as caught, stream:
                if body is not None:
                    raise body
            assert caught.value is interrupt
    finally:
        stream.close(5)


@pytest.mark.parametrize("cleanup", [RuntimeError("join"), KeyboardInterrupt()])
def test_start_cleanup_failure_is_reported_and_preserves_live_handle_for_test_cleanup(
    monkeypatch, cleanup
):
    real_start = implementation.Thread.start
    error = ValueError("start")
    captured = []

    def producer(ctx):
        while True:
            yield 1

    def failed_start(self):
        real_start(self)
        raise error

    def failed_join(self, timeout):
        captured.append(self)
        raise cleanup

    try:
        with monkeypatch.context() as patch:
            patch.setattr(implementation.Thread, "start", failed_start)
            patch.setattr(implementation.TaskStream, "_join", failed_join)
            expected = error if isinstance(cleanup, Exception) else cleanup
            with pytest.raises(type(expected)) as caught:
                start_task_stream(producer)
            assert caught.value is expected and expected.__notes__
    finally:
        for stream in captured:
            stream.close(5)


@pytest.mark.parametrize("part", ["TaskStreamItem", "TaskStreamResult"])
def test_result_allocation_failure_is_settled_and_generator_is_closed(monkeypatch, part):
    finalized = Event()
    error = MemoryError("allocation")

    def producer(ctx):
        try:
            yield 1
        finally:
            finalized.set()

    def failed(*args):
        raise error

    with monkeypatch.context() as patch:
        patch.setattr(implementation, part, failed)
        with start_task_stream(producer) as stream:
            with pytest.raises(MemoryError) as caught:
                stream.completion(5)
            assert caught.value is error and finalized.is_set() and stream.done()


def test_internal_missing_result_is_diagnosed_not_reported_success(monkeypatch):
    def producer(ctx):
        yield 1

    with monkeypatch.context() as patch:
        patch.setattr(implementation.TaskStream, "_produce", lambda *args: None)
        with (
            start_task_stream(producer) as stream,
            pytest.raises(RuntimeError, match="no completion result"),
        ):
            stream.completion(5)
    with start_task_stream(producer) as stream:
        stream.completion(5)
        stream._result = None
        with pytest.raises(RuntimeError, match="no result"):
            stream.completion(0)


def test_offline_example_really_observes_partial_progress(capsys):
    import runpy
    from pathlib import Path

    runpy.run_path(
        str(Path(__file__).parents[1] / "examples" / "task_stream.py"), run_name="__main__"
    )
    output = capsys.readouterr().out
    assert "producer still running" in output
    assert "status='succeeded', produced=2" in output
