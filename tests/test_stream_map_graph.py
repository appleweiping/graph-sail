"""A yielded item can feed a bounded downstream map before source completion."""

from __future__ import annotations

from threading import Event, Thread

import pytest

from graph_sail import (
    StreamMap,
    StreamMapConfig,
    StreamMapResult,
    StreamMapSerializationError,
    TaskCancelled,
    ValidationError,
    start_stream_map,
)

_FLAKY_UNPICKLE_ATTEMPTS = 0


def _flaky_unpickle() -> str:
    global _FLAKY_UNPICKLE_ATTEMPTS
    _FLAKY_UNPICKLE_ATTEMPTS += 1
    if _FLAKY_UNPICKLE_ATTEMPTS == 1:
        raise ValueError("transient-unpickle")
    return "recovered"


class _FlakyUnpickle:
    def __reduce__(self):
        return _flaky_unpickle, ()


def test_result_deserialization_failure_is_terminal() -> None:
    global _FLAKY_UNPICKLE_ATTEMPTS
    _FLAKY_UNPICKLE_ATTEMPTS = 0

    def source(context):
        yield 1

    def mapper(context, value):
        return _FlakyUnpickle()

    handle = start_stream_map(source, mapper, config=StreamMapConfig(max_pending=1, max_workers=1))
    try:
        with pytest.raises(ValueError, match="transient-unpickle"):
            handle.next(3)
        with pytest.raises(ValueError, match="transient-unpickle"):
            handle.next(3)
        assert _FLAKY_UNPICKLE_ATTEMPTS == 1
        with pytest.raises(ValueError, match="transient-unpickle"):
            handle.completion(3)
    finally:
        handle.close(3)


def test_first_map_completes_before_source_eof() -> None:
    source_can_finish = Event()
    mapped = Event()

    def source(context):
        yield 3
        source_can_finish.wait(5)
        context.cancellation.raise_if_cancelled()

    def mapper(context, value):
        assert context.sequence == 0
        mapped.set()
        return value * 2

    handle = start_stream_map(source, mapper, config=StreamMapConfig(max_pending=1, max_workers=1))
    try:
        assert mapped.wait(3)
        assert not handle.done()
        with pytest.raises(TimeoutError, match="completion is not ready"):
            handle.completion(0)
        assert iter(handle) is handle
        item = handle.next(3)
        assert (item.sequence, item.value) == (0, 6)
        source_can_finish.set()
        assert handle.completion(3).status == "succeeded"
    finally:
        source_can_finish.set()
        handle.close(5)


def test_public_constructor_returns_a_running_owned_edge() -> None:
    def source(_context):
        yield 1

    def mapper(_context, value):
        return value + 2

    handle = StreamMap(source, mapper, StreamMapConfig(1, 1))
    try:
        assert handle.next(3).value == 3
        assert handle.completion(3).status == "succeeded"
    finally:
        handle.close(5)


def test_pending_credit_prevents_unseen_source_advance() -> None:
    mapper_entered = Event()
    mapper_finished = Event()
    release_map = Event()
    source_advanced = Event()

    def source(_context):
        yield 1
        source_advanced.set()
        yield 2

    def mapper(_context, value):
        if value == 1:
            mapper_entered.set()
            release_map.wait(5)
            mapper_finished.set()
        return value * 10

    handle = start_stream_map(source, mapper, config=StreamMapConfig(1, 1))
    try:
        assert mapper_entered.wait(3)
        assert not source_advanced.wait(0.15)
        release_map.set()
        assert mapper_finished.wait(3)
        assert not source_advanced.wait(0.15)
        assert handle.next(3).value == 10
        assert source_advanced.wait(3)
        assert handle.next(3).value == 20
        result = handle.completion(3)
        assert (result.status, result.accepted, result.published) == ("succeeded", 2, 2)
    finally:
        release_map.set()
        handle.close(5)


def test_out_of_order_map_completion_publishes_in_source_order() -> None:
    second_done = Event()
    release_first = Event()

    def source(_context):
        yield 0
        yield 1

    def mapper(_context, value):
        if value == 0:
            release_first.wait(5)
        else:
            second_done.set()
        return value + 10

    handle = start_stream_map(source, mapper, config=StreamMapConfig(2, 2))
    try:
        assert second_done.wait(3)
        with pytest.raises(TimeoutError, match="not ready"):
            handle.next(0.05)
        release_first.set()
        assert [(item.sequence, item.value) for item in (handle.next(3), handle.next(3))] == [
            (0, 10),
            (1, 11),
        ]
        assert handle.completion(3).status == "succeeded"
    finally:
        release_first.set()
        handle.close(5)


def test_source_failure_keeps_completed_prefix() -> None:
    def source(_context):
        yield "prefix"
        raise ValueError("source-fault")

    def mapper(_context, value):
        return value.upper()

    handle = start_stream_map(source, mapper, config=StreamMapConfig(1, 1))
    try:
        assert handle.next(3).value == "PREFIX"
        with pytest.raises(ValueError, match="source-fault"):
            handle.next(3)
        with pytest.raises(ValueError, match="source-fault"):
            handle.completion(3)
    finally:
        handle.close(5)


def test_source_cleanup_failure_is_visible_after_accepted_prefix() -> None:
    def source(_context):
        try:
            yield 1
        finally:
            raise RuntimeError("source-cleanup-fault")

    def mapper(_context, value):
        return value + 1

    handle = start_stream_map(source, mapper, config=StreamMapConfig(1, 1, max_yields=1))
    try:
        assert handle.next(3).value == 2
        with pytest.raises(RuntimeError, match="source-cleanup-fault"):
            handle.next(3)
        with pytest.raises(RuntimeError, match="source-cleanup-fault"):
            handle.completion(3)
    finally:
        handle.close(5)


def test_map_failure_keeps_only_ordered_prefix() -> None:
    first_entered = Event()
    release_first = Event()
    second_failed = Event()

    def source(_context):
        yield 0
        yield 1
        yield 2

    def mapper(_context, value):
        if value == 0:
            first_entered.set()
            release_first.wait(5)
        if value == 1:
            second_failed.set()
            raise ValueError("map-fault")
        return value + 10

    handle = start_stream_map(source, mapper, config=StreamMapConfig(3, 2))
    try:
        assert first_entered.wait(3)
        assert second_failed.wait(3)
        release_first.set()
        assert handle.next(3).value == 10
        with pytest.raises(ValueError, match="map-fault"):
            handle.next(3)
        with pytest.raises(ValueError, match="map-fault"):
            handle.completion(3)
    finally:
        release_first.set()
        handle.close(5)


def test_mapper_stop_iteration_is_a_failure_not_clean_eof() -> None:
    def source(_context):
        yield 1

    def mapper(_context, _value):
        raise StopIteration("not-source-eof")

    handle = start_stream_map(source, mapper, config=StreamMapConfig(1, 1))
    try:
        with pytest.raises(ValidationError, match="callback raised StopIteration") as item_error:
            handle.next(3)
        assert isinstance(item_error.value.__cause__, StopIteration)
        with pytest.raises(ValidationError, match="callback raised StopIteration"):
            handle.completion(3)
    finally:
        handle.close(5)


def test_earlier_mapper_failure_precedes_later_source_cleanup_failure() -> None:
    source_advancing = Event()
    pause = Event()

    def source(context):
        try:
            yield 1
            source_advancing.set()
            while not context.cancellation.cancelled:
                pause.wait(0.005)
        finally:
            raise RuntimeError("later-source-cleanup")

    def mapper(_context, _value):
        if not source_advancing.wait(3):
            raise TimeoutError("source did not advance")
        raise ValueError("earlier-mapper-failure")

    handle = start_stream_map(source, mapper, config=StreamMapConfig(2, 2))
    try:
        with pytest.raises(ValueError, match="earlier-mapper-failure"):
            handle.next(3)
        with pytest.raises(ValueError, match="earlier-mapper-failure"):
            handle.completion(3)
    finally:
        handle.close(5)


def test_source_value_is_snapshotted_before_next_advance() -> None:
    original = [1]

    def source(_context):
        yield original
        original.append(2)
        yield 0

    def mapper(_context, value):
        return value

    handle = start_stream_map(source, mapper, config=StreamMapConfig(2, 2))
    try:
        first = handle.next(3)
        second = handle.next(3)
        assert first.value == [1]
        assert second.value == 0
        assert original == [1, 2]
    finally:
        handle.close(5)


def test_exact_yield_limit_never_peeks() -> None:
    advanced = []

    def source(_context):
        for value in range(9):
            advanced.append(value)
            yield value

    def mapper(_context, value):
        return value

    handle = start_stream_map(source, mapper, config=StreamMapConfig(2, 1, max_yields=2))
    try:
        assert handle.completion(3).status == "limited"
        assert [next(handle).value, handle.next(3).value] == [0, 1]
        assert advanced == [0, 1]
        with pytest.raises(StopIteration):
            handle.next(3)
    finally:
        handle.close(5)


def test_cancel_timeout_retains_owner_until_blocked_mapper_finishes() -> None:
    entered = Event()
    release = Event()

    def source(_context):
        yield 1

    def mapper(_context, value):
        entered.set()
        release.wait(5)
        return value

    handle = start_stream_map(source, mapper, config=StreamMapConfig(1, 1))
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
    with pytest.raises(StopIteration):
        handle.next(0)


def test_cancel_after_source_eof_while_mapper_runs_is_not_success() -> None:
    at_eof = Event()
    mapper_entered = Event()
    release = Event()

    def source(_context):
        yield 1
        at_eof.set()

    def mapper(_context, value):
        mapper_entered.set()
        release.wait(5)
        return value

    handle = start_stream_map(source, mapper, config=StreamMapConfig(2, 2))
    try:
        assert mapper_entered.wait(3)
        assert at_eof.wait(3)
        assert handle.cancel()
        release.set()
        result = handle.completion(3)
        assert (result.status, result.accepted, result.published) == ("cancelled", 1, 0)
        with pytest.raises(StopIteration):
            handle.next(0)
    finally:
        release.set()
        handle.close(5)


def test_cancel_full_pending_window_does_not_advance_source_again() -> None:
    source_advanced = Event()

    def source(_context):
        yield 1
        source_advanced.set()
        yield 2

    def mapper(_context, value):
        return value

    handle = start_stream_map(source, mapper, config=StreamMapConfig(1, 1))
    try:
        with handle._condition:
            assert handle._condition.wait_for(lambda: 0 in handle._completed, 3)
        assert not source_advanced.is_set()
        assert handle.cancel()
        assert handle.completion(3).status == "cancelled"
        assert not source_advanced.is_set()
        with pytest.raises(StopIteration):
            handle.next(0)
    finally:
        handle.close(5)


def test_source_cooperatively_acknowledges_cancellation() -> None:
    source_advancing = Event()
    pause = Event()

    def source(context):
        yield 1
        source_advancing.set()
        while not context.cancellation.cancelled:
            pause.wait(0.005)
        context.cancellation.raise_if_cancelled()

    def mapper(_context, value):
        return value

    handle = start_stream_map(source, mapper, config=StreamMapConfig(2, 2))
    try:
        assert source_advancing.wait(3)
        assert handle.cancel()
        assert handle.completion(3).status == "cancelled"
    finally:
        handle.close(5)


def test_unrequested_source_task_cancelled_is_failure() -> None:
    def source(_context):
        if False:
            yield None
        raise TaskCancelled("unexpected-source-cancel")

    def mapper(_context, value):
        return value

    handle = start_stream_map(source, mapper)
    try:
        with pytest.raises(TaskCancelled, match="unexpected-source-cancel"):
            handle.next(3)
        with pytest.raises(TaskCancelled, match="unexpected-source-cancel"):
            handle.completion(3)
    finally:
        handle.close(5)


def test_mapper_cannot_wait_for_its_own_owner() -> None:
    owner = []
    source_entered = Event()
    begin_map = Event()

    def source(_context):
        yield 4
        source_entered.set()

    def mapper(_context, value):
        begin_map.wait(5)
        with pytest.raises(RuntimeError, match="owned worker"):
            owner[0].completion(0)
        with pytest.raises(RuntimeError, match="owned worker"):
            owner[0].close(0)
        return value * 2

    handle = start_stream_map(source, mapper, config=StreamMapConfig(2, 2))
    owner.append(handle)
    try:
        assert source_entered.wait(3)
        begin_map.set()
        assert handle.next(3).value == 8
        assert handle.completion(3).status == "succeeded"
    finally:
        begin_map.set()
        handle.close(5)


def test_source_cannot_wait_for_its_own_coordinator() -> None:
    owner = []
    begin_source = Event()

    def source(_context):
        begin_source.wait(5)
        with pytest.raises(RuntimeError, match="owned worker"):
            owner[0].completion(0)
        yield 9

    def mapper(_context, value):
        return value

    handle = start_stream_map(source, mapper, config=StreamMapConfig(1, 1))
    owner.append(handle)
    try:
        begin_source.set()
        assert handle.next(3).value == 9
        assert handle.completion(3).status == "succeeded"
    finally:
        begin_source.set()
        handle.close(5)


def test_snapshot_limit_fails_without_publishing_result() -> None:
    def source(_context):
        yield "x" * 100

    def mapper(_context, value):
        return value

    handle = start_stream_map(
        source,
        mapper,
        config=StreamMapConfig(1, 1, max_item_bytes=16, max_result_bytes=128),
    )
    try:
        with pytest.raises(StreamMapSerializationError):
            handle.next(3)
        with pytest.raises(StreamMapSerializationError):
            handle.completion(3)
    finally:
        handle.close(5)


def test_map_result_snapshot_limit_is_a_failure_not_a_truncated_value() -> None:
    def source(_context):
        yield 1

    def mapper(_context, _value):
        return "x" * 100

    handle = start_stream_map(
        source,
        mapper,
        config=StreamMapConfig(1, 1, max_item_bytes=128, max_result_bytes=16),
    )
    try:
        with pytest.raises(StreamMapSerializationError):
            handle.next(3)
        with pytest.raises(StreamMapSerializationError):
            handle.completion(3)
    finally:
        handle.close(5)


def test_close_discards_result_even_after_successful_completion() -> None:
    def source(_context):
        yield 1

    def mapper(_context, value):
        return value + 1

    handle = start_stream_map(source, mapper, config=StreamMapConfig(2, 1))
    assert handle.completion(3).status == "succeeded"
    handle.close(3)
    assert handle.closed
    with pytest.raises(StopIteration):
        handle.next(0)


def test_concurrent_consumers_cannot_take_the_same_ordered_item() -> None:
    entered = Event()
    release = Event()
    first: list[object] = []

    def source(_context):
        yield 5

    def mapper(_context, value):
        entered.set()
        release.wait(5)
        return value + 1

    handle = start_stream_map(source, mapper, config=StreamMapConfig(1, 1))

    def read_first() -> None:
        try:
            first.append(handle.next(3))
        except BaseException as error:
            first.append(error)

    reader = Thread(target=read_first)
    reader.start()
    try:
        assert entered.wait(3)
        with handle._condition:
            assert handle._condition.wait_for(lambda: handle._reading, 3)
        with pytest.raises(RuntimeError, match="consuming reader"):
            handle.next(0)
        release.set()
        reader.join(3)
        assert not reader.is_alive()
        assert len(first) == 1
        assert getattr(first[0], "value", None) == 6
        assert handle.completion(3).published == 1
    finally:
        release.set()
        reader.join(3)
        handle.close(5)


def test_async_mapper_is_rejected_before_source_starts() -> None:
    source_called = Event()

    def source(_context):
        source_called.set()
        yield 1

    async def mapper(_context, value):
        return value

    with pytest.raises(ValidationError, match="synchronous"):
        start_stream_map(source, mapper)
    assert not source_called.is_set()


def test_sync_wrapper_returning_coroutine_is_closed_and_rejected() -> None:
    def source(_context):
        yield 1

    async def hidden(value):
        return value

    def mapper(_context, value):
        return hidden(value)

    handle = start_stream_map(source, mapper, config=StreamMapConfig(1, 1))
    try:
        with pytest.raises(ValidationError, match="synchronously"):
            handle.next(3)
    finally:
        handle.close(5)


@pytest.mark.parametrize(
    "config",
    [
        lambda: StreamMapConfig(max_pending=0),
        lambda: StreamMapConfig(max_pending=1, max_workers=2),
        lambda: StreamMapConfig(max_item_bytes=8_388_609),
        lambda: StreamMapConfig(max_pending=64, max_item_bytes=1_048_576),
    ],
)
def test_invalid_capacity_profiles_are_rejected(config) -> None:
    with pytest.raises(ValidationError):
        config()


def test_invalid_result_profile_is_rejected() -> None:
    with pytest.raises(ValidationError, match="status"):
        StreamMapResult("unknown", 0, 0)  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="published"):
        StreamMapResult("succeeded", 0, 1)
