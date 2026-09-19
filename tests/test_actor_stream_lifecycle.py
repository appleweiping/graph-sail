"""Real spawned cancellation, actor ownership, observers and failure boundaries."""

import asyncio
import multiprocessing
import threading

import pytest

import graph_sail as gs
from tests.actor_stream_functions import InvalidStream
from tests.test_actor_method_streams import actor, until


def reaped(owner):
    assert not owner.alive
    assert owner.exitcode is not None
    assert all(child.pid != owner.pid for child in multiprocessing.active_children())
    assert not owner._thread.is_alive()


@pytest.mark.parametrize("cooperative", [True, False])
def test_running_cancel_retires_real_actor_and_wait_cancel_does_not(tmp_path, cooperative):
    owner = actor()
    marker = tmp_path / "entered"
    stream = owner.stream(
        "wait",
        args=(str(marker), cooperative),
        stream_config=gs.ActorStreamConfig(cancellation_grace_seconds=0.05),
    )
    try:
        until(marker.exists)

        async def wait_only():
            task = asyncio.create_task(stream.next_async())
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert owner.alive
            with pytest.raises(TimeoutError):
                await stream.completion_async(0)

        asyncio.run(wait_only())
        assert stream.cancel()
        assert not stream.cancel()
        assert stream.completion(10).status == "cancelled"
        assert list(stream) == []
    finally:
        stream.close()
        owner.close()
    reaped(owner)
    assert stream.closed and stream.state.retirement_requested
    assert not stream.state.actor_reusable_at_release
    if cooperative:
        assert (tmp_path / "entered.closed").exists()


@pytest.mark.parametrize("action", ["close", "terminate"])
def test_actor_shutdown_wakes_full_mailbox_without_consumer(action):
    owner = actor()
    stream = owner.stream("values", args=(100,), config=gs.TaskStreamConfig(max_buffered=1))
    try:
        assert stream.wait_ready(5)
        getattr(owner, action)()
    finally:
        stream.close()
        owner.close()
    reaped(owner)
    assert stream.closed


@pytest.mark.parametrize("kind", ["exit", "interrupt"])
def test_child_controls_are_worker_death_not_parent_controls(kind):
    owner = actor()
    stream = owner.stream("control", args=(kind,))
    try:
        assert stream.next(5).value == 1
        with pytest.raises(gs.ActorDiedError):
            stream.next(10)
    finally:
        stream.close()
        owner.close()
    reaped(owner)


def test_illegal_close_poison_and_real_finish_deadline(tmp_path):
    for method, args in (("illegal", ()), ("close_wait", (str(tmp_path / "finish"),))):
        owner = actor()
        stream = owner.stream(
            method,
            args=args,
            config=gs.TaskStreamConfig(max_yields=1),
            stream_config=gs.ActorStreamConfig(
                finish_timeout_seconds=0.05, shutdown_timeout_seconds=0
            ),
        )
        try:
            assert stream.next(5).value == 1
            with pytest.raises((gs.ActorRemoteError, gs.ActorTimeoutError)):
                stream.completion(10)
        finally:
            stream.close()
            owner.close()
        assert not stream.state.generator_closed
        assert not stream.state.actor_reusable_at_release
        reaped(owner)


def test_pending_ordinary_call_blocks_stream_admission(tmp_path):
    owner = actor()
    marker, release = tmp_path / "entered", tmp_path / "release"
    try:
        running = owner.submit("block", args=(str(marker), str(release)))
        until(marker.exists)
        queued = owner.submit("add", args=(2,))
        queued.cancel()
        with pytest.raises(gs.ActorStreamBusyError):
            owner.stream("values")
        release.touch()
        running.result(5)
        until(lambda: owner.pending_count == 0)
        with owner.stream("values", args=(0,)) as stream:
            assert list(stream) == []
    finally:
        release.touch()
        owner.close()


@pytest.mark.parametrize("method", ["ordinary", "coroutine", "asynchronous", "static"])
def test_actual_spawn_rejects_wrong_generator_binding(method):
    with pytest.raises(gs.ActorStartupError):
        gs.ProcessActor(
            gs.ActorRegistry({"bad": gs.ActorDefinition(InvalidStream, ("state",), (method,))}),
            "bad",
        )


def test_fresh_async_failure_wrappers_and_cross_loop_consumption():
    with actor() as owner:
        with owner.stream("values", args=(3, "source")) as stream:
            assert stream.next(5).value == 1

            async def failure():
                with pytest.raises(gs.AsyncSourceError) as caught:
                    await stream.next_async(5)
                return caught.value

            first = asyncio.run(failure())
            second = asyncio.run(failure())
            assert first is not second and first.__cause__ is second.__cause__
        with owner.stream("values", args=(20,)) as stream:
            results, errors = [], []

            def consume():
                async def run():
                    async for item in stream:
                        results.append(item.sequence)

                try:
                    asyncio.run(run())
                except BaseException as error:
                    errors.append(error)

            threads = [threading.Thread(target=consume) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(10)
                assert not thread.is_alive()
            assert not errors and sorted(results) == list(range(20))


def test_positive_finish_uses_shorter_actor_cap_and_waits_are_observers(tmp_path):
    owner = actor(config=gs.ActorConfig(method_timeout_seconds=0.05))
    stream = owner.stream(
        "close_wait",
        args=(str(tmp_path / "finish"),),
        config=gs.TaskStreamConfig(max_yields=1),
        stream_config=gs.ActorStreamConfig(finish_timeout_seconds=30),
    )
    try:

        async def observe():
            assert await stream.wait_ready_async(5)
            assert (await stream.next_async()).value == 1
            with pytest.raises(gs.AsyncSourceError) as caught:
                await stream.completion_async(10)
            assert isinstance(caught.value.__cause__, gs.ActorTimeoutError)

        asyncio.run(observe())
        assert stream.done()
    finally:
        stream.close()
        owner.close()
    reaped(owner)


def test_async_completion_success_keeps_same_actor():
    with actor() as owner:
        with owner.stream("values", args=(2,)) as stream:

            async def observe():
                result = await stream.completion_async(5)
                assert result.status == "succeeded" and result.state.actor_reusable_at_release
                assert [item.value async for item in stream] == [1, 2]

            asyncio.run(observe())
        assert owner.submit("add").result(5) == 102
