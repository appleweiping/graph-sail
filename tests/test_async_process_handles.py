"""Real spawn transport completion, not a simulated future-only adapter test."""

import asyncio
import multiprocessing
import os

import pytest
from async_handle_tasks import MarkerGate
from process_execution_tasks import Value, pid_task
from test_execution import graph_for

import graph_sail as api


@pytest.fixture(autouse=True)
def no_owned_children_left():
    before = {child.pid for child in multiprocessing.active_children()}
    yield
    assert {child.pid for child in multiprocessing.active_children()} == before


def test_real_actor_wait_cancel_timeout_remote_failure_and_owner_cleanup(tmp_path):
    entered, release = tmp_path / "entered", tmp_path / "release"
    registry = api.ActorRegistry({"gate": api.ActorDefinition(MarkerGate, ("read", "fail", "pid"))})
    with api.ProcessActor(registry, "gate") as actor:
        call = actor.submit("read", args=(str(entered), str(release)))

        async def scenario():
            try:
                async with asyncio.timeout(35):
                    while not entered.exists():
                        await asyncio.sleep(0.005)
                waiting = asyncio.create_task(call.result_async())
                await asyncio.sleep(0)
                assert call._completion.waiter_count == 1
                waiting.cancel("wait only")
                with pytest.raises(asyncio.CancelledError, match="wait only"):
                    await waiting
                assert call.running() and not call.cancelled()
                with pytest.raises(TimeoutError):
                    await call.result_async(0)
                waiting = asyncio.create_task(call.result_async(35))
                await asyncio.sleep(0)
                release.write_text("released", encoding="ascii")
                value = await waiting
                assert value == (9, int(entered.read_text(encoding="ascii")))
                assert value[1] != os.getpid()
                assert await call.result_async(0) is value
                failed = actor.submit("fail")
                with pytest.raises(api.AsyncSourceError) as caught:
                    await failed.result_async(35)
                assert isinstance(caught.value.__cause__, api.ActorRemoteError)
                assert caught.value.__cause__.remote_type == "ValueError"
                assert await actor.submit("pid").result_async(35) == value[1]
                assert actor.alive
            finally:
                release.write_text("released", encoding="ascii")

        asyncio.run(scenario())
    assert not actor.alive and actor.exitcode == 0


def test_real_process_dag_async_dependencies_wait_and_final_joined_result():
    graph = graph_for(("a", "b"), (("a", "b"),))
    with api.start_process_graph(
        graph,
        api.TaskRegistry({"a": Value(6), "b": pid_task}),
        {"a": "cpu", "b": "cpu"},
        config=api.ExecutionConfig(max_workers=1),
    ) as handle:

        async def scenario():
            assert await handle.node("a").result_async(35) == 6
            metadata = await handle.node("b").execution_async(35)
            assert metadata.status == "succeeded"
            assert await handle.wait_async(("b", "a"), count=2) == api.WaitResult(("b", "a"), ())
            value = await handle.node("b").result_async(0)
            assert value[0] != os.getpid() and value[1:] == ("b", 1, {"a": 6})
            result = await handle.result_async(35)
            assert result.execution.outputs["b"] is value
            assert all(worker.exitcode == 0 for worker in result.workers)
            assert not handle.closed
            assert handle._completion.waiter_count == 0

        asyncio.run(scenario())
    assert handle.closed
