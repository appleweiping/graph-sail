"""Offline async observations; sync owners still explicitly join their workers."""

import asyncio
import os
from threading import Event

from graph_sail import (
    ActorDefinition,
    ActorRegistry,
    ExecutionConfig,
    ProcessActor,
    TaskRegistry,
    graph_from_dict,
    start_graph,
)


class SumActor:
    def __init__(self) -> None:
        self.total = 0

    def add(self, value: int) -> tuple[int, int]:
        self.total += value
        return self.total, os.getpid()


def main() -> None:
    registry = ActorRegistry({"sum": ActorDefinition(SumActor, ("add",))})
    with ProcessActor(registry, "sum") as actor:

        async def observe_actor() -> None:
            calls = [actor.submit("add", args=(value,)) for value in (2, 5, 11)]
            results = await asyncio.gather(*(call.result_async(35) for call in calls))
            assert [result[0] for result in results] == [2, 7, 18]
            assert all(result[1] == actor.pid != os.getpid() for result in results)
            print("actor running totals:", [result[0] for result in results])

        asyncio.run(observe_actor())
    assert not actor.alive and actor.exitcode == 0

    release = Event()
    graph = graph_from_dict(
        {
            "name": "async-results",
            "devices": [{"name": "cpu", "memory_mb": 10}],
            "nodes": [
                {"id": node, "kind": "demo", "memory_mb": 1, "latency_ms": {"cpu": 1}}
                for node in ("a", "b", "c")
            ],
            "edges": [{"source": "a", "target": "c"}],
        }
    )

    def blocked(_):
        if not release.wait(10):
            raise RuntimeError("example did not release its blocked task")
        return 7

    with start_graph(
        graph,
        TaskRegistry({"a": lambda _: 6, "b": blocked, "c": lambda ctx: ctx.dependencies["a"] * 2}),
        dict.fromkeys(("a", "b", "c"), "cpu"),
        config=ExecutionConfig(max_workers=2, device_workers={"cpu": 2}),
    ) as handle:

        async def observe_graph() -> None:
            waiting = asyncio.create_task(handle.node("b").result_async())
            try:
                await asyncio.sleep(0)
                waiting.cancel("stop waiting, not working")
                cancelled = await asyncio.gather(waiting, return_exceptions=True)
                assert isinstance(cancelled[0], asyncio.CancelledError)
                assert await handle.node("c").result_async(5) == 12
                partial = await handle.wait_async(("c", "b", "a"), count=2)
                assert partial.ready == ("c", "a") and partial.pending == ("b",)
                print("partial:", partial)
            finally:
                release.set()
                if not waiting.done():
                    waiting.cancel()
                await asyncio.gather(waiting, return_exceptions=True)
            result = await handle.result_async(5)
            assert result.outputs == {"a": 6, "b": 7, "c": 12}
            print("final:", dict(result.outputs))

        asyncio.run(observe_graph())
    assert handle.closed


if __name__ == "__main__":
    main()
