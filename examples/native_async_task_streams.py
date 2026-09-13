"""Offline, event-gated native async production and awaited cleanup."""

from __future__ import annotations

import asyncio
import json

from graph_sail import TaskStreamConfig, start_async_task_stream


def expect(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


async def demonstrate():
    source_started, release_source = asyncio.Event(), asyncio.Event()
    cleanup_started, release_cleanup = asyncio.Event(), asyncio.Event()
    cleanup_finished = []
    advances = []
    values = [{"square": index * index} for index in range(4)]

    async def squares(context):
        source_started.set()
        try:
            await release_source.wait()
            for index, value in enumerate(values):
                context.cancellation.raise_if_cancelled()
                advances.append(index)
                yield value
            raise AssertionError("the capped source was advanced once too far")
        finally:
            cleanup_started.set()
            await release_cleanup.wait()
            cleanup_finished.append(True)

    stream = start_async_task_stream(
        squares,
        config=TaskStreamConfig(max_buffered=1, max_yields=4),
    )
    try:
        await source_started.wait()
        expect(not stream.done() and not advances, "source advanced before application release")
        # This application action can proceed while the native source awaits.
        release_source.set()
        rows = [await stream.next_async(5) for _ in range(4)]
        expect([item.sequence for item in rows] == [0, 1, 2, 3], "accepted sequence differs")
        expect(
            all(item.value is value for item, value in zip(rows, values, strict=True)),
            "borrowed object identity changed",
        )
        await cleanup_started.wait()
        expect(
            advances == [0, 1, 2, 3] and not stream.done() and not cleanup_finished,
            "yield cap or pending cleanup state differs",
        )
        cancelled = stream.cancel()  # This operation must also execute under -O.
        expect(not cancelled, "known cleanup accepted a new cancellation")
        release_cleanup.set()
        result = await stream.completion_async(5)
        expect((result.status, result.produced) == ("limited", 4), "terminal result differs")
        expect(cleanup_finished == [True], "owned cleanup was not awaited exactly once")
    finally:
        release_source.set()
        release_cleanup.set()
        await stream.aclose(5)
    expect(stream.closed and not stream.cleanup_incomplete, "owner closure was not acknowledged")
    return {
        "native_async": [item.value["square"] for item in rows],
        "status": result.status,
        "awaited_cleanup": cleanup_finished == [True],
        "owner_closed": stream.closed,
    }


if __name__ == "__main__":
    print(json.dumps(asyncio.run(demonstrate()), sort_keys=True))
