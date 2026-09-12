"""Offline async consumers; synchronous startup/close stays outside the event loop."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path
from threading import Event

from graph_sail import TaskStreamConfig, start_process_task_stream, start_task_stream


def process_squares(context, marker):
    try:
        for index in range(4):
            context.cancellation.raise_if_cancelled()
            yield {"pid": os.getpid(), "square": index * index}
    finally:
        Path(marker).write_text("generator closed", encoding="ascii")


async def observe(thread_stream, process_stream, gate):
    async def read_thread():
        first = await thread_stream.next_async(5)
        assert first.sequence == 0 and first.value == 0
        assert not gate.is_set() and not thread_stream.done()
        for _ in range(20):
            await asyncio.sleep(0)  # Independent application event-loop work.
        gate.set()
        rows = [first, *[item async for item in thread_stream]]
        assert [(item.sequence, item.value) for item in rows] == [(i, i * i) for i in range(4)]
        result = await thread_stream.completion_async(5)
        assert result.status == "succeeded" and result.produced == 4
        return [item.value for item in rows]

    async def read_process():
        rows = [item async for item in process_stream]
        result = await process_stream.completion_async(10)
        assert [item.sequence for item in rows] == [0, 1, 2, 3]
        assert [item.value["square"] for item in rows] == [0, 1, 4, 9]
        assert {item.value["pid"] for item in rows} == {result.worker.pid}
        assert result.worker.pid != os.getpid()
        assert result.status == "limited" and result.produced == 4
        assert result.worker.generator_closed and result.worker.resources_closed
        return [item.value["square"] for item in rows]

    return await asyncio.gather(read_thread(), read_process())


def main():
    gate = Event()

    def thread_squares(context):
        yield 0
        assert gate.wait(10)
        for index in range(1, 4):
            context.cancellation.raise_if_cancelled()
            yield index * index

    with tempfile.TemporaryDirectory(prefix="graph-sail-async-stream-") as directory:
        marker = Path(directory) / "finally.txt"
        with (
            start_task_stream(thread_squares, config=TaskStreamConfig(max_buffered=1)) as thread,
            start_process_task_stream(
                process_squares,
                args=(str(marker),),
                config=TaskStreamConfig(max_buffered=1, max_yields=4),
            ) as process,
        ):
            try:
                values = asyncio.run(observe(thread, process, gate))
            finally:
                gate.set()
        assert thread.closed and process.closed
        assert marker.read_text(encoding="ascii") == "generator closed"
        print(json.dumps({"thread": values[0], "process": values[1], "owners_closed": True}))


if __name__ == "__main__":
    main()
