"""Offline native-process streaming demo. Run as a guarded importable file."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from graph_sail import ProcessStreamConfig, TaskStreamConfig, start_process_task_stream


def squares(context, count, marker):
    try:
        for index in range(count):
            context.cancellation.raise_if_cancelled()
            yield {"pid": os.getpid(), "square": index * index}
    finally:
        Path(marker).write_text("producer finally executed", encoding="ascii")


def main():
    with tempfile.TemporaryDirectory(prefix="graph-sail-process-stream-") as directory:
        marker = Path(directory) / "finally.txt"
        with start_process_task_stream(
            squares,
            args=(4, str(marker)),
            config=TaskStreamConfig(max_buffered=2, max_yields=4),
            process_config=ProcessStreamConfig(max_message_bytes=4096),
        ) as stream:
            values = [item.value for item in stream]
            result = stream.completion()
            assert [value["square"] for value in values] == [0, 1, 4, 9]
            assert {value["pid"] for value in values} == {result.worker.pid}
            assert result.worker.pid != os.getpid()
            assert result.status == "limited"  # No fifth pull to probe for EOF.
            assert result.worker.generator_closed and result.worker.resources_closed
            assert marker.read_text(encoding="ascii") == "producer finally executed"
        assert stream.closed
        print(
            {
                "squares": [0, 1, 4, 9],
                "worker": result.worker.pid,
                "status": result.status,
                "native_resources_closed": stream.closed,
            }
        )


if __name__ == "__main__":
    main()
