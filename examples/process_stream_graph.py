"""Offline process-backed DAG per yield with a hand-derived arithmetic oracle."""

from __future__ import annotations

import os
from threading import Event

from graph_sail import (
    ExecutionConfig,
    ProcessTaskConfig,
    StreamMapConfig,
    TaskRegistry,
    start_process_stream_graph,
)
from graph_sail.models import DeviceSpec, EdgeSpec, GraphSpec, NodeSpec


def left(context):
    return context.dependencies["input"] * 2, os.getpid()


def right(context):
    return context.dependencies["input"] + 1, os.getpid()


def join(context):
    left_value, left_pid = context.dependencies["left"]
    right_value, right_pid = context.dependencies["right"]
    return left_value + right_value, (left_pid, right_pid, os.getpid())


def _graph() -> GraphSpec:
    return GraphSpec(
        "process-stream-example",
        (DeviceSpec("cpu", 64),),
        tuple(NodeSpec(node, "work", 1, {"cpu": 1}) for node in ("input", "left", "right", "join")),
        (
            EdgeSpec("input", "left"),
            EdgeSpec("input", "right"),
            EdgeSpec("left", "join"),
            EdgeSpec("right", "join"),
        ),
    )


def main() -> None:
    release = Event()
    source_ended = Event()

    def source(context):
        yield 4
        release.wait(30)
        context.cancellation.raise_if_cancelled()
        source_ended.set()

    try:
        with start_process_stream_graph(
            source,
            _graph(),
            TaskRegistry({"left": left, "right": right, "join": join}),
            dict.fromkeys(("input", "left", "right", "join"), "cpu"),
            input_node="input",
            output_node="join",
            config=StreamMapConfig(max_pending=1, max_workers=1),
            execution_config=ExecutionConfig(max_workers=3, device_workers={"cpu": 3}),
            process_config=ProcessTaskConfig(max_message_bytes=1_048_576),
        ) as stream:
            item = stream.next(60)
            value, pids = item.value
            if item.sequence != 0 or value != 13 or any(pid == os.getpid() for pid in pids):
                raise RuntimeError("spawned process graph violated the independent oracle")
            if source_ended.is_set():
                raise RuntimeError("source ended before the first process graph output")
            release.set()
            if stream.completion(60).status != "succeeded":
                raise RuntimeError("process stream graph did not finish successfully")
            print(f"sequence={item.sequence} value={value} child_pids={pids}")
    finally:
        release.set()


if __name__ == "__main__":
    main()
