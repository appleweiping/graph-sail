"""Offline proof that one spawned task can stop while a sibling succeeds."""

from __future__ import annotations

import multiprocessing
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from graph_sail import ExecutionConfig, TaskRegistry, graph_from_dict, start_process_graph


@dataclass(frozen=True)
class Stop:
    marker: str

    def __call__(self, context):
        Path(self.marker).write_text(str(os.getpid()), encoding="ascii")
        while True:
            context.cancellation.raise_if_cancelled()
            time.sleep(0.005)


def answer(context):
    return (os.getpid(), 6 * 7)


def main() -> None:
    graph = graph_from_dict(
        {
            "name": "one-node-cancel",
            "devices": [{"name": "cpu", "memory_mb": 4}],
            "nodes": [
                {"id": node, "kind": "task", "memory_mb": 1, "latency_ms": {"cpu": 1}}
                for node in ("answer", "stop")
            ],
            "edges": [],
        }
    )
    before = {child.pid for child in multiprocessing.active_children()}
    with tempfile.TemporaryDirectory(prefix="graph-sail-node-cancel-") as directory:
        marker = Path(directory) / "stop.pid"
        with start_process_graph(
            graph,
            TaskRegistry({"answer": answer, "stop": Stop(str(marker))}),
            {"answer": "cpu", "stop": "cpu"},
            config=ExecutionConfig(max_workers=2, device_workers={"cpu": 2}),
        ) as handle:
            deadline = time.monotonic() + 35
            stopped_pid = None
            while time.monotonic() < deadline:
                if marker.exists():
                    value = marker.read_text(encoding="ascii")
                    if value.isdecimal():
                        stopped_pid = int(value)
                        break
                time.sleep(0.005)
            if stopped_pid is None:
                raise RuntimeError("spawned stop task did not enter")
            if not handle.cancel_node("stop"):
                raise RuntimeError("stop task was not cancellable")
            result = handle.result(35)
            child_pid, value = result.execution.outputs["answer"]
            if child_pid == os.getpid() or value != 42:
                raise RuntimeError("independent process answer was not 42")
            if result.execution.status != "cancelled":
                raise RuntimeError("selective stop did not settle as cancelled")
            if not any(
                worker.pid == stopped_pid and worker.cancellation_requested
                for worker in result.workers
            ):
                raise RuntimeError("cancelled child evidence is missing")
    if {child.pid for child in multiprocessing.active_children()} != before:
        raise RuntimeError("process node cancellation left an owned child")
    print("process node cancellation: sibling 42, joined child, no replay")


if __name__ == "__main__":
    main()
