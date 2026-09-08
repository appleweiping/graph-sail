"""Offline process DAG: an immutable 2 MiB object feeds two independent readers."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass

from graph_sail import (
    ExecutionConfig,
    LocalObjectClient,
    LocalObjectStore,
    ObjectRef,
    ProcessTaskConfig,
    TaskContext,
    TaskRegistry,
    execute_process_graph,
    graph_from_dict,
)


@dataclass(frozen=True)
class Source:
    ref: ObjectRef

    def __call__(self, context: TaskContext) -> ObjectRef:
        return self.ref


@dataclass(frozen=True)
class InspectBytes:
    client: LocalObjectClient

    def __call__(self, context: TaskContext) -> tuple[int, int, str, int]:
        ref = context.dependencies["source"]
        if not isinstance(ref, ObjectRef):
            raise TypeError("source must return an ObjectRef")
        data = self.client.get(ref)
        return len(data), sum(data), hashlib.sha256(data).hexdigest(), os.getpid()


def verify(context: TaskContext) -> dict[str, object]:
    left, right = context.dependencies["left"], context.dependencies["right"]
    if not isinstance(left, tuple) or not isinstance(right, tuple) or left[:3] != right[:3]:
        raise ValueError("independent readers disagreed")
    if left[:2] != (2097152, 267386880):
        raise ValueError("generated-byte oracle failed")
    return {"bytes": left[0], "sum": left[1], "sha256": left[2], "reader_pids": [left[3], right[3]]}


def main() -> None:
    graph = graph_from_dict(
        {
            "name": "offline-object-process-dag",
            "devices": [{"name": "cpu", "memory_mb": 32}],
            "nodes": [
                {"id": name, "kind": "python", "memory_mb": 4, "latency_ms": {"cpu": 1}}
                for name in ("source", "left", "right", "verify")
            ],
            "edges": [
                {"source": source, "target": target}
                for source, target in (
                    ("source", "left"),
                    ("source", "right"),
                    ("left", "verify"),
                    ("right", "verify"),
                )
            ],
        }
    )
    with (
        tempfile.TemporaryDirectory(prefix="graph-sail-process-example-") as directory,
        LocalObjectStore(directory) as store,
    ):
        ref = store.put(bytes(range(256)) * 8192)
        result = execute_process_graph(
            graph,
            TaskRegistry(
                {
                    "source": Source(ref),
                    "left": InspectBytes(store.client()),
                    "right": InspectBytes(store.client()),
                    "verify": verify,
                }
            ),
            dict.fromkeys(("source", "left", "right", "verify"), "cpu"),
            config=ExecutionConfig(max_workers=2, device_workers={"cpu": 2}),
            process_config=ProcessTaskConfig(max_message_bytes=4096),
        )
        if result.execution.status != "succeeded":
            raise RuntimeError(result.to_dict())
        print(json.dumps(result.execution.outputs["verify"], indent=2))
        print(json.dumps(result.to_dict(), indent=2))


if __name__ == "__main__":
    main()
