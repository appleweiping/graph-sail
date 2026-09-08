"""Actual fractional parallelism followed by whole-pool dependent admission."""

from __future__ import annotations

import json
from threading import Barrier

from graph_sail import ExecutionConfig, LogicalResources, TaskRegistry, graph_from_dict, start_graph


def main() -> None:
    graph = graph_from_dict(
        {
            "name": "fractional-local-example",
            "devices": [{"name": "cpu", "memory_mb": 8}],
            "nodes": [
                {"id": node, "kind": "example", "memory_mb": 1, "latency_ms": {"cpu": 1}}
                for node in ("left", "right", "sum")
            ],
            "edges": [
                {"source": "left", "target": "sum"},
                {"source": "right", "target": "sum"},
            ],
        }
    )
    barrier = Barrier(2, timeout=10)

    def left(_):
        barrier.wait()
        return 17

    def right(_):
        barrier.wait()
        return 25

    def total(context):
        return context.dependencies["left"] + context.dependencies["right"]

    policy = LogicalResources(
        {"CPU": 1, "license": 1},
        {"left": {"CPU": 0.5, "license": 1}, "right": {"CPU": 0.5}, "sum": {"CPU": 1}},
    )
    with start_graph(
        graph,
        TaskRegistry({"left": left, "right": right, "sum": total}),
        dict.fromkeys(("left", "right", "sum"), "cpu"),
        config=ExecutionConfig(2, {"cpu": 2}, resources=policy),
    ) as handle:
        result = handle.result(15)
    assert result.status == "succeeded" and result.outputs["sum"] == 42
    usage = result.resource_usage
    assert usage is not None
    assert usage.peak_units == {"CPU": 10_000, "license": 10_000}
    assert usage.reservations == usage.releases == 3
    print(json.dumps({"result": 42, "resources": usage.to_dict()}, sort_keys=True))


if __name__ == "__main__":
    main()
