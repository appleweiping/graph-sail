"""Execute trusted local multimodal-style branches and join their actual values."""

from graph_sail import ExecutionConfig, GreedyPlanner, TaskRegistry, execute_graph, graph_from_dict

graph = graph_from_dict(
    {
        "name": "local-branch-example",
        "devices": [{"name": "cpu", "memory_mb": 16}],
        "nodes": [
            {"id": node, "kind": "python", "memory_mb": 1, "latency_ms": {"cpu": 1}}
            for node in ("image", "audio", "join")
        ],
        "edges": [{"source": source, "target": "join"} for source in ("image", "audio")],
    }
)
registry = TaskRegistry(
    {
        "image": lambda context: {"pixels": 640 * 480},
        "audio": lambda context: {"samples": 16_000},
        "join": lambda context: dict(context.dependencies),
    }
)
result = execute_graph(
    graph,
    registry,
    GreedyPlanner().plan(graph).placements,
    config=ExecutionConfig(max_workers=2, device_workers={"cpu": 2}),
)
assert result.status == "succeeded"
assert result.outputs["join"] == {"audio": {"samples": 16_000}, "image": {"pixels": 307_200}}
print(result.outputs["join"])
print("Measured elapsed milliseconds:", result.elapsed_ms)
