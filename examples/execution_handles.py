"""Prove selective results are available while another real branch is blocked."""

from threading import Event

from graph_sail import ExecutionConfig, TaskRegistry, graph_from_dict, start_graph


def main() -> None:
    entered, release = Event(), Event()
    graph = graph_from_dict(
        {
            "name": "partial-results",
            "devices": [{"name": "cpu", "memory_mb": 10}],
            "nodes": [
                {"id": node, "kind": "demo", "memory_mb": 1, "latency_ms": {"cpu": 1}}
                for node in ("a", "b", "c")
            ],
            "edges": [{"source": "a", "target": "c"}],
        }
    )

    def independent(_):
        entered.set()
        if not release.wait(10):
            raise RuntimeError("example did not release its blocked branch")
        return 7

    with start_graph(
        graph,
        TaskRegistry(
            {"a": lambda _: 6, "b": independent, "c": lambda ctx: ctx.dependencies["a"] * 2}
        ),
        dict.fromkeys(("a", "b", "c"), "cpu"),
        config=ExecutionConfig(max_workers=2, device_workers={"cpu": 2}),
    ) as handle:
        try:
            assert entered.wait(5)
            assert handle.node("c").result(5) == 12
            assert not handle.node("b").done()
            partial = handle.wait(("c", "a", "b"), count=2)
            assert partial.ready == ("c", "a") and partial.pending == ("b",)
            print(f"ready={partial.ready}, pending={partial.pending}")
        finally:
            release.set()
        result = handle.result(5)
        assert result.status == "succeeded" and result.outputs == {"a": 6, "b": 7, "c": 12}
        print(dict(result.outputs))
    assert handle.closed


if __name__ == "__main__":
    main()
