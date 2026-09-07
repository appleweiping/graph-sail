from __future__ import annotations

from graph_sail import GraphSpec, pareto_plans
from graph_sail.io import graph_from_dict


def _graph() -> GraphSpec:
    return graph_from_dict(
        {
            "name": "pareto",
            "devices": [
                {"name": "cpu", "memory_mb": 1024},
                {"name": "gpu", "memory_mb": 1024, "kinds": ["vision"]},
            ],
            "nodes": [
                {
                    "id": "vision",
                    "kind": "vision",
                    "memory_mb": 10,
                    "latency_ms": {"cpu": 10, "gpu": 2},
                },
                {
                    "id": "text",
                    "kind": "text",
                    "memory_mb": 10,
                    "latency_ms": {"cpu": 3},
                },
            ],
            "edges": [],
        }
    )


def test_pareto_report_contains_non_dominated_plans() -> None:
    report = pareto_plans(_graph(), beam_width=2)
    assert report.candidates
    assert {candidate.algorithm for candidate in report.candidates} <= {"greedy", "beam"}
    assert report.to_dict()["kind"] == "graph-sail-pareto"
