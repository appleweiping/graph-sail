from __future__ import annotations

import pytest

from graph_sail import BeamPlanner, GraphSpec, simulate_plan
from graph_sail.errors import PlanningError
from graph_sail.io import graph_from_dict


def _graph() -> GraphSpec:
    return graph_from_dict(
        {
            "name": "simulation",
            "devices": [{"name": "cpu", "memory_mb": 1024}],
            "nodes": [
                {"id": "a", "kind": "text", "memory_mb": 10, "latency_ms": {"cpu": 2}},
                {"id": "b", "kind": "text", "memory_mb": 20, "latency_ms": {"cpu": 3}},
            ],
            "edges": [{"source": "a", "target": "b", "payload_mb": 0}],
        }
    )


def test_simulation_accounts_for_serial_schedule() -> None:
    graph = _graph()
    plan = BeamPlanner(beam_width=2).plan(graph)
    result = simulate_plan(graph, plan)
    assert result.makespan_ms == pytest.approx(5)
    assert result.device_busy_ms == {"cpu": pytest.approx(5)}
    assert result.device_utilization == {"cpu": pytest.approx(1)}
    assert result.peak_memory_mb == {"cpu": pytest.approx(30)}
    assert [event.node for event in result.events] == ["a", "b"]
    assert result.to_dict()["kind"] == "graph-sail-simulation"


def test_simulation_rejects_wrong_graph() -> None:
    graph = _graph()
    plan = BeamPlanner().plan(graph)
    other = graph_from_dict(
        {
            "name": "other",
            "devices": [{"name": "cpu", "memory_mb": 1024}],
            "nodes": [{"id": "a", "kind": "text", "memory_mb": 10, "latency_ms": {"cpu": 2}}],
            "edges": [],
        }
    )
    with pytest.raises(PlanningError):
        simulate_plan(other, plan)
