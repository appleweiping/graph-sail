from __future__ import annotations

import pytest

from graph_sail.demo import demo_graph, demo_payload
from graph_sail.errors import PlanningError
from graph_sail.io import graph_from_dict
from graph_sail.models import LinkSpec, PlanResult
from graph_sail.planner import BeamPlanner, GreedyPlanner


def test_demo_beam_plan_is_deterministic():
    graph = demo_graph()
    first = BeamPlanner().plan(graph)
    second = BeamPlanner().plan(graph)
    assert first.to_dict() == second.to_dict()
    assert first.placements == {
        "decode-audio": "cpu",
        "audio-encoder": "gpu-1",
        "decode-image": "cpu",
        "vision-encoder": "gpu-0",
        "language-core": "gpu-0",
        "format-response": "cpu",
    }


def test_plan_result_summary_properties():
    plan = BeamPlanner().plan(demo_graph())
    assert plan.makespan_ms == pytest.approx(53.135)
    assert plan.critical_node == "format-response"
    assert plan.algorithm == "beam-earliest-finish:16"


def test_greedy_chooses_earliest_finish():
    plan = GreedyPlanner().plan(demo_graph())
    assert plan.placements["vision-encoder"] == "gpu-0"


def test_beam_can_avoid_greedy_memory_dead_end():
    payload = {
        "name": "memory-lookahead",
        "devices": [
            {"name": "cpu", "memory_mb": 10},
            {"name": "gpu", "memory_mb": 10},
        ],
        "nodes": [
            {"id": "a", "kind": "x", "memory_mb": 5, "latency_ms": {"cpu": 2, "gpu": 1}},
            {
                "id": "b",
                "kind": "x",
                "memory_mb": 6,
                "latency_ms": {"gpu": 1},
                "pinned_device": "gpu",
            },
        ],
        "edges": [],
    }
    graph = graph_from_dict(payload)
    with pytest.raises(PlanningError, match=r"no feasible placement.*b"):
        GreedyPlanner().plan(graph)
    assert BeamPlanner(beam_width=4).plan(graph).placements == {"a": "cpu", "b": "gpu"}


def test_same_device_nodes_are_serialized():
    payload = demo_payload()
    payload["nodes"] = [
        {"id": "a", "kind": "decode", "memory_mb": 1, "latency_ms": {"cpu": 2}},
        {"id": "b", "kind": "decode", "memory_mb": 1, "latency_ms": {"cpu": 3}},
    ]
    payload["edges"] = []
    graph = graph_from_dict(payload)
    plan = GreedyPlanner().plan(graph)
    assert [(item.start_ms, item.finish_ms) for item in plan.schedule] == [(0, 2), (2, 5)]


def test_independent_nodes_can_overlap_on_different_devices():
    payload = {
        "name": "parallel",
        "devices": [{"name": "a", "memory_mb": 10}, {"name": "b", "memory_mb": 10}],
        "nodes": [
            {"id": "x", "kind": "k", "memory_mb": 10, "latency_ms": {"a": 5}},
            {"id": "y", "kind": "k", "memory_mb": 10, "latency_ms": {"b": 7}},
        ],
        "edges": [],
    }
    plan = GreedyPlanner().plan(graph_from_dict(payload))
    assert [item.start_ms for item in plan.schedule] == [0, 0]
    assert plan.makespan_ms == 7


def test_cross_device_dependency_waits_for_transfer():
    payload = {
        "name": "transfer",
        "default_bandwidth_mb_s": 1_000,
        "default_link_latency_ms": 1,
        "devices": [{"name": "a", "memory_mb": 10}, {"name": "b", "memory_mb": 10}],
        "nodes": [
            {"id": "x", "kind": "k", "memory_mb": 1, "latency_ms": {"a": 2}},
            {"id": "y", "kind": "k", "memory_mb": 1, "latency_ms": {"b": 3}},
        ],
        "edges": [{"source": "x", "target": "y", "payload_mb": 4}],
    }
    item = GreedyPlanner().plan(graph_from_dict(payload)).schedule[-1]
    assert item.start_ms == pytest.approx(7)
    assert item.incoming_transfer_ms == pytest.approx(5)


def test_transfer_arithmetic_overflow_is_rejected():
    payload = {
        "default_bandwidth_mb_s": 1e-308,
        "devices": [{"name": "a", "memory_mb": 1}, {"name": "b", "memory_mb": 1}],
        "nodes": [
            {
                "id": "source",
                "kind": "k",
                "memory_mb": 0,
                "latency_ms": {"a": 1},
                "pinned_device": "a",
            },
            {
                "id": "target",
                "kind": "k",
                "memory_mb": 0,
                "latency_ms": {"b": 1},
                "pinned_device": "b",
            },
        ],
        "edges": [{"source": "source", "target": "target", "payload_mb": 1e308}],
    }
    with pytest.raises(PlanningError, match=r"transfer estimate overflowed"):
        GreedyPlanner().plan(graph_from_dict(payload))


def test_configured_link_transfer_arithmetic_overflow_is_rejected():
    link = LinkSpec(source="a", target="b", bandwidth_mb_s=1e-308)
    with pytest.raises(PlanningError, match=r"transfer estimate overflowed for link"):
        link.transfer_ms(1e308)


def test_incoming_transfer_total_overflow_is_rejected(monkeypatch):
    payload = {
        "devices": [{"name": "device", "memory_mb": 1}],
        "nodes": [
            {"id": node_id, "kind": "k", "memory_mb": 0, "latency_ms": {"device": 1}}
            for node_id in ("source-a", "source-b", "target")
        ],
        "edges": [
            {"source": "source-a", "target": "target"},
            {"source": "source-b", "target": "target"},
        ],
    }
    graph = graph_from_dict(payload)

    def huge_transfer(*_args, **_kwargs):
        return 1e308

    monkeypatch.setattr(type(graph), "transfer_ms", huge_transfer)
    with pytest.raises(PlanningError, match=r"incoming transfer total overflowed"):
        GreedyPlanner().plan(graph)


def test_schedule_time_overflow_is_rejected():
    payload = {
        "devices": [{"name": "device", "memory_mb": 1}],
        "nodes": [
            {
                "id": node_id,
                "kind": "k",
                "memory_mb": 0,
                "latency_ms": {"device": 1e308},
            }
            for node_id in ("first", "second")
        ],
        "edges": [{"source": "first", "target": "second"}],
    }
    with pytest.raises(PlanningError, match=r"schedule time overflowed"):
        GreedyPlanner().plan(graph_from_dict(payload))


def test_beam_uses_the_documented_fixed_ready_node_order():
    payload = {
        "devices": [{"name": "d1", "memory_mb": 1}, {"name": "d2", "memory_mb": 1}],
        "nodes": [
            {
                "id": "a-long",
                "kind": "k",
                "memory_mb": 0,
                "latency_ms": {"d1": 100},
                "pinned_device": "d1",
            },
            {
                "id": "b-unlock",
                "kind": "k",
                "memory_mb": 0,
                "latency_ms": {"d1": 1},
                "pinned_device": "d1",
            },
            {
                "id": "c-tail",
                "kind": "k",
                "memory_mb": 0,
                "latency_ms": {"d2": 100},
                "pinned_device": "d2",
            },
        ],
        "edges": [{"source": "b-unlock", "target": "c-tail"}],
    }
    plan = BeamPlanner(beam_width=100).plan(graph_from_dict(payload))
    assert [item.node for item in plan.schedule] == ["a-long", "b-unlock", "c-tail"]
    assert plan.makespan_ms == 201


def test_exact_memory_capacity_is_allowed():
    payload = demo_payload()
    payload["devices"][0]["memory_mb"] = 180
    graph = graph_from_dict(payload)
    plan = GreedyPlanner().plan(graph)
    assert plan.memory_used_mb["cpu"] == 180


def test_allowed_devices_rejection_is_traced():
    payload = demo_payload()
    payload["nodes"][2]["allowed_devices"] = ["gpu-1"]
    plan = GreedyPlanner().plan(graph_from_dict(payload))
    decision = next(item for item in plan.decisions if item.node == "vision-encoder")
    rejected = next(item for item in decision.candidates if item.device == "gpu-0")
    assert rejected.feasible is False
    assert rejected.reason == "device is not in allowed_devices"


def test_kind_rejection_is_traced():
    decision = GreedyPlanner().plan(demo_graph()).decisions[0]
    rejected = next(item for item in decision.candidates if item.device == "gpu-0")
    assert "does not support kind" in rejected.reason


@pytest.mark.parametrize("width", [0, -1, True, 1.5])
def test_beam_width_must_be_positive_integer(width):
    with pytest.raises(ValueError, match="positive integer"):
        BeamPlanner(width)


def test_plan_to_dict_is_json_ready():
    document = BeamPlanner().plan(demo_graph()).to_dict()
    assert document["graph_name"] == "multimodal-assistant"
    assert document["schedule"][0]["node"] == "decode-audio"
    assert document["decisions"][0]["candidates"]


def test_empty_plan_properties():
    plan = PlanResult("empty", "test", (), {}, ())
    assert plan.makespan_ms == 0
    assert plan.critical_node is None
    assert plan.placements == {}
