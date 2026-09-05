from __future__ import annotations

import copy

import pytest

from graph_sail.analysis import critical_chain
from graph_sail.calibration import graph_to_dict
from graph_sail.demo import demo_graph, demo_payload
from graph_sail.errors import PlanningError, ValidationError
from graph_sail.io import graph_from_dict
from graph_sail.limits import MAX_BATCH_SIZE, MAX_COTENANTS
from graph_sail.models import (
    BatchSpec,
    ContentionSpec,
    DeviceSpec,
    NodeSpec,
    ScheduledNode,
)
from graph_sail.planner import BeamPlanner, GreedyPlanner
from graph_sail.report import render_html


def _chain_payload() -> dict[str, object]:
    """Two chained nodes where the fast device wins only without contention."""

    return {
        "name": "contention",
        "default_bandwidth_mb_s": 1_000,
        "default_link_latency_ms": 0,
        "devices": [
            {"name": "fast", "memory_mb": 100},
            {"name": "slow", "memory_mb": 100},
        ],
        "nodes": [
            {"id": "a", "kind": "k", "memory_mb": 1, "latency_ms": {"fast": 10, "slow": 15}},
            {"id": "b", "kind": "k", "memory_mb": 1, "latency_ms": {"fast": 10, "slow": 15}},
        ],
        "edges": [{"source": "a", "target": "b", "payload_mb": 0}],
    }


def _with_contention(payload: dict[str, object], **fields: object) -> dict[str, object]:
    updated = copy.deepcopy(payload)
    devices = updated["devices"]
    assert isinstance(devices, list)
    devices[0]["contention"] = dict(fields)
    return updated


def test_contention_changes_a_placement_decision() -> None:
    payload = _chain_payload()
    baseline = GreedyPlanner().plan(graph_from_dict(payload))
    assert baseline.placements == {"a": "fast", "b": "fast"}

    contended = _with_contention(payload, slowdown_per_cotenant=1.0, max_cotenants=4)
    graph = graph_from_dict(contended)
    assert GreedyPlanner().plan(graph).placements == {"a": "fast", "b": "slow"}
    assert BeamPlanner(beam_width=8).plan(graph).placements == {"a": "fast", "b": "slow"}


def test_contention_scales_only_the_co_resident_component() -> None:
    payload = _with_contention(_chain_payload(), slowdown_per_cotenant=0.25, max_cotenants=4)
    nodes = payload["nodes"]
    assert isinstance(nodes, list)
    for node in nodes:
        node["pinned_device"] = "fast"
    plan = GreedyPlanner().plan(graph_from_dict(payload))
    first, second = plan.schedule
    assert (first.latency_scale, first.compute_ms) == (1.0, 10.0)
    assert (second.latency_scale, second.compute_ms) == (1.25, 12.5)
    assert plan.makespan_ms == pytest.approx(22.5)


def test_contention_saturates_at_the_declared_cotenant_ceiling() -> None:
    payload = {
        "name": "saturating",
        "devices": [
            {
                "name": "device",
                "memory_mb": 100,
                "contention": {"slowdown_per_cotenant": 0.5, "max_cotenants": 1},
            }
        ],
        "nodes": [
            {"id": node_id, "kind": "k", "memory_mb": 1, "latency_ms": {"device": 10}}
            for node_id in ("a", "b", "c")
        ],
        "edges": [],
    }
    plan = GreedyPlanner().plan(graph_from_dict(payload))
    assert [item.latency_scale for item in plan.schedule] == [1.0, 1.5, 1.5]


def test_contention_is_recorded_only_when_it_applies() -> None:
    unmodelled = GreedyPlanner().plan(demo_graph()).to_dict()
    assert all("latency_scale" not in item for item in unmodelled["schedule"])

    payload = _with_contention(_chain_payload(), slowdown_per_cotenant=0.5, max_cotenants=2)
    nodes = payload["nodes"]
    assert isinstance(nodes, list)
    for node in nodes:
        node["pinned_device"] = "fast"
    scaled = GreedyPlanner().plan(graph_from_dict(payload)).to_dict()
    assert [item.get("latency_scale") for item in scaled["schedule"]] == [None, 1.5]


def test_declaring_no_contention_reproduces_the_unmodelled_plan() -> None:
    payload = demo_payload()
    baseline = BeamPlanner().plan(graph_from_dict(payload)).to_dict()
    explicit = _with_contention(payload, slowdown_per_cotenant=0.0, max_cotenants=1)
    assert BeamPlanner().plan(graph_from_dict(explicit)).to_dict() == baseline


def test_contention_survives_the_canonical_graph_round_trip() -> None:
    payload = _with_contention(_chain_payload(), slowdown_per_cotenant=0.25, max_cotenants=3)
    document = graph_to_dict(graph_from_dict(payload))
    assert document["devices"][0]["contention"] == {
        "slowdown_per_cotenant": 0.25,
        "max_cotenants": 3,
    }
    assert "contention" not in document["devices"][1]
    assert graph_to_dict(graph_from_dict(document)) == document


def test_contention_appears_in_the_html_report_only_when_configured() -> None:
    demo = demo_graph()
    assert "Latency modelling" not in render_html(demo, BeamPlanner().plan(demo))

    payload = _with_contention(_chain_payload(), slowdown_per_cotenant=0.5, max_cotenants=2)
    nodes = payload["nodes"]
    assert isinstance(nodes, list)
    for node in nodes:
        node["pinned_device"] = "fast"
    graph = graph_from_dict(payload)
    report = render_html(graph, GreedyPlanner().plan(graph))
    assert "Latency modelling" in report
    assert "<td>10.000</td><td>15.000</td><td>1.5000</td>" in report


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"slowdown_per_cotenant": -0.1, "max_cotenants": 2}, "zero or greater"),
        ({"slowdown_per_cotenant": "0.1", "max_cotenants": 2}, "must be a number"),
        ({"slowdown_per_cotenant": 0.1, "max_cotenants": 0}, "integer from 1"),
        ({"slowdown_per_cotenant": 0.1, "max_cotenants": 1.5}, "must be an integer"),
        ({"slowdown_per_cotenant": 0.1, "max_cotenants": True}, "must be an integer"),
        (
            {"slowdown_per_cotenant": 0.1, "max_cotenants": MAX_COTENANTS + 1},
            "integer from 1",
        ),
        ({"slowdown_per_cotenant": 0.1}, "max_cotenants must be an integer"),
        ({"slowdown_per_cotenant": 0.1, "max_cotenants": 2, "extra": 1}, "unknown field"),
    ],
)
def test_invalid_contention_configuration_is_rejected(fields, message) -> None:
    payload = _with_contention(_chain_payload(), **fields)
    with pytest.raises(ValidationError, match=message):
        graph_from_dict(payload)


def test_contention_must_be_an_object() -> None:
    payload = _chain_payload()
    devices = payload["devices"]
    assert isinstance(devices, list)
    devices[0]["contention"] = 0.5
    with pytest.raises(ValidationError, match=r"contention must be an object"):
        graph_from_dict(payload)


def test_direct_contention_construction_is_validated() -> None:
    with pytest.raises(ValidationError, match="max_cotenants must be an integer from 1"):
        ContentionSpec(0.1, 0)
    with pytest.raises(ValidationError, match="max_cotenants must be an integer"):
        ContentionSpec(0.1, 1.5)  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="must be a ContentionSpec"):
        DeviceSpec("d", 1, frozenset(), object())  # type: ignore[arg-type]
    assert DeviceSpec("d", 1).contention_factor(3) == 1.0


def test_contention_factor_rejects_invalid_cotenant_counts() -> None:
    spec = ContentionSpec(0.5, 4)
    assert spec.factor(0) == 1.0
    for value in (-1, 1.5, True):
        with pytest.raises(PlanningError, match="non-negative integer"):
            spec.factor(value)  # type: ignore[arg-type]


def test_contention_factor_overflow_is_rejected() -> None:
    with pytest.raises(PlanningError, match="contention factor overflowed"):
        ContentionSpec(1e308, MAX_COTENANTS).factor(MAX_COTENANTS)


def test_planner_revalidates_hostile_contention_mutation() -> None:
    graph = demo_graph()
    object.__setattr__(graph.devices[0], "contention", object())
    with pytest.raises(ValidationError, match="must be a ContentionSpec"):
        GreedyPlanner().plan(graph)


def _batch_payload(**batch: object) -> dict[str, object]:
    """Two independent nodes where the second follows the first onto ``fast``."""

    node: dict[str, object] = {
        "id": "n1",
        "kind": "k",
        "memory_mb": 1,
        "latency_ms": {"fast": 20, "slow": 21},
    }
    if batch:
        node["batch"] = dict(batch)
    return {
        "name": "batching",
        "default_bandwidth_mb_s": 1_000,
        "default_link_latency_ms": 0,
        "devices": [
            {"name": "fast", "memory_mb": 100},
            {"name": "slow", "memory_mb": 100},
        ],
        "nodes": [
            node,
            {"id": "n2", "kind": "k", "memory_mb": 1, "latency_ms": {"fast": 4, "slow": 60}},
        ],
        "edges": [],
    }


def test_batch_amortisation_changes_a_placement_decision() -> None:
    payload = _batch_payload()
    nodes = payload["nodes"]
    assert isinstance(nodes, list)
    nodes[1]["latency_ms"] = {"fast": 4, "slow": 10}
    baseline = GreedyPlanner().plan(graph_from_dict(payload))
    assert baseline.placements == {"n1": "fast", "n2": "slow"}

    batched = copy.deepcopy(payload)
    batched_nodes = batched["nodes"]
    assert isinstance(batched_nodes, list)
    batched_nodes[0]["batch"] = {"size": 4, "window_ms": 0, "fixed_fraction": 1.0}
    graph = graph_from_dict(batched)
    plan = GreedyPlanner().plan(graph)
    assert plan.schedule[0].compute_ms == pytest.approx(5.0)
    assert plan.placements == {"n1": "fast", "n2": "fast"}

    unbatched_beam = BeamPlanner(beam_width=8).plan(graph_from_dict(payload))
    assert unbatched_beam.placements == {"n1": "fast", "n2": "slow"}
    assert BeamPlanner(beam_width=8).plan(graph).placements == {"n1": "slow", "n2": "fast"}


def test_batch_window_changes_a_placement_decision() -> None:
    assert GreedyPlanner().plan(graph_from_dict(_batch_payload())).placements == {
        "n1": "fast",
        "n2": "fast",
    }
    graph = graph_from_dict(_batch_payload(size=1, window_ms=60, fixed_fraction=0.5))
    assert GreedyPlanner().plan(graph).placements == {"n1": "fast", "n2": "slow"}
    assert BeamPlanner(beam_width=8).plan(graph).placements == {"n1": "fast", "n2": "slow"}


def test_batch_window_delays_the_start_and_is_recorded() -> None:
    payload = _batch_payload(size=1, window_ms=60, fixed_fraction=0.5)
    plan = GreedyPlanner().plan(graph_from_dict(payload))
    batched = plan.schedule[0]
    assert (batched.start_ms, batched.finish_ms) == (60.0, 80.0)
    assert (batched.batch_window_ms, batched.latency_scale) == (60.0, 1.0)
    document = plan.to_dict()
    assert document["schedule"][0]["batch_window_ms"] == 60.0
    assert "latency_scale" not in document["schedule"][0]
    assert "batch_window_ms" not in document["schedule"][1]


def test_batch_amortisation_follows_the_declared_affine_model() -> None:
    assert BatchSpec(1, 0.0, 0.9).factor() == 1.0
    assert BatchSpec(8, 0.0, 0.0).factor() == 1.0
    assert BatchSpec(4, 0.0, 1.0).factor() == pytest.approx(0.25)
    assert BatchSpec(2, 0.0, 0.5).factor() == pytest.approx(0.75)


def test_batch_window_is_charged_in_the_critical_chain() -> None:
    payload = {
        "name": "chain",
        "devices": [{"name": "d", "memory_mb": 100}],
        "nodes": [
            {"id": "a", "kind": "k", "memory_mb": 1, "latency_ms": {"d": 10}},
            {
                "id": "b",
                "kind": "k",
                "memory_mb": 1,
                "latency_ms": {"d": 5},
                "batch": {"size": 1, "window_ms": 7, "fixed_fraction": 0},
            },
        ],
        "edges": [{"source": "a", "target": "b", "payload_mb": 0}],
    }
    graph = graph_from_dict(payload)
    plan = GreedyPlanner().plan(graph)
    assert plan.schedule[1].start_ms == pytest.approx(17.0)
    assert critical_chain(graph, plan) == ("a", "b")


def test_contention_and_batching_compose_multiplicatively() -> None:
    payload = {
        "name": "combined",
        "devices": [
            {
                "name": "d",
                "memory_mb": 100,
                "contention": {"slowdown_per_cotenant": 0.5, "max_cotenants": 2},
            }
        ],
        "nodes": [
            {
                "id": node_id,
                "kind": "k",
                "memory_mb": 1,
                "latency_ms": {"d": 10},
                "batch": {"size": 2, "window_ms": 0, "fixed_fraction": 1.0},
            }
            for node_id in ("a", "b")
        ],
        "edges": [],
    }
    plan = GreedyPlanner().plan(graph_from_dict(payload))
    assert [item.latency_scale for item in plan.schedule] == [0.5, 0.75]
    assert [item.compute_ms for item in plan.schedule] == [5.0, 7.5]


def test_declaring_a_neutral_batch_reproduces_the_unmodelled_plan() -> None:
    payload = demo_payload()
    baseline = BeamPlanner().plan(graph_from_dict(payload)).to_dict()
    nodes = payload["nodes"]
    assert isinstance(nodes, list)
    for node in nodes:
        node["batch"] = {"size": 1, "window_ms": 0, "fixed_fraction": 0.75}
    assert BeamPlanner().plan(graph_from_dict(payload)).to_dict() == baseline


def test_batch_survives_the_canonical_graph_round_trip() -> None:
    payload = _batch_payload(size=4, window_ms=1.5, fixed_fraction=0.5)
    document = graph_to_dict(graph_from_dict(payload))
    assert document["nodes"][0]["batch"] == {"size": 4, "window_ms": 1.5, "fixed_fraction": 0.5}
    assert "batch" not in document["nodes"][1]
    assert graph_to_dict(graph_from_dict(document)) == document


def test_batch_window_appears_in_the_html_report() -> None:
    graph = graph_from_dict(_batch_payload(size=2, window_ms=3, fixed_fraction=1.0))
    report = render_html(graph, GreedyPlanner().plan(graph))
    assert "Batch window ms" in report
    assert "<td>20.000</td><td>10.000</td><td>0.5000</td><td>3.000</td>" in report


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"size": 0, "fixed_fraction": 0.5}, "integer from 1"),
        ({"size": 1.5, "fixed_fraction": 0.5}, "must be an integer"),
        ({"size": MAX_BATCH_SIZE + 1, "fixed_fraction": 0.5}, "integer from 1"),
        ({"size": 2, "fixed_fraction": -0.1}, "zero or greater"),
        ({"size": 2, "fixed_fraction": 1.5}, "between zero and one"),
        ({"size": 2, "fixed_fraction": "half"}, "must be a number"),
        ({"size": 2}, "fixed_fraction must be a number"),
        ({"size": 2, "fixed_fraction": 0.5, "window_ms": -1}, "zero or greater"),
        ({"size": 2, "fixed_fraction": 0.5, "extra": 1}, "unknown field"),
    ],
)
def test_invalid_batch_configuration_is_rejected(fields, message) -> None:
    with pytest.raises(ValidationError, match=message):
        graph_from_dict(_batch_payload(**fields))


def test_batch_must_be_an_object() -> None:
    payload = _batch_payload()
    nodes = payload["nodes"]
    assert isinstance(nodes, list)
    nodes[0]["batch"] = 4
    with pytest.raises(ValidationError, match=r"batch must be an object"):
        graph_from_dict(payload)


def test_direct_batch_construction_is_validated() -> None:
    with pytest.raises(ValidationError, match="between zero and one"):
        BatchSpec(2, 0.0, 2.0)
    with pytest.raises(ValidationError, match="must be a BatchSpec"):
        NodeSpec("n", "k", 0, {"d": 1}, frozenset(), None, object())  # type: ignore[arg-type]
    node = NodeSpec("n", "k", 0, {"d": 1})
    assert (node.batch_factor(), node.batch_window_ms()) == (1.0, 0.0)


def test_scheduled_node_rejects_a_start_before_its_batch_window() -> None:
    assert ScheduledNode("n", "d", 5, 6, 1, 0, 0, 1.0, 5).batch_window_ms == 5
    with pytest.raises(PlanningError, match="must not precede its batch window"):
        ScheduledNode("n", "d", 0, 1, 1, 0, 0, 1.0, 5)


def test_planner_revalidates_hostile_batch_mutation() -> None:
    graph = demo_graph()
    object.__setattr__(graph.nodes[0], "batch", object())
    with pytest.raises(ValidationError, match="must be a BatchSpec"):
        GreedyPlanner().plan(graph)
