from __future__ import annotations

import itertools
from collections.abc import Iterator, Mapping
from dataclasses import FrozenInstanceError, replace

import pytest

import graph_sail.models as models_module
from graph_sail.demo import demo_graph
from graph_sail.errors import PlanningError, ValidationError
from graph_sail.io import load_graph
from graph_sail.limits import (
    MAX_BEAM_WIDTH,
    MAX_DEVICES,
    MAX_INPUT_BYTES,
    MAX_LATENCY_CELLS_PER_NODE,
    MAX_TEXT_LENGTH,
)
from graph_sail.models import (
    CandidateTrace,
    DeviceSpec,
    GraphSpec,
    LinkSpec,
    NodeSpec,
    PlacementDecision,
    PlanResult,
    ScheduledNode,
)
from graph_sail.planner import BeamPlanner, GreedyPlanner


def test_input_models_snapshot_mutable_collections() -> None:
    latencies = {"cpu": 3.0}
    allowed = ["cpu"]
    node = NodeSpec("node", "kind", 1, latencies, allowed)  # type: ignore[arg-type]
    latencies["cpu"] = 99
    allowed.append("ghost")
    assert dict(node.latency_ms) == {"cpu": 3.0}
    assert node.allowed_devices == frozenset({"cpu"})
    with pytest.raises(TypeError):
        node.latency_ms["cpu"] = 4  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        node.id = "changed"  # type: ignore[misc]


def test_graph_snapshots_list_inputs_and_returns_read_only_indexes() -> None:
    graph = demo_graph()
    devices = list(graph.devices)
    rebuilt = GraphSpec(
        graph.name, devices, list(graph.nodes), list(graph.edges), list(graph.links)
    )  # type: ignore[arg-type]
    devices.clear()
    assert len(rebuilt.devices) == len(graph.devices)
    with pytest.raises(TypeError):
        rebuilt.node_map["new"] = graph.nodes[0]  # type: ignore[index]


@pytest.mark.parametrize(
    "factory",
    [
        lambda: DeviceSpec("", 1),
        lambda: DeviceSpec("d", True),
        lambda: NodeSpec("n", "k", 0, {}),
        lambda: NodeSpec("n", "k", 0, {"d": float("nan")}),
        lambda: NodeSpec("x" * (MAX_TEXT_LENGTH + 1), "k", 0, {"d": 1}),
        lambda: NodeSpec(
            "n",
            "k",
            0,
            {f"d-{index}": 1 for index in range(MAX_LATENCY_CELLS_PER_NODE + 1)},
        ),
    ],
)
def test_direct_input_model_construction_has_stable_validation_errors(factory) -> None:
    with pytest.raises(ValidationError):
        factory()


@pytest.mark.parametrize(
    "factory",
    [
        lambda: DeviceSpec(1, 1),
        lambda: DeviceSpec("bad\ud800", 1),
        lambda: DeviceSpec("bad\x00", 1),
        lambda: DeviceSpec("d", 10**400),
        lambda: DeviceSpec("d", 1, ["k", "k"]),
        lambda: NodeSpec("n", "k", -1, {"d": 1}),
        lambda: NodeSpec("n", "k", 0, []),
        lambda: GraphSpec("g", "bad", (), ()),
    ],
)
def test_additional_direct_input_errors_are_domain_errors(factory) -> None:
    with pytest.raises(ValidationError):
        factory()


def test_replace_rechecks_output_models_and_snapshots_memory() -> None:
    plan = BeamPlanner().plan(demo_graph())
    source = dict(plan.memory_used_mb)
    copied = replace(plan, memory_used_mb=source)
    source["cpu"] = 999
    assert copied.memory_used_mb["cpu"] != 999
    with pytest.raises(TypeError):
        copied.memory_used_mb["cpu"] = 0  # type: ignore[index]
    with pytest.raises(PlanningError, match="schedule and decisions"):
        replace(plan, decisions=())
    with pytest.raises(PlanningError, match="feasible candidate"):
        CandidateTrace("cpu", True, "feasible")


def test_planner_revalidates_hostile_graph_mutation() -> None:
    graph = demo_graph()
    object.__setattr__(graph, "default_bandwidth_mb_s", 0.0)
    with pytest.raises(ValidationError, match="greater than zero"):
        BeamPlanner().plan(graph)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("devices", [], "must be a tuple"),
        ("devices", (object(),), "DeviceSpec"),
        ("nodes", (object(),), "NodeSpec"),
        ("edges", (object(),), "EdgeSpec"),
        ("links", (object(),), "LinkSpec"),
    ],
)
def test_graph_validate_handles_hostile_collection_mutation(field, value, message) -> None:
    graph = demo_graph()
    object.__setattr__(graph, field, value)
    with pytest.raises(ValidationError, match=message):
        graph.validate()


def test_runtime_transfer_and_compatibility_boundaries() -> None:
    graph = demo_graph()
    assert graph.nodes[0].can_run_on("bad") is False  # type: ignore[arg-type]
    assert graph.edge_map
    with pytest.raises(PlanningError, match="unknown device"):
        graph.transfer_ms("ghost", "cpu", 1)
    with pytest.raises(PlanningError, match="invalid link"):
        graph.transfer_ms("cpu", "gpu-0", 1, _link_index={("cpu", "gpu-0"): object()})  # type: ignore[dict-item]
    with pytest.raises(PlanningError, match="payload_mb"):
        LinkSpec("a", "b", 1).transfer_ms(-1)


def test_output_record_invariants_are_checked_directly() -> None:
    feasible = CandidateTrace("cpu", True, "ok", 0, 1, 0)
    rejected = CandidateTrace("gpu", False, "no")
    with pytest.raises(PlanningError, match="boolean"):
        CandidateTrace("cpu", 1, "bad")  # type: ignore[arg-type]
    with pytest.raises(PlanningError, match="must not include"):
        CandidateTrace("cpu", False, "bad", 0, 1, 0)
    with pytest.raises(PlanningError, match="precede"):
        CandidateTrace("cpu", True, "bad", 2, 1, 0)
    with pytest.raises(PlanningError, match="must not be empty"):
        PlacementDecision("n", "cpu", ())
    with pytest.raises(PlanningError, match="selected_device"):
        PlacementDecision("n", "gpu", (feasible, rejected))
    with pytest.raises(PlanningError, match="duplicate candidate"):
        PlacementDecision("n", "cpu", (feasible, feasible))


def test_plan_result_rejects_inconsistent_summaries() -> None:
    scheduled = ScheduledNode("n", "cpu", 0, 1, 1, 0, 2)
    decision = PlacementDecision("n", "cpu", (CandidateTrace("cpu", True, "ok", 0, 1, 0),))
    with pytest.raises(PlanningError, match="must be a mapping"):
        PlanResult("g", "a", (scheduled,), [], (decision,))  # type: ignore[arg-type]
    with pytest.raises(PlanningError, match="inconsistent"):
        PlanResult("g", "a", (scheduled,), {"cpu": 1}, (decision,))
    with pytest.raises(PlanningError, match="unexplained"):
        PlanResult("g", "a", (), {"cpu": 1}, ())
    wrong = PlacementDecision("n", "gpu", (CandidateTrace("gpu", True, "ok", 0, 1, 0),))
    with pytest.raises(PlanningError, match="scheduled device"):
        PlanResult("g", "a", (scheduled,), {"cpu": 2}, (wrong,))
    memory = {f"d-{index}": 0 for index in range(MAX_DEVICES + 1)}
    with pytest.raises(PlanningError, match="entry limit"):
        PlanResult("g", "a", (), memory, ())


def test_planner_type_and_work_guards(monkeypatch) -> None:
    with pytest.raises(PlanningError, match="GraphSpec"):
        GreedyPlanner().plan("bad")  # type: ignore[arg-type]
    monkeypatch.setattr("graph_sail.planner.MAX_PLANNER_EXPANSIONS", 1)
    with pytest.raises(PlanningError, match="planning work"):
        BeamPlanner().plan(demo_graph())


def test_beam_width_has_a_resource_ceiling() -> None:
    with pytest.raises(ValueError, match="no greater"):
        BeamPlanner(MAX_BEAM_WIDTH + 1)


def test_device_collection_has_a_resource_ceiling() -> None:
    devices = tuple(DeviceSpec(f"device-{index}", 1) for index in range(MAX_DEVICES + 1))
    with pytest.raises(ValidationError, match="devices exceeds"):
        GraphSpec("large", devices, (NodeSpec("n", "k", 0, {"device-0": 1}),), ())


def test_graph_file_has_a_preparse_byte_ceiling(tmp_path) -> None:
    path = tmp_path / "too-large.json"
    path.write_bytes(b" " * (MAX_INPUT_BYTES + 1))
    with pytest.raises(ValidationError, match="byte limit"):
        load_graph(path)


def test_empty_plan_memory_is_defensively_immutable() -> None:
    memory: dict[str, float] = {}
    plan = PlanResult("empty", "test", (), memory, ())
    memory["late"] = 1
    assert dict(plan.memory_used_mb) == {}


@pytest.mark.parametrize("field", ["start_ms", "finish_ms", "transfer_ms"])
def test_plan_result_rejects_trace_times_that_do_not_describe_the_schedule(field: str) -> None:
    plan = GreedyPlanner().plan(demo_graph())
    decision = plan.decisions[0]
    selected = next(
        candidate
        for candidate in decision.candidates
        if candidate.device == decision.selected_device
    )
    original = getattr(selected, field)
    assert original is not None
    forged = replace(selected, **{field: original + 1e-4})
    candidates = tuple(
        forged if candidate.device == selected.device else candidate
        for candidate in decision.candidates
    )
    decisions = (
        replace(decision, candidates=candidates),
        *plan.decisions[1:],
    )

    with pytest.raises(PlanningError, match=rf"{field}.*inconsistent"):
        replace(plan, decisions=decisions)


def test_output_models_snapshot_nested_collections_and_replace_revalidates_them() -> None:
    candidate_source = [CandidateTrace("cpu", True, "ok", 0, 1, 0)]
    decision = PlacementDecision("n", "cpu", candidate_source)  # type: ignore[arg-type]
    candidate_source.clear()
    assert len(decision.candidates) == 1

    source_plan = GreedyPlanner().plan(demo_graph())
    schedule_source = list(source_plan.schedule)
    decision_source = list(source_plan.decisions)
    copied = PlanResult(
        source_plan.graph_name,
        source_plan.algorithm,
        schedule_source,  # type: ignore[arg-type]
        source_plan.memory_used_mb,
        decision_source,  # type: ignore[arg-type]
    )
    original_start = copied.schedule[0].start_ms
    object.__setattr__(schedule_source[0], "start_ms", original_start + 1)
    schedule_source.clear()
    decision_source.clear()
    assert copied.schedule[0].start_ms == original_start

    plan = GreedyPlanner().plan(demo_graph())
    selected = next(
        candidate
        for candidate in plan.decisions[0].candidates
        if candidate.device == plan.decisions[0].selected_device
    )
    assert selected.start_ms is not None
    object.__setattr__(selected, "start_ms", selected.start_ms + 1e-4)
    with pytest.raises(PlanningError, match=r"start_ms.*inconsistent"):
        replace(plan)


def test_plan_serialization_revalidates_forced_nested_mutation() -> None:
    plan = GreedyPlanner().plan(demo_graph())
    selected = next(
        candidate
        for candidate in plan.decisions[0].candidates
        if candidate.device == plan.decisions[0].selected_device
    )
    assert selected.start_ms is not None
    object.__setattr__(selected, "start_ms", selected.start_ms + 1e-4)

    with pytest.raises(PlanningError, match=r"start_ms.*inconsistent"):
        plan.to_dict()


def test_plan_serialization_rejects_forced_unbounded_collections(monkeypatch) -> None:
    plan = PlanResult("empty", "test", (), {}, ())
    scheduled = ScheduledNode("n", "cpu", 0, 1, 1, 0, 1)
    monkeypatch.setattr(models_module, "MAX_NODES", 2)
    object.__setattr__(plan, "schedule", itertools.repeat(scheduled))

    with pytest.raises(PlanningError, match="schedule exceeds"):
        plan.to_dict()


class _EndlessMapping(Mapping[str, float]):
    def __init__(self) -> None:
        self.yielded = 0

    def __getitem__(self, key: str) -> float:
        return 1.0

    def __iter__(self) -> Iterator[str]:
        index = 0
        while True:
            self.yielded += 1
            yield f"device-{index}"
            index += 1

    def __len__(self) -> int:
        # Deliberately dishonest: public boundaries must count while iterating.
        return 0


def test_public_mapping_inputs_are_bounded_during_snapshot(monkeypatch) -> None:
    monkeypatch.setattr(models_module, "MAX_LATENCY_CELLS_PER_NODE", 2)
    latencies = _EndlessMapping()
    with pytest.raises(ValidationError, match="latency_ms exceeds the 2-entry limit"):
        NodeSpec("n", "k", 0, latencies)
    assert latencies.yielded == 3

    monkeypatch.setattr(models_module, "MAX_DEVICES", 2)
    memory = _EndlessMapping()
    with pytest.raises(PlanningError, match="memory_used_mb exceeds the 2-entry limit"):
        PlanResult("empty", "test", (), memory, ())
    assert memory.yielded == 3
