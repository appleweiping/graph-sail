"""How much a plan depends on the estimates it was given."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

import graph_sail.sensitivity as sensitivity_module
from graph_sail.cli import main
from graph_sail.demo import demo_graph, demo_payload
from graph_sail.errors import GraphSailError
from graph_sail.limits import MAX_TEXT_LENGTH
from graph_sail.models import CandidateTrace, GraphSpec, PlacementDecision
from graph_sail.planner import BeamPlanner, GreedyPlanner
from graph_sail.sensitivity import (
    DEFAULT_MAX_FACTOR,
    DEFAULT_MIN_FACTOR,
    DEFAULT_PROBE,
    DEFAULT_SAMPLES_PER_OCTAVE,
    DEFAULT_TOLERANCE,
    MAX_SAMPLES_PER_OCTAVE,
    MakespanSensitivity,
    PlacementStability,
    SensitivityReport,
    analyze_sensitivity,
    makespan_sensitivity,
    perturb,
    placement_stability,
)


@pytest.fixture(scope="module")
def graph() -> GraphSpec:
    return demo_graph()


@pytest.fixture(scope="module")
def report(graph: GraphSpec):
    planner = BeamPlanner()
    return analyze_sensitivity(graph, planner.plan(graph), planner)


# ---------------------------------------------------------------------------
# Perturbing one estimate.
# ---------------------------------------------------------------------------


def test_only_the_named_device_estimate_moves(graph: GraphSpec) -> None:
    """Scaling every device would ask whether the node is slow.

    The question here is narrower: whether this estimate, for this device, is
    right.
    """

    node = graph.nodes[0]
    device = next(iter(node.latency_ms))
    changed = perturb(graph, node.id, device, 2.0)
    moved = next(item for item in changed.nodes if item.id == node.id)
    assert moved.latency_ms[device] == pytest.approx(node.latency_ms[device] * 2.0)
    for other in node.latency_ms:
        if other != device:
            assert moved.latency_ms[other] == node.latency_ms[other]


def test_every_other_node_is_untouched(graph: GraphSpec) -> None:
    node = graph.nodes[0]
    device = next(iter(node.latency_ms))
    changed = perturb(graph, node.id, device, 3.0)
    assert [item.id for item in changed.nodes] == [item.id for item in graph.nodes]
    for before, after in zip(graph.nodes[1:], changed.nodes[1:], strict=True):
        assert before == after


def test_an_unknown_node_or_device_is_refused(graph: GraphSpec) -> None:
    node = graph.nodes[0]
    with pytest.raises(GraphSailError, match="unknown node"):
        perturb(graph, "not-a-node", "cpu", 2.0)
    with pytest.raises(GraphSailError, match="no estimate for device"):
        perturb(graph, node.id, "not-a-device", 2.0)


@pytest.mark.parametrize("factor", [0.0, -1.0])
def test_a_non_positive_factor_is_refused(graph: GraphSpec, factor: float) -> None:
    with pytest.raises(GraphSailError, match="must be positive"):
        perturb(graph, graph.nodes[0].id, next(iter(graph.nodes[0].latency_ms)), factor)


@pytest.mark.parametrize("factor", ["not-a-number", float("nan"), float("inf"), True])
def test_perturb_validates_factor_before_comparing_it(graph: GraphSpec, factor) -> None:
    with pytest.raises(GraphSailError):
        perturb(graph, graph.nodes[0].id, next(iter(graph.nodes[0].latency_ms)), factor)


# ---------------------------------------------------------------------------
# Which estimates reach the makespan.
# ---------------------------------------------------------------------------


def test_a_critical_node_passes_on_every_added_millisecond(report) -> None:
    core = next(item for item in report.makespan if item.node == "language-core")
    assert core.on_critical_chain
    assert core.response == pytest.approx(1.0, abs=1e-6)


def test_a_parallel_node_with_slack_passes_on_none_of_it(report) -> None:
    """The check that a naive slack calculation would have failed.

    Subtracting a node's finish from the makespan calls anything short of the
    end slack, including nodes the last one is waiting on.
    """

    audio = next(item for item in report.makespan if item.node == "audio-encoder")
    assert not audio.on_critical_chain
    assert audio.response == pytest.approx(0.0, abs=1e-9)
    assert not audio.matters


def test_a_node_that_feeds_the_last_one_is_not_reported_as_slack(report) -> None:
    # `language-core` finishes before the makespan, but `format-response` waits
    # on it, so none of that gap is slack.
    core = next(item for item in report.makespan if item.node == "language-core")
    assert core.baseline_makespan_ms > core.estimate_ms
    assert core.matters


def test_the_response_is_measured_against_the_probed_makespan(report) -> None:
    for item in report.makespan:
        if item.placement_changed or item.estimate_ms == 0.0:
            continue
        expected = (item.probed_makespan_ms - item.baseline_makespan_ms) / (
            item.estimate_ms * DEFAULT_PROBE
        )
        assert item.response == pytest.approx(expected, abs=1e-9)


@pytest.mark.parametrize("probe", [0.0, 1.0, -0.1])
def test_a_bad_probe_is_refused(graph: GraphSpec, probe: float) -> None:
    planner = BeamPlanner()
    with pytest.raises(GraphSailError, match="probe"):
        makespan_sensitivity(graph, planner.plan(graph), planner, probe=probe)


@pytest.mark.parametrize("probe", ["not-a-number", float("nan"), float("inf"), True])
def test_makespan_sensitivity_validates_probe_before_comparing_it(graph: GraphSpec, probe) -> None:
    planner = BeamPlanner()
    with pytest.raises(GraphSailError):
        makespan_sensitivity(graph, planner.plan(graph), planner, probe=probe)


# ---------------------------------------------------------------------------
# How far an estimate can move before the placement changes.
# ---------------------------------------------------------------------------


def test_the_weakest_estimate_is_found_and_is_small(report) -> None:
    weakest = report.weakest
    assert weakest is not None
    assert weakest.node == "language-core"
    assert weakest.margin is not None
    assert 0.0 < weakest.margin < 0.5


def test_a_reported_flip_point_really_flips_the_placement(graph: GraphSpec, report) -> None:
    """The number has to survive being acted on, not just reported."""

    planner = BeamPlanner()
    baseline = {item.node: item.device for item in planner.plan(graph).schedule}
    for item in report.stability:
        if item.slower_factor is None:
            continue
        flipped = planner.plan(perturb(graph, item.node, item.device, item.slower_factor))
        assert {entry.node: entry.device for entry in flipped.schedule} != baseline


def test_just_inside_a_flip_point_the_placement_still_holds(graph: GraphSpec, report) -> None:
    planner = BeamPlanner()
    baseline = {item.node: item.device for item in planner.plan(graph).schedule}
    for item in report.stability:
        if item.slower_factor is None:
            continue
        held = planner.plan(perturb(graph, item.node, item.device, item.slower_factor * 0.9))
        assert {entry.node: entry.device for entry in held.schedule} == baseline


def test_a_stable_estimate_reports_only_the_factors_it_was_probed_at(report) -> None:
    stable = [item for item in report.stability if item.stable]
    assert stable
    for item in stable:
        assert item.margin is None
        assert item.searched_range == (DEFAULT_MIN_FACTOR, DEFAULT_MAX_FACTOR)
        assert item.samples_per_octave == DEFAULT_SAMPLES_PER_OCTAVE
        assert DEFAULT_MIN_FACTOR in item.probed_factors
        assert DEFAULT_MAX_FACTOR in item.probed_factors
        assert 0.5 in item.probed_factors
        assert 2.0 in item.probed_factors


def test_default_geometric_grid_includes_half_double_and_endpoints(graph: GraphSpec) -> None:
    plan = BeamPlanner().plan(graph)

    class ConstantPlanner:
        def plan(self, _graph: GraphSpec):
            return plan

    records = placement_stability(graph, plan, ConstantPlanner())
    for item in records:
        assert item.probed_factors == (2.0, 4.0, 0.5, 0.25)


@pytest.mark.parametrize(
    ("max_factor", "min_factor"),
    [(0.5, 0.25), (4.0, 1.5), (4.0, 0.0), (1.0, 0.5)],
)
def test_an_inverted_search_range_is_refused(
    graph: GraphSpec, max_factor: float, min_factor: float
) -> None:
    planner = BeamPlanner()
    with pytest.raises(GraphSailError, match="factors must satisfy"):
        placement_stability(
            graph, planner.plan(graph), planner, max_factor=max_factor, min_factor=min_factor
        )


@pytest.mark.parametrize("tolerance", [0.0, 1.0, -0.5])
def test_a_bad_tolerance_is_refused(graph: GraphSpec, tolerance: float) -> None:
    planner = BeamPlanner()
    with pytest.raises(GraphSailError, match="tolerance"):
        placement_stability(graph, planner.plan(graph), planner, tolerance=tolerance)


@pytest.mark.parametrize(
    ("keyword", "value"),
    [
        ("min_factor", "not-a-number"),
        ("min_factor", float("nan")),
        ("max_factor", "not-a-number"),
        ("max_factor", float("nan")),
        ("tolerance", "not-a-number"),
        ("tolerance", float("nan")),
        ("samples_per_octave", 0),
        ("samples_per_octave", True),
        ("samples_per_octave", MAX_SAMPLES_PER_OCTAVE + 1),
    ],
)
def test_placement_stability_validates_scalars_before_comparing_them(
    graph: GraphSpec, keyword: str, value
) -> None:
    planner = BeamPlanner()
    with pytest.raises(GraphSailError):
        placement_stability(graph, planner.plan(graph), planner, **{keyword: value})


def test_probe_grid_and_total_planner_work_are_bounded(
    graph: GraphSpec, monkeypatch: pytest.MonkeyPatch
) -> None:
    planner = BeamPlanner()
    plan = planner.plan(graph)
    with pytest.raises(GraphSailError, match="probe grid would require"):
        placement_stability(graph, plan, planner, max_factor=2.0**65)

    monkeypatch.setattr(sensitivity_module, "MAX_STABILITY_PLANNER_CALLS", 1)
    with pytest.raises(GraphSailError, match="planner calls"):
        placement_stability(graph, plan, planner)


def test_combined_call_budget_includes_baseline_makespan_and_stability_probes(
    graph: GraphSpec, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline = BeamPlanner().plan(graph)

    class CountingConstantPlanner:
        def __init__(self) -> None:
            self.calls = 0

        def plan(self, _graph: GraphSpec):
            self.calls += 1
            return baseline

    coarse_probes = 4  # 2x, 4x, 0.5x, and 0.25x at the default density.
    cells = len(baseline.schedule)
    conservative_bound = 1 + cells * (
        1 + coarse_probes + 2 * sensitivity_module.MAX_BISECTION_STEPS
    )
    planner = CountingConstantPlanner()
    monkeypatch.setattr(sensitivity_module, "MAX_STABILITY_PLANNER_CALLS", conservative_bound - 1)
    with pytest.raises(GraphSailError, match=f"requested bound is {conservative_bound}"):
        analyze_sensitivity(graph, baseline, planner)
    assert planner.calls == 0

    monkeypatch.setattr(sensitivity_module, "MAX_STABILITY_PLANNER_CALLS", conservative_bound)
    analyze_sensitivity(graph, baseline, planner)
    assert planner.calls == 1 + cells * (1 + coarse_probes)


def test_a_tighter_tolerance_does_not_move_the_answer_much(graph: GraphSpec) -> None:
    planner = BeamPlanner()
    plan = planner.plan(graph)
    coarse = placement_stability(graph, plan, planner, tolerance=0.05)
    fine = placement_stability(graph, plan, planner, tolerance=0.001)
    for left, right in zip(coarse, fine, strict=True):
        if left.slower_factor is None or right.slower_factor is None:
            assert left.slower_factor is None and right.slower_factor is None
            continue
        assert left.slower_factor == pytest.approx(right.slower_factor, rel=0.1)


def test_public_sensitivity_boundaries_use_domain_errors(graph: GraphSpec) -> None:
    planner = BeamPlanner()
    plan = planner.plan(graph)
    node = graph.nodes[0]
    device = next(iter(node.latency_ms))
    factories = (
        lambda: perturb(None, node.id, device, 2),
        lambda: makespan_sensitivity(None, plan, planner),
        lambda: makespan_sensitivity(graph, object(), planner),
        lambda: placement_stability(graph, plan, object()),
        lambda: analyze_sensitivity(graph, replace(plan, graph_name="other"), planner),
        lambda: analyze_sensitivity(graph, plan, planner, probe=float("nan")),
    )
    for factory in factories:
        with pytest.raises(GraphSailError):
            factory()


class _InvalidResultPlanner:
    def plan(self, graph: GraphSpec):
        return object()


def test_sensitivity_rejects_a_planner_result_outside_the_public_contract(
    graph: GraphSpec,
) -> None:
    baseline = BeamPlanner().plan(graph)
    operations = (
        lambda: makespan_sensitivity(graph, baseline, _InvalidResultPlanner()),
        lambda: placement_stability(graph, baseline, _InvalidResultPlanner()),
        lambda: analyze_sensitivity(graph, baseline, _InvalidResultPlanner()),
    )
    for operation in operations:
        with pytest.raises(GraphSailError, match=r"baseline result.*PlanResult"):
            operation()


def _plan_with_first_node_on(plan, device: str):
    original = plan.schedule[0]
    scheduled = replace(original, device=device)
    selected = CandidateTrace(
        device,
        True,
        "forced for boundary test",
        original.start_ms,
        original.finish_ms,
        original.incoming_transfer_ms,
    )
    decision = PlacementDecision(original.node, device, (selected,))
    schedule = (scheduled, *plan.schedule[1:])
    decisions = (decision, *plan.decisions[1:])
    memory = dict.fromkeys(plan.memory_used_mb, 0.0)
    if device not in memory:
        memory[device] = 0.0
    for item in schedule:
        memory[item.device] += item.memory_mb
    return replace(plan, schedule=schedule, decisions=decisions, memory_used_mb=memory)


def test_sensitivity_binds_a_plan_to_the_graph_it_describes(graph: GraphSpec) -> None:
    planner = BeamPlanner()
    plan = planner.plan(graph)

    kept_schedule = plan.schedule[:-1]
    kept_nodes = {item.node for item in kept_schedule}
    partial_memory = dict.fromkeys(plan.memory_used_mb, 0.0)
    for item in kept_schedule:
        partial_memory[item.device] += item.memory_mb
    partial = replace(
        plan,
        schedule=kept_schedule,
        decisions=tuple(item for item in plan.decisions if item.node in kept_nodes),
        memory_used_mb=partial_memory,
    )
    unknown_memory = replace(plan, memory_used_mb={**dict(plan.memory_used_mb), "ghost": 0.0})
    decision = plan.decisions[0]
    unknown_candidate = replace(
        decision,
        candidates=(*decision.candidates, CandidateTrace("ghost", False, "not considered")),
    )
    unknown_trace = replace(plan, decisions=(unknown_candidate, *plan.decisions[1:]))

    cases = (
        (partial, "every graph node"),
        (_plan_with_first_node_on(plan, "ghost"), "unknown device"),
        (_plan_with_first_node_on(plan, "gpu-0"), "incompatible device"),
        (unknown_memory, "memory summary names an unknown device"),
        (unknown_trace, "decision.*unknown device"),
    )
    for invalid, message in cases:
        with pytest.raises(GraphSailError, match=message):
            makespan_sensitivity(graph, invalid, planner)


class _RejectingPlanner:
    def plan(self, graph: GraphSpec):
        raise GraphSailError("deliberately unplaceable")


class _PerturbationRejectingPlanner:
    def __init__(self, baseline_graph: GraphSpec) -> None:
        self._baseline_graph = baseline_graph
        self._beam = BeamPlanner()

    def plan(self, graph: GraphSpec):
        if graph != self._baseline_graph:
            raise GraphSailError("deliberately unplaceable after perturbation")
        return self._beam.plan(graph)


def test_geometric_probes_catch_a_change_that_returns_by_the_range_endpoint(
    graph: GraphSpec,
) -> None:
    """Endpoint-only probing used to misreport this deterministic planner as stable."""

    class ReentrantPlanner:
        def __init__(self) -> None:
            self._beam = BeamPlanner()

        def plan(self, candidate: GraphSpec):
            factor = candidate.node_map["language-core"].latency_ms["gpu-0"] / 31.0
            selected = "gpu-1" if 1.5 <= factor <= 2.5 else "gpu-0"
            constrained = replace(
                candidate,
                nodes=tuple(
                    replace(node, pinned_device=selected) if node.id == "language-core" else node
                    for node in candidate.nodes
                ),
            )
            return self._beam.plan(constrained)

    planner = ReentrantPlanner()
    baseline = planner.plan(graph)
    assert planner.plan(perturb(graph, "language-core", "gpu-0", 2.0)).placements != (
        baseline.placements
    )
    assert planner.plan(perturb(graph, "language-core", "gpu-0", 4.0)).placements == (
        baseline.placements
    )

    record = next(
        item
        for item in placement_stability(graph, baseline, planner)
        if item.node == "language-core"
    )
    assert record.slower_factor is not None
    assert 1.5 <= record.slower_factor <= 2.0
    assert 2.0 in record.probed_factors
    assert 4.0 not in record.probed_factors
    assert not record.stable


def test_expected_planning_failures_are_reported_as_placement_changes(graph: GraphSpec) -> None:
    baseline = BeamPlanner().plan(graph)
    planner = _PerturbationRejectingPlanner(graph)
    makespan = makespan_sensitivity(graph, baseline, planner)
    stability = placement_stability(graph, baseline, planner, tolerance=0.5)
    assert all(item.placement_changed for item in makespan)
    assert all(not item.stable for item in stability)


def test_planner_must_reproduce_the_unperturbed_baseline(graph: GraphSpec) -> None:
    baseline = BeamPlanner().plan(graph)

    class MismatchedPlanner:
        def plan(self, candidate: GraphSpec):
            constrained = replace(
                candidate,
                nodes=tuple(
                    replace(node, pinned_device="gpu-1") if node.id == "language-core" else node
                    for node in candidate.nodes
                ),
            )
            return BeamPlanner().plan(constrained)

    operations = (
        lambda planner: makespan_sensitivity(graph, baseline, planner),
        lambda planner: placement_stability(graph, baseline, planner),
        lambda planner: analyze_sensitivity(graph, baseline, planner),
    )
    for operation in operations:
        with pytest.raises(GraphSailError, match=r"does not reproduce.*baseline placement"):
            operation(MismatchedPlanner())

    last = baseline.schedule[-1]
    retimed_last = replace(last, finish_ms=last.finish_ms + 1.0, compute_ms=last.compute_ms + 1.0)
    retimed_decisions = tuple(
        replace(
            decision,
            candidates=tuple(
                replace(candidate, finish_ms=retimed_last.finish_ms)
                if decision.node == last.node and candidate.device == decision.selected_device
                else candidate
                for candidate in decision.candidates
            ),
        )
        if decision.node == last.node
        else decision
        for decision in baseline.decisions
    )
    retimed = replace(
        baseline,
        schedule=(*baseline.schedule[:-1], retimed_last),
        decisions=retimed_decisions,
    )

    class RetimedPlanner:
        def plan(self, _candidate: GraphSpec):
            return retimed

    for operation in operations:
        with pytest.raises(GraphSailError, match=r"does not reproduce.*schedule and timing"):
            operation(RetimedPlanner())

    for operation in operations:
        with pytest.raises(GraphSailError, match=r"could not reproduce.*unperturbed graph"):
            operation(_RejectingPlanner())


@pytest.mark.parametrize(
    "changes",
    [
        {"probe": 0.0},
        {"min_factor": 0.0},
        {"tolerance": 0.0},
        {"samples_per_octave": 0},
    ],
)
def test_combined_analysis_validates_ranges_before_using_the_plan(
    graph: GraphSpec, changes
) -> None:
    planner = BeamPlanner()
    with pytest.raises(GraphSailError):
        analyze_sensitivity(graph, planner.plan(graph), planner, **changes)


# ---------------------------------------------------------------------------
# The report.
# ---------------------------------------------------------------------------


def test_the_short_list_is_influential_and_fragile(report) -> None:
    """Influential alone is nearly every node; fragile alone includes the safe."""

    influential = {item.node for item in report.makespan if item.matters}
    fragile = {item.node for item in report.stability if item.margin is not None}
    assert set(report.load_bearing) == influential & fragile
    assert set(report.load_bearing) == {"language-core", "vision-encoder"}
    assert "audio-encoder" not in report.load_bearing  # fragile, but not influential
    assert "format-response" not in report.load_bearing  # influential, but not fragile


def test_the_report_names_the_algorithm_it_describes(graph: GraphSpec) -> None:
    """Stability belongs to the plan and the algorithm that produced it."""

    greedy = GreedyPlanner()
    beam = BeamPlanner()
    assert analyze_sensitivity(graph, greedy.plan(graph), greedy).algorithm == greedy.name
    assert analyze_sensitivity(graph, beam.plan(graph), beam).algorithm.startswith(beam.name)
    # These planners happen to reproduce the same placement on the demo. The
    # label still comes from the planner's unperturbed reproduction, not the
    # supplied plan's metadata.
    assert analyze_sensitivity(graph, beam.plan(graph), greedy).algorithm == greedy.name


def test_the_report_serializes_without_non_finite_values(report) -> None:
    payload = report.as_dict()
    assert payload["schema_version"] == 1
    assert payload["stability_sampling"] == {
        "samples_per_octave": DEFAULT_SAMPLES_PER_OCTAVE,
        "scope": "recorded_probe_factors_only",
        "caveat": "narrow non-monotonic changes between probes can be missed",
    }
    assert payload["load_bearing"]
    assert payload["weakest"]["node"] == "language-core"
    assert payload["stability"][0]["probed_factors"]
    assert payload["stability"][0]["stability_scope"] == "recorded_probe_factors_only"
    json.dumps(payload, allow_nan=False)


def test_a_single_node_graph_has_nothing_to_flip_to(graph: GraphSpec) -> None:
    kept = tuple(device for device in graph.devices if device.name in graph.nodes[0].latency_ms)[:1]
    names = {device.name for device in kept}
    lone = replace(
        graph,
        nodes=(graph.nodes[0],),
        edges=(),
        devices=kept,
        # Links naming a device that is no longer present fail validation, so
        # trimming the inventory means trimming the fabric with it.
        links=tuple(link for link in graph.links if link.source in names and link.target in names),
    )
    planner = BeamPlanner()
    found = analyze_sensitivity(lone, planner.plan(lone), planner)
    assert found.weakest is None
    assert found.load_bearing == ()


# ---------------------------------------------------------------------------
# The command.
# ---------------------------------------------------------------------------


def test_the_command_writes_a_report(tmp_path: Path, capsys) -> None:
    graph_path = tmp_path / "graph.json"
    graph_path.write_text(json.dumps(demo_payload()), encoding="utf-8")
    output = tmp_path / "sensitivity.json"
    assert main(["sensitivity", str(graph_path), "--output", str(output)]) == 0
    printed = capsys.readouterr().out
    assert "weakest observed change" in printed
    assert "worth re-measuring" in printed
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["weakest"]["node"] == "language-core"


def test_the_command_accepts_a_narrower_search(tmp_path: Path, capsys) -> None:
    graph_path = tmp_path / "graph.json"
    graph_path.write_text(json.dumps(demo_payload()), encoding="utf-8")
    output = tmp_path / "narrow.json"
    assert (
        main(
            [
                "sensitivity",
                str(graph_path),
                "--output",
                str(output),
                "--max-factor",
                "1.05",
                "--min-factor",
                "0.95",
                "--tolerance",
                str(DEFAULT_TOLERANCE),
                "--samples-per-octave",
                "2",
            ]
        )
        == 0
    )
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["searched" if "searched" in payload else "stability"]
    assert all(item["searched_range"] == [0.95, 1.05] for item in payload["stability"])
    assert payload["stability_sampling"]["samples_per_octave"] == 2


def test_the_command_says_when_it_found_no_flip(tmp_path: Path, capsys) -> None:
    """An empty result must not read as proof the placement is unconditional."""

    graph_path = tmp_path / "graph.json"
    graph_path.write_text(json.dumps(demo_payload()), encoding="utf-8")
    assert (
        main(
            [
                "sensitivity",
                str(graph_path),
                "--output",
                str(tmp_path / "none.json"),
                "--max-factor",
                "1.001",
                "--min-factor",
                "0.999",
            ]
        )
        == 0
    )
    printed = capsys.readouterr().out
    assert "recorded probe factors" in printed
    assert "between probes" in printed


def test_the_command_rejects_an_invalid_range(tmp_path: Path) -> None:
    graph_path = tmp_path / "graph.json"
    graph_path.write_text(json.dumps(demo_payload()), encoding="utf-8")
    assert (
        main(
            [
                "sensitivity",
                str(graph_path),
                "--output",
                str(tmp_path / "bad.json"),
                "--max-factor",
                "0.5",
            ]
        )
        != 0
    )


def _one_sensitivity(node: str = "node") -> MakespanSensitivity:
    return MakespanSensitivity(
        node=node,
        device="cpu",
        estimate_ms=2,
        baseline_makespan_ms=10,
        probed_makespan_ms=10,
        response=0,
        on_critical_chain=False,
        placement_changed=False,
    )


def _one_stability(node: str = "node") -> PlacementStability:
    return PlacementStability(
        node=node,
        device="cpu",
        estimate_ms=2,
        slower_factor=None,
        faster_factor=None,
        searched_range=(0.25, 4),
        samples_per_octave=1,
        probed_factors=(2.0, 4.0, 0.5, 0.25),
    )


@pytest.mark.parametrize(
    "factory",
    [
        lambda: replace(_one_sensitivity(), response=float("nan")),
        lambda: replace(_one_sensitivity(), response=1.1),
        lambda: replace(_one_sensitivity(), on_critical_chain=1),
        lambda: replace(_one_sensitivity(), estimate_ms=0),
        lambda: replace(_one_sensitivity(), baseline_makespan_ms=-1),
        lambda: replace(_one_sensitivity(), baseline_makespan_ms=10**400),
        lambda: replace(_one_sensitivity(), response="bad"),
        lambda: replace(_one_sensitivity(), node=1),
        lambda: replace(_one_sensitivity(), node=""),
        lambda: replace(_one_sensitivity(), node="x" * (MAX_TEXT_LENGTH + 1)),
        lambda: replace(_one_sensitivity(), node="bad\ud800"),
        lambda: replace(_one_sensitivity(), node="bad\nnode"),
        lambda: replace(_one_stability(), searched_range=()),
        lambda: replace(_one_stability(), searched_range=(0.25, "bad")),
        lambda: replace(_one_stability(), searched_range=(1, 4)),
        lambda: replace(_one_stability(), slower_factor=0.5),
        lambda: replace(_one_stability(), faster_factor=2),
        lambda: replace(_one_stability(), samples_per_octave=0),
        lambda: replace(_one_stability(), samples_per_octave=True),
        lambda: replace(_one_stability(), probed_factors=()),
        lambda: replace(_one_stability(), probed_factors=(2.0, 2.0, 0.5, 0.25)),
        lambda: replace(_one_stability(), probed_factors=(1.0, 2.0, 0.5, 0.25)),
        lambda: replace(_one_stability(), probed_factors=(8.0, 0.5, 0.25)),
        lambda: replace(_one_stability(), probed_factors=(2.0, 4.0)),
        lambda: replace(_one_stability(), probed_factors=(2.0, 0.5, 0.25)),
        lambda: replace(_one_stability(), slower_factor=1.5),
        lambda: replace(_one_stability(), faster_factor=0.75),
    ],
)
def test_sensitivity_output_records_reject_impossible_scalars(factory) -> None:
    with pytest.raises(GraphSailError):
        factory()


def test_sensitivity_report_snapshots_collections_and_checks_cross_record_links() -> None:
    source_sensitivity = _one_sensitivity()
    source_stability = _one_stability()
    makespan_source = [source_sensitivity]
    stability_source = [source_stability]
    report = SensitivityReport("graph", "planner", 10, makespan_source, stability_source)  # type: ignore[arg-type]
    object.__setattr__(source_sensitivity, "baseline_makespan_ms", 999)
    object.__setattr__(source_stability, "estimate_ms", 999)
    makespan_source.clear()
    stability_source.clear()
    assert len(report.makespan) == len(report.stability) == 1
    assert report.makespan[0].baseline_makespan_ms == 10
    assert report.stability[0].estimate_ms == 2

    with pytest.raises(GraphSailError, match="same cells"):
        replace(report, stability=())
    with pytest.raises(GraphSailError, match=r"baseline.*inconsistent"):
        replace(report, baseline_makespan_ms=11)
    with pytest.raises(GraphSailError, match=r"estimates.*inconsistent"):
        replace(report, stability=(replace(report.stability[0], estimate_ms=3),))
    with pytest.raises(GraphSailError, match="samples_per_octave"):
        replace(report, samples_per_octave=2)

    with pytest.raises(GraphSailError, match="unique and sorted"):
        SensitivityReport(
            "graph",
            "planner",
            10,
            (_one_sensitivity("z"), _one_sensitivity("a")),
            (_one_stability("z"), _one_stability("a")),
        )
    with pytest.raises(GraphSailError, match="unique and sorted"):
        SensitivityReport(
            "graph",
            "planner",
            10,
            (_one_sensitivity("a"), _one_sensitivity("z")),
            (_one_stability("z"), _one_stability("a")),
        )
    with pytest.raises(GraphSailError, match="must be an iterable"):
        SensitivityReport("graph", "planner", 10, None, ())  # type: ignore[arg-type]
    with pytest.raises(GraphSailError, match="MakespanSensitivity"):
        SensitivityReport("graph", "planner", 10, ("bad",), ())  # type: ignore[arg-type]


def test_sensitivity_nested_iterables_are_bounded_before_snapshot(monkeypatch) -> None:
    monkeypatch.setattr(sensitivity_module, "MAX_NODES", 2)
    yielded = 0

    def endless_makespan():
        nonlocal yielded
        while True:
            yielded += 1
            yield _one_sensitivity()

    with pytest.raises(GraphSailError, match="exceeds the 2-item limit"):
        SensitivityReport("graph", "planner", 10, endless_makespan(), ())  # type: ignore[arg-type]
    assert yielded == 3


def test_stability_iterables_are_bounded_and_replace_revalidates_nested_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    yielded = 0

    def endless_range():
        nonlocal yielded
        while True:
            yielded += 1
            yield 0.25

    with pytest.raises(GraphSailError, match="exceeds the 2-item limit"):
        replace(_one_stability(), searched_range=endless_range())  # type: ignore[arg-type]
    assert yielded == 3

    source = _one_stability()
    original_probe_limit = sensitivity_module.MAX_STABILITY_PROBES_PER_RECORD
    monkeypatch.setattr(sensitivity_module, "MAX_STABILITY_PROBES_PER_RECORD", 2)
    probe_yields = 0

    def endless_probes():
        nonlocal probe_yields
        while True:
            probe_yields += 1
            yield 0.5

    with pytest.raises(GraphSailError, match="exceeds the 2-item limit"):
        replace(source, probed_factors=endless_probes())  # type: ignore[arg-type]
    assert probe_yields == 3
    monkeypatch.setattr(sensitivity_module, "MAX_STABILITY_PROBES_PER_RECORD", original_probe_limit)

    report = SensitivityReport("graph", "planner", 10, (_one_sensitivity(),), (_one_stability(),))
    object.__setattr__(report.makespan[0], "baseline_makespan_ms", 11)
    with pytest.raises(GraphSailError, match=r"baseline.*inconsistent"):
        replace(report)
    with pytest.raises(GraphSailError, match=r"baseline.*inconsistent"):
        report.as_dict()


def test_report_serialization_revalidates_recorded_probe_factors() -> None:
    report = SensitivityReport("graph", "planner", 10, (_one_sensitivity(),), (_one_stability(),))
    object.__setattr__(report.stability[0], "probed_factors", (2.0, 4.0))
    with pytest.raises(GraphSailError, match="both sides"):
        report.as_dict()


def test_public_derived_properties_revalidate_forced_mutations() -> None:
    sensitivity = _one_sensitivity()
    object.__setattr__(sensitivity, "response", float("nan"))
    with pytest.raises(GraphSailError, match="response must be finite"):
        _ = sensitivity.matters

    stability = _one_stability()
    object.__setattr__(stability, "probed_factors", (2.0, 4.0))
    for read_property in (lambda: stability.stable, lambda: stability.margin):
        with pytest.raises(GraphSailError, match="both sides"):
            read_property()

    report = SensitivityReport("graph", "planner", 10, (_one_sensitivity(),), (_one_stability(),))
    object.__setattr__(report.makespan[0], "response", float("nan"))
    for read_property in (lambda: report.load_bearing, lambda: report.weakest):
        with pytest.raises(GraphSailError, match="response must be finite"):
            read_property()
