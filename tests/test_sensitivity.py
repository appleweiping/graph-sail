"""How much a plan depends on the estimates it was given."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from graph_sail.cli import main
from graph_sail.demo import demo_graph, demo_payload
from graph_sail.errors import GraphSailError
from graph_sail.models import GraphSpec
from graph_sail.planner import BeamPlanner, GreedyPlanner
from graph_sail.sensitivity import (
    DEFAULT_MAX_FACTOR,
    DEFAULT_MIN_FACTOR,
    DEFAULT_PROBE,
    DEFAULT_TOLERANCE,
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
        flipped = planner.plan(perturb(graph, item.node, item.device, item.slower_factor * 1.02))
        assert {entry.node: entry.device for entry in flipped.schedule} != baseline


def test_just_inside_a_flip_point_the_placement_still_holds(graph: GraphSpec, report) -> None:
    planner = BeamPlanner()
    baseline = {item.node: item.device for item in planner.plan(graph).schedule}
    for item in report.stability:
        if item.slower_factor is None:
            continue
        held = planner.plan(perturb(graph, item.node, item.device, item.slower_factor * 0.9))
        assert {entry.node: entry.device for entry in held.schedule} == baseline


def test_a_stable_estimate_reports_the_range_it_survived(report) -> None:
    stable = [item for item in report.stability if item.stable]
    assert stable
    for item in stable:
        assert item.margin is None
        assert item.searched_range == (DEFAULT_MIN_FACTOR, DEFAULT_MAX_FACTOR)


def test_a_wider_search_can_only_find_more(graph: GraphSpec) -> None:
    planner = BeamPlanner()
    plan = planner.plan(graph)
    narrow = placement_stability(graph, plan, planner, max_factor=1.2, min_factor=0.9)
    wide = placement_stability(graph, plan, planner, max_factor=8.0, min_factor=0.1)
    narrow_found = {item.node for item in narrow if not item.stable}
    wide_found = {item.node for item in wide if not item.stable}
    assert narrow_found <= wide_found


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


def test_the_report_serializes_without_non_finite_values(report) -> None:
    payload = report.as_dict()
    assert payload["schema_version"] == 1
    assert payload["load_bearing"]
    assert payload["weakest"]["node"] == "language-core"
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
    assert "weakest estimate" in printed
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
            ]
        )
        == 0
    )
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["searched" if "searched" in payload else "stability"]
    assert all(item["searched_range"] == [0.95, 1.05] for item in payload["stability"])


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
    assert "not the same as none being able to" in capsys.readouterr().out


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
