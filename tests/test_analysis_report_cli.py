from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from graph_sail import __version__
from graph_sail.analysis import analyze_plan, critical_chain
from graph_sail.cli import main
from graph_sail.demo import demo_graph, demo_payload
from graph_sail.errors import OutputError, PlanningError
from graph_sail.io import graph_from_dict
from graph_sail.planner import BeamPlanner
from graph_sail.report import _dot, render_dot, render_html, write_report_bundle


def test_cli_reports_package_version(capsys):
    with pytest.raises(SystemExit) as error:
        main(["--version"])
    assert error.value.code == 0
    assert capsys.readouterr().out == f"graph-sail {__version__}\n"


@pytest.fixture
def demo_plan():
    graph = demo_graph()
    return graph, BeamPlanner().plan(graph)


def test_analysis_counts_compute_transfer_and_cross_edges(demo_plan):
    graph, plan = demo_plan
    metrics = analyze_plan(graph, plan)
    assert metrics.total_compute_ms == pytest.approx(57.7)
    assert metrics.cross_device_edges == 4
    assert metrics.total_transfer_ms > 0


def test_analysis_utilization_is_bounded(demo_plan):
    graph, plan = demo_plan
    metrics = analyze_plan(graph, plan)
    assert set(metrics.device_utilization) == {"cpu", "gpu-0", "gpu-1"}
    assert all(0 <= value <= 1 for value in metrics.device_utilization.values())
    assert all(0 <= value <= 1 for value in metrics.memory_utilization.values())


def test_critical_chain_reaches_finishing_node(demo_plan):
    graph, plan = demo_plan
    chain = critical_chain(graph, plan)
    assert chain[-1] == "format-response"
    assert "language-core" in chain


def test_critical_node_and_chain_use_the_same_tie_break():
    payload = {
        "devices": [{"name": "d1", "memory_mb": 1}, {"name": "d2", "memory_mb": 1}],
        "nodes": [
            {
                "id": "a",
                "kind": "k",
                "memory_mb": 1,
                "latency_ms": {"d1": 5},
                "pinned_device": "d1",
            },
            {
                "id": "z",
                "kind": "k",
                "memory_mb": 1,
                "latency_ms": {"d2": 5},
                "pinned_device": "d2",
            },
        ],
    }
    graph = graph_from_dict(payload)
    plan = BeamPlanner().plan(graph)
    assert plan.critical_node == "z"
    assert critical_chain(graph, plan)[-1] == plan.critical_node


def test_report_bundle_contains_real_plan_data(tmp_path, demo_plan):
    graph, plan = demo_plan
    paths = write_report_bundle(graph, plan, tmp_path)
    payload = json.loads(paths["plan"].read_text(encoding="utf-8"))
    assert payload["makespan_ms"] == pytest.approx(plan.makespan_ms)
    assert payload["metrics"]["critical_chain"][-1] == "format-response"
    assert paths["report"].stat().st_size > 2_000
    assert paths["graph"].read_text(encoding="utf-8").startswith("digraph")


def test_scheduled_node_rejects_nonfinite_manual_result(demo_plan):
    _, plan = demo_plan
    with pytest.raises(PlanningError, match=r"finish_ms must be finite"):
        replace(plan.schedule[-1], finish_ms=float("inf"))


def test_scheduled_node_rejects_inconsistent_compute_time(demo_plan):
    _, plan = demo_plan
    with pytest.raises(PlanningError, match=r"finish_ms must equal"):
        replace(plan.schedule[0], compute_ms=1e308)


def test_scheduled_node_replace_rechecks_interval(demo_plan):
    _, plan = demo_plan
    with pytest.raises(PlanningError, match=r"finish_ms must equal"):
        replace(plan.schedule[0], finish_ms=1e-308, compute_ms=1e308)


def test_plan_replace_rechecks_memory_summary(demo_plan):
    _, plan = demo_plan
    with pytest.raises(PlanningError, match=r"memory used.*must be finite"):
        replace(plan, memory_used_mb={**plan.memory_used_mb, "cpu": float("inf")})


def test_analysis_rejects_overflowing_transfer_total(demo_plan, monkeypatch):
    graph, plan = demo_plan

    def huge_transfer(*_args, **_kwargs):
        return 1e308

    monkeypatch.setattr(type(graph), "transfer_ms", huge_transfer)
    with pytest.raises(PlanningError, match=r"total transfer estimate overflowed"):
        analyze_plan(graph, plan)


def test_critical_chain_rejects_nonfinite_ready_time(demo_plan, monkeypatch):
    graph, plan = demo_plan

    def infinite_transfer(*_args, **_kwargs):
        return float("inf")

    monkeypatch.setattr(type(graph), "transfer_ms", infinite_transfer)
    with pytest.raises(PlanningError, match=r"critical-chain time overflowed"):
        critical_chain(graph, plan)


def test_critical_chain_of_empty_plan_is_empty():
    graph = demo_graph()
    empty = BeamPlanner().plan(graph)
    empty = replace(empty, schedule=(), memory_used_mb={}, decisions=())
    assert critical_chain(graph, empty) == ()


def test_plan_json_serializer_forbids_nonstandard_nan(tmp_path, demo_plan, monkeypatch):
    graph, plan = demo_plan
    invalid_metrics = replace(analyze_plan(graph, plan), total_transfer_ms=float("nan"))
    monkeypatch.setattr("graph_sail.report.analyze_plan", lambda graph, plan: invalid_metrics)
    with pytest.raises(OutputError, match=r"Out of range float"):
        write_report_bundle(graph, plan, tmp_path)


def test_html_escapes_graph_name():
    payload = demo_payload()
    payload["name"] = "<script>alert(1)</script>"
    graph = demo_graph().__class__(
        name=payload["name"],
        devices=demo_graph().devices,
        nodes=demo_graph().nodes,
        edges=demo_graph().edges,
        links=demo_graph().links,
        default_bandwidth_mb_s=demo_graph().default_bandwidth_mb_s,
        default_link_latency_ms=demo_graph().default_link_latency_ms,
    )
    report = render_html(graph, BeamPlanner().plan(graph))
    assert "<script>alert" not in report
    assert "&lt;script&gt;" in report


def test_dot_escapes_labels(demo_plan):
    graph, plan = demo_plan
    document = render_dot(graph, plan)
    assert 'subgraph "cluster_cpu"' in document
    assert '"vision-encoder" -> "language-core"' in document


def test_dot_encoder_never_emits_raw_control_characters():
    encoded = _dot("safe\r\x00value")
    assert "\r" not in encoded
    assert "\x00" not in encoded
    assert r"\\u000d" in encoded
    assert r"\\u0000" in encoded


def test_dot_encoder_escapes_graphviz_string_metacharacters():
    assert _dot('path\\part"line\nnext') == 'path\\\\part\\"line\\nnext'


def test_cli_validate(tmp_path, capsys):
    path = tmp_path / "graph.json"
    path.write_text(json.dumps(demo_payload()), encoding="utf-8")
    assert main(["validate", str(path)]) == 0
    assert "valid: multimodal-assistant" in capsys.readouterr().out


@pytest.mark.parametrize("algorithm", ["greedy", "beam"])
def test_cli_plan_writes_bundle(tmp_path, capsys, algorithm):
    path = tmp_path / "graph.json"
    path.write_text(json.dumps(demo_payload()), encoding="utf-8")
    output = tmp_path / "output"
    assert main(["plan", str(path), "--algorithm", algorithm, "--output", str(output)]) == 0
    assert (output / "plan.json").exists()
    assert "planned 6 nodes" in capsys.readouterr().out


def test_cli_demo_can_write_input(tmp_path):
    output = tmp_path / "demo"
    assert main(["demo", "--output", str(output), "--write-input"]) == 0
    assert json.loads((output / "graph.json").read_text())["nodes"]


def test_checked_in_demo_text_matches_current_generator(tmp_path):
    output = tmp_path / "demo"
    assert main(["demo", "--output", str(output), "--write-input"]) == 0
    checked_in = Path(__file__).resolve().parents[1] / "examples" / "demo-output"
    for filename in ("graph.json", "plan.json", "plan.dot", "report.html"):
        assert (output / filename).read_bytes() == (checked_in / filename).read_bytes()


def test_ignore_rules_keep_the_checked_in_demo_visible():
    ignore_lines = {
        line.strip()
        for line in (Path(__file__).resolve().parents[1] / ".gitignore").read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    assert "/demo-output/" in ignore_lines
    assert "demo-output/" not in ignore_lines


def test_cli_returns_two_for_invalid_graph(tmp_path, capsys):
    path = tmp_path / "broken.json"
    path.write_text("[]", encoding="utf-8")
    assert main(["validate", str(path)]) == 2
    assert "graph-sail: error" in capsys.readouterr().err


def test_cli_output_target_file_returns_two(tmp_path, capsys):
    path = tmp_path / "graph.json"
    path.write_text(json.dumps(demo_payload()), encoding="utf-8")
    output = tmp_path / "already-a-file"
    output.write_text("keep", encoding="utf-8")
    assert main(["plan", str(path), "--output", str(output)]) == 2
    assert "cannot write report bundle" in capsys.readouterr().err
    assert output.read_text(encoding="utf-8") == "keep"
