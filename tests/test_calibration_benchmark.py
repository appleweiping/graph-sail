from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

import graph_sail.benchmark as benchmark_module
import graph_sail.calibration as calibration_module
from graph_sail.benchmark import (
    BenchmarkResult,
    PlannerBenchmark,
    benchmark_graph,
    write_benchmark,
)
from graph_sail.calibration import (
    CalibrationCell,
    CalibrationResult,
    LatencyObservation,
    calibrate_graph,
    graph_to_dict,
    load_observations,
    write_calibration_bundle,
)
from graph_sail.cli import main
from graph_sail.demo import demo_graph, demo_payload
from graph_sail.errors import OutputError, ValidationError
from graph_sail.io import graph_from_dict
from graph_sail.planner import GreedyPlanner


def test_calibration_uses_median_and_preserves_unobserved_cells():
    graph = demo_graph()
    result = calibrate_graph(
        graph,
        (
            LatencyObservation("vision-encoder", "gpu-0", 12, "r1"),
            LatencyObservation("vision-encoder", "gpu-0", 100, "outlier"),
            LatencyObservation("vision-encoder", "gpu-0", 14, "r2"),
        ),
    )
    calibrated = result.graph.node_map["vision-encoder"]
    assert calibrated.latency_ms["gpu-0"] == 14
    assert calibrated.latency_ms["gpu-1"] == graph.node_map["vision-encoder"].latency_ms["gpu-1"]
    assert result.cells[0].to_dict() == {
        "node": "vision-encoder",
        "device": "gpu-0",
        "samples": 3,
        "median_ms": 14.0,
        "minimum_ms": 12.0,
        "maximum_ms": 100.0,
        "run_ids": ["outlier", "r1", "r2"],
    }


def test_calibration_strict_and_relaxed_unknown_cells():
    unknown = (LatencyObservation("ghost", "cpu", 2),)
    with pytest.raises(ValidationError, match="unknown latency cell"):
        calibrate_graph(demo_graph(), unknown)
    with pytest.raises(ValidationError, match="no observations matched"):
        calibrate_graph(demo_graph(), unknown, strict=False)
    mixed = (*unknown, LatencyObservation("decode-image", "cpu", 3))
    result = calibrate_graph(demo_graph(), mixed, strict=False)
    assert result.ignored_cells == (("ghost", "cpu"),)


def test_observation_loader_is_strict_and_reports_lines(tmp_path):
    path = tmp_path / "observations.jsonl"
    path.write_text(
        '\n{"node":"decode-image","device":"cpu","latency_ms":4,"run_id":"a"}\n',
        encoding="utf-8",
    )
    assert load_observations(path)[0].run_id == "a"
    path.write_text('{"node":"a","node":"b","device":"cpu","latency_ms":1}', encoding="utf-8")
    with pytest.raises(ValidationError, match=r"line 1.*duplicate"):
        load_observations(path)


@pytest.mark.parametrize(
    "document",
    [
        "[]",
        '{"node":"n","device":"d","latency_ms":0}',
        '{"node":"n","device":"d","latency_ms":NaN}',
        '{"node":"n","device":"d","latency_ms":1,"extra":2}',
    ],
)
def test_observation_loader_rejects_malformed_rows(tmp_path, document):
    path = tmp_path / "bad.jsonl"
    path.write_text(document, encoding="utf-8")
    with pytest.raises(ValidationError):
        load_observations(path)


def test_observation_loader_rejects_empty_and_missing_files(tmp_path):
    empty = tmp_path / "empty.jsonl"
    empty.write_text("\n", encoding="utf-8")
    with pytest.raises(ValidationError, match="at least one"):
        load_observations(empty)
    with pytest.raises(ValidationError, match="cannot read"):
        load_observations(tmp_path / "missing")


def test_observation_loader_resource_limits(tmp_path, monkeypatch):
    path = tmp_path / "observations.jsonl"
    line = '{"node":"decode-image","device":"cpu","latency_ms":1}'
    path.write_text(line + "\n" + line + "\n", encoding="utf-8")
    monkeypatch.setattr(calibration_module, "MAX_OBSERVATIONS", 1)
    with pytest.raises(ValidationError, match="record limit"):
        load_observations(path)
    monkeypatch.setattr(calibration_module, "MAX_INPUT_BYTES", 10)
    with pytest.raises(ValidationError, match="byte limit"):
        load_observations(path)


def test_graph_serialization_round_trips():
    graph = demo_graph()
    restored = graph_from_dict(graph_to_dict(graph))
    assert restored == graph


def test_calibration_bundle_is_machine_readable(tmp_path):
    result = calibrate_graph(demo_graph(), (LatencyObservation("decode-image", "cpu", 4.25),))
    paths = write_calibration_bundle(result, tmp_path / "out")
    assert graph_from_dict(json.loads(paths["graph"].read_text())) == result.graph
    report = json.loads(paths["calibration"].read_text())
    assert report["schema_version"] == 1
    assert report["estimator"] == "median"


def test_calibration_bundle_rejects_file_target(tmp_path):
    target = tmp_path / "file"
    target.write_text("keep", encoding="utf-8")
    result = calibrate_graph(demo_graph(), (LatencyObservation("decode-image", "cpu", 4),))
    with pytest.raises(OutputError, match="cannot write"):
        write_calibration_bundle(result, target)


def test_calibration_atomic_write_cleans_staging_file_on_replace_failure(tmp_path, monkeypatch):
    result = calibrate_graph(demo_graph(), (LatencyObservation("decode-image", "cpu", 4),))
    target = tmp_path / "bundle"

    def fail_replace(_self, _target):
        raise OSError("simulated replace failure")

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(OutputError, match="simulated replace failure"):
        write_calibration_bundle(result, target)
    assert list(target.iterdir()) == []


def test_benchmark_compares_deterministic_plans_with_fake_clock():
    ticks = iter(range(0, 100_000_000, 1_000_000))
    result = benchmark_graph(
        demo_graph(), repeats=3, warmups=1, beam_width=4, clock_ns=lambda: next(ticks)
    )
    payload = result.to_dict()
    assert payload["schema_version"] == 1
    assert payload["protocol"]["runtime_scope"] == "planner.plan only"
    assert [item["algorithm"] for item in payload["planners"]] == [
        "greedy-earliest-finish",
        "beam-earliest-finish:4",
    ]
    assert all(item["median_runtime_ms"] == 1 for item in payload["planners"])
    assert all(len(item["plan_sha256"]) == 64 for item in payload["planners"])


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [({"repeats": 0}, "repeats"), ({"warmups": -1}, "warmups"), ({"repeats": True}, "repeats")],
)
def test_benchmark_validates_protocol(kwargs, message):
    with pytest.raises(ValueError, match=message):
        benchmark_graph(demo_graph(), **kwargs)


def test_benchmark_type_and_work_resource_guards(monkeypatch):
    with pytest.raises(ValueError, match="GraphSpec"):
        benchmark_graph("bad")  # type: ignore[arg-type]
    monkeypatch.setattr(benchmark_module, "MAX_BENCHMARK_WORK", 1)
    with pytest.raises(ValueError, match="benchmark work"):
        benchmark_graph(demo_graph(), repeats=1, warmups=0)


def test_benchmark_rejects_non_monotonic_clock():
    ticks = iter([2, 1])
    with pytest.raises(ValueError, match="monotonic"):
        benchmark_graph(demo_graph(), repeats=1, warmups=0, clock_ns=lambda: next(ticks))


def test_benchmark_writer_and_cli(tmp_path):
    result = benchmark_graph(demo_graph(), repeats=1, warmups=0)
    path = write_benchmark(result, tmp_path / "nested" / "benchmark.json")
    assert json.loads(path.read_text())["workload"]["node_count"] == 6
    target = tmp_path / "directory"
    target.mkdir()
    with pytest.raises(OutputError, match="cannot write"):
        write_benchmark(result, target)


def test_benchmark_atomic_write_cleans_staging_file_on_replace_failure(tmp_path, monkeypatch):
    result = benchmark_graph(demo_graph(), repeats=1, warmups=0)
    before = set(tmp_path.iterdir())

    def fail_replace(_self, _target):
        raise OSError("simulated replace failure")

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(OutputError, match="simulated replace failure"):
        write_benchmark(result, tmp_path / "benchmark.json")
    assert set(tmp_path.iterdir()) == before


def test_greedy_large_chain_runtime_guard():
    payload = {
        "name": "250-node-chain",
        "devices": [{"name": "cpu", "memory_mb": 10_000}],
        "nodes": [
            {"id": f"n-{index:03}", "kind": "stage", "memory_mb": 1, "latency_ms": {"cpu": 1}}
            for index in range(250)
        ],
        "edges": [
            {"source": f"n-{index:03}", "target": f"n-{index + 1:03}"} for index in range(249)
        ],
    }
    graph = graph_from_dict(payload)
    started = time.perf_counter()
    plan = GreedyPlanner().plan(graph)
    assert time.perf_counter() - started < 5.0
    assert plan.makespan_ms == 250


def test_demo_payload_can_be_serialized_by_public_converter():
    assert graph_to_dict(graph_from_dict(demo_payload())) == graph_to_dict(demo_graph())


def test_checked_in_measurements_calibrate_demo_graph():
    root = Path(__file__).parents[1]
    observations = load_observations(root / "examples" / "measurements.jsonl")
    result = calibrate_graph(demo_graph(), observations)
    assert len(result.cells) == 2
    assert result.cells[0].run_ids == ("trial-01", "trial-02", "trial-03")


def test_checked_in_reference_benchmark_has_current_functional_digests():
    reference_path = Path(__file__).parents[1] / "benchmarks" / "results" / "windows-python314.json"
    reference = json.loads(reference_path.read_text(encoding="utf-8"))
    ticks = iter(range(0, 100_000_000, 1_000_000))
    current = benchmark_graph(
        demo_graph(),
        repeats=7,
        warmups=2,
        beam_width=16,
        clock_ns=lambda: next(ticks),
    ).to_dict()
    assert reference["workload"] == current["workload"]
    for saved, regenerated in zip(reference["planners"], current["planners"], strict=True):
        for key in (
            "algorithm",
            "repeats",
            "makespan_ms",
            "relative_makespan",
            "plan_sha256",
        ):
            assert saved[key] == regenerated[key]
    assert reference["environment"]["implementation"] == "CPython"
    assert reference["environment"]["python"].startswith("3.14.")


def test_calibrate_and_benchmark_cli(tmp_path, capsys):
    graph_path = tmp_path / "graph.json"
    graph_path.write_text(json.dumps(demo_payload()), encoding="utf-8")
    observations = tmp_path / "observations.jsonl"
    observations.write_text(
        '{"node":"decode-image","device":"cpu","latency_ms":4.5}\n', encoding="utf-8"
    )
    calibrated = tmp_path / "calibrated"
    assert main(["calibrate", str(graph_path), str(observations), "--output", str(calibrated)]) == 0
    assert "calibrated 1 latency cells" in capsys.readouterr().out
    benchmark = tmp_path / "benchmark.json"
    assert (
        main(
            [
                "benchmark",
                str(graph_path),
                "--repeats",
                "1",
                "--warmups",
                "0",
                "--output",
                str(benchmark),
            ]
        )
        == 0
    )
    assert json.loads(benchmark.read_text())["schema_version"] == 1
    assert "benchmarked 6 nodes" in capsys.readouterr().out


def test_public_benchmark_records_reject_nonfinite_huge_and_inconsistent_values(tmp_path):
    digest = "a" * 64
    good = PlannerBenchmark("a", 1, 1, 1, 2, 1, digest)
    with pytest.raises(ValueError, match="p95"):
        PlannerBenchmark("a", 1, 2, 1, 2, 1, digest)
    with pytest.raises(ValueError, match="finite"):
        PlannerBenchmark("a", 1, float("nan"), 1, 2, 1, digest)
    with pytest.raises(ValueError, match="repeats"):
        PlannerBenchmark("a", 10**100, 1, 1, 2, 1, digest)
    with pytest.raises(ValueError, match="finite"):
        PlannerBenchmark("a", 1, 10**400, 10**400, 2, 1, digest)
    with pytest.raises(ValueError, match="Unicode"):
        PlannerBenchmark("bad\ud800", 1, 1, 1, 2, 1, digest)
    with pytest.raises(ValueError, match="inconsistent"):
        BenchmarkResult(
            "g",
            1,
            0,
            "c" * 64,
            0,
            (good, PlannerBenchmark("b", 1, 1, 1, 4, 1, "b" * 64)),
        )


def test_direct_calibration_records_are_validated():
    with pytest.raises(ValidationError, match="latency"):
        LatencyObservation("decode-image", "cpu", float("nan"))
    with pytest.raises(ValidationError, match="calibrated median"):
        calibrate_graph(
            demo_graph(),
            (
                LatencyObservation("decode-image", "cpu", 1e308),
                LatencyObservation("decode-image", "cpu", 1e308),
            ),
        )
    with pytest.raises(ValidationError, match="GraphSpec"):
        calibrate_graph("bad", ())  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="iterable"):
        calibrate_graph(demo_graph(), None)  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="LatencyObservation"):
        calibrate_graph(demo_graph(), ("bad",))  # type: ignore[arg-type]


def test_calibration_public_collection_type_guards():
    with pytest.raises(ValueError, match="run_ids"):
        CalibrationCell("n", "d", 1, 1, 1, 1, None)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="collections"):
        CalibrationResult(demo_graph(), None, ())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="CalibrationCell"):
        calibration_module._validate_cell("bad")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="CalibrationResult"):
        calibration_module._validate_result("bad")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="GraphSpec"):
        forged = object.__new__(CalibrationResult)
        object.__setattr__(forged, "graph", "bad")
        object.__setattr__(forged, "cells", ())
        object.__setattr__(forged, "ignored_cells", ())
        calibration_module._validate_result(forged)


@pytest.mark.parametrize(
    "factory",
    [
        lambda: PlannerBenchmark("", 1, 1, 1, 1, 1, "a" * 64),
        lambda: PlannerBenchmark("a", 1, 1, 1, 1, 0.5, "a" * 64),
        lambda: PlannerBenchmark("a", 1, 1, 1, 1, 1, "short"),
        lambda: PlannerBenchmark("a", 1, 1, 1, 1, 1, "z" * 64),
        lambda: PlannerBenchmark("bad\x00name", 1, 1, 1, 1, 1, "a" * 64),
        lambda: PlannerBenchmark("a", 1, True, 1, 1, 1, "a" * 64),
    ],
)
def test_public_planner_benchmark_rejects_malformed_records(factory):
    with pytest.raises(ValueError):
        factory()


def test_public_benchmark_rejects_bad_collection_shapes():
    one = PlannerBenchmark("a", 1, 1, 1, 1, 1, "a" * 64)
    two_repeats = PlannerBenchmark("b", 2, 1, 1, 1, 1, "b" * 64)
    for factory in (
        lambda: BenchmarkResult("g", 1, 0, "c" * 64, 0, None),
        lambda: BenchmarkResult("g", 1, 0, "c" * 64, 0, ()),
        lambda: BenchmarkResult("g", 1, 0, "c" * 64, 0, (one, one)),
        lambda: BenchmarkResult("g", 1, 0, "c" * 64, 0, (one, two_repeats)),
        lambda: BenchmarkResult("g", 1, 0, "c" * 64, 0, ("bad",)),
        lambda: BenchmarkResult("", 1, 0, "c" * 64, 0, (one,)),
        lambda: BenchmarkResult("g", 10**100, 0, "c" * 64, 0, (one,)),
        lambda: BenchmarkResult("g", 1, 0, "short", 0, (one,)),
    ):
        with pytest.raises(ValueError):
            factory()
    with pytest.raises(ValueError, match="BenchmarkResult"):
        benchmark_module._validate_benchmark_result("bad")  # type: ignore[arg-type]


def test_public_calibration_records_reject_inconsistent_shapes(tmp_path):
    graph = demo_graph()
    good = CalibrationCell("decode-image", "cpu", 1, 4, 4, 4)
    bad_cell_factories = (
        lambda: CalibrationCell("decode-image", "cpu", True, 4, 4, 4),
        lambda: CalibrationCell("decode-image", "cpu", 1, 3, 4, 2),
        lambda: CalibrationCell("decode-image", "cpu", 1, float("nan"), 4, 4),
        lambda: CalibrationCell("decode-image", "cpu", 1, 4, 4, 4, ("z", "a")),
    )
    for factory in bad_cell_factories:
        with pytest.raises(ValueError):
            factory()
    result_factories = (
        lambda: CalibrationResult(graph, (), ()),
        lambda: CalibrationResult(graph, (good, good), ()),
        lambda: CalibrationResult(graph, (good,), (("z", "d"), ("a", "d"))),
        lambda: CalibrationResult(graph, (good,), (("bad",),)),
    )
    for factory in result_factories:
        with pytest.raises(ValueError):
            factory()
