from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from benchmarks.benchmark_actors import Workload, _oracle_sum, benchmark, expected_digest, run_trial


def test_closed_form_oracle_matches_independent_literal_sums():
    for value in (0, 1, 96, 97, 101, 213):
        for rounds in (1, 2, 96, 97, 98, 195, 1000):
            assert _oracle_sum(value, rounds) == sum(
                (value + index) % 97 for index in range(rounds)
            )


@pytest.mark.parametrize("actors,window", [(1, 1), (2, 1), (2, 4)])
def test_real_actor_trials_preserve_sharded_state_and_bound_inflight_requests(actors, window):
    config = Workload(7, 99, actors, window)
    direct = run_trial(config, "direct")
    process = run_trial(config, "process")
    assert direct["output_sha256"] == process["output_sha256"] == expected_digest(config)
    assert process["observed_processes"] == actors
    assert direct["observed_processes"] == 1
    assert process["max_pending_observed"] == min(window, config.requests)
    for trial in (direct, process):
        assert trial["processing_ns"] > 0
        assert trial["total_ns"] == sum(
            trial[key] for key in ("startup_ns", "processing_ns", "shutdown_ns")
        )
        assert trial["requests_per_second"] > 0


def test_benchmark_keeps_all_trials_and_reports_nearest_rank_percentile():
    result = benchmark(Workload(4, 5, 1, 2), repeats=2, warmups=1)
    for mode in result["modes"]:
        assert len(mode["trials"]) == 2
        assert mode["p95_processing_ns"] == max(trial["processing_ns"] for trial in mode["trials"])
        assert mode["trials"][0]["output_sha256"] == result["expected_output_sha256"]


@pytest.mark.parametrize(
    "field,value", [("requests", True), ("rounds", 0), ("actors", 9), ("window", 257)]
)
def test_invalid_workload_settings_refused(field, value):
    with pytest.raises(ValueError):
        replace(Workload(), **{field: value})


def test_total_work_and_repetition_bounds():
    with pytest.raises(ValueError):
        Workload(100_000, 100_000)
    with pytest.raises(ValueError):
        Workload(1, 1, 2)
    for options in ({"repeats": True}, {"repeats": 22}, {"warmups": -1}, {"warmups": True}):
        with pytest.raises(ValueError):
            benchmark(Workload(), **options)
    with pytest.raises(ValueError):
        benchmark(Workload(1000, 10_000), repeats=6, warmups=0)
    with pytest.raises(ValueError):
        run_trial(Workload(), "unknown")


def test_script_entrypoint_is_spawn_safe_and_rejects_invalid_work_without_output():
    script = Path(__file__).parents[1] / "benchmarks" / "benchmark_actors.py"
    command = [
        sys.executable,
        str(script),
        "--requests",
        "3",
        "--rounds",
        "2",
        "--actors",
        "1",
        "--repeats",
        "1",
        "--warmups",
        "0",
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=True, timeout=45)
    assert json.loads(completed.stdout)["kind"] == "graph-sail-process-actor-benchmark"
    refused = subprocess.run(
        [*command, "--requests", "0"], capture_output=True, text=True, timeout=45
    )
    assert refused.returncode == 2 and refused.stdout == ""
