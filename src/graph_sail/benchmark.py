"""Reproducible planner baseline comparison and machine-readable metrics."""

from __future__ import annotations

import hashlib
import json
import math
import platform
import statistics
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from graph_sail.errors import OutputError
from graph_sail.limits import MAX_BEAM_WIDTH, MAX_BENCHMARK_WORK, MAX_DEVICES, MAX_NODES
from graph_sail.models import GraphSpec, PlanResult
from graph_sail.planner import BeamPlanner, GreedyPlanner


@dataclass(frozen=True, slots=True)
class PlannerBenchmark:
    """Quality and host-side runtime observations for one planner."""

    algorithm: str
    repeats: int
    median_runtime_ms: float
    p95_runtime_ms: float
    makespan_ms: float
    relative_makespan: float
    plan_sha256: str

    def __post_init__(self) -> None:
        _validate_planner_benchmark(self)

    def to_dict(self) -> dict[str, Any]:
        _validate_planner_benchmark(self)
        return {
            "algorithm": self.algorithm,
            "repeats": self.repeats,
            "median_runtime_ms": round(self.median_runtime_ms, 6),
            "p95_runtime_ms": round(self.p95_runtime_ms, 6),
            "makespan_ms": round(self.makespan_ms, 6),
            "relative_makespan": round(self.relative_makespan, 9),
            "plan_sha256": self.plan_sha256,
        }


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    """One reproducible comparison of the bundled planning baselines."""

    graph_name: str
    node_count: int
    edge_count: int
    graph_sha256: str
    warmups: int
    planners: tuple[PlannerBenchmark, ...]

    def __post_init__(self) -> None:
        if isinstance(self.planners, (str, bytes, bytearray)):
            raise ValueError("planners must be an iterable of PlannerBenchmark records")
        try:
            planners_list: list[Any] = []
            for planner in self.planners:
                if len(planners_list) == MAX_DEVICES:
                    raise ValueError(f"planners exceeds the {MAX_DEVICES}-item limit")
                planners_list.append(planner)
        except TypeError as exc:
            raise ValueError("planners must be an iterable") from exc
        planners = tuple(planners_list)
        object.__setattr__(self, "planners", planners)
        _validate_benchmark_result(self)

    def to_dict(self) -> dict[str, Any]:
        _validate_benchmark_result(self)
        return {
            "schema_version": 1,
            "workload": {
                "graph_name": self.graph_name,
                "node_count": self.node_count,
                "edge_count": self.edge_count,
                "graph_sha256": self.graph_sha256,
            },
            "protocol": {
                "clock": "perf_counter_ns",
                "warmups": self.warmups,
                "runtime_scope": "planner.plan only",
                "quality_metric": "estimated makespan_ms; lower is better",
            },
            "environment": {
                "python": platform.python_version(),
                "implementation": platform.python_implementation(),
                "platform": platform.platform(),
                "executable_bits": 64 if sys.maxsize > 2**32 else 32,
            },
            "planners": [planner.to_dict() for planner in self.planners],
        }


def benchmark_graph(
    graph: GraphSpec,
    *,
    repeats: int = 7,
    warmups: int = 1,
    beam_width: int = 16,
    clock_ns: Callable[[], int] = time.perf_counter_ns,
) -> BenchmarkResult:
    """Compare greedy and beam baselines under a documented timing protocol."""

    for label, value, minimum in (("repeats", repeats, 1), ("warmups", warmups, 0)):
        _bounded_count(value, label, minimum=minimum, maximum=10_000)
    _bounded_count(beam_width, "beam_width", minimum=1, maximum=MAX_BEAM_WIDTH)
    if not isinstance(graph, GraphSpec):
        raise ValueError("graph must be a GraphSpec")
    graph.validate()
    work = len(graph.nodes) * len(graph.devices) * (beam_width + 1) * (repeats + warmups)
    if work > MAX_BENCHMARK_WORK:
        raise ValueError(f"benchmark work exceeds the {MAX_BENCHMARK_WORK}-candidate limit")
    planners = (GreedyPlanner(), BeamPlanner(beam_width))
    raw: list[tuple[str, tuple[float, ...], PlanResult]] = []
    for planner in planners:
        for _ in range(warmups):
            planner.plan(graph)
        durations: list[float] = []
        plans: list[PlanResult] = []
        for _ in range(repeats):
            started = clock_ns()
            plan = planner.plan(graph)
            ended = clock_ns()
            elapsed = (ended - started) / 1_000_000
            if elapsed < 0 or not math.isfinite(elapsed):
                raise ValueError("benchmark clock must be monotonic and finite")
            durations.append(elapsed)
            plans.append(plan)
        digests = {_plan_digest(plan) for plan in plans}
        if len(digests) != 1:
            raise AssertionError(f"planner {planner.name} produced non-deterministic plans")
        raw.append((plans[0].algorithm, tuple(durations), plans[0]))
    best = min(plan.makespan_ms for _, _, plan in raw)
    summaries = tuple(
        PlannerBenchmark(
            algorithm=algorithm,
            repeats=repeats,
            median_runtime_ms=statistics.median(durations),
            p95_runtime_ms=_nearest_rank(durations, 0.95),
            makespan_ms=plan.makespan_ms,
            relative_makespan=plan.makespan_ms / best if best else 1.0,
            plan_sha256=_plan_digest(plan),
        )
        for algorithm, durations, plan in raw
    )
    return BenchmarkResult(
        graph_name=graph.name,
        node_count=len(graph.nodes),
        edge_count=len(graph.edges),
        graph_sha256=_graph_digest(graph),
        warmups=warmups,
        planners=summaries,
    )


def write_benchmark(result: BenchmarkResult, path: str | Path) -> Path:
    """Write a strict, stable-key benchmark JSON document."""

    target = Path(path)
    try:
        document = json.dumps(result.to_dict(), indent=2, sort_keys=True, allow_nan=False) + "\n"
        _atomic_write(target, document)
    except (OSError, TypeError, ValueError) as exc:
        raise OutputError(f"cannot write benchmark result to {target}: {exc}") from exc
    return target


def _plan_digest(plan: PlanResult) -> str:
    payload = json.dumps(
        plan.to_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _graph_digest(graph: GraphSpec) -> str:
    from graph_sail.calibration import graph_to_dict

    payload = json.dumps(
        graph_to_dict(graph), sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _nearest_rank(values: tuple[float, ...], quantile: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(quantile * len(ordered)) - 1)
    return ordered[index]


def _validate_planner_benchmark(value: PlannerBenchmark) -> None:
    _safe_text(value.algorithm, "algorithm")
    _bounded_count(value.repeats, "repeats", minimum=1, maximum=10_000)
    for label, number in (
        ("median_runtime_ms", value.median_runtime_ms),
        ("p95_runtime_ms", value.p95_runtime_ms),
        ("makespan_ms", value.makespan_ms),
        ("relative_makespan", value.relative_makespan),
    ):
        _finite(number, label, minimum=0.0)
    if value.p95_runtime_ms < value.median_runtime_ms:
        raise ValueError("p95_runtime_ms must be >= median_runtime_ms")
    if value.relative_makespan < 1.0 - 1e-9:
        raise ValueError("relative_makespan must be >= 1")
    _digest(value.plan_sha256, "plan_sha256")


def _validate_benchmark_result(value: BenchmarkResult) -> None:
    if not isinstance(value, BenchmarkResult):
        raise ValueError("result must be a BenchmarkResult")
    _safe_text(value.graph_name, "graph_name")
    _bounded_count(value.node_count, "node_count", minimum=1, maximum=MAX_NODES)
    _bounded_count(value.edge_count, "edge_count", minimum=0, maximum=100_000)
    _digest(value.graph_sha256, "graph_sha256")
    _bounded_count(value.warmups, "warmups", minimum=0, maximum=10_000)
    if not isinstance(value.planners, tuple) or not value.planners:
        raise ValueError("planners must be a non-empty tuple")
    if len(value.planners) > MAX_DEVICES:
        raise ValueError(f"planners exceeds the {MAX_DEVICES}-item limit")
    for planner in value.planners:
        if not isinstance(planner, PlannerBenchmark):
            raise ValueError("planners must contain PlannerBenchmark records")
        _validate_planner_benchmark(planner)
    if len({planner.algorithm for planner in value.planners}) != len(value.planners):
        raise ValueError("planner algorithms must be unique")
    if len({planner.repeats for planner in value.planners}) != 1:
        raise ValueError("all planner records must use the same repeat count")
    best = min(planner.makespan_ms for planner in value.planners)
    for planner in value.planners:
        expected = planner.makespan_ms / best if best else 1.0
        if not math.isclose(planner.relative_makespan, expected, rel_tol=1e-9, abs_tol=1e-9):
            raise ValueError("relative_makespan is inconsistent with planner makespans")


def _bounded_count(value: Any, label: str, *, minimum: int, maximum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{label} must be an integer from {minimum} to {maximum}")


def _finite(value: Any, label: str, *, minimum: float) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be numeric")
    try:
        valid = math.isfinite(value)
    except (OverflowError, TypeError):
        valid = False
    if not valid or value < minimum:
        raise ValueError(f"{label} must be finite and >= {minimum:g}")


def _safe_text(value: Any, label: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(f"{label} must contain valid Unicode scalar values") from exc
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"{label} must not contain control characters")


def _digest(value: Any, label: str) -> None:
    if not isinstance(value, str) or len(value) != 64:
        raise ValueError(f"{label} must be a 64-character lowercase hexadecimal digest")
    if any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} must be a 64-character lowercase hexadecimal digest")


def _atomic_write(target: Path, document: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", newline="\n", dir=target.parent, delete=False
        ) as handle:
            handle.write(document)
            handle.flush()
            temporary = Path(handle.name)
        temporary.replace(target)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()
