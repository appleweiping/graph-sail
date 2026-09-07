"""Command-line interface for validation, planning, and the built-in demo."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from graph_sail import __version__
from graph_sail.benchmark import benchmark_graph, write_benchmark
from graph_sail.calibration import calibrate_graph, load_observations, write_calibration_bundle
from graph_sail.demo import demo_graph, demo_payload
from graph_sail.errors import GraphSailError
from graph_sail.exact import ExactPlanner
from graph_sail.graph import topological_order
from graph_sail.io import load_graph
from graph_sail.planner import BeamPlanner, GreedyPlanner
from graph_sail.report import write_report_bundle
from graph_sail.sensitivity import (
    DEFAULT_MAX_FACTOR,
    DEFAULT_MIN_FACTOR,
    DEFAULT_SAMPLES_PER_OCTAVE,
    DEFAULT_TOLERANCE,
    analyze_sensitivity,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="graph-sail",
        description="Plan heterogeneous multimodal execution graphs with an auditable cost model.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate", help="validate a graph JSON document")
    validate.add_argument("graph", type=Path)

    plan = subparsers.add_parser("plan", help="place a graph and write a report bundle")
    plan.add_argument("graph", type=Path)
    plan.add_argument("--output", type=Path, default=Path("graph-sail-output"))
    plan.add_argument("--algorithm", choices=("greedy", "beam", "exact"), default="beam")
    plan.add_argument("--beam-width", type=int, default=16)

    calibrate = subparsers.add_parser(
        "calibrate", help="replace graph latency estimates from measured JSONL observations"
    )
    calibrate.add_argument("graph", type=Path)
    calibrate.add_argument("observations", type=Path)
    calibrate.add_argument("--output", type=Path, default=Path("graph-sail-calibrated"))
    calibrate.add_argument(
        "--ignore-unknown", action="store_true", help="record rather than reject unmatched cells"
    )

    benchmark = subparsers.add_parser(
        "benchmark", help="compare greedy and beam baselines on one graph"
    )
    benchmark.add_argument("graph", type=Path)
    benchmark.add_argument("--output", type=Path, default=Path("benchmark.json"))
    benchmark.add_argument("--repeats", type=int, default=7)
    benchmark.add_argument("--warmups", type=int, default=1)
    benchmark.add_argument("--beam-width", type=int, default=16)

    sensitivity = subparsers.add_parser(
        "sensitivity", help="report how much the plan depends on each estimate"
    )
    sensitivity.add_argument("graph", type=Path)
    sensitivity.add_argument("--output", type=Path, default=Path("sensitivity.json"))
    sensitivity.add_argument("--algorithm", choices=("greedy", "beam", "exact"), default="beam")
    sensitivity.add_argument("--beam-width", type=int, default=16)
    sensitivity.add_argument(
        "--max-factor",
        type=float,
        default=DEFAULT_MAX_FACTOR,
        help="largest multiplier sampled when an estimate is made worse",
    )
    sensitivity.add_argument(
        "--min-factor",
        type=float,
        default=DEFAULT_MIN_FACTOR,
        help="smallest multiplier sampled when an estimate is made better",
    )
    sensitivity.add_argument(
        "--tolerance",
        type=float,
        default=DEFAULT_TOLERANCE,
        help="relative width at which an observed flip bracket stops refining",
    )
    sensitivity.add_argument(
        "--samples-per-octave",
        type=int,
        default=DEFAULT_SAMPLES_PER_OCTAVE,
        help="geometric placement probes per factor-of-two interval",
    )

    demo = subparsers.add_parser("demo", help="run the built-in multimodal graph")
    demo.add_argument("--output", type=Path, default=Path("demo-output"))
    demo.add_argument("--write-input", action="store_true", help="also write the demo graph JSON")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "validate":
            graph = load_graph(args.graph)
            order = topological_order(graph)
            print(
                f"valid: {graph.name} ({len(graph.nodes)} nodes, {len(graph.edges)} edges, "
                f"order: {' -> '.join(order)})"
            )
            return 0
        if args.command == "plan":
            graph = load_graph(args.graph)
            planner = _planner(args.algorithm, args.beam_width)
            plan = planner.plan(graph)
            paths = write_report_bundle(graph, plan, args.output)
            _print_summary(plan.makespan_ms, plan.placements, paths)
            return 0
        if args.command == "calibrate":
            graph = load_graph(args.graph)
            observations = load_observations(args.observations)
            result = calibrate_graph(graph, observations, strict=not args.ignore_unknown)
            paths = write_calibration_bundle(result, args.output)
            print(
                f"calibrated {len(result.cells)} latency cells from "
                f"{sum(cell.samples for cell in result.cells)} observations "
                f"({len(result.ignored_cells)} ignored)"
            )
            for label, path in paths.items():
                print(f"  {label:<24} {path}")
            return 0
        if args.command == "benchmark":
            graph = load_graph(args.graph)
            benchmark_result = benchmark_graph(
                graph,
                repeats=args.repeats,
                warmups=args.warmups,
                beam_width=args.beam_width,
            )
            output = write_benchmark(benchmark_result, args.output)
            summaries = benchmark_result.planners
            print(f"benchmarked {len(graph.nodes)} nodes with {len(summaries)} baselines")
            for summary in summaries:
                print(
                    f"  {summary.algorithm:<24} makespan={summary.makespan_ms:.3f} ms "
                    f"median-runtime={summary.median_runtime_ms:.3f} ms"
                )
            print(f"  {'result':<24} {output}")
            return 0
        if args.command == "sensitivity":
            graph = load_graph(args.graph)
            planner = _planner(args.algorithm, args.beam_width)
            report = analyze_sensitivity(
                graph,
                planner.plan(graph),
                planner,
                max_factor=args.max_factor,
                min_factor=args.min_factor,
                tolerance=args.tolerance,
                samples_per_octave=args.samples_per_octave,
            )
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(
                json.dumps(report.as_dict(), indent=2, allow_nan=False) + "\n",
                encoding="utf-8",
                newline="\n",
            )
            print(
                f"probed {len(report.makespan)} estimates against "
                f"{report.baseline_makespan_ms:.3f} ms"
            )
            weakest = report.weakest
            if weakest is not None and weakest.margin is not None:
                print(
                    f"  weakest observed change: {weakest.node} on {weakest.device} near "
                    f"{weakest.margin:.0%}"
                )
            else:
                print(
                    "  no estimate changed the placement at the recorded probe factors; "
                    "changes between probes or outside the range remain possible"
                )
            if report.load_bearing:
                print(f"  worth re-measuring: {', '.join(report.load_bearing)}")
            print(f"  {'result':<24} {args.output}")
            return 0
        if args.command == "demo":
            graph = demo_graph()
            plan = BeamPlanner().plan(graph)
            paths = write_report_bundle(graph, plan, args.output)
            if args.write_input:
                input_path = args.output / "graph.json"
                input_path.write_text(
                    json.dumps(demo_payload(), indent=2, allow_nan=False) + "\n",
                    encoding="utf-8",
                    newline="\n",
                )
                paths["input"] = input_path
            _print_summary(plan.makespan_ms, plan.placements, paths)
            return 0
    except (GraphSailError, OSError, ValueError) as exc:
        print(f"graph-sail: error: {exc}", file=sys.stderr)
        return 2
    return 2


def _planner(algorithm: str, beam_width: int) -> GreedyPlanner | BeamPlanner | ExactPlanner:
    if algorithm == "greedy":
        return GreedyPlanner()
    if algorithm == "exact":
        return ExactPlanner()
    return BeamPlanner(beam_width=beam_width)


def _print_summary(makespan_ms: float, placements: dict[str, str], paths: dict[str, Path]) -> None:
    print(f"planned {len(placements)} nodes in {makespan_ms:.3f} ms")
    for node, device in placements.items():
        print(f"  {node:<24} -> {device}")
    for label, path in paths.items():
        print(f"  {label:<24} {path}")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
