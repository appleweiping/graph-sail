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
from graph_sail.graph import topological_order
from graph_sail.io import load_graph
from graph_sail.planner import BeamPlanner, GreedyPlanner
from graph_sail.report import write_report_bundle


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
    plan.add_argument("--algorithm", choices=("greedy", "beam"), default="beam")
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
            result = benchmark_graph(
                graph,
                repeats=args.repeats,
                warmups=args.warmups,
                beam_width=args.beam_width,
            )
            output = write_benchmark(result, args.output)
            print(f"benchmarked {len(graph.nodes)} nodes with {len(result.planners)} baselines")
            for planner in result.planners:
                print(
                    f"  {planner.algorithm:<24} makespan={planner.makespan_ms:.3f} ms "
                    f"median-runtime={planner.median_runtime_ms:.3f} ms"
                )
            print(f"  {'result':<24} {output}")
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


def _planner(algorithm: str, beam_width: int) -> GreedyPlanner | BeamPlanner:
    if algorithm == "greedy":
        return GreedyPlanner()
    return BeamPlanner(beam_width=beam_width)


def _print_summary(makespan_ms: float, placements: dict[str, str], paths: dict[str, Path]) -> None:
    print(f"planned {len(placements)} nodes in {makespan_ms:.3f} ms")
    for node, device in placements.items():
        print(f"  {node:<24} -> {device}")
    for label, path in paths.items():
        print(f"  {label:<24} {path}")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
