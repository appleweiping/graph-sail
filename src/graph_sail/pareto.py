"""Bounded multi-objective planning helpers.

Different deployments value latency, compute cost, and memory headroom
differently.  A single weighted score hides that trade-off.  This module runs
the existing bounded planners and returns the non-dominated plans under three
transparent objectives, preserving each planner's decision trace.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from graph_sail.errors import PlanningError
from graph_sail.models import GraphSpec, PlanResult
from graph_sail.planner import BeamPlanner, GreedyPlanner


@dataclass(frozen=True, slots=True)
class PlanCost:
    makespan_ms: float
    compute_ms: float
    transfer_ms: float
    peak_memory_mb: float

    def to_dict(self) -> dict[str, float]:
        return {
            "makespan_ms": round(self.makespan_ms, 6),
            "compute_ms": round(self.compute_ms, 6),
            "transfer_ms": round(self.transfer_ms, 6),
            "peak_memory_mb": round(self.peak_memory_mb, 6),
        }


@dataclass(frozen=True, slots=True)
class ParetoCandidate:
    algorithm: str
    cost: PlanCost
    plan: PlanResult

    def to_dict(self) -> dict[str, Any]:
        return {
            "algorithm": self.algorithm,
            "cost": self.cost.to_dict(),
            "plan": self.plan.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class ParetoReport:
    graph_name: str
    candidates: tuple[ParetoCandidate, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "1.0",
            "kind": "graph-sail-pareto",
            "graph_name": self.graph_name,
            "candidate_count": len(self.candidates),
            "candidates": [candidate.to_dict() for candidate in self.candidates],
        }


def plan_cost(plan: PlanResult) -> PlanCost:
    """Extract stable objective values from one validated plan."""

    if not isinstance(plan, PlanResult):
        raise PlanningError("plan must be a PlanResult")
    compute = sum(item.compute_ms for item in plan.schedule)
    transfer = sum(item.incoming_transfer_ms for item in plan.schedule)
    peak = max(
        (
            sum(item.memory_mb for item in plan.schedule if item.device == device)
            for device in {item.device for item in plan.schedule}
        ),
        default=0.0,
    )
    return PlanCost(plan.makespan_ms, compute, transfer, peak)


def pareto_plans(
    graph: GraphSpec,
    *,
    beam_width: int = 16,
    include_exact: bool = False,
) -> ParetoReport:
    """Return non-dominated greedy/beam (and optionally exact) candidates.

    Exact enumeration is opt-in because its state ceiling is exponential in
    the number of nodes.  If an optional planner cannot produce a feasible plan,
    the error is propagated rather than presenting an incomplete frontier as a
    complete answer.
    """

    if not isinstance(graph, GraphSpec):
        raise PlanningError("graph must be a GraphSpec")
    graph.validate()
    planners: list[tuple[str, Any]] = [
        ("greedy", GreedyPlanner()),
        ("beam", BeamPlanner(beam_width)),
    ]
    if include_exact:
        from graph_sail.exact import ExactPlanner

        planners.append(("exact", ExactPlanner()))
    candidates = [
        ParetoCandidate(name, plan_cost(plan), plan)
        for name, planner in planners
        for plan in [planner.plan(graph)]
    ]
    frontier = tuple(
        candidate
        for candidate in candidates
        if not any(
            _dominates(other.cost, candidate.cost) for other in candidates if other != candidate
        )
    )
    return ParetoReport(
        graph_name=graph.name,
        candidates=tuple(
            sorted(frontier, key=lambda item: (item.cost.makespan_ms, item.algorithm))
        ),
    )


def _dominates(left: PlanCost, right: PlanCost) -> bool:
    dimensions = ("makespan_ms", "compute_ms", "transfer_ms", "peak_memory_mb")
    values_left = tuple(getattr(left, dimension) for dimension in dimensions)
    values_right = tuple(getattr(right, dimension) for dimension in dimensions)
    return all(a <= b for a, b in zip(values_left, values_right, strict=True)) and any(
        a < b for a, b in zip(values_left, values_right, strict=True)
    )


__all__ = ["ParetoCandidate", "ParetoReport", "PlanCost", "pareto_plans", "plan_cost"]
