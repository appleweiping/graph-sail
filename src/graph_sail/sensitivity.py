"""How much a plan depends on the estimates it was given.

Graph Sail plans from numbers somebody typed. The README says so repeatedly and
the decision trace exists to expose that rather than hide it, but the plan
itself arrived as a single placement with no indication of how much of it
rested on any one figure. An estimate that could be wrong by a factor of two
without changing anything deserves less worry than one that flips the placement
at fifteen percent, and nothing distinguished them.

Two questions are worth asking separately, because they have different answers
and different costs.

The first is which estimates move the makespan at all. Only work on the
critical chain does; everything else is absorbed by slack. It is tempting to
answer that by subtracting finish times, and wrong: the gap between a node's
finish and the end of the plan is not its slack, because a node feeding the
last one has successors waiting on it. The scheduler is asked instead.

The second is how far an estimate can move before the *placement* changes,
which is the question a reader actually has. Placement is a discrete decision,
so it does not drift: it holds, and then at some multiplier it does not. That
is answered by re-planning at perturbed values and bisecting for the point
where the answer flips.

The second question is the expensive one, and it is bounded rather than
approximated away. The search runs to a stated tolerance over a stated range,
and reports when it found no flip inside that range rather than implying the
placement is unconditionally stable.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Protocol

from graph_sail.analysis import critical_chain
from graph_sail.errors import GraphSailError
from graph_sail.models import GraphSpec, NodeSpec, PlanResult

#: Largest multiplier a search will try. An estimate that survives being
#: quadrupled is not the reason a plan is fragile.
DEFAULT_MAX_FACTOR = 4.0

#: Smallest multiplier a search will try. Below a quarter of the estimate the
#: question stops being "was the estimate off" and becomes "was it the right
#: quantity".
DEFAULT_MIN_FACTOR = 0.25

#: Relative width at which a bisection stops. Reporting a flip point to better
#: than a percent would imply a precision the underlying estimate never had.
DEFAULT_TOLERANCE = 0.01

#: Bisection steps allowed per direction. The range spans a factor of sixteen,
#: so this reaches the tolerance with room to spare and cannot run away.
MAX_BISECTION_STEPS = 32

#: Relative increase used to measure a makespan response. Small enough that the
#: schedule keeps its shape, large enough to stay clear of floating-point noise
#: in the millisecond arithmetic.
DEFAULT_PROBE = 0.05


class Planner(Protocol):
    """The part of a planner this module needs."""

    def plan(self, graph: GraphSpec) -> PlanResult: ...


@dataclass(frozen=True, slots=True)
class MakespanSensitivity:
    """How the makespan responds to one node's compute estimate.

    `response` is the fraction of an added millisecond that reaches the
    makespan. One means the node is on the critical chain and every extra
    millisecond is an extra millisecond of plan; zero means the schedule
    absorbs it entirely.

    It is measured by perturbing the estimate and re-planning rather than
    derived from finish times. The gap between a node's finish and the makespan
    is not its slack: a node that feeds the last one has successors waiting on
    it, and subtracting finish times ignores them. Asking the scheduler is both
    shorter and right.
    """

    node: str
    device: str
    estimate_ms: float
    baseline_makespan_ms: float
    probed_makespan_ms: float
    response: float
    on_critical_chain: bool
    placement_changed: bool

    @property
    def matters(self) -> bool:
        """Whether a slightly worse estimate changes the plan at all."""

        return self.placement_changed or self.response > 1e-9

    def as_dict(self) -> dict[str, Any]:
        return {
            "node": self.node,
            "device": self.device,
            "estimate_ms": self.estimate_ms,
            "baseline_makespan_ms": self.baseline_makespan_ms,
            "probed_makespan_ms": self.probed_makespan_ms,
            "response": self.response,
            "on_critical_chain": self.on_critical_chain,
            "placement_changed": self.placement_changed,
            "matters": self.matters,
        }


@dataclass(frozen=True, slots=True)
class PlacementStability:
    """The multipliers at which one estimate stops supporting the placement."""

    node: str
    device: str
    estimate_ms: float
    slower_factor: float | None
    faster_factor: float | None
    searched_range: tuple[float, float]

    @property
    def stable(self) -> bool:
        """Whether the placement held across the whole searched range."""

        return self.slower_factor is None and self.faster_factor is None

    @property
    def margin(self) -> float | None:
        """Closest relative change that flips the placement, if any does.

        A margin of 0.15 means a fifteen percent error in this estimate is
        enough to make a different placement preferable. None means no flip was
        found inside the searched range, which is not the same as none existing.
        """

        candidates = [
            abs(factor - 1.0)
            for factor in (self.slower_factor, self.faster_factor)
            if factor is not None
        ]
        return min(candidates) if candidates else None

    def as_dict(self) -> dict[str, Any]:
        return {
            "node": self.node,
            "device": self.device,
            "estimate_ms": self.estimate_ms,
            "slower_factor": self.slower_factor,
            "faster_factor": self.faster_factor,
            "searched_range": list(self.searched_range),
            "stable": self.stable,
            "margin": self.margin,
        }


@dataclass(frozen=True, slots=True)
class SensitivityReport:
    """What the plan rests on, ordered by how much it rests on each thing."""

    graph_name: str
    algorithm: str
    baseline_makespan_ms: float
    makespan: tuple[MakespanSensitivity, ...]
    stability: tuple[PlacementStability, ...]

    @property
    def load_bearing(self) -> tuple[str, ...]:
        """Estimates that are both influential and fragile.

        Influential alone is too many: on a mostly serial pipeline nearly every
        node reaches the makespan. Fragile alone is too many the other way: an
        estimate that flips the placement only after tripling is not a worry.
        The pair is the short list worth re-measuring.
        """

        flippable = {item.node for item in self.stability if item.margin is not None}
        return tuple(sorted({item.node for item in self.makespan if item.matters} & flippable))

    @property
    def weakest(self) -> PlacementStability | None:
        """The estimate whose smallest error changes the placement."""

        ranked = [item for item in self.stability if item.margin is not None]
        if not ranked:
            return None
        return min(ranked, key=lambda item: (item.margin or 0.0, item.node))

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "graph_name": self.graph_name,
            "algorithm": self.algorithm,
            "baseline_makespan_ms": self.baseline_makespan_ms,
            "load_bearing": list(self.load_bearing),
            "weakest": self.weakest.as_dict() if self.weakest else None,
            "makespan": [item.as_dict() for item in self.makespan],
            "stability": [item.as_dict() for item in self.stability],
        }


def _scaled_node(node: NodeSpec, device: str, factor: float) -> NodeSpec:
    """Return the node with one device estimate multiplied.

    Only the estimate for the device in question moves. Scaling every device
    together would ask a different question: whether the node is slow, rather
    than whether this estimate for this device is right.
    """

    latency = dict(node.latency_ms)
    if device not in latency:
        raise GraphSailError(f"node {node.id!r} has no estimate for device {device!r}")
    latency[device] = latency[device] * factor
    return replace(node, latency_ms=latency)


def perturb(graph: GraphSpec, node_id: str, device: str, factor: float) -> GraphSpec:
    """Return the graph with one node's estimate for one device multiplied."""

    if factor <= 0.0:
        raise GraphSailError("perturbation factor must be positive")
    found = False
    nodes = []
    for node in graph.nodes:
        if node.id == node_id:
            nodes.append(_scaled_node(node, device, factor))
            found = True
        else:
            nodes.append(node)
    if not found:
        raise GraphSailError(f"unknown node {node_id!r}")
    return replace(graph, nodes=tuple(nodes))


def _placement(plan: PlanResult) -> dict[str, str]:
    return {item.node: item.device for item in plan.schedule}


def _makespan(plan: PlanResult) -> float:
    return max((item.finish_ms for item in plan.schedule), default=0.0)


def _plans_the_same(planner: Planner, graph: GraphSpec, baseline: dict[str, str]) -> bool:
    """Whether the perturbed graph still yields the baseline placement.

    A perturbation that makes the graph unplaceable counts as a change, because
    a placement that cannot be produced is certainly not the baseline one.
    """

    try:
        return _placement(planner.plan(graph)) == baseline
    except GraphSailError:
        return False


def _flip_factor(
    planner: Planner,
    graph: GraphSpec,
    node_id: str,
    device: str,
    baseline: dict[str, str],
    limit: float,
    tolerance: float,
) -> float | None:
    """Bisect for the multiplier at which the placement stops holding.

    Returns None when the placement survived all the way to `limit`, which says
    the flip is outside the searched range rather than that none exists.
    """

    if _plans_the_same(planner, perturb(graph, node_id, device, limit), baseline):
        return None
    low, high = 1.0, limit
    for _ in range(MAX_BISECTION_STEPS):
        if abs(high - low) <= tolerance * max(abs(low), 1e-9):
            break
        middle = (low + high) / 2.0
        if _plans_the_same(planner, perturb(graph, node_id, device, middle), baseline):
            low = middle
        else:
            high = middle
    return high


def makespan_sensitivity(
    graph: GraphSpec,
    plan: PlanResult,
    planner: Planner,
    *,
    probe: float = DEFAULT_PROBE,
) -> tuple[MakespanSensitivity, ...]:
    """Measure how much of an added millisecond each estimate passes on.

    The probe is a small relative increase, applied to one estimate at a time
    and answered by the scheduler. Small enough that the schedule keeps its
    shape, large enough to stay well clear of floating-point noise.
    """

    if not 0.0 < probe < 1.0:
        raise GraphSailError("probe must lie between 0 and 1")
    baseline_makespan = _makespan(plan)
    baseline_placement = _placement(plan)
    chain = set(critical_chain(graph, plan))
    results = []
    for item in sorted(plan.schedule, key=lambda entry: entry.node):
        added = item.compute_ms * probe
        probed_makespan = baseline_makespan
        changed = False
        response = 0.0
        if added > 0.0:
            try:
                probed = planner.plan(perturb(graph, item.node, item.device, 1.0 + probe))
            except GraphSailError:
                changed = True
            else:
                changed = _placement(probed) != baseline_placement
                probed_makespan = _makespan(probed)
                if not changed:
                    response = (probed_makespan - baseline_makespan) / added
        results.append(
            MakespanSensitivity(
                node=item.node,
                device=item.device,
                estimate_ms=item.compute_ms,
                baseline_makespan_ms=baseline_makespan,
                probed_makespan_ms=probed_makespan,
                response=response,
                on_critical_chain=item.node in chain,
                placement_changed=changed,
            )
        )
    return tuple(results)


def placement_stability(
    graph: GraphSpec,
    plan: PlanResult,
    planner: Planner,
    *,
    max_factor: float = DEFAULT_MAX_FACTOR,
    min_factor: float = DEFAULT_MIN_FACTOR,
    tolerance: float = DEFAULT_TOLERANCE,
) -> tuple[PlacementStability, ...]:
    """Find, per estimate, how far it can move before the placement changes.

    Both directions are searched, because a node being slower than estimated
    and being faster than estimated push a placement different ways and either
    may be the fragile one.
    """

    if not 0.0 < min_factor < 1.0 < max_factor:
        raise GraphSailError("factors must satisfy 0 < min_factor < 1 < max_factor")
    if tolerance <= 0.0 or tolerance >= 1.0:
        raise GraphSailError("tolerance must lie between 0 and 1")
    baseline = _placement(plan)
    results = []
    for item in sorted(plan.schedule, key=lambda entry: entry.node):
        results.append(
            PlacementStability(
                node=item.node,
                device=item.device,
                estimate_ms=item.compute_ms,
                slower_factor=_flip_factor(
                    planner, graph, item.node, item.device, baseline, max_factor, tolerance
                ),
                faster_factor=_flip_factor(
                    planner, graph, item.node, item.device, baseline, min_factor, tolerance
                ),
                searched_range=(min_factor, max_factor),
            )
        )
    return tuple(results)


def analyze_sensitivity(
    graph: GraphSpec,
    plan: PlanResult,
    planner: Planner,
    *,
    max_factor: float = DEFAULT_MAX_FACTOR,
    min_factor: float = DEFAULT_MIN_FACTOR,
    tolerance: float = DEFAULT_TOLERANCE,
    probe: float = DEFAULT_PROBE,
) -> SensitivityReport:
    """Answer both questions about one plan under one planner.

    The planner is passed in rather than chosen here, because stability is a
    property of the plan *and* the algorithm that produced it. A beam search
    and a greedy pass can disagree about how fragile the same graph is, and
    reporting one while the reader used the other would be misleading.
    """

    return SensitivityReport(
        graph_name=plan.graph_name,
        algorithm=plan.algorithm,
        baseline_makespan_ms=_makespan(plan),
        makespan=makespan_sensitivity(graph, plan, planner, probe=probe),
        stability=placement_stability(
            graph,
            plan,
            planner,
            max_factor=max_factor,
            min_factor=min_factor,
            tolerance=tolerance,
        ),
    )
