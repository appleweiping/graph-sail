"""How much a plan depends on the estimates it was given.

Graph Sail plans from numbers somebody typed. The README says so repeatedly and
the decision trace exists to expose that rather than hide it, but the plan
itself arrived as a single placement with no indication of how much of it
rested on any one figure. An estimate with no change at broad sampled factors
deserves less worry than one with an observed placement change near fifteen
percent, and nothing distinguished them.

Two questions are worth asking separately, because they have different answers
and different costs.

The first is which estimates move the makespan at all. Only work on the
critical chain does; everything else is absorbed by slack. It is tempting to
answer that by subtracting finish times, and wrong: the gap between a node's
finish and the end of the plan is not its slack, because a node feeding the
last one has successors waiting on it. The scheduler is asked instead.

The second is how far an estimate can move before the *placement* changes,
which is the question a reader actually has. A planner's discrete decisions
need not vary monotonically with an estimate: a placement can change and later
return. The search therefore probes an explicit geometric grid out from one.
When a probe first changes the placement, the interval from the preceding
unchanged probe is bisected to refine that observed boundary.

The second question is the expensive one, so both grid density and total work
are bounded. A result records every factor actually probed. ``stable`` means
only that those recorded probes did not change the placement; a narrow
non-monotonic change between probes can still be missed.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import Any, Protocol, TypeVar, cast

from graph_sail.analysis import critical_chain
from graph_sail.errors import GraphSailError
from graph_sail.limits import MAX_NODES, MAX_TEXT_LENGTH
from graph_sail.models import GraphSpec, NodeSpec, PlanResult

_T = TypeVar("_T")

#: Largest multiplier included in the default sampled range.
DEFAULT_MAX_FACTOR = 4.0

#: Smallest multiplier included in the default sampled range. Below a quarter
#: of the estimate the question stops being "was the estimate off" and becomes
#: "was it the right quantity".
DEFAULT_MIN_FACTOR = 0.25

#: Relative width at which a bisection stops. Reporting a flip point to better
#: than a percent would imply a precision the underlying estimate never had.
DEFAULT_TOLERANCE = 0.01

#: Bisection steps allowed per direction. The range spans a factor of sixteen,
#: so this reaches the tolerance with room to spare and cannot run away.
MAX_BISECTION_STEPS = 32

#: Default density of the outward geometric probe grid. One sample per octave
#: means the default range explicitly probes 0.5, 0.25, 2, and 4.
DEFAULT_SAMPLES_PER_OCTAVE = 1

#: User-selectable probe density and range are bounded independently, then a
#: whole-operation ceiling prevents their product with graph size from running
#: away. The ceiling includes the unperturbed reproduction, makespan probes,
#: coarse placement probes, and the conservative maximum number of bisections.
MAX_SAMPLES_PER_OCTAVE = 16
MAX_COARSE_PROBES_PER_DIRECTION = 64
MAX_STABILITY_PROBES_PER_RECORD = 2 * (MAX_COARSE_PROBES_PER_DIRECTION + MAX_BISECTION_STEPS)
MAX_STABILITY_PLANNER_CALLS = 1_000_000

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

    def __post_init__(self) -> None:
        object.__setattr__(self, "node", _safe_text(self.node, "sensitivity node"))
        object.__setattr__(self, "device", _safe_text(self.device, "sensitivity device"))
        object.__setattr__(
            self, "estimate_ms", _finite(self.estimate_ms, "sensitivity estimate_ms", positive=True)
        )
        object.__setattr__(
            self,
            "baseline_makespan_ms",
            _finite(self.baseline_makespan_ms, "sensitivity baseline_makespan_ms"),
        )
        object.__setattr__(
            self,
            "probed_makespan_ms",
            _finite(self.probed_makespan_ms, "sensitivity probed_makespan_ms"),
        )
        response = _finite(self.response, "sensitivity response", allow_negative=True)
        if not -1e-9 <= response <= 1.0 + 1e-9:
            raise GraphSailError("sensitivity response must be between zero and one")
        object.__setattr__(self, "response", response)
        _boolean(self.on_critical_chain, "sensitivity on_critical_chain")
        _boolean(self.placement_changed, "sensitivity placement_changed")

    @property
    def matters(self) -> bool:
        """Whether a slightly worse estimate changes the plan at all."""

        return _matters_unchecked(_snapshot_makespan(self))

    def as_dict(self) -> dict[str, Any]:
        value = _snapshot_makespan(self)
        return {
            "node": value.node,
            "device": value.device,
            "estimate_ms": value.estimate_ms,
            "baseline_makespan_ms": value.baseline_makespan_ms,
            "probed_makespan_ms": value.probed_makespan_ms,
            "response": value.response,
            "on_critical_chain": value.on_critical_chain,
            "placement_changed": value.placement_changed,
            "matters": _matters_unchecked(value),
        }


@dataclass(frozen=True, slots=True)
class PlacementStability:
    """Observed placement changes and the factors that were actually probed."""

    node: str
    device: str
    estimate_ms: float
    slower_factor: float | None
    faster_factor: float | None
    searched_range: tuple[float, float]
    samples_per_octave: int
    probed_factors: tuple[float, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "node", _safe_text(self.node, "stability node"))
        object.__setattr__(self, "device", _safe_text(self.device, "stability device"))
        object.__setattr__(
            self, "estimate_ms", _finite(self.estimate_ms, "stability estimate_ms", positive=True)
        )
        raw_range: tuple[int | float, ...] = _bounded_records(
            self.searched_range,
            "stability searched_range",
            (int, float),
            2,
        )
        if len(raw_range) != 2:
            raise GraphSailError("stability searched_range must contain two values")
        minimum = _finite(raw_range[0], "stability searched_range minimum", positive=True)
        maximum = _finite(raw_range[1], "stability searched_range maximum", positive=True)
        if not minimum < 1.0 < maximum:
            raise GraphSailError("stability searched_range must satisfy minimum < 1 < maximum")
        object.__setattr__(self, "searched_range", (minimum, maximum))

        samples_per_octave = _sample_density(self.samples_per_octave)
        raw_probes: tuple[int | float, ...] = _bounded_records(
            self.probed_factors,
            "stability probed_factors",
            (int, float),
            MAX_STABILITY_PROBES_PER_RECORD,
        )
        probes = tuple(
            _finite(factor, f"stability probed_factors[{index}]", positive=True)
            for index, factor in enumerate(raw_probes)
        )
        if not probes:
            raise GraphSailError("stability probed_factors must not be empty")
        if len(probes) != len(set(probes)):
            raise GraphSailError("stability probed_factors must be unique")
        if any(not minimum <= factor <= maximum or factor == 1.0 for factor in probes):
            raise GraphSailError(
                "stability probed_factors must lie inside searched_range and exclude 1"
            )
        if not any(factor < 1.0 for factor in probes) or not any(factor > 1.0 for factor in probes):
            raise GraphSailError("stability probed_factors must cover both sides of 1")
        object.__setattr__(self, "samples_per_octave", samples_per_octave)
        object.__setattr__(self, "probed_factors", probes)

        slower = _optional_factor(self.slower_factor, "stability slower_factor")
        faster = _optional_factor(self.faster_factor, "stability faster_factor")
        if slower is not None and not 1.0 < slower <= maximum:
            raise GraphSailError("stability slower_factor must lie within (1, maximum]")
        if faster is not None and not minimum <= faster < 1.0:
            raise GraphSailError("stability faster_factor must lie within [minimum, 1)")
        if slower is not None and slower not in probes:
            raise GraphSailError("stability slower_factor must identify a recorded probe")
        if faster is not None and faster not in probes:
            raise GraphSailError("stability faster_factor must identify a recorded probe")
        if slower is None and maximum not in probes:
            raise GraphSailError(
                "stability without a slower flip must record the maximum probe factor"
            )
        if faster is None and minimum not in probes:
            raise GraphSailError(
                "stability without a faster flip must record the minimum probe factor"
            )
        object.__setattr__(self, "slower_factor", slower)
        object.__setattr__(self, "faster_factor", faster)

    @property
    def stable(self) -> bool:
        """Whether no placement change was observed at the recorded probes."""

        return _stable_unchecked(_snapshot_stability(self))

    @property
    def margin(self) -> float | None:
        """Closest refined placement change observed from the probe grid.

        A margin of 0.15 means a fifteen percent error in this estimate is
        enough to produce a different placement at the refined probe. None means
        no change was observed at the recorded probes; it does not rule out a
        narrow change between them or a change outside the configured range.
        """

        return _margin_unchecked(_snapshot_stability(self))

    def as_dict(self) -> dict[str, Any]:
        value = _snapshot_stability(self)
        return {
            "node": value.node,
            "device": value.device,
            "estimate_ms": value.estimate_ms,
            "slower_factor": value.slower_factor,
            "faster_factor": value.faster_factor,
            "searched_range": list(value.searched_range),
            "samples_per_octave": value.samples_per_octave,
            "probed_factors": list(value.probed_factors),
            "stable": _stable_unchecked(value),
            "stability_scope": "recorded_probe_factors_only",
            "margin": _margin_unchecked(value),
        }


@dataclass(frozen=True, slots=True)
class SensitivityReport:
    """What the plan rests on, ordered by how much it rests on each thing."""

    graph_name: str
    algorithm: str
    baseline_makespan_ms: float
    makespan: tuple[MakespanSensitivity, ...]
    stability: tuple[PlacementStability, ...]
    samples_per_octave: int = DEFAULT_SAMPLES_PER_OCTAVE

    def __post_init__(self) -> None:
        object.__setattr__(self, "graph_name", _safe_text(self.graph_name, "report graph_name"))
        object.__setattr__(self, "algorithm", _safe_text(self.algorithm, "report algorithm"))
        object.__setattr__(
            self,
            "baseline_makespan_ms",
            _finite(self.baseline_makespan_ms, "report baseline_makespan_ms"),
        )
        object.__setattr__(self, "samples_per_octave", _sample_density(self.samples_per_octave))
        raw_makespan = _bounded_records(
            self.makespan,
            "report makespan",
            MakespanSensitivity,
            MAX_NODES,
        )
        raw_stability = _bounded_records(
            self.stability,
            "report stability",
            PlacementStability,
            MAX_NODES,
        )
        makespan = tuple(_snapshot_makespan(item) for item in raw_makespan)
        stability = tuple(_snapshot_stability(item) for item in raw_stability)
        makespan_keys = tuple((item.node, item.device) for item in makespan)
        stability_keys = tuple((item.node, item.device) for item in stability)
        if makespan_keys != tuple(sorted(makespan_keys)) or len(makespan_keys) != len(
            set(makespan_keys)
        ):
            raise GraphSailError("report makespan entries must be unique and sorted")
        if stability_keys != tuple(sorted(stability_keys)) or len(stability_keys) != len(
            set(stability_keys)
        ):
            raise GraphSailError("report stability entries must be unique and sorted")
        if makespan_keys != stability_keys:
            raise GraphSailError("report makespan and stability entries must name the same cells")
        if any(item.samples_per_octave != self.samples_per_octave for item in stability):
            raise GraphSailError("report stability entries must use the report samples_per_octave")
        for sensitivity, stable in zip(makespan, stability, strict=True):
            if not math.isclose(
                sensitivity.baseline_makespan_ms,
                self.baseline_makespan_ms,
                rel_tol=1e-12,
                abs_tol=1e-9,
            ):
                raise GraphSailError(
                    f"sensitivity baseline for node {sensitivity.node!r} "
                    "is inconsistent with report"
                )
            if not math.isclose(
                sensitivity.estimate_ms, stable.estimate_ms, rel_tol=1e-12, abs_tol=1e-9
            ):
                raise GraphSailError(
                    f"sensitivity estimates for node {sensitivity.node!r} are inconsistent"
                )
        object.__setattr__(self, "makespan", makespan)
        object.__setattr__(self, "stability", stability)

    @property
    def load_bearing(self) -> tuple[str, ...]:
        """Estimates that influence makespan and have an observed placement change.

        This is a descriptive intersection of recorded observations, not a
        user-specific risk threshold. Callers decide whether an observed margin
        is operationally important.
        """

        return _load_bearing_unchecked(_snapshot_report(self))

    @property
    def weakest(self) -> PlacementStability | None:
        """The estimate with the closest observed placement-changing probe."""

        return _weakest_unchecked(_snapshot_report(self))

    def as_dict(self) -> dict[str, Any]:
        report = _snapshot_report(self)
        weakest = _weakest_unchecked(report)
        return {
            "schema_version": 1,
            "graph_name": report.graph_name,
            "algorithm": report.algorithm,
            "baseline_makespan_ms": report.baseline_makespan_ms,
            "stability_sampling": {
                "samples_per_octave": report.samples_per_octave,
                "scope": "recorded_probe_factors_only",
                "caveat": "narrow non-monotonic changes between probes can be missed",
            },
            "load_bearing": list(_load_bearing_unchecked(report)),
            "weakest": weakest.as_dict() if weakest else None,
            "makespan": [item.as_dict() for item in report.makespan],
            "stability": [item.as_dict() for item in report.stability],
        }


def _safe_text(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise GraphSailError(f"{label} must be a string")
    text = value.strip()
    if not text:
        raise GraphSailError(f"{label} must not be empty")
    if len(text) > MAX_TEXT_LENGTH:
        raise GraphSailError(f"{label} exceeds the {MAX_TEXT_LENGTH}-character limit")
    try:
        text.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise GraphSailError(f"{label} must contain valid Unicode scalar values") from exc
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise GraphSailError(f"{label} must not contain control characters")
    return text


def _finite(
    value: Any,
    label: str,
    *,
    positive: bool = False,
    allow_negative: bool = False,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise GraphSailError(f"{label} must be a number")
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise GraphSailError(f"{label} must be finite") from exc
    if not math.isfinite(number):
        raise GraphSailError(f"{label} must be finite")
    if positive and number <= 0:
        raise GraphSailError(f"{label} must be greater than zero")
    if not positive and not allow_negative and number < 0:
        raise GraphSailError(f"{label} must be zero or greater")
    return number


def _boolean(value: Any, label: str) -> None:
    if not isinstance(value, bool):
        raise GraphSailError(f"{label} must be a boolean")


def _optional_factor(value: Any, label: str) -> float | None:
    if value is None:
        return None
    return _finite(value, label, positive=True)


def _sample_density(value: Any) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 1 <= value <= MAX_SAMPLES_PER_OCTAVE
    ):
        raise GraphSailError(
            f"samples_per_octave must be an integer from 1 to {MAX_SAMPLES_PER_OCTAVE}"
        )
    return value


def _bounded_records(
    value: Any,
    label: str,
    item_type: type[_T] | tuple[type[Any], ...],
    maximum: int,
) -> tuple[_T, ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Iterable):
        raise GraphSailError(f"{label} must be an iterable")
    result: list[_T] = []
    for item in value:
        if len(result) == maximum:
            raise GraphSailError(f"{label} exceeds the {maximum}-item limit")
        if not isinstance(item, item_type):
            if isinstance(item_type, tuple):
                expected = " or ".join(kind.__name__ for kind in item_type)
            else:
                expected = item_type.__name__
            raise GraphSailError(f"{label} entries must be {expected} instances")
        result.append(item)
    return tuple(result)


def _snapshot_makespan(value: MakespanSensitivity) -> MakespanSensitivity:
    return MakespanSensitivity(
        node=value.node,
        device=value.device,
        estimate_ms=value.estimate_ms,
        baseline_makespan_ms=value.baseline_makespan_ms,
        probed_makespan_ms=value.probed_makespan_ms,
        response=value.response,
        on_critical_chain=value.on_critical_chain,
        placement_changed=value.placement_changed,
    )


def _snapshot_stability(value: PlacementStability) -> PlacementStability:
    return PlacementStability(
        node=value.node,
        device=value.device,
        estimate_ms=value.estimate_ms,
        slower_factor=value.slower_factor,
        faster_factor=value.faster_factor,
        searched_range=value.searched_range,
        samples_per_octave=value.samples_per_octave,
        probed_factors=value.probed_factors,
    )


def _snapshot_report(value: SensitivityReport) -> SensitivityReport:
    return SensitivityReport(
        graph_name=value.graph_name,
        algorithm=value.algorithm,
        baseline_makespan_ms=value.baseline_makespan_ms,
        makespan=value.makespan,
        stability=value.stability,
        samples_per_octave=value.samples_per_octave,
    )


def _matters_unchecked(value: MakespanSensitivity) -> bool:
    return value.placement_changed or value.response > 1e-9


def _stable_unchecked(value: PlacementStability) -> bool:
    return value.slower_factor is None and value.faster_factor is None


def _margin_unchecked(value: PlacementStability) -> float | None:
    candidates = [
        abs(factor - 1.0)
        for factor in (value.slower_factor, value.faster_factor)
        if factor is not None
    ]
    return min(candidates) if candidates else None


def _load_bearing_unchecked(value: SensitivityReport) -> tuple[str, ...]:
    flippable = {item.node for item in value.stability if _margin_unchecked(item) is not None}
    influential = {item.node for item in value.makespan if _matters_unchecked(item)}
    return tuple(sorted(influential & flippable))


def _weakest_unchecked(value: SensitivityReport) -> PlacementStability | None:
    ranked = [item for item in value.stability if _margin_unchecked(item) is not None]
    if not ranked:
        return None
    return min(ranked, key=lambda item: (_margin_unchecked(item) or 0.0, item.node))


def _validated_graph(value: Any) -> GraphSpec:
    if not isinstance(value, GraphSpec):
        raise GraphSailError("graph must be a GraphSpec")
    value.validate()
    return value


def _validated_plan_for_graph(graph: GraphSpec, value: Any) -> PlanResult:
    if not isinstance(value, PlanResult):
        raise GraphSailError("plan must be a PlanResult")
    plan = replace(value)
    if plan.graph_name != graph.name:
        raise GraphSailError("plan graph_name does not match the graph")

    graph_nodes = graph.node_map
    graph_devices = graph.device_map
    if {item.node for item in plan.schedule} != set(graph_nodes):
        raise GraphSailError("plan schedule must name every graph node exactly once")
    for item in plan.schedule:
        node = graph_nodes[item.node]
        device = graph_devices.get(item.device)
        if device is None:
            raise GraphSailError(f"plan schedules node {item.node!r} on an unknown device")
        if not node.can_run_on(device):
            raise GraphSailError(
                f"plan schedules node {item.node!r} on an incompatible device {item.device!r}"
            )
    if set(plan.memory_used_mb) - set(graph_devices):
        raise GraphSailError("plan memory summary names an unknown device")
    for decision in plan.decisions:
        if any(candidate.device not in graph_devices for candidate in decision.candidates):
            raise GraphSailError(f"decision for node {decision.node!r} names an unknown device")
    return plan


def _validated_planner(value: Any) -> Planner:
    if not callable(getattr(value, "plan", None)):
        raise GraphSailError("planner must provide a callable plan(graph) method")
    return cast(Planner, value)


def _validated_context(
    graph: Any, plan: Any, planner: Any
) -> tuple[GraphSpec, PlanResult, Planner]:
    checked_graph = _validated_graph(graph)
    checked_plan = _validated_plan_for_graph(checked_graph, plan)
    checked_planner = _validated_planner(planner)
    return checked_graph, checked_plan, checked_planner


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


def _perturb_validated(graph: GraphSpec, node_id: str, device: str, factor: float) -> GraphSpec:
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


def perturb(graph: GraphSpec, node_id: str, device: str, factor: float) -> GraphSpec:
    """Return the graph with one node's estimate for one device multiplied."""

    factor = _finite(factor, "perturbation factor", allow_negative=True)
    if factor <= 0.0:
        raise GraphSailError("perturbation factor must be positive")
    node_id = _safe_text(node_id, "perturbation node_id")
    device = _safe_text(device, "perturbation device")
    graph = _validated_graph(graph)
    return _perturb_validated(graph, node_id, device, factor)


def _placement(plan: PlanResult) -> dict[str, str]:
    return {item.node: item.device for item in plan.schedule}


def _makespan(plan: PlanResult) -> float:
    return max((item.finish_ms for item in plan.schedule), default=0.0)


def _require_planner_call_budget(maximum_calls: int) -> None:
    if maximum_calls > MAX_STABILITY_PLANNER_CALLS:
        raise GraphSailError(
            "sensitivity analysis can require at most "
            f"{MAX_STABILITY_PLANNER_CALLS} planner calls; requested bound is {maximum_calls}"
        )


def _reproduce_baseline(graph: GraphSpec, plan: PlanResult, planner: Planner) -> PlanResult:
    """Require one unperturbed reproduction of the sensitivity-relevant plan."""

    try:
        candidate = planner.plan(graph)
    except GraphSailError as exc:
        raise GraphSailError(
            "planner could not reproduce the supplied baseline on the unperturbed graph"
        ) from exc
    try:
        reproduced = _validated_plan_for_graph(graph, candidate)
    except GraphSailError as exc:
        raise GraphSailError(f"planner baseline result is invalid: {exc}") from exc
    if _placement(reproduced) != _placement(plan):
        raise GraphSailError(
            "planner does not reproduce the supplied baseline placement on the unperturbed graph"
        )
    if reproduced.schedule != plan.schedule:
        raise GraphSailError(
            "planner does not reproduce the supplied baseline schedule and timing "
            "on the unperturbed graph"
        )
    return reproduced


def _stability_calls_per_cell(
    slower_samples: tuple[float, ...], faster_samples: tuple[float, ...]
) -> int:
    return len(slower_samples) + len(faster_samples) + 2 * MAX_BISECTION_STEPS


def _plans_the_same(planner: Planner, graph: GraphSpec, baseline: dict[str, str]) -> bool:
    """Whether the perturbed graph still yields the baseline placement.

    A perturbation that makes the graph unplaceable counts as a change, because
    a placement that cannot be produced is certainly not the baseline one.
    """

    try:
        candidate = planner.plan(graph)
    except GraphSailError:
        return False
    return _placement(_validated_plan_for_graph(graph, candidate)) == baseline


def _geometric_probe_factors(limit: float, samples_per_octave: int) -> tuple[float, ...]:
    """Return a bounded geometric grid from one out to ``limit``.

    The endpoint is always present. At the default density the default range
    therefore probes 2 and 4 in the slower direction, and 0.5 and 0.25 in the
    faster direction.
    """

    span = abs(math.log2(limit))
    steps = max(1, math.ceil(span * samples_per_octave))
    if steps > MAX_COARSE_PROBES_PER_DIRECTION:
        raise GraphSailError(
            "stability probe grid would require "
            f"{steps} factors in one direction; limit is "
            f"{MAX_COARSE_PROBES_PER_DIRECTION}"
        )
    direction = 1.0 if limit > 1.0 else -1.0
    factors: list[float] = []
    for step in range(1, steps + 1):
        factor = limit if step == steps else math.exp2(direction * step / samples_per_octave)
        if not factors or factor != factors[-1]:
            factors.append(factor)
    return tuple(factors)


def _flip_factor(
    planner: Planner,
    graph: GraphSpec,
    node_id: str,
    device: str,
    baseline: dict[str, str],
    sample_factors: tuple[float, ...],
    tolerance: float,
) -> tuple[float | None, tuple[float, ...]]:
    """Find the first sampled change and refine its preceding bracket.

    ``None`` means every recorded coarse probe preserved the baseline. It says
    nothing about unsampled factors between those probes.
    """

    probes: list[float] = []
    preceding_unchanged = 1.0
    for sampled in sample_factors:
        probes.append(sampled)
        if _plans_the_same(
            planner,
            _perturb_validated(graph, node_id, device, sampled),
            baseline,
        ):
            preceding_unchanged = sampled
            continue

        changed = sampled
        unchanged = preceding_unchanged
        for _ in range(MAX_BISECTION_STEPS):
            if abs(changed - unchanged) <= tolerance * max(abs(unchanged), 1e-9):
                break
            middle = (unchanged + changed) / 2.0
            if middle in probes:
                break
            probes.append(middle)
            if _plans_the_same(
                planner,
                _perturb_validated(graph, node_id, device, middle),
                baseline,
            ):
                unchanged = middle
            else:
                changed = middle
        return changed, tuple(probes)
    return None, tuple(probes)


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

    probe = _finite(probe, "probe", allow_negative=True)
    if not 0.0 < probe < 1.0:
        raise GraphSailError("probe must lie between 0 and 1")
    graph, plan, planner = _validated_context(graph, plan, planner)
    _require_planner_call_budget(1 + len(plan.schedule))
    _reproduce_baseline(graph, plan, planner)
    return _makespan_sensitivity_validated(graph, plan, planner, probe)


def _makespan_sensitivity_validated(
    graph: GraphSpec,
    plan: PlanResult,
    planner: Planner,
    probe: float,
) -> tuple[MakespanSensitivity, ...]:
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
                probed = planner.plan(
                    _perturb_validated(graph, item.node, item.device, 1.0 + probe)
                )
            except GraphSailError:
                changed = True
            else:
                probed = _validated_plan_for_graph(graph, probed)
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
    samples_per_octave: int = DEFAULT_SAMPLES_PER_OCTAVE,
) -> tuple[PlacementStability, ...]:
    """Probe, per estimate, for factors that change the placement.

    Both directions use an explicit geometric grid. The first sampled change is
    refined against the preceding unchanged sample. A stable result is limited
    to the recorded factors and does not assert continuity between them.
    """

    min_factor = _finite(min_factor, "min_factor", allow_negative=True)
    max_factor = _finite(max_factor, "max_factor", allow_negative=True)
    tolerance = _finite(tolerance, "tolerance", allow_negative=True)
    samples_per_octave = _sample_density(samples_per_octave)
    if not 0.0 < min_factor < 1.0 < max_factor:
        raise GraphSailError("factors must satisfy 0 < min_factor < 1 < max_factor")
    if not 0.0 < tolerance < 1.0:
        raise GraphSailError("tolerance must lie between 0 and 1")
    graph, plan, planner = _validated_context(graph, plan, planner)
    slower_samples = _geometric_probe_factors(max_factor, samples_per_octave)
    faster_samples = _geometric_probe_factors(min_factor, samples_per_octave)
    _require_planner_call_budget(
        1 + len(plan.schedule) * _stability_calls_per_cell(slower_samples, faster_samples)
    )
    _reproduce_baseline(graph, plan, planner)
    return _placement_stability_validated(
        graph,
        plan,
        planner,
        min_factor,
        max_factor,
        tolerance,
        samples_per_octave,
        slower_samples,
        faster_samples,
    )


def _placement_stability_validated(
    graph: GraphSpec,
    plan: PlanResult,
    planner: Planner,
    min_factor: float,
    max_factor: float,
    tolerance: float,
    samples_per_octave: int,
    slower_samples: tuple[float, ...],
    faster_samples: tuple[float, ...],
) -> tuple[PlacementStability, ...]:
    baseline = _placement(plan)
    results = []
    for item in sorted(plan.schedule, key=lambda entry: entry.node):
        slower_factor, slower_probes = _flip_factor(
            planner,
            graph,
            item.node,
            item.device,
            baseline,
            slower_samples,
            tolerance,
        )
        faster_factor, faster_probes = _flip_factor(
            planner,
            graph,
            item.node,
            item.device,
            baseline,
            faster_samples,
            tolerance,
        )
        results.append(
            PlacementStability(
                node=item.node,
                device=item.device,
                estimate_ms=item.compute_ms,
                slower_factor=slower_factor,
                faster_factor=faster_factor,
                searched_range=(min_factor, max_factor),
                samples_per_octave=samples_per_octave,
                probed_factors=slower_probes + faster_probes,
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
    samples_per_octave: int = DEFAULT_SAMPLES_PER_OCTAVE,
) -> SensitivityReport:
    """Answer both questions about one plan under one planner.

    The planner is passed in rather than chosen here, because stability is a
    property of the plan *and* the algorithm that produced it. Before probing,
    the planner must reproduce the supplied placement and schedule/timing on the
    unperturbed graph. The report uses the algorithm label from that reproduction.
    """

    probe = _finite(probe, "probe", allow_negative=True)
    min_factor = _finite(min_factor, "min_factor", allow_negative=True)
    max_factor = _finite(max_factor, "max_factor", allow_negative=True)
    tolerance = _finite(tolerance, "tolerance", allow_negative=True)
    samples_per_octave = _sample_density(samples_per_octave)
    if not 0.0 < probe < 1.0:
        raise GraphSailError("probe must lie between 0 and 1")
    if not 0.0 < min_factor < 1.0 < max_factor:
        raise GraphSailError("factors must satisfy 0 < min_factor < 1 < max_factor")
    if not 0.0 < tolerance < 1.0:
        raise GraphSailError("tolerance must lie between 0 and 1")
    graph, plan, planner = _validated_context(graph, plan, planner)
    slower_samples = _geometric_probe_factors(max_factor, samples_per_octave)
    faster_samples = _geometric_probe_factors(min_factor, samples_per_octave)
    _require_planner_call_budget(
        1 + len(plan.schedule) * (1 + _stability_calls_per_cell(slower_samples, faster_samples))
    )
    reproduced = _reproduce_baseline(graph, plan, planner)
    return SensitivityReport(
        graph_name=plan.graph_name,
        algorithm=reproduced.algorithm,
        baseline_makespan_ms=_makespan(plan),
        makespan=_makespan_sensitivity_validated(graph, plan, planner, probe),
        stability=_placement_stability_validated(
            graph,
            plan,
            planner,
            min_factor,
            max_factor,
            tolerance,
            samples_per_octave,
            slower_samples,
            faster_samples,
        ),
        samples_per_octave=samples_per_octave,
    )
