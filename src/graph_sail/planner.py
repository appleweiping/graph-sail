"""Deterministic greedy and beam-search placement planners."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass

from graph_sail.errors import PlanningError
from graph_sail.graph import predecessor_edges, topological_order
from graph_sail.limits import MAX_BEAM_WIDTH, MAX_PLANNER_EXPANSIONS
from graph_sail.models import (
    CandidateTrace,
    DeviceSpec,
    EdgeSpec,
    GraphSpec,
    LinkSpec,
    NodeSpec,
    PlacementDecision,
    PlanResult,
    ScheduledNode,
)

_EPSILON = 1e-9


@dataclass(frozen=True, slots=True)
class _PlanState:
    schedule: tuple[ScheduledNode, ...]
    by_node: dict[str, ScheduledNode]
    memory_used: dict[str, float]
    device_ready: dict[str, float]
    cotenants: dict[str, int]
    decisions: tuple[PlacementDecision, ...]

    @classmethod
    def empty(cls, graph: GraphSpec) -> _PlanState:
        return cls(
            schedule=(),
            by_node={},
            memory_used={device.name: 0.0 for device in graph.devices},
            device_ready={device.name: 0.0 for device in graph.devices},
            cotenants={device.name: 0 for device in graph.devices},
            decisions=(),
        )

    @property
    def makespan_ms(self) -> float:
        return max((item.finish_ms for item in self.schedule), default=0.0)


@dataclass(frozen=True, slots=True)
class _EvaluatedCandidate:
    trace: CandidateTrace
    scheduled: ScheduledNode | None


class GreedyPlanner:
    """Choose the locally earliest-finishing feasible device for each node."""

    name = "greedy-earliest-finish"

    def plan(self, graph: GraphSpec) -> PlanResult:
        """Place and schedule every node, or raise a detailed feasibility error."""

        _validate_planning_work(graph, beam_width=1)
        state = _PlanState.empty(graph)
        incoming = predecessor_edges(graph)
        node_index = graph.node_map
        link_index = graph.link_map
        devices = tuple(sorted(graph.devices, key=lambda item: item.name))
        for node_id in topological_order(graph):
            node = node_index[node_id]
            evaluated = tuple(
                _evaluate_candidate(graph, node, device, state, incoming[node_id], link_index)
                for device in devices
            )
            feasible = [item.scheduled for item in evaluated if item.scheduled is not None]
            if not feasible:
                raise _no_placement_error(node, evaluated)
            chosen = min(
                feasible,
                key=lambda item: (item.finish_ms, item.start_ms, item.device),
            )
            state = _extend_state(state, chosen, tuple(item.trace for item in evaluated))
        return _to_result(graph, state, self.name)


class BeamPlanner:
    """Explore several partial placements to avoid obvious greedy dead ends."""

    name = "beam-earliest-finish"

    def __init__(self, beam_width: int = 16) -> None:
        if (
            isinstance(beam_width, bool)
            or not isinstance(beam_width, int)
            or not 1 <= beam_width <= MAX_BEAM_WIDTH
        ):
            raise ValueError(
                f"beam_width must be a positive integer no greater than {MAX_BEAM_WIDTH}"
            )
        self.beam_width = beam_width

    def plan(self, graph: GraphSpec) -> PlanResult:
        """Search up to ``beam_width`` deterministic partial placements per level."""

        _validate_planning_work(graph, beam_width=self.beam_width)
        states = [_PlanState.empty(graph)]
        incoming = predecessor_edges(graph)
        node_index = graph.node_map
        link_index = graph.link_map
        devices = tuple(sorted(graph.devices, key=lambda item: item.name))
        for node_id in topological_order(graph):
            node = node_index[node_id]
            expanded: list[_PlanState] = []
            last_evaluated: tuple[_EvaluatedCandidate, ...] = ()
            for state in states:
                evaluated = tuple(
                    _evaluate_candidate(graph, node, device, state, incoming[node_id], link_index)
                    for device in devices
                )
                last_evaluated = evaluated
                traces = tuple(item.trace for item in evaluated)
                for candidate in evaluated:
                    if candidate.scheduled is not None:
                        expanded.append(_extend_state(state, candidate.scheduled, traces))
            if not expanded:
                raise _no_placement_error(node, last_evaluated)
            expanded.sort(key=_state_rank)
            states = expanded[: self.beam_width]
        return _to_result(graph, min(states, key=_state_rank), f"{self.name}:{self.beam_width}")


def _evaluate_candidate(
    graph: GraphSpec,
    node: NodeSpec,
    device: DeviceSpec,
    state: _PlanState,
    incoming_edges: tuple[EdgeSpec, ...],
    link_index: Mapping[tuple[str, str], LinkSpec],
) -> _EvaluatedCandidate:
    if not node.can_run_on(device):
        return _EvaluatedCandidate(
            trace=CandidateTrace(
                device=device.name,
                feasible=False,
                reason=_static_rejection_reason(node, device),
            ),
            scheduled=None,
        )

    remaining = device.memory_mb - state.memory_used[device.name]
    if node.memory_mb > remaining + _EPSILON:
        return _EvaluatedCandidate(
            trace=CandidateTrace(
                device=device.name,
                feasible=False,
                reason=(
                    f"memory budget exceeded: needs {node.memory_mb:g} MB, "
                    f"has {max(remaining, 0.0):g} MB free"
                ),
            ),
            scheduled=None,
        )

    dependency_ready = 0.0
    transfer_total = 0.0
    for edge in incoming_edges:
        predecessor = state.by_node[edge.source]
        transfer = graph.transfer_ms(
            predecessor.device, device.name, edge.payload_mb, _link_index=link_index
        )
        transfer_total += transfer
        if not math.isfinite(transfer_total):
            raise PlanningError(f"incoming transfer total overflowed for node {node.id!r}")
        dependency_ready = max(dependency_ready, predecessor.finish_ms + transfer)

    window = node.batch_window_ms()
    start = max(state.device_ready[device.name], dependency_ready + window)
    scale = device.contention_factor(state.cotenants[device.name]) * node.batch_factor()
    compute = node.latency_ms[device.name] * scale
    finish = start + compute
    if not math.isfinite(start) or not math.isfinite(finish):
        raise PlanningError(
            f"schedule time overflowed for node {node.id!r} on device {device.name!r}"
        )
    scheduled = ScheduledNode(
        node=node.id,
        device=device.name,
        start_ms=start,
        finish_ms=finish,
        compute_ms=compute,
        incoming_transfer_ms=transfer_total,
        memory_mb=node.memory_mb,
        latency_scale=scale,
        batch_window_ms=window,
    )
    return _EvaluatedCandidate(
        trace=CandidateTrace(
            device=device.name,
            feasible=True,
            reason="feasible",
            start_ms=start,
            finish_ms=finish,
            transfer_ms=transfer_total,
        ),
        scheduled=scheduled,
    )


def _static_rejection_reason(node: NodeSpec, device: DeviceSpec) -> str:
    if node.pinned_device is not None and node.pinned_device != device.name:
        return f"node is pinned to {node.pinned_device}"
    if node.allowed_devices and device.name not in node.allowed_devices:
        return "device is not in allowed_devices"
    if not device.supports(node.kind):
        return f"device does not support kind {node.kind}"
    if device.name not in node.latency_ms:
        return "node has no latency estimate for this device"
    return "incompatible placement"


def _extend_state(
    state: _PlanState,
    scheduled: ScheduledNode | None,
    traces: tuple[CandidateTrace, ...],
) -> _PlanState:
    if scheduled is None:  # pragma: no cover - guarded by callers and type narrowing
        raise AssertionError("cannot extend a plan with an infeasible candidate")
    by_node = dict(state.by_node)
    by_node[scheduled.node] = scheduled
    memory = dict(state.memory_used)
    memory[scheduled.device] += scheduled.memory_mb
    ready = dict(state.device_ready)
    ready[scheduled.device] = scheduled.finish_ms
    cotenants = dict(state.cotenants)
    cotenants[scheduled.device] += 1
    decision = PlacementDecision(
        node=scheduled.node,
        selected_device=scheduled.device,
        candidates=traces,
    )
    return _PlanState(
        schedule=(*state.schedule, scheduled),
        by_node=by_node,
        memory_used=memory,
        device_ready=ready,
        cotenants=cotenants,
        decisions=(*state.decisions, decision),
    )


def _state_rank(state: _PlanState) -> tuple[float, float, tuple[tuple[str, str], ...]]:
    placements = tuple((item.node, item.device) for item in state.schedule)
    total_finish = sum(item.finish_ms for item in state.schedule)
    return (state.makespan_ms, total_finish, placements)


def _no_placement_error(
    node: NodeSpec, evaluated: tuple[_EvaluatedCandidate, ...]
) -> PlanningError:
    reasons = "; ".join(f"{item.trace.device}: {item.trace.reason}" for item in evaluated)
    return PlanningError(f"no feasible placement for node {node.id!r} ({reasons})")


def _to_result(graph: GraphSpec, state: _PlanState, algorithm: str) -> PlanResult:
    return PlanResult(
        graph_name=graph.name,
        algorithm=algorithm,
        schedule=state.schedule,
        memory_used_mb=state.memory_used,
        decisions=state.decisions,
    )


def _validate_planning_work(graph: GraphSpec, *, beam_width: int) -> None:
    if not isinstance(graph, GraphSpec):
        raise PlanningError("graph must be a GraphSpec")
    graph.validate()
    work = len(graph.nodes) * len(graph.devices) * beam_width
    if work > MAX_PLANNER_EXPANSIONS:
        raise PlanningError(f"planning work exceeds the {MAX_PLANNER_EXPANSIONS}-candidate limit")
