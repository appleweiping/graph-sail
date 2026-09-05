"""Derived metrics and critical-chain analysis for completed plans."""

from __future__ import annotations

import math
from dataclasses import dataclass

from graph_sail.errors import PlanningError
from graph_sail.models import GraphSpec, PlanResult, ScheduledNode


@dataclass(frozen=True, slots=True)
class PlanMetrics:
    """Compact metrics suitable for logs, reports, and regression assertions."""

    makespan_ms: float
    total_compute_ms: float
    total_transfer_ms: float
    cross_device_edges: int
    critical_chain: tuple[str, ...]
    device_utilization: dict[str, float]
    memory_utilization: dict[str, float]


def analyze_plan(graph: GraphSpec, plan: PlanResult) -> PlanMetrics:
    """Compute deterministic aggregate metrics from a valid plan."""

    by_node = {item.node: item for item in plan.schedule}
    link_index = graph.link_map
    compute = sum(item.compute_ms for item in plan.schedule)
    if not math.isfinite(compute):
        raise PlanningError("total compute estimate overflowed")
    transfer = 0.0
    cross_edges = 0
    for edge in graph.edges:
        source = by_node[edge.source]
        target = by_node[edge.target]
        if source.device != target.device:
            cross_edges += 1
            transfer += graph.transfer_ms(
                source.device, target.device, edge.payload_mb, _link_index=link_index
            )
            if not math.isfinite(transfer):
                raise PlanningError("total transfer estimate overflowed")

    makespan = plan.makespan_ms
    if not math.isfinite(makespan):
        raise PlanningError("plan makespan is not finite")
    device_compute: dict[str, float] = {device.name: 0.0 for device in graph.devices}
    for item in plan.schedule:
        device_compute[item.device] += item.compute_ms
    utilization: dict[str, float] = {}
    for device_name, value in device_compute.items():
        usage = value / makespan if makespan else 0.0
        if not math.isfinite(usage):
            raise PlanningError(f"compute utilization is not finite for device {device_name!r}")
        utilization[device_name] = usage
    memory: dict[str, float] = {}
    for device in graph.devices:
        usage = plan.memory_used_mb.get(device.name, 0.0) / device.memory_mb
        if not math.isfinite(usage):
            raise PlanningError(f"memory utilization is not finite for device {device.name!r}")
        memory[device.name] = usage
    return PlanMetrics(
        makespan_ms=makespan,
        total_compute_ms=compute,
        total_transfer_ms=transfer,
        cross_device_edges=cross_edges,
        critical_chain=critical_chain(graph, plan),
        device_utilization=utilization,
        memory_utilization=memory,
    )


def critical_chain(graph: GraphSpec, plan: PlanResult) -> tuple[str, ...]:
    """Trace the dependency/device predecessor that determined each start time."""

    if not plan.schedule:
        return ()
    by_node = {item.node: item for item in plan.schedule}
    link_index = graph.link_map
    dependency_edges: dict[str, list[tuple[float, str]]] = {item.node: [] for item in plan.schedule}
    for edge in graph.edges:
        source = by_node[edge.source]
        target = by_node[edge.target]
        ready = (
            source.finish_ms
            + graph.transfer_ms(
                source.device, target.device, edge.payload_mb, _link_index=link_index
            )
            + target.batch_window_ms
        )
        if not math.isfinite(ready):
            raise PlanningError(f"critical-chain time overflowed for node {target.node!r}")
        dependency_edges[target.node].append((ready, source.node))

    previous_on_device: dict[str, ScheduledNode] = {}
    cause: dict[str, str] = {}
    for item in plan.schedule:
        candidates = dependency_edges[item.node]
        previous = previous_on_device.get(item.device)
        if previous is not None:
            candidates.append((previous.finish_ms, previous.node))
        if candidates:
            ready, predecessor = max(candidates, key=lambda pair: (pair[0], pair[1]))
            if abs(ready - item.start_ms) <= 1e-6:
                cause[item.node] = predecessor
        previous_on_device[item.device] = item

    cursor = max(plan.schedule, key=lambda item: (item.finish_ms, item.node)).node
    reversed_chain = [cursor]
    while cursor in cause:
        cursor = cause[cursor]
        reversed_chain.append(cursor)
    return tuple(reversed(reversed_chain))
