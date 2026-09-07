"""Post-plan execution simulation and resource accounting.

Graph Sail's planners produce deterministic intervals.  This module turns that
schedule into the operational view users need for capacity reviews: device
busy time, utilization, peak persistent memory, and a stable event timeline.
It deliberately does not pretend to model queueing or runtime variance that
the input graph did not specify.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from itertools import pairwise
from typing import Any

from graph_sail.errors import PlanningError
from graph_sail.models import GraphSpec, PlanResult


@dataclass(frozen=True, slots=True)
class SimulationEvent:
    node: str
    device: str
    start_ms: float
    finish_ms: float
    compute_ms: float
    memory_after_mb: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "node": self.node,
            "device": self.device,
            "start_ms": round(self.start_ms, 6),
            "finish_ms": round(self.finish_ms, 6),
            "compute_ms": round(self.compute_ms, 6),
            "memory_after_mb": round(self.memory_after_mb, 6),
        }


@dataclass(frozen=True, slots=True)
class SimulationResult:
    graph_name: str
    plan_algorithm: str
    makespan_ms: float
    device_busy_ms: dict[str, float]
    device_utilization: dict[str, float]
    peak_memory_mb: dict[str, float]
    events: tuple[SimulationEvent, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "1.0",
            "kind": "graph-sail-simulation",
            "graph_name": self.graph_name,
            "plan_algorithm": self.plan_algorithm,
            "makespan_ms": round(self.makespan_ms, 6),
            "device_busy_ms": {key: round(value, 6) for key, value in self.device_busy_ms.items()},
            "device_utilization": {
                key: round(value, 6) for key, value in self.device_utilization.items()
            },
            "peak_memory_mb": {key: round(value, 6) for key, value in self.peak_memory_mb.items()},
            "events": [event.to_dict() for event in self.events],
        }


def simulate_plan(graph: GraphSpec, plan: PlanResult) -> SimulationResult:
    """Account for a plan and reject plans that do not fit the graph inventory."""

    if not isinstance(graph, GraphSpec) or not isinstance(plan, PlanResult):
        raise PlanningError("graph must be GraphSpec and plan must be PlanResult")
    graph.validate()
    if plan.graph_name != graph.name:
        raise PlanningError("plan graph_name does not match graph")
    devices = {device.name for device in graph.devices}
    node_ids = {node.id for node in graph.nodes}
    if {item.node for item in plan.schedule} != node_ids:
        raise PlanningError("plan must schedule every graph node exactly once")
    busy = dict.fromkeys(devices, 0.0)
    memory = dict.fromkeys(devices, 0.0)
    peak = dict.fromkeys(devices, 0.0)
    events: list[SimulationEvent] = []
    by_device: dict[str, list[SimulationEvent]] = {device: [] for device in devices}
    capacities = {device.name: device.memory_mb for device in graph.devices}
    for item in plan.schedule:
        if item.device not in devices:
            raise PlanningError(f"plan uses unknown device {item.device!r}")
        if item.node not in node_ids:
            raise PlanningError(f"plan uses unknown node {item.node!r}")
        memory[item.device] += item.memory_mb
        peak[item.device] = max(peak[item.device], memory[item.device])
        if memory[item.device] > capacities[item.device] + 1e-9:
            raise PlanningError(f"plan exceeds persistent memory on device {item.device!r}")
        event = SimulationEvent(
            node=item.node,
            device=item.device,
            start_ms=item.start_ms,
            finish_ms=item.finish_ms,
            compute_ms=item.compute_ms,
            memory_after_mb=memory[item.device],
        )
        by_device[item.device].append(event)
        events.append(event)
        busy[item.device] += item.compute_ms
    makespan = plan.makespan_ms
    if makespan < 0 or not math.isfinite(makespan):
        raise PlanningError("plan makespan must be finite and non-negative")
    for device, entries in by_device.items():
        ordered = sorted(entries, key=lambda entry: (entry.start_ms, entry.finish_ms, entry.node))
        if any(right.start_ms + 1e-9 < left.finish_ms for left, right in pairwise(ordered)):
            raise PlanningError(f"plan overlaps execution intervals on device {device!r}")
    utilization = {
        device: (busy[device] / makespan if makespan > 0 else 0.0) for device in sorted(devices)
    }
    if any(
        not math.isfinite(value) or value < 0 or value > 1 + 1e-9 for value in utilization.values()
    ):
        raise PlanningError("device utilization must be finite and between zero and one")
    return SimulationResult(
        graph_name=graph.name,
        plan_algorithm=plan.algorithm,
        makespan_ms=makespan,
        device_busy_ms={key: busy[key] for key in sorted(busy)},
        device_utilization={key: utilization[key] for key in sorted(utilization)},
        peak_memory_mb={key: peak[key] for key in sorted(peak)},
        events=tuple(sorted(events, key=lambda event: (event.start_ms, event.device, event.node))),
    )


__all__ = ["SimulationEvent", "SimulationResult", "simulate_plan"]
