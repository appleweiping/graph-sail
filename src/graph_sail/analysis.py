"""Derived metrics and critical-chain analysis for completed plans."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from graph_sail.errors import PlanningError
from graph_sail.limits import MAX_DEVICES, MAX_EDGES, MAX_NODES, MAX_TEXT_LENGTH
from graph_sail.models import GraphSpec, PlanResult, ScheduledNode


@dataclass(frozen=True, slots=True)
class PlanMetrics:
    """Compact metrics suitable for logs, reports, and regression assertions."""

    makespan_ms: float
    total_compute_ms: float
    total_transfer_ms: float
    cross_device_edges: int
    critical_chain: tuple[str, ...]
    device_utilization: Mapping[str, float]
    memory_utilization: Mapping[str, float]

    def __post_init__(self) -> None:
        for field_name in ("makespan_ms", "total_compute_ms", "total_transfer_ms"):
            object.__setattr__(
                self,
                field_name,
                _metric_number(getattr(self, field_name), field_name),
            )
        if (
            isinstance(self.cross_device_edges, bool)
            or not isinstance(self.cross_device_edges, int)
            or not 0 <= self.cross_device_edges <= MAX_EDGES
        ):
            raise PlanningError(f"cross_device_edges must be an integer from 0 to {MAX_EDGES}")
        chain = _bounded_chain(self.critical_chain)
        devices = _bounded_utilization(self.device_utilization, "device_utilization")
        memory = _bounded_utilization(self.memory_utilization, "memory_utilization")
        if devices.keys() != memory.keys():
            raise PlanningError("device and memory utilization must name the same devices")
        object.__setattr__(self, "critical_chain", chain)
        object.__setattr__(self, "device_utilization", MappingProxyType(devices))
        object.__setattr__(self, "memory_utilization", MappingProxyType(memory))


def _metric_number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PlanningError(f"{label} must be a number")
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise PlanningError(f"{label} must be finite") from exc
    if not math.isfinite(number) or number < 0:
        raise PlanningError(f"{label} must be finite and zero or greater")
    return number


def _metric_name(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise PlanningError(f"{label} must be a string")
    text = value.strip()
    if not text:
        raise PlanningError(f"{label} must not be empty")
    if len(text) > MAX_TEXT_LENGTH:
        raise PlanningError(f"{label} exceeds the {MAX_TEXT_LENGTH}-character limit")
    try:
        text.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise PlanningError(f"{label} must contain valid Unicode scalar values") from exc
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise PlanningError(f"{label} must not contain control characters")
    return text


def _bounded_chain(value: Any) -> tuple[str, ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Iterable):
        raise PlanningError("critical_chain must be an iterable")
    result: list[str] = []
    seen: set[str] = set()
    for item in value:
        if len(result) == MAX_NODES:
            raise PlanningError(f"critical_chain exceeds the {MAX_NODES}-item limit")
        node = _metric_name(item, "critical_chain entry")
        if node in seen:
            raise PlanningError(f"critical_chain contains duplicate node {node!r}")
        result.append(node)
        seen.add(node)
    return tuple(result)


def _bounded_utilization(value: Any, label: str) -> dict[str, float]:
    if not isinstance(value, Mapping):
        raise PlanningError(f"{label} must be a mapping")
    result: dict[str, float] = {}
    for entry_count, (raw_device, raw_usage) in enumerate(value.items()):
        if entry_count == MAX_DEVICES:
            raise PlanningError(f"{label} exceeds the {MAX_DEVICES}-entry limit")
        device = _metric_name(raw_device, f"{label} key")
        if device in result:
            raise PlanningError(f"{label} contains duplicate device {device!r}")
        usage = _metric_number(raw_usage, f"{label} for {device!r}")
        if usage > 1.0 + 1e-9:
            raise PlanningError(f"{label} for {device!r} must not exceed one")
        result[device] = usage
    return result


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
