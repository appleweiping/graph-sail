"""Defensively immutable domain models for validation, planning, and reporting."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, TypeVar

from graph_sail.errors import PlanningError, ValidationError
from graph_sail.limits import (
    MAX_DEVICES,
    MAX_EDGES,
    MAX_KINDS_PER_DEVICE,
    MAX_LATENCY_CELLS_PER_NODE,
    MAX_LINKS,
    MAX_NODES,
    MAX_TEXT_LENGTH,
)

_T = TypeVar("_T")
_EPSILON = 1e-9


def _text(value: Any, label: str, *, allow_empty: bool = False, output: bool = False) -> str:
    error = PlanningError if output else ValidationError
    if not isinstance(value, str):
        raise error(f"{label} must be a string")
    text = value.strip()
    if not text and not allow_empty:
        raise error(f"{label} must not be empty")
    if len(text) > MAX_TEXT_LENGTH:
        raise error(f"{label} exceeds the {MAX_TEXT_LENGTH}-character limit")
    try:
        text.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise error(f"{label} must contain valid Unicode scalar values") from exc
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise error(f"{label} must not contain control characters")
    return text


def _number(
    value: Any,
    label: str,
    *,
    positive: bool = False,
    output: bool = False,
) -> float:
    error = PlanningError if output else ValidationError
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise error(f"{label} must be a number")
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise error(f"{label} must be finite") from exc
    if not math.isfinite(number):
        raise error(f"{label} must be finite")
    if positive and number <= 0:
        raise error(f"{label} must be greater than zero")
    if not positive and number < 0:
        raise error(f"{label} must be zero or greater")
    return number


def _bounded_tuple(
    value: Any, label: str, item_type: type[_T], limit: int, *, output: bool = False
) -> tuple[_T, ...]:
    error = PlanningError if output else ValidationError
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Iterable):
        raise error(f"{label} must be an iterable")
    result: list[_T] = []
    for item in value:
        if len(result) == limit:
            raise error(f"{label} exceeds the {limit}-item limit")
        if not isinstance(item, item_type):
            raise error(f"{label} entries must be {item_type.__name__} instances")
        result.append(item)
    return tuple(result)


def _string_set(value: Any, label: str, limit: int) -> frozenset[str]:
    values = _bounded_tuple(value, label, str, limit)
    normalized = frozenset(_text(item, f"{label} entry") for item in values)
    if len(normalized) != len(values):
        raise ValidationError(f"{label} must not contain duplicates")
    return normalized


@dataclass(frozen=True, slots=True)
class DeviceSpec:
    """A compute device with a persistent model-memory budget."""

    name: str
    memory_mb: float
    kinds: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _text(self.name, "device name"))
        object.__setattr__(
            self, "memory_mb", _number(self.memory_mb, "device memory_mb", positive=True)
        )
        object.__setattr__(
            self, "kinds", _string_set(self.kinds, "device kinds", MAX_KINDS_PER_DEVICE)
        )

    def supports(self, kind: str) -> bool:
        """Return whether this device accepts a node kind."""

        return not self.kinds or kind in self.kinds


@dataclass(frozen=True, slots=True)
class NodeSpec:
    """A component in the logical multimodal execution graph."""

    id: str
    kind: str
    memory_mb: float
    latency_ms: Mapping[str, float]
    allowed_devices: frozenset[str] = field(default_factory=frozenset)
    pinned_device: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _text(self.id, "node id"))
        object.__setattr__(self, "kind", _text(self.kind, "node kind"))
        object.__setattr__(self, "memory_mb", _number(self.memory_mb, "node memory_mb"))
        if not isinstance(self.latency_ms, Mapping):
            raise ValidationError("node latency_ms must be a mapping")
        if not self.latency_ms:
            raise ValidationError("node latency_ms must contain at least one device estimate")
        if len(self.latency_ms) > MAX_LATENCY_CELLS_PER_NODE:
            raise ValidationError(
                f"node latency_ms exceeds the {MAX_LATENCY_CELLS_PER_NODE}-entry limit"
            )
        latencies: dict[str, float] = {}
        for raw_device, raw_latency in self.latency_ms.items():
            device = _text(raw_device, "node latency_ms key")
            if device in latencies:
                raise ValidationError(f"duplicate node latency_ms device {device!r}")
            latencies[device] = _number(raw_latency, f"latency for {device!r}", positive=True)
        object.__setattr__(self, "latency_ms", MappingProxyType(latencies))
        object.__setattr__(
            self,
            "allowed_devices",
            _string_set(self.allowed_devices, "node allowed_devices", MAX_LATENCY_CELLS_PER_NODE),
        )
        if self.pinned_device is not None:
            object.__setattr__(
                self, "pinned_device", _text(self.pinned_device, "node pinned_device")
            )

    def can_run_on(self, device: DeviceSpec) -> bool:
        """Return whether static constraints permit this placement."""

        if not isinstance(device, DeviceSpec):
            return False
        if self.pinned_device is not None and self.pinned_device != device.name:
            return False
        if self.allowed_devices and device.name not in self.allowed_devices:
            return False
        return device.supports(self.kind) and device.name in self.latency_ms


@dataclass(frozen=True, slots=True)
class EdgeSpec:
    """A data dependency and its estimated payload size."""

    source: str
    target: str
    payload_mb: float = 0.0
    label: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", _text(self.source, "edge source"))
        object.__setattr__(self, "target", _text(self.target, "edge target"))
        object.__setattr__(self, "payload_mb", _number(self.payload_mb, "edge payload_mb"))
        object.__setattr__(self, "label", _text(self.label, "edge label", allow_empty=True))
        if self.source == self.target:
            raise ValidationError(f"node {self.source!r} cannot have a self edge")


@dataclass(frozen=True, slots=True)
class LinkSpec:
    """A directed device link used for cross-device transfer estimates."""

    source: str
    target: str
    bandwidth_mb_s: float
    latency_ms: float = 0.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", _text(self.source, "link source"))
        object.__setattr__(self, "target", _text(self.target, "link target"))
        object.__setattr__(
            self,
            "bandwidth_mb_s",
            _number(self.bandwidth_mb_s, "link bandwidth_mb_s", positive=True),
        )
        object.__setattr__(self, "latency_ms", _number(self.latency_ms, "link latency_ms"))
        if self.source == self.target:
            raise ValidationError(f"device link {self.source!r} cannot target itself")

    def transfer_ms(self, payload_mb: float) -> float:
        """Estimate one transfer, including fixed and payload-dependent cost."""

        payload = _number(payload_mb, "transfer payload_mb", output=True)
        duration = self.latency_ms + (payload / self.bandwidth_mb_s * 1000.0)
        if not math.isfinite(duration):
            raise PlanningError(
                f"transfer estimate overflowed for link {self.source!r} -> {self.target!r}"
            )
        return duration


@dataclass(frozen=True, slots=True)
class GraphSpec:
    """A validated logical graph and physical device inventory."""

    name: str
    devices: tuple[DeviceSpec, ...]
    nodes: tuple[NodeSpec, ...]
    edges: tuple[EdgeSpec, ...]
    links: tuple[LinkSpec, ...] = ()
    default_bandwidth_mb_s: float = 1_000.0
    default_link_latency_ms: float = 0.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _text(self.name, "graph name"))
        object.__setattr__(
            self, "devices", _bounded_tuple(self.devices, "devices", DeviceSpec, MAX_DEVICES)
        )
        object.__setattr__(self, "nodes", _bounded_tuple(self.nodes, "nodes", NodeSpec, MAX_NODES))
        object.__setattr__(self, "edges", _bounded_tuple(self.edges, "edges", EdgeSpec, MAX_EDGES))
        object.__setattr__(self, "links", _bounded_tuple(self.links, "links", LinkSpec, MAX_LINKS))
        object.__setattr__(
            self,
            "default_bandwidth_mb_s",
            _number(self.default_bandwidth_mb_s, "default_bandwidth_mb_s", positive=True),
        )
        object.__setattr__(
            self,
            "default_link_latency_ms",
            _number(self.default_link_latency_ms, "default_link_latency_ms"),
        )
        self.validate()

    def validate(self) -> None:
        """Recheck the complete invariant set, including after hostile mutation."""

        _text(self.name, "graph name")
        _number(self.default_bandwidth_mb_s, "default_bandwidth_mb_s", positive=True)
        _number(self.default_link_latency_ms, "default_link_latency_ms")
        for collection, label, maximum in (
            (self.devices, "devices", MAX_DEVICES),
            (self.nodes, "nodes", MAX_NODES),
            (self.edges, "edges", MAX_EDGES),
            (self.links, "links", MAX_LINKS),
        ):
            if not isinstance(collection, tuple):
                raise ValidationError(f"{label} must be a tuple")
            if len(collection) > maximum:
                raise ValidationError(f"{label} exceeds the {maximum}-item limit")
        if not self.devices:
            raise ValidationError("devices must contain at least one device")
        if not self.nodes:
            raise ValidationError("nodes must contain at least one node")
        if not all(isinstance(item, DeviceSpec) for item in self.devices):
            raise ValidationError("devices entries must be DeviceSpec instances")
        if not all(isinstance(item, NodeSpec) for item in self.nodes):
            raise ValidationError("nodes entries must be NodeSpec instances")
        if not all(isinstance(item, EdgeSpec) for item in self.edges):
            raise ValidationError("edges entries must be EdgeSpec instances")
        if not all(isinstance(item, LinkSpec) for item in self.links):
            raise ValidationError("links entries must be LinkSpec instances")
        for device in self.devices:
            DeviceSpec(device.name, device.memory_mb, device.kinds)
        for node in self.nodes:
            NodeSpec(
                node.id,
                node.kind,
                node.memory_mb,
                node.latency_ms,
                node.allowed_devices,
                node.pinned_device,
            )
        for edge in self.edges:
            EdgeSpec(edge.source, edge.target, edge.payload_mb, edge.label)
        for link in self.links:
            LinkSpec(link.source, link.target, link.bandwidth_mb_s, link.latency_ms)

        device_names = [device.name for device in self.devices]
        node_ids = [node.id for node in self.nodes]
        _unique(device_names, "device name")
        _unique(node_ids, "node id")
        _unique(((edge.source, edge.target) for edge in self.edges), "edge")
        _unique(((link.source, link.target) for link in self.links), "link")
        device_set = set(device_names)
        node_set = set(node_ids)

        for node in self.nodes:
            unknown_profiles = sorted(set(node.latency_ms) - device_set)
            if unknown_profiles:
                raise ValidationError(
                    f"node {node.id!r} has latency estimates for unknown device(s): "
                    f"{', '.join(unknown_profiles)}"
                )
            unknown_allowed = sorted(set(node.allowed_devices) - device_set)
            if unknown_allowed:
                raise ValidationError(
                    f"node {node.id!r} allows unknown device(s): {', '.join(unknown_allowed)}"
                )
            if node.pinned_device is not None and node.pinned_device not in device_set:
                raise ValidationError(
                    f"node {node.id!r} is pinned to unknown device {node.pinned_device!r}"
                )
            if not any(node.can_run_on(device) for device in self.devices):
                raise ValidationError(f"node {node.id!r} has no statically compatible device")

        for edge in self.edges:
            if edge.source not in node_set:
                raise ValidationError(f"edge source {edge.source!r} does not name a node")
            if edge.target not in node_set:
                raise ValidationError(f"edge target {edge.target!r} does not name a node")
        for link in self.links:
            if link.source not in device_set or link.target not in device_set:
                raise ValidationError(
                    f"link {link.source!r} -> {link.target!r} references an unknown device"
                )
        from graph_sail.graph import topological_order

        topological_order(self)

    @property
    def node_map(self) -> Mapping[str, NodeSpec]:
        return MappingProxyType({node.id: node for node in self.nodes})

    @property
    def device_map(self) -> Mapping[str, DeviceSpec]:
        return MappingProxyType({device.name: device for device in self.devices})

    @property
    def edge_map(self) -> Mapping[tuple[str, str], EdgeSpec]:
        return MappingProxyType({(edge.source, edge.target): edge for edge in self.edges})

    @property
    def link_map(self) -> Mapping[tuple[str, str], LinkSpec]:
        return MappingProxyType({(link.source, link.target): link for link in self.links})

    def transfer_ms(
        self,
        source_device: str,
        target_device: str,
        payload_mb: float,
        *,
        _link_index: Mapping[tuple[str, str], LinkSpec] | None = None,
    ) -> float:
        """Return zero for local edges and a configured/default estimate otherwise."""

        source = _text(source_device, "source_device", output=True)
        target = _text(target_device, "target_device", output=True)
        payload = _number(payload_mb, "transfer payload_mb", output=True)
        devices = self.device_map
        if source not in devices or target not in devices:
            raise PlanningError(f"transfer references unknown device {source!r} -> {target!r}")
        if source == target:
            return 0.0
        links = self.link_map if _link_index is None else _link_index
        link = links.get((source, target))
        if link is not None:
            if not isinstance(link, LinkSpec):
                raise PlanningError("link index contains an invalid link")
            return link.transfer_ms(payload)
        duration = self.default_link_latency_ms + payload / self.default_bandwidth_mb_s * 1000.0
        if not math.isfinite(duration):
            raise PlanningError(
                f"transfer estimate overflowed for devices {source!r} -> {target!r}"
            )
        return duration


@dataclass(frozen=True, slots=True)
class CandidateTrace:
    """Why one device was accepted or rejected for a node."""

    device: str
    feasible: bool
    reason: str
    start_ms: float | None = None
    finish_ms: float | None = None
    transfer_ms: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "device", _text(self.device, "candidate device", output=True))
        if not isinstance(self.feasible, bool):
            raise PlanningError("candidate feasible must be a boolean")
        object.__setattr__(self, "reason", _text(self.reason, "candidate reason", output=True))
        times = (self.start_ms, self.finish_ms, self.transfer_ms)
        if self.feasible and any(value is None for value in times):
            raise PlanningError("feasible candidate must include start, finish, and transfer times")
        if not self.feasible and any(value is not None for value in times):
            raise PlanningError("infeasible candidate must not include schedule times")
        if self.feasible:
            start = _number(self.start_ms, "candidate start_ms", output=True)
            finish = _number(self.finish_ms, "candidate finish_ms", output=True)
            transfer = _number(self.transfer_ms, "candidate transfer_ms", output=True)
            if finish + _EPSILON < start:
                raise PlanningError("candidate finish_ms must not precede start_ms")
            object.__setattr__(self, "start_ms", start)
            object.__setattr__(self, "finish_ms", finish)
            object.__setattr__(self, "transfer_ms", transfer)


@dataclass(frozen=True, slots=True)
class PlacementDecision:
    """The selected device and alternatives considered for a node."""

    node: str
    selected_device: str
    candidates: tuple[CandidateTrace, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "node", _text(self.node, "decision node", output=True))
        object.__setattr__(
            self,
            "selected_device",
            _text(self.selected_device, "decision selected_device", output=True),
        )
        candidates = _bounded_tuple(
            self.candidates, "decision candidates", CandidateTrace, MAX_DEVICES, output=True
        )
        if not candidates:
            raise PlanningError("decision candidates must not be empty")
        _unique_output((candidate.device for candidate in candidates), "candidate device")
        selected = [
            candidate
            for candidate in candidates
            if candidate.device == self.selected_device and candidate.feasible
        ]
        if len(selected) != 1:
            raise PlanningError("selected_device must identify one feasible candidate")
        object.__setattr__(self, "candidates", candidates)


@dataclass(frozen=True, slots=True)
class ScheduledNode:
    """One node's placement and deterministic schedule interval."""

    node: str
    device: str
    start_ms: float
    finish_ms: float
    compute_ms: float
    incoming_transfer_ms: float
    memory_mb: float

    def __post_init__(self) -> None:
        object.__setattr__(self, "node", _text(self.node, "scheduled node", output=True))
        object.__setattr__(self, "device", _text(self.device, "scheduled device", output=True))
        for field_name in (
            "start_ms",
            "finish_ms",
            "compute_ms",
            "incoming_transfer_ms",
            "memory_mb",
        ):
            object.__setattr__(
                self, field_name, _number(getattr(self, field_name), field_name, output=True)
            )
        if not math.isclose(
            self.finish_ms, self.start_ms + self.compute_ms, rel_tol=1e-12, abs_tol=1e-9
        ):
            raise PlanningError("scheduled finish_ms must equal start_ms plus compute_ms")


@dataclass(frozen=True, slots=True)
class PlanResult:
    """A complete placement, schedule, resource summary, and decision trace."""

    graph_name: str
    algorithm: str
    schedule: tuple[ScheduledNode, ...]
    memory_used_mb: Mapping[str, float]
    decisions: tuple[PlacementDecision, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "graph_name", _text(self.graph_name, "graph_name", output=True))
        object.__setattr__(self, "algorithm", _text(self.algorithm, "algorithm", output=True))
        schedule = _bounded_tuple(self.schedule, "schedule", ScheduledNode, MAX_NODES, output=True)
        decisions = _bounded_tuple(
            self.decisions, "decisions", PlacementDecision, MAX_NODES, output=True
        )
        if not isinstance(self.memory_used_mb, Mapping):
            raise PlanningError("memory_used_mb must be a mapping")
        if len(self.memory_used_mb) > MAX_DEVICES:
            raise PlanningError(f"memory_used_mb exceeds the {MAX_DEVICES}-entry limit")
        memory: dict[str, float] = {}
        for raw_device, raw_value in self.memory_used_mb.items():
            device = _text(raw_device, "memory_used_mb key", output=True)
            if device in memory:
                raise PlanningError(f"duplicate memory_used_mb key {device!r}")
            memory[device] = _number(raw_value, f"memory used for {device!r}", output=True)

        _unique_output((item.node for item in schedule), "scheduled node")
        _unique_output((item.node for item in decisions), "decision node")
        if {item.node for item in schedule} != {item.node for item in decisions}:
            raise PlanningError("schedule and decisions must name the same nodes")
        decisions_by_node = {item.node: item for item in decisions}
        for item in schedule:
            decision = decisions_by_node[item.node]
            if decision.selected_device != item.device:
                raise PlanningError(
                    f"decision for node {item.node!r} does not match its scheduled device"
                )
        computed: dict[str, float] = defaultdict(float)
        for item in schedule:
            computed[item.device] += item.memory_mb
            if not math.isfinite(computed[item.device]):
                raise PlanningError(f"memory total overflowed for device {item.device!r}")
        for device, used in computed.items():
            if device not in memory or not math.isclose(
                memory[device], used, rel_tol=1e-12, abs_tol=1e-9
            ):
                raise PlanningError(f"memory_used_mb is inconsistent for device {device!r}")
        for device, used in memory.items():
            if device not in computed and used != 0:
                raise PlanningError(f"memory_used_mb has unexplained usage for device {device!r}")

        object.__setattr__(self, "schedule", schedule)
        object.__setattr__(self, "decisions", decisions)
        object.__setattr__(self, "memory_used_mb", MappingProxyType(memory))

    @property
    def makespan_ms(self) -> float:
        return max((item.finish_ms for item in self.schedule), default=0.0)

    @property
    def placements(self) -> dict[str, str]:
        return {item.node: item.device for item in self.schedule}

    @property
    def critical_node(self) -> str | None:
        if not self.schedule:
            return None
        return max(self.schedule, key=lambda item: (item.finish_ms, item.node)).node

    def to_dict(self) -> dict[str, Any]:
        """Return a detached, stable JSON-serializable representation."""

        return {
            "graph_name": self.graph_name,
            "algorithm": self.algorithm,
            "makespan_ms": round(self.makespan_ms, 6),
            "critical_node": self.critical_node,
            "placements": self.placements,
            "memory_used_mb": {key: round(value, 6) for key, value in self.memory_used_mb.items()},
            "schedule": [
                {
                    "node": item.node,
                    "device": item.device,
                    "start_ms": item.start_ms,
                    "finish_ms": item.finish_ms,
                    "compute_ms": item.compute_ms,
                    "incoming_transfer_ms": item.incoming_transfer_ms,
                    "memory_mb": item.memory_mb,
                }
                for item in self.schedule
            ],
            "decisions": [
                {
                    "node": decision.node,
                    "selected_device": decision.selected_device,
                    "candidates": [
                        {
                            "device": candidate.device,
                            "feasible": candidate.feasible,
                            "reason": candidate.reason,
                            "start_ms": candidate.start_ms,
                            "finish_ms": candidate.finish_ms,
                            "transfer_ms": candidate.transfer_ms,
                        }
                        for candidate in decision.candidates
                    ],
                }
                for decision in self.decisions
            ],
        }


def _unique(values: Iterable[Any], label: str) -> None:
    seen: set[Any] = set()
    for value in values:
        if value in seen:
            raise ValidationError(f"duplicate {label}(s): {value!r}")
        seen.add(value)


def _unique_output(values: Iterable[Any], label: str) -> None:
    seen: set[Any] = set()
    for value in values:
        if value in seen:
            raise PlanningError(f"duplicate {label}: {value!r}")
        seen.add(value)
