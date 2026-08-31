"""Immutable domain models used by validation, planning, and reporting."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from typing import Any

from graph_sail.errors import PlanningError


@dataclass(frozen=True, slots=True)
class DeviceSpec:
    """A compute device with a persistent model-memory budget."""

    name: str
    memory_mb: float
    kinds: frozenset[str] = field(default_factory=frozenset)

    def supports(self, kind: str) -> bool:
        """Return whether this device accepts a node kind.

        An empty kind set intentionally means "no kind restriction".
        """

        return not self.kinds or kind in self.kinds


@dataclass(frozen=True, slots=True)
class NodeSpec:
    """A component in the logical multimodal execution graph."""

    id: str
    kind: str
    memory_mb: float
    latency_ms: dict[str, float]
    allowed_devices: frozenset[str] = field(default_factory=frozenset)
    pinned_device: str | None = None

    def can_run_on(self, device: DeviceSpec) -> bool:
        """Return whether static constraints permit this placement."""

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


@dataclass(frozen=True, slots=True)
class LinkSpec:
    """A directed device link used for cross-device transfer estimates."""

    source: str
    target: str
    bandwidth_mb_s: float
    latency_ms: float = 0.0

    def transfer_ms(self, payload_mb: float) -> float:
        """Estimate one transfer, including fixed and payload-dependent cost."""

        duration = self.latency_ms + (payload_mb / self.bandwidth_mb_s * 1000.0)
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

    @property
    def node_map(self) -> dict[str, NodeSpec]:
        return {node.id: node for node in self.nodes}

    @property
    def device_map(self) -> dict[str, DeviceSpec]:
        return {device.name: device for device in self.devices}

    @property
    def edge_map(self) -> dict[tuple[str, str], EdgeSpec]:
        return {(edge.source, edge.target): edge for edge in self.edges}

    @property
    def link_map(self) -> dict[tuple[str, str], LinkSpec]:
        return {(link.source, link.target): link for link in self.links}

    def transfer_ms(
        self,
        source_device: str,
        target_device: str,
        payload_mb: float,
        *,
        _link_index: Mapping[tuple[str, str], LinkSpec] | None = None,
    ) -> float:
        """Return zero for local edges and a configured/default estimate otherwise."""

        if source_device == target_device:
            return 0.0
        links = self.link_map if _link_index is None else _link_index
        link = links.get((source_device, target_device))
        if link is not None:
            return link.transfer_ms(payload_mb)
        duration = self.default_link_latency_ms + payload_mb / self.default_bandwidth_mb_s * 1000.0
        if not math.isfinite(duration):
            raise PlanningError(
                f"transfer estimate overflowed for devices {source_device!r} -> {target_device!r}"
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


@dataclass(frozen=True, slots=True)
class PlacementDecision:
    """The selected device and alternatives considered for a node."""

    node: str
    selected_device: str
    candidates: tuple[CandidateTrace, ...]


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


@dataclass(frozen=True, slots=True)
class PlanResult:
    """A complete placement, schedule, resource summary, and decision trace."""

    graph_name: str
    algorithm: str
    schedule: tuple[ScheduledNode, ...]
    memory_used_mb: dict[str, float]
    decisions: tuple[PlacementDecision, ...]

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
        """Return a stable JSON-serializable representation."""

        return {
            "graph_name": self.graph_name,
            "algorithm": self.algorithm,
            "makespan_ms": round(self.makespan_ms, 6),
            "critical_node": self.critical_node,
            "placements": self.placements,
            "memory_used_mb": {key: round(value, 6) for key, value in self.memory_used_mb.items()},
            "schedule": [asdict(item) for item in self.schedule],
            "decisions": [
                {
                    "node": decision.node,
                    "selected_device": decision.selected_device,
                    "candidates": [asdict(candidate) for candidate in decision.candidates],
                }
                for decision in self.decisions
            ],
        }
