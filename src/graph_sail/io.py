"""Strict JSON parsing and graph-contract validation."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from graph_sail.errors import ValidationError
from graph_sail.graph import topological_order
from graph_sail.models import DeviceSpec, EdgeSpec, GraphSpec, LinkSpec, NodeSpec

_TOP_LEVEL_FIELDS = {
    "name",
    "devices",
    "nodes",
    "edges",
    "links",
    "default_bandwidth_mb_s",
    "default_link_latency_ms",
}


def load_graph(path: str | Path) -> GraphSpec:
    """Load and validate a graph JSON document."""

    source = Path(path)
    try:
        payload = json.loads(
            source.read_text(encoding="utf-8"),
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
    except (OSError, UnicodeError) as exc:
        raise ValidationError(f"cannot read graph file {source}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValidationError(
            f"invalid JSON in {source} at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc
    return graph_from_dict(payload)


def graph_from_dict(payload: Any) -> GraphSpec:
    """Parse a Python object into a validated immutable graph."""

    root = _mapping(payload, "$")
    unknown = sorted(set(root) - _TOP_LEVEL_FIELDS)
    if unknown:
        raise ValidationError(f"$ contains unknown field(s): {', '.join(unknown)}")

    name = _text(root.get("name", "graph"), "$.name")
    devices = tuple(
        _parse_device(item, f"$.devices[{index}]")
        for index, item in enumerate(_sequence(root.get("devices"), "$.devices"))
    )
    nodes = tuple(
        _parse_node(item, f"$.nodes[{index}]")
        for index, item in enumerate(_sequence(root.get("nodes"), "$.nodes"))
    )
    edges = tuple(
        _parse_edge(item, f"$.edges[{index}]")
        for index, item in enumerate(_sequence(root.get("edges", []), "$.edges"))
    )
    links = tuple(
        _parse_link(item, f"$.links[{index}]")
        for index, item in enumerate(_sequence(root.get("links", []), "$.links"))
    )
    default_bandwidth = _positive_number(
        root.get("default_bandwidth_mb_s", 1_000.0), "$.default_bandwidth_mb_s"
    )
    default_latency = _nonnegative_number(
        root.get("default_link_latency_ms", 0.0), "$.default_link_latency_ms"
    )

    graph = GraphSpec(
        name=name,
        devices=devices,
        nodes=nodes,
        edges=edges,
        links=links,
        default_bandwidth_mb_s=default_bandwidth,
        default_link_latency_ms=default_latency,
    )
    _validate_relations(graph)
    topological_order(graph)
    return graph


def _parse_device(payload: Any, path: str) -> DeviceSpec:
    item = _mapping(payload, path)
    _reject_unknown(item, {"name", "memory_mb", "kinds"}, path)
    return DeviceSpec(
        name=_text(item.get("name"), f"{path}.name"),
        memory_mb=_positive_number(item.get("memory_mb"), f"{path}.memory_mb"),
        kinds=frozenset(_string_list(item.get("kinds", []), f"{path}.kinds")),
    )


def _parse_node(payload: Any, path: str) -> NodeSpec:
    item = _mapping(payload, path)
    _reject_unknown(
        item,
        {"id", "kind", "memory_mb", "latency_ms", "allowed_devices", "pinned_device"},
        path,
    )
    raw_latencies = _mapping(item.get("latency_ms"), f"{path}.latency_ms")
    if not raw_latencies:
        raise ValidationError(f"{path}.latency_ms must contain at least one device estimate")
    latency_items = [
        (
            _text(device, f"{path}.latency_ms key"),
            _positive_number(value, f"{path}.latency_ms.{device}"),
        )
        for device, value in raw_latencies.items()
    ]
    _require_unique((device for device, _ in latency_items), f"{path}.latency_ms device")
    latencies = dict(latency_items)
    pinned = item.get("pinned_device")
    if pinned is not None:
        pinned = _text(pinned, f"{path}.pinned_device")
    return NodeSpec(
        id=_text(item.get("id"), f"{path}.id"),
        kind=_text(item.get("kind"), f"{path}.kind"),
        memory_mb=_nonnegative_number(item.get("memory_mb", 0.0), f"{path}.memory_mb"),
        latency_ms=latencies,
        allowed_devices=frozenset(
            _string_list(item.get("allowed_devices", []), f"{path}.allowed_devices")
        ),
        pinned_device=pinned,
    )


def _parse_edge(payload: Any, path: str) -> EdgeSpec:
    item = _mapping(payload, path)
    _reject_unknown(item, {"source", "target", "payload_mb", "label"}, path)
    return EdgeSpec(
        source=_text(item.get("source"), f"{path}.source"),
        target=_text(item.get("target"), f"{path}.target"),
        payload_mb=_nonnegative_number(item.get("payload_mb", 0.0), f"{path}.payload_mb"),
        label=_text(item.get("label", ""), f"{path}.label", allow_empty=True),
    )


def _parse_link(payload: Any, path: str) -> LinkSpec:
    item = _mapping(payload, path)
    _reject_unknown(item, {"source", "target", "bandwidth_mb_s", "latency_ms"}, path)
    return LinkSpec(
        source=_text(item.get("source"), f"{path}.source"),
        target=_text(item.get("target"), f"{path}.target"),
        bandwidth_mb_s=_positive_number(item.get("bandwidth_mb_s"), f"{path}.bandwidth_mb_s"),
        latency_ms=_nonnegative_number(item.get("latency_ms", 0.0), f"{path}.latency_ms"),
    )


def _validate_relations(graph: GraphSpec) -> None:
    if not graph.devices:
        raise ValidationError("$.devices must contain at least one device")
    if not graph.nodes:
        raise ValidationError("$.nodes must contain at least one node")
    _require_unique((device.name for device in graph.devices), "device name")
    _require_unique((node.id for node in graph.nodes), "node id")
    _require_unique(((edge.source, edge.target) for edge in graph.edges), "edge")
    _require_unique(((link.source, link.target) for link in graph.links), "link")

    device_names = set(graph.device_map)
    node_ids = set(graph.node_map)
    for node in graph.nodes:
        unknown_profiles = sorted(set(node.latency_ms) - device_names)
        if unknown_profiles:
            raise ValidationError(
                f"node {node.id!r} has latency estimates for unknown device(s): "
                f"{', '.join(unknown_profiles)}"
            )
        unknown_allowed = sorted(set(node.allowed_devices) - device_names)
        if unknown_allowed:
            raise ValidationError(
                f"node {node.id!r} allows unknown device(s): {', '.join(unknown_allowed)}"
            )
        if node.pinned_device is not None and node.pinned_device not in device_names:
            raise ValidationError(
                f"node {node.id!r} is pinned to unknown device {node.pinned_device!r}"
            )
        if not any(node.can_run_on(device) for device in graph.devices):
            raise ValidationError(f"node {node.id!r} has no statically compatible device")

    for edge in graph.edges:
        if edge.source not in node_ids:
            raise ValidationError(f"edge source {edge.source!r} does not name a node")
        if edge.target not in node_ids:
            raise ValidationError(f"edge target {edge.target!r} does not name a node")
        if edge.source == edge.target:
            raise ValidationError(f"node {edge.source!r} cannot have a self edge")

    for link in graph.links:
        if link.source not in device_names or link.target not in device_names:
            raise ValidationError(
                f"link {link.source!r} -> {link.target!r} references an unknown device"
            )
        if link.source == link.target:
            raise ValidationError(f"device link {link.source!r} cannot target itself")


def _mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationError(f"{path} must be an object")
    if not all(isinstance(key, str) for key in value):
        raise ValidationError(f"{path} object keys must be strings")
    return value


def _sequence(value: Any, path: str) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValidationError(f"{path} must be an array")
    return value


def _text(value: Any, path: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{path} must be a string")
    text = value.strip()
    if not text and not allow_empty:
        raise ValidationError(f"{path} must not be empty")
    try:
        text.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValidationError(f"{path} must contain valid Unicode scalar values") from exc
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise ValidationError(f"{path} must not contain control characters")
    return text


def _number(value: Any, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{path} must be a number")
    try:
        number = float(value)
    except OverflowError as exc:
        raise ValidationError(f"{path} must be finite") from exc
    if not math.isfinite(number):
        raise ValidationError(f"{path} must be finite")
    return number


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Build one JSON object while rejecting ambiguous duplicate names."""

    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValidationError(f"JSON object contains duplicate field {key!r}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> Any:
    """Reject the non-standard NaN and Infinity constants accepted by ``json``."""

    raise ValidationError(f"JSON contains non-standard numeric constant {value!r}")


def _positive_number(value: Any, path: str) -> float:
    number = _number(value, path)
    if number <= 0:
        raise ValidationError(f"{path} must be greater than zero")
    return number


def _nonnegative_number(value: Any, path: str) -> float:
    number = _number(value, path)
    if number < 0:
        raise ValidationError(f"{path} must be zero or greater")
    return number


def _string_list(value: Any, path: str) -> tuple[str, ...]:
    sequence = _sequence(value, path)
    values = tuple(_text(item, f"{path}[{index}]") for index, item in enumerate(sequence))
    _require_unique(values, path)
    return values


def _reject_unknown(item: Mapping[str, Any], allowed: set[str], path: str) -> None:
    unknown = sorted(set(item) - allowed)
    if unknown:
        raise ValidationError(f"{path} contains unknown field(s): {', '.join(unknown)}")


def _require_unique(values: Any, label: str) -> None:
    seen: set[Any] = set()
    duplicates: list[Any] = []
    for value in values:
        if value in seen and value not in duplicates:
            duplicates.append(value)
        seen.add(value)
    if duplicates:
        rendered = ", ".join(repr(value) for value in duplicates)
        raise ValidationError(f"duplicate {label}(s): {rendered}")
