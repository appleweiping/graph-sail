"""Measured-profile ingestion and deterministic graph calibration.

The adapter deliberately accepts a small, tool-neutral JSONL contract.  A
profiler or deployment harness only needs to export one completed observation
per line; Graph Sail remains independent of that profiler's runtime.
"""

from __future__ import annotations

import json
import math
import statistics
import tempfile
from collections import defaultdict
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from graph_sail.errors import OutputError, ValidationError
from graph_sail.limits import MAX_INPUT_BYTES, MAX_OBSERVATIONS, MAX_TEXT_LENGTH
from graph_sail.models import GraphSpec, NodeSpec

_FIELDS = {"node", "device", "latency_ms", "run_id"}


@dataclass(frozen=True, slots=True)
class LatencyObservation:
    """One measured component latency on one named Graph Sail device."""

    node: str
    device: str
    latency_ms: float
    run_id: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "node", _text(self.node, "observation.node"))
        object.__setattr__(self, "device", _text(self.device, "observation.device"))
        object.__setattr__(self, "latency_ms", _positive(self.latency_ms, "observation.latency_ms"))
        object.__setattr__(
            self, "run_id", _text(self.run_id, "observation.run_id", allow_empty=True)
        )


@dataclass(frozen=True, slots=True)
class CalibrationCell:
    """Aggregate used to replace one node/device latency estimate."""

    node: str
    device: str
    samples: int
    median_ms: float
    minimum_ms: float
    maximum_ms: float
    run_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if isinstance(self.run_ids, (str, bytes, bytearray)):
            raise ValueError("cell.run_ids must be an iterable of strings")
        try:
            run_ids = _bounded_tuple(self.run_ids, MAX_OBSERVATIONS, "cell.run_ids")
        except TypeError as exc:
            raise ValueError("cell.run_ids must be an iterable") from exc
        object.__setattr__(self, "run_ids", run_ids)
        _validate_cell(self)

    def to_dict(self) -> dict[str, Any]:
        _validate_cell(self)
        return {
            "node": self.node,
            "device": self.device,
            "samples": self.samples,
            "median_ms": round(self.median_ms, 9),
            "minimum_ms": round(self.minimum_ms, 9),
            "maximum_ms": round(self.maximum_ms, 9),
            "run_ids": list(self.run_ids),
        }


@dataclass(frozen=True, slots=True)
class CalibrationResult:
    """Calibrated graph plus an auditable summary of applied observations."""

    graph: GraphSpec
    cells: tuple[CalibrationCell, ...]
    ignored_cells: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        try:
            cells = _bounded_tuple(self.cells, MAX_OBSERVATIONS, "calibration cells")
            ignored_items = _bounded_tuple(self.ignored_cells, MAX_OBSERVATIONS, "ignored_cells")
        except TypeError as exc:
            raise ValueError("calibration collections must be iterable") from exc
        ignored: list[tuple[str, str]] = []
        for item in ignored_items:
            if isinstance(item, (str, bytes, bytearray)):
                raise ValueError("ignored_cells must contain node/device pairs")
            try:
                pair = tuple(item)
            except TypeError as exc:
                raise ValueError("ignored_cells must contain node/device pairs") from exc
            ignored.append(pair)  # type: ignore[arg-type]
        object.__setattr__(self, "cells", cells)
        object.__setattr__(self, "ignored_cells", tuple(ignored))
        _validate_result(self)

    def report_dict(self) -> dict[str, Any]:
        _validate_result(self)
        return {
            "schema_version": 1,
            "estimator": "median",
            "applied": [cell.to_dict() for cell in self.cells],
            "ignored": [{"node": node, "device": device} for node, device in self.ignored_cells],
        }


def load_observations(path: str | Path) -> tuple[LatencyObservation, ...]:
    """Load strict JSONL latency observations while preserving line diagnostics."""

    source = Path(path)
    try:
        lines = _read_text_limited(source).splitlines()
    except (OSError, UnicodeError) as exc:
        raise ValidationError(f"cannot read observations {source}: {exc}") from exc
    observations: list[LatencyObservation] = []
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        if len(observations) == MAX_OBSERVATIONS:
            raise ValidationError(f"observation file exceeds the {MAX_OBSERVATIONS}-record limit")
        try:
            payload = json.loads(
                line,
                object_pairs_hook=_unique_object,
                parse_constant=_reject_constant,
            )
        except json.JSONDecodeError as exc:
            raise ValidationError(
                f"invalid observation JSON at line {line_number}, column {exc.colno}: {exc.msg}"
            ) from exc
        except ValueError as exc:
            raise ValidationError(f"invalid observation JSON at line {line_number}: {exc}") from exc
        observations.append(_observation_from_dict(payload, line_number))
    if not observations:
        raise ValidationError("observation file must contain at least one observation")
    return tuple(observations)


def calibrate_graph(
    graph: GraphSpec,
    observations: tuple[LatencyObservation, ...],
    *,
    strict: bool = True,
) -> CalibrationResult:
    """Replace observed node/device latency cells with their sample median.

    In strict mode every observation must reference an existing latency cell.
    Relaxed mode records unknown cells in ``ignored_cells`` without changing the
    graph, which is useful when a shared profiler export covers several graphs.
    """

    if not isinstance(graph, GraphSpec):
        raise ValidationError("graph must be a GraphSpec")
    graph.validate()
    try:
        observations = _bounded_tuple(observations, MAX_OBSERVATIONS, "observations")
    except TypeError as exc:
        raise ValidationError("observations must be iterable") from exc
    groups: dict[tuple[str, str], list[float]] = defaultdict(list)
    run_ids: dict[tuple[str, str], set[str]] = defaultdict(set)
    ignored: set[tuple[str, str]] = set()
    nodes = graph.node_map
    for observation in observations:
        _validate_observation(observation)
        key = (observation.node, observation.device)
        node = nodes.get(observation.node)
        if node is None or observation.device not in node.latency_ms:
            if strict:
                raise ValidationError(
                    f"observation references unknown latency cell "
                    f"{observation.node!r}/{observation.device!r}"
                )
            ignored.add(key)
            continue
        groups[key].append(observation.latency_ms)
        if observation.run_id:
            run_ids[key].add(observation.run_id)
    if not groups:
        raise ValidationError("no observations matched graph latency cells")

    cells: list[CalibrationCell] = []
    replacements: dict[str, dict[str, float]] = {}
    for (node_id, device), values in sorted(groups.items()):
        median = float(statistics.median(values))
        _positive(median, f"calibrated median for {node_id!r}/{device!r}")
        cells.append(
            CalibrationCell(
                node=node_id,
                device=device,
                samples=len(values),
                median_ms=median,
                minimum_ms=min(values),
                maximum_ms=max(values),
                run_ids=tuple(sorted(run_ids[(node_id, device)])),
            )
        )
        replacements.setdefault(node_id, {})[device] = median

    calibrated_nodes: list[NodeSpec] = []
    for node in graph.nodes:
        latencies = dict(node.latency_ms)
        latencies.update(replacements.get(node.id, {}))
        calibrated_nodes.append(replace(node, latency_ms=latencies))
    return CalibrationResult(
        graph=replace(graph, nodes=tuple(calibrated_nodes)),
        cells=tuple(cells),
        ignored_cells=tuple(sorted(ignored)),
    )


def graph_to_dict(graph: GraphSpec) -> dict[str, Any]:
    """Return the canonical public graph-document representation."""

    payload = {
        "name": graph.name,
        "devices": [
            {
                "name": device.name,
                "memory_mb": device.memory_mb,
                **({"kinds": sorted(device.kinds)} if device.kinds else {}),
            }
            for device in graph.devices
        ],
        "nodes": [
            {
                "id": node.id,
                "kind": node.kind,
                "memory_mb": node.memory_mb,
                "latency_ms": dict(sorted(node.latency_ms.items())),
                **(
                    {"allowed_devices": sorted(node.allowed_devices)}
                    if node.allowed_devices
                    else {}
                ),
                **({"pinned_device": node.pinned_device} if node.pinned_device is not None else {}),
            }
            for node in graph.nodes
        ],
        "edges": [
            {
                "source": edge.source,
                "target": edge.target,
                "payload_mb": edge.payload_mb,
                **({"label": edge.label} if edge.label else {}),
            }
            for edge in graph.edges
        ],
        "links": [
            {
                "source": link.source,
                "target": link.target,
                "bandwidth_mb_s": link.bandwidth_mb_s,
                "latency_ms": link.latency_ms,
            }
            for link in graph.links
        ],
        "default_bandwidth_mb_s": graph.default_bandwidth_mb_s,
        "default_link_latency_ms": graph.default_link_latency_ms,
    }
    from graph_sail.io import graph_from_dict

    graph_from_dict(payload)
    return payload


def write_calibration_bundle(result: CalibrationResult, output: str | Path) -> dict[str, Path]:
    """Write a calibrated graph and provenance report as strict JSON."""

    target = Path(output)
    try:
        _validate_result(result)
        graph_path = target / "graph.json"
        report_path = target / "calibration.json"
        target.mkdir(parents=True, exist_ok=True)
        if not target.is_dir():
            raise OSError("target is not a directory")
        _atomic_write(graph_path, _json(graph_to_dict(result.graph)))
        _atomic_write(report_path, _json(result.report_dict()))
    except (OSError, TypeError, ValueError, ValidationError) as exc:
        raise OutputError(f"cannot write calibration bundle to {target}: {exc}") from exc
    return {"graph": graph_path, "calibration": report_path}


def _observation_from_dict(payload: Any, line_number: int) -> LatencyObservation:
    if not isinstance(payload, dict) or not all(isinstance(key, str) for key in payload):
        raise ValidationError(f"observation at line {line_number} must be an object")
    unknown = sorted(set(payload) - _FIELDS)
    if unknown:
        raise ValidationError(
            f"observation at line {line_number} contains unknown field(s): {', '.join(unknown)}"
        )
    node = _text(payload.get("node"), f"line {line_number}.node")
    device = _text(payload.get("device"), f"line {line_number}.device")
    latency = _positive(payload.get("latency_ms"), f"line {line_number}.latency_ms")
    run_id = _text(payload.get("run_id", ""), f"line {line_number}.run_id", allow_empty=True)
    return LatencyObservation(node=node, device=device, latency_ms=latency, run_id=run_id)


def _text(value: Any, path: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{path} must be a string")
    result = value.strip()
    if not result and not allow_empty:
        raise ValidationError(f"{path} must not be empty")
    if len(result) > MAX_TEXT_LENGTH:
        raise ValidationError(f"{path} exceeds the {MAX_TEXT_LENGTH}-character limit")
    try:
        result.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValidationError(f"{path} must contain valid Unicode scalar values") from exc
    if any(ord(character) < 32 or ord(character) == 127 for character in result):
        raise ValidationError(f"{path} must not contain control characters")
    return result


def _positive(value: Any, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{path} must be a number")
    try:
        number = float(value)
    except OverflowError as exc:
        raise ValidationError(f"{path} must be finite") from exc
    if not math.isfinite(number) or number <= 0:
        raise ValidationError(f"{path} must be finite and greater than zero")
    return number


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate object key {key!r}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite number {value!r} is not permitted")


def _json(payload: Any) -> str:
    return json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"


def _validate_observation(value: LatencyObservation) -> None:
    if not isinstance(value, LatencyObservation):
        raise ValidationError("observations must contain LatencyObservation records")
    _text(value.node, "observation.node")
    _text(value.device, "observation.device")
    _positive(value.latency_ms, "observation.latency_ms")
    _text(value.run_id, "observation.run_id", allow_empty=True)


def _validate_cell(value: CalibrationCell) -> None:
    if not isinstance(value, CalibrationCell):
        raise ValueError("cells must contain CalibrationCell records")
    _text(value.node, "cell.node")
    _text(value.device, "cell.device")
    if (
        isinstance(value.samples, bool)
        or not isinstance(value.samples, int)
        or not 1 <= value.samples <= 10_000_000
    ):
        raise ValueError("cell.samples must be an integer from 1 to 10000000")
    for label, number in (
        ("median_ms", value.median_ms),
        ("minimum_ms", value.minimum_ms),
        ("maximum_ms", value.maximum_ms),
    ):
        try:
            _positive(number, f"cell.{label}")
        except ValidationError as exc:
            raise ValueError(str(exc)) from exc
    if not value.minimum_ms <= value.median_ms <= value.maximum_ms:
        raise ValueError("cell median must be within its minimum/maximum range")
    if not isinstance(value.run_ids, tuple):
        raise ValueError("cell.run_ids must be a tuple")
    for run_id in value.run_ids:
        _text(run_id, "cell.run_ids")
    if list(value.run_ids) != sorted(value.run_ids) or len(value.run_ids) != len(
        set(value.run_ids)
    ):
        raise ValueError("cell.run_ids must be unique and sorted")


def _validate_result(value: CalibrationResult) -> None:
    if not isinstance(value, CalibrationResult):
        raise ValueError("result must be a CalibrationResult")
    if not isinstance(value.graph, GraphSpec):
        raise ValueError("graph must be a GraphSpec")
    graph_to_dict(value.graph)
    if not isinstance(value.cells, tuple) or not value.cells:
        raise ValueError("cells must be a non-empty tuple")
    for cell in value.cells:
        _validate_cell(cell)
    keys = [(cell.node, cell.device) for cell in value.cells]
    if keys != sorted(keys) or len(keys) != len(set(keys)):
        raise ValueError("calibration cells must be unique and sorted")
    if not isinstance(value.ignored_cells, tuple):
        raise ValueError("ignored_cells must be a tuple")
    ignored: list[tuple[str, str]] = []
    for item in value.ignored_cells:
        if not isinstance(item, tuple) or len(item) != 2:
            raise ValueError("ignored_cells must contain node/device pairs")
        node = _text(item[0], "ignored node")
        device = _text(item[1], "ignored device")
        ignored.append((node, device))
    if ignored != sorted(ignored) or len(ignored) != len(set(ignored)):
        raise ValueError("ignored_cells must be unique and sorted")


def _atomic_write(target: Path, document: str) -> None:
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", newline="\n", dir=target.parent, delete=False
        ) as handle:
            handle.write(document)
            handle.flush()
            temporary = Path(handle.name)
        temporary.replace(target)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def _read_text_limited(source: Path) -> str:
    with source.open("rb") as handle:
        raw = handle.read(MAX_INPUT_BYTES + 1)
    if len(raw) > MAX_INPUT_BYTES:
        raise ValidationError(f"observation file exceeds the {MAX_INPUT_BYTES}-byte limit")
    return raw.decode("utf-8")


def _bounded_tuple(value: Any, maximum: int, label: str) -> tuple[Any, ...]:
    if isinstance(value, (str, bytes, bytearray)):
        raise TypeError(f"{label} must be iterable records")
    iterator = iter(value)
    result: list[Any] = []
    for item in iterator:
        if len(result) == maximum:
            raise ValidationError(f"{label} exceeds the {maximum}-record limit")
        result.append(item)
    return tuple(result)
