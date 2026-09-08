"""Bounded logical-resource admission; this does not isolate physical hardware.

Amounts use exact 1/10,000 units. The private pool has one controller owner;
callers must settle actual workers before releasing their leases. Its completed
usage is local accounting, not an authenticated execution certificate.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from types import MappingProxyType

from graph_sail.errors import ValidationError
from graph_sail.limits import MAX_NODES, MAX_TEXT_LENGTH
from graph_sail.models import _text

UNITS_PER_RESOURCE = 10_000
_MAX_AMOUNT = 1_000_000
_MAX_UNITS = _MAX_AMOUNT * UNITS_PER_RESOURCE
_MAX_RESOURCES = 64
_MAX_BINDINGS = 100_000
_MAX_ATTEMPTS = 21
_MAX_RESERVATIONS = MAX_NODES * _MAX_ATTEMPTS
_EMPTY: Mapping[str, int] = MappingProxyType({})


def _name(value: object, label: str) -> str:
    if (
        type(value) is not str
        or not 1 <= len(value) <= MAX_TEXT_LENGTH
        or _text(value, label) != value
    ):
        raise ValidationError(f"{label} must be canonical bounded Unicode text")
    return value


def _integer(value: object, label: str, maximum: int, minimum: int = 0) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValidationError(f"{label} must be an integer in [{minimum}, {maximum}]")
    return value


def _entries(value: object, label: str, limit: int) -> Iterator[tuple[str, object]]:
    if not isinstance(value, Mapping) or len(value) > limit:
        raise ValidationError(f"{label} must be a mapping with at most {limit} entries")
    seen: set[str] = set()
    for index, item in enumerate(value.items()):
        if index == limit:
            raise ValidationError(f"{label} exceeds its entry limit")
        if type(item) is not tuple or len(item) != 2:
            raise ValidationError(f"{label} must contain key/value pairs")
        name = _name(item[0], f"{label} key")
        if name in seen:
            raise ValidationError(f"{label} must not repeat a key")
        seen.add(name)
        yield name, item[1]


def _units(value: object) -> int:
    if (
        not isinstance(value, (int, float))
        or type(value) not in (int, float)
        or not 0 <= value <= _MAX_AMOUNT
    ):
        raise ValidationError("logical amount must be a finite built-in number in [0, 1000000]")
    if type(value) is int:
        return value * UNITS_PER_RESOURCE
    if value == 0:
        return 0
    # Constructing from text and inspecting the tuple do not use Decimal context
    # precision. All arithmetic below is bounded integer arithmetic, not Decimal.
    decimal = Decimal(str(value)).as_tuple()
    if not isinstance(decimal.exponent, int):
        raise ValidationError("logical amount must be finite")
    coefficient = 0
    for digit in decimal.digits:
        coefficient = coefficient * 10 + digit
    power = decimal.exponent + 4
    if power >= 0:
        for _ in range(power):
            coefficient *= 10
        return coefficient
    # Reject tiny nonzero values before constructing an unnecessary large power.
    if -power > len(decimal.digits):
        raise ValidationError("logical amount must be quantized to 0.0001")
    divisor = 1
    for _ in range(-power):
        divisor *= 10
    result, remainder = divmod(coefficient, divisor)
    if remainder:
        raise ValidationError("logical amount must be quantized to 0.0001")
    return result


def _amount(units: int) -> int | float:
    return (
        units // UNITS_PER_RESOURCE
        if units % UNITS_PER_RESOURCE == 0
        else units / UNITS_PER_RESOURCE
    )


def _wire(
    capacities: Mapping[str, int], requests: Mapping[str, Mapping[str, int]]
) -> dict[str, object]:
    return {
        "kind": "graph-sail-logical-resources",
        "schema_version": 1,
        "units_per_resource": UNITS_PER_RESOURCE,
        "capacity_units": dict(sorted(capacities.items())),
        "request_units": {
            node: dict(sorted(values.items())) for node, values in sorted(requests.items())
        },
    }


@dataclass(frozen=True, slots=True)
class LogicalResources:
    """Immutable local/global pool policy, supplied as trusted Python mappings.

    Empty pools and zero requests are valid. An undeclared resource is not valid,
    even when requested with zero quantity. Missing node requests mean zero.
    Mapping protocol methods are caller code, not a sandboxed document parser.
    """

    capacities: Mapping[str, int | float]
    requests: Mapping[str, Mapping[str, int | float]] = field(default_factory=dict)
    _capacity_units: Mapping[str, int] = field(init=False, repr=False)
    _request_units: Mapping[str, Mapping[str, int]] = field(init=False, repr=False)
    _digest: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        capacity_units: dict[str, int] = {}
        capacities: dict[str, int | float] = {}
        for name, value in _entries(self.capacities, "capacities", _MAX_RESOURCES):
            capacity_units[name] = _units(value)
            capacities[name] = _amount(capacity_units[name])
        names = {name: name for name in capacities}
        request_units: dict[str, Mapping[str, int]] = {}
        requests: dict[str, Mapping[str, int | float]] = {}
        bindings = 0
        for node, values in _entries(self.requests, "requests", MAX_NODES):
            node_units: dict[str, int] = {}
            node_values: dict[str, int | float] = {}
            for resource, value in _entries(values, "node requests", _MAX_RESOURCES):
                if bindings == _MAX_BINDINGS:
                    raise ValidationError("logical policy exceeds total request binding limit")
                bindings += 1
                if resource not in names:
                    raise ValidationError("node request names an undeclared resource")
                resource = names[resource]
                units = _units(value)
                if units > capacity_units[resource]:
                    raise ValidationError("node request exceeds the whole logical pool capacity")
                node_units[resource] = units
                node_values[resource] = _amount(units)
            request_units[node] = MappingProxyType(node_units)
            requests[node] = MappingProxyType(node_values)
        # Stream canonical JSON chunks into the hash, never concatenate the full
        # potentially large textual policy. Names/entries bound cumulative work.
        digest = hashlib.sha256()
        encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        for chunk in encoder.iterencode(_wire(capacity_units, request_units)):
            digest.update(chunk.encode("utf-8"))
        object.__setattr__(self, "capacities", MappingProxyType(capacities))
        object.__setattr__(self, "requests", MappingProxyType(requests))
        object.__setattr__(self, "_capacity_units", MappingProxyType(capacity_units))
        object.__setattr__(self, "_request_units", MappingProxyType(request_units))
        object.__setattr__(self, "_digest", digest.hexdigest())

    @property
    def capacity_units(self) -> Mapping[str, int]:
        return self._capacity_units

    @property
    def request_units(self) -> Mapping[str, Mapping[str, int]]:
        return self._request_units

    @property
    def digest(self) -> str:
        return self._digest

    def to_dict(self) -> dict[str, object]:
        """Return a fresh integer-only configuration wire, not an import format."""
        return _wire(self.capacity_units, self.request_units)

    def validate_nodes(self, admitted: tuple[str, ...]) -> None:
        if type(admitted) is not tuple or len(admitted) > MAX_NODES:
            raise ValidationError("admitted resource nodes must be a bounded tuple")
        names = {_name(node, "admitted node") for node in admitted}
        if len(names) != len(admitted):
            raise ValidationError("admitted resource nodes must be unique")
        if any(node not in names for node in self.request_units):
            raise ValidationError("logical requests contain an unknown graph node")


@dataclass(frozen=True, slots=True)
class ResourceUsage:
    """Completed output-only accounting; no active leases or authenticated history."""

    policy_digest: str
    capacity_units: Mapping[str, int]
    peak_units: Mapping[str, int]
    reservations: int
    releases: int

    def __post_init__(self) -> None:
        if (
            type(self.policy_digest) is not str
            or len(self.policy_digest) != 64
            or any(character not in "0123456789abcdef" for character in self.policy_digest)
        ):
            raise ValidationError("resource usage requires a lowercase SHA-256 policy digest")
        capacities = {
            name: _integer(value, "capacity units", _MAX_UNITS)
            for name, value in _entries(self.capacity_units, "usage capacities", _MAX_RESOURCES)
        }
        peaks = {
            name: _integer(value, "peak units", _MAX_UNITS)
            for name, value in _entries(self.peak_units, "usage peaks", _MAX_RESOURCES)
        }
        if capacities.keys() != peaks.keys() or any(
            peaks[name] > value for name, value in capacities.items()
        ):
            raise ValidationError("resource usage peaks must match and fit declared capacities")
        _integer(self.reservations, "resource reservations", _MAX_RESERVATIONS)
        _integer(self.releases, "resource releases", _MAX_RESERVATIONS)
        if self.releases != self.reservations:
            raise ValidationError("completed resource usage requires every lease to be released")
        if self.reservations == 0 and any(peaks.values()):
            raise ValidationError("resource usage cannot peak without a reservation")
        object.__setattr__(self, "capacity_units", MappingProxyType(capacities))
        object.__setattr__(self, "peak_units", MappingProxyType(peaks))

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": "graph-sail-logical-resource-usage",
            "schema_version": 1,
            "units_per_resource": UNITS_PER_RESOURCE,
            "policy_digest": self.policy_digest,
            "capacity_units": dict(sorted(self.capacity_units.items())),
            "peak_units": dict(sorted(self.peak_units.items())),
            "reservations": self.reservations,
            "releases": self.releases,
        }


@dataclass(frozen=True, slots=True)
class _PoolState:
    used: Mapping[str, int]
    peaks: Mapping[str, int]
    active: Mapping[str, int]
    last_attempt: Mapping[str, int]
    reservations: int
    releases: int


class _ResourcePool:
    """Single-controller staged accounting, never a worker lifecycle manager.

    Each node starts at attempt 1 and advances exactly one after release, up to
    the execution API's 21 attempts. Zero-demand attempts also acquire a lease.
    release_all is only safe after the owning executor has genuinely settled.
    """

    __slots__ = ("_policy", "_state")

    def __init__(self, policy: LogicalResources) -> None:
        if type(policy) is not LogicalResources:
            raise ValidationError("resource pool requires LogicalResources")
        self._policy = LogicalResources(policy.capacities, policy.requests)
        zero = MappingProxyType(dict.fromkeys(self._policy.capacity_units, 0))
        self._state = _PoolState(zero, zero, _EMPTY, _EMPTY, 0, 0)

    def fits(self, node: str) -> bool:
        _name(node, "resource node")
        state = self._state
        return node not in state.active and all(
            amount <= self._policy.capacity_units[name] - state.used[name]
            for name, amount in self._policy.request_units.get(node, _EMPTY).items()
        )

    def reserve(self, node: str, attempt: int) -> None:
        _name(node, "resource node")
        _integer(attempt, "resource attempt", _MAX_ATTEMPTS, 1)
        before = self._state
        if node in before.active or attempt != before.last_attempt.get(node, 0) + 1:
            raise ValidationError(
                "resource attempts must be sequential and never overlap or repeat"
            )
        if node not in before.last_attempt and len(before.last_attempt) == MAX_NODES:
            raise ValidationError("resource pool exceeds its node history limit")
        if not self.fits(node):
            raise ValidationError("logical resources are not currently available")
        reservations = _integer(before.reservations + 1, "resource reservations", _MAX_RESERVATIONS)
        used, peaks = dict(before.used), dict(before.peaks)
        for name, amount in self._policy.request_units.get(node, _EMPTY).items():
            used[name] += amount
            peaks[name] = max(peaks[name], used[name])
        active, last_attempt = dict(before.active), dict(before.last_attempt)
        active[node] = last_attempt[node] = attempt
        after = _PoolState(
            MappingProxyType(used),
            MappingProxyType(peaks),
            MappingProxyType(active),
            MappingProxyType(last_attempt),
            reservations,
            before.releases,
        )
        self._state = after

    def release(self, node: str, attempt: int) -> None:
        _name(node, "resource node")
        _integer(attempt, "resource attempt", _MAX_ATTEMPTS, 1)
        before = self._state
        if before.active.get(node) != attempt:
            raise ValidationError("resource release must name the active node and attempt")
        used = dict(before.used)
        for name, amount in self._policy.request_units.get(node, _EMPTY).items():
            used[name] -= amount
        active = dict(before.active)
        del active[node]
        after = _PoolState(
            MappingProxyType(used),
            before.peaks,
            MappingProxyType(active),
            before.last_attempt,
            before.reservations,
            before.releases + 1,
        )
        self._state = after

    def release_all(self) -> None:
        before = self._state
        if not before.active:
            return
        after = _PoolState(
            MappingProxyType(dict.fromkeys(before.used, 0)),
            before.peaks,
            _EMPTY,
            before.last_attempt,
            before.reservations,
            before.reservations,
        )
        self._state = after

    def snapshot(self) -> ResourceUsage:
        state = self._state
        if state.active:
            raise ValidationError("resource usage cannot finalize while leases remain active")
        return ResourceUsage(
            self._policy.digest,
            self._policy.capacity_units,
            state.peaks,
            state.reservations,
            state.releases,
        )


__all__ = ["UNITS_PER_RESOURCE", "LogicalResources", "ResourceUsage"]
