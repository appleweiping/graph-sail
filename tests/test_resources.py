from __future__ import annotations

import hashlib
import json
import random
from collections.abc import Mapping
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal, Inexact, Rounded, localcontext
from fractions import Fraction

import pytest

import graph_sail.resources as module
from graph_sail.errors import ValidationError
from graph_sail.resources import LogicalResources, ResourceUsage, _ResourcePool


@pytest.mark.parametrize(
    "value,expected",
    [
        (0, 0),
        (-0.0, 0),
        (0.0001, 1),
        (0.1, 1000),
        (1.25, 12500),
        (999999.9999, 9999999999),
        (1000000, 10000000000),
    ],
)
def test_quanta_are_exact_and_independent_of_decimal_context(value, expected):
    with localcontext() as context:
        context.prec = 1
        context.traps[Inexact] = True
        context.traps[Rounded] = True
        policy = LogicalResources({"cpu": value}, {"task": {"cpu": value}})
    assert policy.capacity_units == {"cpu": expected}
    assert policy.request_units == {"task": {"cpu": expected}}


@pytest.mark.parametrize(
    "value",
    [
        True,
        False,
        -1,
        -0.0001,
        1000001,
        10**1000,
        float("nan"),
        float("inf"),
        -float("inf"),
        0.1 + 0.2,
        0.00001,
        5e-324,
        Decimal("0.1"),
        "0.1",
        None,
        1 + 0j,
    ],
)
def test_invalid_or_nonquantized_amounts_reject_without_rounding(value):
    with pytest.raises(ValidationError):
        LogicalResources({"cpu": value})


def test_multi_resource_lease_and_retry_are_exactly_once():
    policy = LogicalResources(
        {"cpu": 1, "gpu": 0.5},
        {"a": {"cpu": 0.75, "gpu": 0.5}, "b": {"cpu": 0.25, "gpu": 0.5}},
    )
    pool = _ResourcePool(policy)
    pool.reserve("a", 1)
    assert not pool.fits("b")
    with pytest.raises(ValidationError):
        pool.reserve("b", 1)
    with pytest.raises(ValidationError):
        pool.snapshot()
    pool.release("a", 1)
    with pytest.raises(ValidationError):
        pool.release("a", 1)
    with pytest.raises(ValidationError):
        pool.reserve("a", 1)
    pool.reserve("a", 2)
    pool.release("a", 2)
    pool.reserve("b", 1)
    pool.release_all()
    pool.release_all()
    usage = pool.snapshot()
    assert usage.capacity_units == {"cpu": 10000, "gpu": 5000}
    assert usage.peak_units == {"cpu": 7500, "gpu": 5000}
    assert usage.reservations == usage.releases == 3
    assert usage.policy_digest == policy.digest


def test_policy_and_usage_are_deeply_immutable_snapshots():
    capacities = {"cpu": 1}
    requests = {"a": {"cpu": 0.5}}
    policy = LogicalResources(capacities, requests)
    capacities["cpu"] = 2
    requests["a"]["cpu"] = 3
    assert policy.capacity_units == {"cpu": 10000}
    assert policy.request_units == {"a": {"cpu": 5000}}
    for target in (
        policy.capacities,
        policy.capacity_units,
        policy.requests["a"],
        policy.request_units["a"],
    ):
        with pytest.raises(TypeError):
            target["cpu"] = 9
    with pytest.raises(FrozenInstanceError):
        policy.capacities = {}
    pool = _ResourcePool(policy)
    usage = pool.snapshot()
    with pytest.raises(TypeError):
        usage.peak_units["cpu"] = 1
    with pytest.raises(FrozenInstanceError):
        usage.reservations = 1
    assert isinstance(usage, ResourceUsage)


def test_seeded_quantization_and_human_amount_roundtrip_use_independent_rational_oracle():
    rng = random.Random(71514)
    for _ in range(300):
        expected = rng.randrange(10_000_000_001)
        value = expected / 10000
        oracle = Fraction(str(value)) * 10000
        assert oracle.denominator == 1 and oracle.numerator == expected
        policy = LogicalResources({"cpu": value}, {"a": {"cpu": value}})
        assert policy.capacity_units["cpu"] == expected
        rebuilt = LogicalResources(policy.capacities, policy.requests)
        assert rebuilt.digest == policy.digest
        assert rebuilt.request_units == policy.request_units


def test_integer_wire_and_digest_are_canonical_and_detached_from_returned_dict():
    policy = LogicalResources({"雪": 0.25, "cpu": 2}, {"b": {}, "a": {"雪": 0.125}})
    expected = {
        "kind": "graph-sail-logical-resources",
        "schema_version": 1,
        "units_per_resource": 10000,
        "capacity_units": {"cpu": 20000, "雪": 2500},
        "request_units": {"a": {"雪": 1250}, "b": {}},
    }
    assert policy.to_dict() == expected
    wire = json.dumps(expected, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    assert policy.digest == hashlib.sha256(wire.encode("utf-8")).hexdigest()
    permuted = LogicalResources({"cpu": 2.0, "雪": 0.25}, {"a": {"雪": 0.125}, "b": {}})
    assert permuted.digest == policy.digest
    detached = policy.to_dict()
    detached["capacity_units"]["cpu"] = 9
    detached["request_units"]["a"]["雪"] = 9
    assert policy.to_dict() == expected
    for changed in (
        LogicalResources({"cpu": 2, "雪": 0.5}, policy.requests),
        LogicalResources(policy.capacities, {"a": {"雪": 0.25}, "b": {}}),
        LogicalResources(policy.capacities, {"a": {"雪": 0.125}}),
    ):
        assert changed.digest != policy.digest


@pytest.mark.parametrize(
    "name", ["", " ", " cpu", "cpu ", "a\n", "a\x00", "a\x7f", "\ud800", "x" * 1025, 1, None]
)
def test_resource_and_node_names_are_strict_canonical_unicode(name):
    with pytest.raises(ValidationError):
        LogicalResources({name: 0})
    with pytest.raises(ValidationError):
        LogicalResources({}, {name: {}})


def test_name_length_admission_precedes_strip_or_unicode_encoding(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("text transformations must follow length admission")

    monkeypatch.setattr(module, "_text", forbidden)
    with pytest.raises(ValidationError):
        LogicalResources({"x" * 1025 + " ": 0})


def test_numeric_subclasses_are_not_accepted_as_builtin_resource_amounts():
    class CustomInt(int):
        pass

    class CustomFloat(float):
        pass

    for value in (CustomInt(1), CustomFloat(0.5)):
        with pytest.raises(ValidationError):
            LogicalResources({"cpu": value})


@pytest.mark.parametrize(
    "capacities,requests",
    [
        (None, {}),
        ([], {}),
        ({}, []),
        ({}, {"a": []}),
        ({"cpu": 0}, {"a": {"missing": 0}}),
        ({"cpu": 0}, {"a": {"cpu": 0.0001}}),
        ({"cpu": 1}, {"a": {"cpu": True}}),
        ({"cpu": 1}, {"a": {"cpu": 0.00001}}),
    ],
)
def test_invalid_mapping_unknown_zero_name_and_oversized_request_reject(capacities, requests):
    with pytest.raises(ValidationError):
        LogicalResources(capacities, requests)


def test_graph_node_validation_happens_even_for_empty_requested_bindings():
    policy = LogicalResources({}, {"a": {}})
    policy.validate_nodes(("a", "b"))
    for nodes in ([], ("b",), ("a", "a"), ("a", " bad")):
        with pytest.raises(ValidationError):
            policy.validate_nodes(nodes)
    LogicalResources({}).validate_nodes(())


class DishonestItems(Mapping):
    def __init__(self, entries):
        self.entries = entries

    def __len__(self):
        return 0

    def __iter__(self):
        return iter(())

    def __getitem__(self, key):
        raise KeyError(key)

    def items(self):
        return iter(self.entries)


def test_mapping_protocol_cannot_bypass_cardinality_with_false_length(monkeypatch):
    monkeypatch.setattr(module, "_MAX_RESOURCES", 2)
    with pytest.raises(ValidationError, match="entry limit"):
        LogicalResources(DishonestItems([("a", 1), ("b", 1), ("c", 1)]))
    with pytest.raises(ValidationError, match="repeat"):
        LogicalResources(DishonestItems([("a", 1), ("a", 1)]))
    with pytest.raises(ValidationError, match="key/value"):
        LogicalResources(DishonestItems([("a",)]))


def test_config_count_preflight_before_amount_work(monkeypatch):
    def forbidden(value):
        raise AssertionError("amount work must follow cardinality admission")

    monkeypatch.setattr(module, "_units", forbidden)
    monkeypatch.setattr(module, "_MAX_RESOURCES", 2)
    with pytest.raises(ValidationError, match="at most"):
        LogicalResources({"a": 1, "b": 1, "c": 1})
    monkeypatch.setattr(module, "MAX_NODES", 1)
    with pytest.raises(ValidationError, match="at most"):
        LogicalResources({}, {"a": {}, "b": {}})
    with pytest.raises(ValidationError, match="bounded tuple"):
        LogicalResources({}).validate_nodes(("a", "b"))


def test_total_binding_admission_counts_zero_requests_before_extra_amount_work(monkeypatch):
    original = module._units
    calls = []

    def observe(value):
        calls.append(value)
        assert len(calls) <= 2
        return original(value)

    monkeypatch.setattr(module, "_MAX_BINDINGS", 1)
    monkeypatch.setattr(module, "_units", observe)
    with pytest.raises(ValidationError, match="total request binding"):
        LogicalResources({"cpu": 0}, {"a": {"cpu": 0}, "b": {"cpu": 0}})
    assert calls == [0, 0]


@pytest.mark.parametrize("amount", [0, 0.5])
def test_zero_demand_attempts_and_missing_node_requests_still_count_leases(amount):
    policy = LogicalResources({"cpu": amount}, {"a": {"cpu": 0}})
    pool = _ResourcePool(policy)
    for node in ("a", "missing-request"):
        assert pool.fits(node)
        pool.reserve(node, 1)
        assert not pool.fits(node)
    pool.release_all()
    usage = pool.snapshot()
    assert usage.peak_units == {"cpu": 0}
    assert usage.reservations == usage.releases == 2
    empty = _ResourcePool(LogicalResources({}))
    empty.reserve("task", 1)
    empty.release("task", 1)
    assert empty.snapshot().reservations == 1


@pytest.mark.parametrize("attempt", [0, -1, 22, 10**1000, True, 1.0, None])
def test_attempt_ids_are_strict_and_bounded(attempt):
    pool = _ResourcePool(LogicalResources({}))
    before = pool._state
    for action in (pool.reserve, pool.release):
        with pytest.raises(ValidationError):
            action("a", attempt)
        assert pool._state is before


def test_attempt_holes_overlaps_and_stale_releases_cannot_affect_current_lease():
    pool = _ResourcePool(LogicalResources({}))
    with pytest.raises(ValidationError):
        pool.reserve("a", 2)
    pool.reserve("a", 1)
    with pytest.raises(ValidationError):
        pool.reserve("a", 2)
    with pytest.raises(ValidationError):
        pool.release("a", 2)
    pool.release("a", 1)
    with pytest.raises(ValidationError):
        pool.reserve("a", 3)
    pool.reserve("a", 2)
    before = pool._state
    with pytest.raises(ValidationError):
        pool.release("a", 1)
    assert pool._state is before
    pool.release("a", 2)
    for attempt in range(3, 22):
        pool.reserve("a", attempt)
        pool.release("a", attempt)
    assert len(pool._state.last_attempt) == 1
    assert pool.snapshot().reservations == 21


def test_bounded_node_history_and_reservation_guard_leave_state_unchanged(monkeypatch):
    pool = _ResourcePool(LogicalResources({}))
    pool.reserve("a", 1)
    pool.release("a", 1)
    before = pool._state
    monkeypatch.setattr(module, "MAX_NODES", 1)
    with pytest.raises(ValidationError, match="node history"):
        pool.reserve("b", 1)
    assert pool._state is before
    monkeypatch.setattr(module, "_MAX_RESERVATIONS", 1)
    with pytest.raises(ValidationError, match="resource reservations"):
        pool.reserve("a", 2)
    assert pool._state is before


@pytest.mark.parametrize("action", ["reserve", "release", "release_all"])
@pytest.mark.parametrize("error", [MemoryError, KeyboardInterrupt])
def test_every_multi_resource_state_publication_is_atomic(monkeypatch, action, error):
    pool = _ResourcePool(LogicalResources({"cpu": 1, "gpu": 1}, {"a": {"cpu": 0.5, "gpu": 1}}))
    if action != "reserve":
        pool.reserve("a", 1)
    before = pool._state

    def fail(*args, **kwargs):
        raise error("publication failure")

    monkeypatch.setattr(module, "_PoolState", fail)
    with pytest.raises(error):
        getattr(pool, action)(*([] if action == "release_all" else ["a", 1]))
    assert pool._state is before
    assert dict(before.used) == (
        {"cpu": 0, "gpu": 0} if action == "reserve" else {"cpu": 5000, "gpu": 10000}
    )


def test_seeded_pool_accounting_matches_independent_active_lease_oracle():
    rng = random.Random(15401464)
    nodes = tuple(f"n{index}" for index in range(8))
    amounts = {node: {"cpu": rng.randrange(5), "gpu": rng.randrange(3)} for node in nodes}
    requests = {
        node: {name: count / 4 for name, count in values.items()}
        for node, values in amounts.items()
    }
    pool = _ResourcePool(LogicalResources({"cpu": 1, "gpu": 0.5}, requests))
    active, last = {}, {}
    peaks = {"cpu": 0, "gpu": 0}
    reservations = releases = 0
    caps = {"cpu": 4, "gpu": 2}
    for _ in range(600):
        used = {name: sum(amounts[node][name] for node in active) for name in caps}
        for node in nodes:
            expected = node not in active and all(
                amounts[node][name] + used[name] <= caps[name] for name in caps
            )
            assert pool.fits(node) is expected
        if active and rng.randrange(3) == 0:
            node = rng.choice(tuple(active))
            pool.release(node, active.pop(node))
            releases += 1
        else:
            eligible = [node for node in nodes if node not in active and last.get(node, 0) < 21]
            if not eligible:
                continue
            node = rng.choice(eligible)
            attempt = last.get(node, 0) + 1
            expected = all(amounts[node][name] + used[name] <= caps[name] for name in caps)
            if not expected:
                before = pool._state
                with pytest.raises(ValidationError):
                    pool.reserve(node, attempt)
                assert pool._state is before
                continue
            pool.reserve(node, attempt)
            active[node] = last[node] = attempt
            reservations += 1
            peaks = {name: max(peaks[name], used[name] + amounts[node][name]) for name in caps}
        if not active:
            usage = pool.snapshot()
            assert usage.reservations == reservations and usage.releases == releases
    pool.release_all()
    releases += len(active)
    usage = pool.snapshot()
    assert usage.peak_units == {name: value * 2500 for name, value in peaks.items()}
    assert usage.reservations == reservations == releases == usage.releases


@pytest.mark.parametrize(
    "changes",
    [
        {"policy_digest": "A" * 64},
        {"policy_digest": "x"},
        {"policy_digest": 1},
        {"capacity_units": {"cpu": True}},
        {"capacity_units": {"cpu": 10_000_000_001}},
        {"peak_units": {"cpu": 10001}},
        {"peak_units": {}},
        {"peak_units": {"cpu": 0.0}},
        {"reservations": True},
        {"releases": 1},
        {"reservations": 1},
        {"reservations": 210001, "releases": 210001},
        {"peak_units": {"cpu": 1}},
    ],
)
def test_manual_completed_usage_rejects_contradictory_or_unbounded_fields(changes):
    usage = _ResourcePool(LogicalResources({"cpu": 1})).snapshot()
    with pytest.raises(ValidationError):
        replace(usage, **changes)


def test_usage_to_dict_is_detached_and_all_counters_mean_local_completed_accounting():
    pool = _ResourcePool(LogicalResources({"cpu": 1}, {"a": {"cpu": 0.5}}))
    pool.reserve("a", 1)
    pool.release("a", 1)
    usage = pool.snapshot()
    document = usage.to_dict()
    assert document["kind"] == "graph-sail-logical-resource-usage"
    assert document["units_per_resource"] == 10000
    assert document["reservations"] == document["releases"] == 1
    document["capacity_units"]["cpu"] = 0
    document["peak_units"]["cpu"] = 0
    assert usage.capacity_units == {"cpu": 10000}
    assert usage.peak_units == {"cpu": 5000}


def test_pool_revalidates_policy_input_fields_and_rejects_wrong_policy_type():
    for value in (None, {}, []):
        with pytest.raises(ValidationError):
            _ResourcePool(value)
    policy = LogicalResources({"cpu": 1})
    object.__setattr__(policy, "_capacity_units", {"cpu": 999999999999999})
    assert _ResourcePool(policy).snapshot().capacity_units == {"cpu": 10000}
