from __future__ import annotations

import copy
import json

import pytest

from graph_sail.demo import demo_payload
from graph_sail.errors import ValidationError
from graph_sail.io import graph_from_dict, load_graph
from graph_sail.limits import MAX_TEXT_LENGTH


def test_demo_payload_is_valid():
    graph = graph_from_dict(demo_payload())
    assert graph.name == "multimodal-assistant"
    assert len(graph.devices) == 3
    assert len(graph.nodes) == 6


def test_names_are_trimmed():
    payload = demo_payload()
    payload["name"] = "  assistant  "
    payload["nodes"][0]["id"] = "  decode-image  "
    graph = graph_from_dict(payload)
    assert graph.name == "assistant"
    assert graph.nodes[0].id == "decode-image"


def test_rejects_unknown_top_level_field():
    payload = demo_payload()
    payload["surprise"] = True
    with pytest.raises(ValidationError, match=r"unknown field.*surprise"):
        graph_from_dict(payload)


def test_rejects_unknown_node_field():
    payload = demo_payload()
    payload["nodes"][0]["surprise"] = True
    with pytest.raises(ValidationError, match=r"nodes\[0\].*surprise"):
        graph_from_dict(payload)


@pytest.mark.parametrize("key", ["devices", "nodes"])
def test_requires_nonempty_primary_collections(key):
    payload = demo_payload()
    payload[key] = []
    with pytest.raises(ValidationError, match=f"{key} must contain"):
        graph_from_dict(payload)


def test_rejects_duplicate_device_names():
    payload = demo_payload()
    payload["devices"].append(copy.deepcopy(payload["devices"][0]))
    with pytest.raises(ValidationError, match="duplicate device name"):
        graph_from_dict(payload)


def test_rejects_duplicate_node_ids():
    payload = demo_payload()
    payload["nodes"].append(copy.deepcopy(payload["nodes"][0]))
    with pytest.raises(ValidationError, match="duplicate node id"):
        graph_from_dict(payload)


def test_rejects_duplicate_edges():
    payload = demo_payload()
    payload["edges"].append(copy.deepcopy(payload["edges"][0]))
    with pytest.raises(ValidationError, match="duplicate edge"):
        graph_from_dict(payload)


def test_rejects_duplicate_links():
    payload = demo_payload()
    payload["links"].append(copy.deepcopy(payload["links"][0]))
    with pytest.raises(ValidationError, match="duplicate link"):
        graph_from_dict(payload)


def test_rejects_unknown_latency_device():
    payload = demo_payload()
    payload["nodes"][0]["latency_ms"]["ghost"] = 1
    with pytest.raises(ValidationError, match=r"latency estimates for unknown device.*ghost"):
        graph_from_dict(payload)


def test_rejects_unknown_allowed_device():
    payload = demo_payload()
    payload["nodes"][0]["allowed_devices"] = ["ghost"]
    with pytest.raises(ValidationError, match=r"allows unknown device.*ghost"):
        graph_from_dict(payload)


def test_rejects_unknown_pinned_device():
    payload = demo_payload()
    payload["nodes"][0]["pinned_device"] = "ghost"
    with pytest.raises(ValidationError, match="pinned to unknown device"):
        graph_from_dict(payload)


def test_rejects_node_without_compatible_device_kind():
    payload = demo_payload()
    payload["nodes"][0]["kind"] = "language"
    with pytest.raises(ValidationError, match="no statically compatible device"):
        graph_from_dict(payload)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("source", "ghost", "edge source 'ghost'"),
        ("target", "ghost", "edge target 'ghost'"),
        ("target", "decode-image", "cannot have a self edge"),
    ],
)
def test_rejects_invalid_edge_endpoints(field, value, message):
    payload = demo_payload()
    payload["edges"][0][field] = value
    with pytest.raises(ValidationError, match=message):
        graph_from_dict(payload)


def test_rejects_link_with_unknown_device():
    payload = demo_payload()
    payload["links"][0]["target"] = "ghost"
    with pytest.raises(ValidationError, match="references an unknown device"):
        graph_from_dict(payload)


def test_rejects_self_link():
    payload = demo_payload()
    payload["links"][0]["target"] = payload["links"][0]["source"]
    with pytest.raises(ValidationError, match="cannot target itself"):
        graph_from_dict(payload)


@pytest.mark.parametrize("value", [0, -1, True, float("inf"), float("nan")])
def test_rejects_invalid_default_bandwidth(value):
    payload = demo_payload()
    payload["default_bandwidth_mb_s"] = value
    with pytest.raises(ValidationError, match="default_bandwidth"):
        graph_from_dict(payload)


def test_rejects_empty_latency_map():
    payload = demo_payload()
    payload["nodes"][0]["latency_ms"] = {}
    with pytest.raises(ValidationError, match="at least one device estimate"):
        graph_from_dict(payload)


def test_load_graph_reports_json_location(tmp_path):
    path = tmp_path / "broken.json"
    path.write_text('{"nodes": ]', encoding="utf-8")
    with pytest.raises(ValidationError, match=r"line 1, column"):
        load_graph(path)


def test_load_graph_rejects_duplicate_raw_json_fields(tmp_path):
    path = tmp_path / "duplicate.json"
    path.write_text('{"name":"first","name":"second","devices":[],"nodes":[]}', encoding="utf-8")
    with pytest.raises(ValidationError, match=r"duplicate field 'name'"):
        load_graph(path)


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_load_graph_rejects_nonstandard_json_numeric_constants(tmp_path, constant):
    path = tmp_path / "constant.json"
    path.write_text(
        '{"devices":[{"name":"d","memory_mb":' + constant + '}],"nodes":[]}',
        encoding="utf-8",
    )
    with pytest.raises(ValidationError, match=r"non-standard numeric constant"):
        load_graph(path)


def test_rejects_latency_keys_that_collide_after_normalization():
    payload = demo_payload()
    payload["nodes"][0]["latency_ms"] = {"cpu": 4, " cpu ": 9}
    with pytest.raises(ValidationError, match=r"duplicate .*latency_ms device"):
        graph_from_dict(payload)


def test_rejects_integer_too_large_for_finite_float():
    payload = demo_payload()
    payload["devices"][0]["memory_mb"] = 10**400
    with pytest.raises(ValidationError, match=r"memory_mb must be finite"):
        graph_from_dict(payload)


def test_rejects_control_characters_in_text_fields():
    payload = demo_payload()
    payload["edges"][0]["label"] = "image\rbytes"
    with pytest.raises(ValidationError, match=r"label must not contain control characters"):
        graph_from_dict(payload)


def test_rejects_unpaired_surrogate_in_text_fields():
    payload = demo_payload()
    payload["name"] = "bad\ud800name"
    with pytest.raises(ValidationError, match="valid Unicode scalar"):
        graph_from_dict(payload)


def test_load_graph_reports_missing_file(tmp_path):
    with pytest.raises(ValidationError, match="cannot read graph file"):
        load_graph(tmp_path / "missing.json")


def test_load_graph_wraps_invalid_utf8(tmp_path):
    path = tmp_path / "invalid.json"
    path.write_bytes(b"\xff")
    with pytest.raises(ValidationError, match="cannot read graph file"):
        load_graph(path)


def test_load_graph_round_trip(tmp_path):
    path = tmp_path / "graph.json"
    path.write_text(json.dumps(demo_payload()), encoding="utf-8")
    assert load_graph(path).name == "multimodal-assistant"


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda payload: payload.update({"name": 1}), "must be a string"),
        (lambda payload: payload.update({"name": " "}), "must not be empty"),
        (
            lambda payload: payload.update({"name": "x" * (MAX_TEXT_LENGTH + 1)}),
            "character limit",
        ),
        (lambda payload: payload.update({"devices": "bad"}), "must be an array"),
        (lambda payload: payload["edges"][0].update({"payload_mb": -1}), "zero or greater"),
    ],
)
def test_parser_primitive_boundaries_are_domain_errors(mutate, message):
    payload = demo_payload()
    mutate(payload)
    with pytest.raises(ValidationError, match=message):
        graph_from_dict(payload)


def test_parser_collection_and_mapping_resource_limits(monkeypatch):
    payload = demo_payload()
    monkeypatch.setattr("graph_sail.io.MAX_DEVICES", 1)
    with pytest.raises(ValidationError, match="item limit"):
        graph_from_dict(payload)
    monkeypatch.setattr("graph_sail.io.MAX_DEVICES", 1_024)
    payload = demo_payload()
    payload["nodes"][0]["latency_ms"] = {1: 2}
    with pytest.raises(ValidationError, match="keys must be strings"):
        graph_from_dict(payload)
