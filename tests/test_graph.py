from __future__ import annotations

import pytest

from graph_sail.demo import demo_payload
from graph_sail.errors import ValidationError
from graph_sail.graph import predecessor_edges, successor_edges, terminal_nodes, topological_order
from graph_sail.io import graph_from_dict


def test_topological_order_is_stable_for_independent_nodes():
    payload = demo_payload()
    payload["nodes"].reverse()
    graph = graph_from_dict(payload)
    order = topological_order(graph)
    assert order.index("decode-audio") < order.index("decode-image")
    assert order[-1] == "format-response"


def test_cycle_error_contains_a_concrete_path():
    payload = demo_payload()
    payload["edges"].append(
        {"source": "format-response", "target": "decode-image", "payload_mb": 0}
    )
    with pytest.raises(ValidationError, match=r"decode-image.*format-response.*decode-image"):
        graph_from_dict(payload)


def test_cycle_diagnostic_does_not_depend_on_python_recursion_limit():
    node_count = 1_500
    payload = {
        "devices": [{"name": "device", "memory_mb": 1}],
        "nodes": [
            {
                "id": f"node-{index:04d}",
                "kind": "work",
                "memory_mb": 0,
                "latency_ms": {"device": 1},
            }
            for index in range(node_count)
        ],
        "edges": [
            {
                "source": f"node-{index:04d}",
                "target": f"node-{(index + 1) % node_count:04d}",
            }
            for index in range(node_count)
        ],
    }
    with pytest.raises(ValidationError, match=r"graph contains a cycle: node-"):
        graph_from_dict(payload)


def test_cycle_diagnostic_skips_noncycle_nodes_left_by_kahn():
    payload = {
        "devices": [{"name": "device", "memory_mb": 1}],
        "nodes": [
            {
                "id": node_id,
                "kind": "work",
                "memory_mb": 0,
                "latency_ms": {"device": 1},
            }
            for node_id in ("a-downstream", "z-cycle-a", "z-cycle-b")
        ],
        "edges": [
            {"source": "z-cycle-a", "target": "z-cycle-b"},
            {"source": "z-cycle-b", "target": "z-cycle-a"},
            {"source": "z-cycle-b", "target": "a-downstream"},
        ],
    }
    with pytest.raises(ValidationError, match=r"z-cycle-a -> z-cycle-b -> z-cycle-a"):
        graph_from_dict(payload)


def test_predecessor_index_includes_empty_entries():
    graph = graph_from_dict(demo_payload())
    incoming = predecessor_edges(graph)
    assert incoming["decode-image"] == ()
    assert {edge.source for edge in incoming["language-core"]} == {
        "audio-encoder",
        "vision-encoder",
    }


def test_successor_index_includes_empty_entries():
    graph = graph_from_dict(demo_payload())
    outgoing = successor_edges(graph)
    assert outgoing["format-response"] == ()
    assert outgoing["decode-audio"][0].target == "audio-encoder"


def test_terminal_nodes():
    assert terminal_nodes(graph_from_dict(demo_payload())) == ("format-response",)


def test_transfer_uses_zero_for_local_and_configured_link_for_remote():
    graph = graph_from_dict(demo_payload())
    assert graph.transfer_ms("cpu", "cpu", 100) == 0
    assert graph.transfer_ms("cpu", "gpu-0", 8) == pytest.approx(1.08)


def test_transfer_falls_back_when_link_is_absent():
    payload = demo_payload()
    payload["links"] = []
    graph = graph_from_dict(payload)
    assert graph.transfer_ms("cpu", "gpu-0", 9) == pytest.approx(10.15)
