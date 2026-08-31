"""Graph indexing, deterministic ordering, and cycle diagnostics."""

from __future__ import annotations

import heapq
from collections import defaultdict
from collections.abc import Iterable, Iterator

from graph_sail.errors import ValidationError
from graph_sail.models import EdgeSpec, GraphSpec


def predecessor_edges(graph: GraphSpec) -> dict[str, tuple[EdgeSpec, ...]]:
    """Index incoming edges in stable source/target order."""

    incoming: dict[str, list[EdgeSpec]] = defaultdict(list)
    for edge in graph.edges:
        incoming[edge.target].append(edge)
    return {
        node.id: tuple(sorted(incoming[node.id], key=lambda item: (item.source, item.target)))
        for node in graph.nodes
    }


def successor_edges(graph: GraphSpec) -> dict[str, tuple[EdgeSpec, ...]]:
    """Index outgoing edges in stable source/target order."""

    outgoing: dict[str, list[EdgeSpec]] = defaultdict(list)
    for edge in graph.edges:
        outgoing[edge.source].append(edge)
    return {
        node.id: tuple(sorted(outgoing[node.id], key=lambda item: (item.target, item.source)))
        for node in graph.nodes
    }


def topological_order(graph: GraphSpec) -> tuple[str, ...]:
    """Return a lexicographically stable topological order.

    A min-heap makes plans reproducible even if independent nodes were listed in
    a different input order.
    """

    indegree = {node.id: 0 for node in graph.nodes}
    successors: dict[str, list[str]] = defaultdict(list)
    for edge in graph.edges:
        indegree[edge.target] += 1
        successors[edge.source].append(edge.target)

    ready = [node_id for node_id, degree in indegree.items() if degree == 0]
    heapq.heapify(ready)
    ordered: list[str] = []
    while ready:
        node_id = heapq.heappop(ready)
        ordered.append(node_id)
        for target in sorted(successors[node_id]):
            indegree[target] -= 1
            if indegree[target] == 0:
                heapq.heappush(ready, target)

    if len(ordered) != len(graph.nodes):
        cycle = _find_cycle(indegree, graph.edges)
        detail = " -> ".join(cycle) if cycle else "unknown cycle"
        raise ValidationError(f"graph contains a cycle: {detail}")
    return tuple(ordered)


def _find_cycle(indegree: dict[str, int], edges: Iterable[EdgeSpec]) -> tuple[str, ...]:
    remaining = {node_id for node_id, degree in indegree.items() if degree > 0}
    outgoing: dict[str, list[str]] = defaultdict(list)
    for edge in edges:
        if edge.source in remaining and edge.target in remaining:
            outgoing[edge.source].append(edge.target)

    visited: set[str] = set()
    for start_node in sorted(remaining):
        if start_node in visited:
            continue
        path = [start_node]
        path_index = {start_node: 0}
        stack: list[tuple[str, Iterator[str]]] = [(start_node, iter(sorted(outgoing[start_node])))]
        while stack:
            node_id, targets = stack[-1]
            try:
                target = next(targets)
            except StopIteration:
                stack.pop()
                path.pop()
                path_index.pop(node_id)
                visited.add(node_id)
                continue
            if target in path_index:
                cycle_start = path_index[target]
                return (*path[cycle_start:], target)
            if target not in visited:
                path_index[target] = len(path)
                path.append(target)
                stack.append((target, iter(sorted(outgoing[target]))))
    return ()


def terminal_nodes(graph: GraphSpec) -> tuple[str, ...]:
    """Return nodes without outgoing edges."""

    sources = {edge.source for edge in graph.edges}
    return tuple(sorted(node.id for node in graph.nodes if node.id not in sources))
