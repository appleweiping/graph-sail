"""Bounded exhaustive placement for small graphs and planner validation.

Greedy and beam search are the practical defaults, but a beam can discard the
globally best partial state.  ``ExactPlanner`` enumerates every feasible
placement in the graph's fixed topological order and therefore provides an
auditable optimum for small workloads.  The explicit state ceiling keeps this
reference planner from being mistaken for an unbounded production scheduler.
"""

from __future__ import annotations

from graph_sail.errors import PlanningError
from graph_sail.graph import predecessor_edges, topological_order
from graph_sail.limits import MAX_EXACT_STATES
from graph_sail.models import GraphSpec, PlanResult
from graph_sail.planner import (
    _evaluate_candidate,
    _EvaluatedCandidate,
    _extend_state,
    _no_placement_error,
    _PlanState,
    _state_rank,
    _to_result,
    _validate_planning_work,
)


class ExactPlanner:
    """Enumerate all feasible placements up to an explicit state budget."""

    name = "exact-earliest-finish"

    def __init__(self, max_states: int = MAX_EXACT_STATES) -> None:
        if (
            isinstance(max_states, bool)
            or not isinstance(max_states, int)
            or not 1 <= max_states <= MAX_EXACT_STATES
        ):
            raise ValueError(
                f"max_states must be an integer from 1 to {MAX_EXACT_STATES}"
            )
        self.max_states = max_states

    def plan(self, graph: GraphSpec) -> PlanResult:
        """Return the minimum makespan plan, or a bounded planning error."""

        _validate_planning_work(graph, beam_width=1)
        states = [_PlanState.empty(graph)]
        incoming = predecessor_edges(graph)
        node_index = graph.node_map
        link_index = graph.link_map
        devices = tuple(sorted(graph.devices, key=lambda item: item.name))
        explored = 0
        for node_id in topological_order(graph):
            node = node_index[node_id]
            expanded: list[_PlanState] = []
            last_evaluated: tuple[_EvaluatedCandidate, ...] = ()
            for state in states:
                evaluated = tuple(
                    _evaluate_candidate(graph, node, device, state, incoming[node_id], link_index)
                    for device in devices
                )
                last_evaluated = evaluated
                for candidate in evaluated:
                    explored += 1
                    if explored > self.max_states:
                        raise PlanningError(
                            f"exact planning exceeded the {self.max_states}-state limit"
                        )
                    if candidate.scheduled is not None:
                        expanded.append(
                            _extend_state(
                                state,
                                candidate.scheduled,
                                tuple(item.trace for item in evaluated),
                            )
                        )
            if not expanded:
                raise _no_placement_error(node, last_evaluated)
            expanded.sort(key=_state_rank)
            states = expanded
        if not states:  # pragma: no cover - every non-empty graph has a state or errors
            raise PlanningError("exact planner produced no states")
        return _to_result(graph, min(states, key=_state_rank), self.name)


__all__ = ["ExactPlanner"]
