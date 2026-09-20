"""Run one validated local fork/join DAG for each accepted stream yield.

This composes the bounded source/consumer owner with the existing threaded
graph executor. It is not a distributed scheduler or an ObjectRef protocol.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping

from graph_sail.errors import GraphSailError, ValidationError
from graph_sail.execution import (
    CancellationToken,
    ExecutionConfig,
    ExecutionResult,
    TaskContext,
    TaskDefinition,
    TaskRegistry,
    _prepare_execution,
    execute_graph,
)
from graph_sail.models import GraphSpec
from graph_sail.stream_map import (
    StreamMap,
    StreamMapConfig,
    StreamMapContext,
    StreamMapProducer,
    start_stream_map,
)


class StreamGraphExecutionError(GraphSailError):
    """One accepted item produced a failed or cancelled graph result.

    The underlying graph executor records task attempt telemetry, not the
    original Python exception object. ``result`` carries that telemetry.
    """

    def __init__(self, sequence: int, result: ExecutionResult) -> None:
        self.sequence = sequence
        self.result = result
        self.failed_nodes = tuple(task.node_id for task in result.tasks if task.status == "failed")
        super().__init__(
            f"stream item {sequence} graph {result.status}; failed nodes: "
            f"{', '.join(self.failed_nodes) if self.failed_nodes else 'none'}"
        )


def _reachable(start: str, edges: Mapping[str, tuple[str, ...]]) -> set[str]:
    seen = {start}
    pending = deque((start,))
    while pending:
        for node in edges[pending.popleft()]:
            if node not in seen:
                seen.add(node)
                pending.append(node)
    return seen


def _validate_template(
    graph: GraphSpec,
    registry: TaskRegistry,
    placements: Mapping[str, str],
    input_node: str,
    output_node: str,
    stream_config: StreamMapConfig,
    execution_config: ExecutionConfig | None,
) -> tuple[TaskRegistry, dict[str, str], ExecutionConfig]:
    if not isinstance(graph, GraphSpec) or not isinstance(registry, TaskRegistry):
        raise ValidationError("graph and registry must be GraphSpec and TaskRegistry")
    graph.validate()
    nodes = set(graph.node_map)
    if type(input_node) is not str or input_node not in nodes:
        raise ValidationError("input_node must identify a graph node")
    if type(output_node) is not str or output_node not in nodes or output_node == input_node:
        raise ValidationError("output_node must identify a distinct graph node")
    if set(registry.tasks) != nodes - {input_node}:
        raise ValidationError("registry must name exactly the graph nodes except input_node")
    if any(registry.definition(node).max_retries for node in registry.tasks):
        raise ValidationError("per-yield graph tasks cannot have automatic retries")

    successors: dict[str, tuple[str, ...]] = {
        node: tuple(edge.target for edge in graph.edges if edge.source == node) for node in nodes
    }
    predecessors: dict[str, tuple[str, ...]] = {
        node: tuple(edge.source for edge in graph.edges if edge.target == node) for node in nodes
    }
    if any(not predecessors[node] for node in nodes - {input_node}) or predecessors[input_node]:
        raise ValidationError("input_node must be the only graph root")
    if (
        _reachable(input_node, successors) != nodes
        or _reachable(output_node, predecessors) != nodes
    ):
        raise ValidationError("every graph node must lie on an input-to-output path")

    def injected_input(_context: TaskContext) -> object:
        raise RuntimeError("stream graph admission input must not execute")

    admitted = TaskRegistry({**registry.tasks, input_node: TaskDefinition(injected_input)})
    _, assigned, memory, options = _prepare_execution(
        graph, admitted, placements, execution_config, None
    )
    stream_workers = stream_config.max_workers
    if stream_workers * options.max_workers > 64:
        raise ValidationError("aggregate stream and graph worker budget exceeds 64")
    for device in graph.devices:
        if memory[device.name] * stream_workers > device.memory_mb + 1e-9:
            raise ValidationError(f"aggregate graph memory exceeds device {device.name!r}")
    if options.resources is not None:
        resources = options.resources
        for name, capacity in resources.capacity_units.items():
            requested = sum(amounts.get(name, 0) for amounts in resources.request_units.values())
            if requested * stream_workers > capacity:
                raise ValidationError(f"aggregate logical resource {name!r} exceeds capacity")
    return TaskRegistry(registry.tasks), assigned, options


def start_stream_graph(
    producer: StreamMapProducer,
    graph: GraphSpec,
    registry: TaskRegistry,
    placements: Mapping[str, str],
    *,
    input_node: str,
    output_node: str,
    config: StreamMapConfig | None = None,
    execution_config: ExecutionConfig | None = None,
) -> StreamMap:
    """Start a bounded local source whose every accepted item executes a DAG.

    An accepted source value is independently serialized before its graph
    begins. Results are consumed in source order. Graph tasks share values
    within one invocation and must treat dependencies as read-only.
    """
    if config is not None and type(config) is not StreamMapConfig:
        raise ValidationError("config must be StreamMapConfig")
    stream_options = StreamMapConfig() if config is None else config
    stream_options.__post_init__()
    tasks, assigned, graph_options = _validate_template(
        graph, registry, placements, input_node, output_node, stream_options, execution_config
    )

    def run_item(context: StreamMapContext, value: object) -> object:
        if not isinstance(context.cancellation, CancellationToken):
            raise RuntimeError("stream map did not supply its cancellation token")

        def input_task(task_context: TaskContext) -> object:
            task_context.cancellation.raise_if_cancelled()
            return value

        invocation = TaskRegistry({**tasks.tasks, input_node: TaskDefinition(input_task)})
        result = execute_graph(
            graph,
            invocation,
            assigned,
            config=graph_options,
            cancel_event=context.cancellation._internal,
        )
        if result.status != "succeeded":
            raise StreamGraphExecutionError(context.sequence, result)
        return result.outputs[output_node]

    return start_stream_map(producer, run_item, config=stream_options)


__all__ = ["StreamGraphExecutionError", "start_stream_graph"]
