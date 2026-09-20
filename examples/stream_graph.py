"""Run a fork/join GraphSpec for each bounded native generator yield."""

from graph_sail import (
    ExecutionConfig,
    StreamMapConfig,
    TaskRegistry,
    start_stream_graph,
)
from graph_sail.models import DeviceSpec, EdgeSpec, GraphSpec, NodeSpec


def source(context):
    for value in (1, 2, 3):
        context.cancellation.raise_if_cancelled()
        yield value


def left(context):
    return context.dependencies["input"] * 2


def right(context):
    return context.dependencies["input"] + 1


def join(context):
    return context.dependencies["left"] + context.dependencies["right"]


def main() -> None:
    names = ("input", "left", "right", "join")
    graph = GraphSpec(
        "fork-join-stream",
        (DeviceSpec("cpu", 64),),
        tuple(NodeSpec(name, "work", 1, {"cpu": 1}) for name in names),
        (
            EdgeSpec("input", "left"),
            EdgeSpec("input", "right"),
            EdgeSpec("left", "join"),
            EdgeSpec("right", "join"),
        ),
    )
    with start_stream_graph(
        source,
        graph,
        TaskRegistry({"left": left, "right": right, "join": join}),
        dict.fromkeys(names, "cpu"),
        input_node="input",
        output_node="join",
        config=StreamMapConfig(max_pending=2, max_workers=2),
        execution_config=ExecutionConfig(max_workers=3, device_workers={"cpu": 3}),
    ) as stream:
        for item in stream:
            print(item.sequence, item.value)
        print(stream.completion().status)


if __name__ == "__main__":
    main()
