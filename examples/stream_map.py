"""Run one bounded local source-to-map edge without network or model assets."""

from __future__ import annotations

import json
from threading import Event

from graph_sail import StreamMapConfig, start_stream_map


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def main() -> None:
    release_source = Event()

    def source(context):
        yield {"value": 2}
        release_source.wait(5)
        context.cancellation.raise_if_cancelled()
        yield {"value": 3}

    def mapper(context, value):
        return {"sequence": context.sequence, "doubled": value["value"] * 2}

    with start_stream_map(
        source,
        mapper,
        config=StreamMapConfig(max_pending=1, max_workers=1),
    ) as edge:
        first = edge.next(5)
        require(not edge.done(), "first mapped value waited for source EOF")
        require(first.value == {"sequence": 0, "doubled": 4}, "first mapping mismatch")
        release_source.set()
        second = edge.next(5)
        require(second.value == {"sequence": 1, "doubled": 6}, "second mapping mismatch")
        result = edge.completion(5)
        require(result.status == "succeeded", "source did not reach EOF")
        require((result.accepted, result.published) == (2, 2), "counts mismatch")
    print(json.dumps({"mapped": [first.value, second.value], "status": result.status}))


if __name__ == "__main__":
    main()
