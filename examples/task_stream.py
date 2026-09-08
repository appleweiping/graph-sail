"""Observe a partial result while the remaining computation is still blocked."""

from __future__ import annotations

from collections.abc import Generator
from threading import Event

from graph_sail import TaskStreamConfig, TaskStreamContext, start_task_stream


def main() -> None:
    release = Event()

    def produce(context: TaskStreamContext) -> Generator[object, None, object]:
        yield {"part": "first", "value": 17}
        if not release.wait(5):
            raise TimeoutError("consumer did not release the remaining computation")
        context.cancellation.raise_if_cancelled()
        yield {"part": "second", "value": 25}

    with start_task_stream(produce, config=TaskStreamConfig(max_buffered=1)) as stream:
        try:
            first = stream.next(5)
            if stream.done():
                raise RuntimeError("producer finished before its explicit release")
            print(f"first: {first.sequence} {first.value}; producer still running")
            release.set()
            second = stream.next(5)
            print(f"second: {second.sequence} {second.value}")
            if list(stream):
                raise RuntimeError("unexpected extra output")
            print(stream.completion(5))
        finally:
            release.set()


if __name__ == "__main__":
    main()
