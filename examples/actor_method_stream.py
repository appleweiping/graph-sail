"""Offline real actor state across generator yields, cleanup and later calls."""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path

from graph_sail import (
    ActorDefinition,
    ActorRegistry,
    ActorStreamConfig,
    ProcessActor,
    TaskStreamConfig,
)


class Counter:
    def __init__(self, value=10):
        self.value = value
        self.finishes = 0

    def add(self, amount=0):
        self.value += amount
        return self.value

    def state(self):
        return self.value, self.finishes, os.getpid()

    def totals(self, context, increments):
        try:
            for increment in increments:
                context.cancellation.raise_if_cancelled()
                self.value += increment
                yield self.value
        finally:
            self.finishes += 1

    def waiting(self, context, marker):
        try:
            Path(marker).write_text("entered", encoding="utf-8")
            while not context.cancellation.cancelled:
                time.sleep(0.005)
            yield "discarded late value"
        finally:
            Path(marker + ".closed").write_text("closed", encoding="utf-8")


def expect(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    registry = ActorRegistry(
        {"counter": ActorDefinition(Counter, ("add", "state"), ("totals", "waiting"))}
    )
    with ProcessActor(registry, "counter") as actor:
        pid = actor.pid
        expect(pid != os.getpid(), "actor was not a spawned child")
        expect(actor.submit("add", args=(2,)).result(5) == 12, "ordinary initial mutation")
        with actor.stream(
            "totals", args=((3, 5),), config=TaskStreamConfig(max_buffered=1)
        ) as stream:
            totals = [item.value for item in stream]
            expect(totals == [15, 20], "stateful generator totals")
            expect(stream.completion(5).state.actor_reusable_at_release, "missing closure receipt")
        with actor.stream(
            "totals", args=((7, 100),), config=TaskStreamConfig(max_yields=1)
        ) as limited:
            expect([item.value for item in limited] == [27], "limit peeked at a second mutation")
            expect(limited.completion(5).status == "limited", "exact cap status")
        expect(actor.submit("state").result(5) == (27, 2, pid), "instance or cleanup was lost")
    expect(not actor.alive and actor.exitcode is not None, "stateful actor was not joined")

    with tempfile.TemporaryDirectory(prefix="graph-actor-stream-") as temporary:
        marker = Path(temporary) / "started"
        with ProcessActor(registry, "counter") as cancelled_actor:
            with cancelled_actor.stream(
                "waiting",
                args=(str(marker),),
                stream_config=ActorStreamConfig(cancellation_grace_seconds=1),
            ) as cancelled:
                deadline = time.monotonic() + 10
                while not marker.exists():
                    expect(time.monotonic() < deadline, "cancel fixture never entered")
                    time.sleep(0.005)
                cancelled.cancel()
                result = cancelled.completion(10)
                expect(result.status == "cancelled", "cancellation status")
                expect(result.state.actor_resources_closed, "retired actor resources unresolved")
                expect(not result.state.actor_reusable_at_release, "EOF cancellation was reused")
            expect(Path(str(marker) + ".closed").exists(), "cooperative finally was not observed")
        expect(not cancelled_actor.alive, "cancelled actor remains alive")
    print(
        json.dumps(
            {
                "totals": totals,
                "final_value": 27,
                "generator_closes": 2,
                "actor_pid": pid,
                "cancelled_actor_retired": True,
            }
        )
    )


if __name__ == "__main__":
    main()
