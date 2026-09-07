"""Run with `python examples/process_actor.py`; the main guard is required for spawn."""

from __future__ import annotations

import os

from graph_sail import ActorConfig, ActorDefinition, ActorRegistry, ProcessActor


class RunningMean:
    """State belongs to the actor process and persists across mailbox calls."""

    def __init__(self) -> None:
        self.total = 0.0
        self.count = 0

    def observe(self, value: float) -> dict[str, float | int]:
        self.total += value
        self.count += 1
        return {"mean": self.total / self.count, "count": self.count, "worker_pid": os.getpid()}


def main() -> None:
    registry = ActorRegistry({"mean": ActorDefinition(RunningMean, ("observe",))})
    with ProcessActor(registry, "mean", config=ActorConfig(max_pending=4)) as actor:
        calls = [actor.submit("observe", args=(value,)) for value in (2.0, 8.0, 11.0)]
        for call in calls:
            print(call.request_id, call.result(timeout=5))
        assert calls[-1].result() == {"mean": 7.0, "count": 3, "worker_pid": actor.pid}
        assert actor.pid != os.getpid()
    assert actor.exitcode == 0 and not actor.alive
    print("Actor drained, exited and joined.")


if __name__ == "__main__":
    main()
