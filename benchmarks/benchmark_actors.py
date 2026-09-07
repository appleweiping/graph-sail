"""Offline actor throughput/ownership protocol with a closed-form result oracle."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import statistics
import time
from collections import deque
from contextlib import ExitStack
from dataclasses import asdict, dataclass
from typing import Any

from graph_sail import ActorConfig, ActorDefinition, ActorRegistry, ProcessActor


@dataclass(frozen=True)
class Workload:
    requests: int = 256
    rounds: int = 4096
    actors: int = 2
    window: int = 16

    def __post_init__(self) -> None:
        for name, maximum in (
            ("requests", 100_000),
            ("rounds", 100_000),
            ("actors", 8),
            ("window", 256),
        ):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError(f"{name} must be an integer in 1..{maximum}")
        if self.requests * self.rounds > 10_000_000:
            raise ValueError("a workload must not exceed ten million arithmetic iterations")
        if self.actors > self.requests:
            raise ValueError("every actor must receive a request")


class Accumulator:
    def __init__(self) -> None:
        self.total = 0
        self.count = 0

    def observe(self, value: int, rounds: int) -> tuple[int, int, int]:
        subtotal = 0
        for index in range(rounds):
            subtotal += (value + index) % 97
        self.total += subtotal
        self.count += 1
        return self.count, self.total, os.getpid()


def _oracle_sum(value: int, rounds: int) -> int:
    """Closed form, independently implemented from the worker's arithmetic loop."""
    cycles, tail = divmod(rounds, 97)
    start = value % 97
    first = min(tail, 97 - start)
    wrapped = tail - first
    return (
        cycles * (96 * 97 // 2)
        + first * (2 * start + first - 1) // 2
        + wrapped * (wrapped - 1) // 2
    )


def expected_digest(workload: Workload) -> str:
    counts = [0] * workload.actors
    totals = [0] * workload.actors
    digest = hashlib.sha256()
    for index in range(workload.requests):
        shard = index % workload.actors
        counts[shard] += 1
        totals[shard] += _oracle_sum(index, workload.rounds)
        digest.update(f"{index}:{counts[shard]}:{totals[shard]}\n".encode("ascii"))
    return digest.hexdigest()


def run_trial(workload: Workload, mode: str) -> dict[str, Any]:
    if type(workload) is not Workload or mode not in ("direct", "process"):
        raise ValueError("a trial requires a Workload and direct/process mode")
    expected = expected_digest(workload)
    digest = hashlib.sha256()
    worker_pids: set[int] = set()
    max_pending = 0

    def consume(index: int, result: Any, expected_pid: int) -> None:
        if (
            type(result) is not tuple
            or len(result) != 3
            or any(type(value) is not int for value in result)
            or result[2] != expected_pid
        ):
            raise AssertionError("result violates the worker ownership/value contract")
        count, total, pid = result
        digest.update(f"{index}:{count}:{total}\n".encode("ascii"))
        worker_pids.add(pid)

    start = time.perf_counter_ns()
    with ExitStack() as resources:
        if mode == "process":
            registry = ActorRegistry({"counter": ActorDefinition(Accumulator, ("observe",))})
            actors = [
                resources.enter_context(
                    ProcessActor(
                        registry,
                        "counter",
                        config=ActorConfig(
                            max_pending=workload.window,
                            max_message_bytes=4096,
                            method_timeout_seconds=30,
                        ),
                    )
                )
                for _ in range(workload.actors)
            ]
            if len({actor.pid for actor in actors}) != workload.actors or any(
                actor.pid == os.getpid() for actor in actors
            ):
                raise AssertionError("benchmark requires distinct local worker processes")
        else:
            direct = [Accumulator() for _ in range(workload.actors)]
        ready = time.perf_counter_ns()
        pending: deque[tuple[int, Any, int]] = deque()
        for index in range(workload.requests):
            shard = index % workload.actors
            if mode == "direct":
                consume(index, direct[shard].observe(index, workload.rounds), os.getpid())
                continue
            actor = actors[shard]
            pending.append(
                (index, actor.submit("observe", args=(index, workload.rounds)), actor.pid)
            )
            max_pending = max(max_pending, len(pending))
            if len(pending) == workload.window:
                position, handle, pid = pending.popleft()
                consume(position, handle.result(timeout=35), pid)
        for position, handle, pid in pending:
            consume(position, handle.result(timeout=35), pid)
        finished = time.perf_counter_ns()
    closed = time.perf_counter_ns()
    if mode == "process" and any(actor.alive or actor.exitcode != 0 for actor in actors):
        raise AssertionError("all actor workers must exit and be reaped successfully")
    actual = digest.hexdigest()
    if actual != expected:
        raise AssertionError("worker outputs disagree with the independent closed-form oracle")
    processing_ns = max(1, finished - ready)
    return {
        "startup_ns": ready - start,
        "processing_ns": processing_ns,
        "shutdown_ns": closed - finished,
        "total_ns": closed - start,
        "requests_per_second": workload.requests * 1_000_000_000 / processing_ns,
        "output_sha256": actual,
        "observed_processes": len(worker_pids),
        "max_pending_observed": max_pending,
    }


def benchmark(workload: Workload, *, repeats: int = 3, warmups: int = 1) -> dict[str, Any]:
    if type(repeats) is not int or not 1 <= repeats <= 21:
        raise ValueError("repeats must be an integer in 1..21")
    if type(warmups) is not int or not 0 <= warmups <= 3:
        raise ValueError("warmups must be an integer in 0..3")
    if (repeats + warmups) * workload.requests * workload.rounds > 50_000_000:
        raise ValueError("benchmark exceeds the fifty-million per-mode work limit")
    modes = []
    for mode in ("direct", "process"):
        for _ in range(warmups):
            run_trial(workload, mode)
        trials = [run_trial(workload, mode) for _ in range(repeats)]
        elapsed = sorted(trial["processing_ns"] for trial in trials)
        modes.append(
            {
                "mode": mode,
                "trials": trials,
                "median_processing_ns": statistics.median(elapsed),
                "p95_processing_ns": elapsed[math.ceil(0.95 * len(elapsed)) - 1],
                "median_requests_per_second": statistics.median(
                    trial["requests_per_second"] for trial in trials
                ),
            }
        )
    return {
        "kind": "graph-sail-process-actor-benchmark",
        "schema_version": "1.0",
        "workload": {"algorithm": "mod97-keyed-accumulation-v1", **asdict(workload)},
        "repeats": repeats,
        "warmups": warmups,
        "environment": {
            "python": platform.python_version(),
            "implementation": platform.python_implementation(),
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "logical_cpus": os.cpu_count(),
        },
        "expected_output_sha256": expected_digest(workload),
        "modes": modes,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=int, default=256)
    parser.add_argument("--rounds", type=int, default=4096)
    parser.add_argument("--actors", type=int, default=2)
    parser.add_argument("--window", type=int, default=16)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    args = parser.parse_args()
    try:
        result = benchmark(
            Workload(args.requests, args.rounds, args.actors, args.window),
            repeats=args.repeats,
            warmups=args.warmups,
        )
    except ValueError as exc:
        parser.error(str(exc))
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
