"""Spawn-importable workloads, deliberately independent of the pytest harness.

Their first invocation should measure application work, not import pytest and
reconstruct the entire test module within the production invocation budget.
"""

from __future__ import annotations

import hashlib
import os
import time
from dataclasses import dataclass
from pathlib import Path

from graph_sail import LocalObjectClient, TaskCancelled


def pid_task(context):
    return (os.getpid(), context.node_id, context.attempt, dict(context.dependencies))


def multiply(context):
    return context.dependencies["a"] * context.dependencies["b"]


@dataclass(frozen=True)
class Value:
    value: object

    def __call__(self, context):
        return self.value


@dataclass(frozen=True)
class Rendezvous:
    directory: str

    def __call__(self, context):
        directory = Path(self.directory)
        (directory / context.node_id).write_text(str(os.getpid()), encoding="ascii")
        deadline = time.monotonic() + 10
        while not all((directory / name).exists() for name in ("a", "b")):
            if time.monotonic() > deadline:
                raise RuntimeError("workers did not actually overlap")
            time.sleep(0.005)
        return 6 if context.node_id == "a" else 7


@dataclass(frozen=True)
class StopTask:
    entered: str
    observed: str
    cooperative: bool = True
    translate: bool = False

    def __call__(self, context):
        Path(self.entered).write_text(str(os.getpid()), encoding="ascii")
        while True:
            if self.cooperative and context.cancellation.cancelled:
                Path(self.observed).write_text("stopped", encoding="ascii")
                if self.translate:
                    return "late success must not override cancellation"
                context.cancellation.raise_if_cancelled()
            time.sleep(0.002)


def retry_value_error(context):
    if context.attempt == 1:
        raise ValueError("retry once")
    return (42, os.getpid())


def exit_worker(context):
    os._exit(17)


def mutate_dependency(context):
    context.dependencies["a"].append(9)
    return context.dependencies["a"]


def spontaneously_cancel(context):
    raise TaskCancelled("application stopped")


def unpicklable_result(context):
    return lambda: 3


@dataclass(frozen=True)
class ReadObject:
    client: LocalObjectClient

    def __call__(self, context):
        data = self.client.get(context.dependencies["a"])
        return (len(data), sum(data), hashlib.sha256(data).hexdigest(), os.getpid())


def wait_for(path: Path, seconds=15):
    deadline = time.monotonic() + seconds
    while not path.exists():
        if time.monotonic() > deadline:
            raise AssertionError(f"child did not create {path}")
        time.sleep(0.005)


@dataclass(frozen=True)
class FailAfterOtherEntered:
    other: str

    def __call__(self, context):
        wait_for(Path(self.other))
        raise ValueError("fail-fast trigger")


def abandon_endpoint(sender, entered, release):
    Path(entered).write_text("ready", encoding="ascii")
    wait_for(Path(release))
    # Deliberately bypass Python finalizers: the OS must close this sole writer.
    os._exit(0)
