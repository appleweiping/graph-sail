"""Spawn-importable process-stream DAG callbacks, without pytest imports."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path


def left(context):
    return context.dependencies["input"] * 2, os.getpid()


def right(context):
    return context.dependencies["input"] + 1, os.getpid()


def join(context):
    left_value, left_pid = context.dependencies["left"]
    right_value, right_pid = context.dependencies["right"]
    return left_value + right_value, (left_pid, right_pid, os.getpid())


def fail_on_two(context):
    value = context.dependencies["input"]
    if value == 2:
        raise ValueError("failed process item two")
    return value * 2, os.getpid()


@dataclass(frozen=True)
class BlockFirst:
    directory: str

    def __call__(self, context):
        value = context.dependencies["input"]
        directory = Path(self.directory)
        (directory / f"entered-{value}").write_text(str(os.getpid()), encoding="ascii")
        if value == 1:
            until = time.monotonic() + 20
            while not (directory / "release-first").exists():
                context.cancellation.raise_if_cancelled()
                if time.monotonic() >= until:
                    raise RuntimeError("first process item was not released")
                time.sleep(0.005)
        return value * 2, os.getpid()


def die_on_two(context):
    if context.dependencies["input"] == 2:
        os._exit(17)
    return context.dependencies["input"] * 2, os.getpid()
