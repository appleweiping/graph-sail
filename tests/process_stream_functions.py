"""Importable, dependency-free real-spawn generator fixtures."""

import os
import time
from pathlib import Path


def counted(context, marker, count=4):
    path = Path(marker)
    try:
        for index in range(count):
            with path.open("a", encoding="ascii") as output:
                output.write(f"next:{index}\n")
            yield os.getpid(), index
    finally:
        with path.open("a", encoding="ascii") as output:
            output.write("finally\n")


def empty(context):
    yield from ()


def failing(context):
    yield None
    raise ValueError("expected remote failure")


def cooperative(context, marker):
    try:
        yield os.getpid()
        Path(marker).write_text("entered", encoding="ascii")
        while not context.cancellation.cancelled:
            time.sleep(0.005)
        context.cancellation.raise_if_cancelled()
    finally:
        Path(marker).write_text("finally", encoding="ascii")


def stubborn(context, marker):
    try:
        yield os.getpid()
        Path(marker).write_text("entered", encoding="ascii")
        while True:
            time.sleep(0.01)
    finally:
        Path(marker).write_text("finally", encoding="ascii")


def snapshots(context, *, count=3):
    value = []
    for number in range(count):
        value.append(number)
        yield value
    return "not another yield"


def bad_yield(context, kind):
    yield "prefix"
    # The raw value fits 1 KiB, but the complete protocol frame does not.
    yield bytes(1000) if kind == "large" else lambda: None


def control(context, kind, marker):
    try:
        yield os.getpid()
        if kind == "exit":
            raise SystemExit(7)
        raise KeyboardInterrupt
    finally:
        Path(marker).write_text("finally", encoding="ascii")


def bad_close(context, marker, kind="raise"):
    try:
        yield "prefix"
    finally:
        Path(marker).write_text("entered", encoding="ascii")
        if kind == "raise":
            raise ValueError("generator cleanup failed")
        while True:
            time.sleep(0.01)


def abandon_stream_owner(connection, cancellation_sender, marker, blocked):
    """Real dying communication owner; test separately owns/reaps both PIDs."""
    from graph_sail.actors import _pack, _unpack

    assert _unpack(connection.recv_bytes(1024))[1] == "ready"
    connection.send_bytes(_pack((1, "advance", (0,), {}), 1024))
    assert _unpack(connection.recv_bytes(1024))[1] == "ok"
    if blocked:
        connection.send_bytes(_pack((2, "advance", (1,), {}), 1024))
        deadline = time.monotonic() + 20
        while not Path(marker).exists() and time.monotonic() < deadline:
            time.sleep(0.005)
        assert Path(marker).read_text(encoding="ascii") == "entered"
    # Not a graceful close: the operating system releases this process's last
    # sender endpoints. No cancellation bytes or generator-close RPC is sent.
    os._exit(0)
