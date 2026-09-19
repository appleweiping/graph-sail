"""Importable real stateful actors for the actor-method stream profile."""

import os
import threading
import time
from pathlib import Path


class Counter:
    def __init__(self, initial=0):
        self.value = initial
        self.created = 1
        self.closed = 0
        self.thread = threading.get_ident()

    def add(self, amount=0):
        self.value += amount
        return self.value

    def state(self):
        return self.value, self.closed, self.created, os.getpid(), threading.get_ident()

    def block(self, marker, release):
        Path(marker).write_text("entered")
        while not Path(release).exists():
            time.sleep(0.005)
        return self.value

    def values(self, context, count=3, failure=None, marker=None):
        try:
            for index in range(count):
                self.value += 1
                if marker:
                    Path(marker).write_text(str(self.value))
                if index == 1 and failure == "pickle":
                    yield lambda: None
                elif index == 1 and failure == "large":
                    yield b"x" * 5000
                elif index == 1 and failure == "source":
                    raise ValueError("counter source failed")
                else:
                    yield self.value
        finally:
            self.closed += 1
            self.value += 100

    def wait(self, context, marker, cooperative=True):
        try:
            Path(marker).write_text("entered")
            while not (cooperative and context.cancellation.cancelled):
                time.sleep(0.005)
            yield "late"
        finally:
            Path(marker + ".closed").write_text("closed")

    def illegal(self, context):
        try:
            yield 1
        finally:
            yield 2

    def close_wait(self, context, marker):
        try:
            yield 1
        finally:
            Path(marker).write_text("closing")
            while True:
                time.sleep(0.005)

    def control(self, context, kind):
        yield 1
        if kind == "exit":
            raise SystemExit(7)
        raise KeyboardInterrupt("child control")


class InvalidStream(Counter):
    def ordinary(self, context):
        return iter(())

    async def coroutine(self, context):
        return 1

    async def asynchronous(self, context):
        yield 1

    @staticmethod
    def static(context):
        yield 1


class NoEqualityMeta(type):
    __hash__ = type.__hash__

    def __eq__(cls, other):
        raise AssertionError("factory equality is not a protocol discriminator")


class LegacyFactory(Counter, metaclass=NoEqualityMeta):
    pass
