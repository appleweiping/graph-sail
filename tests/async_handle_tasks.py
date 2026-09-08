"""Trusted spawn-importable async-wait test workloads, without pytest imports."""

import os
import time
from pathlib import Path


class MarkerGate:
    def read(self, entered, release):
        Path(entered).write_text(str(os.getpid()), encoding="ascii")
        deadline = time.monotonic() + 30
        while not Path(release).exists():
            if time.monotonic() >= deadline:
                raise RuntimeError("test gate was never released")
            time.sleep(0.005)
        return 9, os.getpid()

    def fail(self):
        raise ValueError("known remote failure")

    def pid(self):
        return os.getpid()
