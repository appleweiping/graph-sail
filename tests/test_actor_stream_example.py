"""Executable source example, explicitly distinct from installed-wheel gates."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("optimized", [False, True])
def test_real_stateful_example_keeps_checks_under_optimization(tmp_path, optimized):
    root = Path(__file__).resolve().parents[1]
    command = [
        sys.executable,
        *(["-O"] if optimized else []),
        str(root / "examples" / "actor_method_stream.py"),
    ]
    completed = subprocess.run(
        command,
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(root / "src")},
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    value = json.loads(completed.stdout)
    assert value["totals"] == [15, 20]
    assert value["final_value"] == 27 and value["generator_closes"] == 2
    assert value["actor_pid"] != os.getpid()
    assert value["cancelled_actor_retired"] is True
    assert completed.stderr == ""
