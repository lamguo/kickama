#!/usr/bin/env python3
"""Smoke checks for deploy.py total timeout handling."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

import deploy  # noqa: E402


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> int:
    require(deploy.non_negative_timeout("0") == 0, "zero timeout should be accepted")
    require(deploy.non_negative_timeout("15") == 15, "positive timeout should be accepted")
    try:
        deploy.non_negative_timeout("-1")
    except argparse.ArgumentTypeError:
        pass
    else:
        raise AssertionError("negative timeout should be rejected")

    original_run = deploy.subprocess.run
    try:
        recorded_timeout: dict[str, int] = {}

        def fake_run(cmd, **kwargs):
            recorded_timeout["value"] = kwargs["timeout"]
            return subprocess.CompletedProcess(cmd, 0, stdout="ok", stderr="")

        deploy.subprocess.run = fake_run
        deploy.set_deployment_timeout(7)
        code, output = deploy.run_command(["echo", "ok"], capture=True)
        require(code == 0, output)
        require(1 <= recorded_timeout["value"] <= 7, str(recorded_timeout))

        deploy.DEPLOYMENT_DEADLINE = time.monotonic() - 1
        code, output = deploy.run_command(["echo", "late"], capture=True)
        require(code == -1, str(code))
        require("timed out before command could start" in output, output)
    finally:
        deploy.subprocess.run = original_run
        deploy.set_deployment_timeout(0)

    print("deploy timeout checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
