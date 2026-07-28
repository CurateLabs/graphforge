#!/usr/bin/env python3
"""Mutation tests for the concurrency stress configuration gate."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/ci/concurrency-stress-gate.py"
SPEC = importlib.util.spec_from_file_location("concurrency_stress_gate", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise SystemExit("cannot load concurrency stress gate")
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


def main() -> None:
    GATE.validate_config(GATE.DEFAULT_SEED, GATE.DEFAULT_ITERATIONS, GATE.DEFAULT_TIMEOUT_SECONDS)
    try:
        GATE.validate_config(
            GATE.DEFAULT_SEED + 1, GATE.DEFAULT_ITERATIONS, GATE.DEFAULT_TIMEOUT_SECONDS
        )
    except GATE.GateError:
        pass
    else:
        raise AssertionError("altered stress seed was accepted")
    try:
        GATE.validate_config(GATE.DEFAULT_SEED, 0, GATE.DEFAULT_TIMEOUT_SECONDS)
    except GATE.GateError:
        pass
    else:
        raise AssertionError("zero iterations were accepted")
    assert GATE.RSS_GROWTH_BOUND_BYTES > 0
    assert GATE.FD_GROWTH_BOUND > 0
    print("concurrency stress gate mutation tests passed")


if __name__ == "__main__":
    main()
