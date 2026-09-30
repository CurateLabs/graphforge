#!/usr/bin/env python3
"""Mutation tests for the concurrency stress configuration gate."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/ci/concurrency-stress-gate.py"
SPEC = importlib.util.spec_from_file_location("concurrency_stress_gate", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise SystemExit("cannot load concurrency stress gate")
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


def product_journal_phases() -> set[str]:
    """Read the serialized `JournalPhase` vocabulary from the storage crate."""
    control = ROOT / "crates/graphforge-storage/src/project_publication/control.rs"
    source = control.read_text(encoding="utf-8")
    declared = re.search(
        r'#\[serde\(rename_all = "SCREAMING_SNAKE_CASE"\)\]\n'
        r"pub\(crate\) enum JournalPhase \{\n(?P<body>.*?)\n\}",
        source,
        re.DOTALL,
    )
    assert declared, "JournalPhase is no longer a SCREAMING_SNAKE_CASE enum in control.rs"
    variants = re.findall(r"^\s*([A-Z][A-Za-z]+),$", declared.group("body"), re.MULTILINE)
    return {re.sub(r"(?<!^)(?=[A-Z])", "_", name).upper() for name in variants}


def journal_phase_tests() -> None:
    known = GATE.TERMINAL_JOURNAL_PHASES | GATE.IN_FLIGHT_JOURNAL_PHASES
    assert not GATE.TERMINAL_JOURNAL_PHASES & GATE.IN_FLIGHT_JOURNAL_PHASES
    assert known == product_journal_phases(), (
        f"stress gate journal phases {sorted(known)} drifted from the product "
        f"{sorted(product_journal_phases())}"
    )
    with tempfile.TemporaryDirectory() as directory:
        project = Path(directory)
        GATE.require_terminal_journals(project)
        transactions = project / "transactions"
        transactions.mkdir()
        journal = transactions / "journal.json"
        for phase in sorted(GATE.TERMINAL_JOURNAL_PHASES):
            journal.write_text(json.dumps({"phase": phase}), encoding="utf-8")
            GATE.require_terminal_journals(project)
        for phase in [*sorted(GATE.IN_FLIGHT_JOURNAL_PHASES), "COMMITTED", None]:
            journal.write_text(json.dumps({"phase": phase}), encoding="utf-8")
            try:
                GATE.require_terminal_journals(project)
            except GATE.GateError:
                continue
            raise AssertionError(f"in-flight or unknown journal phase was accepted: {phase!r}")
        journal.write_text("{", encoding="utf-8")
        try:
            GATE.require_terminal_journals(project)
        except GATE.GateError:
            pass
        else:
            raise AssertionError("unreadable journal was accepted")


def memory_measurement_tests() -> None:
    """A large earlier child must not leak into a later child's measurement."""
    megabyte = 1024 * 1024
    large = GATE.run_measured(
        [sys.executable, "-c", "block = bytearray(256 * 1024 * 1024); print(len(block))"],
        cwd=ROOT,
        env=None,
        timeout=60,
    )
    small = GATE.run_measured([sys.executable, "-c", "print('ok')"], cwd=ROOT, env=None, timeout=60)
    assert large.returncode == 0 and small.returncode == 0
    assert small.stdout.strip() == "ok"
    assert large.peak_rss_bytes >= 256 * megabyte, large.peak_rss_bytes
    assert small.peak_rss_bytes < 128 * megabyte, (
        f"a later workload inherited an earlier child's peak: {small.peak_rss_bytes}"
    )
    failed = GATE.run_measured(
        [sys.executable, "-c", "import sys; sys.stderr.write('boom'); sys.exit(3)"],
        cwd=ROOT,
        env=None,
        timeout=60,
    )
    assert failed.returncode == 3 and failed.stderr == "boom"
    try:
        GATE.run_measured(
            [sys.executable, "-c", "import time; time.sleep(30)"], cwd=ROOT, env=None, timeout=1
        )
    except subprocess.TimeoutExpired:
        pass
    else:
        raise AssertionError("a hung workload was not timed out")
    bound = GATE.RSS_GROWTH_BOUND_BYTES
    GATE.require_bounded_rss([{"case": "within", "peak_rss_bytes": bound}])
    try:
        GATE.require_bounded_rss(
            [{"case": "within", "peak_rss_bytes": 1}, {"case": "over", "peak_rss_bytes": bound + 1}]
        )
    except GATE.GateError as error:
        assert "case=over" in str(error)
    else:
        raise AssertionError("an over-bound workload was accepted")


def main() -> None:
    journal_phase_tests()
    memory_measurement_tests()
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
