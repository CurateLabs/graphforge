#!/usr/bin/env python3
"""Mutation tests for the concurrency stress configuration gate."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
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


def main() -> None:
    journal_phase_tests()
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
