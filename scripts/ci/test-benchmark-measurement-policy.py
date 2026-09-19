#!/usr/bin/env python3
"""Mutation tests for the benchmark measurement policy guard."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "benchmark_measurement_policy",
    ROOT / "scripts/ci/benchmark-measurement-policy.py",
)
assert SPEC and SPEC.loader
POLICY = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = POLICY
SPEC.loader.exec_module(POLICY)

POLICY_DOC = ROOT / "docs/development/benchmarking.md"
CANONICAL_BENCH = ROOT / "crates/graphforge-core/benches/canonical.rs"

CUSTOM_TIMER_FIXTURE = (
    "fn sample() {\n    let start = std::time::Instant::now();\n    let _ = start.elapsed();\n}\n"
)
LEGACY_TIMER_WITH_MEDIAN_FIXTURE = (
    "fn median_expand() {}\n"
    "fn sample() {\n"
    "    let start = std::time::Instant::now();\n"
    "    let _ = start.elapsed();\n"
    "}\n"
)
DEADLINE_TIMER_FIXTURE = (
    "pub fn wait() {\n"
    "    let start = std::time::Instant::now();\n"
    "    while start.elapsed().as_secs() < 1 {}\n"
    "}\n"
)

MINIMAL_INVENTORY = {
    "version": 1,
    "policy_doc": "docs/development/benchmarking.md",
    "sites": [
        {
            "path": "crates/graphforge-core/benches/canonical.rs",
            "boundary": "in_process",
            "authority": "divan",
            "disposition": "framework_authority",
            "owner_issue": None,
            "notes": "fixture divan-only bench",
        },
        {
            "path": "crates/graphforge-exec/tests/merge_scaling_bench.rs",
            "boundary": "in_process",
            "authority": "custom",
            "disposition": "migrate",
            "owner_issue": 1485,
            "allowed_signals": ["custom_wall_clock"],
            "notes": "fixture legacy merge timer",
        },
    ],
}


class BenchmarkMeasurementPolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self._git("init", "-q")
        self._git("config", "user.name", "GraphForge CI")
        self._git("config", "user.email", "ci@graphforge.invalid")
        (self.root / "config").mkdir(parents=True, exist_ok=True)
        (self.root / "docs/development").mkdir(parents=True, exist_ok=True)
        shutil.copy2(POLICY_DOC, self.root / "docs/development/benchmarking.md")
        self._write_inventory(MINIMAL_INVENTORY)
        for site in MINIMAL_INVENTORY["sites"]:
            path = site["path"]
            if not path.endswith("/"):
                self._write(path, "// fixture stub\n")
        self._git("add", ".")
        self._git("commit", "-qm", "seed policy fixtures")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _git(self, *args: str) -> None:
        subprocess.run(
            ["git", "-C", str(self.root), *args],
            check=True,
            capture_output=True,
        )

    def _write(self, relative: str, content: str) -> None:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    def _write_inventory(self, payload: dict) -> None:
        (self.root / "config/benchmark-measurement-inventory.json").write_text(
            json.dumps(payload, indent=2) + "\n"
        )

    def assert_passes(self) -> None:
        _, errors = POLICY.validate(self.root)
        self.assertEqual(errors, [])

    def assert_rejected(self, expected: str) -> None:
        _, errors = POLICY.validate(self.root)
        self.assertTrue(any(expected in error for error in errors), errors)

    def test_live_repository_inventory_passes(self) -> None:
        _, errors = POLICY.validate(ROOT)
        self.assertEqual(errors, [])

    def test_divan_only_bench_passes_without_custom_signals(self) -> None:
        relative = "crates/graphforge-core/benches/canonical.rs"
        self._write(relative, CANONICAL_BENCH.read_text())
        self._git("add", relative)
        self.assert_passes()

    def test_unclassified_custom_timer_is_rejected(self) -> None:
        relative = "crates/graphforge-exec/tests/bench_new_workload.rs"
        self._write(relative, CUSTOM_TIMER_FIXTURE)
        self._git("add", relative)
        self.assert_rejected("unclassified benchmark measurement machinery")

    def test_reviewed_legacy_exception_passes(self) -> None:
        relative = "crates/graphforge-exec/tests/merge_scaling_bench.rs"
        self._write(relative, CUSTOM_TIMER_FIXTURE)
        self._git("add", relative)
        self.assert_passes()

    def test_stale_inventory_entry_is_rejected(self) -> None:
        inventory = dict(MINIMAL_INVENTORY)
        inventory["sites"] = [
            *MINIMAL_INVENTORY["sites"],
            {
                "path": "crates/missing/benches/ghost.rs",
                "boundary": "in_process",
                "authority": "custom",
                "disposition": "migrate",
                "owner_issue": 1485,
                "allowed_signals": ["custom_wall_clock"],
                "notes": "fixture stale entry",
            },
        ]
        self._write_inventory(inventory)
        self._git("add", "config/benchmark-measurement-inventory.json")
        self.assert_rejected("stale inventory entry")

    def test_untracked_exception_signal_is_rejected(self) -> None:
        relative = "crates/graphforge-exec/tests/merge_scaling_bench.rs"
        self._write(relative, LEGACY_TIMER_WITH_MEDIAN_FIXTURE)
        self._git("add", relative)
        self.assert_rejected("homegrown_statistics is not allowed")

    def test_deadline_style_instant_outside_scan_surface_passes(self) -> None:
        relative = "crates/graphforge-exec/src/deadline.rs"
        self._write(relative, DEADLINE_TIMER_FIXTURE)
        self._git("add", relative)
        self.assert_passes()


if __name__ == "__main__":
    unittest.main()
