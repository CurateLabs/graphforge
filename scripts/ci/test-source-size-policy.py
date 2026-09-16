#!/usr/bin/env python3
"""Real-Git regression tests for tracked source-size enforcement."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "source_size_policy.py"
SPEC = importlib.util.spec_from_file_location("source_size_policy", SCRIPT)
assert SPEC and SPEC.loader
GATE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = GATE
SPEC.loader.exec_module(GATE)
SOURCE = "crates/example/src/main.rs"
ADR = "docs/adr/0001-example.md"


class SourceSizePolicyTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.git("init", "--quiet")
        self.policy = {"default_max_lines": 3000, "exemptions": []}
        self.policy_path = self.root / "policy.json"

    def git(self, *args):
        return subprocess.run(["git", "-C", str(self.root), *args], check=True, capture_output=True)

    def write(self, path, data, tracked=True):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data if isinstance(data, bytes) else data.encode())
        if tracked:
            self.git("add", "--", path)
        return target

    def check(self):
        self.policy_path.write_text(json.dumps(self.policy), encoding="utf-8")
        return GATE.check(self.root, self.policy_path)

    def exempt(self):
        self.write(SOURCE, b"\n" * 3001)
        self.write(ADR, "# Decision\n\n**Status:** Accepted\n")
        self.policy["exemptions"] = [
            {"path": SOURCE, "max_lines": 3500, "adr": ADR, "rationale": "Bounded cohesion."}
        ]

    def test_physical_boundaries_and_final_line(self):
        for data, expected in [
            (b"", 0),
            (b"x", 1),
            (b"x\n", 1),
            (b"\r\n", 1),
            (b"x\r\ny", 2),
            (b"x\ry", 1),
            (b"\n" * 2999 + b"x", 3000),
            (b"\n" * 3000, 3000),
            (b"\n" * 3000 + b"x", 3001),
        ]:
            with self.subTest(expected=expected, tail=data[-8:]):
                self.write(SOURCE, data)
                measurements, errors = self.check()
                self.assertEqual(expected, measurements[0].lines)
                self.assertEqual(expected > 3000, bool(errors))

    def test_scope_order_all_extensions_and_untracked(self):
        included = ["crates/z/src/nested/readme.txt", "crates/a/src/build/nested.bin", SOURCE]
        for path in included:
            self.write(path, b"\xff\n")
        for path in [
            "crates/example/tests/large.rs",
            "crates/group/nested/src/file.rs",
            "src/main.rs",
        ]:
            self.write(path, b"\n" * 3001)
        self.write("crates/example/src/untracked.rs", b"\n" * 3001, tracked=False)
        measurements, errors = self.check()
        self.assertFalse(errors)
        self.assertEqual(sorted(included), [m.path for m in measurements])
        self.git("add", "crates/example/src/untracked.rs")
        self.assertTrue(self.check()[1])
        self.git("rm", "--cached", "crates/example/src/untracked.rs")
        self.assertFalse(self.check()[1])

    def test_exemption_boundary_stale_and_diagnostics(self):
        self.exempt()
        for count, problem in [(3001, None), (3500, None), (3501, "exceeds"), (3000, "obsolete")]:
            with self.subTest(count=count):
                self.write(SOURCE, b"\n" * count)
                measurements, errors = self.check()
                self.assertEqual(problem is not None, bool(errors))
                self.assertEqual(ADR, measurements[0].adr)
                if problem:
                    self.assertIn(f"{SOURCE}: measured={count} bound=3500 ADR={ADR}", errors[0])
                    self.assertIn(problem, errors[0])

    def test_missing_tracked_source(self):
        target = self.write(SOURCE, b"x")
        target.unlink()
        self.assertIn("measured=unavailable bound=3000 ADR=none", self.check()[1][0])

    def test_untracked_exemption_source_and_adr(self):
        for path, marker in [(SOURCE, "source is not tracked"), (ADR, "is not tracked")]:
            with self.subTest(path=path):
                self.exempt()
                self.git("rm", "--cached", path)
                self.assertTrue(any(marker in error for error in self.check()[1]))

    def test_accepted_adr_with_crlf(self):
        self.exempt()
        self.write(ADR, b"# Decision\r\n\r\n**Status:** Accepted\r\n")
        self.assertFalse(self.check()[1])

    def test_missing_unaccepted_and_duplicate_adr_status(self):
        self.exempt()
        (self.root / ADR).unlink()
        self.assertTrue(any("missing" in error for error in self.check()[1]))
        for text in [
            "",
            "**Status:** Proposed\n",
            "**Status:** Superseded\n",
            "**Status:**\nAccepted\n",
            "**Status:** Accepted\n**Status:** Accepted\n",
        ]:
            with self.subTest(text=text):
                self.write(ADR, text)
                self.assertTrue(any("Accepted line" in error for error in self.check()[1]))

    def test_malformed_json_and_duplicate_keys(self):
        for text in [
            "{",
            '{"default_max_lines":3000,"default_max_lines":4000,"exemptions":[]}',
            '{"default_max_lines":3000,"exemptions":[{"path":"x","path":"y"}]}',
        ]:
            with self.subTest(text=text):
                self.policy_path.write_text(text)
                with self.assertRaises(GATE.PolicyError):
                    GATE.check(self.root, self.policy_path)

    def test_invalid_policy_shapes_and_bounds(self):
        for policy in [
            None,
            [],
            {},
            {"default_max_lines": 3000, "exemptions": {}, "extra": 1},
            {"default_max_lines": 3000, "exemptions": {}},
        ]:
            with self.subTest(policy=policy):
                self.policy = policy
                with self.assertRaises(GATE.PolicyError):
                    self.check()
        for bound in [True, False, None, 0, -1, 3000.0, "3000", float("inf"), float("nan")]:
            with self.subTest(bound=bound):
                self.policy = {"default_max_lines": bound, "exemptions": []}
                with self.assertRaises(GATE.PolicyError):
                    self.check()

    def test_invalid_exemptions(self):
        self.exempt()
        original = copy.deepcopy(self.policy)
        changes = [
            ("path", value)
            for value in [
                None,
                "",
                "/tmp/x",
                "crates/a/src/../x",
                "crates/a/src//x",
                "crates/a/src/*.rs",
                "crates/a/src/./x",
                "crates/a/tests/x",
                "crates/a/src/x\\y",
                "crates/a/src/x#anchor",
            ]
        ]
        changes += [("max_lines", value) for value in [True, 0, -1, 3000, 3500.5, "3500", None]]
        changes += [
            ("adr", value)
            for value in [None, "", "docs/adr/../x.md", "docs/adr/x.md#anchor", "docs/x.md"]
        ]
        changes += [("rationale", value) for value in [None, "", " ", 1]]
        for key, value in changes:
            with self.subTest(key=key, value=value):
                self.policy = copy.deepcopy(original)
                self.policy["exemptions"][0][key] = value
                with self.assertRaises(GATE.PolicyError):
                    self.check()
        self.policy = original
        self.policy["exemptions"] *= 2
        with self.assertRaisesRegex(GATE.PolicyError, "duplicate exemption"):
            self.check()

    def test_symlink_sources_and_adrs_are_rejected(self):
        for path in [SOURCE, ADR]:
            with self.subTest(path=path):
                self.exempt()
                target = self.root / path
                data = target.read_bytes()
                target.unlink()
                destination = self.write("retained.txt", data, tracked=False)
                target.symlink_to(destination)
                self.assertTrue(any("non-regular" in error for error in self.check()[1]))
                target.unlink()

    def test_repository_integration_is_unconditional(self):
        repository = SCRIPT.parents[1]
        workflow = (repository / ".github/workflows/test.yml").read_text()
        job = re.search(r"^  policy:\n(.*?)(?=^  [\w-]+:|\Z)", workflow, re.M | re.S)
        self.assertIsNotNone(job)
        body = job.group(1)
        self.assertNotRegex(body, r"(?m)^    if:")
        steps = re.split(r"(?m)^      - ", body)
        makefile = (repository / "Makefile").read_text()
        recipe = re.search(r"^pre-push-fast:[^\n]*\n(.*?)(?=^\S|\Z)", makefile, re.M | re.S)
        self.assertIsNotNone(recipe)
        for command in [
            "python3 scripts/source_size_policy.py",
            "python3 scripts/ci/test-source-size-policy.py",
        ]:
            matching = [step for step in steps if command in step]
            self.assertEqual(1, len(matching))
            self.assertNotRegex(matching[0], r"(?m)^        if:")
            self.assertIn("\t@" + command + "\n", recipe.group(1))

    def test_cli_reports_sorted_failures_and_inventory(self):
        for path in ["crates/z/src/a.rs", "crates/a/src/a.txt"]:
            self.write(path, b"\n" * 3001)
        self.check()
        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--root",
                str(self.root),
                "--policy",
                "policy.json",
                "--inventory",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(1, result.returncode)
        self.assertEqual(sorted(result.stderr.splitlines()), result.stderr.splitlines())
        self.assertIn("2 files checked; 2 violations", result.stdout)
        self.assertIn("measured=3001 bound=3000 ADR=none", result.stderr)


if __name__ == "__main__":
    unittest.main()
