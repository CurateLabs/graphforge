#!/usr/bin/env python3
"""Regression coverage for release-load probe cross-binding identity."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import subprocess
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[2]
NODE_PROBE = ROOT / "crates/graphforge-bindings-node/tests/release-load-probe.mjs"
RUST_PROBE = ROOT / "crates/graphforge-api/examples/release_load_probe.rs"
PYTHON_PROBE = ROOT / "scripts/ci/release-load-python-probe.py"

NODE_OPERATION = "018f0f4e-7b8c-7000-8000-00000000b001"
EDGE_OPERATION = "018f0f4e-7b8c-7000-8000-00000000b002"

# Whole-number floats must keep a decimal so Node matches Python/Rust JSON.
RANK_SAMPLE = [["n-00000001", 2.0], ["n-00000002", 1.0], ["n-00000003", 3.0]]
RANK_SAMPLE_DIGEST = "e6bb4cb9ca052f710659220da812df8ce3fc37f73e181071379cf8ebf89f2ebf"


def python_fingerprint(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


class ReleaseLoadProbeParityTests(unittest.TestCase):
    def test_python_whole_float_json_keeps_decimal(self) -> None:
        encoded = json.dumps(RANK_SAMPLE, separators=(",", ":"))
        self.assertEqual(encoded, '[["n-00000001",2.0],["n-00000002",1.0],["n-00000003",3.0]]')
        self.assertEqual(python_fingerprint(RANK_SAMPLE), RANK_SAMPLE_DIGEST)

    def test_node_canonical_float_fingerprint_matches_python(self) -> None:
        node_source = NODE_PROBE.read_text(encoding="utf-8")
        self.assertIn("function canonicalJson(value)", node_source)
        self.assertIn("Number.isInteger(value)", node_source)
        self.assertIn("${value}.0", node_source)
        self.assertNotIn(
            'createHash("sha256").update(JSON.stringify(value))',
            node_source,
        )

        script = textwrap.dedent(
            r"""
            import { createHash } from "node:crypto";
            function canonicalJson(value) {
              if (value === null) return "null";
              if (typeof value === "boolean") return value ? "true" : "false";
              if (typeof value === "number") {
                if (!Number.isFinite(value)) {
                  throw new Error(`non-finite fingerprint number: ${value}`);
                }
                if (Object.is(value, -0)) return "-0.0";
                if (Number.isInteger(value)) return `${value}.0`;
                return String(value);
              }
              if (typeof value === "string") return JSON.stringify(value);
              if (Array.isArray(value)) {
                return `[${value.map((item) => canonicalJson(item)).join(",")}]`;
              }
              if (typeof value === "object") {
                const keys = Object.keys(value).sort();
                return `{${keys
                  .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
                  .join(",")}}`;
              }
              throw new Error(`unsupported fingerprint value: ${typeof value}`);
            }
            const sample = [
              ["n-00000001", 2.0],
              ["n-00000002", 1.0],
              ["n-00000003", 3.0],
            ];
            if (JSON.stringify(sample) === canonicalJson(sample)) {
              throw new Error("canonicalJson must keep .0 on whole floats");
            }
            process.stdout.write(
              createHash("sha256").update(canonicalJson(sample)).digest("hex"),
            );
            """
        )
        digest = subprocess.check_output(
            ["node", "--input-type=module", "-e", script],
            cwd=ROOT,
            text=True,
        ).strip()
        self.assertEqual(digest, RANK_SAMPLE_DIGEST)
        self.assertEqual(digest, python_fingerprint(RANK_SAMPLE))

    def test_probes_share_fixed_bulk_operation_uuids(self) -> None:
        python_source = PYTHON_PROBE.read_text(encoding="utf-8")
        node_source = NODE_PROBE.read_text(encoding="utf-8")
        rust_source = RUST_PROBE.read_text(encoding="utf-8")
        for source in (python_source, node_source, rust_source):
            self.assertIn(NODE_OPERATION, source)
            self.assertIn(EDGE_OPERATION, source)
        self.assertNotRegex(
            rust_source,
            r"publish_bulk_(?:nodes|edges)\(OperationId\(Uuid::now_v7\(\)\)",
        )
        self.assertRegex(
            rust_source,
            rf'const NODE_OPERATION: &str = "{re.escape(NODE_OPERATION)}"',
        )
        self.assertRegex(
            rust_source,
            rf'const EDGE_OPERATION: &str = "{re.escape(EDGE_OPERATION)}"',
        )


if __name__ == "__main__":
    unittest.main()
