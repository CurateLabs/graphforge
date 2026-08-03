#!/usr/bin/env python3
"""Deterministic mutation sentinels for the Rust coverage ledger."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "coverage_rust_ledger", ROOT / "scripts" / "coverage_rust_ledger.py"
)
assert SPEC is not None and SPEC.loader is not None
ledger_module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ledger_module)


def expect_error(fragment: str, call) -> None:
    try:
        call()
    except ledger_module.LedgerError as error:
        assert fragment in str(error), (fragment, str(error))
    else:
        raise AssertionError(f"expected LedgerError containing {fragment!r}")


def main() -> None:
    head = ledger_module.git_head(ROOT)
    with tempfile.TemporaryDirectory(prefix="gf-rust-coverage-ledger-") as directory:
        temp = Path(directory)
        artifact = temp / "adapter.so"
        runtime = temp / "loaded.so"
        profile = temp / "adapter.profraw"
        artifact.write_bytes(b"same instrumented artifact")
        runtime.write_bytes(artifact.read_bytes())
        time.sleep(0.01)
        profile.write_bytes(b"non-empty profile")
        artifact_hash = ledger_module.sha256(artifact)
        evidence = {
            "source_sha": head,
            "rustc": "rustc test",
            "cargo_llvm_cov": "cargo-llvm-cov test",
            "surfaces": {
                name: {
                    "artifact": str(artifact),
                    "runtime_artifact": str(runtime),
                    "artifact_sha256": artifact_hash,
                    "profiles": [str(profile)],
                }
                for name in ("python_adapter", "node_adapter")
            },
        }

        ledger_module.validate_evidence(evidence, ROOT, head)

        mutated = json.loads(json.dumps(evidence))
        mutated["source_sha"] = "0" * 40
        expect_error("source SHA", lambda: ledger_module.validate_evidence(mutated, ROOT, head))

        mutated = json.loads(json.dumps(evidence))
        mutated["surfaces"].pop("node_adapter")
        expect_error(
            "missing node_adapter", lambda: ledger_module.validate_evidence(mutated, ROOT, head)
        )

        empty = temp / "empty.profraw"
        empty.touch()
        mutated = json.loads(json.dumps(evidence))
        mutated["surfaces"]["python_adapter"]["profiles"] = [str(empty)]
        expect_error(
            "missing or empty", lambda: ledger_module.validate_evidence(mutated, ROOT, head)
        )

        stale = temp / "stale.profraw"
        stale.write_bytes(b"old profile")
        old = artifact.stat().st_mtime - 10
        os.utime(stale, (old, old))
        mutated = json.loads(json.dumps(evidence))
        mutated["surfaces"]["node_adapter"]["profiles"] = [str(stale)]
        expect_error("predates", lambda: ledger_module.validate_evidence(mutated, ROOT, head))

        mutated = json.loads(json.dumps(evidence))
        mutated["surfaces"]["python_adapter"]["artifact_sha256"] = "bad"
        expect_error("artifact hash", lambda: ledger_module.validate_evidence(mutated, ROOT, head))

        lcov = temp / "valid.lcov"
        lcov.write_text(
            f"SF:{ROOT / 'crates/graphforge-api/src/lib.rs'}\nDA:1,1\nend_of_record\n",
            encoding="utf-8",
        )
        assert ledger_module.parse_lcov(lcov, ROOT)
        malformed = temp / "malformed.lcov"
        malformed.write_text("SF:no-lines.rs\nend_of_record\n", encoding="utf-8")
        expect_error("no executable lines", lambda: ledger_module.parse_lcov(malformed, ROOT))

        args = SimpleNamespace(
            root=ROOT,
            core_floor=85.0,
            python_floor=80.0,
            node_floor=80.0,
        )
        valid_ledger = {
            "schema_version": 1,
            "source_sha": head,
            "surfaces": {
                name: {"measured_lines": 100, "line_percent": 90.0}
                for name in ("core", "python_adapter", "node_adapter")
            },
        }
        ledger_module.validate_floors(valid_ledger, args)

        mutated = json.loads(json.dumps(valid_ledger))
        mutated["surfaces"]["python_adapter"]["line_percent"] = 79.99
        expect_error(
            "python_adapter Rust coverage below",
            lambda: ledger_module.validate_floors(mutated, args),
        )

        mutated = json.loads(json.dumps(valid_ledger))
        mutated["surfaces"]["node_adapter"]["measured_lines"] = 0
        expect_error("malformed node_adapter", lambda: ledger_module.validate_floors(mutated, args))

        mutated = json.loads(json.dumps(valid_ledger))
        mutated["schema_version"] = 99
        expect_error("unsupported or missing", lambda: ledger_module.validate_floors(mutated, args))

    print("rust coverage ledger mutation sentinels: ok")


if __name__ == "__main__":
    main()
