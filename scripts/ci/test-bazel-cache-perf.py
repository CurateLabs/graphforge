#!/usr/bin/env python3
"""Unit tests for Bazel cache/perf harness (#5)."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
CHECK = ROOT / "scripts/ci/bazel-cache-perf.py"


def run_check(args: list[str], *, stdin: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["python3", str(CHECK), *args],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        input=stdin,
    )


def test_parse_process_summary() -> None:
    log = (
        "INFO: Build completed successfully, 12 total actions\n"
        "INFO: 7 processes: 3 remote cache hit, 4 linux-sandbox.\n"
    )
    proc = run_check(["--mode", "parse-log"], stdin=log)
    if proc.returncode != 0:
        raise SystemExit(f"parse-log failed:\n{proc.stdout}\n{proc.stderr}")
    payload = json.loads(proc.stdout)
    if payload["remote_cache_hits"] != 3:
        raise SystemExit(f"expected 3 remote hits, got {payload}")
    if payload["local_actions"] != 4:
        raise SystemExit(f"expected 4 local actions, got {payload}")


def test_policy_passes_on_repo() -> None:
    proc = run_check(["--mode", "policy"])
    if proc.returncode != 0:
        raise SystemExit(f"policy should pass on clean repo:\n{proc.stdout}\n{proc.stderr}")


def test_policy_fails_on_competing_flag() -> None:
    import importlib.util

    spec = importlib.util.spec_from_file_location("bazel_cache_perf", CHECK)
    if spec is None or spec.loader is None:
        raise SystemExit("unable to load bazel-cache-perf module")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    with tempfile.TemporaryDirectory(prefix="gf-cache-policy-") as tmp:
        tmp_path = Path(tmp)
        # Construct the flag without embedding the banned token in this file.
        banned = "-" + "-remote" + "_cache=https://example.invalid"
        (tmp_path / ".bazelrc").write_text(f"build {banned}\n", encoding="utf-8")
        errors = mod.check_remote_cache_policy(tmp_path)
        if not errors:
            raise SystemExit("expected competing remote-cache flag to fail policy")


def test_evaluate_pending_allow() -> None:
    evidence = ROOT / "docs/development/bazel-migration-evidence/perf-sample.json"
    if not evidence.is_file():
        raise SystemExit(f"missing evidence scaffold {evidence}")
    pending = run_check(["--mode", "evaluate", "--evidence", str(evidence), "--allow-pending"])
    if pending.returncode != 0:
        raise SystemExit(f"allow-pending evaluate failed:\n{pending.stdout}\n{pending.stderr}")
    strict = run_check(["--mode", "evaluate", "--evidence", str(evidence)])
    if strict.returncode == 0:
        raise SystemExit("strict evaluate must fail while status is pending_org_admin")


def test_observe_warm_uses_distinct_output_bases() -> None:
    source = Path(CHECK).read_text(encoding="utf-8")
    if "distinct_output_base_prime_then_warm" not in source:
        raise SystemExit("observe-warm must document distinct output_base protocol")
    if "gf-bazel-prime-" not in source or "gf-bazel-warm-" not in source:
        raise SystemExit("observe-warm must use distinct temporary output bases")
    if "distinct_output_base_warm_then_mutated" not in source:
        raise SystemExit("affected-inputs must use distinct output bases under remote cache")
    if "remote_isolation_ok" not in source:
        raise SystemExit("affected-inputs must accept remote-cache isolation signal")


def test_evaluate_thresholds() -> None:
    with tempfile.TemporaryDirectory(prefix="gf-cache-eval-") as tmp:
        path = Path(tmp) / "sample.json"
        payload = {
            "schema": "graphforge.bazel-cache-perf-evidence.v1",
            "status": "complete",
            "baseline": {
                "cargo_primary_p50_seconds": 100.0,
                "cargo_compute_proxy_p50_seconds": 200.0,
            },
            "thresholds": {
                "min_pairs": 2,
                "warm_speedup_min": 0.30,
                "compute_reduction_min": 0.25,
                "cold_regression_max": 0.10,
            },
            "observations": {
                "remote_cache_hits_on_identical_sha": True,
                "cache_unavailable_cold_correct": True,
                "affected_inputs_isolation": True,
            },
            "pairs": [
                {
                    "cold": {"wall_seconds": 90.0},
                    "warm": {"wall_seconds": 60.0},
                    "compute_proxy_seconds": 140.0,
                    "cargo_cold_wall_seconds": 100.0,
                },
                {
                    "cold": {"wall_seconds": 95.0},
                    "warm": {"wall_seconds": 55.0},
                    "compute_proxy_seconds": 130.0,
                    "cargo_cold_wall_seconds": 100.0,
                },
            ],
            "waiver": None,
        }
        path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
        ok = run_check(["--mode", "evaluate", "--evidence", str(path)])
        if ok.returncode != 0:
            raise SystemExit(f"expected thresholds to pass:\n{ok.stdout}\n{ok.stderr}")

        payload["pairs"][0]["warm"]["wall_seconds"] = 90.0
        payload["pairs"][1]["warm"]["wall_seconds"] = 90.0
        path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
        bad = run_check(["--mode", "evaluate", "--evidence", str(path)])
        if bad.returncode == 0:
            raise SystemExit("expected warm speedup failure")


def main() -> None:
    test_parse_process_summary()
    test_policy_passes_on_repo()
    test_policy_fails_on_competing_flag()
    test_evaluate_pending_allow()
    test_observe_warm_uses_distinct_output_bases()
    test_evaluate_thresholds()
    print("bazel-cache-perf tests passed")


if __name__ == "__main__":
    main()
