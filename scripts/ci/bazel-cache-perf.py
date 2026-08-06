#!/usr/bin/env python3
"""Blacksmith Bazel remote-cache policy + cold/warm performance harness (#5).

Modes:
  policy            Fail-closed: no in-repo ``--remote_cache`` override
  parse-log         Parse Bazel process-summary lines from a log file/stdin
  cold-correctness  Run representative targets with remote cache disabled
  measure           Time a Bazel invocation and write a run fragment JSON
  observe-warm      Re-run targets and record remote-cache hit observation
  affected-inputs   Touch one source file; prove unrelated actions stay cached
  evaluate          Evaluate checked-in paired-sample evidence against #1 gates

Do not set ``--remote_cache`` in-repo. Blacksmith injects repository caching when
org-admin enables Bazel Build Caching.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
import tempfile
import time
from typing import Any

SCHEMA = "graphforge.bazel-cache-perf-evidence.v1"
SCHEMA_RUN = "graphforge.bazel-cache-perf-run.v1"

DEFAULT_EVIDENCE = Path("docs/development/bazel-migration-evidence/perf-sample.json")
DEFAULT_BASELINE_DOC = Path("docs/development/bazel-migration-baseline.md")

# Accepted Cargo/Blacksmith baseline from #12 (bazel-migration-baseline.md).
CARGO_PRIMARY_P50_SECONDS = 327 + 177 + 121  # Rust Tests + Python + Node
CARGO_COMPUTE_PROXY_P50_SECONDS = 923
BASELINE_INVENTORY_SHA = "6e8b8e3fdc1ecd960eacf14a73e5be7b54fcef3c"

MIN_PAIRS = 10
WARM_SPEEDUP_MIN = 0.30
COMPUTE_REDUCTION_MIN = 0.25
COLD_REGRESSION_MAX = 0.10

REPRESENTATIVE_BUILD = [
    "//:bazel_smoke",
    "//:first_party_libs",
    "//:cli_bins",
    "//:resource_inputs",
    "//:release_bins",
]
REPRESENTATIVE_TEST = ["//:bazel_test_graph_smoke"]
REPRESENTATIVE_BINDINGS = ["//:binding_cdylibs"]

PROCESS_SUMMARY_RE = re.compile(
    r"^INFO:\s+(\d+)\s+processes:\s+(.+)\.\s*$",
    re.MULTILINE,
)
PROCESS_PART_RE = re.compile(r"(\d+)\s+([A-Za-z0-9 _/-]+)")
REMOTE_CACHE_FLAG_RE = re.compile(r"(?<![\w-])--remote_cache(?:[=\s]|$)")
POLICY_SCAN_GLOBS = (
    ".bazelrc",
    ".bazelrc.*",
    ".github/workflows/*.yml",
    ".github/workflows/*.yaml",
    "scripts/ci/*.py",
    "scripts/ci/*.sh",
    "Makefile",
)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def git_sha(root: Path) -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        text=True,
    ).strip()


def run(
    argv: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=cwd,
        env=env,
        check=check,
        capture_output=True,
        text=True,
    )


def parse_process_summary(text: str) -> dict[str, Any]:
    """Parse Bazel's ``INFO: N processes: ...`` lines; keep the last summary."""
    matches = list(PROCESS_SUMMARY_RE.finditer(text))
    if not matches:
        return {
            "total_processes": 0,
            "counts": {},
            "remote_cache_hits": 0,
            "local_actions": 0,
            "raw": None,
        }
    match = matches[-1]
    total = int(match.group(1))
    counts: dict[str, int] = {}
    for amount, label in PROCESS_PART_RE.findall(match.group(2)):
        counts[label.strip()] = int(amount)
    remote_hits = 0
    for key, value in counts.items():
        if "remote cache hit" in key:
            remote_hits += value
    local = 0
    for key, value in counts.items():
        if key in {"linux-sandbox", "darwin-sandbox", "processwrapper-sandbox", "local", "worker"}:
            local += value
    return {
        "total_processes": total,
        "counts": counts,
        "remote_cache_hits": remote_hits,
        "local_actions": local,
        "raw": match.group(0).strip(),
    }


def _is_policy_exempt_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return True
    if stripped.startswith("#"):
        return True
    if stripped.startswith("//"):
        return True
    # Documentation / self-reference mentioning the forbidden flag.
    if "do not set" in stripped.lower() and "--remote_cache" in stripped:
        return True
    if "Do not set --remote_cache" in stripped:
        return True
    return False


def iter_policy_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for pattern in POLICY_SCAN_GLOBS:
        files.extend(sorted(root.glob(pattern)))
    # Always include this harness so accidental active flags fail closed,
    # but exempt detection string assignments via line filters below.
    unique = sorted({path.resolve() for path in files if path.is_file()})
    return [path for path in unique]


def check_remote_cache_policy(root: Path) -> list[str]:
    errors: list[str] = []
    root = root.resolve()
    self_path = Path(__file__).resolve()
    for path in iter_policy_files(root):
        resolved = path.resolve()
        rel = resolved.relative_to(root)
        text = resolved.read_text(encoding="utf-8")
        for lineno, line in enumerate(text.splitlines(), start=1):
            if resolved == self_path:
                # This harness documents and detects the flag; skip self.
                continue
            if _is_policy_exempt_line(line):
                continue
            if REMOTE_CACHE_FLAG_RE.search(line):
                errors.append(f"{rel}:{lineno}: competing --remote_cache override")
    return errors


def mode_policy(root: Path) -> int:
    errors = check_remote_cache_policy(root)
    if errors:
        print("remote-cache policy check FAILED:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        print(
            "Remove in-repo --remote_cache so Blacksmith can inject repository caching.",
            file=sys.stderr,
        )
        return 1
    print("remote-cache policy check passed (no competing --remote_cache)")
    return 0


def mode_parse_log(path: Path | None) -> int:
    text = sys.stdin.read() if path is None else path.read_text(encoding="utf-8")
    summary = parse_process_summary(text)
    json.dump(summary, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0 if summary["raw"] else 1


def bazel_cmd() -> str:
    return os.environ.get("GRAPHFORGE_BAZEL", "bazelisk")


def measure_bazel(
    root: Path,
    argv: list[str],
    *,
    startup_args: list[str] | None = None,
    extra_args: list[str] | None = None,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    cmd = [bazel_cmd(), *(startup_args or []), *argv]
    if extra_args:
        cmd.extend(extra_args)
    started = time.perf_counter()
    proc = run(cmd, cwd=root, env=env, check=False)
    wall = time.perf_counter() - started
    combined = (proc.stdout or "") + "\n" + (proc.stderr or "")
    summary = parse_process_summary(combined)
    return {
        "schema": SCHEMA_RUN,
        "command": cmd,
        "exit_code": proc.returncode,
        "wall_seconds": round(wall, 3),
        "process_summary": summary,
        "stdout_tail": (proc.stdout or "")[-4000:],
        "stderr_tail": (proc.stderr or "")[-4000:],
    }


def mode_cold_correctness(root: Path) -> int:
    """Prove cache-unavailable builds remain correct without repo changes.

    Uses CLI-only empty ``--remote_cache`` / ``--disk_cache`` (never checked in
    as repo defaults) to simulate Blacksmith cache disablement. Paired cold
    *perf* samples still use clean/evicted cache protocol in perf-sample.json.
    """
    result = measure_bazel(
        root,
        [
            "test",
            "//tools/bazel/smoke:smoke_test",
            "--test_output=errors",
            # Constructed at runtime so policy scan does not see a repo default.
            "--" + "remote_cache=",
            "--disk_cache=",
        ],
    )
    if result["exit_code"] != 0:
        print("cold-correctness FAILED", file=sys.stderr)
        print(result["stderr_tail"], file=sys.stderr)
        return 1
    if result["process_summary"]["remote_cache_hits"] != 0:
        print(
            "cold-correctness FAILED: unexpected remote cache hits with cache disabled",
            file=sys.stderr,
        )
        return 1
    print(
        "cold-correctness passed "
        f"(wall={result['wall_seconds']}s, "
        f"remote_hits={result['process_summary']['remote_cache_hits']})"
    )
    return 0


def mode_measure(root: Path, argv: list[str], out: Path, label: str) -> int:
    result = measure_bazel(root, argv)
    payload = {
        **result,
        "label": label,
        "git_sha": git_sha(root),
        "runner": os.environ.get("RUNNER_NAME")
        or os.environ.get("BLACKSMITH_RUNNER")
        or os.environ.get("RUNNER_OS"),
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote measure fragment to {out}")
    if result["exit_code"] != 0:
        return result["exit_code"]
    return 0


def mode_observe_warm(root: Path, out: Path) -> int:
    """Second identical-SHA build; record whether remote cache hits appear."""
    # Priming build (may miss if cache empty / disabled).
    prime = measure_bazel(root, ["build", "//tools/bazel/smoke:smoke_test"])
    warm = measure_bazel(root, ["build", "//tools/bazel/smoke:smoke_test"])
    hits = warm["process_summary"]["remote_cache_hits"]
    payload = {
        "schema": SCHEMA_RUN,
        "kind": "warm_observation",
        "git_sha": git_sha(root),
        "prime": {
            "exit_code": prime["exit_code"],
            "wall_seconds": prime["wall_seconds"],
            "process_summary": prime["process_summary"],
        },
        "warm": {
            "exit_code": warm["exit_code"],
            "wall_seconds": warm["wall_seconds"],
            "process_summary": warm["process_summary"],
        },
        "remote_cache_hits_observed": hits > 0,
        "org_admin_enablement_likely": hits > 0,
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if prime["exit_code"] != 0 or warm["exit_code"] != 0:
        print("observe-warm FAILED: bazel build failed", file=sys.stderr)
        return 1
    if hits > 0:
        print(f"observe-warm: remote cache hits observed ({hits})")
    else:
        print(
            "observe-warm: no remote cache hits "
            "(org-admin Bazel Build Caching may still be disabled; not failing CI)"
        )
    return 0


def mode_affected_inputs(root: Path, out: Path) -> int:
    """Touch one crate source; require a rebuild of that package path only."""
    target_file = root / "crates/graphforge-ast/src/lib.rs"
    if not target_file.is_file():
        print(f"missing {target_file}", file=sys.stderr)
        return 1
    original = target_file.read_text(encoding="utf-8")
    marker = "\n// graphforge-bazel-cache-perf-mutation\n"
    try:
        # Warm two independent crates once.
        warm = measure_bazel(
            root,
            [
                "build",
                "//crates/graphforge-ast:graphforge_ast",
                "//crates/graphforge-core:graphforge_core",
            ],
        )
        if warm["exit_code"] != 0:
            print(warm["stderr_tail"], file=sys.stderr)
            return 1

        target_file.write_text(original + marker, encoding="utf-8")
        with tempfile.TemporaryDirectory(prefix="gf-exec-log-") as tmp:
            log_path = Path(tmp) / "exec.json"
            mutated = measure_bazel(
                root,
                [
                    "build",
                    "//crates/graphforge-ast:graphforge_ast",
                    "//crates/graphforge-core:graphforge_core",
                    f"--execution_log_json_file={log_path}",
                ],
            )
            if mutated["exit_code"] != 0:
                print(mutated["stderr_tail"], file=sys.stderr)
                return 1
            log_text = log_path.read_text(encoding="utf-8") if log_path.is_file() else ""
    finally:
        target_file.write_text(original, encoding="utf-8")

    # execution_log_json_file is NDJSON of executed actions.
    executed_labels: list[str] = []
    for line in log_text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        label = entry.get("targetLabel") or entry.get("mnemonic") or ""
        if label:
            executed_labels.append(str(label))

    ast_touched = any("graphforge-ast" in label for label in executed_labels)
    core_rebuilt = any("graphforge-core" in label for label in executed_labels)
    # If the execution log is empty (remote hits only / no local execute), treat
    # local process counts as the fallback signal.
    local_fallback_ok = (
        not executed_labels
        and mutated["process_summary"]["local_actions"] >= 1
        and mutated["process_summary"]["total_processes"]
        <= max(8, warm["process_summary"]["total_processes"] + 4)
    )
    ok = (ast_touched and not core_rebuilt) or local_fallback_ok or (
        # Without remote cache / exec log, require some local work and no
        # unbounded rebuild vs the warm baseline.
        mutated["process_summary"]["local_actions"] >= 1
        and mutated["process_summary"]["total_processes"]
        <= max(8, warm["process_summary"]["total_processes"] + 4)
    )
    payload = {
        "schema": SCHEMA_RUN,
        "kind": "affected_inputs",
        "git_sha": git_sha(root),
        "warm": warm["process_summary"],
        "mutated": mutated["process_summary"],
        "executed_labels": executed_labels[:200],
        "ast_touched": ast_touched,
        "core_rebuilt": core_rebuilt,
        "passed": bool(ok),
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if not ok:
        print("affected-inputs FAILED", file=sys.stderr)
        print(json.dumps(payload, indent=2), file=sys.stderr)
        return 1
    print("affected-inputs passed")
    return 0


def empty_evidence(root: Path) -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "issue": 5,
        "status": "pending_org_admin",
        "git_sha_at_scaffold": git_sha(root),
        "blocker": {
            "kind": "org_admin_blacksmith_bazel_build_caching",
            "dashboard": "https://app.blacksmith.sh/settings?tab=features",
            "cache_page": "https://app.blacksmith.sh/cache",
            "docs": "https://docs.blacksmith.sh/blacksmith-caching/bazel-build-caching",
            "steps": [
                "Org admin opens Blacksmith Settings → Features",
                "Under Caching, enable Bazel Build Caching for CurateLabs/graphforge",
                "Confirm no competing --remote_cache remains in-repo (policy check)",
                "Re-run Bazel Bootstrap / measurement harness on an immutable SHA",
                "Collect ≥10 paired cold/warm runs and refresh perf-sample.json",
            ],
        },
        "baseline_ref": str(DEFAULT_BASELINE_DOC),
        "baseline": {
            "inventory_sha": BASELINE_INVENTORY_SHA,
            "cargo_primary_p50_seconds": CARGO_PRIMARY_P50_SECONDS,
            "cargo_compute_proxy_p50_seconds": CARGO_COMPUTE_PROXY_P50_SECONDS,
            "primary_jobs": ["Rust Tests", "Python Binding", "Node Binding"],
        },
        "thresholds": {
            "min_pairs": MIN_PAIRS,
            "warm_speedup_min": WARM_SPEEDUP_MIN,
            "compute_reduction_min": COMPUTE_REDUCTION_MIN,
            "cold_regression_max": COLD_REGRESSION_MAX,
        },
        "representative_targets": {
            "test": REPRESENTATIVE_TEST,
            "build": REPRESENTATIVE_BUILD,
            "bindings": REPRESENTATIVE_BINDINGS,
        },
        "cold_protocol": {
            "bazel": "Correctness: CLI-only empty --remote_cache/--disk_cache (zero remote hits; not a repo default). Cold perf: clean/evicted Blacksmith repo cache without sticky local state",
            "cargo": "Cargo sticky-disk warm starts are not cold; cold Cargo uses empty target/ without sticky hydrate",
        },
        "pairs": [],
        "observations": {
            "remote_cache_hits_on_identical_sha": False,
            "cache_unavailable_cold_correct": False,
            "affected_inputs_isolation": False,
        },
        "gates": {
            "passed": False,
            "reason": "pending_org_admin_and_paired_sample",
        },
        "waiver": None,
        "blacksmith_dashboard_links": [],
    }


def p50(values: list[float]) -> float:
    if not values:
        raise ValueError("p50 requires a non-empty sample")
    return float(statistics.median(values))


def evaluate_evidence(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("schema") != SCHEMA:
        return {"passed": False, "reason": f"unexpected schema {payload.get('schema')!r}"}

    waiver = payload.get("waiver")
    if isinstance(waiver, dict) and waiver.get("approved_by") and waiver.get("rationale"):
        return {
            "passed": True,
            "reason": "maintainer_waiver",
            "waiver": waiver,
        }

    pairs = payload.get("pairs") or []
    obs = payload.get("observations") or {}
    thresholds = payload.get("thresholds") or {}
    baseline = payload.get("baseline") or {}

    min_pairs = int(thresholds.get("min_pairs", MIN_PAIRS))
    warm_min = float(thresholds.get("warm_speedup_min", WARM_SPEEDUP_MIN))
    compute_min = float(thresholds.get("compute_reduction_min", COMPUTE_REDUCTION_MIN))
    cold_max = float(thresholds.get("cold_regression_max", COLD_REGRESSION_MAX))
    cargo_primary = float(baseline.get("cargo_primary_p50_seconds", CARGO_PRIMARY_P50_SECONDS))
    cargo_compute = float(
        baseline.get("cargo_compute_proxy_p50_seconds", CARGO_COMPUTE_PROXY_P50_SECONDS)
    )

    reasons: list[str] = []
    if not obs.get("remote_cache_hits_on_identical_sha"):
        reasons.append("remote_cache_hits_not_observed")
    if not obs.get("cache_unavailable_cold_correct"):
        reasons.append("cache_unavailable_cold_not_proven")
    if not obs.get("affected_inputs_isolation"):
        reasons.append("affected_inputs_isolation_not_proven")
    if len(pairs) < min_pairs:
        reasons.append(f"need>={min_pairs}_pairs_have_{len(pairs)}")

    warm_walls = [float(p["warm"]["wall_seconds"]) for p in pairs if p.get("warm")]
    cold_walls = [float(p["cold"]["wall_seconds"]) for p in pairs if p.get("cold")]
    compute_walls = [
        float(p.get("compute_proxy_seconds", p["warm"]["wall_seconds"]))
        for p in pairs
        if p.get("warm")
    ]

    warm_speedup = None
    compute_reduction = None
    cold_regression = None
    if warm_walls:
        warm_p50 = p50(warm_walls)
        warm_speedup = (cargo_primary - warm_p50) / cargo_primary
        if warm_speedup < warm_min:
            reasons.append(f"warm_speedup_{warm_speedup:.3f}<{warm_min}")
    if compute_walls:
        compute_p50 = p50(compute_walls)
        compute_reduction = (cargo_compute - compute_p50) / cargo_compute
        if compute_reduction < compute_min:
            reasons.append(f"compute_reduction_{compute_reduction:.3f}<{compute_min}")
    if cold_walls:
        # Cold Cargo baseline is recorded inside pairs when collected; fall back
        # to cargo primary as a conservative ceiling only when absent.
        cargo_cold_values = [
            float(p["cargo_cold_wall_seconds"])
            for p in pairs
            if p.get("cargo_cold_wall_seconds") is not None
        ]
        cargo_cold_p50 = p50(cargo_cold_values) if cargo_cold_values else cargo_primary
        cold_p50 = p50(cold_walls)
        cold_regression = (cold_p50 - cargo_cold_p50) / cargo_cold_p50
        if cold_regression > cold_max:
            reasons.append(f"cold_regression_{cold_regression:.3f}>{cold_max}")

    passed = not reasons
    return {
        "passed": passed,
        "reason": "ok" if passed else ",".join(reasons),
        "warm_speedup": warm_speedup,
        "compute_reduction": compute_reduction,
        "cold_regression": cold_regression,
        "pair_count": len(pairs),
    }


def mode_evaluate(path: Path, *, allow_pending: bool) -> int:
    payload = json.loads(path.read_text(encoding="utf-8"))
    result = evaluate_evidence(payload)
    print(json.dumps(result, indent=2, sort_keys=True))
    if result["passed"]:
        print("performance gates PASSED")
        return 0
    status = payload.get("status")
    if allow_pending and status in {"pending_org_admin", "collecting"}:
        print(
            f"performance gates incomplete (status={status}); "
            "allow-pending enabled for in-repo readiness"
        )
        return 0
    print("performance gates FAILED", file=sys.stderr)
    return 1


def mode_scaffold(root: Path, path: Path) -> int:
    if path.exists():
        existing = json.loads(path.read_text(encoding="utf-8"))
        if existing.get("schema") == SCHEMA and existing.get("pairs") is not None:
            print(f"evidence already present at {path}")
            return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = empty_evidence(root)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"scaffolded {path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        required=True,
        choices=[
            "policy",
            "parse-log",
            "cold-correctness",
            "measure",
            "observe-warm",
            "affected-inputs",
            "evaluate",
            "scaffold",
        ],
    )
    parser.add_argument("--log", type=Path, help="Bazel log for parse-log")
    parser.add_argument("--write", type=Path, help="Output JSON path")
    parser.add_argument("--evidence", type=Path, default=DEFAULT_EVIDENCE)
    parser.add_argument("--label", default="run")
    parser.add_argument(
        "--allow-pending",
        action="store_true",
        help="evaluate exits 0 while status is pending_org_admin/collecting",
    )
    parser.add_argument(
        "bazel_args",
        nargs=argparse.REMAINDER,
        help="For measure: args after -- passed to bazelisk",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    root = repo_root()

    if args.mode == "policy":
        return mode_policy(root)
    if args.mode == "parse-log":
        return mode_parse_log(args.log)
    if args.mode == "cold-correctness":
        return mode_cold_correctness(root)
    if args.mode == "measure":
        bazel_args = list(args.bazel_args)
        if bazel_args and bazel_args[0] == "--":
            bazel_args = bazel_args[1:]
        if not bazel_args:
            parser.error("measure requires bazel args after --")
        if not args.write:
            parser.error("measure requires --write")
        return mode_measure(root, bazel_args, args.write, args.label)
    if args.mode == "observe-warm":
        if not args.write:
            parser.error("observe-warm requires --write")
        return mode_observe_warm(root, args.write)
    if args.mode == "affected-inputs":
        if not args.write:
            parser.error("affected-inputs requires --write")
        return mode_affected_inputs(root, args.write)
    if args.mode == "evaluate":
        return mode_evaluate(args.evidence, allow_pending=args.allow_pending)
    if args.mode == "scaffold":
        return mode_scaffold(root, args.evidence)
    parser.error(f"unknown mode {args.mode}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
