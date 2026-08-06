#!/usr/bin/env python3
"""Same-SHA Cargo/Bazel dual-build parity gate (#6).

Modes:
  --inventory   Fail-closed ledger + release-platform completeness + label query
  --dual-run    Execute the representative parity suite under Cargo and Bazel
  --all         inventory then dual-run (CI default)

Writes machine-readable evidence JSON when --write-evidence is set.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

SCHEMA = "graphforge.cargo-bazel-parity-evidence.v1"
RELEASE_SCHEMA = "graphforge.bazel-release-platforms.v1"
DEFAULT_MAP = Path("tools/bazel/parity/migration_target_map.json")
DEFAULT_PLATFORMS = Path("tools/bazel/release/release_platforms.json")
DEFAULT_SUITE = Path("tools/bazel/parity/parity_suite.json")
DEFAULT_RC_CONTRACT = Path("tests/contracts/binding-release-candidate-targets.json")
NODE_PACKAGE = Path("crates/graphforge-bindings-node/package.json")


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
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=cwd,
        check=check,
        capture_output=True,
        text=True,
    )


def check_release_platforms(root: Path, platforms_path: Path, rc_path: Path) -> list[str]:
    errors: list[str] = []
    payload = json.loads(platforms_path.read_text(encoding="utf-8"))
    if payload.get("schema") != RELEASE_SCHEMA:
        errors.append(f"unexpected release platform schema: {payload.get('schema')!r}")
        return errors

    rc = json.loads(rc_path.read_text(encoding="utf-8"))
    rc_ids = set((rc.get("targets") or {}).keys())
    platform_ids = {entry["id"] for entry in payload["platforms"]}
    for missing in sorted(rc_ids - platform_ids):
        errors.append(f"release platform missing Bazel model: {missing}")
    for extra in sorted(platform_ids - rc_ids):
        errors.append(f"Bazel release platform not in Binding RC contract: {extra}")

    node_pkg = json.loads((root / NODE_PACKAGE).read_text(encoding="utf-8"))
    napi_targets = set((node_pkg.get("napi") or {}).get("targets") or [])
    modeled_node_triples = {
        entry["rust_triple"] for entry in payload["platforms"] if entry.get("language") == "node"
    }
    for missing in sorted(napi_targets - modeled_node_triples):
        errors.append(f"napi package.json target missing Bazel platform: {missing}")
    for extra in sorted(modeled_node_triples - napi_targets):
        errors.append(f"Bazel node platform not in package.json napi.targets: {extra}")

    for entry in payload["platforms"]:
        label = entry.get("bazel_platform")
        if not label or not str(label).startswith("//platforms:"):
            errors.append(f"platform {entry.get('id')} missing //platforms label")
        if entry.get("language") == "python" and not entry.get("wheel_tag"):
            errors.append(f"python platform {entry.get('id')} missing wheel_tag")
        if entry.get("language") == "node" and not entry.get("node_platform_tag"):
            errors.append(f"node platform {entry.get('id')} missing node_platform_tag")

    host = payload.get("host_release_artifacts") or {}
    for key in (
        "cli",
        "release_load_probe",
        "api_examples",
        "python_wheel_smoke",
        "node_package_smoke",
        "binding_cdylibs",
    ):
        if key not in host:
            errors.append(f"host_release_artifacts missing {key}")
    return errors


def check_labels_exist(root: Path, map_path: Path) -> list[str]:
    payload = json.loads(map_path.read_text(encoding="utf-8"))
    labels = {
        entry["bazel_label"]
        for entry in payload["targets"]
        if entry.get("status") == "mapped" and entry.get("bazel_label")
    }
    platforms = json.loads((root / DEFAULT_PLATFORMS).read_text(encoding="utf-8"))
    for value in (platforms.get("host_release_artifacts") or {}).values():
        if isinstance(value, str) and value.startswith("//"):
            labels.add(value)
    for entry in platforms.get("platforms") or []:
        plat = entry.get("bazel_platform")
        if isinstance(plat, str) and plat.startswith("//"):
            labels.add(plat)

    errors: list[str] = []
    ordered = sorted(labels)
    # `bazel query` accepts a set expression of labels.
    result = run(
        ["bazelisk", "query", f"set({' '.join(ordered)})"],
        cwd=root,
    )
    if result.returncode != 0:
        for label in ordered:
            one = run(["bazelisk", "query", label], cwd=root)
            if one.returncode != 0:
                errors.append(f"bazel label missing: {label}")
        if not errors:
            errors.append(
                "bazelisk query failed for mapped labels:\n" + (result.stderr or result.stdout)
            )
        return errors

    found = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    for label in ordered:
        if label not in found:
            errors.append(f"bazel label missing from query results: {label}")
    return errors


def run_dual_suite(root: Path, suite_path: Path) -> tuple[list[str], list[dict[str, Any]]]:
    suite = json.loads(suite_path.read_text(encoding="utf-8"))
    errors: list[str] = []
    cases: list[dict[str, Any]] = []
    for case in suite.get("cases") or []:
        case_id = case["id"]
        cargo_cmd = ["cargo", *case["cargo"]]
        bazel_cmd = ["bazelisk", "test", *case["bazel"], "--test_output=errors"]
        cargo = run(cargo_cmd, cwd=root)
        bazel = run(bazel_cmd, cwd=root)
        cargo_ok = cargo.returncode == 0
        bazel_ok = bazel.returncode == 0
        record = {
            "id": case_id,
            "cargo_argv": cargo_cmd,
            "bazel_argv": bazel_cmd,
            "cargo_ok": cargo_ok,
            "bazel_ok": bazel_ok,
            "same_outcome": cargo_ok == bazel_ok,
        }
        cases.append(record)
        if cargo_ok != bazel_ok:
            errors.append(
                f"parity outcome mismatch for {case_id}: cargo_ok={cargo_ok} bazel_ok={bazel_ok}"
            )
        if not cargo_ok and not bazel_ok:
            errors.append(
                f"parity suite case failed on both paths: {case_id} "
                "(fix the tests; do not weaken the gate)"
            )
    return errors, cases


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=None)
    parser.add_argument("--map", type=Path, default=DEFAULT_MAP)
    parser.add_argument("--platforms", type=Path, default=DEFAULT_PLATFORMS)
    parser.add_argument("--suite", type=Path, default=DEFAULT_SUITE)
    parser.add_argument("--rc-contract", type=Path, default=DEFAULT_RC_CONTRACT)
    parser.add_argument(
        "--mode",
        choices=("inventory", "dual-run", "all"),
        default="all",
    )
    parser.add_argument(
        "--skip-label-query",
        action="store_true",
        help="Skip bazelisk query (unit tests / environments without Bazel).",
    )
    parser.add_argument("--write-evidence", type=Path, default=None)
    args = parser.parse_args(argv)

    root = args.root.resolve() if args.root else repo_root()
    map_path = args.map if args.map.is_absolute() else root / args.map
    platforms_path = args.platforms if args.platforms.is_absolute() else root / args.platforms
    suite_path = args.suite if args.suite.is_absolute() else root / args.suite
    rc_path = args.rc_contract if args.rc_contract.is_absolute() else root / args.rc_contract

    errors: list[str] = []
    evidence: dict[str, Any] = {
        "schema": SCHEMA,
        "sha": git_sha(root),
        "mode": args.mode,
        "dual_build": True,
        "required_check": "CI Gate",
        "inventory": {},
        "cases": [],
    }

    if args.mode in ("inventory", "all"):
        ledger = run(
            [
                "python3",
                str(root / "scripts/ci/bazel-migration-ledger-check.py"),
                "--map",
                str(map_path),
            ],
            cwd=root,
        )
        if ledger.returncode != 0:
            errors.append("ledger check failed:\n" + (ledger.stderr or ledger.stdout))
        platform_errors = check_release_platforms(root, platforms_path, rc_path)
        errors.extend(platform_errors)
        label_errors: list[str] = []
        if not args.skip_label_query:
            label_errors = check_labels_exist(root, map_path)
            errors.extend(label_errors)
        evidence["inventory"] = {
            "ledger_ok": ledger.returncode == 0,
            "platforms_ok": not platform_errors,
            "labels_ok": not label_errors,
            "label_query_skipped": bool(args.skip_label_query),
        }

    if args.mode in ("dual-run", "all"):
        dual_errors, cases = run_dual_suite(root, suite_path)
        errors.extend(dual_errors)
        evidence["cases"] = cases

    evidence["ok"] = not errors
    if args.write_evidence is not None:
        out = (
            args.write_evidence if args.write_evidence.is_absolute() else root / args.write_evidence
        )
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"wrote evidence {out}")

    if errors:
        print("cargo/bazel parity check FAILED:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1

    print(f"cargo/bazel parity check OK: sha={evidence['sha']} mode={args.mode} dual_build=true")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
