#!/usr/bin/env python3
"""Run local publication dry-runs and write evidence JSON.

Surfaces:
- cargo-package: ``cargo package --list --no-verify`` per crates.io plan order
- cargo-publish: ``cargo publish --dry-run`` (heavy; optional)
- npm: ``npm publish --dry-run`` for Node binding and agent-skills
- docs: ``pnpm docs:build``
- python: ``maturin sdist`` (local packaging; TestPyPI upload is separate/manual)

Never publishes to production registries.

Crate order prefers ``scripts/ci/crate-publish-plan.py list`` (from #2820) when
present; otherwise uses a conservative fallback that excludes bindings/CLI.

Usage:
    python3 scripts/publish_dry_run.py --surface npm,docs --report /tmp/dry-run.json
    python3 scripts/publish_dry_run.py --surface cargo-package,python,npm,docs
    make publish-dry-run
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
CRATE_PLAN = ROOT / "scripts" / "ci" / "crate-publish-plan.py"

# Fallback when crate-publish-plan.py is not on the branch yet (pre-#2820).
# Keep in sync with CRATES_IO_EXCLUDED there: no bindings/CLI.
FALLBACK_CARGO_ORDER = (
    "gf-ast",
    "gf-knowledge",
    "gf-ontology",
    "gf-provenance",
    "gf-ir",
    "gf-plan",
    "gf-storage",
    "gf-io",
    "gf-rel",
    "gf-search",
    "gf-cypher",
    "gf-exec",
    "gf-api",
)

NPM_PACKAGES = (
    ROOT / "crates" / "gf-bindings-node",
    ROOT / "packages" / "agent-skills",
)


def _git_sha() -> str:
    result = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--verify", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def _run(cmd: list[str], *, cwd: Path | None = None) -> dict[str, Any]:
    started = time.time()
    env = os.environ.copy()
    env.setdefault("CARGO_TARGET_DIR", str(ROOT / "target" / "publish-dry-run"))
    result = subprocess.run(
        cmd,
        cwd=str(cwd or ROOT),
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
    )
    return {
        "cmd": cmd,
        "cwd": str((cwd or ROOT).relative_to(ROOT)) if cwd else ".",
        "exit_code": result.returncode,
        "seconds": round(time.time() - started, 3),
        "stdout_tail": result.stdout[-4000:],
        "stderr_tail": result.stderr[-4000:],
        "ok": result.returncode == 0,
    }


def cargo_publish_order() -> tuple[list[str], str]:
    """Return (crate names, source label)."""
    if CRATE_PLAN.exists():
        result = subprocess.run(
            [sys.executable, str(CRATE_PLAN), "list"],
            cwd=str(ROOT),
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if result.returncode == 0:
            names = [line.strip() for line in result.stdout.splitlines() if line.strip()]
            # Skip known foreign conflicts that check would fail closed on.
            names = [name for name in names if name != "gf-core"]
            return names, "crate-publish-plan.py"
    return list(FALLBACK_CARGO_ORDER), "fallback"


def dry_run_cargo_package() -> list[dict[str, Any]]:
    order, source = cargo_publish_order()
    steps: list[dict[str, Any]] = [
        {
            "cmd": ["crate-order-source", source],
            "cwd": ".",
            "exit_code": 0,
            "seconds": 0,
            "stdout_tail": "\n".join(order),
            "stderr_tail": "",
            "ok": True,
        }
    ]
    for name in order:
        steps.append(
            _run(
                [
                    "cargo",
                    "package",
                    "-p",
                    name,
                    "--list",
                    "--allow-dirty",
                    "--no-verify",
                ]
            )
        )
    return steps


def dry_run_cargo_publish() -> list[dict[str, Any]]:
    order, _source = cargo_publish_order()
    steps: list[dict[str, Any]] = []
    for name in order:
        step = _run(
            [
                "cargo",
                "publish",
                "-p",
                name,
                "--dry-run",
                "--allow-dirty",
            ]
        )
        steps.append(step)
        if not step["ok"]:
            break
    return steps


def dry_run_npm() -> list[dict[str, Any]]:
    # Prerelease versions (e.g. 0.5.0-dev.0) require an explicit --tag; dry-run
    # never publishes, so a disposable tag keeps the check green before freeze.
    return [
        _run(
            [
                "npm",
                "publish",
                "--dry-run",
                "--ignore-scripts",
                "--tag",
                "dry-run",
            ],
            cwd=package_dir,
        )
        for package_dir in NPM_PACKAGES
    ]


def dry_run_docs() -> list[dict[str, Any]]:
    return [_run(["pnpm", "docs:build"])]


def dry_run_python_sdist() -> list[dict[str, Any]]:
    out = ROOT / "target" / "publish-dry-run" / "python-dist"
    out.mkdir(parents=True, exist_ok=True)
    return [
        _run(
            [
                "uv",
                "run",
                "maturin",
                "sdist",
                "--manifest-path",
                "crates/gf-bindings-py/Cargo.toml",
                "--out",
                str(out),
            ]
        )
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--surface",
        default="npm,docs,cargo-package,python",
        help="Comma list: npm,docs,cargo-package,cargo-publish,python,all",
    )
    parser.add_argument(
        "--skip-cargo-publish",
        action="store_true",
        help="When surface=all, skip heavy cargo publish --dry-run",
    )
    parser.add_argument("--report", type=Path, help="Write evidence JSON")
    args = parser.parse_args(argv)

    surfaces = {part.strip() for part in args.surface.split(",") if part.strip()}
    if "all" in surfaces:
        surfaces = {"npm", "docs", "cargo-package", "python"}
        if not args.skip_cargo_publish:
            surfaces.add("cargo-publish")

    evidence: dict[str, Any] = {
        "schema_version": 1,
        "git_sha": _git_sha(),
        "surfaces": {},
        "ok": True,
        "note": (
            "Local dry-runs only. TestPyPI upload and production registry "
            "publishes are out of scope (see docs/development/publication-order.md)."
        ),
    }

    runners = {
        "cargo-package": dry_run_cargo_package,
        "cargo-publish": dry_run_cargo_publish,
        "npm": dry_run_npm,
        "docs": dry_run_docs,
        "python": dry_run_python_sdist,
    }
    for name in sorted(surfaces):
        if name not in runners:
            print(f"publish-dry-run: unknown surface {name}", file=sys.stderr)
            return 2
        steps = runners[name]()
        evidence["surfaces"][name] = steps
        if not all(step["ok"] for step in steps):
            evidence["ok"] = False

    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")

    if evidence["ok"]:
        print(f"publish-dry-run: ok sha={evidence['git_sha']} surfaces={sorted(surfaces)}")
        return 0
    print(f"publish-dry-run: FAILED sha={evidence['git_sha']}", file=sys.stderr)
    for name, steps in evidence["surfaces"].items():
        for step in steps:
            if not step["ok"]:
                print(
                    f"  fail: {name} cmd={step['cmd']} exit={step['exit_code']}",
                    file=sys.stderr,
                )
                if step["stderr_tail"]:
                    print(step["stderr_tail"][-1000:], file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
