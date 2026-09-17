#!/usr/bin/env python3
"""Run local publication dry-runs and write evidence JSON.

Surfaces:
- cargo-package: ``cargo package --list --no-verify`` per crates.io plan order
- cargo-publish: ``cargo publish --dry-run`` (heavy; optional)
- npm: ``npm publish --dry-run`` for Node binding, CLI, and agent-skills, under
  the same version-derived dist-tag a real publish would use
- docs: ``pnpm docs:build``
- python: ``maturin sdist`` (local packaging; TestPyPI upload is separate/manual)

Never publishes to production registries.

Crate order has one source: ``scripts/ci/crate-publish-plan.py list``. A missing
or failing plan script is a hard error, never a fallback to a different set.

Usage:
    python3 scripts/publish_dry_run.py --surface npm,docs --report /tmp/dry-run.json
    python3 scripts/publish_dry_run.py --surface python,npm,docs
    make publish-dry-run
"""

from __future__ import annotations

import argparse
import functools
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
CRATE_PLAN = ROOT / "scripts" / "ci" / "crate-publish-plan.py"
NPM_PUBLISHER = ROOT / "scripts" / "publish_npm_artifacts.py"

NPM_PACKAGES = (
    ROOT / "crates" / "graphforge-bindings-node",
    ROOT / "packages" / "cli",
    ROOT / "packages" / "agent-skills",
)


@functools.cache
def _npm_publisher() -> Any:
    """Load the real npm publisher so the dry run shares its dist-tag policy."""
    if str(ROOT / "scripts" / "ci") not in sys.path:
        sys.path.insert(0, str(ROOT / "scripts" / "ci"))
    spec = importlib.util.spec_from_file_location("publish_npm_artifacts", NPM_PUBLISHER)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {NPM_PUBLISHER}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def npm_dist_tag(package_dir: Path) -> str:
    """Return the dist-tag a real publish of ``package_dir`` would use."""
    manifest = package_dir / "package.json"
    version = json.loads(manifest.read_text(encoding="utf-8"))["version"]
    return _npm_publisher().dist_tag_for(version)


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


class CratePlanError(RuntimeError):
    """The crates.io publish plan could not be read."""


def cargo_publish_order() -> tuple[list[str], str]:
    """Return (crate names, source label) from the single authoritative plan.

    ``scripts/ci/crate-publish-plan.py`` is the only source of publish order.
    There is no fallback list: publishing a different crate set than the plan
    names would be worse than failing, so a missing, failing or empty plan is a
    hard error (#1377).
    """
    if not CRATE_PLAN.exists():
        raise CratePlanError(
            f"crates.io publish plan is missing: {CRATE_PLAN.relative_to(ROOT)}. "
            "It is the only source of crate publish order; restore it before "
            "running the cargo dry-run surfaces."
        )
    result = subprocess.run(
        [sys.executable, str(CRATE_PLAN), "list"],
        cwd=str(ROOT),
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        raise CratePlanError(
            f"crate-publish-plan.py list failed (exit {result.returncode}): "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )
    names = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if not names:
        raise CratePlanError("crate-publish-plan.py list returned no crates")
    return names, "crate-publish-plan.py"


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
    # The dist-tag is derived from each package version by the same classifier the
    # real publisher uses, so the dry run shows the tag publication would assign.
    # A version that cannot be classified fails the surface instead of defaulting.
    steps = [_run(["pnpm", "install", "--frozen-lockfile"])]
    if not steps[0]["ok"]:
        return steps
    for package_dir in NPM_PACKAGES:
        try:
            dist_tag = npm_dist_tag(package_dir)
        except (OSError, ValueError, KeyError) as error:
            steps.append(
                {
                    "cmd": ["npm-dist-tag", str(package_dir.relative_to(ROOT))],
                    "cwd": str(package_dir.relative_to(ROOT)),
                    "exit_code": 2,
                    "seconds": 0,
                    "stdout_tail": "",
                    "stderr_tail": f"cannot derive npm dist-tag: {error}",
                    "ok": False,
                }
            )
            return steps
        if package_dir.name == "cli":
            command = [
                "pnpm",
                "publish",
                "--dry-run",
                "--no-git-checks",
                "--tag",
                dist_tag,
            ]
        else:
            command = [
                "npm",
                "publish",
                "--dry-run",
                "--ignore-scripts",
                "--tag",
                dist_tag,
            ]
        steps.append(_run(command, cwd=package_dir))
    return steps


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
                "crates/graphforge-bindings-py/Cargo.toml",
                "--out",
                str(out),
            ]
        )
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--surface",
        default="npm,docs,python",
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
        surfaces = {"npm", "docs", "python"}
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
        try:
            steps = runners[name]()
        except CratePlanError as error:
            print(f"publish-dry-run: {name}: {error}", file=sys.stderr)
            return 2
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
