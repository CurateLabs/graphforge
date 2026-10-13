#!/usr/bin/env python3
"""Compute and validate crates.io publish order for GraphForge workspace crates.

Usage:
    python3 scripts/ci/crate-publish-plan.py list
    python3 scripts/ci/crate-publish-plan.py check
    python3 scripts/ci/crate-publish-plan.py dry-run-commands
    python3 scripts/ci/crate-publish-plan.py write-readmes

Language-binding implementation crates are not published to crates.io; their
public distributions ship through PyPI and npm. The Rust CLI is part of the
crates.io surface. Versioned dev-dependencies are kept in published manifests and
therefore contribute publish-order edges; path-only dev-dependencies are
stripped by Cargo and do not.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
CRATES_DIR = ROOT / "crates"
README_TEMPLATE = ROOT / "scripts" / "templates" / "crate-readme.md"

QUICKSTARTS = {
    "graphforge-api": """
## Quickstart

Create a Rust binary with `cargo new my-graph` and enter its directory. Add
`graphforge-api` at the version shown on this crate page (replace `<version>`):

```sh
cargo add graphforge-api@<version>
```

Replace `src/main.rs` with:

```rust
use graphforge_api::GraphForge;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let graph = GraphForge::new(None)?;
    graph.execute("CREATE (:Person {name: 'Alice'})")?;
    let result = graph.execute("MATCH (p:Person) RETURN p.name AS name")?;
    for batch in result.batches {
        println!("{batch:?}");
    }
    Ok(())
}
```

Run `cargo run`. This example uses the full engine in memory; its data lasts
only for this process. Query results are Arrow record batches.
""",
    "graphforge-cli": """
## Quickstart

Install the version shown on this crate page (replace `<version>`):

```sh
cargo install graphforge-cli --version <version> --locked
gf --version
gf --help
mkdir my-graph
cd my-graph
gf init
gf skills status
```

`gf init` creates repository-local GraphForge configuration and installs the
embedded agent skills. Durable projects require an ext4, xfs, or btrfs volume
at the project root. Follow the
[first graph guide](https://docs.graphforge.sh/guide/quickstart/) to load and
query connected data.
""",
}


def expected_readme(package: dict[str, Any]) -> str:
    """Render a crate page from one template and Cargo's description."""
    return README_TEMPLATE.read_text(encoding="utf-8").format(
        name=package["name"],
        description=package["description"].rstrip("."),
        quickstart=QUICKSTARTS.get(package["name"], ""),
    )


def check_readmes(packages: list[dict[str, Any]], names: list[str]) -> list[str]:
    """Require current generated READMEs in every publishable package."""
    errors = []
    for package in packages:
        if package["name"] not in names:
            continue
        path = Path(package["manifest_path"]).parent / "README.md"
        if not path.is_file() or path.read_text(encoding="utf-8") != expected_readme(package):
            errors.append(f"{package['name']}: missing or stale README.md; run write-readmes")
    return errors


# Implementation crates published through language registries, not crates.io.
CRATES_IO_EXCLUDED = frozenset(
    {
        "graphforge-bindings-py",
        "graphforge-bindings-node",
    }
)


def _workspace_manifests() -> set[Path]:
    """Return manifests that define the crates.io candidate workspace set."""
    return {manifest.resolve() for manifest in CRATES_DIR.glob("*/Cargo.toml")}


def _cargo_metadata() -> dict[str, Any]:
    result = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return json.loads(result.stdout)


def classify_dependencies(
    packages: list[dict[str, Any]], crate_names: set[str]
) -> tuple[dict[str, set[str]], dict[str, list[str]]]:
    """Classify metadata dependencies as publish edges or missing versions."""
    deps_by_crate: dict[str, set[str]] = {}
    missing_versions: dict[str, list[str]] = {}
    for package in packages:
        name = package["name"]
        if name not in crate_names:
            continue
        deps: set[str] = set()
        missing: set[str] = set()
        for dependency in package.get("dependencies", []):
            dep = dependency["name"]
            if dep not in crate_names:
                continue
            kind = dependency.get("kind")
            requirement = dependency["req"]
            if kind in (None, "build"):
                deps.add(dep)
                if requirement == "*":
                    missing.add(dep)
            elif kind == "dev" and requirement != "*":
                deps.add(dep)
        deps_by_crate[name] = deps
        if missing:
            missing_versions[name] = sorted(missing)
    return deps_by_crate, missing_versions


def _classify_workspace() -> tuple[dict[str, set[str]], dict[str, list[str]]]:
    """Classify the dependencies of every crates/*/Cargo.toml package."""
    manifests = _workspace_manifests()
    packages = [
        package
        for package in _cargo_metadata()["packages"]
        if Path(package["manifest_path"]).resolve() in manifests
    ]
    crate_names = {package["name"] for package in packages}
    return classify_dependencies(packages, crate_names)


def load_workspace() -> dict[str, set[str]]:
    """Map crate name → workspace dependencies that affect publish order."""
    deps, _ = _classify_workspace()
    return deps


def topological_publish_order(crates: dict[str, set[str]]) -> list[str]:
    """Return crates.io candidates in dependency order (bindings excluded)."""
    remaining = {
        name: {dep for dep in deps if dep in crates and dep not in CRATES_IO_EXCLUDED}
        for name, deps in crates.items()
        if name not in CRATES_IO_EXCLUDED
    }
    # Drop edges to excluded crates from remaining dependency sets.
    for name in list(remaining):
        remaining[name] = {dep for dep in remaining[name] if dep in remaining}

    order: list[str] = []
    while remaining:
        ready = sorted(name for name, deps in remaining.items() if not deps)
        if not ready:
            cycle = ", ".join(sorted(remaining))
            raise SystemExit(f"dependency cycle among crates.io candidates: {cycle}")
        for name in ready:
            order.append(name)
            del remaining[name]
        for deps in remaining.values():
            deps.difference_update(ready)
    return order


def path_deps_missing_versions() -> dict[str, list[str]]:
    """Return crate → path deps that lack version= (blocks cargo publish)."""
    _, missing = _classify_workspace()
    return {name: deps for name, deps in missing.items() if name not in CRATES_IO_EXCLUDED}


def cmd_list(_: argparse.Namespace) -> int:
    order = topological_publish_order(load_workspace())
    for name in order:
        print(name)
    return 0


def cmd_check(_: argparse.Namespace) -> int:
    crates = load_workspace()
    order = topological_publish_order(crates)
    errors: list[str] = []

    unexpected = sorted(name for name in order if not name.startswith("graphforge-"))
    for name in unexpected:
        errors.append(f"{name}: publishable package must use the graphforge-* namespace")

    missing = path_deps_missing_versions()
    for name, deps in missing.items():
        errors.append(
            f"{name}: path dependencies missing version= for cargo publish: " + ", ".join(deps)
        )

    errors.extend(check_readmes(_cargo_metadata()["packages"], order))

    if errors:
        print("crate publish plan check FAILED:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        print(
            "See RELEASING.md (crates.io publication plan).",
            file=sys.stderr,
        )
        return 1

    print(f"crate publish plan OK ({len(order)} crates)")
    for name in order:
        print(f"  {name}")
    return 0


def cmd_dry_run_commands(_: argparse.Namespace) -> int:
    order = topological_publish_order(load_workspace())
    for name in order:
        print(f"cargo publish -p {name} --dry-run --locked")
    return 0


def cmd_write_readmes(_: argparse.Namespace) -> int:
    names = topological_publish_order(load_workspace())
    for package in _cargo_metadata()["packages"]:
        if package["name"] in names:
            path = Path(package["manifest_path"]).parent / "README.md"
            path.write_text(expected_readme(package), encoding="utf-8")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="Print crates.io publish order").set_defaults(func=cmd_list)
    sub.add_parser(
        "check",
        help="Fail on name conflicts, cycles, missing path+version deps, or stale READMEs",
    ).set_defaults(func=cmd_check)
    sub.add_parser(
        "dry-run-commands",
        help="Print cargo publish --dry-run commands when unblocked",
    ).set_defaults(func=cmd_dry_run_commands)
    sub.add_parser(
        "write-readmes", help="Generate crate READMEs from the shared template"
    ).set_defaults(func=cmd_write_readmes)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
