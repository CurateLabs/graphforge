"""Keep benchmark packages private and engine access behind the public facade."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
from typing import Any

from graphforge_bench.smoke import workspace_root


def validate_metadata(metadata: dict[str, Any], root: Path) -> int:
    root = root.resolve()
    packages = {package["id"]: package for package in metadata["packages"]}
    local = {key: value for key, value in packages.items() if value["source"] is None}
    if not local:
        raise RuntimeError("benchmark Rust workspace has no local packages")
    manifests = {key: Path(value["manifest_path"]).resolve() for key, value in local.items()}
    benchmarks = {key for key, path in manifests.items() if root in path.parents}
    dependencies = {
        node["id"]: {dependency["pkg"] for dependency in node["deps"]}
        for node in metadata["resolve"]["nodes"]
    }
    infrastructure = {
        ("graphforge-benchmark-certify", root / "runners/certify/Cargo.toml"): {
            "graphforge-filesystem",
            "graphforge-storage",
        },
        (
            "graphforge-benchmark-graph500-generator",
            root / "runners/graph500-generator/Cargo.toml",
        ): {"graphforge-filesystem"},
    }
    pending: list[str] = []
    for key in benchmarks:
        package = local[key]
        if package["publish"] != []:
            raise RuntimeError(f"benchmark package is publishable: {package['name']}")
        allowed = {"graphforge-api"} | infrastructure.get((package["name"], manifests[key]), set())
        for dependency in (dependencies.get(key, set()) & local.keys()) - benchmarks:
            name = local[dependency]["name"]
            if name not in allowed or manifests[dependency] != (
                root.parent / "crates" / name / "Cargo.toml"
            ):
                raise RuntimeError(
                    f"benchmark package bypasses approved engine boundary: {package['name']}"
                )
            pending.append(dependency)
    engine: set[str] = set()
    while pending:
        package = pending.pop()
        if package not in engine:
            engine.add(package)
            pending.extend(dependencies.get(package, set()) - engine)
    for key in local.keys() - benchmarks:
        if key not in engine or manifests[key].parent.parent != root.parent / "crates":
            raise RuntimeError(f"local dependency escapes public engine graph: {manifests[key]}")
    return len(local)


def main() -> None:
    root = workspace_root().resolve()
    environment = os.environ.copy()
    for variable in ("MAKEFLAGS", "MFLAGS", "CARGO_MAKEFLAGS"):
        environment.pop(variable, None)
    output = subprocess.run(
        [
            "cargo",
            "metadata",
            "--locked",
            "--format-version",
            "1",
            "--manifest-path",
            str(root / "Cargo.toml"),
        ],
        check=True,
        env=environment,
        stdout=subprocess.PIPE,
        text=True,
    )
    metadata = json.loads(output.stdout)
    count = validate_metadata(metadata, root)
    print(f"benchmark Rust public facade boundary verified: local_packages={count}")


if __name__ == "__main__":
    main()
