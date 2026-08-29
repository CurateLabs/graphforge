"""No-cost dependency and fixture-discovery smoke."""

from __future__ import annotations

import importlib
import json
from pathlib import Path

from jsonschema import Draft202012Validator

FIXTURE_DIRECTORIES = ("profiles", "suites", "schemas", "fly")


def workspace_root() -> Path:
    return Path(__file__).resolve().parents[2]


def discover_fixtures(root: Path | None = None) -> dict[str, tuple[Path, ...]]:
    base = root or workspace_root()
    discovered: dict[str, tuple[Path, ...]] = {}
    for directory in FIXTURE_DIRECTORIES:
        fixture_root = base / directory
        fixtures = tuple(sorted(fixture_root.rglob("*.json")))
        if not fixtures:
            raise RuntimeError(f"no fixtures found in {directory}")
        for fixture in fixtures:
            with fixture.open(encoding="utf-8") as handle:
                document = json.load(handle)
            if not isinstance(document, dict) or not document.get("schema"):
                raise RuntimeError(f"fixture has no schema: {fixture.name}")
            if directory == "schemas":
                Draft202012Validator.check_schema(document)
        discovered[directory] = fixtures
    return discovered


def main() -> None:
    for module in ("reframe", "benchexec"):
        importlib.import_module(module)
    discovered = discover_fixtures()
    print(
        "benchmark workspace smoke passed: "
        + ", ".join(f"{name}={len(files)}" for name, files in discovered.items())
    )


if __name__ == "__main__":
    main()
