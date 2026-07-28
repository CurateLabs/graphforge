"""Tests for local publication dry-run helpers."""

import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "publish_dry_run.py"
SPEC = importlib.util.spec_from_file_location("publish_dry_run", SCRIPT)
assert SPEC and SPEC.loader
publish_dry_run = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publish_dry_run)


def test_cargo_order_skips_gf_core_conflict() -> None:
    order, _source = publish_dry_run.cargo_publish_order()
    assert order
    assert "gf-core" not in order
    assert "gf-bindings-py" not in order
    assert "gf-bindings-node" not in order
    assert "gf-cli" not in order


def test_npm_dry_run_surfaces_ok() -> None:
    steps = publish_dry_run.dry_run_npm()
    assert len(steps) == 2
    assert all(step["ok"] for step in steps)
