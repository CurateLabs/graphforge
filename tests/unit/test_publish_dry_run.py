"""Tests for local publication dry-run helpers."""

import importlib.util
from pathlib import Path

import yaml

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "publish_dry_run.py"
SPEC = importlib.util.spec_from_file_location("publish_dry_run", SCRIPT)
assert SPEC and SPEC.loader
publish_dry_run = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publish_dry_run)


def test_cargo_order_contains_complete_public_surface() -> None:
    order, _source = publish_dry_run.cargo_publish_order()
    assert len(order) == 15
    assert order[0] == "graphforge-core"
    assert order[-1] == "graphforge-cli"
    assert "graphforge-bindings-py" not in order
    assert "graphforge-bindings-node" not in order


def _step(cmd: list[str], *, ok: bool = True) -> dict[str, object]:
    return {
        "cmd": cmd,
        "cwd": ".",
        "exit_code": 0 if ok else 1,
        "seconds": 0,
        "stdout_tail": "",
        "stderr_tail": "",
        "ok": ok,
    }


def test_npm_dry_run_constructs_every_publication_command(monkeypatch) -> None:
    calls: list[tuple[list[str], Path | None]] = []

    def fake_run(cmd: list[str], *, cwd: Path | None = None) -> dict[str, object]:
        calls.append((cmd, cwd))
        return _step(cmd)

    monkeypatch.setattr(publish_dry_run, "_run", fake_run)
    steps = publish_dry_run.dry_run_npm()

    assert len(steps) == 4
    assert all(step["ok"] for step in steps)
    assert calls == [
        (["pnpm", "install", "--frozen-lockfile"], None),
        (
            [
                "npm",
                "publish",
                "--dry-run",
                "--ignore-scripts",
                "--tag",
                "dry-run",
            ],
            publish_dry_run.NPM_PACKAGES[0],
        ),
        (
            [
                "pnpm",
                "publish",
                "--dry-run",
                "--no-git-checks",
                "--tag",
                "dry-run",
            ],
            publish_dry_run.NPM_PACKAGES[1],
        ),
        (
            [
                "npm",
                "publish",
                "--dry-run",
                "--ignore-scripts",
                "--tag",
                "dry-run",
            ],
            publish_dry_run.NPM_PACKAGES[2],
        ),
    ]


def test_npm_dry_run_stops_when_dependency_install_fails(monkeypatch) -> None:
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], *, cwd: Path | None = None) -> dict[str, object]:
        del cwd
        calls.append(cmd)
        return _step(cmd, ok=False)

    monkeypatch.setattr(publish_dry_run, "_run", fake_run)

    assert publish_dry_run.dry_run_npm() == [
        _step(["pnpm", "install", "--frozen-lockfile"], ok=False)
    ]
    assert calls == [["pnpm", "install", "--frozen-lockfile"]]


def test_release_candidate_keeps_the_real_npm_dry_run_gate() -> None:
    workflow_path = (
        Path(__file__).resolve().parents[2]
        / ".github"
        / "workflows"
        / "binding-release-candidate.yml"
    )
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    steps = workflow["jobs"]["release_candidate"]["steps"]
    publication_step = next(
        step for step in steps if step.get("name") == "Record publication dry-runs"
    )
    assert publication_step["run"].split() == [
        "python3",
        "scripts/publish_dry_run.py",
        "--surface",
        "npm,docs,python,cargo-package",
        "--report",
        "candidate/release-artifacts/evidence/publication-dry-run.json",
    ]
