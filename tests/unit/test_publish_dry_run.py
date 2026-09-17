"""Tests for local publication dry-run helpers."""

import importlib.util
import json
from pathlib import Path

import pytest
import yaml

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "publish_dry_run.py"
SPEC = importlib.util.spec_from_file_location("publish_dry_run", SCRIPT)
assert SPEC and SPEC.loader
publish_dry_run = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publish_dry_run)


def test_cargo_order_contains_complete_public_surface() -> None:
    order, _source = publish_dry_run.cargo_publish_order()
    assert len(order) == 20
    assert order[0] == "graphforge-core"
    assert order.index("graphforge-core") < order.index("graphforge-portable-oci")
    assert order.index("graphforge-portable-oci") < order.index("graphforge-api")
    assert order.index("graphforge-core") < order.index("graphforge-value")
    assert order.index("graphforge-value") < order.index("graphforge-ir")
    assert order[-1] == "graphforge-cli"
    assert "graphforge-discovery" in order
    assert order.index("graphforge-observability") < order.index("graphforge-api")
    assert "graphforge-bindings-py" not in order
    assert "graphforge-bindings-node" not in order


def test_missing_crate_plan_is_a_hard_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """#1377: no fallback crate list; an absent plan fails instead of publishing a
    different set."""
    absent = publish_dry_run.ROOT / "scripts" / "ci" / "crate-publish-plan-absent.py"
    monkeypatch.setattr(publish_dry_run, "CRATE_PLAN", absent)
    with pytest.raises(publish_dry_run.CratePlanError) as raised:
        publish_dry_run.cargo_publish_order()
    assert "publish plan is missing" in str(raised.value)
    assert not hasattr(publish_dry_run, "FALLBACK_CARGO_ORDER")


def test_failing_crate_plan_is_a_hard_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    class Failed:
        returncode = 3
        stdout = ""
        stderr = "plan exploded"

    def failed_run(*_args: object, **_kwargs: object) -> Failed:
        return Failed()

    monkeypatch.setattr(publish_dry_run.subprocess, "run", failed_run)
    with pytest.raises(publish_dry_run.CratePlanError) as raised:
        publish_dry_run.cargo_publish_order()
    assert "plan exploded" in str(raised.value)


def test_cargo_surface_exits_nonzero_without_a_plan(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    absent = publish_dry_run.ROOT / "scripts" / "ci" / "crate-publish-plan-absent.py"
    monkeypatch.setattr(publish_dry_run, "CRATE_PLAN", absent)
    assert publish_dry_run.main(["--surface", "cargo-package"]) == 2
    assert "publish plan is missing" in capsys.readouterr().err


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
    monkeypatch.setattr(publish_dry_run, "npm_dist_tag", lambda _package_dir: "next")
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
                "next",
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
                "next",
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
                "next",
            ],
            publish_dry_run.NPM_PACKAGES[2],
        ),
    ]


def test_npm_dry_run_uses_the_publisher_dist_tag_policy() -> None:
    """The dry run must show the tag a real publish would assign, not a placeholder."""
    publisher = publish_dry_run._npm_publisher()
    assert publisher.dist_tag_for("0.6.0") == "latest"
    assert publisher.dist_tag_for("0.6.0-rc.1") == "next"
    for package_dir in publish_dry_run.NPM_PACKAGES:
        version = json.loads((package_dir / "package.json").read_text(encoding="utf-8"))["version"]
        assert publish_dry_run.npm_dist_tag(package_dir) == publisher.dist_tag_for(version)


def test_npm_dry_run_fails_closed_on_an_unclassifiable_version(monkeypatch) -> None:
    def fake_run(cmd: list[str], *, cwd: Path | None = None) -> dict[str, object]:
        del cwd
        return _step(cmd)

    def unclassifiable(_package_dir: Path) -> str:
        raise ValueError("cannot classify version '0.6.0rc1'")

    monkeypatch.setattr(publish_dry_run, "_run", fake_run)
    monkeypatch.setattr(publish_dry_run, "npm_dist_tag", unclassifiable)
    steps = publish_dry_run.dry_run_npm()

    assert len(steps) == 2
    assert steps[-1]["ok"] is False
    assert "cannot derive npm dist-tag" in steps[-1]["stderr_tail"]


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
