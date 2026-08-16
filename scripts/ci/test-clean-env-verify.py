#!/usr/bin/env python3
"""Unit tests for clean-environment verification harness (#2795)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any

SCRIPT = Path(__file__).with_name("clean-env-verify.py")
SPEC = importlib.util.spec_from_file_location("clean_env_verify", SCRIPT)
assert SPEC and SPEC.loader
cev = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = cev  # required for dataclasses under Python 3.9
SPEC.loader.exec_module(cev)

assert len(cev.DEFAULT_CRATES) == 16
assert cev.DEFAULT_CRATES[0] == "graphforge-core"
assert cev.DEFAULT_CRATES[-1] == "graphforge-cli"
assert cev.LANE_ISSUES["cargo"] == 185
assert cev.LANE_RUNNERS["cargo"] is cev.lane_cargo


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def sample_record(version: str = "0.5.0") -> dict[str, Any]:
    return {
        "schema": cev.RELEASE_RECORD_SCHEMA,
        "version": version,
        "tag": f"v{version}",
        "commit_sha": "a" * 40,
        "artifacts": [
            {
                "surface": "pypi",
                "name": "graphforge",
                "version": version,
                "filename": f"graphforge-{version}-py3-none-any.whl",
                "sha256": "b" * 64,
            },
        ],
    }


def test_reject_dev_version() -> None:
    try:
        cev.require_version("0.5.0-dev")
    except cev.VerifyError as exc:
        assert "non-release" in str(exc)
    else:
        raise AssertionError("expected VerifyError for dev version")


def test_validate_release_record_ok() -> None:
    cev.validate_release_record(sample_record())
    candidate = sample_record()
    candidate["schema"] = cev.RELEASE_CANDIDATE_SCHEMA
    cev.validate_release_record(candidate)


def test_validate_release_record_bad_digest() -> None:
    record = sample_record()
    record["artifacts"][0]["sha256"] = "not-hex"
    try:
        cev.validate_release_record(record)
    except cev.VerifyError:
        return
    raise AssertionError("expected VerifyError")


def test_validate_evidence_ok() -> None:
    evidence = cev.build_evidence(
        "0.5.0",
        [
            cev.LaneResult(name="pip", issue=180, ok=True),
            cev.LaneResult(name="urls", issue=186, ok=True),
        ],
        preflight=cev.LaneResult(name="preflight", issue=None, ok=True),
    )
    cev.validate_evidence(evidence)
    assert evidence["ok"] is True
    assert evidence["issue_map"]["pip"] == 180


def test_preflight_fails_closed_when_unpublished() -> None:
    def fetch(_url: str) -> tuple[int, bytes, dict[str, str]]:
        return 404, b"missing", {}

    with tempfile.TemporaryDirectory() as tmp:
        ctx = cev.Context(
            version="0.5.0",
            work_root=Path(tmp),
            docs_base=cev.DEFAULT_DOCS_BASE,
            crates=(),
            release_record=None,
            fetch=fetch,
            run_cmd=cev.run_subprocess,
            allow_network_install=False,
        )
        result = cev.run_preflight(ctx)
        assert result.ok is False
        assert result.error is not None
        assert "#192" in result.error
        assert "unpublished" in " ".join(result.notes)


def test_preflight_ok_when_published() -> None:
    def fetch(url: str) -> tuple[int, bytes, dict[str, str]]:
        if "pypi.org/pypi/graphforge/0.5.0/json" in url:
            return 200, json.dumps({"info": {"version": "0.5.0"}}).encode(), {}
        if "registry.npmjs.org/@curatelabs/graphforge/0.5.0" in url:
            return 200, b"{}", {}
        if "registry.npmjs.org/@curatelabs/graphforge-cli/0.5.0" in url:
            return 200, b"{}", {}
        if "registry.npmjs.org/@curatelabs/graphforge-agent-skills/0.5.0" in url:
            return 200, b"{}", {}
        if "crates.io/api/v1/crates/graphforge-api/0.5.0" in url:
            return 200, json.dumps({"version": {"num": "0.5.0"}}).encode(), {}
        return 404, b"", {}

    with tempfile.TemporaryDirectory() as tmp:
        ctx = cev.Context(
            version="0.5.0",
            work_root=Path(tmp),
            docs_base=cev.DEFAULT_DOCS_BASE,
            crates=(),
            release_record=None,
            fetch=fetch,
            run_cmd=cev.run_subprocess,
            allow_network_install=False,
        )
        result = cev.run_preflight(ctx)
        assert result.ok is True, result.error
        assert result.notes == [
            "PyPI and npm (@curatelabs/graphforge, @curatelabs/graphforge-cli, "
            "@curatelabs/graphforge-agent-skills); "
            "no crates.io packages configured; probes OK for v0.5.0"
        ]

        crate_ctx = cev.Context(
            version="0.5.0",
            work_root=Path(tmp),
            docs_base=cev.DEFAULT_DOCS_BASE,
            crates=("graphforge-api",),
            release_record=None,
            fetch=fetch,
            run_cmd=cev.run_subprocess,
            allow_network_install=False,
        )
        crate_result = cev.run_preflight(crate_ctx)
        assert crate_result.ok is True, crate_result.error
        assert crate_result.notes == [
            "PyPI and npm (@curatelabs/graphforge, @curatelabs/graphforge-cli, "
            "@curatelabs/graphforge-agent-skills), "
            "plus crates.io (graphforge-api); probes OK for v0.5.0"
        ]


def test_urls_lane_reports_failures() -> None:
    def fetch(url: str) -> tuple[int, bytes, dict[str, str]]:
        if "quickstart" in url:
            return 404, b"", {}
        return 200, b"ok", {}

    with tempfile.TemporaryDirectory() as tmp:
        ctx = cev.Context(
            version="0.5.0",
            work_root=Path(tmp),
            docs_base=cev.DEFAULT_DOCS_BASE,
            crates=(),
            release_record=None,
            fetch=fetch,
            run_cmd=cev.run_subprocess,
            allow_network_install=False,
        )
        result = cev.lane_urls(ctx)
        assert result.ok is False
        assert result.error and "docs_quickstart" in result.error


def test_urls_lane_tolerates_optional_html_cdn_blocks() -> None:
    def fetch(url: str) -> tuple[int, bytes, dict[str, str]]:
        if "www.npmjs.com" in url or url.startswith("https://crates.io/crates/"):
            return 403, b"blocked", {}
        return 200, b"ok", {}

    with tempfile.TemporaryDirectory() as tmp:
        ctx = cev.Context(
            version="0.5.2",
            work_root=Path(tmp),
            docs_base=cev.DEFAULT_DOCS_BASE,
            crates=("graphforge-api",),
            release_record=None,
            fetch=fetch,
            run_cmd=cev.run_subprocess,
            allow_network_install=False,
        )
        result = cev.lane_urls(ctx)
        assert result.ok is True
        assert any("optional human HTML" in note for note in result.notes)
        assert result.artifacts["statuses"]["npm_node"] == 200
        assert result.artifacts["statuses"]["crates_graphforge-api"] == 200
        assert result.artifacts["statuses"]["docs_licensing"] == 200


def test_checksums_match_release_record() -> None:
    record = sample_record()

    def fetch(url: str) -> tuple[int, bytes, dict[str, str]]:
        if "pypi.org/pypi/graphforge/0.5.0/json" in url:
            return (
                200,
                json.dumps(
                    {
                        "urls": [
                            {
                                "filename": "graphforge-0.5.0-py3-none-any.whl",
                                "digests": {"sha256": "b" * 64},
                            }
                        ]
                    }
                ).encode(),
                {},
            )
        if "registry.npmjs.org/@curatelabs/graphforge/0.5.0" in url:
            return (
                200,
                json.dumps(
                    {
                        "dist": {
                            "tarball": "https://example.test/curatelabs-graphforge-0.5.0.tgz",
                            "integrity": "sha256-abc",
                        }
                    }
                ).encode(),
                {},
            )
        if "registry.npmjs.org/@curatelabs/graphforge-cli/0.5.0" in url:
            return (
                200,
                json.dumps(
                    {
                        "dist": {
                            "tarball": ("https://example.test/curatelabs-graphforge-cli-0.5.0.tgz"),
                            "integrity": "sha256-cli",
                        }
                    }
                ).encode(),
                {},
            )
        if "registry.npmjs.org/@curatelabs/graphforge-agent-skills/0.5.0" in url:
            return (
                200,
                json.dumps(
                    {
                        "dist": {
                            "tarball": "https://example.test/agent-skills-0.5.0.tgz",
                            "integrity": "sha256-def",
                        }
                    }
                ).encode(),
                {},
            )
        return 404, b"", {}

    with tempfile.TemporaryDirectory() as tmp:
        ctx = cev.Context(
            version="0.5.0",
            work_root=Path(tmp),
            docs_base=cev.DEFAULT_DOCS_BASE,
            crates=(),
            release_record=record,
            fetch=fetch,
            run_cmd=cev.run_subprocess,
            allow_network_install=False,
        )
        result = cev.lane_checksums(ctx)
        assert result.ok is True, result.error
        assert "graphforge-0.5.0-py3-none-any.whl" in " ".join(result.artifacts["matched"])


def test_cli_lane_installs_and_executes_published_package() -> None:
    commands: list[list[str]] = []

    def run(
        argv: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        commands.append(argv)
        stdout = "graphforge 0.5.0\n" if argv[0] == "npx" else ""
        return subprocess.CompletedProcess(argv, 0, stdout=stdout, stderr="")

    with tempfile.TemporaryDirectory() as tmp:
        ctx = cev.Context(
            version="0.5.0",
            work_root=Path(tmp),
            docs_base=cev.DEFAULT_DOCS_BASE,
            crates=(),
            release_record=None,
            fetch=lambda _url: (200, b"{}", {}),
            run_cmd=run,
        )
        result = cev.lane_cli(ctx)
        assert result.ok is True, result.error
        assert ["npm", "install", "@curatelabs/graphforge-cli@0.5.0"] in commands
        assert [
            "npx",
            "--offline",
            "--no-install",
            "graphforge",
            "--version",
        ] in commands


def test_cli_validate_release_record() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "record.json"
        write_json(path, sample_record())
        completed = subprocess.run(
            [sys.executable, str(SCRIPT), "validate-release-record", str(path)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr


def test_cli_run_refuses_without_lanes() -> None:
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), "run", "--version", "0.5.0"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode != 0
    assert "lane" in (completed.stderr + completed.stdout).lower()


def main() -> None:
    test_reject_dev_version()
    test_validate_release_record_ok()
    test_validate_release_record_bad_digest()
    test_validate_evidence_ok()
    test_preflight_fails_closed_when_unpublished()
    test_preflight_ok_when_published()
    test_urls_lane_reports_failures()
    test_checksums_match_release_record()
    test_cli_lane_installs_and_executes_published_package()
    test_cli_validate_release_record()
    test_cli_run_refuses_without_lanes()
    print("clean-env-verify tests passed")


if __name__ == "__main__":
    main()
