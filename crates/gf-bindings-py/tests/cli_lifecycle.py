"""Packed-wheel acceptance for the complete repository CLI lifecycle.

Every GraphForge invocation starts a new process.  CI can set
``GRAPHFORGE_UVX_WHEEL`` to exercise the wheel through ``uvx --from``; the
installed-wheel test loop exercises the same scenario through the generated
console script.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sysconfig
import tempfile
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
CONTRACT = json.loads((ROOT / "tests/contracts/repository-cli-lifecycle.json").read_text())
IDENTITIES = CONTRACT["identities"]
REQUIRED_SCENARIOS = {scenario["name"] for scenario in CONTRACT["requiredScenarios"]}


def executable() -> list[str]:
    override = os.environ.get("GRAPHFORGE_CLI")
    if override:
        script = Path(override)
        assert script.is_file(), f"missing GRAPHFORGE_CLI executable: {script}"
        return [str(script)]
    wheel = os.environ.get("GRAPHFORGE_UVX_WHEEL")
    if wheel:
        return ["uvx", "--offline", "--from", wheel, "graphforge"]
    scripts = Path(sysconfig.get_path("scripts"))
    name = "graphforge.exe" if os.name == "nt" else "graphforge"
    script = scripts / name
    assert script.is_file(), f"missing installed console script: {script}"
    return [str(script)]


def invoke(
    command: list[str],
    project: Path,
    *,
    expected: int = 0,
    json_result: bool = True,
) -> subprocess.CompletedProcess[bytes]:
    arguments = [
        *executable(),
        "--project-dir",
        str(project),
        *(["--json"] if json_result else []),
        *command,
    ]
    completed = subprocess.run(arguments, check=False, capture_output=True)
    assert completed.returncode == expected, (
        arguments,
        completed.returncode,
        completed.stdout,
        completed.stderr,
    )
    return completed


def result(command: list[str], project: Path, *, expected: int = 0) -> Any:
    completed = invoke(command, project, expected=expected)
    assert completed.stderr == b"", completed.stderr
    return json.loads(completed.stdout)


def error(command: list[str], project: Path, *, expected: int = 2) -> Any:
    completed = invoke(command, project, expected=expected)
    assert completed.stdout == b"", completed.stdout
    return json.loads(completed.stderr)


def git(project: Path, *arguments: str) -> bytes:
    completed = subprocess.run(
        ["git", "-C", str(project), *arguments], check=False, capture_output=True
    )
    assert completed.returncode == 0, (arguments, completed.stdout, completed.stderr)
    return completed.stdout


def column(result_value: dict[str, Any], name: str) -> Any:
    names = [item["name"] for item in result_value["columns"]]
    return result_value["rows"][0][names.index(name)]


def assert_git_boundary(project: Path) -> None:
    ignored_payloads = {
        ".graphforge/state/should-never-stage.parquet": b"graph data",
        ".graphforge/imports/source.arrow": b"source data",
        ".graphforge/exports/archive.gfportable": b"export data",
    }
    for relative, payload in ignored_payloads.items():
        target = project / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)

    git(project, "add", "--all")
    staged = set(git(project, "diff", "--cached", "--name-only").decode().splitlines())
    required = {
        ".graphforge/graphforge.yaml",
        ".graphforge/ontology/keep.yaml",
        ".graphforge/schemas/example.json",
        ".graphforge/migrations/001.yaml",
        ".graphforge/seeds/example.yaml",
    }
    assert required <= staged, (required - staged, staged)
    assert not (set(ignored_payloads) & staged), set(ignored_payloads) & staged
    forbidden_suffixes = (".arrow", ".parquet", ".db", ".sqlite", ".gfportable")
    assert not any(path.endswith(forbidden_suffixes) for path in staged), staged


def assert_tracked_data_guard(parent: Path) -> None:
    unsafe = parent / "unsafe-tracked-data"
    unsafe.mkdir()
    git(unsafe, "init", "--quiet")
    payloads = {
        ".graphforge/seeds/materialized/seed.json": b"materialized seed",
        ".graphforge/snapshots/snapshot.json": b"snapshot",
        ".graphforge/source.parquet": b"source graph",
    }
    for relative, payload in payloads.items():
        target = unsafe / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
    git(unsafe, "add", "--force", "--all")
    rejected = error(["init"], unsafe)
    assert rejected["error"]["code"] == "GF_VALIDATION"
    assert "graph data is tracked by Git" in rejected["error"]["message"]
    assert not (unsafe / ".graphforge/state").exists()


def main() -> None:
    assert CONTRACT["contract"] == "graphforge-packed-cli-lifecycle/1"
    assert CONTRACT["scope"]["portableInterchange"] == "complete_project_generation"
    assert CONTRACT["scope"]["ontologyLifecycle"] == {
        "owner": "#236",
        "operations": [
            "inspect_runtime_catalog",
            "suggest_ontology",
            "validate_ontology",
            "export_ontology",
        ],
    }
    assert CONTRACT["scope"]["ontologyBindingParity"] == {
        "owner": "#237",
        "operations": [
            "inspect_runtime_catalog",
            "suggest_ontology",
            "validate_ontology",
            "export_ontology",
            "adopt_ontology",
            "clear_ontology",
        ],
    }
    covered: set[str] = set()

    with tempfile.TemporaryDirectory(prefix="graphforge-python-lifecycle-") as directory:
        parent = Path(directory)
        source = parent / "source"
        source.mkdir()
        git(source, "init", "--quiet")

        initialized = result(["init"], source)
        assert initialized["created_config"] is True
        assert initialized["skills"]["changed"] is True
        assert (source / ".agents/skills/.graphforge-managed.json").is_file()
        assert (source / ".agents/skills/graphforge-bootstrap/SKILL.md").is_file()
        assert (source / ".agents/skills/graphforge-build-knowledge/SKILL.md").is_file()
        first_current = (source / ".graphforge/state/CURRENT").read_bytes()

        reopened = result(["init"], source)
        assert reopened["created_config"] is False
        assert reopened["ignore_changed"] is False
        assert reopened["skills"]["changed"] is False
        assert (source / ".graphforge/state/CURRENT").read_bytes() == first_current
        ignore = (source / ".gitignore").read_text()
        for entry in (
            "/.graphforge/state/",
            "/.graphforge/imports/",
            "/.graphforge/exports/",
        ):
            assert ignore.count(entry) == 1, ignore
        covered.add("init_and_reopen")

        shutil.copyfile(
            ROOT / "docs/contracts/examples/graphforge-v1.yaml",
            source / ".graphforge/graphforge.yaml",
        )
        before_static = (source / ".graphforge/state/CURRENT").read_bytes()
        assert result(["config", "validate"], source) == {"valid": True}
        resolved = invoke(["config", "resolve"], source)
        assert resolved.stderr == b""
        assert (
            resolved.stdout
            == (ROOT / "docs/contracts/examples/graphforge-resolved-v1.json").read_bytes()
        )
        infra = invoke(["infra", "validate", "--target", "production"], source)
        assert infra.stderr == b""
        assert (
            infra.stdout
            == (
                ROOT / "docs/contracts/examples/graphforge-infra-validation-production-v1.json"
            ).read_bytes()
        )
        assert (source / ".graphforge/state/CURRENT").read_bytes() == before_static
        covered.add("configuration_and_static_infra")

        external_payload = source / ".graphforge/imports/unrelated.parquet"
        external_payload.parent.mkdir(parents=True, exist_ok=True)
        external_payload.write_bytes(b"must not be scanned or ingested")
        before_check = (source / ".graphforge/state/CURRENT").read_bytes()
        check = result(["sync", "--check"], source, expected=4)
        assert check["status"] == "drift"
        assert (source / ".graphforge/state/CURRENT").read_bytes() == before_check
        sync = result(
            [
                "sync",
                "--idempotency-key",
                IDENTITIES["syncOperation"],
                "--actor-uuid",
                IDENTITIES["syncActor"],
            ],
            source,
        )
        assert sync["status"] == "published"
        assert sync["requested_operation_uuid"] == IDENTITIES["syncOperation"]
        assert sync["snapshot_actor_uuid"] == IDENTITIES["syncActor"]
        after_sync = (source / ".graphforge/state/CURRENT").read_bytes()
        assert after_sync != before_check
        replay = result(
            [
                "sync",
                "--idempotency-key",
                IDENTITIES["syncOperation"],
                "--actor-uuid",
                IDENTITIES["syncActor"],
            ],
            source,
        )
        assert replay["status"] == "in_sync"
        assert replay["idempotent_replay"] is True
        assert (source / ".graphforge/state/CURRENT").read_bytes() == after_sync
        assert result(["sync", "--check"], source)["status"] == "in_sync"
        assert external_payload.read_bytes() == b"must not be scanned or ingested"
        covered.add("sync_check_apply_and_replay")

        checkpoint = result(
            [
                "checkpoint",
                "create",
                "before-change",
                "--idempotency-key",
                IDENTITIES["checkpointCreateOperation"],
            ],
            source,
        )
        checkpoint_uuid = column(checkpoint, "checkpoint_uuid")
        listed = result(["checkpoint", "list"], source)
        assert listed["rows"]
        shown = result(["checkpoint", "show", "before-change"], source)
        assert column(shown, "checkpoint_uuid") == checkpoint_uuid
        diff = result(
            [
                "checkpoint",
                "diff",
                "--from",
                "before-change",
                "--to-current",
                "--scope",
                "summary",
                "--detail",
                "summary",
            ],
            source,
        )
        assert diff["contract"] == "graphforge-cli-result/1"

        before_revert = (source / ".graphforge/state/CURRENT").read_bytes()
        preview = result(["revert", "before-change", "--preview"], source)
        assert preview["contract"] == "graphforge-revert-preview/1"
        assert (source / ".graphforge/state/CURRENT").read_bytes() == before_revert
        refusal = error(
            [
                "revert",
                "before-change",
                "--reason",
                "packed lifecycle acceptance",
                "--idempotency-key",
                IDENTITIES["revertOperation"],
                "--actor-uuid",
                IDENTITIES["revertActor"],
            ],
            source,
        )
        assert refusal["error"]["code"] == "GF_VALIDATION"
        assert (source / ".graphforge/state/CURRENT").read_bytes() == before_revert
        reverted = result(
            [
                "revert",
                "before-change",
                "--reason",
                "packed lifecycle acceptance",
                "--idempotency-key",
                IDENTITIES["revertOperation"],
                "--actor-uuid",
                IDENTITIES["revertActor"],
                "--yes",
            ],
            source,
        )
        assert column(reverted, "operation_uuid") == IDENTITIES["revertOperation"]
        after_revert = (source / ".graphforge/state/CURRENT").read_bytes()
        assert after_revert != before_revert
        replayed_revert = result(
            [
                "revert",
                "before-change",
                "--reason",
                "packed lifecycle acceptance",
                "--idempotency-key",
                IDENTITIES["revertOperation"],
                "--actor-uuid",
                IDENTITIES["revertActor"],
                "--yes",
            ],
            source,
        )
        assert column(replayed_revert, "operation_uuid") == IDENTITIES["revertOperation"]
        assert (source / ".graphforge/state/CURRENT").read_bytes() == after_revert
        assert result(["checkpoint", "show", "before-change"], source)["rows"]
        deleted = result(
            [
                "checkpoint",
                "delete",
                "before-change",
                "--idempotency-key",
                IDENTITIES["checkpointDeleteOperation"],
            ],
            source,
        )
        assert column(deleted, "operation_uuid") == IDENTITIES["checkpointDeleteOperation"]
        covered.add("checkpoint_and_top_level_revert")

        exports = source / ".graphforge/exports"
        exports.mkdir(parents=True, exist_ok=True)
        envelope = exports / "current.gfportable"
        exported = result(
            ["export", "--current", "--output", str(envelope)],
            source,
        )
        assert exported["contract"] == "graphforge-portable-export/1"
        assert exported["source"] == "current"
        assert exported["checkpoint"] is None
        assert envelope.is_file()
        duplicate = exports / "current-duplicate.gfportable"
        duplicate_export = result(
            ["export", "--current", "--output", str(duplicate)],
            source,
        )
        assert duplicate_export["envelope_sha256"] == exported["envelope_sha256"]
        assert duplicate.read_bytes() == envelope.read_bytes()

        destination = parent / "destination"
        destination.mkdir()
        git(destination, "init", "--quiet")
        result(["init"], destination)
        imported = result(
            [
                "import",
                "--input",
                str(envelope),
                "--idempotency-key",
                IDENTITIES["importOperation"],
            ],
            destination,
        )
        assert imported["contract"] == "graphforge-portable-import/1"
        assert imported["source_generation_uuid"] == exported["generation_uuid"]
        assert imported["envelope_sha256"] == exported["envelope_sha256"]
        assert imported["idempotent_replay"] is False
        destination_current = (destination / ".graphforge/state/CURRENT").read_bytes()
        assert result(["checkpoint", "list"], destination)["contract"] == (
            "graphforge-cli-result/1"
        )
        assert (destination / ".graphforge/state/CURRENT").read_bytes() == (destination_current)
        covered.add("portable_export_import_and_reopen")

        for relative, body in {
            ".graphforge/ontology/keep.yaml": "version: 1\n",
            ".graphforge/schemas/example.json": "{}\n",
            ".graphforge/migrations/001.yaml": "version: 1\n",
            ".graphforge/seeds/example.yaml": "version: 1\n",
        }.items():
            path = source / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(body)
        assert_git_boundary(source)
        assert_tracked_data_guard(parent)
        covered.add("git_data_boundary")

        state_before_refusal = (source / ".graphforge/state/CURRENT").read_bytes()
        remove_error = error(["remove"], source)
        assert remove_error["error"]["code"] == "GF_VALIDATION"
        assert (source / ".graphforge/state/CURRENT").read_bytes() == state_before_refusal
        removed = result(["remove", "--yes"], source)
        assert removed == {"removed": True, "target": ".graphforge/state"}
        assert not (source / ".graphforge/state").exists()
        assert (source / ".graphforge/ontology/keep.yaml").is_file()
        assert envelope.is_file()
        assert (source / ".graphforge/imports/source.arrow").is_file()
        assert (source / ".agents/skills/.graphforge-managed.json").is_file()
        covered.add("remove_refusal_and_confirmation")

    assert covered == REQUIRED_SCENARIOS, (REQUIRED_SCENARIOS - covered, covered)


if __name__ == "__main__":
    main()
