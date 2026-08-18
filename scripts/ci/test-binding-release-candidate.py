#!/usr/bin/env python3
"""Deterministic negative tests for the binding RC evidence validator."""

from __future__ import annotations

import copy
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

from workflow_policy import (
    job_needs,
    job_required_run_scalars,
    job_runs_exact,
    job_scalar,
    normalize_run,
    workflow_jobs,
)

ROOT = Path(__file__).resolve().parents[2]
VALIDATOR = ROOT / "scripts/ci/validate-binding-release-candidate.py"
CONTRACT = ROOT / "tests/contracts/binding-release-candidate-targets.json"
RC_WORKFLOW = ROOT / ".github/workflows/binding-release-candidate.yml"
README = ROOT / "README.md"
PUBLISH_WORKFLOW = ROOT / ".github/workflows/publish.yaml"
ARTIFACT_VALIDATOR = ROOT / "scripts/ci/validate-napi-artifacts.py"
WRAPPER_PREPARER = ROOT / "scripts/ci/prepare-rustc-wrapper.py"
STRICT_ADD_NODE = ROOT / "crates/graphforge-bindings-py/tests/strict_add_node.py"
SHA = "a" * 40
ARTIFACT_COMMAND = "pnpm exec napi artifacts --output-dir artifacts --npm-dir npm"


def load_validator():
    spec = importlib.util.spec_from_file_location("binding_rc_validator", VALIDATOR)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_artifact_validator():
    spec = importlib.util.spec_from_file_location("napi_artifact_validator", ARTIFACT_VALIDATOR)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def report(target: str, settings: dict[str, str]) -> dict[str, object]:
    mode = settings["execution_mode"]
    return {
        "schema": "graphforge-binding-rc-target/1",
        "source_sha": SHA,
        "language": settings["language"],
        "target": target,
        "package_version": "0.5.0.dev0",
        "artifact": {"name": "artifact", "sha256": "b" * 64},
        "classification": {
            "name": "policy.json",
            "sha256": "c" * 64,
            "schema": 1
            if settings["language"] == "node"
            else "graphforge-python-non-cypher-parity/1",
        },
        "execution": {
            "mode": mode,
            "rationale": "incompatible GitHub-hosted runner architecture"
            if mode == "package-validation"
            else None,
        },
        "fallback_execution": False,
        "cases": [{"identity": "native contract", "outcome": "passed", "sanitized_error": None}],
        "sanitized_parity_diff": [],
    }


def rejected(module, reports, contract, message: str) -> None:
    try:
        module.validate(reports, contract, SHA)
    except ValueError as error:
        assert message in str(error), error
    else:
        raise AssertionError(f"validator accepted invalid evidence: {message}")


def node_matrix_entries(workflow: Path, text: str) -> list[str]:
    if workflow == RC_WORKFLOW:
        return re.findall(r"(?ms)^          - os:.*?(?=^          - os:|^    steps:)", text)
    return [line.strip() for line in text.splitlines() if line.startswith("          - { host:")]


def next_step_environment(base: dict[str, str], github_env: Path) -> dict[str, str]:
    """Apply simple GitHub environment-file assignments to a later step."""
    result = base.copy()
    for assignment in github_env.read_text().splitlines():
        name, value = assignment.split("=", 1)
        result[name] = value
    return result


def required_section(text: str, start: str, end: str) -> str:
    """Return a required policy section or reject a missing delimiter."""
    _, start_found, remainder = text.partition(start)
    assert start_found, f"missing workflow marker: {start}"
    section, end_found, _ = remainder.partition(end)
    assert end_found, f"missing workflow marker: {end}"
    return section


def assert_active_lines(section: str, *expected: str) -> None:
    """Require exact, non-commented workflow lines.

    Expectations ending in ``@`` match any SHA-pinned ``uses: …@<sha> # tag`` line
    with that prefix (Dependabot-style action pins).
    """
    active = {
        line.strip()
        for line in section.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    for line in expected:
        if line.endswith("@"):
            assert any(entry.startswith(line) for entry in active), (
                f"missing active workflow line prefix: {line}"
            )
        else:
            assert line in active, f"missing active workflow line: {line}"


def workflow_step(section: str, marker: str) -> str:
    """Return only the workflow step containing marker."""
    before, found, after = section.partition(marker)
    assert found, f"missing workflow marker: {marker}"
    start = before.rfind("\n      - ")
    assert start >= 0, f"marker is not inside a workflow step: {marker}"
    remainder = before[start + 1 :] + found + after
    end = remainder.find("\n      - ", 1)
    return remainder if end < 0 else remainder[:end]


WINDOWS_DURABILITY_JOB = "windows-graphforge-storage-locks"
MACOS_DURABILITY_JOB = "macos-graphforge-storage-durability"
WINDOWS_RUNNER = "blacksmith-4vcpu-windows-2025"
MACOS_RUNNER = "blacksmith-12vcpu-macos-15"
WINDOWS_PROJECT_LOCK_COMMAND = (
    "cargo test -p graphforge-storage project_generation::tests:: --lib --no-fail-fast"
)
STORAGE_ADMISSION_COMMAND = (
    "cargo test -p graphforge-storage filesystem_admission::tests:: --lib --no-fail-fast"
)
FILESYSTEM_COMMAND = "cargo test -p graphforge-filesystem --lib --no-fail-fast"
NATIVE_ORACLE_CROSSCHECK_COMMAND = (
    "cargo test -p graphforge-storage --features test-failpoints --lib "
    "project_recovery::tests::subprocess_kill_matrix_never_exposes_a_partial_generation "
    "--no-fail-fast -- --exact --nocapture"
)
INJECTED_OPERATION_ERROR_COMMAND = (
    "cargo test -p graphforge-storage --features test-failpoints --lib "
    "project_recovery::tests::injected_operation_errors_report_exact_commit_state "
    "--no-fail-fast -- --exact --nocapture"
)
COMMIT_LOCK_CLONED_DESCRIPTOR_COMMAND = (
    "cargo test -p graphforge-storage --lib "
    "project_publication::tests::commit_lock_guard_unlocks_before_a_cloned_descriptor_closes "
    "--no-fail-fast -- --exact --nocapture"
)
CHECKPOINT_LOCK_CLONED_DESCRIPTOR_COMMAND = (
    "cargo test -p graphforge-storage --lib "
    "project_checkpoints::tests::checkpoint_read_guard_unlocks_before_a_cloned_descriptor_closes "
    "--no-fail-fast -- --exact --nocapture"
)
WINDOWS_OPTIMISTIC_PROMOTION_COMMAND = (
    "cargo test -p graphforge-storage --lib "
    "project_publication::tests::"
    "optimistic_promotion_closes_staged_handles_before_directory_rename "
    "--no-fail-fast -- --exact --nocapture"
)
UPLOAD_ARTIFACT_ACTION = "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1"
NATIVE_ORACLE_EVIDENCE_ENV = "GRAPHFORGE_NATIVE_ORACLE_EVIDENCE"
WINDOWS_NATIVE_ORACLE_EVIDENCE = (
    "${{ runner.temp }}/durability-certification-evidence/native-oracle-windows.json"
)
MACOS_NATIVE_ORACLE_EVIDENCE = (
    "${{ runner.temp }}/durability-certification-evidence/native-oracle-macos.json"
)
NATIVE_ORACLE_ARTIFACT_PATH = "${{ runner.temp }}/durability-certification-evidence"


def validate_native_oracle_artifact(
    job_body: str,
    *,
    platform_name: str,
    evidence_path: str,
) -> None:
    """Require active evidence output and a pinned, fail-closed artifact upload."""
    oracle_step = workflow_step(
        job_body,
        "project_recovery::tests::subprocess_kill_matrix_never_exposes_a_partial_generation",
    )
    assert_active_lines(
        oracle_step,
        f"{NATIVE_ORACLE_EVIDENCE_ENV}: {evidence_path}",
    )
    assert oracle_step.count(NATIVE_ORACLE_EVIDENCE_ENV) == 1

    upload_name = f"Upload {platform_name} native oracle evidence"
    assert job_body.count(upload_name) == 1
    upload_step = workflow_step(job_body, upload_name)
    artifact_slug = platform_name.lower()
    assert_active_lines(
        upload_step,
        f"- name: {upload_name}",
        "if: always()",
        f"uses: {UPLOAD_ARTIFACT_ACTION}",
        f"name: native-oracle-{artifact_slug}-${{{{ github.sha }}}}",
        f"path: {NATIVE_ORACLE_ARTIFACT_PATH}",
        "if-no-files-found: error",
        "retention-days: 1",
    )
    assert "continue-on-error" not in upload_step


def validate_native_test_workflow(workflow_text: str) -> None:
    """Prove native durability jobs and their CI Gate aggregation structurally."""
    jobs = workflow_jobs(workflow_text)
    windows = jobs[WINDOWS_DURABILITY_JOB]
    macos = jobs[MACOS_DURABILITY_JOB]
    assert job_scalar(windows, "runs-on") == WINDOWS_RUNNER
    assert job_scalar(macos, "runs-on") == MACOS_RUNNER
    for body in (windows, macos):
        assert job_needs(body) == {"changes"}
        assert job_scalar(body, "if") == "needs.changes.outputs.rust == 'true'"
        assert job_runs_exact(body, STORAGE_ADMISSION_COMMAND)
        assert job_runs_exact(body, FILESYSTEM_COMMAND)
        assert job_runs_exact(body, NATIVE_ORACLE_CROSSCHECK_COMMAND)
        assert job_runs_exact(body, INJECTED_OPERATION_ERROR_COMMAND)
        assert job_runs_exact(body, COMMIT_LOCK_CLONED_DESCRIPTOR_COMMAND)
        assert job_runs_exact(body, CHECKPOINT_LOCK_CLONED_DESCRIPTOR_COMMAND)
    assert job_runs_exact(windows, WINDOWS_PROJECT_LOCK_COMMAND)
    assert job_runs_exact(windows, WINDOWS_OPTIMISTIC_PROMOTION_COMMAND)
    validate_native_oracle_artifact(
        windows,
        platform_name="Windows",
        evidence_path=WINDOWS_NATIVE_ORACLE_EVIDENCE,
    )
    validate_native_oracle_artifact(
        macos,
        platform_name="macOS",
        evidence_path=MACOS_NATIVE_ORACLE_EVIDENCE,
    )

    gate = jobs["ci-gate"]
    assert {WINDOWS_DURABILITY_JOB, MACOS_DURABILITY_JOB} <= job_needs(gate)
    gate_scalars = [
        scalar
        for scalar in job_required_run_scalars(gate, "scripts/ci/require-gates.sh")
        if normalize_run(scalar).startswith("scripts/ci/require-gates.sh ")
    ]
    assert len(gate_scalars) == 1, "CI Gate must have one active require-gates.sh run scalar"
    gate_scalar = normalize_run(gate_scalars[0])
    assert f"needs.{WINDOWS_DURABILITY_JOB}.result" in gate_scalar
    assert f"needs.{MACOS_DURABILITY_JOB}.result" in gate_scalar


def validate_native_workflow_negative_fixtures() -> None:
    """Reject tokens hidden in comments, env, nested fields, or echo commands."""
    fixture = f"""jobs:
  {WINDOWS_DURABILITY_JOB}:
    runs-on: {WINDOWS_RUNNER}
    needs: changes
    if: needs.changes.outputs.rust == 'true'
    steps:
      - run: >-
          {WINDOWS_PROJECT_LOCK_COMMAND}
      - run: >-
          {STORAGE_ADMISSION_COMMAND}
      - run: >-
          {FILESYSTEM_COMMAND}
      - name: Cross-check Windows publication-kill results against the fault oracle
        env:
          {NATIVE_ORACLE_EVIDENCE_ENV}: {WINDOWS_NATIVE_ORACLE_EVIDENCE}
        run: >-
          {NATIVE_ORACLE_CROSSCHECK_COMMAND}
      - name: Upload Windows native oracle evidence
        if: always()
        uses: {UPLOAD_ARTIFACT_ACTION}
        with:
          name: native-oracle-windows-${{{{ github.sha }}}}
          path: {NATIVE_ORACLE_ARTIFACT_PATH}
          if-no-files-found: error
          retention-days: 1
      - run: >-
          {INJECTED_OPERATION_ERROR_COMMAND}
      - run: >-
          {COMMIT_LOCK_CLONED_DESCRIPTOR_COMMAND}
      - run: >-
          {CHECKPOINT_LOCK_CLONED_DESCRIPTOR_COMMAND}
      - run: >-
          {WINDOWS_OPTIMISTIC_PROMOTION_COMMAND}
  {MACOS_DURABILITY_JOB}:
    runs-on: {MACOS_RUNNER}
    needs: changes
    if: needs.changes.outputs.rust == 'true'
    steps:
      - run: >-
          {STORAGE_ADMISSION_COMMAND}
      - run: >-
          {FILESYSTEM_COMMAND}
      - name: Cross-check macOS publication-kill results against the fault oracle
        env:
          {NATIVE_ORACLE_EVIDENCE_ENV}: {MACOS_NATIVE_ORACLE_EVIDENCE}
        run: >-
          {NATIVE_ORACLE_CROSSCHECK_COMMAND}
      - name: Upload macOS native oracle evidence
        if: always()
        uses: {UPLOAD_ARTIFACT_ACTION}
        with:
          name: native-oracle-macos-${{{{ github.sha }}}}
          path: {NATIVE_ORACLE_ARTIFACT_PATH}
          if-no-files-found: error
          retention-days: 1
      - run: >-
          {INJECTED_OPERATION_ERROR_COMMAND}
      - run: >-
          {COMMIT_LOCK_CLONED_DESCRIPTOR_COMMAND}
      - run: >-
          {CHECKPOINT_LOCK_CLONED_DESCRIPTOR_COMMAND}
  ci-gate:
    runs-on: blacksmith-4vcpu-ubuntu-2404
    needs:
      - {WINDOWS_DURABILITY_JOB}
      - {MACOS_DURABILITY_JOB}
    steps:
      - run: >-
          scripts/ci/require-gates.sh
          "${{{{ needs.{WINDOWS_DURABILITY_JOB}.result }}}}"
          "${{{{ needs.{MACOS_DURABILITY_JOB}.result }}}}"
"""
    validate_native_test_workflow(fixture)

    adversarial = [
        fixture.replace(
            f"    runs-on: {WINDOWS_RUNNER}",
            f"    runs-on: wrong\n    # runs-on: {WINDOWS_RUNNER}",
            1,
        ),
        fixture.replace(
            f"      - run: >-\n          {FILESYSTEM_COMMAND}",
            f'      - run: echo "{FILESYSTEM_COMMAND}"\n        env:\n'
            f'          CLAIMED_COMMAND: "{FILESYSTEM_COMMAND}"',
            1,
        ),
        fixture.replace(
            "    needs: changes",
            "    needs: wrong\n    strategy:\n      needs: changes",
            1,
        ),
        fixture.replace(
            f"          {STORAGE_ADMISSION_COMMAND}",
            f'          echo "{STORAGE_ADMISSION_COMMAND}"',
            1,
        ),
        fixture.replace(
            f"          {NATIVE_ORACLE_CROSSCHECK_COMMAND}",
            f'          echo "{NATIVE_ORACLE_CROSSCHECK_COMMAND}"',
            1,
        ),
        fixture.replace(
            f"          {INJECTED_OPERATION_ERROR_COMMAND}",
            f'          echo "{INJECTED_OPERATION_ERROR_COMMAND}"',
            1,
        ),
        fixture.replace(
            f"          {COMMIT_LOCK_CLONED_DESCRIPTOR_COMMAND}",
            f'          echo "{COMMIT_LOCK_CLONED_DESCRIPTOR_COMMAND}"',
            1,
        ),
        fixture.replace(
            f"          {CHECKPOINT_LOCK_CLONED_DESCRIPTOR_COMMAND}",
            f'          echo "{CHECKPOINT_LOCK_CLONED_DESCRIPTOR_COMMAND}"',
            1,
        ),
        fixture.replace(
            f"          {WINDOWS_OPTIMISTIC_PROMOTION_COMMAND}",
            f'          echo "{WINDOWS_OPTIMISTIC_PROMOTION_COMMAND}"',
            1,
        ),
        fixture.replace(
            f"          {NATIVE_ORACLE_EVIDENCE_ENV}: {WINDOWS_NATIVE_ORACLE_EVIDENCE}",
            f"          # {NATIVE_ORACLE_EVIDENCE_ENV}: {WINDOWS_NATIVE_ORACLE_EVIDENCE}",
            1,
        ),
        fixture.replace(
            f"          path: {NATIVE_ORACLE_ARTIFACT_PATH}",
            "          path: wrong-native-oracle-evidence.json",
            1,
        ),
        fixture.replace(
            f"        uses: {UPLOAD_ARTIFACT_ACTION}",
            "        uses: actions/upload-artifact@unapproved # wrong",
            1,
        ),
        fixture.replace("        if: always()", "        if: false", 1),
        fixture.replace(
            "          if-no-files-found: error", "          if-no-files-found: warn", 1
        ),
        fixture.replace(
            "      - name: Upload macOS native oracle evidence",
            "      # - name: Upload macOS native oracle evidence",
            1,
        ),
        fixture.replace(
            f'          "${{{{ needs.{WINDOWS_DURABILITY_JOB}.result }}}}"\n'
            f'          "${{{{ needs.{MACOS_DURABILITY_JOB}.result }}}}"',
            f'          "${{{{ needs.changes.result }}}}"\n'
            f'      - run: echo "${{{{ needs.{WINDOWS_DURABILITY_JOB}.result }}}}"\n'
            f'      - run: echo "${{{{ needs.{MACOS_DURABILITY_JOB}.result }}}}"',
            1,
        ),
        fixture.replace(
            f"          {FILESYSTEM_COMMAND}",
            f"          {FILESYSTEM_COMMAND} || true",
            1,
        ),
        fixture.replace(
            "          scripts/ci/require-gates.sh",
            "          scripts/ci/require-gates.sh || echo ignored",
            1,
        ),
        fixture.replace(
            f'          "${{{{ needs.{MACOS_DURABILITY_JOB}.result }}}}"',
            f'          "${{{{ needs.{MACOS_DURABILITY_JOB}.result }}}}" ; true',
            1,
        ),
        fixture.replace(
            "          scripts/ci/require-gates.sh",
            "          ! scripts/ci/require-gates.sh",
            1,
        ),
        fixture.replace(
            f"          {FILESYSTEM_COMMAND}",
            f"          {FILESYSTEM_COMMAND} &",
            1,
        ),
        fixture.replace(
            "          scripts/ci/require-gates.sh",
            "          scripts/ci/require-gates.sh ; set +o errexit",
            1,
        ),
        fixture.replace(
            "          scripts/ci/require-gates.sh",
            "          scripts/ci/require-gates.sh ; set +o pipefail",
            1,
        ),
        fixture.replace(
            "          scripts/ci/require-gates.sh",
            "          set +o errexit\n          scripts/ci/require-gates.sh",
            1,
        ),
        fixture.replace(
            f'          "${{{{ needs.{MACOS_DURABILITY_JOB}.result }}}}"',
            f'          "${{{{ needs.{MACOS_DURABILITY_JOB}.result }}}}" && true; echo ignored',
            1,
        ),
        fixture.replace(
            f'          "${{{{ needs.{MACOS_DURABILITY_JOB}.result }}}}"',
            f'          "${{{{ needs.{MACOS_DURABILITY_JOB}.result }}}}"'
            " ok && false; echo ignored && true",
            1,
        ),
    ]
    for hostile in adversarial:
        try:
            validate_native_test_workflow(hostile)
        except AssertionError:
            continue
        raise AssertionError("native workflow policy accepted an adversarial fixture")


def validate_python_evidence_policy(workflow_text: str) -> None:
    """Reject drift from the cross-platform, read-only-wheel evidence contract."""
    prepare_step = "Prepare writable Python RC evidence directory"
    native_step = "Clean-install and execute native contract"
    write_step = "Write target evidence"
    stage_step = "Stage Python report for aggregate job"
    transfer_step = "uses: actions/upload-artifact@"
    assert workflow_text.count(prepare_step) == 1
    python_job = required_section(workflow_text, "  python:\n", "  node:\n")
    assert (
        python_job.count("PYTHON_RC_EVIDENCE_DIR: ${{ runner.temp }}/graphforge-python-rc-evidence")
        == 4
    )
    assert "binding_rc_bazel_native.py python" in python_job
    assert "native_builder: bazel" in python_job
    assert "native_builder: maturin" in python_job
    _, maturin_found, post_maturin = python_job.partition("uses: PyO3/maturin-action@")
    assert maturin_found, "missing maturin build marker"
    assert (
        post_maturin.index(prepare_step)
        < post_maturin.index(native_step)
        < post_maturin.index(write_step)
        < post_maturin.index(stage_step)
        < post_maturin.index(transfer_step)
    )
    prepare = required_section(post_maturin, prepare_step, f"- name: {native_step}")
    assert_active_lines(
        prepare,
        "PYTHON_RC_EVIDENCE_DIR: ${{ runner.temp }}/graphforge-python-rc-evidence",
        'if ! mkdir -p "$PYTHON_RC_EVIDENCE_DIR" || \\',
        '! touch "$PYTHON_RC_EVIDENCE_DIR/.graphforge-write-probe" || \\',
        '! rm "$PYTHON_RC_EVIDENCE_DIR/.graphforge-write-probe"; then',
        "printf 'python_rc_evidence_state=unwritable "
        "target=runner-temp/graphforge-python-rc-evidence\\n' >&2",
        "exit 1",
        "printf 'python_rc_evidence_state=ready "
        "target=runner-temp/graphforge-python-rc-evidence\\n'",
    )
    assert prepare.count("python_rc_evidence_state=unwritable") == 1
    assert prepare.count("python_rc_evidence_state=ready") == 1
    assert "target=runner-temp/graphforge-python-rc-evidence" in prepare
    assert "printf 'python_rc_evidence_state=ready target=runner-temp/" in prepare
    assert "printf 'python_rc_evidence_state=unwritable target=runner-temp/" in prepare
    native = required_section(post_maturin, f"- name: {native_step}", f"- name: {write_step}")
    report = '"$PYTHON_RC_EVIDENCE_DIR/python-classification.json"'
    assert_active_lines(
        native,
        "PYTHON_RC_EVIDENCE_DIR: ${{ runner.temp }}/graphforge-python-rc-evidence",
        "test -f crates/graphforge-bindings-py/tests/smoke.py",
        "test -f crates/graphforge-bindings-py/tests/gil_release.py",
        f"GRAPHFORGE_PYTHON_PARITY_REPORT={report} \\",
        'uv run --isolated --no-project --with "${wheels[0]}" \\',
        "python crates/graphforge-bindings-py/tests/non_cypher_release.py \\",
        "--classification-only",
    )
    assert "crates/graphforge-bindings-py/tests/*.py" not in native
    assert "GRAPHFORGE_PYTHON_PARITY_REPORT=dist/" not in native
    write = required_section(
        post_maturin, f"- name: {write_step}", "- name: Stage Python report for aggregate job"
    )
    assert_active_lines(
        write,
        "PYTHON_RC_EVIDENCE_DIR: ${{ runner.temp }}/graphforge-python-rc-evidence",
        "python3 scripts/ci/write-binding-parity-evidence.py \\",
        f"--classification {report} \\",
        '--output "$PYTHON_RC_EVIDENCE_DIR/${{ matrix.target }}.json"',
    )
    assert "--classification dist/" not in write
    assert '--output "dist/' not in write
    stage = workflow_step(post_maturin, stage_step)
    assert_active_lines(stage, "mkdir -p binding-rc-reports")
    transfer = workflow_step(post_maturin, transfer_step)
    assert_active_lines(
        transfer,
        transfer_step,
        "name: binding-rc-report-${{ github.run_id }}-${{ matrix.target }}",
        "path: binding-rc-reports/${{ matrix.target }}.json",
        "if-no-files-found: error",
        "retention-days: 1",
    )
    for forbidden in ("chmod", "continue-on-error", "|| true", "retry"):
        assert forbidden not in post_maturin.lower()
    assert not re.search(r"(?mi)^\s*if:\s*(?:false|\$\{\{\s*false\s*\}\})\s*$", post_maturin)


def rejected_python_evidence_policy(workflow_text: str) -> None:
    try:
        validate_python_evidence_policy(workflow_text)
    except (AssertionError, ValueError):
        return
    raise AssertionError("workflow policy accepted Python RC evidence path drift")


def validate_windows_node_cold_start_policy(workflow_text: str) -> None:
    """Keep the Windows cold-start allowance narrow and the RC gate fail-closed."""
    node_job = required_section(workflow_text, "  node:\n", "  aggregate:\n")
    windows_entries = [
        entry
        for entry in node_matrix_entries(RC_WORKFLOW, workflow_text)
        if "target: x86_64-pc-windows-msvc" in entry
    ]
    assert len(windows_entries) == 1
    windows_entry = windows_entries[0]
    assert windows_entry.count("timeout_minutes: 90") == 1
    assert all(
        "timeout_minutes:" not in entry
        for entry in node_matrix_entries(RC_WORKFLOW, workflow_text)
        if entry != windows_entry
    )
    assert_active_lines(node_job, "timeout-minutes: ${{ matrix.timeout_minutes || 60 }}")

    assert node_job.count("actions/cache@") == 1
    assert "actions/cache/restore@" not in node_job
    assert "actions/cache/save@" not in node_job
    assert node_job.count("actions/upload-artifact@") == 2
    assert_active_lines(
        node_job,
        "name: binding-rc-report-${{ github.run_id }}-${{ matrix.report_target }}",
        "path: binding-rc-reports/${{ matrix.report_target }}.json",
        "if-no-files-found: error",
        "retention-days: 1",
    )
    assert_active_lines(
        node_job,
        "name: binding-rc-addon-${{ github.run_id }}-${{ matrix.target }}",
        "path: crates/graphforge-bindings-node/*.node",
        "if-no-files-found: error",
        "retention-days: 1",
    )
    assert "path: target" not in workflow_step(node_job, "name: Cache Cargo registry")
    assert "key: ${{ runner.os }}-cargo-registry-v1-${{ hashFiles('Cargo.lock') }}" in node_job
    assert "binding_rc_bazel_native.py node" in node_job
    assert "native_builder: bazel" in node_job
    assert "native_builder: napi" in node_job
    assert node_job.index("uses: dtolnay/rust-toolchain@") < node_job.index(
        "pnpm --filter @curatelabs/graphforge exec napi build --platform --release"
    )

    assert_active_lines(
        node_job,
        "pnpm --filter @curatelabs/graphforge exec napi build --platform --release",
        "python3 scripts/ci/binding_rc_bazel_native.py node \\",
        "pnpm --filter @curatelabs/graphforge test:smoke",
        "tests/non-cypher-release-parity.test.mjs \\",
        "tests/async-errors.test.mjs",
        "cmp built-addon.sha256 tested-addon.sha256",
    )
    for forbidden in ("continue-on-error", "retry", "sleep", "--debug", "|| true"):
        assert forbidden not in node_job.lower()
    false_condition = r"(?mi)^\s*if:\s*(?:false|\$\{\{\s*false\s*\}\})\s*(?:#.*)?$"
    assert not re.search(false_condition, node_job)

    _, aggregate_found, aggregate = workflow_text.partition("  aggregate:\n")
    assert aggregate_found, "missing aggregate job"
    aggregate_head, steps_found, _ = aggregate.partition("    steps:\n")
    assert steps_found, "missing aggregate steps"
    assert_active_lines(
        aggregate_head,
        "if: always() && needs.validate_source.result == 'success'",
        "needs: [validate_source, python, node]",
    )
    assert_active_lines(
        aggregate,
        "uses: actions/download-artifact@",
        "pattern: binding-rc-report-${{ github.run_id }}-*",
        "path: binding-rc-reports",
        "merge-multiple: true",
        "python3 scripts/ci/validate-binding-release-candidate.py",
    )
    assert "continue-on-error" not in aggregate.lower()
    assert "|| true" not in aggregate.lower()
    assert not re.search(false_condition, aggregate)


def rejected_windows_node_cold_start_policy(workflow_text: str, mutation: str = "") -> None:
    try:
        validate_windows_node_cold_start_policy(workflow_text)
    except (AssertionError, StopIteration, ValueError):
        return
    raise AssertionError(f"workflow policy accepted Windows Node cold-start drift: {mutation}")


def validate_post_merge_source_policy(workflow_text: str) -> None:
    """Reject branch-head/stale evidence before any platform matrix starts."""
    assert_active_lines(
        workflow_text,
        "group: binding-rc-${{ inputs.commit_sha }}",
        "cancel-in-progress: true",
    )
    validate_job = required_section(workflow_text, "  validate_source:\n", "  python:\n")
    assert_active_lines(
        validate_job,
        "ref: main",
        "fetch-depth: 1",
        "REQUESTED_SHA: ${{ inputs.commit_sha }}",
        "+refs/heads/main:refs/remotes/origin/main",
        'main_sha="$(git rev-parse refs/remotes/origin/main)"',
        'test "$REQUESTED_SHA" = "$main_sha"',
        "evidence_sha: ${{ steps.source.outputs.evidence_sha }}",
    )
    for job_name, next_job in (("python", "node"), ("node", "aggregate")):
        job = required_section(workflow_text, f"  {job_name}:\n", f"  {next_job}:\n")
        assert_active_lines(
            job,
            "needs: validate_source",
            "EVIDENCE_SHA: ${{ needs.validate_source.outputs.evidence_sha }}",
            "ref: ${{ needs.validate_source.outputs.evidence_sha }}",
        )
        assert "ref: ${{ inputs.commit_sha }}" not in job


def rejected_post_merge_source_policy(workflow_text: str, mutation: str) -> None:
    try:
        validate_post_merge_source_policy(workflow_text)
    except (AssertionError, ValueError):
        return
    raise AssertionError(f"workflow accepted non-main RC source drift: {mutation}")


def main() -> None:
    rc_workflow_text = RC_WORKFLOW.read_text()
    readme_text = README.read_text()
    workflows_readme_text = (ROOT / ".github/workflows/README.md").read_text()
    assert "`Binding Release Candidate` is post-merge, `main`-only evidence" in readme_text
    assert (
        "rejects branch heads and stale commits before any platform matrix build starts"
        in " ".join(readme_text.split())
    )
    assert "proves user-facing use of the installed wheel" in workflows_readme_text
    assert "Windows graphforge-storage Locks" in workflows_readme_text
    assert "second MSVC" in workflows_readme_text
    # Binding RC must not re-host the Rust lock suite; Test Suite does (#2700).
    binding_rc_readme = required_section(
        workflows_readme_text,
        "### `binding-release-candidate.yml`",
        "### `Concurrency Matrix` job in `test.yml`",
    )
    assert "project_generation::tests::" not in binding_rc_readme
    assert "Windows graphforge-storage Locks" in binding_rc_readme
    assert "Test Suite" in binding_rc_readme
    validate_post_merge_source_policy(rc_workflow_text)
    for original, invalid in (
        ("cancel-in-progress: true", "cancel-in-progress: false"),
        ('test "$REQUESTED_SHA" = "$main_sha"', "true"),
        (
            "needs: validate_source",
            "needs: []",
        ),
        (
            "ref: ${{ needs.validate_source.outputs.evidence_sha }}",
            "ref: ${{ inputs.commit_sha }}",
        ),
    ):
        rejected_post_merge_source_policy(rc_workflow_text.replace(original, invalid, 1), original)
    validate_python_evidence_policy(rc_workflow_text)
    validate_windows_node_cold_start_policy(rc_workflow_text)
    windows_entry = next(
        entry
        for entry in node_matrix_entries(RC_WORKFLOW, rc_workflow_text)
        if "target: x86_64-pc-windows-msvc" in entry
    )
    rejected_windows_node_cold_start_policy(
        rc_workflow_text.replace(windows_entry, windows_entry + windows_entry, 1),
        "duplicate Windows matrix entry",
    )
    install_marker = "      - name: Install workspace dependencies"
    for cache_action in ("actions/cache/restore@", "actions/cache/save@"):
        injected = (
            "      - name: Unapproved Windows cache transfer\n"
            f"        uses: {cache_action}\n"
            "        with:\n"
            "          path: target\n"
            "          key: unapproved-windows-build-cache\n"
        )
        rejected_windows_node_cold_start_policy(
            rc_workflow_text.replace(install_marker, injected + install_marker, 1),
            cache_action,
        )
    for original, invalid in (
        ("timeout_minutes: 90", "timeout_minutes: 60"),
        ("--platform --release", "--platform --debug"),
        ("pnpm --filter @curatelabs/graphforge test:smoke", "sleep 1"),
        ("pnpm --filter @curatelabs/graphforge test:smoke", "retry native-smoke"),
        (
            "pnpm --filter @curatelabs/graphforge exec napi build --platform --release",
            "false # disabled napi build",
        ),
        (
            "binding_rc_bazel_native.py node",
            "false # disabled bazel node build",
        ),
        ("tests/non-cypher-release-parity.test.mjs", "tests/skipped-parity.test.mjs"),
        ("cmp built-addon.sha256 tested-addon.sha256", "true"),
        ("if: always()", "if: false"),
        ("if: always()", "if: false # disabled"),
        ("needs: [validate_source, python, node]", "needs: [python, node]"),
        (
            "- name: Validate and aggregate\n        run: >-",
            "- name: Validate and aggregate\n        continue-on-error: true\n        run: >-",
        ),
        (
            "- name: Validate and aggregate\n        run: >-",
            "- name: Validate and aggregate\n        if: false # disabled\n        run: >-",
        ),
        (
            "python3 scripts/ci/validate-binding-release-candidate.py",
            "python3 scripts/ci/validate-binding-release-candidate.py || true",
        ),
    ):
        rejected_windows_node_cold_start_policy(
            rc_workflow_text.replace(original, invalid, 1), original
        )
    for original, invalid in (
        (
            "Prepare writable Python RC evidence directory",
            "Clean-install and execute native contract",
        ),
        ("$PYTHON_RC_EVIDENCE_DIR/python-classification.json", "dist/python-classification.json"),
        (
            "path: binding-rc-reports",
            "path: dist/python-target.json",
        ),
        ("python_rc_evidence_state=ready", "python_rc_evidence_state=ready; retry"),
        (
            'mkdir -p "$PYTHON_RC_EVIDENCE_DIR"',
            'chmod 777 dist; mkdir -p "$PYTHON_RC_EVIDENCE_DIR"',
        ),
    ):
        rejected_python_evidence_policy(rc_workflow_text.replace(original, invalid, 1))
    for disabled in ("if: false", "if: ${{ false }}"):
        invalid = rc_workflow_text.replace(
            "      - name: Prepare writable Python RC evidence directory",
            f"      - name: Prepare writable Python RC evidence directory\n        {disabled}",
            1,
        )
        rejected_python_evidence_policy(invalid)
    prepare_marker = "Prepare writable Python RC evidence directory"
    native_marker = "Clean-install and execute native contract"
    write_marker = "Write target evidence"
    transfer_marker = "uses: actions/upload-artifact@"
    for marker, active_line in (
        (
            prepare_marker,
            "PYTHON_RC_EVIDENCE_DIR: ${{ runner.temp }}/graphforge-python-rc-evidence",
        ),
        (prepare_marker, 'if ! mkdir -p "$PYTHON_RC_EVIDENCE_DIR" || \\'),
        (prepare_marker, '! touch "$PYTHON_RC_EVIDENCE_DIR/.graphforge-write-probe" || \\'),
        (prepare_marker, '! rm "$PYTHON_RC_EVIDENCE_DIR/.graphforge-write-probe"; then'),
        (
            prepare_marker,
            "printf 'python_rc_evidence_state=unwritable "
            "target=runner-temp/graphforge-python-rc-evidence\\n' >&2",
        ),
        (prepare_marker, "exit 1"),
        (
            prepare_marker,
            "printf 'python_rc_evidence_state=ready "
            "target=runner-temp/graphforge-python-rc-evidence\\n'",
        ),
        (native_marker, "PYTHON_RC_EVIDENCE_DIR: ${{ runner.temp }}/graphforge-python-rc-evidence"),
        (
            native_marker,
            "GRAPHFORGE_PYTHON_PARITY_REPORT="
            '"$PYTHON_RC_EVIDENCE_DIR/python-classification.json" \\',
        ),
        (native_marker, 'uv run --isolated --no-project --with "${wheels[0]}" \\'),
        (native_marker, "python crates/graphforge-bindings-py/tests/non_cypher_release.py \\"),
        (native_marker, "--classification-only"),
        (write_marker, "PYTHON_RC_EVIDENCE_DIR: ${{ runner.temp }}/graphforge-python-rc-evidence"),
        (write_marker, "python3 scripts/ci/write-binding-parity-evidence.py \\"),
        (write_marker, '--classification "$PYTHON_RC_EVIDENCE_DIR/python-classification.json" \\'),
        (write_marker, '--output "$PYTHON_RC_EVIDENCE_DIR/${{ matrix.target }}.json"'),
        (
            transfer_marker,
            "path: binding-rc-reports/${{ matrix.target }}.json",
        ),
    ):
        prefix, marker_found, remainder = rc_workflow_text.partition(marker)
        assert marker_found
        assert active_line in remainder
        invalid = prefix + marker_found + remainder.replace(active_line, f"# {active_line}", 1)
        rejected_python_evidence_policy(invalid)
    prefix, marker_found, remainder = rc_workflow_text.partition(prepare_marker)
    assert marker_found and "exit 1" in remainder
    rejected_python_evidence_policy(
        prefix + marker_found + remainder.replace("exit 1", "exit 1 || true", 1)
    )
    for marker in (
        "  python:\n",
        "  node:\n",
        "uses: PyO3/maturin-action@",
        "binding_rc_bazel_native.py python",
        "Prepare writable Python RC evidence directory",
        "Clean-install and execute native contract",
        "Write target evidence",
        "uses: actions/upload-artifact@",
    ):
        rejected_python_evidence_policy(rc_workflow_text.replace(marker, "", 1))
    wrapper_step = "Prepare Rust compiler wrapper for native contracts"
    target_step = "Verify writable Cargo target for native contracts"
    native_step = "Clean-install and execute native contract"
    assert rc_workflow_text.count(wrapper_step) == 1
    assert rc_workflow_text.count(target_step) == 1
    assert rc_workflow_text.count(native_step) == 1
    assert (
        rc_workflow_text.index(wrapper_step)
        < rc_workflow_text.index(target_step)
        < rc_workflow_text.index(native_step)
    )
    python_job_body = rc_workflow_text.split("  python:", 1)[1].split("  node:", 1)[0]
    assert "CARGO_TARGET_DIR: ${{ github.workspace }}/target" in rc_workflow_text
    assert 'test "$CARGO_TARGET_DIR" = "$GITHUB_WORKSPACE/target"' in python_job_body
    assert "cargo_target_state=unwritable" in python_job_body
    assert "cargo_target_state=ready" in python_job_body
    # Bazelisk install uses sudo install -m 0755; forbid chmod on the Cargo sticky reclaim path.
    maturin_lane = python_job_body.split("uses: PyO3/maturin-action@", 1)[1]
    assert "chmod" not in maturin_lane
    assert python_job_body.count('sudo chown -R "$(id -u):$(id -g)" "$CARGO_TARGET_DIR"') == 1
    assert "continue-on-error" not in python_job_body
    assert "|| true" not in python_job_body
    assert "retry" not in python_job_body.lower()
    assert "if: false" not in python_job_body
    assert "if: matrix.native_builder == 'maturin'" in python_job_body
    assert "if: matrix.native_builder == 'bazel'" in python_job_body
    strict_add_node_text = STRICT_ADD_NODE.read_text()
    cargo_invocation = strict_add_node_text.split('"cargo",', 1)[1].split("check=True,", 1)[0]
    assert "env=" not in cargo_invocation
    assert "target: python-ubuntu" in rc_workflow_text
    assert "target: python-macos" in rc_workflow_text
    assert "target: python-windows" in rc_workflow_text
    # python-windows proves installed-wheel use for users, not a second MSVC
    # graphforge-storage release cargo-test on the Binding RC critical path (#2699).
    # The #[cfg(windows)] lock suite lives in Test Suite (#2700).
    python_job = rc_workflow_text.split("  python:", 1)[1].split("  node:", 1)[0]
    assert "Prove Windows project-root lock contract" not in python_job
    assert "cargo test --release -p graphforge-storage" not in python_job
    assert "project_generation::tests::" not in python_job
    assert "Clean-install and execute native contract" in python_job
    assert "uses: PyO3/maturin-action@" in python_job
    assert "binding_rc_bazel_native.py python" in python_job
    assert "native_builder: bazel" in python_job
    assert "native_builder: maturin" in python_job
    test_workflow_text = (ROOT / ".github/workflows/test.yml").read_text()
    validate_native_test_workflow(test_workflow_text)
    validate_native_workflow_negative_fixtures()
    assert "macos-latest" not in rc_workflow_text
    assert "macos-15-intel" not in rc_workflow_text
    assert "windows-latest" not in rc_workflow_text
    assert rc_workflow_text.count("os: blacksmith-12vcpu-macos-15") == 3
    assert rc_workflow_text.count("os: blacksmith-8vcpu-windows-2025") == 2
    assert "architecture: ${{ matrix.node_arch }}" in rc_workflow_text
    assert 'test "$(node -p \'process.arch\')" = "$EXPECTED_NODE_ARCH"' in rc_workflow_text
    assert "scripts/ci/prepare-rustc-wrapper.py" in rc_workflow_text
    assert rc_workflow_text.count("uses: useblacksmith/stickydisk@") == 3
    assert rc_workflow_text.count("uses: actions/cache@") == 3
    shared_linux_key = (
        "${{ github.repository }}-binding-rc-linux-rust-1.96.0-"
        "${{ hashFiles('Cargo.lock') }}-release-target-v1"
    )
    assert rc_workflow_text.count(shared_linux_key) == 2
    assert (
        "${{ github.repository }}-release_candidate-rust-1.96.0-"
        "${{ hashFiles('Cargo.lock') }}-release-target-v1"
    ) in rc_workflow_text
    assert 'sccache: "true"' not in rc_workflow_text
    assert "path: target\n          key: ${{ runner.os }}-cargo-registry" not in rc_workflow_text
    assert "Reclaim sticky-disk ownership after maturin" in rc_workflow_text
    assert (
        "matrix.native_builder == 'maturin' && matrix.sticky_target == 'true'" in rc_workflow_text
    )
    package_validation_step = rc_workflow_text.split("- name: Validate cross-built package", 1)[
        1
    ].split("- name: Write target evidence", 1)[0]
    validator_from_workspace = 'python3 "$GITHUB_WORKSPACE/scripts/ci/validate-napi-artifacts.py"'
    assert "working-directory: crates/graphforge-bindings-node" in package_validation_step
    assert package_validation_step.count(validator_from_workspace) == 1
    assert "../../../scripts/ci/validate-napi-artifacts.py" not in package_validation_step
    assert (ROOT / "scripts/ci/validate-napi-artifacts.py").samefile(ARTIFACT_VALIDATOR)

    assert "Save tested wheel for release-candidate assembly" in rc_workflow_text
    assert "Save tested addon for release-candidate assembly" in rc_workflow_text
    assert "Assemble immutable release candidate" in rc_workflow_text
    release_candidate_job = rc_workflow_text.split("  release_candidate:\n", 1)[1]
    assert 'node-version: "22"' in release_candidate_job
    assert validator_from_workspace in release_candidate_job
    assert "../../../scripts/ci/validate-napi-artifacts.py" not in release_candidate_job
    assert "--skip-optional-publish --no-gh-release" in release_candidate_job
    assert 'npm pack "./$package_dir"' in release_candidate_job
    assert "scripts/ci/prepare-napi-packages.py" in release_candidate_job
    assert (
        "pnpm exec napi build --platform --release --target x86_64-unknown-linux-gnu"
        not in release_candidate_job
    ), "assemble must not recompile natives; emit loaders from retained addon"
    assert "binding_rc_bazel_native.py" in release_candidate_job
    assert "emit-node-loaders" in release_candidate_job
    assert "test -f index.js" in release_candidate_job
    assert "test -f index.d.ts" in release_candidate_job
    assert "package/index.js" in release_candidate_job
    assert 'pnpm --dir "$GITHUB_WORKSPACE/packages/cli" pack' in release_candidate_job
    assert 'pnpm --dir "$GITHUB_WORKSPACE/packages/agent-skills" pack' in release_candidate_job
    assert 'wait "$cli_pack_pid"' in release_candidate_job
    assert 'wait "$agent_skills_pack_pid"' in release_candidate_job
    assert 'cargo package "${package_args[@]}" --allow-dirty --no-verify' in (release_candidate_job)
    assert 'cargo package -p "$crate"' not in release_candidate_job
    assert "scripts/set_release_version.py --check" in release_candidate_job
    assert "${crate}-${RELEASE_VERSION}.crate" in release_candidate_job
    assert '--version "$RELEASE_VERSION"' in release_candidate_job
    assert "--version 0.5.0" not in release_candidate_job
    assert "${crate}-0.5.0.crate" not in release_candidate_job
    assert "scripts/ci/release-candidate.py validate" in rc_workflow_text
    assert "Create the pre-rehearsal candidate manifest" in release_candidate_job
    assert "Rehearse exact partitioned release artifacts offline" in release_candidate_job
    assert "scripts/ci/release_rehearsal.py artifacts" in release_candidate_job
    assert "--manifest candidate/rehearsal-manifest.json" in release_candidate_job
    assert "--out candidate/release-artifacts/evidence/offline-rehearsal.json" in (
        release_candidate_job
    )
    assert "rm candidate/rehearsal-manifest.json" in release_candidate_job
    assert release_candidate_job.index("Rehearse exact partitioned release artifacts offline") < (
        release_candidate_job.index("Create and validate the immutable candidate manifest")
    )
    for group in ("manifest", "python", "npm", "crates", "evidence"):
        assert (
            f"Release-Candidate-{group}-${{{{ needs.validate_source.outputs.evidence_sha }}}}"
        ) in rc_workflow_text

    publish_workflow_text = PUBLISH_WORKFLOW.read_text()
    assert "Release-Candidate-manifest-$RELEASE_SHA" in publish_workflow_text
    assert "PyO3/maturin-action" not in publish_workflow_text
    assert "napi build" not in publish_workflow_text
    assert "candidate/release-artifacts/python/*" in publish_workflow_text
    assert "uv publish candidate/release-artifacts/python/*" in publish_workflow_text

    with tempfile.TemporaryDirectory() as directory:
        temp = Path(directory)
        github_env = temp / "github-env"
        stale_env = os.environ.copy()
        stale_env.update(
            {
                "GITHUB_ENV": str(github_env),
                "RUSTC_WRAPPER": "unavailable-graphforge-sccache",
            }
        )
        stale = subprocess.run(
            [
                sys.executable,
                str(WRAPPER_PREPARER),
                "--platform",
                "python-ubuntu",
                "--contract",
                "python-native-contracts",
            ],
            check=False,
            capture_output=True,
            text=True,
            env=stale_env,
        )
        assert stale.returncode == 0, stale.stderr[-1000:]
        assert github_env.read_text() == "RUSTC_WRAPPER=\n"
        assert "state=cleared" in stale.stdout
        assert "PATH" not in stale.stdout

        cargo = shutil.which("cargo")
        assert cargo is not None, "cargo is required for wrapper contract validation"
        crate = temp / "wrapper-contract"
        (crate / "src").mkdir(parents=True)
        (crate / "Cargo.toml").write_text(
            '[package]\nname = "wrapper-contract"\nversion = "0.0.0"\nedition = "2021"\n',
            encoding="utf-8",
        )
        (crate / "src/lib.rs").write_text("pub fn contract() {}\n", encoding="utf-8")
        stale_cargo = subprocess.run(
            [cargo, "check", "--offline", "--quiet"],
            cwd=crate,
            check=False,
            capture_output=True,
            text=True,
            env=next_step_environment(stale_env, github_env),
        )
        assert stale_cargo.returncode == 0, (stale_cargo.stdout + stale_cargo.stderr)[-1000:]

        hostile_wrapper = "secret\nvalue-" + ("x" * 200)
        hostile_env = os.environ.copy()
        hostile_env.update({"GITHUB_ENV": str(github_env), "RUSTC_WRAPPER": hostile_wrapper})
        hostile = subprocess.run(
            [
                sys.executable,
                str(WRAPPER_PREPARER),
                "--platform",
                "python-ubuntu\nsecret-platform",
                "--contract",
                "native\nsecret-contract",
            ],
            check=False,
            capture_output=True,
            text=True,
            env=hostile_env,
        )
        assert hostile.returncode == 0, hostile.stderr[-1000:]
        assert len(hostile.stdout.strip()) < 220
        assert hostile.stdout.count("\n") == 1
        assert "secret\n" not in hostile.stdout
        assert "x" * 65 not in hostile.stdout

        wrapper = temp / "sccache"
        wrapper_log = temp / "wrapper-invocations"
        wrapper.write_text(
            f'#!/bin/sh\nprintf "%s\\n" invoked >> "{wrapper_log}"\nexec "$@"\n',
            encoding="utf-8",
        )
        wrapper.chmod(0o755)
        github_env.write_text("", encoding="utf-8")
        available_env = os.environ.copy()
        available_env.update({"GITHUB_ENV": str(github_env), "RUSTC_WRAPPER": str(wrapper)})
        available = subprocess.run(
            [
                sys.executable,
                str(WRAPPER_PREPARER),
                "--platform",
                "python-macos",
                "--contract",
                "python-native-contracts",
            ],
            check=False,
            capture_output=True,
            text=True,
            env=available_env,
        )
        assert available.returncode == 0, available.stderr[-1000:]
        assert github_env.read_text() == ""
        assert "command=sccache state=available" in available.stdout
        assert str(temp) not in available.stdout
        available_cargo = subprocess.run(
            [
                cargo,
                "check",
                "--offline",
                "--quiet",
                "--target-dir",
                str(temp / "available-target"),
            ],
            cwd=crate,
            check=False,
            capture_output=True,
            text=True,
            env=next_step_environment(available_env, github_env),
        )
        assert available_cargo.returncode == 0, (available_cargo.stdout + available_cargo.stderr)[
            -1000:
        ]
        assert wrapper_log.read_text().splitlines()

    for workflow in (RC_WORKFLOW,):
        workflow_text = workflow.read_text()
        assert "run build --" not in workflow_text, (
            f"{workflow.name} forwards napi options to Cargo after `--`"
        )
        assert "exec napi build --platform --release" in workflow_text, (
            f"{workflow.name} must invoke napi directly so cross options stay with napi"
        )
        entries = node_matrix_entries(workflow, workflow_text)
        arm_entry = next(entry for entry in entries if "target: aarch64-unknown-linux-gnu" in entry)
        assert arm_entry.count("arm_cflags:") == 1
        assert arm_entry.count("-D__ARM_ARCH=8") == 1
        assert all(
            "arm_cflags:" not in entry and "-D__ARM_ARCH=8" not in entry
            for entry in entries
            if entry != arm_entry
        ), f"{workflow.name} leaks ARMv8 flags to a non-ARM matrix entry"
        assert workflow_text.count("CFLAGS_aarch64_unknown_linux_gnu:") == 1, (
            f"{workflow.name} must pass the ARM flag through target-scoped cc-rs CFLAGS"
        )
        assert "arm_cflags || ''" in workflow_text, (
            f"{workflow.name} must source target-scoped CFLAGS from its matrix entry"
        )
        assert workflow_text.count(ARTIFACT_COMMAND) == 2, (
            f"{workflow.name} must use the shared explicit napi artifact command"
        )
        assert "napi artifacts --dir" not in workflow_text, (
            f"{workflow.name} uses the unsupported napi artifacts --dir option"
        )

    publish_text = PUBLISH_WORKFLOW.read_text()
    assert "exec napi build --platform --release" not in publish_text
    assert ARTIFACT_COMMAND not in publish_text
    assert "Release-Candidate-manifest-$RELEASE_SHA" in publish_text

    pnpm = shutil.which("pnpm")
    assert pnpm is not None, "pnpm is required for napi CLI contract validation"
    help_result = subprocess.run(
        [pnpm, "--filter", "@curatelabs/graphforge", "exec", "napi", "artifacts", "--help"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    help_text = help_result.stdout + help_result.stderr
    help_diagnostic = help_text.strip()[-4000:]
    assert help_result.returncode == 0, f"napi CLI help failed: {help_diagnostic}"
    assert "--output-dir" in help_text, (
        f"pinned napi CLI no longer accepts --output-dir: {help_diagnostic}"
    )
    assert "--npm-dir" in help_text, (
        f"pinned napi CLI no longer accepts --npm-dir: {help_diagnostic}"
    )

    artifact_validator = load_artifact_validator()
    with tempfile.TemporaryDirectory() as directory:
        npm_dir = Path(directory)
        manifest = npm_dir / "package.json"
        manifest.write_text(
            json.dumps({"napi": {"targets": ["aarch64-unknown-linux-gnu"]}}),
            encoding="utf-8",
        )
        undeclared = subprocess.run(
            [
                sys.executable,
                str(ARTIFACT_VALIDATOR),
                "--npm-dir",
                str(npm_dir),
                "--manifest",
                str(manifest),
                "--target",
                "x86_64-unknown-linux-gnu",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        undeclared_output = (undeclared.stdout + undeclared.stderr).strip()
        undeclared_diagnostic = undeclared_output[-4000:]
        assert undeclared.returncode != 0, undeclared_diagnostic
        assert "requested targets are not declared" in undeclared_output, undeclared_diagnostic

        expected_dir = npm_dir / "linux-arm64-gnu"
        expected_dir.mkdir()
        addon = expected_dir / "graphforge.linux-arm64-gnu.node"
        addon.touch()
        artifact_validator.validate(npm_dir, ["aarch64-unknown-linux-gnu"])

        duplicate = expected_dir / "duplicate.node"
        duplicate.touch()
        try:
            artifact_validator.validate(npm_dir, ["aarch64-unknown-linux-gnu"])
        except ValueError as error:
            assert "exactly one addon" in str(error)
        else:
            raise AssertionError("artifact validator accepted duplicate addons")
        duplicate.unlink()

        addon.unlink()
        try:
            artifact_validator.validate(npm_dir, ["aarch64-unknown-linux-gnu"])
        except ValueError as error:
            assert "exactly one addon" in str(error)
        else:
            raise AssertionError("artifact validator accepted a missing addon")

        addon.touch()
        wrong_dir = npm_dir / "linux-x64-gnu"
        wrong_dir.mkdir()
        addon.replace(wrong_dir / addon.name)
        try:
            artifact_validator.validate(npm_dir, ["aarch64-unknown-linux-gnu"])
        except ValueError as error:
            assert "wrong target package" in str(error)
        else:
            raise AssertionError("artifact validator accepted an addon in the wrong package")

    module = load_validator()
    contract = json.loads(CONTRACT.read_text())
    reports = [report(target, settings) for target, settings in contract["targets"].items()]
    aggregate = module.validate(reports, contract, SHA)
    assert aggregate["status"] == "passed"
    assert len(aggregate["targets"]) == len(contract["targets"])

    rejected(module, reports[:-1], contract, "target report mismatch")

    invalid = copy.deepcopy(reports)
    invalid[0]["source_sha"] = "d" * 40
    rejected(module, invalid, contract, "source SHA drift")

    invalid = copy.deepcopy(reports)
    same_language = next(
        index
        for index, value in enumerate(invalid[1:], 1)
        if value["language"] == invalid[0]["language"]
    )
    invalid[same_language]["package_version"] = "0.5.1.dev0"
    rejected(module, invalid, contract, "mixed package versions")

    invalid = copy.deepcopy(reports)
    invalid.append(copy.deepcopy(reports[0]))
    rejected(module, invalid, contract, "duplicate target reports")

    invalid = copy.deepcopy(reports)
    invalid[0]["sanitized_parity_diff"] = ["result mismatch"]
    rejected(module, invalid, contract, "parity differences are non-empty")

    invalid = copy.deepcopy(reports)
    invalid[0]["classification"]["sha256"] = "not-a-digest"
    rejected(module, invalid, contract, "missing classification SHA-256")

    invalid = copy.deepcopy(reports)
    invalid[0]["classification"]["schema"] = "unsupported/99"
    rejected(module, invalid, contract, "unsupported classification schema")


if __name__ == "__main__":
    main()
