#!/usr/bin/env python3
"""Enforce Blacksmith-first, consumer-driven CI transfer storage.

Speed and honesty share one storage model on Blacksmith runners:

Allowed
-------
- ``useblacksmith/stickydisk`` for ``target/``, optional ``.sccache``, and other
  large build trees (persist compile products across RC/publish-track runs;
  ~3s hydrate vs multi-minute cache blobs).
- Upstream ``actions/cache@v6`` for ``~/.cargo/registry`` + git (and pnpm/uv)
  with exact lockfile keys — Blacksmith colocates this cache.
- Local ``sccache`` with ``SCCACHE_DIR`` on a sticky disk (cross-crate compile
  cache without GitHub-backed maturin sccache).
- Larger Blacksmith runners for Binding RC cells when wall-clock requires them.

Still forbidden
---------------
- Putting ``target/`` (or other large build trees) into ``actions/cache`` blobs —
  wrong tool; use sticky disks.
- Maturin-action ``sccache: true`` (GHA-integrated backend) — prefer sticky
  ``SCCACHE_DIR`` / sticky ``target/`` we control.
- Unbounded artifact uploads — keep consumer-driven retention for candidate
  partitions (1-day transfer vs 30-day publication groups).

Expected Binding RC Linux sticky keys use repository + lane + rustc +
Cargo.lock hash + ``release-target-v1``. After #4 cutover, Test Suite no longer
mounts PR job-isolated Cargo ``target/`` sticky disks; Binding RC / fuzz / release-certification
release-load retain sticky for packaging and retained-tool lanes.

This module inventories workflow storage steps and fails closed on drift.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
import re

from workflow_policy import (
    job_needs,
    job_required_run_scalars,
    job_run_contains,
    job_run_scalars,
    job_runs_exact,
    job_scalar,
    normalize_run,
    workflow_jobs,
)

ROOT = Path(__file__).resolve().parents[2]

_SHA_RE = re.compile(r"^[0-9a-f]{40}$", re.IGNORECASE)


def uses_approved(uses: str | None, action: str, *tags: str) -> bool:
    """Accept ``action@tag`` or Dependabot-style ``action@<sha> # tag`` pins."""
    if uses is None:
        return False
    for tag in tags:
        if uses == f"{action}@{tag}":
            return True
    prefix = f"{action}@"
    if not uses.startswith(prefix) or "#" not in uses:
        return False
    ref, _, comment = uses.partition("#")
    sha = ref[len(prefix) :].strip()
    note = comment.strip().split()[0] if comment.strip() else ""
    if not _SHA_RE.match(sha):
        return False
    return any(note == tag or note.startswith(f"{tag}.") for tag in tags)


WORKFLOWS = ROOT / ".github" / "workflows"
EXPECTED_ARTIFACT_UPLOADS = Counter(
    {
        "binding-rc-report-${{ github.run_id }}-${{ matrix.target }}": 1,
        "binding-rc-report-${{ github.run_id }}-${{ matrix.report_target }}": 1,
        "binding-rc-wheel-${{ github.run_id }}-${{ matrix.target }}": 1,
        "binding-rc-addon-${{ github.run_id }}-${{ matrix.target }}": 1,
        "Rust-Non-Cypher-${{ env.EVIDENCE_SHA }}": 1,
        "Binding-Release-Candidate-${{ needs.validate_source.outputs.evidence_sha }}": 1,
        "Release-Load-${{ github.run_id }}": 1,
        "Release-Candidate-manifest-${{ needs.validate_source.outputs.evidence_sha }}": 1,
        "Release-Candidate-python-${{ needs.validate_source.outputs.evidence_sha }}": 1,
        "Release-Candidate-npm-${{ needs.validate_source.outputs.evidence_sha }}": 1,
        "Release-Candidate-crates-${{ needs.validate_source.outputs.evidence_sha }}": 1,
        "Release-Candidate-evidence-${{ needs.validate_source.outputs.evidence_sha }}": 1,
        "Release-Reconciliation-${{ github.run_id }}": 1,
        "visualization-limits-stress-${{ github.sha }}": 1,
        "pr-python-wheel-${{ github.sha }}": 1,
        "pr-node-addon-${{ github.sha }}": 1,
        "cargo-bazel-parity-evidence-${{ github.run_id }}": 1,
        "bazel-cache-perf-evidence-${{ github.run_id }}": 1,
        "durability-certification-evidence-${{ github.sha }}": 1,
        "native-oracle-windows-${{ github.sha }}": 1,
        "native-oracle-macos-${{ github.sha }}": 1,
        "native-durability-aggregate-${{ github.sha }}": 1,
        "m6-memory-${{ github.sha }}-blacksmith-4vcpu-ubuntu-2404": 1,
        "native-local-admission-${{ matrix.authority }}-${{ github.sha }}": 1,
    }
)
EXPECTED_ARTIFACT_DOWNLOADS = Counter(
    {
        "binding-rc-report-${{ github.run_id }}-*": 1,
        "binding-rc-wheel-${{ github.run_id }}-*": 1,
        "binding-rc-addon-${{ github.run_id }}-*": 1,
        "Rust-Non-Cypher-${{ needs.validate_source.outputs.evidence_sha }}": 1,
        "Binding-Release-Candidate-${{ needs.validate_source.outputs.evidence_sha }}": 1,
        "Release-Load-${{ github.run_id }}": 1,
        "Release-Candidate-manifest-${{ steps.source.outputs.release_sha }}": 1,
        "Release-Candidate-python-${{ steps.source.outputs.release_sha }}": 1,
        "Release-Candidate-npm-${{ steps.source.outputs.release_sha }}": 1,
        "Release-Candidate-crates-${{ steps.source.outputs.release_sha }}": 1,
        "Release-Candidate-evidence-${{ steps.source.outputs.release_sha }}": 1,
        "Release-Candidate-manifest-${{ needs.resolve_source.outputs.release_sha }}": 1,
        "Release-Candidate-python-${{ needs.resolve_source.outputs.release_sha }}": 1,
        "Release-Candidate-npm-${{ needs.resolve_source.outputs.release_sha }}": 1,
        "Release-Candidate-crates-${{ needs.resolve_source.outputs.release_sha }}": 1,
        "Release-Candidate-evidence-${{ needs.resolve_source.outputs.release_sha }}": 1,
        "pr-python-wheel-${{ github.sha }}": 1,
        "pr-node-addon-${{ github.sha }}": 1,
        "native-oracle-windows-${{ github.sha }}": 1,
        "native-oracle-macos-${{ github.sha }}": 1,
    }
)
EXPECTED_DEPENDENCY_KEYS = Counter(
    {
        # test.yml: policy + rust-lint + python/node binding + Windows/macOS
        # durability (6);
        # Binding RC: 3. PR Cargo sticky disks retired after #4 cutover.
        "${{ runner.os }}-cargo-registry-v1-${{ hashFiles('Cargo.lock') }}": 9,
        "${{ runner.os }}-snap-ego-facebook-v1": 1,
        "${{ runner.os }}-fuzz-${{ hashFiles('fuzz/Cargo.toml', '**/Cargo.lock') }}": 1,
    }
)
EXPECTED_STICKY_KEYS = Counter(
    {
        # PR job-isolated Cargo target/ sticky disks retired after #4.
        # Binding RC / fuzz / release-certification / release-load retain sticky packaging.
        (
            "${{ github.repository }}-binding-rc-linux-rust-1.96.0-"
            "${{ hashFiles('Cargo.lock') }}-release-target-v1"
        ): 2,
        (
            "${{ github.repository }}-release_candidate-rust-1.96.0-"
            "${{ hashFiles('Cargo.lock') }}-release-target-v1"
        ): 1,
        (
            "${{ github.repository }}-daily-fuzz-"
            "${{ hashFiles('fuzz/Cargo.toml', '**/Cargo.lock') }}-target-v1"
        ): 1,
        "${{ github.repository }}-release-load-${{ inputs.commit_sha }}-target-v3": 1,
    }
)
EXPECTED_STICKY_DELETES = Counter(
    {"${{ github.repository }}-release-load-${{ inputs.commit_sha }}-target-v3": 1}
)
EXPECTED_SAVES = Counter(
    {
        "checkpoint-transfer-${{ github.run_id }}-rust": 1,
        "checkpoint-transfer-${{ github.run_id }}-python": 1,
        "checkpoint-transfer-${{ github.run_id }}-node": 1,
        "knowledge-transfer-${{ github.run_id }}-rust": 1,
        "knowledge-transfer-${{ github.run_id }}-python": 1,
        "knowledge-transfer-${{ github.run_id }}-node": 1,
        "epistemic-transfer-${{ github.run_id }}-rust": 1,
        "epistemic-transfer-${{ github.run_id }}-python": 1,
        "epistemic-transfer-${{ github.run_id }}-node": 1,
    }
)
EXPECTED_RESTORES = Counter(
    {
        "checkpoint-transfer-${{ github.run_id }}-rust": 1,
        "checkpoint-transfer-${{ github.run_id }}-python": 1,
        "checkpoint-transfer-${{ github.run_id }}-node": 1,
        "knowledge-transfer-${{ github.run_id }}-rust": 1,
        "knowledge-transfer-${{ github.run_id }}-python": 1,
        "knowledge-transfer-${{ github.run_id }}-node": 1,
        "epistemic-transfer-${{ github.run_id }}-rust": 1,
        "epistemic-transfer-${{ github.run_id }}-python": 1,
        "epistemic-transfer-${{ github.run_id }}-node": 1,
    }
)


def action_steps(text: str, action_prefix: str) -> list[list[str]]:
    """Return matching action steps without accepting fields from later steps."""
    lines = text.splitlines()
    steps: list[list[str]] = []
    for index, line in enumerate(lines):
        normalized = line.strip().removeprefix("- ").removeprefix("uses:").strip().strip("'\"")
        if not normalized.startswith(action_prefix):
            continue
        uses_indent = len(line) - len(line.lstrip())
        start = index
        if not line.lstrip().startswith("- "):
            while start >= 0:
                candidate = lines[start]
                if candidate.lstrip().startswith("- ") and (
                    len(candidate) - len(candidate.lstrip()) < uses_indent
                ):
                    break
                start -= 1
            assert start >= 0, f"cache action at line {index + 1} is not in a step"
        step_indent = len(lines[start]) - len(lines[start].lstrip())
        end = start + 1
        while end < len(lines):
            candidate = lines[end].lstrip()
            candidate_indent = len(lines[end]) - len(candidate)
            if candidate.startswith("- ") and candidate_indent <= step_indent:
                break
            if candidate and candidate_indent < step_indent:
                break
            end += 1
        steps.append(lines[start:end])
    return steps


def cache_steps(text: str) -> list[list[str]]:
    return action_steps(text, "actions/cache/")


def field(step: list[str], name: str) -> str | None:
    matched: str | None = None
    for index, line in enumerate(step):
        stripped = line.strip().removeprefix("- ")
        if not stripped.startswith(name + ":"):
            continue
        value = stripped.split(":", 1)[1].strip().strip("'\"")
        if value in {"|", "|-", ">", ">-"}:
            indent = len(line) - len(line.lstrip())
            collected: list[str] = []
            for follow in step[index + 1 :]:
                if not follow.strip():
                    continue
                follow_indent = len(follow) - len(follow.lstrip())
                if follow_indent <= indent:
                    break
                collected.append(follow.strip())
            matched = "\n".join(collected)
        else:
            matched = value
    return matched


def artifact_contracts(text: str) -> tuple[list[str], list[str]]:
    uploaded: list[str] = []
    downloaded: list[str] = []
    for step in action_steps(text, "actions/upload-artifact@"):
        uses = field(step, "uses")
        assert uses_approved(uses, "actions/upload-artifact", "v7"), (
            f"unapproved artifact action: {uses}"
        )
        name = field(step, "name")
        assert name is not None, "artifact upload has no exact name"
        assert field(step, "if-no-files-found") == "error", (
            f"artifact upload is not fail-closed: {name}"
        )
        publication = name.startswith(("Release-", "Binding-", "Rust-"))
        certification = name.startswith(
            (
                "durability-certification-evidence-",
                "native-oracle-windows-",
                "native-oracle-macos-",
                "native-durability-aggregate-",
                "g500-certification-",
            )
        )
        expected_retention = "30" if publication else "14" if certification else "1"
        assert field(step, "retention-days") == expected_retention, (
            f"artifact retention drift: {name}"
        )
        path = field(step, "path")
        assert path in {
            "binding-rc-reports/${{ matrix.target }}.json",
            "binding-rc-reports/${{ matrix.report_target }}.json",
            "dist/*.whl",
            "crates/graphforge-bindings-node/*.node",
            (
                "crates/graphforge-bindings-node/*.node\n"
                "crates/graphforge-bindings-node/index.js\n"
                "crates/graphforge-bindings-node/index.d.ts"
            ),
            "non-cypher-evidence/",
            "binding-rc-aggregate/report.json",
            "release-load-evidence",
            "candidate/v${{ env.RELEASE_VERSION }}-artifacts.json",
            "candidate/release-artifacts/python/",
            "candidate/release-artifacts/npm/",
            "candidate/release-artifacts/crates/",
            ("candidate/release-artifacts/evidence/\ncandidate/release-artifacts/node-addons/"),
            "reconciliation/summary.json",
            "examples/visualization/stress/results/",
            "dist/cargo-bazel-parity-evidence.json",
            (
                "dist/bazel-warm-observation.json\n"
                "dist/bazel-affected-inputs.json\n"
                "dist/bazel-cache-perf-ci-observation.json\n"
                "dist/bazel-representative-build.summary.json\n"
                "dist/perf-sample-collected.json\n"
                "docs/development/bazel-migration-evidence/perf-sample.json"
            ),
            "${{ runner.temp }}/durability-certification-evidence",
            "native/native-durability-aggregate.json",
            "replay-memory.txt\ncompaction-memory.txt",
            (
                "${{ runner.temp }}/g500-certification-evidence.json\n"
                "${{ runner.temp }}/g500-certification-phase-journal.json"
            ),
            "benchmarks/outputs/local-admission-evidence.json",
            ("${{ runner.temp }}/fly-q958-ledger.json\n${{ runner.temp }}/fly-q958-plan.json"),
            ("${{ runner.temp }}/fly-q958-result.json\n${{ runner.temp }}/fly-q958-evidence.json"),
            "${{ runner.temp }}/fly-q958-cleanup-result.json",
            "${{ runner.temp }}/fly-q958-manual-cleanup-result.json",
        }, f"artifact upload contains unapproved bytes: {path}"
        uploaded.append(name)
    for step in action_steps(text, "actions/download-artifact@"):
        uses = field(step, "uses")
        assert uses_approved(uses, "actions/download-artifact", "v8"), (
            f"unapproved artifact action: {uses}"
        )
        pattern = field(step, "pattern")
        name = field(step, "name")
        selector = pattern if pattern is not None else name
        assert selector is not None
        path = field(step, "path")
        assert path in {
            "binding-rc-reports",
            "candidate/release-artifacts/python",
            "candidate/release-artifacts/node-addons",
            "non-cypher-evidence",
            "binding-rc-aggregate",
            "release-load-evidence",
            "candidate",
            "candidate/release-artifacts/npm",
            "candidate/release-artifacts/crates",
            "candidate/release-artifacts",
            "dist",
            "crates/graphforge-bindings-node",
            "native/windows",
            "native/macos",
            "${{ runner.temp }}",
        }, f"artifact download path drift: {selector}"
        if pattern is not None:
            assert field(step, "merge-multiple") == "true", (
                f"artifact reports are not merged: {pattern}"
            )
            assert field(step, "run-id") is None, (
                f"same-run artifact pattern unexpectedly crosses runs: {pattern}"
            )
        else:
            assert field(step, "merge-multiple") is None, (
                f"single artifact unexpectedly merged: {name}"
            )
            same_run = (
                name == "Release-Load-${{ github.run_id }}"
                or name.startswith(("pr-", "native-oracle-"))
                or (name == "${{ env.PREFLIGHT_ARTIFACT }}" and field(step, "run-id") is None)
            )
            cross_run = not same_run
            if cross_run:
                assert field(step, "github-token") == "${{ github.token }}", (
                    f"cross-run artifact has no token: {name}"
                )
                assert field(step, "repository") == "${{ github.repository }}", (
                    f"cross-run artifact repository drift: {name}"
                )
                expected_run_ids = {
                    "non-cypher-evidence": {"${{ inputs.rust_run_id }}"},
                    "binding-rc-aggregate": {"${{ inputs.binding_rc_run_id }}"},
                    "candidate": {
                        "${{ steps.candidate.outputs.run_id }}",
                        "${{ needs.locate_candidate.outputs.candidate_run_id }}",
                    },
                    "candidate/release-artifacts/python": {
                        "${{ steps.candidate.outputs.run_id }}",
                        "${{ needs.locate_candidate.outputs.candidate_run_id }}",
                    },
                    "candidate/release-artifacts/npm": {
                        "${{ steps.candidate.outputs.run_id }}",
                        "${{ needs.locate_candidate.outputs.candidate_run_id }}",
                    },
                    "candidate/release-artifacts/crates": {
                        "${{ steps.candidate.outputs.run_id }}",
                        "${{ needs.locate_candidate.outputs.candidate_run_id }}",
                    },
                    "candidate/release-artifacts": {
                        "${{ steps.candidate.outputs.run_id }}",
                        "${{ needs.locate_candidate.outputs.candidate_run_id }}",
                    },
                    "${{ runner.temp }}": {"${{ inputs.source_run_id }}"},
                }[path]
                assert field(step, "run-id") in expected_run_ids, (
                    f"cross-run artifact run ID drift: {name}"
                )
            else:
                assert field(step, "run-id") is None, (
                    f"same-run load artifact unexpectedly crosses runs: {name}"
                )
        downloaded.append(selector)
    return uploaded, downloaded


def validate_operator_handoffs_have_no_artifacts() -> None:
    for workflow, gate in (
        ("g500-certification.yml", "g500-certification"),
        ("fly-tiny-qualification.yml", "fly-tiny-qualification"),
        ("fly-tiny-recovery.yml", "fly-tiny-recovery"),
    ):
        text = (WORKFLOWS / workflow).read_text()
        uploaded, downloaded = artifact_contracts(text)
        assert not uploaded and not downloaded, f"operator handoff transfers artifacts: {workflow}"
        command = f"python3 scripts/ci/gate-registry.py command {gate}"
        matches = [
            scalar
            for body in workflow_jobs(text).values()
            for scalar in job_required_run_scalars(body, command)
        ]
        assert matches, f"operator handoff does not execute its registry command: {workflow}"
        inactive = text.replace(command, f'echo "{command}"', 1)
        inactive_matches = [
            scalar
            for body in workflow_jobs(inactive).values()
            for scalar in job_required_run_scalars(body, command)
        ]
        assert not inactive_matches, f"inactive operator handoff passed policy: {workflow}"


def cache_contracts(text: str) -> tuple[list[str], list[str]]:
    saved: list[str] = []
    restored: list[str] = []
    for step in cache_steps(text):
        uses = field(step, "uses")
        if uses is None or not uses.startswith("actions/cache/"):
            continue
        assert uses_approved(uses, "actions/cache/save", "v6") or uses_approved(
            uses, "actions/cache/restore", "v6"
        ), f"unapproved cache transfer action: {uses}"
        key = field(step, "key")
        assert key is not None, f"{uses} step has no exact key"
        if uses_approved(uses, "actions/cache/save", "v6"):
            saved.append(key)
        else:
            assert field(step, "fail-on-cache-miss") == "true", f"restore is not fail-closed: {key}"
            restored.append(key)
    return saved, restored


def dependency_contracts(text: str) -> list[str]:
    keys: list[str] = []
    for step in action_steps(text, "actions/cache@"):
        assert uses_approved(field(step, "uses"), "actions/cache", "v6"), (
            "dependency cache must use actions/cache@v6 (tag or SHA pin)"
        )
        key = field(step, "key")
        assert key is not None, "dependency cache has no exact key"
        rendered = "\n".join(step)
        assert "target" not in rendered, (
            f"large build tree stored in actions/cache (use stickydisk): {key}"
        )
        assert "crates/**/*.rs" not in key and "crates/**" not in key, (
            f"dependency cache is keyed by source files: {key}"
        )
        keys.append(key)
    return keys


def sticky_contracts(text: str) -> tuple[list[str], list[str]]:
    mounted: list[str] = []
    deleted: list[str] = []
    for step in action_steps(text, "useblacksmith/stickydisk"):
        uses = field(step, "uses")
        if uses_approved(uses, "useblacksmith/stickydisk", "v1"):
            key = field(step, "key")
            assert key is not None, "sticky disk has no exact key"
            mounted.append(key)
        elif uses_approved(uses, "useblacksmith/stickydisk-delete", "v1"):
            key = field(step, "delete-key")
            assert key is not None, "sticky disk deletion has no exact key"
            deleted.append(key)
        else:
            raise AssertionError(f"unapproved sticky-disk action: {uses}")
    return mounted, deleted


def validate_maturin_storage(text: str) -> None:
    for step in action_steps(text, "PyO3/maturin-action@"):
        assert uses_approved(field(step, "uses"), "PyO3/maturin-action", "v1"), (
            "unapproved Maturin action"
        )
        sccache = field(step, "sccache")
        assert sccache is None or sccache.lower() == "false", (
            "Maturin-action sccache:true uses the GitHub-integrated backend; "
            "use sticky SCCACHE_DIR / sticky target/ instead "
            f"(got sccache={sccache!r})"
        )


def validate_test_suite_trigger(text: str) -> None:
    trigger, found, _ = text.partition("\npermissions:\n")
    assert found, "Test Suite workflow is missing its permissions boundary"
    assert '  pull_request:\n    branches: ["main"]' in trigger, (
        "Test Suite must gate pull requests to main"
    )
    assert "  push:" not in trigger, (
        "Test Suite must not duplicate exact-head PR validation after merge"
    )


def validate_required_run_negative_fixtures() -> None:
    """Required command matching rejects common shell failure suppression."""
    command = "python3 scripts/ci/cargo-bazel-parity-check.py --mode inventory"
    fixture = f"""jobs:
  probe:
    steps:
      - run: >-
          {command}
"""
    assert job_run_contains(workflow_jobs(fixture)["probe"], command)
    for suffix in (" &> command.log", " 2>&1"):
        allowed = fixture.replace(command, f"{command}{suffix}")
        assert job_run_contains(workflow_jobs(allowed)["probe"], command)
    for suffix, prefix in (
        (" || true", ""),
        (" || echo ignored", ""),
        (" ; true", ""),
        ("", "! "),
        (" &", ""),
        (" ; set +o errexit", ""),
        (" ; set +o pipefail", ""),
        ("", "set +o errexit\n          "),
        (" && true; echo ignored", ""),
        (" ok && false; echo ignored && true", ""),
        ("; exit 0", ""),
    ):
        hostile = fixture.replace(command, f"{prefix}{command}{suffix}")
        try:
            accepted = job_run_contains(workflow_jobs(hostile)["probe"], command)
        except AssertionError:
            continue
        if not accepted:
            continue
        raise AssertionError("required run policy accepted failure suppression")
    for wrapper in (
        f"if false; then\n          {command}\n          fi",
        f"if ! true; then\n          {command}\n          fi",
        f"while false; do\n          {command}\n          done",
        f"until true; do\n          {command}\n          done",
        f"for item in one; do\n          {command}\n          done",
        f"run_gate() {{\n          {command}\n          }}",
        f"cat <<'EOF'\n          {command}\n          EOF",
    ):
        hostile = fixture.replace(command, wrapper)
        try:
            accepted = job_run_contains(workflow_jobs(hostile)["probe"], command)
        except AssertionError:
            continue
        assert not accepted

    separated_jobs = f"""jobs:
  first:
    steps:
      - run: echo first
# A top-level comment must not hide later jobs.

  probe:
    steps:
      - run: {command}
"""
    assert job_run_contains(workflow_jobs(separated_jobs)["probe"], command)


def validate_ci_gate_cutover(text: str) -> None:
    """#4: Bazel authority under CI Gate; Cargo rust-test + PR sticky retired."""
    jobs = workflow_jobs(text)
    assert "rust-test" not in jobs, (
        "Cargo rust-test job must stay retired after CI Gate cutover (#4)"
    )
    for job_id, body in jobs.items():
        assert job_scalar(body, "name") != "Rust Tests", (
            f"job {job_id!r} must not restore retired Cargo Rust Tests display name"
        )
        sticky, _ = sticky_contracts(body)
        assert not sticky, f"Test Suite job {job_id!r} must not mount Cargo sticky disks (#4)"

    authoritative = [
        job_id
        for job_id, body in jobs.items()
        if job_run_contains(body, "bazelisk test //:ci_rust_tests")
        or job_run_contains(body, "bazelisk test --config=ci //:ci_rust_tests")
    ]
    assert len(authoritative) == 1, (
        "exactly one Test Suite job must run authoritative bazelisk test //:ci_rust_tests"
    )
    auth_job = authoritative[0]

    gate_jobs = [job_id for job_id, body in jobs.items() if job_scalar(body, "name") == "CI Gate"]
    assert len(gate_jobs) == 1, "required check context must remain exactly one CI Gate job"
    gate_id = gate_jobs[0]
    gate_body = jobs[gate_id]
    needed = job_needs(gate_body)
    assert auth_job in needed, (
        f"CI Gate must depend on authoritative Bazel job {auth_job!r} (needs={sorted(needed)})"
    )
    assert "rust-test" not in needed, "CI Gate must not aggregate the retired rust-test job"
    assert "bazel-diagnostics" not in needed, (
        "CI Gate must not require bazel-diagnostics (non-required diagnostic lane)"
    )
    gate_runs = [
        normalize_run(scalar)
        for scalar in job_required_run_scalars(gate_body, "scripts/ci/require-gates.sh")
        if normalize_run(scalar).startswith("scripts/ci/require-gates.sh ")
    ]
    assert len(gate_runs) == 1, "CI Gate must have one active require-gates.sh run scalar"
    gate_run = gate_runs[0]
    assert f"needs.{auth_job}.result" in gate_run, (
        f"CI Gate must require {auth_job}.result via require-gates.sh"
    )
    assert "needs.rust-test.result" not in gate_run, (
        "CI Gate must not reference needs.rust-test.result"
    )
    assert "needs.bazel-diagnostics.result" not in gate_run, (
        "CI Gate must not reference needs.bazel-diagnostics.result"
    )
    assert "bazel-diagnostics" in jobs, "diagnostic dual-build/cache observe job must exist"
    diag_body = jobs["bazel-diagnostics"]
    assert job_run_contains(diag_body, "python3 scripts/ci/cargo-bazel-parity-check.py"), (
        "bazel-diagnostics must run dual-build parity"
    )
    assert "|| echo" not in diag_body, (
        "bazel-diagnostics must fail closed; no fabricated zero-hit JSON fallback"
    )
    assert not job_run_contains(
        jobs[auth_job], "python3 scripts/ci/cargo-bazel-parity-check.py --mode all"
    ), "authoritative bazel-bootstrap must not run dual-build parity"
    assert job_run_contains(
        jobs[auth_job], "python3 scripts/ci/cargo-bazel-parity-check.py --mode inventory"
    ), "authoritative bazel-bootstrap must run live suite-membership inventory"
    inventory_lines = [
        line
        for scalar in job_run_scalars(jobs[auth_job])
        for line in scalar.splitlines()
        if "cargo-bazel-parity-check.py --mode inventory" in line
    ]
    assert inventory_lines, "live inventory command line must be present in bazel-bootstrap"
    assert all("--skip-label-query" not in line for line in inventory_lines), (
        "authoritative bazel-bootstrap inventory must not skip bazelisk label query"
    )


def main() -> None:
    texts = {path: path.read_text(encoding="utf-8") for path in sorted(WORKFLOWS.glob("*.y*ml"))}
    test_suite = texts[WORKFLOWS / "test.yml"]
    validate_test_suite_trigger(test_suite)
    validate_required_run_negative_fixtures()
    validate_operator_handoffs_have_no_artifacts()
    validate_ci_gate_cutover(test_suite)
    jobs = workflow_jobs(test_suite)
    for job_id, runner in (
        ("windows-graphforge-storage-locks", "blacksmith-4vcpu-windows-2025"),
        ("macos-graphforge-storage-durability", "blacksmith-12vcpu-macos-15"),
    ):
        body = jobs[job_id]
        assert job_scalar(body, "runs-on") == runner
        assert job_runs_exact(body, "cargo test -p graphforge-filesystem --lib --no-fail-fast")
        assert job_runs_exact(
            body,
            "cargo test -p graphforge-storage filesystem_admission::tests:: --lib --no-fail-fast",
        )
    gate = jobs["ci-gate"]
    gate_dependencies = job_needs(gate)
    native_jobs = (
        "windows-graphforge-storage-locks",
        "macos-graphforge-storage-durability",
    )
    gate_runs = [
        normalize_run(scalar)
        for scalar in job_required_run_scalars(gate, "scripts/ci/require-gates.sh")
        if normalize_run(scalar).startswith("scripts/ci/require-gates.sh ")
    ]
    assert len(gate_runs) == 1
    for job_id in native_jobs:
        assert job_id in gate_dependencies
        assert f"needs.{job_id}.result" in gate_runs[0]

    artifact_uploads: list[str] = []
    artifact_downloads: list[str] = []
    saved: list[str] = []
    restored: list[str] = []
    dependency_keys: list[str] = []
    sticky_keys: list[str] = []
    sticky_deletes: list[str] = []
    for text in texts.values():
        file_uploads, file_downloads = artifact_contracts(text)
        artifact_uploads.extend(file_uploads)
        artifact_downloads.extend(file_downloads)
        file_saves, file_restores = cache_contracts(text)
        saved.extend(file_saves)
        restored.extend(file_restores)
        dependency_keys.extend(dependency_contracts(text))
        file_sticky, file_deletes = sticky_contracts(text)
        sticky_keys.extend(file_sticky)
        sticky_deletes.extend(file_deletes)
        validate_maturin_storage(text)
    assert Counter(artifact_uploads) == EXPECTED_ARTIFACT_UPLOADS, (
        "CI artifact producer contract drift"
    )
    assert Counter(artifact_downloads) == EXPECTED_ARTIFACT_DOWNLOADS, (
        "CI artifact consumer contract drift"
    )
    assert Counter(saved) == EXPECTED_SAVES, "CI transfer producer contract drift"
    assert Counter(restored) == EXPECTED_RESTORES, "CI transfer consumer contract drift"
    assert Counter(dependency_keys) == EXPECTED_DEPENDENCY_KEYS, "dependency cache contract drift"
    assert Counter(sticky_keys) == EXPECTED_STICKY_KEYS, "sticky-disk contract drift"
    assert Counter(sticky_deletes) == EXPECTED_STICKY_DELETES, "sticky-disk cleanup contract drift"
    print(
        f"CI storage policy passed: {len(artifact_uploads)} bounded artifact producers, "
        f"{len(artifact_downloads)} consumer, {len(saved)} cache transfer producers, "
        f"{len(restored)} consumers, "
        f"{len(dependency_keys)} dependency caches, {len(sticky_keys)} bounded sticky disks, "
        "one-day transfer, 14-day certification, and 30-day publication artifact retention"
    )


if __name__ == "__main__":
    main()
