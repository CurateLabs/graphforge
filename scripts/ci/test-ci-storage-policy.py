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
Cargo.lock hash + ``release-target-v1``. PR sticky keys stay job-isolated with
``${{ github.job }}`` and ``target-v1``.

This module inventories workflow storage steps and fails closed on drift.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"
EXPECTED_ARTIFACT_UPLOADS = Counter(
    {
        "binding-rc-report-${{ github.run_id }}-${{ matrix.target }}": 1,
        "binding-rc-report-${{ github.run_id }}-${{ matrix.report_target }}": 1,
        "binding-rc-wheel-${{ github.run_id }}-${{ matrix.target }}": 1,
        "binding-rc-addon-${{ github.run_id }}-${{ matrix.target }}": 1,
        "M1-Rust-Non-Cypher-${{ env.EVIDENCE_SHA }}": 1,
        "M1-Binding-Release-Candidate-${{ needs.validate_source.outputs.evidence_sha }}": 1,
        "M1-Release-Load-${{ github.run_id }}": 1,
        "M1-Release-Candidate-manifest-${{ needs.validate_source.outputs.evidence_sha }}": 1,
        "M1-Release-Candidate-python-${{ needs.validate_source.outputs.evidence_sha }}": 1,
        "M1-Release-Candidate-npm-${{ needs.validate_source.outputs.evidence_sha }}": 1,
        "M1-Release-Candidate-crates-${{ needs.validate_source.outputs.evidence_sha }}": 1,
        "M1-Release-Candidate-evidence-${{ needs.validate_source.outputs.evidence_sha }}": 1,
        "M1-Release-Reconciliation-${{ github.run_id }}": 1,
        "visualization-limits-stress-${{ github.sha }}": 1,
    }
)
EXPECTED_ARTIFACT_DOWNLOADS = Counter(
    {
        "binding-rc-report-${{ github.run_id }}-*": 1,
        "binding-rc-wheel-${{ github.run_id }}-*": 1,
        "binding-rc-addon-${{ github.run_id }}-*": 1,
        "M1-Rust-Non-Cypher-${{ needs.validate_source.outputs.evidence_sha }}": 1,
        "M1-Binding-Release-Candidate-${{ needs.validate_source.outputs.evidence_sha }}": 1,
        "M1-Release-Load-${{ github.run_id }}": 1,
        "M1-Release-Candidate-manifest-${{ steps.source.outputs.release_sha }}": 1,
        "M1-Release-Candidate-python-${{ steps.source.outputs.release_sha }}": 1,
        "M1-Release-Candidate-npm-${{ steps.source.outputs.release_sha }}": 1,
        "M1-Release-Candidate-crates-${{ steps.source.outputs.release_sha }}": 1,
        "M1-Release-Candidate-evidence-${{ steps.source.outputs.release_sha }}": 1,
        "M1-Release-Candidate-manifest-${{ needs.resolve_source.outputs.release_sha }}": 1,
        "M1-Release-Candidate-python-${{ needs.resolve_source.outputs.release_sha }}": 1,
        "M1-Release-Candidate-npm-${{ needs.resolve_source.outputs.release_sha }}": 1,
        "M1-Release-Candidate-crates-${{ needs.resolve_source.outputs.release_sha }}": 1,
        "M1-Release-Candidate-evidence-${{ needs.resolve_source.outputs.release_sha }}": 1,
    }
)
EXPECTED_DEPENDENCY_KEYS = Counter(
    {
        "${{ runner.os }}-cargo-registry-v1-${{ hashFiles('Cargo.lock') }}": 10,
        "${{ runner.os }}-fuzz-${{ hashFiles('fuzz/Cargo.toml', '**/Cargo.lock') }}": 1,
    }
)
EXPECTED_STICKY_KEYS = Counter(
    {
        "${{ github.repository }}-${{ github.job }}-${{ hashFiles('Cargo.lock') }}-target-v1": 5,
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
        "${{ github.repository }}-m1-release-load-${{ inputs.commit_sha }}-target-v3": 1,
    }
)
EXPECTED_STICKY_DELETES = Counter(
    {"${{ github.repository }}-m1-release-load-${{ inputs.commit_sha }}-target-v3": 1}
)
EXPECTED_SAVES = Counter(
    {
        "checkpoint-transfer-${{ github.run_id }}-rust": 1,
        "checkpoint-transfer-${{ github.run_id }}-python": 1,
        "checkpoint-transfer-${{ github.run_id }}-node": 1,
        "m20-transfer-${{ github.run_id }}-rust": 1,
        "m20-transfer-${{ github.run_id }}-python": 1,
        "m20-transfer-${{ github.run_id }}-node": 1,
        "m21-transfer-${{ github.run_id }}-rust": 1,
        "m21-transfer-${{ github.run_id }}-python": 1,
        "m21-transfer-${{ github.run_id }}-node": 1,
    }
)
EXPECTED_RESTORES = Counter(
    {
        "checkpoint-transfer-${{ github.run_id }}-rust": 1,
        "checkpoint-transfer-${{ github.run_id }}-python": 1,
        "checkpoint-transfer-${{ github.run_id }}-node": 1,
        "m20-transfer-${{ github.run_id }}-rust": 1,
        "m20-transfer-${{ github.run_id }}-python": 1,
        "m20-transfer-${{ github.run_id }}-node": 1,
        "m21-transfer-${{ github.run_id }}-rust": 1,
        "m21-transfer-${{ github.run_id }}-python": 1,
        "m21-transfer-${{ github.run_id }}-node": 1,
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
        assert uses == "actions/upload-artifact@v7", f"unapproved artifact action: {uses}"
        name = field(step, "name")
        assert name is not None, "artifact upload has no exact name"
        assert field(step, "if-no-files-found") == "error", (
            f"artifact upload is not fail-closed: {name}"
        )
        publication = name.startswith("M1-")
        expected_retention = "30" if publication else "1"
        assert field(step, "retention-days") == expected_retention, (
            f"artifact retention drift: {name}"
        )
        path = field(step, "path")
        assert path in {
            "binding-rc-reports/${{ matrix.target }}.json",
            "binding-rc-reports/${{ matrix.report_target }}.json",
            "dist/*.whl",
            "crates/graphforge-bindings-node/*.node",
            "non-cypher-evidence/",
            "binding-rc-aggregate/report.json",
            "m1-release-load-evidence",
            "candidate/v${{ env.RELEASE_VERSION }}-artifacts.json",
            "candidate/release-artifacts/python/",
            "candidate/release-artifacts/npm/",
            "candidate/release-artifacts/crates/",
            ("candidate/release-artifacts/evidence/\ncandidate/release-artifacts/node-addons/"),
            "reconciliation/summary.json",
            "examples/visualization/stress/results/",
        }, f"artifact upload contains unapproved bytes: {path}"
        uploaded.append(name)
    for step in action_steps(text, "actions/download-artifact@"):
        uses = field(step, "uses")
        assert uses == "actions/download-artifact@v8", f"unapproved artifact action: {uses}"
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
            "m1-release-load-evidence",
            "candidate",
            "candidate/release-artifacts/npm",
            "candidate/release-artifacts/crates",
            "candidate/release-artifacts",
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
            cross_run = name != "M1-Release-Load-${{ github.run_id }}"
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


def cache_contracts(text: str) -> tuple[list[str], list[str]]:
    saved: list[str] = []
    restored: list[str] = []
    for step in cache_steps(text):
        uses = field(step, "uses")
        if uses is None or not uses.startswith("actions/cache/"):
            continue
        assert uses in {"actions/cache/save@v6", "actions/cache/restore@v6"}, (
            f"unapproved cache transfer action: {uses}"
        )
        key = field(step, "key")
        assert key is not None, f"{uses} step has no exact key"
        if uses == "actions/cache/save@v6":
            saved.append(key)
        else:
            assert field(step, "fail-on-cache-miss") == "true", f"restore is not fail-closed: {key}"
            restored.append(key)
    return saved, restored


def dependency_contracts(text: str) -> list[str]:
    keys: list[str] = []
    for step in action_steps(text, "actions/cache@"):
        assert field(step, "uses") == "actions/cache@v6", (
            "dependency cache must use actions/cache@v6"
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
        if uses == "useblacksmith/stickydisk@v1":
            key = field(step, "key")
            assert key is not None, "sticky disk has no exact key"
            mounted.append(key)
        elif uses == "useblacksmith/stickydisk-delete@v1":
            key = field(step, "delete-key")
            assert key is not None, "sticky disk deletion has no exact key"
            deleted.append(key)
        else:
            raise AssertionError(f"unapproved sticky-disk action: {uses}")
    return mounted, deleted


def validate_maturin_storage(text: str) -> None:
    for step in action_steps(text, "PyO3/maturin-action@"):
        assert field(step, "uses") == "PyO3/maturin-action@v1", "unapproved Maturin action"
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


def main() -> None:
    texts = {path: path.read_text(encoding="utf-8") for path in sorted(WORKFLOWS.glob("*.y*ml"))}
    validate_test_suite_trigger(texts[WORKFLOWS / "test.yml"])

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
        "one-day transfer and 30-day publication artifact retention"
    )


if __name__ == "__main__":
    main()
