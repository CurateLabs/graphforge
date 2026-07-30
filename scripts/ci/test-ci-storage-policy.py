#!/usr/bin/env python3
"""Enforce Blacksmith-only, consumer-driven CI transfer storage."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"
FORBIDDEN = ("actions/upload-artifact@", "actions/download-artifact@", "retention-days:")
EXPECTED_DEPENDENCY_KEYS = Counter(
    {
        "${{ runner.os }}-cargo-registry-v1-${{ hashFiles('Cargo.lock') }}": 7,
        "${{ runner.os }}-fuzz-${{ hashFiles('fuzz/Cargo.toml', '**/Cargo.lock') }}": 1,
    }
)
EXPECTED_STICKY_KEYS = Counter(
    {
        "${{ github.repository }}-${{ github.job }}-${{ hashFiles('Cargo.lock') }}-target-v1": 5,
        (
            "${{ github.repository }}-daily-fuzz-"
            "${{ hashFiles('fuzz/Cargo.toml', '**/Cargo.lock') }}-target-v1"
        ): 1,
        "${{ github.repository }}-m22-load-${{ inputs.commit_sha }}-target-v3": 1,
    }
)
EXPECTED_STICKY_DELETES = Counter(
    {"${{ github.repository }}-m22-load-${{ inputs.commit_sha }}-target-v3": 1}
)
EXPECTED_SAVES = Counter(
    {
        "binding-rc-transfer-${{ github.run_id }}-${{ matrix.target }}": 1,
        "binding-rc-transfer-${{ github.run_id }}-${{ matrix.report_target }}": 1,
        "binding-release-candidate-${{ needs.validate_source.outputs.evidence_sha }}": 1,
        "checkpoint-transfer-${{ github.run_id }}-rust": 1,
        "checkpoint-transfer-${{ github.run_id }}-python": 1,
        "checkpoint-transfer-${{ github.run_id }}-node": 1,
        "m20-transfer-${{ github.run_id }}-rust": 1,
        "m20-transfer-${{ github.run_id }}-python": 1,
        "m20-transfer-${{ github.run_id }}-node": 1,
        "m21-transfer-${{ github.run_id }}-rust": 1,
        "m21-transfer-${{ github.run_id }}-python": 1,
        "m21-transfer-${{ github.run_id }}-node": 1,
        "m22-load-${{ github.run_id }}": 1,
        "publish-node-${{ github.run_id }}-${{ matrix.settings.target }}": 1,
        "publish-python-${{ github.run_id }}-${{ matrix.os }}": 1,
        "publish-python-${{ github.run_id }}-sdist": 1,
        "rust-non-cypher-${{ env.EVIDENCE_SHA }}": 1,
    }
)
EXPECTED_RESTORES = Counter(
    {
        "binding-rc-transfer-${{ github.run_id }}-python-ubuntu": 1,
        "binding-rc-transfer-${{ github.run_id }}-python-macos": 1,
        "binding-rc-transfer-${{ github.run_id }}-python-windows": 1,
        "binding-rc-transfer-${{ github.run_id }}-node-x86_64-apple-darwin": 1,
        "binding-rc-transfer-${{ github.run_id }}-node-aarch64-apple-darwin": 1,
        "binding-rc-transfer-${{ github.run_id }}-node-x86_64-unknown-linux-gnu": 1,
        "binding-rc-transfer-${{ github.run_id }}-node-aarch64-unknown-linux-gnu": 1,
        "binding-rc-transfer-${{ github.run_id }}-node-x86_64-pc-windows-msvc": 1,
        "binding-release-candidate-${{ needs.validate_source.outputs.evidence_sha }}": 1,
        "checkpoint-transfer-${{ github.run_id }}-rust": 1,
        "checkpoint-transfer-${{ github.run_id }}-python": 1,
        "checkpoint-transfer-${{ github.run_id }}-node": 1,
        "m20-transfer-${{ github.run_id }}-rust": 1,
        "m20-transfer-${{ github.run_id }}-python": 1,
        "m20-transfer-${{ github.run_id }}-node": 1,
        "m21-transfer-${{ github.run_id }}-rust": 1,
        "m21-transfer-${{ github.run_id }}-python": 1,
        "m21-transfer-${{ github.run_id }}-node": 1,
        "m22-load-${{ github.run_id }}": 1,
        "publish-node-${{ github.run_id }}-x86_64-apple-darwin": 1,
        "publish-node-${{ github.run_id }}-aarch64-apple-darwin": 1,
        "publish-node-${{ github.run_id }}-x86_64-unknown-linux-gnu": 1,
        "publish-node-${{ github.run_id }}-aarch64-unknown-linux-gnu": 1,
        "publish-node-${{ github.run_id }}-x86_64-pc-windows-msvc": 1,
        "publish-python-${{ github.run_id }}-blacksmith-4vcpu-ubuntu-2404": 1,
        "publish-python-${{ github.run_id }}-blacksmith-6vcpu-macos-15": 1,
        "publish-python-${{ github.run_id }}-blacksmith-4vcpu-windows-2025": 1,
        "publish-python-${{ github.run_id }}-sdist": 1,
        "rust-non-cypher-${{ needs.validate_source.outputs.evidence_sha }}": 1,
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
    for line in step:
        stripped = line.strip().removeprefix("- ")
        if stripped.startswith(name + ":"):
            return stripped.split(":", 1)[1].strip().strip("'\"")
    return None


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
        assert "target" not in rendered, f"large build tree stored in actions/cache: {key}"
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
            f"Maturin sccache uses GitHub storage: {sccache}"
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
    for path, text in texts.items():
        for forbidden in FORBIDDEN:
            assert forbidden not in text, f"{path.relative_to(ROOT)} uses {forbidden}"

    saved: list[str] = []
    restored: list[str] = []
    dependency_keys: list[str] = []
    sticky_keys: list[str] = []
    sticky_deletes: list[str] = []
    for text in texts.values():
        file_saves, file_restores = cache_contracts(text)
        saved.extend(file_saves)
        restored.extend(file_restores)
        dependency_keys.extend(dependency_contracts(text))
        file_sticky, file_deletes = sticky_contracts(text)
        sticky_keys.extend(file_sticky)
        sticky_deletes.extend(file_deletes)
        validate_maturin_storage(text)
    assert Counter(saved) == EXPECTED_SAVES, "CI transfer producer contract drift"
    assert Counter(restored) == EXPECTED_RESTORES, "CI transfer consumer contract drift"
    assert Counter(dependency_keys) == EXPECTED_DEPENDENCY_KEYS, "dependency cache contract drift"
    assert Counter(sticky_keys) == EXPECTED_STICKY_KEYS, "sticky-disk contract drift"
    assert Counter(sticky_deletes) == EXPECTED_STICKY_DELETES, "sticky-disk cleanup contract drift"
    print(
        f"CI storage policy passed: {len(saved)} transfer producers, {len(restored)} consumers, "
        f"{len(dependency_keys)} dependency caches, {len(sticky_keys)} bounded sticky disks, "
        "zero retained GitHub artifacts"
    )


if __name__ == "__main__":
    main()
