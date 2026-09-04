#!/usr/bin/env python3
"""Keep direct UUIDv5 construction inside the shared core identity module."""

from __future__ import annotations

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
SHARED = Path("crates/graphforge-core/src/uuid.rs")
DERIVATION = re.compile(r"(?:\b(?:[A-Za-z_]\w*\s*::\s*)+|>\s*::\s*)(?:new_v5|from_sha1_bytes)\b")


def violations(path: Path, source: str) -> list[str]:
    """Reject associated constructors, including aliased types and split paths."""
    if path == SHARED:
        return []
    result = []
    for match in DERIVATION.finditer(source):
        spelling = re.sub(r"\s+", "", match.group())
        if spelling == "graphforge_core::uuid::new_v5":
            continue
        if spelling == "crate::uuid::new_v5" and path.parts[:2] == ("crates", "graphforge-core"):
            continue
        line = source.count("\n", 0, match.start()) + 1
        result.append(f"{path}:{line}: use graphforge_core::uuid, found {spelling}")
    return result


def main() -> None:
    """Exercise the guard, then scan all first-party Rust sources and fixtures."""
    caller = Path("crates/graphforge-api/src/example.rs")
    for expression in (
        "Uuid::new_v5(&namespace, name)",
        "uuid::Uuid::new_v5(&namespace, name)",
        "use uuid::Uuid as Identity; Identity :: new_v5(&namespace, name)",
        "Uuid\n::\nnew_v5(&namespace, name)",
        "<uuid::Uuid>::new_v5(&namespace, name)",
        "let derive = Uuid::new_v5; derive(&namespace, name)",
        "uuid::Builder::from_sha1_bytes(digest)",
    ):
        assert violations(caller, expression), expression
        assert not violations(SHARED, expression), expression
    for expression in (
        "graphforge_core::uuid::new_v5(&namespace, name)",
        "use graphforge_core::uuid::new_v5; new_v5(&namespace, name)",
        "Uuid::now_v7()",
        "Uuid::from_bytes(bytes)",
    ):
        assert not violations(caller, expression), expression
    assert violations(caller, "crate::uuid::new_v5(&namespace, name)")
    failures = [
        failure
        for path in sorted((ROOT / "crates").rglob("*.rs"))
        for failure in violations(path.relative_to(ROOT), path.read_text(encoding="utf-8"))
    ]
    assert not failures, "\n".join(failures)
    print("UUID derivation policy and mutation tests passed")


if __name__ == "__main__":
    main()
