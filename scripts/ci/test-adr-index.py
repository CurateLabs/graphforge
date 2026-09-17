#!/usr/bin/env python3
"""Mutation tests for scripts/ci/adr-index.py (#1390).

A passing check proves nothing here: the whole issue exists because four ADR
indexes silently disagreed with ``docs/adr/``. Every case below mutates a copy
of the real tree, asserts the gate fails, and restores it.
"""

from __future__ import annotations

from collections.abc import Callable
import importlib.util
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = Path(__file__).with_name("adr-index.py")

COPIED = (
    "docs/adr",
    "docs/engineering/adrs/README.md",
    "docs-site/scripts/sync-content.mjs",
    "docs-site/astro.config.mjs",
)


def load_module():
    spec = importlib.util.spec_from_file_location("adr_index", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


mod = load_module()


def retarget(root: Path) -> None:
    """Point the module's path constants at a scratch copy of the tree."""
    mod.ROOT = root
    mod.ADR_DIR = root / "docs" / "adr"
    mod.ADR_README = mod.ADR_DIR / "README.md"
    mod.ENGINEERING_README = root / "docs" / "engineering" / "adrs" / "README.md"
    mod.SYNC_CONTENT = root / "docs-site" / "scripts" / "sync-content.mjs"
    mod.ASTRO_CONFIG = root / "docs-site" / "astro.config.mjs"
    mod.GENERATED = (
        (mod.SYNC_CONTENT, mod.sync_content_region),
        (mod.ASTRO_CONFIG, mod.astro_sidebar_region),
    )


def populate(root: Path) -> None:
    for relative in COPIED:
        source = ROOT / relative
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            shutil.copytree(source, target)
        else:
            shutil.copy2(source, target)


def failures(root: Path) -> list[str]:
    """Run the whole gate, turning a directory-level error into one problem."""
    try:
        records = mod.load_records(root)
    except mod.AdrError as error:
        return [str(error)]
    try:
        return mod.check(records)
    except mod.AdrError as error:
        return [str(error)]


def drop_line(path: Path, needle: str) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    kept = [line for line in lines if needle not in line]
    assert len(kept) < len(lines), f"{path}: nothing matched {needle!r}"
    path.write_text("\n".join(kept) + "\n", encoding="utf-8")


def substitute(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    assert old in text, f"{path}: {old!r} not present"
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


CASES: list[tuple[str, Callable[[Path], None], str]] = []


def case(name: str, expected: str) -> Callable[[Callable[[Path], None]], None]:
    def register(mutate: Callable[[Path], None]) -> None:
        CASES.append((name, mutate, expected))

    return register


# --- a record in the directory but absent from an index ---------------------


@case("record missing from docs/adr/README.md", "ADR 0026 is on disk but missing")
def _(root: Path) -> None:
    drop_line(root / "docs/adr/README.md", "| 0026 |")


@case("record missing from the engineering log", "ADR 0026 is on disk but missing")
def _(root: Path) -> None:
    drop_line(root / "docs/engineering/adrs/README.md", "| 0026 |")


@case("record missing from the publication allowlist", "sync-content.mjs")
def _(root: Path) -> None:
    drop_line(root / "docs-site/scripts/sync-content.mjs", "'adr/0026-read-plan-resources.md'")


@case("record missing from the site sidebar", "astro.config.mjs")
def _(root: Path) -> None:
    drop_line(root / "docs-site/astro.config.mjs", "adr/0026-read-plan-resources'")


# --- an index row naming a file that does not exist -------------------------


@case("index row names a missing file", "which does not exist")
def _(root: Path) -> None:
    substitute(
        root / "docs/adr/README.md",
        "0026-read-plan-resources.md",
        "0026-read-plan-resources-typo.md",
    )


@case("engineering row names a missing file", "which does not exist")
def _(root: Path) -> None:
    substitute(
        root / "docs/engineering/adrs/README.md",
        "0026-read-plan-resources.md",
        "0026-read-plan-resources-typo.md",
    )


# --- a record present in some indexes but not others ------------------------


@case("new record in no index at all", "0099")
def _(root: Path) -> None:
    (root / "docs/adr/0099-untracked-record.md").write_text(
        "# ADR 0099: Untracked record\n\n**Status:** Accepted\n", encoding="utf-8"
    )


# --- a record in the active directory that the allowlist omits --------------


@case("superseded record leaks into the allowlist", "sync-content.mjs")
def _(root: Path) -> None:
    substitute(
        root / "docs-site/scripts/sync-content.mjs",
        "  'adr/0001-rust-core.md',",
        "  'adr/0001-rust-core.md',\n  'adr/0033-prerelease-version-identity.md',",
    )


# --- a superseded record still listed as active -----------------------------


@case("superseded body left in the active directory", "still in the active directory")
def _(root: Path) -> None:
    shutil.move(
        str(root / "docs/adr/superseded/0033-prerelease-version-identity.md"),
        str(root / "docs/adr/0033-prerelease-version-identity.md"),
    )


@case(
    "engineering log calls a superseded record Accepted",
    "ADR 0033 status is 'Accepted', but docs/adr/ says 'Superseded by ADR 0036'",
)
def _(root: Path) -> None:
    substitute(
        root / "docs/engineering/adrs/README.md",
        "| 0033 | Prereleases share one version with per-ecosystem spelling "
        "| Superseded by ADR 0036 |",
        "| 0033 | Prereleases share one version with per-ecosystem spelling | Accepted |",
    )


# --- record integrity -------------------------------------------------------


@case("title drifts from the ADR body", "ADR 0026 title is 'Read plans bind stuff")
def _(root: Path) -> None:
    substitute(root / "docs/adr/README.md", "[Read plans bind resources", "[Read plans bind stuff")


@case("two records claim one number", "is used twice")
def _(root: Path) -> None:
    shutil.copy2(
        root / "docs/adr/0026-read-plan-resources.md",
        root / "docs/adr/0026-read-plan-duplicate.md",
    )


@case("a record loses its status line", "no '**Status:**' line")
def _(root: Path) -> None:
    drop_line(root / "docs/adr/0026-read-plan-resources.md", "**Status:**")


@case("supersession points at a record that does not exist", "which does not exist")
def _(root: Path) -> None:
    substitute(
        root / "docs/adr/superseded/0033-prerelease-version-identity.md",
        "**Status:** Superseded by ADR 0036",
        "**Status:** Superseded by ADR 0077",
    )


@case("an index row loses a cell", "cells, expected")
def _(root: Path) -> None:
    substitute(
        root / "docs/adr/README.md",
        "| 0001 | [Rust Core](0001-rust-core.md) | `0001-rust-core.md` |",
        "| 0001 | [Rust Core](0001-rust-core.md) |",
    )


@case("a generated marker region is removed", "exactly one")
def _(root: Path) -> None:
    drop_line(root / "docs-site/astro.config.mjs", mod.END_MARKER)


def run_cases() -> None:
    for name, mutate, expected in CASES:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            populate(root)
            retarget(root)
            assert failures(root) == [], f"{name}: baseline copy is not clean"

            mutate(root)
            problems = failures(root)
            assert problems, f"{name}: mutation did not fail the gate"
            joined = "\n".join(problems)
            assert expected in joined, f"{name}: expected {expected!r} in:\n{joined}"

            again = failures(root)
            assert again == problems, f"{name}: gate is not deterministic"
        print(f"  fails closed: {name}")


def run_generate_is_idempotent() -> None:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        populate(root)
        retarget(root)
        records = mod.load_records(root)
        assert mod.generate(records) == [], "generate rewrote a clean tree"

        drop_line(root / "docs-site/scripts/sync-content.mjs", "'adr/0026-read-plan-resources.md'")
        drop_line(root / "docs-site/astro.config.mjs", "adr/0026-read-plan-resources'")
        assert len(mod.generate(records)) == 2, "generate did not repair both files"
        assert failures(root) == [], "generate left the tree failing"
        assert mod.generate(records) == [], "generate is not idempotent"
    print("  generate: repairs drift, then is a no-op")


def run_cli() -> None:
    for command in ("list", "check"):
        result = subprocess.run(
            [sys.executable, str(SCRIPT), command],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, f"{command}: {result.stderr}"
    print("  cli: list and check pass on the real tree")


retarget(ROOT)
assert failures(ROOT) == [], f"real tree already fails: {failures(ROOT)}"
run_cli()
run_generate_is_idempotent()
run_cases()
print(f"adr-index tests passed ({len(CASES)} mutations proven to fail closed)")
