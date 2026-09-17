#!/usr/bin/env python3
"""Reconcile every ADR index with ``docs/adr/`` itself (#1390).

Four hand-maintained places have to agree with the ADR directory:

==================================== ==========================================
``docs/adr/README.md``               primary index table
``docs/engineering/adrs/README.md``  engineering decision log, with a status
                                     column
``docs-site/scripts/sync-content.mjs`` publication allowlist
``docs-site/astro.config.mjs``       published site sidebar
==================================== ==========================================

The last two are machine-consumed by the docs build and carry no human prose, so
they are *generated* from the directory between marker comments rather than
checked: drift is removed instead of detected. The two markdown tables carry
hand-written titles and statuses, so they are checked.

Usage::

    python3 scripts/ci/adr-index.py list      # one record per line
    python3 scripts/ci/adr-index.py generate  # rewrite the docs-site regions
    python3 scripts/ci/adr-index.py check     # fail closed on any disagreement
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
ADR_DIR = ROOT / "docs" / "adr"
ADR_README = ADR_DIR / "README.md"
ENGINEERING_README = ROOT / "docs" / "engineering" / "adrs" / "README.md"
SYNC_CONTENT = ROOT / "docs-site" / "scripts" / "sync-content.mjs"
ASTRO_CONFIG = ROOT / "docs-site" / "astro.config.mjs"

BEGIN_MARKER = "BEGIN generated ADR records — scripts/ci/adr-index.py generate"
END_MARKER = "END generated ADR records"

FILENAME_RE = re.compile(r"^(\d{4})-([a-z0-9]+(?:-[a-z0-9.]+)*)\.md$")
TITLE_RE = re.compile(r"^#\s+ADR\s+(\d{4}):\s*(.+?)\s*$")
STATUS_RE = re.compile(r"^\*\*Status:\*\*\s*(.+?)\s*$")
SUPERSEDED_STATUS_RE = re.compile(r"^Superseded by ADR (\d{4})$")
SIMPLE_STATUSES = frozenset({"Proposed", "Accepted", "Deprecated"})

# Prettier keeps a sidebar entry on one line while it fits the 100-column bound
# used across this repository (.editorconfig max_line_length).
SIDEBAR_WIDTH = 100
SIDEBAR_INDENT = " " * 16
SYNC_INDENT = " " * 2


class AdrError(RuntimeError):
    """The ADR directory or one of its indexes is inconsistent."""


@dataclass(frozen=True)
class Record:
    """One ADR on disk."""

    number: str
    filename: str
    title: str
    status: str
    superseded: bool
    superseded_by: str | None

    @property
    def relpath(self) -> str:
        """Path relative to ``docs/adr/``."""
        return f"superseded/{self.filename}" if self.superseded else self.filename

    @property
    def slug(self) -> str:
        """Starlight slug for the published page."""
        return f"adr/{self.filename[:-3]}"

    @property
    def label(self) -> str:
        """Sidebar label: number, em dash, title."""
        return f"{self.number} — {self.title}"


def _parse_record(path: Path, *, superseded: bool) -> Record:
    match = FILENAME_RE.match(path.name)
    if match is None:
        raise AdrError(f"{path.relative_to(ROOT)}: filename is not NNNN-slug.md")
    number = match.group(1)
    lines = path.read_text(encoding="utf-8").splitlines()

    title_match = next((TITLE_RE.match(line) for line in lines[:1]), None)
    if title_match is None:
        raise AdrError(f"{path.relative_to(ROOT)}: first line must be '# ADR {number}: <title>'")
    if title_match.group(1) != number:
        raise AdrError(
            f"{path.relative_to(ROOT)}: title says ADR {title_match.group(1)}, "
            f"filename says {number}"
        )

    status: str | None = None
    for line in lines:
        status_match = STATUS_RE.match(line)
        if status_match is not None:
            status = status_match.group(1)
            break
    if status is None:
        raise AdrError(f"{path.relative_to(ROOT)}: no '**Status:**' line")

    superseded_by = SUPERSEDED_STATUS_RE.match(status)
    if superseded_by is None and status not in SIMPLE_STATUSES:
        raise AdrError(
            f"{path.relative_to(ROOT)}: status {status!r} is not one of "
            "Proposed / Accepted / Deprecated / 'Superseded by ADR NNNN'"
        )
    if superseded and superseded_by is None:
        raise AdrError(
            f"{path.relative_to(ROOT)}: lives under superseded/ but its status is "
            f"{status!r}, not 'Superseded by ADR NNNN'"
        )
    if not superseded and superseded_by is not None:
        raise AdrError(
            f"{path.relative_to(ROOT)}: status is {status!r} but the record is still "
            "in the active directory; move it to docs/adr/superseded/"
        )

    return Record(
        number=number,
        filename=path.name,
        title=title_match.group(2),
        status=status,
        superseded=superseded,
        superseded_by=superseded_by.group(1) if superseded_by else None,
    )


def load_records(root: Path = ROOT) -> list[Record]:
    """Return every ADR on disk, active and superseded, ordered by number."""
    adr_dir = root / "docs" / "adr"
    superseded_dir = adr_dir / "superseded"
    records: list[Record] = []
    for path in sorted(adr_dir.glob("*.md")):
        if path.name == "README.md":
            continue
        records.append(_parse_record(path, superseded=False))
    if superseded_dir.is_dir():
        for path in sorted(superseded_dir.glob("*.md")):
            if path.name == "README.md":
                continue
            records.append(_parse_record(path, superseded=True))

    records.sort(key=lambda record: record.number)
    seen: dict[str, Record] = {}
    for record in records:
        previous = seen.get(record.number)
        if previous is not None:
            raise AdrError(
                f"ADR {record.number} is used twice: {previous.relpath} and "
                f"{record.relpath}. A number is never reassigned."
            )
        seen[record.number] = record

    numbers = set(seen)
    for record in records:
        if record.superseded_by is not None and record.superseded_by not in numbers:
            raise AdrError(
                f"{record.relpath}: superseded by ADR {record.superseded_by}, which does not exist"
            )
    if not records:
        raise AdrError("docs/adr/ holds no ADR records")
    return records


def active(records: list[Record]) -> list[Record]:
    return [record for record in records if not record.superseded]


def superseded(records: list[Record]) -> list[Record]:
    return [record for record in records if record.superseded]


# --------------------------------------------------------------------------
# Generated regions (docs-site)
# --------------------------------------------------------------------------


def _js_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def sync_content_region(records: list[Record]) -> list[str]:
    """Publication allowlist lines: every active record, superseded excluded."""
    return [f"{SYNC_INDENT}{_js_string('adr/' + r.filename)}," for r in active(records)]


def astro_sidebar_region(records: list[Record]) -> list[str]:
    """Sidebar entry lines: every active record, superseded excluded."""
    lines: list[str] = []
    for record in active(records):
        label = _js_string(record.label)
        slug = _js_string(record.slug)
        single = f"{SIDEBAR_INDENT}{{ label: {label}, slug: {slug} }},"
        if len(single) <= SIDEBAR_WIDTH:
            lines.append(single)
            continue
        lines.append(f"{SIDEBAR_INDENT}{{")
        lines.append(f"{SIDEBAR_INDENT}  label: {label},")
        lines.append(f"{SIDEBAR_INDENT}  slug: {slug},")
        lines.append(f"{SIDEBAR_INDENT}}},")
    return lines


def _split_region(path: Path) -> tuple[list[str], list[str], list[str]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    begins = [i for i, line in enumerate(lines) if BEGIN_MARKER in line]
    ends = [i for i, line in enumerate(lines) if END_MARKER in line]
    if len(begins) != 1 or len(ends) != 1 or ends[0] < begins[0]:
        raise AdrError(
            f"{path.relative_to(ROOT)}: expected exactly one "
            f"'{BEGIN_MARKER}' … '{END_MARKER}' region"
        )
    return lines[: begins[0] + 1], lines[begins[0] + 1 : ends[0]], lines[ends[0] :]


def render(path: Path, body: list[str]) -> str:
    head, _current, tail = _split_region(path)
    return "\n".join([*head, *body, *tail]) + "\n"


GENERATED = (
    (SYNC_CONTENT, sync_content_region),
    (ASTRO_CONFIG, astro_sidebar_region),
)


def generate(records: list[Record]) -> list[Path]:
    """Rewrite the generated regions in place; return the files that changed."""
    changed: list[Path] = []
    for path, builder in GENERATED:
        wanted = render(path, builder(records))
        if path.read_text(encoding="utf-8") != wanted:
            path.write_text(wanted, encoding="utf-8")
            changed.append(path)
    return changed


def check_generated(records: list[Record]) -> list[str]:
    problems: list[str] = []
    for path, builder in GENERATED:
        wanted = render(path, builder(records))
        if path.read_text(encoding="utf-8") != wanted:
            problems.append(
                f"{path.relative_to(ROOT)}: generated ADR region is stale. "
                "Run: python3 scripts/ci/adr-index.py generate"
            )
    return problems


# --------------------------------------------------------------------------
# Checked tables (markdown)
# --------------------------------------------------------------------------


def _tables(text: str, header: str) -> list[list[list[str]]]:
    """Return every table whose header row equals ``header``, as cell rows."""
    tables: list[list[list[str]]] = []
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line.strip() != header:
            continue
        rows: list[list[str]] = []
        for row in lines[index + 2 :]:
            stripped = row.strip()
            if not stripped.startswith("|"):
                break
            rows.append([cell.strip() for cell in stripped.strip("|").split("|")])
        tables.append(rows)
    return tables


def _one_table(path: Path, header: str, which: int, total: int) -> list[list[str]]:
    tables = _tables(path.read_text(encoding="utf-8"), header)
    if len(tables) != total:
        raise AdrError(
            f"{path.relative_to(ROOT)}: expected {total} table(s) with header "
            f"{header!r}, found {len(tables)}"
        )
    width = len(header.strip("|").split("|"))
    for row in tables[which]:
        if len(row) != width:
            raise AdrError(
                f"{path.relative_to(ROOT)}: row {row} under {header!r} has "
                f"{len(row)} cells, expected {width}"
            )
    return tables[which]


LINK_RE = re.compile(r"^\[(?P<text>.+)\]\((?P<target>[^)]+)\)$")
CODE_RE = re.compile(r"^`(?P<value>.+)`$")


def _unlink(cell: str) -> tuple[str, str | None]:
    """Split ``[text](target)`` into text and target; plain text has no target."""
    match = LINK_RE.match(cell)
    if match is None:
        return cell, None
    return match.group("text"), match.group("target")


def _uncode(cell: str) -> str:
    match = CODE_RE.match(cell)
    return match.group("value") if match else cell


def _compare(
    label: str,
    rows: list[tuple[str, tuple[str, ...]]],
    expected: list[tuple[str, tuple[str, ...]]],
    *,
    exists: set[str],
    names: tuple[str, ...],
) -> list[str]:
    """Diff index rows against records, keyed by ADR number."""
    problems: list[str] = []
    seen: dict[str, tuple[str, ...]] = {}
    for number, fields in rows:
        if number in seen:
            problems.append(f"{label}: ADR {number} is listed twice")
            continue
        seen[number] = fields
        for target in dict.fromkeys(field for field in fields if field.endswith(".md")):
            if target not in exists:
                problems.append(f"{label}: ADR {number} names {target}, which does not exist")
    wanted = dict(expected)

    for number in sorted(set(wanted) - set(seen)):
        problems.append(f"{label}: ADR {number} is on disk but missing from this index")
    for number in sorted(set(seen) - set(wanted)):
        problems.append(
            f"{label}: ADR {number} is listed here but not in that section of docs/adr/"
        )
    for number in sorted(set(seen) & set(wanted)):
        for name, found, want in zip(names, seen[number], wanted[number], strict=True):
            if found != want:
                problems.append(
                    f"{label}: ADR {number} {name} is {found!r}, but docs/adr/ says {want!r}"
                )
    return problems


def check_adr_readme(records: list[Record]) -> list[str]:
    """``docs/adr/README.md``: active table and superseded table."""
    on_disk = {record.relpath for record in records}
    problems: list[str] = []

    rows = []
    for cells in _one_table(ADR_README, "| ADR | Title | File |", 0, 1):
        number = cells[0]
        title, target = _unlink(cells[1])
        rows.append((number, (title, target or "", _uncode(cells[2]))))
    expected = [(r.number, (r.title, r.relpath, r.relpath)) for r in active(records)]
    problems += _compare(
        "docs/adr/README.md",
        rows,
        expected,
        exists=on_disk,
        names=("title", "title link target", "file cell"),
    )

    rows = []
    header = "| ADR | Title | Superseded by | File |"
    for cells in _one_table(ADR_README, header, 0, 1):
        number = cells[0]
        title, target = _unlink(cells[1])
        rows.append((number, (title, target or "", cells[2], _uncode(cells[3]))))
    expected = [
        (
            r.number,
            (r.title, r.relpath, f"ADR {r.superseded_by}", r.relpath),
        )
        for r in superseded(records)
    ]
    problems += _compare(
        "docs/adr/README.md superseded table",
        rows,
        expected,
        exists=on_disk,
        names=("title", "title link target", "superseded-by cell", "file cell"),
    )
    return problems


def check_engineering_readme(records: list[Record]) -> list[str]:
    """``docs/engineering/adrs/README.md``: decision log and superseded table."""
    header = "| ADR | Title | Status | Path |"
    on_disk = {f"../../adr/{record.relpath}" for record in records}
    problems: list[str] = []

    for which, subset, name in (
        (0, active(records), "decision log"),
        (1, superseded(records), "superseded table"),
    ):
        rows = []
        for cells in _one_table(ENGINEERING_README, header, which, 2):
            text, target = _unlink(cells[3])
            rows.append(
                (cells[0], (cells[1], cells[2], _uncode(text), _uncode(target or cells[3])))
            )
        expected = [
            (
                r.number,
                (r.title, r.status, f"../../adr/{r.relpath}", f"../../adr/{r.relpath}"),
            )
            for r in subset
        ]
        problems += _compare(
            f"docs/engineering/adrs/README.md {name}",
            rows,
            expected,
            exists=on_disk,
            names=("title", "status", "path cell", "path link target"),
        )
    return problems


def check(records: list[Record]) -> list[str]:
    return check_adr_readme(records) + check_engineering_readme(records) + check_generated(records)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("list", "generate", "check"))
    args = parser.parse_args(argv)

    try:
        records = load_records()
    except AdrError as error:
        print(f"adr-index: {error}", file=sys.stderr)
        return 1

    if args.command == "list":
        for record in records:
            print(f"{record.number}\t{record.status}\t{record.relpath}")
        return 0

    try:
        if args.command == "generate":
            changed = generate(records)
            for path in changed:
                print(f"adr-index: rewrote {path.relative_to(ROOT)}")
            print(
                f"adr-index: generated ADR regions for {len(active(records))} active "
                f"records ({len(changed)} file(s) changed)"
            )
            return 0
        problems = check(records)
    except AdrError as error:
        print(f"adr-index: {error}", file=sys.stderr)
        return 1

    if problems:
        print("adr-index: the ADR indexes disagree with docs/adr/", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1
    print(
        f"adr-index: ok — {len(active(records))} active and "
        f"{len(superseded(records))} superseded records agree across all four indexes"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
