#!/usr/bin/env python3
"""
Parse a pytest JUnit XML file and report OpenCypher TCK compliance metrics.

Reads counts directly from the <testsuite> element attributes, which pytest
always populates correctly — avoiding fragile grep-based line counting.

Usage:
    python3 scripts/tck_metrics.py [junit_xml_path]

Default path: test-results-tck.xml
Exit codes: 0 = all tests passed, 1 = failures/errors present,
2 = infrastructure error (missing/malformed XML).
"""

from __future__ import annotations

from pathlib import Path
import sys

import defusedxml.ElementTree as ET  # noqa: N817


def find_testsuite(tree: ET.ElementTree) -> ET.Element | None:
    """Return the first <testsuite> element, handling both root patterns.

    pytest can produce either:
      <testsuite ...>          (root is the testsuite)
      <testsuites><testsuite ...>  (root is a wrapper)
    """
    root = tree.getroot()
    if root.tag == "testsuite":
        return root
    # Look one level down for <testsuite>
    return root.find("testsuite")


def parse_counts(testsuite: ET.Element) -> dict[str, int]:
    """Extract test counts from <testsuite> attributes."""

    def _int(attr: str) -> int:
        raw = testsuite.get(attr, "0")
        try:
            return int(raw)
        except (ValueError, TypeError):
            return 0

    total = _int("tests")
    failures = _int("failures")
    errors = _int("errors")
    skipped = _int("skipped")
    passed = max(0, total - failures - errors - skipped)

    return {
        "total": total,
        "passed": passed,
        "failures": failures,
        "errors": errors,
        "skipped": skipped,
    }


def format_report(counts: dict[str, int]) -> str:
    total = counts["total"]
    passed = counts["passed"]
    failures = counts["failures"]
    errors = counts["errors"]
    skipped = counts["skipped"]

    lines = [
        "=" * 42,
        "  OpenCypher TCK Compliance Report",
        "=" * 42,
    ]

    if total > 0:
        pass_rate = passed * 100 / total
        lines += [
            f"  Passed:   {passed:>6}",
            f"  Failed:   {failures:>6}",
            f"  Errors:   {errors:>6}",
            f"  Skipped:  {skipped:>6}",
            f"  Total:    {total:>6}",
            "-" * 42,
            f"  Pass rate: {pass_rate:.1f}%",
            "=" * 42,
            "",
            "Note: TCK tests measure OpenCypher compliance progress.",
            "Results are tracked over time via Codecov artifacts.",
            "Exit 1 if failures or errors > 0; exit 2 on missing/malformed XML.",
        ]
    else:
        lines += [
            "  No test cases found in XML.",
            "=" * 42,
        ]

    return "\n".join(lines)


def check_failures(counts: dict[str, int]) -> int:
    """Return non-zero exit code if there are TCK failures or errors."""
    failures = counts["failures"]
    errors = counts["errors"]
    if failures > 0 or errors > 0:
        print(f"\nERROR: TCK compliance check failed — {failures} failure(s), {errors} error(s)")
        return 1
    return 0


def main() -> None:
    xml_path = Path(sys.argv[1] if len(sys.argv) > 1 else "test-results-tck.xml")

    if not xml_path.exists():
        print(f"ERROR: {xml_path} not found -- cannot evaluate TCK compliance.", file=sys.stderr)
        sys.exit(2)

    try:
        tree = ET.parse(xml_path)
    except ET.ParseError as exc:
        print(f"ERROR: could not parse {xml_path}: {exc}", file=sys.stderr)
        sys.exit(2)

    testsuite = find_testsuite(tree)
    if testsuite is None:
        print(f"ERROR: no <testsuite> element found in {xml_path}", file=sys.stderr)
        sys.exit(2)

    counts = parse_counts(testsuite)
    report = format_report(counts)
    print(report)
    sys.exit(check_failures(counts))


if __name__ == "__main__":
    main()
