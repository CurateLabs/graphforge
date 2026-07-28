#!/usr/bin/env python3
"""Parse a pytest JUnit XML and report wall-clock timing for slow TCK tests.

Identifies tests that exceed a configurable threshold and emits a summary
table, making parser/executor performance regressions visible in CI.

Usage:
    python3 scripts/tck_perf_report.py [junit_xml_path] [--threshold SECONDS]

Default path: test-results-tck.xml
Default threshold: 2.0 seconds
Exits 0 always (informational only).
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

import defusedxml.ElementTree as ET  # noqa: N817

DEFAULT_THRESHOLD = 2.0
TARGET_TESTS = {
    "test_generate_the_movie_graph",
    "test_many_create_clauses",
}


def find_testsuites(tree: ET.ElementTree) -> list[ET.Element]:
    root = tree.getroot()
    if root.tag == "testsuite":
        return [root]
    if root.tag == "testsuites":
        return root.findall("testsuite")
    return []


def _tc_status(tc: ET.Element) -> str:
    if tc.find("failure") is not None or tc.find("error") is not None:
        return "FAIL"
    if tc.find("skipped") is not None:
        return "SKIPPED"
    return "PASS"


def collect_slow_tests(testsuites: list[ET.Element], threshold: float) -> list[dict]:
    slow = []
    for testsuite in testsuites:
        for tc in testsuite.iter("testcase"):
            name = tc.get("name", "")
            classname = tc.get("classname", "")
            time_str = tc.get("time", "")
            try:
                elapsed = float(time_str)
            except (ValueError, TypeError):
                continue
            if elapsed >= threshold:
                slow.append(
                    {
                        "name": name,
                        "classname": classname,
                        "elapsed": elapsed,
                        "status": _tc_status(tc),
                    }
                )
    return sorted(slow, key=lambda x: x["elapsed"], reverse=True)


def collect_target_tests(testsuites: list[ET.Element]) -> list[dict]:
    results = []
    for testsuite in testsuites:
        for tc in testsuite.iter("testcase"):
            name = tc.get("name", "")
            if any(t in name for t in TARGET_TESTS):
                time_str = tc.get("time", "")
                try:
                    elapsed = float(time_str)
                except (ValueError, TypeError):
                    elapsed = 0.0
                results.append(
                    {
                        "name": name,
                        "elapsed": elapsed,
                        "status": _tc_status(tc),
                    }
                )
    return sorted(results, key=lambda x: x["elapsed"], reverse=True)


def format_report(slow: list[dict], targets: list[dict], threshold: float) -> str:
    lines = [
        "=" * 60,
        "  TCK Performance Report",
        "=" * 60,
    ]

    if targets:
        lines += [
            "",
            "  Tracked use-case tests:",
            f"  {'Test':<45} {'Time':>8}  Status",
            "  " + "-" * 56,
        ]
        for t in targets:
            short = t["name"][-43:] if len(t["name"]) > 43 else t["name"]
            lines.append(f"  {short:<45} {t['elapsed']:>7.1f}s  {t['status']}")

    if slow:
        lines += [
            "",
            f"  Tests exceeding {threshold:.1f}s threshold:",
            f"  {'Test':<45} {'Time':>8}  Status",
            "  " + "-" * 56,
        ]
        for t in slow[:20]:
            short = t["name"][-43:] if len(t["name"]) > 43 else t["name"]
            lines.append(f"  {short:<45} {t['elapsed']:>7.1f}s  {t['status']}")
        if len(slow) > 20:
            lines.append(f"  ... and {len(slow) - 20} more")
    else:
        lines += [
            "",
            f"  No tests exceeded the {threshold:.1f}s threshold.",
        ]

    lines += [
        "",
        "=" * 60,
        "",
        "Note: Performance data is tracked for trend analysis.",
        "This step does not fail CI -- it is informational only.",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "xml_path",
        nargs="?",
        default="test-results-tck.xml",
        help="JUnit XML file path",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help=f"Report tests slower than this (default: {DEFAULT_THRESHOLD}s)",
    )
    args = parser.parse_args()

    xml_path = Path(args.xml_path)
    if not xml_path.exists():
        print(
            f"Warning: {xml_path} not found -- no performance results to report.",
            file=sys.stderr,
        )
        return

    try:
        tree = ET.parse(xml_path)
    except ET.ParseError as exc:
        print(f"Warning: could not parse {xml_path}: {exc}", file=sys.stderr)
        return

    testsuites = find_testsuites(tree)
    if not testsuites:
        print(f"Warning: no <testsuite> element in {xml_path}", file=sys.stderr)
        return

    slow = collect_slow_tests(testsuites, args.threshold)
    targets = collect_target_tests(testsuites)
    report = format_report(slow, targets, args.threshold)
    print(report)


if __name__ == "__main__":
    main()
