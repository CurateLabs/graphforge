#!/usr/bin/env python3
"""Validate the closed, sanitized Fly S20 evidence contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
from typing import Any

import jsonschema

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = ROOT / "docs/development/evidence/fly-g500-s20.schema.json"
SENSITIVE_KEY = re.compile(r"(?:token|secret|machine[_-]?id|volume[_-]?id)", re.I)
ABSOLUTE_PATH = re.compile(r"^(?:/|\\\\|\\[^\\]|[A-Za-z]:\\)")


class EvidenceError(RuntimeError):
    pass


def validate(value: Any, sha: str, digest: str, region: str) -> None:
    try:
        jsonschema.validate(
            value, json.loads(SCHEMA.read_text()), format_checker=jsonschema.FormatChecker()
        )
    except jsonschema.ValidationError as error:
        raise EvidenceError(f"schema violation: {error.message}") from None
    if value["git_sha"] != sha or value["image_digest"] != digest or value["region"] != region:
        raise EvidenceError("evidence identity differs from the pinned run")
    counts = value["counts"]
    if (
        value["result"] == "passed"
        and len({counts["generated_edges"], counts["source_edges"], counts["imported_edges"]}) != 1
    ):
        raise EvidenceError(
            "passing evidence must reconcile generated, source, and imported counts"
        )
    if counts["raw_attempts"] != (
        counts["generated_edges"] + counts["self_loops_rejected"] + counts["duplicates_rejected"]
    ):
        raise EvidenceError("generator attempt accounting does not reconcile")
    lifecycle = value["lifecycle"]
    if lifecycle["source_nodes"] != lifecycle["imported_nodes"]:
        raise EvidenceError("source/import node counts differ")
    for suffix in ("one_hop", "two_hop"):
        if (
            lifecycle[f"source_{suffix}"]["fingerprint"]
            != lifecycle[f"imported_{suffix}"]["fingerprint"]
        ):
            raise EvidenceError(f"source/import {suffix} fingerprints differ")
    if lifecycle["source_authority_fingerprint"] != lifecycle["imported_authority_fingerprint"]:
        raise EvidenceError("source/import authority fingerprints differ")
    for name in ("source_storage", "imported_storage"):
        attribution = lifecycle[name]
        fields = (
            "logical_references",
            "logical_bytes",
            "physical_objects",
            "physical_logical_bytes",
            "allocated_bytes",
        )
        for field in fields:
            observed = sum(category[field] for category in attribution["categories"].values())
            if observed != attribution[field]:
                raise EvidenceError(f"{name} category {field} does not reconcile")
        if any(attribution["categories"]["other"][field] for field in fields):
            raise EvidenceError(f"{name} contains unclassified artifacts")
    if value["storage"]["peak_allocated_bytes"] > value["storage"]["capacity_bytes"]:
        raise EvidenceError("allocated storage exceeds volume capacity")
    for phase, memory in value["phase_memory"].items():
        if memory["rss_peak_bytes"] == 0 or memory["hwm_bytes"] < memory["rss_peak_bytes"]:
            raise EvidenceError(f"phase {phase} lacks truthful RSS/HWM sampling")
        if memory["anonymous_peak_bytes"] + memory["file_peak_bytes"] < memory["rss_peak_bytes"]:
            raise EvidenceError(f"phase {phase} memory categories understate RSS")

    def reject_sensitive(item: Any) -> None:
        if isinstance(item, dict):
            for key, child in item.items():
                if SENSITIVE_KEY.search(key):
                    raise EvidenceError("evidence contains a forbidden identifier or credential")
                reject_sensitive(child)
        elif isinstance(item, list):
            for child in item:
                reject_sensitive(child)
        elif isinstance(item, str) and ABSOLUTE_PATH.match(item):
            raise EvidenceError("evidence contains an absolute path")

    reject_sensitive(value)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence", type=Path)
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--expected-image-digest", required=True)
    parser.add_argument("--expected-region", required=True)
    args = parser.parse_args()
    try:
        validate(
            json.loads(args.evidence.read_text()),
            args.expected_sha,
            args.expected_image_digest,
            args.expected_region,
        )
    except (OSError, json.JSONDecodeError, EvidenceError) as error:
        print(f"error: {error}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
