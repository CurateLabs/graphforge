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
    if value["result"] == "passed" and len(set(counts.values())) != 1:
        raise EvidenceError(
            "passing evidence must reconcile generated, source, and imported counts"
        )
    if value["storage"]["allocated_bytes"] > value["storage"]["capacity_bytes"]:
        raise EvidenceError("allocated storage exceeds volume capacity")

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
