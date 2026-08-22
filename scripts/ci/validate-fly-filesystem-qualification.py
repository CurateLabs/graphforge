#!/usr/bin/env python3
"""Validate the small, sanitized Fly filesystem qualification artifact."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
from typing import Any

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = ROOT / "docs/development/evidence/fly-filesystem-qualification.schema.json"
FORBIDDEN_KEY = re.compile(r"(?:^|_)(?:id|token|secret|credential|password|path|name)(?:$|_)", re.I)
ABSOLUTE_PATH = re.compile(r"^(?:/|\\|[A-Za-z]:[\\/])")


class EvidenceError(ValueError):
    pass


def reject_sensitive(value: Any, trail: str = "$") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if FORBIDDEN_KEY.search(key):
                raise EvidenceError(f"forbidden identity, secret, or path field at {trail}.{key}")
            reject_sensitive(child, f"{trail}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            reject_sensitive(child, f"{trail}[{index}]")
    elif isinstance(value, str) and ABSOLUTE_PATH.search(value):
        raise EvidenceError(f"absolute path at {trail}")


def validate(evidence: dict[str, Any], *, sha: str, digest: str, region: str) -> None:
    contract = json.loads(SCHEMA.read_text(encoding="utf-8"))
    errors = sorted(
        Draft202012Validator(contract).iter_errors(evidence), key=lambda e: list(e.path)
    )
    if errors:
        location = ".".join(map(str, errors[0].absolute_path)) or "$"
        raise EvidenceError(f"schema violation at {location}: {errors[0].message}")
    reject_sensitive(evidence)
    if evidence["git_sha"] != sha:
        raise EvidenceError("git_sha does not match the exact source commit")
    if evidence["image_digest"] != digest:
        raise EvidenceError("image_digest does not match the launched image")
    if evidence["region"] != region:
        raise EvidenceError("region does not match the fixed launch region")
    accepted = evidence["admission"]["status"] == "accepted"
    if accepted != (evidence["result"] == "qualified"):
        raise EvidenceError("admission and result disagree")
    if accepted and (
        evidence["admission"]["code"] is not None or evidence["admission"]["cause"] is not None
    ):
        raise EvidenceError("accepted admission cannot contain failure classification")
    if not accepted and (not evidence["admission"]["code"] or not evidence["admission"]["cause"]):
        raise EvidenceError("rejected admission requires typed code and cause")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence", type=Path)
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--expected-image-digest", required=True)
    parser.add_argument("--expected-region", required=True)
    args = parser.parse_args()
    try:
        value = json.loads(args.evidence.read_text(encoding="utf-8"))
        validate(
            value,
            sha=args.expected_sha,
            digest=args.expected_image_digest,
            region=args.expected_region,
        )
    except (OSError, json.JSONDecodeError, EvidenceError) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
