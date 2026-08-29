"""Fail-closed validator for authoritative native-admission evidence."""

from __future__ import annotations

import json
from pathlib import Path
import sys

from jsonschema import Draft202012Validator


def validate(evidence: Path, schema: Path) -> None:
    document = json.loads(evidence.read_text(encoding="utf-8"))
    contract = json.loads(schema.read_text(encoding="utf-8"))
    Draft202012Validator(contract).validate(document)
    if document["result"] != "passed":
        raise ValueError(f"native admission was {document['result']}")


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: validate_local_admission EVIDENCE SCHEMA")
    validate(Path(sys.argv[1]), Path(sys.argv[2]))


if __name__ == "__main__":
    main()
