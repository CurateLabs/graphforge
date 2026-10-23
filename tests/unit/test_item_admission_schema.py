"""Check owner-handle schema verdicts against the native admission corpus."""

import json
from pathlib import Path

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "docs/reference/hub-publish/v1"


def test_owner_handle_schema_matches_native_admission() -> None:
    """Preserve native handle acceptance, including absolute end and dot rules."""
    schema = json.loads((ARTIFACTS / "item-admission.schema.json").read_text())
    Draft202012Validator.check_schema(schema)
    variants = schema["$defs"]["fact"]["properties"]["claim"]["oneOf"]
    handle_schema = next(
        variant["properties"]["owner_handle"]
        for variant in variants
        if "owner_handle" in variant["properties"]
    )
    validator = Draft202012Validator(handle_schema)
    corpus = json.loads((ARTIFACTS / "item-admission-conformance.json").read_text())
    cases = [case for case in corpus["cases"] if case["name"].startswith("owner-handle-")]
    assert cases, "native handle regression cases are required"
    for case in cases:
        handle = case["input"]["facts"][0]["claim"]["owner_handle"]
        assert validator.is_valid(handle) == ("plan" in case["expected"]), case["name"]
