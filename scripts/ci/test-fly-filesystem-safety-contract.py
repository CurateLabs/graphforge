#!/usr/bin/env python3
"""Static and mutation tests for the disposable Fly qualification boundary."""

from __future__ import annotations

import json
from pathlib import Path
import re
import shutil
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
CONTROLLER = Path("scripts/fly-filesystem-qualification.py")
VALIDATOR = Path("scripts/ci/validate-fly-filesystem-qualification.py")
SCHEMA = Path("docs/development/evidence/fly-filesystem-qualification.schema.json")
DOCKERFILE = Path("containers/fly-filesystem-qualification/Dockerfile")
ENTRYPOINT = Path("containers/fly-filesystem-qualification/run-smoke.sh")

PUBLIC_SURFACE = re.compile(
    r"(?:\bEXPOSE\b|\bhttp_service\b|\[\[services\]\]|--(?:port|ports)\b|"
    r"\bfly(?:ctl)?\s+(?:deploy|launch|ips)\b|\b0\.0\.0\.0\b|"
    r"allocate-(?:v4|v6)|public[_ -]?ip)",
    re.IGNORECASE,
)
FORBIDDEN_EVIDENCE_KEY = re.compile(
    r"(?:app|machine|volume|runner|resource|host|absolute)_(?:id|path|name)$|"
    r"(?:secret|credential|token|password)",
    re.IGNORECASE,
)


class ContractError(AssertionError):
    pass


def read(root: Path, relative: Path) -> str:
    path = root / relative
    if not path.is_file():
        raise ContractError(f"missing Fly qualification contract file: {relative}")
    return path.read_text(encoding="utf-8")


def property_names(schema: object) -> list[str]:
    if isinstance(schema, dict):
        names = list(schema.get("properties", {}))
        for value in schema.values():
            names.extend(property_names(value))
        return names
    if isinstance(schema, list):
        return [name for value in schema for name in property_names(value)]
    return []


def validate_contract(root: Path) -> None:
    controller = read(root, CONTROLLER)
    validator = read(root, VALIDATOR)
    dockerfile = read(root, DOCKERFILE)
    entrypoint = read(root, ENTRYPOINT)

    combined_runtime = "\n".join((controller, dockerfile, entrypoint))
    match = PUBLIC_SURFACE.search(combined_runtime)
    if match:
        raise ContractError(f"Fly qualification may not expose a public service: {match.group(0)}")

    if "machine" not in controller or "run" not in controller:
        raise ContractError("controller must create the disposable Fly Machine explicitly")
    if "@" not in controller or "sha256:" not in controller or "{64}" not in controller:
        raise ContractError("controller must require a full immutable image digest")
    if "--restart" not in controller or "--rm" not in controller:
        raise ContractError("machine must be non-restarting and disposable")

    if "finally" not in controller:
        raise ContractError("cleanup must be protected by a finally block")
    for resource in ("machine", "volumes", "apps"):
        if f'["{resource}", "destroy"' not in controller:
            raise ContractError(f"cleanup must destroy the disposable Fly {resource}")

    try:
        schema = json.loads(read(root, SCHEMA))
    except json.JSONDecodeError as error:
        raise ContractError(f"invalid qualification evidence schema: {error}") from error
    if schema.get("additionalProperties") is not False:
        raise ContractError("evidence root must reject additional properties")
    unsafe = sorted(
        {name for name in property_names(schema) if FORBIDDEN_EVIDENCE_KEY.search(name)}
    )
    if unsafe:
        raise ContractError("evidence schema exposes resource IDs or paths: " + ", ".join(unsafe))
    if "additionalProperties" not in read(root, SCHEMA):
        raise ContractError("evidence schema must be closed")

    for marker in ("absolute", "id", "path", "secret", "token"):
        if marker not in validator.lower():
            raise ContractError(f"validator must fail closed on evidence {marker} leakage")


class FlySafetyContractTests(unittest.TestCase):
    def test_repository_contract(self) -> None:
        validate_contract(ROOT)

    def test_mutations_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory)
            for relative in (CONTROLLER, VALIDATOR, SCHEMA, DOCKERFILE, ENTRYPOINT):
                destination = fixture / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT / relative, destination)

            mutations = {
                "public_port": (ENTRYPOINT, "\npython3 -m http.server 8080 --bind 0.0.0.0\n"),
                "mutable_image": (CONTROLLER, "sha256:"),
                "missing_cleanup": (CONTROLLER, "finally"),
                "missing_machine_cleanup": (CONTROLLER, 'fly.run(["machine", "destroy"'),
                "missing_volume_cleanup": (CONTROLLER, 'fly.run(["volumes", "destroy"'),
                "missing_app_cleanup": (CONTROLLER, 'fly.run(["apps", "destroy"'),
                "leaked_id": (
                    SCHEMA,
                    '"properties": {',
                    '"properties": {"machine_id": {"type": "string"},',
                ),
            }
            for name, mutation in mutations.items():
                with self.subTest(name=name):
                    target = fixture / mutation[0]
                    original = target.read_text(encoding="utf-8")
                    if name == "public_port":
                        target.write_text(original + mutation[1], encoding="utf-8")
                    elif name == "leaked_id":
                        target.write_text(
                            original.replace(mutation[1], mutation[2], 1), encoding="utf-8"
                        )
                    else:
                        self.assertIn(mutation[1], original)
                        target.write_text(original.replace(mutation[1], ""), encoding="utf-8")
                    with self.assertRaises(ContractError):
                        validate_contract(fixture)
                    target.write_text(original, encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
