#!/usr/bin/env python3
"""Run the real scale-1 lifecycle through the public certification binaries."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

import jsonschema


def executable(value: str) -> Path:
    path = Path(value).resolve(strict=True)
    if not path.is_file() or not os.access(path, os.X_OK):
        raise SystemExit(f"not an executable file: {path}")
    return path


def decoded_objects(text: str) -> list[dict[str, object]]:
    objects = []
    for line in text.splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            objects.append(value)
    return objects


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gf", required=True, type=executable)
    parser.add_argument("--certify", required=True, type=executable)
    parser.add_argument("--generator", required=True, type=executable)
    parser.add_argument("--workspace-root", required=True, type=Path)
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    workspace_root = args.workspace_root.resolve(strict=True)
    filesystem = subprocess.run(
        ["stat", "-f", "-c", "%T", workspace_root],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if filesystem not in {"ext2/ext3", "ext2/ext3/ext4", "xfs", "btrfs"}:
        raise SystemExit(f"native durable filesystem required, found {filesystem}")

    environment = os.environ.copy()
    environment["PATH"] = os.pathsep.join(
        [
            str(args.gf.parent),
            str(args.certify.parent),
            str(args.generator.parent),
            environment["PATH"],
        ]
    )
    with tempfile.TemporaryDirectory(prefix="gf-tiny-lifecycle-", dir=workspace_root) as directory:
        work = Path(directory)
        evidence_path = work / "evidence.json"
        completed = subprocess.run(
            [
                str(args.certify),
                "run",
                str(root / "fixtures/progressive/tiny-executable.json"),
                str(evidence_path),
            ],
            cwd=work,
            env=environment,
            check=False,
        )
        if completed.returncode != 0:
            evidence = json.loads(evidence_path.read_text()) if evidence_path.is_file() else {}
            failed_phase = evidence.get("failed_phase")
            diagnostic: dict[str, object] = {
                "failed_phase": failed_phase,
                "returncode": completed.returncode,
                "evidence_present": evidence_path.is_file(),
            }
            if failed_phase == "clean_import":
                profile = json.loads(
                    (root / "fixtures/progressive/tiny-executable.json").read_text()
                )
                command = profile["phases"][8]["action"]["args"]
                replay = subprocess.run(
                    [str(args.gf), *command],
                    cwd=work,
                    env=environment,
                    check=False,
                    capture_output=True,
                    text=True,
                )
                ordinary = []
                for value in decoded_objects(replay.stdout):
                    ordinary.append(
                        {
                            "contract": value.get("contract"),
                            "has_transient_peak": "transient_peak_allocated_bytes" in value,
                            "transient_peak_allocated_bytes": value.get(
                                "transient_peak_allocated_bytes"
                            ),
                        }
                    )
                diagnostic["clean_import_replay"] = {
                    "returncode": replay.returncode,
                    "receipts": ordinary,
                    "error": [
                        {
                            "code": value.get("error", {}).get("code"),
                            "kind": value.get("error", {}).get("details", {}).get("kind"),
                            "semantic_code": value.get("error", {})
                            .get("details", {})
                            .get("semantic_code"),
                            "diagnostics": [
                                {
                                    "code": item.get("code"),
                                    "message": item.get("message"),
                                }
                                for item in value.get("error", {}).get("diagnostics", [])
                                if isinstance(item, dict)
                            ],
                        }
                        for value in decoded_objects(replay.stderr)
                    ],
                }
            print(json.dumps({"tiny_lifecycle_failure": diagnostic}, sort_keys=True))
            raise SystemExit("real tiny lifecycle certification failed")
        evidence = json.loads(evidence_path.read_text())
        schema = json.loads((root / "schemas/certification-evidence.json").read_text())
        jsonschema.Draft202012Validator(schema).validate(evidence)
        if evidence["status"] != "passed" or len(evidence["phases"]) != 10:
            raise SystemExit("tiny lifecycle did not assemble complete passed evidence")
        queries = {
            phase["phase"]: [
                receipt
                for receipt in phase.get("receipts", [])
                if receipt.get("contract") == "graphforge-result-sink/2"
            ]
            for phase in evidence["phases"]
        }
        if [len(queries[name]) for name in ("recount", "query", "reopen_proof")] != [2, 2, 4]:
            raise SystemExit("tiny lifecycle omitted ordinary source/imported query receipts")
        for index, expected in enumerate((2, 32)):
            if (
                queries["recount"][index].get("scalar_u64") != expected
                or queries["reopen_proof"][index].get("scalar_u64") != expected
            ):
                raise SystemExit("tiny stored/imported counts differ from SCALE1 raw input")
        for source, imported in zip(
            queries["recount"] + queries["query"], queries["reopen_proof"], strict=True
        ):
            if (
                source.get("complete") is not True
                or imported.get("complete") is not True
                or not source.get("result_sha256")
                or source.get("result_sha256") != imported.get("result_sha256")
            ):
                raise SystemExit("tiny source/imported result fingerprints differ")
        receipts = [
            (phase["phase"], receipt)
            for phase in evidence["phases"]
            for receipt in phase.get("receipts", [])
            if receipt.get("contract") == "graphforge-lifecycle-storage/1"
        ]
        if len(receipts) != 1 or receipts[0][0] != "reopen_proof":
            raise SystemExit("expected exactly one lifecycle receipt at reopen_proof")
        receipt = receipts[0][1]
        retained = receipt.get("retained_storage_bytes")
        peak = receipt.get("transient_peak_storage_bytes")
        if (
            not isinstance(retained, int)
            or retained <= 0
            or not isinstance(peak, int)
            or peak < retained
        ):
            raise SystemExit("lifecycle receipt is not a closed allocation high-water")


if __name__ == "__main__":
    main()
