#!/usr/bin/env python3
"""Run the real scale-1 lifecycle through the public certification binaries."""

from __future__ import annotations

import argparse
import importlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
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


def storage_totals(root: Path) -> dict[str, int]:
    """Independent regular-file oracle, used only after every child has exited.

    Directories are not data objects. Hardlinked references count once by the
    host's device/inode identity; allocation comes from stat blocks, not EOF.
    This final inventory cannot establish an operation's historical peak.
    """
    identities: dict[tuple[int, int], tuple[int, int]] = {}
    references = logical_bytes = 0
    if not stat.S_ISDIR(root.lstat().st_mode):
        raise SystemExit("allocation oracle requires a non-linked directory")
    for directory, directories, files in os.walk(root, followlinks=False):
        for name in directories:
            if not stat.S_ISDIR((Path(directory) / name).lstat().st_mode):
                raise SystemExit("allocation oracle encountered a linked directory")
        for name in files:
            path = Path(directory) / name
            before = path.lstat()
            if not stat.S_ISREG(before.st_mode):
                raise SystemExit("allocation oracle encountered a non-regular file")
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                observed = os.fstat(descriptor)
                after = path.lstat()
            finally:
                os.close(descriptor)
            facts = [
                (
                    value.st_dev,
                    value.st_ino,
                    value.st_size,
                    value.st_blocks,
                    value.st_mtime_ns,
                    value.st_ctime_ns,
                )
                for value in (before, observed, after)
            ]
            if facts[0] != facts[1] or facts[1] != facts[2]:
                raise SystemExit("allocation oracle requires a quiescent inventory")
            identity = (observed.st_dev, observed.st_ino)
            physical = (observed.st_size, observed.st_blocks * 512)
            if identity in identities and identities[identity] != physical:
                raise SystemExit("allocation oracle found inconsistent alias facts")
            identities[identity] = physical
            references += 1
            logical_bytes += observed.st_size
    return {
        "logical_references": references,
        "logical_bytes": logical_bytes,
        "physical_objects": len(identities),
        "physical_logical_bytes": sum(logical for logical, _ in identities.values()),
        "allocated_bytes": sum(allocated for _, allocated in identities.values()),
    }


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
        # Native refresh materializations must share the admitted project volume.
        runtime_tmp = work / "runtime-tmp"
        runtime_tmp.mkdir()
        environment["TMPDIR"] = str(runtime_tmp)
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
        snapshots = [
            receipt["storage"]
            for phase in evidence["phases"]
            for receipt in phase.get("receipts", [])
            if receipt.get("contract") == "graphforge-storage-attribution-command/1"
        ]
        if len(snapshots) != 2 or any(
            not 0 < snapshot["physical_objects"] < snapshot["logical_references"]
            for snapshot in snapshots
        ):
            raise SystemExit(
                "tiny source/imported snapshots did not prove shared-object deduplication"
            )
        receipts = [
            (phase["phase"], receipt)
            for phase in evidence["phases"]
            for receipt in phase.get("receipts", [])
            if receipt.get("contract") == "graphforge-lifecycle-storage/2"
        ]
        if len(receipts) != 1 or receipts[0][0] != "reopen_proof":
            raise SystemExit("expected exactly one lifecycle receipt at reopen_proof")
        receipt = receipts[0][1]
        sys.path.insert(0, str(root / "harness"))
        consumer = importlib.import_module("graphforge_bench.progressive_run")
        consumer.validate_lifecycle_storage_receipt(receipt)
        retained = receipt.get("retained_storage_bytes")
        peak = receipt.get("transient_peak_storage_bytes")
        if (
            not isinstance(retained, int)
            or retained <= 0
            or not isinstance(peak, int)
            or peak < retained
        ):
            raise SystemExit("lifecycle receipt is not a closed allocation high-water")
        workspace = work / "workspace" / "tiny"
        measured = storage_totals(workspace)["allocated_bytes"]
        if retained != measured:
            raise SystemExit(
                f"lifecycle retained allocation {retained} "
                f"differs from independent union {measured}"
            )
        for name, private in {
            "source-project-construction": workspace / "source" / ".graphforge-construction",
            "source-project-import": workspace / "source" / "import-sessions",
            "source-project-transactions": workspace / "source" / "transactions",
            "imported-project-transactions": workspace / "imported" / "transactions",
        }.items():
            measured_owner = storage_totals(private)
            if measured_owner["allocated_bytes"] <= 0:
                raise SystemExit("tiny lifecycle omitted live private/control allocation")
            if receipt["retained_owners"][name]["totals"] != measured_owner:
                raise SystemExit(f"lifecycle owner {name} differs from independent raw facts")


if __name__ == "__main__":
    main()
