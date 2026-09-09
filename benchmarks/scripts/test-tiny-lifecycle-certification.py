#!/usr/bin/env python3
"""Run tiny and ownership-growth lifecycles through public certification binaries."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import itertools
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


def storage_totals(root: Path, selected: set[Path] | None = None) -> dict[str, int]:
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
            if selected is not None and path not in selected:
                continue
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


OWNER_SUFFIXES = ("construction", "import", "transactions", "locks", "admission_lock", "published")
OWNER_NAMES = {
    f"{project}-project-{suffix}" for project in ("source", "imported") for suffix in OWNER_SUFFIXES
} | {"generated-inputs", "query-results", "portable-package"}


def owner_inventories(workspace: Path) -> dict[str, set[Path]]:
    """Classify every quiescent route independently of the producer's owner map."""
    owners = {name: set() for name in OWNER_NAMES}
    admissions = {}
    for project in ("source", "imported"):
        digest = hashlib.sha256(
            b"graphforge-project-lifecycle-lock/v1\0"
            + os.fsencode(workspace)
            + b"\0"
            + os.fsencode(project)
        ).hexdigest()
        admissions[f".graphforge-admission-{digest}.lock"] = f"{project}-project-admission_lock"
    for directory, directories, files in os.walk(workspace, followlinks=False):
        for name in directories:
            if not stat.S_ISDIR((Path(directory) / name).lstat().st_mode):
                raise SystemExit("owner oracle encountered linked directory")
        for name in files:
            path = Path(directory) / name
            parts = path.relative_to(workspace).parts
            if len(parts) == 1 and name in admissions:
                owner = admissions[name]
            elif len(parts) == 1 and name in {"nodes.parquet", "edges.parquet"}:
                owner = "generated-inputs"
            elif len(parts) == 1 and name in {
                "node-count.arrow",
                "edge-count.arrow",
                "one-hop.arrow",
                "two-hop.arrow",
                "imported-node-count.arrow",
                "imported-edge-count.arrow",
                "imported-one-hop.arrow",
                "imported-two-hop.arrow",
            }:
                owner = "query-results"
            elif parts[0] == "portable":
                owner = "portable-package"
            elif parts[0] in {"source", "imported"} and len(parts) > 1:
                suffix = {
                    ".graphforge-construction": "construction",
                    "import-sessions": "import",
                    "transactions": "transactions",
                    "locks": "locks",
                }.get(parts[1])
                if parts[1] in {"FORMAT", "CURRENT", "generations", "graph-objects"}:
                    suffix = (
                        "locks"
                        if len(parts) == 4
                        and parts[1] == "generations"
                        and parts[3] == "lease.lock"
                        else "published"
                    )
                if suffix is None:
                    raise SystemExit("unclassified project owner route")
                owner = f"{parts[0]}-project-{suffix}"
            else:
                raise SystemExit("unclassified workspace owner route")
            owners[owner].add(path)
    return owners


DATA_OWNERS = {
    "generated-inputs",
    "query-results",
    "portable-package",
    "source-project-construction",
    "source-project-import",
    "source-project-published",
    "imported-project-published",
}
ZERO_OWNERS = {"imported-project-construction", "imported-project-import"}
BYTE_FIELDS = ("logical_bytes", "physical_logical_bytes", "allocated_bytes")


def positive_slopes(name: str, values: list[int], work: list[int]) -> None:
    deltas = [values[i + 1] - values[i] for i in (0, 1)]
    if min(deltas) <= 0:
        raise ValueError(f"{name}: data growth is flat or decreasing")
    left = deltas[0] * (work[2] - work[1])
    right = deltas[1] * (work[1] - work[0])
    if left > 2 * right or right > 2 * left:
        raise ValueError(f"{name}: adjacent normalized slopes differ by more than factor2")


def normalized_ceiling(
    name: str, values: list[int], work: list[int], *, nondecreasing: bool = True
) -> None:
    if any(value <= 0 for value in values):
        raise ValueError(f"{name}: positive evidence required")
    if nondecreasing and any(b < a for a, b in itertools.pairwise(values)):
        raise ValueError(f"{name}: nondecreasing evidence required")
    if any(values[i] * work[0] > 2 * values[0] * work[i] for i in (1, 2)):
        raise ValueError(f"{name}: exceeds existing factor2 normalized ceiling")


def validate_growth(observations: list[dict[str, object]]) -> None:
    """Existing factor2 ceiling, plus strict EOF slopes; no output-fitted constants.

    Allocated owner bytes may plateau due to per-file allocation quanta. They
    retain the existing quantized normalized ceiling, not an EOF upper bound.
    Fixed empty locks and absent owners are checked separately. Complete current
    and peak must actually grow, using the same adjacent slope bound as EOF.
    """
    if [o["scale"] for o in observations] != [6, 7, 8]:
        raise ValueError("growth requires exactly scale6/7/8")
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "harness"))
    consumer = importlib.import_module("graphforge_bench.progressive_run")
    work = []
    for o in observations:
        try:
            consumer.validate_lifecycle_storage_receipt(o["receipt"])
        except consumer.ControllerError as error:
            raise ValueError(str(error)) from error
        n, e = o["live_nodes"], o["live_edges"]
        if n != 1 << o["scale"] or e != 16 * n:
            raise ValueError("invalid authoritative live denominator")
        if o["query_rows"] != [n, e] or o["imported_query_rows"] != [n, e]:
            raise ValueError("growth query output was truncated or duplicated")
        work.append(n + e)
        ratios = o["ratios"]
        for field in ("retained_storage_bytes", "transient_peak_storage_bytes"):
            if ratios[field] != {
                "per_live_node": [o["receipt"][field], n],
                "per_live_edge": [o["receipt"][field], e],
            }:
                raise ValueError("fabricated normalized ratio")
        owners = o["receipt"]["retained_owners"]
        if set(owners) != OWNER_NAMES:
            raise ValueError("missing or unknown growth owner")
    for owner in sorted(OWNER_NAMES):
        totals = [o["receipt"]["retained_owners"][owner]["totals"] for o in observations]
        if any(
            set(t)
            != {
                "logical_references",
                "logical_bytes",
                "physical_objects",
                "physical_logical_bytes",
                "allocated_bytes",
            }
            for t in totals
        ):
            raise ValueError("missing owner metric")
        for field in totals[0]:
            values = [t[field] for t in totals]
            if any(type(v) is not int or v < 0 for v in values):
                raise ValueError("invalid owner numerator")
            if owner in ZERO_OWNERS:
                if any(values):
                    raise ValueError(f"{owner}: absent owner became nonzero")
            elif owner.endswith(("-locks", "-admission_lock")):
                if field in BYTE_FIELDS and any(values):
                    raise ValueError(f"{owner}: empty lock gained payload")
                if len(set(values)) != 1:
                    raise ValueError(f"{owner}: fixed lock inventory changed")
            elif owner in DATA_OWNERS:
                # Published inventories include content-addressed radix nodes. Their
                # count depends on hashed path prefixes, not graph row count; a
                # larger graph can need fewer nodes. Each run still reconciles
                # every physical file independently before this growth check.
                published_count = owner in {
                    "source-project-published",
                    "imported-project-published",
                } and field in {"logical_references", "physical_objects"}
                normalized_ceiling(
                    f"{owner}.{field}", values, work, nondecreasing=not published_count
                )
                if field in ("logical_bytes", "physical_logical_bytes"):
                    positive_slopes(f"{owner}.{field}", values, work)
            # Transaction journals have fixed protocol shape and fixed-width
            # UUID/digest fields; no graph payload or numeric row inventory.
            elif len(set(values)) != 1:
                raise ValueError(f"{owner}: fixed transaction protocol changed")
    for field in ("retained_storage_bytes", "transient_peak_storage_bytes"):
        values = [o["receipt"][field] for o in observations]
        normalized_ceiling(field, values, work)
        positive_slopes(field, values, work)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gf", required=True, type=executable)
    parser.add_argument("--certify", required=True, type=executable)
    parser.add_argument("--generator", required=True, type=executable)
    parser.add_argument("--workspace-root", required=True, type=Path)
    parser.add_argument("--growth", action="store_true")
    args = parser.parse_args()

    if args.growth:
        observations = [run(args, scale) for scale in (6, 7, 8)]
        validate_growth(observations)
        print(json.dumps({"lifecycle_growth": observations}, sort_keys=True))
    else:
        run(args, 1)


def run(args: argparse.Namespace, scale: int) -> dict[str, object]:
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
        profile = json.loads((root / "fixtures/progressive/tiny-executable.json").read_text())
        generator_args = profile["phases"][1]["action"]["args"]
        generator_args[generator_args.index("--scale") + 1] = str(scale)
        if args.growth:
            payloads = [
                "MATCH (n) RETURN n.node_uuid AS id ORDER BY id",
                "MATCH ()-[r]->() RETURN r.edge_uuid AS id ORDER BY id",
            ]
            for phase_name, indices in (("query", (0, 1)), ("reopen_proof", (2, 3))):
                phase = next(item for item in profile["phases"] if item["phase"] == phase_name)
                for index, query in zip(indices, payloads, strict=True):
                    command = phase["action"]["commands"][index]
                    command[command.index("--cypher") + 1] = query
        profile_path = work / "profile.json"
        profile_path.write_text(json.dumps(profile))
        completed = subprocess.run(
            [
                str(args.certify),
                "run",
                str(profile_path),
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
        for index, expected in enumerate((1 << scale, (1 << scale) * 16)):
            if (
                queries["recount"][index].get("scalar_u64") != expected
                or queries["reopen_proof"][index].get("scalar_u64") != expected
            ):
                raise SystemExit("tiny stored/imported counts differ from deterministic raw input")
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
        for name, paths in owner_inventories(workspace).items():
            measured_owner = storage_totals(workspace, paths)
            if receipt["retained_owners"][name]["totals"] != measured_owner:
                raise SystemExit(f"lifecycle owner {name} differs from independent raw facts")
        for name in (
            "source-project-construction",
            "source-project-import",
            "source-project-transactions",
            "imported-project-transactions",
        ):
            if receipt["retained_owners"][name]["totals"]["allocated_bytes"] <= 0:
                raise SystemExit("tiny lifecycle omitted live private/control allocation")

        return {
            "scale": scale,
            "live_nodes": queries["recount"][0]["scalar_u64"],
            "live_edges": queries["recount"][1]["scalar_u64"],
            "query_rows": [q["rows"] for q in queries["query"]],
            "imported_query_rows": [q["rows"] for q in queries["reopen_proof"][2:]],
            "receipt": receipt,
            "allocation_quantum": os.statvfs(workspace).f_frsize,
            "ratios": {
                field: {
                    "per_live_node": [receipt[field], queries["recount"][0]["scalar_u64"]],
                    "per_live_edge": [receipt[field], queries["recount"][1]["scalar_u64"]],
                }
                for field in ("retained_storage_bytes", "transient_peak_storage_bytes")
            },
        }


if __name__ == "__main__":
    main()
