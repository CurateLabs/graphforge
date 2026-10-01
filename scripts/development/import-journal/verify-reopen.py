#!/usr/bin/env python3
"""Save fresh-process S22 reopen/query proof outside the measured workflow."""

import argparse
import json
import os
from pathlib import Path
import subprocess

from measurement_contract import (
    digest,
    read_json,
    require_external_output,
    validate_qualification,
    write_json,
)

parser = argparse.ArgumentParser()
parser.add_argument("--lane", required=True, choices=("baseline", "candidate"))
parser.add_argument("--pair", required=True, type=int)
parser.add_argument("--binary", required=True, type=Path)
parser.add_argument("--worktree", required=True, type=Path)
parser.add_argument("--evidence-root", required=True, type=Path)
args = parser.parse_args()
root = args.evidence_root.resolve() / f"pair-{args.pair}" / args.lane
require_external_output(root, args.worktree, Path(__file__).resolve().parent)
proof_root = root / "reopen"
proof_root.mkdir()
proof = {
    "contract": "graphforge-import-reopen-proof/1",
    "lane": args.lane,
    "pair": args.pair,
    "completed": False,
    "verified": False,
    "query_exit_code": None,
    "artifact_sha256": {},
    "method_sha256": digest(Path(__file__)),
}


def run():
    import pyarrow.parquet as pq

    identity = read_json(root / "identity.json")
    assert identity["lane"] == args.lane and identity["pair"] == args.pair
    validate_qualification(root, identity)
    assert digest(args.binary) == identity["binary_sha256"], "proof binary differs from lane"
    source = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=args.worktree, text=True
    ).strip()
    assert source == identity["source_sha"], "proof source differs from lane"
    subprocess.run(["git", "diff", "--quiet", "HEAD"], cwd=args.worktree, check=True)
    command = [str(args.binary.resolve()), "--json", "--project", str(root / "project"), "query"]
    queries = (
        ("MATCH (n) RETURN count(n) AS node_count", "nodes.parquet"),
        ("MATCH ()-[r]->() RETURN count(r) AS edge_count", "edges.parquet"),
        ("MATCH (n) RETURN n LIMIT 1", "sample.parquet"),
    )
    for query, name in queries:
        command.extend(("--cypher", query, "--output", str(proof_root / name)))
    proof.update(
        command=command,
        source_sha=source,
        binary_sha256=identity["binary_sha256"],
        input_manifest_sha256=digest(root / "inputs.sha256"),
        qualification_sha256=digest(root / "qualification.json"),
    )
    environment = os.environ.copy()
    environment["TMPDIR"] = str(root / "tmp")
    proof["temporary_directory"] = environment["TMPDIR"]
    with (
        (proof_root / "query.jsonl").open("w") as output,
        (proof_root / "query.stderr").open("w") as error,
    ):
        result = subprocess.run(
            command,
            cwd=proof_root,
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=error,
            env=environment,
            check=False,
        )
    proof.update(completed=True, query_exit_code=result.returncode)
    assert result.returncode == 0, f"fresh query process exited {result.returncode}"
    counts = {}
    for filename, column, expected in (
        ("nodes.parquet", "node_count", 4194304),
        ("edges.parquet", "edge_count", 67108864),
    ):
        table = pq.read_table(proof_root / filename)
        assert table.num_rows == 1 and table.column_names == [column], "invalid count result shape"
        values = table.column(column).to_pylist()
        assert len(values) == 1 and type(values[0]) is int and values[0] == expected, (
            "published count mismatch",
            column,
            values,
            expected,
        )
        counts[column] = values[0]
    sample = pq.read_table(proof_root / "sample.parquet")
    assert sample.num_rows == 1, "bounded non-count query must return one row"
    proof["counts"] = counts
    proof["sample_rows"] = sample.num_rows
    assert digest(args.binary) == identity["binary_sha256"], "proof binary changed"
    assert (
        source
        == subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=args.worktree, text=True
        ).strip()
    )
    subprocess.run(["git", "diff", "--quiet", "HEAD"], cwd=args.worktree, check=True)
    proof["verified"] = True


try:
    run()
except BaseException as error:
    proof["failure"] = str(error)
    raise
finally:
    proof["artifact_sha256"] = {
        path.name: digest(path) for path in proof_root.iterdir() if path.is_file()
    }
    write_json(proof_root / "proof.json", proof)
print(json.dumps(proof))
