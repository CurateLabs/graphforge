#!/usr/bin/env python3
"""Exercise persisted reopen proof using tiny real Parquet query-result fixtures."""

import contextlib
import io
from pathlib import Path
import subprocess
import sys
import tempfile
from unittest.mock import patch

from measurement_contract import digest, read_json, write_json
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent
METHOD = ROOT / "verify-reopen.py"


def verify_case(case):
    with tempfile.TemporaryDirectory(prefix="gf1624-reopen-fixture-") as temporary:
        root = Path(temporary)
        repo = root / "repo"
        repo.mkdir()
        binary = root / "fixture-binary"
        binary.write_bytes(b"never executed; query subprocess replaced with fixture producer")
        lane = root / "evidence/pair-1/candidate"
        lane.mkdir(parents=True)
        (lane / "tmp").mkdir()
        (lane / "project").mkdir()
        identity = {
            "lane": "candidate",
            "pair": 1,
            "source_sha": "a" * 40,
            "binary_sha256": digest(binary),
        }
        write_json(lane / "identity.json", identity)
        write_json(lane / "qualification.json", {"fixture": True})
        (lane / "inputs.sha256").write_text("fixture input manifest identity\n")
        arguments = [
            str(METHOD),
            "--lane",
            "candidate",
            "--pair",
            "1",
            "--binary",
            str(binary),
            "--worktree",
            str(repo),
            "--evidence-root",
            str(root / "evidence"),
        ]
        calls = []

        def query(command, **kwargs):
            if command[0] == "git":
                return subprocess.CompletedProcess(command, 0)
            calls.append(command)
            assert command[:5] == [
                str(binary),
                "--json",
                "--project",
                str(lane / "project"),
                "query",
            ]
            assert kwargs["cwd"] == lane / "reopen" and kwargs["stdin"] == subprocess.DEVNULL
            assert kwargs["env"]["TMPDIR"] == str(lane / "tmp")
            assert len(command) == 17  # Three bounded result sinks in one fresh child.
            values = [4194304 if case != "count" else 4194303]
            if case == "shape":
                values.append(4194304)
            node_column = "wrong_column" if case == "column" else "node_count"
            output = lane / "reopen"
            pq.write_table(pa.table({node_column: values}), output / "nodes.parquet")
            pq.write_table(pa.table({"edge_count": [67108864]}), output / "edges.parquet")
            pq.write_table(
                pa.table({"node": [] if case == "sample" else ["fixture-node"]}),
                output / "sample.parquet",
            )
            kwargs["stdout"].write('{"fixture": "query receipt"}\n')
            return subprocess.CompletedProcess(command, 7 if case == "exit" else 0)

        with (
            patch.object(sys, "argv", arguments),
            patch("subprocess.check_output", return_value="a" * 40),
            patch("subprocess.run", side_effect=query),
            # Full qualification is independently covered by test-measurement-method.py.
            patch("measurement_contract.validate_qualification", return_value={"qualified": True}),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            try:
                exec(
                    compile(METHOD.read_text(), str(METHOD), "exec"),
                    {"__name__": "__main__", "__file__": str(METHOD)},
                )
            except AssertionError:
                if case == "valid":
                    raise
            else:
                assert case == "valid", f"invalid proof {case} admitted"
        proof = read_json(lane / "reopen/proof.json")
        assert len(calls) == 1 and proof["command"] == calls[0] and proof["completed"] is True
        assert proof["verified"] is (case == "valid")
        assert proof["query_exit_code"] == (7 if case == "exit" else 0)
        assert proof["method_sha256"] == digest(METHOD)
        assert set(proof["artifact_sha256"]) == {
            "query.jsonl",
            "query.stderr",
            "nodes.parquet",
            "edges.parquet",
            "sample.parquet",
        }
        for name, checksum in proof["artifact_sha256"].items():
            assert digest(lane / "reopen" / name) == checksum
        if case == "valid":
            assert proof["counts"] == {"node_count": 4194304, "edge_count": 67108864}
            assert proof["sample_rows"] == 1
        else:
            assert proof["failure"]


for case in ("valid", "exit", "count", "shape", "column", "sample"):
    verify_case(case)
print(
    "PASS: real Parquet result decoding; persisted command/exit/count/digest proof; "
    "failed query, wrong count/shape/column and missing sample refused"
)
