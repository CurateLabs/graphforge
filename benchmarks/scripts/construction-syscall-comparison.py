#!/usr/bin/env python3
"""Compare construction syscalls with profile commands and BenchExec process-tree timing."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def execute(binary, command, cwd, stdout, inputs, trace=None):
    command = command.copy()
    if "register-parquet" in command:
        index = command.index("--path") + 1
        name = Path(command[index]).name
        if name not in {"nodes.parquet", "edges.parquet"}:
            raise RuntimeError("profile registration source is not a generated input")
        command[index] = str(inputs / name)
    args = [str(binary), "--diagnostics", *command]
    if trace is not None:
        args = ["strace", "-f", "-c", "-o", str(trace), *args]
    with Path(stdout).open("xb") as output, Path(str(stdout) + ".stderr").open("xb") as errors:
        subprocess.run(
            args, cwd=cwd, stdin=subprocess.DEVNULL, stdout=output, stderr=errors, check=True
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["generate", "workflow", "queries", "run"])
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--run", type=Path)
    parser.add_argument("--trace", action="store_true")
    args = parser.parse_args()
    args.profile = args.profile.resolve()
    args.binary = args.binary.resolve()
    args.inputs = args.inputs.resolve()
    if args.mode != "generate" and args.run is None:
        parser.error("--run is required except for generate")
    args.run = args.run.resolve() if args.run else None
    profile = json.loads(args.profile.read_text())
    scale = profile["scale"]
    phases = {item["phase"]: item["action"] for item in profile["phases"]}
    if args.mode == "generate":
        args.inputs.mkdir(parents=True, exist_ok=False)
        command = phases["generate"]["args"].copy()
        for flag, name in [("--nodes", "nodes.parquet"), ("--edges", "edges.parquet")]:
            command[command.index(flag) + 1] = str(args.inputs / name)
        with (args.inputs / "generator.json").open("xb") as output:
            subprocess.run([str(args.binary), *command], stdout=output, check=True)
        (args.inputs / "identity.json").write_text(
            json.dumps(
                {
                    "profile_sha256": digest(args.profile),
                    "generator_binary_sha256": digest(args.binary),
                    "profile_generator": profile["generator"],
                    "inputs_sha256": {
                        name: digest(args.inputs / name)
                        for name in ["nodes.parquet", "edges.parquet"]
                    },
                },
                indent=2,
            )
            + "\n"
        )
        return
    if args.mode == "workflow":
        for index, command in enumerate(phases["ingest"]["commands"]):
            execute(
                args.binary,
                command,
                args.run,
                args.run / f"receipt-{index}.json",
                args.inputs,
                args.run / "validate.strace" if args.trace and "validate" in command else None,
            )
        return
    if args.mode == "queries":
        for phase in ["reopen", "recount", "query"]:
            for index, command in enumerate(phases[phase]["commands"]):
                execute(
                    args.binary, command, args.run, args.run / f"{phase}-{index}.json", args.inputs
                )
        answers = []
        for phase in ["recount", "query"]:
            for path in sorted(args.run.glob(f"{phase}-*.json")):
                for line in path.read_text().splitlines():
                    receipt = json.loads(line)
                    if receipt.get("contract") == "graphforge-result-sink/2":
                        result_digest = receipt.get("result_sha256")
                        if (
                            receipt.get("complete") is not True
                            or not isinstance(result_digest, str)
                            or len(result_digest) != 64
                            or any(c not in "0123456789abcdef" for c in result_digest)
                            or not isinstance(receipt.get("rows"), int)
                        ):
                            raise RuntimeError("query receipt lacks a complete result digest")
                        answers.append(
                            {
                                key: receipt.get(key)
                                for key in ["result_sha256", "rows", "scalar_u64"]
                            }
                        )
        if len(answers) != 4:
            raise RuntimeError(f"expected four profile query answers, got {len(answers)}")
        (args.run / "answers.json").write_text(json.dumps(answers, indent=2) + "\n")
        return
    busy_names = ["cargo", "rustc", "gf", "gf.real", "runexec", "maturin", "perf", "strace"]

    def quiet():
        for name in busy_names:
            result = subprocess.run(["pgrep", "-x", name], stdout=subprocess.DEVNULL, check=False)
            if result.returncode != 1:
                raise RuntimeError(f"quiet-host refusal: {name}, pgrep exit {result.returncode}")

    quiet()
    args.run.mkdir(parents=True, exist_ok=False)
    workspace = args.run / "workspace" / f"s{scale}"
    workspace.mkdir(parents=True)
    for name in ["nodes.parquet", "edges.parquet"]:
        path = args.inputs / name
        if path.is_symlink() or not path.is_file():
            raise RuntimeError("registration input must be a regular non-linked file")
    input_identity = json.loads((args.inputs / "identity.json").read_text())
    actual_inputs = {
        name: digest(args.inputs / name) for name in ["nodes.parquet", "edges.parquet"]
    }
    if actual_inputs != input_identity["inputs_sha256"]:
        raise RuntimeError("input digest changed")
    (args.run / "tmp").mkdir()
    os.environ["TMPDIR"] = str(args.run / "tmp")
    (args.run / "provenance.json").write_text(
        json.dumps(
            {
                "binary_sha256": digest(args.binary),
                "profile_sha256": digest(args.profile),
                "driver_sha256": digest(Path(__file__)),
                "input_identity": input_identity,
                "cores": list(range(16)),
                "memory": "4000MB",
                "diagnostics": True,
                "allocation_diagnostics": False,
                "validate_strace": args.trace,
                "cache": (
                    "warm hashed inputs; shared identical input files; fresh project per run"
                ),
            },
            indent=2,
        )
        + "\n"
    )
    child = [
        sys.executable,
        str(Path(__file__).resolve()),
        "workflow",
        "--profile",
        str(args.profile),
        "--binary",
        str(args.binary),
        "--inputs",
        str(args.inputs),
        "--run",
        str(args.run),
    ]
    if args.trace:
        child.append("--trace")
    with (
        (args.run / "runexec.txt").open("xb") as output,
        (args.run / "runexec.stderr").open("xb") as errors,
    ):
        subprocess.run(
            [
                "runexec",
                "--no-container",
                "--cores",
                "0-15",
                "--memlimit",
                "4000MB",
                "--output",
                str(args.run / "workflow.log"),
                "--",
                *child,
            ],
            stdout=output,
            stderr=errors,
            check=True,
        )
    quiet()
    result = (args.run / "runexec.txt").read_text()
    if "returnvalue=0\n" not in result:
        raise RuntimeError(f"workflow failed; inspect {args.run}")
    subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "queries",
            "--profile",
            str(args.profile),
            "--binary",
            str(args.binary),
            "--inputs",
            str(args.inputs),
            "--run",
            str(args.run),
        ],
        check=True,
    )


if __name__ == "__main__":
    main()
