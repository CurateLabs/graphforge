#!/usr/bin/env python3
"""Run one qualified cold-cache S22 lane; raw artifacts stay outside the repo."""

import argparse
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import time

from measurement_contract import (
    BUILD_NAMES,
    CONTRACT,
    RECEIPTS,
    ambient_resources,
    build_settings,
    digest,
    quiet_metrics,
    read_json,
    require_external_output,
    runexec_result,
    successful_workload,
    validate_resource_policy,
    write_json,
)

SCRIPT_ROOT = Path(__file__).resolve().parent
parser = argparse.ArgumentParser()
parser.add_argument("--lane", required=True, choices=("baseline", "candidate"))
parser.add_argument("--pair", required=True, type=int)
parser.add_argument("--binary", required=True, type=Path)
parser.add_argument("--worktree", required=True, type=Path)
parser.add_argument("--expected-source-sha", required=True)
parser.add_argument("--build-source-file", required=True, type=Path)
parser.add_argument("--build-provenance-file", required=True, type=Path)
parser.add_argument("--evidence-root", type=Path, required=True)
parser.add_argument("--inputs-sha-file", type=Path, required=True)
parser.add_argument("--nodes", type=Path, required=True)
parser.add_argument("--edges", type=Path, required=True)
args = parser.parse_args()
BASE = args.evidence_root.resolve() / f"pair-{args.pair}" / args.lane
require_external_output(BASE, args.worktree, SCRIPT_ROOT)
if BASE.exists():
    raise SystemExit("Refusing to overwrite existing lane evidence")
BASE.mkdir(parents=True)
(BASE / "run").mkdir()
(BASE / "tmp").mkdir()
qualification = {
    "contract": CONTRACT,
    "lane": args.lane,
    "pair": args.pair,
    "completed": False,
    "qualified": False,
    "runexec_exit_code": None,
    "workload_result": None,
    "quiet_window_passed": False,
    "cold_cache_reset": False,
    "inputs_verified": False,
    "compiler_overlap_samples": None,
    "during_samples": 0,
    "artifact_sha256": {},
}


def sample():
    processes = []
    for path in Path("/proc").iterdir():
        if not path.name.isdigit():
            continue
        try:
            name = (path / "comm").read_text().strip()
            stat = (path / "stat").read_text().rsplit(")", 1)[1].split()
            if stat[0] in ("T", "t", "Z"):
                continue
            processes.append(
                {
                    "pid": int(path.name),
                    "parent_pid": int(stat[1]),
                    "name": name,
                    "cpu_ticks": int(stat[11]) + int(stat[12]),
                }
            )
        except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError):
            pass
    cpu = [int(value) for value in Path("/proc/stat").read_text().splitlines()[0].split()[1:9]]
    return {
        "system_busy_ticks": sum(cpu[index] for index in (0, 1, 2, 5, 6, 7)),
        "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "mono": time.monotonic(),
        "load": os.getloadavg(),
        "processes": processes,
    }


def run():
    # Bind exactly the two S22 input paths used by the driver to their pinned
    # content digests; arbitrary extra files cannot substitute for these inputs.
    expected_inputs = {
        str(
            args.nodes.resolve()
        ): "bcbcbea526e61ceb63f6006ee5f56de6bb4f74cffdd68dc6eff6d230d3897f06",
        str(
            args.edges.resolve()
        ): "1c0ff75485f75e904cbd59b6f5d42da1d8b1af6ddac59ee6c495a4948a428d13",
    }
    entries = []
    for line in args.inputs_sha_file.read_text().splitlines():
        checksum, separator, path = line.partition("  ")
        assert separator and path.startswith("/"), "expected absolute sha256sum input paths"
        entries.append((path, checksum))
    assert len(entries) == 2 and dict(entries) == expected_inputs, "unmatched S22 driver inputs"
    shutil.copyfile(args.inputs_sha_file, BASE / "inputs.sha256")
    with (BASE / "input-verification.txt").open("w") as output:
        subprocess.run(
            ["sha256sum", "--check", str(BASE / "inputs.sha256")], check=True, stdout=output
        )
    qualification["inputs_verified"] = True
    source_sha = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=args.worktree, text=True
    ).strip()
    assert source_sha == args.expected_source_sha
    assert args.build_source_file.read_text().strip() == source_sha
    subprocess.run(["git", "diff", "--quiet", "HEAD"], cwd=args.worktree, check=True)
    binary_sha = digest(args.binary)
    build = read_json(args.build_provenance_file)
    assert build["source_sha"] == source_sha and build["binary_sha256"] == binary_sha
    settings = build_settings(build)
    shutil.copyfile(args.build_provenance_file, BASE / "build-provenance.json")
    method_sha = {
        name: digest(SCRIPT_ROOT / name)
        for name in ("measure-lane.py", "driver.sh", "compare-pair.py", "measurement_contract.py")
    }
    identity = {
        "lane": args.lane,
        "pair": args.pair,
        "source_sha": source_sha,
        "binary": str(args.binary.resolve()),
        "binary_sha256": binary_sha,
        "host": platform.node(),
        "kernel": platform.release(),
        "logical_cpus": os.cpu_count(),
        "cpu_affinity": "0-15",
        "observer": "--json --diagnostics",
        "batch_rows": 65536,
        "inputs": (BASE / "inputs.sha256").read_text(),
        "method_sha256": method_sha,
        "build_settings": settings,
        "build_provenance_sha256": digest(BASE / "build-provenance.json"),
        "ambient_resources": ambient_resources(),
    }
    validate_resource_policy(identity["ambient_resources"])
    qualification.update(
        {key: identity[key] for key in ("source_sha", "binary_sha256", "method_sha256")}
    )
    write_json(BASE / "identity.json", identity)
    subprocess.run(["sync"], check=True)
    subprocess.run(["sudo", "-n", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"], check=True)
    qualification["cold_cache_reset"] = True
    quiet = []
    for index in range(13):
        quiet.append(sample())
        write_json(BASE / "quiet-before.json", quiet)
        if any(
            p["name"] in BUILD_NAMES
            or p["name"] in ("gf", "gf.real")
            or p["name"].startswith("graphforge_")
            for p in quiet[-1]["processes"]
        ):
            raise RuntimeError("compiler or benchmark overlap before ingest")
        if index < 12:
            time.sleep(5)
    cpu = quiet_metrics(quiet, os.sysconf("SC_CLK_TCK"))
    write_json(BASE / "quiet-cpu.json", cpu)
    assert cpu["mean_busy_cores"] <= 0.2 and cpu["max_busy_cores"] <= 0.5, "busy quiet window"
    qualification["quiet_window_passed"] = True
    with (BASE / "runexec.txt").open("w") as output, (BASE / "runexec.stderr").open("w") as error:
        env = os.environ.copy()
        env.update(
            GF1624_NODES=str(args.nodes.resolve()),
            GF1624_EDGES=str(args.edges.resolve()),
            GF1624_BINARY=str(args.binary.resolve()),
            GF1624_PROJECT=str(BASE / "project"),
            GF1624_OUT=str(BASE / "run"),
            GF1624_TMP=str(BASE / "tmp"),
            GF1624_SESSION=f"00000000-0000-4000-8000-{args.pair:06d}01624"
            + ("0" if args.lane == "baseline" else "1"),
        )
        proc = subprocess.Popen(
            [
                "runexec",
                "--no-container",
                "--output",
                str(BASE / "workload.log"),
                "--cores",
                "0-15",
                "--",
                str(SCRIPT_ROOT / "driver.sh"),
            ],
            stdout=output,
            stderr=error,
            env=env,
            cwd=BASE,
        )
        during = []
        while True:
            during.append(sample())
            code = proc.poll()
            if code is not None:
                break
            time.sleep(5)
    identity["runexec_pid"] = proc.pid
    write_json(BASE / "identity.json", identity)
    write_json(BASE / "host-during.json", during)
    qualification.update(
        completed=True,
        runexec_exit_code=code,
        during_samples=len(during),
        compiler_overlap_samples=sum(
            any(p["name"] in BUILD_NAMES for p in row["processes"]) for row in during
        ),
    )
    assert code == 0, f"runexec process exited {code}"
    qualification["workload_result"] = runexec_result(BASE / "runexec.txt")
    successful_workload(qualification["workload_result"])
    assert qualification["compiler_overlap_samples"] == 0, "compiler overlap during ingest"
    assert (
        source_sha
        == subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=args.worktree, text=True
        ).strip()
    )
    subprocess.run(["git", "diff", "--quiet", "HEAD"], cwd=args.worktree, check=True)
    after_resources = ambient_resources()
    write_json(BASE / "resource-policy-after.json", after_resources)
    validate_resource_policy(after_resources)
    assert after_resources == identity["ambient_resources"], (
        "resource policy changed during measurement"
    )
    assert digest(args.binary) == binary_sha, "binary changed during measurement"
    assert method_sha == {name: digest(SCRIPT_ROOT / name) for name in method_sha}, (
        "method changed during measurement"
    )
    artifacts = (
        "quiet-before.json",
        "quiet-cpu.json",
        "host-during.json",
        "build-provenance.json",
        "inputs.sha256",
        "input-verification.txt",
        "identity.json",
        "runexec.txt",
        "runexec.stderr",
        "workload.log",
        "resource-policy-after.json",
        *(f"run/{name}" for name in RECEIPTS),
    )
    qualification["artifact_sha256"] = {name: digest(BASE / name) for name in artifacts}
    qualification["qualified"] = True


try:
    run()
except BaseException as error:
    qualification["failure"] = str(error)
    raise
finally:
    write_json(BASE / "qualification.json", qualification)
print(json.dumps(qualification))
