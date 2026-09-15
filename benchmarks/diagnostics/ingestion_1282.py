"""Preselected, first-failure ingestion measurements; never admission evidence.

Run on the authorized idle host, inside the existing 96-GB cgroup ceiling.
The collector also enforces the existing process-RSS envelope and disk reserve.
Raw commands, traces, receipts and query outputs stay in a private output root.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import pwd
import shutil
import signal
import subprocess
import time

from graphforge_bench.ingestion_attribution import expected_commands
from pyarrow import parquet
from rss_1278 import active_campaigns, digest, input_oracle

RESERVE = 141258578535
ROOT = Path(__file__).resolve().parents[2]
PROFILE = ROOT / "benchmarks/profiles/graph500/s18-local.json"
BOUNDARIES = [(n, n) for n in (31, 32, 33, 1023, 1024, 1025)] + [(16, 256), (64, 1024)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gf", type=Path, required=True)
    parser.add_argument("--generator", type=Path, required=True)
    parser.add_argument("--boundary-test", type=Path)
    parser.add_argument("--instrument", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--suite", choices=["scaling", "boundary", "diagnostic", "perf", "sync"], required=True
    )
    args = parser.parse_args()
    args.gf = args.gf.resolve(strict=True)
    args.generator = args.generator.resolve(strict=True)
    if args.boundary_test:
        args.boundary_test = args.boundary_test.resolve(strict=True)
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "tmp").mkdir()
    boundary = args.suite == "boundary"
    profiling = args.suite in {"perf", "sync"}
    diagnostic = args.instrument or args.suite in {"diagnostic", "perf", "sync"}
    cases = (
        [{"name": f"b{n}-{e}", "nodes": n, "edges": e} for n, e in BOUNDARIES]
        if boundary
        else [
            {"name": f"s{s}", "scale": s, "nodes": 1 << s, "edges": 16 << s}
            for s in ([18] if profiling else [16, 17, 18])
        ]
    )
    repetitions = 1 if diagnostic else 3
    selection = [{**case, "repetition": rep} for rep in range(repetitions) for case in cases]
    cpus = ",".join(map(str, sorted(os.sched_getaffinity(0))[:16]))
    profile = json.loads(PROFILE.read_text())
    planned = expected_commands(selection, profile, boundary)
    membership = Path("/proc/self/cgroup").read_text().strip().removeprefix("0::")
    cgroup = Path("/sys/fs/cgroup") / membership.lstrip("/")
    ceilings = []
    while cgroup != Path("/sys/fs"):
        memory_max = cgroup / "memory.max"
        if memory_max.exists() and memory_max.read_text().strip() != "max":
            ceilings.append(int(memory_max.read_text()))
        cgroup = cgroup.parent
    if not ceilings or min(ceilings) > 96_000_000_000:
        raise RuntimeError("run inside the existing 96-GB cgroup ceiling")
    summary = {
        "issue": 1282,
        "claim": "diagnostic_only_not_admission",
        "suite": args.suite,
        "instrumented": diagnostic,
        "source_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "source_diff_sha256": hashlib.sha256(
            subprocess.check_output(["git", "diff", "HEAD"], cwd=ROOT)
        ).hexdigest(),
        "gf_sha256": digest(args.gf),
        "generator_sha256": digest(args.generator),
        "script_sha256": digest(__file__),
        "profile_sha256": digest(PROFILE),
        "cargo_lock_sha256": digest(ROOT / "Cargo.lock"),
        "rustc": subprocess.check_output(["rustc", "-Vv"], cwd=ROOT, text=True),
        "cpu_affinity": cpus,
        "reserve_bytes": RESERVE,
        "process_rss_limit_bytes": 4 * 1024**3,
        "timeout_seconds": 14400,
        "cgroup_memory_ceiling_bytes": min(ceilings),
        "selection": selection,
        "observations": [],
        "cache_policy": "Fresh projects; natural cache state; no system-wide cache changes.",
        "io_wait_available": Path("/proc/sys/kernel/task_delayacct").read_text().strip() == "1",
    }
    if boundary:
        if not args.boundary_test:
            parser.error("boundary suite requires --boundary-test")
        summary["boundary_test_sha256"] = digest(args.boundary_test)

    def save():
        (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

    source_paths = subprocess.check_output(
        [
            "git",
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "--",
            "crates",
            "benchmarks",
            "Cargo.toml",
            "Cargo.lock",
            "rust-toolchain.toml",
        ],
        cwd=ROOT,
        text=True,
    ).splitlines()
    source_manifest = {
        path: digest(ROOT / path)
        for path in sorted(set(source_paths))
        if path.endswith((".rs", ".py", ".toml", ".lock"))
    }
    (args.output / "source-manifest.json").write_text(json.dumps(source_manifest, sort_keys=True))
    summary["source_manifest_sha256"] = digest(args.output / "source-manifest.json")
    save()
    (args.output / "selection.json").write_text(
        json.dumps({"cases": selection, "commands": planned}, indent=2) + "\n"
    )
    summary["selection_sha256"] = digest(args.output / "selection.json")
    env = dict(os.environ, TMPDIR=str(args.output / "tmp"))
    for key in (
        "RUSTFLAGS",
        "CARGO_ENCODED_RUSTFLAGS",
        "LLVM_PROFILE_FILE",
        "GRAPHFORGE_INGEST_DIAGNOSTICS",
    ):
        env.pop(key, None)
    if args.instrument or args.suite == "diagnostic":
        env["GRAPHFORGE_INGEST_DIAGNOSTICS"] = "1282"

    def run(label, command, phase, case, extra_env=None):
        competitors = active_campaigns()
        if competitors:
            raise RuntimeError(f"host busy before {label}: {competitors}")
        if shutil.disk_usage(args.output).free <= RESERVE:
            raise RuntimeError("disk reserve before command")
        actual = ["taskset", "-c", cpus, *map(str, command)]
        if profiling and phase == "ingest":
            if args.suite == "perf":
                workload = ["runuser", "-u", pwd.getpwuid(os.getuid()).pw_name, "--", *actual]
                actual = [
                    "sudo",
                    "-n",
                    "perf",
                    "record",
                    "-F",
                    "199",
                    "-e",
                    "cpu-clock",
                    "--call-graph",
                    "dwarf",
                    "-o",
                    str(args.output / f"{label}.perf.data"),
                    "--",
                    *workload,
                ]
            else:
                actual = [
                    "strace",
                    "-f",
                    "-ttt",
                    "-T",
                    "-e",
                    "trace=fsync,fdatasync",
                    "-o",
                    str(args.output / f"{label}.strace"),
                    *actual,
                ]
        entry = {
            "label": label,
            "case": case["name"],
            "repetition": case["repetition"],
            "phase": phase,
            "started_unix": time.time(),
            "host_activity_before": competitors,
            "free_bytes_before": shutil.disk_usage(args.output).free,
        }
        summary["observations"].append(entry)
        command_file = args.output / f"{label}.command.json"
        command_file.write_text(json.dumps(actual))
        entry["command_sha256"] = digest(command_file)
        save()
        start = time.monotonic()
        samples = []
        failure = None
        with (
            (args.output / f"{label}.stdout").open("wb") as out,
            (args.output / f"{label}.stderr").open("wb") as err,
        ):
            process = subprocess.Popen(
                actual,
                cwd=args.output,
                env={**env, **(extra_env or {})},
                stdout=out,
                stderr=err,
                start_new_session=True,
            )
            while True:
                targets = [process.pid]
                for target in targets:
                    try:
                        children = (
                            Path(f"/proc/{target}/task/{target}/children").read_text().split()
                        )
                        targets.extend(
                            int(child) for child in children if int(child) not in targets
                        )
                        if (
                            Path(f"/proc/{target}/exe").resolve(strict=True)
                            != Path(command[0]).resolve()
                        ):
                            continue
                        target_cgroup = (
                            Path(f"/proc/{target}/cgroup").read_text().strip().removeprefix("0::")
                        )
                        if target_cgroup != membership:
                            failure = "workload_left_resource_cgroup"
                        status = Path(f"/proc/{target}/status").read_text()
                        sample = {"elapsed_seconds": time.monotonic() - start}
                        for line in status.splitlines():
                            key, _, value = line.partition(":")
                            if key in {"VmHWM", "VmRSS"}:
                                sample[key] = int(value.split()[0]) * 1024
                        if summary["io_wait_available"]:
                            stat = (
                                Path(f"/proc/{target}/stat").read_text().rsplit(")", 1)[1].split()
                            )
                            sample["io_delay_ticks"] = int(stat[39])
                        samples.append(sample)
                        if sample.get("VmRSS", 0) > 4 * 1024**3:
                            failure = "process_rss_limit"
                    except (FileNotFoundError, ProcessLookupError):
                        pass
                if int(time.monotonic() - start) > len(entry.get("host_activity", [])):
                    activity = [item for item in active_campaigns() if item["pid"] not in targets]
                    entry.setdefault("host_activity", []).append(activity)
                    if activity:
                        failure = "concurrent_campaign"
                if shutil.disk_usage(args.output).free <= RESERVE:
                    failure = "disk_reserve"
                if time.monotonic() - case_start > 14400:
                    failure = "timeout"
                if failure:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except PermissionError:
                        subprocess.run(
                            ["sudo", "-n", "kill", "-KILL", "--", f"-{process.pid}"], check=True
                        )
                pid, status, usage = os.wait4(process.pid, os.WNOHANG)
                if pid:
                    process.returncode = os.waitstatus_to_exitcode(status)
                    break
                time.sleep(0.01)
        entry.update(
            exit_code=process.returncode,
            failure=failure,
            wall_seconds=time.monotonic() - start,
            user_seconds=usage.ru_utime,
            system_seconds=usage.ru_stime,
            wait4_input_bytes=usage.ru_inblock * 512,
            wait4_output_bytes=usage.ru_oublock * 512,
            sampled_process_peak_bytes=max((s.get("VmHWM", 0) for s in samples), default=0),
            free_bytes_after=shutil.disk_usage(args.output).free,
        )
        (args.output / f"{label}.proc.json").write_text(json.dumps(samples))
        for suffix in ("stdout", "stderr", "proc.json"):
            entry[suffix + "_sha256"] = digest(args.output / f"{label}.{suffix}")
        for suffix in ("perf.data", "strace"):
            artifact = args.output / f"{label}.{suffix}"
            if artifact.exists():
                if suffix == "perf.data":
                    subprocess.run(
                        ["sudo", "-n", "chown", f"{os.getuid()}:{os.getgid()}", str(artifact)],
                        check=True,
                    )
                entry[suffix + "_sha256"] = digest(artifact)
        save()
        print(label, entry["wall_seconds"], entry["exit_code"], flush=True)
        if process.returncode or failure:
            raise RuntimeError(f"first failure: {label}")
        return entry

    profile = json.loads(PROFILE.read_text())
    for case in selection:
        case_start = time.monotonic()
        tag = f"{case['name']}-r{case['repetition']}"
        relative = f"workspace/{tag}"
        workspace = args.output / relative
        workspace.mkdir(parents=True)
        if boundary:
            run(
                f"{tag}-ingest",
                [
                    args.boundary_test,
                    "--exact",
                    "ingestion_family_boundaries_preserve_facade_results",
                    "--nocapture",
                    "--test-threads=1",
                ],
                "ingest",
                case,
                {
                    "GF_1282_PROJECT": str(workspace / "source"),
                    "GF_1282_NODES": str(case["nodes"]),
                    "GF_1282_EDGES": str(case["edges"]),
                },
            )
        for phase in profile["phases"]:
            if boundary and phase["phase"] in {"admission", "generate", "ingest"}:
                continue
            action = phase["action"]
            for index, original in enumerate(action.get("commands", [action.get("args")])):
                command = [item.replace("workspace/s18", relative) for item in original]
                executable = args.generator if phase["phase"] == "generate" else args.gf
                if phase["phase"] == "generate":
                    command[command.index("--scale") + 1] = str(case["scale"])
                run(f"{tag}-{phase['phase']}-{index}", [executable, *command], phase["phase"], case)
        if boundary:
            topology = [
                (i % case["nodes"] + 1, (i + 1) % case["nodes"] + 1) for i in range(case["edges"])
            ]
            one = sorted(t for _, t in topology)[:1000]
            two = sorted(
                t
                for i, (_, m) in enumerate(topology)
                for j, (s, t) in enumerate(topology)
                if i != j and m == s
            )[:1000]
            oracle = {
                "one-hop": [n.to_bytes(16, "big") for n in one],
                "two-hop": [n.to_bytes(16, "big") for n in two],
            }
        else:
            oracle = input_oracle(workspace / "edges.parquet")
        for prefix in ("", "imported-"):
            for name, expected in (
                ("node-count", [case["nodes"]]),
                ("edge-count", [case["edges"]]),
                ("one-hop", oracle["one-hop"]),
                ("two-hop", oracle["two-hop"]),
            ):
                path = workspace / f"{prefix}{name}.arrow"
                values = parquet.read_table(path).column(0).to_pylist()
                if values != expected:
                    raise RuntimeError(f"independent query oracle mismatch {tag} {prefix}{name}")
        summary.setdefault("completed_cases", []).append(
            {
                "case": tag,
                "nodes": case["nodes"],
                "edges": case["edges"],
                "independent_oracles": 8,
                "full_lifecycle": True,
            }
        )
        save()
    summary["status"] = "passed"
    save()


if __name__ == "__main__":
    main()
