"""Private, non-admission RSS diagnostic using unchanged Graph500/CLI actions.

Run from the repository with an existing frozen gf/generator and an empty output
root. Raw commands and receipts remain local; summary.json contains no graph rows.
"""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time

from pyarrow import parquet

RESERVE = 141258578535
QUERIES = {
    "nodes": "MATCH (n) RETURN count(n)",
    "edges": "MATCH ()-[r]->() RETURN count(r)",
    "one-hop": "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000",
    "two-hop": "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000",
}


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def input_oracle(edge_path):
    """Independent edge-multiset oracle; do not use GraphForge query operators."""
    incoming = Counter()
    for batch in parquet.ParquetFile(edge_path).iter_batches(columns=["target_uuid"]):
        incoming.update(batch.column(0).to_pylist())
    paths = Counter()
    for batch in parquet.ParquetFile(edge_path).iter_batches(
        columns=["source_uuid", "target_uuid"]
    ):
        for source, target in zip(
            batch.column(0).to_pylist(), batch.column(1).to_pylist(), strict=True
        ):
            # Cypher cannot reuse the same relationship in a two-hop path.
            paths[target] += incoming[source] - int(source == target)
    result = {}
    for name, counts in [("one-hop", incoming), ("two-hop", paths)]:
        values = []
        for key, count in sorted(counts.items()):
            values.extend([key] * min(count, 1000 - len(values)))
            if len(values) == 1000:
                break
        result[name] = values
    return result


def active_campaigns():
    names = {"cargo", "rustc", "gf", "graphforge-benc", "gf-symbolized", "perf"}
    active = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            name = (proc / "comm").read_text().strip()
            if name in names:
                active.append({"pid": int(proc.name), "name": name})
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    return active


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gf", type=Path, required=True)
    parser.add_argument("--generator", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.gf = args.gf.resolve(strict=True)
    args.generator = args.generator.resolve(strict=True)
    args.output = args.output.resolve()
    root = Path(__file__).resolve().parents[2]
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "tmp").mkdir()
    records = []
    summary = {
        "claim": "diagnostic_only_not_admission",
        "scales": [16, 17, 18],
        "source_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True
        ).strip(),
        "gf_sha256": digest(args.gf),
        "generator_sha256": digest(args.generator),
        "generator_source_sha256": digest(
            root / "benchmarks/runners/graph500-generator/src/main.rs"
        ),
        "cargo_lock_sha256": digest(root / "Cargo.lock"),
        "profile_sha256": digest(root / "benchmarks/profiles/graph500/s18-local.json"),
        "script_sha256": digest(__file__),
        "reserve_bytes": RESERVE,
        "rss_limit_bytes": 4 * 1024**3,
        "command_timeout_seconds": 14400,
        "repetitions": 3,
        "observations": records,
    }

    def save():
        (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

    save()
    cpus = ",".join(map(str, sorted(os.sched_getaffinity(0))[:16]))
    summary["cpu_affinity"] = cpus
    env = dict(os.environ, TMPDIR=str(args.output / "tmp"))
    for name in ["LLVM_PROFILE_FILE", "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS"]:
        env.pop(name, None)

    def run(label, command, **metadata):
        if shutil.disk_usage(args.output).free <= RESERVE:
            raise RuntimeError("reserve exhausted before command")
        wait_started = time.monotonic()
        while active_campaigns():
            if time.monotonic() - wait_started > 1800:
                raise RuntimeError(f"host unavailable before {label}")
            time.sleep(2)
        entry = dict(label=label, host_wait_seconds=time.monotonic() - wait_started, **metadata)
        records.append(entry)
        (args.output / f"{label}.command.json").write_text(json.dumps(command))
        started = time.monotonic()
        samples = []
        with (
            (args.output / f"{label}.stdout").open("wb") as stdout,
            (args.output / f"{label}.stderr").open("wb") as stderr,
        ):
            process = subprocess.Popen(
                ["taskset", "-c", cpus, *command],
                cwd=args.output,
                env=env,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
            )
            killed = None
            while True:
                try:
                    # Fork inherits the Python parent RSS high-water in wait4.
                    # Read only the target after exec; match native VmHWM authority.
                    observed_executable = Path(f"/proc/{process.pid}/exe").resolve(strict=True)
                    if observed_executable != Path(command[0]).resolve():
                        time.sleep(0.005)
                        continue
                    status = Path(f"/proc/{process.pid}/status").read_text()
                    sample = {"elapsed_seconds": time.monotonic() - started}
                    for line in status.splitlines():
                        key, _, value = line.partition(":")
                        if key in {"VmHWM", "VmRSS", "RssAnon", "RssFile", "RssShmem"}:
                            sample[key] = int(value.split()[0]) * 1024
                    samples.append(sample)
                    if sample.get("VmRSS", 0) > summary["rss_limit_bytes"]:
                        killed = "rss_limit"
                except FileNotFoundError:
                    pass
                if shutil.disk_usage(args.output).free <= RESERVE:
                    killed = "disk_reserve"
                if time.monotonic() - started > 14400:
                    killed = "time_limit"
                if killed:
                    os.killpg(process.pid, signal.SIGKILL)
                pid, status, usage = os.wait4(process.pid, os.WNOHANG)
                if pid:
                    process.returncode = os.waitstatus_to_exitcode(status)
                    break
                time.sleep(0.005)
        (args.output / f"{label}.proc.json").write_text(json.dumps(samples))
        entry.update(
            exit_code=process.returncode,
            wall_seconds=time.monotonic() - started,
            peak_rss_bytes=max((s.get("VmHWM", 0) for s in samples), default=0),
            wait4_maxrss_bytes_includes_pre_exec=usage.ru_maxrss * 1024,
            user_seconds=usage.ru_utime,
            system_seconds=usage.ru_stime,
            samples=len(samples),
            failure=killed,
            sampled_max={
                key: max((s.get(key, 0) for s in samples), default=0)
                for key in ["VmHWM", "VmRSS", "RssAnon", "RssFile", "RssShmem"]
            },
        )
        save()
        print(label, entry["peak_rss_bytes"], process.returncode, flush=True)
        if process.returncode or killed:
            raise RuntimeError(f"first failure: {label}")
        return entry

    profile = json.loads((root / "benchmarks/profiles/graph500/s18-local.json").read_text())
    for scale in summary["scales"]:
        workspace = args.output / "workspace" / f"s{scale}"
        workspace.mkdir(parents=True)
        for phase in profile["phases"]:
            action = phase["action"]
            commands = action.get("commands", [action.get("args")])
            for index, original_command in enumerate(commands):
                command = [
                    item.replace("workspace/s18", f"workspace/s{scale}")
                    for item in original_command
                ]
                executable = args.generator if phase["phase"] == "generate" else args.gf
                if phase["phase"] == "generate":
                    command[command.index("--scale") + 1] = str(scale)
                run(
                    f"s{scale}-{phase['phase']}-{index}",
                    [str(executable), *command],
                    scale=scale,
                    phase=phase["phase"],
                    command_index=index,
                    kind="lifecycle",
                )
        oracle = input_oracle(workspace / "edges.parquet")
        expected = {}
        for repetition in range(3):
            for project in ["source", "imported"]:
                for name, query in QUERIES.items():
                    label = f"s{scale}-{project}-{name}-r{repetition}"
                    output = workspace / f"{project}-{name}-r{repetition}.arrow"
                    entry = run(
                        label,
                        [
                            str(args.gf),
                            "--json",
                            "--project",
                            str(workspace / project),
                            "query",
                            "--cypher",
                            query,
                            "--output",
                            str(output),
                        ],
                        scale=scale,
                        project=project,
                        query=name,
                        repetition=repetition,
                        kind="query",
                    )
                    table = parquet.read_table(output)
                    rows = table.to_pylist()
                    if name in {"nodes", "edges"}:
                        assert len(rows) == 1
                        assert list(rows[0].values()) == [
                            (1 << scale) * (16 if name == "edges" else 1)
                        ]
                    else:
                        assert len(rows) == 1000
                        ids = table.column(0).to_pylist()
                        assert ids == oracle[name], f"independent oracle mismatch {label}"
                    if name in expected:
                        assert table.equals(expected[name]), f"query mismatch {label}"
                    else:
                        expected[name] = table
                    entry["rows"] = len(rows)
                    entry["matches_source_and_repetitions"] = True
                    entry["matches_input_oracle"] = True
                    entry["result_file_sha256"] = digest(output)
                    save()
    summary["status"] = "passed"
    save()


if __name__ == "__main__":
    main()
