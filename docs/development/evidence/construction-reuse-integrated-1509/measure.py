#!/usr/bin/env python3
"""Paired complete-ingest measurements for the #1509 integrated experiment.

Derived from the #1507 driver (`../spill-memory-pool-1507/measure.py`). One
release `gf` built with `graphforge-storage/test-support` runs every mode;
modes differ only by environment. Each observation ingests a Graph500 input
into a fresh project through the ladder profile's five `import-session`
commands, after a page-cache drop and a sustained quiet-host window. Mode
order rotates per round. A sampler checks the quiet-host helper every second
during the run; a run that saw BUSY is retained under `contended/` and
retried, never summarized.

After each accepted ingest (untimed) the project is reopened by `gf query`:
node and edge recounts plus the ladder profile's one-hop and two-hop queries.
Their `result_sha256` values are the cross-mode correctness signal, because
published file bytes differ between any two fresh projects.

Usage:
  measure.py --gf GF --generator GEN --out DIR --budget BYTES
             [--scales 18 20] [--rounds 3] [--modes ...]
  measure.py ... --calibrate 18 --budgets B1 B2 ...
"""

import argparse
import functools
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import threading
import time

SEED = "13907095936298285200"
TIGHT_POOL_BYTES = "65536"
# `{budget}` is replaced by the recorded partition budget chosen for the run.
MODES = {
    "baseline": {},
    "rayon": {"GF_SHAPE_LOAD_SCHEDULER": "rayon"},
    "tokio": {"GF_SHAPE_LOAD_SCHEDULER": "tokio-blocking"},
    "hybrid": {
        "GF_SHAPE_MAX_PARTITION_BYTES": "{budget}",
        "GF_SHAPE_SPILL_SPIKE": "datafusion",
    },
    "hybrid-rayon": {
        "GF_SHAPE_MAX_PARTITION_BYTES": "{budget}",
        "GF_SHAPE_SPILL_SPIKE": "datafusion",
        "GF_SHAPE_LOAD_SCHEDULER": "rayon",
    },
    "hybrid-rayon-tight": {
        "GF_SHAPE_MAX_PARTITION_BYTES": "{budget}",
        "GF_SHAPE_SPILL_SPIKE": "datafusion",
        "GF_SHAPE_LOAD_SCHEDULER": "rayon",
        "GF_SHAPE_SPILL_POOL_BYTES": TIGHT_POOL_BYTES,
    },
}
DEFAULT_MODES = ["baseline", "rayon", "tokio", "hybrid", "hybrid-rayon"]
EXPERIMENT_ENV = (
    "GF_SHAPE_",
    "GF_CONSTRUCTION_FAILPOINT",
    "GF_ENCODE_SEAM_SPIKE",
)
QUERIES = (
    ("node-count", "MATCH (n) RETURN count(n)"),
    ("edge-count", "MATCH ()-[r]->() RETURN count(r)"),
    ("one-hop", "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000"),
    ("two-hop", "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000"),
)


def mode_env(mode, budget):
    return {key: value.replace("{budget}", str(budget)) for key, value in MODES[mode].items()}


QUIET = os.environ.get("QUIET_HELPER", str(Path("~/.claude/gf-quiet-host.sh").expanduser()))


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def quiet():
    report = subprocess.run([QUIET], capture_output=True, text=True, check=False)
    return report.stdout.startswith("QUIET")


def wait_quiet(samples, interval=5):
    streak = 0
    while streak < samples:
        streak = streak + 1 if quiet() else 0
        if streak < samples:
            time.sleep(interval)


def vmstat():
    values = {}
    with Path("/proc/vmstat").open() as stream:
        for line in stream:
            key, value = line.split()
            if key in ("nr_dirtied", "nr_written"):
                values[key] = int(value)
    return values


class Sampler(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.busy = 0
        self.samples = 0
        self.stop = threading.Event()

    def run(self):
        while not self.stop.wait(1.0):
            self.samples += 1
            if not quiet():
                self.busy += 1


def run_command(argv, env, stdout, stderr):
    started = time.monotonic()
    process = subprocess.Popen(argv, env=env, stdout=stdout, stderr=stderr)
    _, status, usage = os.wait4(process.pid, 0)
    process.returncode = os.waitstatus_to_exitcode(status)
    return {
        "argv": argv[3:6],
        "exit": process.returncode,
        "wall_s": time.monotonic() - started,
        "user_s": usage.ru_utime,
        "sys_s": usage.ru_stime,
        "max_rss_kib": usage.ru_maxrss,
        "inblock": usage.ru_inblock,
        "oublock": usage.ru_oublock,
    }


def ingest(gf, scale, workspace, run_dir, mode_env, extra_env=None):
    uuid = f"00000000-0000-4000-8000-{scale:012d}"
    project = run_dir / "project"
    scratch = run_dir / "scratch"
    scratch.mkdir(parents=True)
    env = dict(os.environ)
    for key in list(env):
        if key.startswith(EXPERIMENT_ENV):
            del env[key]
    env.update(mode_env)
    env.update(extra_env or {})
    env["GF_SHAPE_SPILL_DIR"] = str(scratch)
    env["TMPDIR"] = str(run_dir.parent.parent / "tmp")
    inputs = workspace / f"s{scale}"
    commands = [
        ["begin", "--operation-uuid", uuid],
        [
            "register-parquet",
            "--session-uuid",
            uuid,
            "--path",
            str(inputs / "nodes.parquet"),
            "--kind",
            "nodes",
        ],
        [
            "register-parquet",
            "--session-uuid",
            uuid,
            "--path",
            str(inputs / "edges.parquet"),
            "--kind",
            "edges",
        ],
        ["validate", "--session-uuid", uuid],
        ["commit", "--session-uuid", uuid],
    ]
    steps = []
    before = vmstat()
    started = time.monotonic()
    with (run_dir / "stderr.txt").open("wb") as stderr:
        for index, command in enumerate(commands):
            argv = [gf, "--json", "--project", str(project), "import-session", *command]
            with (run_dir / f"receipt-{index}-{command[0]}.json").open("wb") as stdout:
                step = run_command(argv, env, stdout, stderr)
            steps.append(step)
            if step["exit"] != 0:
                break
    wall = time.monotonic() - started
    after = vmstat()
    leftovers = sorted(str(path.relative_to(scratch)) for path in scratch.rglob("*"))
    return {
        "ok": all(step["exit"] == 0 for step in steps) and len(steps) == len(commands),
        "wall_s": wall,
        "cpu_s": sum(step["user_s"] + step["sys_s"] for step in steps),
        "max_rss_kib": max(step["max_rss_kib"] for step in steps),
        "validate_wall_s": steps[3]["wall_s"] if len(steps) > 3 else None,
        "validate_cpu_s": (steps[3]["user_s"] + steps[3]["sys_s"]) if len(steps) > 3 else None,
        "validate_max_rss_kib": steps[3]["max_rss_kib"] if len(steps) > 3 else None,
        "oublock_512": sum(step["oublock"] for step in steps),
        "host_pages_dirtied": after["nr_dirtied"] - before["nr_dirtied"],
        "host_pages_written": after["nr_written"] - before["nr_written"],
        "scratch_leftovers": leftovers,
        "steps": steps,
    }


def append_jsonl(path, record):
    with path.open("a") as stream:
        stream.write(json.dumps(record) + "\n")


def sum_of(partitions, key):
    return sum(partition[key] for partition in partitions)


def max_of(partitions, key):
    return max((partition[key] for partition in partitions), default=0)


def receipts_summary(run_dir):
    summary = {}
    for path in sorted(run_dir.glob("receipt-*.json")):
        summary[path.name] = sha256(path)
    return summary


def drop_page_cache():
    subprocess.run(["sync"], check=True)
    subprocess.run(
        ["sudo", "-n", "tee", "/proc/sys/vm/drop_caches"],
        input=b"3",
        stdout=subprocess.DEVNULL,
        check=True,
    )


def reopen_and_query(gf, run_dir):
    """Untimed: reopen the published project and hash the query answers."""
    argv = [gf, "--json", "--project", str(run_dir / "project"), "query"]
    for name, cypher in QUERIES:
        argv += ["--cypher", cypher, "--output", str(run_dir / f"{name}.arrow")]
    report = subprocess.run(argv, capture_output=True, stdin=subprocess.DEVNULL, check=False)
    if report.returncode != 0:
        return {"ok": False, "stderr": report.stderr.decode(errors="replace")[-2000:]}
    answers = {}
    for (name, _), line in zip(QUERIES, report.stdout.decode().splitlines(), strict=True):
        receipt = json.loads(line)
        answers[name] = {
            "result_sha256": receipt["result_sha256"],
            "rows": receipt["rows"],
            "scalar_u64": receipt.get("scalar_u64"),
        }
    return {"ok": True, "answers": answers}


def commit_evidence(run_dir):
    """GraphForge-accounted construction evidence from the commit receipt."""
    path = run_dir / "receipt-4-commit.json"
    if not path.exists():
        return None
    receipt = json.loads(path.read_text())
    construction = receipt.get("construction", {})
    totals = construction.get("application_io", {}).get("totals", {})
    return {
        "accepted_chunks": construction.get("accepted_chunks"),
        "input_rows": construction.get("input_rows"),
        "application_write_bytes": totals.get("write_bytes"),
        "application_read_bytes": totals.get("read_bytes"),
        "application_fsync_calls": totals.get("fsync_calls"),
        "receipt_sha256": sha256(path),
    }


def instrumented_run(args, workspace, out, scale, mode, budget, name):
    """One untimed run with per-partition external-sort metrics."""
    run_dir = out / "runs" / name
    if run_dir.exists():
        shutil.rmtree(run_dir)
    result = ingest(
        args.gf,
        scale,
        workspace,
        run_dir,
        mode_env(mode, budget),
        {"GF_SHAPE_SPILL_METRICS": "1"},
    )
    partitions = []
    for line in (run_dir / "stderr.txt").read_text().splitlines():
        if line.startswith("SHAPE_SPILL "):
            partitions.append(json.loads(line[len("SHAPE_SPILL ") :]))
    total = functools.partial(sum_of, partitions)
    peak = functools.partial(max_of, partitions)
    record = {
        "name": name,
        "scale": scale,
        "mode": mode,
        "budget": budget,
        "ok": result["ok"],
        "error_tail": (None if result["ok"] else (run_dir / "stderr.txt").read_text()[-2000:]),
        "external_partitions": len(partitions),
        "spilling_partitions": sum(1 for partition in partitions if partition["spill_count"] > 0),
        "records": total("records"),
        "input_wire_bytes": total("input_wire_bytes"),
        "max_partition_input_wire_bytes": peak("input_wire_bytes"),
        "spill_count": total("spill_count"),
        "spilled_bytes": total("spilled_bytes"),
        "spilled_rows": total("spilled_rows"),
        "peak_pool_bytes": peak("peak_pool_bytes"),
        "pool_limit_bytes": peak("pool_limit_bytes"),
        "peak_spill_disk_bytes": peak("peak_spill_disk_bytes"),
        "peak_spill_files": peak("peak_spill_files"),
        "sort_wall_s": total("sort_wall_ns") / 1e9,
        "merge_wall_s": total("merge_wall_ns") / 1e9,
        "wall_s": result["wall_s"],
        "max_rss_kib": result["max_rss_kib"],
        "validate_max_rss_kib": result["validate_max_rss_kib"],
        "scratch_leftovers": result["scratch_leftovers"],
        "commit": commit_evidence(run_dir),
    }
    (run_dir / "stderr.txt").unlink(missing_ok=True)
    shutil.rmtree(run_dir / "project", ignore_errors=True)
    append_jsonl(out / "instrumented.jsonl", record)
    return record


def generate_inputs(args, workspace, manifest):
    for scale in sorted(set(args.scales + ([args.calibrate] if args.calibrate else []))):
        inputs = workspace / f"s{scale}"
        if not (inputs / "edges.parquet").exists():
            inputs.mkdir(parents=True, exist_ok=True)
            subprocess.run(
                [
                    args.generator,
                    "--scale",
                    str(scale),
                    "--edge-factor",
                    "16",
                    "--seed",
                    SEED,
                    "--nodes",
                    str(inputs / "nodes.parquet"),
                    "--edges",
                    str(inputs / "edges.parquet"),
                ],
                check=True,
                stdout=subprocess.DEVNULL,
            )
        manifest[f"s{scale}_inputs"] = {
            name: sha256(inputs / name) for name in ("nodes.parquet", "edges.parquet")
        }


def summarize(observations, scales, modes, out):
    summary = {}
    keys = (
        "wall_s",
        "cpu_s",
        "max_rss_kib",
        "validate_wall_s",
        "validate_cpu_s",
        "validate_max_rss_kib",
        "commit_wall_s",
        "oublock_512",
        "host_pages_dirtied",
        "host_pages_written",
    )
    for scale in scales:
        for mode in modes:
            accepted = [o for o in observations if o["scale"] == scale and o["mode"] == mode]
            if not accepted:
                continue
            summary[f"s{scale}/{mode}"] = {
                key: {
                    "median": statistics.median(o[key] for o in accepted),
                    "min": min(o[key] for o in accepted),
                    "max": max(o[key] for o in accepted),
                }
                for key in keys
            } | {
                "n": len(accepted),
                "answers": sorted(
                    {json.dumps(o["verify"]["answers"], sort_keys=True) for o in accepted}
                ),
                "commit_evidence": sorted(
                    {
                        json.dumps(
                            {k: v for k, v in o["commit"].items() if k != "receipt_sha256"},
                            sort_keys=True,
                        )
                        for o in accepted
                    }
                ),
            }
    (out / "summary.json").write_text(json.dumps(summary, indent=1))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gf", required=True)
    parser.add_argument("--generator", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--budget", type=int, help="recorded partition budget for hybrid modes")
    parser.add_argument("--scales", type=int, nargs="+", default=[18, 20])
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--quiet-samples", type=int, default=12)
    parser.add_argument("--modes", nargs="+", default=DEFAULT_MODES)
    parser.add_argument("--instrumented", nargs="*", default=["hybrid-rayon"])
    parser.add_argument("--calibrate", type=int, help="scale for budget calibration only")
    parser.add_argument("--budgets", type=int, nargs="*", default=[])
    args = parser.parse_args()
    # A build without graphforge-storage/test-support silently ignores every
    # mode variable and measures the baseline under every name.
    binary = Path(args.gf).read_bytes()
    needles = (b"GF_SHAPE_SPILL_SPIKE", b"GF_SHAPE_LOAD_SCHEDULER", b"GF_SHAPE_MAX_PARTITION_BYTES")
    for needle in needles:
        if needle not in binary:
            raise SystemExit(f"{args.gf} was built without {needle.decode()}")
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    (out / "tmp").mkdir(exist_ok=True)
    workspace = out / "workspace"
    manifest = {
        "gf": args.gf,
        "gf_sha256": sha256(args.gf),
        "generator_sha256": sha256(args.generator),
        "seed": SEED,
        "edge_factor": 16,
        "budget": args.budget,
        "modes": {mode: mode_env(mode, args.budget) for mode in args.modes},
        "quiet_samples_before_start": args.quiet_samples,
        "uname": os.uname().release,
    }
    generate_inputs(args, workspace, manifest)
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1))

    if args.calibrate:
        # Untimed. The predeclared rule picks the budget from these records.
        for budget in args.budgets:
            instrumented_run(
                args,
                workspace,
                out,
                args.calibrate,
                "hybrid",
                budget,
                f"calibrate-s{args.calibrate}-{budget}",
            )
        return
    if args.budget is None:
        raise SystemExit("--budget is required for measurement")

    observations = []
    for scale in args.scales:
        for round_index in range(args.rounds):
            shift = round_index % len(args.modes)
            order = args.modes[shift:] + args.modes[:shift]
            for mode in order:
                for attempt in range(1, 6):
                    name = f"s{scale}-r{round_index + 1}-{mode}-a{attempt}"
                    run_dir = out / "runs" / name
                    drop_page_cache()
                    wait_quiet(args.quiet_samples)
                    sampler = Sampler()
                    sampler.start()
                    result = ingest(args.gf, scale, workspace, run_dir, mode_env(mode, args.budget))
                    sampler.stop.set()
                    sampler.join()
                    commit_step = result["steps"][4] if len(result["steps"]) > 4 else None
                    result.update(
                        {
                            "name": name,
                            "scale": scale,
                            "round": round_index + 1,
                            "mode": mode,
                            "attempt": attempt,
                            "busy_samples": sampler.busy,
                            "samples": sampler.samples,
                            "commit_wall_s": commit_step["wall_s"] if commit_step else None,
                            "receipts": receipts_summary(run_dir),
                            "commit": commit_evidence(run_dir),
                        }
                    )
                    contended = sampler.busy > 0 or not quiet()
                    result["accepted"] = result["ok"] and not contended
                    if result["ok"]:
                        result["verify"] = reopen_and_query(args.gf, run_dir)
                        if not result["verify"]["ok"]:
                            append_jsonl(out / "observations.jsonl", result)
                            raise SystemExit(f"{name}: reopen/query failed; see {run_dir}")
                    append_jsonl(out / "observations.jsonl", result)
                    shutil.rmtree(run_dir / "project", ignore_errors=True)
                    for arrow in run_dir.glob("*.arrow"):
                        arrow.unlink()
                    if result["accepted"]:
                        observations.append(result)
                        summarize(observations, args.scales, args.modes, out)
                        break
                    if not result["ok"]:
                        raise SystemExit(f"{name} failed; see {run_dir}")
                    (out / "contended").mkdir(exist_ok=True)
                    run_dir.rename(out / "contended" / name)
        for mode in args.instrumented:
            instrumented_run(
                args, workspace, out, scale, mode, args.budget, f"s{scale}-instrumented-{mode}"
            )
    summarize(observations, args.scales, args.modes, out)


if __name__ == "__main__":
    main()
