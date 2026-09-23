#!/usr/bin/env python3
"""Paired complete-ingest measurements for the #1507 spill/memory-pool spike.

One release `gf` built with `graphforge-storage/test-support` runs every mode;
modes differ only by environment. Each observation ingests a Graph500 input
into a fresh project through the ladder profile's five `import-session`
commands. Mode order rotates per pair. A run starts only after the quiet-host
helper reports QUIET for `--quiet-samples` consecutive samples, and a sampler
checks it every second during the run; a run that saw BUSY is retained under
`contended/` and retried, never summarized.

Usage:
  measure.py --gf GF --generator GEN --out DIR [--scales 18 20] [--pairs 3]
"""

import argparse
import hashlib
import json
import os
import shutil
import statistics
import subprocess
import threading
import time
from pathlib import Path

SEED = "13907095936298285200"
TIGHT_POOL_BYTES = "65536"
MODES = {
    "baseline": {},
    "external": {"GF_SHAPE_SPILL_SPIKE": "datafusion-always"},
    "external-tight": {
        "GF_SHAPE_SPILL_SPIKE": "datafusion-always",
        "GF_SHAPE_SPILL_POOL_BYTES": TIGHT_POOL_BYTES,
    },
}
QUIET = os.environ.get("QUIET_HELPER", os.path.expanduser("~/.claude/gf-quiet-host.sh"))


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def quiet():
    return subprocess.run([QUIET], capture_output=True, text=True).stdout.startswith("QUIET")


def wait_quiet(samples, interval=5):
    streak = 0
    while streak < samples:
        streak = streak + 1 if quiet() else 0
        if streak < samples:
            time.sleep(interval)


def vmstat():
    values = {}
    with open("/proc/vmstat") as stream:
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
        if key.startswith("GF_SHAPE_"):
            del env[key]
    env.update(mode_env)
    env.update(extra_env or {})
    env["GF_SHAPE_SPILL_DIR"] = str(scratch)
    env["TMPDIR"] = str(run_dir.parent.parent / "tmp")
    inputs = workspace / f"s{scale}"
    commands = [
        ["begin", "--operation-uuid", uuid],
        ["register-parquet", "--session-uuid", uuid, "--path", str(inputs / "nodes.parquet"), "--kind", "nodes"],
        ["register-parquet", "--session-uuid", uuid, "--path", str(inputs / "edges.parquet"), "--kind", "edges"],
        ["validate", "--session-uuid", uuid],
        ["commit", "--session-uuid", uuid],
    ]
    steps = []
    before = vmstat()
    started = time.monotonic()
    with open(run_dir / "stderr.txt", "wb") as stderr:
        for index, command in enumerate(commands):
            argv = [gf, "--json", "--project", str(project), "import-session", *command]
            with open(run_dir / f"receipt-{index}-{command[0]}.json", "wb") as stdout:
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
        "oublock_512": sum(step["oublock"] for step in steps),
        "host_pages_dirtied": after["nr_dirtied"] - before["nr_dirtied"],
        "host_pages_written": after["nr_written"] - before["nr_written"],
        "scratch_leftovers": leftovers,
        "steps": steps,
    }


def receipts_summary(run_dir):
    summary = {}
    for path in sorted(run_dir.glob("receipt-*.json")):
        try:
            summary[path.name] = sha256(path)
        except OSError:
            pass
    commit = run_dir / "receipt-4-commit.json"
    if commit.exists():
        text = commit.read_text()
        summary["commit_bytes"] = len(text)
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gf", required=True)
    parser.add_argument("--generator", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--scales", type=int, nargs="+", default=[18, 20])
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--quiet-samples", type=int, default=12)
    parser.add_argument("--modes", nargs="+", default=list(MODES))
    parser.add_argument("--instrumented-only", action="store_true")
    args = parser.parse_args()
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
        "modes": {mode: MODES[mode] for mode in args.modes},
        "quiet_samples_before_start": args.quiet_samples,
        "uname": os.uname().release,
    }
    for scale in args.scales:
        inputs = workspace / f"s{scale}"
        if not (inputs / "edges.parquet").exists():
            inputs.mkdir(parents=True, exist_ok=True)
            subprocess.run([
                args.generator, "--scale", str(scale), "--edge-factor", "16", "--seed", SEED,
                "--nodes", str(inputs / "nodes.parquet"), "--edges", str(inputs / "edges.parquet"),
            ], check=True)
        manifest[f"s{scale}_inputs"] = {
            name: sha256(inputs / name) for name in ("nodes.parquet", "edges.parquet")
        }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1))
    observations = []
    log = open(out / "observations.jsonl", "a")
    for scale in args.scales:
        if not args.instrumented_only:
            for pair in range(args.pairs):
                order = args.modes[pair % len(args.modes):] + args.modes[: pair % len(args.modes)]
                for mode in order:
                    for attempt in range(1, 6):
                        name = f"s{scale}-p{pair + 1}-{mode}-a{attempt}"
                        run_dir = out / "runs" / name
                        wait_quiet(args.quiet_samples)
                        sampler = Sampler()
                        sampler.start()
                        result = ingest(args.gf, scale, workspace, run_dir, MODES[mode])
                        sampler.stop.set()
                        sampler.join()
                        result.update({
                            "name": name, "scale": scale, "pair": pair + 1, "mode": mode,
                            "attempt": attempt, "busy_samples": sampler.busy,
                            "samples": sampler.samples, "receipts": receipts_summary(run_dir),
                        })
                        contended = sampler.busy > 0 or not quiet()
                        result["accepted"] = result["ok"] and not contended
                        log.write(json.dumps(result) + "\n")
                        log.flush()
                        shutil.rmtree(run_dir / "project", ignore_errors=True)
                        if result["accepted"]:
                            observations.append(result)
                            break
                        if not result["ok"]:
                            raise SystemExit(f"{name} failed; see {run_dir}")
                        (out / "contended").mkdir(exist_ok=True)
                        run_dir.rename(out / "contended" / name)
        # One instrumented run per external mode, excluded from timing.
        for mode in [mode for mode in args.modes if mode != "baseline"]:
            name = f"s{scale}-instrumented-{mode}"
            run_dir = out / "runs" / name
            if run_dir.exists():
                shutil.rmtree(run_dir)
            result = ingest(args.gf, scale, workspace, run_dir, MODES[mode], {"GF_SHAPE_SPILL_METRICS": "1"})
            partitions = []
            for line in (run_dir / "stderr.txt").read_text().splitlines():
                if line.startswith("SHAPE_SPILL "):
                    partitions.append(json.loads(line[len("SHAPE_SPILL "):]))
            total = lambda key: sum(partition[key] for partition in partitions)
            peak = lambda key: max((partition[key] for partition in partitions), default=0)
            instrumented = {
                "name": name, "scale": scale, "mode": mode, "ok": result["ok"],
                "external_partitions": len(partitions),
                "spilling_partitions": sum(1 for partition in partitions if partition["spill_count"] > 0),
                "records": total("records"),
                "input_wire_bytes": total("input_wire_bytes"),
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
                "scratch_leftovers": result["scratch_leftovers"],
            }
            (run_dir / "stderr.txt").unlink()
            shutil.rmtree(run_dir / "project", ignore_errors=True)
            with open(out / "instrumented.jsonl", "a") as stream:
                stream.write(json.dumps(instrumented) + "\n")
    summary = {}
    for scale in args.scales:
        for mode in args.modes:
            accepted = [o for o in observations if o["scale"] == scale and o["mode"] == mode]
            if not accepted:
                continue
            summary[f"s{scale}/{mode}"] = {
                key: {
                    "median": statistics.median(o[key] for o in accepted),
                    "min": min(o[key] for o in accepted),
                    "max": max(o[key] for o in accepted),
                }
                for key in ("wall_s", "cpu_s", "max_rss_kib", "validate_wall_s", "validate_cpu_s",
                            "oublock_512", "host_pages_dirtied", "host_pages_written")
            } | {"n": len(accepted)}
    (out / "summary.json").write_text(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
