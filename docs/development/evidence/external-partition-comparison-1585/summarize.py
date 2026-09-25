#!/usr/bin/env python3
"""Summarize measurement runs for #1585 comparison.
Usage: python summarize.py [runs_dir]
Reads runs/<workload>-<candidate>-<n>/run.log and validate.stderr.
"""
import json, re, sys
from pathlib import Path

RUNS_DIR = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "runs"

def parse_run(run_dir: Path) -> dict:
    result = {"run_dir": str(run_dir), "workload": None, "candidate": None, "run_num": None}
    # Parse name
    name = run_dir.name  # e.g. star-9m-datafusion-1
    for candidate in ("datafusion", "native"):
        if f"-{candidate}-" in name:
            idx = name.index(f"-{candidate}-")
            result["workload"] = name[:idx]
            result["candidate"] = candidate
            result["run_num"] = int(name.split("-")[-1])
            break
    else:
        return result

    # Parse run.log
    log = (run_dir / "run.log").read_text(errors="replace") if (run_dir / "run.log").exists() else ""
    # wall time for validate
    m = re.search(r"validate exit=(\d+) wall=(\d+)ms", log)
    if m:
        result["validate_exit"] = int(m.group(1))
        result["validate_wall_ms"] = int(m.group(2))
    m = re.search(r"commit exit=(\d+) wall=(\d+)ms", log)
    if m:
        result["commit_wall_ms"] = int(m.group(2))
    m = re.search(r"rss_kib=(\S+)", log)
    if m and m.group(1) != "?":
        result["rss_kib"] = int(m.group(1))
    # edge count and sha
    m = re.search(r"edges: (\d+) (.+)", log)
    if m:
        result["edge_count"] = int(m.group(1)) if m.group(1) != "?" else None
        result["result_sha256"] = m.group(2).strip()

    # Parse validate.time for RSS if not in log
    time_file = run_dir / "validate.time"
    if time_file.exists():
        time_text = time_file.read_text(errors="replace")
        m = re.search(r"Maximum resident set size \(kbytes\): (\d+)", time_text)
        if m:
            result["rss_kib"] = int(m.group(1))

    # Parse spill metrics from validate.stderr
    stderr_file = run_dir / "validate.stderr"
    if stderr_file.exists():
        stderr = stderr_file.read_text(errors="replace")
        # DataFusion: "SHAPE_SPILL {...}"
        # Native: "SHAPE_SPILL_NATIVE {...}"
        spill_lines = []
        for line in stderr.splitlines():
            if line.startswith("SHAPE_SPILL"):
                try:
                    prefix, json_str = line.split(" ", 1)
                    data = json.loads(json_str)
                    data["_type"] = prefix
                    spill_lines.append(data)
                except Exception:
                    pass
        result["spill_metrics"] = spill_lines
        if spill_lines:
            # Aggregate
            result["total_spill_count"] = sum(d.get("spill_count", 0) for d in spill_lines)
            result["total_spilled_bytes"] = sum(d.get("spilled_bytes", 0) for d in spill_lines)
            result["peak_pool_bytes"] = max((d.get("peak_pool_bytes", 0) for d in spill_lines), default=0)
            result["sort_wall_ns"] = sum(d.get("sort_wall_ns", 0) for d in spill_lines)
            result["merge_wall_ns"] = sum(d.get("merge_wall_ns", 0) for d in spill_lines)

    return result

def main():
    if not RUNS_DIR.exists():
        print(f"No runs dir: {RUNS_DIR}")
        return

    runs = []
    for d in sorted(RUNS_DIR.iterdir()):
        if d.is_dir():
            r = parse_run(d)
            if r.get("workload"):
                runs.append(r)

    if not runs:
        print("No completed runs found.")
        return

    # Group by workload × candidate
    from collections import defaultdict
    groups = defaultdict(list)
    for r in runs:
        k = (r["workload"], r["candidate"])
        groups[k].append(r)

    print(f"{'Workload':<12} {'Candidate':<12} {'N':>2} {'Wall(ms)':>10} {'RSS(MiB)':>9} {'Spill#':>7} {'Spilled(MB)':>12} {'SHA256'}")
    print("-" * 100)
    for (workload, candidate), group in sorted(groups.items()):
        walls = [r.get("validate_wall_ms") for r in group if r.get("validate_wall_ms")]
        rsses = [r.get("rss_kib") for r in group if r.get("rss_kib")]
        spills = [r.get("total_spill_count", 0) for r in group]
        spilled = [r.get("total_spilled_bytes", 0) for r in group]
        shas = list({r.get("result_sha256", "?") for r in group if r.get("result_sha256")})

        avg_wall = sum(walls) / len(walls) if walls else 0
        avg_rss = sum(rsses) / len(rsses) / 1024 if rsses else 0  # KiB → MiB
        avg_spill = sum(spills) / len(spills) if spills else 0
        avg_spilled = sum(spilled) / len(spilled) / 1e6 if spilled else 0  # bytes → MB
        sha_display = shas[0][:16] if shas else "?"
        sha_consistent = "✓" if len(shas) <= 1 else "MISMATCH"

        print(f"{workload:<12} {candidate:<12} {len(group):>2} {avg_wall:>10.0f} {avg_rss:>9.1f} {avg_spill:>7.0f} {avg_spilled:>12.1f} {sha_display} {sha_consistent}")

    # Cross-candidate sha comparison
    print("\n=== SHA256 cross-candidate check ===")
    workloads = sorted(set(w for (w, _) in groups))
    for wl in workloads:
        shas = {}
        for cand in ("datafusion", "native"):
            group = groups.get((wl, cand), [])
            sha_set = {r.get("result_sha256") for r in group if r.get("result_sha256")}
            shas[cand] = sha_set
        if shas.get("datafusion") and shas.get("native"):
            same = bool(shas["datafusion"] & shas["native"])
            df_sha = list(shas["datafusion"])[0][:16] if shas["datafusion"] else "?"
            nat_sha = list(shas["native"])[0][:16] if shas["native"] else "?"
            print(f"  {wl:<12}: datafusion={df_sha} native={nat_sha} {'MATCH' if same else 'DIFFER'}")
        else:
            print(f"  {wl:<12}: missing data (df={len(groups.get((wl,'datafusion'),[]))} nat={len(groups.get((wl,'native'),[]))} runs)")

if __name__ == "__main__":
    main()
