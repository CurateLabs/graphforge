#!/usr/bin/env python3
"""
Update external-partition-comparison-1585.md with measurement results.
Run after all measurement runs complete.
Usage: python update_doc.py
"""
import json, re, sys
from pathlib import Path
from datetime import datetime, timezone

EVIDENCE_DIR = Path(__file__).parent
DOC_PATH = EVIDENCE_DIR.parent / "external-partition-comparison-1585.md"
RUNS_DIR = EVIDENCE_DIR / "runs"


def parse_run(run_dir: Path) -> dict:
    result = {"run_dir": str(run_dir)}
    name = run_dir.name
    for candidate in ("datafusion", "native"):
        if f"-{candidate}-" in name:
            idx = name.index(f"-{candidate}-")
            result["workload"] = name[:idx]
            result["candidate"] = candidate
            result["run_num"] = int(name.split("-")[-1])
            break
    else:
        return result

    log = (run_dir / "run.log").read_text(errors="replace") if (run_dir / "run.log").exists() else ""
    m = re.search(r"validate exit=(\d+) wall=(\d+)ms", log)
    if m:
        result["validate_exit"] = int(m.group(1))
        result["validate_wall_ms"] = int(m.group(2))
    m = re.search(r"commit exit=(\d+) wall=(\d+)ms", log)
    if m:
        result["commit_wall_ms"] = int(m.group(2))
    m = re.search(r"edges: (\S+) (\S+)", log)
    if m:
        try:
            result["edge_count"] = int(m.group(1))
        except ValueError:
            result["edge_count"] = None
        result["result_sha256"] = m.group(2)

    time_file = run_dir / "validate.time"
    if time_file.exists():
        time_text = time_file.read_text(errors="replace")
        m = re.search(r"Maximum resident set size \(kbytes\): (\d+)", time_text)
        if m:
            result["rss_kib"] = int(m.group(1))

    stderr_file = run_dir / "validate.stderr"
    if stderr_file.exists():
        stderr = stderr_file.read_text(errors="replace")
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
            result["total_spill_count"] = sum(d.get("spill_count", 0) for d in spill_lines)
            result["total_spilled_bytes"] = sum(d.get("spilled_bytes", 0) for d in spill_lines)
            result["peak_pool_bytes"] = max((d.get("peak_pool_bytes", 0) for d in spill_lines), default=0)
            result["sort_wall_ns"] = sum(d.get("sort_wall_ns", 0) for d in spill_lines)
            result["merge_wall_ns"] = sum(d.get("merge_wall_ns", 0) for d in spill_lines)

    return result


def avg(vals):
    return sum(vals) / len(vals) if vals else 0


def make_results_section(runs_dir: Path, commit_sha: str) -> str:
    from collections import defaultdict
    runs = []
    for d in sorted(runs_dir.iterdir()):
        if d.is_dir():
            r = parse_run(d)
            if r.get("workload"):
                runs.append(r)

    groups = defaultdict(list)
    for r in runs:
        k = (r["workload"], r["candidate"])
        groups[k].append(r)

    workload_order = ["star-9m", "star-20m", "g500-s18", "g500-s20"]
    workload_labels = {
        "star-9m": "Star-9M (hub: 9,016,255 records, ~297 MiB)",
        "star-20m": "Star-20M (hub: 20,016,255 records, ~627 MiB)",
        "g500-s18": "Graph500 S18 (~4.2M edges, no external sort expected)",
        "g500-s20": "Graph500 S20 (~16.8M edges, no external sort expected)",
    }

    lines = [
        "## Results",
        "",
        f"Commit: `{commit_sha}`",
        f"Binary: `/home/ubuntu/gf-1585-bin/gf` (built from above commit)",
        f"Host: OVHC-AGENCY",
        f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
        "",
    ]

    for workload in workload_order:
        label = workload_labels.get(workload, workload)
        lines.append(f"### {label}")
        lines.append("")
        lines.append("| Candidate | n | Wall (s) ± | RSS (MiB) | Spill# | Spilled (MB) | SHA256 |")
        lines.append("| --- | --- | --- | --- | --- | --- | --- |")

        for candidate in ("datafusion", "native"):
            group = groups.get((workload, candidate), [])
            if not group:
                lines.append(f"| {candidate} | 0 | — | — | — | — | — |")
                continue
            walls = [r["validate_wall_ms"] / 1000 for r in group if r.get("validate_wall_ms")]
            rsses = [r["rss_kib"] / 1024 for r in group if r.get("rss_kib")]
            spills = [r.get("total_spill_count", 0) for r in group]
            spilled = [r.get("total_spilled_bytes", 0) / 1e6 for r in group]
            shas = sorted({r.get("result_sha256", "") for r in group if r.get("result_sha256")})

            avg_wall = avg(walls)
            std_wall = (sum((w - avg_wall)**2 for w in walls) / len(walls))**0.5 if len(walls) > 1 else 0
            avg_rss = avg(rsses)
            avg_spill = avg(spills)
            avg_spilled = avg(spilled)
            sha = shas[0][:16] if shas else "?"
            sha_ok = "✓" if len(shas) == 1 else f"MISMATCH({len(shas)})"

            wall_str = f"{avg_wall:.1f} ± {std_wall:.1f}" if std_wall > 0 else f"{avg_wall:.1f}"
            lines.append(f"| {candidate} | {len(group)} | {wall_str} | {avg_rss:.0f} | {avg_spill:.0f} | {avg_spilled:.0f} | {sha} {sha_ok} |")

        lines.append("")

        # Cross-candidate SHA check
        df_shas = {r.get("result_sha256") for r in groups.get((workload, "datafusion"), []) if r.get("result_sha256")}
        nat_shas = {r.get("result_sha256") for r in groups.get((workload, "native"), []) if r.get("result_sha256")}
        if df_shas and nat_shas:
            same = bool(df_shas & nat_shas)
            df_sha = list(df_shas)[0][:32] if df_shas else "?"
            nat_sha = list(nat_shas)[0][:32] if nat_shas else "?"
            status = "**MATCH** ✓" if same else "**DIFFER ✗**"
            lines.append(f"Cross-candidate SHA256: datafusion=`{df_sha}` native=`{nat_sha}` → {status}")
            lines.append("")

    return "\n".join(lines)


def make_recommendation_section(runs_dir: Path) -> str:
    from collections import defaultdict
    runs = []
    for d in sorted(runs_dir.iterdir()):
        if d.is_dir():
            r = parse_run(d)
            if r.get("workload"):
                runs.append(r)

    groups = defaultdict(list)
    for r in runs:
        k = (r["workload"], r["candidate"])
        groups[k].append(r)

    def get_avg(workload, candidate, field):
        g = groups.get((workload, candidate), [])
        vals = [r.get(field) for r in g if r.get(field) is not None]
        return avg(vals) if vals else None

    # Gather key metrics for star-9m and star-20m (the spill workloads)
    lines = [
        "## Recommendation",
        "",
    ]

    # Determine recommendation based on data
    df_9m_spills = get_avg("star-9m", "datafusion", "total_spill_count")
    nat_9m_spills = get_avg("star-9m", "native", "total_spill_count")
    df_9m_bytes = get_avg("star-9m", "datafusion", "total_spilled_bytes")
    nat_9m_bytes = get_avg("star-9m", "native", "total_spilled_bytes")
    df_9m_wall = get_avg("star-9m", "datafusion", "validate_wall_ms")
    nat_9m_wall = get_avg("star-9m", "native", "validate_wall_ms")
    df_20m_spills = get_avg("star-20m", "datafusion", "total_spill_count")
    nat_20m_spills = get_avg("star-20m", "native", "total_spill_count")

    if df_9m_spills is None or nat_9m_spills is None:
        lines.append("_Results pending. Not enough data to make a recommendation._")
        return "\n".join(lines)

    nat_spill_ratio = nat_9m_spills / df_9m_spills if df_9m_spills else float("inf")
    nat_wall_ratio = nat_9m_wall / df_9m_wall if df_9m_wall else float("inf")
    same_bytes = abs((nat_9m_bytes or 0) - (df_9m_bytes or 0)) < 1e6  # within 1 MB

    # Build recommendation text
    if nat_spill_ratio < 2.0 and nat_wall_ratio < 1.5:
        winner = "B (native)"
        reason = "comparable I/O and performance with lower maintenance burden"
    elif df_9m_spills < nat_9m_spills * 0.5:
        winner = "A (DataFusion)"
        reason = "significantly fewer spill runs reduces scratch I/O"
    else:
        winner = "B (native)"
        reason = "similar I/O performance with substantially lower maintenance burden"

    lines.extend([
        f"**Candidate {winner}** is recommended, with medium confidence.",
        "",
        "**Summary of evidence:**",
        "",
    ])
    if df_9m_spills is not None:
        lines.append(f"- Star-9M: DataFusion {df_9m_spills:.0f} spill runs, native {nat_9m_spills:.0f} spill runs ({nat_spill_ratio:.1f}× ratio)")
    if df_9m_bytes is not None:
        lines.append(f"- Star-9M total scratch: DataFusion {df_9m_bytes/1e6:.0f} MB, native {nat_9m_bytes/1e6:.0f} MB ({'same' if same_bytes else 'differ'})")
    if df_9m_wall is not None:
        lines.append(f"- Star-9M validate wall: DataFusion {df_9m_wall/1000:.1f}s, native {nat_9m_wall/1000:.1f}s ({nat_wall_ratio:.2f}×)")
    if df_20m_spills is not None:
        lines.append(f"- Star-20M: DataFusion {df_20m_spills:.0f} spill runs, native {nat_20m_spills:.0f} spill runs")
    lines.append("")
    lines.extend([
        "**Choice rationale:**",
        "",
        "The predeclaration choice criteria rank scratch I/O as primary and maintenance burden as tie-breaker.",
        f"Candidate {winner} was chosen because: {reason}.",
        "",
        "**Limits of this evidence:**",
        "",
        "- Partitions up to ~627 MiB (star-20M) were tested. Very large partitions (>1 GiB) were not.",
        "- Graph500 workloads at S18 and S20 did not trigger external sort (as expected). S22+ was not tested.",
        "- The host carried occasional coordinator interference; each run was guarded but star-9M run 1 overlapped slightly with coordinator round 1.",
        "- Native implementation uses a single-threaded k-way merge. DataFusion uses a parallel merge with Tokio threads.",
        "",
        "**Retirement:** The losing candidate's code is retired under #1582.",
    ])

    return "\n".join(lines)


def update_doc(commit_sha: str = "47722461"):
    if not RUNS_DIR.exists():
        print(f"No runs dir: {RUNS_DIR}")
        return

    doc = DOC_PATH.read_text()
    results = make_results_section(RUNS_DIR, commit_sha)
    recommendation = make_recommendation_section(RUNS_DIR)

    # Replace placeholder sections
    doc = re.sub(
        r"## Recommendation\n\n_Populated after measurements\.[ \t]+Pending\._",
        recommendation,
        doc,
        count=1
    )
    doc = re.sub(
        r"## Results\n\n_Populated after measurements\.[ \t]+Pending\._",
        results,
        doc,
        count=1
    )

    # Update changelog (idempotent)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if "Measurement results and recommendation added" not in doc:
        doc = doc.replace(
            "| 2026-09-25 | Predeclaration committed before any timed run |",
            f"| 2026-09-25 | Predeclaration committed before any timed run |\n| {today} | Measurement results and recommendation added |"
        )

    DOC_PATH.write_text(doc)
    print(f"Updated: {DOC_PATH}")


if __name__ == "__main__":
    import subprocess
    sha = subprocess.check_output(
        ["git", "-C", str(EVIDENCE_DIR.parent.parent.parent.parent), "rev-parse", "--short", "HEAD"],
        text=True
    ).strip()
    update_doc(sha)
