#!/usr/bin/env python3
"""Summarize a #1688 read-path A/B run (see ab.sh).

Reports, per scale, query and pair series: each pair's delta and sign (X - A)
for wall time, CPU time and peak memory; the median and range of the deltas;
and the same-sign verdict from docs/development/cypher-read-path-inventory.md
§6.5. A run is accepted only when it returned 0, ended QUIET, left the project
unchanged, followed its candidate's expected plan, and produced A's result
digest. Every other run is listed with the reason, never averaged in.

Usage: summarize.py OUT_DIR > summary.md
"""

import json
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

# operator_rss labels each candidate must (or must not) show per query.
FAST = {"recount-edges": "edge_count", "one-hop": "ordered_one_hop", "two-hop": "ordered_two_hop"}
CUSTOM = {"edge_count", "ordered_one_hop", "ordered_two_hop", "expand"}
SECONDS = re.compile(r"^([0-9.]+)s$")
BYTES = re.compile(r"^([0-9]+)B$")


def read_run(directory: Path) -> dict:
    meta = dict(line.split("=", 1) for line in (directory / "meta.txt").read_text().splitlines())
    rx = {}
    for line in (directory / "runexec.txt").read_text().splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            rx[key] = value
    run = {**meta, "rx": rx, "dir": directory.name}
    run["wall"] = float(SECONDS.match(rx.get("walltime", "nans")).group(1)) if "walltime" in rx else None
    run["cpu"] = float(SECONDS.match(rx.get("cputime", "nans")).group(1)) if "cputime" in rx else None
    run["mem"] = int(BYTES.match(rx["memory"]).group(1)) if "memory" in rx else None
    log = (directory / "gf.log").read_text(errors="replace") if (directory / "gf.log").exists() else ""
    brace = log.find("{")
    receipt = None
    if brace >= 0:
        try:
            receipt = json.loads(log[brace:].splitlines()[0])
        except (json.JSONDecodeError, IndexError):
            receipt = None
    run["receipt"] = receipt
    run["digest"] = receipt.get("result_sha256") if receipt else None
    rss = (receipt or {}).get("query_evidence", {}).get("operator_rss", []) or []
    run["operators"] = sorted({entry["operator"] for entry in rss})
    return run


def path_ok(run: dict) -> tuple[bool, str]:
    label = FAST.get(run["query"])
    ops = set(run["operators"])
    if label is None:
        return True, ""
    if run["candidate"] in ("current", "structural"):
        return (label in ops, f"expected {label}, saw {sorted(ops)}")
    return (not (ops & CUSTOM), f"stock must not use {sorted(ops & CUSTOM)}")


def refusal(run: dict) -> str:
    term = run["rx"].get("terminationreason")
    if term:
        return f"refused ({term})"
    if run["rx"].get("returnvalue") != "0":
        return f"failed (returnvalue={run['rx'].get('returnvalue')})"
    return ""


def fmt_delta(value: float, unit: str) -> str:
    sign = "+" if value > 0 else ("−" if value < 0 else "0")
    if unit == "MiB":
        return f"{sign}{abs(value) / 2**20:.1f}"
    return f"{sign}{abs(value):.3f}"


def main() -> None:
    out = Path(sys.argv[1])
    runs = [read_run(d) for d in sorted((out / "runs").iterdir()) if (d / "meta.txt").exists()]
    by_name = {run["dir"]: run for run in runs}
    # Reference digest per (scale, query): candidate A in the experiment build.
    reference = {}
    for run in runs:
        if run["candidate"] == "current" and run["digest"] and not refusal(run):
            reference.setdefault((run["scale"], run["query"]), run["digest"])

    rejected = []

    def accept(run: dict) -> bool:
        reasons = []
        if (why := refusal(run)):
            reasons.append(why)
        if run["after"] != "QUIET":
            reasons.append("contended (after=BUSY)")
        if run["project_changed"] != "no":
            reasons.append("project changed")
        ok, why = path_ok(run)
        if not ok:
            reasons.append(f"plan: {why}")
        ref = reference.get((run["scale"], run["query"]))
        if run["digest"] and ref and run["digest"] != ref:
            reasons.append("result digest differs from A")
        if reasons:
            rejected.append((run["dir"], "; ".join(reasons)))
        return not reasons

    series = defaultdict(list)
    pattern = re.compile(r"^s(\d+)-(.+)-(stock|structural|control)-p(\d+)-(.+)$")
    for name in by_name:
        match = pattern.match(name)
        if match:
            scale, query, kind, index, _ = match.groups()
            series[(int(scale), query, kind)].append(int(index))

    print("| Scale | Query | Series | Pair | Δ wall s | Δ CPU s | Δ peak MiB | A wall s | X wall s |")
    print("|---|---|---|---:|---:|---:|---:|---:|---:|")
    verdicts = []
    for (scale, query, kind), indices in sorted(series.items()):
        a_label, x_label = ("A0", "A") if kind == "control" else ("A", kind)
        deltas = {"wall": [], "cpu": [], "mem": []}
        for index in sorted(set(indices)):
            stem = f"s{scale}-{query}-{kind}-p{index}"
            a, x = by_name.get(f"{stem}-{a_label}"), by_name.get(f"{stem}-{x_label}")
            if not a or not x:
                continue
            a_ok, x_ok = accept(a), accept(x)
            if not (a_ok and x_ok):
                print(f"| S{scale} | {query} | {kind} | {index} | — | — | — | {a['wall'] or '—'} | {x['wall'] or '—'} |")
                continue
            for metric in deltas:
                deltas[metric].append(x[metric] - a[metric])
            print(
                f"| S{scale} | {query} | {kind} | {index} | {fmt_delta(x['wall'] - a['wall'], 's')} "
                f"| {fmt_delta(x['cpu'] - a['cpu'], 's')} | {fmt_delta(x['mem'] - a['mem'], 'MiB')} "
                f"| {a['wall']:.3f} | {x['wall']:.3f} |"
            )
        verdicts.append(((scale, query, kind), deltas))

    print("\n| Scale | Query | Series | Pairs | Metric | Median Δ | Range | Signs | Verdict |")
    print("|---|---|---|---:|---|---:|---|---|---|")
    for (scale, query, kind), deltas in verdicts:
        for metric, values in deltas.items():
            n = len(values)
            if n == 0:
                print(f"| S{scale} | {query} | {kind} | 0 | {metric} | — | — | — | no accepted pairs |")
                continue
            unit = "MiB" if metric == "mem" else "s"
            positive, negative = sum(v > 0 for v in values), sum(v < 0 for v in values)
            same = positive == n or negative == n
            if n < 3:
                verdict = "too few pairs"
            elif same and n >= 6:
                verdict = "real (all same sign, n≥6)"
            elif same:
                verdict = "suggestive (all same sign, extend to 6)"
            else:
                verdict = "no distinguishable difference at this n"
            print(
                f"| S{scale} | {query} | {kind} | {n} | {metric} | {fmt_delta(statistics.median(values), unit)} "
                f"| {fmt_delta(min(values), unit)} … {fmt_delta(max(values), unit)} | +{positive}/−{negative} | {verdict} |"
            )

    refused = sorted(p.name for p in (out / "refused").iterdir()) if (out / "refused").exists() else []
    print("\n**Refused (candidate-query, skipped at this and larger scales):** " + (", ".join(refused) or "none"))
    print("\n**Runs not accepted:**")
    for name, why in sorted(set(rejected)) or [("none", "")]:
        print(f"- {name}: {why}" if why else "- none")


if __name__ == "__main__":
    main()
