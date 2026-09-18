#!/usr/bin/env python3
"""Tabulate perf stat outputs from run-perf.sh. Usage: summarize-perf.py <out-dir>

Reads <name>.pass{A,A2,B,C,M}.txt, prints raw counters and derived ratios per workload.
"""

from pathlib import Path
import re
import sys

NUM = re.compile(r"^\s*([\d,]+|<not supported>|<not counted>)\s+(\S+)")
TIME = re.compile(r"^\s*([\d.]+) seconds (time elapsed|user|sys)")
METRIC = re.compile(r"#\s+([\d.]+)\s+(\S+)")
PASSES = ("A", "A2", "B", "C", "M")
TIMES = ("time elapsed", "user", "sys")
HEADER = [
    "workload", "IPC[A]", "IPC[A2]", "IPC[B]", "IPC[C]", "front-stall%[A]", "cache-miss%[A]",
    "br-miss%[A]", "L2fillwait idx[B]", "DRAM fills/kI[B]", "L2 fills/kI[B]",
    "L3/CCX fills/kI[B]", "dc acc/I[B]", "ldq-stall%[C]", "stq-stall%[C]", "retire-stall%[C]",
    "dTLB miss/kI[C]", "ic-stall%[C]", "cores[A]",
]  # fmt: skip
STALLS1 = "de_dis_dispatch_token_stalls1."
STALLS0 = "de_dis_dispatch_token_stalls0."
REFILLS = "ls_refills_from_sys.ls_mabresp_lcl_"


def parse(path: Path) -> dict:
    """Counters, times and metric lines from one perf stat output file."""
    counters: dict = {"_metrics": []}
    for line in path.read_text().splitlines():
        if line.strip().startswith("#"):
            continue
        if match := NUM.match(line):
            value = match.group(1)
            counters[match.group(2)] = (
                None if value.startswith("<") else int(value.replace(",", ""))
            )
        elif match := TIME.match(line):
            counters[match.group(2)] = float(match.group(1))
        if match := METRIC.search(line):
            counters["_metrics"].append((match.group(2), float(match.group(1))))
    return counters


def ratio(a: float | None, b: float | None, scale: float = 1.0) -> float:
    """scale * a / b, or nan when either side is missing."""
    return float("nan") if not a or not b else scale * a / b


def derived(name: str, passes: dict) -> list[str]:
    """One derived row; the pass each figure comes from is in the header."""
    pa, pa2, pb, pc = (passes.get(p, {}) for p in ("A", "A2", "B", "C"))
    cyc_a, cyc_b, cyc_c = pa.get("cycles"), pb.get("cycles"), pc.get("cycles")
    ins_a, ins_b, ins_c = pa.get("instructions"), pb.get("instructions"), pc.get("instructions")
    cpu = (pa.get("user") or 0) + (pa.get("sys") or 0)
    return [
        name,
        f"{ratio(ins_a, cyc_a):.2f}",
        f"{ratio(pa2.get('instructions'), pa2.get('cycles')):.2f}",
        f"{ratio(ins_b, cyc_b):.2f}",
        f"{ratio(ins_c, cyc_c):.2f}",
        f"{ratio(pa.get('stalled-cycles-frontend'), cyc_a, 100):.2f}",
        f"{ratio(pa.get('cache-misses'), pa.get('cache-references'), 100):.2f}",
        f"{ratio(pa.get('branch-misses'), pa.get('branches'), 100):.2f}",
        f"{ratio(pb.get('l2_latency.l2_cycles_waiting_on_fills'), cyc_b, 4):.3f}",
        f"{ratio(pb.get(REFILLS + 'dram'), ins_b, 1000):.3f}",
        f"{ratio(pb.get(REFILLS + 'l2'), ins_b, 1000):.3f}",
        f"{ratio(pb.get(REFILLS + 'cache'), ins_b, 1000):.3f}",
        f"{ratio(pb.get('ls_dc_accesses'), ins_b):.3f}",
        f"{ratio(pc.get(STALLS1 + 'load_queue_token_stall'), cyc_c, 100):.3f}",
        f"{ratio(pc.get(STALLS1 + 'store_queue_token_stall'), cyc_c, 100):.3f}",
        f"{ratio(pc.get(STALLS0 + 'retire_token_stall'), cyc_c, 100):.2f}",
        f"{ratio(pc.get('ls_l1_d_tlb_miss.all'), ins_c, 1000):.3f}",
        f"{ratio(pc.get('ic_fetch_stall.ic_stall_any'), cyc_c, 100):.1f}",
        f"{ratio(cpu, pa.get('time elapsed')):.2f}",
    ]


def main() -> None:
    """Print raw counters per pass, then the derived table."""
    out = Path(sys.argv[1])
    names = sorted({p.name.split(".pass")[0] for p in out.glob("*.pass*.txt")})
    rows = {}
    for name in names:
        rows[name] = {}
        for pass_id in PASSES:
            file = out / f"{name}.pass{pass_id}.txt"
            if file.exists():
                rows[name][pass_id] = parse(file)
    print("== raw counters (per pass; each pass is a separate run of the workload)")
    for name in names:
        for pass_id, counters in rows[name].items():
            elapsed, user, sys_s = (counters.get(k) for k in TIMES)
            print(f"-- {name} pass {pass_id}: elapsed={elapsed}s user={user}s sys={sys_s}s")
            for key, value in counters.items():
                if key not in TIMES and key != "_metrics":
                    shown = value if value is not None else "<not supported>"
                    print(f"     {key:60s} {shown:>18}")
            for key, value in counters["_metrics"]:
                print(f"     metric {key:53s} {value:>18}")
    print()
    print("== derived (pass in brackets)")
    print("\t".join(HEADER))
    for name in names:
        print("\t".join(derived(name, rows[name])))


if __name__ == "__main__":
    main()
