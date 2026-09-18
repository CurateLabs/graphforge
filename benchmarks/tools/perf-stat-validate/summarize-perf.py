#!/usr/bin/env python3
"""Tabulate perf stat outputs from run-perf.sh. Usage: summarize-perf.py <out-dir>
Reads <name>.pass{A,A2,B,C,M}.txt, prints raw counters and derived ratios per workload."""
import glob
import os
import re
import sys

NUM = re.compile(r"^\s*([\d,]+|<not supported>|<not counted>)\s+(\S+)")
TIME = re.compile(r"^\s*([\d.]+) seconds (time elapsed|user|sys)")


def parse(path):
    c = {}
    for line in open(path):
        m = NUM.match(line)
        if m:
            v = m.group(1)
            c[m.group(2)] = None if v.startswith("<") else int(v.replace(",", ""))
            continue
        m = TIME.match(line)
        if m:
            c[m.group(2)] = float(m.group(1))
        m = re.match(r"^\s*([\d.,]+)\s+(\S+)\s+#\s+([\d.]+)\s+(\S.*?)\s*$", line)  # metric lines
        if m and "=" not in m.group(2):
            pass
    return c


def metric_lines(path):
    out = []
    for line in open(path):
        m = re.search(r"#\s+([\d.]+)\s+(\S+)", line)
        if m and not line.strip().startswith("#"):
            out.append((m.group(2), float(m.group(1))))
    return out


def main():
    out = sys.argv[1]
    names = sorted({os.path.basename(p).split(".pass")[0] for p in glob.glob(os.path.join(out, "*.pass*.txt"))})
    rows = {}
    for n in names:
        rows[n] = {}
        for p in ("A", "A2", "B", "C", "M"):
            f = os.path.join(out, f"{n}.pass{p}.txt")
            if os.path.exists(f):
                rows[n][p] = parse(f)
                rows[n][p]["_metrics"] = metric_lines(f)

    def g(n, p, k):
        return rows[n].get(p, {}).get(k)

    def ratio(a, b, scale=1.0):
        return float("nan") if not a or not b else scale * a / b

    print("== raw counters (per pass; each pass is a separate run of the workload)")
    for n in names:
        for p, c in rows[n].items():
            print(f"-- {n} pass {p}: elapsed={c.get('time elapsed')}s user={c.get('user')}s sys={c.get('sys')}s")
            for k, v in c.items():
                if k not in ("time elapsed", "user", "sys", "_metrics"):
                    print(f"     {k:60s} {v if v is not None else '<not supported>':>18}")
            for k, v in c.get("_metrics", []):
                print(f"     metric {k:53s} {v:>18}")
    print()
    print("== derived (pass in brackets)")
    hdr = ["workload", "IPC[A]", "IPC[A2]", "IPC[B]", "IPC[C]", "front-stall%[A]", "cache-miss%[A]", "br-miss%[A]",
           "L2fillwait idx[B]", "DRAM fills/kI[B]", "L2 fills/kI[B]", "L3/CCX fills/kI[B]", "dc acc/I[B]",
           "ldq-stall%[C]", "stq-stall%[C]", "retire-stall%[C]", "dTLB miss/kI[C]", "ic-stall%[C]", "cores[A]"]
    print("\t".join(hdr))
    for n in names:
        A, A2, B, C = (rows[n].get(p, {}) for p in ("A", "A2", "B", "C"))
        cyc = lambda c: c.get("cycles"); ins = lambda c: c.get("instructions")
        vals = [n,
                f"{ratio(ins(A), cyc(A)):.2f}", f"{ratio(ins(A2), cyc(A2)):.2f}", f"{ratio(ins(B), cyc(B)):.2f}", f"{ratio(ins(C), cyc(C)):.2f}",
                f"{ratio(A.get('stalled-cycles-frontend'), cyc(A), 100):.2f}",
                f"{ratio(A.get('cache-misses'), A.get('cache-references'), 100):.2f}",
                f"{ratio(A.get('branch-misses'), A.get('branches'), 100):.2f}",
                f"{ratio(B.get('l2_latency.l2_cycles_waiting_on_fills'), cyc(B), 4):.3f}",
                f"{ratio(B.get('ls_refills_from_sys.ls_mabresp_lcl_dram'), ins(B), 1000):.3f}",
                f"{ratio(B.get('ls_refills_from_sys.ls_mabresp_lcl_l2'), ins(B), 1000):.3f}",
                f"{ratio(B.get('ls_refills_from_sys.ls_mabresp_lcl_cache'), ins(B), 1000):.3f}",
                f"{ratio(B.get('ls_dc_accesses'), ins(B)):.3f}",
                f"{ratio(C.get('de_dis_dispatch_token_stalls1.load_queue_token_stall'), cyc(C), 100):.3f}",
                f"{ratio(C.get('de_dis_dispatch_token_stalls1.store_queue_token_stall'), cyc(C), 100):.3f}",
                f"{ratio(C.get('de_dis_dispatch_token_stalls0.retire_token_stall'), cyc(C), 100):.2f}",
                f"{ratio(C.get('ls_l1_d_tlb_miss.all'), ins(C), 1000):.3f}",
                f"{ratio(C.get('ic_fetch_stall.ic_stall_any'), cyc(C), 100):.1f}",
                f"{ratio((A.get('user') or 0) + (A.get('sys') or 0), A.get('time elapsed')):.2f}"]
        print("\t".join(vals))


if __name__ == "__main__":
    main()
