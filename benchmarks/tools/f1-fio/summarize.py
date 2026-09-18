#!/usr/bin/env python3
"""Summarise F1 fio outputs (normal+json) into one table. Usage: summarize.py <out-dir>"""
import glob
import json
import os
import sys


def load(path):
    text = open(path).read()
    start = text.index("\n{\n") + 1 if "\n{\n" in text else text.index("{")
    return json.JSONDecoder().raw_decode(text[start:])[0]


def pct(lat, p):
    return lat.get("percentile", {}).get(p, float("nan")) / 1e6  # ns -> ms


def main():
    out = sys.argv[1]
    print(f"{'run':28s} {'job':26s} {'nj':>2s} {'R MB/s':>8s} {'W MB/s':>8s} {'R IOPS':>7s} {'W IOPS':>7s} {'fsync/s':>7s} "
          f"{'fs mean':>8s} {'fs p50':>7s} {'fs p99':>7s} {'fs max':>8s} {'%fsync':>6s} {'%write':>6s} {'%read':>6s} {'runtime':>7s}")
    for path in sorted(glob.glob(os.path.join(out, "*.out"))):
        d = load(path)
        for j in d["jobs"]:
            nj = d["global options"].get("numjobs", "1")
            r, w, s = j["read"], j["write"], j.get("sync", {})
            rt_ms = max(r.get("runtime", 0), w.get("runtime", 0)) or j["job_runtime"]
            # time inside calls, summed over threads, as a share of (runtime x threads)
            threads = int(nj)
            qd1 = d["global options"].get("iodepth", "1") == "1" and j.get("job options", {}).get("iodepth", "1") == "1"
            def share(sec):
                if not qd1:
                    return float("nan")  # time-in-call share only means "blocked" at queue depth 1
                ios = sec.get("total_ios", 0); mean = sec.get("lat_ns", sec.get("clat_ns", {})).get("mean", 0)
                return 100 * ios * mean / 1e6 / (rt_ms * threads) if rt_ms else float("nan")
            fs_ios = s.get("total_ios", 0); fs_lat = s.get("lat_ns", {})
            print(f"{os.path.basename(path)[:-4]:28s} {j['jobname']:26s} {nj:>2s} {r['bw_bytes']/1e6:8.0f} {w['bw_bytes']/1e6:8.0f} "
                  f"{r['iops']:7.0f} {w['iops']:7.0f} {fs_ios/(rt_ms/1000):7.0f} "
                  f"{fs_lat.get('mean',0)/1e6:8.2f} {pct(fs_lat,'50.000000'):7.2f} {pct(fs_lat,'99.000000'):7.2f} {fs_lat.get('max',0)/1e6:8.1f} "
                  f"{share(s):6.1f} {share(w):6.1f} {share(r):6.1f} {rt_ms/1000:7.0f}")


if __name__ == "__main__":
    main()
