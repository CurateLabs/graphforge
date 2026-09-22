#!/usr/bin/env python3
"""Summarise F1 fio outputs (normal+json) into one table. Usage: summarize.py <out-dir>"""

import json
from pathlib import Path
import sys

HEADER = (
    f"{'run':28s} {'job':26s} {'nj':>2s} {'R MB/s':>8s} {'W MB/s':>8s} {'R IOPS':>7s} "
    f"{'W IOPS':>7s} {'fsync/s':>7s} {'fs mean':>8s} {'fs p50':>7s} {'fs p99':>7s} "
    f"{'fs max':>8s} {'%fsync':>6s} {'%write':>6s} {'%read':>6s} {'runtime':>7s}"
)


def load(path: Path) -> dict:
    """Return the JSON object embedded in fio's combined normal+json output."""
    text = path.read_text()
    start = text.index("\n{\n") + 1 if "\n{\n" in text else text.index("{")
    return json.JSONDecoder().raw_decode(text[start:])[0]


def pct(lat: dict, p: str) -> float:
    """Latency percentile in milliseconds."""
    return lat.get("percentile", {}).get(p, float("nan")) / 1e6


def share(section: dict, runtime_ms: float, threads: int, qd1: bool) -> float:
    """Per-thread share of elapsed time spent inside calls of this section."""
    if not qd1 or not runtime_ms:
        return float("nan")  # time-in-call share only means "blocked" at queue depth 1
    ios = section.get("total_ios", 0)
    mean = section.get("lat_ns", section.get("clat_ns", {})).get("mean", 0)
    return 100 * ios * mean / 1e6 / (runtime_ms * threads)


def row(path: Path, options: dict, job: dict) -> str:
    """Format one fio job as a table row."""
    options = options | job.get("job options", {})
    nj = str(options.get("numjobs", "1"))
    read, write, sync = job["read"], job["write"], job.get("sync", {})
    runtime_ms = max(read.get("runtime", 0), write.get("runtime", 0)) or job["job_runtime"]
    threads = int(nj)
    qd1 = int(options.get("iodepth", "1")) == 1
    fs_ios = sync.get("total_ios", 0)
    fs_lat = sync.get("lat_ns", {})
    return (
        f"{path.name[:-4]:28s} {job['jobname']:26s} {nj:>2s} "
        f"{read['bw_bytes'] / 1e6:8.0f} {write['bw_bytes'] / 1e6:8.0f} "
        f"{read['iops']:7.0f} {write['iops']:7.0f} {fs_ios / (runtime_ms / 1000):7.0f} "
        f"{fs_lat.get('mean', 0) / 1e6:8.2f} {pct(fs_lat, '50.000000'):7.2f} "
        f"{pct(fs_lat, '99.000000'):7.2f} {fs_lat.get('max', 0) / 1e6:8.1f} "
        f"{share(sync, runtime_ms, threads, qd1):6.1f} "
        f"{share(write, runtime_ms, threads, qd1):6.1f} "
        f"{share(read, runtime_ms, threads, qd1):6.1f} {runtime_ms / 1000:7.0f}"
    )


def main() -> None:
    """Print one row per fio job found under the output directory."""
    out = Path(sys.argv[1])
    print(HEADER)
    for path in sorted(out.glob("*.out")):
        data = load(path)
        for job in data["jobs"]:
            print(row(path, data["global options"], job))


if __name__ == "__main__":
    main()
