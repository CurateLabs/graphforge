"""Summarize a candidate.sh output directory for #1448.

Prints the S18/S20 A/B (per run, medians and per-pair deltas), the shaping
region per run, query-answer identity, the S22 region comparison and the
candidate's core-count curve with the agreed core-use criterion.
usage: summarize_candidate.py OUT_DIR
"""

import json
from pathlib import Path
import statistics
import sys

QUERIES = ["nodes", "node-scan", "edges", "edge-scan"]
SHAPING = "import_command/validate/seal/shaping"


def runexec(run: Path) -> dict:
    with (run / "runexec.txt").open() as handle:
        fields = dict(line.strip().split("=", 1) for line in handle if "=" in line)
    return {
        "wall": float(fields["walltime"].rstrip("s")),
        "cpu": float(fields["cputime"].rstrip("s")),
        "memory_mib": int(fields["memory"].rstrip("B")) / 2**20,
        "cpu_pressure": float(fields["pressure-cpu-some"].rstrip("s")),
        "io_pressure": float(fields["pressure-io-some"].rstrip("s")),
    }


def region(run: Path, path: str) -> tuple[float, float]:
    with (run / "receipt-3-validate.json").open() as handle:
        regions = json.load(handle)["region_diagnostics"]["regions"]
    row = regions[path]["inclusive"]
    return row["wall_ns"] / 1e9, (row.get("process_cpu_ns") or 0) / 1e9


def answers(run: Path) -> tuple:
    out = []
    for query in QUERIES:
        with (run / f"{query}.json").open() as handle:
            receipt = json.loads(handle.readline())
        out.append((query, receipt["rows"], receipt.get("scalar_u64"), receipt["result_sha256"]))
    return tuple(out)


def ab(root: Path, scale: str) -> None:
    print(f"## A/B {scale}")
    walls = {"base": [], "cand": []}
    for arm in ("base", "cand"):
        for rnd in ("r1", "r2", "r3"):
            run = root / "runs" / f"ab-{scale}-{rnd}-{arm}"
            measured = runexec(run)
            shaping_wall, shaping_cpu = region(run, SHAPING)
            walls[arm].append(measured["wall"])
            print(
                f"{arm} {rnd}: wall {measured['wall']:.2f} s, cpu {measured['cpu']:.2f} s, "
                f"cpu/wall {measured['cpu'] / measured['wall']:.2f}, "
                f"shaping {shaping_wall:.2f} s (cpu/wall {shaping_cpu / shaping_wall:.2f}), "
                f"peak {measured['memory_mib']:.0f} MiB"
            )
    base, cand = statistics.median(walls["base"]), statistics.median(walls["cand"])
    print(f"median wall: base {base:.2f} s, cand {cand:.2f} s, change {cand / base - 1:+.1%}")
    deltas = [c - b for b, c in zip(walls["base"], walls["cand"], strict=True)]
    print("per-pair cand - base: " + ", ".join(f"{delta:+.2f} s" for delta in deltas))
    sets = {
        answers(root / "runs" / f"ab-{scale}-{rnd}-{arm}")
        for arm in walls
        for rnd in ("r1", "r2", "r3")
    }
    print(f"distinct answer sets: {len(sets)}")
    for query, rows, scalar, digest in next(iter(sets)):
        print(f"  {query}: rows={rows} scalar={scalar} sha256={digest}")


def curve(root: Path) -> None:
    print("## Candidate core-count curve, S18")
    one = None
    for cores in (1, 2, 4, 8, 16):
        runs = sorted((root / "runs").glob(f"curve-s18-c{cores}-r*"))
        measured = [runexec(run) for run in runs]
        wall = statistics.median(m["wall"] for m in measured)
        cpu = statistics.median(m["cpu"] for m in measured)
        if cores == 1:
            one = wall
        each = ", ".join(f"{m['wall']:.2f}" for m in measured)
        print(
            f"{cores:>2} cores ({len(runs)} runs): wall {wall:.2f} s "
            f"[{each}], cpu {cpu:.2f} s, "
            f"effective cores {cpu / wall:.2f}, relative throughput {one / wall:.2f}, "
            f"cpu pressure {statistics.median(m['cpu_pressure'] for m in measured):.2f} s, "
            f"io pressure {statistics.median(m['io_pressure'] for m in measured):.2f} s, "
            f"peak {max(m['memory_mib'] for m in measured):.0f} MiB"
        )
        if cores == 8:
            print(
                f"   criterion: throughput {one / wall:.2f}x (need >= 2.0), "
                f"effective cores {cpu / wall:.2f} (need >= 2.0)"
            )


def s22(root: Path) -> None:
    print("## S22 regions (one run each)")
    for arm in ("base", "cand"):
        run = root / "runs" / f"regions-s22-{arm}"
        measured = runexec(run)
        shaping_wall, shaping_cpu = region(run, SHAPING)
        ratio = measured["cpu"] / measured["wall"]
        print(
            f"{arm}: wall {measured['wall']:.2f} s, cpu/wall {ratio:.2f}, "
            f"shaping {shaping_wall:.2f} s (cpu/wall {shaping_cpu / shaping_wall:.2f})"
        )


def main(root: Path) -> None:
    ab(root, "s18")
    ab(root, "s20")
    s22(root)
    curve(root)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
