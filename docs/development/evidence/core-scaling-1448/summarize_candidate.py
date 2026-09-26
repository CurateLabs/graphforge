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


def accepted_runs(root: Path) -> dict[str, bool]:
    """Each run's host check after it ended: True when QUIET."""
    after = {}
    with (root / "driver.log").open() as handle:
        for line in handle:
            parts = line.split()
            if len(parts) >= 4 and parts[1] == "end":
                after[parts[2]] = parts[3] == "after=QUIET"
    return after


def ab(root: Path, scale: str) -> None:
    print(f"## A/B {scale}")
    quiet = accepted_runs(root)
    rounds = sorted({run.name.split("-")[2] for run in (root / "runs").glob(f"ab-{scale}-r*-base")})
    walls = {"base": {}, "cand": {}}
    for arm in ("base", "cand"):
        for rnd in rounds:
            name = f"ab-{scale}-{rnd}-{arm}"
            run = root / "runs" / name
            measured = runexec(run)
            shaping_wall, shaping_cpu = region(run, SHAPING)
            accepted = quiet.get(name, False)
            if accepted:
                walls[arm][rnd] = measured["wall"]
            print(
                f"{arm} {rnd}: wall {measured['wall']:.2f} s, cpu {measured['cpu']:.2f} s, "
                f"cpu/wall {measured['cpu'] / measured['wall']:.2f}, "
                f"shaping {shaping_wall:.2f} s (cpu/wall {shaping_cpu / shaping_wall:.2f}), "
                f"peak {measured['memory_mib']:.0f} MiB, "
                f"cpu pressure {measured['cpu_pressure']:.3f} s"
                + ("" if accepted else " [BUSY after the run: excluded]")
            )
    base = statistics.median(walls["base"].values())
    cand = statistics.median(walls["cand"].values())
    print(
        f"median wall (accepted runs: base {len(walls['base'])}, cand {len(walls['cand'])}): "
        f"base {base:.2f} s, cand {cand:.2f} s, change {cand / base - 1:+.1%}"
    )
    pairs = [rnd for rnd in rounds if rnd in walls["base"] and rnd in walls["cand"]]
    deltas = [walls["cand"][rnd] - walls["base"][rnd] for rnd in pairs]
    print(
        "per-pair cand - base (accepted pairs): "
        + ", ".join(f"{rnd} {delta:+.2f} s" for rnd, delta in zip(pairs, deltas, strict=True))
    )
    sets = {
        answers(root / "runs" / f"ab-{scale}-{rnd}-{arm}")
        for arm in ("base", "cand")
        for rnd in rounds
    }
    print(f"distinct answer sets: {len(sets)}")
    for query, rows, scalar, digest in next(iter(sets)):
        print(f"  {query}: rows={rows} scalar={scalar} sha256={digest}")


def curve(root: Path) -> None:
    print("## Candidate core-count curve, S18")
    quiet = accepted_runs(root)
    one = None
    for cores in (1, 2, 4, 8, 16):
        every = sorted((root / "runs").glob(f"curve-s18-c{cores}-*"))
        runs = [run for run in every if quiet.get(run.name, False)]
        excluded = [run.name for run in every if run not in runs]
        if excluded:
            print(f"   excluded as BUSY after the run: {', '.join(excluded)}")
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
