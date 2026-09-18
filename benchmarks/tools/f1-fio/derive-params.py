#!/usr/bin/env python3
"""Derive the F1 pattern-faithful fio parameters from a ladder rung JSON.

Every number the job files use is printed here with the field it came from,
so the fio parameters are traceable to recorded evidence rather than guessed.
Usage: derive-params.py <rung.json> [--env]   (--env prints shell exports)
"""

import json
from pathlib import Path
import sys


def kib4(n: float) -> int:
    """Round a byte count to the nearest 4 KiB multiple (O_DIRECT alignment)."""
    return max(4096, round(n / 4096) * 4096)


def split(say, buckets: dict[str, tuple[int, int]], total: int) -> str:
    """Render one bssplit string: per-phase mean size weighted by call count."""
    parts = []
    say("   phase                                   mean-size  calls  weight")
    for name, (size, calls) in sorted(buckets.items(), key=lambda kv: -kv[1][1]):
        weight = round(100 * calls / total)
        if weight == 0:
            continue
        say(f"   {name:40s} {size // 1024:5d} KiB {calls:8d} {weight:3d}%")
        parts.append((size, weight))
    drift = 100 - sum(weight for _, weight in parts)
    parts[0] = (parts[0][0], parts[0][1] + drift)  # rounding residue to the largest bucket
    return "/".join(f"{size // 1024}k/{weight}" for size, weight in parts)


def main() -> None:
    """Print the derivation, or (with --env) the shell exports the runner needs."""
    path = Path(sys.argv[1])
    env = "--env" in sys.argv
    with path.open() as handle:
        rung = json.load(handle)
    aio = rung["storage_attribution"]["construction"]["application_io"]
    tot = aio["totals"]
    edges = rung["live_edges"]
    r_b, r_c = tot["read_bytes"], tot["read_calls"]
    w_b, w_c = tot["write_bytes"], tot["write_calls"]
    fs = tot["fsync_calls"]

    def say(line: str) -> None:
        if not env:
            print(line)

    say(f"source: {path}  rung={rung['scale']} live_edges={edges}")
    say("field                                    value")
    say(f"totals.read_bytes                        {r_b}")
    say(f"totals.read_calls                        {r_c}")
    say(f"totals.write_bytes                       {w_b}")
    say(f"totals.write_calls                       {w_c}")
    say(f"totals.fsync_calls                       {fs}")
    say(f"-> mean read call    (read_bytes/read_calls)     {r_b / r_c / 1024:.1f} KiB")
    say(f"-> mean write call   (write_bytes/write_calls)   {w_b / w_c / 1024:.1f} KiB")
    read_share = 100 * r_c / (r_c + w_c)
    say(f"-> read share by calls (read_calls/(read+write)) {read_share:.1f} %  -> rwmixread")
    say(f"-> read share by bytes                           {100 * r_b / (r_b + w_b):.1f} %")
    say(f"-> writes per fsync  (write_calls/fsync_calls)   {w_c / fs:.2f}  -> fsync=")
    say(f"-> bytes per fsync   (write_bytes/fsync_calls)   {w_b / fs / 1024:.0f} KiB")
    say(f"-> fsyncs per M edges                            {fs / edges * 1e6:.0f}")
    # Block-size distribution: one bucket per construction phase, weighted by call count.
    reads: dict[str, tuple[int, int]] = {}
    writes: dict[str, tuple[int, int]] = {}
    for name, phase in aio["phases"].items():
        if phase["read_calls"]:
            reads[name] = (kib4(phase["read_bytes"] / phase["read_calls"]), phase["read_calls"])
        if phase["write_calls"]:
            writes[name] = (
                kib4(phase["write_bytes"] / phase["write_calls"]),
                phase["write_calls"],
            )
    say("read block-size split (phases[*].read_bytes/read_calls, weight=read_calls):")
    rsplit = split(say, reads, r_c)
    say("write block-size split (phases[*].write_bytes/write_calls, weight=write_calls):")
    wsplit = split(say, writes, w_c)
    say(f"-> bssplit reads  = {rsplit}")
    say(f"-> bssplit writes = {wsplit}")
    if env:
        print(f"export F1_RWMIXREAD={round(read_share)}")
        print(f"export F1_FSYNC_EVERY={round(w_c / fs)}")
        print(f"export F1_BSSPLIT='{rsplit},{wsplit}'")


if __name__ == "__main__":
    main()
