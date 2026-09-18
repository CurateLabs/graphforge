#!/usr/bin/env python3
"""Derive the F1 pattern-faithful fio parameters from a ladder rung JSON.

Every number the job files use is printed here with the field it came from,
so the fio parameters are traceable to recorded evidence rather than guessed.
Usage: derive-params.py <rung.json> [--env]   (--env prints shell exports)
"""
import json
import sys


def kib4(n: float) -> int:
    """Round a byte count to the nearest 4 KiB multiple (O_DIRECT alignment)."""
    return max(4096, int(round(n / 4096)) * 4096)


def main() -> None:
    path = sys.argv[1]
    env = "--env" in sys.argv
    rung = json.load(open(path))
    aio = rung["storage_attribution"]["construction"]["application_io"]
    tot = aio["totals"]
    edges = rung["live_edges"]
    r_b, r_c = tot["read_bytes"], tot["read_calls"]
    w_b, w_c = tot["write_bytes"], tot["write_calls"]
    fs = tot["fsync_calls"]
    say = (lambda *a: None) if env else print
    say(f"source: {path}  rung={rung['scale']} live_edges={edges}")
    say("field                                    value")
    say(f"totals.read_bytes                        {r_b}")
    say(f"totals.read_calls                        {r_c}")
    say(f"totals.write_bytes                       {w_b}")
    say(f"totals.write_calls                       {w_c}")
    say(f"totals.fsync_calls                       {fs}")
    say(f"-> mean read call    (read_bytes/read_calls)     {r_b / r_c / 1024:.1f} KiB")
    say(f"-> mean write call   (write_bytes/write_calls)   {w_b / w_c / 1024:.1f} KiB")
    say(f"-> read share by calls (read_calls/(read+write)) {100 * r_c / (r_c + w_c):.1f} %  -> rwmixread")
    say(f"-> read share by bytes                           {100 * r_b / (r_b + w_b):.1f} %")
    say(f"-> writes per fsync  (write_calls/fsync_calls)   {w_c / fs:.2f}  -> fsync=")
    say(f"-> bytes per fsync   (write_bytes/fsync_calls)   {w_b / fs / 1024:.0f} KiB")
    say(f"-> fsyncs per M edges                            {fs / edges * 1e6:.0f}")
    # Block-size distribution: one bucket per construction phase, weighted by call count.
    rs, ws = {}, {}
    for name, p in aio["phases"].items():
        if p["read_calls"]:
            rs[name] = (kib4(p["read_bytes"] / p["read_calls"]), p["read_calls"])
        if p["write_calls"]:
            ws[name] = (kib4(p["write_bytes"] / p["write_calls"]), p["write_calls"])

    def split(d, total):
        parts = []
        say("   phase                                   mean-size  calls  weight")
        for name, (sz, calls) in sorted(d.items(), key=lambda kv: -kv[1][1]):
            w = round(100 * calls / total)
            if w == 0:
                continue
            say(f"   {name:40s} {sz // 1024:5d} KiB {calls:8d} {w:3d}%")
            parts.append((sz, w))
        drift = 100 - sum(w for _, w in parts)
        parts[0] = (parts[0][0], parts[0][1] + drift)  # rounding residue to the largest bucket
        return "/".join(f"{sz // 1024}k/{w}" for sz, w in parts)

    say("read block-size split (phases[*].read_bytes/read_calls, weight=read_calls):")
    rsplit = split(rs, r_c)
    say("write block-size split (phases[*].write_bytes/write_calls, weight=write_calls):")
    wsplit = split(ws, w_c)
    say(f"-> bssplit reads  = {rsplit}")
    say(f"-> bssplit writes = {wsplit}")
    if env:
        print(f"export F1_RWMIXREAD={round(100 * r_c / (r_c + w_c))}")
        print(f"export F1_FSYNC_EVERY={round(w_c / fs)}")
        print(f"export F1_BSSPLIT='{rsplit},{wsplit}'")


if __name__ == "__main__":
    main()
