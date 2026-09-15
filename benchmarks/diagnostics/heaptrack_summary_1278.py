"""Summarize Heaptrack 1.5 file-v3 allocation events for issue #1278.

Run with one .zst trace path. Output contains aggregate bytes/calls, not stack
paths or graph values. Per-category peaks occur independently; do not sum them.
Cross-check event count, global peak and final live bytes with heaptrack_print.
"""

import collections
import json
from pathlib import Path
import subprocess
import sys

path = Path(sys.argv[1])
strings = [""]
ips = [[]]
traces = [[]]
allocs = []
live = collections.Counter()
peak = collections.Counter()
counts = collections.Counter()
global_live = 0
global_peak = 0
at_peak = {}
time_ms = 0
events = []
proc = subprocess.Popen(["zstdcat", str(path)], stdout=subprocess.PIPE, text=True)
for line in proc.stdout:
    parts = line.rstrip("\n").split(" ")
    kind = parts[0]
    if kind == "v":
        assert parts[2] == "3"
    elif kind == "s":
        strings.append(line.rstrip("\n").split(" ", 2)[2])
    elif kind == "i":
        frame = []
        for offset in range(3, len(parts), 3):
            if parts[offset]:
                frame.append(strings[int(parts[offset], 16)])
        ips.append(frame)
    elif kind == "t":
        traces.append(ips[int(parts[1], 16)] + traces[int(parts[2], 16)])
    elif kind == "a":
        size = int(parts[1], 16)
        frames = traces[int(parts[2], 16)]
        tag = "other"
        for candidate in [
            "RunCursor",
            "decode_csr",
            "write_csr_shard",
            "ShardedCsrWriter",
            "EntryGroup",
        ]:
            if any(candidate in f for f in frames):
                tag = candidate
                break
        if tag == "RunCursor" and not any("bufreader" in frame.lower() for frame in frames):
            tag = "RunCursor_other"
        allocs.append((size, tag))
    elif kind in {"+", "-"}:
        size, tag = allocs[int(parts[1], 16)]
        change = size * (1 if kind == "+" else -1)
        global_live += change
        live[tag] += change
        assert live[tag] >= 0
        if kind == "+":
            counts[tag] += 1
        peak[tag] = max(peak[tag], live[tag])
        if global_live > global_peak:
            global_peak = global_live
            at_peak = dict(live)
    elif kind == "c":
        time_ms = int(parts[1], 16)
        events.append({"time_ms": time_ms, "total": global_live, **dict(live)})
assert proc.wait() == 0
out = {
    "format": "heaptrack-1.5-file-v3",
    "allocation_events": sum(counts.values()),
    "global_peak_live_heap_bytes": global_peak,
    "categories_at_global_peak_bytes": at_peak,
    "category_independent_peak_bytes": dict(peak),
    "category_allocation_calls": dict(counts),
    "final_live_bytes": global_live,
    "timeline": events,
}
path.with_suffix(".summary.json").write_text(json.dumps(out, indent=2) + "\n")
print(json.dumps({k: v for k, v in out.items() if k != "timeline"}, indent=2))
