"""Validate and summarize the #1600 quiet-host measurement set."""

import importlib.util
import json
from pathlib import Path
import statistics
import sys

shared = Path(__file__).parent.parent / "core-scaling-1448" / "summarize_candidate.py"
spec = importlib.util.spec_from_file_location("core_scaling", shared)
assert spec is not None and spec.loader is not None
summary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(summary)

root = Path(sys.argv[1])
expected = {
    f"ab-s{scale}-r{repeat}-{arm}"
    for scale in (18, 20)
    for repeat in (1, 2, 3)
    for arm in ("base", "cand")
} | {
    f"curve-s18-c{cores}-r{repeat}"
    for cores in (1, 2, 4, 8, 16)
    for repeat in ((1, 2, 3) if cores in (1, 8) else (1,))
}
quiet = summary.accepted_runs(root)
assert set(quiet) == expected, "missing or unexpected completed runs"
assert all(quiet.values()), "a run ended on a busy host"
for name in sorted(expected):
    run = root / "runs" / name
    fields = dict(
        line.split("=", 1) for line in (run / "runexec.txt").read_text().splitlines() if "=" in line
    )
    assert fields["returnvalue"] == "0", f"failed ingest: {name}"

for scale in (18, 20):
    answers = {
        summary.answers(root / "runs" / name)
        for name in expected
        if name.startswith(f"ab-s{scale}-")
    }
    assert len(answers) == 1, f"query answers differ at S{scale}"
    summary.ab(root, f"s{scale}")

print("\n## Canonical encoding, S18, 8 usable cores")
ratios = []
for repeat in (1, 2, 3):
    run = root / "runs" / f"curve-s18-c8-r{repeat}"
    receipt = json.loads((run / "receipt-3-validate.json").read_text())
    regions = receipt["region_diagnostics"]["regions"]
    region = regions["import_command/validate/seal/canonical_encoding"]["inclusive"]
    wall = region["wall_ns"] / 1e9
    cpu = region["process_cpu_ns"] / 1e9
    ratios.append(cpu / wall)
    print(f"run {repeat}: wall={wall:.3f}s, CPU={cpu:.3f}s, CPU/wall={cpu / wall:.4f}")
median = statistics.median(ratios)
print(f"Median canonical-encoding CPU/wall: {median:.4f} (required >= 2.0)")
assert median >= 2.0, "canonical encoding CPU/wall criterion remains unmet"
summary.curve(root)
