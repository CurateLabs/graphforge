"""Validate and summarize the #1606 quiet-host routing measurement set."""

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

print("\n## Staged routing, S18, 8 usable cores")
ratios = []
route_wall = []
route_cpu = []
for repeat in (1, 2, 3):
    run = root / "runs" / f"curve-s18-c8-r{repeat}"
    receipt = json.loads((run / "receipt-3-validate.json").read_text())
    regions = receipt["region_diagnostics"]["regions"]
    region = regions["import_command/validate/seal/shaping/shape_routing"]
    inclusive = region["inclusive"]
    residual = region["residual"]
    wall = inclusive["wall_ns"] / 1e9
    cpu = inclusive["process_cpu_ns"] / 1e9
    ratio = cpu / wall
    ratios.append(ratio)
    route_wall.append(wall)
    route_cpu.append(cpu)
    residual_wall = residual["wall_ns"] / 1e9
    residual_cpu = residual["process_cpu_ns"] / 1e9
    print(
        f"run {repeat}: route inclusive wall={wall:.3f}s CPU={cpu:.3f}s "
        f"CPU/wall={ratio:.4f}; residual wall={residual_wall:.3f}s "
        f"CPU={residual_cpu:.3f}s"
    )
median = statistics.median(ratios)
print(f"Median routing CPU/wall: {median:.4f} (required >= 2.0)")
print(
    "Median routing inclusive wall/CPU: "
    f"{statistics.median(route_wall):.3f}s / {statistics.median(route_cpu):.3f}s"
)
print("\n## All-run host pressure and memory")
memory_mib = []
for name in sorted(expected):
    run = root / "runs" / name
    measured = summary.runexec(run)
    memory_mib.append(measured["memory_mib"])
    receipt = json.loads((run / "receipt-3-validate.json").read_text())
    construction = receipt["construction"]
    shape_io = construction["application_io"]["phases"]["shape_consume_reauthentication"]
    print(
        f"{name}: wall={measured['wall']:.2f}s cpu={measured['cpu']:.2f}s "
        f"cpu-pressure={measured['cpu_pressure']:.3f}s "
        f"io-pressure={measured['io_pressure']:.3f}s peak={measured['memory_mib']:.0f}MiB "
        f"shape-I/O read={shape_io['read_bytes']}B/{shape_io['read_calls']} calls "
        f"write={shape_io['write_bytes']}B/{shape_io['write_calls']} calls "
        f"cache-release={construction['cache_release_operations']} "
        f"cache-window={construction['peak_cache_release_window_bytes']}B"
    )
memory_gate = max(memory_mib) <= 4000
print(
    f"4,000 MiB memory limit: {'PASS' if memory_gate else 'FAIL'} "
    f"(observed maximum {max(memory_mib):.0f} MiB)"
)
print("\n## Whole-ingest adoption gate")
improvements = {}
ceilings = {}
for scale in ("s18", "s20"):
    rounds = (root / "runs").glob(f"ab-{scale}-r*-base")
    rows = {}
    for base_run in rounds:
        repeat = base_run.name.split("-")[2]
        base = summary.runexec(base_run)["wall"]
        candidate = summary.runexec(root / "runs" / f"ab-{scale}-{repeat}-cand")["wall"]
        rows[repeat] = 1 - candidate / base
    base_median = statistics.median(
        summary.runexec(root / "runs" / f"ab-{scale}-r{repeat}-base")["wall"]
        for repeat in (1, 2, 3)
    )
    candidate_median = statistics.median(
        summary.runexec(root / "runs" / f"ab-{scale}-r{repeat}-cand")["wall"]
        for repeat in (1, 2, 3)
    )
    improvements[scale] = 1 - candidate_median / base_median
    print(
        f"{scale.upper()}: baseline median={base_median:.2f}s, "
        f"candidate median={candidate_median:.2f}s, "
        f"median wall improvement={improvements[scale]:+.1%}; per-pair improvements={rows}"
    )
    baseline_names = [root / "runs" / f"ab-{scale}-r{repeat}-base" for repeat in (1, 2, 3)]
    route_fractions = []
    for base_run in baseline_names:
        receipt = json.loads((base_run / "receipt-3-validate.json").read_text())
        route = receipt["region_diagnostics"]["regions"][
            "import_command/validate/seal/shaping/shape_routing"
        ]["inclusive"]
        route_wall_seconds = route["wall_ns"] / 1e9
        ingest_wall_seconds = summary.runexec(base_run)["wall"]
        route_fractions.append(route_wall_seconds / ingest_wall_seconds)
    fraction = statistics.median(route_fractions)
    ceilings[scale] = 1 / (1 - fraction)
    print(
        f"{scale.upper()} ideal ceiling if routing vanished: {ceilings[scale]:.3f}x "
        f"(route/ingest fractions={route_fractions})"
    )
route_gate = median >= 2.0
ingest_gate = all(value >= 0.10 for value in improvements.values())
answers_match = all(
    len(
        {
            summary.answers(root / "runs" / name)
            for name in expected
            if name.startswith(f"ab-s{scale}-")
        }
    )
    == 1
    for scale in (18, 20)
)
print(f"route CPU/wall gate: {'PASS' if route_gate else 'FAIL'}")
print(f"S18 and S20 ingest wall gates: {'PASS' if ingest_gate else 'FAIL'}")
print(f"query counts and full-scan digests: {'PASS' if answers_match else 'FAIL'}")
print(
    f"adoption: {'GO' if route_gate and ingest_gate and answers_match and memory_gate else 'NO-GO'}"
)
summary.curve(root)
