"""Validate and summarize the #1607 append-lane quiet-host runs."""

import importlib.util
import json
from pathlib import Path
import statistics
import sys

SHARED = Path(__file__).parents[1] / "core-scaling-1448" / "summarize_candidate.py"
spec = importlib.util.spec_from_file_location("core_scaling", SHARED)
assert spec is not None and spec.loader is not None
core = importlib.util.module_from_spec(spec)
spec.loader.exec_module(core)

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
quiet = core.accepted_runs(root)
assert set(quiet) == expected, "missing or unexpected completed runs"
assert all(quiet.values()), "a run ended on a busy host"
for name in sorted(expected):
    run = root / "runs" / name
    fields = dict(
        line.strip().split("=", 1)
        for line in (run / "runexec.txt").read_text().splitlines()
        if "=" in line
    )
    assert fields["returnvalue"] == "0", f"failed ingest: {name}"
    assert core.runexec(run)["memory_mib"] <= 4_000, f"memory cap exceeded: {name}"

for scale in (18, 20):
    names = [f"ab-s{scale}-r{repeat}-{arm}" for repeat in (1, 2, 3) for arm in ("base", "cand")]
    answers = {core.answers(root / "runs" / name) for name in names}
    assert len(answers) == 1, f"query answer digests differ at S{scale}"


def region(run: Path, name: str) -> tuple[float, float, float, float]:
    receipt = json.loads((run / "receipt-3-validate.json").read_text())
    row = receipt["region_diagnostics"]["regions"][name]
    inclusive = row["inclusive"]
    residual = row["residual"]
    return (
        inclusive["wall_ns"] / 1e9,
        (inclusive.get("process_cpu_ns") or 0) / 1e9,
        residual["wall_ns"] / 1e9,
        (residual.get("process_cpu_ns") or 0) / 1e9,
    )


def append_profile(run: Path, arm: str) -> None:
    parent = "import_command/validate/append"
    phases = (
        [
            "append_input_digest",
            "append_fixed_run_preparation",
            "append_arrow_preparation",
            "append_durable",
        ]
        if arm == "base"
        else ["append_preparation", "append_durable"]
    )
    wall, cpu, residual_wall, residual_cpu = region(run, parent)
    print(
        f"  append inclusive={wall:.3f}s/{cpu:.3f}s CPU ({cpu / wall:.2f} CPU/wall); "
        f"residual={residual_wall:.3f}s/{residual_cpu:.3f}s"
    )
    for phase in phases:
        values = region(run, f"{parent}/{phase}")
        print(f"    {phase}: inclusive={values[0]:.3f}s/{values[1]:.3f}s CPU")


print("## Post-routing A/B")
for scale in (18, 20):
    walls = {"base": [], "cand": []}
    print(f"### S{scale}")
    for repeat in (1, 2, 3):
        for arm in ("base", "cand"):
            name = f"ab-s{scale}-r{repeat}-{arm}"
            run = root / "runs" / name
            values = core.runexec(run)
            walls[arm].append(values["wall"])
            print(
                f"{name}: wall={values['wall']:.2f}s CPU={values['cpu']:.2f}s "
                f"CPU/wall={values['cpu'] / values['wall']:.2f} peak={values['memory_mib']:.0f}MiB "
                f"CPU-pressure={values['cpu_pressure']:.2f}s "
                f"IO-pressure={values['io_pressure']:.2f}s"
            )
            append_profile(run, arm)
    baseline = statistics.median(walls["base"])
    candidate = statistics.median(walls["cand"])
    improvement = 1 - candidate / baseline
    print(
        f"S{scale} medians: base={baseline:.2f}s candidate={candidate:.2f}s "
        f"wall improvement={improvement:.1%} (gate >=10%)"
    )

print("## Candidate S18 core curve")
one = None
for cores in (1, 2, 4, 8, 16):
    names = sorted((root / "runs").glob(f"curve-s18-c{cores}-r*"))
    values = [core.runexec(run) for run in names]
    wall = statistics.median(item["wall"] for item in values)
    cpu = statistics.median(item["cpu"] for item in values)
    if cores == 1:
        one = wall
    print(
        f"{cores} CPUs ({len(values)} runs): wall={wall:.2f}s CPU={cpu:.2f}s "
        f"CPU/wall={cpu / wall:.2f} throughput={one / wall:.2f}x"
    )
    if cores == 8:
        append_ratios = [
            region(run, "import_command/validate/append")[1]
            / region(run, "import_command/validate/append")[0]
            for run in names
        ]
        print(
            f"  append CPU/wall={append_ratios}; median={statistics.median(append_ratios):.3f} "
            "(gate >=2.0)"
        )
