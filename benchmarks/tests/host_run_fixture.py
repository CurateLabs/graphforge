"""Ordinary bounded native receipts for host controller/consumer tests."""

import json
from pathlib import Path

from graphforge_bench.progressive_run import Executables, assemble_rung_evidence
from tests.test_progressive_run import authoritative_receipts, benchexec, graphforge

ROOT = Path(__file__).resolve().parents[1]


def executables(base: Path) -> Executables:
    paths = [base / name for name in ("gf", "certify", "generator")]
    for path in paths:
        path.write_bytes(b"bounded executable fixture")
        path.chmod(0o755)
    return Executables(*paths, ROOT / ".venv/bin/python")


def write_host_bundle(output: Path, scale: int, plan: dict | None = None) -> None:
    from graphforge_bench.progressive_host_run import producer_digest
    from graphforge_bench.progressive_qualification import load_profiles, project
    from tests.test_progressive_host_run import host_capacity, host_result, passed_rung, sha256

    output.mkdir(parents=True, exist_ok=True)
    result = host_result(scale)
    if plan is None:
        tools = executables(output.parent)
        identities = result["identities"]
        identities.update(
            {
                "producer_sha256": producer_digest(ROOT),
                "host_profile_sha256": sha256(ROOT / "profiles/local-linux-cgroups-v2.json"),
                "profile_sha256": sha256(
                    ROOT
                    / f"profiles/graph500/s{scale}-{'local' if scale < 20 else 'provider'}.json"
                ),
                "generator": "sha256:" + sha256(ROOT / "runners/graph500-generator/src/main.rs"),
                "generator_executable_sha256": sha256(tools.generator),
                "gf_sha256": sha256(tools.gf),
                "certify_sha256": sha256(tools.certify),
                "benchexec_python_sha256": sha256(tools.benchexec_python),
            }
        )
    plan = plan or {
        "schema": "graphforge-progressive-host-run-plan/1",
        "rung": f"S{scale}",
        "execution": "native_linux_benchexec_host",
        "identities": result["identities"],
        "limits": {"wall_seconds": 14400, "memory_bytes": 4294967296, "cores": 16},
        "outputs": [
            f"s{scale}-{name}.json"
            for name in ("plan", "benchexec", "graphforge", "rung", "result")
        ],
        "claim": "engineering_evidence_only",
    }
    if scale >= 20 and not (output / f"s{scale}-projection.json").exists():
        profile = next(p for p in load_profiles() if p.scale == scale)
        projection = project(
            profile, [passed_rung(s) for s in profile.projection_sources], host_capacity()
        )
        path = output / f"s{scale}-projection.json"
        path.write_text(json.dumps(projection) + "\n")
        plan["identities"]["admitted_projection_sha256"] = sha256(path)
    receipts = authoritative_receipts(scale)
    # This shared fixture includes retained construction staging as an owner.
    # Keep its native union above every owner view and below their sum.
    receipts["reopen_proof"][-1]["retained_storage_bytes"] = 500
    receipts["reopen_proof"][-1]["transient_peak_storage_bytes"] = 600
    gf = graphforge(scale, receipts)
    gf["profile_id"] = plan["identities"]["profile_id"]
    authority = benchexec(gf)
    rung = assemble_rung_evidence(
        root=ROOT,
        scale=scale,
        graphforge=gf,
        benchexec=authority,
        profile_id=plan["identities"]["profile_id"],
        source="progressive_profile" if scale < 20 else "canonical_ladder",
    )
    for name, value in (
        ("plan", plan),
        ("graphforge", gf),
        ("benchexec", authority),
        ("rung", rung),
    ):
        (output / f"s{scale}-{name}.json").write_text(json.dumps(value) + "\n")
    result["identities"] = plan["identities"]
    result["artifacts"] = {
        f"{name}_sha256": sha256(output / f"s{scale}-{name}.json")
        for name in ("plan", "graphforge", "benchexec", "rung")
    }
    (output / f"s{scale}-result.json").write_text(json.dumps(result) + "\n")
