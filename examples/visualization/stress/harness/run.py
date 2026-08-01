#!/usr/bin/env python3
"""On-demand visualization stress harness for issue #299.

Not invoked by PR/push/scheduled/required/release CI. Maintainers run locally
or via the workflow_dispatch-only GitHub Actions workflow.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import resource
import sys
import traceback
from typing import Any

STRESS_ROOT = Path(__file__).resolve().parents[1]
if str(STRESS_ROOT) not in sys.path:
    sys.path.insert(0, str(STRESS_ROOT))

from harness.adapters import ADAPTERS  # noqa: E402
from harness.contract import build_step_projection  # noqa: E402
from harness.schema import OPTIONS, empty_result, validate_result_record  # noqa: E402


def _peak_rss_mb() -> float | None:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if usage <= 0:
        return None
    # Linux: KB; macOS: bytes
    if sys.platform == "darwin":
        return usage / (1024 * 1024)
    return usage / 1024


def _load_ladder() -> dict[str, Any]:
    return json.loads((STRESS_ROOT / "size_ladder.json").read_text(encoding="utf-8"))


def _env_manifest() -> dict[str, Any]:
    versions: dict[str, Any] = {
        "python": sys.version,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python_implementation": platform.python_implementation(),
    }
    try:
        import graphforge

        versions["graphforge"] = getattr(graphforge, "__version__", "unknown")
    except Exception as exc:
        versions["graphforge"] = f"unavailable: {exc}"
    for pkg in ("plotly", "jaal", "pyvis", "pandas", "networkx"):
        try:
            mod = __import__(pkg)
            versions[pkg] = getattr(mod, "__version__", "unknown")
        except Exception:  # noqa: PERF203 — optional deps probed independently
            versions[pkg] = "not-installed"
    try:
        import subprocess

        node = subprocess.run(["node", "-v"], capture_output=True, text=True, check=False)
        versions["node"] = node.stdout.strip() or node.stderr.strip()
    except Exception as exc:
        versions["node"] = f"unavailable: {exc}"
    mem_bytes = (
        os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") if hasattr(os, "sysconf") else None
    )
    try:
        if mem_bytes is None:
            raise AttributeError
        versions["host_memory_mb"] = round(mem_bytes / (1024 * 1024))
    except Exception:
        versions["host_memory_mb"] = None
    versions["cpu_count"] = os.cpu_count()
    return versions


def run_matrix(
    *,
    options: list[str],
    max_step_nodes: int | None,
    use_graphforge: bool,
    output_dir: Path,
) -> dict[str, Any]:
    ladder = _load_ladder()
    seed = int(ladder["seed"])
    timeout_s = float(ladder["stopping"]["per_option_timeout_seconds"])
    stop_on_failure = bool(ladder["stopping"]["ladder_stop_on_first_failure_or_timeout"])
    rss_limit = float(ladder["stopping"]["max_peak_rss_mb"])

    steps = [
        step
        for step in ladder["steps"]
        if max_step_nodes is None or int(step["target_nodes"]) <= max_step_nodes
    ]

    results: list[dict[str, Any]] = []
    stopped: dict[str, str] = {}

    for step in steps:
        step_id = step["id"]
        target_nodes = int(step["target_nodes"])
        projection, gf_seconds = build_step_projection(
            target_nodes, seed, use_graphforge=use_graphforge
        )
        node_count = len(projection.nodes)
        edge_count = len(projection.edges)

        for option in options:
            if option in stopped:
                record = empty_result(
                    option=option,
                    runtime=ADAPTERS[option][0],
                    step_id=step_id,
                    node_count=node_count,
                    edge_count=edge_count,
                    seed=seed,
                    status="skipped",
                )
                record["error"] = f"stopped earlier: {stopped[option]}"
                results.append(record)
                continue

            runtime, adapter = ADAPTERS[option]
            record = empty_result(
                option=option,
                runtime=runtime,
                step_id=step_id,
                node_count=node_count,
                edge_count=edge_count,
                seed=seed,
                status="success",
            )
            record["graphforge_projection_seconds"] = gf_seconds
            record["dataset_id"] = projection.dataset_id
            record["dataset_checksum"] = projection.dataset_checksum

            try:
                # Soft timeout via alarm on Unix when available.
                def _timeout_handler(signum, frame):  # noqa: ARG001
                    raise TimeoutError(f"exceeded {timeout_s}s")

                if hasattr(__import__("signal"), "SIGALRM"):
                    import signal

                    signal.signal(signal.SIGALRM, _timeout_handler)
                    signal.setitimer(signal.ITIMER_REAL, timeout_s)
                try:
                    outcome = adapter(projection)
                finally:
                    if hasattr(__import__("signal"), "SIGALRM"):
                        import signal

                        signal.setitimer(signal.ITIMER_REAL, 0)

                record["viz_prep_seconds"] = outcome.get("viz_prep_seconds")
                record["renderer_init_seconds"] = outcome.get("renderer_init_seconds")
                record["payload_bytes"] = outcome.get("payload_bytes")
                record["divergence_notes"] = outcome.get("divergence_notes")
                record["artifact_kind"] = outcome.get("artifact_kind")
                rss = _peak_rss_mb()
                record["peak_rss_mb"] = None if rss is None else round(rss, 3)
                if rss is not None and rss > rss_limit:
                    record["status"] = "resource_limit"
                    record["error"] = f"peak_rss_mb {rss:.1f} exceeded limit {rss_limit}"
                    if stop_on_failure:
                        stopped[option] = record["error"]
            except TimeoutError as exc:
                record["status"] = "timeout"
                record["error"] = str(exc)
                record["peak_rss_mb"] = _peak_rss_mb()
                if stop_on_failure:
                    stopped[option] = str(exc)
            except Exception as exc:
                record["status"] = "failure"
                record["error"] = f"{type(exc).__name__}: {exc}"
                record["traceback"] = traceback.format_exc(limit=5)
                record["peak_rss_mb"] = _peak_rss_mb()
                if stop_on_failure:
                    stopped[option] = record["error"]

            violations = validate_result_record(record)
            if violations:
                record["schema_warnings"] = violations
            results.append(record)

    report = {
        "issue": 299,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "disclaimer": (
            "Hosted-runner / local numbers are comparative observations for this "
            "environment only — not hardware-independent benchmarks or production "
            "capacity guarantees. GraphForge projection time is recorded separately "
            "from visualization preparation and renderer initialization."
        ),
        "environment": _env_manifest(),
        "ladder": {
            "seed": seed,
            "stopping": ladder["stopping"],
            "steps": steps,
        },
        "results": results,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = output_dir / f"results-{stamp}.json"
    out_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    latest = output_dir / "results-latest.json"
    latest.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_markdown_summary(report, output_dir / "REPORT.generated.md")
    return report


def _write_markdown_summary(report: dict[str, Any], path: Path) -> None:
    lines = [
        "# Visualization limits — generated comparison",
        "",
        report["disclaimer"],
        "",
        f"Generated: `{report['generated_at']}`",
        "",
        "## Environment",
        "",
        "```json",
        json.dumps(report["environment"], indent=2, sort_keys=True),
        "```",
        "",
        "## Results",
        "",
        "| option | step | nodes | edges | status | gf_s | prep_s | init_s | rss_mb | payload_B |",
        "| --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in report["results"]:
        lines.append(
            "| {option} | {step_id} | {node_count} | {edge_count} | {status} | {gf} | {prep} | {init} | {rss} | {payload} |".format(
                option=row["option"],
                step_id=row["step_id"],
                node_count=row["node_count"],
                edge_count=row["edge_count"],
                status=row["status"],
                gf=_fmt(row.get("graphforge_projection_seconds")),
                prep=_fmt(row.get("viz_prep_seconds")),
                init=_fmt(row.get("renderer_init_seconds")),
                rss=_fmt(row.get("peak_rss_mb")),
                payload=row.get("payload_bytes") if row.get("payload_bytes") is not None else "",
            )
        )
    lines.extend(["", "## Per-option largest success", ""])
    for option in OPTIONS:
        successes = [
            r for r in report["results"] if r["option"] == option and r["status"] == "success"
        ]
        if not successes:
            lines.append(f"- **{option}**: no successful step")
            continue
        best = max(successes, key=lambda r: (r["node_count"], r["edge_count"]))
        lines.append(
            f"- **{option}**: step `{best['step_id']}` "
            f"({best['node_count']} nodes / {best['edge_count']} edges)"
        )
        note = best.get("divergence_notes")
        if note:
            lines.append(f"  - divergence: {note}")
        err_rows = [
            r
            for r in report["results"]
            if r["option"] == option and r["status"] in {"failure", "timeout", "resource_limit"}
        ]
        for err in err_rows:
            lines.append(f"  - {err['status']} at `{err['step_id']}`: {err.get('error')}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--options",
        default=",".join(OPTIONS),
        help="Comma-separated subset of options",
    )
    parser.add_argument(
        "--max-nodes",
        type=int,
        default=None,
        help="Only run ladder steps with target_nodes <= this value",
    )
    parser.add_argument(
        "--no-graphforge",
        action="store_true",
        help="Build projections without the native GraphForge binding (unit/dev)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=STRESS_ROOT / "results",
        help="Directory for machine-readable results",
    )
    args = parser.parse_args(argv)
    options = [part.strip() for part in args.options.split(",") if part.strip()]
    unknown = set(options) - set(OPTIONS)
    if unknown:
        parser.error(f"unknown options: {sorted(unknown)}")
    report = run_matrix(
        options=options,
        max_step_nodes=args.max_nodes,
        use_graphforge=not args.no_graphforge,
        output_dir=args.output,
    )
    successes = sum(1 for r in report["results"] if r["status"] == "success")
    print(
        f"wrote {args.output}/results-latest.json "
        f"({len(report['results'])} rows, {successes} successes)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
