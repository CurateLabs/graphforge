"""Build and render a GDC scorecard card in #952's format.

The card headlines the largest passing rung of one suite's ladder and names
what stopped the next rung. Every number is read from that rung's published
documents, whose SHA-256s its result lists: load and conversion wall time from
the BenchExec documents, latency and throughput from the query driver clock's
evidence (refused by ``assert_query_latency_authority`` otherwise), peak RSS
from each phase's process high-water mark under BenchExec, on-disk bytes from
the product's storage-attribution receipt, and correctness from the reference
check. The card carries the LDBC fair-use label, the one-line variance from the
specification, every variance (rewrites, spec variances, reference readings,
count discrepancies) and the CC-BY 4.0 attribution.

Graphalytics has its own metric shape: per-algorithm processing time ``Tp``
(the mean of three driver-clock runs), the shared load time ``Tl``, and EVPS.
Makespan is not measured yet, and the card says so.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import math
import os
from pathlib import Path
from typing import Any

from graphforge_bench.gdc_measurement_policy import (
    CARD_METRIC_SOURCES,
    GdcMeasurementBoundaryError,
    assert_card_metric_sources,
    assert_query_latency_authority,
    nearest_rank,
)
from graphforge_bench.gdc_rung_inputs import (
    LadderSpec,
    RungInputError,
    read_json,
    sha256_file,
    validate_schema,
)
from graphforge_bench.progressive_run import publish_json_no_clobber

CARD_SCHEMA = "graphforge-gdc-scorecard-card/1"
FAIR_USE_LABEL = "These are not LDBC Benchmark Results."
GRAPHALYTICS_RUNS = 3
LABEL_WIDTH = 14


class CardError(ValueError):
    """The card cannot be built from the ladder's evidence."""

    def __init__(self, cause: str, message: str) -> None:
        super().__init__(message)
        self.cause = cause


def _results(spec: LadderSpec, output_dir: Path) -> list[Mapping[str, Any]]:
    """The ladder's results in rung order, up to and including the first non-pass."""
    results: list[Mapping[str, Any]] = []
    for rung in spec.document["rungs"]:
        path = output_dir / f"{spec.suite_id}-{rung['id']}-result.json"
        if not path.is_file():
            break
        result = read_json(path)
        validate_schema(spec.root, "gdc-rung-result.json", result)
        results.append(result)
        if result["status"] != "passed":
            break
    return results


def _documents(output_dir: Path, result: Mapping[str, Any]) -> dict[str, Any]:
    documents = {}
    for name, digest in result["documents"].items():
        path = output_dir / name
        if not path.is_file() or sha256_file(path) != digest:
            raise CardError("evidence_digest_mismatch", f"{name} is missing or changed")
        documents[name] = read_json(path)
    return documents


def _benchexec(
    spec: LadderSpec, documents: Mapping[str, Any], result: Mapping[str, Any], phase: str
) -> Mapping[str, Any]:
    name = result["phases"][phase]["benchexec_document"]
    document = documents.get(name)
    if document is None:
        raise CardError("benchexec_document_missing", f"no BenchExec document for {phase}")
    validate_schema(spec.root, "benchexec-run-evidence.json", document)
    if document["outcome"] != "passed":
        raise CardError("benchexec_not_passed", f"{phase} BenchExec outcome {document['outcome']}")
    return document


def _on_disk_bytes(query: Mapping[str, Any]) -> int:
    receipt = query["graphforge"].get("receipts", {}).get("storage_attribution", {})
    storage = receipt.get("storage") if isinstance(receipt, Mapping) else None
    if (
        not isinstance(storage, Mapping)
        or storage.get("contract") != "graphforge-storage-attribution/1"
        or receipt.get("reopen_agrees") is not True
        or not isinstance(storage.get("allocated_physical_bytes"), int)
    ):
        raise CardError("storage_receipt_invalid", "no agreeing storage-attribution receipt")
    return int(storage["allocated_physical_bytes"])


def _latency(evidence: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    latencies = [
        sample["latency_ns"]
        for variant in evidence["variants"]
        for sample in variant["samples"]
        if sample["status"] == "measured"
    ]
    seconds = math.fsum(latencies) / 1e9
    return (
        {
            "p50_ns": nearest_rank(latencies, 50),
            "p95_ns": nearest_rank(latencies, 95),
            "samples": len(latencies),
        },
        {
            "operations": len(latencies),
            "measured_seconds": seconds,
            "operations_per_second": len(latencies) / seconds,
        },
    )


def _graphalytics(
    evidence: Mapping[str, Any], load_wall: float, nodes: int, edges: int
) -> dict[str, Any]:
    algorithms = []
    for variant in evidence["variants"]:
        latencies = [s["latency_ns"] for s in variant["samples"] if s["status"] == "measured"]
        if len(latencies) != GRAPHALYTICS_RUNS or len(variant["samples"]) != GRAPHALYTICS_RUNS:
            raise CardError(
                "graphalytics_runs_invalid",
                f"{variant['query_id']}: Tp is the mean of {GRAPHALYTICS_RUNS} measured runs",
            )
        tp = math.fsum(latencies) / len(latencies) / 1e9
        algorithms.append(
            {
                "algorithm": variant["query_id"],
                "tp_seconds": tp,
                "runs": len(latencies),
                "evps": (nodes + edges) / tp,
                "makespan_seconds": None,
            }
        )
    return {"tl_seconds": load_wall, "algorithms": algorithms}


def _next_rung(
    spec: LadderSpec, index: int, results: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    rungs = spec.document["rungs"]
    if index + 1 >= len(rungs):
        return {"label": None, "outcome": "top_of_ladder", "cause": None}
    label = rungs[index + 1]["label"]
    if index + 1 >= len(results):
        return {"label": label, "outcome": "not_attempted", "cause": None}
    result = results[index + 1]
    if result["status"] == "not_admitted":
        return {"label": label, "outcome": "not_admitted", "cause": result["failure"]["cause"]}
    return {"label": label, "outcome": "typed_failure", "cause": result["failure"]["cause"]}


def build_card(spec: LadderSpec, output_dir: Path) -> dict[str, Any]:
    results = _results(spec, output_dir)
    passed = [result for result in results if result["status"] == "passed"]
    if not passed:
        raise CardError("no_passing_rung", f"{spec.suite_id} has no passing rung to headline")
    headline = passed[-1]
    index = len(passed) - 1
    rung = spec.rung(headline["rung_id"])
    prefix = f"{spec.suite_id}-{headline['rung_id']}"
    documents = _documents(output_dir, headline)
    convert = _benchexec(spec, documents, headline, "convert")
    load = _benchexec(spec, documents, headline, "load")
    query = _benchexec(spec, documents, headline, "query")
    evidence = documents[f"{prefix}-query-evidence.json"]
    assert_query_latency_authority(evidence)
    if evidence["status"] != "passed":
        raise CardError("query_evidence_failed", "a passing rung's query evidence must pass")
    correctness = documents[f"{prefix}-correctness.json"]
    reconciliation = evidence["reconciliation"]
    nodes, edges = reconciliation["nodes"]["observed"], reconciliation["edges"]["observed"]
    counts = headline["counts"]
    workload = read_json(spec.resolve(rung["workload"]))
    load_wall = float(load["authority"]["wall_seconds"])
    latency, throughput = _latency(evidence)
    graphalytics = None
    if spec.document["metric_shape"] == "graphalytics":
        graphalytics = _graphalytics(evidence, load_wall, nodes, edges)
        latency, throughput = None, None
    card = {
        "schema": CARD_SCHEMA,
        "suite_id": spec.suite_id,
        "certification": False,
        "graphforge": {
            "version": headline["identities"]["graphforge_version"],
            "commit": headline["identities"]["commit"],
        },
        "dataset": {"rung_id": headline["rung_id"], "label": headline["label"]},
        "fair_use_label": FAIR_USE_LABEL,
        "variance_line": spec.document["variance_line"],
        "hardware": headline["host"],
        "graph": {
            "nodes": nodes,
            "edges": edges,
            "published": counts["published"],
            "published_snapshot": counts["published_snapshot"],
        },
        "load": {
            "wall_seconds": load_wall,
            "conversion_wall_seconds": float(convert["authority"]["wall_seconds"]),
        },
        "on_disk_bytes": _on_disk_bytes(query),
        "coverage": {
            "supported": len(workload["variants"]),
            "total": len(workload["variants"]) + len(rung["refused"]),
            "refused": list(rung["refused"]),
        },
        "throughput": throughput,
        "latency": latency,
        "graphalytics": graphalytics,
        "peak_rss": {
            "load_bytes": int(load["graphforge"]["peak_rss_bytes"]),
            "query_bytes": int(query["graphforge"]["peak_rss_bytes"]),
        },
        "correctness": {
            "status": correctness["status"],
            "matched": correctness["matched"],
            "checked": correctness["checked"],
            "reference_source": correctness["reference_source"],
            "reference_note": rung.get("reference_note"),
        },
        "next_rung": _next_rung(spec, index, results),
        "variances": [
            *spec.document["variances"],
            *rung["variances"],
            *counts["discrepancies"],
        ],
        "attribution": spec.document["attribution"],
        "metric_sources": dict(CARD_METRIC_SOURCES),
        "evidence": {
            f"{prefix}-result.json": sha256_file(output_dir / f"{prefix}-result.json"),
            **headline["documents"],
        },
    }
    validate_schema(spec.root, "gdc-scorecard-card.json", card)
    return card


def _count(value: int) -> str:
    return f"{value:,}"


def _seconds(value: float) -> str:
    return f"{value:.1f} s" if value >= 1 else f"{value * 1000:.1f} ms"


def _bytes(value: int) -> str:
    for unit, size in (("GiB", 1024**3), ("MiB", 1024**2), ("KiB", 1024)):
        if value >= size:
            return f"{value / size:.2f} {unit}"
    return f"{value} B"


def _duration_ns(value: int) -> str:
    for unit, size in (("s", 1e9), ("ms", 1e6), ("µs", 1e3)):
        if value >= size:
            return f"{value / size:.2f} {unit}"
    return f"{value} ns"


def _line(label: str, text: str) -> str:
    return f"{label + ':':<{LABEL_WIDTH}}{text}"


def _graph_line(graph: Mapping[str, Any]) -> str:
    loaded = (graph["nodes"], graph["edges"])
    published = (graph["published"]["nodes"], graph["published"]["edges"])
    snapshot = graph["published_snapshot"]
    text = f"{_count(loaded[0])} nodes / {_count(loaded[1])} edges loaded (LDBC published: "
    text += f"{_count(published[0])} / {_count(published[1])}"
    if loaded == published:
        return text + "; exact match)"
    if snapshot is not None and loaded == (snapshot["nodes"], snapshot["edges"]):
        return text + " whole network, snapshot exact match)"
    return text + "; reconciled to the pinned archive's records, see variances)"


def _coverage_line(coverage: Mapping[str, Any]) -> str:
    by_cause: dict[str, list[str]] = {}
    for refusal in coverage["refused"]:
        by_cause.setdefault(refusal["cause"], []).append(refusal["query_id"])
    refused = "; ".join(f"{', '.join(ids)} ({cause})" for cause, ids in by_cause.items())
    return f"{coverage['supported']}/{coverage['total']} queries; refused: {refused or 'none'}"


def _correctness_line(correctness: Mapping[str, Any]) -> str:
    if correctness["status"] == "not_reference_checked":
        note = correctness["reference_note"]
        return f"not reference-checked ({note})" if note else "not reference-checked"
    return (
        f"{correctness['matched']}/{correctness['checked']} supported results match "
        f"{correctness['reference_source']}"
    )


def _next_line(next_rung: Mapping[str, Any]) -> str:
    outcome = next_rung["outcome"]
    if outcome == "top_of_ladder":
        return "none — top of the declared ladder"
    label = next_rung["label"]
    if outcome == "not_attempted":
        return f"{label} — not attempted"
    if outcome == "not_admitted":
        return f"{label} — not admitted ({next_rung['cause']})"
    return f"{label} — typed failure ({next_rung['cause']})"


def render_card(card: Mapping[str, Any]) -> str:
    """The card text in #952's format, refusing a card whose numbers lack their authority."""
    assert_card_metric_sources(card)
    if card["fair_use_label"] != FAIR_USE_LABEL or card["certification"] is not False:
        raise CardError("fair_use_label_missing", "every card states it is not an LDBC result")
    hardware = card["hardware"]
    title = (
        f"GraphForge {card['graphforge']['version']} ({card['graphforge']['commit'][:12]}) — "
        f"GDC {card['suite_id']}, {card['dataset']['label']}, unaudited"
    )
    lines = [
        title,
        f"{card['fair_use_label']} {card['variance_line']}",
        _line(
            "Hardware",
            f"{hardware['label']} — {hardware['cores']} cores, "
            f"{hardware['memory_bytes'] // 1024**3} GiB, {hardware['storage_medium']} "
            f"{hardware['filesystem']}, {hardware['os']}",
        ),
        _line("Graph", _graph_line(card["graph"])),
        _line(
            "Load time",
            f"{_seconds(card['load']['wall_seconds'])}   (CSV→Parquet conversion reported "
            f"separately: {_seconds(card['load']['conversion_wall_seconds'])})",
        ),
        _line("On disk", _bytes(card["on_disk_bytes"])),
        _line("Coverage", _coverage_line(card["coverage"])),
    ]
    graphalytics = card["graphalytics"]
    if graphalytics is None:
        throughput = card["throughput"]
        rate = throughput["operations_per_second"]
        rate_text = f"{rate:.1f} ops/s" if rate >= 1 else f"{rate * 3600:.1f} queries/hour"
        latency = card["latency"]
        lines += [
            _line(
                "Throughput",
                f"{rate_text} (read-only, single client; not the LDBC power or throughput score)",
            ),
            _line(
                "Latency",
                f"p50 {_duration_ns(latency['p50_ns'])}  p95 {_duration_ns(latency['p95_ns'])}  "
                "(per query in the evidence; all-query on the card)",
            ),
        ]
    else:
        algorithms = graphalytics["algorithms"]
        lines += [
            _line(
                "Tl", f"{_seconds(graphalytics['tl_seconds'])} (one load serves every algorithm)"
            ),
            _line(
                "Tp",
                ", ".join(f"{a['algorithm']} {_seconds(a['tp_seconds'])}" for a in algorithms)
                + f" (mean of {GRAPHALYTICS_RUNS} driver-clock runs)",
            ),
            _line("Makespan", "not measured"),
            _line("EVPS", ", ".join(f"{a['algorithm']} {a['evps']:.3g}" for a in algorithms)),
        ]
    lines += [
        _line(
            "Peak RAM",
            f"load {_bytes(card['peak_rss']['load_bytes'])}, query "
            f"{_bytes(card['peak_rss']['query_bytes'])} (largest single-process peak RSS "
            "under BenchExec)",
        ),
        _line("Correctness", _correctness_line(card["correctness"])),
        _line("Next rung", _next_line(card["next_rung"])),
    ]
    if card["variances"]:
        lines.append("Variances:")
        lines += [
            f"  - {variance['kind']} ({variance['subject']}): {variance['text']}"
            for variance in card["variances"]
        ]
    lines.append(_line("Attribution", card["attribution"]))
    return "\n".join(lines) + "\n"


def write_card(root: Path, spec: LadderSpec, output_dir: Path) -> dict[str, Path]:
    """Publish `<suite>-card.json` and `<suite>-card.txt` once; neither is overwritten."""
    del root  # the spec carries the benchmarks root
    try:
        card = build_card(spec, output_dir)
    except (RungInputError, GdcMeasurementBoundaryError) as error:
        raise CardError(error.cause, str(error)) from error
    text = render_card(card)
    json_path = output_dir / f"{spec.suite_id}-card.json"
    text_path = output_dir / f"{spec.suite_id}-card.txt"
    publish_json_no_clobber(json_path, card)
    descriptor = os.open(text_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(text)
    return {"json": json_path, "text": text_path}
