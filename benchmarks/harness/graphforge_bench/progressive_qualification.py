"""Declarative progressive Graph500 qualification and projection policy."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

WALL_LIMIT_SECONDS = 4 * 60 * 60
RSS_LIMIT_BYTES = 4 * 1024**3
VOLUME_LIMIT_BYTES = 500 * 1024**3
TIME_HEADROOM = 0.20
RSS_HEADROOM = 0.20
STORAGE_HEADROOM = 0.15
MAX_RSS_GROWTH_FRACTION = 0.10
PHASES = (
    "admission",
    "generate",
    "ingest",
    "reopen",
    "recount",
    "query",
    "export",
    "verify",
    "clean_import",
    "reopen_proof",
)
METRICS = (
    "wall_seconds",
    "peak_rss_bytes",
    "retained_storage_bytes",
    "transient_peak_storage_bytes",
    "logical_read_bytes",
    "logical_write_bytes",
    "physical_read_bytes",
    "physical_write_bytes",
    "reader_calls",
    "publication_work_units",
)


class QualificationError(ValueError):
    """Qualification input is missing, malformed, or contradictory."""


class Decision(StrEnum):
    ADMITTED = "admitted"
    REFUSED = "refused"


@dataclass(frozen=True)
class Profile:
    id: str
    scale: int
    execution: str
    projection_sources: tuple[int, int] | None


def profile_root() -> Path:
    return Path(__file__).resolve().parents[2] / "profiles" / "graph500"


def load_profiles(root: Path | None = None) -> tuple[Profile, ...]:
    import json

    from jsonschema import Draft202012Validator

    values = []
    profiles_root = root or profile_root()
    schema = json.loads(
        (profiles_root.parents[1] / "schemas/progressive-qualification-profile.json").read_text(
            encoding="utf-8"
        )
    )
    validator = Draft202012Validator(schema)
    expected = {
        18: ("graph500-s18-local", "local", None),
        19: ("graph500-s19-local", "local", None),
        20: ("graph500-s20-provider", "provider", [18, 19]),
        22: ("graph500-s22-provider", "provider", [19, 20]),
        26: ("graph500-s26-provider", "provider", [24, 25]),
    }
    for path in sorted(profiles_root.glob("*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        error = next(validator.iter_errors(raw), None)
        if error is not None:
            raise QualificationError(f"invalid progressive profile: {error.message}")
        gate = raw["gate"]
        sources = gate.get("projection_source_scales")
        if expected.get(raw["scale"]) != (raw["id"], raw["execution"], sources):
            raise QualificationError("profile identity, environment, or gate contradicts scale")
        values.append(
            Profile(
                id=raw["id"],
                scale=raw["scale"],
                execution=raw["execution"],
                projection_sources=tuple(sources) if sources is not None else None,
            )
        )
    if [value.scale for value in values] != [18, 19, 20, 22, 26]:
        raise QualificationError("progressive profiles must be S18,S19,S20,S22,S26")
    return tuple(values)


def select_next(
    profiles: Sequence[Profile],
    completed: Sequence[Mapping[str, Any]],
    provider_capacity: Mapping[str, Any] | None = None,
) -> Profile | None:
    """Return only the first unexecuted rung; a failure authorizes nothing larger."""
    by_scale = {int(item.get("scale", -1)): item for item in completed}
    for profile in profiles:
        evidence = by_scale.get(profile.scale)
        if evidence is None:
            if any(item.get("status") != "passed" for item in completed):
                return None
            if profile.execution == "provider":
                try:
                    evidence = project(profile, completed, provider_capacity)
                except QualificationError:
                    return None
                if evidence["decision"] != Decision.ADMITTED:
                    return None
            return profile
        if evidence.get("status") != "passed":
            return None
    return None


def _integer(metrics: Mapping[str, Any], name: str) -> int:
    value = metrics.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise QualificationError(f"missing or invalid metric: {name}")
    return value


def _validate_rung(item: Mapping[str, Any]) -> None:
    if item.get("status") != "passed" or item.get("correctness") is not True:
        raise QualificationError("projection sources must be completed and correct")
    if tuple(item.get("phases", ())) != PHASES:
        raise QualificationError("projection source did not run the ordinary lifecycle")
    scale = item.get("scale")
    if not isinstance(scale, int) or item.get("live_edges") != 16 * (1 << scale):
        raise QualificationError("projection source edge cardinality contradicts its scale")
    source = item.get("source")
    if source not in {"progressive_profile", "canonical_ladder"}:
        raise QualificationError("projection source provenance is missing")
    if not isinstance(item.get("profile_id"), str):
        raise QualificationError("projection source profile identity is missing")
    metrics = item.get("metrics")
    if not isinstance(metrics, Mapping):
        raise QualificationError("projection source metrics are missing")
    for name in METRICS:
        _integer(metrics, name)


def _ceil_ratio(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise QualificationError("projection denominator must be positive")
    return (numerator + denominator - 1) // denominator


def _project(low: Mapping[str, Any], high: Mapping[str, Any], target_edges: int, name: str) -> int:
    low_edges = int(low["live_edges"])
    high_edges = int(high["live_edges"])
    if not 0 < low_edges < high_edges < target_edges:
        raise QualificationError("projection sources and target must increase by live edge count")
    low_value = _integer(low["metrics"], name)
    high_value = _integer(high["metrics"], name)
    delta = max(0, high_value - low_value)
    adjacent_delta = high_value + _ceil_ratio(
        delta * (target_edges - high_edges), high_edges - low_edges
    )
    latest_ratio = _ceil_ratio(high_value * target_edges, high_edges)
    return max(high_value, adjacent_delta, latest_ratio)


def project(
    profile: Profile,
    completed: Sequence[Mapping[str, Any]],
    provider_capacity: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Project one provider rung from its two declared completed source scales."""
    if profile.execution != "provider" or profile.projection_sources is None:
        raise QualificationError("only provider profiles have projection gates")
    by_scale = {int(item.get("scale", -1)): item for item in completed}
    try:
        low, high = (by_scale[scale] for scale in profile.projection_sources)
    except KeyError as error:
        raise QualificationError("both declared projection source rungs must complete") from error
    _validate_rung(low)
    _validate_rung(high)
    if profile.scale == 20 and profile.projection_sources != (18, 19):
        raise QualificationError("S20 requires adjacent S18 and S19 evidence")
    if profile.scale == 26 and profile.projection_sources != (24, 25):
        raise QualificationError("S26 requires adjacent S24 and S25 evidence")
    if profile.scale == 26 and any(
        item.get("source") != "canonical_ladder" for item in (low, high)
    ):
        raise QualificationError("S26 requires canonical S24 and S25 ladder evidence")

    target_edges = (1 << profile.scale) * 16
    projected = {name: _project(low, high, target_edges, name) for name in METRICS}
    # RSS is expected to plateau; a per-edge ratio would encode the architectural
    # failure we are trying to detect as a capacity requirement.
    projected["peak_rss_bytes"] = max(
        _integer(high["metrics"], "peak_rss_bytes"),
        _integer(high["metrics"], "peak_rss_bytes")
        + max(
            0,
            _integer(high["metrics"], "peak_rss_bytes")
            - _integer(low["metrics"], "peak_rss_bytes"),
        ),
    )
    storage_peak = max(
        projected["retained_storage_bytes"], projected["transient_peak_storage_bytes"]
    )
    rss_low = _integer(low["metrics"], "peak_rss_bytes")
    rss_high = _integer(high["metrics"], "peak_rss_bytes")
    rss_growth_fraction = (rss_high - rss_low) / max(1, rss_low)
    capacity = provider_capacity or {}
    usable_seconds = int(WALL_LIMIT_SECONDS * (1 - TIME_HEADROOM))
    required_rates = {
        "physical_read_bytes_per_second": _ceil_ratio(
            projected["physical_read_bytes"], usable_seconds
        ),
        "physical_write_bytes_per_second": _ceil_ratio(
            projected["physical_write_bytes"], usable_seconds
        ),
        "reader_calls_per_second": _ceil_ratio(projected["reader_calls"], usable_seconds),
        "publication_work_per_second": _ceil_ratio(
            projected["publication_work_units"], usable_seconds
        ),
    }
    measured_capacity = all(
        isinstance(capacity.get(name), int) and not isinstance(capacity.get(name), bool)
        for name in required_rates
    )
    work_headroom = measured_capacity and all(
        required * 5 <= capacity[name] * 4 for name, required in required_rates.items()
    )
    rss_growth = rss_high - rss_low
    checks = {
        "time_headroom": projected["wall_seconds"] <= WALL_LIMIT_SECONDS * 80 // 100,
        "rss_headroom": projected["peak_rss_bytes"] <= RSS_LIMIT_BYTES * 80 // 100,
        "retained_storage_headroom": projected["retained_storage_bytes"]
        <= VOLUME_LIMIT_BYTES * 85 // 100,
        "transient_storage_headroom": projected["transient_peak_storage_bytes"]
        <= VOLUME_LIMIT_BYTES * 85 // 100,
        "storage_headroom": storage_peak <= VOLUME_LIMIT_BYTES * 85 // 100,
        "rss_bounded_or_plateaued": rss_growth <= rss_low * 10 // 100,
        "io_reader_publication_capacity_measured": measured_capacity,
        "io_reader_publication_headroom": work_headroom,
        "correctness": True,
    }
    return {
        "schema": "graphforge-progressive-qualification-evidence/1",
        "target": f"S{profile.scale}",
        "source_scales": list(profile.projection_sources),
        "decision": Decision.ADMITTED if all(checks.values()) else Decision.REFUSED,
        "limits": {
            "wall_seconds": WALL_LIMIT_SECONDS,
            "rss_bytes": RSS_LIMIT_BYTES,
            "volume_bytes": VOLUME_LIMIT_BYTES,
        },
        "headroom": {
            "time_fraction": TIME_HEADROOM,
            "rss_fraction": RSS_HEADROOM,
            "storage_fraction": STORAGE_HEADROOM,
        },
        "projected": projected | {"storage_peak_bytes": storage_peak},
        "required_rates": required_rates,
        "provider_capacity": dict(capacity) if measured_capacity else None,
        "slopes_observed": {
            name: _integer(high["metrics"], name) - _integer(low["metrics"], name)
            for name in (
                "logical_read_bytes",
                "logical_write_bytes",
                "physical_read_bytes",
                "physical_write_bytes",
                "reader_calls",
                "publication_work_units",
            )
        },
        "rss_growth_fraction": rss_growth_fraction,
        "checks": checks,
        "claim": "engineering_evidence_only",
    }
