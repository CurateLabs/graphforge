#!/usr/bin/env python3
"""Produce S18/S19 Fly qualification evidence from real child observations."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
import importlib.util
import json
import math
import os
from pathlib import Path
import subprocess
import tempfile
import time
from typing import Any, Protocol

ROOT = Path(__file__).resolve().parents[1]
CONTROLLER_PATH = ROOT / "scripts/fly-g500-s20.py"
SPEC = importlib.util.spec_from_file_location("fly_g500_s20", CONTROLLER_PATH)
assert SPEC and SPEC.loader
controller = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(controller)

OBSERVATION_SCHEMA = "graphforge-fly-s20-qualification-observation/1"
CLEANUP_SCHEMA = "graphforge-fly-s20-qualification-cleanup/1"
QUALIFICATION_SCHEMA = "graphforge-fly-s20-qualification/1"
PLATFORM = "linux/amd64"
SCALES = (18, 19)
QUALIFICATION_TTL_S = 4 * 3600
MEMORY_REFUSAL = "memory_headroom_exceeded"
MAX_CANDIDATES = 8


class ProducerError(RuntimeError):
    pass


class ObservationRunner(Protocol):
    def observe(
        self,
        *,
        scale: int,
        candidate: dict[str, Any],
        output: Path,
        timeout: float,
    ) -> dict[str, Any]: ...


class ChildCommandRunner:
    """Run one adapter which owns a disposable observation and its cleanup."""

    def __init__(
        self,
        command: Path,
        *,
        sha: str,
        image_digest: str,
        region: str,
        volume_gb: int,
    ) -> None:
        self.command = command
        self.sha = sha
        self.image_digest = image_digest
        self.region = region
        self.volume_gb = volume_gb

    def observe(
        self,
        *,
        scale: int,
        candidate: dict[str, Any],
        output: Path,
        timeout: float,
    ) -> dict[str, Any]:
        environment = os.environ.copy()
        environment.update(
            {
                "GF_QUALIFICATION_SCALE": str(scale),
                "GF_QUALIFICATION_EXPECTED_SHA": self.sha,
                "GF_QUALIFICATION_IMAGE_DIGEST": self.image_digest,
                "GF_QUALIFICATION_PLATFORM": PLATFORM,
                "GF_QUALIFICATION_REGION": self.region,
                "GF_QUALIFICATION_MACHINE": candidate["name"],
                "GF_QUALIFICATION_CPUS": str(candidate["cpus"]),
                "GF_QUALIFICATION_MEMORY_MB": str(candidate["memory_mb"]),
                "GF_QUALIFICATION_VOLUME_GB": str(self.volume_gb),
                "GF_QUALIFICATION_EVIDENCE_OUT": str(output),
            }
        )
        cleanup_output = output.with_suffix(".cleanup.json")
        observation_error: ProducerError | None = None
        value: Any = None
        try:
            environment["GF_QUALIFICATION_ACTION"] = "observe"
            result = subprocess.run(
                [str(self.command)],
                cwd=ROOT,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
                timeout=max(1.0, timeout),
            )
            if not output.is_file():
                observation_error = ProducerError(
                    f"qualification adapter returned {result.returncode} without child evidence"
                )
            else:
                try:
                    value = json.loads(output.read_text())
                except (OSError, UnicodeError, json.JSONDecodeError):
                    observation_error = ProducerError("qualification adapter emitted invalid JSON")
        except subprocess.TimeoutExpired:
            observation_error = ProducerError(
                "qualification observation exceeded the total deadline"
            )
        except OSError:
            observation_error = ProducerError("qualification observation command failed")
        environment["GF_QUALIFICATION_ACTION"] = "cleanup"
        environment["GF_QUALIFICATION_CLEANUP_EVIDENCE_OUT"] = str(cleanup_output)
        try:
            cleanup = subprocess.run(
                [str(self.command)],
                cwd=ROOT,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
                timeout=controller.CLEANUP_TTL_S,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise ProducerError("qualification adapter cleanup failed or timed out") from error
        if cleanup.returncode != 0 or not cleanup_output.is_file():
            raise ProducerError("qualification adapter did not complete cleanup")
        try:
            cleanup_evidence = json.loads(cleanup_output.read_text())
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise ProducerError("qualification adapter emitted invalid cleanup evidence") from error
        expected_cleanup = {
            "schema": CLEANUP_SCHEMA,
            "git_sha": self.sha,
            "image_digest": self.image_digest,
            "platform": PLATFORM,
            "region": self.region,
            "scale": scale,
            "runtime": {
                "machine": candidate["name"],
                "cpus": candidate["cpus"],
                "memory_mb": candidate["memory_mb"],
            },
            "verified": True,
            "resources_absent": True,
        }
        if cleanup_evidence != expected_cleanup:
            raise ProducerError("qualification adapter cleanup proof mismatches the observation")
        if observation_error is not None:
            raise observation_error
        if not isinstance(value, dict):
            raise ProducerError("qualification observation must be a JSON object")
        value["cleanup"] = {"verified": True, "resources_absent": True}
        return value


def validate_candidates(values: Any) -> list[dict[str, Any]]:
    if not isinstance(values, list) or not 1 <= len(values) <= MAX_CANDIDATES:
        raise ProducerError(f"qualification requires 1..{MAX_CANDIDATES} Machine candidates")
    candidates: list[dict[str, Any]] = []
    for value in values:
        if not isinstance(value, dict) or set(value) != {
            "name",
            "cpus",
            "memory_mb",
            "observation_max_usd",
        }:
            raise ProducerError("Machine candidates require exact resource and cost fields")
        if (
            not isinstance(value["name"], str)
            or not controller.SAFE_NAME.fullmatch(value["name"])
            or type(value["cpus"]) is not int
            or value["cpus"] <= 0
            or type(value["memory_mb"]) is not int
            or not 0 < value["memory_mb"] <= controller.MAX_MEMORY_MB
            or type(value["observation_max_usd"]) not in (int, float)
            or not math.isfinite(value["observation_max_usd"])
            or value["observation_max_usd"] <= 0
        ):
            raise ProducerError("Machine candidate contains an invalid value")
        candidates.append(value)
    order = [(value["memory_mb"], value["cpus"], value["name"]) for value in candidates]
    if order != sorted(set(order)):
        raise ProducerError("Machine candidates must be unique and smallest-first")
    return candidates


def load_candidates(path: Path) -> list[dict[str, Any]]:
    try:
        values = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise ProducerError("Machine candidate file is invalid") from error
    return validate_candidates(values)


def validate_observation(
    value: dict[str, Any],
    *,
    sha: str,
    digest: str,
    region: str,
    scale: int,
    candidate: dict[str, Any],
) -> None:
    expected_binding = {
        "schema": OBSERVATION_SCHEMA,
        "git_sha": sha,
        "image_digest": digest,
        "platform": PLATFORM,
        "region": region,
        "scale": scale,
        "runtime": {
            "machine": candidate["name"],
            "cpus": candidate["cpus"],
            "memory_mb": candidate["memory_mb"],
        },
        "runtime_contract": controller.REQUIRED_IMAGE_CONTRACT,
        "measurement_contract": controller.REQUIRED_MEASUREMENT_CONTRACT,
        "construction_contract": controller.REQUIRED_CONSTRUCTION_CONTRACT,
    }
    for key, expected in expected_binding.items():
        if value.get(key) != expected:
            raise ProducerError(f"qualification child evidence mismatches {key}")
    cleanup = value.get("cleanup")
    if cleanup != {"verified": True, "resources_absent": True}:
        raise ProducerError("qualification child did not prove disposable-resource cleanup")
    cost = value.get("cost_usd")
    if type(cost) not in (int, float) or not math.isfinite(cost) or cost < 0:
        raise ProducerError("qualification child lacks nonnegative observed cost")
    if cost > candidate["observation_max_usd"]:
        raise ProducerError("qualification child exceeded its pre-admitted observation cost")
    result = value.get("result")
    if result == "capacity_exceeded":
        if value.get("failure") != {"code": MEMORY_REFUSAL}:
            raise ProducerError("candidate escalation requires a typed memory-headroom refusal")
        return
    if result != "pass" or value.get("failure") is not None:
        raise ProducerError("qualification child returned a non-capacity failure")
    rung = value.get("rung")
    if not isinstance(rung, dict):
        raise ProducerError("passing child evidence lacks a rung")
    # Reuse the authoritative consumer for exact phases, nonzero construction
    # counters, density, disk projection, and RSS plateau after both rungs exist.
    if rung.get("scale") != scale or rung.get("runtime") != expected_binding["runtime"]:
        raise ProducerError("qualification child rung is not bound to its requested run")


def produce(
    *,
    sha: str,
    digest: str,
    region: str,
    candidates: list[dict[str, Any]],
    runner: ObservationRunner,
    evidence_out: Path,
    ceiling_usd: float,
    reserve_usd: float,
    admission: Callable[[], None],
    now: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """Admit first, then select the smallest candidate with two valid rungs."""
    if ceiling_usd != 10.0 or reserve_usd < 1.0 or reserve_usd >= ceiling_usd:
        raise ProducerError("qualification requires the approved $10 ceiling and >=$1 reserve")
    if not evidence_out.parent.is_dir():
        raise ProducerError("qualification output parent must already exist")
    candidates = validate_candidates(candidates)
    admission()  # Must precede even the first adapter call/resource creation.
    deadline = now() + QUALIFICATION_TTL_S
    spent = 0.0
    available = ceiling_usd - reserve_usd
    selected_index: int | None = None
    selected_rungs: list[dict[str, Any]] = []
    capacity_candidates: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="graphforge-s20-qualification-") as directory:
        root = Path(directory)
        for candidate_index, candidate in enumerate(candidates):
            planned_pair = 2 * float(candidate["observation_max_usd"])
            if spent + planned_pair > available:
                raise ProducerError("qualification candidate would exceed the total cost ceiling")
            pair: list[dict[str, Any]] = []
            capacity = False
            for scale in SCALES:
                remaining = deadline - now()
                if remaining <= 0:
                    raise ProducerError("qualification exceeded the total four-hour deadline")
                path = root / f"candidate-{candidate_index}-s{scale}.json"
                value = runner.observe(
                    scale=scale,
                    candidate=candidate,
                    output=path,
                    timeout=remaining,
                )
                validate_observation(
                    value,
                    sha=sha,
                    digest=digest,
                    region=region,
                    scale=scale,
                    candidate=candidate,
                )
                spent += float(value["cost_usd"])
                if spent > available:
                    raise ProducerError(
                        "qualification observations exceeded the total cost ceiling"
                    )
                if value["result"] == "capacity_exceeded":
                    capacity = True
                    capacity_candidates.append(candidate)
                    break
                pair.append(value["rung"])
            if capacity:
                continue
            if len(pair) != len(SCALES):
                raise ProducerError("candidate did not produce both adjacent qualification rungs")
            selected_index = candidate_index
            selected_rungs = pair
            break
    if selected_index is None:
        raise ProducerError("no candidate completed adjacent S18/S19 qualification")
    selected = candidates[selected_index]
    output = {
        "schema": QUALIFICATION_SCHEMA,
        "region": region,
        "image_digest": digest,
        "qualification_observed_cost_usd": spent,
        "max_phase_rss_growth_ratio": 1.2,
        # Exclude candidates empirically refused below the selected size. The
        # consumer still independently selects the smallest remaining option.
        "machine_candidates": [
            {key: item[key] for key in ("name", "cpus", "memory_mb")}
            for item in candidates[selected_index:]
        ],
        "rungs": selected_rungs,
    }
    temporary = evidence_out.with_name(f".{evidence_out.name}.tmp")
    temporary.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    committed = False
    try:
        resources = controller.load_qualification(temporary, digest, region)
        if resources["machine"] != selected["name"]:
            raise ProducerError("successful pair does not qualify its measured candidate")
        required = max(
            resources["qualified_peak_rss_bytes"] + controller.MIN_MEMORY_HEADROOM_BYTES,
            int(resources["qualified_peak_rss_bytes"] * controller.MEMORY_HEADROOM_RATIO),
        )
        if any(item["memory_mb"] * 1024 * 1024 >= required for item in capacity_candidates):
            raise ProducerError("escalation is contradicted by measured RSS headroom")
        temporary.replace(evidence_out)
        committed = True
    finally:
        if not committed:
            temporary.unlink(missing_ok=True)
    return output


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--expected-sha", required=True)
    result.add_argument("--image", required=True)
    result.add_argument("--region", default="dfw")
    result.add_argument("--candidates-json", type=Path, required=True)
    result.add_argument("--observation-command", type=Path, required=True)
    result.add_argument("--volume-gb", type=int, required=True)
    result.add_argument("--ceiling-usd", type=float, default=10.0)
    result.add_argument("--unpriced-reserve-usd", type=float, default=1.0)
    result.add_argument("--evidence-out", type=Path, required=True)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        match = controller.CHILD_IMAGE.fullmatch(args.image)
        if not match or not controller.SHA.fullmatch(args.expected_sha):
            raise ProducerError("qualification requires exact source and platform-child digests")
        if args.region != "dfw" or not 1 <= args.volume_gb <= controller.MAX_VOLUME_GB:
            raise ProducerError("qualification region or volume exceeds the certification envelope")
        if not args.observation_command.is_file() or not os.access(
            args.observation_command, os.X_OK
        ):
            raise ProducerError("qualification observation command must be executable")
        digest = match.group("digest")
        candidates = load_candidates(args.candidates_json)
        runner = ChildCommandRunner(
            args.observation_command,
            sha=args.expected_sha,
            image_digest=digest,
            region=args.region,
            volume_gb=args.volume_gb,
        )

        def admission() -> None:
            controller.check_source(args.expected_sha)
            controller.assert_platform_child(args.image, args.expected_sha)

        produce(
            sha=args.expected_sha,
            digest=digest,
            region=args.region,
            candidates=candidates,
            runner=runner,
            evidence_out=args.evidence_out,
            ceiling_usd=args.ceiling_usd,
            reserve_usd=args.unpriced_reserve_usd,
            admission=admission,
        )
    except (
        ProducerError,
        controller.ControllerError,
        OSError,
        subprocess.SubprocessError,
        json.JSONDecodeError,
    ) as error:
        print(f"Fly S20 qualification refused: {error}", file=__import__("sys").stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
