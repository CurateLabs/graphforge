#!/usr/bin/env python3
"""Adversarial tests for the S18/S19 qualification producer."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile

ROOT = Path(__file__).resolve().parents[2]


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


producer = load(ROOT / "scripts/produce-fly-s20-qualification.py", "qualification_producer")
controller_tests = load(ROOT / "scripts/ci/test-fly-g500-s20.py", "controller_tests")

SHA = "a" * 40
DIGEST = "sha256:" + "b" * 64
SMALL = {
    "name": "performance-1x",
    "cpus": 1,
    "memory_mb": 2048,
    "observation_max_usd": 0.5,
}
SELECTED = {
    "name": "performance-2x",
    "cpus": 2,
    "memory_mb": 4096,
    "observation_max_usd": 0.75,
}


def observation(scale: int, candidate: dict, rung: dict | None = None) -> dict:
    return {
        "schema": producer.OBSERVATION_SCHEMA,
        "git_sha": SHA,
        "image_digest": DIGEST,
        "platform": producer.PLATFORM,
        "region": "dfw",
        "scale": scale,
        "runtime": {
            "machine": candidate["name"],
            "cpus": candidate["cpus"],
            "memory_mb": candidate["memory_mb"],
        },
        "runtime_contract": producer.controller.REQUIRED_IMAGE_CONTRACT,
        "measurement_contract": producer.controller.REQUIRED_MEASUREMENT_CONTRACT,
        "construction_contract": producer.controller.REQUIRED_CONSTRUCTION_CONTRACT,
        "result": "pass" if rung is not None else "capacity_exceeded",
        "failure": None if rung is not None else {"code": producer.MEMORY_REFUSAL},
        "cost_usd": 0.1,
        "cleanup": {"verified": True, "resources_absent": True},
        "rung": rung,
    }


def rungs(candidate: dict, *, growth: float = 1.1) -> dict[int, dict]:
    artifact = controller_tests.qualification(DIGEST, growth=growth)
    for rung in artifact["rungs"]:
        rung["runtime"] = {
            "machine": candidate["name"],
            "cpus": candidate["cpus"],
            "memory_mb": candidate["memory_mb"],
        }
    return {rung["scale"]: rung for rung in artifact["rungs"]}


class FakeRunner:
    def __init__(self, callback):
        self.callback = callback
        self.calls = []

    def observe(self, *, scale, candidate, output, timeout):
        self.calls.append((scale, candidate["name"], timeout))
        value = self.callback(scale, candidate)
        output.write_text(json.dumps(value))
        return value


def expect_error(fragment: str, callback) -> None:
    try:
        callback()
    except (producer.ProducerError, producer.controller.ControllerError) as error:
        assert fragment in str(error), str(error)
    else:
        raise AssertionError(f"expected refusal containing {fragment!r}")


def invoke(root: Path, runner: FakeRunner, admission=lambda: None):
    return producer.produce(
        sha=SHA,
        digest=DIGEST,
        region="dfw",
        candidates=[SMALL, SELECTED],
        runner=runner,
        evidence_out=root / "qualification.json",
        ceiling_usd=10.0,
        reserve_usd=1.0,
        admission=admission,
    )


def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)

        # Executable/image admission is a strict pre-resource barrier.
        never = FakeRunner(lambda _scale, _candidate: (_ for _ in ()).throw(AssertionError()))
        expect_error(
            "contract unavailable",
            lambda: invoke(
                root,
                never,
                admission=lambda: (_ for _ in ()).throw(
                    producer.ProducerError("contract unavailable")
                ),
            ),
        )
        assert never.calls == []
        assert not (root / "qualification.json").exists()

        # The real command adapter invokes an idempotent cleanup action even
        # when its observation action fails before writing evidence.
        adapter = root / "fake-observation-adapter.py"
        adapter.write_text(
            """#!/usr/bin/env python3
import json
import os
from pathlib import Path
if os.environ["GF_QUALIFICATION_ACTION"] != "cleanup":
    raise SystemExit(7)
runtime = {
    "machine": os.environ["GF_QUALIFICATION_MACHINE"],
    "cpus": int(os.environ["GF_QUALIFICATION_CPUS"]),
    "memory_mb": int(os.environ["GF_QUALIFICATION_MEMORY_MB"]),
}
value = {
    "schema": "graphforge-fly-s20-qualification-cleanup/1",
    "git_sha": os.environ["GF_QUALIFICATION_EXPECTED_SHA"],
    "image_digest": os.environ["GF_QUALIFICATION_IMAGE_DIGEST"],
    "platform": os.environ["GF_QUALIFICATION_PLATFORM"],
    "region": os.environ["GF_QUALIFICATION_REGION"],
    "scale": int(os.environ["GF_QUALIFICATION_SCALE"]),
    "runtime": runtime,
    "verified": True,
    "resources_absent": True,
}
Path(os.environ["GF_QUALIFICATION_CLEANUP_EVIDENCE_OUT"]).write_text(json.dumps(value))
"""
        )
        adapter.chmod(0o700)
        command_runner = producer.ChildCommandRunner(
            adapter,
            sha=SHA,
            image_digest=DIGEST,
            region="dfw",
            volume_gb=20,
        )
        command_output = root / "failed-observation.json"
        expect_error(
            "without child evidence",
            lambda: command_runner.observe(
                scale=18,
                candidate=SMALL,
                output=command_output,
                timeout=5,
            ),
        )
        assert (
            json.loads(command_output.with_suffix(".cleanup.json").read_text())["resources_absent"]
            is True
        )

        selected_rungs = rungs(SELECTED)

        def escalate(scale, candidate):
            if candidate["name"] == SMALL["name"]:
                return observation(scale, candidate)
            return observation(scale, candidate, selected_rungs[scale])

        runner = FakeRunner(escalate)
        produced = invoke(root, runner)
        assert [(scale, name) for scale, name, _ in runner.calls] == [
            (18, SMALL["name"]),
            (18, SELECTED["name"]),
            (19, SELECTED["name"]),
        ]
        assert produced["machine_candidates"][0]["name"] == SELECTED["name"]
        assert json.loads((root / "qualification.json").read_text()) == produced
        resources = producer.controller.load_qualification(
            root / "qualification.json", DIGEST, "dfw"
        )
        assert resources["machine"] == SELECTED["name"]

        # Every child binding is authenticated; a different image cannot be mixed in.
        def mismatch(scale, candidate):
            value = observation(scale, candidate, rungs(candidate)[scale])
            value["image_digest"] = "sha256:" + "c" * 64
            return value

        expect_error("image_digest", lambda: invoke(root, FakeRunner(mismatch)))

        # Cleanup evidence is mandatory even for the typed escalation path.
        def dirty_cleanup(scale, candidate):
            value = observation(scale, candidate)
            value["cleanup"]["resources_absent"] = False
            return value

        expect_error("cleanup", lambda: invoke(root, FakeRunner(dirty_cleanup)))

        # A capacity claim cannot justify a larger Machine when observed RSS fits it.
        low_peak = rungs(SELECTED)
        for rung in low_peak.values():
            for phase in rung["phases"]:
                for key in (
                    "cgroup_current_before_bytes",
                    "cgroup_peak_bytes",
                    "cgroup_current_after_bytes",
                    "smaps_rss_bytes",
                    "smaps_anon_bytes",
                    "smaps_file_bytes",
                ):
                    phase["memory"][key] //= 4

        def false_escalation(scale, candidate):
            if candidate["name"] == SMALL["name"]:
                return observation(scale, candidate)
            return observation(scale, candidate, low_peak[scale])

        expect_error("contradicted", lambda: invoke(root, FakeRunner(false_escalation)))

        # The existing consumer is the final authority for nonzero I/O and RSS plateau.
        zero = rungs(SELECTED)
        zero[18]["phases"][2]["io"]["blocks"] = 0

        def zero_io(scale, candidate):
            if candidate["name"] == SMALL["name"]:
                return observation(scale, candidate)
            return observation(scale, candidate, zero[scale])

        expect_error("nonzero", lambda: invoke(root, FakeRunner(zero_io)))

        growing = rungs(SELECTED, growth=1.3)

        def no_plateau(scale, candidate):
            if candidate["name"] == SMALL["name"]:
                return observation(scale, candidate)
            return observation(scale, candidate, growing[scale])

        expect_error("does not plateau", lambda: invoke(root, FakeRunner(no_plateau)))

        over_budget = dict(SELECTED)
        over_budget["observation_max_usd"] = 5.0
        expect_error(
            "cost ceiling",
            lambda: producer.produce(
                sha=SHA,
                digest=DIGEST,
                region="dfw",
                candidates=[over_budget],
                runner=FakeRunner(lambda _scale, _candidate: None),
                evidence_out=root / "qualification.json",
                ceiling_usd=10.0,
                reserve_usd=1.0,
                admission=lambda: None,
            ),
        )

    print("Fly S18/S19 qualification producer tests passed")


if __name__ == "__main__":
    main()
