from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shlex
import tempfile
import unittest

from graphforge_bench.fly_adapter import (
    AdapterError,
    FlyAttempt,
    LifecycleInvocation,
    ResourceLedger,
    accepted_rung_reclamation,
    cleanup_commands,
    inventory_commands,
    pin_remote_image,
    provisioning_commands,
    remote_build_command,
    retrieval_commands,
    sanitized_failure,
    validate_attempt,
    verify_checked_in_profile,
    verify_download,
    verify_empty_inventory,
)


def attempt(**changes: object) -> FlyAttempt:
    lifecycle = LifecycleInvocation(
        commit="a" * 40,
        rung=18,
        profile="benchmarks/profiles/graph500/s18-local.json",
        profile_sha256="sha256:" + "b" * 64,
        argv=(
            "python3",
            "-m",
            "graphforge_bench.progressive_run",
            "--rung",
            "S18",
            "--output-dir",
            "/work/evidence",
        ),
        evidence_files=(
            "s18-plan.json",
            "s18-benchexec.json",
            "s18-graphforge.json",
            "s18-rung.json",
            "s18-result.json",
        ),
    )
    values = {
        "organization": "fixture-org",
        "app": "gf-fixture",
        "region": "den",
        "volume_name": "gf-data",
        "volume_gib": 500,
        "machine_class": "performance-4x",
        "image": "registry.fly.io/gf-fixture@sha256:" + "c" * 64,
        "maximum_authorized_scale": 18,
        "prerequisites": {955: "merged", 956: "merged", 957: "merged"},
        "lifecycle": lifecycle,
    }
    values.update(changes)
    return FlyAttempt(**values)  # type: ignore[arg-type]


class FlyAdapterTests(unittest.TestCase):
    def test_remote_build_is_remote_only_pushed_and_commit_pinned(self) -> None:
        command = remote_build_command(
            app="gf-fixture", source=Path(), dockerfile=Path("Dockerfile"), commit="a" * 40
        )
        self.assertIn("--remote-only", command.argv)
        self.assertIn("--build-only", command.argv)
        self.assertIn("--push", command.argv)
        self.assertNotIn("--local-only", command.argv)
        self.assertIn("GRAPHFORGE_COMMIT=" + "a" * 40, command.argv)
        self.assertEqual(pin_remote_image("gf-fixture", "sha256:" + "c" * 64), attempt().image)
        with self.assertRaisesRegex(AdapterError, "immutable"):
            pin_remote_image("gf-fixture", "latest")

    def test_provisioning_is_private_fixed_and_thin(self) -> None:
        commands = provisioning_commands(attempt())
        machine = next(c for c in commands if c.operation == "create_machine").argv
        volume = next(c for c in commands if c.operation == "create_volume").argv
        execute = next(c for c in commands if c.operation == "execute_lifecycle").argv
        self.assertIn("den", machine)
        self.assertIn("performance-4x", machine)
        self.assertIn("--restart", machine)
        self.assertIn("no", machine)
        self.assertIn("--autostop", machine)
        self.assertIn("--rm", machine)
        self.assertIn("--scheduled-snapshots=false", volume)
        self.assertNotIn("--port", machine)
        self.assertEqual(shlex.split(execute[-1]), list(attempt().lifecycle.argv))
        self.assertNotIn("threshold", " ".join(execute))

    def test_refuses_unknown_machine_class_and_cross_app_image(self) -> None:
        with self.assertRaisesRegex(AdapterError, "machine class"):
            validate_attempt(attempt(machine_class="gpu-a100-80gb"))
        with self.assertRaisesRegex(AdapterError, "requested app"):
            validate_attempt(attempt(image="registry.fly.io/another-app@sha256:" + "c" * 64))

    def test_refuses_unmerged_prerequisite_and_unauthorized_scale(self) -> None:
        with self.assertRaisesRegex(AdapterError, "prerequisite"):
            validate_attempt(attempt(prerequisites={955: "merged", 956: "open", 957: "merged"}))
        changed = attempt().lifecycle.__class__(
            **{
                **attempt().lifecycle.__dict__,
                "rung": 19,
                "profile": "benchmarks/profiles/graph500/s19-local.json",
            }
        )
        with self.assertRaisesRegex(AdapterError, "authorization"):
            validate_attempt(attempt(lifecycle=changed))

    def test_refuses_mutable_image_and_non_evidence_retrieval(self) -> None:
        with self.assertRaisesRegex(AdapterError, "immutable"):
            validate_attempt(attempt(image="registry.fly.io/gf-fixture:latest"))
        changed = attempt().lifecycle.__class__(
            **{**attempt().lifecycle.__dict__, "evidence_files": ("dataset.parquet",)}
        )
        with self.assertRaisesRegex(AdapterError, "canonical"):
            retrieval_commands(attempt(lifecycle=changed), Path("out"))

    def test_retrieval_is_only_allowlisted_files(self) -> None:
        commands = retrieval_commands(attempt(), Path("out"))
        self.assertEqual(len(commands), 5)
        self.assertTrue(all("/work/evidence/" in c.argv[4] for c in commands))
        self.assertTrue(all("--machine" in c.argv for c in commands))

    def test_cleanup_is_owned_ordered_and_idempotent(self) -> None:
        ledger = ResourceLedger(
            app_owned=True,
            volume_id="vol_fixture123",
            machine_id="abcdef01234567",
            secret_names=["temporary-token"],
        )
        first = cleanup_commands("gf-fixture", ledger)
        self.assertEqual(
            [c.operation for c in first],
            ["destroy_machine", "destroy_volume", "unset_secret", "destroy_app"],
        )
        self.assertEqual(cleanup_commands("gf-fixture", ResourceLedger()), ())
        self.assertEqual(first, cleanup_commands("gf-fixture", ledger))

    def test_ledger_load_closes_shape_types_and_identifiers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ledger.json"
            ledger = ResourceLedger(
                app_owned=True,
                volume_id="vol_fixture123",
                machine_id="abcdef01234567",
                image_digest=attempt().image,
                secret_names=["temporary-token"],
                token_material_present=True,
            )
            ledger.save(path)
            self.assertEqual(ResourceLedger.load(path), ledger)
            value = json.loads(path.read_text(encoding="utf-8"))
            value["machine_id"] = "--force"
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(AdapterError, "machine identifier"):
                ResourceLedger.load(path)
            value["machine_id"] = "abcdef01234567"
            value["unexpected"] = True
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(AdapterError, "fields"):
                ResourceLedger.load(path)

    def test_inventory_must_independently_be_empty(self) -> None:
        self.assertEqual(len(inventory_commands("gf-fixture")), 3)
        verify_empty_inventory(machines=[], volumes=[], secrets=[], app_exists=False)
        with self.assertRaisesRegex(AdapterError, "not empty"):
            verify_empty_inventory(
                machines=[{"id": "never-exposed"}], volumes=[], secrets=[], app_exists=False
            )

    def test_failure_is_closed_typed_and_identifier_free(self) -> None:
        value = sanitized_failure("lifecycle_failed")
        self.assertEqual(
            value,
            {
                "schema": "graphforge-fly-adapter-result/1",
                "status": "failed",
                "failure": "lifecycle_failed",
            },
        )
        self.assertNotIn("id", json.dumps(value))
        with self.assertRaisesRegex(AdapterError, "failure type"):
            sanitized_failure("provider said machine-123 failed")

    def test_profile_digest_is_verified_from_repository(self) -> None:
        root = Path(__file__).resolve().parents[2]
        profile = root / attempt().lifecycle.profile
        digest = "sha256:" + hashlib.sha256(profile.read_bytes()).hexdigest()
        lifecycle = attempt().lifecycle.__class__(
            **{**attempt().lifecycle.__dict__, "profile_sha256": digest}
        )
        verify_checked_in_profile(root, lifecycle)
        with self.assertRaisesRegex(AdapterError, "digest"):
            verify_checked_in_profile(root, attempt().lifecycle)

    def test_reclamation_refuses_current_running_and_unaccepted_rungs(self) -> None:
        for args in (
            {
                "accepted_rung": 18,
                "current_rung": 19,
                "running": True,
                "evidence_accepted": True,
                "lifecycle_argv": ("reclaim", "S18"),
            },
            {
                "accepted_rung": 19,
                "current_rung": 19,
                "running": False,
                "evidence_accepted": True,
                "lifecycle_argv": ("reclaim", "S19"),
            },
            {
                "accepted_rung": 18,
                "current_rung": 19,
                "running": False,
                "evidence_accepted": False,
                "lifecycle_argv": ("reclaim", "S18"),
            },
        ):
            with self.assertRaisesRegex(AdapterError, "not reclaimable"):
                accepted_rung_reclamation(**args)
        command = accepted_rung_reclamation(
            accepted_rung=18,
            current_rung=19,
            running=False,
            evidence_accepted=True,
            lifecycle_argv=("controller-reclaim", "S18"),
        )
        self.assertEqual(command.operation, "reclaim_accepted_rung")
        self.assertEqual(shlex.split(command.argv[-1]), ["controller-reclaim", "S18"])

    def test_download_requires_typed_json_and_digest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "s18-result.json"
            path.write_text(
                json.dumps({"schema": "fixture/1", "status": "passed"}), encoding="utf-8"
            )
            digest = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
            verify_download(path, digest)
            with self.assertRaisesRegex(AdapterError, "digest"):
                verify_download(path, "sha256:" + "0" * 64)


if __name__ == "__main__":
    unittest.main()
