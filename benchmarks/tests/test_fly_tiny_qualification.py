from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from graphforge_bench.fly_adapter import AdapterError, ResourceLedger
from graphforge_bench.fly_tiny_qualification import (
    FlyctlTransport,
    QualificationError,
    TinyQualificationInvocation,
    _machine_command,
    execute,
    validate_invocation,
    verify_live_capacity,
)

DIGEST = "sha256:" + "b" * 64
IMAGE = "registry.fly.io/gf-q958-test@" + DIGEST


def invocation(**changes: object) -> TinyQualificationInvocation:
    values = {
        "commit": "a" * 40,
        "organization": "personal",
        "app": "gf-q958-test",
        "region": "dfw",
        "volume_name": "gf_q958_test",
        "machine_name": "gf-q958-machine",
        "prerequisites": {955: "merged", 956: "merged", 957: "merged"},
        "machine_class": "performance-1x",
        "volume_gib": 10,
    }
    values.update(changes)
    return TinyQualificationInvocation(**values)  # type: ignore[arg-type]


def evidence() -> dict[str, object]:
    return {
        "schema": "graphforge-fly-filesystem-qualification/1",
        "git_sha": "a" * 40,
        "image_digest": DIGEST,
        "provider": "fly.io",
        "region": "dfw",
        "host": {"os": "Linux", "filesystem": "ext4", "memory_bytes": 2_147_483_648},
        "volume": {"mount_role": "process_work_root", "capacity_bytes": 10_000_000_000},
        "phase_peak_rss_bytes": {
            "filesystem_admission": 100_000_000,
            "durable_reopen": 110_000_000,
            "portable_verify": 120_000_000,
            "portable_import_reopen": 125_000_000,
        },
        "admission": {"status": "accepted", "code": None, "cause": None},
        "result": "qualified",
        "full_run_authorized": False,
    }


class FakeTransport:
    def __init__(
        self,
        *,
        build_fails: bool = False,
        teardown_machine: bool = False,
        teardown_machine_polls: int = 0,
        auto_destroy: bool = True,
        restart_state: object = "valid",
        mount_state: object = "valid",
    ):
        self.commands: list[tuple[str, ...]] = []
        self.machine_lists = 0
        self.build_fails = build_fails
        self.teardown_machine = teardown_machine
        self.teardown_machine_polls = teardown_machine_polls
        self.machine_created = False
        self.auto_destroy = auto_destroy
        self.restart_state = {"policy": "no"} if restart_state == "valid" else restart_state
        self.mount_state = (
            {"volume": "vol_fixture123", "path": "/work"}
            if mount_state == "valid"
            else mount_state
        )
        self.app_created = False

    def run(
        self, argv: tuple[str, ...], *, timeout: int, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        self.commands.append(argv)
        if argv[:2] == ("flyctl", "deploy"):
            if self.build_fails:
                raise subprocess.CalledProcessError(1, argv)
            return subprocess.CompletedProcess(argv, 0, "", "")
        if argv[:3] == ("flyctl", "apps", "create"):
            self.app_created = True
        if argv[:3] == ("flyctl", "apps", "destroy"):
            self.app_created = False
        if argv[:4] == ("flyctl", "ssh", "sftp", "get"):
            Path(argv[5]).write_text(json.dumps(evidence()), encoding="utf-8")
        if argv[:3] == ("flyctl", "machine", "run"):
            self.machine_created = True
        return subprocess.CompletedProcess(argv, 0, "", "")

    def resolve_image(self, app: str, tag: str, *, timeout: int) -> str:
        self.commands.append(("resolve-image", app, tag))
        self.assert_resolution(app, tag, timeout)
        return IMAGE

    def machine_state(self, app: str, machine_id: str, *, timeout: int) -> object:
        self.commands.append(("machine-state", app, machine_id))
        if app != "gf-q958-test" or machine_id != "abcdef01234567" or timeout <= 0:
            raise AssertionError("unexpected Machine state lookup")
        return {
            "id": "abcdef01234567",
            "region": "dfw",
            "image_ref": {"digest": DIGEST},
            "config": {
                "auto_destroy": self.auto_destroy,
                "restart": self.restart_state,
                "services": [],
                "guest": {"cpu_kind": "performance", "cpus": 1, "memory_mb": 2048},
                "mounts": [self.mount_state],
            },
        }

    @staticmethod
    def assert_resolution(app: str, tag: str, timeout: int) -> None:
        if app != "gf-q958-test" or tag != "a" * 40 or timeout <= 0:
            raise AssertionError("unexpected image resolution")

    def json(self, argv: tuple[str, ...], *, timeout: int) -> object:
        self.commands.append(argv)
        if argv[1:3] == ("platform", "regions"):
            return [{"code": "dfw", "deprecated": False, "capacity": 1}]
        if argv[1:3] == ("platform", "vm-sizes"):
            return {
                "performance-1x": {
                    "cpu_kind": "performance",
                    "cpus": 1,
                    "memory_mb": 2048,
                },
                "performance-2x": {
                    "cpu_kind": "performance",
                    "cpus": 2,
                    "memory_mb": 4096,
                },
                "shared-cpu-1x": {"cpu_kind": "shared", "cpus": 1, "memory_mb": 256},
            }
        if argv[1:3] == ("volumes", "create"):
            return {"id": "vol_fixture123"}
        if argv[1:3] == ("machine", "list"):
            if not self.machine_created:
                return []
            self.machine_lists += 1
            if self.machine_lists == 1:
                return [{"id": "abcdef01234567", "name": "gf-q958-machine"}]
            if self.machine_lists <= self.teardown_machine_polls + 1:
                return [{"id": "abcdef01234567", "state": "destroying"}]
            return [{"id": "abcdef01234567"}] if self.teardown_machine else []
        if argv[1:3] in {("volumes", "list"), ("secrets", "list")}:
            return []
        if argv[1:3] == ("apps", "list"):
            return [{"Name": "gf-q958-test"}] if self.app_created else []
        raise AssertionError(f"unexpected JSON command: {argv}")


class FlyTinyQualificationTests(unittest.TestCase):
    def test_registry_and_machine_api_tokens_remain_in_memory(self) -> None:
        class TokenTransport(FlyctlTransport):
            def run(
                self, argv: tuple[str, ...], *, timeout: int, check: bool = True
            ) -> subprocess.CompletedProcess[str]:
                del timeout, check
                self.last_command = argv
                return subprocess.CompletedProcess(argv, 0, "secret-token\n", "")

        class Response:
            def __init__(self, payload: object | None = None):
                self.headers = {"Docker-Content-Digest": DIGEST}
                self.payload = payload

            def __enter__(self) -> Response:
                return self

            def __exit__(self, *args: object) -> None:
                del args

            def read(self) -> bytes:
                return json.dumps(self.payload).encode()

        transport = TokenTransport()
        requests: list[object] = []

        def open_request(request: object, timeout: int) -> Response:
            self.assertGreater(timeout, 0)
            requests.append(request)
            if "registry.fly.io" in request.full_url:  # type: ignore[attr-defined]
                return Response()
            return Response({"id": "abcdef01234567", "config": {}})

        with patch("graphforge_bench.fly_tiny_qualification.urllib.request.urlopen", open_request):
            self.assertEqual(transport.resolve_image("gf-q958-test", "fixture", timeout=30), IMAGE)
            self.assertEqual(
                transport.machine_state("gf-q958-test", "abcdef01234567", timeout=30)["id"],
                "abcdef01234567",
            )
        self.assertTrue(
            all(request.headers["Authorization"] == "Bearer secret-token" for request in requests)
        )  # type: ignore[attr-defined]
        self.assertNotIn("secret-token", json.dumps(evidence()))

    def test_tiny_invocation_is_not_a_ladder_rung(self) -> None:
        validate_invocation(invocation())
        with self.assertRaisesRegex(AdapterError, "smallest performance"):
            validate_invocation(invocation(machine_class="performance-2x"))
        with self.assertRaisesRegex(AdapterError, "prerequisite"):
            validate_invocation(
                invocation(prerequisites={955: "merged", 956: "open", 957: "merged"})
            )
        self.assertFalse(hasattr(invocation(), "rung"))

    def test_live_capacity_requires_current_region_and_smallest_preset(self) -> None:
        transport = FakeTransport()
        verify_live_capacity(transport, invocation())
        with self.assertRaisesRegex(QualificationError, "smallest performance preset"):
            verify_live_capacity(transport, invocation(machine_class="performance-2x"))
        with self.assertRaisesRegex(QualificationError, "not currently admitted"):
            verify_live_capacity(transport, invocation(region="ord"))

    def test_machine_argv_is_private_bounded_and_auto_destroying(self) -> None:
        command = _machine_command(invocation(), image=IMAGE, volume_id="vol_fixture123")
        self.assertIn("performance-1x", command)
        self.assertIn("vol_fixture123:/work", command)
        self.assertIn("--rm", command)
        self.assertIn("--restart", command)
        self.assertIn("no", command)
        self.assertIn("--autostop", command)
        self.assertIn("off", command)
        self.assertNotIn("--port", command)
        self.assertNotIn("S18", " ".join(command))

    @patch("graphforge_bench.fly_tiny_qualification.check_source")
    def test_success_persists_ledger_retrieves_only_evidence_and_tears_down(
        self, check_source: object
    ) -> None:
        del check_source
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger_path = root / "ledger.json"
            evidence_path = root / "evidence.json"
            transport = FakeTransport()
            result = execute(
                invocation(),
                transport=transport,
                root=root,
                ledger_path=ledger_path,
                evidence_out=evidence_path,
                dry_run=False,
            )
            self.assertEqual(result["status"], "passed")
            self.assertEqual(ResourceLedger.load(ledger_path), ResourceLedger())
            self.assertEqual(json.loads(evidence_path.read_text()), evidence())
            self.assertNotIn("token", json.dumps(result).lower())
            self.assertNotIn("token", evidence_path.read_text().lower())
            joined = "\n".join(" ".join(command) for command in transport.commands)
            self.assertIn("--scheduled-snapshots=false", joined)
            self.assertIn("machine destroy", joined)
            self.assertIn("volumes destroy", joined)
            self.assertIn("apps destroy", joined)
            self.assertNotIn("secrets set", joined)
            self.assertNotIn("dataset", joined)

    @patch("graphforge_bench.fly_tiny_qualification.check_source")
    def test_build_failure_is_typed_and_still_destroys_app(self, check_source: object) -> None:
        del check_source
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            transport = FakeTransport(build_fails=True)
            result = execute(
                invocation(),
                transport=transport,
                root=root,
                ledger_path=root / "ledger.json",
                evidence_out=root / "evidence.json",
                dry_run=False,
            )
            self.assertEqual(result["failure"], "build_failed")
            self.assertIn(
                ("flyctl", "apps", "destroy", "gf-q958-test", "--yes"),
                transport.commands,
            )

    @patch("graphforge_bench.fly_tiny_qualification.check_source")
    def test_nonempty_teardown_inventory_fails_closed(self, check_source: object) -> None:
        del check_source
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("graphforge_bench.fly_tiny_qualification.time.sleep"):
                result = execute(
                    invocation(),
                    transport=FakeTransport(teardown_machine=True),
                    root=root,
                    ledger_path=root / "ledger.json",
                    evidence_out=root / "evidence.json",
                    dry_run=False,
                )
            self.assertEqual(result["failure"], "teardown_failed")

    @patch("graphforge_bench.fly_tiny_qualification.check_source")
    def test_teardown_polls_transient_destroying_machine(self, check_source: object) -> None:
        del check_source
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("graphforge_bench.fly_tiny_qualification.time.sleep") as sleep:
                result = execute(
                    invocation(),
                    transport=FakeTransport(teardown_machine_polls=2),
                    root=root,
                    ledger_path=root / "ledger.json",
                    evidence_out=root / "evidence.json",
                    dry_run=False,
                )
            self.assertEqual(result["status"], "passed")
            self.assertEqual(sleep.call_count, 1)

    @patch("graphforge_bench.fly_tiny_qualification.check_source")
    def test_observed_auto_destroy_mismatch_fails_closed(self, check_source: object) -> None:
        del check_source
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = execute(
                invocation(),
                transport=FakeTransport(auto_destroy=False),
                root=root,
                ledger_path=root / "ledger.json",
                evidence_out=root / "evidence.json",
                dry_run=False,
            )
            self.assertEqual(result["failure"], "provision_failed")

    @patch("graphforge_bench.fly_tiny_qualification.check_source")
    def test_malformed_machine_members_fail_through_typed_result(
        self, check_source: object
    ) -> None:
        del check_source
        for changes in ({"restart_state": None}, {"mount_state": "not-a-mount"}):
            with self.subTest(changes=changes), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                result = execute(
                    invocation(),
                    transport=FakeTransport(**changes),
                    root=root,
                    ledger_path=root / "ledger.json",
                    evidence_out=root / "evidence.json",
                    dry_run=False,
                )
                self.assertEqual(result["failure"], "provision_failed")


if __name__ == "__main__":
    unittest.main()
