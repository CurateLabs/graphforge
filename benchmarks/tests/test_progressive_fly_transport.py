from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from graphforge_bench.fly_adapter import machine_run_image_ref
from graphforge_bench.progressive_fly_transport import (
    FlyctlMachineBoundary,
    FlyProviderTransport,
    FlyTransportError,
)
from graphforge_bench.progressive_provider_attempt import AttemptError, AttemptInvocation, execute
from tests.test_progressive_provider_attempt import APP, IMAGE, ROOT, authorization, planner

NOW = datetime(2026, 6, 1, tzinfo=timezone.utc)
MACHINE_ID = "abcdef01234567"
VOLUME_ID = "vol_fixture123"


class FakeBoundary:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []
        self.app_exists = False
        self.keep_app = False
        self.destroy_raises = False
        self.inventory_raises = False
        self.observed_digest = "sha256:" + "1" * 64
        self.observed_repository = APP
        self.fail_prefix: tuple[str, ...] | None = None
        self.secrets: list[dict[str, str]] = []
        self.machine_list_responses: list[object] | None = None
        self.machine_list_state = "started"
        self.list_fail_remaining = 0

    def run(
        self, argv: tuple[str, ...], *, timeout: int, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append(("run", argv, timeout, check))
        if argv[:3] == ("flyctl", "apps", "create"):
            self.app_exists = True
        if self.fail_prefix is not None and argv[: len(self.fail_prefix)] == self.fail_prefix:
            raise OSError("secret-canary provider failure")
        if len(argv) >= 3 and argv[:3] == ("flyctl", "apps", "destroy"):
            if self.destroy_raises:
                raise RuntimeError("sensitive provider diagnostic")
            if not self.keep_app:
                self.app_exists = False
        if argv[:3] == ("flyctl", "sftp", "get"):
            Path(argv[4]).write_text("{}", encoding="utf-8")
        return subprocess.CompletedProcess(argv, 0, "", "")

    def json(self, argv: tuple[str, ...], *, timeout: int) -> object:
        self.calls.append(("json", argv, timeout))
        if self.fail_prefix is not None and argv[: len(self.fail_prefix)] == self.fail_prefix:
            raise OSError("secret-canary provider failure")
        if self.inventory_raises and argv[:4] == ("flyctl", "apps", "list", "--json"):
            raise OSError("sensitive provider diagnostic")
        if argv[:4] == ("flyctl", "apps", "list", "--json"):
            return [{"Name": APP}] if self.app_exists else []
        if argv[:3] == ("flyctl", "volumes", "create"):
            return {"id": VOLUME_ID}
        if argv[:3] == ("flyctl", "machine", "list"):
            if self.list_fail_remaining > 0:
                self.list_fail_remaining -= 1
                raise OSError("transient machine list failure")
            if self.machine_list_responses is not None:
                if self.machine_list_responses:
                    return self.machine_list_responses.pop(0)
                return [
                    {
                        "id": MACHINE_ID,
                        "name": f"{APP}-worker",
                        "state": "started",
                    }
                ]
            return [
                {
                    "id": MACHINE_ID,
                    "name": f"{APP}-worker",
                    "state": self.machine_list_state,
                }
            ]
        if argv[:3] == ("flyctl", "volumes", "list"):
            return [{"id": VOLUME_ID}]
        if argv[:3] == ("flyctl", "secrets", "list"):
            return self.secrets
        raise AssertionError(argv)

    def machine_state(self, app: str, machine_id: str, *, timeout: int) -> object:
        self.calls.append(("machine_state", app, machine_id, timeout))
        return {
            "id": MACHINE_ID,
            "name": f"{APP}-worker",
            "state": "started",
            "region": "dfw",
            "private_ip": "fdaa::1",
            "config": {
                "image": IMAGE,
                "auto_destroy": True,
                "init": {
                    "entrypoint": ["/bin/sleep"],
                    "cmd": [str(self.authorization.maximum_machine_seconds)],
                },
                "restart": {"policy": "no"},
                "services": [],
                "guest": {"cpu_kind": "performance", "cpus": 4, "memory_mb": 8192},
                "mounts": [{"path": "/work", "volume": VOLUME_ID}],
                "metadata": {
                    "graphforge_attempt_nonce": self.authorization.attempt_nonce,
                    "graphforge_commit": self.authorization.commit,
                    "graphforge_owner": self.authorization.teardown_owner,
                    "graphforge_machine_class": self.authorization.machine_class,
                },
            },
            "image_ref": {
                "registry": "registry.fly.io",
                "repository": self.observed_repository,
                "digest": self.observed_digest,
            },
        }

    def api_json(self, path: str, *, timeout: int) -> object:
        self.calls.append(("api_json", path, timeout))
        if path == f"/v1/apps/{APP}":
            return {
                "name": APP,
                "organization": {"slug": self.authorization.organization},
            }
        if path == f"/v1/apps/{APP}/volumes/{VOLUME_ID}":
            return {
                "id": VOLUME_ID,
                "name": f"{APP}-data",
                "region": self.authorization.region,
                "size_gb": self.authorization.volume_gib,
                "auto_backup_enabled": False,
                "attached_machine_id": MACHINE_ID,
            }
        raise AssertionError(path)


class ProgressiveFlyTransportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.boundary = FakeBoundary()
        self.auth = authorization(20)
        self.boundary.authorization = self.auth
        self.transport = FlyProviderTransport(
            self.boundary, clock=lambda: NOW, sleeper=lambda _seconds: None
        )
        self.invocation = AttemptInvocation(
            ROOT,
            self.base / "evidence",
            self.base / "ledger.json",
            self.auth.commit,
        )
        self.deadline = NOW + timedelta(hours=1)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def provision(self) -> None:
        result = self.transport.provision(self.invocation, self.auth, deadline=self.deadline)
        self.assertEqual(result.image_digest, IMAGE)
        self.assertEqual(result.resources, {"machine_id": MACHINE_ID, "volume_id": VOLUME_ID})

    def test_provision_is_zero_build_private_fixed_and_provider_observed(self) -> None:
        self.provision()
        commands = [call[1] for call in self.boundary.calls if call[0] == "run"]
        rendered = "\n".join(" ".join(command) for command in commands)
        self.assertNotIn(" deploy ", f" {rendered} ")
        self.assertNotIn("build", rendered)
        machine = next(
            command for command in commands if command[:3] == ("flyctl", "machine", "run")
        )
        self.assertEqual(
            machine,
            (
                "flyctl",
                "machine",
                "run",
                machine_run_image_ref(IMAGE, self.auth.commit),
                str(self.auth.maximum_machine_seconds),
                "--app",
                APP,
                "--name",
                f"{APP}-worker",
                "--metadata",
                f"graphforge_attempt_nonce={self.auth.attempt_nonce}",
                "--metadata",
                f"graphforge_commit={self.auth.commit}",
                "--metadata",
                f"graphforge_owner={self.auth.teardown_owner}",
                "--metadata",
                f"graphforge_machine_class={self.auth.machine_class}",
                "--region",
                "dfw",
                "--vm-size",
                "performance-4x",
                "--volume",
                f"{VOLUME_ID}:/work",
                "--entrypoint",
                "/bin/sleep",
                "--restart",
                "no",
                "--autostop",
                "off",
                "--autostart=false",
                "--rootfs-persist",
                "never",
                "--rm",
                "--skip-dns-registration",
                "--detach",
            ),
        )
        self.assertTrue(any(call[0] == "machine_state" for call in self.boundary.calls))

    def test_machine_readback_digest_is_not_synthesized_from_authorization(self) -> None:
        self.boundary.observed_digest = "sha256:" + "2" * 64
        result = self.transport.provision(self.invocation, self.auth, deadline=self.deadline)
        self.assertEqual(
            result.image_digest,
            f"registry.fly.io/{APP}@sha256:" + "2" * 64,
        )
        self.assertNotEqual(result.image_digest, self.auth.image_digest)

    def test_wrong_provider_repository_is_refused_even_with_authorized_digest(self) -> None:
        self.boundary.observed_repository = "another-app"
        with self.assertRaisesRegex(FlyTransportError, "differs"):
            self.transport.provision(self.invocation, self.auth, deadline=self.deadline)

    def test_existing_app_refuses_before_mutation(self) -> None:
        self.boundary.app_exists = True
        with self.assertRaisesRegex(FlyTransportError, "already exists"):
            self.transport.provision(self.invocation, self.auth, deadline=self.deadline)
        self.assertFalse(any(call[0] == "run" for call in self.boundary.calls))

    def test_expired_deadline_refuses_before_provider_call(self) -> None:
        with self.assertRaisesRegex(FlyTransportError, "expired"):
            self.transport.provision(
                self.invocation, self.auth, deadline=NOW - timedelta(seconds=1)
            )
        self.assertEqual(self.boundary.calls, [])

    def test_upload_execute_and_result_first_retrieval_are_canonical(self) -> None:
        self.provision()
        plan = self.base / "control-plan.json"
        plan.write_text("{}", encoding="utf-8")
        self.transport.upload_plan(rung=20, plan_path=plan, deadline=self.deadline)
        with tempfile.TemporaryDirectory(dir=self.base) as directory:
            stage = Path(directory)
            names = tuple(
                f"s20-{suffix}.json" for suffix in ("plan", "benchexec", "graphforge", "rung")
            )
            with self.assertRaisesRegex(FlyTransportError, "canonical"):
                self.transport.retrieve_success_artifacts(
                    rung=20,
                    names=names,
                    destination=stage,
                    deadline=self.deadline,
                )
            self.assertEqual(
                self.transport.execute_rung(rung=20, image_digest=IMAGE, deadline=self.deadline),
                0,
            )
            self.transport.retrieve_result(
                rung=20,
                destination=stage / "s20-result.json",
                deadline=self.deadline,
            )
            self.transport.retrieve_success_artifacts(
                rung=20,
                names=names,
                destination=stage,
                deadline=self.deadline,
            )
        transfers = [
            call[1] for call in self.boundary.calls if call[0] == "run" and call[1][1] == "sftp"
        ]
        self.assertEqual(transfers[0][2], "put")
        self.assertEqual(transfers[1][3], "/work/evidence/s20-result.json")
        self.assertEqual(
            [command[3] for command in transfers[2:]],
            [f"/work/evidence/{name}" for name in names],
        )
        executed = next(
            call
            for call in self.boundary.calls
            if call[0] == "run" and call[1][1:3] == ("machine", "exec")
        )
        self.assertFalse(executed[3])
        self.assertIn("/usr/local/bin/run-progressive-qualification", executed[1][-1])
        self.assertNotIn("FLY_API_TOKEN", " ".join(executed[1]))

    def test_teardown_is_best_effort_then_independently_observed(self) -> None:
        self.boundary.app_exists = True
        self.boundary.destroy_raises = True
        self.boundary.keep_app = True
        observed = self.transport.teardown(
            {
                "owner_app": APP,
                "machine_id": MACHINE_ID,
                "volume_id": VOLUME_ID,
            }
        )
        self.assertEqual(observed, {"app_exists": True, "machines": 1, "volumes": 1, "secrets": 0})
        operations = [call[1][:3] for call in self.boundary.calls if call[0] == "run"]
        self.assertEqual(
            operations,
            [
                ("flyctl", "machine", "destroy"),
                ("flyctl", "volumes", "destroy"),
                ("flyctl", "apps", "destroy"),
            ],
        )

    def test_owner_only_crash_recovery_destroys_app(self) -> None:
        self.boundary.app_exists = True
        observed = self.transport.teardown({"owner_app": APP})
        self.assertEqual(observed, {"app_exists": False, "machines": 0, "volumes": 0, "secrets": 0})

    def test_inventory_failure_is_typed_and_sanitized(self) -> None:
        self.boundary.app_exists = True
        self.boundary.inventory_raises = True
        with self.assertRaises(AttemptError) as raised:
            self.transport.teardown({"owner_app": APP})
        self.assertEqual(raised.exception.failure, "inventory_unavailable")
        self.assertNotIn("sensitive", str(raised.exception))

    def test_subsecond_deadline_never_rounds_up_into_provider_call(self) -> None:
        with self.assertRaisesRegex(FlyTransportError, "expired"):
            self.transport.provision(
                self.invocation,
                self.auth,
                deadline=NOW + timedelta(milliseconds=999),
            )
        self.assertEqual(self.boundary.calls, [])

    def test_machine_identity_is_revalidated_immediately_before_execution(self) -> None:
        self.provision()
        plan = self.base / "control-plan.json"
        plan.write_text("{}", encoding="utf-8")
        self.transport.upload_plan(rung=20, plan_path=plan, deadline=self.deadline)
        self.boundary.observed_digest = "sha256:" + "2" * 64
        before = len(self.boundary.calls)
        with self.assertRaisesRegex(FlyTransportError, "uploaded immutable plan"):
            self.transport.execute_rung(rung=20, image_digest=IMAGE, deadline=self.deadline)
        self.assertTrue(any(call[0] == "machine_state" for call in self.boundary.calls[before:]))
        self.assertFalse(
            any(
                call[0] == "run" and call[1][1:3] == ("machine", "exec")
                for call in self.boundary.calls[before:]
            )
        )

    def test_ambiguous_mutation_faults_always_reach_inventory_teardown(self) -> None:
        for index, prefix in enumerate(
            (
                ("flyctl", "apps", "create"),
                ("flyctl", "volumes", "create"),
                ("flyctl", "machine", "run"),
            )
        ):
            with self.subTest(prefix=prefix):
                boundary = FakeBoundary()
                auth = authorization(20)
                boundary.authorization = auth
                boundary.fail_prefix = prefix
                transport = FlyProviderTransport(
                    boundary, clock=lambda: NOW, sleeper=lambda _seconds: None
                )
                case = self.base / f"fault-{index}"
                outcome = execute(
                    AttemptInvocation(
                        ROOT,
                        case / "evidence",
                        case / "ledger.json",
                        auth.commit,
                    ),
                    auth,
                    transport=transport,
                    planner=planner,
                    prefix_reader=lambda *_args, **_kwargs: [
                        {"scale": 18},
                        {"scale": 19},
                    ],
                    now=NOW,
                    clock=lambda: NOW,
                )
                self.assertEqual(outcome.failure, "provision_failed")
                self.assertTrue(
                    any(
                        call[0] == "run" and call[1][:3] == ("flyctl", "apps", "destroy")
                        for call in boundary.calls
                    )
                )

    def test_owner_only_teardown_discovers_and_removes_every_resource(self) -> None:
        self.boundary.app_exists = True
        self.boundary.json = mock.Mock(wraps=self.boundary.json)
        observed = self.transport.teardown({"owner_app": APP})
        self.assertEqual(observed, {"app_exists": False, "machines": 0, "volumes": 0, "secrets": 0})
        operations = [call[1][:3] for call in self.boundary.calls if call[0] == "run"]
        self.assertEqual(
            operations,
            [
                ("flyctl", "machine", "destroy"),
                ("flyctl", "volumes", "destroy"),
                ("flyctl", "apps", "destroy"),
            ],
        )

    def test_unexpected_secret_is_removed_but_reported_as_sanitized_anomaly(self) -> None:
        self.boundary.app_exists = True
        self.boundary.secrets = [{"Name": "SECRET_CANARY"}]
        with self.assertRaises(AttemptError) as raised:
            self.transport.teardown({"owner_app": APP})
        self.assertEqual(raised.exception.failure, "inventory_unavailable")
        self.assertNotIn("SECRET_CANARY", str(raised.exception))
        unset = next(
            call[1]
            for call in self.boundary.calls
            if call[0] == "run" and call[1][:3] == ("flyctl", "secrets", "unset")
        )
        self.assertIn("SECRET_CANARY", unset)
        self.assertTrue(
            any(
                call[0] == "run" and call[1][:3] == ("flyctl", "apps", "destroy")
                for call in self.boundary.calls
            )
        )

    def test_concrete_boundary_uses_exact_environment_and_never_a_shell(self) -> None:
        token = "FlyV1 secret-canary"
        environment = {
            "FLY_API_TOKEN": token,
            "HOME": "/tmp/fly-home",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "XDG_CONFIG_HOME": "/tmp/fly-xdg",
        }
        boundary = FlyctlMachineBoundary(environment, APP, cwd=self.base)
        command = ("flyctl", "apps", "list", "--json")
        completed = subprocess.CompletedProcess(command, 0, "{}", "")
        with mock.patch("subprocess.run", return_value=completed) as run:
            boundary.run(command, timeout=7)
        _, kwargs = run.call_args
        self.assertIs(kwargs["shell"], False)
        self.assertEqual(kwargs["env"], environment)
        self.assertEqual(kwargs["timeout"], 7)
        self.assertNotIn(token, " ".join(run.call_args.args[0]))
        with self.assertRaisesRegex(FlyTransportError, "not allowed"):
            boundary.run(("flyctl", "auth", "token"), timeout=7)
        with self.assertRaisesRegex(FlyTransportError, "not allowed"):
            boundary.run(("flyctl", "apps", "list", "--access-token", token), timeout=7)
        for argument in (
            f"--access-token={token}",
            f"-t{token}",
            "--config=/tmp/foreign.toml",
        ):
            with (
                self.subTest(argument=argument),
                self.assertRaisesRegex(FlyTransportError, "not allowed"),
            ):
                boundary.run(("flyctl", "apps", "list", "--json", argument), timeout=7)
        with self.assertRaisesRegex(FlyTransportError, "not allowed"):
            boundary.run(("flyctl", "apps", "destroy", "production-app", "--yes"), timeout=7)
        with self.assertRaisesRegex(FlyTransportError, "not allowed"):
            boundary.run(
                ("flyctl", "machine", "list", "--app", "production-app", "--json"),
                timeout=7,
            )
        with self.assertRaisesRegex(FlyTransportError, "not allowed"):
            boundary.run(
                ("flyctl", "machine", "list", "-aproduction-app", "--json"),
                timeout=7,
            )
        with self.assertRaisesRegex(FlyTransportError, "not allowed"):
            boundary.run(("flyctl", "machine", "list", "--json"), timeout=7)

    def test_concrete_api_is_scoped_to_owned_app_and_resource_shapes(self) -> None:
        environment = {
            "FLY_API_TOKEN": "fixture-token",
            "HOME": "/tmp/fly-home",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "XDG_CONFIG_HOME": "/tmp/fly-xdg",
        }
        boundary = FlyctlMachineBoundary(environment, APP)
        for path in (
            "/v1/apps/production-app",
            f"/v1/apps/{APP}/machines/not-a-machine",
            f"/v1/apps/{APP}/volumes/production-volume",
            f"/v1/apps/{APP}/machines/{MACHINE_ID}/metadata",
        ):
            with self.subTest(path=path), self.assertRaisesRegex(FlyTransportError, "not allowed"):
                boundary.api_json(path, timeout=1)

    def test_concrete_api_failure_does_not_retain_token_canary(self) -> None:
        token = "FlyV1 secret-canary"
        environment = {
            "FLY_API_TOKEN": token,
            "HOME": "/tmp/fly-home",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "XDG_CONFIG_HOME": "/tmp/fly-xdg",
        }

        def fail(*_args: object, **_kwargs: object) -> object:
            raise OSError(token)

        boundary = FlyctlMachineBoundary(environment, APP, urlopen=fail)
        with self.assertRaises(FlyTransportError) as raised:
            boundary.machine_state(APP, MACHINE_ID, timeout=1)
        self.assertNotIn(token, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)

    def test_delayed_readiness_converges_without_mutation_retries(self) -> None:
        worker = {"id": MACHINE_ID, "name": f"{APP}-worker", "state": "starting"}
        started = {"id": MACHINE_ID, "name": f"{APP}-worker", "state": "started"}
        self.boundary.machine_list_responses = [[], [worker], [started]]
        sleeps: list[float] = []
        self.transport = FlyProviderTransport(
            self.boundary, clock=lambda: NOW, sleeper=sleeps.append
        )
        self.provision()
        self.assertEqual(len(sleeps), 2)
        machine_runs = [
            call[1]
            for call in self.boundary.calls
            if call[0] == "run" and call[1][:3] == ("flyctl", "machine", "run")
        ]
        self.assertEqual(len(machine_runs), 1)

    def test_terminal_machine_state_fails_immediately(self) -> None:
        self.boundary.machine_list_responses = [
            [{"id": MACHINE_ID, "name": f"{APP}-worker", "state": "stopped"}]
        ]
        with self.assertRaisesRegex(FlyTransportError, "terminal"):
            self.transport.provision(self.invocation, self.auth, deadline=self.deadline)

    def test_extra_machine_fails_immediately(self) -> None:
        self.boundary.machine_list_responses = [
            [
                {"id": MACHINE_ID, "name": f"{APP}-worker", "state": "started"},
                {"id": "1234567890abcd", "name": "other", "state": "started"},
            ]
        ]
        with self.assertRaisesRegex(FlyTransportError, "unexpected"):
            self.transport.provision(self.invocation, self.auth, deadline=self.deadline)

    def test_readiness_timeout_is_typed_and_stops_polling(self) -> None:
        self.boundary.machine_list_responses = [[], [], [], [], [], [], [], []]
        clock = {"now": NOW}

        def advance() -> datetime:
            return clock["now"]

        def sleep(seconds: float) -> None:
            clock["now"] = clock["now"] + timedelta(seconds=seconds)

        self.transport = FlyProviderTransport(self.boundary, clock=advance, sleeper=sleep)
        with self.assertRaises(AttemptError) as raised:
            self.transport.provision(
                self.invocation,
                self.auth,
                deadline=NOW + timedelta(seconds=2),
            )
        self.assertEqual(raised.exception.failure, "readiness_timeout")
        machine_runs = [
            call[1]
            for call in self.boundary.calls
            if call[0] == "run" and call[1][:3] == ("flyctl", "machine", "run")
        ]
        self.assertEqual(len(machine_runs), 1)

    def test_live_surfaces_are_wired_in_operator(self) -> None:
        repository = ROOT.parent
        operator = (
            repository / "benchmarks/harness/graphforge_bench/qualification_operator.py"
        ).read_text(encoding="utf-8")
        controller = (
            repository / "benchmarks/harness/graphforge_bench/progressive_ladder_qualification.py"
        ).read_text(encoding="utf-8")
        registry = (repository / "config/gate-registry.json").read_text(encoding="utf-8")
        self.assertIn("progressive_ladder_qualification", operator)
        self.assertIn("FlyProviderTransport", controller)
        self.assertIn("execute_attempt", controller)
        registry_doc = json.loads(registry)
        records = registry_doc["workflows"] + registry_doc.get("operator_gates", [])
        progressive = next(gate for gate in records if gate["id"] == "progressive-ladder")
        self.assertEqual(progressive["control_plane"], "pulumi_esc")
        self.assertEqual(
            progressive["path"],
            ".github/workflows/progressive-ladder.yml",
        )


if __name__ == "__main__":
    unittest.main()
