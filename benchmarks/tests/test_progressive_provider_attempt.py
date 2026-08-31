from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from graphforge_bench.progressive_provider_attempt import (
    AttemptError,
    AttemptInvocation,
    AttemptRequest,
    ProvisionedAttempt,
    SpendAuthorization,
    _publish,
    cleanup_only,
    execute,
    execute_attempt,
    load_ledger,
    parse_spend_authorization,
)
from jsonschema import Draft202012Validator
from tests.test_progressive_provider_plan import result as local_result
from tests.test_progressive_provider_plan import rung as rung_evidence
from tests.test_progressive_run import benchexec as benchexec_evidence
from tests.test_progressive_run import graphforge as graphforge_evidence
from tests.test_progressive_run import passed_rung as provider_rung_evidence

ROOT = Path(__file__).resolve().parents[1]
COMMIT = subprocess.run(
    ["git", "-C", str(ROOT.parent), "rev-parse", "HEAD"],
    capture_output=True,
    check=True,
    text=True,
).stdout.strip()
NONCE = "c" * 32
APP = "gf-progressive-" + NONCE
IMAGE = f"registry.fly.io/{APP}@sha256:" + "1" * 64
NOW = datetime(2026, 6, 1, tzinfo=timezone.utc)


def first_plan() -> dict[str, object]:
    return {
        "status": "admitted",
        "execution_authorized": True,
        "execution_refusal": None,
        "next_rung": "S20",
        "image_digest": IMAGE,
    }


def authorization_document(maximum_scale: int = 26) -> dict[str, object]:
    """Exact benchmarks/schemas/progressive-spend-authorization.json fixture."""
    return {
        "schema": "graphforge-progressive-spend-authorization/1",
        "status": "authorized",
        "provider": "fly",
        "commit": COMMIT,
        "admitted_plan_sha256": hashlib.sha256(
            (json.dumps(first_plan(), indent=2, sort_keys=True) + "\n").encode("utf-8")
        ).hexdigest(),
        "image_digest": IMAGE,
        "organization": "fixture-org",
        "region": "dfw",
        "machine_class": "performance-4x",
        "volume_gib": 500,
        "rung": "S20",
        "maximum_scale": maximum_scale,
        "attempt_nonce": NONCE,
        "app": APP,
        "issued_at": "2026-06-01T00:00:00Z",
        "expires_at": "2026-06-01T05:00:00Z",
        "teardown_owner": "qualification-operator",
        "maximum_machine_seconds": 18_000,
        "resource_limits": {"apps": 1, "volumes": 1, "machines": 1, "image_builds": 0},
        "pricing": {
            "currency": "USD",
            "machine_microusd_per_hour": 1,
            "volume_microusd_per_gib_hour": 1,
            "transfer_allowance_microusd": 1,
            "estimated_total_microusd": 2506,
            "maximum_total_microusd": 3000,
        },
        "claim": "spend_authorization_only",
    }


def authorization(maximum_scale: int = 26) -> SpendAuthorization:
    return parse_spend_authorization(authorization_document(maximum_scale))


def write_provider_bundle(directory: Path, scale: int) -> None:
    """Write the real five-document provider result contract; never synthesize a rung."""
    directory.mkdir(parents=True, exist_ok=True)
    profile = ROOT / "profiles" / "graph500" / f"s{scale}-provider.json"
    identities = {
        "commit": COMMIT,
        "profile_id": f"graph500-s{scale}-provider",
        "profile_sha256": hashlib.sha256(profile.read_bytes()).hexdigest(),
        "image_digest": IMAGE,
        "generator": "sha256:" + "0" * 64,
        "generator_executable_sha256": "0" * 64,
        "gf_sha256": "0" * 64,
        "certify_sha256": "0" * 64,
        "benchexec_python_sha256": "0" * 64,
        "benchexec_version": "1.0",
        "admitted_plan_sha256": "0" * 64,
        "source_tree_sha256": "0" * 64,
    }
    paths = {
        kind: directory / f"s{scale}-{kind}.json"
        for kind in ("plan", "benchexec", "graphforge", "rung", "result")
    }
    plan = {
        "schema": "graphforge-progressive-provider-execution-plan/1",
        "rung": f"S{scale}",
        "execution": "provider_native_linux_benchexec",
        "identities": identities,
        "limits": {"wall_seconds": 14_400, "memory_bytes": 4_294_967_296, "cores": 16},
        "outputs": [path.name for path in paths.values()],
        "claim": "engineering_evidence_only",
    }
    graphforge = graphforge_evidence(scale)
    graphforge["profile_id"] = f"graph500-s{scale}-provider"
    rung = provider_rung_evidence(scale)
    rung.update(profile_id=f"graph500-s{scale}-provider", source="canonical_ladder")
    documents = {
        "plan": plan,
        "benchexec": benchexec_evidence(graphforge),
        "graphforge": graphforge,
        "rung": rung,
    }
    for kind, document in documents.items():
        paths[kind].write_text(json.dumps(document), encoding="utf-8")
    result = {
        "schema": "graphforge-progressive-provider-run-result/1",
        "rung": f"S{scale}",
        "status": "passed",
        "failure": None,
        "identities": identities,
        "artifacts": {
            f"{kind}_sha256": hashlib.sha256(paths[kind].read_bytes()).hexdigest()
            for kind in ("plan", "benchexec", "graphforge", "rung")
        },
        "claim": "engineering_evidence_only",
    }
    paths["result"].write_text(json.dumps(result), encoding="utf-8")


def planner(**values: object) -> dict[str, object]:
    output = Path(values["output_dir"])  # type: ignore[arg-type]
    present = {int(path.name[1:].split("-", 1)[0]) for path in output.glob("s*-rung.json")}
    scale = next(item for item in (20, 22, 24, 25, 26) if item not in present)
    return {
        "status": "admitted",
        "execution_authorized": True,
        "execution_refusal": None,
        "next_rung": f"S{scale}",
        "image_digest": values["image_digest"],
    }


class FakeTransport:
    def __init__(
        self,
        remote: Path,
        *,
        observed_image: str = IMAGE,
        fail_rung: int | None = None,
        omit_rung: int | None = None,
        corrupt_rung: int | None = None,
        fail_provision: bool = False,
        fail_teardown: bool = False,
        teardown_inventory: dict[str, object] | None = None,
        resources: dict[str, str] | None = None,
        diagnostic: str = "fixture failure",
    ) -> None:
        self.remote = remote
        self.observed_image = observed_image
        self.fail_rung = fail_rung
        self.omit_rung = omit_rung
        self.corrupt_rung = corrupt_rung
        self.fail_provision = fail_provision
        self.fail_teardown = fail_teardown
        self.teardown_inventory = teardown_inventory or {
            "app_exists": False,
            "machines": 0,
            "volumes": 0,
            "secrets": 0,
        }
        self.resources = resources or {
            "machine_id": "abcdef01234567",
            "volume_id": "vol_fixture123",
        }
        self.diagnostic = diagnostic
        self.calls: list[tuple[object, ...]] = []

    def provision(
        self,
        _invocation: AttemptInvocation,
        authorization: SpendAuthorization,
        *,
        deadline: datetime,
    ) -> ProvisionedAttempt:
        self.calls.append(("provision", authorization.app, deadline.isoformat()))
        if self.fail_provision:
            raise OSError(self.diagnostic)
        return ProvisionedAttempt(
            image_digest=self.observed_image,
            resources=self.resources,
        )

    def upload_plan(self, *, rung: int, plan_path: Path, deadline: datetime) -> None:
        self.calls.append(("upload_plan", rung))
        if not plan_path.is_file():
            raise AssertionError("admitted plan was not persisted")

    def execute_rung(self, *, rung: int, image_digest: str, deadline: datetime) -> int:
        self.calls.append(("execute_rung", rung, image_digest))
        write_provider_bundle(self.remote, rung)
        if rung == self.fail_rung:
            path = self.remote / f"s{rung}-result.json"
            value = json.loads(path.read_text(encoding="utf-8"))
            value.update(status="failed", failure="benchexec_failed", artifacts=None)
            path.write_text(json.dumps(value), encoding="utf-8")
            return 1
        if rung == self.omit_rung:
            (self.remote / f"s{rung}-graphforge.json").unlink()
        if rung == self.corrupt_rung:
            with (self.remote / f"s{rung}-benchexec.json").open("a", encoding="utf-8") as stream:
                stream.write("\n")
        return 0

    def retrieve_result(
        self, *, rung: int, destination: Path, deadline: datetime
    ) -> None:
        self.calls.append(("retrieve_result", rung))
        shutil.copyfile(self.remote / f"s{rung}-result.json", destination)

    def retrieve_success_artifacts(
        self,
        *,
        rung: int,
        names: tuple[str, ...],
        destination: Path,
        deadline: datetime,
    ) -> None:
        self.calls.append(("retrieve_success_artifacts", rung))
        for name in names:
            source = self.remote / name
            if source.is_file():
                shutil.copyfile(source, destination / name)

    def teardown(self, resources: dict[str, str]) -> dict[str, object]:
        self.calls.append(("teardown", tuple(sorted(resources))))
        if self.fail_teardown:
            raise OSError(self.diagnostic)
        return self.teardown_inventory


class ProgressiveProviderAttemptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.output = self.base / "evidence"
        self.remote = self.base / "remote"
        self.remote.mkdir()
        self.ledger = self.base / "attempt-ledger.json"
        self.invocation = AttemptInvocation(ROOT, self.output, self.ledger, COMMIT)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write_prefix(self, output: Path, *scales: int) -> None:
        output.mkdir(parents=True, exist_ok=True)
        for scale in scales:
            (output / f"s{scale}-rung.json").write_text(
                json.dumps(rung_evidence(scale)), encoding="utf-8"
            )
            (output / f"s{scale}-result.json").write_text(
                json.dumps(local_result(scale)), encoding="utf-8"
            )

    def execute_attempt(
        self, auth: SpendAuthorization, transport: FakeTransport
    ) -> dict[str, object]:
        return asdict(
            execute(
                self.invocation,
                auth,
                transport=transport,
                planner=planner,
                now=NOW,
                clock=lambda: NOW,
            )
        )

    def test_spend_refusal_precedes_every_mutation(self) -> None:
        invalid = authorization_document()
        invalid["status"] = "refused"
        transport = FakeTransport(self.remote)
        with self.assertRaises(AttemptError) as raised:
            parse_spend_authorization(invalid)
        self.assertEqual(raised.exception.failure, "authorization_refused")
        expired = authorization()
        with self.assertRaisesRegex(AttemptError, "expired"):
            execute(
                self.invocation,
                expired,
                transport=transport,
                planner=planner,
                now=datetime(2028, 1, 1, tzinfo=timezone.utc),
            )
        self.assertEqual(transport.calls, [])
        self.assertFalse(self.ledger.exists())

    def test_first_plan_hash_is_bound_before_provisioning(self) -> None:
        self.write_prefix(self.output, 18, 19)
        document = authorization_document()
        document["admitted_plan_sha256"] = "f" * 64
        transport = FakeTransport(self.remote)
        with self.assertRaisesRegex(AttemptError, "admitted plan"):
            execute(
                self.invocation,
                parse_spend_authorization(document),
                transport=transport,
                planner=planner,
                now=NOW,
                clock=lambda: NOW,
            )
        self.assertEqual(transport.calls, [])
        self.assertFalse(self.ledger.exists())

    def test_spend_lifetime_and_integer_ceiling_are_closed(self) -> None:
        too_long = authorization_document()
        too_long["expires_at"] = "2026-06-02T00:00:00Z"
        with self.assertRaisesRegex(AttemptError, "lifetime"):
            parse_spend_authorization(too_long)
        bool_money = authorization_document()
        bool_money["pricing"] = {**bool_money["pricing"], "maximum_total_microusd": True}  # type: ignore[dict-item]
        with self.assertRaises(AttemptError) as raised:
            parse_spend_authorization(bool_money)
        self.assertEqual(raised.exception.failure, "authorization_refused")
        zero_rate = authorization_document()
        zero_rate["pricing"] = {  # type: ignore[assignment]
            **zero_rate["pricing"],  # type: ignore[dict-item]
            "machine_microusd_per_hour": 0,
        }
        with self.assertRaises(AttemptError) as raised:
            parse_spend_authorization(zero_rate)
        self.assertEqual(raised.exception.failure, "authorization_refused")

    def test_execution_deadline_is_rechecked_between_rungs(self) -> None:
        self.write_prefix(self.output, 18, 19)
        deadline = datetime(2026, 6, 1, 5, tzinfo=timezone.utc)
        observations = iter((NOW,) * 8 + (deadline,) * 2)
        transport = FakeTransport(self.remote)
        outcome = asdict(
            execute(
                self.invocation,
                authorization(),
                transport=transport,
                planner=planner,
                clock=lambda: next(observations),
            )
        )
        executed = [call[1] for call in transport.calls if call[0] == "execute_rung"]
        self.assertEqual(executed, [20])
        self.assertEqual(outcome["failure"], "authorization_refused")

    def test_deadline_crossing_during_provider_operations_cannot_pass(self) -> None:
        deadline = datetime(2026, 6, 1, 5, tzinfo=timezone.utc)
        for stage, live_observations, expected_call in (
            ("upload", 3, "upload_plan"),
            ("result", 5, "retrieve_result"),
            ("final", 7, "retrieve_success_artifacts"),
        ):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                output, remote = base / "evidence", base / "remote"
                remote.mkdir()
                self.write_prefix(output, 18, 19)
                observations = iter(
                    (NOW,) * live_observations + (deadline,) * 3
                )
                transport = FakeTransport(remote)
                outcome = asdict(
                    execute(
                        AttemptInvocation(ROOT, output, base / "ledger.json", COMMIT),
                        authorization(20),
                        transport=transport,
                        planner=planner,
                        clock=lambda observations=observations: next(observations),
                    )
                )
                self.assertTrue(any(call[0] == expected_call for call in transport.calls))
                self.assertEqual(outcome["failure"], "authorization_refused")
                self.assertEqual(transport.calls[-1][0], "teardown")
                self.assertFalse(any(output.glob("s20-*.json")))

    def test_s18_and_s19_are_required_before_mutation(self) -> None:
        for prefix in ((), (18,)):
            with self.subTest(prefix=prefix), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "evidence"
                self.write_prefix(output, *prefix)
                invocation = AttemptInvocation(
                    ROOT, output, Path(directory) / "ledger.json", COMMIT
                )
                transport = FakeTransport(Path(directory) / "remote")
                transport.remote.mkdir()
                with self.assertRaisesRegex(AttemptError, "S18 and S19"):
                    execute(
                        invocation,
                        authorization(),
                        transport=transport,
                        planner=planner,
                        now=NOW,
                        clock=lambda: NOW,
                    )
                self.assertEqual(transport.calls, [])

    def test_order_maximum_and_first_failure_stop(self) -> None:
        self.write_prefix(self.output, 18, 19)
        transport = FakeTransport(self.remote, fail_rung=24)
        outcome = self.execute_attempt(authorization(25), transport)
        executed = [call[1] for call in transport.calls if call[0] == "execute_rung"]
        self.assertEqual(executed, [20, 22, 24])
        self.assertEqual(outcome["completed_scales"], (18, 19, 20, 22))
        self.assertEqual(outcome["first_failed_rung"], 24)
        self.assertEqual(transport.calls[-1][0], "teardown")

    def test_maximum_scale_is_a_hard_stop(self) -> None:
        self.write_prefix(self.output, 18, 19)
        transport = FakeTransport(self.remote)
        outcome = self.execute_attempt(authorization(22), transport)
        executed = [call[1] for call in transport.calls if call[0] == "execute_rung"]
        self.assertEqual(executed, [20, 22])
        self.assertEqual(outcome["status"], "passed")

    def test_missing_or_tampered_bundle_cannot_advance(self) -> None:
        for mutation in ("missing", "tampered"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                output, remote = base / "evidence", base / "remote"
                remote.mkdir()
                self.write_prefix(output, 18, 19)
                invocation = AttemptInvocation(ROOT, output, base / "ledger.json", COMMIT)
                transport = FakeTransport(
                    remote,
                    omit_rung=20 if mutation == "missing" else None,
                    corrupt_rung=20 if mutation == "tampered" else None,
                )
                outcome = asdict(
                    execute(
                        invocation,
                        authorization(),
                        transport=transport,
                        planner=planner,
                        now=NOW,
                        clock=lambda: NOW,
                    )
                )
                executed = [call[1] for call in transport.calls if call[0] == "execute_rung"]
                self.assertEqual(executed, [20])
                self.assertEqual(outcome["status"], "failed")
                self.assertNotIn(20, outcome["completed_scales"])
                self.assertEqual(transport.calls[-1][0], "teardown")

    def test_partial_publication_is_rolled_back(self) -> None:
        self.write_prefix(self.output, 18, 19)
        calls = 0

        def fail_third(source: Path, destination: Path) -> None:
            nonlocal calls
            calls += 1
            if calls == 3:
                raise AttemptError("retrieval_failed", "injected publication failure")
            _publish(source, destination)

        with patch(
            "graphforge_bench.progressive_provider_attempt._publish",
            side_effect=fail_third,
        ):
            outcome = self.execute_attempt(authorization(20), FakeTransport(self.remote))
        self.assertEqual(outcome["failure"], "retrieval_failed")
        self.assertFalse(any(self.output.glob("s20-*.json")))

    def test_rollback_io_failure_never_skips_teardown(self) -> None:
        self.write_prefix(self.output, 18, 19)
        transport = FakeTransport(self.remote, fail_rung=20)
        with patch(
            "graphforge_bench.progressive_provider_attempt._rollback_rung",
            side_effect=OSError("injected rollback failure"),
        ):
            outcome = self.execute_attempt(authorization(20), transport)
        self.assertEqual(transport.calls[-1][0], "teardown")
        self.assertEqual(outcome["cleanup_failure"], "evidence_cleanup_failed")
        self.assertEqual(load_ledger(self.ledger).phase, "cleanup_failed")

    def test_observed_image_mismatch_blocks_every_rung(self) -> None:
        self.write_prefix(self.output, 18, 19)
        wrong = f"registry.fly.io/{APP}@sha256:" + "2" * 64
        transport = FakeTransport(self.remote, observed_image=wrong)
        outcome = self.execute_attempt(authorization(), transport)
        self.assertEqual(outcome["failure"], "machine_identity_mismatch")
        self.assertFalse(any(call[0] == "execute_rung" for call in transport.calls))
        self.assertEqual(transport.calls[-1][0], "teardown")

    def test_malformed_provider_ids_still_teardown_owned_app(self) -> None:
        self.write_prefix(self.output, 18, 19)
        transport = FakeTransport(
            self.remote,
            resources={"machine_id": "bad", "volume_id": "also-bad"},
        )
        outcome = self.execute_attempt(authorization(), transport)
        self.assertEqual(outcome["failure"], "provision_failed")
        self.assertEqual(sum(call[0] == "teardown" for call in transport.calls), 1)
        self.assertIn("owner_app", transport.calls[-1][1])

    def test_complete_prefix_refuses_without_transport(self) -> None:
        transport = FakeTransport(self.remote)
        prefix = [rung_evidence(scale) for scale in (18, 19, 20, 22, 24, 25, 26)]
        with self.assertRaisesRegex(AttemptError, "already complete"):
            execute(
                self.invocation,
                authorization(),
                transport=transport,
                planner=planner,
                prefix_reader=lambda *_args, **_kwargs: prefix,
                now=NOW,
                clock=lambda: NOW,
            )
        self.assertEqual(transport.calls, [])

    def test_teardown_always_runs_and_incomplete_cleanup_keeps_ledger(self) -> None:
        self.write_prefix(self.output, 18, 19)
        for fail_provision, fail_rung in ((True, None), (False, 20)):
            with tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                output, remote = base / "evidence", base / "remote"
                remote.mkdir()
                self.write_prefix(output, 18, 19)
                transport = FakeTransport(
                    remote, fail_provision=fail_provision, fail_rung=fail_rung
                )
                outcome = asdict(
                    execute(
                        AttemptInvocation(ROOT, output, base / "ledger.json", COMMIT),
                        authorization(),
                        transport=transport,
                        planner=planner,
                        now=NOW,
                        clock=lambda: NOW,
                    )
                )
                self.assertEqual(sum(call[0] == "teardown" for call in transport.calls), 1)
                self.assertEqual(outcome["status"], "failed")

        transport = FakeTransport(self.remote, fail_rung=20, fail_teardown=True)
        outcome = self.execute_attempt(authorization(), transport)
        self.assertEqual(outcome["cleanup_failure"], "teardown_failed")
        persisted = load_ledger(self.ledger)
        self.assertEqual(persisted.phase, "cleanup_failed")
        self.assertEqual(persisted.resources["machine_id"], "abcdef01234567")

        recovery = FakeTransport(self.remote)
        result_path = self.base / "recovery-result.json"
        (self.output / "s20-plan.json").write_text("partial")
        (self.output / "s20-result.json").write_text("partial")
        recovered = cleanup_only(self.ledger, result_path, transport=recovery)
        self.assertEqual(recovered["teardown_status"], "empty")
        self.assertFalse(any(self.output.glob("s20-*.json")))
        self.assertEqual(load_ledger(self.ledger).phase, "closed")
        repeated = cleanup_only(self.ledger, result_path, transport=recovery)
        self.assertEqual(repeated["teardown_status"], "empty")

    def test_cleanup_only_tears_down_when_evidence_rollback_fails(self) -> None:
        self.write_prefix(self.output, 18, 19)
        execute(
            self.invocation,
            authorization(20),
            transport=FakeTransport(self.remote, fail_rung=20, fail_teardown=True),
            planner=planner,
            now=NOW,
            clock=lambda: NOW,
        )
        transport = FakeTransport(self.remote)
        with patch(
            "graphforge_bench.progressive_provider_attempt._rollback_rung",
            side_effect=OSError("injected rollback failure"),
        ):
            outcome = cleanup_only(
                self.ledger, self.base / "rollback-recovery.json", transport=transport
            )
        self.assertEqual(transport.calls[-1][0], "teardown")
        self.assertEqual(outcome["cleanup_failure"], "evidence_cleanup_failed")
        ledger = load_ledger(self.ledger)
        self.assertEqual(ledger.phase, "cleanup_failed")
        self.assertFalse(ledger.resources)

    def test_public_outcome_excludes_sensitive_provider_diagnostics(self) -> None:
        self.write_prefix(self.output, 18, 19)
        diagnostic = "token=secret Bearer abc@example.com vol_private abcdef01234567 /Users/private"
        outcome = self.execute_attempt(
            authorization(),
            FakeTransport(self.remote, fail_provision=True, diagnostic=diagnostic),
        )
        encoded = json.dumps(outcome)
        for fragment in diagnostic.split():
            self.assertNotIn(fragment, encoded)

    def test_nonempty_inventory_is_a_cleanup_failure(self) -> None:
        self.write_prefix(self.output, 18, 19)
        transport = FakeTransport(
            self.remote,
            teardown_inventory={
                "app_exists": True,
                "machines": 2,
                "volumes": 0,
                "secrets": 1,
            },
        )
        outcome = self.execute_attempt(authorization(20), transport)
        self.assertEqual(outcome["cleanup_failure"], "inventory_not_empty")
        self.assertEqual(outcome["teardown_status"], "failed")
        self.assertTrue(load_ledger(self.ledger).resources)

    def test_cleanup_refuses_mismatched_ledger_owner_before_transport(self) -> None:
        self.write_prefix(self.output, 18, 19)
        execute(
            self.invocation,
            authorization(20),
            transport=FakeTransport(self.remote, fail_teardown=True),
            planner=planner,
            now=NOW,
            clock=lambda: NOW,
        )
        document = json.loads(self.ledger.read_text())
        document["attempt_id"] = "d" * 32
        self.ledger.write_text(json.dumps(document))
        transport = FakeTransport(self.remote)
        with self.assertRaisesRegex(AttemptError, "ownership"):
            cleanup_only(self.ledger, self.base / "recovery.json", transport=transport)
        self.assertEqual(transport.calls, [])

    def test_written_ledger_result_and_teardown_inventory_match_schemas(self) -> None:
        self.write_prefix(self.output, 18, 19)
        result_path = self.base / "attempt-result.json"
        document = authorization_document(20)
        outcome = execute_attempt(
            AttemptRequest(
                commit=COMMIT,
                organization="fixture-org",
                app=APP,
                region="dfw",
                machine_class="performance-4x",
                volume_gib=500,
                image_digest=IMAGE,
                maximum_scale=20,
                spend_authorization=document,
            ),
            root=ROOT,
            output_dir=self.output,
            ledger_path=self.ledger,
            result_path=result_path,
            boundary=FakeTransport(self.remote),
            planner=planner,
            now=NOW,
            clock=lambda: NOW,
        )
        inventory_path = self.base / "attempt-result-teardown-inventory.json"
        for schema_name, value in (
            ("progressive-spend-authorization.json", document),
            ("progressive-provider-attempt-ledger.json", json.loads(self.ledger.read_text())),
            ("progressive-provider-attempt-result.json", outcome),
            (
                "progressive-provider-teardown-inventory.json",
                json.loads(inventory_path.read_text()),
            ),
        ):
            schema = json.loads((ROOT / "schemas" / schema_name).read_text())
            Draft202012Validator(schema).validate(value)


if __name__ == "__main__":
    unittest.main()
