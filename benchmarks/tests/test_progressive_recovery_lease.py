from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

from graphforge_bench.progressive_provider_attempt import AttemptError
from graphforge_bench.progressive_recovery_lease import (
    ack_recovery_lease,
    build_recovery_lease,
    cleanup_expired_lease,
    load_recovery_lease,
    save_recovery_lease,
)
from tests.test_progressive_provider_attempt import APP, authorization

NOW = datetime(2026, 6, 1, tzinfo=timezone.utc)


class FakeTeardownTransport:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []
        self.inventory = {
            "app_exists": False,
            "machines": 0,
            "volumes": 0,
            "secrets": 0,
        }

    def teardown(self, resources: dict[str, str]) -> dict[str, object]:
        self.calls.append(dict(resources))
        return dict(self.inventory)


class ProgressiveRecoveryLeaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.auth = authorization(20)
        self.execution_deadline = NOW + timedelta(seconds=self.auth.maximum_machine_seconds)
        self.lease = build_recovery_lease(
            self.auth,
            execution_deadline=self.execution_deadline,
            acknowledged_at=NOW,
        )
        self.path = self.base / "attempt.recovery-lease.json"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_ack_persists_schema_valid_credential_free_receipt(self) -> None:
        loaded = ack_recovery_lease(self.path, self.lease)
        document = self.path.read_text(encoding="utf-8")
        self.assertNotIn("FLY_API_TOKEN", document)
        self.assertNotIn("machine_id", document)
        self.assertNotIn("volume_id", document)
        self.assertNotIn(str(self.base), document)
        self.assertEqual(loaded.app, APP)
        self.assertEqual(loaded.claim, "recovery_lease_only")
        again = load_recovery_lease(self.path)
        self.assertEqual(again.authorization_sha256, self.auth.authorization_sha256)
        self.assertEqual(again.cleanup_deadline, self.auth.expires_at)

    def test_tampered_lease_is_refused(self) -> None:
        save_recovery_lease(self.path, self.lease)
        text = self.path.read_text(encoding="utf-8").replace(APP, "gf-progressive-" + "d" * 32)
        self.path.write_text(text, encoding="utf-8")
        with self.assertRaises(AttemptError) as raised:
            load_recovery_lease(self.path)
        self.assertEqual(raised.exception.failure, "recovery_refused")

    def test_pre_expiry_cleanup_refuses_without_factory_call(self) -> None:
        calls: list[object] = []

        def factory(_lease: object) -> FakeTeardownTransport:
            calls.append(_lease)
            return FakeTeardownTransport()

        with self.assertRaises(AttemptError) as raised:
            cleanup_expired_lease(self.lease, transport_factory=factory, clock=lambda: NOW)
        self.assertEqual(raised.exception.failure, "recovery_refused")
        self.assertEqual(calls, [])

    def test_expired_cleanup_is_idempotent_and_owner_confined(self) -> None:
        transport = FakeTeardownTransport()
        factories = 0

        def factory(lease: object) -> FakeTeardownTransport:
            nonlocal factories
            factories += 1
            self.assertEqual(lease.app, APP)  # type: ignore[attr-defined]
            return transport

        after = self.auth.expires_at + timedelta(seconds=1)
        first = cleanup_expired_lease(self.lease, transport_factory=factory, clock=lambda: after)
        second = cleanup_expired_lease(self.lease, transport_factory=factory, clock=lambda: after)
        self.assertEqual(first, second)
        self.assertEqual(factories, 2)
        self.assertEqual(transport.calls, [{"owner_app": APP}, {"owner_app": APP}])

    def test_identity_mismatch_refuses_cleanup(self) -> None:
        other = build_recovery_lease(
            authorization(22),
            execution_deadline=self.execution_deadline,
            acknowledged_at=NOW,
        )
        with self.assertRaises(AttemptError) as raised:
            cleanup_expired_lease(
                self.lease,
                transport_factory=lambda _lease: FakeTeardownTransport(),
                clock=lambda: self.auth.expires_at + timedelta(seconds=1),
                expected=other,
            )
        self.assertEqual(raised.exception.failure, "recovery_refused")

    def test_secret_canaries_stay_out_of_diagnostics(self) -> None:
        token = "FLY_API_TOKEN=super-secret-canary"

        class NoisyTransport(FakeTeardownTransport):
            def teardown(self, resources: dict[str, str]) -> dict[str, object]:
                raise RuntimeError(token)

        with self.assertRaises(Exception) as raised:
            cleanup_expired_lease(
                self.lease,
                transport_factory=lambda _lease: NoisyTransport(),
                clock=lambda: self.auth.expires_at + timedelta(seconds=1),
            )
        # Factory errors propagate; callers must not embed them into lease docs.
        self.assertIn(token, str(raised.exception))
        document = ack_recovery_lease(self.path, self.lease)
        self.assertNotIn("super-secret", self.path.read_text(encoding="utf-8"))
        self.assertEqual(document.app, APP)


if __name__ == "__main__":
    unittest.main()
