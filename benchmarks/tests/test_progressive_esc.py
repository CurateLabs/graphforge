from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
import unittest
from unittest.mock import patch

from graphforge_bench import progressive_esc
from graphforge_bench.progressive_esc import (
    FLY_TOKEN_ENV,
    SPEND_AUTHORIZATION_ENV,
    EscCapsuleError,
    load_progressive_esc,
)

COMMIT = "a" * 40
NONCE = "b" * 32
APP = f"gf-progressive-{NONCE}"
IMAGE = f"registry.fly.io/{APP}@sha256:" + "c" * 64
TOKEN = "FlyV1 fixture-secret-token"


def authorization_document() -> dict[str, object]:
    plan = {
        "schema": "graphforge-progressive-provider-plan/1",
        "status": "admitted",
        "execution_authorized": True,
        "execution_refusal": None,
        "next_rung": "S20",
        "image_digest": IMAGE,
    }
    plan_sha = hashlib.sha256(
        (json.dumps(plan, indent=2, sort_keys=True) + "\n").encode()
    ).hexdigest()
    return {
        "schema": "graphforge-progressive-spend-authorization/1",
        "status": "authorized",
        "provider": "fly",
        "commit": COMMIT,
        "admitted_plan_sha256": plan_sha,
        "image_digest": IMAGE,
        "organization": "fixture-org",
        "region": "dfw",
        "machine_class": "performance-4x",
        "volume_gib": 500,
        "rung": "S20",
        "maximum_scale": 20,
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


def projected_environment(**extra: str) -> dict[str, str]:
    return {
        FLY_TOKEN_ENV: TOKEN,
        SPEND_AUTHORIZATION_ENV: json.dumps(authorization_document()),
        "UNRELATED_AMBIENT_VALUE": "must-not-propagate",
        **extra,
    }


class ProgressiveEscTests(unittest.TestCase):
    def test_consumes_projections_once_and_redacts_representation(self) -> None:
        environ = projected_environment()
        with load_progressive_esc(environ) as capsule:
            self.assertNotIn(FLY_TOKEN_ENV, environ)
            self.assertNotIn(SPEND_AUTHORIZATION_ENV, environ)
            encoded = repr(capsule)
            self.assertNotIn(TOKEN, encoded)
            self.assertNotIn(COMMIT, encoded)
            self.assertEqual(capsule.take_spend_authorization().commit, COMMIT)
            with self.assertRaises(EscCapsuleError):
                capsule.take_spend_authorization()
        with self.assertRaises(EscCapsuleError):
            load_progressive_esc(environ)

    def test_provider_environment_is_minimal_and_uses_fresh_config(self) -> None:
        with load_progressive_esc(projected_environment()) as capsule:
            provider = capsule.subprocess_environment()
            self.assertEqual(
                set(provider),
                {FLY_TOKEN_ENV, "HOME", "LANG", "LC_ALL", "PATH", "XDG_CONFIG_HOME"},
            )
            self.assertEqual(provider[FLY_TOKEN_ENV], TOKEN)
            self.assertNotIn(TOKEN, repr(provider))
            self.assertNotIn(SPEND_AUTHORIZATION_ENV, provider)
            self.assertNotIn("UNRELATED_AMBIENT_VALUE", provider)
            for name in ("HOME", "XDG_CONFIG_HOME"):
                path = Path(provider[name])
                self.assertTrue(path.is_dir())
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o700)
                self.assertTrue(str(path).startswith(str(capsule.home.parent)))
        self.assertFalse(capsule.home.parent.exists())
        self.assertEqual(provider[FLY_TOKEN_ENV], "")
        with self.assertRaises(EscCapsuleError):
            capsule.subprocess_environment()

    def test_missing_or_malformed_projections_are_scrubbed(self) -> None:
        cases = (
            {},
            {FLY_TOKEN_ENV: TOKEN},
            {SPEND_AUTHORIZATION_ENV: json.dumps(authorization_document())},
            projected_environment(**{FLY_TOKEN_ENV: " token-with-whitespace "}),
            projected_environment(**{SPEND_AUTHORIZATION_ENV: "{}"}),
        )
        for environ in cases:
            with self.subTest(environ=set(environ)), self.assertRaises(EscCapsuleError):
                load_progressive_esc(environ)
            self.assertNotIn(FLY_TOKEN_ENV, environ)
            self.assertNotIn(SPEND_AUTHORIZATION_ENV, environ)

    def test_rejects_aliases_and_network_or_logging_overrides(self) -> None:
        for name in (
            "FLY_ACCESS_TOKEN",
            "fly_access_token",
            "HTTP_PROXY",
            "https_proxy",
            "ALL_PROXY",
            "NO_PROXY",
            "FLY_DEBUG",
            "FLY_LOG_LEVEL",
            "LOG_LEVEL",
            "PULUMI_LOG_LEVEL",
            "RUST_LOG",
            "DEBUG",
        ):
            environ = projected_environment(**{name: "secret-alias"})
            with self.subTest(name=name), self.assertRaisesRegex(EscCapsuleError, "override"):
                load_progressive_esc(environ)
            self.assertNotIn(FLY_TOKEN_ENV, environ)
            self.assertNotIn(SPEND_AUTHORIZATION_ENV, environ)
            if name.upper() == "FLY_ACCESS_TOKEN":
                self.assertNotIn(name, environ)

    def test_rejects_and_scrubs_all_case_variants_and_duplicate_aliases(self) -> None:
        cases = (
            {"fly_api_token": "lower-token"},
            {"graphforge_progressive_spend_authorization": "lower-authorization"},
            {"FLY_ACCESS_TOKEN": "first-alias", "fly_access_token": "second-alias"},
        )
        protected = {
            FLY_TOKEN_ENV,
            SPEND_AUTHORIZATION_ENV,
            "FLY_ACCESS_TOKEN",
        }
        for extras in cases:
            environ = projected_environment(**extras)
            with self.subTest(extras=set(extras)), self.assertRaises(EscCapsuleError):
                load_progressive_esc(environ)
            self.assertFalse(any(name.upper() in protected for name in environ))

    def test_invalid_authorization_is_not_retained_by_exception_chain(self) -> None:
        authorization_canary = "protected-authorization-canary"
        token_canary = "protected-token-canary"
        environ = {
            FLY_TOKEN_ENV: token_canary,
            SPEND_AUTHORIZATION_ENV: f'{{"canary":"{authorization_canary}", BROKEN',
        }
        with self.assertRaises(EscCapsuleError) as raised:
            load_progressive_esc(environ)
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)
        self.assertNotIn(authorization_canary, repr(raised.exception))
        self.assertNotIn(token_canary, repr(raised.exception))

    def test_cleanup_failure_disables_capsule_and_remains_retryable(self) -> None:
        capsule = load_progressive_esc(projected_environment())
        root = capsule.home.parent
        cleanup = capsule._temporary.cleanup
        attempts = 0

        def flaky_cleanup() -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise OSError("fixture cleanup failure")
            cleanup()

        with patch.object(capsule._temporary, "cleanup", side_effect=flaky_cleanup):
            with self.assertRaises(OSError):
                capsule.close()
            with self.assertRaises(EscCapsuleError):
                capsule.subprocess_environment()
            with self.assertRaises(EscCapsuleError):
                capsule.take_spend_authorization()
            self.assertTrue(root.exists())
            capsule.close()
        self.assertEqual(attempts, 2)
        self.assertFalse(root.exists())

    def test_module_has_no_command_execution_surface(self) -> None:
        source = Path(progressive_esc.__file__).read_text(encoding="utf-8")
        self.assertNotIn("import subprocess", source)
        self.assertNotIn("flyctl", source)
        self.assertNotIn("def main(", source)

    def test_default_loader_consumes_real_process_environment(self) -> None:
        before = dict(os.environ)
        try:
            os.environ.clear()
            os.environ.update(projected_environment())
            with load_progressive_esc() as capsule:
                self.assertEqual(capsule.take_spend_authorization().app, APP)
                self.assertNotIn(FLY_TOKEN_ENV, os.environ)
                self.assertNotIn(SPEND_AUTHORIZATION_ENV, os.environ)
        finally:
            os.environ.clear()
            os.environ.update(before)


if __name__ == "__main__":
    unittest.main()
