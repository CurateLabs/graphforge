from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from graphforge_bench.esc_readiness import (
    EscReadinessError,
    assert_esc_ready,
    esc_readiness_status,
)
from graphforge_bench.progressive_esc import FLY_TOKEN_ENV, SPEND_AUTHORIZATION_ENV
from tests.test_progressive_provider_attempt import authorization_document


class EscReadinessTests(unittest.TestCase):
    def test_empty_environment_not_ready(self) -> None:
        with patch(
            "graphforge_bench.esc_readiness._open_environment",
            return_value={"environmentVariables": {}},
        ):
            status = esc_readiness_status("curatelabs/graphforge/qualification")
        self.assertFalse(status["ready"])
        self.assertFalse(status["spend_authorization_valid"])
        json.dumps(status)

    def test_ready_when_projections_and_authorization_validate(self) -> None:
        document = authorization_document()
        with patch(
            "graphforge_bench.esc_readiness._open_environment",
            return_value={
                "environmentVariables": {
                    FLY_TOKEN_ENV: "fixture-token",
                    SPEND_AUTHORIZATION_ENV: json.dumps(document),
                }
            },
        ):
            status = esc_readiness_status(
                "curatelabs/graphforge/qualification", gate="progressive-ladder"
            )
        self.assertTrue(status["ready"])
        self.assertTrue(status["spend_authorization_valid"])

    def test_fly_tiny_requires_only_token(self) -> None:
        with patch(
            "graphforge_bench.esc_readiness._open_environment",
            return_value={"environmentVariables": {FLY_TOKEN_ENV: "fixture-token"}},
        ):
            status = esc_readiness_status("curatelabs/graphforge/qualification", gate="fly-tiny")
        self.assertTrue(status["ready"])
        self.assertTrue(status["spend_authorization_valid"])

    def test_assert_ready_raises_for_missing_projections(self) -> None:
        with (
            patch(
                "graphforge_bench.esc_readiness._open_environment",
                return_value={"environmentVariables": {}},
            ),
            self.assertRaises(EscReadinessError),
        ):
            assert_esc_ready("curatelabs/graphforge/qualification")

    def test_invalid_environment_name(self) -> None:
        status = esc_readiness_status("../invalid")
        self.assertFalse(status["ready"])
        self.assertIn("invalid", status["failure"])


if __name__ == "__main__":
    unittest.main()
