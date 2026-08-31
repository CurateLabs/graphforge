"""Validate Pulumi ESC projections required for live progressive qualification."""

from __future__ import annotations

from collections.abc import Mapping
import json
import subprocess
from typing import Any

from graphforge_bench.progressive_esc import FLY_TOKEN_ENV, SPEND_AUTHORIZATION_ENV
from graphforge_bench.progressive_provider_attempt import parse_spend_authorization
from graphforge_bench.qualification_operator import ESC_ENVIRONMENT

READINESS_SCHEMA = "graphforge-progressive-esc-readiness/1"
REQUIRED_VARIABLES = (FLY_TOKEN_ENV, SPEND_AUTHORIZATION_ENV)


class EscReadinessError(ValueError):
    """The protected ESC environment is missing or malformed."""


def _open_environment(environment: str) -> Mapping[str, Any]:
    if ESC_ENVIRONMENT.fullmatch(environment) is None:
        raise EscReadinessError("Pulumi ESC environment name is invalid")
    try:
        completed = subprocess.run(
            ("pulumi", "env", "open", environment),
            check=False,
            text=True,
            capture_output=True,
        )
    except OSError as error:
        raise EscReadinessError("unable to open Pulumi ESC environment") from error
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "unknown error"
        raise EscReadinessError(f"unable to open Pulumi ESC environment: {detail}")
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise EscReadinessError("Pulumi ESC environment output is malformed") from error
    if not isinstance(value, Mapping):
        raise EscReadinessError("Pulumi ESC environment output is malformed")
    return value


def _projected_variables(document: Mapping[str, Any]) -> Mapping[str, Any]:
    variables = document.get("environmentVariables")
    if not isinstance(variables, Mapping):
        return {}
    return variables


def _non_empty_string(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def esc_readiness_status(environment: str) -> dict[str, Any]:
    """Report whether an ESC environment projects the progressive qualification inputs."""
    try:
        opened = _open_environment(environment)
    except EscReadinessError as error:
        return {
            "schema": READINESS_SCHEMA,
            "environment": environment,
            "ready": False,
            "failure": str(error),
            "projections": [],
        }

    variables = _projected_variables(opened)
    projections: list[dict[str, Any]] = []
    ready = True
    for name in REQUIRED_VARIABLES:
        present = name in variables
        valid = present and _non_empty_string(variables.get(name))
        if not valid:
            ready = False
        projections.append({"name": name, "present": present, "valid": valid})

    spend_valid = False
    if ready:
        try:
            parse_spend_authorization(str(variables[SPEND_AUTHORIZATION_ENV]))
            spend_valid = True
        except Exception:
            ready = False
            spend_valid = False

    return {
        "schema": READINESS_SCHEMA,
        "environment": environment,
        "ready": ready,
        "failure": None if ready else "protected projections are unavailable or invalid",
        "spend_authorization_valid": spend_valid,
        "projections": projections,
    }


def assert_esc_ready(environment: str) -> None:
    """Fail closed when the ESC environment cannot authorize live qualification."""
    status = esc_readiness_status(environment)
    if not status["ready"]:
        raise EscReadinessError(status.get("failure") or "ESC environment is not ready")
