"""Execute the disposable Fly S18-S26 progressive ladder under ESC authority.

This controller consumes the protected ESC projections once, binds them to the
offline whole-attempt state machine, and delegates provider I/O to the
production-shaped Fly transport.  It never copies credentials into evidence.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping
import json
from pathlib import Path
import re
from typing import Any

from graphforge_bench.progressive_esc import EscCapsuleError, load_progressive_esc
from graphforge_bench.progressive_fly_transport import FlyctlMachineBoundary, FlyProviderTransport
from graphforge_bench.progressive_provider_attempt import (
    ATTEMPT_SCHEMA,
    AttemptError,
    AttemptRequest,
    SpendAuthorization,
    cleanup_only,
    execute_attempt,
)

COMMIT = re.compile(r"^[0-9a-f]{40}$")
BENCHMARKS_ROOT = Path(__file__).resolve().parents[2]
REPO_ROOT = BENCHMARKS_ROOT.parent
REFUSED_COMMIT = "0" * 40
REFUSED_DIGEST = "0" * 64
REFUSED_IMAGE = f"registry.fly.io/gf-progressive-{'0' * 32}@sha256:{REFUSED_DIGEST}"


class QualificationError(RuntimeError):
    """A typed, sanitized terminal failure before provider execution."""

    def __init__(self, failure: str, message: str):
        super().__init__(message)
        self.failure = failure


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _authorization_document(authorization: SpendAuthorization) -> dict[str, Any]:
    return {
        "schema": authorization.schema,
        "status": authorization.status,
        "provider": authorization.provider,
        "commit": authorization.commit,
        "admitted_plan_sha256": authorization.admitted_plan_sha256,
        "image_digest": authorization.image_digest,
        "organization": authorization.organization,
        "region": authorization.region,
        "machine_class": authorization.machine_class,
        "volume_gib": authorization.volume_gib,
        "rung": authorization.rung,
        "maximum_scale": authorization.maximum_scale,
        "attempt_nonce": authorization.attempt_nonce,
        "app": authorization.app,
        "issued_at": authorization.issued_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "expires_at": authorization.expires_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "teardown_owner": authorization.teardown_owner,
        "maximum_machine_seconds": authorization.maximum_machine_seconds,
        "resource_limits": dict(authorization.resource_limits),
        "pricing": dict(authorization.pricing),
        "claim": authorization.claim,
    }


def _refused_result(failure: str, *, commit: str = REFUSED_COMMIT) -> dict[str, Any]:
    return {
        "schema": ATTEMPT_SCHEMA,
        "status": "failed",
        "failure": failure,
        "cleanup_failure": None,
        "commit": commit,
        "authorized_maximum_scale": 20,
        "completed_scales": [18, 19],
        "first_failed_rung": None,
        "authorization_sha256": REFUSED_DIGEST,
        "admitted_plan_sha256": REFUSED_DIGEST,
        "authorized_image_digest": REFUSED_IMAGE,
        "observed_image_digest": None,
        "teardown_status": "not_required",
        "teardown_inventory_sha256": None,
        "claim": "engineering_evidence_only",
    }


def _load_provider_capacity(path: Path | None) -> Mapping[str, Any] | None:
    if path is None:
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise QualificationError(
            "authorization_refused", "provider capacity is malformed"
        ) from error
    if not isinstance(value, Mapping):
        raise QualificationError("authorization_refused", "provider capacity is malformed")
    return value


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    result.add_argument("--expected-sha", required=True)
    result.add_argument("--output-dir", type=Path, required=True)
    result.add_argument("--ledger", type=Path, required=True)
    result.add_argument("--result-out", type=Path, required=True)
    result.add_argument("--provider-capacity", type=Path)
    mode = result.add_mutually_exclusive_group(required=True)
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--cleanup-only", action="store_true")
    result.add_argument("--confirm-disposable", action="store_true")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if (args.execute or args.cleanup_only) and not args.confirm_disposable:
        _atomic_json(args.result_out, _refused_result("authorization_refused"))
        return 1
    if COMMIT.fullmatch(args.expected_sha) is None:
        _atomic_json(args.result_out, _refused_result("authorization_refused"))
        return 1

    try:
        provider_capacity = _load_provider_capacity(args.provider_capacity)
    except QualificationError as error:
        _atomic_json(args.result_out, _refused_result(error.failure, commit=args.expected_sha))
        return 1

    try:
        with load_progressive_esc() as capsule:
            authorization = capsule.take_spend_authorization()
            if args.expected_sha != authorization.commit:
                raise QualificationError(
                    "authorization_refused", "expected commit contradicts spend authorization"
                )
            boundary = FlyctlMachineBoundary(
                capsule.subprocess_environment(),
                authorization.app,
                cwd=REPO_ROOT,
            )
            transport = FlyProviderTransport(boundary)
            request = AttemptRequest(
                commit=authorization.commit,
                organization=authorization.organization,
                app=authorization.app,
                region=authorization.region,
                machine_class=authorization.machine_class,
                volume_gib=authorization.volume_gib,
                image_digest=authorization.image_digest,
                maximum_scale=authorization.maximum_scale,
                spend_authorization=_authorization_document(authorization),
                provider_capacity=provider_capacity,
            )
            if args.cleanup_only:
                outcome = cleanup_only(args.ledger, args.result_out, transport=transport)
            else:
                outcome = execute_attempt(
                    request,
                    root=BENCHMARKS_ROOT,
                    output_dir=args.output_dir,
                    ledger_path=args.ledger,
                    result_path=args.result_out,
                    boundary=transport,
                )
    except EscCapsuleError:
        _atomic_json(
            args.result_out, _refused_result("authorization_refused", commit=args.expected_sha)
        )
        return 1
    except QualificationError as error:
        _atomic_json(args.result_out, _refused_result(error.failure, commit=args.expected_sha))
        return 1
    except AttemptError as error:
        if not args.result_out.is_file():
            _atomic_json(
                args.result_out,
                _refused_result(error.failure, commit=args.expected_sha),
            )
        return 1

    print(json.dumps(outcome, sort_keys=True))
    return 0 if outcome["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
