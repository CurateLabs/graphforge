"""CLI for progressive qualification ESC readiness checks."""

from __future__ import annotations

import argparse
import json
import sys

from graphforge_bench.esc_readiness import EscReadinessError, assert_esc_ready, esc_readiness_status


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    result.add_argument("--environment", required=True)
    result.add_argument(
        "--assert-ready",
        action="store_true",
        help="exit non-zero when required projections are missing or invalid",
    )
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.assert_ready:
        try:
            assert_esc_ready(args.environment)
        except EscReadinessError as error:
            print(f"esc readiness refused: {error}", file=sys.stderr)
            return 2
        print(json.dumps(esc_readiness_status(args.environment), indent=2, sort_keys=True))
        return 0
    status = esc_readiness_status(args.environment)
    print(json.dumps(status, indent=2, sort_keys=True))
    return 0 if status["ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
