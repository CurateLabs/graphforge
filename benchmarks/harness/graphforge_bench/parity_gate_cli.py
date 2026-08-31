"""CLI entrypoint for the #959 parity gate status report."""

from __future__ import annotations

import json
import sys

from graphforge_bench.parity_gate import assert_tiny_parity_ready, parity_gate_status


def main() -> int:
    status = parity_gate_status()
    print(json.dumps(status, indent=2, sort_keys=True))
    try:
        assert_tiny_parity_ready()
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 1
    if not status["ready_for_retirement"]:
        print(
            "parity gate: tiny shadow OK; retirement blocked on #900 ladder bundles",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
