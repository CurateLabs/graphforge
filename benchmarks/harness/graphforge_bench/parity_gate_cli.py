"""CLI entrypoint for the #959 parity gate status report."""

from __future__ import annotations

import json

from graphforge_bench.parity_gate import parity_gate_status


def main() -> int:
    status = parity_gate_status()
    print(json.dumps(status, indent=2, sort_keys=True))
    if not (status["structural_retirement_ready"] and status["prefix_parity_ready"]):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
