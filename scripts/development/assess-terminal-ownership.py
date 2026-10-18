#!/usr/bin/env python3
"""Assess one exclusively created, bounded Rust ownership capture."""

import argparse
import json
from pathlib import Path

from terminal_ownership import assess_capture


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture", type=Path)
    parser.add_argument("--output", type=Path, help="create a new assessment; never overwrite")
    args = parser.parse_args()
    result = assess_capture(args.capture)
    serialized = json.dumps(result, indent=2) + "\n"
    if args.output:
        with args.output.open("x") as target:
            target.write(serialized)
    else:
        print(serialized, end="")
    return 0 if result["capture_complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
