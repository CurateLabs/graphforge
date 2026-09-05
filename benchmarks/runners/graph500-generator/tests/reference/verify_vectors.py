#!/usr/bin/env python3
"""Verify frozen vectors with Python integer arithmetic and pinned upstream C.

Run directly with Python 3 and a C compiler. --write deliberately replaces the
fixtures when reviewing an intentional generator contract change.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import tempfile

MASK = (1 << 64) - 1
HERE = Path(__file__).resolve().parent


def splitmix(seed: int):
    while True:
        seed = (seed + 0x9E3779B97F4A7C15) & MASK
        z = ((seed ^ (seed >> 30)) * 0xBF58476D1CE4E5B9) & MASK
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK
        yield z ^ (z >> 31)


def raw_edges(scale: int, seed: int, count: int):
    random = splitmix(seed)
    for _ in range(count):
        source = target = 0
        for _ in range(scale):
            # 2**64 = 100*q + 16; rejection leaves equally sized residue classes.
            value = next(random)
            while value < 16:
                value = next(random)
            quadrant = value % 100
            source = 2 * source + int(quadrant >= 76)
            target = 2 * target + int(57 <= quadrant < 76 or quadrant >= 95)
        yield source, target


def upstream_scramble(executable: Path, cases: list[tuple[int, int, int, int]]) -> list[int]:
    output = subprocess.run(
        [str(executable)],
        input="".join(" ".join(map(str, row)) + "\n" for row in cases),
        text=True,
        capture_output=True,
        check=True,
    )
    values = [int(value) for value in output.stdout.splitlines()]
    assert len(values) == len(cases)
    return values


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as directory:
        executable = Path(directory) / "graph500-scramble"
        subprocess.run(
            [
                "cc",
                "-std=c99",
                "-Wall",
                "-Wextra",
                "-Werror",
                str(HERE / "graph500_scramble.c"),
                "-o",
                str(executable),
            ],
            check=True,
        )
        cases = [
            (scale, vertex, key0, key1)
            for scale in (1, 6, 26, 48, 63)
            for vertex in (0, 1, (1 << scale) - 1)
            for key0, key1 in ((0, 0), (MASK, MASK), (0x123456789ABCDEF0, 0xFEDCBA9876543210))
        ]
        vectors = {
            "scramble-vectors.tsv": "# scale vertex key0 key1 scrambled\n"
            + "".join(
                " ".join(map(str, (*case, value))) + "\n"
                for case, value in zip(cases, upstream_scramble(executable, cases), strict=True)
            )
        }
        rows = ["# scale seed tuple_index source target\n"]
        for scale, seed, count in ((1, 1, 32), (6, 7, 16), (26, MASK, 16)):
            keys = splitmix(seed ^ int.from_bytes(b"GRAPH500", "big"))
            key0, key1 = next(keys), next(keys)
            raw = list(raw_edges(scale, seed, count))
            endpoints = upstream_scramble(
                executable,
                [(scale, vertex, key0, key1) for edge in raw for vertex in edge],
            )
            for index in range(count):
                rows.append(
                    f"{scale} {seed} {index} {endpoints[2 * index]} {endpoints[2 * index + 1]}\n"
                )
        vectors["generator-vectors.tsv"] = "".join(rows)
        for name, expected in vectors.items():
            path = HERE.parent / name
            if args.write:
                path.write_text(expected)
            assert path.read_text() == expected, f"{name} differs from independent reference"
        print("45 pinned upstream scramble vectors and 64 complete-generator tuples verified")


if __name__ == "__main__":
    main()
