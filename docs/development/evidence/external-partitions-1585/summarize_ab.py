"""Summarize an ab.sh output directory.

Reports wall and CPU time per run, per-pair deltas, query-answer identity and
CAS digest overlap.  usage: summarize_ab.py AB_DIR
"""

import itertools
import json
from pathlib import Path
import statistics
import sys

QUERIES = ["nodes", "node-scan", "edges", "edge-scan"]


def first_json_line(path: Path) -> dict:
    with path.open() as handle:
        return json.loads(handle.readline())


def load(run: Path) -> dict:
    with (run / "runexec.txt").open() as handle:
        fields = dict(line.strip().split("=", 1) for line in handle if "=" in line)
    answers = {}
    for query in QUERIES:
        receipt = first_json_line(run / f"{query}.json")
        answers[query] = (receipt["rows"], receipt.get("scalar_u64"), receipt["result_sha256"])
    return {
        "wall": float(fields["walltime"].rstrip("s")),
        "cpu": float(fields["cputime"].rstrip("s")),
        "answers": answers,
        "cas": set((run / "cas-digests.txt").read_text().split()),
    }


def main(root: Path) -> None:
    runs = {run.name: load(run) for run in sorted((root / "runs").glob("s*-r*-*"))}
    for scale in ["s18", "s20"]:
        sel = {name: run for name, run in runs.items() if name.startswith(scale)}
        print(f"## {scale}")
        for arm in ["main", "branch"]:
            walls = [run["wall"] for name, run in sel.items() if name.endswith(arm)]
            cpus = [run["cpu"] for name, run in sel.items() if name.endswith(arm)]
            print(
                f"{arm}: wall {[round(wall, 2) for wall in walls]} "
                f"median {statistics.median(walls):.2f}; "
                f"cpu median {statistics.median(cpus):.2f}"
            )
        for round_ in ["r1", "r2", "r3"]:
            main_run = sel[f"{scale}-{round_}-main"]
            branch_run = sel[f"{scale}-{round_}-branch"]
            print(
                f"{round_}: branch - main wall {branch_run['wall'] - main_run['wall']:+.2f}s "
                f"cpu {branch_run['cpu'] - main_run['cpu']:+.2f}s"
            )
        answer_sets = {json.dumps(run["answers"], sort_keys=True) for run in sel.values()}
        print("distinct answer sets:", len(answer_sets))
        for query, answer in next(iter(sel.values()))["answers"].items():
            print(f"  {query}: rows={answer[0]} scalar={answer[1]} sha256={answer[2]}")
        cas = [run["cas"] for run in sel.values()]
        common = set.intersection(*cas)
        diffs = {len(a - b) for a, b in itertools.combinations(cas, 2)}
        print(
            f"CAS objects {sorted({len(c) for c in cas})}, common to every run {len(common)}, "
            f"differing between any two runs {sorted(diffs)}"
        )


if __name__ == "__main__":
    main(Path(sys.argv[1]))
