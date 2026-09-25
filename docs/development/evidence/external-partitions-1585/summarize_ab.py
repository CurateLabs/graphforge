"""Summarize an ab.sh output directory: wall/CPU per run, per-pair deltas,
query-answer identity and CAS digest overlap.  usage: summarize_ab.py AB_DIR"""
import glob, itertools, json, statistics, sys

root = sys.argv[1]
runs = {}
for d in sorted(glob.glob(f"{root}/runs/s*-r*-*")):
    name = d.rsplit("/", 1)[1]
    kv = dict(l.strip().split("=", 1) for l in open(f"{d}/runexec.txt") if "=" in l)
    answers = {}
    for q in ["nodes", "node-scan", "edges", "edge-scan"]:
        r = json.loads(open(f"{d}/{q}.json").readline())
        answers[q] = (r["rows"], r.get("scalar_u64"), r["result_sha256"])
    runs[name] = {
        "wall": float(kv["walltime"].rstrip("s")),
        "cpu": float(kv["cputime"].rstrip("s")),
        "answers": answers,
        "cas": set(open(f"{d}/cas-digests.txt").read().split()),
    }
for scale in ["s18", "s20"]:
    sel = {k: v for k, v in runs.items() if k.startswith(scale)}
    print(f"## {scale}")
    for b in ["main", "branch"]:
        w = [v["wall"] for k, v in sel.items() if k.endswith(b)]
        c = [v["cpu"] for k, v in sel.items() if k.endswith(b)]
        print(f"{b}: wall {[round(x, 2) for x in w]} median {statistics.median(w):.2f}; "
              f"cpu median {statistics.median(c):.2f}")
    for r in ["r1", "r2", "r3"]:
        m, b = sel[f"{scale}-{r}-main"], sel[f"{scale}-{r}-branch"]
        print(f"{r}: branch - main wall {b['wall'] - m['wall']:+.2f}s cpu {b['cpu'] - m['cpu']:+.2f}s")
    print("distinct answer sets:", len({json.dumps(v["answers"], sort_keys=True) for v in sel.values()}))
    for q, a in next(iter(sel.values()))["answers"].items():
        print(f"  {q}: rows={a[0]} scalar={a[1]} sha256={a[2]}")
    cas = [v["cas"] for v in sel.values()]
    common = set.intersection(*cas)
    diffs = {len(a - b) for a, b in itertools.combinations(cas, 2)}
    print(f"CAS objects {sorted({len(c) for c in cas})}, common to every run {len(common)}, "
          f"differing between any two runs {sorted(diffs)}")
