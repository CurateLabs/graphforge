#!/usr/bin/env python3
"""Pinned, comment/string-masked source-expression census; not runtime syscall counts."""

import argparse
import collections
import hashlib
import json
from pathlib import Path
import re
import subprocess


# Mask comments and literals while preserving offsets/newlines. Character literals
# are intentionally left alone: none can contain a dot method invocation.
def mask(s):
    out = list(s)
    i = 0
    raw = re.compile(r'(?:b)?r(#+)?"')

    def blank(lo, hi):
        for j in range(lo, hi):
            if out[j] != "\n":
                out[j] = " "

    while i < len(s):
        start = i
        if s.startswith("//", i):
            i = s.find("\n", i)
            i = len(s) if i < 0 else i
            blank(start, i)
        elif s.startswith("/*", i):
            depth = 1
            i += 2
            while i < len(s) and depth:
                if s.startswith("/*", i):
                    depth += 1
                    i += 2
                elif s.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
            blank(start, i)
        elif m := raw.match(s, i):
            end = '"' + (m.group(1) or "")
            i = s.find(end, m.end())
            i = len(s) if i < 0 else i + len(end)
            blank(start, i)
        elif s[i] == '"':
            i += 1
            while i < len(s):
                if s[i] == "\\":
                    i += 2
                elif s[i] == '"':
                    i += 1
                    break
                else:
                    i += 1
            blank(start, i)
        else:
            i += 1
    return "".join(out)


def test_ranges(code):
    ranges = []
    for m in re.finditer(r"#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]", code):
        start = m.end()
        brace = code.find("{", start)
        semi = code.find(";", start)
        if brace < 0 or (semi >= 0 and semi < brace):
            continue
        depth = 1
        i = brace + 1
        while i < len(code) and depth:
            depth += (code[i] == "{") - (code[i] == "}")
            i += 1
        ranges.append((m.start(), i))
    return ranges


def family(path):
    rel = path.split("/src/", 1)[1]
    if rel == "filesystem_admission.rs":
        return "admission/probe/lock"
    if rel.startswith(("project_publication", "project_generation", "project_recovery")):
        return "project/CURRENT/control"
    if rel.startswith(
        ("durable_rewrite", "staging", "uuid_membership", "ordinal_identity", "project_checkpoints")
    ):
        return "rewrite/UUID/ordinal/checkpoint"
    if rel.startswith(
        ("graph_construction", "construction_directory", "adjacency", "graph_delta_journal")
    ):
        return "construction/encoding/CSR"
    if rel.startswith(
        (
            "graph_object_store",
            "graph_files",
            "graph_projection",
            "route_component",
            "research_versions",
            "graph_manifest",
        )
    ):
        return "CAS/graph/research/route"
    if rel.startswith(("project_portable", "portable_bytes")):
        return "portable"
    if rel.startswith(("embedding", "search_publication", "vector_store")):
        return "embedding/search/vector"
    return "other:" + rel.split("/")[0]


SCRATCH = {
    (
        "crates/graphforge-storage/src/uuid_membership/rebuild.rs",
        "flush_surrogate_run",
    ),
    (
        "crates/graphforge-storage/src/uuid_membership/rebuild.rs",
        "flush_entity_surrogate_run",
    ),
    (
        "crates/graphforge-storage/src/uuid_membership/rebuild.rs",
        "merge_surrogate_runs",
    ),
    (
        "crates/graphforge-storage/src/uuid_membership/rebuild.rs",
        "scan_pinned_entity_surrogate_runs",
    ),
    (
        "crates/graphforge-storage/src/uuid_membership/rebuild.rs",
        "merge_node_surrogate_group",
    ),
    (
        "crates/graphforge-storage/src/uuid_membership/topology_delta.rs",
        "external_sort_v4_nodes",
    ),
    (
        "crates/graphforge-storage/src/uuid_membership/rebuild.rs",
        "scan_entity_surrogate_runs",
    ),
    (
        "crates/graphforge-storage/src/uuid_membership/rebuild.rs",
        "build_surrogate_run",
    ),
}


def durability_role(path, function, classification):
    if classification.startswith("test") or (
        path.endswith("filesystem_admission.rs") and function != "acquire"
    ):
        return "evidence/probe"
    if (path, function) in SCRATCH:
        return "transient"
    if path.endswith("filesystem_admission.rs"):
        return "lock-file"
    if "durable_rewrite" in path or path.endswith("project_checkpoints/registry.rs"):
        return "journal/recovery"
    return "publication"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("repo")
    p.add_argument("commit")
    p.add_argument("output")
    a = p.parse_args()
    working = a.commit == "WORKTREE"

    def git(*args):
        return subprocess.check_output(["git", "-C", a.repo, *args])

    sha = "WORKTREE" if working else git("rev-parse", a.commit).decode().strip()
    files = (
        sorted(
            str(x.relative_to(Path(a.repo)))
            for x in (Path(a.repo) / "crates/graphforge-storage/src").rglob("*.rs")
        )
        if working
        else [
            x
            for x in git("ls-tree", "-r", "--name-only", sha, "crates/graphforge-storage/src")
            .decode()
            .splitlines()
            if x.endswith(".rs")
        ]
    )
    methods = (
        r"observed_sync_all|observed_sync_data|sync_all|sync_data|sync|"
        r"sync_all_and_release|sync_all_retained|sync_parent_dir"
    )
    rows = []
    inputs = []
    wrappers = []
    for path in files:
        data = (Path(a.repo) / path).read_bytes() if working else git("show", sha + ":" + path)
        source = data.decode()
        code = mask(source)
        ranges = test_ranges(code)
        inputs.append((path, hashlib.sha256(data).hexdigest()))
        path_test = bool(re.search(r"(?:^|/)(?:tests?|[^/]+_tests)(?:/|\.rs$)", path))
        functions = list(re.finditer(r"\bfn\s+(\w+)\s*(?:<[^{};]*>)?\s*\(", code))
        for m in re.finditer(r"\.\s*(" + methods + r")\s*\(", code):
            before = [f for f in functions if f.start() < m.start()]
            owner = before[-1].group(1) if before else None
            test = path_test or any(lo <= m.start() < hi for lo, hi in ranges)
            cls = "test/fault-oracle" if test else "production"
            if (
                not test
                and m.group(1) == "sync"
                and (
                    (
                        path.endswith("/graph_construction/intake.rs")
                        and code[: m.start()].rstrip().endswith("parquet")
                    )
                    or (
                        path.endswith("/graph_construction/partition_shaping.rs")
                        and code[: m.start()].rstrip().endswith("writer")
                    )
                )
            ):
                cls = "nonphysical codec flush"
            if m.group(1) in {"sync_all_and_release", "sync_all_retained", "sync_parent_dir"}:
                cls = "test wrapper" if test else "production layered wrapper/cache"
            rows.append(
                {
                    "durability_role": durability_role(path, owner, cls),
                    "file": path,
                    "line": source.count("\n", 0, m.start()) + 1,
                    "method": m.group(1),
                    "function": owner,
                    "family": family(path),
                    "classification": cls,
                }
            )
        for m in re.finditer(
            r"\b(sync_directory|sync_directory_handle|sync_dir|sync_parent|sync_file|sync_directory_tree|sync_tree|sync_materialized_tree)\s*\(",
            code,
        ):
            if re.search(r"\bfn\s*$", code[max(0, m.start() - 20) : m.start()]):
                continue
            test = path_test or any(lo <= m.start() < hi for lo, hi in ranges)
            wrappers.append(
                {
                    "file": path,
                    "line": source.count("\n", 0, m.start()) + 1,
                    "method": m.group(1),
                    "family": family(path),
                    "classification": "test/fault-oracle" if test else "production layered wrapper",
                }
            )
    report = {
        "commit": sha,
        "file_count": len(files),
        "input_sha256": hashlib.sha256(
            json.dumps(inputs, separators=(",", ":")).encode()
        ).hexdigest(),
        "rows": rows,
        "wrapper_rows": wrappers,
    }
    Path(a.output).write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                "commit": sha,
                "files": len(files),
                "counts": dict(
                    collections.Counter((r["method"] + " | " + r["classification"]) for r in rows)
                ),
            },
            indent=2,
        )
    )
    for fam in sorted({r["family"] for r in rows}):
        counts = collections.Counter(
            r["method"] for r in rows if r["family"] == fam and r["classification"] == "production"
        )
        print(fam, dict(counts))


if __name__ == "__main__":
    main()
