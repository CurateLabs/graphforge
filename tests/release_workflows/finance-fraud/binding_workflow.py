"""Same-SHA native Python replay for #2468."""

from __future__ import annotations

import argparse
import gc
import hashlib
import importlib.metadata
import json
from pathlib import Path

import graphforge
import graphforge._graphforge_rs as native


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", type=Path, required=True)
    parser.add_argument("--ontology", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()

    args.project.mkdir(parents=True)
    forge = graphforge.GraphForge(str(args.project))
    forge.load_ontology(str(args.ontology))
    accounts = [
        forge.add_node(
            "Account",
            name=f"Review Account {index}",
            account_key=f"PY-A-{index:02}",
            currency="USD",
            balance=10_000.0 + index,
        )
        for index in range(5)
    ]
    pairs = [(0, 1), (0, 1), (1, 2), (2, 0), (2, 3), (3, 4), (4, 2), (1, 4)]
    for index, (source, target) in enumerate(pairs):
        forge.add_edge(
            accounts[source],
            "TRANSFERRED_TO",
            accounts[target],
            amount=100.0 + index * 25.0,
            transaction_key=f"PY-T-{index:02}",
        )
    search = forge.find("Review Account", label="Account", limit=5)
    rank = forge.rank("Account", by="degree", via="TRANSFERRED_TO")
    cluster = forge.cluster("Account", by="components", via="TRANSFERRED_TO")
    path = forge.paths(accounts[0], accounts[4], by="bfs", via="TRANSFERRED_TO", directed=True)
    similar = forge.similar("Account", by="node_similarity", via="TRANSFERRED_TO", k=4)
    if search.num_rows != 5 or rank.num_rows != 5 or cluster.num_rows != 5:
        raise AssertionError("unexpected algorithm row counts")
    if path.num_rows != 1 or similar.num_rows <= 0:
        raise AssertionError("unexpected paths or similarity row counts")
    try:
        forge.find()
    except graphforge.GraphForgeError as error:
        invalid_code = error.code
    else:
        raise AssertionError("invalid scope unexpectedly succeeded")
    if invalid_code != "GF_VALIDATION":
        raise AssertionError(f"unexpected invalid error code: {invalid_code}")

    account_uuids = [str(account.uuid) for account in accounts]
    forge.close()
    del forge
    gc.collect()
    reopened = graphforge.GraphForge(str(args.project))
    reopened.load_ontology(str(args.ontology))
    rank_equal = reopened.rank("Account", by="degree", via="TRANSFERRED_TO").equals(rank)
    search_equal = reopened.find("Review Account", label="Account", limit=5).equals(search)
    reopen_equal = rank_equal and search_equal
    if not reopen_equal:
        raise AssertionError("reopen equality check failed")

    module_path = Path(native.__file__).resolve()
    evidence = {
        "schema_version": 1,
        "scenario_id": "finance-fraud",
        "binding": "python",
        "account_uuids": account_uuids,
        "operation_rows": {
            "search": search.num_rows,
            "rank": rank.num_rows,
            "cluster": cluster.num_rows,
            "paths": path.num_rows,
            "similar": similar.num_rows,
        },
        "invalid_error": invalid_code,
        "fraud_determination": False,
        "reopen_equal": reopen_equal,
        "package_version": importlib.metadata.version("graphforge"),
        "native_version": graphforge.version(),
        "native_module_path": str(module_path),
        "native_module_sha256": hashlib.sha256(module_path.read_bytes()).hexdigest(),
    }
    reopened.close()
    args.evidence.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
