"""FinBench Transaction scorecard inputs for the GDC rung runner (#952, #1909).

A rung runs the twelve complex reads as the Cypher of the Rust query catalog
(``queries.rs``, #1890) over every binding LDBC publishes. This module builds
that rung's query workload at rung time from two committed or pinned inputs:

* ``profiles/gdc/finbench-transaction-scorecard-queries.json``: the Cypher,
  its parameters and its ordering per read, a copy of what
  ``graphforge-benchmark-gdc-finbench-transaction list-queries`` prints. The
  rung runner cannot call that binary, so the copy is committed and a test
  holds it to the catalog (``catalog_drift``);
* the ``complex_<n>_param.csv`` files of the rung's pinned read-parameter
  archive, parsed with ``read_ldbc_parameters`` of the spec-derived reference
  module, so a binding id (``line-<n>``) means the same line to the workload
  and to the reference it is checked against.

The simple reads (TSR1-TSR6) have no published parameters: LDBC's driver
derives them during a run. They are never built into a workload; the ladder
spec lists them as refused, so they count against coverage.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
import json
from pathlib import Path
import re
import sys
from typing import Any

from graphforge_bench.gdc_finbench_transaction_reference import (
    LDBC_PARAMETERS,
    read_ldbc_parameters,
)

QUERIES_SCHEMA = "graphforge-gdc-finbench-scorecard-queries/1"
WORKLOAD_SCHEMA = "graphforge-gdc-query-workload/1"
SUITE_ID = "finbench-transaction"
TRUNCATION_ORDER = "TIMESTAMP_DESCENDING"
SIMPLE_READS = tuple(f"TSR{index}" for index in range(1, 7))
INTEGER_KINDS = ("id", "epoch_millis", "truncation_limit")


class ScorecardInputError(ValueError):
    """The scorecard inputs are malformed or disagree, with a typed cause."""

    def __init__(self, cause: str, message: str) -> None:
        super().__init__(message)
        self.cause = cause


def read_queries(path: Path) -> dict[str, Any]:
    """The committed query definitions, checked to cover exactly the complex reads."""
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("schema") != QUERIES_SCHEMA or document.get("suite_id") != SUITE_ID:
        raise ScorecardInputError("invalid_queries", f"{path} is not {QUERIES_SCHEMA}")
    ids = [query["id"] for query in document["queries"]]
    if ids != list(LDBC_PARAMETERS):
        raise ScorecardInputError(
            "invalid_queries", f"queries {ids} are not {list(LDBC_PARAMETERS)}"
        )
    for query in document["queries"]:
        names = [parameter["name"] for parameter in query["parameters"]]
        if names != list(LDBC_PARAMETERS[query["id"]]):
            raise ScorecardInputError(
                "invalid_queries",
                f"{query['id']} takes {names}; the published files carry "
                f"{list(LDBC_PARAMETERS[query['id']])}",
            )
    return document


def _literal(query_id: str, binding_id: str, parameter: Mapping[str, str], value: Any) -> Any:
    kind = parameter["kind"]
    if kind in INTEGER_KINDS and isinstance(value, int) and not isinstance(value, bool):
        return {"type": "Int", "value": value}
    if kind == "float" and isinstance(value, float):
        return {"type": "Float", "value": value}
    raise ScorecardInputError(
        "invalid_parameters",
        f"{query_id} {binding_id} parameter {parameter['name']} is {value!r}, not a {kind}",
    )


def build_workload(queries: Mapping[str, Any], parameters_dir: Path) -> dict[str, Any]:
    """Every published binding of every complex read, under its ``line-<n>`` id.

    A binding with a truncation order the Cypher does not implement is refused
    with ``truncation_order_not_supported``, never run in another order.
    """
    published = read_ldbc_parameters(parameters_dir)
    variants = []
    for query in queries["queries"]:
        query_id = query["id"]
        bindings = []
        for binding_id, values in published[query_id]:
            params = {}
            for parameter in query["parameters"]:
                value = values[parameter["name"]]
                if parameter["kind"] == "truncation_order":
                    if value != TRUNCATION_ORDER:
                        raise ScorecardInputError(
                            "truncation_order_not_supported",
                            f"{query_id} {binding_id} asks for {value!r}; only "
                            f"{TRUNCATION_ORDER} is implemented",
                        )
                    continue
                params[parameter["name"]] = _literal(query_id, binding_id, parameter, value)
            bindings.append({"id": binding_id, "params": params})
        variants.append(
            {
                "id": query_id,
                "ordered": bool(query["ordered"]),
                "operation": {"kind": "cypher", "text": query["cypher"]},
                "bindings": bindings,
            }
        )
    return {"schema": WORKLOAD_SCHEMA, "suite": SUITE_ID, "variants": variants}


def queries_from_catalog(catalog: Mapping[str, Any]) -> dict[str, Any]:
    """The committed query definitions for the complex reads of a ``list-queries`` catalog."""
    queries = []
    for query in catalog["queries"]:
        if query["operation"] in SIMPLE_READS:
            continue
        queries.append(
            {
                "id": query["operation"],
                "cypher": query["cypher"],
                "ordered": query["validation"] == "exact",
                "parameters": [
                    {"name": parameter["name"], "kind": parameter["kind"]}
                    for parameter in query["parameters"]
                ],
            }
        )
    return {"schema": QUERIES_SCHEMA, "suite_id": SUITE_ID, "queries": queries}


def catalog_drift(committed: Mapping[str, Any], catalog: Mapping[str, Any]) -> Sequence[str]:
    """What differs between the committed definitions and the Rust catalog, by read."""
    current = {query["id"]: query for query in queries_from_catalog(catalog)["queries"]}
    kept = {query["id"]: query for query in committed["queries"]}
    drift = [f"{name} is not in the catalog" for name in kept if name not in current]
    drift += [f"{name} is not committed" for name in current if name not in kept]
    for name in kept.keys() & current.keys():
        for field in ("cypher", "ordered", "parameters"):
            if kept[name][field] != current[name][field]:
                drift.append(f"{name} {field} differs")
    return drift


def write_queries(catalog: Mapping[str, Any], path: Path) -> None:
    path.write_text(json.dumps(queries_from_catalog(catalog), indent=2) + "\n", encoding="utf-8")


# `alias.property` in the Cypher. A parameter is `$name`, and a number such as
# `0.5` or a range such as `[0..$n]` does not start with an identifier.
_PROPERTY = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\.([A-Za-z_][A-Za-z0-9_]*)")


def cypher_properties(queries: Mapping[str, Any]) -> set[str]:
    """Every property name the committed Cypher reads."""
    return {name for query in queries["queries"] for name in _PROPERTY.findall(query["cypher"])}


def mapping_drift(queries: Mapping[str, Any], mapping: Mapping[str, Any]) -> list[str]:
    """Where the load mapping no longer stores what the Cypher reads.

    The Cypher compares ``timestamp`` and ``createTime`` with epoch-millisecond
    integers (the form LDBC's parameters take), so the mapping must store an
    edge's ``timestamp`` on every edge table and every node table's
    ``createTime`` as ``int64``. Every other property the Cypher reads must be
    one some table stores. An empty list means the windows select the rows the
    specification selects.
    """
    stored: set[str] = set()
    drift: list[str] = []
    for table in [*mapping["node_tables"], *mapping["edge_tables"]]:
        kinds = {prop.get("name") or prop["column"]: prop["type"] for prop in table["properties"]}
        stored |= kinds.keys()
        wanted = ["timestamp"] if "rel_type" in table else ["createTime"]
        for name in wanted:
            if kinds.get(name) != "int64":
                drift.append(
                    f"table {table['id']} stores {name} as {kinds.get(name, 'nothing')}, "
                    "not int64 epoch milliseconds"
                )
    drift += [
        f"the Cypher reads {name}, which no table stores"
        for name in sorted(cypher_properties(queries) - stored)
    ]
    return drift


def build_inputs(
    *,
    spec_workload: Mapping[str, Any],
    profile_root: Path,
    parameters_root: Path,
    mapping: Mapping[str, Any],
) -> dict[str, Any]:
    """The rung's workload, refused before any query runs if the mapping has drifted.

    The Cypher compares epoch-millisecond integers; a mapping that no longer
    stores them would select the wrong rows, so it is a typed failure here.
    """
    queries = read_queries(profile_root / spec_workload["queries"])
    drift = mapping_drift(queries, mapping)
    if drift:
        raise ScorecardInputError("mapping_drift", "; ".join(drift))
    return build_workload(queries, parameters_root / spec_workload["parameters"]["path"])


def main(argv: Sequence[str] | None = None) -> int:
    """Build a rung's workload in a child process (the rung runner's builder protocol)."""
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    request = json.loads(args.request.read_text(encoding="utf-8"))
    try:
        workload = build_inputs(
            spec_workload=request["workload"],
            profile_root=Path(request["profile_root"]),
            parameters_root=Path(request["parameters_root"]),
            mapping=json.loads(Path(request["mapping"]).read_text(encoding="utf-8")),
        )
    except ScorecardInputError as error:
        print(json.dumps({"cause": error.cause, "message": str(error)}), file=sys.stderr)
        return 3
    except (KeyError, OSError, ValueError) as error:
        print(json.dumps({"cause": "inputs_build_failed", "message": repr(error)}), file=sys.stderr)
        return 3
    (args.output_dir / "workload.json").write_text(
        json.dumps(workload, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
