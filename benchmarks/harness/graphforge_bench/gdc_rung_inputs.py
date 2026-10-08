"""What one GDC scorecard rung consumes, and how its answers are checked.

A suite registers its ladder with one ``graphforge-gdc-scorecard-ladder-spec/1``
document (``schemas/gdc-scorecard-ladder-spec.json``): the identity profile to
acquire from, the count ladder, and per rung the query workload, the queries
refused at mapping with their typed causes, the pinned reference and the
variances the card must carry. Every path in it is relative to the spec's
``profile_root`` (the benchmarks root for real suites).

This module derives the rung's ``graphforge-gdc-expected-counts/1`` document
from the suite's count ladder (Graphalytics ``listed_edges``; the LDBC CSV
suites' ``listed`` archive records, which is the reconciliation rule for SNB
Interactive v1 and FinBench), and checks the driver's written results against
the reference with the workload's matching rule: ``exact``, ``epsilon``
(relative tolerance on numeric cells) or ``equivalence`` (equal partitions up
to relabelling).
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

LADDER_SPEC_SCHEMA = "graphforge-gdc-scorecard-ladder-spec/1"
REFERENCE_SCHEMA = "graphforge-gdc-rung-reference/1"
CORRECTNESS_SCHEMA = "graphforge-gdc-rung-correctness/1"
EXPECTED_COUNTS_SCHEMA = "graphforge-gdc-expected-counts/1"
QUERY_RESULT_SCHEMA = "graphforge-gdc-query-result/1"
RESULT_DIGEST = "graphforge-gdc-result-digest/1"
GRAPHALYTICS_LADDER = "graphforge-gdc-graphalytics-scorecard-ladder/1"
LDBC_CSV_LADDER = "graphforge-gdc-ldbc-csv-scorecard-ladder/1"


class RungInputError(ValueError):
    """A rung input is malformed or contradicts another, with a typed cause."""

    def __init__(self, cause: str, message: str) -> None:
        super().__init__(message)
        self.cause = cause


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise RungInputError("invalid_document", f"{path}: {error}") from error


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_schema(root: Path, name: str, document: Any) -> None:
    schema = read_json(root / "schemas" / name)
    error = next(Draft202012Validator(schema).iter_errors(document), None)
    if error is not None:
        location = "/".join(str(part) for part in error.absolute_path)
        raise RungInputError("invalid_document", f"{name} at /{location}: {error.message}")


@dataclass(frozen=True)
class LadderSpec:
    """A validated ladder spec with its paths resolved."""

    root: Path
    path: Path
    document: Mapping[str, Any]

    @property
    def suite_id(self) -> str:
        return str(self.document["suite_id"])

    @property
    def profile_root(self) -> Path:
        return self.root / str(self.document["profile_root"])

    def resolve(self, relative: str) -> Path:
        return self.profile_root / relative

    @property
    def suite_declaration(self) -> Path:
        return self.profile_root / "suites" / f"gdc-{self.suite_id}.json"

    def counts_ladder(self) -> Mapping[str, Any]:
        return read_json(self.resolve(str(self.document["counts_ladder"])))

    def rung(self, rung_id: str) -> Mapping[str, Any]:
        for rung in self.document["rungs"]:
            if rung["id"] == rung_id:
                return rung
        raise RungInputError("invalid_rung_spec", f"no rung {rung_id!r} in {self.path}")


def load_ladder_spec(root: Path, path: Path) -> LadderSpec:
    document = read_json(path)
    validate_schema(root, "gdc-scorecard-ladder-spec.json", document)
    spec = LadderSpec(root, path, document)
    ids = [rung["id"] for rung in document["rungs"]]
    if len(ids) != len(set(ids)):
        raise RungInputError("invalid_rung_spec", "rung ids repeat")
    if (document["suite_id"] == "graphalytics") != (document["metric_shape"] == "graphalytics"):
        raise RungInputError(
            "invalid_rung_spec", "Graphalytics, and only Graphalytics, uses the graphalytics shape"
        )
    ladder = spec.counts_ladder()
    if ladder.get("suite_id") != spec.suite_id:
        raise RungInputError("invalid_rung_spec", "count ladder belongs to another suite")
    for rung in document["rungs"]:
        if is_unpinned(rung):
            continue
        _ladder_entry(ladder, rung["id"])
        if isinstance(rung["workload"], Mapping):
            # Built at rung time from pinned parameters (gdc_snb_scorecard);
            # the builder refuses a variant that is also refused here.
            queries = read_json(spec.resolve(rung["workload"]["queries"]))
            if not isinstance(queries, Mapping) or not queries.get("queries"):
                raise RungInputError(
                    "invalid_rung_spec", f"rung {rung['id']} query definitions are empty"
                )
            continue
        workload = read_json(spec.resolve(rung["workload"]))
        variants = workload.get("variants") if isinstance(workload, Mapping) else None
        if not isinstance(variants, list) or not variants:
            raise RungInputError("invalid_rung_spec", f"rung {rung['id']} workload has no variants")
        refused = {item["query_id"] for item in rung["refused"]}
        both = refused & {variant.get("id") for variant in variants}
        if both:
            raise RungInputError(
                "invalid_rung_spec", f"rung {rung['id']} both runs and refuses {sorted(both)}"
            )
    return spec


def is_unpinned(rung: Mapping[str, Any]) -> bool:
    """A declared rung whose dataset is not pinned (recorded, never run)."""
    return "not_pinned" in rung


def _ladder_entry(ladder: Mapping[str, Any], rung_id: str) -> Mapping[str, Any]:
    entries = ladder.get("datasets") if ladder.get("schema") == GRAPHALYTICS_LADDER else None
    if ladder.get("schema") == LDBC_CSV_LADDER:
        entries = ladder.get("rungs")
    if entries is None:
        raise RungInputError("invalid_rung_spec", f"unknown count ladder {ladder.get('schema')!r}")
    for entry in entries:
        if entry.get("id") == rung_id:
            return entry
    raise RungInputError("invalid_rung_spec", f"count ladder has no rung {rung_id!r}")


@dataclass(frozen=True)
class RungCounts:
    """The expected-counts document plus what the card shows beside it."""

    expected: dict[str, Any]
    published: dict[str, int]
    published_snapshot: dict[str, int] | None
    discrepancies: list[dict[str, str]]
    directed: bool | None


def _manifest_labels(manifest: Mapping[str, Any], table: str) -> dict[str, int]:
    for output in manifest.get("outputs", []):
        if output.get("table") == table and output.get("kind") == "nodes":
            labels = output.get("labels")
            if isinstance(labels, Mapping) and labels:
                return {str(key): int(value) for key, value in labels.items()}
            return {str(output["label"]): int(output["rows"])}
    raise RungInputError("count_mismatch", f"conversion manifest has no node table {table!r}")


def _add(counts: dict[str, int], key: str, value: int) -> None:
    counts[key] = counts.get(key, 0) + value


def expected_counts(
    spec: LadderSpec, rung_id: str, manifest: Mapping[str, Any] | None
) -> RungCounts:
    """Derive the counts a reopened rung must reproduce exactly.

    Graphalytics reconciles to ``listed_edges``, the edges the archive lists.
    The LDBC CSV suites reconcile every table to ``listed``, the records of the
    pinned archive's loaded snapshot. A node table whose stored label comes
    from a column has only a table total in the ladder; its per-label split is
    the conversion manifest's, accepted only when it sums to that total.
    """
    ladder = spec.counts_ladder()
    entry = _ladder_entry(ladder, rung_id)
    labels: dict[str, int] = {}
    types: dict[str, int] = {}
    discrepancies: list[dict[str, str]] = []
    if ladder["schema"] == GRAPHALYTICS_LADDER:
        mapping = read_json(spec.resolve(entry["load_mapping"]))
        node_tables, edge_tables = mapping["node_tables"], mapping["edge_tables"]
        if len(node_tables) != 1 or len(edge_tables) != 1:
            raise RungInputError("invalid_rung_spec", "a Graphalytics mapping has one node table")
        labels[node_tables[0]["label"]] = int(entry["vertices"])
        types[edge_tables[0]["rel_type"]] = int(entry["listed_edges"])
        published = {"nodes": int(entry["vertices"]), "edges": int(entry["edges"])}
        if "discrepancy" in entry:
            discrepancies.append(
                {"kind": "discrepancy", "subject": rung_id, "text": entry["discrepancy"]}
            )
        source = f"{ladder['counts_source']} ({rung_id}; edges reconcile to listed_edges)"
        return RungCounts(
            {
                "schema": EXPECTED_COUNTS_SCHEMA,
                "source": source,
                "nodes": sum(labels.values()),
                "edges": sum(types.values()),
                "labels": labels,
                "types": types,
            },
            published,
            None,
            discrepancies,
            bool(entry["directed"]),
        )
    mapping = read_json(spec.resolve(ladder["load_mapping"]))
    node_tables = {table["id"]: table for table in mapping["node_tables"]}
    edge_tables = {table["id"]: table for table in mapping["edge_tables"]}
    listed_tables = {table["table"] for table in entry["tables"]}
    if listed_tables != set(node_tables) | set(edge_tables):
        raise RungInputError(
            "invalid_rung_spec",
            f"count ladder tables {sorted(listed_tables)} differ from the load mapping's",
        )
    published = {"nodes": 0, "edges": 0}
    texts: dict[str, list[str]] = {}
    for table in entry["tables"]:
        name, listed = table["table"], int(table["listed"])
        published[table["kind"]] += int(table["published"])
        if "discrepancy" in table:
            texts.setdefault(table["discrepancy"], []).append(name)
        if table["kind"] == "edges":
            _add(types, edge_tables[name]["rel_type"], listed)
            continue
        node_table = node_tables[name]
        if node_table.get("label_column") is None:
            _add(labels, node_table["label"], listed)
            continue
        if manifest is None:
            raise RungInputError("invalid_rung_spec", f"{name} needs the conversion manifest")
        split = _manifest_labels(manifest, name)
        if sum(split.values()) != listed:
            raise RungInputError(
                "count_mismatch",
                f"table {name}: converted {sum(split.values())} rows, the archive lists {listed}",
            )
        for label, count in split.items():
            _add(labels, label, count)
    if "published_totals" in entry:
        published = {key: int(value) for key, value in entry["published_totals"].items()}
    snapshot = entry.get("published_snapshot_totals")
    loaded = {"nodes": sum(labels.values()), "edges": sum(types.values())}
    if snapshot is not None and "snapshot_totals_discrepancy" not in entry and loaded != snapshot:
        raise RungInputError(
            "invalid_rung_spec",
            f"listed totals {loaded} differ from LDBC's snapshot totals {snapshot} unexplained",
        )
    if "snapshot_totals_discrepancy" in entry:
        discrepancies.append(
            {
                "kind": "discrepancy",
                "subject": f"{rung_id} snapshot totals",
                "text": entry["snapshot_totals_discrepancy"],
            }
        )
    for text, tables in texts.items():
        discrepancies.append({"kind": "discrepancy", "subject": ", ".join(tables), "text": text})
    source = (
        f"{ladder['counts_source']['url']} ({rung_id}; every table reconciles to the "
        "listed records of the pinned archive)"
    )
    return RungCounts(
        {
            "schema": EXPECTED_COUNTS_SCHEMA,
            "source": source,
            "nodes": loaded["nodes"],
            "edges": loaded["edges"],
            "labels": labels,
            "types": types,
        },
        published,
        dict(snapshot) if snapshot is not None else None,
        discrepancies,
        None,
    )


def _cell(text: str) -> bytes:
    encoded = text.encode("utf-8")
    return b"V" + str(len(encoded)).encode("ascii") + b":" + encoded


def result_digest(
    columns: Sequence[Mapping[str, str]], rows: Sequence[Sequence[Any]], ordered: bool
) -> str:
    """``graphforge-gdc-result-digest/1`` recomputed from a written result."""
    header = _cell(RESULT_DIGEST) + _cell("ordered" if ordered else "unordered")
    for column in columns:
        header += _cell(column["name"]) + _cell(column["type"])
    encoded = [
        b"".join(b"N" if value is None else _cell(value) for value in row) + b"\n" for row in rows
    ]
    if not ordered:
        encoded.sort()
    digest = hashlib.sha256(header + b"\n")
    for row in encoded:
        digest.update(row)
    return digest.hexdigest()


def _read_result(path: Path) -> Mapping[str, Any]:
    document = read_json(path)
    if not isinstance(document, Mapping) or document.get("schema") != QUERY_RESULT_SCHEMA:
        raise RungInputError("invalid_document", f"{path.name} is not a query result")
    return document


class ResultFiles(Mapping[tuple[str, str], Mapping[str, Any]]):
    """The driver's written results by (query id, binding id), read from disk on access.

    A Graphalytics result has one row per vertex, millions at the larger rungs,
    so results are not all held in memory at once: each access reads its file.
    """

    def __init__(self, paths: Mapping[tuple[str, str], Path]) -> None:
        self._paths = dict(paths)

    def __getitem__(self, key: tuple[str, str]) -> Mapping[str, Any]:
        return _read_result(self._paths[key])

    def __iter__(self) -> Iterator[tuple[str, str]]:
        return iter(self._paths)

    def __len__(self) -> int:
        return len(self._paths)


def read_results(results_dir: Path) -> ResultFiles:
    """Every result the driver wrote, keyed by (query id, binding id)."""
    paths: dict[tuple[str, str], Path] = {}
    for path in sorted(results_dir.glob("*.json")):
        document = _read_result(path)
        key = (str(document["query_id"]), str(document["binding_id"]))
        if key in paths:
            raise RungInputError("invalid_document", f"result {key} is written twice")
        paths[key] = path
    return ResultFiles(paths)


def _sort_key(row: Sequence[Any]) -> list[tuple[bool, str]]:
    return [(value is None, value or "") for value in row]


def _number(text: Any) -> float | None:
    if not isinstance(text, str):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _match_exact(actual: list[list[Any]], expected: list[list[Any]], ordered: bool) -> bool:
    if ordered:
        return actual == expected
    return sorted(actual, key=_sort_key) == sorted(expected, key=_sort_key)


def list_elements(text: str) -> list[str] | None:
    """The top-level elements of an Arrow list's display text, ``[a, {b: 1, c: 2}]``.

    Elements split at ``", "`` outside brackets and braces. An element whose
    own text contains ``", "`` at top level splits further, so a set compared
    this way can only fail on such an element, never match wrongly.
    """
    if len(text) < 2 or text[0] != "[" or text[-1] != "]":
        return None
    body, depth, start, elements = text[1:-1], 0, 0, []
    if not body:
        return []
    for index, character in enumerate(body):
        if character in "[{":
            depth += 1
        elif character in "]}":
            depth -= 1
            if depth < 0:
                return None
        elif depth == 0 and body.startswith(", ", index):
            elements.append(body[start:index])
            start = index + 2
    if depth != 0:
        return None
    elements.append(body[start:])
    return elements


def _set_cells_equal(actual: Any, expected: Any) -> bool:
    """A set-valued list cell: the same elements, in any order."""
    if actual is None or expected is None:
        return actual is expected
    mine, theirs = list_elements(actual), list_elements(expected)
    return mine is not None and theirs is not None and sorted(mine) == sorted(theirs)


def _match_rows_with_sets(
    actual: list[list[Any]], expected: list[list[Any]], set_positions: set[int]
) -> bool:
    """Ordered rows, equal cell by cell; cells in ``set_positions`` compare as sets."""
    if len(actual) != len(expected):
        return False
    for mine, theirs in zip(actual, expected):
        if len(mine) != len(theirs):
            return False
        for position, (left, right) in enumerate(zip(mine, theirs)):
            if position in set_positions:
                if not _set_cells_equal(left, right):
                    return False
            elif left != right:
                return False
    return True


Keyed = dict[tuple[Any, ...], dict[str, Any]]


def _keyed(columns: Sequence[str], rows: list[list[Any]], key: Sequence[str]) -> Keyed | None:
    """Rows by their key cells; None when a key repeats or a row has the wrong width."""
    keyed: Keyed = {}
    for row in rows:
        if len(row) != len(columns):
            return None
        cells = dict(zip(columns, row))
        identity = tuple(cells[name] for name in key)
        if identity in keyed:
            return None
        keyed[identity] = cells
    return keyed


def within_epsilon(value: float, reference: float, epsilon: float) -> bool:
    """Graphalytics' epsilon match: ``|r - s| <= epsilon * |r|`` for reference ``r``.

    The bound is relative to the reference value alone (the specification's
    rule, not a symmetric tolerance), so a zero reference demands exactly zero.
    An infinite value matches only the same infinity; NaN matches nothing.
    """
    if math.isinf(value) or math.isinf(reference):
        return value == reference
    return abs(reference - value) <= epsilon * abs(reference)


def _match_epsilon(left: Keyed, right: Keyed, values: Sequence[str], epsilon: float) -> bool:
    """A numeric cell may differ from the reference by `epsilon`, relative; others exactly."""
    for identity, cells in left.items():
        wanted = right[identity]
        for name in values:
            value, reference = cells[name], wanted[name]
            number, reference_number = _number(value), _number(reference)
            if number is None or reference_number is None:
                if value != reference:
                    return False
            elif not within_epsilon(number, reference_number, epsilon):
                return False
    return True


def _match_keyed_exact(left: Keyed, right: Keyed, values: Sequence[str]) -> bool:
    """Every reference cell is identical in the row with the same key."""
    return all(
        cells[name] == right[identity][name] for identity, cells in left.items() for name in values
    )


def _match_equivalence(left: Keyed, right: Keyed, label: str) -> bool:
    """Component labels that relabel one-to-one: the same partition of the keys."""
    forward: dict[Any, Any] = {}
    backward: dict[Any, Any] = {}
    for identity, cells in left.items():
        mine, theirs = cells[label], right[identity][label]
        if forward.setdefault(mine, theirs) != theirs:
            return False
        if backward.setdefault(theirs, mine) != mine:
            return False
    return True


def matches(
    rule: Mapping[str, Any], result: Mapping[str, Any], reference: Mapping[str, Any]
) -> bool:
    """Whether one written result matches its reference under the query's rule.

    ``exact`` without a key compares every column and every cell, in order when
    the variant is ordered; its ``set_columns`` (ordered results only) compare
    as sets of list elements. ``projection`` compares the result's cells in the
    reference's columns exactly, so an analyst verb's other columns do not take
    part. ``exact`` with a key, ``epsilon`` and ``equivalence`` compare the
    result projected onto the reference's columns, with rows paired by the
    rule's key columns, so an analyst verb's extra node columns do not take
    part; both sides must hold exactly the same keys, and an ordered
    ``epsilon`` result must also keep the reference's row order.
    """
    names = [column["name"] for column in result["columns"]]
    wanted = list(reference["columns"])
    actual, expected = list(result["rows"]), list(reference["rows"])
    ordered = bool(result["ordered"])
    if rule["matching"] == "exact" and "key" not in rule:
        if names != wanted:
            return False
        set_columns = list(rule.get("set_columns", []))
        if not set_columns:
            return _match_exact(actual, expected, ordered)
        if not ordered or not set(set_columns) <= set(names):
            return False
        return _match_rows_with_sets(actual, expected, {names.index(n) for n in set_columns})
    if rule["matching"] == "projection":
        if not set(wanted) <= set(names) or len(set(names)) != len(names):
            return False
        if any(len(row) != len(names) for row in actual):
            return False
        positions = [names.index(name) for name in wanted]
        projected = [[row[position] for position in positions] for row in actual]
        return _match_exact(projected, expected, ordered)
    key = list(rule["key"])
    if not set(wanted) <= set(names) or not set(key) <= set(wanted):
        return False
    left, right = _keyed(names, actual, key), _keyed(wanted, expected, key)
    if left is None or right is None or left.keys() != right.keys():
        return False
    values = [name for name in wanted if name not in key]
    if rule["matching"] == "exact":
        return _match_keyed_exact(left, right, values)
    if rule["matching"] == "epsilon":
        # An ordered result keeps the reference's row order as well.
        if ordered and list(left) != list(right):
            return False
        return _match_epsilon(left, right, values, float(rule["epsilon"]))
    label = str(rule["label"])
    return set(wanted) == {*key, label} and _match_equivalence(left, right, label)


def _samples(evidence: Mapping[str, Any]) -> dict[tuple[str, str], Mapping[str, Any]]:
    return {
        (variant["query_id"], sample["binding_id"]): sample
        for variant in evidence["variants"]
        for sample in variant["samples"]
    }


def check_reference(
    *,
    reference: Mapping[str, Any] | None,
    reference_sha256: str | None,
    evidence: Mapping[str, Any],
    results: Mapping[tuple[str, str], Mapping[str, Any]],
) -> dict[str, Any]:
    """Check every referenced result; never count a refused or failed query as correct.

    A written result must reproduce the digest the driver measured, so the
    checked cells are the measured answer. A reference binding the workload
    never ran is a spec error, not a pass. Measured bindings without a
    reference entry are counted as unchecked.
    """
    samples = _samples(evidence)
    mismatches: list[dict[str, Any]] = []
    # Each written result is read once: its digest is checked and, when the
    # reference has an entry for it, its match is decided in the same pass.
    matched_keys: dict[tuple[str, str], bool] = {}
    for key, sample in samples.items():
        if sample.get("status") != "measured":
            continue
        written = results.get(key)
        if written is None:
            if reference is not None:
                mismatches.append(_mismatch(key, "result_missing", "the driver wrote no result"))
            continue
        recomputed = result_digest(written["columns"], written["rows"], bool(written["ordered"]))
        if not recomputed == written["result_sha256"] == sample["result_sha256"]:
            mismatches.append(
                _mismatch(
                    key, "result_digest_mismatch", "written cells are not the measured result"
                )
            )
        rule = reference["queries"].get(key[0]) if reference is not None else None
        if rule is not None and key[1] in rule["bindings"]:
            matched_keys[key] = matches(rule, written, rule["bindings"][key[1]])
        del written
    if reference is None:
        return {
            "schema": CORRECTNESS_SCHEMA,
            "status": "not_reference_checked",
            "reference_source": None,
            "reference_sha256": None,
            "checked": 0,
            "matched": 0,
            "unchecked": len(samples),
            "queries": {},
            "mismatches": mismatches,
        }
    checked = matched = 0
    queries: dict[str, dict[str, Any]] = {}
    referenced: set[tuple[str, str]] = set()
    for query_id, rule in reference["queries"].items():
        tally = queries.setdefault(
            query_id, {"matching": rule["matching"], "checked": 0, "matched": 0}
        )
        for binding_id in rule["bindings"]:
            key = (query_id, binding_id)
            referenced.add(key)
            sample = samples.get(key)
            if sample is None:
                mismatches.append(
                    _mismatch(key, "reference_unmatched", "the workload never ran this binding")
                )
                continue
            checked += 1
            tally["checked"] += 1
            if sample.get("status") != "measured":
                mismatches.append(_mismatch(key, "query_failed", str(sample.get("error_code"))))
                continue
            if key not in matched_keys:  # no written result: already result_missing
                continue
            if matched_keys[key]:
                matched += 1
                tally["matched"] += 1
            else:
                mismatches.append(
                    _mismatch(key, "reference_mismatch", f"{rule['matching']} match failed")
                )
    return {
        "schema": CORRECTNESS_SCHEMA,
        "status": "passed" if not mismatches and checked > 0 else "failed",
        "reference_source": reference["source"],
        "reference_sha256": reference_sha256,
        "checked": checked,
        "matched": matched,
        "unchecked": len(set(samples) - referenced),
        "queries": queries,
        "mismatches": mismatches,
    }


def _mismatch(key: tuple[str, str], cause: str, detail: str) -> dict[str, Any]:
    return {"query_id": key[0], "binding_id": key[1], "cause": cause, "detail": detail}
