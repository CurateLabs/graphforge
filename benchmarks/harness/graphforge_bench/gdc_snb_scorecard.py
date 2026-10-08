"""Measured workloads and references for the SNB BI and Interactive v1 scorecards (#1904).

A scorecard rung whose ladder spec declares a workload *builder* gets its query
workload and its reference built here, from pinned LDBC inputs, before the
rung's measured phases run. Both are published as rung documents, so their
SHA-256s are in the rung result and on the card's evidence list.

**Workloads.** Every runnable read comes from the suite's committed query
definitions (``profiles/gdc/snb-*-scorecard-queries.json``, the Rust runners'
``list-queries`` output). Its measured bindings are a declared, deterministic
subset of the pinned LDBC parameters: the first ``per_variant`` rows of each
parameter file, in file order.

- SNB BI reads ``parameters-sfN/bi-<n>[a|b].csv``. Each file is one variant
  (``BI2a``, ``BI2b``, ...). LDBC's SF10 validation output (Umbra) covers
  exactly the first 30 rows of every SF10 file, so 30 per variant makes the
  SF10 workload the validated one.
- SNB Interactive v1 reads ``interactive_<n>_param.txt`` for the complex reads.
  IC3 and IC4 take ``endDate``, as the Cypher reference does, so it is
  ``startDate + durationDays`` days. The short reads have no substitution file;
  their ids are the short-read parameters of the pinned validation stream, in
  stream order, excluding ids that the stream's own updates create.
  A binding whose person or message is absent from the loaded snapshot is
  skipped and counted, so every measured binding reads loaded data.

**References.**

- SNB BI: Umbra's SF10 validation output (``results.csv``: query, variant,
  parameters JSON, result JSON). Each line is paired with the workload binding
  of the same variant and position, and refused unless its parameters equal
  that binding's. Values are converted to the query driver's cell format
  (Arrow display text) by the declared column kind. Reads with a float column
  match with a relative epsilon, everything else exactly.
- SNB Interactive v1: a spec-derived reference. The pinned validation stream
  begins with an update at position 0 at every published scale factor, so no
  validation read describes the bulk-load snapshot. Instead
  ``graphforge_bench.gdc_snb_interactive_reference``, which never runs
  GraphForge, evaluates every measured binding over the snapshot read straight
  from the archive's CSV files (#952 decision 2026-10-08).
"""

from __future__ import annotations

import argparse
from collections.abc import Iterator, Mapping, Sequence
from datetime import date
import json
import math
from pathlib import Path
import re
import sys
from typing import Any

from graphforge_bench.gdc_rung_inputs import RungInputError

WORKLOAD_SCHEMA = "graphforge-gdc-query-workload/1"
REFERENCE_SCHEMA = "graphforge-gdc-rung-reference/1"
BI_QUERIES_SCHEMA = "graphforge-gdc-snb-bi-queries/1"
INTERACTIVE_QUERIES_SCHEMA = "graphforge-gdc-snb-interactive-queries/1"
INPUTS_SCHEMA = "graphforge-gdc-snb-scorecard-inputs/1"
DAY_MS = 86_400_000
EPOCH = date(1970, 1, 1)
# Relative tolerance for BI float cells. Umbra prints some doubles with 15
# significant digits (BI13 zombieScore 0.166666666666667), and GraphForge
# computes the same ratios in a different order.
BI_EPSILON = 1e-9
SPEC_DERIVED_SOURCE = (
    "a spec-derived reference (gdc_snb_interactive_reference over the loaded snapshot), not "
    "the LDBC validation set (the pinned validation stream begins with an update at position 0)"
)

Literal = dict[str, Any]


def _fail(cause: str, message: str) -> RungInputError:
    return RungInputError(cause, message)


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise _fail("invalid_document", f"{path}: {error}") from error


# --------------------------------------------------------------------------
# Typed parameter literals (IrLiteral's tagged JSON form)
# --------------------------------------------------------------------------


def int_literal(text: Any) -> Literal:
    if isinstance(text, bool):
        raise _fail("invalid_parameter", f"not an integer: {text!r}")
    if isinstance(text, int):
        return {"type": "Int", "value": text}
    if not isinstance(text, str) or not re.fullmatch(r"-?[0-9]+", text):
        raise _fail("invalid_parameter", f"not an integer: {text!r}")
    return {"type": "Int", "value": int(text)}


def str_literal(text: Any) -> Literal:
    if not isinstance(text, str):
        raise _fail("invalid_parameter", f"not a string: {text!r}")
    return {"type": "Str", "value": text}


def string_list_literal(text: Any) -> Literal:
    """An LDBC ``STRING[]`` cell: items separated by ``;``."""
    if not isinstance(text, str) or not text or any(item == "" for item in text.split(";")):
        raise _fail("invalid_parameter", f"not a ';'-separated string list: {text!r}")
    return {"type": "List", "value": [str_literal(item) for item in text.split(";")]}


_DATETIME = re.compile(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d{3})\+00:00")


def utc_datetime_literal(text: Any) -> Literal:
    """An LDBC ``DATE`` (midnight UTC) or ``DATETIME`` (UTC) as a zoned datetime.

    The BI queries compare these with stored ``datetime`` properties, so a
    ``DATE`` binds as the instant its day starts, as the BI fixture does.
    """
    if not isinstance(text, str):
        raise _fail("invalid_parameter", f"not a date or datetime: {text!r}")
    nanos = 0
    match = _DATETIME.fullmatch(text)
    try:
        if match is not None:
            year, month, day, hour, minute, second, millis = (int(part) for part in match.groups())
            day_value = date(year, month, day)
            nanos = ((hour * 60 + minute) * 60 + second) * 1_000_000_000 + millis * 1_000_000
            if hour > 23 or minute > 59 or second > 59:
                raise ValueError("time out of range")
        elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
            day_value = date.fromisoformat(text)
        else:
            raise ValueError("not YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS.mmm+00:00")
    except ValueError as error:
        raise _fail("invalid_parameter", f"{text!r}: {error}") from error
    return {"type": "ZonedDateTime", "value": [(day_value - EPOCH).days, nanos, 0, None]}


def plain_value(literal: Literal) -> Any:
    """The Python value of a literal this module builds (for the spec-derived reference)."""
    if literal["type"] == "List":
        return [plain_value(item) for item in literal["value"]]
    return literal["value"]


# --------------------------------------------------------------------------
# LDBC parameter files
# --------------------------------------------------------------------------


def read_parameter_file(path: Path) -> tuple[list[tuple[str, str | None]], list[list[str]]]:
    """Header ``(name, type)`` pairs and rows of a pipe-separated LDBC parameter file.

    BI headers carry a Neo4j import type (``date:DATE``); Interactive headers
    are bare names, whose type is ``None``.
    """
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        raise _fail("parameters_missing", f"{path}: {error}") from error
    if not lines:
        raise _fail("parameters_missing", f"{path} is empty")
    header: list[tuple[str, str | None]] = []
    for field in lines[0].split("|"):
        name, _, kind = field.partition(":")
        header.append((name, kind or None))
    rows = []
    for number, line in enumerate(lines[1:], start=2):
        if not line:
            continue
        values = line.split("|")
        if len(values) != len(header):
            raise _fail("invalid_parameter", f"{path}:{number}: {len(values)} fields")
        rows.append(values)
    return header, rows


def _queries(document: Mapping[str, Any], schema: str) -> list[Mapping[str, Any]]:
    if document.get("schema") != schema:
        raise _fail("invalid_document", f"query definitions must be {schema}")
    return list(document["queries"])


# --------------------------------------------------------------------------
# SNB BI workload
# --------------------------------------------------------------------------

BI_LITERALS = {
    "datetime": ({"DATE", "DATETIME"}, utc_datetime_literal),
    "string": ({"STRING"}, str_literal),
    "int64": ({"INT", "ID"}, int_literal),
    "string_list": ({"STRING[]"}, string_list_literal),
}


def _bi_files(parameters_dir: Path, number: str) -> list[tuple[str, Path]]:
    single = parameters_dir / f"bi-{number}.csv"
    split = [parameters_dir / f"bi-{number}{suffix}.csv" for suffix in "ab"]
    if single.is_file() and not any(path.is_file() for path in split):
        return [("", single)]
    if all(path.is_file() for path in split) and not single.is_file():
        return [(suffix, path) for suffix, path in zip("ab", split)]
    raise _fail("parameters_missing", f"no unambiguous parameter file for BI{number}")


def bi_binding(
    query: Mapping[str, Any], values: Mapping[str, Any], header_types: Mapping[str, str | None]
) -> dict[str, Literal]:
    """Typed parameters of one BI binding; every declared parameter, nothing else."""
    declared = {parameter["name"] for parameter in query["parameters"]}
    if set(values) != declared:
        raise _fail(
            "invalid_parameter",
            f"{query['operation']} takes {sorted(declared)}, the parameters are {sorted(values)}",
        )
    params: dict[str, Literal] = {}
    for parameter in query["parameters"]:
        name, kind = parameter["name"], parameter["kind"]
        accepted, convert = BI_LITERALS[kind]
        header_type = header_types.get(name)
        if header_type is not None and header_type not in accepted:
            raise _fail(
                "invalid_parameter", f"{query['operation']}.{name}: file type {header_type}"
            )
        literal = convert(values[name])
        if parameter.get("fixed") is not None and literal["value"] != parameter["fixed"]:
            raise _fail(
                "invalid_parameter",
                f"{query['operation']}.{name} is fixed at {parameter['fixed']}, not {values[name]}",
            )
        params[name] = literal
    return params


def bi_workload(
    queries_document: Mapping[str, Any], parameters_dir: Path, per_variant: int
) -> dict[str, Any]:
    """The BI workload: one variant per parameter file, its first ``per_variant`` rows."""
    variants = []
    for query in _queries(queries_document, BI_QUERIES_SCHEMA):
        number = str(query["operation"]).removeprefix("BI")
        for suffix, path in _bi_files(parameters_dir, number):
            header, rows = read_parameter_file(path)
            types = dict(header)
            names = [name for name, _kind in header]
            bindings = [
                {"id": f"p{index:03d}", "params": bi_binding(query, dict(zip(names, row)), types)}
                for index, row in enumerate(rows[:per_variant])
            ]
            if not bindings:
                raise _fail("parameters_missing", f"{path} has no rows")
            variants.append(
                {
                    "id": f"{query['operation']}{suffix}",
                    "ordered": True,
                    "operation": {"kind": "cypher", "text": query["cypher"]},
                    "bindings": bindings,
                }
            )
    return {"schema": WORKLOAD_SCHEMA, "suite": "snb-bi", "variants": variants}


# --------------------------------------------------------------------------
# Driver cell format (Arrow display text)
# --------------------------------------------------------------------------


def render_int(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _fail("reference_unconvertible", f"not an integer: {value!r}")
    return str(value)


def render_float(value: Any) -> str:
    """A float cell; float columns are matched numerically, never as text."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise _fail("reference_unconvertible", f"not a finite number: {value!r}")
    return repr(float(value))


def render_bool(value: Any) -> str:
    if not isinstance(value, bool):
        raise _fail("reference_unconvertible", f"not a boolean: {value!r}")
    return "true" if value else "false"


def render_string(value: Any) -> str:
    if not isinstance(value, str):
        raise _fail("reference_unconvertible", f"not a string: {value!r}")
    return value


_UMBRA_DATETIME = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}(\+00:00|Z)")


def render_utc_datetime(value: Any) -> str:
    """A stored UTC ``datetime`` as the driver renders it.

    The converter stores a datetime as GraphForge's canonical datetime struct,
    and a query returns that struct: days since the epoch, the local time
    (``Time64(ns)``, which Arrow displays without a fraction when it is zero),
    the UTC offset in seconds and a null zone, for example
    ``{date: 15404, time: 21:42:45.869, offset: 0, zone: }``.
    """
    if not isinstance(value, str) or _UMBRA_DATETIME.fullmatch(value) is None:
        raise _fail("reference_unconvertible", f"not a UTC datetime: {value!r}")
    days = (date.fromisoformat(value[:10]) - EPOCH).days
    clock, millis = value[11:19], value[20:23]
    fraction = "" if millis == "000" else f".{millis}"
    return f"{{date: {days}, time: {clock}{fraction}, offset: 0, zone: }}"


def render_string_list(value: Any) -> str:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise _fail("reference_unconvertible", f"not a string list: {value!r}")
    return "[" + ", ".join(value) + "]"


def render_map(value: Any, fields: Sequence[tuple[str, str]]) -> str:
    """A Cypher map as the driver renders it: an Arrow struct, ``{k: v, ...}``.

    GraphForge returns a map's keys as struct fields in name order, and a null
    field renders as nothing (``{city: , classYear: , name: }``).
    """
    if not isinstance(value, Mapping) or set(value) != {name for name, _ in fields}:
        raise _fail("reference_unconvertible", f"not a map with {fields}: {value!r}")
    parts = []
    for name, kind in sorted(fields):
        item = value[name]
        parts.append(f"{name}: {'' if item is None else RENDERERS[kind](item)}")
    return "{" + ", ".join(parts) + "}"


RENDERERS = {
    "int": render_int,
    "float": render_float,
    "bool": render_bool,
    "string": render_string,
    "datetime": render_utc_datetime,
    "string_list": render_string_list,
}


def render_cell(kind: str | tuple[str, Sequence[tuple[str, str]]], value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(kind, tuple):
        # A list of maps (IC1 universities and companies).
        if not isinstance(value, list):
            raise _fail("reference_unconvertible", f"not a list: {value!r}")
        return "[" + ", ".join(render_map(item, kind[1]) for item in value) + "]"
    return RENDERERS[kind](value)


# --------------------------------------------------------------------------
# SNB BI reference (Umbra SF10 validation output)
# --------------------------------------------------------------------------

# Result column kinds, in the order of each query's declared columns. Umbra and
# the Cypher texts return the specification's columns in the same order.
BI_COLUMN_KINDS: dict[str, tuple[str, ...]] = {
    "BI1": ("int", "bool", "int", "int", "float", "int", "float"),
    "BI2": ("string", "int", "int", "int"),
    "BI3": ("int", "string", "datetime", "int", "int"),
    "BI4": ("int", "string", "string", "datetime", "int"),
    "BI5": ("int", "int", "int", "int", "int"),
    "BI6": ("int", "int"),
    "BI7": ("string", "int"),
    "BI8": ("int", "int", "int"),
    "BI9": ("int", "string", "string", "int", "int"),
    "BI10": ("int", "string", "int"),
    "BI11": ("int",),
    "BI12": ("int", "int"),
    "BI13": ("int", "int", "int", "float"),
    "BI14": ("int", "int", "string", "int"),
    "BI16": ("int", "int", "int"),
    "BI17": ("int", "int"),
    "BI18": ("int", "int", "int"),
}


def bi_rule(query: Mapping[str, Any]) -> dict[str, Any]:
    kinds = BI_COLUMN_KINDS[query["operation"]]
    columns = list(query["columns"])
    if len(kinds) != len(columns):
        raise _fail("invalid_document", f"{query['operation']}: column kinds do not match")
    if "float" not in kinds:
        return {"matching": "exact"}
    key = [name for name, kind in zip(columns, kinds) if kind != "float"]
    return {"matching": "epsilon", "epsilon": BI_EPSILON, "key": key}


def _umbra_lines(results: Path) -> Iterator[tuple[int, str, Any, Any]]:
    try:
        lines = results.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        raise _fail("reference_missing", f"{results}: {error}") from error
    for number, line in enumerate(lines, start=1):
        if not line:
            continue
        parts = line.split("|", 3)
        if len(parts) != 4:
            raise _fail("reference_unconvertible", f"{results}:{number}: not q|variant|params|rows")
        try:
            yield number, parts[1], json.loads(parts[2]), json.loads(parts[3])
        except json.JSONDecodeError as error:
            raise _fail("reference_unconvertible", f"{results}:{number}: {error}") from error


def bi_reference(
    *,
    queries_document: Mapping[str, Any],
    workload: Mapping[str, Any],
    results: Path,
    suite_id: str,
    rung_id: str,
    source: str,
) -> dict[str, Any]:
    """Convert Umbra's validation output into the rung reference.

    Line ``j`` of a variant is the variant's ``j``-th binding, and must carry
    exactly that binding's parameters. A variant the workload does not run
    (a refused read) is skipped; a measured variant with no Umbra line is
    simply not checked.
    """
    queries = {query["operation"]: query for query in _queries(queries_document, BI_QUERIES_SCHEMA)}
    variants = {variant["id"]: variant for variant in workload["variants"]}
    seen: dict[str, int] = {}
    converted: dict[str, dict[str, Any]] = {}
    for number, variant_code, params, rows in _umbra_lines(results):
        variant_id = f"BI{variant_code}"
        variant = variants.get(variant_id)
        if variant is None:
            continue
        query = queries[re.sub(r"[ab]$", "", variant_id)]
        position = seen.get(variant_id, 0)
        seen[variant_id] = position + 1
        if position >= len(variant["bindings"]):
            continue
        binding = variant["bindings"][position]
        if not isinstance(params, Mapping):
            raise _fail("reference_unconvertible", f"{results}:{number}: parameters not an object")
        expected_params = bi_binding(
            query, {name: str(value) for name, value in params.items()}, {}
        )
        if expected_params != binding["params"]:
            raise _fail(
                "reference_binding_mismatch",
                f"{results}:{number}: {variant_id} line {position} has parameters {params}, "
                f"the workload's binding {binding['id']} differs",
            )
        kinds = BI_COLUMN_KINDS[query["operation"]]
        if not isinstance(rows, list):
            raise _fail("reference_unconvertible", f"{results}:{number}: rows not a list")
        cells = []
        for row in rows:
            if not isinstance(row, Mapping) or len(row) != len(kinds):
                raise _fail(
                    "reference_unconvertible",
                    f"{results}:{number}: a row has {len(row)} values, {query['operation']} "
                    f"returns {len(kinds)}",
                )
            cells.append([render_cell(kind, value) for kind, value in zip(kinds, row.values())])
        entry = converted.setdefault(variant_id, {**bi_rule(query), "bindings": {}})
        entry["bindings"][binding["id"]] = {"columns": list(query["columns"]), "rows": cells}
    if not converted:
        raise _fail("reference_not_applied", f"{results} covers no measured variant")
    return {
        "schema": REFERENCE_SCHEMA,
        "suite_id": suite_id,
        "rung_id": rung_id,
        "source": source,
        "queries": converted,
    }


# --------------------------------------------------------------------------
# SNB Interactive v1 snapshot, workload and spec-derived reference
# --------------------------------------------------------------------------

_EMPTY: dict[str, Any] = {}


def _csv_rows(path: Path) -> Iterator[list[str]]:
    with path.open(encoding="utf-8") as stream:
        next(stream)
        for raw in stream:
            line = raw.rstrip("\n")
            if line:
                yield line.split("|")


def _resolve_files(input_root: Path, patterns: Sequence[str]) -> list[Path]:
    files: list[Path] = []
    for pattern in patterns:
        matched = sorted(path for path in input_root.glob(pattern) if not path.name.startswith("."))
        if not matched:
            raise _fail("input_missing", f"{pattern} matches nothing under {input_root}")
        files.extend(matched)
    return files


def _header(path: Path) -> list[str]:
    with path.open(encoding="utf-8") as stream:
        names = stream.readline().rstrip("\n").split("|")
    seen: dict[str, int] = {}
    unique = []
    for name in names:
        count = seen.get(name, 0)
        seen[name] = count + 1
        unique.append(name if count == 0 else f"{name}.{count}")
    return unique


def _property_value(prop: Mapping[str, Any], text: str) -> Any:
    if text == "":
        return None
    if prop["type"] == "int64":
        return int(text)
    if prop["type"] == "list":
        return text.split(prop.get("separator", ";"))
    if prop["type"] == "string":
        return text
    raise _fail("invalid_mapping", f"the Interactive reader has no {prop['type']} type")


def snapshot_document(input_root: Path, mapping: Mapping[str, Any]) -> dict[str, Any]:
    """The loaded snapshot read from the archive's CSV files, in the reference's model.

    It reads the same load mapping as the converter, independently: one node
    per row with its stored label (``label_column`` mapped through
    ``label_values``) and mapped properties, one edge per row. Nodes and edges
    are generators, so ``Graph`` indexes them without an intermediate list.
    """
    keys: dict[str, dict[int, str]] = {}
    for table in mapping["node_tables"]:
        index = keys.setdefault(table["label"], {})
        for path in _resolve_files(input_root, table["files"]):
            header = _header(path)
            position = {name: number for number, name in enumerate(header)}
            label_at = position.get(table.get("label_column") or "")
            for values in _csv_rows(path):
                identity = int(values[position[table["id_column"]]])
                label = table["label"]
                if label_at is not None:
                    label = table["label_values"][values[label_at]]
                if identity in index:
                    raise _fail("duplicate_identity", f"{table['label']} {identity} repeats")
                index[identity] = sys.intern(f"{label}:{identity}")

    def nodes() -> Iterator[list[Any]]:
        for table in mapping["node_tables"]:
            index = keys[table["label"]]
            for path in _resolve_files(input_root, table["files"]):
                header = _header(path)
                position = {name: number for number, name in enumerate(header)}
                for values in _csv_rows(path):
                    key = index[int(values[position[table["id_column"]]])]
                    properties = {}
                    for prop in table["properties"]:
                        value = _property_value(prop, values[position[prop["column"]]])
                        if value is not None:
                            properties[prop.get("name", prop["column"])] = value
                    yield [key, key.split(":", 1)[0], properties]

    def edges() -> Iterator[list[Any]]:
        for table in mapping["edge_tables"]:
            sources = keys[table["source"]["label"]]
            targets = keys[table["target"]["label"]]
            for path in _resolve_files(input_root, table["files"]):
                header = _header(path)
                position = {name: number for number, name in enumerate(header)}
                source_at = position[table["source"]["column"]]
                target_at = position[table["target"]["column"]]
                props = table.get("properties", [])
                for values in _csv_rows(path):
                    try:
                        source = sources[int(values[source_at])]
                        target = targets[int(values[target_at])]
                    except KeyError as error:
                        raise _fail("dangling_edge", f"{path.name}: {error}") from error
                    properties = _EMPTY
                    if props:
                        properties = {}
                        for prop in props:
                            value = _property_value(prop, values[position[prop["column"]]])
                            if value is not None:
                                properties[prop.get("name", prop["column"])] = value
                    yield [source, table["rel_type"], target, properties]

    return {"nodes": nodes(), "edges": edges()}


# Interactive validation stream: the short-read parameter key of each IS read,
# and the id keys an update creates (IU1 person, IU2-IU8 add posts, comments,
# forums and edges; only node creations make a new id).
SHORT_READ_KEYS = {
    "personIdSQ1": ("IS1", "personId"),
    "personIdSQ2": ("IS2", "personId"),
    "personIdSQ3": ("IS3", "personId"),
    "messageIdContent": ("IS4", "messageId"),
    "messageIdCreator": ("IS5", "messageId"),
    "messageForumId": ("IS6", "messageId"),
    "messageRepliesId": ("IS7", "messageId"),
}
READ_KEY = re.compile(
    r"Q\d+|SQ\d|messageIdContent|messageIdCreator|messageForumId|messageRepliesId"
)


def stream_operations(validation: Path) -> Iterator[tuple[int, dict[str, Any]]]:
    """``(position, parameters)`` of every operation in a v1 validation stream."""
    with validation.open(encoding="utf-8") as stream:
        for position, line in enumerate(stream):
            params_text, separator, _result = line.rstrip("\n").partition("|")
            try:
                params = json.loads(params_text)
            except json.JSONDecodeError as error:
                raise _fail("invalid_parameter", f"{validation}:{position}: {error}") from error
            if not separator or not isinstance(params, dict):
                raise _fail("invalid_parameter", f"{validation}:{position}: not params|result")
            yield position, params


def is_update(params: Mapping[str, Any]) -> bool:
    return not any(READ_KEY.search(name) for name in params)


def created_ids(params: Mapping[str, Any]) -> set[tuple[str, int]]:
    """Node ids an update creates: IU1 a person, IU6 a post, IU7 a comment, IU4 a forum."""
    created = set()
    if "personFirstName" in params:
        created.add(("person", int(params["personId"])))
    if "imageFile" in params and "postId" in params:
        created.add(("message", int(params["postId"])))
    if "replyToPostId" in params:
        created.add(("message", int(params["commentId"])))
    return created


PERSON_PARAMETERS = {"personId", "person1Id", "person2Id"}
MESSAGE_PARAMETERS = {"messageId"}
INTERACTIVE_FILE_RENAMES = {"IC13": {}}  # names already equal the reference's


def _interactive_literal(parameter: Mapping[str, Any], text: str) -> Literal:
    return int_literal(text) if parameter["data_type"] == "int64" else str_literal(text)


def _exists(graph: Any, params: Mapping[str, Literal]) -> bool:
    for name, literal in params.items():
        if name in PERSON_PARAMETERS and graph.by_id("Person", literal["value"]) is None:
            return False
        if name in MESSAGE_PARAMETERS and (
            graph.by_id("Post", literal["value"]) is None
            and graph.by_id("Comment", literal["value"]) is None
        ):
            return False
    return True


def interactive_workload(
    *,
    queries_document: Mapping[str, Any],
    substitution_dir: Path,
    validation: Path,
    per_variant: int,
    graph: Any,
) -> tuple[dict[str, Any], dict[str, int]]:
    """The Interactive workload and, per variant, how many candidate bindings were absent."""
    variants = []
    absent: dict[str, int] = {}
    queries = _queries(queries_document, INTERACTIVE_QUERIES_SCHEMA)
    short: dict[str, list[dict[str, Literal]]] = {}
    seen_short: set[tuple[str, int]] = set()
    created: set[tuple[str, int]] = set()
    operations = list(stream_operations(validation))
    for _position, params in operations:
        if is_update(params):
            created |= created_ids(params)
    for _position, params in operations:
        for key, (operation, name) in SHORT_READ_KEYS.items():
            if key in params:
                value = int(params[key])
                kind = "person" if name == "personId" else "message"
                if (kind, value) in created or (operation, value) in seen_short:
                    continue
                seen_short.add((operation, value))
                short.setdefault(operation, []).append({name: int_literal(value)})
    for query in queries:
        operation = query["operation"]
        declared = {parameter["name"]: parameter for parameter in query["parameters"]}
        candidates: list[tuple[str, dict[str, Literal]]] = []
        if operation.startswith("IC"):
            path = substitution_dir / f"interactive_{operation.removeprefix('IC')}_param.txt"
            header, rows = read_parameter_file(path)
            names = [name for name, _kind in header]
            for index, row in enumerate(rows):
                values = dict(zip(names, row))
                if "durationDays" in values and "endDate" in declared:
                    start, days = int(values["startDate"]), int(values.pop("durationDays"))
                    values["endDate"] = str(start + days * DAY_MS)
                if set(values) != set(declared):
                    raise _fail(
                        "invalid_parameter",
                        f"{operation} takes {sorted(declared)}, {path.name} has {sorted(values)}",
                    )
                params = {
                    name: _interactive_literal(declared[name], values[name]) for name in declared
                }
                candidates.append((f"p{index:03d}", params))
        else:
            for params in short.get(operation, []):
                ((name, literal),) = params.items()
                if set(declared) != {name}:
                    raise _fail("invalid_parameter", f"{operation} takes {sorted(declared)}")
                candidates.append((f"{name}-{literal['value']}", params))
        bindings = []
        absent[operation] = 0
        for binding_id, params in candidates:
            if len(bindings) == per_variant:
                break
            if not _exists(graph, params):
                absent[operation] += 1
                continue
            bindings.append({"id": binding_id, "params": params})
        if not bindings:
            raise _fail("parameters_missing", f"{operation} has no binding on loaded data")
        variants.append(
            {
                "id": operation,
                "ordered": True,
                "operation": _interactive_operation(query),
                "bindings": bindings,
            }
        )
    return {"schema": WORKLOAD_SCHEMA, "suite": "snb-interactive", "variants": variants}, absent


def _interactive_operation(query: Mapping[str, Any]) -> dict[str, Any]:
    if query["interface"] == "cypher":
        return {"kind": "cypher", "text": query["cypher"]}
    if query["operation"] != "IC13":
        raise _fail("invalid_document", f"{query['operation']}: unknown interface")
    return {
        "kind": "paths",
        "by": "bfs",
        "directed": False,
        "via": "KNOWS",
        "source": {"label": "Person", "property": "id", "param": "person1Id"},
        "target": {"label": "Person", "property": "id", "param": "person2Id"},
    }


# Result column kinds of every Interactive read, in declared column order.
_UNIVERSITY = ("map_list", (("name", "string"), ("classYear", "int"), ("city", "string")))
_COMPANY = ("map_list", (("name", "string"), ("workFrom", "int"), ("country", "string")))
INTERACTIVE_COLUMN_KINDS: dict[str, tuple[Any, ...]] = {
    "IC1": (
        "int",
        "string",
        "int",
        "int",
        "int",
        "string",
        "string",
        "string",
        "string_list",
        "string_list",
        "string",
        _UNIVERSITY,
        _COMPANY,
    ),
    "IC2": ("int", "string", "string", "int", "string", "int"),
    "IC3": ("int", "string", "string", "int", "int", "int"),
    "IC4": ("string", "int"),
    "IC5": ("string", "int"),
    "IC6": ("string", "int"),
    "IC7": ("int", "string", "string", "int", "int", "string", "int", "bool"),
    "IC8": ("int", "string", "string", "int", "int", "string"),
    "IC9": ("int", "string", "string", "int", "string", "int"),
    "IC10": ("int", "string", "string", "int", "string", "string"),
    "IC11": ("int", "string", "string", "string", "int"),
    "IC12": ("int", "string", "string", "string_list", "int"),
    "IS1": ("string", "string", "int", "string", "string", "int", "string", "int"),
    "IS2": ("int", "string", "int", "int", "int", "string", "string"),
    "IS3": ("int", "string", "string", "int"),
    "IS4": ("int", "string"),
    "IS5": ("int", "string", "string"),
    "IS6": ("int", "string", "int", "string", "string"),
    "IS7": ("int", "string", "int", "int", "string", "string", "bool"),
}
# The bfs verb's hop count column, compared on its own (IC13).
IC13_COST_COLUMN = "cost"


def _interactive_rule(query: Mapping[str, Any]) -> dict[str, Any]:
    if query["operation"] == "IC13":
        return {"matching": "projection"}
    sets = list(query.get("unordered_list_columns") or [])
    return {"matching": "exact", **({"set_columns": sets} if sets else {})}


def _interactive_rows(
    query: Mapping[str, Any], rows: Sequence[Sequence[Any]]
) -> tuple[list[str], list[list[str | None]]]:
    operation = query["operation"]
    if operation == "IC13":
        (length,) = rows[0] if len(rows) == 1 else (None,)
        if not isinstance(length, int) or isinstance(length, bool):
            raise _fail("reference_unconvertible", f"IC13 length {rows!r}")
        # paths(by=bfs) returns no row when the persons are not connected
        # (the reference's -1) and one row whose cost is the hop count otherwise.
        return [IC13_COST_COLUMN], ([] if length < 0 else [[render_float(length)]])
    kinds = INTERACTIVE_COLUMN_KINDS[operation]
    columns = list(query["columns"])
    if len(kinds) != len(columns):
        raise _fail("invalid_document", f"{operation}: column kinds do not match")
    cells = []
    for row in rows:
        if len(row) != len(kinds):
            raise _fail("reference_unconvertible", f"{operation}: row width {len(row)}")
        rendered = []
        for kind, value in zip(kinds, row):
            if isinstance(kind, tuple) and kind[0] == "map_list":
                rendered.append(render_cell(("map_list", kind[1]), value))
            else:
                rendered.append(render_cell(kind, value))
        cells.append(rendered)
    return columns, cells


def interactive_reference(
    *,
    queries_document: Mapping[str, Any],
    workload: Mapping[str, Any],
    graph: Any,
    suite_id: str,
    rung_id: str,
) -> dict[str, Any]:
    """Evaluate every measured binding with the spec-derived reference."""
    from graphforge_bench import gdc_snb_interactive_reference as reference

    queries = {
        query["operation"]: query
        for query in _queries(queries_document, INTERACTIVE_QUERIES_SCHEMA)
    }
    converted: dict[str, dict[str, Any]] = {}
    for variant in workload["variants"]:
        query = queries[variant["id"]]
        evaluate = reference.EVALUATORS[variant["id"]]
        entry = converted.setdefault(variant["id"], {**_interactive_rule(query), "bindings": {}})
        for binding in variant["bindings"]:
            params = {name: plain_value(literal) for name, literal in binding["params"].items()}
            rows = evaluate(graph, params)
            columns, cells = _interactive_rows(query, rows)
            entry["bindings"][binding["id"]] = {"columns": columns, "rows": cells}
    return {
        "schema": REFERENCE_SCHEMA,
        "suite_id": suite_id,
        "rung_id": rung_id,
        "source": SPEC_DERIVED_SOURCE,
        "queries": converted,
    }


# --------------------------------------------------------------------------
# Rung entry point: build the inputs a ladder spec declares
# --------------------------------------------------------------------------


def build_inputs(
    *,
    spec_workload: Mapping[str, Any],
    spec_reference: Mapping[str, Any] | None,
    profile_root: Path,
    input_root: Path,
    parameters_root: Path,
    reference_root: Path | None,
    suite_id: str,
    rung_id: str,
    mapping: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Build one rung's workload and reference from its declared builder."""
    queries_document = read_json(profile_root / spec_workload["queries"])
    per_variant = int(spec_workload["per_variant"])
    parameters = parameters_root / spec_workload["parameters"]["path"]
    builder = spec_workload["builder"]
    notes: dict[str, Any] = {
        "schema": INPUTS_SCHEMA,
        "builder": builder,
        "per_variant": per_variant,
    }
    if builder == "snb-bi":
        workload = bi_workload(queries_document, parameters, per_variant)
        reference = None
        if spec_reference is not None:
            if reference_root is None:
                raise _fail("reference_missing", "the BI reference archive was not acquired")
            reference = bi_reference(
                queries_document=queries_document,
                workload=workload,
                results=reference_root / spec_reference["path"],
                suite_id=suite_id,
                rung_id=rung_id,
                source=spec_reference["source"],
            )
        return {"workload": workload, "reference": reference, "notes": notes}
    if builder != "snb-interactive":
        raise _fail("invalid_rung_spec", f"unknown workload builder {builder!r}")
    from graphforge_bench.gdc_snb_interactive_reference import Graph

    if mapping is None or reference_root is None:
        raise _fail("invalid_rung_spec", "the Interactive builder needs the mapping and stream")
    graph = Graph(snapshot_document(input_root, mapping))
    workload, absent = interactive_workload(
        queries_document=queries_document,
        substitution_dir=parameters,
        validation=reference_root / spec_workload["short_reads"]["path"],
        per_variant=per_variant,
        graph=graph,
    )
    notes["absent_bindings"] = absent
    reference = None
    if spec_reference is not None:
        if spec_reference.get("builder") != "snb-interactive-spec-derived":
            raise _fail("invalid_rung_spec", "Interactive references are spec-derived")
        reference = interactive_reference(
            queries_document=queries_document,
            workload=workload,
            graph=graph,
            suite_id=suite_id,
            rung_id=rung_id,
        )
    return {"workload": workload, "reference": reference, "notes": notes}


def main(argv: Sequence[str] | None = None) -> int:
    """Build a rung's inputs in a child process, so the snapshot index's memory is returned."""
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    request = read_json(args.request)
    try:
        built = build_inputs(
            spec_workload=request["workload"],
            spec_reference=request["reference"],
            profile_root=Path(request["profile_root"]),
            input_root=Path(request["input_root"]),
            parameters_root=Path(request["parameters_root"]),
            reference_root=Path(request["reference_root"]) if request["reference_root"] else None,
            suite_id=request["suite_id"],
            rung_id=request["rung_id"],
            mapping=read_json(Path(request["mapping"])) if request["mapping"] else None,
        )
    except RungInputError as error:
        print(json.dumps({"cause": error.cause, "message": str(error)}), file=sys.stderr)
        return 3
    except (AssertionError, KeyError, ValueError) as error:
        print(
            json.dumps({"cause": "reference_derivation_failed", "message": repr(error)}),
            file=sys.stderr,
        )
        return 3
    for name in ("workload", "reference", "notes"):
        if built[name] is not None:
            (args.output_dir / f"{name}.json").write_text(
                json.dumps(built[name], separators=(",", ":")) + "\n", encoding="utf-8"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
