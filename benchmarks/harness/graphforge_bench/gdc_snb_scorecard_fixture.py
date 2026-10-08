"""Build the SNB scorecard CI fixture (#1904) from the committed query fixtures.

The fixture puts the two query fixtures into the shapes of the real pinned
LDBC archives, so the rung runner can climb them with the real converter,
``gf`` and query driver, the real load mappings and the real workload and
reference builders:

- SNB BI: ``fixtures/gdc/snb-bi-queries/graph`` becomes a
  ``composite-projected-fk`` initial snapshot (gzip CSV parts); its
  ``parameters.json`` becomes ``parameters-sf0/bi-<n>.csv``; and its
  independently derived ``expected/BI<n>.json`` rows become an Umbra-format
  ``results.csv``, the file the SF10 reference converter reads. A Forum
  ``HAS_TAG`` edge is added for every forum and tag, so a BI read that lost its
  ``Post OR Comment`` predicate would count forums.
- SNB Interactive v1: the ``gdc_snb_interactive_reference`` fixture graph
  becomes ``social_network-*/{static,dynamic}/*_0_0.csv``; its parameters become
  ``interactive_<n>_param.txt`` files (IC3 and IC4 as ``startDate`` plus whole
  ``durationDays``); and a small validation stream supplies the short-read ids.
  Like the real streams it begins with an update, and one update creates a
  message that a later short read names, which the builder must exclude.

``python -m graphforge_bench.gdc_snb_scorecard_fixture --write`` rebuilds
``fixtures/gdc/snb-scorecard/archives`` and the counts in the fixture's count
ladders; without ``--write`` it fails when a committed archive's members or a
count differ from a fresh build. Archive bytes are not compared, since
``zstd`` output may differ between versions; the identity profiles pin the
committed bytes.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
import gzip
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any

from graphforge_bench import gdc_snb_interactive_reference as interactive_reference
from graphforge_bench.gdc_contracts import workspace_root

DAY_MS = 86_400_000
ROOT = workspace_root()
FIXTURE = ROOT / "fixtures" / "gdc" / "snb-scorecard"
BI_QUERIES = ROOT / "fixtures" / "gdc" / "snb-bi-queries"
BI_MAPPING = ROOT / "profiles" / "gdc" / "snb-bi-load-mapping.json"
INTERACTIVE_MAPPING = ROOT / "profiles" / "gdc" / "snb-interactive-load-mapping.json"
BI_SNAPSHOT = "graphs/csv/bi/composite-projected-fk/initial_snapshot"

Files = dict[str, bytes]


def _csv(header: Sequence[str], rows: Sequence[Sequence[Any]]) -> bytes:
    lines = ["|".join(header)]
    lines += ["|".join("" if value is None else str(value) for value in row) for row in rows]
    return ("\n".join(lines) + "\n").encode("utf-8")


def _read(name: str) -> list[dict[str, str]]:
    lines = (BI_QUERIES / "graph" / f"{name}.csv").read_text(encoding="utf-8").splitlines()
    header = lines[0].split("|")
    return [dict(zip(header, line.split("|"), strict=True)) for line in lines[1:] if line]


def _gzip(data: bytes) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as stream:
        stream.write(data)
    return buffer.getvalue()


# --------------------------------------------------------------------------
# SNB BI
# --------------------------------------------------------------------------


def bi_tables() -> dict[str, tuple[list[str], list[list[Any]]]]:
    """The BI query fixture as the real archive's tables (header, rows)."""
    people = _read("Person")
    cities, countries = _read("City"), _read("Country")
    city_country = {r["CityId"]: r["CountryId"] for r in _read("City_isPartOf_Country")}
    forums, posts, comments = _read("Forum"), _read("Post"), _read("Comment")
    tags, tag_classes = _read("Tag"), _read("TagClass")
    created = {r["id"]: r["creationDate"] for r in people}
    created.update({r["id"]: r["creationDate"] for r in forums})
    created.update({r["id"]: r["creationDate"] for r in posts})
    created.update({r["id"]: r["creationDate"] for r in comments})
    first_country = countries[0]["id"]
    tables: dict[str, tuple[list[str], list[list[Any]]]] = {
        "static/Organisation": (
            ["id", "type", "name", "url"],
            [
                [9001, "University", "University_of_Fixture", ""],
                [9002, "Company", "Fixture_Air", ""],
            ],
        ),
        "static/Organisation_isLocatedIn_Place": (
            ["OrganisationId", "PlaceId"],
            [[9001, cities[0]["id"]], [9002, first_country]],
        ),
        "static/Place": (
            ["id", "name", "url", "type"],
            [[r["id"], r["name"], "", "City"] for r in cities]
            + [[r["id"], r["name"], "", "Country"] for r in countries],
        ),
        "static/Place_isPartOf_Place": (
            ["Place1Id", "Place2Id"],
            [[city, country] for city, country in city_country.items()],
        ),
        "static/Tag": (["id", "name", "url"], [[r["id"], r["name"], ""] for r in tags]),
        "static/TagClass": (
            ["id", "name", "url"],
            [[r["id"], r["name"], ""] for r in tag_classes],
        ),
        # No mapped BI read follows IS_SUBCLASS_OF; one edge keeps the table non-empty.
        "static/TagClass_isSubclassOf_TagClass": (
            ["TagClass1Id", "TagClass2Id"],
            [[tag_classes[1]["id"], tag_classes[0]["id"]]],
        ),
        "static/Tag_hasType_TagClass": (
            ["TagId", "TagClassId"],
            [[r["TagId"], r["TagClassId"]] for r in _read("Tag_hasType_TagClass")],
        ),
        "dynamic/Person": (
            [
                "creationDate",
                "id",
                "firstName",
                "lastName",
                "gender",
                "birthday",
                "locationIP",
                "browserUsed",
                "language",
                "email",
            ],
            [
                [r["creationDate"], r["id"], r["firstName"], r["lastName"], "", "", "", "", "", ""]
                for r in people
            ],
        ),
        "dynamic/Forum": (
            ["creationDate", "id", "title"],
            [[r["creationDate"], r["id"], r["title"]] for r in forums],
        ),
        "dynamic/Post": (
            [
                "creationDate",
                "id",
                "imageFile",
                "locationIP",
                "browserUsed",
                "language",
                "content",
                "length",
            ],
            [
                [r["creationDate"], r["id"], "", "", "", r["language"], r["content"], r["length"]]
                for r in posts
            ],
        ),
        "dynamic/Comment": (
            ["creationDate", "id", "locationIP", "browserUsed", "content", "length"],
            [[r["creationDate"], r["id"], "", "", r["content"], r["length"]] for r in comments],
        ),
        # Every forum carries every tag: a read that lost its Post-or-Comment
        # predicate would count these (forums have a creationDate too).
        "dynamic/Forum_hasTag_Tag": (
            ["creationDate", "ForumId", "TagId"],
            [[f["creationDate"], f["id"], t["id"]] for f in forums for t in tags],
        ),
        "dynamic/Comment_isLocatedIn_Country": (
            ["creationDate", "CommentId", "CountryId"],
            [[r["creationDate"], r["id"], first_country] for r in comments],
        ),
        "dynamic/Post_isLocatedIn_Country": (
            ["creationDate", "PostId", "CountryId"],
            [[r["creationDate"], r["id"], first_country] for r in posts],
        ),
        "dynamic/Person_studyAt_University": (
            ["creationDate", "PersonId", "UniversityId", "classYear"],
            [[people[0]["creationDate"], people[0]["id"], 9001, 2005]],
        ),
        "dynamic/Person_workAt_Company": (
            ["creationDate", "PersonId", "CompanyId", "workFrom"],
            [[people[0]["creationDate"], people[0]["id"], 9002, 2008]],
        ),
    }

    def edges(
        target: str, source: str, columns: Sequence[str], keys: Sequence[str], stamp: str
    ) -> None:
        rows = []
        for r in _read(source):
            when = r.get("creationDate") or created[r[stamp]]
            rows.append([when, *(r[key] for key in keys)])
        tables[target] = (["creationDate", *columns], rows)

    edges(
        "dynamic/Comment_hasCreator_Person",
        "Comment_hasCreator_Person",
        ["CommentId", "PersonId"],
        ["CommentId", "PersonId"],
        "CommentId",
    )
    edges(
        "dynamic/Comment_hasTag_Tag",
        "Comment_hasTag_Tag",
        ["CommentId", "TagId"],
        ["CommentId", "TagId"],
        "CommentId",
    )
    edges(
        "dynamic/Comment_replyOf_Comment",
        "Comment_replyOf_Comment",
        ["Comment1Id", "Comment2Id"],
        ["CommentId", "ParentCommentId"],
        "CommentId",
    )
    edges(
        "dynamic/Comment_replyOf_Post",
        "Comment_replyOf_Post",
        ["CommentId", "PostId"],
        ["CommentId", "PostId"],
        "CommentId",
    )
    edges(
        "dynamic/Forum_containerOf_Post",
        "Forum_containerOf_Post",
        ["ForumId", "PostId"],
        ["ForumId", "PostId"],
        "PostId",
    )
    edges(
        "dynamic/Forum_hasMember_Person",
        "Forum_hasMember_Person",
        ["ForumId", "PersonId"],
        ["ForumId", "PersonId"],
        "ForumId",
    )
    edges(
        "dynamic/Forum_hasModerator_Person",
        "Forum_hasModerator_Person",
        ["ForumId", "PersonId"],
        ["ForumId", "PersonId"],
        "ForumId",
    )
    edges(
        "dynamic/Person_hasInterest_Tag",
        "Person_hasInterest_Tag",
        ["personId", "interestId"],
        ["PersonId", "TagId"],
        "PersonId",
    )
    edges(
        "dynamic/Person_isLocatedIn_City",
        "Person_isLocatedIn_City",
        ["PersonId", "CityId"],
        ["PersonId", "CityId"],
        "PersonId",
    )
    edges(
        "dynamic/Person_knows_Person",
        "Person_knows_Person",
        ["Person1Id", "Person2Id"],
        ["Person1Id", "Person2Id"],
        "Person1Id",
    )
    edges(
        "dynamic/Person_likes_Comment",
        "Person_likes_Comment",
        ["PersonId", "CommentId"],
        ["PersonId", "CommentId"],
        "CommentId",
    )
    edges(
        "dynamic/Person_likes_Post",
        "Person_likes_Post",
        ["PersonId", "PostId"],
        ["PersonId", "PostId"],
        "PostId",
    )
    edges(
        "dynamic/Post_hasCreator_Person",
        "Post_hasCreator_Person",
        ["PostId", "PersonId"],
        ["PostId", "PersonId"],
        "PostId",
    )
    edges(
        "dynamic/Post_hasTag_Tag",
        "Post_hasTag_Tag",
        ["PostId", "TagId"],
        ["PostId", "TagId"],
        "PostId",
    )
    return tables


def bi_dataset_files(name: str) -> Files:
    files: Files = {}
    for table, (header, rows) in sorted(bi_tables().items()):
        directory = f"{name}/{BI_SNAPSHOT}/{table}"
        files[f"{directory}/part-00000-fixture.csv.gz"] = _gzip(_csv(header, rows))
        files[f"{directory}/_SUCCESS"] = b""
    return files


LDBC_TYPES = {"datetime": "DATETIME", "string": "STRING", "int64": "INT", "string_list": "STRING[]"}


def _bi_parameters() -> dict[str, dict[str, Any]]:
    document = json.loads((BI_QUERIES / "parameters.json").read_text(encoding="utf-8"))
    return dict(document["queries"])


def _ldbc_text(kind: str, value: Any) -> str:
    return ";".join(value) if kind == "string_list" else str(value)


def bi_parameter_files() -> Files:
    files: Files = {}
    for operation, params in _bi_parameters().items():
        header = [f"{name}:{LDBC_TYPES[spec['kind']]}" for name, spec in params.items()]
        row = [_ldbc_text(spec["kind"], spec["value"]) for spec in params.values()]
        number = operation.removeprefix("BI")
        files[f"fixture-parameters/parameters-sf0/bi-{number}.csv"] = _csv(header, [row])
    return files


def _umbra_value(value: Any) -> Any:
    if isinstance(value, str) and len(value) == 24 and value.endswith("Z") and value[10] == "T":
        return value[:23] + "+00:00"
    return value


def bi_umbra_files(queries: Mapping[str, Any]) -> Files:
    """Umbra-format ``results.csv`` from the independently derived expected rows."""
    columns = {query["operation"]: query["columns"] for query in queries["queries"]}
    lines = []
    for operation, params in _bi_parameters().items():
        expected = json.loads(
            (BI_QUERIES / "expected" / f"{operation}.json").read_text(encoding="utf-8")
        )
        rows = [
            {name: _umbra_value(value) for name, value in zip(columns[operation], row, strict=True)}
            for row in expected["rows"]
        ]
        number = operation.removeprefix("BI")
        umbra_params = {
            name: _ldbc_text(spec["kind"], spec["value"]) for name, spec in params.items()
        }
        lines.append(
            f"{number}|{number}|{json.dumps(umbra_params)}|{json.dumps(rows, ensure_ascii=False)}"
        )
    return {"output/output-sf0/results.csv": ("\n".join(lines) + "\n").encode("utf-8")}


# --------------------------------------------------------------------------
# SNB Interactive v1
# --------------------------------------------------------------------------


def _identity_tables(mapping: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    """Node label (stored or identity) to its mapping node table."""
    tables: dict[str, Mapping[str, Any]] = {}
    for table in mapping["node_tables"]:
        tables[table["label"]] = table
        for label in (table.get("label_values") or {}).values():
            tables[label] = table
    return tables


def interactive_dataset_files(name: str) -> Files:
    mapping = json.loads(INTERACTIVE_MAPPING.read_text(encoding="utf-8"))
    document = interactive_reference.build_fixture()
    by_label = _identity_tables(mapping)
    identity = {}
    rows: dict[str, list[list[Any]]] = {table["id"]: [] for table in mapping["node_tables"]}
    rows.update({table["id"]: [] for table in mapping["edge_tables"]})
    headers: dict[str, list[str]] = {}
    for table in mapping["node_tables"]:
        columns = [table["id_column"]]
        if table.get("label_column"):
            columns.append(table["label_column"])
        columns += [p["column"] for p in table["properties"] if p["column"] not in columns]
        headers[table["id"]] = columns
    for key, label, properties in document["nodes"]:
        table = by_label[label]
        identity[key] = table["label"]
        stored = {table["id_column"]: properties["id"]}
        if table.get("label_column"):
            inverse = {value: raw for raw, value in table["label_values"].items()}
            stored[table["label_column"]] = inverse[label]
        for prop in table["properties"]:
            value = properties.get(prop.get("name", prop["column"]))
            if isinstance(value, list):
                value = prop.get("separator", ";").join(value)
            stored.setdefault(prop["column"], value)
        rows[table["id"]].append([stored.get(column) for column in headers[table["id"]]])
    edge_tables = {
        (t["source"]["label"], t["rel_type"], t["target"]["label"]): t
        for t in mapping["edge_tables"]
    }
    for table in mapping["edge_tables"]:
        headers[table["id"]] = [
            table["source"]["column"],
            table["target"]["column"],
            *(p["column"] for p in table.get("properties", [])),
        ]
    for source, rel_type, destination, properties in document["edges"]:
        table = edge_tables[(identity[source], rel_type, identity[destination])]
        values = [int(source.split(":")[1]), int(destination.split(":")[1])]
        values += [properties.get(p.get("name", p["column"])) for p in table.get("properties", [])]
        rows[table["id"]].append(values)
    files: Files = {}
    for table in mapping["node_tables"] + mapping["edge_tables"]:
        (pattern,) = table["files"]
        relative = pattern.removeprefix("*/")
        # The archive's header repeats a name (Person.id|Person.id); the
        # converter suffixes the repeat with .1.
        header = [column.removesuffix(".1") for column in headers[table["id"]]]
        files[f"{name}/{relative}"] = _csv(header, rows[table["id"]])
    return files


def interactive_parameter_files() -> Files:
    files: Files = {}
    for operation, bindings in interactive_reference.PARAMETERS.items():
        if not operation.startswith("IC"):
            continue
        rows = []
        header: list[str] = []
        for params in bindings:
            values = dict(params)
            if "endDate" in values:
                start = values["startDate"]
                days = round((values.pop("endDate") - start) / DAY_MS)
                values["durationDays"] = days
            header = list(values)
            rows.append(list(values.values()))
        number = operation.removeprefix("IC")
        files[f"substitution_parameters-sf0/interactive_{number}_param.txt"] = _csv(header, rows)
    return files


SHORT_KEYS = {
    "IS1": "personIdSQ1",
    "IS2": "personIdSQ2",
    "IS3": "personIdSQ3",
    "IS4": "messageIdContent",
    "IS5": "messageIdCreator",
    "IS6": "messageForumId",
    "IS7": "messageRepliesId",
}
# A message id the stream's own IU6 creates; the short read naming it must be skipped.
CREATED_POST = 999_001


def interactive_validation_files() -> Files:
    """A Neo4j-format validation stream: an update first, as in every pinned stream."""
    lines = [
        json.dumps({"forumId": 1, "personId": 2000, "joinDate": 1_347_528_962_967}) + '|"-1"',
        json.dumps(
            {
                "postId": CREATED_POST,
                "imageFile": "",
                "creationDate": 1_347_528_963_000,
                "locationIp": "1.2.3.4",
                "browserUsed": "Firefox",
                "language": "en",
                "content": "new",
                "length": 3,
                "authorPersonId": 2000,
                "forumId": 1,
                "countryId": 9010,
                "tagIds": [],
            }
        )
        + '|"-1"',
        json.dumps({SHORT_KEYS["IS4"]: CREATED_POST}) + "|{}",
    ]
    for operation, bindings in interactive_reference.PARAMETERS.items():
        if operation in SHORT_KEYS:
            for params in bindings:
                (value,) = params.values()
                lines.append(json.dumps({SHORT_KEYS[operation]: value}) + "|{}")
        elif operation == "IC1":
            lines.append(json.dumps({"personIdQ1": 2048, "firstName": "Jose", "limit": 20}) + "|[]")
    return {"validation_params-sf0.csv": ("\n".join(lines) + "\n").encode("utf-8")}


# --------------------------------------------------------------------------
# Deliberately wrong query texts (the fixture's failing rung)
# --------------------------------------------------------------------------

# Each mutation makes GraphForge return a wrong answer on the fixture, so the
# rung that runs it must fail its reference check, naming exactly this query.
BI_MUTATION = ("BI17", "  AND person2 <> person3\n", "")
INTERACTIVE_MUTATION = (
    "IC2",
    "message.creationDate <= $maxDate",
    "message.creationDate < $maxDate",
)


def _mutated(
    queries: Mapping[str, Any], mutation: tuple[str, str, str], field: str
) -> dict[str, Any]:
    operation, old, new = mutation
    document = json.loads(json.dumps(queries))
    (query,) = [query for query in document["queries"] if query["operation"] == operation]
    if query[field].count(old) != 1:
        raise ValueError(f"{operation}: the mutation no longer applies")
    query[field] = query[field].replace(old, new)
    return document


def mutated_bi_queries(queries: Mapping[str, Any]) -> dict[str, Any]:
    return _mutated(queries, BI_MUTATION, "cypher")


def mutated_interactive_queries(queries: Mapping[str, Any]) -> dict[str, Any]:
    return _mutated(queries, INTERACTIVE_MUTATION, "cypher")


# --------------------------------------------------------------------------
# Archives and counts
# --------------------------------------------------------------------------


def _tar(files: Files, destination: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="gdc-snb-fixture-") as scratch:
        root = Path(scratch)
        for name, data in files.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        top = sorted({name.split("/", 1)[0] for name in files})
        subprocess.run(
            [
                "tar",
                "--zstd",
                "--sort=name",
                "--mtime=@0",
                "--owner=0",
                "--group=0",
                "--numeric-owner",
                "--mode=u+rw,go+r-w",
                "-cf",
                str(destination),
                "-C",
                str(root),
                *top,
            ],
            check=True,
        )


def _members(archive: Path) -> Files:
    with tempfile.TemporaryDirectory(prefix="gdc-snb-fixture-") as scratch:
        subprocess.run(["tar", "--zstd", "-xf", str(archive), "-C", scratch], check=True)
        root = Path(scratch)
        return {
            path.relative_to(root).as_posix(): path.read_bytes()
            for path in sorted(root.rglob("*"))
            if path.is_file()
        }


def _decoded(files: Files) -> Files:
    """Member contents with gzip parts decompressed (gzip headers are not compared)."""
    return {
        name: gzip.decompress(data) if name.endswith(".gz") else data
        for name, data in files.items()
    }


def archives(queries_bi: Mapping[str, Any]) -> dict[str, Files]:
    return {
        "snb-bi-fixture-sf0.tar.zst": bi_dataset_files("bi-fixture-sf0"),
        "snb-bi-fixture-parameters.tar.zst": bi_parameter_files(),
        "snb-bi-fixture-umbra.tar.zst": bi_umbra_files(queries_bi),
        "snb-interactive-fixture-sf0.tar.zst": interactive_dataset_files(
            "social_network-fixture-sf0"
        ),
        "snb-interactive-fixture-substitution.tar.zst": interactive_parameter_files(),
        "snb-interactive-fixture-validation.tar.zst": interactive_validation_files(),
    }


def table_counts(files: Files, mapping: Mapping[str, Any]) -> dict[str, int]:
    """Rows per mapping table in a dataset archive's members."""
    counts = {}
    decoded = _decoded(files)
    for table in mapping["node_tables"] + mapping["edge_tables"]:
        (pattern,) = table["files"]
        suffix = pattern.split("*/", 1)[1].replace("*.csv.gz", "")
        total = 0
        for name, data in decoded.items():
            relative = name.split("/", 1)[1]
            if relative == suffix or (
                suffix.endswith("/") and relative.startswith(suffix) and name.endswith(".csv.gz")
            ):
                total += max(0, len(data.decode("utf-8").splitlines()) - 1)
        counts[table["id"]] = total
    return counts


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args(argv)
    queries_bi = json.loads(
        (ROOT / "profiles" / "gdc" / "snb-bi-scorecard-queries.json").read_text(encoding="utf-8")
    )
    built = archives(queries_bi)
    directory = FIXTURE / "archives"
    stale = []
    for name, files in built.items():
        path = directory / name
        if args.write:
            directory.mkdir(parents=True, exist_ok=True)
            _tar(files, path)
        elif not path.is_file() or _decoded(_members(path)) != _decoded(files):
            stale.append(name)
    if stale:
        print(f"stale fixture archives: {', '.join(stale)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
