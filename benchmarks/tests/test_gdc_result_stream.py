"""The GDC correctness check reads each written result row by row (#1914).

A Graphalytics result has one row per vertex, millions at the larger rungs, and the
check used to load every result file whole, twice, and build two dictionaries of
dictionaries to pair rows. These tests hold the streaming check to the answers of
the old one: the same digest, the same verdict under each matching rule, the same
check document, with a bound on how many rows are alive at once.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import random
import tempfile
import tracemalloc
from typing import Any
import unittest

from graphforge_bench.gdc_result_stream import QUERY_RESULT_SCHEMA, StreamedResult
from graphforge_bench.gdc_rung_inputs import (
    IndexCache,
    ResultDigest,
    RungInputError,
    _match_exact,
    check_reference,
    matches,
    read_results,
    result_digest,
    within_epsilon,
)

COLUMNS = [{"name": "id", "type": "Utf8"}, {"name": "score", "type": "Float64"}]


def result_document(rows: list[list[Any]], **overrides: Any) -> dict[str, Any]:
    document: dict[str, Any] = {
        "schema": QUERY_RESULT_SCHEMA,
        "query_id": "q",
        "binding_id": "b",
        "ordered": False,
        "result_sha256": "0" * 64,
        "columns": COLUMNS,
        "rows": rows,
    }
    document.update(overrides)
    return document


class ScratchTest(unittest.TestCase):
    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="gdc-result-stream-")
        self.addCleanup(scratch.cleanup)
        self.scratch = Path(scratch.name)

    def write(
        self, document: dict[str, Any] | str, name: str = "result.json", **dumps: Any
    ) -> Path:
        path = self.scratch / name
        path.write_text(
            document if isinstance(document, str) else json.dumps(document, **dumps),
            encoding="utf-8",
        )
        return path


TRICKY_ROWS: list[list[Any]] = [
    ["1", "0.5"],
    ["2", None],
    ["a],[b", 'x"y\\z'],
    ["ünï", "☃ \U0001f600"],
    ["[{c: 1, d: x}, b]", "]"],
    [],
    ["", ""],
    ["12345678901234567890", "-1e-7"],
]


class StreamedResultTests(ScratchTest):
    def test_the_rows_are_those_of_the_whole_document_at_every_chunk_size(self) -> None:
        rows = [*TRICKY_ROWS, *([str(n), f"{n}.25"] for n in range(40))]
        document = result_document(rows)
        for layout in ({"sort_keys": True, "separators": (",", ":")}, {"indent": 2}, {}):
            path = self.write(document, **layout)
            for chunk in (1, 2, 3, 5, 8, 13, 64, 1 << 20):
                streamed = StreamedResult.open(path, chunk=chunk)
                self.assertEqual(list(streamed["rows"]), rows, (layout, chunk))
                self.assertEqual(streamed["columns"], COLUMNS)
                self.assertIs(streamed["ordered"], False)
                self.assertEqual((streamed["query_id"], streamed["binding_id"]), ("q", "b"))
                # Every access starts a new pass over the file.
                self.assertEqual(list(streamed["rows"]), rows)

    def test_a_result_with_no_rows_is_empty(self) -> None:
        path = self.write(result_document([]), sort_keys=True)
        self.assertEqual(list(StreamedResult.open(path, chunk=4)["rows"]), [])

    def test_the_schema_may_follow_the_rows_and_is_checked_after_them(self) -> None:
        # serde_json writes keys sorted, so the driver's `schema` comes after `rows`.
        good = self.write(result_document([["1", "2"]]), "good.json", sort_keys=True)
        self.assertLess(good.read_text().index('"rows"'), good.read_text().index('"schema"'))
        self.assertEqual(list(StreamedResult.open(good)["rows"]), [["1", "2"]])
        wrong = self.write(
            result_document([["1", "2"]], schema="other/1"), "wrong.json", sort_keys=True
        )
        streamed = StreamedResult.open(wrong)
        with self.assertRaisesRegex(RungInputError, "is not a query result"):
            list(streamed["rows"])
        document = result_document([["1", "2"]])
        del document["schema"]
        missing = StreamedResult.open(self.write(document, "missing.json"))
        with self.assertRaisesRegex(RungInputError, "is not a query result"):
            list(missing["rows"])

    def test_a_schema_before_the_rows_is_refused_at_once_when_wrong(self) -> None:
        path = self.write(result_document([["1", "2"]], schema="other/1"))
        with self.assertRaisesRegex(RungInputError, "is not a query result"):
            StreamedResult.open(path)

    def test_rows_before_the_members_the_check_needs_are_refused_not_loaded_whole(self) -> None:
        document = result_document([["1", "2"]])
        reordered = {"rows": document["rows"], **{k: v for k, v in document.items() if k != "rows"}}
        with self.assertRaisesRegex(RungInputError, "lists its rows before"):
            StreamedResult.open(self.write(reordered))

    def test_malformed_files_are_typed_not_crashes(self) -> None:
        head = '{"binding_id":"b","columns":[],"ordered":false,"query_id":"q","result_sha256":"x",'
        cases = {
            "truncated in a row": head + '"rows":[["1","2"],["3"',
            "truncated before the end": head + '"rows":[["1","2"]',
            "trailing text": head + '"rows":[],"schema":"' + QUERY_RESULT_SCHEMA + '"} x',
            "a row that is not an array": head
            + '"rows":[1],"schema":"'
            + QUERY_RESULT_SCHEMA
            + '"}',
            "a missing comma": head + '"rows":[["1"]["2"]],"schema":"' + QUERY_RESULT_SCHEMA + '"}',
            "a trailing comma": head + '"rows":[["1"],],"schema":"' + QUERY_RESULT_SCHEMA + '"}',
            "a repeated member": head + '"query_id":"q","rows":[]}',
            "no rows": head.rstrip(",") + "}",
            "not an object": "[]",
            "empty": "",
        }
        for name, text in cases.items():
            path = self.write(text, "bad.json")
            with self.subTest(name), self.assertRaises(RungInputError):
                list(StreamedResult.open(path, chunk=7)["rows"])

    def test_the_file_is_read_a_chunk_at_a_time_not_whole(self) -> None:
        rows = [[str(n), f"{n}.5"] for n in range(120_000)]
        path = self.write(result_document(rows), sort_keys=True, separators=(",", ":"))
        self.assertGreater(path.stat().st_size, 2_000_000)
        streamed = StreamedResult.open(path, chunk=1 << 16)
        tracemalloc.start()
        try:
            count = sum(1 for _ in streamed["rows"])
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(count, len(rows))
        # Holding the rows would cost tens of MB; one chunk and one row cost well under 1.
        self.assertLess(peak, 1 << 20)


def legacy_digest(columns: list[dict[str, str]], rows: list[list[Any]], ordered: bool) -> str:
    """The digest as the check computed it before streaming: every row encoded, then sorted."""

    def cell(text: str) -> bytes:
        encoded = text.encode("utf-8")
        return b"V" + str(len(encoded)).encode("ascii") + b":" + encoded

    header = cell("graphforge-gdc-result-digest/1") + cell("ordered" if ordered else "unordered")
    for column in columns:
        header += cell(column["name"]) + cell(column["type"])
    encoded = [b"".join(b"N" if v is None else cell(v) for v in row) + b"\n" for row in rows]
    if not ordered:
        encoded.sort()
    digest = hashlib.sha256(header + b"\n")
    for row in encoded:
        digest.update(row)
    return digest.hexdigest()


class DigestTests(unittest.TestCase):
    def test_the_streamed_digest_is_the_defined_digest(self) -> None:
        rng = random.Random(1914)
        alphabet = ["0", "1", "x", "", "a b", "é", "☃", "\U0001f600", "N", "V1:z", "]", None]
        for ordered in (False, True):
            for _ in range(200):
                rows = [
                    [rng.choice(alphabet) for _ in range(rng.randint(0, 3))]
                    for _ in range(rng.randint(0, 12))
                ]
                expected = legacy_digest(COLUMNS, rows, ordered)
                self.assertEqual(result_digest(COLUMNS, rows, ordered), expected)
                self.assertEqual(result_digest(COLUMNS, iter(rows), ordered), expected)

    def test_a_row_of_ascii_and_a_row_of_unicode_encode_by_bytes_not_characters(self) -> None:
        self.assertEqual(
            result_digest(COLUMNS, [["é", "a"]], True), legacy_digest(COLUMNS, [["é", "a"]], True)
        )
        self.assertNotEqual(
            result_digest(COLUMNS, [["é", "a"]], True), result_digest(COLUMNS, [["e", "a"]], True)
        )

    def test_the_tap_adds_each_row_as_it_passes_and_yields_it_unchanged(self) -> None:
        rows = [["1", "2"], ["3", None]]
        digest = ResultDigest(COLUMNS, False)
        self.assertEqual(list(digest.tap(iter(rows))), rows)
        self.assertEqual(digest.hexdigest(), legacy_digest(COLUMNS, rows, False))


# The keyed rules as they were when every result and reference was built into dictionaries.
def _legacy_keyed(columns: Any, rows: Any, key: Any) -> Any:
    keyed: dict[Any, Any] = {}
    for row in rows:
        if len(row) != len(columns):
            return None
        cells = dict(zip(columns, row))
        identity = tuple(cells[name] for name in key)
        if identity in keyed:
            return None
        keyed[identity] = cells
    return keyed


def _legacy_number(text: Any) -> float | None:
    if not isinstance(text, str):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def legacy_keyed_matches(rule: Any, result: Any, reference: Any) -> bool:
    names = [column["name"] for column in result["columns"]]
    wanted = list(reference["columns"])
    actual, expected = list(result["rows"]), list(reference["rows"])
    ordered = bool(result["ordered"])
    key = list(rule["key"])
    if not set(wanted) <= set(names) or not set(key) <= set(wanted):
        return False
    left, right = _legacy_keyed(names, actual, key), _legacy_keyed(wanted, expected, key)
    if left is None or right is None or left.keys() != right.keys():
        return False
    values = [name for name in wanted if name not in key]
    if rule["matching"] == "exact":
        return all(
            cells[name] == right[identity][name]
            for identity, cells in left.items()
            for name in values
        )
    if rule["matching"] == "epsilon":
        if ordered and list(left) != list(right):
            return False
        for identity, cells in left.items():
            for name in values:
                value, wanted_value = cells[name], right[identity][name]
                number, reference_number = _legacy_number(value), _legacy_number(wanted_value)
                if number is None or reference_number is None:
                    if value != wanted_value:
                        return False
                elif not within_epsilon(number, reference_number, float(rule["epsilon"])):
                    return False
        return True
    label = str(rule["label"])
    if set(wanted) != {*key, label}:
        return False
    forward: dict[Any, Any] = {}
    backward: dict[Any, Any] = {}
    for identity, cells in left.items():
        mine, theirs = cells[label], right[identity][label]
        if forward.setdefault(mine, theirs) != theirs:
            return False
        if backward.setdefault(theirs, mine) != mine:
            return False
    return True


class Row(list):  # type: ignore[type-arg]
    """A row that counts how many of its kind are alive."""

    alive = 0
    peak = 0

    def __init__(self, cells: list[Any]) -> None:
        super().__init__(cells)
        Row.alive += 1
        Row.peak = max(Row.peak, Row.alive)

    def __del__(self) -> None:
        Row.alive -= 1


class KeyedMatchingTests(unittest.TestCase):
    def random_case(self, rng: random.Random) -> tuple[Any, dict[str, Any], dict[str, Any]]:
        kind = rng.choice(["exact", "epsilon", "equivalence"])
        key_width = rng.choice([1, 1, 2])
        keys = [f"k{n}" for n in range(key_width)]
        label = "v0"
        values = [label] if kind == "equivalence" else [f"v{n}" for n in range(rng.randint(1, 2))]
        wanted = [*keys, *values]
        count = rng.randint(0, 9)
        numbers = ["0.5", "1", "2.00001", "1e-9", "inf", "-0.0", "nan", "3", "text", None]
        reference_rows: list[list[Any]] = []
        for n in range(count):
            reference_rows.append(
                [
                    *(f"{n}.{part}" for part in range(key_width)),
                    *(rng.choice(numbers) for _ in values),
                ]
            )
        rule: dict[str, Any] = {"matching": kind, "key": keys}
        if kind == "epsilon":
            rule["epsilon"] = 1e-4
        if kind == "equivalence":
            rule["label"] = label
            for n, row in enumerate(reference_rows):
                row[-1] = str(n // 2)
        names = list(wanted)
        rows = [list(row) for row in reference_rows]
        for _ in range(rng.choice([0, 0, 1, 2])):
            action = rng.choice(
                ["drop", "extra", "dup", "nudge", "far", "relabel", "width", "none_key", "swapcol"]
            )
            if action == "drop" and rows:
                rows.pop(rng.randrange(len(rows)))
            elif action == "extra":
                rows.append(
                    [f"x{rng.randint(0, 3)}"] * key_width + [rng.choice(numbers)] * len(values)
                )
            elif action == "dup" and rows:
                rows.append(list(rng.choice(rows)))
            elif action == "nudge" and rows:
                row = rng.choice(rows)
                if row[-1] and _legacy_number(row[-1]) is not None:
                    row[-1] = repr(float(row[-1]) * (1 + 5e-5))
            elif action == "far" and rows:
                rng.choice(rows)[-1] = rng.choice(numbers)
            elif action == "relabel":
                for row in rows:
                    row[-1] = "L" + str(row[-1])
            elif action == "width" and rows:
                rng.choice(rows).append("extra")
            elif action == "none_key" and rows:
                rng.choice(rows)[0] = None
            elif action == "swapcol" and len(names) > 1:
                names.reverse()
                rows = [list(reversed(row)) for row in rows]
        if rng.random() < 0.3:
            names.append("extra")
            rows = [[*row, "e"] if len(row) == len(names) - 1 else row for row in rows]
        if rng.random() < 0.5:
            rng.shuffle(rows)
        result = {
            "columns": [{"name": name, "type": "Utf8"} for name in names],
            "ordered": rng.random() < 0.5,
            "rows": rows,
        }
        return rule, result, {"columns": wanted, "rows": reference_rows}

    def test_every_keyed_rule_gives_the_answer_it_gave_before_streaming(self) -> None:
        rng = random.Random(20261009)
        verdicts = {True: 0, False: 0}
        for case in range(6000):
            rule, result, reference = self.random_case(rng)
            expected = legacy_keyed_matches(rule, result, reference)
            streamed = {**result, "rows": iter(result["rows"])}
            self.assertEqual(
                matches(rule, result, reference), expected, (case, rule, result, reference)
            )
            self.assertEqual(matches(rule, streamed, reference), expected, (case, rule))
            verdicts[expected] += 1
        self.assertGreater(verdicts[True], 500)
        self.assertGreater(verdicts[False], 500)

    def test_a_shared_index_serves_the_bindings_of_one_query_and_only_those(self) -> None:
        rule = {"matching": "exact", "key": ["id"]}
        reference = {"columns": ["id", "v"], "rows": [["1", "a"], ["2", "b"]]}
        other = {"columns": ["id", "v"], "rows": [["1", "a"], ["2", "z"]]}
        result = {
            "columns": [{"name": "id", "type": "Utf8"}, {"name": "v", "type": "Utf8"}],
            "ordered": False,
            "rows": [["2", "b"], ["1", "a"]],
        }
        cache = IndexCache()
        for _ in range(3):
            self.assertTrue(matches(rule, result, reference, cache=cache))
        # The index is the reference's: another reference is not served from it.
        self.assertFalse(matches(rule, result, other, cache=cache))
        self.assertTrue(matches(rule, result, reference, cache=cache))
        # The same rows under another key are not served from the first key's index.
        by_value = {"matching": "exact", "key": ["v"]}
        self.assertTrue(matches(by_value, result, reference, cache=cache))
        reference["rows"][1][1] = "changed"
        self.assertFalse(matches(by_value, result, reference, cache=IndexCache()))

    def test_a_result_is_never_held_whole(self) -> None:
        count = 3000
        reference_rows = [[str(n), str(n % 7)] for n in range(count)]
        for rule in (
            {"matching": "exact", "key": ["id"]},
            {"matching": "epsilon", "epsilon": 1e-4, "key": ["id"]},
            {"matching": "equivalence", "key": ["id"], "label": "v"},
        ):
            Row.alive = Row.peak = 0

            def rows() -> Any:
                for cells in reference_rows:
                    yield Row(list(cells))

            result = {
                "columns": [{"name": "id", "type": "Utf8"}, {"name": "v", "type": "Utf8"}],
                "ordered": False,
                "rows": rows(),
            }
            self.assertTrue(matches(rule, result, {"columns": ["id", "v"], "rows": reference_rows}))
            self.assertLess(Row.peak, 8, rule)


class UnkeyedMatchingTests(unittest.TestCase):
    def test_unkeyed_rules_still_take_one_shot_rows(self) -> None:
        result = {
            "columns": [{"name": "a", "type": "Utf8"}],
            "ordered": False,
            "rows": iter([["2"], ["1"]]),
        }
        self.assertTrue(
            matches({"matching": "exact"}, result, {"columns": ["a"], "rows": [["1"], ["2"]]})
        )
        self.assertTrue(_match_exact([["1"]], [["1"]], True))


class CheckFromFilesTests(ScratchTest):
    def evidence(self, digest: str) -> dict[str, Any]:
        return {
            "variants": [
                {
                    "query_id": "q",
                    "samples": [{"binding_id": "b", "status": "measured", "result_sha256": digest}],
                }
            ]
        }

    def reference(self, rows: list[list[Any]]) -> dict[str, Any]:
        rule = {
            "matching": "exact",
            "key": ["id"],
            "bindings": {"b": {"columns": ["id", "score"], "rows": rows}},
        }
        return {"source": "test", "queries": {"q": rule}}

    def test_files_and_in_memory_results_give_the_same_check(self) -> None:
        rows = [[str(n), f"{n}.5"] for n in range(500)]
        digest = result_digest(COLUMNS, rows, False)
        directory = self.scratch / "results"
        directory.mkdir()
        self.write_result(directory / "00000000.json", rows, digest)
        from_files = check_reference(
            reference=self.reference(rows),
            reference_sha256="r",
            evidence=self.evidence(digest),
            results=read_results(directory),
        )
        in_memory = check_reference(
            reference=self.reference(rows),
            reference_sha256="r",
            evidence=self.evidence(digest),
            results={("q", "b"): result_document(rows, result_sha256=digest)},
        )
        self.assertEqual(from_files, in_memory)
        self.assertEqual(
            (from_files["status"], from_files["checked"], from_files["matched"]), ("passed", 1, 1)
        )

    def write_result(self, path: Path, rows: list[list[Any]], digest: str) -> None:
        path.write_text(
            json.dumps(
                result_document(rows, result_sha256=digest), sort_keys=True, separators=(",", ":")
            ),
            encoding="utf-8",
        )

    def test_a_wrong_answer_is_both_a_mismatch_and_a_digest_check_over_every_row(self) -> None:
        rows = [[str(n), f"{n}.5"] for n in range(50)]
        wrong = [list(row) for row in rows]
        wrong[40][1] = "99.5"  # the match fails at row 40; the digest must still cover all 50
        digest = result_digest(COLUMNS, wrong, False)
        directory = self.scratch / "results"
        directory.mkdir()
        self.write_result(directory / "00000000.json", wrong, digest)
        checked = check_reference(
            reference=self.reference(rows),
            reference_sha256="r",
            evidence=self.evidence(digest),
            results=read_results(directory),
        )
        self.assertEqual([m["cause"] for m in checked["mismatches"]], ["reference_mismatch"])
        # The same file claiming the digest of different rows is caught after the match failed.
        self.write_result(directory / "00000000.json", wrong, result_digest(COLUMNS, rows, False))
        checked = check_reference(
            reference=self.reference(rows),
            reference_sha256="r",
            evidence=self.evidence(result_digest(COLUMNS, rows, False)),
            results=read_results(directory),
        )
        self.assertEqual(
            sorted(m["cause"] for m in checked["mismatches"]),
            ["reference_mismatch", "result_digest_mismatch"],
        )

    def test_a_result_listing_its_last_row_wrongly_still_fails_the_digest(self) -> None:
        rows = [[str(n), f"{n}.5"] for n in range(30)]
        digest = result_digest(COLUMNS, rows, False)
        tampered = [list(row) for row in rows]
        tampered[-1][1] = "tampered"
        directory = self.scratch / "results"
        directory.mkdir()
        self.write_result(directory / "00000000.json", tampered, digest)
        checked = check_reference(
            reference=self.reference(rows),
            reference_sha256="r",
            evidence=self.evidence(digest),
            results=read_results(directory),
        )
        self.assertIn("result_digest_mismatch", [m["cause"] for m in checked["mismatches"]])

    def test_results_written_twice_are_refused(self) -> None:
        directory = self.scratch / "twice"
        directory.mkdir()
        for name in ("00000000.json", "00000001.json"):
            self.write_result(directory / name, [["1", "2"]], "0" * 64)
        with self.assertRaisesRegex(RungInputError, "written twice"):
            read_results(directory)


if __name__ == "__main__":
    unittest.main()
