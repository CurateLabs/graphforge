"""SNB BI and Interactive v1 scorecards through the rung runner (#1904).

The committed fixture (``fixtures/gdc/snb-scorecard``, built by
``graphforge_bench.gdc_snb_scorecard_fixture``) puts the two query fixtures in
the shapes of the real pinned LDBC archives. Each suite's fixture ladder has
three rungs: sf0 passes against its reference, sf1 is declared but not pinned
and is passed over, and sf2 runs one deliberately wrong query text, so its
reference check must fail naming exactly that query. The real converter,
``gf``, query driver, load mappings and workload and reference builders run;
only BenchExec is replaced (``FakeBenchExec``, as in ``test_gdc_rung``).
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
from typing import Any
import unittest
from unittest.mock import patch

from graphforge_bench import gdc_rung, gdc_snb_bi, gdc_snb_interactive
from graphforge_bench import gdc_snb_interactive_reference as interactive_reference
from graphforge_bench import gdc_snb_scorecard as scorecard
from graphforge_bench import gdc_snb_scorecard_fixture as fixture
from graphforge_bench.gdc_contracts import (
    load_suite_declaration,
    resolve_pinned_identity,
    workspace_root,
)
from graphforge_bench.gdc_rung_inputs import (
    RungInputError,
    list_elements,
    load_ladder_spec,
    matches,
)
from graphforge_bench.gdc_scorecard_card import render_card, write_card

from tests.test_gdc_rung import QUIET, FakeBenchExec
from tests.test_gdc_scorecard_load import converter_binary, gf_binary

ROOT = workspace_root()
FIXTURE = ROOT / "fixtures" / "gdc" / "snb-scorecard"
PROFILES = ROOT / "profiles" / "gdc"
COMMIT = "f013587f0123456789abcdef0123456789abcdef"


def read(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def serve_fixture_archives(url: str) -> contextlib.AbstractContextManager[io.BytesIO]:
    """The dataset cache's opener, serving the committed archives with no network."""
    name = url.rsplit("/", 1)[1]
    return contextlib.closing(io.BytesIO((FIXTURE / "archives" / name).read_bytes()))


def card_lines(text: str) -> dict[str, str]:
    lines = text.splitlines()
    return {line.split(":", 1)[0]: line.split(":", 1)[1].strip() for line in lines[2:12]}


class LadderCase(unittest.TestCase):
    ladder_file = ""
    suite = ""

    @classmethod
    def setUpClass(cls) -> None:
        cls.executables = gdc_rung.GdcExecutables(
            gf=gf_binary(), driver=converter_binary(), benchexec_python=Path(sys.executable)
        )

    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="gdc-snb-scorecard-")
        self.addCleanup(scratch.cleanup)
        self.scratch = Path(scratch.name)
        self.work = self.scratch / "work"
        self.work.mkdir()
        self.output = self.scratch / "evidence"
        swap = patch.object(
            gdc_rung, "_host_swap_counters", return_value={"pswpin": 0, "pswpout": 0}
        )
        swap.start()
        self.addCleanup(swap.stop)

    def climb(self) -> tuple[gdc_rung.Ladder, list[dict[str, Any]], FakeBenchExec]:
        benchexec = FakeBenchExec()
        ladder = gdc_rung.prepare(
            root=ROOT,
            ladder_path=FIXTURE / self.ladder_file,
            output_dir=self.output,
            work_root=self.work,
            cache_root=self.scratch / "cache",
            executables=self.executables,
            commit=COMMIT,
            host_label="CI-RUNNER",
            storage_medium="SSD",
            opener=serve_fixture_archives,
            benchexec=benchexec,
        )
        results = gdc_rung.climb(
            ladder,
            reserved_headroom_bytes=0,
            quiet_host_wait_seconds=0,
            quiet_host=lambda _wait: QUIET,
        )
        return ladder, results, benchexec

    def document(self, name: str) -> Any:
        return read(self.output / name)

    def assert_ladder(
        self, *, checked: int, wrong_query: str, coverage: str
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """The shared shape: sf0 passes, sf1 is passed over, sf2 fails on one query."""
        ladder, results, benchexec = self.climb()
        prefix = self.suite
        self.assertEqual(
            [(r["rung_id"], r["status"]) for r in results],
            [("sf0", "passed"), ("sf1", "not_admitted"), ("sf2", "failed")],
        )
        self.assertTrue(gdc_rung.ladder_finished(ladder.spec, results))
        # The unpinned rung launches nothing: three BenchExec phases per run rung.
        self.assertEqual(len(benchexec.stages), 6)
        self.assertEqual(
            results[1]["failures"],
            [
                {
                    "phase": "admission",
                    "cause": "rung_not_pinned",
                    "detail": ladder.spec.rung("sf1")["not_pinned"],
                }
            ],
        )
        for result in results:
            self.assertEqual(result["inventory"], {"empty": True, "entries": []})
        passed = results[0]
        # The built workload, reference and notes are rung documents with digests.
        for name in ("workload", "reference", "notes"):
            document = f"{prefix}-sf0-inputs-{name}.json"
            self.assertIn(document, passed["documents"])
            digest = hashlib.sha256((self.output / document).read_bytes()).hexdigest()
            self.assertEqual(passed["documents"][document], digest)
        correctness = self.document(f"{prefix}-sf0-correctness.json")
        self.assertEqual(
            (correctness["status"], correctness["checked"], correctness["matched"]),
            ("passed", checked, checked),
        )
        self.assertEqual(correctness["unchecked"], 0)
        # sf2 runs one wrong query text; the check names exactly that query.
        failed = results[2]
        self.assertEqual({f["phase"] for f in failed["failures"]}, {"check"})
        self.assertEqual({f["cause"] for f in failed["failures"]}, {"reference_mismatch"})
        self.assertEqual({f["detail"].split("/")[0] for f in failed["failures"]}, {wrong_query})
        wrong = self.document(f"{prefix}-sf2-correctness.json")
        self.assertEqual(wrong["checked"] - wrong["matched"], len(failed["failures"]))

        paths = write_card(ROOT, ladder.spec, self.output)
        text = paths["text"].read_text(encoding="utf-8")
        card = read(paths["json"])
        self.assertEqual(render_card(card), text)
        lines = card_lines(text)
        self.assertEqual(lines["Coverage"].split(";")[0], coverage)
        self.assertEqual(lines["Next rung"], "fixture SF2 — typed failure (reference_mismatch)")
        self.assertIn("  - scope (fixture SF1 not pinned): Fixture:", text)
        self.assertEqual(card["latency"]["samples"], checked)
        return passed, card


class BiLadderTests(LadderCase):
    ladder_file = "bi-ladder-spec.json"
    suite = "snb-bi"

    def test_bi_ladder_checks_the_converted_reference_and_catches_a_wrong_answer(self) -> None:
        _passed, card = self.assert_ladder(checked=17, wrong_query="BI17", coverage="17/39 queries")
        self.assertEqual(
            card["correctness"]["reference_source"],
            "Umbra-format results derived by graphforge_bench.gdc_snb_bi_reference",
        )
        reference = self.document("snb-bi-sf0-inputs-reference.json")
        # Float columns match with an epsilon, keyed by the other columns.
        self.assertEqual(reference["queries"]["BI1"]["matching"], "epsilon")
        self.assertEqual(
            reference["queries"]["BI13"]["key"], ["zombieId", "zombieLikeCount", "totalLikeCount"]
        )
        self.assertEqual(reference["queries"]["BI3"]["matching"], "exact")
        # Umbra's datetimes become the driver's datetime struct text.
        self.assertEqual(
            reference["queries"]["BI3"]["bindings"]["p000"]["rows"][0][2],
            "{date: 15404, time: 21:42:45, offset: 0, zone: }",
        )


class InteractiveLadderTests(LadderCase):
    ladder_file = "interactive-ladder-spec.json"
    suite = "snb-interactive"

    def test_interactive_ladder_checks_the_spec_derived_reference(self) -> None:
        short = {"IS1", "IS2", "IS3", "IS4", "IS5", "IS6", "IS7"}
        bindings = sum(len(v) for v in interactive_reference.PARAMETERS.values())
        _passed, card = self.assert_ladder(
            checked=bindings, wrong_query="IC2", coverage="20/29 queries"
        )
        self.assertTrue(
            card["correctness"]["reference_source"].startswith("a spec-derived reference")
        )
        self.assertIn("not the LDBC validation set", card["correctness"]["reference_source"])
        workload = self.document("snb-interactive-sf0-inputs-workload.json")
        variants = {v["id"]: v for v in workload["variants"]}
        self.assertEqual(set(variants), set(interactive_reference.EVALUATORS))
        # Short-read ids come from the stream, minus the message an update creates.
        is4 = [b["params"]["messageId"]["value"] for b in variants["IS4"]["bindings"]]
        self.assertNotIn(fixture.CREATED_POST, is4)
        self.assertEqual(is4, [p["messageId"] for p in interactive_reference.PARAMETERS["IS4"]])
        for operation in short:
            self.assertTrue(variants[operation]["bindings"], operation)
        # IC3 binds endDate = startDate + durationDays, as the Cypher reference does.
        (ic3,) = variants["IC3"]["bindings"]
        start = ic3["params"]["startDate"]["value"]
        self.assertEqual(
            (ic3["params"]["endDate"]["value"] - start) % fixture.DAY_MS, 0, ic3["params"]
        )
        # IC13 runs the path verb between both persons.
        self.assertEqual(variants["IC13"]["operation"]["target"]["param"], "person2Id")


class WorkloadBuilderTests(unittest.TestCase):
    def test_ldbc_parameter_literals(self) -> None:
        self.assertEqual(
            scorecard.utc_datetime_literal("2010-06-08"),
            {"type": "ZonedDateTime", "value": [14768, 0, 0, None]},
        )
        self.assertEqual(
            scorecard.utc_datetime_literal("2010-08-01T12:12:48.123+00:00")["value"],
            [14822, (12 * 3600 + 12 * 60 + 48) * 10**9 + 123 * 10**6, 0, None],
        )
        for bad in ("2010-08-01T12:12:48.000+01:00", "2010-13-01", "yesterday"):
            with self.assertRaises(RungInputError, msg=bad):
                scorecard.utc_datetime_literal(bad)
        self.assertEqual(
            scorecard.string_list_literal("es;pt"),
            {
                "type": "List",
                "value": [{"type": "Str", "value": "es"}, {"type": "Str", "value": "pt"}],
            },
        )
        with self.assertRaises(RungInputError):
            scorecard.string_list_literal("es;;pt")
        self.assertEqual(scorecard.int_literal("2199023302404")["value"], 2199023302404)

    def test_bi10_path_distances_must_be_the_fixed_ones(self) -> None:
        queries = read(PROFILES / "snb-bi-scorecard-queries.json")
        (bi10,) = [q for q in queries["queries"] if q["operation"] == "BI10"]
        values = {
            "personId": "1",
            "country": "China",
            "tagClass": "Writer",
            "minPathDistance": "3",
            "maxPathDistance": "4",
        }
        self.assertEqual(scorecard.bi_binding(bi10, values, {})["minPathDistance"]["value"], 3)
        with self.assertRaises(RungInputError) as raised:
            scorecard.bi_binding(bi10, {**values, "maxPathDistance": "5"}, {})
        self.assertEqual(raised.exception.cause, "invalid_parameter")
        with self.assertRaises(RungInputError):
            scorecard.bi_binding(bi10, {**values, "extra": "1"}, {})

    def bi_inputs(self, scratch: Path, results: str | None = None) -> dict[str, Any]:
        archives = {}
        for name in ("snb-bi-fixture-parameters.tar.zst", "snb-bi-fixture-umbra.tar.zst"):
            archives[name] = fixture._decoded(fixture._members(FIXTURE / "archives" / name))
        for files in archives.values():
            for name, data in files.items():
                (scratch / name).parent.mkdir(parents=True, exist_ok=True)
                (scratch / name).write_bytes(data)
        if results is not None:
            (scratch / "output/output-sf0/results.csv").write_text(results, encoding="utf-8")
        spec = read(FIXTURE / "bi-ladder-spec.json")["rungs"][0]
        return scorecard.build_inputs(
            spec_workload=spec["workload"],
            spec_reference=spec["reference"],
            profile_root=FIXTURE,
            input_root=scratch,
            parameters_root=scratch,
            reference_root=scratch,
            suite_id="snb-bi",
            rung_id="sf0",
            mapping=None,
        )

    def test_umbra_lines_must_carry_the_bindings_parameters(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            scratch = Path(directory)
            built = self.bi_inputs(scratch)
            self.assertEqual(len(built["reference"]["queries"]), 17)
            results = (scratch / "output/output-sf0/results.csv").read_text(encoding="utf-8")
            # Mutation: Umbra's BI18 line names another tag than the workload binds.
            tampered = results.replace('{"tag": "', '{"tag": "x', 1)
            self.assertNotEqual(tampered, results)
            with self.assertRaises(RungInputError) as raised:
                self.bi_inputs(scratch, tampered)
            self.assertEqual(raised.exception.cause, "reference_binding_mismatch")
            # Mutation: a row with a missing value cannot be converted.
            short_row = results.replace(', "messageCount": 11}', "}", 1)
            self.assertNotEqual(short_row, results)
            with self.assertRaises(RungInputError) as raised:
                self.bi_inputs(scratch, short_row)
            self.assertEqual(raised.exception.cause, "reference_unconvertible")

    def test_short_read_bindings_exclude_ids_the_streams_updates_create(self) -> None:
        """Even when the snapshot has an entity with that id, a created id is never bound."""

        class EveryId:
            def by_id(self, label: str, node_id: int) -> str:
                return f"{label}:{node_id}"

        queries = read(PROFILES / "snb-interactive-scorecard-queries.json")
        queries = {**queries, "queries": [q for q in queries["queries"] if q["operation"] == "IS4"]}
        with tempfile.TemporaryDirectory() as directory:
            stream = Path(directory) / "validation.csv"
            stream.write_text(
                "\n".join(
                    [
                        '{"messageIdContent": 5}|{}',
                        '{"commentId": 7, "replyToPostId": 5, "replyToCommentId": -1}|"-1"',
                        '{"messageIdContent": 7}|{}',
                        '{"messageIdContent": 5}|{}',
                        '{"messageIdContent": 8}|{}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            workload, absent = scorecard.interactive_workload(
                queries_document=queries,
                substitution_dir=Path(directory),
                validation=stream,
                per_variant=30,
                graph=EveryId(),
            )
        (variant,) = workload["variants"]
        self.assertEqual([b["params"]["messageId"]["value"] for b in variant["bindings"]], [5, 8])
        self.assertEqual(absent, {"IS4": 0})

    def test_short_read_ids_skip_entities_the_stream_creates(self) -> None:
        stream = [
            ({"commentId": 7, "creationDate": 1, "replyToPostId": 1, "replyToCommentId": -1}, True),
            ({"personIdSQ1": 5}, False),
            ({"messageIdContent": 7}, False),
            ({"messageIdContent": 8}, False),
            ({"messageIdContent": 8}, False),
        ]
        self.assertEqual([scorecard.is_update(p) for p, _ in stream], [u for _, u in stream])
        self.assertEqual(scorecard.created_ids(stream[0][0]), {("message", 7)})
        self.assertEqual(
            scorecard.created_ids({"personId": 3, "personFirstName": "A"}), {("person", 3)}
        )
        self.assertTrue(scorecard.is_update({"forumId": 1, "personId": 2, "joinDate": 3}))


class MatchingRuleTests(unittest.TestCase):
    def result(
        self, columns: list[str], rows: list[list[Any]], ordered: bool = True
    ) -> dict[str, Any]:
        return {
            "columns": [{"name": name, "type": "Utf8"} for name in columns],
            "rows": rows,
            "ordered": ordered,
        }

    def test_set_columns_compare_list_elements_in_any_order(self) -> None:
        rule = {"matching": "exact", "set_columns": ["tags"]}
        reference = {"columns": ["id", "tags"], "rows": [["1", "[a, b, {c: 1, d: x}]"]]}
        self.assertTrue(
            matches(rule, self.result(["id", "tags"], [["1", "[{c: 1, d: x}, b, a]"]]), reference)
        )
        self.assertFalse(matches(rule, self.result(["id", "tags"], [["1", "[a, b]"]]), reference))
        self.assertFalse(
            matches(rule, self.result(["id", "tags"], [["2", "[a, b, {c: 1, d: x}]"]]), reference)
        )
        self.assertFalse(
            matches(
                {"matching": "exact"},
                self.result(["id", "tags"], [["1", "[b, a, {c: 1, d: x}]"]]),
                reference,
            )
        )
        self.assertEqual(list_elements("[]"), [])
        self.assertIsNone(list_elements("[a, b"))

    def test_projection_compares_only_the_reference_columns(self) -> None:
        rule = {"matching": "projection"}
        reference = {"columns": ["cost"], "rows": [["3.0"]]}
        self.assertTrue(
            matches(
                rule, self.result(["source", "cost", "path"], [["u", "3.0", "[u, v]"]]), reference
            )
        )
        self.assertFalse(matches(rule, self.result(["source", "cost"], [["u", "4.0"]]), reference))
        self.assertFalse(matches(rule, self.result(["source", "cost"], []), reference))
        self.assertTrue(matches(rule, self.result(["cost"], []), {"columns": ["cost"], "rows": []}))

    def test_ordered_epsilon_keeps_the_reference_row_order(self) -> None:
        rule = {"matching": "epsilon", "epsilon": 1e-9, "key": ["k"]}
        reference = {"columns": ["k", "v"], "rows": [["a", "0.166666666666667"], ["b", "2.0"]]}
        rows = [["a", "0.16666666666666666"], ["b", "2.0"]]
        self.assertTrue(matches(rule, self.result(["k", "v"], rows), reference))
        self.assertFalse(matches(rule, self.result(["k", "v"], rows[::-1]), reference))
        self.assertTrue(
            matches(rule, self.result(["k", "v"], rows[::-1], ordered=False), reference)
        )
        self.assertFalse(
            matches(rule, self.result(["k", "v"], [["a", "0.1667"], ["b", "2.0"]]), reference)
        )


class LadderSpecTests(unittest.TestCase):
    def test_real_ladders_declare_every_rung_and_mark_the_unpinned_ones(self) -> None:
        for name, suite, ladder in (
            (
                "snb-bi-scorecard-ladder-spec.json",
                "snb-bi",
                ["sf1", "sf3", "sf10", "sf30", "sf100"],
            ),
            (
                "snb-interactive-scorecard-ladder-spec.json",
                "snb-interactive",
                ["sf1", "sf3", "sf10", "sf30"],
            ),
        ):
            spec = load_ladder_spec(ROOT, PROFILES / name)
            self.assertEqual(spec.suite_id, suite)
            rungs = spec.document["rungs"]
            self.assertEqual([r["id"] for r in rungs], ladder)
            unpinned = [r["id"] for r in rungs if "not_pinned" in r]
            self.assertEqual(unpinned, [r for r in ladder if r not in ("sf1", "sf10")])
            pin = resolve_pinned_identity(
                load_suite_declaration(spec.suite_declaration),
                {"identity_profile": "scorecard"},
                ROOT,
            )
            datasets = {item["id"] for item in pin["datasets"]}
            references = {f"{r['dataset_id']}:{r['workload_key']}" for r in pin["references"]}
            for rung in rungs:
                if "not_pinned" in rung:
                    continue
                self.assertIn(rung["dataset_id"], datasets)
                self.assertIn(rung["workload"]["parameters"]["dataset_id"], datasets)
                self.assertEqual(rung["workload"]["per_variant"], 30)
                for key in ("short_reads", "reference"):
                    item = rung["workload"].get(key) if key == "short_reads" else rung["reference"]
                    if item and "workload_key" in item:
                        self.assertIn(f"{rung['dataset_id']}:{item['workload_key']}", references)

    def test_bi_refusals_and_rewrites_follow_the_query_definitions(self) -> None:
        queries = read(PROFILES / "snb-bi-scorecard-queries.json")
        spec = read(PROFILES / "snb-bi-scorecard-ladder-spec.json")
        refused = {(r["query_id"], r["cause"]) for r in spec["rungs"][0]["refused"]}
        expected = {
            (f"{r['operation']}{suffix}", r["cause"]) for r in queries["refused"] for suffix in "ab"
        }
        expected |= {
            (f"{kind}{n}", "bi_batch_update_stream_not_exposed")
            for kind in ("INS", "DEL")
            for n in range(1, 9)
        }
        self.assertEqual(refused, expected)
        rewrites = {v["subject"]: v["text"] for v in spec["variances"] if v["kind"] == "rewrite"}
        self.assertEqual(
            rewrites, {q["operation"]: q["rewrite"] for q in queries["queries"] if q["rewrite"]}
        )
        message = [q["operation"] for q in queries["queries"] if ":Post OR " in q["cypher"]]
        self.assertEqual(len(message), 15)
        for operation in message:
            self.assertIn("`:Message` supertype label", rewrites[operation])
        self.assertFalse(any(":Message" in q["cypher"] for q in queries["queries"]))

    def test_interactive_refusals_and_variances_follow_the_query_definitions(self) -> None:
        queries = read(PROFILES / "snb-interactive-scorecard-queries.json")
        spec = read(PROFILES / "snb-interactive-scorecard-ladder-spec.json")
        self.assertEqual(
            [(r["query_id"], r["cause"]) for r in spec["rungs"][0]["refused"]],
            [("IC14", "weighted_interaction_path_enumeration_not_exposed")]
            + [(f"IU{n}", "interactive_update_stream_not_exposed") for n in range(1, 9)],
        )
        variances = {(v["kind"], v["subject"]): v["text"] for v in spec["variances"]}
        for query in queries["queries"]:
            if query["spec_variance"]:
                self.assertEqual(
                    variances[("spec_variance", query["operation"])], query["spec_variance"]
                )
        self.assertIn("position 0", variances[("reference_reading", "reference")])

    def test_committed_query_definitions_are_the_runners_list_queries_output(self) -> None:
        self.assertEqual(
            read(PROFILES / "snb-bi-scorecard-queries.json"),
            gdc_snb_bi.query_definitions_document(),
        )
        self.assertEqual(
            read(PROFILES / "snb-interactive-scorecard-queries.json"),
            gdc_snb_interactive.query_definitions_document(),
        )


class FixtureTests(unittest.TestCase):
    def test_archives_hold_a_fresh_build_of_the_query_fixtures(self) -> None:
        self.assertEqual(fixture.main([]), 0)

    def test_fixture_profiles_agree_with_the_real_ones_and_the_archives(self) -> None:
        fixture_profiles = FIXTURE / "profiles" / "gdc"
        for name in (
            "snb-bi-load-mapping.json",
            "snb-interactive-load-mapping.json",
            "snb-bi-scorecard-queries.json",
            "snb-interactive-scorecard-queries.json",
        ):
            self.assertEqual(read(fixture_profiles / name), read(PROFILES / name), name)
        self.assertEqual(
            read(fixture_profiles / "snb-bi-mutated-queries.json"),
            fixture.mutated_bi_queries(read(PROFILES / "snb-bi-scorecard-queries.json")),
        )
        self.assertEqual(
            read(fixture_profiles / "snb-interactive-mutated-queries.json"),
            fixture.mutated_interactive_queries(
                read(PROFILES / "snb-interactive-scorecard-queries.json")
            ),
        )
        for suite, files in (
            ("snb-bi", fixture.bi_dataset_files("bi-fixture-sf0")),
            ("snb-interactive", fixture.interactive_dataset_files("social_network-fixture-sf0")),
        ):
            mapping = read(fixture_profiles / f"{suite}-load-mapping.json")
            counts = fixture.table_counts(files, mapping)
            ladder = read(fixture_profiles / f"{suite}-fixture-scorecard-ladder.json")
            for rung in ladder["rungs"]:
                self.assertEqual({t["table"]: t["listed"] for t in rung["tables"]}, counts)
                self.assertTrue(all(count > 0 for count in counts.values()), counts)
            identity = read(fixture_profiles / f"{suite}-fixture-identity.json")
            for item in identity["datasets"] + identity["references"]:
                archive = FIXTURE / "archives" / item["source"].rsplit("/", 1)[1]
                self.assertEqual(
                    hashlib.sha256(archive.read_bytes()).hexdigest(), item["checksum_sha256"]
                )

    def test_bi_fixture_forums_carry_tags_a_message_read_must_not_count(self) -> None:
        tables = fixture.bi_tables()
        header, rows = tables["dynamic/Forum_hasTag_Tag"]
        self.assertEqual(header, ["creationDate", "ForumId", "TagId"])
        self.assertEqual(len(rows), len(tables["dynamic/Forum"][1]) * len(tables["static/Tag"][1]))


if __name__ == "__main__":
    unittest.main()
