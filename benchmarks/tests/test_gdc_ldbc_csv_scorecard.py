"""The committed LDBC CSV scorecard pins, load mappings and count ladders agree.

SNB BI, SNB Interactive v1 and FinBench Transaction each pin their SF1 and SF10
archives (#1878). Every count here is transcribed from an LDBC table at a fixed
commit or measured from the pinned archive; these tests hold the documents to
each other and to the arithmetic that explains every difference.
"""

from __future__ import annotations

import json
from pathlib import Path
import unittest

from graphforge_bench import gdc_dataset_cache as cache
from graphforge_bench.gdc_contracts import (
    load_pinned_identity,
    load_suite_declaration,
    resolve_pinned_identity,
    workspace_root,
)
from jsonschema import Draft202012Validator

ROOT = workspace_root()
SUITES = ("snb-bi", "snb-interactive", "finbench-transaction")


def _json(relative: str) -> dict:
    return json.loads((ROOT / relative).read_text(encoding="utf-8"))


class LdbcCsvScorecardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schema = Draft202012Validator(_json("schemas/gdc-ldbc-csv-scorecard-ladder.json"))
        cls.pins = {}
        cls.ladders = {}
        cls.mappings = {}
        for suite_id in SUITES:
            suite = load_suite_declaration(ROOT / "suites" / f"gdc-{suite_id}.json")
            cls.pins[suite_id] = resolve_pinned_identity(
                suite, {"identity_profile": "scorecard"}, ROOT
            )
            cls.ladders[suite_id] = _json(f"profiles/gdc/{suite_id}-scorecard-ladder.json")
            cls.mappings[suite_id] = _json(cls.ladders[suite_id]["load_mapping"])

    def test_scorecard_pins_are_the_ldbc_archives_with_distinct_recorded_digests(self) -> None:
        for suite_id, pin in self.pins.items():
            self.assertEqual(
                pin, load_pinned_identity(ROOT / f"profiles/gdc/{suite_id}-scorecard-identity.json")
            )
            sources = [item["source"] for item in pin["datasets"] + pin["references"]]
            for source in sources:
                cache._archive_name(source)  # raises unless an accepted LDBC archive
            digests = {item["source"]: item["checksum_sha256"] for item in pin["datasets"]}
            digests.update({item["source"]: item["checksum_sha256"] for item in pin["references"]})
            self.assertEqual(len(set(digests.values())), len(digests), suite_id)
            dataset_ids = {item["id"] for item in pin["datasets"] if item["role"] == "dataset"}
            for reference in pin["references"]:
                self.assertIn(reference["dataset_id"], dataset_ids, suite_id)
            roles = sorted(item["role"] for item in pin["datasets"])
            self.assertEqual(roles.count("dataset"), 2, suite_id)
            self.assertEqual(roles.count("parameter"), 1 if suite_id == "snb-bi" else 2, suite_id)

    def test_known_pins_and_published_counts(self) -> None:
        """Spot values, transcribed by hand from the pins and LDBC tables."""
        by_source = {
            item["source"].rsplit("/", 1)[1]: item["checksum_sha256"][:12]
            for pin in self.pins.values()
            for item in pin["datasets"]
        }
        self.assertEqual(by_source["bi-sf1-composite-projected-fk.tar.zst"], "79c135ed3eb7")
        self.assertEqual(
            by_source["social_network-sf10-CsvComposite-LongDateFormatter.tar.zst"],
            "99faef3d8041",
        )
        self.assertEqual(by_source["sf1.tar.gz"], "598d82e0bc44")
        published = {
            (suite_id, rung["id"], table["table"]): table["published"]
            for suite_id, ladder in self.ladders.items()
            for rung in ladder["rungs"]
            for table in rung["tables"]
        }
        # table-number-of-entities-bi-initial.tex
        self.assertEqual(published[("snb-bi", "sf1", "comment")], 1_739_438)
        self.assertEqual(published[("snb-bi", "sf10", "person_knows_person")], 1_839_354)
        # legacy/table-number-of-entities-interactive.tex
        self.assertEqual(published[("snb-interactive", "sf1", "person")], 11_000)
        self.assertEqual(published[("snb-interactive", "sf10", "comment")], 26_540_464)
        # number-of-entities-transaction.tex; transfer is transfer + loanTransfer
        self.assertEqual(published[("finbench-transaction", "sf1", "account")], 264_075)
        self.assertEqual(
            published[("finbench-transaction", "sf10", "account_transfer_account")],
            11_005_032 + 3_625_556,
        )

    def test_ladders_validate_and_name_exactly_the_pinned_datasets(self) -> None:
        for suite_id, ladder in self.ladders.items():
            self.schema.validate(ladder)
            self.assertEqual(ladder["suite_id"], suite_id)
            self.assertEqual(
                ladder["load_mapping"], f"profiles/gdc/{suite_id}-load-mapping.json"
            )
            pinned = {
                item["id"]: item["source"]
                for item in self.pins[suite_id]["datasets"]
                if item["role"] == "dataset"
            }
            laddered = {rung["dataset_id"]: rung["archive_url"] for rung in ladder["rungs"]}
            self.assertEqual(laddered, pinned, suite_id)
            self.assertEqual(
                [rung["order"] for rung in ladder["rungs"]],
                list(range(1, len(ladder["rungs"]) + 1)),
            )
            self.assertEqual([rung["id"] for rung in ladder["rungs"]], ["sf1", "sf10"])
            for rung in ladder["rungs"]:
                self.assertEqual(rung["id"], f"sf{rung['scale_factor']}")

    def test_every_rung_counts_exactly_the_mapped_tables(self) -> None:
        for suite_id, ladder in self.ladders.items():
            mapping = self.mappings[suite_id]
            mapped = {table["id"]: "nodes" for table in mapping["node_tables"]}
            mapped.update({table["id"]: "edges" for table in mapping["edge_tables"]})
            for rung in ladder["rungs"]:
                counted = {table["table"]: table["kind"] for table in rung["tables"]}
                self.assertEqual(counted, mapped, f"{suite_id} {rung['id']}")
                self.assertEqual(len(rung["tables"]), len(counted), "a table is counted twice")

    def test_every_difference_from_the_published_count_is_explained_and_adds_up(self) -> None:
        for suite_id, ladder in self.ladders.items():
            for rung in ladder["rungs"]:
                for table in rung["tables"]:
                    where = f"{suite_id} {rung['id']} {table['table']}"
                    if table["listed"] == table["published"] and "held_back" not in table:
                        self.assertNotIn("discrepancy", table, where)
                        continue
                    self.assertIn("discrepancy", table, where)
                    self.assertEqual(
                        table["published"],
                        table["listed"] + table["held_back"] + table["residual"],
                        where,
                    )
                    if "deleted_in_raw" in table:
                        self.assertNotEqual(table["residual"], 0, where)
                        self.assertLessEqual(table["residual"], table["deleted_in_raw"], where)

    def test_bi_matches_its_initial_snapshot_table_exactly(self) -> None:
        for rung in self.ladders["snb-bi"]["rungs"]:
            for table in rung["tables"]:
                self.assertEqual(table["listed"], table["published"], table["table"])
            self.assertEqual(
                rung["member_md5_manifest"]["dataset_id"], "bi-composite-projected-fk-md5sums"
            )
            self.assertEqual(
                rung["member_md5_manifest"]["path"], f"{rung['dataset_id']}-md5sum.txt"
            )

    def test_per_type_counts_sum_to_the_published_totals(self) -> None:
        """Checks the per-type transcription against each LDBC table's own totals."""
        for suite_id, ladder in self.ladders.items():
            for rung in ladder["rungs"]:
                if "published_totals" not in rung:
                    continue
                for kind in ("nodes", "edges"):
                    total = sum(t["published"] for t in rung["tables"] if t["kind"] == kind)
                    self.assertEqual(
                        total, rung["published_totals"][kind], f"{suite_id} {rung['id']} {kind}"
                    )
        self.assertIn("published_totals", self.ladders["snb-bi"]["rungs"][0])
        self.assertIn("published_totals", self.ladders["snb-interactive"]["rungs"][0])

    def test_interactive_snapshot_rows_equal_ldbc_snapshot_totals_or_say_why_not(self) -> None:
        for rung in self.ladders["snb-interactive"]["rungs"]:
            listed = {
                kind: sum(t["listed"] for t in rung["tables"] if t["kind"] == kind)
                for kind in ("nodes", "edges")
            }
            published = rung["published_snapshot_totals"]
            self.assertEqual(listed["nodes"], published["nodes"], rung["id"])
            if "snapshot_totals_discrepancy" in rung:
                self.assertNotEqual(listed, published, f"{rung['id']}: stale discrepancy")
                self.assertIn(
                    f"{published['edges'] - listed['edges']} above",
                    rung["snapshot_totals_discrepancy"],
                )
            else:
                self.assertEqual(listed, published, rung["id"])
        sf1 = self.ladders["snb-interactive"]["rungs"][0]
        self.assertNotIn("snapshot_totals_discrepancy", sf1, "SF1 reconciles exactly")


if __name__ == "__main__":
    unittest.main()
