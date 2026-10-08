"""The spec-derived FinBench reference over published LDBC CSV (no GraphForge)."""

from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
import random
import tempfile
import unittest

from graphforge_bench.gdc_contracts import workspace_root
from graphforge_bench.gdc_finbench_transaction_reference import (
    COLUMNS,
    LDBC_EDGE_FILES,
    LDBC_PARAMETERS,
    REFERENCE_SCHEMA,
    Edge,
    Graph,
    arrow_float,
    derive_ldbc_reference,
    ldbc_epoch_millis,
    load_ldbc_snapshot,
    main,
    read_ldbc_parameters,
    shortest_transfer_path,
    shortest_transfer_path_forward,
)
from jsonschema import Draft202012Validator

WINDOW = "1000|9000"
LIMIT = "500|TIMESTAMP_DESCENDING"
NODE_HEADERS = {
    "Account": (
        "accountId|createTime|isBlocked|accoutType|nickname|phonenum|email|freqLoginType|"
        "lastLoginTime|accountLevel"
    ),
    "Company": "companyId|companyName|isBlocked|createTime|country|city|business|description|url",
    "Loan": "loanId|loanAmount|balance|createTime|loanUsage|interestRate",
    "Medium": "mediumId|mediumType|isBlocked|createTime|lastLoginTime|riskLevel",
    "Person": "personId|personName|isBlocked|createTime|gender|birthday|country|city",
}
EDGE_HEADERS = {
    "AccountTransferAccount": "fromId|toId|amount|createTime|orderNum|comment|payType|goodsType",
    "AccountWithdrawAccount": "fromId|toId|amount|createTime",
    "AccountRepayLoan": "accountId|loanId|amount|createTime",
    "LoanDepositAccount": "loanId|accountId|amount|createTime",
    "MediumSignInAccount": "mediumId|accountId|createTime|location",
    "PersonOwnAccount": "personId|accountId|createTime",
    "CompanyOwnAccount": "companyId|accountId|createTime",
    "PersonApplyLoan": "personId|loanId|createTime|org",
    "CompanyApplyLoan": "companyId|loanId|createTime|org",
    "PersonInvestCompany": "investorId|companyId|ratio|createTime",
    "CompanyInvestCompany": "investorId|companyId|ratio|createTime",
    "PersonGuaranteePerson": "fromId|toId|createTime|relation",
    "CompanyGuaranteeCompany": "fromId|toId|createTime|relation",
}


def ldbc_time(millis: int) -> str:
    """An LDBC datetime for ``millis`` after the epoch, fraction zeros dropped."""
    moment = datetime.fromtimestamp(millis / 1000, tz=UTC)
    text = moment.strftime("%Y-%m-%d %H:%M:%S")
    fraction = f"{millis % 1000:03d}".rstrip("0")
    return f"{text}.{fraction}" if fraction else text


def write_snapshot(root: Path, rows: dict[str, list[str]]) -> Path:
    snapshot = root / "snapshot"
    snapshot.mkdir()
    for stem, header in {**NODE_HEADERS, **EDGE_HEADERS}.items():
        lines = [header, *rows.get(stem, [])]
        (snapshot / f"{stem}.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return snapshot


def write_params(root: Path, rows: dict[str, list[str]]) -> Path:
    params = root / "params"
    params.mkdir()
    for operation in LDBC_PARAMETERS:
        number = operation.removeprefix("TCR")
        lines = rows.get(operation, [])
        (params / f"complex_{number}_param.csv").write_text(
            "".join(f"{line}\n" for line in lines), encoding="utf-8"
        )
    return params


# Person 7 and Company 7 share an id, as LDBC ids do across labels; so do
# Account 30 and Loan 30. Every read must keep them apart.
T = 1_627_020_616_747
SNAPSHOT_ROWS = {
    "Account": [
        f"{account}|{ldbc_time(T)}|{blocked}|{kind}|n|p|e|f|0|l"
        for account, blocked, kind in [
            (10, "false", "debit card"),
            (11, "false", "brokerage account"),
            (12, "true", "trust account"),
            (30, "false", "debit card"),
        ]
    ],
    "Company": [f"7|Acme|false|{ldbc_time(T)}|c|c|b|d|u"],
    "Loan": [f"30|1000.0|400.0|{ldbc_time(T)}|u|0.01"],
    "Medium": [f"40|POS|true|{ldbc_time(T)}|0|r"],
    "Person": [f"7|Kim|false|{ldbc_time(T)}|f|1988-10-17 00:00:00|c|c"],
    "AccountTransferAccount": [
        f"10|11|100.2|{ldbc_time(T + 10)}|1|a | comment with pipes|p|g",
        f"11|12|50.4|{ldbc_time(T + 20)}|2|c|p|g",
        f"12|30|7.0|{ldbc_time(T + 30)}|3|c|p|g",
    ],
    "MediumSignInAccount": [f"40|12|{ldbc_time(T + 5)}|loc"],
    "PersonOwnAccount": [f"7|10|{ldbc_time(T)}"],
    "CompanyOwnAccount": [f"7|11|{ldbc_time(T)}", f"7|12|{ldbc_time(T)}"],
    "LoanDepositAccount": [f"30|10|300.0|{ldbc_time(T + 1)}"],
    "PersonApplyLoan": [f"7|30|{ldbc_time(T)}|o"],
}
WINDOW_PARAMS = f"{T - 1000}|{T + 1000}"
PARAM_ROWS = {
    "TCR1": ["...", f"10|{WINDOW_PARAMS}|{LIMIT}"],
    "TCR2": ["...", f"7|{WINDOW_PARAMS}|{LIMIT}"],
    "TCR3": [f"10|30|{WINDOW_PARAMS}", f"30|10|{WINDOW_PARAMS}"],
    "TCR4": ["...", f"10|11|{WINDOW_PARAMS}"],
    "TCR5": ["...", f"7|{WINDOW_PARAMS}|{LIMIT}"],
    "TCR6": ["...", f"30|0.0|0.0|{WINDOW_PARAMS}|{LIMIT}"],
    "TCR7": ["...", f"11|0.0|{WINDOW_PARAMS}|{LIMIT}"],
    "TCR8": ["...", f"30|0.0|{WINDOW_PARAMS}|{LIMIT}"],
    "TCR9": ["...", f"10|0.0|{WINDOW_PARAMS}|{LIMIT}"],
    "TCR10": ["...", f"7|7|{WINDOW_PARAMS}"],
    "TCR11": ["...", f"7|{WINDOW_PARAMS}|{LIMIT}"],
    "TCR12": ["...", f"7|{WINDOW_PARAMS}|{LIMIT}"],
}


class ArrowFloatTests(unittest.TestCase):
    def test_layout_matches_ryu(self) -> None:
        # Expected texts are ryu 1.0.23's output (arrow-cast 58.4 Float64 display).
        cases = [
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (-1.0, "-1.0"),
            (100.0, "100.0"),
            (0.667, "0.667"),
            (1234.5, "1234.5"),
            (123456789.125, "123456789.125"),
            (1e15, "1000000000000000.0"),
            (9999999999999998.0, "9999999999999998.0"),
            (1e16, "1e16"),
            (1.5e17, "1.5e17"),
            (0.001, "0.001"),
            (1e-5, "0.00001"),
            (1.5e-5, "0.000015"),
            (1e-6, "1e-6"),
            (1.25e-7, "1.25e-7"),
            (2.0**63, "9.223372036854776e18"),
            (float("inf"), "inf"),
        ]
        for value, text in cases:
            self.assertEqual(arrow_float(value), text, value)


class LdbcInputTests(unittest.TestCase):
    def test_epoch_millis_reads_every_fraction_width(self) -> None:
        for millis in [0, 1, 10, 100, 460, 999, T, T + 1, 1_669_690_342_640]:
            self.assertEqual(ldbc_epoch_millis(ldbc_time(millis)), millis, ldbc_time(millis))
        self.assertEqual(ldbc_epoch_millis("2020-05-05 21:16:49.46"), 1_588_713_409_460)
        for bad in [
            "2020-05-05T21:16:49",
            "2020-05-05 21:16:49.",
            "2020-05-05 21:16:49.1234",
            "2020-05-05 21:16:49+01:00",
            "2020-05-05",
        ]:
            with self.assertRaises(ValueError, msg=bad):
                ldbc_epoch_millis(bad)

    def test_parameters_skip_the_placeholder_and_keep_file_lines(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bindings = read_ldbc_parameters(write_params(Path(tmp), PARAM_ROWS))
        self.assertEqual(list(bindings), list(LDBC_PARAMETERS))
        self.assertEqual([binding_id for binding_id, _ in bindings["TCR1"]], ["line-2"])
        self.assertEqual([binding_id for binding_id, _ in bindings["TCR3"]], ["line-1", "line-2"])
        self.assertEqual(
            bindings["TCR6"][0][1],
            {
                "id": 30,
                "threshold1": 0.0,
                "threshold2": 0.0,
                "startTime": T - 1000,
                "endTime": T + 1000,
                "truncationLimit": 500,
                "truncationOrder": "TIMESTAMP_DESCENDING",
            },
        )

    def test_parameters_refuse_a_short_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            params = write_params(Path(tmp), {**PARAM_ROWS, "TCR7": ["...", f"11|{WINDOW}"]})
            with self.assertRaisesRegex(ValueError, "complex_7_param.csv:2"):
                read_ldbc_parameters(params)

    def test_snapshot_refuses_a_dangling_endpoint(self) -> None:
        rows = {**SNAPSHOT_ROWS, "PersonOwnAccount": [f"8|10|{ldbc_time(T)}"]}
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = write_snapshot(Path(tmp), rows)
            with self.assertRaisesRegex(ValueError, "dangling"):
                load_ldbc_snapshot(snapshot)

    def test_every_edge_file_is_read(self) -> None:
        self.assertEqual({stem for stem, *_ in LDBC_EDGE_FILES}, set(EDGE_HEADERS))


class LdbcReferenceTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.snapshot = write_snapshot(self.root, SNAPSHOT_ROWS)
        self.params = write_params(self.root, PARAM_ROWS)

    def test_reference_is_in_driver_cell_form_and_keeps_labels_apart(self) -> None:
        reference = derive_ldbc_reference(self.snapshot, self.params, "sf-test")
        self.assertEqual(reference["schema"], REFERENCE_SCHEMA)
        self.assertEqual(
            (reference["suite_id"], reference["rung_id"]), ("finbench-transaction", "sf-test")
        )
        schema = json.loads(
            (workspace_root() / "schemas" / "gdc-rung-reference.json").read_text(encoding="utf-8")
        )
        Draft202012Validator(schema).validate(reference)
        queries = reference["queries"]
        self.assertEqual(list(queries), list(LDBC_PARAMETERS))
        for operation, query in queries.items():
            self.assertEqual(query["matching"], "exact")
            for binding in query["bindings"].values():
                self.assertEqual(binding["columns"], [name for name, _ in COLUMNS[operation]])

        def rows(operation: str, binding_id: str = "line-2") -> list[list[str]]:
            return queries[operation]["bindings"][binding_id]["rows"]

        # 10 -> 11 -> 12, signed in by blocked medium 40 at hop 2.
        self.assertEqual(rows("TCR1"), [["12", "2", "40", "POS"]])
        # Person 7 owns only account 10; Company 7's accounts 11 and 12 are not
        # the person's. Company-owned 11 is the only target of 10's transfers.
        self.assertEqual(rows("TCR12"), [["11", "100.2"]])
        self.assertEqual(rows("TCR5"), [["[10, 11, 12, 30]"], ["[10, 11, 12]"], ["[10, 11]"]])
        # Loan 30 and Account 30 share an id: TCR3 walks accounts only.
        self.assertEqual(rows("TCR3", "line-1"), [["3"]])
        self.assertEqual(rows("TCR3", "line-2"), [["-1"]])
        # Loan 30 deposited into 10; every hop qualifies with threshold 0.
        self.assertEqual(
            rows("TCR8"),
            [["30", "0.007", "4"], ["12", "0.05", "3"], ["11", "0.1", "2"]],
        )
        # Account 30 is a card account, but nothing withdrew to it.
        self.assertEqual(rows("TCR6"), [])
        self.assertEqual(rows("TCR7"), [["1", "1", "1.988"]])
        self.assertEqual(rows("TCR9"), [["-1.0", "2.994", "0.0"]])
        # Person 7 applied for loan 30 but guarantees nobody: one aggregate row.
        self.assertEqual(rows("TCR11"), [["0.0", "0"]])
        self.assertEqual(rows("TCR10"), [["0.0"]])
        self.assertEqual(rows("TCR2"), [])
        self.assertEqual(rows("TCR4"), [])

    def test_cli_writes_a_new_file_and_never_overwrites(self) -> None:
        output = self.root / "reference.json"
        argv = [
            "ldbc",
            "--snapshot",
            str(self.snapshot),
            "--params",
            str(self.params),
            "--rung",
            "sf-test",
            "--output",
            str(output),
        ]
        self.assertEqual(main(argv), 0)
        first = output.read_bytes()
        self.assertEqual(json.loads(first)["rung_id"], "sf-test")
        with self.assertRaises(FileExistsError):
            main(argv)
        self.assertEqual(output.read_bytes(), first)
        second = self.root / "second.json"
        self.assertEqual(main([*argv[:-1], str(second)]), 0)
        self.assertEqual(second.read_bytes(), first)


class ShortestPathTests(unittest.TestCase):
    def test_bidirectional_search_equals_forward_search(self) -> None:
        generator = random.Random(1894)
        for trial in range(200):
            graph = Graph()
            size = generator.randint(2, 30)
            for node in range(size):
                graph.add_node("Account", node, {})
            for index in range(generator.randint(0, size * 3)):
                graph.add_edge(
                    Edge(
                        kind="transfer",
                        index=index,
                        src_label="Account",
                        src=generator.randrange(size),
                        dst_label="Account",
                        dst=generator.randrange(size),
                        timestamp=generator.randint(0, 20),
                        amount=1.0,
                    )
                )
            for _ in range(10):
                binding = {
                    "id1": generator.randrange(size),
                    "id2": generator.randrange(size),
                    "startTime": 2,
                    "endTime": 18,
                }
                self.assertEqual(
                    shortest_transfer_path(graph, binding),
                    shortest_transfer_path_forward(graph, binding),
                    (trial, binding),
                )


if __name__ == "__main__":
    unittest.main()
