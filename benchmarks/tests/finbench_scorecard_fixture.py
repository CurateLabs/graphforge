"""The tiny FinBench Transaction rung fixture, rendered from the query fixture.

``fixtures/gdc/finbench-transaction-queries`` holds a FinBench-shaped graph
and parameter bindings with integer epoch-millisecond timestamps. The rung
fixture (``fixtures/gdc/finbench-scorecard-fixture``) is the same graph in the
shape LDBC publishes it: pipe-delimited ``snapshot/`` CSV files with LDBC's
headers and naive-UTC ``createTime`` text, and the ``complex_<n>_param.csv``
read parameters. ``render`` produces those files; ``tests`` hold the committed
copy to it, and the archives are built from it with the commands in
``ARCHIVE_COMMANDS``.

Run ``python -m tests.finbench_scorecard_fixture`` from ``benchmarks`` to
rewrite the committed ``source/`` after changing the query fixture.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
import json
from pathlib import Path
from typing import Any

from graphforge_bench.gdc_contracts import workspace_root
from graphforge_bench.gdc_finbench_transaction_reference import LDBC_PARAMETERS

ROOT = workspace_root()
QUERY_FIXTURE = ROOT / "fixtures" / "gdc" / "finbench-transaction-queries"
FIXTURE = ROOT / "fixtures" / "gdc" / "finbench-scorecard-fixture"
SNAPSHOT_DIR = "sf1/snapshot"
PARAMETER_DIR = "finbench_fixture_read_params"
ARCHIVE_COMMANDS = (
    "tar --zstd --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner "
    "--mode=u+rw,go+r-w -cf archives/finbench-fixture-sf1.tar.zst -C source sf1",
    "python3 -m tests.finbench_scorecard_fixture --zip  # read-params zip",
)

# File stem -> column order, as the published LDBC files carry them.
NODE_FILES: dict[str, tuple[str, tuple[str, ...]]] = {
    "Person": (
        "Person",
        (
            "personId",
            "personName",
            "isBlocked",
            "createTime",
            "gender",
            "birthday",
            "country",
            "city",
        ),
    ),
    "Company": (
        "Company",
        (
            "companyId",
            "companyName",
            "isBlocked",
            "createTime",
            "country",
            "city",
            "business",
            "description",
            "url",
        ),
    ),
    "Account": (
        "Account",
        (
            "accountId",
            "createTime",
            "isBlocked",
            "accoutType",
            "nickname",
            "phonenum",
            "email",
            "freqLoginType",
            "lastLoginTime",
            "accountLevel",
        ),
    ),
    "Loan": (
        "Loan",
        ("loanId", "loanAmount", "balance", "createTime", "loanUsage", "interestRate"),
    ),
    "Medium": (
        "Medium",
        ("mediumId", "mediumType", "isBlocked", "createTime", "lastLoginTime", "riskLevel"),
    ),
}
# Fixture column -> LDBC column, per node label.
NODE_COLUMNS: dict[str, dict[str, str]] = {
    "Person": {"id": "personId", "name": "personName", "isBlocked": "isBlocked"},
    "Company": {"id": "companyId", "name": "companyName", "isBlocked": "isBlocked"},
    "Account": {
        "id": "accountId",
        "createTime": "createTime",
        "isBlocked": "isBlocked",
        "type": "accoutType",
        "nickname": "nickname",
    },
    "Loan": {"id": "loanId", "loanAmount": "loanAmount", "balance": "balance"},
    "Medium": {"id": "mediumId", "type": "mediumType", "isBlocked": "isBlocked"},
}
# (edge type, source label, target label) -> (file, columns, source column, target column).
EDGE_FILES: dict[tuple[str, str, str], tuple[str, tuple[str, ...], str, str]] = {
    ("own", "Person", "Account"): (
        "PersonOwnAccount",
        ("personId", "accountId", "createTime"),
        "personId",
        "accountId",
    ),
    ("own", "Company", "Account"): (
        "CompanyOwnAccount",
        ("companyId", "accountId", "createTime"),
        "companyId",
        "accountId",
    ),
    ("transfer", "Account", "Account"): (
        "AccountTransferAccount",
        ("fromId", "toId", "amount", "createTime", "orderNum", "comment", "payType", "goodsType"),
        "fromId",
        "toId",
    ),
    ("withdraw", "Account", "Account"): (
        "AccountWithdrawAccount",
        ("fromId", "toId", "amount", "createTime"),
        "fromId",
        "toId",
    ),
    ("deposit", "Loan", "Account"): (
        "LoanDepositAccount",
        ("loanId", "accountId", "amount", "createTime"),
        "loanId",
        "accountId",
    ),
    ("repay", "Account", "Loan"): (
        "AccountRepayLoan",
        ("accountId", "loanId", "amount", "createTime"),
        "accountId",
        "loanId",
    ),
    ("signIn", "Medium", "Account"): (
        "MediumSignInAccount",
        ("mediumId", "accountId", "createTime", "location"),
        "mediumId",
        "accountId",
    ),
    ("apply", "Person", "Loan"): (
        "PersonApplyLoan",
        ("personId", "loanId", "createTime", "org"),
        "personId",
        "loanId",
    ),
    ("apply", "Company", "Loan"): (
        "CompanyApplyLoan",
        ("companyId", "loanId", "createTime", "org"),
        "companyId",
        "loanId",
    ),
    ("invest", "Person", "Company"): (
        "PersonInvestCompany",
        ("investorId", "companyId", "ratio", "createTime"),
        "investorId",
        "companyId",
    ),
    ("invest", "Company", "Company"): (
        "CompanyInvestCompany",
        ("investorId", "companyId", "ratio", "createTime"),
        "investorId",
        "companyId",
    ),
    ("guarantee", "Person", "Person"): (
        "PersonGuaranteePerson",
        ("fromId", "toId", "createTime", "relation"),
        "fromId",
        "toId",
    ),
    ("guarantee", "Company", "Company"): (
        "CompanyGuaranteeCompany",
        ("fromId", "toId", "createTime", "relation"),
        "fromId",
        "toId",
    ),
}
_EPOCH = datetime(1970, 1, 1)


def ldbc_time(millis: int) -> str:
    """``YYYY-MM-DD HH:MM:SS[.f]``: the generator drops trailing fraction zeros."""
    moment = _EPOCH + timedelta(milliseconds=millis)
    text = moment.strftime("%Y-%m-%d %H:%M:%S")
    fraction = f"{moment.microsecond // 1000:03d}".rstrip("0")
    return f"{text}.{fraction}" if fraction else text


def _cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _row(columns: Sequence[str], values: Mapping[str, Any]) -> str:
    return "|".join(_cell(values.get(column)) for column in columns)


def render_snapshot(graph: Mapping[str, Any]) -> dict[str, str]:
    """Every ``snapshot/`` CSV file of the graph, by file name."""
    labels: dict[int, str] = {}
    files: dict[str, list[str]] = {}
    for label, (stem, columns) in NODE_FILES.items():
        files[stem] = ["|".join(columns)]
        table = graph["nodes"][label]
        renames = NODE_COLUMNS[label]
        for row in table["rows"]:
            fixture = dict(zip(table["columns"], row, strict=True))
            labels[fixture["id"]] = label
            values = {renames[name]: value for name, value in fixture.items() if name in renames}
            if "createTime" in values:
                values["createTime"] = ldbc_time(values["createTime"])
            files[stem].append(_row(columns, values))
    for stem, columns, _, _ in EDGE_FILES.values():
        files[stem] = ["|".join(columns)]
    for kind, table in graph["edges"].items():
        for row in table["rows"]:
            fixture = dict(zip(table["columns"], row, strict=True))
            key = (kind, labels[fixture["from"]], labels[fixture["to"]])
            stem, columns, source, target = EDGE_FILES[key]
            values = {
                source: fixture["from"],
                target: fixture["to"],
                "createTime": ldbc_time(fixture["timestamp"]),
            }
            for name in ("amount", "ratio"):
                if name in fixture:
                    values[name] = fixture[name]
            files[stem].append(_row(columns, values))
    return {f"{stem}.csv": "\n".join(lines) + "\n" for stem, lines in files.items()}


def render_parameters(parameters: Mapping[str, Any]) -> dict[str, str]:
    """``complex_<n>_param.csv`` for TCR1-TCR12; all but ``complex_3`` start with ``...``."""
    files = {}
    for operation, names in LDBC_PARAMETERS.items():
        number = operation.removeprefix("TCR")
        lines = [] if number == "3" else ["..."]
        bindings = list(parameters["bindings"][operation])
        if "startTime" in names:
            # Windows whose bounds sit exactly on, and one millisecond inside, the
            # fixture's edge timestamps (1100, 1200, ...): the bounds are open, so
            # an inclusive or shifted window selects other edges.
            bindings += [
                {**bindings[0], "startTime": start, "endTime": end}
                for start, end in ((1100, 1500), (1099, 1501), (1101, 1499))
            ]
        for binding in bindings:
            fields = []
            for name in names:
                value = binding[name]
                fields.append(repr(float(value)) if name.startswith("threshold") else str(value))
            lines.append("|".join(fields))
        files[f"complex_{number}_param.csv"] = "\n".join(lines) + "\n"
    return files


def render() -> dict[str, str]:
    """The committed ``source/`` tree: relative path -> text."""
    graph = json.loads((QUERY_FIXTURE / "graph.json").read_text(encoding="utf-8"))
    parameters = json.loads((QUERY_FIXTURE / "parameters.json").read_text(encoding="utf-8"))
    tree = {f"{SNAPSHOT_DIR}/{name}": text for name, text in render_snapshot(graph).items()}
    tree |= {
        f"{PARAMETER_DIR}/{name}": text for name, text in render_parameters(parameters).items()
    }
    return tree


def write_source(target: Path) -> None:
    for relative, text in render().items():
        path = target / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")


def write_parameter_zip(target: Path, source: Path) -> None:
    """The read-parameter zip, with fixed member timestamps so it is reproducible."""
    import zipfile

    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as bundle:
        for path in sorted((source / PARAMETER_DIR).glob("*.csv")):
            info = zipfile.ZipInfo(f"{PARAMETER_DIR}/{path.name}", (1980, 1, 1, 0, 0, 0))
            info.external_attr = 0o644 << 16
            bundle.writestr(info, path.read_bytes(), zipfile.ZIP_DEFLATED)


if __name__ == "__main__":
    import sys

    if "--zip" in sys.argv:
        write_parameter_zip(
            FIXTURE / "archives" / "finbench-fixture-read-params.zip", FIXTURE / "source"
        )
    else:
        write_source(FIXTURE / "source")
