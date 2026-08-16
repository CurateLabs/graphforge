"""Native acceptance for transaction and maintenance parity (#755)."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile

import graphforge as g
from graphforge._graphforge_rs import _cli_execute

OP_COMMIT = "018f0f4e-7b8c-7000-8000-000000007501"
OP_ROLLBACK = "018f0f4e-7b8c-7000-8000-000000007502"
OP_DROP = "018f0f4e-7b8c-7000-8000-000000007503"
OP_CLI = "018f0f4e-7b8c-7000-8000-000000007504"
OP_CERT = "018f0f4e-7b8c-7000-8000-000000007560"
NODE_BULK = "018f0f4e-7b8c-7000-8000-000000007511"
NODE_GHOST = "018f0f4e-7b8c-7000-8000-000000007512"
NODE_CERT = "018f0f4e-7b8c-7000-8000-000000007561"


def check_mixed_commit_and_rollback(project: Path) -> None:
    forge = g.GraphForge(str(project))
    before = forge.project_open_recovery()["selected_generation_uuid"]

    tx = forge.begin_transaction(operation_uuid=OP_COMMIT)
    status = tx.status()
    assert status["phase"] == "open"
    assert status["committed"] is False
    tx.stage_cypher("CREATE (:Person {name: 'Cypher'})")
    tx.stage_add_node(NODE_BULK, "Person", {"name": "Bulk"})
    tx.validate()
    generation = tx.commit()
    assert generation != before
    names = (
        forge.execute("MATCH (n:Person) RETURN n.name AS name ORDER BY name")
        .column("name")
        .to_pylist()
    )
    assert names == ["Bulk", "Cypher"]

    rolled = forge.begin_transaction(operation_uuid=OP_ROLLBACK)
    rolled.stage_add_node(NODE_GHOST, "Person", {"name": "Ghost"})
    rolled.rollback()
    names = (
        forge.execute("MATCH (n:Person) RETURN n.name AS name ORDER BY name")
        .column("name")
        .to_pylist()
    )
    assert names == ["Bulk", "Cypher"]


def check_dropped_handle_never_commits(project: Path) -> None:
    forge = g.GraphForge(str(project))
    before = forge.execute("MATCH (n:Person) RETURN count(n) AS c").column("c").to_pylist()[0]
    tx = forge.begin_transaction(operation_uuid=OP_DROP)
    tx.stage_cypher("CREATE (:Person {name: 'Dropped'})")
    del tx
    after = forge.execute("MATCH (n:Person) RETURN count(n) AS c").column("c").to_pylist()[0]
    assert after == before
    reopened = g.GraphForge(str(project))
    names = (
        reopened.execute("MATCH (n:Person) RETURN n.name AS name ORDER BY name")
        .column("name")
        .to_pylist()
    )
    assert "Dropped" not in names


def check_maintenance_preview_execute_reconcile(project: Path) -> None:
    forge = g.GraphForge(str(project))
    recovery = forge.project_open_recovery()
    assert "selected_generation_uuid" in recovery
    preview = forge.preview_project_cleanup(retained_ancestors=2)
    execute = forge.execute_project_cleanup(retained_ancestors=2)
    assert preview["candidates"] == execute["candidates"]
    assert preview["reachable_count"] == execute["reachable_count"]
    assert [entry["generation_uuid"] for entry in preview["entries"]] == [
        entry["generation_uuid"] for entry in execute["entries"]
    ]
    status = forge.graph_delta_compaction_status()
    assert isinstance(status["run_count"], int)
    assert status["run_count"] >= 0


def check_cli_parity(project: Path) -> None:
    code, stdout, _stderr = _cli_execute(
        [
            "gf",
            "--project",
            str(project),
            "--json",
            "transaction",
            "commit",
            "--operation-uuid",
            OP_CLI,
            "--cypher",
            "CREATE (:Person {name: 'Cli'})",
        ]
    )
    assert code == 0, stdout
    payload = json.loads(stdout.decode())
    assert payload["phase"] == "committed"
    code, stdout, _stderr = _cli_execute(
        [
            "gf",
            "--project",
            str(project),
            "--json",
            "recovery",
        ]
    )
    assert code == 0, stdout
    recovery = json.loads(stdout.decode())
    assert "selected_generation_uuid" in recovery


def check_certification_observation_parity(project: Path) -> None:
    """Public surface observation agreement for #756 (generation + recovery)."""
    forge = g.GraphForge(str(project))
    tx = forge.begin_transaction(operation_uuid=OP_CERT)
    tx.stage_add_node(NODE_CERT, "Person", {"name": "Cert"})
    generation = tx.commit()
    recovery = forge.project_open_recovery()
    assert recovery["selected_generation_uuid"] == generation
    reopened = g.GraphForge(str(project))
    again = reopened.project_open_recovery()
    assert again["selected_generation_uuid"] == generation
    code, stdout, _stderr = _cli_execute(
        [
            "gf",
            "--project",
            str(project),
            "--json",
            "recovery",
        ]
    )
    assert code == 0, stdout
    cli_recovery = json.loads(stdout.decode())
    assert cli_recovery["selected_generation_uuid"] == generation


def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        project = Path(directory)
        check_mixed_commit_and_rollback(project)
        check_dropped_handle_never_commits(project)
        check_maintenance_preview_execute_reconcile(project)
        check_cli_parity(project)
        check_certification_observation_parity(project)
    print("transaction_parity: ok")


if __name__ == "__main__":
    main()
