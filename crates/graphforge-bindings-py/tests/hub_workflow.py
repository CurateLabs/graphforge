"""Native Hub publish/1 + discovery/1 collaboration via the typed Python facade.

Build with --features hub-test-support. The feature injects ReferenceHub at the
native transport boundary; these tests never replace _cli_execute or its output.
"""

from contextlib import closing
from itertools import count
import json
import os
from pathlib import Path
import tempfile
import unittest

from core_version_warning import _relabel_generation
import pyarrow as pa

from graphforge import GraphForge, HubProject, HubWorkflowError
from graphforge import _graphforge_rs as native

REPOSITORY = "https://hub.test/curate/claims"
AUTHOR = "Alice <alice@example.org>"
BOB = "Bob <bob@example.org>"
COUNTER = count(1)


def operation():
    return f"018f0f4e-7b8c-7000-8000-{next(COUNTER):012x}"


class HubWorkflowTests(unittest.TestCase):
    def setUp(self):
        # Explicit fixture activation is per native thread, reset for every test.
        os.environ["GF_WORKFLOW_TEST_TOKEN"] = native._test_hub_start()
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.addCleanup(native._test_hub_stop)
        self.addCleanup(os.environ.pop, "GF_WORKFLOW_TEST_TOKEN", None)

    def project(self, name):
        return HubProject(Path(self.root.name) / name, token_env="GF_WORKFLOW_TEST_TOKEN")

    def seed(self):
        source = self.project("source")
        with closing(GraphForge(source.path)) as graph:
            graph.execute("CREATE (:Item {score:0})")
        source.branch("main", author=AUTHOR)
        receipt = source.publish(REPOSITORY, operation_uuid=operation())
        self.assertEqual(receipt["repository"], {"owner": "curate", "repository": "claims"})
        self.assertEqual(receipt["refs"], ["main"])
        return source

    def test_clone_commit_tag_push_pull_log_and_reopened_arrow_data(self):
        self.seed()
        alice = self.project("alice")
        cloned = alice.clone(REPOSITORY, ref="main")
        self.assertEqual(cloned["contract"], "graphforge-hub-clone/1")
        self.assertEqual(cloned["destination"], alice.path)
        self.assertEqual(cloned["resolved_ref"], "main")
        before = alice.log(remote=True)
        self.assertEqual(cloned["research_version_uuid"], before[0]["version_uuid"])

        identity = operation()
        first = alice.commit(
            "First score",
            "MATCH (n:Item) SET n.score=1",
            author=AUTHOR,
            committer=AUTHOR,
            operation_uuid=identity,
        )
        replay = alice.commit(
            "First score",
            "MATCH (n:Item) SET n.score=1",
            author=AUTHOR,
            committer=AUTHOR,
            operation_uuid=identity,
        )
        self.assertEqual(replay["version_uuid"], first["version_uuid"])
        self.assertEqual(replay["generation_uuid"], first["generation_uuid"])
        self.assertTrue(replay["replayed"])
        second = alice.commit(
            "Second score", "MATCH (n:Item) SET n.score=2", author="Alice", committer="Alice"
        )
        self.assertEqual(second["parents"], [first["version_uuid"]])
        tagged = alice.tag("v1")
        self.assertEqual(tagged["version_uuid"], second["version_uuid"])
        publish_id = operation()
        pushed = alice.push(operation_uuid=publish_id)
        self.assertEqual(alice.push(operation_uuid=publish_id), pushed)
        self.assertEqual(pushed["operation_uuid"], publish_id)
        self.assertEqual(alice.log("v1")[0]["version_uuid"], second["version_uuid"])

        bob = self.project("bob")
        bob.clone(REPOSITORY)
        collaborator = bob.commit("Collaborator score", "MATCH (n:Item) SET n.score=3", author=BOB)
        bob.push()
        pull_id = operation()
        pulled = alice.pull(author=AUTHOR, operation_uuid=pull_id)
        self.assertIn(collaborator["version_uuid"], pulled["parents"])
        self.assertEqual(pulled["conflicts"], [])
        self.assertIsInstance(pulled["incorporated"], int)
        self.assertTrue(alice.pull(author=AUTHOR, operation_uuid=pull_id)["replayed"])
        self.assertTrue(alice.pull(author=AUTHOR)["up_to_date"])

        history = alice.log(all=True)
        messages = [entry["description"] for entry in history]
        for message in ["First score", "Second score", "Collaborator score"]:
            self.assertIn(message, messages)
        self.assertEqual(history[0]["version_uuid"], pulled["version_uuid"])
        local_first = next(row for row in history if row["version_uuid"] == first["version_uuid"])
        self.assertEqual(local_first["author"]["name"], "Alice")
        self.assertEqual(local_first["committer"]["email"], "alice@example.org")
        local_second = next(row for row in history if row["version_uuid"] == second["version_uuid"])
        self.assertEqual(local_second["author"], {"name": "Alice"})
        self.assertEqual(local_second["committer"], {"name": "Alice"})
        remote = alice.log(remote=True, all=True, max_count=2)
        self.assertEqual(remote[0]["version_uuid"], collaborator["version_uuid"])
        self.assertEqual(len(remote), 2)

        # New native handle, not a wrapper cache: durable selected-Branch data,
        # historical data and immutable tag identity all survive reopening.
        with closing(GraphForge(alice.path)) as reopened:
            current = reopened.research_version_retention()["current_branch"]
            table = reopened.query_research_branch(
                current, "MATCH (n:Item) RETURN n.score AS score"
            )
            self.assertIsInstance(table, pa.Table)
            self.assertEqual(table["score"].to_pylist(), [3])
            old = reopened.query_research_version(
                first["version_uuid"], "MATCH (n:Item) RETURN n.score AS score"
            )
            self.assertEqual(old["score"].to_pylist(), [1])
        tagged_clone = self.project("tagged")
        self.assertEqual(
            tagged_clone.clone(REPOSITORY, tag="v1")["research_version_uuid"],
            second["version_uuid"],
        )

    def test_log_retains_typed_entries_and_native_version_warnings(self):
        source = self.seed()
        _relabel_generation(Path(source.path), "0.0.1")
        history = source.log()
        self.assertIsInstance(history, list)
        self.assertTrue(history[0]["version_uuid"])
        self.assertEqual(source.warnings[0]["code"], "GF_CORE_VERSION_MISMATCH")
        self.assertEqual(source.warnings[0]["producing_core_version"], "0.0.1")

    def test_errors_retain_authoritative_native_codes_and_original_envelope(self):
        source = self.seed()
        identity = operation()
        source.commit("one", "MATCH (n:Item) SET n.score=1", author=AUTHOR, operation_uuid=identity)
        with self.assertRaises(HubWorkflowError) as conflict:
            source.commit(
                "different", "MATCH (n:Item) SET n.score=2", author=AUTHOR, operation_uuid=identity
            )
        error = conflict.exception
        self.assertEqual(error.native_code, "GF_IDEMPOTENCY_CONFLICT")
        self.assertEqual(error.envelope, json.loads(error.stderr))
        self.assertEqual(error.code, error.details.get("semantic_code", error.native_code))
        self.assertNotEqual(error.exit_code, 0)

        # Identical native arguments yield the identical authoritative refusal.
        status, stdout, stderr = native._cli_execute(
            [
                "gf",
                "--json",
                f"--project={source.path}",
                "commit",
                "--message=different",
                "--query=MATCH (n:Item) SET n.score=2",
                f"--author={AUTHOR}",
                f"--operation-uuid={identity}",
            ]
        )
        self.assertEqual((error.exit_code, error.stdout, error.stderr), (status, stdout, stderr))
        with self.assertRaises(HubWorkflowError) as refused:
            self.project("unsafe").clone("http://127.0.0.1/private")
        self.assertEqual(refused.exception.code, "hub.unsafe_location")
        with self.assertRaises(HubWorkflowError) as missing:
            self.project("absent").clone("https://hub.test/curate/missing")
        self.assertEqual(missing.exception.code, "hub.not_found")


if __name__ == "__main__":
    unittest.main()
