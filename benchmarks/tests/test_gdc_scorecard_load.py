"""Convert a committed fixture and load it through the public `gf` CLI.

The converter binary and `gf` are built on demand (or taken from
GRAPHFORGE_GDC_SCORECARD_BIN / GRAPHFORGE_GF_BIN), exactly like the other
runner-backed suites. Counts are read back from a reopened project through
`gf query`, never from the converter's own manifest.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time
import unittest
import uuid

from graphforge_bench.gdc_contracts import workspace_root

ROOT = workspace_root()
REPOSITORY = ROOT.parent
FIXTURE = ROOT / "fixtures" / "gdc" / "load-fixture"
CONVERTER = "graphforge-benchmark-gdc-scorecard"


def _build(manifest: Path, package: str, binary: str, target: Path) -> Path:
    path = target / "debug" / binary
    completed = subprocess.run(
        ["cargo", "build", "--locked", "--manifest-path", str(manifest), "-p", package]
        + (["--bin", binary] if package == "graphforge-cli" else []),
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "CARGO_TARGET_DIR": str(target)},
    )
    if completed.returncode != 0:
        raise AssertionError(f"failed to build {package}\n{completed.stdout}\n{completed.stderr}")
    assert path.is_file(), path
    return path


def converter_binary() -> Path:
    override = os.environ.get("GRAPHFORGE_GDC_SCORECARD_BIN")
    if override:
        return Path(override)
    return _build(
        ROOT / "Cargo.toml", "graphforge-benchmark-gdc-scorecard", CONVERTER, ROOT / "target"
    )


def gf_binary() -> Path:
    override = os.environ.get("GRAPHFORGE_GF_BIN")
    if override:
        return Path(override)
    target = Path(os.environ.get("CARGO_TARGET_DIR", REPOSITORY / "target"))
    candidate = target / "debug" / "gf"
    if candidate.is_file():
        return candidate
    return _build(REPOSITORY / "Cargo.toml", "graphforge-cli", "gf", target)


def uuid7() -> str:
    """A UUIDv7 operation identity (import requires version 7)."""
    millis = int(time.time() * 1000)
    value = (millis << 80) | (0x7 << 76) | (secrets.randbits(12) << 64)
    value |= (0b10 << 62) | secrets.randbits(62)
    return str(uuid.UUID(int=value))


class Gf:
    """Drives the `gf` binary against one project directory."""

    def __init__(self, binary: Path, project: Path) -> None:
        self.binary = binary
        self.project = project

    def run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        completed = subprocess.run(
            [str(self.binary), "--json", "--project", str(self.project), *args],
            check=False,
            capture_output=True,
            text=True,
        )
        if check and completed.returncode != 0:
            raise AssertionError(f"gf {args} failed: {completed.stdout}{completed.stderr}")
        return completed

    def load(self, converted: Path) -> dict:
        session = json.loads(
            self.run("import-session", "begin", "--operation-uuid", uuid7()).stdout
        )["session_uuid"]
        for kind, directory in (("nodes", "nodes"), ("edges", "edges")):
            for path in sorted((converted / directory).glob("*.parquet")):
                self.run(
                    "import-session",
                    "register-parquet",
                    "--session-uuid",
                    session,
                    "--path",
                    str(path),
                    "--kind",
                    kind,
                )
        self.run("import-session", "validate", "--session-uuid", session)
        return json.loads(self.run("import-session", "commit", "--session-uuid", session).stdout)

    def count(self, cypher: str, scratch: Path) -> int:
        """Run a counting query against a freshly opened project."""
        output = scratch / f"result-{secrets.token_hex(4)}.arrow"
        receipt = json.loads(
            self.run(
                "query", "--format", "arrow-ipc", "--cypher", cypher, "--output", str(output)
            ).stdout
        )
        self.assertion_rows(receipt)
        return receipt["scalar_u64"]

    @staticmethod
    def assertion_rows(receipt: dict) -> None:
        assert receipt["complete"] is True and receipt["rows"] == 1, receipt


class ScorecardLoadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.converter = converter_binary()
        cls.gf = gf_binary()

    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.scratch = Path(scratch.name)

    def convert(self, mapping: Path, input_root: Path, output: Path):
        return subprocess.run(
            [
                str(self.converter),
                "convert",
                "--mapping",
                str(mapping),
                "--input-root",
                str(input_root),
                "--output-dir",
                str(output),
            ],
            check=False,
            capture_output=True,
            text=True,
        )

    def test_fixture_converts_loads_reopens_and_reads_back_exact_counts(self) -> None:
        converted = self.scratch / "converted"
        completed = self.convert(FIXTURE / "mapping.json", FIXTURE, converted)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        manifest = json.loads((converted / "conversion-manifest.json").read_text())
        self.assertEqual(manifest["schema"], "graphforge-gdc-conversion-manifest/1")

        project = self.scratch / "project"
        committed = Gf(self.gf, project).load(converted)
        self.assertEqual(committed["outcome"], "committed")

        # A new process reopens the committed project through the public CLI.
        reopened = Gf(self.gf, project)
        count = lambda cypher: reopened.count(cypher, self.scratch)  # noqa: E731
        self.assertEqual(count("MATCH (n:Vertex) RETURN count(n)"), 5)
        self.assertEqual(count("MATCH (n:Person) RETURN count(n)"), 3)
        self.assertEqual(count("MATCH (n:Place) RETURN count(n)"), 2)
        self.assertEqual(count("MATCH (n:Organisation) RETURN count(n)"), 1)
        self.assertEqual(count("MATCH (n) RETURN count(n)"), 11)
        self.assertEqual(count("MATCH ()-[r:LINK]->() RETURN count(r)"), 4)
        self.assertEqual(count("MATCH ()-[r:KNOWS]->() RETURN count(r)"), 3)
        self.assertEqual(count("MATCH ()-[r]->() RETURN count(r)"), 7)
        # Graphalytics vertices 4 and 5 appear in .v but in no .e line.
        self.assertEqual(count("MATCH (n:Vertex) WHERE NOT (n)--() RETURN count(n)"), 2)
        self.assertEqual(count("MATCH (n:Vertex {id: 5}) WHERE NOT (n)--() RETURN count(n)"), 1)
        # One sample node, every typed property including a null.
        self.assertEqual(
            count(
                "MATCH (n:Person {id: 3, first_name: 'Carol', score: 3.5, active: true}) "
                "WHERE n.age IS NULL RETURN count(n)"
            ),
            1,
        )
        self.assertEqual(
            count("MATCH (n:Person {id: 1, first_name: 'Alice', age: 30}) RETURN count(n)"), 1
        )
        # One sample edge with its properties, one with a null property.
        self.assertEqual(
            count(
                "MATCH (:Person {id: 1})-[r:KNOWS {strength: 5}]->(:Person {id: 2}) "
                "WHERE r.creation_date = '2020-01-01T00:00:00.000+0000' RETURN count(r)"
            ),
            1,
        )
        self.assertEqual(
            count(
                "MATCH (:Person {id: 1})-[r:KNOWS]->(:Person {id: 3}) "
                "WHERE r.strength IS NULL RETURN count(r)"
            ),
            1,
        )
        self.assertEqual(
            count(
                "MATCH (:Vertex {id: 1})-[r:LINK {weight: 0.5}]->(:Vertex {id: 2}) RETURN count(r)"
            ),
            1,
        )

    def test_converter_rejects_a_duplicate_label_and_id_with_a_typed_error(self) -> None:
        source = self.scratch / "input"
        source.mkdir()
        (source / "person.csv").write_text("id|name\n1|a\n2|b\n1|c\n")
        mapping = self.scratch / "mapping.json"
        mapping.write_text(
            json.dumps(
                {
                    "schema": "graphforge-gdc-load-mapping/1",
                    "node_tables": [
                        {
                            "id": "person",
                            "format": "ldbc-csv",
                            "files": ["person.csv"],
                            "label": "Person",
                            "id_column": "id",
                            "properties": [{"column": "name", "type": "string"}],
                        }
                    ],
                }
            )
        )
        completed = self.convert(mapping, source, self.scratch / "out")
        self.assertEqual(completed.returncode, 2)
        error = json.loads(completed.stderr)["error"]
        self.assertEqual(error["cause"], "duplicate_node_identity")
        self.assertIn("(Person, 1)", error["message"])
        self.assertFalse((self.scratch / "out" / "conversion-manifest.json").exists())

    def test_import_refuses_node_identities_registered_twice(self) -> None:
        converted = self.scratch / "converted"
        self.assertEqual(self.convert(FIXTURE / "mapping.json", FIXTURE, converted).returncode, 0)
        gf = Gf(self.gf, self.scratch / "project")
        session = json.loads(gf.run("import-session", "begin", "--operation-uuid", uuid7()).stdout)[
            "session_uuid"
        ]
        place = converted / "nodes" / "place.parquet"
        for _ in range(2):
            gf.run(
                "import-session", "register-parquet", "--session-uuid", session,
                "--path", str(place), "--kind", "nodes",
            )  # fmt: skip
        refused = gf.run("import-session", "validate", "--session-uuid", session, check=False)
        self.assertNotEqual(refused.returncode, 0)
        self.assertIn("duplicate identity", refused.stdout + refused.stderr)

    def test_conversion_is_reproducible_across_runs(self) -> None:
        digests = []
        for name in ("one", "two"):
            out = self.scratch / name
            self.assertEqual(self.convert(FIXTURE / "mapping.json", FIXTURE, out).returncode, 0)
            manifest = json.loads((out / "conversion-manifest.json").read_text())
            digests.append((manifest["mapping_sha256"], manifest["inputs"], manifest["outputs"]))
        self.assertEqual(digests[0], digests[1])


if __name__ == "__main__":
    unittest.main()
