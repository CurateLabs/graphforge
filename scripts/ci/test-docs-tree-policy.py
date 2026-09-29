#!/usr/bin/env python3
"""Tests for scripts/ci/docs-tree-policy.py (#1625).

Each rule is proven against a known positive on a synthetic tree, then the real
tree is checked so a clean run is a fact about the repository, not an artifact
of the check.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("docs-tree-policy.py")
spec = importlib.util.spec_from_file_location("docs_tree_policy", SCRIPT)
assert spec is not None and spec.loader is not None
policy = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = policy
spec.loader.exec_module(policy)


def write(root: Path, relative: str, content: bytes | str = "x\n") -> str:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, str):
        content = content.encode()
    path.write_bytes(content)
    return relative


class SyntheticTreeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.files: list[str] = []

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def add(self, relative: str, content: bytes | str = "x\n") -> str:
        self.files.append(write(self.root, relative, content))
        return relative

    def test_prose_images_and_allowlisted_data_pass(self) -> None:
        self.add("docs/index.md")
        self.add("docs/guide/a.png", b"\x89PNG")
        self.add("docs/stylesheets/extra.css")
        self.add("docs/contracts/thing-v1.schema.json", "{}")
        self.add("docs/contracts/examples/thing.yaml")
        self.add("docs/reference/discovery/v1/conformance.json", "{}")
        self.add("docs/reference/knowledge-schema-inventory.sha256")
        self.add("docs/releases/records/v0.6.0-artifacts.json", "{}")
        self.add("docs/reference/BUILD.bazel")
        result = policy.census(self.root, self.files)
        self.assertEqual(result.evidence_shaped, [])
        self.assertEqual(result.oversize, [])
        self.assertEqual(result.problems, [])

    def test_evidence_shaped_files_fail_outside_the_allowlist(self) -> None:
        bad = [
            self.add("docs/development/evidence/run-1234.json", "{}"),
            self.add("docs/development/perf.log"),
            self.add("docs/development/MANIFEST.sha256"),
            self.add("docs/book/receipt.jsonl"),
            self.add("docs/development/candidate.patch.gz", b"\x1f\x8b"),
        ]
        result = policy.census(self.root, self.files)
        self.assertEqual(result.evidence_shaped, bad)
        self.assertTrue(all("evidence-shaped" in p for p in result.problems))

    def test_schema_directory_outside_docs_is_not_checked(self) -> None:
        self.add("scripts/ci/schemas/g500-certification.schema.json", "{}")
        self.add("benchmarks/tests/fixtures/rungs/s20-result.json", "{}")
        result = policy.census(self.root, self.files)
        self.assertEqual(result.docs_files, 0)
        self.assertEqual(result.problems, [])

    def test_oversize_file_fails_even_when_prose(self) -> None:
        big = self.add("docs/book/huge.md", b"x" * (policy.MAX_FILE_BYTES + 1))
        self.add("docs/book/fine.md", b"x" * policy.MAX_FILE_BYTES)
        result = policy.census(self.root, self.files)
        self.assertEqual([path for path, _ in result.oversize], [big])

    def test_unreferenced_development_doc_is_reported(self) -> None:
        self.add("docs/development/linked.md")
        self.add("docs/development/orphan.md")
        self.add("docs/development/self-only.md", "see self-only.md")
        self.add("docs/README.md", "[linked](development/linked.md)")
        result = policy.census(self.root, self.files)
        self.assertEqual(
            result.unreferenced,
            ["docs/development/orphan.md", "docs/development/self-only.md"],
        )

    def test_reference_from_any_tracked_text_source_counts(self) -> None:
        self.add("docs/development/from-makefile.md")
        self.add("docs/development/from-site.md")
        self.add("docs/development/from-agents.md")
        self.add("docs/development/from-binary-only.md")
        self.add("Makefile", "see docs/development/from-makefile.md")
        self.add("docs-site/scripts/sync-content.mjs", "'development/from-site.md',")
        self.add("AGENTS.md", "from-agents.md")
        self.add("docs/guide/pic.png", b"from-binary-only.md")
        result = policy.census(self.root, self.files)
        self.assertEqual(result.unreferenced, ["docs/development/from-binary-only.md"])

    def test_subdirectories_of_development_are_not_development_docs(self) -> None:
        self.add("docs/development/archive/old.md")
        self.add("docs/development/sessions/log.md")
        result = policy.census(self.root, self.files)
        self.assertEqual(result.development_docs, 0)
        self.assertEqual(result.unreferenced, [])


class RealTreeTests(unittest.TestCase):
    def test_repository_passes(self) -> None:
        result = policy.census(policy.ROOT)
        self.assertGreater(result.docs_files, 0)
        self.assertGreater(result.development_docs, 0)
        self.assertEqual(result.problems, [])

    def test_real_tree_catches_a_planted_dump(self) -> None:
        planted = "docs/development/planted-9999.json"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root, planted, "{}")
            write(root, "docs/development/topic.md", "planted")
            write(root, "docs/README.md", "[topic](development/topic.md)")
            result = policy.census(root, [planted, "docs/development/topic.md", "docs/README.md"])
        self.assertEqual(result.evidence_shaped, [planted])
        self.assertEqual(result.unreferenced, [])


if __name__ == "__main__":
    unittest.main()
