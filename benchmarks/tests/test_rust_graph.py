from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import tempfile
import unittest

from graphforge_bench.check_rust_graph import validate_metadata


class RustGraphTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name) / "benchmarks"
        self.root.mkdir()
        self.metadata = {
            "packages": [
                self.package("runner", self.root / "runners" / "tiny" / "Cargo.toml", []),
                self.package(
                    "graphforge-api",
                    self.root.parent / "crates" / "graphforge-api" / "Cargo.toml",
                    None,
                ),
                self.package(
                    "graphforge-core",
                    self.root.parent / "crates" / "graphforge-core" / "Cargo.toml",
                    None,
                ),
            ],
            "resolve": {
                "nodes": [
                    {"id": "runner", "deps": [{"pkg": "graphforge-api"}]},
                    {"id": "graphforge-api", "deps": [{"pkg": "graphforge-core"}]},
                    {"id": "graphforge-core", "deps": []},
                ]
            },
        }

    @staticmethod
    def package(name: str, manifest: Path, publish: list[str] | None) -> dict:
        return {
            "id": name,
            "name": name,
            "source": None,
            "manifest_path": str(manifest),
            "publish": publish,
        }

    def test_facade_and_its_engine_dependencies_are_allowed(self) -> None:
        self.assertEqual(validate_metadata(self.metadata, self.root), 3)

    def test_isolated_benchmark_without_engine_is_allowed(self) -> None:
        metadata = deepcopy(self.metadata)
        metadata["packages"] = metadata["packages"][:1]
        metadata["resolve"]["nodes"] = [{"id": "runner", "deps": []}]
        self.assertEqual(validate_metadata(metadata, self.root), 1)

    def test_direct_internal_dependency_is_rejected_even_in_facade_closure(self) -> None:
        self.metadata["resolve"]["nodes"][0]["deps"].append({"pkg": "graphforge-core"})
        with self.assertRaisesRegex(RuntimeError, "bypasses approved engine boundary"):
            validate_metadata(self.metadata, self.root)

    def test_unrelated_local_crate_is_rejected(self) -> None:
        self.metadata["packages"].append(
            self.package("unrelated", self.root.parent / "crates" / "unrelated" / "Cargo.toml", [])
        )
        with self.assertRaisesRegex(RuntimeError, "escapes public engine graph"):
            validate_metadata(self.metadata, self.root)

    def test_transitive_dependency_outside_repository_crates_is_rejected(self) -> None:
        self.metadata["packages"][2]["manifest_path"] = str(
            self.root.parent / "foreign" / "Cargo.toml"
        )
        with self.assertRaisesRegex(RuntimeError, "escapes public engine graph"):
            validate_metadata(self.metadata, self.root)

    def test_facade_name_cannot_authorize_another_path(self) -> None:
        self.metadata["packages"][1]["manifest_path"] = str(
            self.root.parent / "foreign" / "Cargo.toml"
        )
        with self.assertRaisesRegex(RuntimeError, "bypasses approved engine boundary"):
            validate_metadata(self.metadata, self.root)

    def test_symlinked_engine_escape_is_rejected(self) -> None:
        crates = self.root.parent / "crates"
        crates.mkdir()
        foreign = self.root.parent / "foreign"
        foreign.mkdir()
        (crates / "graphforge-core").symlink_to(foreign, target_is_directory=True)
        with self.assertRaisesRegex(RuntimeError, "escapes public engine graph"):
            validate_metadata(self.metadata, self.root)

    def test_benchmark_packages_remain_unpublishable(self) -> None:
        self.metadata["packages"][0]["publish"] = None
        with self.assertRaisesRegex(RuntimeError, "benchmark package is publishable"):
            validate_metadata(self.metadata, self.root)

    def add_infrastructure(self, runner: str, directory: str, dependency: str) -> None:
        self.metadata["packages"].extend(
            [
                self.package(runner, self.root / "runners" / directory / "Cargo.toml", []),
                self.package(
                    dependency,
                    self.root.parent / "crates" / dependency / "Cargo.toml",
                    None,
                ),
            ]
        )
        self.metadata["resolve"]["nodes"].extend(
            [
                {"id": runner, "deps": [{"pkg": dependency}]},
                {"id": dependency, "deps": []},
            ]
        )

    def test_existing_certifier_storage_attribution_boundary_is_allowed(self) -> None:
        self.add_infrastructure("graphforge-benchmark-certify", "certify", "graphforge-storage")
        self.assertEqual(validate_metadata(self.metadata, self.root), 5)

    def test_existing_generator_filesystem_helper_is_allowed(self) -> None:
        self.add_infrastructure(
            "graphforge-benchmark-graph500-generator", "graph500-generator", "graphforge-filesystem"
        )
        self.assertEqual(validate_metadata(self.metadata, self.root), 5)

    def test_infrastructure_name_does_not_authorize_another_runner_path(self) -> None:
        self.add_infrastructure("graphforge-benchmark-certify", "other", "graphforge-storage")
        with self.assertRaisesRegex(RuntimeError, "bypasses approved engine boundary"):
            validate_metadata(self.metadata, self.root)
