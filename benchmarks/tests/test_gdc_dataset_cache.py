from __future__ import annotations

import contextlib
import hashlib
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from graphforge_bench import gdc_dataset_cache as cache
from graphforge_bench.gdc_contracts import (
    GdcContractError,
    load_pinned_identity,
    validate_acquisition,
    workspace_root,
)
from jsonschema import Draft202012Validator

ROOT = workspace_root()
ARCHIVE = ROOT / "fixtures" / "gdc" / "dataset-cache" / "tiny-graph.tar.zst"
ARCHIVE_SHA256 = "c3c821ebe9cf61570737dce6b0ed942b76a84f1fbfb2fbcac58b81a39bb27b61"
SOURCE = "https://datasets.ldbcouncil.org/graphalytics/tiny-graph.tar.zst"


class ServedBytes:
    """An opener serving fixed bytes and counting requests, with no network."""

    def __init__(self, payload: bytes, *, fail_after: int | None = None) -> None:
        self.payload = payload
        self.fail_after = fail_after
        self.requests: list[str] = []

    def __call__(self, url: str):
        self.requests.append(url)
        payload = self.payload
        if self.fail_after is not None:
            return _Failing(payload[: self.fail_after])
        return contextlib.closing(io.BytesIO(payload))


class _Failing(io.BytesIO):
    def read(self, size: int = -1) -> bytes:
        data = super().read(size)
        if not data:
            raise OSError("connection reset")
        return data

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _same_device(_path: Path) -> int:
    return 1


class CacheTestCase(unittest.TestCase):
    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.scratch = Path(scratch.name)
        # A stand-in benchmarks root: the real suite declaration, with a scorecard pin
        # naming the committed fixture archive.
        self.root = self.scratch / "benchmarks"
        (self.root / "suites").mkdir(parents=True)
        (self.root / "profiles" / "gdc").mkdir(parents=True)
        shutil.copy(ROOT / "suites" / "gdc-graphalytics.json", self.root / "suites")
        for name in (
            "graphalytics-live-identity.json",
            "graphalytics-static-identity.json",
        ):
            shutil.copy(ROOT / "profiles" / "gdc" / name, self.root / "profiles" / "gdc")
        real = load_pinned_identity(ROOT / "profiles/gdc/graphalytics-scorecard-identity.json")
        self.pin = {
            **real,
            "datasets": [
                {
                    "id": "tiny-graph",
                    "role": "dataset",
                    "checksum_sha256": ARCHIVE_SHA256,
                    "license": "Apache-2.0",
                    "acquisition": "download",
                    "source": SOURCE,
                }
            ],
        }
        self.write_pin(self.pin)
        self.cache_root = self.scratch / "cache"
        self.work_root = self.scratch / "work"
        self.payload = ARCHIVE.read_bytes()

    def write_pin(self, pin: dict) -> None:
        (self.root / "profiles/gdc/graphalytics-scorecard-identity.json").write_text(
            json.dumps(pin, indent=2), encoding="utf-8"
        )

    def acquire(self, opener, **overrides):
        arguments = {
            "suite_path": self.root / "suites" / "gdc-graphalytics.json",
            "profile": "scorecard",
            "dataset_ids": ["tiny-graph"],
            "cache_root": self.cache_root,
            "work_root": self.work_root,
            "repo_root": self.scratch / "repo",
            "root": self.root,
            "opener": opener,
            "device_of": _same_device,
        }
        arguments.update(overrides)
        return cache.acquire(**arguments)

    def leftovers(self) -> list[Path]:
        return sorted(self.cache_root.rglob("*.partial"))


class CommittedFixtureTests(unittest.TestCase):
    def test_fixture_archive_is_the_pinned_bytes(self) -> None:
        self.assertEqual(cache.sha256_file(ARCHIVE), ARCHIVE_SHA256)


class AcquireTests(CacheTestCase):
    def test_good_pin_downloads_verifies_extracts_and_emits_a_valid_document(self) -> None:
        opener = ServedBytes(self.payload)
        result = self.acquire(opener)
        self.assertEqual(opener.requests, [SOURCE])
        archive = result["archive_dir"] / "tiny-graph.tar.zst"
        self.assertEqual(cache.sha256_file(archive), ARCHIVE_SHA256)
        extracted = result["extracted"]["tiny-graph"]
        self.assertEqual((extracted / "tiny-graph.v").read_text(), "1\n2\n3\n")
        self.assertEqual(self.leftovers(), [])
        self.assertEqual(result["evidence"]["status"], "passed")
        self.assertEqual(
            result["evidence"]["datasets"],
            [{"id": "tiny-graph", "checksum_sha256": ARCHIVE_SHA256, "license": "Apache-2.0"}],
        )
        document = json.loads(result["acquisition_path"].read_text(encoding="utf-8"))
        self.assertEqual(document["schema"], "graphforge-gdc-acquisition/1")
        self.assertEqual(document["identity_profile"], "scorecard")
        validate_acquisition(self.pin, document, result["archive_dir"])

    def test_second_acquisition_reuses_the_verified_archive_without_the_network(self) -> None:
        self.acquire(ServedBytes(self.payload))
        offline = ServedBytes(b"must not be requested")
        self.acquire(offline)
        self.assertEqual(offline.requests, [])

    def test_checksum_mismatch_is_typed_unretried_and_leaves_no_partial(self) -> None:
        opener = ServedBytes(self.payload[:-1] + b"\x00")
        with self.assertRaises(cache.DatasetCacheError) as raised:
            self.acquire(opener)
        self.assertEqual(raised.exception.cause, "checksum_mismatch")
        self.assertEqual(len(opener.requests), 1, "a mismatch is never retried")
        self.assertEqual(self.leftovers(), [])
        self.assertEqual(list(self.cache_root.rglob("*.tar.zst")), [])

    def test_interrupted_download_leaves_no_partial_and_no_archive(self) -> None:
        opener = ServedBytes(self.payload, fail_after=40)
        with self.assertRaises(cache.DatasetCacheError) as raised:
            self.acquire(opener)
        self.assertEqual(raised.exception.cause, "download_failed")
        self.assertEqual(len(opener.requests), 1, "a failed download is not retried")
        self.assertEqual(self.leftovers(), [])
        self.assertEqual(list(self.cache_root.rglob("*.tar.zst")), [])

    def test_stale_partial_is_replaced_not_resumed(self) -> None:
        archives = self.cache_root / "graphalytics" / "archives"
        archives.mkdir(parents=True)
        (archives / "tiny-graph.tar.zst.partial").write_bytes(b"stale prefix")
        self.acquire(ServedBytes(self.payload))
        self.assertEqual(self.leftovers(), [])

    def test_cached_archive_that_drifted_from_its_pin_is_a_checksum_mismatch(self) -> None:
        result = self.acquire(ServedBytes(self.payload))
        archive = result["archive_dir"] / "tiny-graph.tar.zst"
        archive.write_bytes(self.payload + b"drift")
        offline = ServedBytes(self.payload)
        with self.assertRaises(cache.DatasetCacheError) as raised:
            self.acquire(offline)
        self.assertEqual(raised.exception.cause, "checksum_mismatch")
        self.assertEqual(offline.requests, [], "drift is reported, not silently re-downloaded")
        self.assertTrue(archive.exists(), "the drifted file is kept for inspection")

    def test_sources_outside_the_ldbc_dataset_host_are_refused_before_any_request(self) -> None:
        for source in (
            "https://example.org/graphalytics/tiny-graph.tar.zst",
            "https://datasets.ldbcouncil.org/graphalytics/tiny-graph.zip",
        ):
            pin = {**self.pin, "datasets": [{**self.pin["datasets"][0], "source": source}]}
            self.write_pin(pin)
            opener = ServedBytes(self.payload)
            with self.assertRaises(cache.DatasetCacheError) as raised:
                self.acquire(opener)
            self.assertEqual(raised.exception.cause, "unsupported_source", source)
            self.assertEqual(opener.requests, [])

    def test_plain_http_sources_are_refused_by_the_pin_schema(self) -> None:
        source = "http://datasets.ldbcouncil.org/graphalytics/tiny-graph.tar.zst"
        self.write_pin({**self.pin, "datasets": [{**self.pin["datasets"][0], "source": source}]})
        opener = ServedBytes(self.payload)
        with self.assertRaises(GdcContractError) as raised:
            self.acquire(opener)
        self.assertEqual(raised.exception.cause, "invalid_document")
        self.assertEqual(opener.requests, [])

    def test_unknown_dataset_is_refused(self) -> None:
        with self.assertRaises(cache.DatasetCacheError) as raised:
            self.acquire(ServedBytes(self.payload), dataset_ids=["other"])
        self.assertEqual(raised.exception.cause, "unknown_dataset")

    def test_corrupt_archive_extraction_fails_typed_and_leaves_no_directory(self) -> None:
        garbage = b"not a zstd stream"
        digest = hashlib.sha256(garbage).hexdigest()
        pin = {**self.pin, "datasets": [{**self.pin["datasets"][0], "checksum_sha256": digest}]}
        self.write_pin(pin)
        with self.assertRaises(cache.DatasetCacheError) as raised:
            self.acquire(ServedBytes(garbage))
        self.assertEqual(raised.exception.cause, "extraction_failed")
        self.assertEqual(list((self.cache_root / "graphalytics").glob("extracted/*")), [])


class ManifestDriftTests(CacheTestCase):
    def emitted(self) -> tuple[dict, Path]:
        result = self.acquire(ServedBytes(self.payload))
        return json.loads(result["acquisition_path"].read_text()), result["archive_dir"]

    def test_recorded_identity_drift_is_refused(self) -> None:
        document, archive_dir = self.emitted()
        document["recorded_driver"] = {**document["recorded_driver"], "release": "converter-v2"}
        with self.assertRaises(GdcContractError) as raised:
            validate_acquisition(self.pin, document, archive_dir)
        self.assertEqual(raised.exception.cause, "identity_drift")

    def test_recorded_checksum_drift_is_refused(self) -> None:
        document, archive_dir = self.emitted()
        document["assets"][0]["checksum_sha256"] = "0" * 64
        with self.assertRaises(GdcContractError) as raised:
            validate_acquisition(self.pin, document, archive_dir)
        self.assertEqual(raised.exception.cause, "checksum_mismatch")

    def test_archive_replaced_after_acquisition_is_refused(self) -> None:
        document, archive_dir = self.emitted()
        (archive_dir / "tiny-graph.tar.zst").write_bytes(b"replaced")
        with self.assertRaises(GdcContractError) as raised:
            validate_acquisition(self.pin, document, archive_dir)
        self.assertEqual(raised.exception.cause, "checksum_mismatch")

    def test_missing_pinned_dataset_is_refused(self) -> None:
        document, archive_dir = self.emitted()
        document["assets"] = []
        with self.assertRaises(GdcContractError) as raised:
            validate_acquisition(self.pin, document, archive_dir)
        self.assertEqual(raised.exception.cause, "missing_assets")


class CacheRootTests(CacheTestCase):
    def require(self, cache_root: Path, **overrides) -> Path:
        arguments = {
            "repo_root": self.scratch / "repo",
            "work_root": self.work_root,
            "device_of": _same_device,
        }
        arguments.update(overrides)
        return cache.require_cache_root(cache_root, **arguments)

    def test_root_on_the_process_root_device_outside_repo_and_work_root_is_accepted(self) -> None:
        resolved = self.require(self.scratch / "new" / "cache")
        self.assertTrue(resolved.is_dir())

    def test_root_on_another_device_is_refused_before_anything_is_created(self) -> None:
        target = self.scratch / "other-device" / "cache"
        root_device = Path("/")

        def device_of(path: Path) -> int:
            return 1 if path == root_device else 2

        with self.assertRaises(cache.DatasetCacheError) as raised:
            self.require(target, device_of=device_of)
        self.assertEqual(raised.exception.cause, "cache_root_wrong_device")
        self.assertFalse(target.exists())

    def test_root_inside_the_repository_is_refused(self) -> None:
        repo = self.scratch / "repo"
        for candidate in (repo, repo / "benchmarks" / "cache"):
            with self.assertRaises(cache.DatasetCacheError) as raised:
                self.require(candidate)
            self.assertEqual(raised.exception.cause, "cache_root_in_repository")

    def test_root_overlapping_the_work_root_is_refused_in_both_directions(self) -> None:
        for candidate in (self.work_root, self.work_root / "cache", self.scratch):
            with self.assertRaises(cache.DatasetCacheError) as raised:
                self.require(candidate)
            self.assertEqual(raised.exception.cause, "cache_root_overlaps_work_root")

    def test_symlinked_path_into_the_repository_is_resolved_and_refused(self) -> None:
        repo = self.scratch / "repo"
        repo.mkdir()
        link = self.scratch / "link"
        link.symlink_to(repo)
        with self.assertRaises(cache.DatasetCacheError) as raised:
            self.require(link / "cache")
        self.assertEqual(raised.exception.cause, "cache_root_in_repository")

    def test_acquire_refuses_a_bad_root_before_requesting_anything(self) -> None:
        opener = ServedBytes(self.payload)
        with self.assertRaises(cache.DatasetCacheError):
            self.acquire(opener, cache_root=self.scratch / "repo" / "cache")
        self.assertEqual(opener.requests, [])


class ScorecardProfileTests(unittest.TestCase):
    """The committed scorecard pin, ladder and mappings agree with each other."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.pin = load_pinned_identity(ROOT / "profiles/gdc/graphalytics-scorecard-identity.json")
        cls.ladder = json.loads(
            (ROOT / "profiles/gdc/graphalytics-scorecard-ladder.json").read_text(encoding="utf-8")
        )

    def test_pin_names_the_four_archives_with_recorded_digests(self) -> None:
        by_id = {item["id"]: item for item in self.pin["datasets"]}
        self.assertEqual(
            sorted(by_id), ["cit-Patents", "datagen-7_5-fb", "graph500-22", "wiki-Talk"]
        )
        for name, item in by_id.items():
            self.assertEqual(
                item["source"], f"https://datasets.ldbcouncil.org/graphalytics/{name}.tar.zst"
            )
            self.assertRegex(item["checksum_sha256"], r"^[a-f0-9]{64}$")
            self.assertEqual(item["acquisition"], "download")
        self.assertEqual(len({item["checksum_sha256"] for item in by_id.values()}), 4)

    def test_ladder_validates_and_carries_the_published_counts(self) -> None:
        schema = json.loads(
            (ROOT / "schemas/gdc-graphalytics-scorecard-ladder.json").read_text(encoding="utf-8")
        )
        Draft202012Validator(schema).validate(self.ladder)
        counts = {
            item["id"]: (item["vertices"], item["edges"], item["directed"], item["weighted"])
            for item in self.ladder["datasets"]
        }
        self.assertEqual(
            counts,
            {
                "wiki-Talk": (2_394_385, 5_021_410, True, False),
                "cit-Patents": (3_774_768, 16_518_948, True, False),
                "datagen-7_5-fb": (633_432, 34_185_747, False, True),
                "graph500-22": (2_396_657, 64_155_735, False, False),
            },
        )
        self.assertEqual(
            self.ladder["counts_source"],
            "https://ldbcouncil.org/benchmarks/graphalytics/datasets/",
        )
        self.assertEqual([item["order"] for item in self.ladder["datasets"]], [1, 2, 3, 4])

    def test_ladder_datasets_are_exactly_the_pinned_datasets_with_the_same_source(self) -> None:
        pinned = {item["id"]: item["source"] for item in self.pin["datasets"]}
        laddered = {item["id"]: item["archive_url"] for item in self.ladder["datasets"]}
        self.assertEqual(pinned, laddered)

    def test_each_mapping_matches_its_dataset_shape(self) -> None:
        for item in self.ladder["datasets"]:
            mapping = json.loads((ROOT / item["load_mapping"]).read_text(encoding="utf-8"))
            self.assertEqual(mapping["schema"], "graphforge-gdc-load-mapping/1")
            (nodes,) = mapping["node_tables"]
            (edges,) = mapping["edge_tables"]
            self.assertEqual(nodes["files"], [f"{item['id']}.v"])
            self.assertEqual(edges["files"], [f"{item['id']}.e"])
            weight = [p["column"] for p in edges["properties"]]
            self.assertEqual(weight, ["weight"] if item["weighted"] else [])


if __name__ == "__main__":
    unittest.main()
