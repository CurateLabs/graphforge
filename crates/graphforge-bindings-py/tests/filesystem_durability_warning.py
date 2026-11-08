"""Runtime warning count and typed filesystem qualification on the real binding."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import uuid
import warnings

import graphforge as gf


def check_import_warning_policy(root: Path, package: str) -> None:
    for existing in (False, True):
        target = root / f"warning-policy-{existing}"
        if existing:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", gf.FilesystemDurabilityWarning)
                gf.GraphForge(str(target)).close()
        before = {
            str(path.relative_to(target)): path.read_bytes()
            for path in target.rglob("*")
            if path.is_file()
        }
        operation = str(uuid.uuid4())
        with warnings.catch_warnings():
            warnings.simplefilter("error", gf.FilesystemDurabilityWarning)
            try:
                gf.GraphForge.import_portable(
                    project_root=str(target), input=package, operation_id=operation
                )
            except gf.FilesystemDurabilityWarning as warning:
                assert warning.code == "GF_UNVERIFIED_FILESYSTEM"
            else:
                raise AssertionError("expected warning promoted to error")
        assert target.exists() == existing, "warning error created the destination"
        after = {
            str(path.relative_to(target)): path.read_bytes()
            for path in target.rglob("*")
            if path.is_file()
        }
        assert after == before, "warning error changed the existing destination"
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", gf.FilesystemDurabilityWarning)
            receipt = gf.GraphForge.import_portable(
                project_root=str(target), input=package, operation_id=operation
            )
            assert len(caught) == 1
            assert not receipt["idempotent_replay"], "refused attempt committed an import"
            assert receipt["warnings"][0]["code"] == "GF_UNVERIFIED_FILESYSTEM"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", gf.FilesystemDurabilityWarning)
            forge = gf.GraphForge(str(target))
            assert forge.execute("MATCH (n:Item) RETURN n.value AS value").to_pylist() == [
                {"value": 42}
            ]
            forge.close()


def main() -> None:
    if sys.platform != "linux":
        # Linux tmpfs fixture; other platform classifiers have Rust coverage.
        return
    with tempfile.TemporaryDirectory(dir="/dev/shm") as root:
        project = str(Path(root) / "project")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            forge = gf.GraphForge(project)
            assert len(caught) == 1
            warning = caught[0].message
            assert isinstance(warning, gf.FilesystemDurabilityWarning)
            assert warning.code == "GF_UNVERIFIED_FILESYSTEM"
            assert warning.phase == "CLASSIFY"
            assert warning.outcome == "unverified"
            assert warning.cause == "filesystem_class_unproven"
            assert warning.filesystem_type == "tmpfs"
            assert project not in str(warning)
            forge.execute("CREATE (:Item {value: 42})")
            forge.close()
            assert len(caught) == 1
            forge = gf.GraphForge(project)
            assert len(caught) == 2
            assert forge.execute("MATCH (n:Item) RETURN n").num_rows == 1
            package = str(Path(root) / "package.gfpb")
            exported = forge.export_portable(
                output_path=package, representation="bundle", profile="complete"
            )
            assert exported["source_unverified_durability"]["filesystem_type"] == "tmpfs"
            verified = gf.GraphForge.verify_portable(package, mode="full")
            assert (
                verified["source_unverified_durability"] == exported["source_unverified_durability"]
            )
            forge.close()
            memory = gf.GraphForge()
            memory.close()
            assert len(caught) == 2
        check_import_warning_policy(Path(root), package)
        for policy, code in [("strict", "GF_UNSUPPORTED_FILESYSTEM"), ("invalid", "GF_VALIDATION")]:
            env = dict(os.environ, GRAPHFORGE_FILESYSTEM_DURABILITY=policy)
            child = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    (
                        "import graphforge as gf, sys\n"
                        "try: gf.GraphForge(sys.argv[1])\n"
                        "except gf.GraphForgeError as e: assert e.code == sys.argv[2], e\n"
                        "else: raise AssertionError('admitted')"
                    ),
                    project,
                    code,
                ],
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            assert child.returncode == 0, child.stdout + child.stderr
            imported = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    (
                        "import graphforge as gf, sys, uuid\n"
                        "try: gf.GraphForge.import_portable(project_root=sys.argv[1], "
                        "input=sys.argv[2], operation_id=str(uuid.uuid4()))\n"
                        "except gf.GraphForgeError as e: assert e.code == sys.argv[3], e\n"
                        "else: raise AssertionError('admitted')"
                    ),
                    str(Path(root) / "refused-import"),
                    package,
                    code,
                ],
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            assert imported.returncode == 0, imported.stdout + imported.stderr


if __name__ == "__main__":
    main()
