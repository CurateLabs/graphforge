"""Runtime warning count and typed filesystem qualification on the real binding."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import warnings

import graphforge as gf


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
