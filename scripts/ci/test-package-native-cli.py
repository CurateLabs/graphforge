#!/usr/bin/env python3
"""Native release archives preserve executable bytes, mode, notices and digests."""

import hashlib
import importlib.util
from pathlib import Path
import tarfile
import tempfile
import zipfile

spec = importlib.util.spec_from_file_location(
    "package_native_cli", Path(__file__).with_name("package-native-cli.py")
)
assert spec and spec.loader
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

with tempfile.TemporaryDirectory() as directory:
    root = Path(directory)
    binary = root / "binary"
    binary.write_bytes(b"native executable fixture")
    binary.chmod(0o755)
    for target in (
        "aarch64-apple-darwin",
        "x86_64-apple-darwin",
        "aarch64-unknown-linux-gnu",
        "x86_64-unknown-linux-gnu",
        "x86_64-pc-windows-msvc",
    ):
        archive = module.package(binary, target, "0.6.0-rc.8", root / "dist")
        checksum = archive.with_name(archive.name + ".sha256").read_text()
        assert checksum == f"{hashlib.sha256(archive.read_bytes()).hexdigest()}  {archive.name}\n"
        if archive.suffix == ".zip":
            with zipfile.ZipFile(archive) as packed:
                assert set(packed.namelist()) == {
                    "gf.exe",
                    "LICENSE",
                    "NOTICE",
                    "THIRD_PARTY_NOTICES.md",
                }
                assert packed.read("gf.exe") == binary.read_bytes()
        else:
            with tarfile.open(archive) as packed:
                assert set(packed.getnames()) == {
                    "gf",
                    "LICENSE",
                    "NOTICE",
                    "THIRD_PARTY_NOTICES.md",
                }
                assert packed.getmember("gf").mode & 0o111 == 0o111
                with packed.extractfile("gf") as executable:
                    assert executable.read() == binary.read_bytes()
print("native CLI archives and checksums: PASS (five target formats)")
