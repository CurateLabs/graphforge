"""Regression checks for split native binding source ownership."""

from pathlib import Path
import runpy
import tempfile

HELPERS = runpy.run_path(str(Path(__file__).with_name("native_sources.py")))
production_source = HELPERS["production_source"]
production_sources = HELPERS["production_sources"]


def check_discovery() -> None:
    """Keep declared methods discoverable and fail closed on invalid ownership."""
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        src = root / "src"
        src.mkdir()
        entry = src / "lib.rs"
        domain = src / "domain.rs"
        (src / "domain").mkdir()
        nested = src / "domain" / "nested.rs"
        entry.write_text(
            "mod domain;\n#[cfg(test)] mod missing_tests;\n"
            '// mod missing_comment;\nconst X: &str = r#"mod missing_string;"#;\n'
            "#[pymethods] impl GraphForge { fn retained(&self) {} }\n"
        )
        domain.write_text("mod nested;\n#[pymethods] impl GraphForge { fn moved(&self) {} }\n")
        nested.write_text("fn helper() {}\n")
        (src / "orphan.rs").write_text("#[pymethods] impl GraphForge { fn orphan(&self) {} }\n")
        stub_gate = runpy.run_path(str(Path(__file__).with_name("stub_surface.py")))
        members = stub_gate["_native_members"]
        entry.write_text(
            entry.read_text()
            + "\n".join(
                f"#[pymethods] impl {receiver} {{}}"
                for receiver in stub_gate["RECEIVERS"]
                if receiver != "GraphForge"
            )
        )
        baseline = production_source(entry)
        assert [p.name for p, _ in production_sources(entry)] == [
            "lib.rs",
            "domain.rs",
            "nested.rs",
        ]
        assert members(baseline)["GraphForge"] == {"retained", "moved"}
        assert "orphan" not in baseline and "missing_tests" in baseline

        def rejects(fragment: str) -> None:
            try:
                production_source(entry)
            except ValueError as error:
                assert fragment in str(error), str(error)
            else:
                raise AssertionError(f"invalid source ownership accepted: {fragment}")

        original = entry.read_text()
        entry.write_text(original.replace("mod domain;", ""))
        assert members(production_source(entry))["GraphForge"] == {"retained"}
        entry.write_text(original)
        domain.unlink()
        rejects("missing or ambiguous module domain")
        domain.write_text("mod nested;\n#[pymethods] impl GraphForge { fn moved(&self) {} }\n")
        (src / "domain" / "mod.rs").write_text("")
        rejects("ambiguous module domain")
        (src / "domain" / "mod.rs").unlink()
        for attribute in ["#[cfg(windows)]", '#[path = "other.rs"]']:
            entry.write_text(attribute + "\n" + original)
            rejects("unsupported conditional/path module domain")
        entry.write_text(original)

        runfiles = root / "runfiles"
        runfiles.mkdir()
        (runfiles / "lib.rs").symlink_to(entry)
        (runfiles / "domain.rs").symlink_to(domain)
        (runfiles / "domain").mkdir()
        (runfiles / "domain" / "nested.rs").symlink_to(nested)
        assert production_source(runfiles / "lib.rs") == baseline
        outside = root / "outside.rs"
        outside.write_text("fn escaped() {}\n")
        domain.unlink()
        domain.symlink_to(outside)
        rejects("escapes source authority")


if __name__ == "__main__":
    check_discovery()
    print("native source discovery regressions: ok")
