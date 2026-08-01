"""PyPI landing-page contract for the Python package README (#304)."""

from __future__ import annotations

import email.parser
import subprocess
import tarfile
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
README = ROOT / "crates" / "graphforge-bindings-py" / "README.md"
PYPROJECT = ROOT / "crates" / "graphforge-bindings-py" / "pyproject.toml"
CANONICAL_DOCS = "https://docs.graphforge.sh/"
STALE_DOCS = "https://curatelabs.github.io/graphforge/"


def test_python_readme_is_concise_pypi_landing_page() -> None:
    text = README.read_text(encoding="utf-8")
    lines = [line.rstrip() for line in text.splitlines()]
    assert lines[0] == "# GraphForge for Python"
    assert "pip install graphforge" in text
    assert "from graphforge import GraphForge" in text
    assert "forge.execute(" in text
    assert CANONICAL_DOCS in text
    assert STALE_DOCS not in text
    # Must not open with an uncontextualized CLI command inventory.
    first_fence = text.find("```")
    assert first_fence > 0
    assert "uvx graphforge init" not in text[:first_fence]
    assert text.count("uvx graphforge") == 0
    assert len(text) < 2500


def test_pyproject_points_at_readme_and_canonical_docs() -> None:
    text = PYPROJECT.read_text(encoding="utf-8")
    assert 'readme = "README.md"' in text
    assert f'Homepage = "{CANONICAL_DOCS}"' in text
    assert f'Documentation = "{CANONICAL_DOCS}"' in text


def test_python_sdist_embeds_readme_in_pkg_info() -> None:
    """Build a local sdist and verify PKG-INFO carries the landing-page README."""
    out = Path(tempfile.mkdtemp(prefix="graphforge-py-readme-"))
    result = subprocess.run(
        [
            "uv",
            "run",
            "maturin",
            "sdist",
            "--manifest-path",
            "crates/graphforge-bindings-py/Cargo.toml",
            "--out",
            str(out),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    sdists = sorted(out.glob("graphforge-*.tar.gz"))
    assert len(sdists) == 1, sdists

    with tarfile.open(sdists[0], "r:gz") as archive:
        members = archive.getnames()
        pkg_info_name = next(name for name in members if name.endswith("/PKG-INFO"))
        extracted = archive.extractfile(pkg_info_name)
        assert extracted is not None
        metadata = email.parser.Parser().parsestr(extracted.read().decode("utf-8"))

    assert metadata.get("Name") == "graphforge"
    assert metadata.get("Description-Content-Type", "").startswith("text/markdown")
    description = metadata.get_payload()
    assert isinstance(description, str)
    assert "# GraphForge for Python" in description
    assert "pip install graphforge" in description
    assert "from graphforge import GraphForge" in description
    assert CANONICAL_DOCS in description
    assert STALE_DOCS not in description
    assert "uvx graphforge init" not in description
