#!/usr/bin/env python3
"""Bump version in pyproject.toml and update CHANGELOG.md.

Usage:
    python scripts/bump_version.py patch  # 0.1.1 → 0.1.2
    python scripts/bump_version.py minor  # 0.1.1 → 0.2.0
    python scripts/bump_version.py major  # 0.1.1 → 1.0.0
"""

from datetime import date
from pathlib import Path
import re
import sys


def get_current_version(pyproject_path: Path) -> tuple[int, int, int]:
    """Extract current version from pyproject.toml."""
    content = pyproject_path.read_text()
    match = re.search(r'version = "(\d+)\.(\d+)\.(\d+)"', content)
    if not match:
        raise ValueError("Could not find version in pyproject.toml")
    return tuple(map(int, match.groups()))


def bump_version(current: tuple[int, int, int], bump_type: str) -> tuple[int, int, int]:
    """Calculate new version based on bump type."""
    major, minor, patch = current

    if bump_type == "major":
        return (major + 1, 0, 0)
    elif bump_type == "minor":
        return (major, minor + 1, 0)
    elif bump_type == "patch":
        return (major, minor, patch + 1)
    else:
        raise ValueError(f"Invalid bump type '{bump_type}'. Use: major, minor, or patch")


def update_pyproject(pyproject_path: Path, new_version: str) -> None:
    """Update version in pyproject.toml."""
    content = pyproject_path.read_text()
    new_content = re.sub(
        r'version = "\d+\.\d+\.\d+"',
        f'version = "{new_version}"',
        content,
    )
    pyproject_path.write_text(new_content)


def update_changelog(changelog_path: Path, new_version: str) -> None:
    """Update CHANGELOG.md with new version section."""
    if not changelog_path.exists():
        print(f"Warning: {changelog_path} not found. Skipping CHANGELOG update.")
        return

    content = changelog_path.read_text()
    today = date.today().isoformat()

    # Replace [Unreleased] with new version
    # Find the [Unreleased] section
    unreleased_pattern = r"## \[Unreleased\]"
    match = re.search(unreleased_pattern, content)

    if not match:
        print("Warning: Could not find [Unreleased] section in CHANGELOG.md")
        return

    # Insert new version section after [Unreleased]
    # Keep [Unreleased] section empty for future changes
    new_section = f"""## [Unreleased]

## [{new_version}] - {today}"""

    updated_content = re.sub(
        unreleased_pattern,
        new_section,
        content,
        count=1,
    )

    # Update comparison links at the bottom
    # Find the [Unreleased] link
    unreleased_link_pattern = r"\[Unreleased\]: (.+?)/compare/v([\d.]+)\.\.\.HEAD"
    match = re.search(unreleased_link_pattern, updated_content)

    if match:
        repo_url = match.group(1)
        old_version = match.group(2)

        # Update links
        new_links = f"""[Unreleased]: {repo_url}/compare/v{new_version}...HEAD
[{new_version}]: {repo_url}/compare/v{old_version}...v{new_version}"""

        updated_content = re.sub(
            unreleased_link_pattern + r"\n\[" + re.escape(old_version) + r"\]: .+",
            new_links,
            updated_content,
        )

    changelog_path.write_text(updated_content)


def main():
    """Main entry point."""
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)

    bump_type = sys.argv[1].lower()
    if bump_type not in ("major", "minor", "patch"):
        print(f"Error: Invalid bump type '{bump_type}'")
        print("Use: major, minor, or patch")
        sys.exit(1)

    # Paths
    root = Path(__file__).parent.parent
    pyproject_path = root / "pyproject.toml"
    changelog_path = root / "CHANGELOG.md"

    # Get current version
    try:
        current = get_current_version(pyproject_path)
        current_str = ".".join(map(str, current))
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Calculate new version
    new = bump_version(current, bump_type)
    new_str = ".".join(map(str, new))

    print(f"Current version: {current_str}")
    print(f"New version: {new_str}")
    print()

    # Update files
    print(f"Updating {pyproject_path}...")
    update_pyproject(pyproject_path, new_str)

    print(f"Updating {changelog_path}...")
    update_changelog(changelog_path, new_str)

    print()
    print("✅ Version bumped successfully!")
    print()
    print("Next steps:")
    print("  1. Review changes: git diff")
    print("  2. Update CHANGELOG.md with actual changes")
    print(f"  3. Commit: git commit -am 'chore(release): bump version to {new_str}'")
    print(f"  4. Tag: git tag -a v{new_str} -m 'Release version {new_str}'")
    print("  5. Push: git push origin main --tags")


if __name__ == "__main__":
    main()
