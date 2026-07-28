#!/usr/bin/env python3
"""Check if a release is needed based on unreleased changes.

This script analyzes the CHANGELOG.md [Unreleased] section to determine
if enough changes have accumulated to warrant a release.

Usage:
    python scripts/check_release_needed.py
"""

from datetime import datetime
from pathlib import Path
import re
import sys


def count_unreleased_changes(changelog_path: Path) -> dict:
    """Count items in [Unreleased] section by type.

    Returns:
        dict with counts: {'added': int, 'changed': int, 'fixed': int, 'total': int}
    """
    if not changelog_path.exists():
        return {
            "added": 0,
            "changed": 0,
            "fixed": 0,
            "deprecated": 0,
            "removed": 0,
            "security": 0,
            "total": 0,
        }

    content = changelog_path.read_text()

    # Extract [Unreleased] section
    match = re.search(r"## \[Unreleased\](.*?)(?=## \[|\Z)", content, re.DOTALL)

    if not match:
        return {
            "added": 0,
            "changed": 0,
            "fixed": 0,
            "deprecated": 0,
            "removed": 0,
            "security": 0,
            "total": 0,
        }

    unreleased = match.group(1)

    # Count by category
    counts = {
        "added": len(
            re.findall(
                r"^- .+",
                unreleased.split("### Added")[1].split("###")[0]
                if "### Added" in unreleased
                else "",
                re.MULTILINE,
            )
        ),
        "changed": len(
            re.findall(
                r"^- .+",
                unreleased.split("### Changed")[1].split("###")[0]
                if "### Changed" in unreleased
                else "",
                re.MULTILINE,
            )
        ),
        "fixed": len(
            re.findall(
                r"^- .+",
                unreleased.split("### Fixed")[1].split("###")[0]
                if "### Fixed" in unreleased
                else "",
                re.MULTILINE,
            )
        ),
        "deprecated": len(
            re.findall(
                r"^- .+",
                unreleased.split("### Deprecated")[1].split("###")[0]
                if "### Deprecated" in unreleased
                else "",
                re.MULTILINE,
            )
        ),
        "removed": len(
            re.findall(
                r"^- .+",
                unreleased.split("### Removed")[1].split("###")[0]
                if "### Removed" in unreleased
                else "",
                re.MULTILINE,
            )
        ),
        "security": len(
            re.findall(
                r"^- .+",
                unreleased.split("### Security")[1].split("###")[0]
                if "### Security" in unreleased
                else "",
                re.MULTILINE,
            )
        ),
    }

    counts["total"] = sum(counts.values())
    return counts


def get_last_release_date(changelog_path: Path) -> datetime | None:
    """Get date of last release from CHANGELOG."""
    if not changelog_path.exists():
        return None

    content = changelog_path.read_text()

    # Find first version entry: ## [X.Y.Z] - YYYY-MM-DD
    match = re.search(r"## \[\d+\.\d+\.\d+\] - (\d{4}-\d{2}-\d{2})", content)

    if not match:
        return None

    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d")
    except ValueError:
        return None


def determine_version_bump(counts: dict) -> str:
    """Determine if patch, minor, or major version bump is appropriate.

    Returns:
        'major', 'minor', 'patch', or 'none'
    """
    if counts["total"] == 0:
        return "none"

    # If there are breaking changes (removed/deprecated), suggest major
    if counts["removed"] > 0 or counts["deprecated"] > 0:
        return "major"

    # If there are new features (added), suggest minor
    if counts["added"] > 0:
        return "minor"

    # If there are only fixes/changes, suggest patch
    if counts["fixed"] > 0 or counts["changed"] > 0:
        return "patch"

    return "none"


def main():
    """Main entry point."""
    root = Path(__file__).parent.parent
    changelog = root / "CHANGELOG.md"

    if not changelog.exists():
        print("❌ CHANGELOG.md not found")
        sys.exit(1)

    # Count changes
    counts = count_unreleased_changes(changelog)
    total = counts["total"]

    # Get last release date
    last_release = get_last_release_date(changelog)
    days_since_release = None

    if last_release:
        days_since_release = (datetime.now() - last_release).days

    # Determine suggested version bump
    bump_type = determine_version_bump(counts)

    # Print summary
    print("=" * 60)
    print("📊 GraphForge Release Status")
    print("=" * 60)
    print()

    if total == 0:
        print("✅ No unreleased changes")
        print()
        print("Status: Nothing to release")
        print()
        sys.exit(0)

    # Show counts by type
    print(f"📦 Unreleased Changes: {total}")
    print()
    if counts["added"] > 0:
        print(f"   ✨ Added:      {counts['added']}")
    if counts["changed"] > 0:
        print(f"   🔄 Changed:    {counts['changed']}")
    if counts["fixed"] > 0:
        print(f"   🐛 Fixed:      {counts['fixed']}")
    if counts["deprecated"] > 0:
        print(f"   ⚠️  Deprecated: {counts['deprecated']}")
    if counts["removed"] > 0:
        print(f"   🗑️  Removed:    {counts['removed']}")
    if counts["security"] > 0:
        print(f"   🔒 Security:   {counts['security']}")
    print()

    # Show time since last release
    if days_since_release is not None:
        print(f"📅 Days Since Last Release: {days_since_release}")
        if days_since_release >= 28:
            print("   ⚠️  Over 4 weeks - scheduled release window")
        elif days_since_release >= 21:
            print("   ⏰ Approaching 4 weeks - consider releasing soon")
        else:
            print(f"   ✅ Within normal cycle ({28 - days_since_release} days until 4 weeks)")
        print()

    # Suggested action
    print(f"💡 Suggested Version Bump: {bump_type.upper()}")
    print()

    # Recommendation
    print("📋 Recommendation:")
    print()

    if counts["security"] > 0:
        print("   🚨 SECURITY FIXES PRESENT - Release immediately!")
        print()
        print("   Action:")
        print(f"     python scripts/bump_version.py {bump_type}")
        print("     # Complete release process immediately")
        sys.exit(2)

    if total >= 10:
        print("   🚨 Large number of changes accumulated")
        print("   → Time to release!")
        print()
    elif total >= 5 and days_since_release and days_since_release >= 14:
        print("   ⚠️  Good amount of changes + 2+ weeks elapsed")
        print("   → Consider releasing")
        print()
    elif total >= 5:
        print("   ⏳ Good amount of changes accumulated")
        print("   → Consider releasing when convenient")
        print()
    elif days_since_release and days_since_release >= 28:
        print("   ⏰ Scheduled release window (4 weeks)")
        print("   → Release on schedule")
        print()
    else:
        print("   ✅ Can continue accumulating changes")
        print(f"   → {5 - total} more changes recommended before release")
        print()

    print("Next Steps:")
    print("  1. Review changes: cat CHANGELOG.md")
    print(f"  2. Bump version: python scripts/bump_version.py {bump_type}")
    print("  3. Follow release process: see RELEASING.md")
    print()

    # Exit code: 0 = no action needed, 1 = consider release, 2 = release needed
    if counts["security"] > 0:
        sys.exit(2)
    elif total >= 10 or (total >= 5 and days_since_release and days_since_release >= 21):
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
