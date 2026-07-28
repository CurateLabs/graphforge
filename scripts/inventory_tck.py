#!/usr/bin/env python3
"""
Inventory all TCK test scenarios and generate a comprehensive report.
"""

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
import re
import sys


def extract_scenarios(feature_file):
    """Extract scenario names and types from a feature file."""
    scenarios = []
    feature_name = None

    with feature_file.open(encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            # Extract feature name
            if line.strip().startswith("Feature:"):
                feature_name = line.split("Feature:", 1)[1].strip()

            # Extract scenarios
            if re.match(r"^\s*(Scenario|Scenario Outline):", line):
                scenario_type = "Scenario Outline" if "Outline" in line else "Scenario"
                scenario_name = line.split(":", 1)[1].strip()
                scenarios.append({"name": scenario_name, "type": scenario_type, "line": line_num})

    return feature_name, scenarios


def categorize_file(file_path):
    """Categorize a feature file based on its path."""
    parts = file_path.parts

    # Handle official TCK structure
    if "official" in parts:
        idx = parts.index("official")
        if idx + 1 < len(parts):
            category = parts[idx + 1]  # clauses, expressions, etc.
            if idx + 2 < len(parts):
                subcategory = parts[idx + 2]  # match, aggregation, etc.
                # Check if subcategory is actually the feature filename
                if subcategory == file_path.name or file_path.suffix == ".feature":
                    return category
                return f"{category}/{subcategory}"
            return category

    # Handle non-official files (legacy)
    return "legacy"


def main():
    tck_dir = Path("tests/tck/features")

    # Check if TCK directory exists
    if not tck_dir.exists() or not tck_dir.is_dir():
        print(f"ERROR: TCK directory not found: {tck_dir}")
        print("Make sure to run this script from the repository root.")
        sys.exit(1)

    # Find all feature files
    feature_files = sorted(tck_dir.glob("**/*.feature"))

    # Organize by category
    categories = defaultdict(list)
    total_scenarios = 0

    for file_path in feature_files:
        relative_path = file_path.relative_to(tck_dir)
        category = categorize_file(relative_path)

        feature_name, scenarios = extract_scenarios(file_path)

        categories[category].append(
            {
                "file": str(relative_path),
                "feature_name": feature_name,
                "scenarios": scenarios,
                "scenario_count": len(scenarios),
            }
        )

        total_scenarios += len(scenarios)

    # Generate markdown report
    output = []
    output.append("# TCK Test Scenario Inventory\n")
    output.append(
        "Comprehensive inventory of all OpenCypher Technology Compatibility Kit (TCK) "
        "test scenarios in GraphForge.\n"
    )
    generated_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
    output.append(f"**Generated:** {generated_time}\n")
    output.append(f"**Total feature files:** {len(feature_files)}\n")
    output.append(f"**Total scenarios:** {total_scenarios}\n")

    # Table of contents
    output.append("\n## Table of Contents\n")
    for category in sorted(categories.keys()):
        count = sum(f["scenario_count"] for f in categories[category])
        link = category.replace("/", "-")
        output.append(
            f"- [{category}](#{link}) ({len(categories[category])} files, {count} scenarios)\n"
        )

    # Summary statistics
    output.append("\n## Summary Statistics\n")
    output.append("| Category | Feature Files | Scenarios | Avg Scenarios/File |\n")
    output.append("|----------|---------------|-----------|--------------------|\n")

    for category in sorted(categories.keys()):
        files = categories[category]
        scenario_count = sum(f["scenario_count"] for f in files)
        avg = scenario_count / len(files) if files else 0
        output.append(f"| {category} | {len(files)} | {scenario_count} | {avg:.1f} |\n")

    # Calculate average, handling empty feature_files
    total_avg = total_scenarios / len(feature_files) if feature_files else 0
    output.append(
        f"| **TOTAL** | **{len(feature_files)}** | **{total_scenarios}** | **{total_avg:.1f}** |\n"
    )

    # Detailed listings by category
    output.append("\n## Detailed Inventory by Category\n")

    for category in sorted(categories.keys()):
        files = categories[category]
        scenario_count = sum(f["scenario_count"] for f in files)

        output.append(f"\n### {category}\n")
        output.append(f"\n**{len(files)} feature files, {scenario_count} scenarios**\n")

        for file_info in sorted(files, key=lambda x: x["file"]):
            output.append(f"\n#### `{file_info['file']}`\n")
            if file_info["feature_name"]:
                output.append(f"\n**Feature:** {file_info['feature_name']}\n")
            output.append(f"\n**Scenarios:** {file_info['scenario_count']}\n")

            if file_info["scenarios"]:
                output.append("\n")
                for i, scenario in enumerate(file_info["scenarios"], 1):
                    output.append(
                        f"{i}. {scenario['name']} ({scenario['type']}, line {scenario['line']})\n"
                    )

    # Write output
    output_file = Path("docs/reference/tck-inventory.md")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text("".join(output))
    print(f"✅ TCK inventory written to {output_file}")
    print(f"   {len(feature_files)} feature files")
    print(f"   {total_scenarios} total scenarios")
    print(f"   {len(categories)} categories")


if __name__ == "__main__":
    main()
