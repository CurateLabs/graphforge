#!/usr/bin/env python3
"""Build a native GraphForge project from OpenCypher feature documentation.

Creates a queryable graph database mapping:
- OpenCypher features
- Implementation status
- TCK test scenarios
- Feature categories and dependencies

The default output is ``docs/feature-graph.db``. Pass ``--output`` during
validation so the installed wheel writes only inside a temporary directory.
"""

import argparse
import json
from pathlib import Path
import re
import shutil

from graphforge import GraphForge

RESULT_PREFIX = "GRAPHFORGE_CONSUMER_RESULT="


def parse_implementation_status(status_file):
    """Parse implementation status from markdown file."""
    features = []

    content = status_file.read_text(encoding="utf-8")

    # Extract features with status markers
    # Pattern: ### FeatureName followed by **Status:** ✅/⚠️/❌
    # Use negative lookahead to prevent crossing into next ### section
    pattern = (
        r"###\s+(.+?)\n(?:(?!###).)*?\*\*Status:\*\*\s+(✅|⚠️|❌)\s+"
        r"(COMPLETE|PARTIAL|NOT_IMPLEMENTED)"
    )

    matches = re.finditer(pattern, content, re.DOTALL)

    for match in matches:
        feature_name = match.group(1).strip()
        _emoji = match.group(2)  # Captured but not used, status_text is more reliable
        status_text = match.group(3)

        # Map status based on status text (more reliable than emoji)
        if status_text == "COMPLETE":
            status = "complete"
        elif status_text == "PARTIAL":
            status = "partial"
        else:
            status = "not_implemented"

        # Extract file reference if present (search until next ### or end of section)
        section_end = content.find("###", match.end())
        if section_end == -1:
            section_end = len(content)
        section = content[match.end() : section_end]
        file_match = re.search(r"\*\*File[s]?:\*\*\s+`?([^`\n]+)", section)
        file_path = file_match.group(1) if file_match else None

        features.append({"name": feature_name, "status": status, "file_path": file_path})

    return features


def parse_category_status(category, subcategory_fn=None):
    """Parse implementation status for a given category.

    Args:
        category: Feature category (clause, function, operator, pattern)
        subcategory_fn: Function to determine subcategory from feature name

    Returns:
        List of feature dictionaries with category and subcategory fields
    """
    status_file = Path(f"docs/reference/implementation-status/{category}s.md")
    if not status_file.exists():
        return []

    features = parse_implementation_status(status_file)

    # Add category and subcategory
    for f in features:
        f["category"] = category
        if subcategory_fn:
            f["subcategory"] = subcategory_fn(f["name"])
        else:
            f["subcategory"] = "other"

    return features


def determine_clause_subcategory(name):
    """Determine clause subcategory."""
    name_lower = name.lower()
    if any(k in name_lower for k in ["match", "optional"]):
        return "reading"
    elif any(k in name_lower for k in ["return", "with", "unwind"]):
        return "projecting"
    elif any(k in name_lower for k in ["create", "merge", "delete", "set", "remove"]):
        return "writing"
    elif any(k in name_lower for k in ["union"]):
        return "set_operations"
    else:
        return "other"


def determine_function_subcategory(name):
    """Determine function subcategory."""
    name_lower = name.lower()
    if any(
        k in name_lower
        for k in [
            "substring",
            "trim",
            "upper",
            "lower",
            "split",
            "replace",
            "reverse",
            "left",
            "right",
            "tostring",
        ]
    ):
        return "string"
    elif any(
        k in name_lower
        for k in ["abs", "ceil", "floor", "round", "sign", "tointeger", "tofloat", "sqrt", "rand"]
    ):
        return "numeric"
    elif (
        any(
            k in name_lower
            for k in ["size", "head", "tail", "last", "range", "extract", "filter", "reduce"]
        )
        and "path" not in name_lower
    ):
        return "list"
    elif any(
        k in name_lower
        for k in ["count", "sum", "avg", "min", "max", "collect", "percentile", "stdev"]
    ):
        return "aggregation"
    elif any(k in name_lower for k in ["all", "any", "none", "single", "exists", "isempty"]):
        return "predicate"
    elif any(
        k in name_lower
        for k in [
            "id",
            "type",
            "labels",
            "properties",
            "keys",
            "coalesce",
            "toboolean",
            "timestamp",
        ]
    ):
        return "scalar"
    elif any(
        k in name_lower
        for k in [
            "date",
            "datetime",
            "time",
            "localtime",
            "localdatetime",
            "duration",
            "year",
            "month",
            "day",
            "hour",
            "minute",
            "second",
            "truncate",
        ]
    ):
        return "temporal"
    elif any(k in name_lower for k in ["point", "distance"]):
        return "spatial"
    elif any(k in name_lower for k in ["length", "nodes", "relationships", "shortestpath"]):
        # Path-related functions: check if any path keyword is present
        return "path"
    else:
        return "other"


def determine_operator_subcategory(name):
    """Determine operator subcategory."""
    name_lower = name.lower()
    if any(k in name_lower for k in ["equals", "not equals", "less", "greater", "is null"]):
        return "comparison"
    elif any(k in name_lower for k in ["and", "or", "not", "xor"]):
        return "logical"
    elif any(
        k in name_lower
        for k in ["addition", "subtraction", "multiplication", "division", "modulo", "power"]
    ):
        return "arithmetic"
    elif any(k in name_lower for k in ["concatenation", "regex", "starts", "ends", "contains"]):
        return "string"
    elif any(k in name_lower for k in ["membership", "index", "slicing", "list"]):
        return "list"
    else:
        return "other"


def parse_tck_mappings():
    """Parse TCK scenario mappings from feature-mapping docs."""
    mappings = {}

    # Parse clause-to-tck mapping
    clause_file = Path("docs/reference/feature-mapping/clause-to-tck.md")
    if clause_file.exists():
        content = clause_file.read_text(encoding="utf-8")

        # Extract mappings: ### FeatureName followed by **Total TCK Coverage:** N scenarios
        pattern = r"###\s+(.+?)\n.*?\*\*Total TCK Coverage:\*\*\s+(\d+)\s+scenarios"
        for match in re.finditer(pattern, content, re.DOTALL):
            feature_name = match.group(1).strip()
            scenario_count = int(match.group(2))
            mappings[feature_name] = scenario_count

    # Parse function-to-tck mapping
    function_file = Path("docs/reference/feature-mapping/function-to-tck.md")
    if function_file.exists():
        content = function_file.read_text(encoding="utf-8")

        # Extract function mappings with simpler pattern matching scenario counts
        pattern = r"###\s+(.+?)\n.*?\*\*Total TCK Coverage:\*\*\s+(\d+)\s+scenarios"
        for match in re.finditer(pattern, content, re.DOTALL):
            feature_name = match.group(1).strip()
            scenario_count = int(match.group(2))
            mappings[feature_name] = scenario_count

    return mappings


def build_graph(db_path: Path) -> dict[str, int | str]:
    """Build the feature graph at ``db_path`` and return its summary."""

    print("Building OpenCypher feature knowledge graph...")

    if db_path.exists():
        if db_path.is_dir():
            shutil.rmtree(db_path)
        else:
            db_path.unlink()
        print(f"  Removed existing database: {db_path}")
    db_path.mkdir(parents=True)

    gf = GraphForge(str(db_path))
    print(f"  Created database: {db_path}")

    # Each native construction call commits atomically.
    try:
        # Parse all features
        print("\nParsing feature documentation...")
        clauses = parse_category_status("clause", determine_clause_subcategory)
        functions = parse_category_status("function", determine_function_subcategory)
        operators = parse_category_status("operator", determine_operator_subcategory)
        patterns = parse_category_status("pattern", lambda _: "pattern_matching")

        all_features = clauses + functions + operators + patterns
        print(f"  Found {len(all_features)} features:")
        print(f"    Clauses: {len(clauses)}")
        print(f"    Functions: {len(functions)}")
        print(f"    Operators: {len(operators)}")
        print(f"    Patterns: {len(patterns)}")

        # Parse TCK mappings
        tck_mappings = parse_tck_mappings()
        print(f"  Found {len(tck_mappings)} TCK mappings")

        # Create Category nodes
        print("\nCreating Category nodes...")
        categories = {
            "Reading Clauses": "Query clauses for reading data from the graph",
            "Projecting Clauses": "Query clauses for projecting and transforming results",
            "Writing Clauses": "Query clauses for creating and modifying graph data",
            "Set Operations": "Query clauses for combining results",
            "String Functions": "Functions for string manipulation",
            "Numeric Functions": "Functions for mathematical operations",
            "List Functions": "Functions for list operations",
            "Aggregation Functions": "Functions for aggregating values",
            "Predicate Functions": "Functions for testing conditions",
            "Scalar Functions": "Functions for scalar operations",
            "Temporal Functions": "Functions for date and time operations",
            "Spatial Functions": "Functions for spatial operations",
            "Path Functions": "Functions for path operations",
            "Comparison Operators": "Operators for comparing values",
            "Logical Operators": "Operators for logical operations",
            "Arithmetic Operators": "Operators for arithmetic operations",
            "String Operators": "Operators for string operations",
            "List Operators": "Operators for list operations",
            "Pattern Operators": "Operators for pattern matching",
            "Pattern Matching": "Pattern matching features",
            "Other": "Uncategorized features",
        }

        category_nodes = {}
        for name, description in categories.items():
            node = gf.add_node("Category", name=name, description=description)
            category_nodes[name] = node
            print(f"  Created: {name}")

        # Create Feature nodes
        print("\nCreating Feature nodes...")
        feature_nodes = {}
        for feature in all_features:
            node = gf.add_node(
                "Feature",
                name=feature["name"],
                category=feature["category"],
                subcategory=feature["subcategory"],
            )
            feature_nodes[feature["name"]] = node

        print(f"  Created {len(feature_nodes)} Feature nodes")

        # Create Implementation nodes
        print("\nCreating Implementation nodes...")
        impl_count = 0
        for feature in all_features:
            if feature.get("file_path"):
                gf.add_node(
                    "Implementation",
                    feature_name=feature["name"],
                    file_path=feature["file_path"],
                    status=feature["status"],
                )

                # Create IMPLEMENTED_IN relationship
                completeness = (
                    1.0
                    if feature["status"] == "complete"
                    else 0.5
                    if feature["status"] == "partial"
                    else 0.0
                )
                gf.execute(
                    "MATCH (feature:Feature {name: $feature}), "
                    "(implementation:Implementation {feature_name: $feature}) "
                    "CREATE (feature)-[:IMPLEMENTED_IN {completeness: $completeness}]->"
                    "(implementation)",
                    {
                        "feature": feature["name"],
                        "completeness": completeness,
                    },
                )
                impl_count += 1

        print(f"  Created {impl_count} Implementation nodes")

        # Create BELONGS_TO_CATEGORY relationships
        print("\nCreating category relationships...")
        # Map (category, subcategory) tuples to category names
        category_map = {
            ("clause", "reading"): "Reading Clauses",
            ("clause", "projecting"): "Projecting Clauses",
            ("clause", "writing"): "Writing Clauses",
            ("clause", "set_operations"): "Set Operations",
            ("clause", "other"): "Other",
            ("function", "string"): "String Functions",
            ("function", "numeric"): "Numeric Functions",
            ("function", "list"): "List Functions",
            ("function", "aggregation"): "Aggregation Functions",
            ("function", "predicate"): "Predicate Functions",
            ("function", "scalar"): "Scalar Functions",
            ("function", "temporal"): "Temporal Functions",
            ("function", "spatial"): "Spatial Functions",
            ("function", "path"): "Path Functions",
            ("function", "other"): "Other",
            ("operator", "comparison"): "Comparison Operators",
            ("operator", "logical"): "Logical Operators",
            ("operator", "arithmetic"): "Arithmetic Operators",
            ("operator", "string"): "String Operators",
            ("operator", "list"): "List Operators",
            ("operator", "other"): "Other",
            ("pattern", "pattern_matching"): "Pattern Matching",
        }

        rel_count = 0
        for feature in all_features:
            category_key = (feature["category"], feature["subcategory"])
            category_name = category_map.get(category_key)

            if category_name and category_name in category_nodes:
                gf.execute(
                    "MATCH (feature:Feature {name: $feature}), "
                    "(category:Category {name: $category}) "
                    "CREATE (feature)-[:BELONGS_TO_CATEGORY]->(category)",
                    {
                        "feature": feature["name"],
                        "category": category_name,
                    },
                )
                rel_count += 1

        print(f"  Created {rel_count} BELONGS_TO_CATEGORY relationships")

        # Create TCK Scenario nodes and TESTED_BY relationships
        print("\nCreating TCK scenario relationships...")
        tck_count = 0
        for feature_name, scenario_count in tck_mappings.items():
            # Find matching feature node
            if feature_name in feature_nodes:
                # Create or merge TCK scenario node (deduplicate by feature name)
                gf.add_node("TCKScenario", feature_name=feature_name, scenario_count=scenario_count)

                # Create TESTED_BY relationship
                gf.execute(
                    "MATCH (feature:Feature {name: $feature}), "
                    "(scenario:TCKScenario {feature_name: $feature}) "
                    "CREATE (feature)-[:TESTED_BY {scenario_count: $scenario_count}]->"
                    "(scenario)",
                    {
                        "feature": feature_name,
                        "scenario_count": scenario_count,
                    },
                )
                tck_count += 1

        print(f"  Created {tck_count} TCK scenario relationships")

    except Exception:
        gf.close()
        raise

    try:
        return _summarize_graph(gf, db_path)
    finally:
        gf.close()


def _summarize_graph(gf: GraphForge, db_path: Path) -> dict[str, int | str]:
    """Query and report the completed graph while its native handle is open."""
    # Print summary statistics
    print("\n" + "=" * 60)
    print("GRAPH BUILD COMPLETE")
    print("=" * 60)

    # Query statistics
    print("\nNode Statistics:")
    result = gf.execute("MATCH (n:Feature) RETURN count(n) AS count").to_pylist()
    feature_count = result[0]["count"] if result else 0
    print(f"  Features: {feature_count}")

    result = gf.execute("MATCH (n:Category) RETURN count(n) AS count").to_pylist()
    category_count = result[0]["count"] if result else 0
    print(f"  Categories: {category_count}")

    result = gf.execute("MATCH (n:Implementation) RETURN count(n) AS count").to_pylist()
    implementation_count = result[0]["count"] if result else 0
    print(f"  Implementations: {implementation_count}")

    print("\nRelationship Statistics:")
    result = gf.execute("MATCH ()-[r:IMPLEMENTED_IN]->() RETURN count(r) AS count").to_pylist()
    implemented_in_count = result[0]["count"] if result else 0
    print(f"  IMPLEMENTED_IN: {implemented_in_count}")

    result = gf.execute("MATCH ()-[r:BELONGS_TO_CATEGORY]->() RETURN count(r) AS count").to_pylist()
    belongs_count = result[0]["count"] if result else 0
    print(f"  BELONGS_TO_CATEGORY: {belongs_count}")

    print("\nImplementation Status:")
    result = gf.execute(
        """
        MATCH (f:Feature)-[:IMPLEMENTED_IN]->(i:Implementation)
        RETURN i.status AS status, count(f) AS count
        """
    ).to_pylist()
    sorted_result = sorted(result, key=lambda row: row["count"], reverse=True)
    for row in sorted_result:
        print(f"  {row['status']}: {row['count']}")

    print("\nFeatures by Category:")
    result = gf.execute(
        """
        MATCH (c:Category)<-[:BELONGS_TO_CATEGORY]-(f:Feature)
        RETURN c.name AS category, count(f) AS count
        """
    ).to_pylist()
    sorted_result = sorted(result, key=lambda row: row["count"], reverse=True)[:10]
    for row in sorted_result:
        print(f"  {row['category']}: {row['count']}")

    print("\n" + "=" * 60)
    print(f"Graph saved to: {db_path.absolute()}")
    print("=" * 60)

    return {
        "consumer": "scripts/build_feature_graph.py",
        "features": feature_count,
        "categories": category_count,
        "implementations": implementation_count,
        "implemented_in": implemented_in_count,
        "belongs_to_category": belongs_count,
        "project_created": db_path.is_dir(),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("docs/feature-graph.db"))
    parser.add_argument("--json", action="store_true", help="emit CI evidence")
    arguments = parser.parse_args()
    summary = build_graph(arguments.output)
    if arguments.json:
        print(f"{RESULT_PREFIX}{json.dumps(summary, sort_keys=True)}")
    else:
        print("\nFeature graph built successfully.")
        print(f"Query it with: GraphForge({str(arguments.output)!r})")
