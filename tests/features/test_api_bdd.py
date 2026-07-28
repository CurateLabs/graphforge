"""pytest-bdd runner for all tests/features/api/*.feature files.

scenarios() is called here (in the test file) so pytest-bdd can resolve
feature paths relative to this file's location.

All scenarios are expected to xfail at the stub stage. They are un-xfailed
milestone by milestone as real implementations land.
"""

from pathlib import Path

from pytest_bdd import scenarios

# Import step definitions so pytest discovers them.
import tests.features.steps.api_steps  # noqa: F401

_API_DIR = Path(__file__).parent / "api"

scenarios(
    str(_API_DIR / "execute.feature"),
    str(_API_DIR / "construction.feature"),
    str(_API_DIR / "rank.feature"),
    str(_API_DIR / "cluster.feature"),
    str(_API_DIR / "analyze.feature"),
    str(_API_DIR / "find.feature"),
    str(_API_DIR / "index.feature"),
    str(_API_DIR / "introspection.feature"),
    str(_API_DIR / "explain.feature"),
    str(_API_DIR / "lifecycle.feature"),
    str(_API_DIR / "errors.feature"),
    str(_API_DIR / "edge_cases.feature"),
    str(_API_DIR / "type_errors.feature"),
    str(_API_DIR / "validation.feature"),
    str(_API_DIR / "recipes.feature"),
    str(_API_DIR / "ontology.feature"),
)
