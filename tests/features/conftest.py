"""pytest-bdd configuration for API feature tests."""

import pytest

# pytest-bdd 8.x requires step modules to be declared as pytest_plugins so that
# the step functions are registered in the correct scope.
pytest_plugins = ["tests.features.steps.api_steps"]


def pytest_collection_modifyitems(items):
    """Report issue-backed product gaps as excluded, never passed or xfailed."""
    for item in items:
        if (
            item.get_closest_marker("excluded-api-bdd") is None
            and item.get_closest_marker("excluded_api_bdd") is None
        ):
            continue
        issue_markers = [
            marker.name.removeprefix("issue_").removeprefix("issue-")
            for marker in item.iter_markers()
            if marker.name.startswith(("issue_", "issue-"))
        ]
        if len(issue_markers) != 1:
            raise RuntimeError("excluded API BDD scenario needs one issue tag")
        item.add_marker(
            pytest.mark.skip(reason=f"excluded API BDD contract: issue #{issue_markers[0]}")
        )


# Register markers so --strict-markers doesn't reject them.
def pytest_configure(config):
    config.addinivalue_line("markers", "api: GraphForge public API BDD scenarios")
    config.addinivalue_line("markers", "execute: forge.execute() scenarios")
    config.addinivalue_line("markers", "construction: graph construction scenarios")
    config.addinivalue_line("markers", "rank: forge.rank() scenarios")
    config.addinivalue_line("markers", "cluster: forge.cluster() scenarios")
    config.addinivalue_line("markers", "find: forge.find() scenarios")
    config.addinivalue_line("markers", "introspection: introspection API scenarios")
    config.addinivalue_line("markers", "lifecycle: lifecycle and transaction scenarios")
    config.addinivalue_line("markers", "errors: error-handling scenarios")
    config.addinivalue_line("markers", "edge_cases: edge-case scenarios")
    config.addinivalue_line("markers", "types: type-error scenarios")
    config.addinivalue_line("markers", "validation: input-validation scenarios")
    config.addinivalue_line("markers", "recipes: recipes API scenarios")
    config.addinivalue_line("markers", "ontology: ontology API scenarios")
    config.addinivalue_line("markers", "transactions: transaction scenarios")
    config.addinivalue_line("markers", "persistence: persistence scenarios")
    config.addinivalue_line("markers", "excluded-api-bdd: issue-backed product exclusion")
    config.addinivalue_line("markers", "excluded_api_bdd: issue-backed product exclusion")
    config.addinivalue_line("markers", "excluded-node-api-bdd: Node-only issue-backed exclusion")
    config.addinivalue_line("markers", "excluded_node_api_bdd: Node-only issue-backed exclusion")
    config.addinivalue_line(
        "markers", "binding-only: runtime binding contract not applicable to Rust"
    )
    config.addinivalue_line(
        "markers", "binding_only: runtime binding contract not applicable to Rust"
    )
    for issue in (352, 353, 354, 355, 356, 357):
        config.addinivalue_line("markers", f"issue-{issue}: exclusion tracking issue")
        config.addinivalue_line("markers", f"issue_{issue}: exclusion tracking issue")


@pytest.fixture
def tmp_parquet_dir(tmp_path):
    """A temporary directory that exists on disk (valid Parquet path)."""
    d = tmp_path / "graph"
    d.mkdir()
    return d


@pytest.fixture
def nonexistent_path(tmp_path):
    """A path that does not exist on disk."""
    return tmp_path / "does_not_exist"
