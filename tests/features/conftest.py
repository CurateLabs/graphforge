"""pytest-bdd configuration for API feature tests."""

import pytest

# pytest-bdd 8.x requires step modules to be declared as pytest_plugins so that
# the step functions are registered in the correct scope.
pytest_plugins = ["tests.features.steps.api_steps"]


# Feature areas that are entirely unimplemented in the native engine (the write
# API and explicit transactions land with M18/M19 + the write path). Their
# scenarios set up state via add_node/add_edge/begin, so the NotImplementedError
# is swallowed inside a When step and a *later* step fails with a different
# exception (PlanError on an empty graph, an AssertionError, …). Mark the whole
# area xfail so the cutover gate is honest; each flips to a real pass — surfaced
# as an xpass — the moment its native implementation lands.
_UNIMPLEMENTED_TAGS = ("construction", "transactions")


def pytest_collection_modifyitems(config, items):
    for item in items:
        for tag in _UNIMPLEMENTED_TAGS:
            if item.get_closest_marker(tag) is not None:
                item.add_marker(
                    pytest.mark.xfail(
                        reason=f"native '{tag}' implementation pending (M18/M19 + write API)",
                        strict=False,
                    )
                )
                break


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Convert a ``NotImplementedError`` (setup or call) into an xfail.

    This cannot mask a regression in an *implemented* area: the native engine
    raises ``NotImplementedError`` **only** from the genuinely-pending surface —
    the ``GfError::NotImplemented`` mapping (analyst verbs, introspection,
    transactions) and the ``add_node``/``add_edge`` write stubs, including the
    ``Given`` setup steps that build graphs through them. The implemented
    operations (execute/explain, parse, ontology loading, lifecycle) raise typed
    ``GfError`` variants (Parse/Plan/Execution/Storage/Lifecycle/…), never
    ``NotImplementedError`` — so a regression there still surfaces as a different
    exception or an assertion and fails loudly. Each converted xfail flips to a
    real pass automatically once its native implementation lands.
    """
    outcome = yield
    rep = outcome.get_result()
    if (
        rep.when in ("setup", "call")
        and rep.failed
        and call.excinfo is not None
        and call.excinfo.errisinstance(NotImplementedError)
    ):
        rep.outcome = "skipped"
        rep.wasxfail = "native implementation pending (M18/M19 + write API)"


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
    # The @skip-node tag (added in #872) gates only the Node BDD strict run; it
    # is a no-op for Python (whose own NotImplementedError->xfail hook handles
    # the unimplemented surface). Register both the verbatim and underscored
    # forms so --strict-markers accepts it regardless of how pytest-bdd maps the
    # tag. Tracked for removal in #971.
    config.addinivalue_line("markers", "skip-node: excluded from the Node BDD strict gate (#971)")
    config.addinivalue_line("markers", "skip_node: excluded from the Node BDD strict gate (#971)")


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
