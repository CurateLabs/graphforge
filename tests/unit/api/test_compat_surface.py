"""Import-surface coverage for thin Python compatibility modules."""

from __future__ import annotations

from graphforge import GraphForge, api, exceptions


def test_api_reexports_graphforge() -> None:
    assert api.GraphForge is GraphForge
    forge = api.GraphForge()
    assert forge.path is None


def test_exceptions_reexport_hierarchy() -> None:
    assert issubclass(exceptions.StorageError, exceptions.GraphForgeError)
    assert issubclass(exceptions.ParseError, exceptions.GraphForgeError)
    assert issubclass(exceptions.ValidationError, exceptions.GraphForgeError)
