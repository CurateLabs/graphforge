"""GraphForge public API (compatibility module).

Re-exports the native ``GraphForge``, handles, and cancellation token so
their compatibility imports keep working.
"""

# Re-exported from the compiled extension (typed by _graphforge_rs.pyi).
from graphforge._graphforge_rs import CancellationToken, EdgeHandle, GraphForge, NodeHandle

__all__ = ["CancellationToken", "EdgeHandle", "GraphForge", "NodeHandle"]
