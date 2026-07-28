"""GraphForge exception hierarchy (compatibility module).

Re-exports the native exception classes so ``from graphforge.exceptions import
…`` keeps working. The classes themselves are defined in the compiled extension
(see ``graphforge._graphforge_rs``).
"""

# Re-exported from the compiled extension (typed by _graphforge_rs.pyi).
from graphforge._graphforge_rs import (
    ExecutionError,
    GraphForgeError,
    LifecycleError,
    OntologyError,
    ParseError,
    PlanError,
    StorageError,
    ValidationError,
)

__all__ = [
    "ExecutionError",
    "GraphForgeError",
    "LifecycleError",
    "OntologyError",
    "ParseError",
    "PlanError",
    "StorageError",
    "ValidationError",
]
