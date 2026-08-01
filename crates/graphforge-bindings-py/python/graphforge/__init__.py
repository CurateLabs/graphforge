"""GraphForge — embedded openCypher graph engine (native Rust core).

This package re-exports the engine from the compiled extension
``graphforge._graphforge_rs``. The query/verb surface lands across the M16
follow-up PRs; this scaffold (#585/#588) exposes construction, version, and the
exception hierarchy.
"""

# Re-exported from the compiled extension (typed by _graphforge_rs.pyi).
from graphforge._graphforge_rs import (
    CancellationToken,
    EdgeHandle,
    ExecutionError,
    GraphForge,
    GraphForgeError,
    InvocationDescriptor,
    LifecycleError,
    NodeHandle,
    OntologyError,
    ParseError,
    PlanError,
    RecordedAlgorithmResult,
    StorageError,
    ValidationError,
    __version__,
    composite_provenance_uuid,
    version,
)

__all__ = [
    "CancellationToken",
    "EdgeHandle",
    "ExecutionError",
    "GraphForge",
    "GraphForgeError",
    "InvocationDescriptor",
    "LifecycleError",
    "NodeHandle",
    "OntologyError",
    "ParseError",
    "PlanError",
    "RecordedAlgorithmResult",
    "StorageError",
    "ValidationError",
    "__version__",
    "composite_provenance_uuid",
    "version",
]
