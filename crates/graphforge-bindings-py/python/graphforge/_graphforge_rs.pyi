"""Type stub for the compiled GraphForge extension (`graphforge._graphforge_rs`).

Hand-written to match the PyO3 surface in `crates/graphforge-bindings-py/src/lib.rs`.
`execute_polars` is typed as `Any` so the stub doesn't force the optional
`polars` dependency onto consumers' type-checkers; the Arrow-returning methods
use `pyarrow` (a hard dependency).
"""

from collections.abc import Callable
from typing import Any, Literal, overload
import uuid

import pyarrow

__version__: str

def _cli_execute(args: list[str]) -> tuple[int, bytes, bytes]: ...
def version() -> str: ...
def composite_provenance_uuid(
    operation_uuid: str,
    event_kind: str,
    recorded_at_micros: int,
    actor_uuid: str | None = None,
) -> str: ...

class NodeHandle:
    @property
    def uuid(self) -> str: ...
    @property
    def label(self) -> str: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class EdgeHandle:
    @property
    def uuid(self) -> str: ...
    @property
    def rel_type(self) -> str: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class CancellationToken:
    def __init__(self) -> None: ...
    def cancel(self) -> None: ...
    @property
    def is_cancelled(self) -> bool: ...

class GraphImportSession:
    @property
    def session_uuid(self) -> str: ...
    def status(self) -> dict[str, Any]: ...
    def append_arrow(self, kind: str, data: Any) -> None: ...
    def register_parquet(self, kind: str, path: str) -> None: ...
    def checkpoint(self) -> dict[str, Any]: ...
    def validate(self, *, cancellation: CancellationToken | None = None) -> dict[str, Any]: ...
    def commit(self, *, cancellation: CancellationToken | None = None) -> str: ...
    def abort(self) -> dict[str, Any]: ...

class GraphTransaction:
    def status(self) -> dict[str, Any]: ...
    def stage_cypher(self, query: str, params: dict[str, Any] | None = None) -> None: ...
    def stage_add_node(
        self,
        node_uuid: str,
        label: str,
        properties: dict[str, Any] | None = None,
    ) -> None: ...
    def stage_add_edge(
        self,
        edge_uuid: str,
        rel_type: str,
        source_uuid: str,
        target_uuid: str,
        properties: dict[str, Any] | None = None,
    ) -> None: ...
    def validate(self) -> None: ...
    def commit(self, *, cancellation: CancellationToken | None = None) -> str: ...
    def rollback(self) -> None: ...
    def __enter__(self) -> GraphTransaction: ...
    def __exit__(
        self,
        exc_type: Any = None,
        _exc: Any = None,
        _tb: Any = None,
    ) -> bool: ...

class CheckpointView:
    @property
    def checkpoint_uuid(self) -> str: ...
    @property
    def generation_uuid(self) -> str: ...
    def execute(self, query: str) -> pyarrow.Table: ...
    def project_capabilities(self) -> pyarrow.Table: ...
    def inspect_adjacency(self) -> dict[str, Any]: ...

class InvocationDescriptor:
    @property
    def canonical_bytes(self) -> bytes: ...
    @property
    def fingerprint(self) -> str: ...
    @property
    def projection_fingerprint(self) -> str: ...
    @property
    def verb(self) -> str: ...
    @property
    def algorithm(self) -> str: ...

class RecordedAlgorithmResult:
    @property
    def run_uuid(self) -> str: ...
    @property
    def result(self) -> pyarrow.Table: ...

class ResolvedRecordedAlgorithmResult:
    @property
    def run_uuid(self) -> str: ...
    @property
    def result(self) -> pyarrow.Table: ...
    @property
    def attachment_state(self) -> Literal["attached", "attachment_failed"]: ...
    @property
    def attachment(self) -> pyarrow.Table | None: ...
    @property
    def attachment_uuid(self) -> str | None: ...
    @property
    def attachment_error_code(self) -> str | None: ...

class ResolvedBeliefProjection:
    @property
    def source_generation_uuid(self) -> str: ...
    @property
    def graph_content_fingerprint(self) -> str: ...
    @property
    def policy_bytes(self) -> bytes: ...
    @property
    def policy_fingerprint(self) -> str: ...
    @property
    def snapshot_fingerprint(self) -> str: ...
    @property
    def valid_time_fingerprint(self) -> str | None: ...
    @property
    def source_record_uuids(self) -> list[str]: ...
    @property
    def transaction_cutoff(self) -> int: ...
    @property
    def valid_time(self) -> int | None: ...
    def prepare_rank_invocation(
        self,
        label: str,
        *,
        by: str,
        via: str | None = None,
        directed: bool = True,
    ) -> InvocationDescriptor: ...
    def prepare_cluster_invocation(
        self,
        label: str,
        *,
        by: str,
        vector_property: str | None = None,
        via: str | None = None,
        directed: bool = False,
    ) -> InvocationDescriptor: ...
    def prepare_paths_invocation(
        self,
        source: str | NodeHandle | dict[str, Any] | None = None,
        target: str | NodeHandle | dict[str, Any] | None = None,
        *,
        by: str,
        via: str | None = None,
        directed: bool = True,
        k: int = 1,
        weight: str | None = None,
        capacity_property: str | None = None,
        cost_property: str | None = None,
        heuristic: str | None = None,
        walk_length: int | None = None,
        seed: int | None = None,
        terminal_uuids: list[str] | None = None,
        prize_property: str | None = None,
    ) -> InvocationDescriptor: ...
    def prepare_analyze_invocation(
        self,
        label: str | None = None,
        *,
        by: str,
        via: str | None = None,
        directed: bool = True,
        weight: str | None = None,
        partition_property: str | None = None,
        k: int | None = None,
    ) -> InvocationDescriptor: ...
    def prepare_similar_invocation(
        self,
        label: str,
        *,
        by: str,
        k: int = 10,
        vector_property: str | None = None,
        via: str | None = None,
    ) -> InvocationDescriptor: ...

class GraphForge:
    def __init__(
        self,
        path: str | None = None,
        *,
        write_mode: Literal[
            "single_writer", "queued_writer", "optimistic_multi_writer"
        ] = "single_writer",
        write_queue_capacity: int = 64,
        max_rebase_attempts: int = 3,
    ) -> None: ...
    def project_capabilities(self) -> pyarrow.Table: ...
    def checkpoint(
        self,
        *,
        name: str,
        idempotency_key: str | uuid.UUID,
        description: str | None = None,
        actor_uuid: str | None = None,
    ) -> pyarrow.Table: ...
    def list_checkpoints(
        self,
        *,
        limit: int = 100,
        after: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def open_checkpoint(self, name: str) -> CheckpointView: ...
    def delete_checkpoint(
        self,
        *,
        name: str,
        idempotency_key: str | uuid.UUID,
        actor_uuid: str | None = None,
    ) -> pyarrow.Table: ...
    def revert_to_checkpoint(
        self,
        *,
        name: str,
        reason: str,
        idempotency_key: str | uuid.UUID,
        actor_uuid: str | None = None,
    ) -> pyarrow.Table: ...
    def committed_generation_identity(self) -> dict[str, bytes]: ...
    def diff_committed_generations(
        self,
        *,
        source_generation_uuid: bytes,
        source_manifest_sha256: bytes,
        target_generation_uuid: bytes,
        target_manifest_sha256: bytes,
        max_records_per_generation: int = 1_000_000,
        max_output_bytes: int = 268_435_456,
        cancellation: CancellationToken | None = None,
    ) -> dict[str, Any]: ...
    def preview_portable_v2_selection(
        self,
        *,
        checkpoint: str | None = None,
        profile: str = "complete",
        identities: list[dict[str, str]] | None = None,
        strict: bool = False,
        limits: dict[str, Any] | None = None,
    ) -> dict[str, Any]: ...
    def preview_portable_v2_graph_subset(
        self,
        *,
        subset: dict[str, Any],
        checkpoint: str | None = None,
        limits: dict[str, Any] | None = None,
    ) -> dict[str, Any]: ...
    def export_portable_v2(
        self,
        *,
        output_path: str,
        representation: str = "bundle",
        profile: str = "complete",
        identities: list[dict[str, str]] | None = None,
        checkpoint: str | None = None,
        subset: dict[str, Any] | None = None,
        limits: dict[str, Any] | None = None,
        cancellation: CancellationToken | None = None,
        progress: Callable[[dict[str, int]], object] | None = None,
    ) -> dict[str, Any]: ...
    @staticmethod
    def verify_portable_v2(
        input: str,
        *,
        mode: str = "full",
        limits: dict[str, Any] | None = None,
        cancellation: CancellationToken | None = None,
    ) -> dict[str, Any]: ...
    @staticmethod
    def import_portable_v2(
        project_root: str,
        *,
        input: str,
        operation_id: str,
        limits: dict[str, Any] | None = None,
        cancellation: CancellationToken | None = None,
    ) -> dict[str, Any]: ...
    @staticmethod
    def publish_portable_v2_oci(
        *,
        package_path: str,
        registry: str,
        repository: str,
        tag: str | None = None,
        limits: dict[str, Any] | None = None,
        authenticity: dict[str, Any] | None = None,
        signature: dict[str, Any] | None = None,
        insecure_http: bool = False,
        credential: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> dict[str, Any]: ...
    @staticmethod
    def pull_portable_v2_oci(
        *,
        registry: str,
        repository: str,
        reference: str,
        destination: str,
        expected_oci_digest: str | None = None,
        limits: dict[str, Any] | None = None,
        authenticity: dict[str, Any] | None = None,
        insecure_http: bool = False,
        credential: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> dict[str, Any]: ...
    def begin_import_session(
        self,
        *,
        operation_uuid: str,
        batch_rows: int | None = None,
        max_source_bytes: int | None = None,
        max_files: int | None = None,
        max_rejected_rows: int | None = None,
        io_concurrency: int | None = None,
    ) -> GraphImportSession: ...
    def resume_import_session(self, session_uuid: str) -> GraphImportSession: ...
    def cleanup_stale_import_sessions(self, *, max_age_secs: int) -> int: ...
    def execute_to_parquet_stream(
        self,
        query: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        max_row_group_rows: int = 65536,
        max_batch_rows: int = 65536,
        cancellation: CancellationToken | None = None,
    ) -> dict[str, Any]: ...
    def execute_to_arrow_ipc_stream(
        self,
        query: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        max_row_group_rows: int = 65536,
        max_batch_rows: int = 65536,
        cancellation: CancellationToken | None = None,
    ) -> dict[str, Any]: ...
    def diff_checkpoints(
        self,
        *,
        from_checkpoint: str | None = None,
        to_checkpoint: str | None = None,
        scope: Literal[
            "summary",
            "graph",
            "ontology",
            "configuration",
            "capabilities",
            "provenance",
            "knowledge",
            "epistemic",
            "all",
        ] = "summary",
        detail: Literal["summary", "records"] = "summary",
        limit: int = 100,
        after: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def enable_capability(
        self,
        *,
        operation_uuid: str,
        capability_id: Literal["graph", "provenance", "knowledge", "epistemic", "valid_time"],
        capability_version: int,
        actor_uuid: str | None = None,
    ) -> pyarrow.Table: ...
    def provenance_event(
        self,
        provenance_uuid: str,
        *,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def list_provenance_history(
        self,
        *,
        subject_uuid: str | None = None,
        operation_uuid: str | None = None,
        limit: int = 100,
        after: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def create_assertion(
        self,
        *,
        operation_uuid: str,
        assertion_uuid: str,
        claim: str,
        graph_refs: list[
            dict[
                Literal["graph_uuid", "graph_kind", "role", "ordinal"],
                str | int,
            ]
        ],
        actor_uuid: str | None = None,
    ) -> pyarrow.Table: ...
    def create_assertion_with_evidence(
        self,
        *,
        operation_uuid: str,
        assertion_uuid: str,
        claim: str,
        graph_refs: list[
            dict[
                Literal["graph_uuid", "graph_kind", "role", "ordinal"],
                str | int,
            ]
        ],
        evidence: list[
            dict[
                Literal["evidence_uuid", "source_uuid", "source_kind", "role", "weight"],
                str | float | None,
            ]
        ],
        actor_uuid: str | None = None,
    ) -> pyarrow.Table: ...
    def assertion(
        self,
        assertion_uuid: str,
        *,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def list_assertions(
        self,
        *,
        graph_uuid: str | None = None,
        limit: int = 100,
        after: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def assertion_graph_refs(
        self,
        assertion_uuid: str,
        *,
        limit: int = 100,
        after: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def assess_confidence(
        self,
        *,
        operation_uuid: str,
        confidence_uuid: str,
        assertion_uuid: str,
        policy: Literal["explicit", "conservative_min"],
        value: float | None = None,
        input_confidence_uuids: list[str] | None = None,
        actor_uuid: str | None = None,
    ) -> pyarrow.Table: ...
    def confidence_assessment(
        self,
        confidence_uuid: str,
        *,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def list_confidence_assessments(
        self,
        *,
        assertion_uuid: str | None = None,
        limit: int = 100,
        after: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def confidence_inputs(
        self,
        confidence_uuid: str,
        *,
        limit: int = 100,
        after: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def attach_evidence(
        self,
        *,
        operation_uuid: str,
        evidence_uuid: str,
        assertion_uuid: str,
        source_uuid: str,
        source_kind: Literal["document", "observation", "graph_node", "graph_edge"],
        role: Literal["supports", "contradicts", "context"],
        weight: float | None = None,
        actor_uuid: str | None = None,
    ) -> pyarrow.Table: ...
    def evidence_link(
        self,
        evidence_uuid: str,
        *,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def list_evidence_links(
        self,
        *,
        assertion_uuid: str | None = None,
        source_uuid: str | None = None,
        limit: int = 100,
        after: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def create_assertion_with_status(
        self,
        *,
        operation_uuid: str,
        assertion_uuid: str,
        claim: str,
        graph_refs: list[dict[str, Any]],
        status_event_uuid: str,
        status: Literal[
            "hypothesis", "supported", "refuted", "disputed", "retracted", "superseded"
        ],
        actor_uuid: str | None = None,
    ) -> pyarrow.Table: ...
    def record_reasoning(
        self,
        *,
        operation_uuid: str,
        reasoning_uuid: str,
        assertion_uuid: str,
        kind: str,
        content_format: str,
        content: bytes,
        provenance_uuid: str,
        supersedes_reasoning_uuid: str | None = None,
        actor_uuid: str | None = None,
    ) -> pyarrow.Table: ...
    def reasoning(
        self,
        reasoning_uuid: str,
        *,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def list_reasoning(
        self,
        *,
        assertion_uuid: str | None = None,
        limit: int = 100,
        after: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def record_assertion_status(
        self,
        *,
        operation_uuid: str,
        status_event_uuid: str,
        assertion_uuid: str,
        status: Literal[
            "hypothesis", "supported", "refuted", "disputed", "retracted", "superseded"
        ],
        provenance_uuid: str,
        confidence_uuid: str | None = None,
        reasoning_uuid: str | None = None,
        actor_uuid: str | None = None,
    ) -> pyarrow.Table: ...
    def assertion_status(self, assertion_uuid: str) -> pyarrow.Table: ...
    def list_assertion_status(
        self,
        *,
        assertion_uuid: str | None = None,
        limit: int = 100,
        after: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def supersede_assertion(
        self,
        *,
        operation_uuid: str,
        supersession_uuid: str,
        prior_assertion_uuid: str,
        replacement_assertion_uuid: str,
        status_event_uuid: str,
        reasoning_uuid: str,
        provenance_uuid: str,
        actor_uuid: str | None = None,
    ) -> pyarrow.Table: ...
    def list_assertion_supersessions(
        self,
        *,
        prior_assertion_uuid: str | None = None,
        replacement_assertion_uuid: str | None = None,
        limit: int = 100,
        after: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def create_hypothesis_group(
        self,
        *,
        operation_uuid: str,
        group_uuid: str,
        question_key: str,
        provenance_uuid: str,
        actor_uuid: str | None = None,
    ) -> pyarrow.Table: ...
    def record_hypothesis_membership(
        self,
        *,
        operation_uuid: str,
        membership_event_uuid: str,
        group_uuid: str,
        assertion_uuid: str,
        action: Literal["added", "removed"],
        reasoning_uuid: str,
        provenance_uuid: str,
        actor_uuid: str | None = None,
    ) -> pyarrow.Table: ...
    def record_hypothesis_selection(
        self,
        *,
        operation_uuid: str,
        selection_event_uuid: str,
        group_uuid: str,
        reasoning_uuid: str,
        provenance_uuid: str,
        selected_assertion_uuid: str | None = None,
        actor_uuid: str | None = None,
    ) -> pyarrow.Table: ...
    def remove_hypothesis_member(
        self,
        *,
        operation_uuid: str,
        membership_event_uuid: str,
        selection_event_uuid: str,
        group_uuid: str,
        assertion_uuid: str,
        reasoning_uuid: str,
        provenance_uuid: str,
        selected_assertion_uuid: str | None = None,
        actor_uuid: str | None = None,
    ) -> pyarrow.Table: ...
    def list_hypothesis_groups(
        self,
        *,
        question_key: str | None = None,
        limit: int = 100,
        after: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def list_hypothesis_membership(
        self,
        *,
        group_uuid: str | None = None,
        assertion_uuid: str | None = None,
        limit: int = 100,
        after: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def list_hypothesis_selection(
        self,
        *,
        group_uuid: str | None = None,
        limit: int = 100,
        after: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def hypothesis_members(self, group_uuid: str) -> pyarrow.Table: ...
    def hypothesis_selection(self, group_uuid: str) -> pyarrow.Table: ...
    def epistemic_snapshot(self, *, transaction_cutoff: int) -> pyarrow.Table: ...
    def record_assertion_validity(
        self,
        *,
        operation_uuid: str,
        validity_event_uuid: str,
        assertion_uuid: str,
        provenance_uuid: str,
        valid_from: int | None = None,
        valid_to: int | None = None,
        reasoning_uuid: str | None = None,
        actor_uuid: str | None = None,
    ) -> pyarrow.Table: ...
    def list_assertion_validity(
        self,
        *,
        assertion_uuid: str | None = None,
        limit: int = 100,
        after: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def apply_valid_time(self, *, transaction_cutoff: int, valid_time: int) -> pyarrow.Table: ...
    def execute(self, query: str, params: dict[str, Any] | None = None) -> pyarrow.Table: ...
    def execute_polars(self, query: str, params: dict[str, Any] | None = None) -> Any: ...
    def execute_stream(
        self, query: str, params: dict[str, Any] | None = None
    ) -> pyarrow.RecordBatchReader: ...
    def explain(self, query: str) -> str: ...
    def load_ontology(self, path: str) -> None: ...
    def inspect_runtime_catalog(self) -> dict[str, Any]: ...
    def suggest_ontology(self, ontology_id: str, version: str) -> dict[str, Any]: ...
    def validate_ontology(self, document: dict[str, Any]) -> dict[str, Any]: ...
    def export_ontology(
        self,
        source: Literal["suggested", "loaded", "adopted"],
        destination: str,
        format: Literal["yaml", "yml", "json"],
        *,
        document: dict[str, Any] | None = None,
    ) -> None: ...
    def workspace_ontology(self) -> dict[str, Any]: ...
    def adopt_ontology(
        self,
        path: str,
        mode: Literal["advisory", "strict"],
        *,
        operation_uuid: str,
        actor_uuid: str | None = None,
    ) -> None: ...
    def clear_ontology(self, *, operation_uuid: str, actor_uuid: str | None = None) -> None: ...
    def ontology_modules(self) -> list[dict[str, Any]]: ...
    def ontology_authority_state(self) -> dict[str, Any]: ...
    def inspect_ontology_module(
        self,
        ontology_id: str,
        *,
        authored_version: str | None = None,
        canonical_digest: str | None = None,
    ) -> dict[str, Any]: ...
    def validate_ontology_module(self, document: dict[str, Any]) -> dict[str, Any]: ...
    def create_ontology_module(
        self,
        document: dict[str, Any],
        dependencies: list[dict[str, Any]],
        *,
        enforcement: Literal["exploratory", "advisory", "strict"] | None = None,
    ) -> dict[str, Any]: ...
    def import_ontology_module(
        self,
        text: str,
        dependencies: list[dict[str, Any]],
        *,
        format: Literal["auto", "json", "yaml", "yml"] = "auto",
    ) -> dict[str, Any]: ...
    def adopt_ontology_module(
        self,
        candidate: dict[str, Any],
        *,
        expected_project_generation_uuid: str,
        expected_composition_fingerprint: str | None,
        operation_uuid: str,
        actor_uuid: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> dict[str, Any]: ...
    def preview_update_ontology_module(
        self,
        ontology_id: str,
        document: dict[str, Any],
        dependencies: list[dict[str, Any]],
        *,
        authored_version: str | None = None,
        canonical_digest: str | None = None,
    ) -> dict[str, Any]: ...
    def update_ontology_module(
        self,
        ontology_id: str,
        document: dict[str, Any],
        dependencies: list[dict[str, Any]],
        *,
        authored_version: str | None = None,
        canonical_digest: str | None = None,
        enforcement: Literal["exploratory", "advisory", "strict"] | None = None,
        expected_project_generation_uuid: str,
        expected_composition_fingerprint: str | None,
        operation_uuid: str,
        actor_uuid: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> dict[str, Any]: ...
    def preview_migrate_ontology_module(
        self,
        ontology_id: str,
        document: dict[str, Any],
        dependencies: list[dict[str, Any]],
        *,
        authored_version: str | None = None,
        canonical_digest: str | None = None,
        enforcement: Literal["exploratory", "advisory", "strict"] | None = None,
        expected_project_generation_uuid: str,
        expected_composition_fingerprint: str | None,
        operation_uuid: str,
        actor_uuid: str | None = None,
    ) -> dict[str, Any]: ...
    def migrate_ontology_module(
        self,
        ontology_id: str,
        document: dict[str, Any],
        dependencies: list[dict[str, Any]],
        preview: dict[str, Any],
        *,
        authored_version: str | None = None,
        canonical_digest: str | None = None,
        enforcement: Literal["exploratory", "advisory", "strict"] | None = None,
        expected_project_generation_uuid: str,
        expected_composition_fingerprint: str | None,
        operation_uuid: str,
        actor_uuid: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> dict[str, Any]: ...
    def multi_ontology_certification_report(
        self,
        composition_before: str,
        migration_plan_digest: str,
        rows_scanned: int,
    ) -> dict[str, Any]: ...
    def preview_delete_ontology_module(
        self,
        ontology_id: str,
        *,
        authored_version: str | None = None,
        canonical_digest: str | None = None,
    ) -> dict[str, Any]: ...
    def delete_ontology_module(
        self,
        ontology_id: str,
        *,
        authored_version: str | None = None,
        canonical_digest: str | None = None,
        expected_project_generation_uuid: str,
        expected_composition_fingerprint: str | None,
        operation_uuid: str,
        actor_uuid: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> dict[str, Any]: ...
    def export_ontology_module(
        self,
        ontology_id: str,
        *,
        format: Literal["json", "yaml", "yml"],
        authored_version: str | None = None,
        canonical_digest: str | None = None,
    ) -> str: ...
    def ontology_bridges(self) -> list[dict[str, Any]]: ...
    def inspect_ontology_bridge(
        self,
        bridge_id: str,
        *,
        authored_version: str | None = None,
        canonical_digest: str | None = None,
    ) -> dict[str, Any]: ...
    def validate_ontology_bridge(self, document: dict[str, Any]) -> dict[str, Any]: ...
    def create_ontology_bridge(self, document: dict[str, Any]) -> dict[str, Any]: ...
    def import_ontology_bridge(
        self, text: str, *, format: Literal["auto", "json", "yaml", "yml"] = "auto"
    ) -> dict[str, Any]: ...
    def adopt_ontology_bridge(
        self,
        candidate: dict[str, Any],
        *,
        expected_project_generation_uuid: str,
        expected_composition_fingerprint: str | None,
        operation_uuid: str,
        actor_uuid: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> dict[str, Any]: ...
    def preview_update_ontology_bridge(
        self,
        bridge_id: str,
        document: dict[str, Any],
        *,
        authored_version: str | None = None,
        canonical_digest: str | None = None,
    ) -> dict[str, Any]: ...
    def update_ontology_bridge(
        self,
        bridge_id: str,
        document: dict[str, Any],
        *,
        authored_version: str | None = None,
        canonical_digest: str | None = None,
        expected_project_generation_uuid: str,
        expected_composition_fingerprint: str | None,
        operation_uuid: str,
        actor_uuid: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> dict[str, Any]: ...
    def preview_delete_ontology_bridge(
        self,
        bridge_id: str,
        *,
        authored_version: str | None = None,
        canonical_digest: str | None = None,
    ) -> dict[str, Any]: ...
    def delete_ontology_bridge(
        self,
        bridge_id: str,
        *,
        authored_version: str | None = None,
        canonical_digest: str | None = None,
        expected_project_generation_uuid: str,
        expected_composition_fingerprint: str | None,
        operation_uuid: str,
        actor_uuid: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> dict[str, Any]: ...
    def export_ontology_bridge(
        self,
        bridge_id: str,
        *,
        format: Literal["json", "yaml", "yml"],
        authored_version: str | None = None,
        canonical_digest: str | None = None,
    ) -> str: ...
    def ontology_activation_profile(self) -> dict[str, Any]: ...
    def change_ontology_activation_profile(
        self,
        profile_default: Literal["exploratory", "advisory", "strict"],
        activation: list[dict[str, Any]],
        *,
        expected_project_generation_uuid: str,
        expected_composition_fingerprint: str | None,
        operation_uuid: str,
        actor_uuid: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> dict[str, Any]: ...
    def validate_ontology_composition(self, candidate: dict[str, Any]) -> dict[str, Any]: ...
    def preflight_ontology_composition(
        self,
        candidate: dict[str, Any],
        *,
        expected_project_generation_uuid: str,
        expected_composition_fingerprint: str | None,
        operation_uuid: str,
        actor_uuid: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> dict[str, Any]: ...
    def explain_ontology_resolution(
        self,
        kind: Literal["entity", "relation", "property"],
        local_id: str,
        *,
        module: dict[str, Any] | None = None,
        max_candidates: int = 16,
    ) -> dict[str, Any]: ...
    def portable_ontology_staging(self) -> dict[str, Any] | None: ...
    def adopt_portable_ontology_staging(
        self,
        *,
        expected_project_generation_uuid: str,
        expected_composition_fingerprint: str | None,
        operation_uuid: str,
        actor_uuid: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> dict[str, Any]: ...
    def rank(
        self,
        label: str,
        *,
        by: str,
        via: str | None = None,
        directed: bool = True,
        write_property: str | None = None,
    ) -> pyarrow.Table: ...
    def prepare_rank_invocation(
        self,
        label: str,
        *,
        by: str,
        via: str | None = None,
        directed: bool = True,
    ) -> InvocationDescriptor: ...
    def invoke_descriptor(self, descriptor: InvocationDescriptor) -> pyarrow.Table: ...
    def invoke_descriptor_bytes(self, descriptor: bytes) -> pyarrow.Table: ...
    def invoke_recorded(
        self,
        *,
        operation_uuid: str,
        run_uuid: str,
        descriptor: InvocationDescriptor,
        actor_uuid: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> RecordedAlgorithmResult: ...
    def resolve_belief_projection(
        self,
        *,
        transaction_cutoff: int,
        included_statuses: list[
            Literal[
                "hypothesis",
                "supported",
                "refuted",
                "disputed",
                "retracted",
                "superseded",
            ]
        ],
        statusless: Literal["reject", "exclude", "include"],
        supersession_branches: Literal["reject", "include_all_leaves"],
        hypotheses: Literal[
            "require_selected",
            "exclude_unselected_group",
            "include_all_current_members",
        ],
        valid_time: int | None = None,
    ) -> ResolvedBeliefProjection: ...
    def invoke_resolved_recorded(
        self,
        *,
        projection: ResolvedBeliefProjection,
        operation_uuid: str,
        run_uuid: str,
        attachment_uuid: str,
        descriptor: InvocationDescriptor,
        actor_uuid: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> ResolvedRecordedAlgorithmResult: ...
    def attach_resolved_run(
        self,
        *,
        projection: ResolvedBeliefProjection,
        operation_uuid: str,
        attachment_uuid: str,
        run_uuid: str,
        descriptor: InvocationDescriptor,
        actor_uuid: str | None = None,
    ) -> pyarrow.Table: ...
    def algorithm_run(
        self,
        run_uuid: str,
        *,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def list_algorithm_runs(
        self,
        *,
        algorithm: str | None = None,
        limit: int = 100,
        after: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def algorithm_run_events(
        self,
        run_uuid: str,
        *,
        limit: int = 100,
        after: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> pyarrow.Table: ...
    def cluster(
        self,
        label: str,
        *,
        by: str,
        vector_property: str | None = None,
        via: str | None = None,
        directed: bool = False,
        write_property: str | None = None,
    ) -> pyarrow.Table: ...
    def paths(
        self,
        source: str | NodeHandle | dict[str, Any] | None = None,
        target: str | NodeHandle | dict[str, Any] | None = None,
        *,
        by: str,
        via: str | None = None,
        directed: bool = True,
        k: int = 1,
        weight: str | None = None,
        capacity_property: str | None = None,
        cost_property: str | None = None,
        heuristic: str | None = None,
        walk_length: int | None = None,
        seed: int | None = None,
        terminal_uuids: list[str] | None = None,
        prize_property: str | None = None,
    ) -> pyarrow.Table: ...
    def analyze(
        self,
        label: str | None = None,
        *,
        by: str,
        via: str | None = None,
        directed: bool = True,
        weight: str | None = None,
        partition_property: str | None = None,
        k: int | None = None,
    ) -> pyarrow.Table: ...
    def similar(
        self,
        label: str,
        *,
        by: str,
        k: int = 10,
        vector_property: str | None = None,
        via: str | None = None,
    ) -> pyarrow.Table: ...
    def configure_openrouter(
        self,
        credential: str,
        *,
        origin: str,
        model: str,
        revision: str = "unavailable",
        response_contract_version: str = "v1",
        capabilities: list[
            Literal["document_embeddings", "query_embeddings", "candidate_reranking"]
        ]
        | None = None,
        max_input_tokens: int = 1_000_000,
        transport_timeout_millis: int = 30_000,
        estimated_cost_microunits_per_token: int = 1,
    ) -> None: ...
    def inspect_provider_embedding_plan(
        self,
        name: str,
        label: str,
        properties: list[str],
        *,
        dimensions: int,
        normalization: Literal["none", "l2"] = "none",
        replace: bool = False,
    ) -> dict[str, Any]: ...
    def publish_provider_embeddings(
        self,
        name: str,
        label: str,
        properties: list[str],
        *,
        dimensions: int,
        normalization: Literal["none", "l2"] = "none",
        replace: bool = False,
    ) -> dict[str, Any]: ...
    def find(
        self,
        query: str | None = None,
        *,
        label: str | None = None,
        vector: list[float] | None = None,
        similar_to: str | NodeHandle | dict[str, Any] | None = None,
        semantic_query: str | None = None,
        limit: int = 10,
        space: str | None = None,
        force_stale: bool = False,
        rerank: dict[str, Any] | None = None,
        suppress_rerank_advisory: bool = False,
    ) -> pyarrow.Table: ...
    def publish_caller_embeddings(
        self,
        name: str,
        rows: list[dict[str, Any]],
        *,
        dimensions: int,
        source_projection: dict[str, str],
        contract_version: str = "graphforge_binding_caller_v1",
        normalization: Literal["none", "l2"] = "none",
        replace: bool = False,
    ) -> str: ...
    def publish_algorithm_embeddings(
        self,
        name: str,
        result: pyarrow.Table,
        *,
        algorithm: Literal["node2vec", "graphsage", "fast_random_projection", "hash_gnn"],
        algorithm_version: str,
        dimensions: int,
        input_recipe: dict[str, Any],
        source_projection: dict[str, Any],
        hyperparameters: dict[str, Any] | None = None,
        normalization: Literal["none", "l2"] = "none",
        replace: bool = False,
    ) -> str: ...
    def embedding_spaces(self) -> list[dict[str, Any]]: ...
    def embedding_space(self, name: str | None = None) -> dict[str, Any]: ...
    def bind_embedding_space_alias(
        self, name: str, compatibility_id: str, *, replace: bool = False
    ) -> dict[str, Any]: ...
    def remove_embedding_space_alias(self, name: str) -> bool: ...
    def delete_embedding_space(self, name: str | None = None) -> bool: ...
    def set_default_embedding_space(self, name: str | None = None) -> dict[str, Any] | None: ...
    def inspect_embedding_space_freshness(
        self, name: str | None = None, *, force_stale: bool = False
    ) -> dict[str, Any]: ...
    def embedding_refresh_project_policy(self) -> dict[str, Any]: ...
    def set_embedding_refresh_project_policy(
        self, *, proactive: bool, debounce_millis: int, max_concurrent_jobs: int
    ) -> dict[str, Any]: ...
    def set_embedding_refresh_space_policy(
        self,
        name: str | None = None,
        *,
        proactive: bool | None = None,
        debounce_millis: int | None = None,
        clear: bool = False,
    ) -> dict[str, Any]: ...
    def inspect_embedding_refresh(self, name: str | None = None) -> dict[str, Any]: ...
    @overload
    def index(
        self,
        label: str,
        *,
        properties: list[str] | None,
        rebuild: bool = False,
    ) -> dict[str, Any]: ...
    @overload
    def index(self, label: str, *, rebuild: bool) -> dict[str, Any]: ...
    @overload
    def index(
        self,
        label: str,
        *,
        node: str | NodeHandle | dict[str, Any],
        vector: list[float],
        space: str,
    ) -> None: ...
    @overload
    def index(self, label: Literal["adjacency"]) -> None: ...
    def inspect_text_index(
        self, label: str, *, properties: list[str] | None = None
    ) -> dict[str, Any]: ...
    def index_adjacency(self) -> dict[str, Any]: ...
    def inspect_adjacency(self) -> dict[str, Any]: ...
    def rebuild_adjacency(
        self, *, cancellation: CancellationToken | None = None
    ) -> dict[str, Any]: ...
    def add_node(self, label: str, **props: Any) -> NodeHandle: ...
    def add_edge(self, src: Any, rel_type: str, dst: Any, **props: Any) -> EdgeHandle: ...
    def begin_transaction(
        self, *, operation_uuid: str, actor_uuid: str | None = None
    ) -> GraphTransaction: ...
    def project_open_recovery(self) -> dict[str, Any]: ...
    def inspect_project_reachability(
        self,
        *,
        retained_ancestors: int | None = None,
        max_entries: int | None = None,
        max_bytes_scanned: int | None = None,
        max_work_units: int | None = None,
        cleanup_batch: int | None = None,
    ) -> dict[str, Any]: ...
    def preview_project_cleanup(
        self,
        *,
        retained_ancestors: int | None = None,
        max_entries: int | None = None,
        max_bytes_scanned: int | None = None,
        max_work_units: int | None = None,
        cleanup_batch: int | None = None,
    ) -> dict[str, Any]: ...
    def execute_project_cleanup(
        self,
        *,
        retained_ancestors: int | None = None,
        max_entries: int | None = None,
        max_bytes_scanned: int | None = None,
        max_work_units: int | None = None,
        cleanup_batch: int | None = None,
    ) -> dict[str, Any]: ...
    def graph_delta_compaction_status(
        self,
        *,
        compact_when_runs: int | None = None,
        compact_when_run_bytes: int | None = None,
        compact_when_replay_memory_bytes: int | None = None,
    ) -> dict[str, Any]: ...
    def preview_graph_delta_compaction(
        self,
        *,
        transaction_uuid: str,
        generation_uuid: str,
        through_run_sequence: int | None = None,
        cleanup_after_commit: bool = False,
        retained_ancestors: int | None = None,
        cancellation: CancellationToken | None = None,
    ) -> dict[str, Any]: ...
    def compact_graph_delta(
        self,
        *,
        transaction_uuid: str,
        generation_uuid: str,
        through_run_sequence: int | None = None,
        cleanup_after_commit: bool = False,
        retained_ancestors: int | None = None,
        cancellation: CancellationToken | None = None,
    ) -> dict[str, Any]: ...
    def publish_composite_transaction(
        self,
        *,
        operation_uuid: str,
        graph_mutations: list[dict[str, Any]],
        knowledge: dict[str, list[dict[str, Any]]] | None = None,
        actor_uuid: str | None = None,
        contract_version: int = 1,
    ) -> pyarrow.Table: ...
    def publish_bulk_nodes(self, operation_uuid: str, data: Any) -> pyarrow.Table: ...
    def publish_bulk_edges(self, operation_uuid: str, data: Any) -> pyarrow.Table: ...
    def add_nodes(self, label: str, data: Any, *, operation_uuid: str) -> pyarrow.Table: ...
    def add_edges(
        self,
        rel_type: str,
        data: Any,
        *,
        operation_uuid: str,
        src: str = "src_id",
        dst: str = "dst_id",
    ) -> pyarrow.Table: ...
    def clear(self) -> None: ...
    def schema(self) -> pyarrow.Table: ...
    def labels(self) -> list[str]: ...
    def relationship_types(self) -> list[str]: ...
    def node_count(self, label: str | None = None) -> int: ...
    def graph_directedness(self) -> str | None: ...
    def set_graph_directedness(
        self,
        directedness: str | None = None,
        *,
        operation_uuid: str,
        actor_uuid: str | None = None,
    ) -> None: ...
    def profile_gsi(self) -> GraphScaleIndexProfile: ...
    def close(self) -> None: ...
    @property
    def path(self) -> str | None: ...
    @property
    def ontology_mode(self) -> str: ...
    def __repr__(self) -> str: ...

class GraphScaleIndexProfile:
    @property
    def gsi(self) -> str: ...
    @property
    def directedness(self) -> str: ...
    @property
    def node_count(self) -> int: ...
    @property
    def edge_count(self) -> int: ...
    @property
    def density(self) -> float: ...
    @property
    def scale_code(self) -> str: ...
    @property
    def size_tag(self) -> str: ...
    @property
    def density_integer(self) -> int: ...
    def __repr__(self) -> str: ...

class GraphForgeError(Exception): ...

class ParseError(GraphForgeError):
    span: tuple[int, int]

class PlanError(GraphForgeError): ...
class ExecutionError(GraphForgeError): ...
class StorageError(GraphForgeError): ...
class LifecycleError(GraphForgeError): ...
class ValidationError(GraphForgeError): ...
class OntologyError(GraphForgeError): ...
