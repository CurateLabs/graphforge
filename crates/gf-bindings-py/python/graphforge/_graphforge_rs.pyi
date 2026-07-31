"""Type stub for the compiled GraphForge extension (`graphforge._graphforge_rs`).

Hand-written to match the PyO3 surface in `crates/gf-bindings-py/src/lib.rs`.
`execute_polars` is typed as `Any` so the stub doesn't force the optional
`polars` dependency onto consumers' type-checkers; the Arrow-returning methods
use `pyarrow` (a hard dependency).
"""

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
    def clear_ontology(
        self, *, operation_uuid: str, actor_uuid: str | None = None
    ) -> None: ...
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
    def publish_m18_embeddings(
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
    def begin(self) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def clear(self) -> None: ...
    def schema(self) -> pyarrow.Table: ...
    def labels(self) -> list[str]: ...
    def relationship_types(self) -> list[str]: ...
    def node_count(self, label: str | None = None) -> int: ...
    def close(self) -> None: ...
    @property
    def path(self) -> str | None: ...
    @property
    def ontology_mode(self) -> str: ...
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
