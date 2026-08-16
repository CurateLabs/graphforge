//! Uniform Rust-owned mutation transaction lifecycle (#754).
//!
//! Supported mutation families stage into one handle pinned to a base generation
//! and [`WriteContext`]. Commit publishes every staged participant atomically;
//! rollback, cancellation, and drop leave the prior generation unchanged.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::ThreadId;
use std::time::Instant;

use arrow::array::{Array, FixedSizeBinaryArray};
use arrow::record_batch::RecordBatch;
use graphforge_core::{GfError, ProjectErrorCode, PropValue};
use graphforge_ir::IrLiteral;
use uuid::Uuid;

use crate::bulk_construction::{BulkEdgeRow, BulkNodeRow};
use crate::composite_receipt::composite_receipt_schema;
use crate::composite_transaction::{
    COMPOSITE_TRANSACTION_CONTRACT_VERSION, CompositeGraphMutation, CompositeKnowledgeParticipants,
    CompositeTransactionRequest, MAX_COMPOSITE_TRANSACTION_ENTRIES,
};
use crate::write_modes::ProjectWriteMode;
use crate::{CancellationToken, GraphForge, WriteContext};

/// Closed classification of mutation families relative to an explicit transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MutationFamily {
    /// Write openCypher (`CREATE` / `MERGE` / `SET` / `REMOVE` / `DELETE`).
    WriteCypher,
    /// Scalar `add_node` / `add_edge` construction.
    ScalarConstruction,
    /// Arrow bulk node/edge construction.
    BulkConstruction,
    /// Composite graph mutations plus explicit knowledge/epistemic rows.
    CompositeDomainRows,
    /// Opt-in algorithm property write-back.
    AlgorithmWriteback,
    /// Index rebuild / search-admin mutations.
    IndexAdministration,
    /// Capability enablement.
    CapabilityAdministration,
    /// Checkpoint create / delete / revert.
    CheckpointAdministration,
    /// Ontology adopt / clear.
    OntologyAdministration,
    /// Embedding space publish / delete / alias binding.
    EmbeddingAdministration,
    /// Portable import / export publication.
    PortablePublication,
}

/// Whether a mutation family may join an explicit multi-mutation transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TransactionSupport {
    /// May stage into [`GraphTransaction`] and publish with other supported ops.
    Supported,
    /// Must run as its own one-shot generation; rejected inside an open transaction.
    SoloOnly,
    /// Rejected before mutation inside a transaction (and not offered as stage API).
    Rejected,
}

/// Return the frozen transaction-support classification for one mutation family.
#[must_use]
pub const fn transaction_support(family: MutationFamily) -> TransactionSupport {
    match family {
        MutationFamily::WriteCypher
        | MutationFamily::ScalarConstruction
        | MutationFamily::BulkConstruction
        | MutationFamily::CompositeDomainRows
        | MutationFamily::AlgorithmWriteback => TransactionSupport::Supported,
        MutationFamily::IndexAdministration
        | MutationFamily::CapabilityAdministration
        | MutationFamily::CheckpointAdministration
        | MutationFamily::OntologyAdministration
        | MutationFamily::EmbeddingAdministration
        | MutationFamily::PortablePublication => TransactionSupport::Rejected,
    }
}

/// Lifecycle phase visible through [`GraphTransaction::status`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TransactionPhase {
    /// Handle is open and may accept staged mutations.
    Open,
    /// Staged content passed validate without publishing.
    Validated,
    /// Commit published one generation (or exact idempotent replay).
    Committed,
    /// Explicit rollback, cancel, or drop abandoned staged work.
    RolledBack,
}

/// Safe, content-free transaction status snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionStatus {
    /// Caller operation identity.
    pub operation_uuid: Uuid,
    /// Generation pinned at begin.
    pub base_generation_uuid: Uuid,
    /// Current lifecycle phase.
    pub phase: TransactionPhase,
    /// Selected embedded write mode.
    pub write_mode: ProjectWriteMode,
    /// Number of staged mutation entries (graph + knowledge rows + cypher ops).
    pub staged_entry_count: usize,
    /// Whether commit acknowledged a durable generation.
    pub committed: bool,
}

/// Receipt returned by [`GraphTransaction::commit`].
#[derive(Clone, Debug)]
pub struct TransactionCommitReceipt {
    /// Published (or exactly replayed) generation UUID.
    pub generation_uuid: Uuid,
    /// Canonical composite receipt when the commit used the composite vocabulary.
    pub composite_receipt: Option<RecordBatch>,
}

struct TransactionState {
    phase: TransactionPhase,
    graph_mutations: Vec<CompositeGraphMutation>,
    knowledge: CompositeKnowledgeParticipants,
    cypher: Vec<(String, HashMap<String, IrLiteral>)>,
    entry_count: usize,
}

impl TransactionState {
    fn staged_entry_count(&self) -> usize {
        self.entry_count
    }

    fn is_empty(&self) -> bool {
        self.graph_mutations.is_empty()
            && self.cypher.is_empty()
            && knowledge_row_count(&self.knowledge) == 0
    }

    fn into_composite_request(self, context: WriteContext) -> CompositeTransactionRequest {
        CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context,
            graph_mutations: self.graph_mutations,
            knowledge: self.knowledge,
        }
    }
}

fn knowledge_row_count(knowledge: &CompositeKnowledgeParticipants) -> usize {
    knowledge.counts().iter().sum()
}

/// Explicit multi-mutation transaction handle.
///
/// Pinned to one base generation and one [`WriteContext`]. Not `Send`: the
/// owning thread must drive stage/validate/commit/rollback. Drop rolls back
/// any still-open staged work without holding project writer locks across the
/// handle lifetime (locks are acquired only around commit admission).
///
/// The handle does not borrow [`GraphForge`]: callers pass `&GraphForge` into
/// [`Self::validate`] / [`Self::commit`] so language bindings can keep the
/// facade and transaction as separately owned wrappers.
pub struct GraphTransaction {
    context: WriteContext,
    base_generation_uuid: Uuid,
    write_mode: ProjectWriteMode,
    owner: ThreadId,
    state: Mutex<TransactionState>,
    finished: AtomicBool,
    started_at: Instant,
}

impl GraphTransaction {
    fn ensure_owner(&self) -> Result<(), GfError> {
        if std::thread::current().id() != self.owner {
            return Err(validation(
                "transaction handle used from a non-owning thread",
            ));
        }
        Ok(())
    }

    fn ensure_open(&self) -> Result<(), GfError> {
        self.ensure_owner()?;
        if self.finished.load(Ordering::Acquire) {
            return Err(transaction_failed(
                "transaction handle already committed or rolled back",
            ));
        }
        Ok(())
    }

    /// Inspect lifecycle status without mutating staged work.
    pub fn status(&self) -> Result<TransactionStatus, GfError> {
        self.ensure_owner()?;
        let state = self
            .state
            .lock()
            .map_err(|_| validation("transaction state lock poisoned"))?;
        Ok(TransactionStatus {
            operation_uuid: self.context.operation_uuid.0,
            base_generation_uuid: self.base_generation_uuid,
            phase: state.phase,
            write_mode: self.write_mode,
            staged_entry_count: state.staged_entry_count(),
            committed: state.phase == TransactionPhase::Committed,
        })
    }

    /// Stage explicit composite graph mutations and knowledge participants.
    pub fn stage_composite(
        &self,
        graph_mutations: Vec<CompositeGraphMutation>,
        knowledge: CompositeKnowledgeParticipants,
    ) -> Result<(), GfError> {
        self.ensure_open()?;
        reject_if_unsupported(MutationFamily::CompositeDomainRows)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| validation("transaction state lock poisoned"))?;
        Self::reserve_entries(
            &mut state,
            graph_mutations.len() + knowledge_row_count(&knowledge),
        )?;
        state.graph_mutations.extend(graph_mutations);
        merge_knowledge(&mut state.knowledge, knowledge);
        state.phase = TransactionPhase::Open;
        Ok(())
    }

    /// Stage one write Cypher statement for deferred execution at commit.
    pub fn stage_cypher(
        &self,
        query: impl Into<String>,
        params: HashMap<String, IrLiteral>,
    ) -> Result<(), GfError> {
        self.ensure_open()?;
        reject_if_unsupported(MutationFamily::WriteCypher)?;
        let query = query.into();
        if query.trim().is_empty() {
            return Err(validation("empty query"));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| validation("transaction state lock poisoned"))?;
        Self::reserve_entries(&mut state, 1)?;
        state.cypher.push((query, params));
        state.phase = TransactionPhase::Open;
        Ok(())
    }

    /// Stage scalar node construction as a composite create.
    pub fn stage_add_node(
        &self,
        node_uuid: Uuid,
        label: impl Into<String>,
        properties: HashMap<String, PropValue>,
    ) -> Result<(), GfError> {
        self.ensure_open()?;
        reject_if_unsupported(MutationFamily::ScalarConstruction)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| validation("transaction state lock poisoned"))?;
        Self::reserve_entries(&mut state, 1)?;
        state
            .graph_mutations
            .push(CompositeGraphMutation::CreateNode {
                node_uuid,
                label: label.into(),
                properties,
            });
        state.phase = TransactionPhase::Open;
        Ok(())
    }

    /// Stage scalar edge construction as a composite create.
    pub fn stage_add_edge(
        &self,
        edge_uuid: Uuid,
        rel_type: impl Into<String>,
        source_uuid: Uuid,
        target_uuid: Uuid,
        properties: HashMap<String, PropValue>,
    ) -> Result<(), GfError> {
        self.ensure_open()?;
        reject_if_unsupported(MutationFamily::ScalarConstruction)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| validation("transaction state lock poisoned"))?;
        Self::reserve_entries(&mut state, 1)?;
        state
            .graph_mutations
            .push(CompositeGraphMutation::CreateEdge {
                edge_uuid,
                rel_type: rel_type.into(),
                source_uuid,
                target_uuid,
                properties,
            });
        state.phase = TransactionPhase::Open;
        Ok(())
    }

    /// Stage validated bulk node rows as composite creates.
    pub fn stage_bulk_nodes(&self, rows: &[BulkNodeRow]) -> Result<(), GfError> {
        self.ensure_open()?;
        reject_if_unsupported(MutationFamily::BulkConstruction)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| validation("transaction state lock poisoned"))?;
        Self::reserve_entries(&mut state, rows.len())?;
        for row in rows {
            state
                .graph_mutations
                .push(CompositeGraphMutation::CreateNode {
                    node_uuid: row.node_uuid,
                    label: row.label.clone(),
                    properties: btree_to_hash(&row.properties),
                });
        }
        state.phase = TransactionPhase::Open;
        Ok(())
    }

    /// Stage validated bulk edge rows as composite creates.
    pub fn stage_bulk_edges(&self, rows: &[BulkEdgeRow]) -> Result<(), GfError> {
        self.ensure_open()?;
        reject_if_unsupported(MutationFamily::BulkConstruction)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| validation("transaction state lock poisoned"))?;
        Self::reserve_entries(&mut state, rows.len())?;
        for row in rows {
            state
                .graph_mutations
                .push(CompositeGraphMutation::CreateEdge {
                    edge_uuid: row.edge_uuid,
                    rel_type: row.rel_type.clone(),
                    source_uuid: row.source_uuid,
                    target_uuid: row.target_uuid,
                    properties: btree_to_hash(&row.properties),
                });
        }
        state.phase = TransactionPhase::Open;
        Ok(())
    }

    /// Stage algorithm write-back property updates as composite sets.
    pub fn stage_algorithm_writeback(
        &self,
        updates: impl IntoIterator<Item = (Uuid, String, PropValue)>,
    ) -> Result<(), GfError> {
        self.ensure_open()?;
        reject_if_unsupported(MutationFamily::AlgorithmWriteback)?;
        let updates = updates.into_iter().collect::<Vec<_>>();
        let mut state = self
            .state
            .lock()
            .map_err(|_| validation("transaction state lock poisoned"))?;
        Self::reserve_entries(&mut state, updates.len())?;
        for (node_uuid, property, value) in updates {
            state
                .graph_mutations
                .push(CompositeGraphMutation::SetNodeProperty {
                    node_uuid,
                    property,
                    value,
                });
        }
        state.phase = TransactionPhase::Open;
        Ok(())
    }

    /// Reject an administrative family before any mutation.
    pub fn reject_admin(&self, family: MutationFamily) -> Result<(), GfError> {
        self.ensure_open()?;
        match transaction_support(family) {
            TransactionSupport::Rejected | TransactionSupport::SoloOnly => Err(transaction_failed(
                format!("mutation family {family:?} cannot join an explicit transaction"),
            )),
            TransactionSupport::Supported => Err(validation(
                "supported mutation families must use the matching stage_* API",
            )),
        }
    }

    /// Validate staged content against the pinned base generation without publishing.
    pub fn validate(&self, graph: &GraphForge) -> Result<(), GfError> {
        self.ensure_open()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| validation("transaction state lock poisoned"))?;
        if state.is_empty() {
            return Err(validation("transaction has no staged mutations"));
        }
        let request = CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: self.context.clone(),
            graph_mutations: state.graph_mutations.clone(),
            knowledge: state.knowledge.clone(),
        };
        if state.cypher.is_empty() {
            let _ = graph.authorize_composite_against_current(&request)?;
        } else if !request.graph_mutations.is_empty() || knowledge_row_count(&request.knowledge) > 0
        {
            request.validate_request_shape()?;
        }
        state.phase = TransactionPhase::Validated;
        let _elapsed = self.started_at.elapsed();
        Ok(())
    }

    /// Publish every staged supported participant as one generation.
    pub fn commit(&self, graph: &GraphForge) -> Result<TransactionCommitReceipt, GfError> {
        self.commit_with_cancellation(graph, None)
    }

    /// Commit with cooperative queued-write cancellation observed only before admission.
    pub fn commit_with_cancellation(
        &self,
        graph: &GraphForge,
        cancellation: Option<CancellationToken>,
    ) -> Result<TransactionCommitReceipt, GfError> {
        self.ensure_open()?;
        let state = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| validation("transaction state lock poisoned"))?;
            if state.is_empty() {
                return Err(validation("transaction has no staged mutations"));
            }
            std::mem::replace(
                &mut *state,
                TransactionState {
                    phase: TransactionPhase::Open,
                    graph_mutations: Vec::new(),
                    knowledge: CompositeKnowledgeParticipants::default(),
                    cypher: Vec::new(),
                    entry_count: 0,
                },
            )
        };

        let result = self.commit_state(graph, state, cancellation);
        if result.is_ok() {
            if let Ok(mut state) = self.state.lock() {
                state.phase = TransactionPhase::Committed;
            }
        } else if let Ok(mut state) = self.state.lock() {
            state.phase = TransactionPhase::RolledBack;
        }
        self.finished.store(true, Ordering::Release);
        result
    }

    fn commit_state(
        &self,
        graph: &GraphForge,
        state: TransactionState,
        cancellation: Option<CancellationToken>,
    ) -> Result<TransactionCommitReceipt, GfError> {
        let has_cypher = !state.cypher.is_empty();
        let cypher = state.cypher.clone();
        let request = state.into_composite_request(self.context.clone());

        if !has_cypher {
            let receipt =
                graph.publish_composite_transaction_with_cancellation(request, cancellation)?;
            let generation_uuid = generation_from_receipt(&receipt)?;
            return Ok(TransactionCommitReceipt {
                generation_uuid,
                composite_receipt: Some(receipt),
            });
        }

        let _visibility = graph.graph_visibility.acquire(cancellation.as_ref())?;
        let prior = graphforge_storage::resolve_project_generation(
            graph.resolved_generation.container_root(),
        )?;
        if prior.generation_uuid() != self.base_generation_uuid
            && self.write_mode != ProjectWriteMode::OptimisticMultiWriter
        {
            return Err(GfError::Project {
                code: ProjectErrorCode::TransactionConflict,
                message: "project generation changed before transaction commit".into(),
            });
        }

        for (query, params) in &cypher {
            graph
                .execute_write_without_publish(query, params)
                .inspect_err(|_| {
                    let _ = crate::rematerialize_graph_workspace(&prior, &graph.dir);
                })?;
        }

        if request.graph_mutations.is_empty() && knowledge_row_count(&request.knowledge) == 0 {
            let recorded_at = (graph.clock.lock().expect("clock lock poisoned"))()?;
            let receipt = graphforge_exec::MutationReceipt::default();
            graph.publish_graph_mutation_with_context(
                &receipt,
                self.context.operation_uuid.0,
                self.context.actor_uuid,
                recorded_at,
            )?;
            let generation = *graph
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned");
            return Ok(TransactionCommitReceipt {
                generation_uuid: generation,
                composite_receipt: None,
            });
        }

        let receipt = graph
            .publish_composite_transaction_admitted(request)
            .inspect_err(|_| {
                let _ = crate::rematerialize_graph_workspace(&prior, &graph.dir);
            })?;
        let generation_uuid = generation_from_receipt(&receipt)?;
        Ok(TransactionCommitReceipt {
            generation_uuid,
            composite_receipt: Some(receipt),
        })
    }

    /// Abandon staged work without publishing.
    pub fn rollback(&self) -> Result<(), GfError> {
        self.ensure_owner()?;
        if self.finished.swap(true, Ordering::AcqRel) {
            return Err(transaction_failed(
                "transaction handle already committed or rolled back",
            ));
        }
        if let Ok(mut state) = self.state.lock() {
            state.graph_mutations.clear();
            state.knowledge = CompositeKnowledgeParticipants::default();
            state.cypher.clear();
            state.entry_count = 0;
            state.phase = TransactionPhase::RolledBack;
        }
        Ok(())
    }

    fn reserve_entries(state: &mut TransactionState, additional: usize) -> Result<(), GfError> {
        let next = state
            .entry_count
            .checked_add(additional)
            .ok_or_else(|| validation("transaction entry count overflow"))?;
        if next > MAX_COMPOSITE_TRANSACTION_ENTRIES {
            return Err(validation(format!(
                "transaction exceeds {MAX_COMPOSITE_TRANSACTION_ENTRIES} staged entries"
            )));
        }
        state.entry_count = next;
        Ok(())
    }
}

impl Drop for GraphTransaction {
    fn drop(&mut self) {
        if !self.finished.swap(true, Ordering::AcqRel)
            && let Ok(mut state) = self.state.lock()
        {
            state.graph_mutations.clear();
            state.knowledge = CompositeKnowledgeParticipants::default();
            state.cypher.clear();
            state.entry_count = 0;
            state.phase = TransactionPhase::RolledBack;
        }
    }
}

impl GraphForge {
    /// Begin an explicit transaction pinned to the current generation and write context.
    ///
    /// The handle stages supported mutations in memory and publishes them
    /// atomically on commit. Administrative families classified as rejected are
    /// refused by [`GraphTransaction::reject_admin`] before mutation.
    pub fn begin_transaction(&self, context: WriteContext) -> Result<GraphTransaction, GfError> {
        if self.read_only {
            return Err(GfError::Project {
                code: ProjectErrorCode::ReadOnlyView,
                message: "checkpoint views cannot begin transactions".into(),
            });
        }
        if context.operation_uuid.0.get_version() != Some(uuid::Version::SortRand) {
            return Err(validation("transaction operation_uuid must be a UUIDv7"));
        }
        let base_generation_uuid = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        Ok(GraphTransaction {
            context,
            base_generation_uuid,
            write_mode: self.write_options.write_mode,
            owner: std::thread::current().id(),
            state: Mutex::new(TransactionState {
                phase: TransactionPhase::Open,
                graph_mutations: Vec::new(),
                knowledge: CompositeKnowledgeParticipants::default(),
                cypher: Vec::new(),
                entry_count: 0,
            }),
            finished: AtomicBool::new(false),
            started_at: Instant::now(),
        })
    }
}

fn reject_if_unsupported(family: MutationFamily) -> Result<(), GfError> {
    match transaction_support(family) {
        TransactionSupport::Supported => Ok(()),
        TransactionSupport::SoloOnly | TransactionSupport::Rejected => Err(transaction_failed(
            format!("mutation family {family:?} cannot join an explicit transaction"),
        )),
    }
}

fn merge_knowledge(
    target: &mut CompositeKnowledgeParticipants,
    incoming: CompositeKnowledgeParticipants,
) {
    target.provenance_events.extend(incoming.provenance_events);
    target.lineage.extend(incoming.lineage);
    target.assertions.extend(incoming.assertions);
    target
        .assertion_graph_refs
        .extend(incoming.assertion_graph_refs);
    target
        .confidence_assessments
        .extend(incoming.confidence_assessments);
    target.confidence_inputs.extend(incoming.confidence_inputs);
    target.evidence.extend(incoming.evidence);
    target.reasoning.extend(incoming.reasoning);
    target.assertion_status.extend(incoming.assertion_status);
    target
        .assertion_supersessions
        .extend(incoming.assertion_supersessions);
    target.hypothesis_groups.extend(incoming.hypothesis_groups);
    target
        .hypothesis_membership
        .extend(incoming.hypothesis_membership);
    target
        .hypothesis_selection
        .extend(incoming.hypothesis_selection);
    target
        .assertion_validity
        .extend(incoming.assertion_validity);
}

fn btree_to_hash(properties: &BTreeMap<String, PropValue>) -> HashMap<String, PropValue> {
    properties
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn generation_from_receipt(receipt: &RecordBatch) -> Result<Uuid, GfError> {
    debug_assert_eq!(
        receipt.schema().as_ref(),
        composite_receipt_schema().as_ref()
    );
    let column = receipt
        .column_by_name("generation_uuid")
        .ok_or_else(|| validation("composite receipt missing generation_uuid"))?;
    let values = column
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| validation("composite receipt generation_uuid has wrong type"))?;
    if values.len() != 1 {
        return Err(validation("composite receipt must be a singleton"));
    }
    Uuid::from_slice(values.value(0)).map_err(|error| {
        GfError::Storage(format!(
            "composite receipt generation_uuid is invalid: {error}"
        ))
    })
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

fn transaction_failed(message: impl Into<String>) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::TransactionFailed,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composite_transaction::COMPOSITE_TRANSACTION_CONTRACT_VERSION;
    use crate::{CancellationToken, OperationId};
    use tempfile::TempDir;

    fn uuid7(seed: u8) -> Uuid {
        let mut bytes = [0_u8; 16];
        bytes[0] = seed;
        bytes[6] = 0x70;
        bytes[8] = 0x80;
        Uuid::from_bytes(bytes)
    }

    fn context(seed: u8) -> WriteContext {
        WriteContext {
            operation_uuid: OperationId(uuid7(seed)),
            actor_uuid: None,
        }
    }

    #[test]
    fn every_mutation_family_has_an_explicit_classification() {
        let families = [
            MutationFamily::WriteCypher,
            MutationFamily::ScalarConstruction,
            MutationFamily::BulkConstruction,
            MutationFamily::CompositeDomainRows,
            MutationFamily::AlgorithmWriteback,
            MutationFamily::IndexAdministration,
            MutationFamily::CapabilityAdministration,
            MutationFamily::CheckpointAdministration,
            MutationFamily::OntologyAdministration,
            MutationFamily::EmbeddingAdministration,
            MutationFamily::PortablePublication,
        ];
        for family in families {
            let _ = transaction_support(family);
        }
        assert_eq!(
            transaction_support(MutationFamily::WriteCypher),
            TransactionSupport::Supported
        );
        assert_eq!(
            transaction_support(MutationFamily::CheckpointAdministration),
            TransactionSupport::Rejected
        );
    }

    #[test]
    fn mixed_cypher_and_bulk_commit_as_one_generation() {
        let directory = TempDir::new().unwrap();
        let graph = GraphForge::new(directory.path().to_str()).unwrap();
        let before = *graph
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        let bulk_node = uuid7(21);
        let tx = graph.begin_transaction(context(10)).unwrap();
        tx.stage_cypher("CREATE (:Person {name: 'Cypher'})", HashMap::new())
            .unwrap();
        tx.stage_bulk_nodes(&[BulkNodeRow {
            node_uuid: bulk_node,
            label: "Person".into(),
            properties: BTreeMap::from([("name".into(), PropValue::Str("Bulk".into()))]),
            row_ordinal: 0,
        }])
        .unwrap();
        let receipt = tx.commit(&graph).unwrap();
        assert_ne!(receipt.generation_uuid, before);
        assert_eq!(
            *graph
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned"),
            receipt.generation_uuid
        );
        let rows = graph
            .execute("MATCH (n:Person) RETURN n.name AS name ORDER BY name")
            .unwrap();
        assert_eq!(
            rows.batches
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            2
        );
    }

    #[test]
    fn rollback_leaves_prior_generation_unchanged_across_reopen() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().to_str().unwrap().to_owned();
        let graph = GraphForge::new(Some(path.as_str())).unwrap();
        let before = *graph
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        let tx = graph.begin_transaction(context(11)).unwrap();
        tx.stage_add_node(
            uuid7(22),
            "Person",
            HashMap::from([("name".into(), PropValue::Str("Ghost".into()))]),
        )
        .unwrap();
        tx.validate(&graph).unwrap();
        tx.rollback().unwrap();
        assert_eq!(
            *graph
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned"),
            before
        );
        drop(tx);
        drop(graph);
        let reopened = GraphForge::new(Some(path.as_str())).unwrap();
        assert_eq!(
            *reopened
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned"),
            before
        );
        let rows = reopened.execute("MATCH (n:Person) RETURN n").unwrap();
        assert_eq!(
            rows.batches
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            0
        );
    }

    #[test]
    fn drop_rolls_back_without_publication() {
        let directory = TempDir::new().unwrap();
        let graph = GraphForge::new(directory.path().to_str()).unwrap();
        let before = *graph
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        {
            let tx = graph.begin_transaction(context(12)).unwrap();
            tx.stage_add_node(uuid7(23), "Person", HashMap::new())
                .unwrap();
        }
        assert_eq!(
            *graph
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned"),
            before
        );
    }

    #[test]
    fn exact_commit_retry_is_idempotent_and_changed_content_conflicts() {
        let directory = TempDir::new().unwrap();
        let graph = GraphForge::new(directory.path().to_str()).unwrap();
        let node = uuid7(24);
        let ctx = context(13);
        let first = graph.begin_transaction(ctx.clone()).unwrap();
        first
            .stage_add_node(
                node,
                "Person",
                HashMap::from([("name".into(), PropValue::Str("Ada".into()))]),
            )
            .unwrap();
        let first_receipt = first.commit(&graph).unwrap();

        let retry = graph.begin_transaction(ctx.clone()).unwrap();
        retry
            .stage_add_node(
                node,
                "Person",
                HashMap::from([("name".into(), PropValue::Str("Ada".into()))]),
            )
            .unwrap();
        let retry_receipt = retry.commit(&graph).unwrap();
        assert_eq!(first_receipt.generation_uuid, retry_receipt.generation_uuid);

        let conflict = graph.begin_transaction(ctx).unwrap();
        conflict
            .stage_add_node(
                node,
                "Person",
                HashMap::from([("name".into(), PropValue::Str("Other".into()))]),
            )
            .unwrap();
        let error = conflict.commit(&graph).unwrap_err();
        assert_eq!(error.code(), "GF_IDEMPOTENCY_CONFLICT");
    }

    #[test]
    fn rejected_admin_family_fails_before_mutation() {
        let graph = GraphForge::new(None).unwrap();
        let before = *graph
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        let tx = graph.begin_transaction(context(14)).unwrap();
        let error = tx
            .reject_admin(MutationFamily::CheckpointAdministration)
            .unwrap_err();
        assert_eq!(error.code(), "GF_TRANSACTION_FAILED");
        assert_eq!(
            *graph
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned"),
            before
        );
    }

    #[test]
    fn use_after_commit_is_rejected() {
        let graph = GraphForge::new(None).unwrap();
        let tx = graph.begin_transaction(context(15)).unwrap();
        tx.stage_add_node(uuid7(25), "Person", HashMap::new())
            .unwrap();
        tx.commit(&graph).unwrap();
        let error = tx
            .stage_add_node(uuid7(26), "Person", HashMap::new())
            .unwrap_err();
        assert_eq!(error.code(), "GF_TRANSACTION_FAILED");
    }

    #[test]
    fn optimistic_write_skew_witness_matches_isolation_table() {
        let directory = TempDir::new().unwrap();
        let options = crate::GraphForgeOptions {
            write_mode: ProjectWriteMode::OptimisticMultiWriter,
            max_rebase_attempts: 3,
            ..crate::GraphForgeOptions::default()
        };
        let graph =
            GraphForge::new_with_options(directory.path().to_str(), options.clone()).unwrap();
        let account = uuid7(30);
        graph
            .publish_composite_transaction(CompositeTransactionRequest {
                contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
                context: context(30),
                graph_mutations: vec![CompositeGraphMutation::CreateNode {
                    node_uuid: account,
                    label: "Account".into(),
                    properties: HashMap::from([
                        ("credit".into(), PropValue::Int(0)),
                        ("debit".into(), PropValue::Int(0)),
                    ]),
                }],
                knowledge: CompositeKnowledgeParticipants::default(),
            })
            .unwrap();

        let left = graph.begin_transaction(context(31)).unwrap();
        left.stage_composite(
            vec![CompositeGraphMutation::SetNodeProperty {
                node_uuid: account,
                property: "credit".into(),
                value: PropValue::Int(1),
            }],
            CompositeKnowledgeParticipants::default(),
        )
        .unwrap();
        let right_graph = GraphForge::new_with_options(directory.path().to_str(), options).unwrap();
        let right = right_graph.begin_transaction(context(32)).unwrap();
        right
            .stage_composite(
                vec![CompositeGraphMutation::SetNodeProperty {
                    node_uuid: account,
                    property: "debit".into(),
                    value: PropValue::Int(1),
                }],
                CompositeKnowledgeParticipants::default(),
            )
            .unwrap();
        left.commit(&graph).unwrap();
        right.commit(&right_graph).unwrap();

        let reopened = GraphForge::new(directory.path().to_str()).unwrap();
        let rows = reopened
            .execute("MATCH (a:Account) RETURN a.credit AS credit, a.debit AS debit")
            .unwrap();
        assert_eq!(
            rows.batches
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            1
        );
    }

    #[test]
    fn transaction_lifecycle_surface_is_complete() {
        let directory = TempDir::new().unwrap();
        let graph = GraphForge::new(directory.path().to_str()).unwrap();
        let source = uuid7(40);
        let target = uuid7(41);
        let edge = uuid7(42);
        let seeded = graph.begin_transaction(context(40)).unwrap();
        seeded
            .stage_add_node(source, "Person", HashMap::new())
            .unwrap();
        seeded
            .stage_add_node(target, "Person", HashMap::new())
            .unwrap();
        seeded.commit(&graph).unwrap();

        let tx = graph.begin_transaction(context(41)).unwrap();
        let status = tx.status().unwrap();
        assert_eq!(status.phase, TransactionPhase::Open);
        assert!(!status.committed);
        tx.stage_add_edge(edge, "KNOWS", source, target, HashMap::new())
            .unwrap();
        tx.stage_algorithm_writeback([(source, "score".into(), PropValue::Float(1.0))])
            .unwrap();
        tx.stage_bulk_edges(&[BulkEdgeRow {
            row_ordinal: 0,
            edge_uuid: uuid7(43),
            rel_type: "KNOWS".into(),
            source_uuid: source,
            target_uuid: target,
            properties: BTreeMap::new(),
        }])
        .unwrap();
        let cancelled = CancellationToken::new();
        let receipt = tx
            .commit_with_cancellation(&graph, Some(cancelled))
            .unwrap();
        assert_ne!(receipt.generation_uuid, Uuid::nil());
    }
}
