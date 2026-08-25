//! Atomic publication of a composite graph + knowledge/epistemic transaction.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;

use arrow::array::{Array, FixedSizeBinaryArray};
use arrow::record_batch::RecordBatch;
use graphforge_core::{GfError, OntologyMode, ProjectErrorCode};
use graphforge_ir::{IrLiteral, RuntimeCatalog};
use graphforge_knowledge::{
    AssertionLedger, AssertionStatusLedger, AssertionSupersessionLedger, AssertionValidityLedger,
    ConfidenceLedger, EvidenceLedger, HypothesisLedger, ReasoningLedger,
};
use graphforge_provenance::ProvenanceLedger;
use graphforge_storage::{
    ProjectCapability, ProjectGenerationRequest, ProjectParticipant, ProjectStageOutcome,
    ResolvedProjectGeneration, RewriteBatch,
};
use uuid::Uuid;

use crate::GraphForge;
use crate::composite_receipt::{
    authorize_composite_transaction, composite_generation_uuid, composite_receipt_schema,
};
use crate::composite_transaction::{CompositeGraphMutation, CompositeTransactionRequest};
use crate::composite_validation::{CompositeOntologySnapshot, CompositeValidationSnapshot};
use crate::construction::prop_literal;

fn eligible_delta_operations(
    request: &CompositeTransactionRequest,
) -> Result<Option<Vec<graphforge_storage::GraphDeltaOp>>, GfError> {
    if request.graph_mutations.is_empty() {
        return Ok(None);
    }
    let mut operations = Vec::with_capacity(request.graph_mutations.len());
    for (index, mutation) in request.graph_mutations.iter().enumerate() {
        let (kind, payload) = match mutation {
            CompositeGraphMutation::SetNodeProperty {
                node_uuid,
                property,
                value,
            } => (
                graphforge_storage::GraphDeltaOpKind::SetNodeProperty,
                graphforge_storage::GraphDeltaPayload::SetNodeProperty {
                    node_uuid: node_uuid.hyphenated().to_string(),
                    property_stem: "_untyped".into(),
                    key: property.clone(),
                    value: graphforge_storage::encode_graph_delta_value(&prop_literal(value)?)?,
                },
            ),
            CompositeGraphMutation::RemoveNodeProperty {
                node_uuid,
                property,
            } => (
                graphforge_storage::GraphDeltaOpKind::RemoveNodeProperty,
                graphforge_storage::GraphDeltaPayload::RemoveNodeProperty {
                    node_uuid: node_uuid.hyphenated().to_string(),
                    property_stem: "_untyped".into(),
                    key: property.clone(),
                },
            ),
            CompositeGraphMutation::SetEdgeProperty {
                edge_uuid,
                property,
                value,
            } => (
                graphforge_storage::GraphDeltaOpKind::SetEdgeProperty,
                graphforge_storage::GraphDeltaPayload::SetEdgeProperty {
                    edge_uuid: edge_uuid.hyphenated().to_string(),
                    property_stem: "_untyped".into(),
                    key: property.clone(),
                    value: graphforge_storage::encode_graph_delta_value(&prop_literal(value)?)?,
                },
            ),
            CompositeGraphMutation::RemoveEdgeProperty {
                edge_uuid,
                property,
            } => (
                graphforge_storage::GraphDeltaOpKind::RemoveEdgeProperty,
                graphforge_storage::GraphDeltaPayload::RemoveEdgeProperty {
                    edge_uuid: edge_uuid.hyphenated().to_string(),
                    property_stem: "_untyped".into(),
                    key: property.clone(),
                },
            ),
            _ => return Ok(None),
        };
        let operation_uuid = Uuid::new_v5(
            &request.context.operation_uuid.0,
            format!("graphforge-composite-delta-operation/1/{index}").as_bytes(),
        );
        operations.push(graphforge_storage::GraphDeltaOp {
            operation_uuid,
            kind,
            payload,
        });
    }
    Ok(Some(operations))
}

impl GraphForge {
    /// Validate, stage, and publish one composite graph + knowledge generation.
    ///
    /// Returns the frozen singleton composite Arrow receipt. Exact retry with the
    /// same request identity returns the identical receipt without restaging.
    /// Conflicting reuse returns `GF_IDEMPOTENCY_CONFLICT` with zero mutation.
    ///
    /// # Errors
    /// Returns the earliest stable validation, ontology, identity, not-found,
    /// idempotency, or publication error. Failures before CURRENT leave the
    /// previous generation authoritative and restore the private workspace.
    #[allow(clippy::too_many_lines, clippy::needless_pass_by_value)]
    pub fn publish_composite_transaction(
        &self,
        request: CompositeTransactionRequest,
    ) -> Result<RecordBatch, GfError> {
        self.publish_composite_transaction_with_cancellation(request, None)
    }

    /// Publish a composite transaction with cooperative queued-write cancellation.
    ///
    /// Cancellation is observed only before this operation starts mutating its
    /// private workspace. Once admitted, the operation runs to a deterministic
    /// publication or rollback boundary.
    ///
    /// # Errors
    /// Returns `GF_CANCELLED` only while queued, plus the errors documented by
    /// [`Self::publish_composite_transaction`].
    #[allow(clippy::too_many_lines, clippy::needless_pass_by_value)]
    pub fn publish_composite_transaction_with_cancellation(
        &self,
        request: CompositeTransactionRequest,
        cancellation: Option<crate::CancellationToken>,
    ) -> Result<RecordBatch, GfError> {
        let _visibility = self.graph_visibility.acquire(cancellation.as_ref())?;
        self.publish_composite_transaction_admitted(request)
    }

    /// Publish a composite transaction after write admission has already been granted.
    ///
    /// Used by the uniform transaction lifecycle so deferred Cypher and composite
    /// participants share one visibility permit without nested lock acquisition.
    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn publish_composite_transaction_admitted(
        &self,
        request: CompositeTransactionRequest,
    ) -> Result<RecordBatch, GfError> {
        let root = self.resolved_generation.container_root();
        let content_fingerprint = request.canonical_fingerprint()?;
        let generation_uuid =
            composite_generation_uuid(request.context.operation_uuid.0, content_fingerprint);
        let transaction_uuid = request.context.operation_uuid.0;

        if let Some(published) =
            graphforge_storage::published_project_transaction(root, transaction_uuid)?
        {
            if published.generation_uuid != generation_uuid {
                return Err(GfError::Project {
                    code: ProjectErrorCode::TransactionConflict,
                    message: "composite request identity reused with different canonical content"
                        .into(),
                });
            }
            // Exact published retry must not re-validate identities against CURRENT;
            // those identities are now occupied by this same committed generation.
            return crate::composite_receipt::build_composite_receipt(&request);
        }

        let optimistic =
            self.write_options.write_mode == crate::ProjectWriteMode::OptimisticMultiWriter;
        let mut rebases = 0_u32;
        let mut baseline = None;
        loop {
            let parent = graphforge_storage::resolve_project_generation(root)?;
            parent.validate_complete_participant_inventory()?;
            let mut reconciled = false;
            let expected_parent = *self
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned");
            if parent.generation_uuid() != expected_parent {
                if !optimistic {
                    return Err(idempotency_conflict(
                        "project generation changed before composite publication",
                    ));
                }
                if administrative_contract(&parent)?
                    != administrative_contract(&self.resolved_generation)?
                {
                    return Err(write_conflict(
                        "project capabilities or workspace configuration changed since open",
                    ));
                }
                reconcile_workspace_to(self, &parent)?;
                reconciled = true;
            }

            if optimistic && baseline.is_none() {
                baseline = Some(capture_rebase_baseline(self, &request, &parent)?);
            }
            let snapshot = build_validation_snapshot(self, &parent)?;
            let receipt =
                authorize_composite_transaction(&request, &snapshot, None).map_err(|error| {
                    if (reconciled || rebases > 0) && error.code() == "GF_IDENTITY_CONFLICT" {
                        write_conflict("concurrent operation occupied a requested identity")
                    } else {
                        error
                    }
                })?;
            require_capabilities(&parent, &request)?;

            match self.publish_composite_attempt(
                &request,
                &parent,
                receipt,
                content_fingerprint,
                generation_uuid,
                optimistic,
            ) {
                Ok(batch) => return Ok(batch),
                Err(error) if optimistic && error.code() == "GF_WRITE_CONFLICT" => {
                    let baseline = baseline
                        .as_ref()
                        .expect("optimistic publication initializes its rebase baseline");
                    if baseline.non_mergeable {
                        return Err(write_conflict(
                            "concurrent change conflicts with delete or administrative graph work",
                        ));
                    }
                    if rebases >= self.write_options.max_rebase_attempts {
                        return Err(GfError::Project {
                            code: ProjectErrorCode::RebaseExhausted,
                            message: format!(
                                "operation_uuid={} attempts={} cause=optimistic_contention",
                                transaction_uuid.hyphenated(),
                                rebases + 1
                            ),
                        });
                    }
                    let latest = graphforge_storage::resolve_project_generation(root)?;
                    reconcile_workspace_to(self, &latest)?;
                    ensure_rebase_compatible(self, &request, &latest, baseline)?;
                    rebases += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Authorize a composite request against the currently pinned generation.
    pub(crate) fn authorize_composite_against_current(
        &self,
        request: &CompositeTransactionRequest,
    ) -> Result<RecordBatch, GfError> {
        let root = self.resolved_generation.container_root();
        let parent = graphforge_storage::resolve_project_generation(root)?;
        let snapshot = build_validation_snapshot(self, &parent)?;
        authorize_composite_transaction(request, &snapshot, None)
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn publish_composite_attempt(
        &self,
        request: &CompositeTransactionRequest,
        parent: &ResolvedProjectGeneration,
        receipt: RecordBatch,
        content_fingerprint: [u8; 32],
        generation_uuid: Uuid,
        optimistic: bool,
    ) -> Result<RecordBatch, GfError> {
        let root = self.resolved_generation.container_root();
        let expected_parent = parent.generation_uuid();
        let transaction_uuid = request.context.operation_uuid.0;

        let prior_generation = parent.clone();
        let prior_catalog = self
            .runtime_catalog
            .lock()
            .expect("runtime catalog poisoned")
            .clone();
        let mut next_catalog = prior_catalog.clone();
        let recorded_at = (self.clock.lock().expect("clock lock poisoned"))()?;

        let publication = (|| -> Result<RecordBatch, GfError> {
            // Select the storage route from explicit typed input before the
            // private workspace is mutated. Capacity exhaustion deliberately
            // falls back to canonical full-Parquet publication.
            let prepared_delta = if !optimistic
                && let Some(operations) = eligible_delta_operations(request)?
            {
                let delta_request = graphforge_storage::GraphDeltaPublishRequest {
                    transaction_uuid,
                    generation_uuid,
                    run_uuid: Uuid::new_v5(&transaction_uuid, b"graphforge-composite-delta-run/1"),
                    operations,
                    limits: graphforge_storage::GraphDeltaJournalLimits::default(),
                };
                match graphforge_storage::prepare_graph_delta(parent, &delta_request) {
                    Ok(prepared) => Some(prepared),
                    Err(error) if error.code() == "GF_RESOURCE_LIMIT" => None,
                    Err(error) => return Err(error),
                }
            } else {
                None
            };
            let property_inventory =
                crate::property_inventory_for_hydrated_generation(parent, &self.dir)?;
            apply_graph_mutations(
                self,
                request,
                &mut next_catalog,
                recorded_at,
                property_inventory.as_ref(),
            )?;
            if self.path.is_some() {
                crate::persist_runtime_catalog(&self.dir, &next_catalog)?;
            }
            let graph = match prepared_delta.as_ref() {
                Some(prepared) => prepared.files_participant.clone(),
                None => graphforge_storage::capture_graph_files(&self.dir)?.1,
            };
            let participants = assemble_composite_participants(self, parent, request, graph)?;
            let capabilities = parent
                .capabilities()
                .into_iter()
                .map(|capability| ProjectCapability {
                    capability_id: capability.capability_id,
                    capability_version: capability.capability_version,
                })
                .collect();
            let publication = ProjectGenerationRequest {
                transaction_uuid,
                generation_uuid,
                capabilities,
                participants,
            };
            let staged = if optimistic {
                graphforge_storage::stage_project_generation_optimistic_with_graph_tree_mode(
                    root,
                    &publication,
                    content_fingerprint,
                    Some(
                        prepared_delta
                            .as_ref()
                            .map_or(self.dir.as_path(), |prepared| prepared.graph_tree_root()),
                    ),
                    self.lifecycle_mode,
                )?
            } else {
                graphforge_storage::stage_project_generation_with_graph_tree_mode(
                    root,
                    &publication,
                    Some(
                        prepared_delta
                            .as_ref()
                            .map_or(self.dir.as_path(), |prepared| prepared.graph_tree_root()),
                    ),
                    self.lifecycle_mode,
                )?
            };
            #[cfg(test)]
            optimistic_publish_barrier_for_test(optimistic);
            let outcome = match staged {
                ProjectStageOutcome::AlreadyPublished(published) => published,
                ProjectStageOutcome::Staged(staged) => staged
                    .validate(
                        |_| Ok(()),
                        |actual_parent, _| {
                            if actual_parent.generation_uuid() != expected_parent {
                                return Err(publication_parent_changed(optimistic));
                            }
                            Ok(())
                        },
                    )?
                    .publish()?,
            };
            if outcome.generation_uuid != generation_uuid {
                return Err(GfError::Validation(
                    "composite publication generation UUID drifted from the authorized receipt"
                        .into(),
                ));
            }
            let committed = graphforge_storage::resolve_project_generation(root)?;
            if committed.generation_uuid() != outcome.generation_uuid {
                return Err(GfError::Storage(
                    "composite property authority did not resolve exact generation".into(),
                ));
            }
            self.install_property_generation(&committed)?;
            Ok(receipt)
        })();

        match publication {
            Ok(batch) => {
                *self
                    .runtime_catalog
                    .lock()
                    .expect("runtime catalog poisoned") = next_catalog;
                self.adjacency_provider.invalidate();
                debug_assert_eq!(batch.schema().as_ref(), composite_receipt_schema().as_ref());
                Ok(batch)
            }
            Err(error) => {
                // Preserve the stable publication error. Recovery is best-effort:
                // callers must not receive an unrelated storage code instead of
                // the validation or conflict that caused publication to abort.
                if let Ok(durable) = graphforge_storage::resolve_project_generation(root) {
                    if durable.generation_uuid() == expected_parent {
                        if crate::rematerialize_graph_workspace(&prior_generation, &self.dir)
                            .is_ok()
                        {
                            *self
                                .runtime_catalog
                                .lock()
                                .expect("runtime catalog poisoned") = prior_catalog;
                        }
                    } else {
                        let _ = reconcile_workspace_to(self, &durable);
                    }
                }
                Err(error)
            }
        }
    }
}

#[cfg(test)]
fn optimistic_publish_barrier_for_test(optimistic: bool) {
    if !optimistic {
        return;
    }
    let barrier = OPTIMISTIC_PUBLISH_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("optimistic publish test barrier poisoned")
        .clone();
    if let Some(barrier) = barrier {
        let (state, changed) = barrier.as_ref();
        let mut state = state
            .lock()
            .expect("optimistic publish test barrier poisoned");
        state.arrived += 1;
        if state.arrived == 2 {
            state.released = true;
            changed.notify_all();
        } else {
            let (next, timeout) = changed
                .wait_timeout_while(state, std::time::Duration::from_secs(5), |state| {
                    !state.released
                })
                .expect("optimistic publish test barrier poisoned");
            assert!(
                !timeout.timed_out(),
                "optimistic publish test barrier timed out waiting for a second writer"
            );
            state = next;
        }
        drop(state);
        *OPTIMISTIC_PUBLISH_BARRIER
            .get()
            .expect("optimistic publish test barrier initialized")
            .lock()
            .expect("optimistic publish test barrier poisoned") = None;
    }
}

#[cfg(test)]
#[derive(Default)]
struct OptimisticPublishBarrierState {
    arrived: usize,
    released: bool,
}

#[cfg(test)]
type OptimisticPublishBarrier = std::sync::Arc<(
    std::sync::Mutex<OptimisticPublishBarrierState>,
    std::sync::Condvar,
)>;

#[cfg(test)]
static OPTIMISTIC_PUBLISH_BARRIER: std::sync::OnceLock<
    std::sync::Mutex<Option<OptimisticPublishBarrier>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
static OPTIMISTIC_PUBLISH_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum RebaseEntity {
    Node(Uuid),
    Edge(Uuid),
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RebaseField {
    entity: RebaseEntity,
    property: String,
}

type AdministrativeContract = Vec<(String, String, u32, [u8; 32])>;

#[derive(Clone, Debug, Default, PartialEq)]
struct RebaseBaseline {
    fields: BTreeMap<RebaseField, Option<IrLiteral>>,
    node_targets: BTreeSet<Uuid>,
    edge_targets: BTreeSet<Uuid>,
    administrative_contract: AdministrativeContract,
    non_mergeable: bool,
}

fn capture_rebase_baseline(
    graph: &GraphForge,
    request: &CompositeTransactionRequest,
    generation: &ResolvedProjectGeneration,
) -> Result<RebaseBaseline, GfError> {
    let created_nodes = request
        .graph_mutations
        .iter()
        .filter_map(|mutation| match mutation {
            CompositeGraphMutation::CreateNode { node_uuid, .. } => Some(*node_uuid),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let created_edges = request
        .graph_mutations
        .iter()
        .filter_map(|mutation| match mutation {
            CompositeGraphMutation::CreateEdge { edge_uuid, .. } => Some(*edge_uuid),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let mut baseline = RebaseBaseline {
        administrative_contract: administrative_contract(generation)?,
        ..RebaseBaseline::default()
    };
    for mutation in &request.graph_mutations {
        match mutation {
            CompositeGraphMutation::SetNodeProperty {
                node_uuid,
                property,
                ..
            }
            | CompositeGraphMutation::RemoveNodeProperty {
                node_uuid,
                property,
            } if !created_nodes.contains(node_uuid) => {
                baseline.node_targets.insert(*node_uuid);
                capture_rebase_field(graph, &mut baseline, *node_uuid, property, false)?;
            }
            CompositeGraphMutation::SetEdgeProperty {
                edge_uuid,
                property,
                ..
            }
            | CompositeGraphMutation::RemoveEdgeProperty {
                edge_uuid,
                property,
            } if !created_edges.contains(edge_uuid) => {
                baseline.edge_targets.insert(*edge_uuid);
                capture_rebase_field(graph, &mut baseline, *edge_uuid, property, true)?;
            }
            CompositeGraphMutation::DeleteNode { .. }
            | CompositeGraphMutation::DeleteEdge { .. } => baseline.non_mergeable = true,
            _ => {}
        }
    }
    Ok(baseline)
}

fn capture_rebase_field(
    graph: &GraphForge,
    baseline: &mut RebaseBaseline,
    uuid: Uuid,
    property: &str,
    is_edge: bool,
) -> Result<(), GfError> {
    let kind = if is_edge {
        graphforge_storage::PropertyRouteKind::Edge
    } else {
        graphforge_storage::PropertyRouteKind::Node
    };
    let inventory = graph.property_inventory_for_session();
    let (rows, _) = graphforge_storage::read_authenticated_property_snapshots_for_inventory(
        &inventory,
        kind,
        "_untyped",
        &BTreeSet::from([uuid.into_bytes()]),
    )?;
    let properties = rows
        .get(&uuid.into_bytes())
        .map_or_else(BTreeMap::new, |row| row.values.clone());
    let entity = if is_edge {
        RebaseEntity::Edge(uuid)
    } else {
        RebaseEntity::Node(uuid)
    };
    baseline.fields.insert(
        RebaseField {
            entity,
            property: property.to_owned(),
        },
        properties.get(property).cloned(),
    );
    Ok(())
}

fn ensure_rebase_compatible(
    graph: &GraphForge,
    request: &CompositeTransactionRequest,
    latest: &ResolvedProjectGeneration,
    baseline: &RebaseBaseline,
) -> Result<(), GfError> {
    if administrative_contract(latest)? != baseline.administrative_contract {
        return Err(write_conflict(
            "concurrent operation changed project capabilities or workspace configuration",
        ));
    }
    let snapshot = build_validation_snapshot(graph, latest)?;
    if !baseline.node_targets.is_subset(&snapshot.nodes)
        || !baseline.edge_targets.is_subset(&snapshot.edges)
    {
        return Err(write_conflict(
            "concurrent operation removed a graph mutation target",
        ));
    }
    let current = capture_rebase_baseline(graph, request, latest)?;
    if current.fields != baseline.fields {
        return Err(write_conflict(
            "concurrent operation changed a requested graph property",
        ));
    }
    Ok(())
}

fn administrative_contract(
    generation: &ResolvedProjectGeneration,
) -> Result<AdministrativeContract, GfError> {
    let mut contract = generation
        .participant_descriptors()?
        .into_iter()
        .filter(|descriptor| descriptor.capability_id == "workspace")
        .map(|descriptor| {
            (
                descriptor.capability_id,
                descriptor.record_family_id,
                descriptor.capability_version,
                descriptor.content_sha256,
            )
        })
        .collect::<Vec<_>>();
    contract.extend(generation.capabilities().into_iter().map(|capability| {
        (
            capability.capability_id,
            String::new(),
            capability.capability_version,
            [0; 32],
        )
    }));
    contract.sort();
    Ok(contract)
}

fn reconcile_workspace_to(
    graph: &GraphForge,
    generation: &ResolvedProjectGeneration,
) -> Result<(), GfError> {
    crate::rematerialize_graph_workspace(generation, &graph.dir)?;
    *graph
        .runtime_catalog
        .lock()
        .expect("runtime catalog poisoned") = crate::load_runtime_catalog(&graph.dir)?;
    graph.install_property_generation(generation)?;
    graph.adjacency_provider.invalidate();
    Ok(())
}

fn idempotency_conflict(message: impl Into<String>) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::TransactionConflict,
        message: message.into(),
    }
}

fn write_conflict(message: impl Into<String>) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::WriteConflict,
        message: message.into(),
    }
}

fn publication_parent_changed(optimistic: bool) -> GfError {
    let message = "project generation changed before composite publication";
    if optimistic {
        write_conflict(message)
    } else {
        idempotency_conflict(message)
    }
}

fn require_capabilities(
    parent: &ResolvedProjectGeneration,
    request: &CompositeTransactionRequest,
) -> Result<(), GfError> {
    let k = &request.knowledge;
    if !request.graph_mutations.is_empty() {
        // Graph is always present in a project generation inventory.
    }
    if !k.provenance_events.is_empty() || !k.lineage.is_empty() {
        parent.require_capability("provenance", 1)?;
    }
    if !k.assertions.is_empty()
        || !k.assertion_graph_refs.is_empty()
        || !k.confidence_assessments.is_empty()
        || !k.confidence_inputs.is_empty()
        || !k.evidence.is_empty()
    {
        parent.require_capability("knowledge", 1)?;
    }
    if !k.reasoning.is_empty()
        || !k.assertion_status.is_empty()
        || !k.assertion_supersessions.is_empty()
        || !k.hypothesis_groups.is_empty()
        || !k.hypothesis_membership.is_empty()
        || !k.hypothesis_selection.is_empty()
    {
        parent.require_capability(
            "epistemic",
            graphforge_knowledge::EPISTEMIC_CAPABILITY_VERSION,
        )?;
    }
    if !k.assertion_validity.is_empty() {
        parent.require_capability("valid_time", 1)?;
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn build_validation_snapshot(
    graph: &GraphForge,
    parent: &ResolvedProjectGeneration,
) -> Result<CompositeValidationSnapshot, GfError> {
    let mut snapshot = CompositeValidationSnapshot {
        ontology: CompositeOntologySnapshot {
            mode: graph.ontology_mode,
            entity_types: BTreeSet::new(),
            relation_types: BTreeSet::new(),
        },
        ..CompositeValidationSnapshot::default()
    };
    if graph.ontology_mode == OntologyMode::Strict
        && let Some(ontology) = graph.ontology.as_ref()
    {
        snapshot.ontology.entity_types = ontology
            .entity_type_names()
            .into_iter()
            .map(str::to_owned)
            .collect();
        snapshot.ontology.relation_types = ontology
            .relation_type_names()
            .into_iter()
            .map(str::to_owned)
            .collect();
    }
    for batch in graphforge_storage::read_nodes(&graph.dir)
        .map_err(|error| GfError::Storage(format!("failed to read node topology: {error}")))?
    {
        let Some(column) = batch.column_by_name("node_uuid") else {
            continue;
        };
        let uuids = column
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| GfError::Storage("node_uuid column has unexpected type".into()))?;
        for row in 0..uuids.len() {
            if !uuids.is_null(row) {
                snapshot
                    .nodes
                    .insert(Uuid::from_slice(uuids.value(row)).map_err(|_| {
                        GfError::Storage("persisted node UUID is malformed".into())
                    })?);
            }
        }
    }
    for batch in graphforge_storage::read_edges(&graph.dir, "*", graph.ontology_mode)
        .map_err(|error| GfError::Storage(format!("failed to read edge topology: {error}")))?
    {
        let Some(column) = batch.column_by_name("edge_uuid") else {
            continue;
        };
        let uuids = column
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| GfError::Storage("edge_uuid column has unexpected type".into()))?;
        for row in 0..uuids.len() {
            if !uuids.is_null(row) {
                snapshot
                    .edges
                    .insert(Uuid::from_slice(uuids.value(row)).map_err(|_| {
                        GfError::Storage("persisted edge UUID is malformed".into())
                    })?);
            }
        }
    }
    if parent.capability("provenance")?.is_some() {
        let ledger = crate::provenance::read_ledger(parent)?;
        snapshot.provenance = ledger
            .events
            .iter()
            .map(|row| row.provenance_uuid)
            .collect();
        snapshot.lineage = ledger.lineage.iter().map(|row| row.lineage_uuid).collect();
    }
    if parent.capability("knowledge")?.is_some() {
        let assertions = crate::knowledge::read_ledger(parent)?;
        snapshot.assertions = assertions
            .assertions
            .iter()
            .map(|row| row.assertion_uuid)
            .collect();
        let confidence = crate::knowledge::read_confidence_ledger(parent)?;
        snapshot.confidence = confidence
            .assessments
            .iter()
            .map(|row| row.confidence_uuid)
            .collect();
        let evidence = crate::knowledge::read_evidence_ledger(parent)?;
        snapshot.evidence = evidence.links.iter().map(|row| row.evidence_uuid).collect();
    }
    if parent.capability("epistemic")?.is_some() {
        let reasoning = crate::knowledge::read_reasoning_ledger(parent)?;
        snapshot.reasoning = reasoning
            .records
            .iter()
            .map(|row| row.reasoning_uuid)
            .collect();
        let status = crate::knowledge::read_status_ledger(parent)?;
        snapshot.status_events = status
            .events
            .iter()
            .map(|row| row.status_event_uuid)
            .collect();
        let supersessions = crate::knowledge::read_supersession_ledger(parent)?;
        snapshot.supersessions = supersessions
            .relations()
            .iter()
            .map(|row| row.supersession_uuid)
            .collect();
        let hypotheses = crate::hypotheses::read_ledger(parent)?;
        snapshot.hypothesis_groups = hypotheses
            .groups()
            .iter()
            .map(|row| row.group_uuid)
            .collect();
        snapshot.membership_events = hypotheses
            .membership_events()
            .iter()
            .map(|row| row.membership_event_uuid)
            .collect();
        snapshot.selection_events = hypotheses
            .selection_events()
            .iter()
            .map(|row| row.selection_event_uuid)
            .collect();
    }
    if parent.capability("valid_time")?.is_some() {
        let validity = crate::valid_time::read_ledger(parent)?;
        snapshot.validity_events = validity
            .events
            .iter()
            .map(|row| row.validity_event_uuid)
            .collect();
    }
    if parent.capability("knowledge")?.is_some()
        && parent
            .participant_snapshot("knowledge", "algorithm_runs")?
            .is_some()
    {
        let runs = crate::algorithm_runs::read_ledger(parent)?;
        snapshot.algorithm_runs = runs.runs.iter().map(|row| row.run_uuid).collect();
    }
    Ok(snapshot)
}

fn register_existing_endpoints(
    writer: &mut graphforge_storage::GraphWriter,
    dir: &Path,
    endpoints: &BTreeSet<Uuid>,
    same_request_nodes: &BTreeSet<Uuid>,
) -> Result<(), GfError> {
    let existing = endpoints
        .difference(same_request_nodes)
        .copied()
        .collect::<Vec<_>>();
    if existing.is_empty() {
        return Ok(());
    }
    if !graphforge_storage::uuid_membership_index_is_fresh(dir)? {
        return Err(GfError::Storage(
            "composite endpoint resolution requires a fresh authenticated UUID index; run the explicit bounded index migration first".into(),
        ));
    }
    writer.register_existing_endpoints(&existing)?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn apply_graph_mutations(
    graph: &GraphForge,
    request: &CompositeTransactionRequest,
    catalog: &mut RuntimeCatalog,
    recorded_at: i64,
    inventory: &graphforge_storage::AuthenticatedPropertyInventory,
) -> Result<(), GfError> {
    if request.graph_mutations.is_empty() {
        return Ok(());
    }
    let mut writer =
        graphforge_storage::GraphWriter::open_at(&graph.dir, graph.ontology_mode, recorded_at)?;
    let endpoints = request
        .graph_mutations
        .iter()
        .filter_map(|mutation| match mutation {
            CompositeGraphMutation::CreateEdge {
                source_uuid,
                target_uuid,
                ..
            } => Some([*source_uuid, *target_uuid]),
            _ => None,
        })
        .flatten()
        .collect::<BTreeSet<_>>();
    let same_request_nodes = request
        .graph_mutations
        .iter()
        .filter_map(|mutation| match mutation {
            CompositeGraphMutation::CreateNode { node_uuid, .. } => Some(*node_uuid),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    register_existing_endpoints(&mut writer, &graph.dir, &endpoints, &same_request_nodes)?;
    let mut node_sets: HashMap<String, HashMap<[u8; 16], HashMap<String, IrLiteral>>> =
        HashMap::new();
    let mut edge_sets: HashMap<String, HashMap<[u8; 16], HashMap<String, IrLiteral>>> =
        HashMap::new();
    let mut node_removes: HashMap<String, HashMap<[u8; 16], HashSet<String>>> = HashMap::new();
    let mut edge_removes: HashMap<String, HashMap<[u8; 16], HashSet<String>>> = HashMap::new();
    let mut delete_nodes = HashSet::new();
    let mut delete_edges = HashSet::new();

    // Allocate every same-request node first so edge endpoints are independent
    // of mutation ordering, as guaranteed by composite validation.
    for mutation in &request.graph_mutations {
        if let CompositeGraphMutation::CreateNode {
            node_uuid, label, ..
        } = mutation
        {
            let type_id = graph
                .ontology
                .as_ref()
                .and_then(|ontology| ontology.entity_type_id(label))
                .unwrap_or_else(|| {
                    graphforge_ir::runtime_entity_type_id(catalog.intern_label(label))
                });
            writer.create_node(*node_uuid, type_id)?;
        }
    }

    for mutation in &request.graph_mutations {
        match mutation {
            CompositeGraphMutation::CreateNode {
                node_uuid,
                label,
                properties,
            } => {
                if !properties.is_empty() {
                    let props = properties
                        .iter()
                        .map(|(name, value)| {
                            catalog.intern_property(name, Some(label));
                            Ok((name.clone(), prop_literal(value)?))
                        })
                        .collect::<Result<HashMap<_, _>, GfError>>()?;
                    let property_route = match graph.ontology_mode {
                        OntologyMode::Advisory | OntologyMode::Strict => label.clone(),
                        OntologyMode::Exploratory => "_untyped".to_owned(),
                    };
                    node_sets
                        .entry(property_route)
                        .or_default()
                        .insert(node_uuid.into_bytes(), props);
                }
            }
            CompositeGraphMutation::CreateEdge {
                edge_uuid,
                rel_type,
                source_uuid,
                target_uuid,
                properties,
            } => {
                catalog.intern_relation_type(rel_type);
                writer.create_edge(*edge_uuid, rel_type, source_uuid, target_uuid)?;
                if !properties.is_empty() {
                    let props = properties
                        .iter()
                        .map(|(name, value)| {
                            catalog.intern_property(name, Some(rel_type));
                            Ok((name.clone(), prop_literal(value)?))
                        })
                        .collect::<Result<HashMap<_, _>, GfError>>()?;
                    edge_sets
                        .entry(rel_type.clone())
                        .or_default()
                        .insert(edge_uuid.into_bytes(), props);
                }
            }
            CompositeGraphMutation::SetNodeProperty {
                node_uuid,
                property,
                value,
            } => {
                catalog.intern_property(property, None);
                let literal = prop_literal(value)?;
                node_sets
                    .entry("_untyped".into())
                    .or_default()
                    .entry(node_uuid.into_bytes())
                    .or_default()
                    .insert(property.clone(), literal);
            }
            CompositeGraphMutation::SetEdgeProperty {
                edge_uuid,
                property,
                value,
            } => {
                catalog.intern_property(property, None);
                let literal = prop_literal(value)?;
                edge_sets
                    .entry("_untyped".into())
                    .or_default()
                    .entry(edge_uuid.into_bytes())
                    .or_default()
                    .insert(property.clone(), literal);
            }
            CompositeGraphMutation::RemoveNodeProperty {
                node_uuid,
                property,
            } => {
                node_removes
                    .entry("_untyped".into())
                    .or_default()
                    .entry(node_uuid.into_bytes())
                    .or_default()
                    .insert(property.clone());
            }
            CompositeGraphMutation::RemoveEdgeProperty {
                edge_uuid,
                property,
            } => {
                edge_removes
                    .entry("_untyped".into())
                    .or_default()
                    .entry(edge_uuid.into_bytes())
                    .or_default()
                    .insert(property.clone());
            }
            CompositeGraphMutation::DeleteNode { node_uuid } => {
                delete_nodes.insert(node_uuid.into_bytes());
            }
            CompositeGraphMutation::DeleteEdge { edge_uuid } => {
                delete_edges.insert(edge_uuid.into_bytes());
            }
        }
    }

    let mut staged = RewriteBatch::new();
    writer.flush_into(&mut staged)?;
    for (stem, updates) in &node_sets {
        graphforge_storage::stage_set_node_properties_authenticated(
            &mut staged,
            &graph.dir,
            inventory,
            stem,
            updates,
        )?;
    }
    for (stem, updates) in &edge_sets {
        graphforge_storage::stage_set_edge_properties_authenticated(
            &mut staged,
            &graph.dir,
            inventory,
            stem,
            updates,
        )?;
    }
    for (stem, removals) in &node_removes {
        graphforge_storage::stage_remove_node_properties_authenticated(
            &mut staged,
            &graph.dir,
            inventory,
            stem,
            removals,
        )?;
    }
    for (stem, removals) in &edge_removes {
        graphforge_storage::stage_remove_edge_properties_authenticated(
            &mut staged,
            &graph.dir,
            inventory,
            stem,
            removals,
        )?;
    }
    graphforge_storage::stage_delete_edges_authenticated(
        &mut staged,
        &graph.dir,
        inventory,
        &delete_edges,
    )?;
    graphforge_storage::stage_delete_nodes_authenticated(
        &mut staged,
        &graph.dir,
        inventory,
        &delete_nodes,
    )?;
    let deleted_nodes = delete_nodes
        .iter()
        .copied()
        .map(Uuid::from_bytes)
        .collect::<Vec<_>>();
    let deleted_edges = delete_edges
        .iter()
        .copied()
        .map(Uuid::from_bytes)
        .collect::<Vec<_>>();
    writer.commit_topology_aware_with_uuid_index(staged, deleted_nodes, deleted_edges)?;
    Ok(())
}

fn assemble_composite_participants(
    graph: &GraphForge,
    parent: &ResolvedProjectGeneration,
    request: &CompositeTransactionRequest,
    graph_participant: ProjectParticipant,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let knowledge = &request.knowledge;
    let replaced = replacement_families(request);
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(replaced.contains(&(
                snapshot.capability_id.as_str(),
                snapshot.record_family_id.as_str(),
            )) || snapshot.capability_id == "graph"
                && matches!(snapshot.record_family_id.as_str(), "snapshot" | "files"))
        })
        .map(crate::knowledge::snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    participants.push(graph_participant);

    if replaced.iter().any(|(cap, _)| *cap == "provenance") {
        let merged = merge_provenance(parent, knowledge)?;
        participants.extend(crate::provenance::encode_ledger(&merged)?);
    }
    if replaced.iter().any(|(cap, fam)| {
        *cap == "knowledge" && matches!(*fam, "assertions" | "assertion_graph_refs")
    }) {
        let merged = merge_assertions(parent, knowledge)?;
        participants.extend(crate::knowledge::encode_ledger(&merged)?);
    }
    if replaced.iter().any(|(cap, fam)| {
        *cap == "knowledge" && matches!(*fam, "confidence_assessments" | "confidence_inputs")
    }) {
        let merged = merge_confidence(parent, knowledge)?;
        participants.extend(crate::knowledge::encode_confidence_ledger(&merged)?);
    }
    if replaced
        .iter()
        .any(|(cap, fam)| *cap == "knowledge" && *fam == "evidence")
    {
        let merged = merge_evidence(parent, knowledge)?;
        participants.extend(crate::knowledge::encode_evidence_ledger(&merged)?);
    }
    if replaced
        .iter()
        .any(|(cap, fam)| *cap == "epistemic" && *fam == "reasoning")
    {
        let merged = merge_reasoning(parent, knowledge)?;
        participants.extend(crate::knowledge::encode_reasoning_ledger(&merged)?);
    }
    if replaced
        .iter()
        .any(|(cap, fam)| *cap == "epistemic" && *fam == "assertion_status_events")
    {
        let merged = merge_status(parent, knowledge)?;
        participants.extend(crate::knowledge::encode_status_ledger(&merged)?);
    }
    if replaced
        .iter()
        .any(|(cap, fam)| *cap == "epistemic" && *fam == "assertion_supersessions")
    {
        let merged = merge_supersessions(parent, knowledge)?;
        participants.extend(crate::knowledge::encode_supersession_ledger(&merged)?);
    }
    if replaced.iter().any(|(cap, fam)| {
        *cap == "epistemic"
            && matches!(
                *fam,
                "hypothesis_groups"
                    | "hypothesis_membership_events"
                    | "hypothesis_selection_events"
            )
    }) {
        let merged = merge_hypotheses(parent, knowledge)?;
        crate::hypotheses::append_ledger_participants(&mut participants, &merged)?;
    }
    if replaced
        .iter()
        .any(|(cap, fam)| *cap == "valid_time" && *fam == "assertion_validity_events")
    {
        let merged = merge_validity(parent, knowledge)?;
        participants.extend(crate::valid_time::encode_ledger(&merged)?);
    }

    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    let _ = graph;
    Ok(participants)
}

fn replacement_families(
    request: &CompositeTransactionRequest,
) -> HashSet<(&'static str, &'static str)> {
    let k = &request.knowledge;
    let mut replaced = HashSet::new();
    if !request.graph_mutations.is_empty() {
        replaced.insert(("graph", "snapshot"));
        replaced.insert(("graph", "files"));
    }
    if !k.provenance_events.is_empty() || !k.lineage.is_empty() {
        replaced.insert(("provenance", "events"));
        replaced.insert(("provenance", "lineage"));
    }
    if !k.assertions.is_empty() || !k.assertion_graph_refs.is_empty() {
        replaced.insert(("knowledge", "assertions"));
        replaced.insert(("knowledge", "assertion_graph_refs"));
    }
    if !k.confidence_assessments.is_empty() || !k.confidence_inputs.is_empty() {
        replaced.insert(("knowledge", "confidence_assessments"));
        replaced.insert(("knowledge", "confidence_inputs"));
    }
    if !k.evidence.is_empty() {
        replaced.insert(("knowledge", "evidence"));
    }
    if !k.reasoning.is_empty() {
        replaced.insert(("epistemic", "reasoning"));
    }
    if !k.assertion_status.is_empty() {
        replaced.insert(("epistemic", "assertion_status_events"));
    }
    if !k.assertion_supersessions.is_empty() {
        replaced.insert(("epistemic", "assertion_supersessions"));
    }
    if !k.hypothesis_groups.is_empty()
        || !k.hypothesis_membership.is_empty()
        || !k.hypothesis_selection.is_empty()
    {
        replaced.insert(("epistemic", "hypothesis_groups"));
        replaced.insert(("epistemic", "hypothesis_membership_events"));
        replaced.insert(("epistemic", "hypothesis_selection_events"));
    }
    if !k.assertion_validity.is_empty() {
        replaced.insert(("valid_time", "assertion_validity_events"));
    }
    replaced
}

fn merge_provenance(
    parent: &ResolvedProjectGeneration,
    knowledge: &crate::composite_transaction::CompositeKnowledgeParticipants,
) -> Result<ProvenanceLedger, GfError> {
    let existing = if parent.capability("provenance")?.is_some() {
        crate::provenance::read_ledger(parent)?
    } else {
        ProvenanceLedger::default()
    };
    let staged = ProvenanceLedger::new(
        knowledge.provenance_events.clone(),
        knowledge.lineage.clone(),
    )
    .map_err(crate::provenance::provenance_error)?;
    existing
        .merge(&staged)
        .map_err(crate::provenance::provenance_error)
}

fn merge_assertions(
    parent: &ResolvedProjectGeneration,
    knowledge: &crate::composite_transaction::CompositeKnowledgeParticipants,
) -> Result<AssertionLedger, GfError> {
    let existing = if parent.capability("knowledge")?.is_some() {
        crate::knowledge::read_ledger(parent)?
    } else {
        AssertionLedger::default()
    };
    let assertion_ids = knowledge
        .assertions
        .iter()
        .map(|row| row.assertion_uuid)
        .collect::<HashSet<_>>();
    let (local_refs, external_refs): (Vec<_>, Vec<_>) = knowledge
        .assertion_graph_refs
        .iter()
        .cloned()
        .partition(|row| assertion_ids.contains(&row.assertion_uuid));
    let staged = AssertionLedger::new(knowledge.assertions.clone(), local_refs)
        .map_err(crate::knowledge::knowledge_error)?;
    let merged = existing
        .merge(&staged)
        .map_err(crate::knowledge::knowledge_error)?;
    if external_refs.is_empty() {
        return Ok(merged);
    }
    let mut refs = merged.graph_refs;
    for row in external_refs {
        if !refs.iter().any(|existing| existing == &row) {
            refs.push(row);
        }
    }
    AssertionLedger::new(merged.assertions, refs).map_err(crate::knowledge::knowledge_error)
}

fn merge_confidence(
    parent: &ResolvedProjectGeneration,
    knowledge: &crate::composite_transaction::CompositeKnowledgeParticipants,
) -> Result<ConfidenceLedger, GfError> {
    let existing = if parent.capability("knowledge")?.is_some() {
        crate::knowledge::read_confidence_ledger(parent)?
    } else {
        ConfidenceLedger::default()
    };
    let owner_ids = knowledge
        .confidence_assessments
        .iter()
        .map(|row| row.confidence_uuid)
        .collect::<HashSet<_>>();
    let (local_inputs, external_inputs): (Vec<_>, Vec<_>) = knowledge
        .confidence_inputs
        .iter()
        .cloned()
        .partition(|row| owner_ids.contains(&row.confidence_uuid));
    let staged = ConfidenceLedger::new(knowledge.confidence_assessments.clone(), local_inputs)
        .map_err(crate::knowledge::knowledge_error)?;
    let merged = existing
        .merge(&staged)
        .map_err(crate::knowledge::knowledge_error)?;
    if external_inputs.is_empty() {
        return Ok(merged);
    }
    let mut inputs = merged.inputs;
    for row in external_inputs {
        if !inputs.iter().any(|existing| existing == &row) {
            inputs.push(row);
        }
    }
    ConfidenceLedger::new(merged.assessments, inputs).map_err(crate::knowledge::knowledge_error)
}

fn merge_evidence(
    parent: &ResolvedProjectGeneration,
    knowledge: &crate::composite_transaction::CompositeKnowledgeParticipants,
) -> Result<EvidenceLedger, GfError> {
    let existing = if parent.capability("knowledge")?.is_some() {
        crate::knowledge::read_evidence_ledger(parent)?
    } else {
        EvidenceLedger::default()
    };
    let staged = EvidenceLedger::new(knowledge.evidence.clone())
        .map_err(crate::knowledge::knowledge_error)?;
    existing
        .merge(&staged)
        .map_err(crate::knowledge::knowledge_error)
}

fn merge_reasoning(
    parent: &ResolvedProjectGeneration,
    knowledge: &crate::composite_transaction::CompositeKnowledgeParticipants,
) -> Result<ReasoningLedger, GfError> {
    let existing = if parent.capability("epistemic")?.is_some() {
        crate::knowledge::read_reasoning_ledger(parent)?
    } else {
        ReasoningLedger::default()
    };
    let staged = ReasoningLedger::new(knowledge.reasoning.clone())
        .map_err(crate::knowledge::knowledge_error)?;
    existing
        .merge(&staged)
        .map_err(crate::knowledge::knowledge_error)
}

fn merge_status(
    parent: &ResolvedProjectGeneration,
    knowledge: &crate::composite_transaction::CompositeKnowledgeParticipants,
) -> Result<AssertionStatusLedger, GfError> {
    let existing = if parent.capability("epistemic")?.is_some() {
        crate::knowledge::read_status_ledger(parent)?
    } else {
        AssertionStatusLedger::default()
    };
    let staged = AssertionStatusLedger::new(knowledge.assertion_status.clone())
        .map_err(crate::knowledge::knowledge_error)?;
    existing
        .merge(&staged)
        .map_err(crate::knowledge::knowledge_error)
}

fn merge_supersessions(
    parent: &ResolvedProjectGeneration,
    knowledge: &crate::composite_transaction::CompositeKnowledgeParticipants,
) -> Result<AssertionSupersessionLedger, GfError> {
    let existing = if parent.capability("epistemic")?.is_some() {
        crate::knowledge::read_supersession_ledger(parent)?
    } else {
        AssertionSupersessionLedger::default()
    };
    let staged = AssertionSupersessionLedger::new(knowledge.assertion_supersessions.clone())
        .map_err(crate::knowledge::knowledge_error)?;
    existing
        .merge(&staged)
        .map_err(crate::knowledge::knowledge_error)
}

fn merge_hypotheses(
    parent: &ResolvedProjectGeneration,
    knowledge: &crate::composite_transaction::CompositeKnowledgeParticipants,
) -> Result<HypothesisLedger, GfError> {
    let existing = if parent.capability("epistemic")?.is_some() {
        crate::hypotheses::read_ledger(parent)?
    } else {
        HypothesisLedger::default()
    };
    let staged = HypothesisLedger::new(
        knowledge.hypothesis_groups.clone(),
        knowledge.hypothesis_membership.clone(),
        knowledge.hypothesis_selection.clone(),
    )
    .map_err(crate::knowledge::knowledge_error)?;
    existing
        .merge(&staged)
        .map_err(crate::knowledge::knowledge_error)
}

fn merge_validity(
    parent: &ResolvedProjectGeneration,
    knowledge: &crate::composite_transaction::CompositeKnowledgeParticipants,
) -> Result<AssertionValidityLedger, GfError> {
    let existing = if parent.capability("valid_time")?.is_some() {
        crate::valid_time::read_ledger(parent)?
    } else {
        AssertionValidityLedger::default()
    };
    let staged = AssertionValidityLedger::new(knowledge.assertion_validity.clone())
        .map_err(crate::knowledge::knowledge_error)?;
    existing
        .merge(&staged)
        .map_err(crate::knowledge::knowledge_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composite_transaction::{
        COMPOSITE_TRANSACTION_CONTRACT_VERSION, CompositeKnowledgeParticipants,
    };
    use crate::{
        CancellationToken, CapabilityId, EnableCapabilityRequest, GraphForgeOptions, OperationId,
        ProjectWriteMode, PropValue, WriteContext,
    };
    use arrow::array::StringArray;
    use graphforge_knowledge::{
        Assertion, AssertionGraphRef, AssertionGraphRole, AssertionStatus, AssertionStatusEvent,
        GraphObjectKind,
    };
    use graphforge_provenance::{
        EventKind, LineageRecord, LineageRole, ProvenanceEvent, SubjectKind,
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    fn uuid7(seed: u8) -> Uuid {
        let mut bytes = [seed; 16];
        bytes[..6].copy_from_slice(&[1, 2, 3, 4, 5, seed]);
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    fn enable(graph: &GraphForge, capability: CapabilityId, seed: u8) {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(seed)),
                    actor_uuid: None,
                },
                capability_id: capability,
                capability_version: 1,
            })
            .unwrap();
    }

    #[test]
    fn composite_endpoint_resolution_uses_index_without_topology_decode() {
        let directory = tempfile::tempdir().unwrap();
        let existing = uuid7(180);
        let mut seed =
            graphforge_storage::GraphWriter::open_at(directory.path(), OntologyMode::Strict, 1)
                .unwrap();
        seed.create_node(existing, graphforge_core::TypeId(0))
            .unwrap();
        seed.flush().unwrap();
        graphforge_storage::rebuild_uuid_membership_indexes(
            directory.path(),
            graphforge_storage::UuidIndexBuildLimits {
                scan_batch_rows: 1,
                run_records: 1,
                merge_fan_in: 2,
            },
        )
        .unwrap();

        let same_request = uuid7(181);
        let endpoints = BTreeSet::from([existing, same_request]);
        let same_request_nodes = BTreeSet::from([same_request]);
        graphforge_storage::io_stats::reset();
        let mut writer =
            graphforge_storage::GraphWriter::open_at(directory.path(), OntologyMode::Strict, 2)
                .unwrap();
        register_existing_endpoints(
            &mut writer,
            directory.path(),
            &endpoints,
            &same_request_nodes,
        )
        .unwrap();
        let io = graphforge_storage::io_stats::snapshot();
        assert_eq!(io.node_full_reads, 0);
        assert_eq!(io.node_filtered_reads, 0);

        writer
            .create_node(same_request, graphforge_core::TypeId(0))
            .unwrap();
        writer
            .create_edge(uuid7(182), "KNOWS", &existing, &same_request)
            .unwrap();
    }

    #[test]
    fn empty_composite_domain_merges_do_not_require_optional_capabilities() {
        let graph = GraphForge::new(None).unwrap();
        let parent = graphforge_storage::resolve_project_generation(
            graph.resolved_generation.container_root(),
        )
        .unwrap();
        let knowledge = CompositeKnowledgeParticipants::default();
        assert!(
            merge_provenance(&parent, &knowledge)
                .unwrap()
                .events
                .is_empty()
        );
        assert!(
            merge_assertions(&parent, &knowledge)
                .unwrap()
                .assertions
                .is_empty()
        );
        assert!(
            merge_confidence(&parent, &knowledge)
                .unwrap()
                .assessments
                .is_empty()
        );
        assert!(
            merge_evidence(&parent, &knowledge)
                .unwrap()
                .links
                .is_empty()
        );
        assert!(
            merge_reasoning(&parent, &knowledge)
                .unwrap()
                .records
                .is_empty()
        );
        assert!(merge_status(&parent, &knowledge).unwrap().events.is_empty());
        assert!(
            merge_supersessions(&parent, &knowledge)
                .unwrap()
                .relations()
                .is_empty()
        );
        let hypotheses = merge_hypotheses(&parent, &knowledge).unwrap();
        assert!(hypotheses.groups().is_empty());
        assert!(hypotheses.membership_events().is_empty());
        assert!(hypotheses.selection_events().is_empty());
        assert!(
            merge_validity(&parent, &knowledge)
                .unwrap()
                .events
                .is_empty()
        );
        assert!(
            replacement_families(&CompositeTransactionRequest {
                contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(99)),
                    actor_uuid: None,
                },
                graph_mutations: Vec::new(),
                knowledge,
            })
            .is_empty()
        );
    }

    fn publish_request() -> CompositeTransactionRequest {
        let operation = uuid7(10);
        let node = uuid7(20);
        let provenance = ProvenanceEvent::new(operation, EventKind::CreateNode, None, 10).unwrap();
        let assertion = Assertion::new(
            uuid7(30),
            "composite publishes atomically".into(),
            provenance.provenance_uuid,
            10,
        )
        .unwrap();
        let status = AssertionStatusEvent::new(
            uuid7(40),
            assertion.assertion_uuid,
            AssertionStatus::Supported,
            None,
            None,
            provenance.provenance_uuid,
            10,
        )
        .unwrap();
        CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: WriteContext {
                operation_uuid: OperationId(operation),
                actor_uuid: None,
            },
            graph_mutations: vec![CompositeGraphMutation::CreateNode {
                node_uuid: node,
                label: "Person".into(),
                properties: HashMap::from([("name".into(), PropValue::Str("Ada".into()))]),
            }],
            knowledge: CompositeKnowledgeParticipants {
                provenance_events: vec![provenance.clone()],
                lineage: vec![
                    LineageRecord::new(
                        provenance.provenance_uuid,
                        node,
                        SubjectKind::Node,
                        LineageRole::Output,
                        0,
                    )
                    .unwrap(),
                ],
                assertions: vec![assertion.clone()],
                assertion_graph_refs: vec![
                    AssertionGraphRef::new(
                        assertion.assertion_uuid,
                        node,
                        GraphObjectKind::Node,
                        AssertionGraphRole::Subject,
                        0,
                    )
                    .unwrap(),
                ],
                assertion_status: vec![status],
                ..CompositeKnowledgeParticipants::default()
            },
        }
    }

    fn graph_request(operation_seed: u8, node_seed: u8, name: &str) -> CompositeTransactionRequest {
        CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: WriteContext {
                operation_uuid: OperationId(uuid7(operation_seed)),
                actor_uuid: None,
            },
            graph_mutations: vec![CompositeGraphMutation::CreateNode {
                node_uuid: uuid7(node_seed),
                label: "Person".into(),
                properties: HashMap::from([("name".into(), PropValue::Str(name.into()))]),
            }],
            knowledge: CompositeKnowledgeParticipants::default(),
        }
    }

    fn property_request(
        operation_seed: u8,
        node_seed: u8,
        property: &str,
        value: &str,
    ) -> CompositeTransactionRequest {
        CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: WriteContext {
                operation_uuid: OperationId(uuid7(operation_seed)),
                actor_uuid: None,
            },
            graph_mutations: vec![CompositeGraphMutation::SetNodeProperty {
                node_uuid: uuid7(node_seed),
                property: property.into(),
                value: PropValue::Str(value.into()),
            }],
            knowledge: CompositeKnowledgeParticipants::default(),
        }
    }

    fn delete_node_request(operation_seed: u8, node_seed: u8) -> CompositeTransactionRequest {
        CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: WriteContext {
                operation_uuid: OperationId(uuid7(operation_seed)),
                actor_uuid: None,
            },
            graph_mutations: vec![CompositeGraphMutation::DeleteNode {
                node_uuid: uuid7(node_seed),
            }],
            knowledge: CompositeKnowledgeParticipants::default(),
        }
    }

    fn optimistic_options(max_rebase_attempts: u32) -> GraphForgeOptions {
        GraphForgeOptions {
            write_mode: ProjectWriteMode::OptimisticMultiWriter,
            max_rebase_attempts,
            ..GraphForgeOptions::default()
        }
    }

    #[test]
    fn publication_parent_drift_is_retriable_only_in_optimistic_mode() {
        assert_eq!(publication_parent_changed(true).code(), "GF_WRITE_CONFLICT");
        assert_eq!(
            publication_parent_changed(false).code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );
    }

    #[test]
    fn in_memory_optimistic_publication_uses_ephemeral_lifecycle_mode() {
        let graph = GraphForge::new_with_options(None, optimistic_options(1)).unwrap();
        assert_eq!(
            graph.lifecycle_mode,
            graphforge_storage::filesystem_admission::ProjectLifecycleMode::Ephemeral
        );
        let before = *graph
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");

        graph
            .publish_composite_transaction(graph_request(240, 241, "ephemeral"))
            .unwrap();

        assert_ne!(
            before,
            *graph
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned")
        );
    }

    #[test]
    fn rebase_compatibility_rejects_administrative_drift_and_removed_targets() {
        let graph = GraphForge::new(None).unwrap();
        let parent = graphforge_storage::resolve_project_generation(
            graph.resolved_generation.container_root(),
        )
        .unwrap();
        let request = publish_request();
        let mut baseline = RebaseBaseline {
            fields: BTreeMap::new(),
            node_targets: BTreeSet::new(),
            edge_targets: BTreeSet::new(),
            administrative_contract: Vec::new(),
            non_mergeable: false,
        };
        assert_eq!(
            ensure_rebase_compatible(&graph, &request, &parent, &baseline)
                .unwrap_err()
                .code(),
            "GF_WRITE_CONFLICT"
        );
        baseline.administrative_contract = administrative_contract(&parent).unwrap();
        baseline.node_targets.insert(uuid7(250));
        assert_eq!(
            ensure_rebase_compatible(&graph, &request, &parent, &baseline)
                .unwrap_err()
                .code(),
            "GF_WRITE_CONFLICT"
        );
    }

    fn publish_concurrently(
        directory: &TempDir,
        options: GraphForgeOptions,
        left: CompositeTransactionRequest,
        right: CompositeTransactionRequest,
    ) -> [Result<RecordBatch, GfError>; 2] {
        let left_graph = Arc::new(
            GraphForge::new_with_options(directory.path().to_str(), options.clone()).unwrap(),
        );
        let right_graph =
            Arc::new(GraphForge::new_with_options(directory.path().to_str(), options).unwrap());
        *OPTIMISTIC_PUBLISH_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .unwrap() = Some(Arc::new((
            std::sync::Mutex::new(OptimisticPublishBarrierState::default()),
            std::sync::Condvar::new(),
        )));
        let left_worker = thread::spawn(move || left_graph.publish_composite_transaction(left));
        let right_worker = thread::spawn(move || right_graph.publish_composite_transaction(right));
        [left_worker.join().unwrap(), right_worker.join().unwrap()]
    }

    #[test]
    fn publish_composite_is_one_generation_with_canonical_receipt() {
        let directory = TempDir::new().unwrap();
        let graph = GraphForge::new(directory.path().to_str()).unwrap();
        enable(&graph, CapabilityId::Provenance, 1);
        enable(&graph, CapabilityId::Knowledge, 2);
        enable(&graph, CapabilityId::Epistemic, 3);
        let before = *graph
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        let request = publish_request();
        let receipt = graph
            .publish_composite_transaction(request.clone())
            .unwrap();
        assert_eq!(
            receipt.schema().as_ref(),
            composite_receipt_schema().as_ref()
        );
        assert_eq!(receipt.num_rows(), 1);
        let after = *graph
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        assert_ne!(before, after);
        let generation = receipt
            .column_by_name("generation_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap()
            .value(0);
        assert_eq!(generation, after.as_bytes());

        let rows = graph
            .execute("MATCH (n:Person) RETURN n.node_uuid AS id, n.name AS name")
            .unwrap();
        assert_eq!(rows.batches[0].num_rows(), 1);
        let assertions = graph
            .list_assertions(crate::ListAssertionsRequest {
                graph_uuid: None,
                page: crate::PageRequest::default(),
            })
            .unwrap();
        assert_eq!(assertions.batches[0].num_rows(), 1);

        let replay = graph.publish_composite_transaction(request).unwrap();
        for index in 0..receipt.num_columns() {
            assert_eq!(
                receipt.column(index).as_ref(),
                replay.column(index).as_ref()
            );
        }
        assert_eq!(
            *graph
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned"),
            after
        );
    }

    #[test]
    fn cancelled_public_composite_publish_never_mutates() {
        let directory = TempDir::new().unwrap();
        let graph = GraphForge::new_with_options(
            directory.path().to_str(),
            GraphForgeOptions {
                write_mode: ProjectWriteMode::QueuedWriter,
                ..GraphForgeOptions::default()
            },
        )
        .unwrap();
        let before = *graph
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let error = graph
            .publish_composite_transaction_with_cancellation(
                graph_request(71, 72, "cancelled"),
                Some(cancellation),
            )
            .unwrap_err();

        assert_eq!(error.code(), "GF_CANCELLED");
        assert_eq!(
            *graph
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned"),
            before
        );
        assert!(
            graphforge_storage::published_project_transaction(directory.path(), uuid7(71))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn invalid_composite_request_does_not_mutate() {
        let directory = TempDir::new().unwrap();
        let graph = GraphForge::new(directory.path().to_str()).unwrap();
        enable(&graph, CapabilityId::Provenance, 1);
        enable(&graph, CapabilityId::Knowledge, 2);
        enable(&graph, CapabilityId::Epistemic, 3);
        let before = *graph
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        let mut request = publish_request();
        request.knowledge.assertion_graph_refs[0].graph_kind = GraphObjectKind::Edge;
        let error = graph.publish_composite_transaction(request).unwrap_err();
        assert_eq!(error.code(), "GF_NOT_FOUND");
        assert_eq!(
            *graph
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned"),
            before
        );
    }
    #[test]
    fn conflicting_composite_identity_reuse_does_not_mutate() {
        let directory = TempDir::new().unwrap();
        let graph = GraphForge::new(directory.path().to_str()).unwrap();
        enable(&graph, CapabilityId::Provenance, 1);
        enable(&graph, CapabilityId::Knowledge, 2);
        enable(&graph, CapabilityId::Epistemic, 3);
        let request = publish_request();
        let receipt = graph
            .publish_composite_transaction(request.clone())
            .unwrap();
        let published = *graph
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        let mut conflict = request;
        conflict.knowledge.assertions[0].claim = "different claim".into();
        let error = graph.publish_composite_transaction(conflict).unwrap_err();
        assert_eq!(error.code(), "GF_IDEMPOTENCY_CONFLICT");
        assert_eq!(
            *graph
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned"),
            published
        );
        let replay = graph
            .publish_composite_transaction(publish_request())
            .unwrap();
        for index in 0..receipt.num_columns() {
            assert_eq!(
                receipt.column(index).as_ref(),
                replay.column(index).as_ref()
            );
        }
    }

    #[test]
    fn optimistic_distinct_creates_rebase_and_both_publish() {
        let _serial = OPTIMISTIC_PUBLISH_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let directory = TempDir::new().unwrap();
        GraphForge::new(directory.path().to_str()).unwrap();
        let results = publish_concurrently(
            &directory,
            optimistic_options(1),
            graph_request(131, 132, "Ada"),
            graph_request(133, 134, "Grace"),
        );
        assert!(
            results.iter().all(Result::is_ok),
            "concurrent results: {results:?}"
        );
        let reopened = GraphForge::new(directory.path().to_str()).unwrap();
        let rows = reopened
            .execute("MATCH (n:Person) RETURN n.node_uuid AS id")
            .unwrap();
        assert_eq!(rows.batches[0].num_rows(), 2);
    }

    #[test]
    fn exploratory_composite_create_projects_properties_after_reopen() {
        let directory = TempDir::new().unwrap();
        let graph = GraphForge::new(directory.path().to_str()).unwrap();
        graph
            .publish_composite_transaction(graph_request(141, 142, "Ada"))
            .unwrap();

        let reopened = GraphForge::new(directory.path().to_str()).unwrap();
        let result = reopened
            .execute("MATCH (n:Person) RETURN n.name AS name")
            .unwrap();
        let names = result.batches[0]
            .column_by_name("name")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .unwrap();
        assert_eq!(names.value(0), "Ada");
    }

    #[test]
    fn eligible_property_commit_preserves_complete_generation_and_reopens_from_delta() {
        let directory = TempDir::new().unwrap();
        let graph = GraphForge::new(directory.path().to_str()).unwrap();
        graph
            .publish_composite_transaction(graph_request(121, 122, "Initial"))
            .unwrap();
        enable(&graph, CapabilityId::Provenance, 123);
        let root = graph.resolved_generation.container_root();
        let parent = graphforge_storage::resolve_project_generation(root).unwrap();
        assert_ne!(
            graph.property_inventory_for_session().generation_uuid(),
            Some(parent.generation_uuid()),
            "capability-only publication deliberately leaves the prior graph inventory cached"
        );
        let parent_graph = parent.graph_files_inventory().unwrap().unwrap();
        let unrelated = parent
            .participant_snapshots()
            .unwrap()
            .into_iter()
            .filter(|snapshot| snapshot.capability_id != "graph")
            .collect::<Vec<_>>();
        let request = property_request(124, 122, "nickname", "delta-visible");
        let first = graph
            .publish_composite_transaction(request.clone())
            .unwrap();
        let retry = graph.publish_composite_transaction(request).unwrap();
        assert_eq!(first, retry);

        let published = graphforge_storage::resolve_project_generation(root).unwrap();
        assert_eq!(
            unrelated,
            published
                .participant_snapshots()
                .unwrap()
                .into_iter()
                .filter(|snapshot| snapshot.capability_id != "graph")
                .collect::<Vec<_>>()
        );
        let published_graph = published.graph_files_inventory().unwrap().unwrap();
        assert_eq!(
            graphforge_storage::list_delta_runs(&published_graph, Default::default())
                .unwrap()
                .len(),
            1
        );
        for entry in parent_graph.files.iter().filter(|entry| {
            std::path::Path::new(&entry.relative_path)
                .extension()
                .is_some_and(|extension| extension == "parquet")
        }) {
            assert!(published_graph.files.iter().any(|candidate| {
                candidate.relative_path == entry.relative_path
                    && candidate.content_sha256 == entry.content_sha256
            }));
        }
        drop(graph);
        let reopened = GraphForge::new(directory.path().to_str()).unwrap();
        let rows = reopened
            .execute("MATCH (n:Person) RETURN n.nickname AS nickname")
            .unwrap();
        let values = rows.batches[0]
            .column_by_name("nickname")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "delta-visible");
    }

    #[test]
    fn optimistic_same_property_change_is_a_write_conflict() {
        let _serial = OPTIMISTIC_PUBLISH_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let directory = TempDir::new().unwrap();
        let bootstrap = GraphForge::new(directory.path().to_str()).unwrap();
        bootstrap
            .publish_composite_transaction(graph_request(135, 136, "Initial"))
            .unwrap();
        drop(bootstrap);
        let results = publish_concurrently(
            &directory,
            optimistic_options(1),
            property_request(137, 136, "nickname", "left"),
            property_request(138, 136, "nickname", "right"),
        );
        let codes = results
            .iter()
            .map(|result| result.as_ref().err().map(GfError::code))
            .collect::<Vec<_>>();
        assert_eq!(codes.iter().filter(|code| code.is_none()).count(), 1);
        assert_eq!(
            codes
                .iter()
                .filter(|code| **code == Some("GF_WRITE_CONFLICT"))
                .count(),
            1
        );
    }

    #[test]
    fn optimistic_delete_and_property_change_publish_exactly_one_complete_result() {
        let _serial = OPTIMISTIC_PUBLISH_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let directory = TempDir::new().unwrap();
        let bootstrap = GraphForge::new(directory.path().to_str()).unwrap();
        bootstrap
            .publish_composite_transaction(graph_request(160, 161, "Initial"))
            .unwrap();
        drop(bootstrap);

        let results = publish_concurrently(
            &directory,
            optimistic_options(1),
            delete_node_request(162, 161),
            property_request(163, 161, "nickname", "survivor"),
        );
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        let conflict = results
            .iter()
            .find_map(|result| result.as_ref().err())
            .unwrap();
        assert_eq!(conflict.code(), "GF_WRITE_CONFLICT");

        let reopened = GraphForge::new(directory.path().to_str()).unwrap();
        let rows = reopened
            .execute("MATCH (n:Person) RETURN n.nickname AS nickname")
            .unwrap();
        assert!(
            !rows.batches.is_empty(),
            "empty MATCH must still surface one schema-bearing batch"
        );
        assert!(rows.batches[0].num_rows() <= 1);
        if rows.batches[0].num_rows() == 1 {
            let nicknames = rows.batches[0]
                .column_by_name("nickname")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(nicknames.value(0), "survivor");
        }
    }

    #[test]
    fn optimistic_first_reconciliation_remaps_identity_collision() {
        let directory = TempDir::new().unwrap();
        GraphForge::new(directory.path().to_str()).unwrap();
        let options = optimistic_options(1);
        let stale =
            GraphForge::new_with_options(directory.path().to_str(), options.clone()).unwrap();
        let concurrent = GraphForge::new_with_options(directory.path().to_str(), options).unwrap();
        concurrent
            .publish_composite_transaction(graph_request(151, 152, "concurrent"))
            .unwrap();

        let error = stale
            .publish_composite_transaction(graph_request(153, 152, "stale"))
            .unwrap_err();

        assert_eq!(error.code(), "GF_WRITE_CONFLICT");
    }

    #[test]
    fn optimistic_retry_budget_is_bounded() {
        let _serial = OPTIMISTIC_PUBLISH_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let directory = TempDir::new().unwrap();
        GraphForge::new(directory.path().to_str()).unwrap();
        let results = publish_concurrently(
            &directory,
            optimistic_options(0),
            graph_request(139, 140, "Ada"),
            graph_request(141, 142, "Grace"),
        );
        let codes = results
            .iter()
            .map(|result| result.as_ref().err().map(GfError::code))
            .collect::<Vec<_>>();
        assert_eq!(codes.iter().filter(|code| code.is_none()).count(), 1);
        assert_eq!(
            codes
                .iter()
                .filter(|code| **code == Some("GF_REBASE_EXHAUSTED"))
                .count(),
            1
        );
    }

    #[test]
    fn composite_graph_mutations_cover_created_and_existing_objects() {
        let directory = TempDir::new().unwrap();
        let graph = GraphForge::new(directory.path().to_str()).unwrap();
        let left = uuid7(201);
        let right = uuid7(202);
        let old_edge = uuid7(203);
        graph
            .publish_composite_transaction(CompositeTransactionRequest {
                contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(200)),
                    actor_uuid: None,
                },
                graph_mutations: vec![
                    CompositeGraphMutation::CreateNode {
                        node_uuid: left,
                        label: "Person".into(),
                        properties: HashMap::from([(
                            "obsolete".into(),
                            PropValue::Str("left".into()),
                        )]),
                    },
                    CompositeGraphMutation::CreateNode {
                        node_uuid: right,
                        label: "Person".into(),
                        properties: HashMap::new(),
                    },
                    CompositeGraphMutation::CreateEdge {
                        edge_uuid: old_edge,
                        rel_type: "KNOWS".into(),
                        source_uuid: left,
                        target_uuid: right,
                        properties: HashMap::from([(
                            "obsolete".into(),
                            PropValue::Str("edge".into()),
                        )]),
                    },
                ],
                knowledge: CompositeKnowledgeParticipants::default(),
            })
            .unwrap();

        let created_node = uuid7(205);
        let created_edge = uuid7(206);
        graph
            .publish_composite_transaction(CompositeTransactionRequest {
                contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(204)),
                    actor_uuid: None,
                },
                graph_mutations: vec![
                    CompositeGraphMutation::CreateNode {
                        node_uuid: created_node,
                        label: "Person".into(),
                        properties: HashMap::new(),
                    },
                    CompositeGraphMutation::SetNodeProperty {
                        node_uuid: created_node,
                        property: "name".into(),
                        value: PropValue::Str("created".into()),
                    },
                    CompositeGraphMutation::CreateEdge {
                        edge_uuid: created_edge,
                        rel_type: "KNOWS".into(),
                        source_uuid: left,
                        target_uuid: created_node,
                        properties: HashMap::new(),
                    },
                    CompositeGraphMutation::SetEdgeProperty {
                        edge_uuid: created_edge,
                        property: "weight".into(),
                        value: PropValue::Int(7),
                    },
                    CompositeGraphMutation::SetNodeProperty {
                        node_uuid: left,
                        property: "nickname".into(),
                        value: PropValue::Str("existing".into()),
                    },
                    CompositeGraphMutation::SetEdgeProperty {
                        edge_uuid: old_edge,
                        property: "weight".into(),
                        value: PropValue::Int(3),
                    },
                    CompositeGraphMutation::RemoveNodeProperty {
                        node_uuid: left,
                        property: "obsolete".into(),
                    },
                    CompositeGraphMutation::RemoveEdgeProperty {
                        edge_uuid: old_edge,
                        property: "obsolete".into(),
                    },
                    CompositeGraphMutation::DeleteEdge {
                        edge_uuid: old_edge,
                    },
                    CompositeGraphMutation::DeleteNode { node_uuid: right },
                ],
                knowledge: CompositeKnowledgeParticipants::default(),
            })
            .unwrap();

        assert_eq!(
            graph
                .execute("MATCH (n:Person) RETURN n.node_uuid AS id")
                .unwrap()
                .batches[0]
                .num_rows(),
            2
        );
    }

    #[test]
    fn strict_composite_snapshot_uses_declared_ontology_types() {
        let directory = TempDir::new().unwrap();
        let mut graph = GraphForge::new(directory.path().to_str()).unwrap();
        let ontology_path = directory.path().join("strict.yaml");
        std::fs::write(
            &ontology_path,
            "ontology_id: composite\nversion: \"1\"\nentity_types:\n  - name: Person\n    abstract: false\nrelation_types:\n  - name: KNOWS\n    src: Person\n    dst: Person\n",
        )
        .unwrap();
        graph
            .adopt_ontology(crate::AdoptOntologyRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(210)),
                    actor_uuid: None,
                },
                path: ontology_path,
                mode: OntologyMode::Strict,
            })
            .unwrap();

        graph
            .publish_composite_transaction(graph_request(211, 212, "Ada"))
            .unwrap();
        assert_eq!(graph.ontology_mode(), OntologyMode::Strict);
    }
}
