//! Atomic publication of a composite graph + M20/M21 transaction.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;

use arrow::array::{Array, FixedSizeBinaryArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use gf_core::{GfError, OntologyMode, ProjectErrorCode, TypeId};
use gf_ir::{IrLiteral, RuntimeCatalog};
use gf_knowledge::{
    AssertionLedger, AssertionStatusLedger, AssertionSupersessionLedger, AssertionValidityLedger,
    ConfidenceLedger, EvidenceLedger, HypothesisLedger, ReasoningLedger,
};
use gf_provenance::ProvenanceLedger;
use gf_storage::{
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
        let _visibility = crate::knowledge::lock_graph_visibility(self);
        let root = self.resolved_generation.container_root();
        let parent = gf_storage::resolve_project_generation(root)?;
        parent.validate_complete_participant_inventory()?;
        let expected_parent = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if parent.generation_uuid() != expected_parent {
            return Err(GfError::Project {
                code: ProjectErrorCode::TransactionConflict,
                message: "project generation changed before composite publication".into(),
            });
        }

        let snapshot = build_validation_snapshot(self, &parent)?;
        let content_fingerprint = request.canonical_fingerprint()?;
        let generation_uuid =
            composite_generation_uuid(request.context.operation_uuid.0, content_fingerprint);
        let transaction_uuid = request.context.operation_uuid.0;

        if let Some(published) = gf_storage::published_project_transaction(root, transaction_uuid)?
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

        let receipt = authorize_composite_transaction(&request, &snapshot, None)?;
        require_capabilities(&parent, &request)?;

        let prior_snapshot = crate::graph_snapshot::capture(&self.dir)?;
        let prior_catalog = self
            .runtime_catalog
            .lock()
            .expect("runtime catalog poisoned")
            .clone();
        let mut next_catalog = prior_catalog.clone();
        let recorded_at = (self.clock.lock().expect("clock lock poisoned"))()?;

        let publication = (|| -> Result<RecordBatch, GfError> {
            apply_graph_mutations(self, &request, &mut next_catalog, recorded_at)?;
            if self.path.is_some() {
                crate::persist_runtime_catalog(&self.dir, &next_catalog)?;
            }
            let graph = crate::graph_snapshot::capture(&self.dir)?;
            let participants = assemble_composite_participants(self, &parent, &request, graph)?;
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
            let outcome = match gf_storage::stage_project_generation(root, &publication)? {
                ProjectStageOutcome::AlreadyPublished(published) => published,
                ProjectStageOutcome::Staged(staged) => staged
                    .validate(
                        |_| Ok(()),
                        |actual_parent, _| {
                            if actual_parent.generation_uuid() != expected_parent {
                                return Err(GfError::Project {
                                    code: ProjectErrorCode::TransactionConflict,
                                    message:
                                        "project generation changed before composite publication"
                                            .into(),
                                });
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
            *self
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned") = outcome.generation_uuid;
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
                let still_prior = *self
                    .current_generation_uuid
                    .lock()
                    .expect("generation UUID lock poisoned")
                    == expected_parent;
                if still_prior {
                    crate::graph_snapshot::restore(&prior_snapshot.bytes, &self.dir)?;
                    *self
                        .runtime_catalog
                        .lock()
                        .expect("runtime catalog poisoned") = prior_catalog;
                } else {
                    *self
                        .runtime_catalog
                        .lock()
                        .expect("runtime catalog poisoned") = next_catalog;
                    self.adjacency_provider.invalidate();
                }
                Err(error)
            }
        }
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
        parent.require_capability("epistemic", gf_knowledge::EPISTEMIC_CAPABILITY_VERSION)?;
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
    for batch in gf_storage::read_nodes(&graph.dir)
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
    for batch in gf_storage::read_edges(&graph.dir, "*", graph.ontology_mode)
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
    writer: &mut gf_storage::GraphWriter,
    dir: &Path,
    endpoints: &BTreeSet<Uuid>,
) -> Result<(), GfError> {
    if endpoints.is_empty() {
        return Ok(());
    }
    let mut unresolved = endpoints.clone();
    for batch in gf_storage::read_nodes(dir)
        .map_err(|error| GfError::Storage(format!("failed to read node topology: {error}")))?
    {
        let uuids = batch
            .column_by_name("node_uuid")
            .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or_else(|| GfError::Storage("node topology has malformed UUID column".into()))?;
        let ids = batch
            .column_by_name("node_id")
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| GfError::Storage("node topology has malformed ID column".into()))?;
        for row in 0..batch.num_rows() {
            let uuid = Uuid::from_slice(uuids.value(row))
                .map_err(|error| GfError::Storage(error.to_string()))?;
            if unresolved.remove(&uuid) {
                writer.register_existing_node(uuid, ids.value(row));
            }
        }
    }
    // Remaining endpoints must be created in this same request.
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn apply_graph_mutations(
    graph: &GraphForge,
    request: &CompositeTransactionRequest,
    catalog: &mut RuntimeCatalog,
    recorded_at: i64,
) -> Result<(), GfError> {
    if request.graph_mutations.is_empty() {
        return Ok(());
    }
    let mut writer =
        gf_storage::GraphWriter::open_at(&graph.dir, graph.ontology_mode, recorded_at)?;
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
    register_existing_endpoints(&mut writer, &graph.dir, &endpoints)?;
    let mut created_nodes = HashSet::new();
    let mut created_edges = HashSet::new();
    let mut node_sets: HashMap<String, HashMap<[u8; 16], HashMap<String, IrLiteral>>> =
        HashMap::new();
    let mut edge_sets: HashMap<String, HashMap<[u8; 16], HashMap<String, IrLiteral>>> =
        HashMap::new();
    let mut node_removes: HashMap<String, HashMap<[u8; 16], HashSet<String>>> = HashMap::new();
    let mut edge_removes: HashMap<String, HashMap<[u8; 16], HashSet<String>>> = HashMap::new();
    let mut delete_nodes = HashSet::new();
    let mut delete_edges = HashSet::new();

    for mutation in &request.graph_mutations {
        match mutation {
            CompositeGraphMutation::CreateNode {
                node_uuid,
                label,
                properties,
            } => {
                let type_id = graph
                    .ontology
                    .as_ref()
                    .and_then(|ontology| ontology.entity_type_id(label))
                    .unwrap_or_else(|| TypeId(catalog.intern_label(label).0));
                writer.create_node(*node_uuid, type_id)?;
                created_nodes.insert(*node_uuid);
                if !properties.is_empty() {
                    let props = properties
                        .iter()
                        .map(|(name, value)| {
                            catalog.intern_property(name, Some(label));
                            Ok((name.clone(), prop_literal(value)?))
                        })
                        .collect::<Result<HashMap<_, _>, GfError>>()?;
                    writer.set_properties(node_uuid, Some(label), props)?;
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
                created_edges.insert(*edge_uuid);
                if !properties.is_empty() {
                    let props = properties
                        .iter()
                        .map(|(name, value)| {
                            catalog.intern_property(name, Some(rel_type));
                            Ok((name.clone(), prop_literal(value)?))
                        })
                        .collect::<Result<HashMap<_, _>, GfError>>()?;
                    writer.set_edge_properties(edge_uuid, Some(rel_type), props)?;
                }
            }
            CompositeGraphMutation::SetNodeProperty {
                node_uuid,
                property,
                value,
            } => {
                catalog.intern_property(property, None);
                let literal = prop_literal(value)?;
                if created_nodes.contains(node_uuid) {
                    writer.set_properties(
                        node_uuid,
                        None,
                        HashMap::from([(property.clone(), literal)]),
                    )?;
                } else {
                    node_sets
                        .entry("_untyped".into())
                        .or_default()
                        .entry(node_uuid.into_bytes())
                        .or_default()
                        .insert(property.clone(), literal);
                }
            }
            CompositeGraphMutation::SetEdgeProperty {
                edge_uuid,
                property,
                value,
            } => {
                catalog.intern_property(property, None);
                let literal = prop_literal(value)?;
                if created_edges.contains(edge_uuid) {
                    writer.set_edge_properties(
                        edge_uuid,
                        None,
                        HashMap::from([(property.clone(), literal)]),
                    )?;
                } else {
                    edge_sets
                        .entry("_untyped".into())
                        .or_default()
                        .entry(edge_uuid.into_bytes())
                        .or_default()
                        .insert(property.clone(), literal);
                }
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
    for (stem, updates) in &node_sets {
        gf_storage::stage_set_node_properties(&mut staged, &graph.dir, stem, updates)?;
    }
    for (stem, updates) in &edge_sets {
        gf_storage::stage_set_edge_properties(&mut staged, &graph.dir, stem, updates)?;
    }
    for (stem, removals) in &node_removes {
        gf_storage::stage_remove_node_properties(&mut staged, &graph.dir, stem, removals)?;
    }
    for (stem, removals) in &edge_removes {
        gf_storage::stage_remove_edge_properties(&mut staged, &graph.dir, stem, removals)?;
    }
    gf_storage::stage_delete_edges(&mut staged, &graph.dir, &delete_edges)?;
    gf_storage::stage_delete_nodes(&mut staged, &graph.dir, &delete_nodes)?;
    writer.flush_into(&mut staged)?;
    gf_storage::commit_topology_aware(staged, &graph.dir)?;
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
            )) || snapshot.capability_id == "graph" && snapshot.record_family_id == "snapshot")
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
    use crate::{CapabilityId, EnableCapabilityRequest, OperationId, PropValue, WriteContext};
    use gf_knowledge::{
        Assertion, AssertionGraphRef, AssertionGraphRole, AssertionStatus, AssertionStatusEvent,
        GraphObjectKind,
    };
    use gf_provenance::{EventKind, LineageRecord, LineageRole, ProvenanceEvent, SubjectKind};
    use std::collections::HashMap;
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
}
