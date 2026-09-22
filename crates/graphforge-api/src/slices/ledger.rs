//! Owner-derived dependency closure; references never become active membership.
use super::{
    GfError, GraphForge, Inclusion, Object, Selection, Uuid, checkpoint,
    graph::{Budget, Topology},
    invalid, limit, unavailable,
};
use crate::knowledge as ledger;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub(super) fn dependencies(
    view: &GraphForge,
    topology: &Topology,
    selection: &mut Selection,
    budget: &mut Budget<'_>,
    context_uuid: Uuid,
) -> Result<(), GfError> {
    let generation = view.generation_for_read()?;
    let has_knowledge = preflight(&generation, budget)?;
    let sources = if has_knowledge {
        ledger::read_source_ledger(&generation)?
    } else {
        graphforge_knowledge::SourceLedger::default()
    };
    let artifacts = if has_knowledge {
        ledger::read_artifact_ledger(&generation)?
    } else {
        graphforge_knowledge::ArtifactLedger::default()
    };
    let assertions = if has_knowledge {
        ledger::read_ledger(&generation)?
    } else {
        graphforge_knowledge::AssertionLedger::default()
    };
    let evidence = if has_knowledge {
        ledger::read_evidence_ledger(&generation)?
    } else {
        graphforge_knowledge::EvidenceLedger::default()
    };
    let derivations = if has_knowledge {
        ledger::read_derivation_ledger(&generation)?
    } else {
        graphforge_knowledge::ArtifactDerivationLedger::default()
    };
    let mut exists: BTreeSet<Object> = topology
        .nodes
        .keys()
        .map(|id| Object::new("node", *id))
        .chain(topology.edges.keys().map(|id| Object::new("edge", *id)))
        .collect();
    let mut adjacency: BTreeMap<Object, BTreeSet<Object>> = BTreeMap::new();
    for (id, edge) in &topology.edges {
        adjacency
            .entry(Object::new("edge", *id))
            .or_default()
            .extend([
                Object::new("node", edge.source),
                Object::new("node", edge.target),
            ]);
    }
    source_dependencies(sources, artifacts, &mut exists, &mut adjacency);
    if has_knowledge {
        for event in ledger::ledger::read_preference_ledger(&generation)?.events {
            checkpoint(budget.cancellation)?;
            let required = adjacency
                .entry(Object::new("source", event.source_uuid))
                .or_default();
            required.insert(Object::new("artifact", event.artifact_uuid));
            required.insert(Object::new("provenance", event.provenance_uuid));
            if let Some(prior) = event.prior_artifact_uuid {
                required.insert(Object::new("artifact", prior));
            }
        }
    }
    assertion_dependencies(
        assertions,
        evidence,
        derivations,
        &mut exists,
        &mut adjacency,
    );
    close_dependencies(&exists, &adjacency, selection, budget)?;
    // This is an explicitly typed source-context locator, never an ontology ID.
    // Historical views use their stable Version UUID, not private hydration identity.
    selection.required.insert(
        Object::new("ontology_context", context_uuid),
        Inclusion::direct(context_uuid, "frozen_ontology_participants"),
    );
    if selection.required.len() > budget.limits.dependencies as usize {
        return Err(limit());
    }
    Ok(())
}

fn preflight(
    generation: &graphforge_storage::ResolvedProjectGeneration,
    budget: &mut Budget<'_>,
) -> Result<bool, GfError> {
    let families = [
        "sources",
        "artifacts",
        "assertions",
        "assertion_graph_refs",
        "evidence",
        "artifact_derivations",
        "artifact_preference_events",
    ];
    let descriptors = generation.participant_descriptors()?;
    // Preflight trusted row counts and Parquet uncompressed sizes before the
    // domain decoders allocate collections or inflate compressed source content.
    for descriptor in descriptors.iter().filter(|d| {
        d.capability_id == "knowledge" && families.contains(&d.record_family_id.as_str())
    }) {
        checkpoint(budget.cancellation)?;
        let file = std::fs::File::open(
            generation.participant_path("knowledge", &descriptor.record_family_id)?,
        )
        .map_err(|e| GfError::Storage(e.to_string()))?;
        let file_bytes = file
            .metadata()
            .map_err(|e| GfError::Storage(e.to_string()))?
            .len();
        if file_bytes > budget.limits.working_bytes {
            return Err(limit());
        }
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|_| invalid("Slice dependency participant is invalid"))?;
        let mut bytes = file_bytes;
        for group in reader.metadata().row_groups() {
            bytes = bytes
                .checked_add(u64::try_from(group.total_byte_size()).map_err(|_| limit())?)
                .ok_or_else(limit)?;
        }
        budget.charge(
            descriptor.row_count,
            bytes
                .saturating_mul(4)
                .saturating_add(descriptor.row_count.saturating_mul(512)),
        )?;
    }

    Ok(descriptors.iter().any(|d| d.capability_id == "knowledge"))
}

fn source_dependencies(
    sources: graphforge_knowledge::SourceLedger,
    artifacts: graphforge_knowledge::ArtifactLedger,
    exists: &mut BTreeSet<Object>,
    adjacency: &mut BTreeMap<Object, BTreeSet<Object>>,
) {
    for source in sources.sources {
        let object = Object::new("source", source.source_uuid);
        exists.insert(object.clone());
        adjacency
            .entry(object)
            .or_default()
            .insert(Object::new("provenance", source.provenance_uuid));
    }
    for artifact in artifacts.artifacts {
        let object = Object::new("artifact", artifact.artifact_uuid);
        exists.insert(object.clone());
        adjacency.entry(object.clone()).or_default().extend([
            Object::new("source", artifact.source_uuid),
            Object::new("provenance", artifact.provenance_uuid),
        ]);
        if let Some(run) = artifact.run_uuid {
            adjacency
                .entry(object)
                .or_default()
                .insert(Object::new("algorithm_run", run));
        }
    }
}

fn assertion_dependencies(
    assertions: graphforge_knowledge::AssertionLedger,
    evidence: graphforge_knowledge::EvidenceLedger,
    derivations: graphforge_knowledge::ArtifactDerivationLedger,
    exists: &mut BTreeSet<Object>,
    adjacency: &mut BTreeMap<Object, BTreeSet<Object>>,
) {
    for assertion in assertions.assertions {
        let object = Object::new("assertion", assertion.assertion_uuid);
        exists.insert(object.clone());
        adjacency
            .entry(object)
            .or_default()
            .insert(Object::new("provenance", assertion.provenance_uuid));
    }
    for reference in assertions.graph_refs {
        adjacency
            .entry(Object::new("assertion", reference.assertion_uuid))
            .or_default()
            .insert(Object::new(
                reference.graph_kind.as_str(),
                reference.graph_uuid,
            ));
    }
    for link in evidence.links {
        let kind = match link.source_kind {
            graphforge_knowledge::EvidenceSourceKind::GraphNode => "node",
            graphforge_knowledge::EvidenceSourceKind::GraphEdge => "edge",
            other => other.as_str(),
        };
        adjacency
            .entry(Object::new("assertion", link.assertion_uuid))
            .or_default()
            .extend([
                Object::new("evidence_link", link.evidence_uuid),
                Object::new(kind, link.source_uuid),
                Object::new("provenance", link.provenance_uuid),
            ]);
    }
    for derivation in derivations.derivations {
        adjacency
            .entry(Object::new(
                derivation.output_kind.as_str(),
                derivation.output_uuid,
            ))
            .or_default()
            .insert(Object::new(
                derivation.input_kind.as_str(),
                derivation.input_uuid,
            ));
    }
}

fn close_dependencies(
    exists: &BTreeSet<Object>,
    adjacency: &BTreeMap<Object, BTreeSet<Object>>,
    selection: &mut Selection,
    budget: &mut Budget<'_>,
) -> Result<(), GfError> {
    budget.charge(
        0,
        (exists.len() as u64 + adjacency.values().map(|v| v.len() as u64).sum::<u64>())
            .saturating_mul(256),
    )?;
    for object in selection.active.keys() {
        if !exists.contains(object) {
            return Err(unavailable());
        }
    }
    let mut queue: VecDeque<_> = selection
        .active
        .keys()
        .map(|object| (object.clone(), object.uuid))
        .collect();
    let mut seen = BTreeSet::new();
    while let Some((object, root)) = queue.pop_front() {
        checkpoint(budget.cancellation)?;
        if !seen.insert(object.clone()) {
            continue;
        }
        for dependency in adjacency.get(&object).into_iter().flatten() {
            if matches!(
                dependency.kind.as_str(),
                "node" | "edge" | "source" | "artifact" | "assertion"
            ) && !exists.contains(dependency)
            {
                return Err(unavailable());
            }
            if !selection.active.contains_key(dependency) {
                selection
                    .required
                    .entry(dependency.clone())
                    .or_insert_with(|| Inclusion {
                        reason: "required_context".into(),
                        root,
                        predecessor: Some(object.uuid),
                        via_edge: None,
                        depth: 0,
                    });
                if selection.required.len() > budget.limits.dependencies as usize {
                    return Err(limit());
                }
            }
            if !seen.contains(dependency) {
                queue.push_back((dependency.clone(), root));
            }
        }
    }

    Ok(())
}
