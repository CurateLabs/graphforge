//! Deterministic portable-v2 graph-data-subset planning (#786).

use std::collections::BTreeSet;

use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::graph_projection::{
    GraphProjectionClosure, GraphProjectionSelection, materialize_portable_graph_tree_projection,
};
use crate::project_portable_v2_export::{
    PortableV2ExportLimits, PortableV2ExportPlan, plan_selected_portable_v2,
};
use crate::project_portable_v2_selection::{
    PortableV2ParticipantId, PortableV2SelectionEntry, PortableV2SelectionPlan,
    PortableV2SelectionReason, component_kind, fingerprint, validate_selection_plan,
};
use crate::{
    GRAPH_CAPABILITY_ID, GRAPH_FILES_FAMILY, PortableV2Error, PortableV2ErrorCode,
    PortableV2Limits, ResolvedProjectGeneration, capture_graph_files,
};

/// Stable UUID selector for one pinned generation.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct PortableV2GraphSelector {
    /// Ordered node UUIDs (hyphenated).
    pub node_uuids: Vec<String>,
    /// Ordered edge UUIDs (hyphenated).
    pub edge_uuids: Vec<String>,
}

/// Portable-v2 on-wire closure tokens.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PortableV2SubsetClosure {
    /// Selected nodes plus edges whose endpoints are both selected.
    InducedEdges,
    /// Selected edges plus both endpoint nodes.
    Referential,
}

/// Property projection/redaction for subset packages.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct PortableV2PropertyProjection {
    /// Property field names excluded from payloads.
    pub exclude: Vec<String>,
}

/// Subset planning request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortableV2SubsetRequest {
    /// Stable UUID selector.
    pub selector: PortableV2GraphSelector,
    /// Closure mode.
    pub closure: PortableV2SubsetClosure,
    /// Property projection.
    pub projection: PortableV2PropertyProjection,
}

/// Content-free graph-subset receipt retained in the semantic manifest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableV2GraphSubsetMeta {
    /// Canonical content-free selector digest token.
    pub selector: String,
    /// On-wire closure token.
    pub closure: String,
}

/// Immutable subset preview consumed by planning and export.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableV2SubsetPlan {
    /// Component selection consumed by the exporter.
    pub selection: PortableV2SelectionPlan,
    /// Graph-subset metadata emitted into the semantic manifest.
    pub graph_subset: PortableV2GraphSubsetMeta,
    /// Resolved node count after closure.
    pub selected_node_count: u64,
    /// Resolved edge count after closure.
    pub selected_edge_count: u64,
    /// Endpoint nodes added beyond the caller's explicit node set.
    pub endpoint_node_count: u64,
    /// Domain-separated projected graph fingerprint.
    pub result_fingerprint: String,
    /// Stable digest over the full subset preview.
    pub subset_fingerprint: String,
    /// Resolved projection used by the planner.
    #[serde(skip)]
    pub(crate) projection: GraphProjectionSelection,
}

/// Resolve one bounded deterministic graph-data subset without writing a package.
pub fn preview_portable_v2_graph_subset(
    generation: &ResolvedProjectGeneration,
    request: &PortableV2SubsetRequest,
    limits: PortableV2Limits,
) -> Result<PortableV2SubsetPlan, PortableV2Error> {
    let graph_root = generation.graph_tree_root();
    if !graph_root.exists() {
        return Err(incompatible("pinned generation has no graph tree"));
    }
    generation
        .graph_files_inventory()
        .map_err(storage)?
        .ok_or_else(|| incompatible("pinned generation has no graph inventory"))?;

    let selector = canonicalize_selector(&request.selector)?;
    if selector.node_uuids.is_empty() && selector.edge_uuids.is_empty() {
        return Err(incompatible("subset selector matched no graph identities"));
    }
    match request.closure {
        PortableV2SubsetClosure::InducedEdges if !selector.edge_uuids.is_empty() => {
            return Err(incompatible(
                "induced-edges closure rejects explicit edge selectors",
            ));
        }
        PortableV2SubsetClosure::Referential if selector.edge_uuids.is_empty() => {
            return Err(incompatible(
                "referential closure requires explicit edge selectors",
            ));
        }
        _ => {}
    }

    let exclude = request
        .projection
        .exclude
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if exclude.iter().any(|name| {
        matches!(
            name.as_str(),
            "node_uuid" | "edge_uuid" | "src_uuid" | "dst_uuid" | "node_id" | "edge_id"
        )
    }) {
        return Err(incompatible("topology identity columns cannot be redacted"));
    }

    let projection = GraphProjectionSelection {
        node_uuids: parse_uuids(&selector.node_uuids)?,
        edge_uuids: parse_uuids(&selector.edge_uuids)?,
        closure: match request.closure {
            PortableV2SubsetClosure::InducedEdges => GraphProjectionClosure::InducedEdges,
            PortableV2SubsetClosure::Referential => GraphProjectionClosure::Referential,
        },
        exclude_properties: exclude.clone(),
    };

    let staging = tempfile::tempdir().map_err(storage_io)?;
    let summary =
        materialize_portable_graph_tree_projection(&graph_root, staging.path(), &projection)
            .map_err(|error| map_projection(&error))?;
    let (captured, _) = capture_graph_files(staging.path()).map_err(storage)?;
    if captured.total_byte_length > limits.max_total_bytes {
        return Err(limit("subset bytes exceed configured limit"));
    }
    if captured.file_count > limits.max_entries {
        return Err(limit("subset entry count exceeds configured limit"));
    }

    let mut selection = build_subset_component_selection(generation, &exclude, limits)?;
    selection.selection_fingerprint = fingerprint(&selection)?;
    let graph_subset = PortableV2GraphSubsetMeta {
        selector: selector_digest(&selector, request.closure, &exclude)?,
        closure: match request.closure {
            PortableV2SubsetClosure::InducedEdges => "induced-edges".into(),
            PortableV2SubsetClosure::Referential => "referential".into(),
        },
    };
    let mut plan = PortableV2SubsetPlan {
        selection,
        graph_subset,
        selected_node_count: summary.node_uuids.len() as u64,
        selected_edge_count: summary.edge_uuids.len() as u64,
        endpoint_node_count: summary.endpoint_node_uuids.len() as u64,
        result_fingerprint: format!("sha256:{}", hex(summary.graph_content_fingerprint)),
        subset_fingerprint: String::new(),
        projection,
    };
    plan.subset_fingerprint = subset_fingerprint(&plan)?;
    Ok(plan)
}

/// Materialize one immutable subset preview into a representation-independent export plan.
pub fn plan_graph_subset_portable_v2(
    generation: &ResolvedProjectGeneration,
    plan: &PortableV2SubsetPlan,
    limits: PortableV2ExportLimits,
) -> Result<PortableV2ExportPlan, PortableV2Error> {
    if plan.selection.package_class != "graph-data-subset"
        || plan.subset_fingerprint != subset_fingerprint(plan)?
    {
        return Err(incompatible("subset plan identity mismatch"));
    }
    validate_selection_plan(generation, &plan.selection)?;

    let staging = tempfile::tempdir().map_err(storage_io)?;
    let summary = materialize_portable_graph_tree_projection(
        &generation.graph_tree_root(),
        staging.path(),
        &plan.projection,
    )
    .map_err(|error| map_projection(&error))?;
    if summary.node_uuids.len() as u64 != plan.selected_node_count
        || summary.edge_uuids.len() as u64 != plan.selected_edge_count
        || format!("sha256:{}", hex(summary.graph_content_fingerprint)) != plan.result_fingerprint
    {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::ConcurrentMutation,
            "subset source changed after preview",
        ));
    }
    let (inventory, inventory_participant) =
        capture_graph_files(staging.path()).map_err(storage)?;
    let mut selection = plan.selection.clone();
    selection.include_graph_tree = false;
    selection.selection_fingerprint = fingerprint(&selection)?;
    let mut export = plan_selected_portable_v2(generation, &selection, limits)?;
    export.replace_graph_tree_with_subset(
        staging,
        &inventory,
        inventory_participant.bytes,
        &plan.graph_subset.selector,
        &plan.graph_subset.closure,
        &plan.subset_fingerprint,
        limits,
    )?;
    Ok(export)
}

fn build_subset_component_selection(
    generation: &ResolvedProjectGeneration,
    redactions: &BTreeSet<String>,
    limits: PortableV2Limits,
) -> Result<PortableV2SelectionPlan, PortableV2Error> {
    let descriptors = generation.participant_descriptors().map_err(storage)?;
    if descriptors.len() as u64 > limits.max_components {
        return Err(limit("selection component count exceeds limit"));
    }
    let mut included = Vec::new();
    let mut excluded = Vec::new();
    let mut total = 0_u64;
    for descriptor in descriptors {
        let identity = PortableV2ParticipantId {
            capability_id: descriptor.capability_id.clone(),
            record_family_id: descriptor.record_family_id.clone(),
        };
        let kind = component_kind(&identity.capability_id, &identity.record_family_id);
        let selected = matches!(kind, "graph-data" | "schema" | "ontology");
        let path = generation
            .participant_path(&identity.capability_id, &identity.record_family_id)
            .map_err(storage)?;
        let bytes = std::fs::metadata(path).map_err(storage_io)?.len();
        if selected {
            total = total
                .checked_add(bytes)
                .ok_or_else(|| limit("selection byte overflow"))?;
            if total > limits.max_total_bytes {
                return Err(limit("selection bytes exceed limit"));
            }
        }
        let entry = PortableV2SelectionEntry {
            kind: kind.into(),
            reason: if selected {
                PortableV2SelectionReason::Requested
            } else {
                PortableV2SelectionReason::ProfileExcluded
            },
            identity,
            estimated_bytes: bytes,
            row_count: descriptor.row_count,
        };
        if selected {
            included.push(entry);
        } else {
            excluded.push(entry);
        }
    }
    if !included.iter().any(|entry| {
        entry.identity.capability_id == GRAPH_CAPABILITY_ID
            && entry.identity.record_family_id == GRAPH_FILES_FAMILY
    }) {
        return Err(incompatible("subset requires graph files inventory"));
    }
    included.sort_by(|a, b| a.identity.cmp(&b.identity));
    excluded.sort_by(|a, b| a.identity.cmp(&b.identity));
    let required_capabilities = included
        .iter()
        .map(|entry| entry.identity.capability_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    Ok(PortableV2SelectionPlan {
        source_generation_uuid: generation.generation_uuid().hyphenated().to_string(),
        source_manifest_sha256: format!("sha256:{}", hex(generation.manifest_sha256())),
        package_class: "graph-data-subset".into(),
        included,
        excluded,
        projected: Vec::new(),
        redactions: redactions.iter().cloned().collect(),
        required_capabilities,
        estimated_payload_bytes: total,
        selection_fingerprint: String::new(),
        include_graph_tree: true,
    })
}

fn subset_fingerprint(plan: &PortableV2SubsetPlan) -> Result<String, PortableV2Error> {
    #[derive(Serialize)]
    struct Signed<'a> {
        selection_fingerprint: &'a str,
        graph_subset: &'a PortableV2GraphSubsetMeta,
        selected_node_count: u64,
        selected_edge_count: u64,
        endpoint_node_count: u64,
        result_fingerprint: &'a str,
    }
    let signed = Signed {
        selection_fingerprint: &plan.selection.selection_fingerprint,
        graph_subset: &plan.graph_subset,
        selected_node_count: plan.selected_node_count,
        selected_edge_count: plan.selected_edge_count,
        endpoint_node_count: plan.endpoint_node_count,
        result_fingerprint: &plan.result_fingerprint,
    };
    let bytes = crate::project_portable_v2::canonical_json(
        &serde_json::to_value(signed).map_err(|_| incompatible("subset serialization"))?,
    )?;
    let mut digest = Sha256::new();
    digest.update(b"graphforge-portable-subset/1\0");
    digest.update(bytes);
    Ok(format!("sha256:{}", hex(digest.finalize().into())))
}

fn canonicalize_selector(
    selector: &PortableV2GraphSelector,
) -> Result<PortableV2GraphSelector, PortableV2Error> {
    let mut nodes = selector.node_uuids.clone();
    let mut edges = selector.edge_uuids.clone();
    nodes.sort();
    edges.sort();
    nodes.dedup();
    edges.dedup();
    if nodes.len() != selector.node_uuids.len() || edges.len() != selector.edge_uuids.len() {
        return Err(incompatible("duplicate subset selector identity"));
    }
    if nodes != selector.node_uuids || edges != selector.edge_uuids {
        return Err(incompatible("subset selector order is not deterministic"));
    }
    Ok(PortableV2GraphSelector {
        node_uuids: nodes,
        edge_uuids: edges,
    })
}

fn selector_digest(
    selector: &PortableV2GraphSelector,
    closure: PortableV2SubsetClosure,
    exclude: &BTreeSet<String>,
) -> Result<String, PortableV2Error> {
    #[derive(Serialize)]
    struct Body<'a> {
        closure: PortableV2SubsetClosure,
        node_uuids: &'a [String],
        edge_uuids: &'a [String],
        exclude_properties: Vec<&'a String>,
    }
    let body = Body {
        closure,
        node_uuids: &selector.node_uuids,
        edge_uuids: &selector.edge_uuids,
        exclude_properties: exclude.iter().collect(),
    };
    let bytes = crate::project_portable_v2::canonical_json(
        &serde_json::to_value(body).map_err(|_| incompatible("selector serialization"))?,
    )?;
    let mut digest = Sha256::new();
    digest.update(b"graphforge-portable-subset-selector/1\0");
    digest.update(bytes);
    Ok(format!("sha256:{}", hex(digest.finalize().into())))
}

fn parse_uuids(values: &[String]) -> Result<BTreeSet<[u8; 16]>, PortableV2Error> {
    let mut out = BTreeSet::new();
    for value in values {
        let uuid = Uuid::parse_str(value).map_err(|_| incompatible("invalid subset UUID"))?;
        if uuid.is_nil() {
            return Err(incompatible("nil UUID is not a portable identity"));
        }
        if !out.insert(*uuid.as_bytes()) {
            return Err(incompatible("duplicate subset selector identity"));
        }
    }
    Ok(out)
}

fn hex(digest: [u8; 32]) -> String {
    use std::fmt::Write as _;
    digest
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        })
}

fn map_projection(error: &crate::GfError) -> PortableV2Error {
    match error {
        crate::GfError::Validation(_) => incompatible("subset projection rejected"),
        _ => PortableV2Error::new(PortableV2ErrorCode::Io, "subset projection failed"),
    }
}
fn storage(_: crate::GfError) -> PortableV2Error {
    PortableV2Error::new(PortableV2ErrorCode::Io, "subset inventory unavailable")
}
fn storage_io(_: std::io::Error) -> PortableV2Error {
    PortableV2Error::new(PortableV2ErrorCode::Io, "subset staging unavailable")
}
fn incompatible(detail: &'static str) -> PortableV2Error {
    PortableV2Error::new(PortableV2ErrorCode::Incompatible, detail)
}
fn limit(_: &str) -> PortableV2Error {
    PortableV2Error::new(PortableV2ErrorCode::LimitExceeded, "subset limit exceeded")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::sync::atomic::AtomicBool;

    use graphforge_core::{OntologyMode, TypeId};
    use graphforge_ir::IrLiteral;
    use uuid::Uuid;

    use super::*;
    use crate::project_portable_v2_export::{PortableV2Output, export_complete_portable_v2};
    use crate::{
        GRAPH_CAPABILITY_VERSION, GraphWriter, PortableV2Mode, ProjectCapability,
        ProjectGenerationRequest, ProjectStageOutcome, capture_graph_files,
        empty_workspace_participants, open_or_initialize_project, resolve_project_generation,
        stage_project_generation_with_graph_tree, verify_portable_v2,
    };

    const TS: i64 = 1_700_000_000_000_000;

    fn uuid(marker: u8) -> Uuid {
        let mut bytes = [0_u8; 16];
        bytes[15] = marker;
        Uuid::from_bytes(bytes)
    }

    fn publish_graph_project() -> (tempfile::TempDir, [Uuid; 3], [Uuid; 2]) {
        let root = tempfile::tempdir().unwrap();
        open_or_initialize_project(root.path()).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let nodes = [uuid(1), uuid(2), uuid(3)];
        let edges = [uuid(11), uuid(12)];
        let mut writer =
            GraphWriter::open_at(workspace.path(), OntologyMode::Exploratory, TS).unwrap();
        for (index, node) in nodes.iter().enumerate() {
            writer.create_node(*node, TypeId(1)).unwrap();
            writer
                .set_properties(
                    node,
                    None,
                    HashMap::from([
                        ("value".into(), IrLiteral::Int(index as i64)),
                        ("secret".into(), IrLiteral::Str(format!("leak-{index}"))),
                    ]),
                )
                .unwrap();
        }
        writer
            .create_edge(edges[0], "KNOWS", &nodes[0], &nodes[1])
            .unwrap();
        writer
            .create_edge(edges[1], "KNOWS", &nodes[1], &nodes[2])
            .unwrap();
        writer.flush().unwrap();
        let (_, files) = capture_graph_files(workspace.path()).unwrap();
        let mut participants = empty_workspace_participants().unwrap();
        participants.insert(0, files);
        let document = graphforge_ontology::OntologyDoc {
            ontology_id: "https://graphforge.dev/ontology/subset".into(),
            version: "v1".into(),
            entity_types: vec![],
            relation_types: vec![],
            properties: vec![],
            constraints: vec![],
            migrations: vec![],
        };
        let legacy = crate::WorkspaceOntology {
            contract_version: 1,
            mode: crate::WorkspaceOntologyMode::Strict,
            source_format: Some(crate::WorkspaceOntologySourceFormat::Json),
            canonical_ontology_sha256: Some("b".repeat(64)),
            canonical_ontology: Some(serde_json::to_value(document).unwrap()),
        };
        let composition = crate::WorkspaceOntologyComposition::virtual_legacy(&legacy)
            .unwrap()
            .unwrap();
        participants.push(composition.to_project_participant().unwrap());
        participants.sort_by(|left, right| {
            (&left.capability_id, &left.record_family_id)
                .cmp(&(&right.capability_id, &right.record_family_id))
        });
        let generation_uuid = Uuid::now_v7();
        let request = ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid,
            capabilities: vec![
                ProjectCapability {
                    capability_id: GRAPH_CAPABILITY_ID.into(),
                    capability_version: GRAPH_CAPABILITY_VERSION,
                },
                ProjectCapability {
                    capability_id: "workspace".into(),
                    capability_version: 1,
                },
            ],
            participants,
        };
        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation_with_graph_tree(root.path(), &request, Some(workspace.path()))
                .unwrap()
        else {
            panic!("expected staged publication");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        (root, nodes, edges)
    }

    #[test]
    fn induced_subset_export_is_deterministic_and_verifies() {
        let (root, nodes, _edges) = publish_graph_project();
        let generation = resolve_project_generation(root.path()).unwrap();
        let limits = PortableV2Limits::default();
        let mut selected = [nodes[0], nodes[1]].map(|uuid| uuid.hyphenated().to_string());
        selected.sort();
        let request = PortableV2SubsetRequest {
            selector: PortableV2GraphSelector {
                node_uuids: selected.to_vec(),
                edge_uuids: vec![],
            },
            closure: PortableV2SubsetClosure::InducedEdges,
            projection: PortableV2PropertyProjection {
                exclude: vec!["secret".into()],
            },
        };
        let preview = preview_portable_v2_graph_subset(&generation, &request, limits).unwrap();
        assert_eq!(preview.selected_node_count, 2);
        assert_eq!(preview.selected_edge_count, 1);
        assert_eq!(preview.graph_subset.closure, "induced-edges");
        assert!(preview.selection.redactions.contains(&"secret".into()));
        let again = preview_portable_v2_graph_subset(&generation, &request, limits).unwrap();
        assert_eq!(preview.subset_fingerprint, again.subset_fingerprint);
        assert_eq!(preview.result_fingerprint, again.result_fingerprint);

        let plan = plan_graph_subset_portable_v2(&generation, &preview, limits).unwrap();
        let out = tempfile::tempdir().unwrap();
        let expanded = out.path().join("subset.gfproject");
        let bundle = out.path().join("subset.gfpb");
        let cancelled = AtomicBool::new(false);
        let a = export_complete_portable_v2(
            &plan,
            &expanded,
            PortableV2Output::Expanded,
            limits,
            &cancelled,
            |_| {},
        )
        .unwrap();
        let b = export_complete_portable_v2(
            &plan,
            &bundle,
            PortableV2Output::Bundle,
            limits,
            &cancelled,
            |_| {},
        )
        .unwrap();
        assert_eq!(a.package_digest, b.package_digest);
        assert_eq!(a.selection_fingerprint, preview.subset_fingerprint);
        let report =
            verify_portable_v2(&expanded, PortableV2Mode::Full, limits, Some(&cancelled)).unwrap();
        assert!(report.ontology_composition.is_some());
        assert_eq!(
            report.package_class,
            crate::PortableV2PackageClass::GraphDataSubset
        );
        let manifest = fs::read_to_string(expanded.join("data/graphforge-project.json")).unwrap();
        assert!(manifest.contains("\"graph-data-subset\""));
        assert!(manifest.contains("\"induced-edges\""));
        assert!(!manifest.contains("leak-"));
    }

    #[test]
    fn referential_subset_closes_endpoints_and_rejects_unordered_selectors() {
        let (root, nodes, edges) = publish_graph_project();
        let generation = resolve_project_generation(root.path()).unwrap();
        let limits = PortableV2Limits::default();
        let edge = edges[0].hyphenated().to_string();
        let preview = preview_portable_v2_graph_subset(
            &generation,
            &PortableV2SubsetRequest {
                selector: PortableV2GraphSelector {
                    node_uuids: vec![],
                    edge_uuids: vec![edge.clone()],
                },
                closure: PortableV2SubsetClosure::Referential,
                projection: PortableV2PropertyProjection::default(),
            },
            limits,
        )
        .unwrap();
        assert_eq!(preview.selected_edge_count, 1);
        assert_eq!(preview.selected_node_count, 2);
        assert_eq!(preview.endpoint_node_count, 2);

        let unordered = PortableV2SubsetRequest {
            selector: PortableV2GraphSelector {
                node_uuids: vec![
                    nodes[1].hyphenated().to_string(),
                    nodes[0].hyphenated().to_string(),
                ],
                edge_uuids: vec![],
            },
            closure: PortableV2SubsetClosure::InducedEdges,
            projection: PortableV2PropertyProjection::default(),
        };
        let error = preview_portable_v2_graph_subset(&generation, &unordered, limits).unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Incompatible);
    }
}
