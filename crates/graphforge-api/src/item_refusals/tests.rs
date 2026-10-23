use super::*;
use crate::{
    CancellationToken, ModuleAdoptionRequest, ModuleDeleteRequest, ModuleSelector,
    OntologyAuthorityExpectation, OperationId, PrepareResearchVersionRequest, WriteContext,
};

const OLD: &str = "https://graphforge.sh/ont_aaaaaaaaaaaaaaaa";
const NEW: &str = "https://graphforge.sh/ont_bbbbbbbbbbbbbbbb";

fn authority(graph: &GraphForge) -> OntologyAuthorityExpectation {
    let state = graph.ontology_authority_state().unwrap();
    OntologyAuthorityExpectation {
        context: WriteContext {
            operation_uuid: OperationId(Uuid::now_v7()),
            actor_uuid: None,
        },
        expected_project_generation_uuid: state.project_generation_uuid,
        expected_composition_fingerprint: state.composition_fingerprint,
    }
}

fn adopt(graph: &mut GraphForge, id: &str) -> crate::OntologyModuleId {
    let document = serde_json::from_value(serde_json::json!({
        "ontology_id": id, "version": "1", "entity_types": [{"name": "Thing"}]
    }))
    .unwrap();
    let candidate = graph
        .create_ontology_module(document, Vec::new(), None)
        .unwrap();
    let id = candidate.id.clone();
    graph
        .adopt_ontology_module(
            &ModuleAdoptionRequest {
                authority: authority(graph),
                candidate,
            },
            None,
        )
        .unwrap();
    id
}

fn capture(graph: &mut GraphForge) -> Uuid {
    let version_uuid = Uuid::now_v7();
    let operation = graph
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid,
            context_uuid: Uuid::now_v7(),
            label: None,
            description: None,
            created_at: 1,
            required_versions: Default::default(),
            author: None,
            committer: None,
        })
        .unwrap();
    graph
        .commit_research_version_operation(operation, &CancellationToken::new())
        .unwrap();
    version_uuid
}

fn item(organization: bool) -> IdTakenItem {
    IdTakenItem {
        kind: ItemKind::OntologyModule,
        id: ItemId::parse_reference(OLD).unwrap().as_str().into(),
        owner: Some("organization".into()),
        publish_as_owner: organization.then_some(true),
    }
}

fn export_digest(graph: &GraphForge, output_path: std::path::PathBuf) -> String {
    // Match publication's Project package: research travels separately in
    // immutable Version packages, while every other participant is exported.
    let plan = graph
        .preview_portable_selection(&crate::PortableSelectionPreviewRequest {
            selection: crate::PortableSelection::Current,
            request: graphforge_core::portable::PortableSelectionRequest {
                profile: crate::PortableSelectionProfile::Complete,
                strict: true,
            },
            limits: crate::PortableLimits::default(),
        })
        .unwrap();
    let profile = crate::PortableSelectionProfile::Custom(
        plan.included
            .into_iter()
            .filter(|entry| entry.kind != "research")
            .map(|entry| entry.identity)
            .collect(),
    );
    graph
        .export_portable(
            &crate::PortableExportRequest {
                selection: crate::PortableSelection::Current,
                output_path,
                representation: crate::PortableOutput::Bundle,
                profile,
                subset: None,
                limits: crate::PortableLimits::default(),
            },
            None,
            |_| {},
        )
        .unwrap()
        .package_digest
}

#[test]
fn exact_refusal_receipt_survives_reopen_and_records_only_a_retained_replacement() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("project");
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    let old = adopt(&mut graph, OLD);
    let base = capture(&mut graph);
    assert!(graph.id_taken_receipt(OLD).unwrap().is_none());
    assert!(graph.record_item_id_regeneration(OLD, NEW, base).is_err());
    assert!(
        graph
            .id_taken_receipt("https://example.org/ontology")
            .is_err()
    );
    let generation = graph.committed_generation_identity().unwrap();
    let package = export_digest(&graph, root.path().join("before.gfpb"));
    graph.record_publish_id_taken(base, &[item(false)]).unwrap();
    assert_eq!(
        export_digest(&graph, root.path().join("after.gfpb")),
        package,
        "private refusal metadata is not exported"
    );
    assert_eq!(graph.committed_generation_identity().unwrap(), generation);
    let stored = graph.id_taken_receipt(OLD).unwrap().unwrap();
    assert_eq!(stored.item, item(false));
    assert_eq!(stored.refused_version_uuid, base);
    assert!(graph.id_taken_receipt(NEW).unwrap().is_none());
    assert!(graph.record_item_id_regeneration(NEW, OLD, base).is_err());
    assert!(
        graph
            .record_item_id_regeneration(OLD, "brg_bbbbbbbbbbbbbbbb", base)
            .is_err()
    );
    assert!(graph.record_item_id_regeneration(OLD, NEW, base).is_err());
    drop(graph);

    let mut graph = GraphForge::new(path.to_str()).unwrap();
    assert_eq!(graph.id_taken_receipt(OLD).unwrap(), Some(stored));
    graph
        .delete_ontology_module(
            &ModuleDeleteRequest {
                authority: authority(&graph),
                selector: ModuleSelector::Exact(old),
            },
            None,
        )
        .unwrap();
    adopt(&mut graph, NEW);
    let replacement = capture(&mut graph);
    assert!(
        graph.record_item_id_regeneration(OLD, NEW, base).is_err(),
        "an older Version does not contain the replacement"
    );
    assert!(
        graph
            .record_item_id_regeneration(OLD, NEW, Uuid::now_v7())
            .is_err()
    );
    let mapped = graph
        .record_item_id_regeneration(OLD, NEW, replacement)
        .unwrap();
    assert_eq!(
        mapped.regenerated.as_ref().unwrap().version_uuid,
        replacement
    );
    assert_eq!(
        mapped.regenerated.as_ref().unwrap().id,
        "ont_bbbbbbbbbbbbbbbb"
    );
    assert_eq!(
        graph
            .record_item_id_regeneration(OLD, NEW, replacement)
            .unwrap(),
        mapped
    );
    graph.record_publish_id_taken(base, &[item(true)]).unwrap();
    assert_eq!(
        graph.id_taken_receipt(OLD).unwrap(),
        Some(mapped.clone()),
        "later refusals retain original regeneration replay evidence"
    );
    drop(graph);
    let graph = GraphForge::new(path.to_str()).unwrap();
    assert_eq!(graph.id_taken_receipt(OLD).unwrap(), Some(mapped));
}

#[test]
fn organization_hint_refuses_regeneration_and_invalid_or_oversized_receipts_fail_closed() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("project");
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    let old = adopt(&mut graph, OLD);
    let base = capture(&mut graph);
    graph.record_publish_id_taken(base, &[item(true)]).unwrap();
    graph
        .delete_ontology_module(
            &ModuleDeleteRequest {
                authority: authority(&graph),
                selector: ModuleSelector::Exact(old),
            },
            None,
        )
        .unwrap();
    adopt(&mut graph, NEW);
    let replacement = capture(&mut graph);
    let error = graph
        .record_item_id_regeneration(OLD, NEW, replacement)
        .unwrap_err();
    assert!(error.to_string().contains("owning organization"));
    let receipt = graph.id_taken_receipt(OLD).unwrap().unwrap();
    assert!(receipt.regenerated.is_none());
    let mut invalid = item(false);
    invalid.publish_as_owner = Some(false);
    assert!(graph.record_publish_id_taken(base, &[invalid]).is_err());
    let control = path.join(DIRECTORY).join(DOCUMENT);
    std::fs::write(&control, b"{}").unwrap();
    assert!(graph.id_taken_receipt(OLD).is_err());
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&control)
        .unwrap();
    file.set_len(MAX_BYTES + 1).unwrap();
    assert!(graph.id_taken_receipt(OLD).is_err());
}
