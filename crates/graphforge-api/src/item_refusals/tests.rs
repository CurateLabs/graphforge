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
    // Count pressure cannot discard the retained old-ID/replacement evidence.
    graph
        .record_publish_id_taken(base, &batch(0, MAX_ID_TAKEN_ITEMS - 1))
        .unwrap();
    let current = batch(MAX_ID_TAKEN_ITEMS, 2);
    graph.record_publish_id_taken(base, &current).unwrap();
    assert_eq!(graph.id_taken_receipt(OLD).unwrap(), Some(mapped.clone()));
    for item in current {
        assert_eq!(
            graph.id_taken_receipt(&item.id).unwrap().unwrap().item,
            item
        );
    }
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
    let mut invalid = item(false);
    invalid.owner = Some("o".repeat(101));
    assert_ne!(
        graph
            .record_publish_id_taken(base, &[invalid])
            .unwrap_err()
            .code(),
        "GF_RESOURCE_LIMIT"
    );
    let control = path.join(DIRECTORY).join(DOCUMENT);
    std::fs::write(&control, b"{}").unwrap();
    assert_eq!(graph.id_taken_receipt(OLD).unwrap_err().code(), "GF_IO");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&control)
        .unwrap();
    file.set_len(MAX_BYTES + 1).unwrap();
    assert_eq!(
        graph.id_taken_receipt(OLD).unwrap_err().code(),
        "GF_VALIDATION"
    );
}

fn batch(start: usize, count: usize) -> Vec<IdTakenItem> {
    (start..start + count)
        .map(|index| IdTakenItem {
            kind: ItemKind::OntologyModule,
            id: format!("ont_{index:016x}"),
            owner: None,
            publish_as_owner: None,
        })
        .collect()
}

#[test]
fn consecutive_refusals_evict_stale_receipts_and_preserve_the_current_batch_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("project");
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    let version = capture(&mut graph);
    let first = batch(0, MAX_ID_TAKEN_ITEMS);
    graph.record_publish_id_taken(version, &first).unwrap();
    let mut current = vec![first[0].clone()];
    current.extend(batch(MAX_ID_TAKEN_ITEMS, 2));
    let recorded = graph.record_publish_id_taken(version, &current).unwrap();
    assert_eq!(recorded.len(), current.len());
    for (receipt, item) in recorded.iter().zip(&current) {
        assert_eq!(&receipt.item, item);
    }
    drop(graph);
    let graph = GraphForge::new(path.to_str()).unwrap();
    assert!(graph.id_taken_receipt(&first[1].id).unwrap().is_none());
    assert!(graph.id_taken_receipt(&first[2].id).unwrap().is_none());
    assert!(graph.id_taken_receipt(&first[3].id).unwrap().is_some());
    assert!(
        graph
            .record_item_id_regeneration(&first[1].id, NEW, version)
            .unwrap_err()
            .to_string()
            .contains("exact ID")
    );
    for receipt in recorded {
        assert_eq!(
            graph.id_taken_receipt(&receipt.item.id).unwrap(),
            Some(receipt)
        );
    }
    let store = Store::open(&graph, false).unwrap().unwrap();
    assert_eq!(store.read().unwrap().receipts.len(), MAX_ID_TAKEN_ITEMS);
}

fn receipt(item: IdTakenItem, version: Uuid, protected: bool) -> ItemIdRefusalReceipt {
    ItemIdRefusalReceipt {
        item,
        refused_version_uuid: version,
        regenerated: protected.then(|| ItemIdRegeneration {
            id: "ont_bbbbbbbbbbbbbbbb".into(),
            version_uuid: version,
        }),
    }
}

#[test]
fn protected_capacity_exhaustion_keeps_the_exact_previous_document_and_no_partial_batch() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("project");
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    let version = capture(&mut graph);
    // Seed a full, structurally valid protected store without manufacturing
    // thousands of unrelated ontology mutations just to exercise retention.
    let mut document = Document {
        format: FORMAT.into(),
        regenerations: BTreeMap::new(),
        receipts: batch(0, MAX_ID_TAKEN_ITEMS)
            .into_iter()
            .enumerate()
            .map(|(index, item)| {
                (
                    item.id.clone(),
                    receipt(item, version, index != MAX_ID_TAKEN_ITEMS - 1),
                )
            })
            .collect(),
    };
    let store = Store::open(&graph, true).unwrap().unwrap();
    store.write(&mut document, &BTreeSet::new()).unwrap();
    drop(store);
    let control = path.join(DIRECTORY).join(DOCUMENT);
    let before = std::fs::read(&control).unwrap();
    let current = batch(MAX_ID_TAKEN_ITEMS, 2);
    let error = graph
        .record_publish_id_taken(version, &current)
        .unwrap_err();
    assert!(matches!(
        error,
        GfError::Api {
            code: graphforge_core::ApiErrorCode::ResourceLimit,
            ..
        }
    ));
    assert_eq!(std::fs::read(&control).unwrap(), before);
    drop(graph);
    let graph = GraphForge::new(path.to_str()).unwrap();
    for item in current {
        assert!(graph.id_taken_receipt(&item.id).unwrap().is_none());
    }
    let store = Store::open(&graph, false).unwrap().unwrap();
    assert_eq!(store.read().unwrap().receipts, document.receipts);
}

#[test]
fn defensive_byte_retention_preserves_current_and_protected_entries_and_exact_sizes() {
    let version = Uuid::now_v7();
    let entries = batch(0, 3);
    let mut document = Document {
        format: FORMAT.into(),
        regenerations: BTreeMap::new(),
        receipts: entries
            .iter()
            .enumerate()
            .map(|(index, item)| (item.id.clone(), receipt(item.clone(), version, index == 0)))
            .collect(),
    };
    let current = BTreeSet::from([entries[2].id.as_str()]);
    let mut expected = document.receipts.clone();
    expected.remove(&entries[1].id);
    // Valid current-schema stores cannot hit 8 MiB before the count limit.
    // Derive a smaller private test budget from genuine serialized receipts
    // to exercise the defensive byte accounting, including JSON separators.
    let expected_bytes = serde_json::to_vec(&Document {
        format: FORMAT.into(),
        regenerations: BTreeMap::new(),
        receipts: expected.clone(),
    })
    .unwrap();
    let bytes = retain_receipts(&mut document, &current, 3, expected_bytes.len()).unwrap();
    assert_eq!(bytes, expected_bytes);
    assert_eq!(document.receipts, expected);
    let error = retain_receipts(&mut document, &current, 3, bytes.len() - 1).unwrap_err();
    assert_eq!(error.code(), "GF_RESOURCE_LIMIT");
    assert_eq!(document.receipts, expected);
}

#[test]
fn pending_regeneration_authorization_is_not_evictable() {
    let version = Uuid::now_v7();
    let old = item(false);
    let disposable = batch(0, 1).remove(0);
    let binding = RegenerationBinding {
        operation_uuid: Uuid::now_v7(),
        old_id: old.id.clone(),
        refusal: receipt(old.clone(), version, false),
        replacement: ItemIdRegeneration {
            id: ItemId::parse_reference(NEW).unwrap().as_str().into(),
            version_uuid: Uuid::now_v7(),
        },
        source_generation: Uuid::now_v7(),
        branch_uuid: Uuid::now_v7(),
        source_version: version,
        created_at: 1,
        request_sha256: None,
    };
    let mut document = Document {
        format: FORMAT.into(),
        receipts: BTreeMap::from([
            (old.id.clone(), receipt(old.clone(), version, false)),
            (
                disposable.id.clone(),
                receipt(disposable.clone(), version, false),
            ),
        ]),
        regenerations: BTreeMap::from([(binding.operation_uuid, binding)]),
    };
    retain_receipts(&mut document, &BTreeSet::new(), 1, MAX_BYTES as usize).unwrap();
    assert_eq!(document.receipts.len(), 1);
    assert!(document.receipts.contains_key(&old.id));
    assert!(!document.receipts.contains_key(&disposable.id));
}
