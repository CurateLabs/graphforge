//! Actual Rust facade reads must consume authenticated checksums, never payload SHA-256.
use std::collections::HashMap;

use graphforge_api::{FindOptions, GraphForge, NodeSelector, PropValue, SearchIndexOptions};
use graphforge_storage::payload_digest::PayloadDigestCapture;

#[test]
fn durable_reopen_properties_uuid_and_delta_query_hash_no_payload_bytes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();
    let first = graph
        .add_node(
            "Person",
            &HashMap::from([("name".into(), PropValue::Str("Ada".into()))]),
        )
        .unwrap();
    let second = graph
        .add_node(
            "Person",
            &HashMap::from([("name".into(), PropValue::Str("Grace".into()))]),
        )
        .unwrap();
    graph
        .add_edge(&first, "KNOWS", &second, &HashMap::new())
        .unwrap();
    drop(graph);
    graphforge_storage::publish_graph_delta(
        root.path(),
        &graphforge_storage::GraphDeltaPublishRequest {
            transaction_uuid: uuid::Uuid::now_v7(),
            generation_uuid: uuid::Uuid::now_v7(),
            run_uuid: uuid::Uuid::now_v7(),
            operations: vec![graphforge_storage::GraphDeltaOp {
                operation_uuid: uuid::Uuid::now_v7(),
                kind: graphforge_storage::GraphDeltaOpKind::SetNodeProperty,
                payload: graphforge_storage::GraphDeltaPayload::SetNodeProperty {
                    node_uuid: first.uuid.to_string(),
                    property_stem: "Person".into(),
                    key: "name".into(),
                    value: graphforge_storage::encode_graph_delta_value(
                        &graphforge_ir::IrLiteral::Str("Ada Lovelace".into()),
                    )
                    .unwrap(),
                },
            }],
            limits: graphforge_storage::GraphDeltaJournalLimits::default(),
        },
    )
    .unwrap();
    let generation = graphforge_storage::resolve_project_generation(root.path()).unwrap();
    assert_eq!(
        graphforge_storage::list_delta_runs(
            &generation.graph_files_inventory().unwrap().unwrap(),
            graphforge_storage::GraphDeltaJournalLimits::default()
        )
        .unwrap()
        .len(),
        1
    );
    drop(generation);

    let capture = PayloadDigestCapture::start();
    let reopened = GraphForge::new(Some(path)).unwrap();
    assert_eq!(reopened.node_count("Person").unwrap(), 2);
    let before_query = capture.snapshot().checksum_bytes;
    let result = reopened
        .execute("MATCH (p:Person) RETURN p.name ORDER BY p.name")
        .unwrap();
    assert_eq!(
        result
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );
    let work = capture.snapshot();
    eprintln!("facade digest work: {work:?}");
    assert_eq!(work.artifact_payload_sha256_bytes, 0, "{work:?}");
    assert_eq!(work.unclassified_sha256_bytes, 0, "{work:?}");
    assert!(work.checksum_bytes > 0, "{work:?}");
    assert!(
        work.checksum_bytes > before_query,
        "property query worker must consume checksum bytes: {work:?}"
    );
    drop(capture);
    let repeated = PayloadDigestCapture::start();
    reopened
        .execute("MATCH (p:Person) RETURN p.name ORDER BY p.name")
        .unwrap();
    let work = repeated.snapshot();
    eprintln!("repeated query digest work: {work:?}");
    assert_eq!(work.artifact_payload_sha256_bytes, 0, "{work:?}");
    assert_eq!(work.unclassified_sha256_bytes, 0, "{work:?}");
    assert!(
        work.checksum_bytes > 0,
        "repeated plan must attach the current operation: {work:?}"
    );
}

#[test]
fn published_vector_find_and_property_shaping_hash_no_payload_bytes() {
    let graph = GraphForge::new(None).unwrap();
    let node = graph
        .add_node(
            "Paper",
            &HashMap::from([(
                "title".into(),
                PropValue::Str("Graph Neural Networks".into()),
            )]),
        )
        .unwrap();
    let vector = vec![1.0; 8];
    graph
        .index_search(
            "Paper",
            SearchIndexOptions::Vector {
                node: NodeSelector::Handle(node),
                vector: vector.clone(),
                space: "sbert".into(),
            },
        )
        .unwrap();

    let capture = PayloadDigestCapture::start();
    let found = graph
        .find(FindOptions {
            label: Some("Paper".into()),
            vector: Some(vector),
            space: Some("sbert".into()),
            limit: 10,
            ..FindOptions::default()
        })
        .unwrap();
    assert_eq!(found.num_rows(), 1);
    let work = capture.snapshot();
    eprintln!("facade digest work: {work:?}");
    assert_eq!(work.artifact_payload_sha256_bytes, 0, "{work:?}");
    assert_eq!(work.unclassified_sha256_bytes, 0, "{work:?}");
    assert!(work.checksum_bytes > 0, "{work:?}");
    assert_eq!(
        work.topology_projections, 1,
        "retrieval and shaping must share one projection: {work:?}"
    );
}

#[cfg(unix)]
#[test]
fn current_facade_refuses_same_inode_same_length_payload_mutation() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();
    graph.add_node("Person", &HashMap::new()).unwrap();
    drop(graph);
    let generation = graphforge_storage::resolve_project_generation(root.path()).unwrap();
    let inventory = generation.graph_files_inventory().unwrap().unwrap();
    let victim = inventory
        .files
        .iter()
        .find(|entry| {
            entry.role == graphforge_storage::GraphFileRole::Topology && entry.byte_length > 16
        })
        .unwrap();
    let object = generation.graph_tree_root().join(&victim.relative_path);
    let original = std::fs::metadata(&object).unwrap();
    let mut bytes = std::fs::read(&object).unwrap();
    bytes[8] ^= 1;
    std::fs::set_permissions(
        &object,
        std::fs::Permissions::from_mode(original.mode() | 0o200),
    )
    .unwrap();
    std::fs::write(&object, &bytes).unwrap();
    std::fs::set_permissions(&object, original.permissions()).unwrap();
    let changed = std::fs::metadata(&object).unwrap();
    assert_eq!(changed.ino(), original.ino());
    assert_eq!(changed.len(), original.len());
    let capture = PayloadDigestCapture::start();
    let error = GraphForge::new(Some(path)).unwrap_err();
    assert!(error.to_string().contains("GF_PROJECT_CORRUPT"), "{error}");
    let work = capture.snapshot();
    eprintln!("corruption refusal digest work: {work:?}");
    assert_eq!(work.artifact_payload_sha256_bytes, 0);
    assert_eq!(work.unclassified_sha256_bytes, 0);
}

#[test]
fn caller_embedding_reopen_and_find_hash_no_payload_bytes() {
    use graphforge_api::{
        CallerEmbeddingBatchRequest, CallerEmbeddingBatchRow, CallerEmbeddingDistance,
        CallerEmbeddingNormalization,
    };
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();
    let node = graph
        .add_node(
            "Paper",
            &HashMap::from([("title".into(), PropValue::Str("checksums".into()))]),
        )
        .unwrap();
    graph
        .publish_caller_embeddings(CallerEmbeddingBatchRequest {
            display_name: "semantic".into(),
            contract_version: "checksum-test-v1".into(),
            dimensions: 2,
            normalization: CallerEmbeddingNormalization::None,
            distance: CallerEmbeddingDistance::Cosine,
            source_projection_recipe: std::collections::BTreeMap::from([(
                "label".into(),
                "Paper".into(),
            )]),
            rows: vec![CallerEmbeddingBatchRow {
                node: NodeSelector::Handle(node),
                vector: vec![1.0, 0.0],
            }],
            replace_alias: false,
        })
        .unwrap();
    drop(graph);
    let capture = PayloadDigestCapture::start();
    let reopened = GraphForge::new(Some(path)).unwrap();
    assert_eq!(reopened.embedding_spaces().unwrap().len(), 1);
    let result = reopened
        .find(FindOptions {
            label: Some("Paper".into()),
            vector: Some(vec![1.0, 0.0]),
            space: Some("semantic".into()),
            ..FindOptions::default()
        })
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let work = capture.snapshot();
    eprintln!("facade digest work: {work:?}");
    assert_eq!(work.artifact_payload_sha256_bytes, 0, "{work:?}");
    assert_eq!(work.unclassified_sha256_bytes, 0, "{work:?}");
    assert!(work.checksum_bytes > 0, "{work:?}");
    assert_eq!(work.topology_projections, 1, "{work:?}");
}

#[test]
fn durable_knowledge_and_provenance_reads_use_participant_checksums() {
    use graphforge_api::{
        AssertionGraphRefInput, AssertionGraphRole, CreateAssertionRequest, GraphObjectKind,
        ListAssertionsRequest, OperationId, ProvenanceHistoryRequest, WriteContext,
    };
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();
    let node = graph.add_node("Person", &HashMap::new()).unwrap();
    enable_knowledge(&graph);
    graph
        .create_assertion(CreateAssertionRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid::Uuid::now_v7()),
                actor_uuid: None,
            },
            assertion_uuid: uuid::Uuid::now_v7(),
            claim: "Person exists".into(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid: node.uuid,
                graph_kind: GraphObjectKind::Node,
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        })
        .unwrap();
    drop(graph);
    let capture = PayloadDigestCapture::start();
    let reopened = GraphForge::new(Some(path)).unwrap();
    let assertions = reopened
        .list_assertions(ListAssertionsRequest::default())
        .unwrap();
    assert_eq!(
        assertions
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    let history = reopened
        .list_provenance_history(ProvenanceHistoryRequest::default())
        .unwrap();
    assert!(
        history
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>()
            > 0
    );
    let work = capture.snapshot();
    eprintln!("knowledge/provenance digest work: {work:?}");
    assert_eq!(work.artifact_payload_sha256_bytes, 0, "{work:?}");
    assert_eq!(work.unclassified_sha256_bytes, 0, "{work:?}");
    assert!(work.checksum_bytes > 0, "{work:?}");
}

#[cfg(unix)]
#[test]
fn knowledge_refuses_same_inode_same_length_participant_corruption() {
    use graphforge_api::{
        AssertionGraphRefInput, AssertionGraphRole, CreateAssertionRequest, GraphObjectKind,
        ListAssertionsRequest, OperationId, WriteContext,
    };
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(root.path().to_str().unwrap())).unwrap();
    let node = graph.add_node("Person", &HashMap::new()).unwrap();
    enable_knowledge(&graph);
    graph
        .create_assertion(CreateAssertionRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid::Uuid::now_v7()),
                actor_uuid: None,
            },
            assertion_uuid: uuid::Uuid::now_v7(),
            claim: "Person exists".into(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid: node.uuid,
                graph_kind: GraphObjectKind::Node,
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        })
        .unwrap();
    let generation = graphforge_storage::resolve_project_generation(root.path()).unwrap();
    let victim = generation
        .participant_path("knowledge", "assertions")
        .unwrap();
    let original = std::fs::metadata(&victim).unwrap();
    let mut bytes = std::fs::read(&victim).unwrap();
    bytes[8] ^= 1;
    std::fs::set_permissions(
        &victim,
        std::fs::Permissions::from_mode(original.mode() | 0o200),
    )
    .unwrap();
    std::fs::write(&victim, &bytes).unwrap();
    std::fs::set_permissions(&victim, original.permissions()).unwrap();
    let changed = std::fs::metadata(&victim).unwrap();
    assert_eq!(
        (original.ino(), original.len()),
        (changed.ino(), changed.len())
    );
    let capture = PayloadDigestCapture::start();
    let error = graph
        .list_assertions(ListAssertionsRequest::default())
        .unwrap_err();
    assert!(error.to_string().contains("checksum"), "{error}");
    let work = capture.snapshot();
    assert_eq!(work.artifact_payload_sha256_bytes, 0, "{work:?}");
    assert_eq!(work.unclassified_sha256_bytes, 0, "{work:?}");
}

fn enable_knowledge(graph: &GraphForge) {
    use graphforge_api::{CapabilityId, EnableCapabilityRequest, OperationId, WriteContext};
    for capability_id in [CapabilityId::Provenance, CapabilityId::Knowledge] {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid::Uuid::now_v7()),
                    actor_uuid: None,
                },
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }
}

#[test]
fn compact_cas_facade_reopen_and_query_hash_no_payload_bytes() {
    use graphforge_core::canonical::{CANONICAL_CONTRACT_VERSION, CanonicalDomain, fingerprint};
    use graphforge_storage::{
        ProjectGenerationRequest, ProjectParticipant, ProjectParticipantEncoding,
        ProjectStageOutcome,
    };
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();
    graph
        .add_node(
            "Person",
            &HashMap::from([("name".into(), PropValue::Str("Ada".into()))]),
        )
        .unwrap();
    drop(graph);
    let parent = graphforge_storage::resolve_project_generation(root.path()).unwrap();
    let inventory = parent.graph_files_inventory().unwrap().unwrap();
    assert_eq!(inventory.format_version, 7);
    let lease = graphforge_storage::begin_graph_object_publication(root.path()).unwrap();
    let (compact, _) =
        graphforge_storage::compact_graph_files(&lease, &parent.graph_tree_root(), &inventory)
            .unwrap();
    assert_eq!(compact.format_version, 8);
    let mut participants = parent
        .participant_snapshots()
        .unwrap()
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "graph" && snapshot.record_family_id == "files")
        })
        .map(|snapshot| ProjectParticipant {
            capability_id: snapshot.capability_id,
            capability_version: snapshot.capability_version,
            record_family_id: snapshot.record_family_id,
            record_version: snapshot.record_version,
            encoding: match snapshot.encoding.as_str() {
                "json" => ProjectParticipantEncoding::Json,
                "arrow" => ProjectParticipantEncoding::Arrow,
                "parquet" => ProjectParticipantEncoding::Parquet,
                other => panic!("unexpected participant encoding {other}"),
            },
            schema_fingerprint: snapshot.schema_fingerprint,
            row_count: snapshot.row_count,
            bytes: snapshot.bytes,
        })
        .collect::<Vec<_>>();
    participants.push(ProjectParticipant {
        capability_id: "graph".into(), capability_version: 1, record_family_id: "files".into(), record_version: 8,
        encoding: ProjectParticipantEncoding::Json,
        schema_fingerprint: fingerprint(CanonicalDomain::Schema, CANONICAL_CONTRACT_VERSION, b"graphforge-graph-files-root/8|root_node_sha256|logical_file_count|logical_byte_length|xxh64/1|semantic-routes/1").unwrap(),
        row_count: compact.logical_file_count, bytes: graphforge_storage::encode_graph_files_root_v2(&compact).unwrap(),
    });
    let request = ProjectGenerationRequest {
        transaction_uuid: uuid::Uuid::now_v7(),
        generation_uuid: uuid::Uuid::now_v7(),
        capabilities: parent
            .capabilities()
            .into_iter()
            .map(|capability| graphforge_storage::ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect(),
        participants,
    };
    let ProjectStageOutcome::Staged(staged) =
        graphforge_storage::stage_project_generation(root.path(), &request).unwrap()
    else {
        panic!("new compact publication must stage")
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish_with_graph_objects(&lease)
        .unwrap();
    drop(parent);
    drop(lease);
    let capture = PayloadDigestCapture::start();
    let reopened = GraphForge::new(Some(path)).unwrap();
    let result = reopened.execute("MATCH (p:Person) RETURN p.name").unwrap();
    assert_eq!(
        result
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    let work = capture.snapshot();
    eprintln!("compact CAS digest work: {work:?}");
    assert_eq!(work.artifact_payload_sha256_bytes, 0, "{work:?}");
    assert_eq!(work.unclassified_sha256_bytes, 0, "{work:?}");
    assert!(work.checksum_bytes > 0, "{work:?}");
}
