//! Publication hashes each newly published artifact payload at most once (#1691).
//!
//! Every assertion bounds actual SHA-256 producer input, captured per operation,
//! by the bytes of inodes the operation newly created under the project root.
//! Re-verifying unchanged or already-captured payload exceeds that bound.
#![cfg(unix)]

use std::collections::HashMap;
use std::os::unix::fs::MetadataExt as _;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{FixedSizeBinaryBuilder, StringArray};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    BulkInputKind, GraphForge, ImportSessionLimits, OperationId, PropValue, bulk_edge_input_schema,
    bulk_node_input_schema,
};
use graphforge_storage::payload_digest::{PayloadDigestCapture, PayloadDigestSnapshot};

const NODES: u128 = 2_000;

type Inodes = HashMap<(u64, u64), u64>;

fn inodes(root: &Path) -> Inodes {
    fn visit(current: &Path, found: &mut Inodes) {
        for entry in std::fs::read_dir(current).unwrap() {
            let entry = entry.unwrap();
            let metadata = std::fs::symlink_metadata(entry.path()).unwrap();
            if metadata.is_dir() {
                visit(&entry.path(), found);
            } else if metadata.is_file() {
                found.insert((metadata.dev(), metadata.ino()), metadata.len());
            }
        }
    }
    let mut found = Inodes::new();
    visit(root, &mut found);
    found
}

/// Bytes of regular-file inodes that exist now but did not exist before.
fn newly_published_bytes(root: &Path, before: &Inodes) -> u64 {
    inodes(root)
        .into_iter()
        .filter(|(inode, _)| !before.contains_key(inode))
        .map(|(_, length)| length)
        .sum()
}

fn assert_bounded(label: &str, work: PayloadDigestSnapshot, published: u64) {
    eprintln!("{label}: published={published} {work:?}");
    assert!(
        work.artifact_payload_sha256_bytes <= published,
        "{label}: artifact SHA {} exceeds newly published bytes {published}: {work:?}",
        work.artifact_payload_sha256_bytes
    );
    assert_eq!(work.unclassified_sha256_bytes, 0, "{label}: {work:?}");
}

/// Deterministic UUIDv7: a fixed timestamp, version 7, RFC variant, and a counter.
fn v7(counter: u128) -> uuid::Uuid {
    uuid::Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_0000 | counter)
}

fn uuid_column(values: impl Iterator<Item = u128>) -> arrow::array::ArrayRef {
    let values = values.collect::<Vec<_>>();
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
    for value in values {
        builder.append_value(v7(value).as_bytes()).unwrap();
    }
    Arc::new(builder.finish())
}

fn bulk_load(graph: &GraphForge, nodes: u128, offset: u128) {
    let node_batch = RecordBatch::try_new(
        bulk_node_input_schema(Vec::new()).unwrap(),
        vec![
            uuid_column(offset..offset + nodes),
            Arc::new(StringArray::from(vec!["Person"; nodes as usize])),
        ],
    )
    .unwrap();
    let edges = nodes - 1;
    let edge_batch = RecordBatch::try_new(
        bulk_edge_input_schema(Vec::new()).unwrap(),
        vec![
            uuid_column((0..edges).map(|i| offset + 1_000_000 + i)),
            Arc::new(StringArray::from(vec!["KNOWS"; edges as usize])),
            uuid_column((0..edges).map(|i| offset + i)),
            uuid_column((0..edges).map(|i| offset + i + 1)),
        ],
    )
    .unwrap();
    let mut session = graph
        .begin_import_session(
            OperationId(uuid::Uuid::now_v7()),
            ImportSessionLimits::default(),
        )
        .unwrap();
    session
        .append_arrow(BulkInputKind::Node, &[node_batch])
        .unwrap();
    session
        .append_arrow(BulkInputKind::Edge, &[edge_batch])
        .unwrap();
    session.validate(graph).unwrap();
    session.commit(graph, None).unwrap();
}

#[test]
fn import_session_commit_hashes_only_newly_published_payload() {
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    let before = inodes(root.path());
    let capture = PayloadDigestCapture::start();
    bulk_load(&graph, NODES, 1);
    let work = capture.snapshot();
    drop(capture);
    assert_bounded(
        "import session",
        work,
        newly_published_bytes(root.path(), &before),
    );
    assert_eq!(
        graph.node_count("Person").unwrap(),
        u64::try_from(NODES).unwrap()
    );
}

#[test]
fn facade_mutation_does_not_rehash_the_unchanged_graph() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_str();
    let graph = GraphForge::new(path).unwrap();
    bulk_load(&graph, NODES, 1);
    let before = inodes(root.path());
    let capture = PayloadDigestCapture::start();
    graph
        .add_node(
            "Person",
            &HashMap::from([("name".into(), PropValue::Str("Ada".into()))]),
        )
        .unwrap();
    let work = capture.snapshot();
    drop(capture);
    assert_bounded(
        "facade add_node",
        work,
        newly_published_bytes(root.path(), &before),
    );
    drop(graph);
    let reopened = GraphForge::new(path).unwrap();
    assert_eq!(
        reopened.node_count("Person").unwrap(),
        u64::try_from(NODES).unwrap() + 1
    );
}

#[test]
fn graph_delta_publish_hashes_only_newly_published_payload() {
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    bulk_load(&graph, NODES, 1);
    drop(graph);
    let before = inodes(root.path());
    let capture = PayloadDigestCapture::start();
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
                    node_uuid: v7(1).to_string(),
                    property_stem: "Person".into(),
                    key: "name".into(),
                    value: graphforge_storage::encode_graph_delta_value(
                        &graphforge_ir::IrLiteral::Str("Ada".into()),
                    )
                    .unwrap(),
                },
            }],
            limits: graphforge_storage::GraphDeltaJournalLimits::default(),
        },
    )
    .unwrap();
    let work = capture.snapshot();
    drop(capture);
    assert_bounded(
        "graph delta",
        work,
        newly_published_bytes(root.path(), &before),
    );
}

#[test]
fn compact_cas_republication_does_not_rehash_inherited_objects() {
    use graphforge_storage::{
        ProjectGenerationRequest, ProjectParticipant, ProjectParticipantEncoding,
        ProjectStageOutcome,
    };
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    bulk_load(&graph, NODES, 1);
    drop(graph);
    let parent = graphforge_storage::resolve_project_generation(root.path()).unwrap();
    let participants = parent
        .participant_snapshots()
        .unwrap()
        .into_iter()
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
    assert!(
        participants
            .iter()
            .any(|participant| participant.capability_id == "graph"
                && participant.record_family_id == "files"
                && participant.record_version == 8),
        "bulk import must publish a compact graph root"
    );
    let capabilities = parent
        .capabilities()
        .into_iter()
        .map(|capability| graphforge_storage::ProjectCapability {
            capability_id: capability.capability_id,
            capability_version: capability.capability_version,
        })
        .collect::<Vec<_>>();
    drop(parent);
    let request = ProjectGenerationRequest {
        transaction_uuid: uuid::Uuid::now_v7(),
        generation_uuid: uuid::Uuid::now_v7(),
        capabilities,
        participants,
    };
    // Every compact object already exists; this publication installs only a
    // generation that names them. Re-hashing inherited objects exceeds the
    // newly published bytes.
    let lease = graphforge_storage::begin_graph_object_publication(root.path()).unwrap();
    let before = inodes(root.path());
    let capture = PayloadDigestCapture::start();
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
    let work = capture.snapshot();
    drop(capture);
    drop(lease);
    assert_bounded(
        "compact CAS republish",
        work,
        newly_published_bytes(root.path(), &before),
    );
    let reopened = GraphForge::new(root.path().to_str()).unwrap();
    assert_eq!(
        reopened.node_count("Person").unwrap(),
        u64::try_from(NODES).unwrap()
    );
}

#[cfg(feature = "knowledge")]
#[test]
fn knowledge_publication_on_a_bulk_imported_project_hashes_only_new_payload() {
    use graphforge_api::{
        AssertionGraphRefInput, AssertionGraphRole, CapabilityId, CreateAssertionRequest,
        EnableCapabilityRequest, GraphObjectKind, WriteContext,
    };
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_str();
    let graph = GraphForge::new(path).unwrap();
    // Bulk import publishes a compact graph root. Every later generation that
    // carries it forward must publish under its CAS lease (#1691).
    bulk_load(&graph, NODES, 1);
    for capability_id in [CapabilityId::Provenance, CapabilityId::Knowledge] {
        let before = inodes(root.path());
        let capture = PayloadDigestCapture::start();
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
        let work = capture.snapshot();
        drop(capture);
        assert_bounded(
            &format!("enable {capability_id:?}"),
            work,
            newly_published_bytes(root.path(), &before),
        );
    }
    let before = inodes(root.path());
    let capture = PayloadDigestCapture::start();
    graph
        .create_assertion(CreateAssertionRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid::Uuid::now_v7()),
                actor_uuid: None,
            },
            assertion_uuid: uuid::Uuid::now_v7(),
            claim: "Person exists".into(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid: v7(1),
                graph_kind: GraphObjectKind::Node,
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        })
        .unwrap();
    let work = capture.snapshot();
    drop(capture);
    assert_bounded(
        "knowledge assertion",
        work,
        newly_published_bytes(root.path(), &before),
    );
    drop(graph);
    let reopened = GraphForge::new(path).unwrap();
    assert_eq!(
        reopened
            .list_assertions(graphforge_api::ListAssertionsRequest::default())
            .unwrap()
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    assert_eq!(
        reopened.node_count("Person").unwrap(),
        u64::try_from(NODES).unwrap()
    );
}
