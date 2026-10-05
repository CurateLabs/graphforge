use super::*;
use crate::ProcedureDefinition;
#[cfg(all(feature = "knowledge", feature = "portable"))]
use crate::{
    AssertionGraphRefInput, AssertionGraphRole, CapabilityId, CreateAssertionRequest,
    EnableCapabilityRequest, GraphObjectKind, OperationId, WriteContext,
};
use std::path::PathBuf;

#[test]
fn clear_repopulation_does_not_reuse_private_adjacency_at_same_generation() {
    use graphforge_exec::AdjacencyProvider;
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute("CREATE (:Person)-[:KNOWS]->(:Person)")
        .unwrap();
    let retained = graph
        .adjacency_provider_for_session()
        .adjacency("KNOWS", graphforge_ir::Direction::Out)
        .unwrap();
    let generation =
        graphforge_storage::generation::read_topology_generation(&graph.dir()).unwrap();
    graph.clear().unwrap();
    graph
        .execute("CREATE (:Person)-[:KNOWS]->(:Person)-[:KNOWS]->(:Person)")
        .unwrap();
    assert_eq!(
        graphforge_storage::generation::read_topology_generation(&graph.dir()).unwrap(),
        generation
    );
    let count = graph
        .execute("MATCH ()-[r]->() RETURN count(r) AS n")
        .unwrap();
    let count = count.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(count.value(0), 2);
    assert_eq!(
        graph
            .execute("MATCH ()-[r:KNOWS]->() RETURN r")
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        2
    );
    // The old view has not touched a shard yet. Its first lazy read must
    // still see the old graph after a different CSR has been published.
    assert_eq!(retained.neighbors(1).unwrap().len(), 1);
    assert!(retained.neighbors(2).unwrap().is_empty());
}

#[cfg(unix)]
#[test]
fn clear_resets_in_memory_state_after_partial_filesystem_failure() {
    use std::os::unix::fs::PermissionsExt;

    struct PermissionGuard {
        path: PathBuf,
        original: Option<std::fs::Permissions>,
    }

    impl PermissionGuard {
        fn restore(&mut self) {
            if let Some(original) = self.original.take() {
                std::fs::set_permissions(&self.path, original)
                    .expect("restore fixture directory permissions");
            }
        }
    }

    impl Drop for PermissionGuard {
        fn drop(&mut self) {
            self.restore();
        }
    }

    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:Person {name: 'Alice'})")
        .expect("seed fixture files and runtime catalog");
    gf.register_procedure(ProcedureDefinition {
        name: "test.fixture".into(),
        inputs: vec![],
        outputs: vec![],
        rows: vec![vec![]],
    })
    .expect("register fixture procedure");

    let original = std::fs::metadata(&gf.dir())
        .expect("fixture directory metadata")
        .permissions();
    let mut restricted = original.clone();
    restricted.set_mode(0o500);
    std::fs::set_permissions(&gf.dir(), restricted)
        .expect("restrict fixture directory permissions");
    let mut guard = PermissionGuard {
        path: gf.dir().to_path_buf(),
        original: Some(original),
    };

    let error = gf
        .clear()
        .expect_err("filesystem cleanup must report the permission failure");
    guard.restore();

    assert!(matches!(error, GfError::Storage(_)));
    let catalog = gf.runtime_catalog.lock().expect("runtime catalog lock");
    assert!(catalog.entity_types().is_empty());
    assert!(catalog.relation_types().is_empty());
    assert_eq!(catalog.property_names().count(), 0);
    drop(catalog);
    assert!(
        gf.execute("CALL test.fixture()").is_err(),
        "procedure registry must reset even when filesystem cleanup fails"
    );

    gf.clear()
        .expect("cleanup succeeds after permissions are restored");
}

#[test]
fn private_wire_boundary_matches_public_error_domain() {
    for encoding in ["parquet", "arrow", "json"] {
        assert!(participant_encoding(encoding).is_ok());
    }
    let encoding = participant_encoding("PARQUET").unwrap_err();
    assert_eq!(encoding.code(), "GF_VALIDATION");
    assert_eq!(
        encoding.to_string(),
        "validation error: committed participant has unsupported encoding"
    );
}

#[cfg(all(feature = "knowledge", feature = "portable"))]
#[test]
fn graph_mutation_reuses_large_unchanged_knowledge_participant() {
    let mut work = Vec::new();
    for repeats in [4_096, 8_192] {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph
            .enable_capability(EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid::Uuid::from_u128(80_000)),
                    actor_uuid: None,
                },
                capability_id: CapabilityId::Provenance,
                capability_version: 1,
            })
            .unwrap();
        graph
            .enable_capability(EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid::Uuid::from_u128(80_001)),
                    actor_uuid: None,
                },
                capability_id: CapabilityId::Knowledge,
                capability_version: 1,
            })
            .unwrap();
        let subject = graph
            .add_node("LedgerSubject", &std::collections::HashMap::new())
            .unwrap();
        let mut random_state = 0x1810_091_u64;
        let ledger_claim = (0..repeats * 32)
            .map(|_| {
                random_state = random_state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                char::from(b'a' + ((random_state >> 32) % 26) as u8)
            })
            .collect::<String>();
        graph
            .create_assertion(CreateAssertionRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid::Uuid::from_u128(80_002)),
                    actor_uuid: None,
                },
                assertion_uuid: uuid::Uuid::now_v7(),
                claim: ledger_claim,
                graph_refs: vec![AssertionGraphRefInput {
                    graph_uuid: subject.uuid,
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Subject,
                    ordinal: 0,
                }],
            })
            .unwrap();
        let before = graphforge_storage::resolve_project_generation(root.path()).unwrap();
        let knowledge_before = before
            .participant_snapshot("knowledge", "assertions")
            .unwrap()
            .unwrap();
        let bytes_before = knowledge_before.bytes.clone();
        eprintln!(
            "graph fixture repeats={repeats} ledger_bytes={}",
            bytes_before.len()
        );
        assert!(bytes_before.len() >= 64 * 1024);
        let portable_limits = graphforge_core::portable::PortableV2Limits::default();
        if repeats == 8_192 {
            let parent_export = graph
                .export_portable_v2(
                    &crate::PortableV2ExportRequest {
                        selection: crate::PortableSelection::Current,
                        output_path: root.path().join("parent.gfpb"),
                        representation: graphforge_core::portable::PortableV2Output::Bundle,
                        profile: graphforge_core::portable::PortableV2SelectionProfile::Complete,
                        subset: None,
                        limits: portable_limits,
                    },
                    None,
                    |_| {},
                )
                .unwrap_or_else(|error| panic!("parent portable export failed: {error:?}"));
            eprintln!(
                "parent portable package bytes={}",
                parent_export.payload_bytes
            );
        }

        let _io_capture = graphforge_storage::lifecycle_io::CaptureScope::install();
        let capture = graphforge_storage::concurrency_attribution::RegionCapture::start("mutation");
        graph
            .add_node("AfterLedger", &std::collections::HashMap::new())
            .unwrap();
        let evidence = capture.finish();
        let io = graphforge_storage::lifecycle_io::snapshot()
            .expect("publication lifecycle I/O is captured");
        let carry = evidence
            .regions
            .iter()
            .find(|(name, _)| name.ends_with("participant_carry_forward"))
            .map(|(_, row)| row)
            .expect("carry-forward phase is attributed");
        let reuse_validation = evidence
            .regions
            .iter()
            .find(|(name, _)| name.ends_with("participant_reuse_validation"))
            .map(|(_, row)| row)
            .expect("reused participant checksum validation is attributed");
        let participant_materialization = evidence
            .regions
            .iter()
            .find(|(name, _)| name.ends_with("participant_materialization"))
            .map(|(_, row)| row)
            .expect("participant snapshot materialization is attributed");
        eprintln!(
            "graph ledger_bytes={} carry_forward={:?}",
            bytes_before.len(),
            carry.work
        );
        assert!(
            carry
                .work
                .get("participant_reused_bytes")
                .copied()
                .unwrap_or_default()
                >= bytes_before.len() as u64
        );
        assert!(
            carry.work.get("written_bytes").copied().unwrap_or_default()
                < bytes_before.len() as u64
        );
        assert!(
            carry.work.get("hashed_bytes").copied().unwrap_or_default()
                < 2 * bytes_before.len() as u64
        );
        let validation_read_bytes = reuse_validation
            .work
            .get("participant_payload_read_bytes")
            .copied()
            .unwrap_or_default();
        let reused_bytes = carry
            .work
            .get("participant_reused_bytes")
            .copied()
            .unwrap_or_default();
        assert_eq!(
            validation_read_bytes, reused_bytes,
            "reuse validation reads each reused sibling once; total includes all siblings"
        );
        let validation_hashed_bytes = reuse_validation
            .work
            .get("hashed_bytes")
            .copied()
            .unwrap_or_default();
        assert_eq!(
            validation_hashed_bytes,
            2 * validation_read_bytes,
            "reuse validation computes SHA-256 and XXH64 over each reused sibling once"
        );
        let materialized_bytes = participant_materialization
            .work
            .get("participant_materialized_bytes")
            .copied()
            .unwrap_or_default();
        assert!(
            materialized_bytes < bytes_before.len() as u64,
            "unrelated large knowledge ledger is not materialized while building the graph mutation"
        );
        assert!(
            io.phases[&graphforge_storage::StorageIoPhase::PublicationPreauthentication].read_bytes
                >= validation_read_bytes,
            "the ordinary lifecycle I/O counter includes the reuse checksum read"
        );
        eprintln!(
            "graph ledger_bytes={} reused_bytes={} validation_read_bytes={} validation_hash_bytes={} materialized_bytes={} changed_written_bytes={} changed_hash_bytes={}",
            bytes_before.len(),
            reused_bytes,
            validation_read_bytes,
            validation_hashed_bytes,
            materialized_bytes,
            carry.work.get("written_bytes").copied().unwrap_or_default(),
            carry.work.get("hashed_bytes").copied().unwrap_or_default(),
        );
        work.push((
            bytes_before.len() as u64,
            carry
                .work
                .get("participant_reused_bytes")
                .copied()
                .unwrap_or_default(),
            carry.work.get("written_bytes").copied().unwrap_or_default(),
            carry.work.get("hashed_bytes").copied().unwrap_or_default(),
            validation_read_bytes,
            validation_hashed_bytes,
        ));

        let after = graphforge_storage::resolve_project_generation(root.path()).unwrap();
        let knowledge_after = after
            .participant_snapshot("knowledge", "assertions")
            .unwrap()
            .unwrap();
        assert_eq!(knowledge_after.bytes, bytes_before);
        assert_eq!(knowledge_after.row_count, knowledge_before.row_count);
        if repeats == 8_192 {
            let child_export = graph
                .export_portable_v2(
                    &crate::PortableV2ExportRequest {
                        selection: crate::PortableSelection::Current,
                        output_path: root.path().join("child.gfpb"),
                        representation: graphforge_core::portable::PortableV2Output::Bundle,
                        profile: graphforge_core::portable::PortableV2SelectionProfile::Complete,
                        subset: None,
                        limits: portable_limits,
                    },
                    None,
                    |_| {},
                )
                .unwrap_or_else(|error| panic!("child portable export failed: {error:?}"));
            let target = root.path().join("portable-import");
            GraphForge::import_portable_v2(
                &target,
                &crate::PortableV2ImportRequest {
                    input: child_export.output,
                    operation_id: OperationId(uuid::Uuid::from_u128(80_004)),
                    limits: portable_limits,
                },
                None,
            )
            .unwrap();
            let portable = graphforge_storage::resolve_project_generation(&target).unwrap();
            assert_eq!(
                portable
                    .participant_snapshot("knowledge", "assertions")
                    .unwrap()
                    .unwrap()
                    .bytes,
                bytes_before
            );
        }
    }
    assert!(work[1].0 > work[0].0 * 19 / 10);
    assert!(work[1].1 > work[0].1 * 18 / 10);
    assert!(work[1].2.abs_diff(work[0].2) <= 64);
    assert!(work[1].3.abs_diff(work[0].3) <= 64);
    assert_eq!(work[0].4, work[0].1);
    assert_eq!(work[1].4, work[1].1);
    assert_eq!(work[0].5, 2 * work[0].4);
    assert_eq!(work[1].5, 2 * work[1].4);
}
