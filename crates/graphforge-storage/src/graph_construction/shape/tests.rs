use super::super::tests::{edge_batch, fixed, node_batch, nonempty_project_with_nodes, open};
use super::super::*;
use super::*;
use arrow::array::StringArray;
use std::sync::Arc;
use tempfile::TempDir;

fn nonempty_project() -> TempDir {
    nonempty_project_with_nodes(2)
}

#[test]
fn resolved_endpoint_windows_use_logarithmic_name_state_at_1x_2x_4x() {
    for windows in [1_u64, 2, 4] {
        let root = TempDir::new().unwrap();
        let budgets = GraphConstructionBudgets {
            max_batch_rows: 2,
            max_run_records: 8,
            merge_fan_in: 2,
            ..GraphConstructionBudgets::default()
        };
        let mut session = GraphConstructionSession::open(
            root.path(),
            Uuid::from_u128(7_100 + u128::from(windows)),
            0,
            budgets,
        )
        .unwrap();
        let nodes = windows + 1;
        for offset in (0..nodes).step_by(2) {
            let rows = usize::try_from((nodes - offset).min(2)).unwrap();
            session
                .append(
                    ConstructionChunkKind::Node,
                    &format!("nodes-{offset}"),
                    &node_batch(1 + u128::from(offset), rows),
                )
                .unwrap();
        }
        for offset in (0..windows).step_by(2) {
            let rows = usize::try_from((windows - offset).min(2)).unwrap();
            session
                .append(
                    ConstructionChunkKind::Edge,
                    &format!("edges-{offset}"),
                    &edge_batch(10_000 + u128::from(offset), 1, u128::from(nodes), rows),
                )
                .unwrap();
        }
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let expected_slots = u64::from(windows.ilog2()).max(1);
        assert!(
            session.evidence().peak_resolved_endpoint_name_slots <= expected_slots,
            "windows={windows} slots={}",
            session.evidence().peak_resolved_endpoint_name_slots
        );
        let shaped_outputs = std::iter::once(&shape.identities)
            .chain(shape.node_details.iter())
            .chain(shape.edge_details.iter())
            .chain(shape.node_rows.iter())
            .chain(shape.edge_rows.iter())
            .chain(shape.edge_endpoints.iter())
            .chain(std::iter::once(&shape.runtime_catalog));
        let expected_payload_bytes = shaped_outputs
            .clone()
            .map(|name| {
                session
                    .root
                    .open_child_file(OsStr::new(name))
                    .unwrap()
                    .metadata()
                    .unwrap()
                    .len()
            })
            .sum::<u64>();
        let retained_shape_files = session
            .root
            .child_names()
            .unwrap()
            .into_iter()
            .filter(|name| name.to_str().is_some_and(is_shape_artifact_name))
            .collect::<Vec<_>>();
        let expected_retained_allocation = retained_shape_files
            .iter()
            .map(|name| {
                let file = session.root.open_child_file(name).unwrap();
                graphforge_filesystem::file_space_usage(&file)
                    .unwrap()
                    .allocated_bytes
            })
            .sum::<u64>();
        assert!(expected_retained_allocation > 0);
        assert_eq!(
            session.evidence().current_merge_temporary_allocated_bytes,
            expected_retained_allocation,
            "retained authenticated shape allocation differs from the actual file inventory: {retained_shape_files:?}"
        );
        let expected_capability_bytes = shaped_outputs
            .map(|name| {
                session
                    .root
                    .open_child_file(OsStr::new(&shape_receipt_name(name)))
                    .unwrap()
                    .metadata()
                    .unwrap()
                    .len()
            })
            .sum::<u64>();
        assert_eq!(
            session.evidence().shaped_output_authentication_bytes,
            expected_capability_bytes
        );
        assert!(expected_capability_bytes < expected_payload_bytes);
        assert!(session.evidence().shaped_output_authentication_operations > 0);
    }
}

#[test]
fn shaping_is_bounded_deterministic_and_multipass_at_1x_2x_4x() {
    // Sized so the recorded cut yields several partitions at every rung: the
    // cut is the recorded count bounded by identities/16, so a handful of rows
    // would shape into one partition and this test would stop exercising the
    // multi-partition concatenation it exists for.
    const NODES_PER_CHUNK: usize = 64;
    const EDGES_PER_CHUNK: usize = 32;
    let mut observed_partitions = Vec::new();
    for chunks in [1_usize, 2, 4] {
        let root = TempDir::new().unwrap();
        let mut session = GraphConstructionSession::open(
            root.path(),
            Uuid::from_u128(7_000 + chunks as u128),
            0,
            GraphConstructionBudgets {
                merge_fan_in: 2,
                ..GraphConstructionBudgets::default()
            },
        )
        .unwrap();
        for chunk in 0..chunks {
            session
                .append(
                    ConstructionChunkKind::Node,
                    &format!("nodes-{chunk}"),
                    &node_batch(1 + (chunk * NODES_PER_CHUNK) as u128, NODES_PER_CHUNK),
                )
                .unwrap();
        }
        session
            .append(
                ConstructionChunkKind::Edge,
                "edges",
                &edge_batch(
                    10_000,
                    1,
                    (chunks * NODES_PER_CHUNK) as u128,
                    chunks * EDGES_PER_CHUNK,
                ),
            )
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        assert_eq!(shape.node_count, (chunks * NODES_PER_CHUNK) as u64);
        assert_eq!(shape.edge_count, (chunks * EDGES_PER_CHUNK) as u64);
        assert_eq!(shape.max_node_surrogate, (chunks * NODES_PER_CHUNK) as u64);
        assert_eq!(shape.max_edge_surrogate, (chunks * EDGES_PER_CHUNK) as u64);
        // Property-free input: the catalog is derived from the details
        // families and no shaped row artifact is produced (#1455).
        assert!(shape.node_rows.is_empty());
        assert!(shape.edge_rows.is_empty());
        assert!(shape.node_details.is_some());
        assert!(shape.edge_details.is_some());
        assert!(shape.edge_endpoints.is_some());
        assert!(
            session
                .root
                .open_child_file(OsStr::new(&shape.runtime_catalog))
                .is_ok()
        );
        // The global order is a concatenation across several partitions at
        // every rung, not a degenerate single sorted run.
        assert!(
            session.evidence().shape_partitions > 1,
            "chunks={chunks} partitions={}",
            session.evidence().shape_partitions
        );
        assert_eq!(
            session.evidence().partitioned_identity_rows,
            (chunks * (NODES_PER_CHUNK + EDGES_PER_CHUNK)) as u64
        );
        assert!(session.evidence().partition_outputs > 0);
        assert!(session.evidence().merge_read_bytes > 0);
        assert!(session.evidence().merge_written_bytes > 0);
        assert!(session.evidence().merge_read_blocks > 0);
        assert!(session.evidence().merge_write_blocks > 0);
        assert!(session.evidence().merge_fsync_operations > 0);
        assert!(session.evidence().merge_read_operations > 0);
        assert!(session.evidence().merge_write_operations > 0);
        // Property-free input opens no Parquet through the counted row
        // reader (#1455): the row partitioner and the catalog's row scan are
        // skipped. The chunk Parquet is still authenticated once per
        // receipt, which the input-validation counters record.
        assert_eq!(session.evidence().parquet_read_operations, 0);
        assert!(session.evidence().shape_input_validation_read_operations > 0);
        assert!(session.evidence().parquet_write_operations > 0);
        // The external merge tree is gone; there are no merge levels left.
        assert_eq!(session.evidence().merge_passes, 0);
        observed_partitions.push(session.evidence().shape_partitions);
    }
    // The partition count rises with the input, which is the partitioned
    // analogue of the merge tree gaining a level at 4x.
    assert!(
        observed_partitions.windows(2).all(|pair| pair[0] < pair[1]),
        "{observed_partitions:?}"
    );
}

#[test]
fn batch_partition_and_resume_produce_identical_canonical_data_fingerprints() {
    let one = TempDir::new().unwrap();
    let mut one_session = open(&one, 7_100);
    one_session
        .append(ConstructionChunkKind::Node, "all", &node_batch(1, 8))
        .unwrap();
    one_session.seal().unwrap();
    let one_shape = one_session
        .shape_canonical_with_cancellation(|| false)
        .unwrap();
    let one_identity = receipt_for_existing(&one_session.root, &one_shape.identities).unwrap();
    let one_details =
        receipt_for_existing(&one_session.root, one_shape.node_details.as_ref().unwrap()).unwrap();

    let two = TempDir::new().unwrap();
    let mut two_session = open(&two, 7_101);
    two_session
        .append(ConstructionChunkKind::Node, "first", &node_batch(1, 4))
        .unwrap();
    two_session
        .append(ConstructionChunkKind::Node, "second", &node_batch(5, 4))
        .unwrap();
    two_session.seal().unwrap();
    drop(two_session);
    let mut resumed = open(&two, 7_101);
    let two_shape = resumed.shape_canonical_with_cancellation(|| false).unwrap();
    let two_identity = receipt_for_existing(&resumed.root, &two_shape.identities).unwrap();
    let two_details =
        receipt_for_existing(&resumed.root, two_shape.node_details.as_ref().unwrap()).unwrap();
    assert_eq!(one_identity.sha256, two_identity.sha256);
    assert_eq!(one_details.sha256, two_details.sha256);
    assert_eq!((one_shape.node_count, one_shape.edge_count), (8, 0));
    assert_eq!((two_shape.node_count, two_shape.edge_count), (8, 0));
}

#[test]
fn immediate_seal_prepare_authenticates_staged_bytes_once_with_constant_factor() {
    let mut observations = Vec::new();
    for rows in [64_usize, 128, 256] {
        let root = TempDir::new().unwrap();
        crate::open_or_initialize_project(root.path()).unwrap();
        let mut session = GraphConstructionSession::open(
            root.path(),
            Uuid::now_v7(),
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, rows))
            .unwrap();
        session
            .seal_and_prepare_canonical_encoding_with_cancellation(1, || false)
            .unwrap();
        let evidence = session.evidence();
        assert_eq!(evidence.authentication_read_bytes, 0);
        assert!(evidence.shape_input_validation_read_bytes > 0);
        assert!(
            evidence.shaped_output_authentication_bytes < evidence.canonical_output_bytes / 4,
            "writer-completed shaped outputs were reopened: auth={} canonical={}",
            evidence.shaped_output_authentication_bytes,
            evidence.canonical_output_bytes
        );
        assert!(
            evidence.shape_input_validation_read_bytes
                <= evidence
                    .write_bytes
                    .checked_mul(3)
                    .expect("shape write-byte bound overflow"),
            "shape authentication exceeded three staged-byte passes"
        );
        observations.push((rows, evidence.shape_input_validation_read_bytes));
    }
    for pair in observations.windows(2) {
        let (smaller_rows, smaller_bytes) = pair[0];
        let (larger_rows, larger_bytes) = pair[1];
        assert_eq!(larger_rows, smaller_rows * 2);
        assert!(larger_bytes >= smaller_bytes);
        assert!(
            larger_bytes
                <= smaller_bytes
                    .checked_mul(3)
                    .expect("shape growth bound overflow"),
            "doubling rows exceeded the bounded linear byte envelope"
        );
    }
}

#[test]
fn interrupted_immediate_seal_reauthenticates_before_resume_consumption() {
    let root = TempDir::new().unwrap();
    crate::open_or_initialize_project(root.path()).unwrap();
    let operation = Uuid::now_v7();
    let mut session = GraphConstructionSession::open(
        root.path(),
        operation,
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 64))
        .unwrap();
    assert!(
        session
            .seal_and_prepare_canonical_encoding_with_cancellation(1, || true)
            .is_err()
    );
    assert_eq!(session.state(), GraphConstructionState::Sealed);
    assert_eq!(session.evidence().authentication_read_bytes, 0);
    drop(session);

    let mut resumed = GraphConstructionSession::open(
        root.path(),
        operation,
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    resumed
        .prepare_canonical_encoding_with_cancellation(1, || false)
        .unwrap();
    assert!(resumed.evidence().shape_input_validation_read_bytes > 0);
}

#[test]
fn shaping_rejects_cross_kind_duplicates_and_missing_endpoints() {
    let root = TempDir::new().unwrap();
    let mut duplicate = open(&root, 8_001);
    duplicate
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
        .unwrap();
    duplicate
        .append(
            ConstructionChunkKind::Edge,
            "edges",
            &edge_batch(1, 1, 2, 1),
        )
        .unwrap();
    duplicate.seal().unwrap();
    assert!(
        duplicate
            .shape_canonical_with_cancellation(|| false)
            .unwrap_err()
            .to_string()
            .contains("duplicate identity")
    );

    let root = TempDir::new().unwrap();
    let mut missing = open(&root, 8_002);
    missing
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
        .unwrap();
    missing
        .append(
            ConstructionChunkKind::Edge,
            "edges",
            &edge_batch(20_000, 1, 3, 2),
        )
        .unwrap();
    missing.seal().unwrap();
    assert!(
        missing
            .shape_canonical_with_cancellation(|| false)
            .unwrap_err()
            .to_string()
            .contains("endpoint")
    );
}

#[test]
fn nonempty_base_rejects_duplicate_cross_kind_and_missing_endpoint_without_copy() {
    let project = nonempty_project();
    let budgets = GraphConstructionBudgets::default();

    let mut duplicate =
        GraphConstructionSession::open(project.path(), Uuid::from_u128(8_101), 1, budgets).unwrap();
    duplicate
        .append(ConstructionChunkKind::Node, "duplicate", &node_batch(1, 1))
        .unwrap();
    duplicate.seal().unwrap();
    assert!(
        duplicate
            .shape_canonical_with_cancellation(|| false)
            .unwrap_err()
            .to_string()
            .contains("conflicts with pinned base")
    );
    assert!(
        !project
            .path()
            .join(PRIVATE_ROOT)
            .join(Uuid::from_u128(8_101).simple().to_string())
            .join("base-identities.run")
            .exists()
    );
    drop(duplicate);

    let edge = |edge_uuid: u128, source: u128, target: u128| {
        RecordBatch::try_new(
            CONSTRUCTION_EDGE_SCHEMA.clone(),
            vec![
                Arc::new(fixed(&[edge_uuid.to_be_bytes()])),
                Arc::new(StringArray::from(vec!["R"])),
                Arc::new(fixed(&[source.to_be_bytes()])),
                Arc::new(fixed(&[target.to_be_bytes()])),
            ],
        )
        .unwrap()
    };
    let mut cross_kind =
        GraphConstructionSession::open(project.path(), Uuid::from_u128(8_102), 1, budgets).unwrap();
    cross_kind
        .append(ConstructionChunkKind::Edge, "cross-kind", &edge(1, 1, 2))
        .unwrap();
    cross_kind.seal().unwrap();
    assert!(
        cross_kind
            .shape_canonical_with_cancellation(|| false)
            .unwrap_err()
            .to_string()
            .contains("conflicts with pinned base")
    );
    drop(cross_kind);

    let mut missing =
        GraphConstructionSession::open(project.path(), Uuid::from_u128(8_103), 1, budgets).unwrap();
    missing
        .append(ConstructionChunkKind::Edge, "missing", &edge(200, 999, 1))
        .unwrap();
    missing.seal().unwrap();
    assert!(
        missing
            .shape_canonical_with_cancellation(|| false)
            .unwrap_err()
            .to_string()
            .contains("endpoint")
    );

    drop(missing);
    let operation = Uuid::from_u128(8_104);
    let mut delta = GraphConstructionSession::open(project.path(), operation, 1, budgets).unwrap();
    delta
        .append(ConstructionChunkKind::Node, "one-new", &node_batch(3, 1))
        .unwrap();
    delta.seal().unwrap();
    let shape = delta.shape_canonical_with_cancellation(|| false).unwrap();
    let operation_root = project
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string());
    assert_eq!(
        std::fs::metadata(operation_root.join(&shape.identities))
            .unwrap()
            .len(),
        BASE_IDENTITY_WIDTH as u64
    );
    assert!(!operation_root.join("staged-identities.run").exists());
    assert_eq!(shape.parent_topology_generation, 1);
    assert!(shape.parent_uuid_manifest_sha256.is_some());

    drop(delta);
    let mut retained_endpoints =
        GraphConstructionSession::open(project.path(), Uuid::from_u128(8_105), 1, budgets).unwrap();
    retained_endpoints
        .append(
            ConstructionChunkKind::Edge,
            "base-endpoints",
            &edge(200, 1, 2),
        )
        .unwrap();
    retained_endpoints.seal().unwrap();
    let shape = retained_endpoints
        .shape_canonical_with_cancellation(|| false)
        .unwrap();
    assert_eq!((shape.node_count, shape.edge_count), (2, 2));
    assert!(retained_endpoints.evidence().retained_probe_read_bytes > 0);
    assert!(retained_endpoints.evidence().retained_probe_block_loads > 0);
    assert!(
        retained_endpoints.evidence().retained_probe_read_bytes
            <= retained_endpoints
                .evidence()
                .retained_probe_block_loads
                .checked_mul(BLOCK_BYTES as u64)
                .expect("retained probe byte bound overflow")
    );
    let mut resolved = BufReader::new(
        retained_endpoints
            .root
            .open_child_file(OsStr::new(shape.edge_endpoints.as_ref().unwrap()))
            .unwrap(),
    );
    assert_eq!(
        u64::from_be_bytes(
            read_fixed::<RESOLVED_ENDPOINT_WIDTH>(&mut resolved)
                .unwrap()
                .unwrap()[RESOLVED_SURROGATE_OFFSET..RESOLVED_ENDPOINT_WIDTH]
                .try_into()
                .unwrap()
        ),
        1
    );
    assert_eq!(
        u64::from_be_bytes(
            read_fixed::<RESOLVED_ENDPOINT_WIDTH>(&mut resolved)
                .unwrap()
                .unwrap()[RESOLVED_SURROGATE_OFFSET..RESOLVED_ENDPOINT_WIDTH]
                .try_into()
                .unwrap()
        ),
        2
    );
}

#[test]
fn packed_endpoint_wire_preserves_full_width_fields_and_refuses_malformed_roles() {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 127_400);
    let node = u128::MAX - 2;
    let edge = u128::MAX;
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(node, 2))
        .unwrap();
    let batch = RecordBatch::try_new(
        CONSTRUCTION_EDGE_SCHEMA.clone(),
        vec![
            Arc::new(fixed(&[edge.to_be_bytes()])),
            Arc::new(StringArray::from(vec!["R"])),
            Arc::new(fixed(&[node.to_be_bytes()])),
            Arc::new(fixed(&[(node + 1).to_be_bytes()])),
        ],
    )
    .unwrap();
    session
        .append(ConstructionChunkKind::Edge, "edge", &batch)
        .unwrap();
    let receipt = session.read_receipt(1).unwrap().endpoints.unwrap();
    let path = session.root.path().join(&receipt.name);
    let mut golden = Vec::new();
    for role in 0..=1_u8 {
        golden.extend_from_slice(&(node + u128::from(role)).to_be_bytes());
        golden.extend_from_slice(&edge.to_be_bytes());
        golden.push(role);
    }
    assert_eq!(golden.len(), 66);
    assert_eq!(std::fs::read(&path).unwrap(), golden);
    for tail in 1..33 {
        assert!(read_fixed::<ENDPOINT_WIDTH>(&mut &golden[..tail]).is_err());
    }
    let current = std::fs::read(root.path().join("CURRENT")).unwrap();
    for malformed in [golden[..65].to_vec(), {
        let mut bytes = golden.clone();
        bytes[32] = 2;
        bytes
    }] {
        std::fs::write(&path, &malformed).unwrap();
        let mut authority = receipt.clone();
        authority.bytes = malformed.len() as u64;
        authority.sha256 = sha256(&malformed);
        assert!(authenticate_artifact(&session.root, &authority, DetailCodec::Compact).is_err());
        assert_eq!(std::fs::read(root.path().join("CURRENT")).unwrap(), current);
    }
    std::fs::write(&path, golden).unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let identities = std::fs::read(session.root.path().join(&shape.identities)).unwrap();
    let mut expected = Vec::new();
    for (uuid, kind, surrogate) in [(node, 0_u8, 1_u64), (node + 1, 0, 2), (edge, 1, 1)] {
        expected.extend_from_slice(&uuid.to_be_bytes());
        expected.extend_from_slice(&[kind, 0]);
        expected.extend_from_slice(&surrogate.to_be_bytes());
    }
    assert_eq!(expected.len(), 78);
    assert_eq!(identities, expected);
    for tail in 1..26 {
        assert!(read_fixed::<BASE_IDENTITY_WIDTH>(&mut &expected[..tail]).is_err());
    }
    let resolved = std::fs::read(session.root.path().join(shape.edge_endpoints.unwrap())).unwrap();
    let mut expected = Vec::new();
    for role in 0..=1_u8 {
        expected.extend_from_slice(&edge.to_be_bytes());
        expected.push(role);
        expected.extend_from_slice(&(u64::from(role) + 1).to_be_bytes());
    }
    assert_eq!(expected.len(), 50);
    assert_eq!(resolved, expected);
    for tail in 1..25 {
        assert!(read_fixed::<RESOLVED_ENDPOINT_WIDTH>(&mut &expected[..tail]).is_err());
    }
}

#[test]
fn fixed_merge_reader_rejects_truncation_and_self_loop_is_valid() {
    assert!(read_fixed::<16>(&mut std::io::Cursor::new(vec![0_u8; 15])).is_err());
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 8_004);
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 1))
        .unwrap();
    let endpoint = [1_u128.to_be_bytes()];
    let edge = RecordBatch::try_new(
        CONSTRUCTION_EDGE_SCHEMA.clone(),
        vec![
            Arc::new(fixed(&[9_000_u128.to_be_bytes()])),
            Arc::new(StringArray::from(vec!["R"])),
            Arc::new(fixed(&endpoint)),
            Arc::new(fixed(&endpoint)),
        ],
    )
    .unwrap();
    session
        .append(ConstructionChunkKind::Edge, "self-loop", &edge)
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    assert_eq!(shape.edge_count, 1);
    let mut resolved = BufReader::new(
        session
            .root
            .open_child_file(OsStr::new(shape.edge_endpoints.as_ref().unwrap()))
            .unwrap(),
    );
    let source = read_fixed::<RESOLVED_ENDPOINT_WIDTH>(&mut resolved)
        .unwrap()
        .unwrap();
    let target = read_fixed::<RESOLVED_ENDPOINT_WIDTH>(&mut resolved)
        .unwrap()
        .unwrap();
    assert_eq!(&source[..16], &9_000_u128.to_be_bytes());
    assert_eq!((source[16], target[16]), (0, 1));
    assert_eq!(
        &source[RESOLVED_SURROGATE_OFFSET..RESOLVED_ENDPOINT_WIDTH],
        &target[RESOLVED_SURROGATE_OFFSET..RESOLVED_ENDPOINT_WIDTH]
    );
    assert!(
        read_fixed::<RESOLVED_ENDPOINT_WIDTH>(&mut resolved)
            .unwrap()
            .is_none()
    );
    drop(session);
    let mut resumed = GraphConstructionSession::open(
        root.path(),
        Uuid::from_u128(8_004),
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    assert_eq!(
        resumed.shape_canonical_with_cancellation(|| false).unwrap(),
        shape
    );
}

#[test]
fn resolved_endpoints_never_cost_one_write_submission_each() {
    // Resolved endpoints are re-keyed by edge UUID away from their node-UUID
    // input order, so they are routed one record at a time. Without a bounded
    // spill buffer behind that route every record reached the descriptor as
    // its own write (#1440: 33.6M submissions for 840 MB at S20, the whole
    // shaping pass having needed 93k before), and the ingest path lost ~23%
    // of its throughput. Every other family is run-batched, so the shaping
    // pass as a whole must submit far fewer writes than there are resolved
    // endpoints.
    const NODES: usize = 256;
    const EDGES: usize = 8_192;
    let root = TempDir::new().unwrap();
    let mut session = GraphConstructionSession::open(
        root.path(),
        Uuid::from_u128(7_443),
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, NODES))
        .unwrap();
    session
        .append(
            ConstructionChunkKind::Edge,
            "edges",
            &edge_batch(10_000, 1, NODES as u128, EDGES),
        )
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    assert_eq!(shape.edge_count, EDGES as u64);
    assert!(shape.edge_endpoints.is_some());
    let resolved_endpoints = 2 * EDGES as u64;
    let submissions = session.evidence().merge_write_operations;
    assert!(
        submissions < resolved_endpoints,
        "shaping submitted {submissions} writes for {resolved_endpoints} resolved endpoints"
    );
}

/// A single partition larger than every candidate buffer must stream without
/// growing the accumulator or changing its authenticated bytes/row counts.
#[test]
fn partition_run_bounds_fixed_records_without_changing_output() {
    let records = (1_u128..=131_073)
        .map(u128::to_be_bytes)
        .collect::<Vec<_>>();
    for bound in [64 * 1024, 256 * 1024, 1024 * 1024] {
        assert_bounded_partition_run(&records, None, bound);
    }
}

#[test]
fn partition_run_keeps_compact_records_whole_across_flushes() {
    let records = (1_u128..=512)
        .map(|key| {
            let mut record = [0; NODE_DETAIL_WIDTH];
            record[..16].copy_from_slice(&key.to_be_bytes());
            let length = if key % 2 == 0 { 255 } else { 1 };
            record[16] = length;
            record[17..17 + usize::from(length)].fill(b'x');
            record
        })
        .collect::<Vec<_>>();
    // The 64-byte case also exercises a record larger than the buffer.
    for bound in [64, 1024, 64 * 1024] {
        assert_bounded_partition_run(&records, Some(DetailCodec::Compact), bound);
    }
}

fn assert_bounded_partition_run<const N: usize>(
    records: &[[u8; N]],
    codec: Option<DetailCodec>,
    bound: usize,
) {
    let root = TempDir::new().unwrap();
    crate::open_or_initialize_project(root.path()).unwrap();
    let mut session = open(&root, 0x1445);
    let mut target =
        FixedRangePartitioner::<N>::new(&session.root, PartitionFamily::Identities, 2, codec, true)
            .unwrap();
    let evidence = &mut session.checkpoint.evidence;
    let mut run = PartitionRun::with_bound(bound);
    let mut expected = Vec::new();
    // Two equally populated partitions exercise partition changes as well as
    // byte-triggered flushes. Partition changes must retain partial buffers.
    for (index, record) in records.iter().enumerate() {
        let wire = codec
            .map_or(Ok(record.as_slice()), |codec| codec.bytes(record))
            .unwrap();
        expected.extend_from_slice(wire);
        run.push(
            index / records.len().div_ceil(2),
            wire,
            &mut target,
            evidence,
        )
        .unwrap();
        assert!(run.bytes.len() <= bound);
        assert!(run.bytes.capacity() <= bound);
    }
    run.flush(&mut target, evidence).unwrap();
    run.flush(&mut target, evidence).unwrap(); // Empty flush cannot duplicate rows.
    assert_eq!(target.balance().total(), records.len() as u64);
    let output = target
        .finish_optional("staged-identities.run", &mut || false, evidence)
        .unwrap()
        .unwrap();
    let mut actual = Vec::new();
    session
        .root
        .open_child_file(OsStr::new(&output))
        .unwrap()
        .read_to_end(&mut actual)
        .unwrap();
    assert_eq!(actual, expected, "bound={bound}");
    assert_eq!(evidence.partition_rows, records.len() as u64);
}
