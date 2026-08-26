//! Integration tests for the authoritative graph delta journal (#752).

use std::collections::{BTreeMap, HashMap};
use std::fs;

use graphforge_core::{OntologyMode, TypeId};
use graphforge_ir::IrLiteral;
use graphforge_storage::{
    GRAPH_CAPABILITY_ID, GRAPH_CAPABILITY_VERSION, GraphDeltaJournalLimits, GraphDeltaOp,
    GraphDeltaOpKind, GraphDeltaPayload, GraphDeltaPublishRequest, GraphFileEntry, GraphFileRole,
    GraphFilesInventory, GraphWriter, ProjectCapability, ProjectGenerationRequest,
    ProjectStageOutcome, PropertyOverlayLimits, PropertyRouteKind, PropertySnapshotRow,
    capture_graph_files, decode_delta_run, decode_graph_delta_value, delta_run_relative_path,
    empty_workspace_participants, encode_delta_run, encode_graph_delta_value,
    enumerate_property_fragments, list_delta_runs, materialize_replayed_graph_tree,
    open_or_initialize_project, publish_graph_delta, read_edges, read_nodes,
    reconstruct_graph_state, resolve_project_generation, stage_project_generation_with_graph_tree,
    visit_authenticated_property_snapshots,
};
use uuid::Uuid;

fn authenticated_property_rows(
    graph_root: &std::path::Path,
    kind: PropertyRouteKind,
    route: &str,
    scratch_root: &std::path::Path,
) -> Vec<PropertySnapshotRow> {
    let mut rows = Vec::new();
    let result = visit_authenticated_property_snapshots(
        graph_root,
        kind,
        route,
        scratch_root,
        PropertyOverlayLimits::default(),
        |row| {
            rows.push(row);
            Ok(())
        },
    );
    if let Err(error) = result {
        let files = graph_root
            .read_dir()
            .into_iter()
            .flatten()
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>();
        panic!("authenticated {kind:?}/{route} failed: {error}; root={files:?}");
    }
    rows
}

fn assert_small_write_property_evidence(view: &std::path::Path, src_uuid: &str) {
    let scratch = tempfile::tempdir().unwrap();
    let person =
        authenticated_property_rows(view, PropertyRouteKind::Node, "Person", scratch.path());
    let untyped =
        authenticated_property_rows(view, PropertyRouteKind::Node, "_untyped", scratch.path());
    assert_eq!(person.len(), 1, "one base entity property snapshot");
    assert_eq!(
        person[0].values.len(),
        2,
        "score and active remain composed"
    );
    assert_eq!(untyped.len(), 1, "one journal property snapshot");
    assert_eq!(
        untyped[0].uuid,
        *Uuid::parse_str(src_uuid).unwrap().as_bytes()
    );
    assert_eq!(untyped[0].values.get("rank"), Some(&IrLiteral::Int(7)));
    assert_eq!(
        person[0].values.len() + untyped[0].values.len(),
        3,
        "three logical property values occupy two canonical UUID snapshot rows"
    );
    assert_eq!(
        enumerate_property_fragments(view, PropertyRouteKind::Node, "Person")
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        enumerate_property_fragments(view, PropertyRouteKind::Node, "_untyped")
            .unwrap()
            .len(),
        1
    );
}

fn sample_ops() -> Vec<GraphDeltaOp> {
    let edge = Uuid::now_v7();
    let src = Uuid::now_v7();
    let dst = Uuid::now_v7();
    vec![
        GraphDeltaOp {
            operation_uuid: Uuid::now_v7(),
            kind: GraphDeltaOpKind::UpsertNode,
            payload: GraphDeltaPayload::UpsertNodeV2 {
                node_uuid: src.hyphenated().to_string(),
                node_id: 3,
                type_ids: vec![1],
                created_at_micros: 1_700_000_000_000_001,
                updated_at_micros: 1_700_000_000_000_001,
            },
        },
        GraphDeltaOp {
            operation_uuid: Uuid::now_v7(),
            kind: GraphDeltaOpKind::UpsertNode,
            payload: GraphDeltaPayload::UpsertNodeV2 {
                node_uuid: dst.hyphenated().to_string(),
                node_id: 4,
                type_ids: vec![1],
                created_at_micros: 1_700_000_000_000_002,
                updated_at_micros: 1_700_000_000_000_002,
            },
        },
        GraphDeltaOp {
            operation_uuid: Uuid::now_v7(),
            kind: GraphDeltaOpKind::UpsertEdge,
            payload: GraphDeltaPayload::UpsertEdgeV2 {
                edge_uuid: edge.hyphenated().to_string(),
                src_uuid: src.hyphenated().to_string(),
                dst_uuid: dst.hyphenated().to_string(),
                rel_type: "KNOWS".into(),
                edge_id: 2,
                src_id: 3,
                dst_id: 4,
                created_at_micros: 1_700_000_000_000_003,
            },
        },
        GraphDeltaOp {
            operation_uuid: Uuid::now_v7(),
            kind: GraphDeltaOpKind::SetNodeProperty,
            payload: GraphDeltaPayload::SetNodeProperty {
                node_uuid: src.hyphenated().to_string(),
                property_stem: "_untyped".into(),
                key: "rank".into(),
                value: encode_graph_delta_value(&IrLiteral::Int(7)).unwrap(),
            },
        },
    ]
}

fn publish_base(container: &std::path::Path) -> Uuid {
    publish_base_with_extra_nodes(container, 0)
}

fn publish_base_with_extra_nodes(container: &std::path::Path, extra_nodes: usize) -> Uuid {
    open_or_initialize_project(container).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let first = Uuid::parse_str("00000000-0000-7000-8000-000000000001").unwrap();
    let second = Uuid::parse_str("00000000-0000-7000-8000-000000000002").unwrap();
    let edge = Uuid::parse_str("00000000-0000-7000-8000-000000000003").unwrap();
    let mut writer = GraphWriter::open_at(
        workspace.path(),
        OntologyMode::Strict,
        1_700_000_000_000_000,
    )
    .unwrap();
    writer.create_node(first, TypeId(1)).unwrap();
    writer.create_node(second, TypeId(1)).unwrap();
    writer.create_edge(edge, "KNOWS", &first, &second).unwrap();
    for _ in 0..extra_nodes {
        writer.create_node(Uuid::now_v7(), TypeId(1)).unwrap();
    }
    writer
        .set_properties(
            &first,
            Some("Person"),
            HashMap::from([
                ("score".into(), IrLiteral::Int(42)),
                ("active".into(), IrLiteral::Bool(true)),
            ]),
        )
        .unwrap();
    writer
        .set_edge_properties(
            &edge,
            Some("KNOWS"),
            HashMap::from([("weight".into(), IrLiteral::Float(0.5))]),
        )
        .unwrap();
    writer.flush().unwrap();
    let (_, files) = capture_graph_files(workspace.path()).unwrap();
    let mut participants = empty_workspace_participants().unwrap();
    participants.insert(0, files);
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
        stage_project_generation_with_graph_tree(container, &request, Some(workspace.path()))
            .unwrap()
    else {
        panic!("base publication replayed");
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
    generation_uuid
}

#[test]
fn streaming_resource_ladder_is_independent_of_base_rows() {
    let mut evidence = Vec::new();
    for extra_nodes in [0, 32, 1_024] {
        let root = tempfile::tempdir().unwrap();
        publish_base_with_extra_nodes(root.path(), extra_nodes);
        let mut operations = sample_ops();
        let src_id = extra_nodes as u64 + 3;
        let dst_id = src_id + 1;
        if let GraphDeltaPayload::UpsertNodeV2 { node_id, .. } = &mut operations[0].payload {
            *node_id = src_id;
        }
        if let GraphDeltaPayload::UpsertNodeV2 { node_id, .. } = &mut operations[1].payload {
            *node_id = dst_id;
        }
        if let GraphDeltaPayload::UpsertEdgeV2 {
            edge_id,
            src_id: edge_src_id,
            dst_id: edge_dst_id,
            ..
        } = &mut operations[2].payload
        {
            *edge_id = 2;
            *edge_src_id = src_id;
            *edge_dst_id = dst_id;
        }
        publish_graph_delta(
            root.path(),
            &GraphDeltaPublishRequest {
                transaction_uuid: Uuid::now_v7(),
                generation_uuid: Uuid::now_v7(),
                run_uuid: Uuid::now_v7(),
                operations,
                limits: GraphDeltaJournalLimits::default(),
            },
        )
        .unwrap();
        let resolved = resolve_project_generation(root.path()).unwrap();
        let inventory = resolved.graph_files_inventory().unwrap().unwrap();
        let target = tempfile::tempdir().unwrap();
        let limits = GraphDeltaJournalLimits {
            max_batch_rows: 7,
            ..GraphDeltaJournalLimits::default()
        };
        let (_, replay) = materialize_replayed_graph_tree(
            &resolved.graph_tree_root(),
            &inventory,
            target.path(),
            limits,
        )
        .unwrap();
        evidence.push((
            replay.estimated_replay_memory_bytes,
            replay.materialization_batch_row_bound,
        ));
    }
    let (minimum, maximum) = evidence
        .iter()
        .map(|(bytes, _)| *bytes)
        .fold((u64::MAX, 0_u64), |(minimum, maximum), bytes| {
            (minimum.min(bytes), maximum.max(bytes))
        });
    assert!(
        maximum - minimum <= 128,
        "only variable-width operation encoding may change replay memory, not base rows"
    );
    assert_eq!(evidence[0].1, 7);
}

#[test]
fn encode_decode_round_trip_and_golden_magic() {
    let ops = sample_ops();
    let bytes = encode_delta_run(
        1,
        Uuid::now_v7(),
        Uuid::now_v7(),
        &ops,
        GraphDeltaJournalLimits::default(),
    )
    .unwrap();
    assert_eq!(&bytes[..4], b"GFDR");
    let decoded = decode_delta_run(&bytes, Some(1), GraphDeltaJournalLimits::default()).unwrap();
    assert_eq!(decoded.records.len(), ops.len());
    assert_eq!(decoded.records[0].operation_uuid, ops[0].operation_uuid);
}

#[test]
fn typed_property_values_round_trip_and_legacy_strings_are_rejected() {
    let values = [
        IrLiteral::Bool(true),
        IrLiteral::Int(i64::MAX),
        IrLiteral::Float(f64::NAN),
        IrLiteral::Str("not reinterpreted".into()),
        IrLiteral::Uuid(*Uuid::now_v7().as_bytes()),
        IrLiteral::DateTime(1_700_000_000_000_000),
        IrLiteral::List(vec![IrLiteral::Int(1), IrLiteral::Str("two".into())]),
    ];
    for value in values {
        let encoded = encode_graph_delta_value(&value).unwrap();
        let decoded = decode_graph_delta_value(&encoded).unwrap();
        if matches!(value, IrLiteral::Float(number) if number.is_nan()) {
            assert!(matches!(decoded, IrLiteral::Float(number) if number.is_nan()));
        } else {
            assert_eq!(decoded, value);
        }
    }
    assert_eq!(
        decode_graph_delta_value("prototype-string")
            .unwrap_err()
            .code(),
        "GF_UNSUPPORTED_PROJECT_FORMAT"
    );
}

#[test]
fn torn_truncated_reordered_and_checksum_invalid_fail_closed() {
    let ops = sample_ops();
    let mut bytes = encode_delta_run(
        1,
        Uuid::now_v7(),
        Uuid::now_v7(),
        &ops,
        GraphDeltaJournalLimits::default(),
    )
    .unwrap();
    let truncated = &bytes[..bytes.len() / 2];
    assert_eq!(
        decode_delta_run(truncated, Some(1), GraphDeltaJournalLimits::default())
            .unwrap_err()
            .code(),
        "GF_PROJECT_CORRUPT"
    );
    let mut corrupted = bytes.clone();
    let flip = corrupted.len() / 3;
    corrupted[flip] ^= 0xff;
    assert_eq!(
        decode_delta_run(&corrupted, Some(1), GraphDeltaJournalLimits::default())
            .unwrap_err()
            .code(),
        "GF_PROJECT_CORRUPT"
    );
    bytes[8..16].copy_from_slice(&2u64.to_le_bytes());
    assert!(decode_delta_run(&bytes, Some(1), GraphDeltaJournalLimits::default()).is_err());
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "the regression proves one end-to-end publish, reopen, and authenticated replay story"
)]
fn small_write_preserves_unchanged_parquet_and_reopen_replays() {
    let root = tempfile::tempdir().unwrap();
    let _base = publish_base(root.path());
    let parent = resolve_project_generation(root.path()).unwrap();
    let parent_inventory = parent.graph_files_inventory().unwrap().unwrap();
    let parent_nodes_digest = parent_inventory
        .files
        .iter()
        .find(|entry| entry.relative_path == "topology/nodes.parquet")
        .unwrap()
        .content_sha256
        .clone();
    let parent_parquet = parent_inventory
        .files
        .iter()
        .filter(|entry| entry.relative_path.ends_with(".parquet"))
        .map(|entry| (entry.relative_path.clone(), entry.content_sha256.clone()))
        .collect::<BTreeMap<_, _>>();

    let ops = sample_ops();
    let edge_uuid = match &ops[2].payload {
        GraphDeltaPayload::UpsertEdgeV2 { edge_uuid, .. } => edge_uuid.clone(),
        _ => unreachable!(),
    };
    let src_uuid = match &ops[0].payload {
        GraphDeltaPayload::UpsertNodeV2 { node_uuid, .. } => node_uuid.clone(),
        _ => unreachable!(),
    };
    let receipt = publish_graph_delta(
        root.path(),
        &GraphDeltaPublishRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            run_uuid: Uuid::now_v7(),
            operations: ops,
            limits: GraphDeltaJournalLimits::default(),
        },
    )
    .unwrap();
    assert!(receipt.preserved_base_parquet_digests);
    assert!(receipt.unchanged_base_files >= 2);

    let reopened = resolve_project_generation(root.path()).unwrap();
    let inventory = reopened.graph_files_inventory().unwrap().unwrap();
    let child_nodes_digest = inventory
        .files
        .iter()
        .find(|entry| entry.relative_path == "topology/nodes.parquet")
        .unwrap()
        .content_sha256
        .clone();
    assert_eq!(parent_nodes_digest, child_nodes_digest);
    let child_files = inventory
        .files
        .iter()
        .map(|entry| (entry.relative_path.as_str(), entry.content_sha256.as_str()))
        .collect::<BTreeMap<_, _>>();
    for (path, digest) in &parent_parquet {
        assert_eq!(
            child_files.get(path.as_str()),
            Some(&digest.as_str()),
            "small journal write must preserve authenticated base artifact {path}"
        );
    }

    let (state, evidence) = reconstruct_graph_state(
        &reopened.graph_tree_root(),
        &inventory,
        GraphDeltaJournalLimits::default(),
    )
    .unwrap();
    assert_eq!(evidence.runs_replayed, 1);
    assert!(state.edges.contains_key(&edge_uuid));
    assert_eq!(
        state.node_properties.len(),
        3,
        "two base keys plus one journal key survive authenticated replay"
    );
    assert_eq!(
        state
            .node_property_stems
            .get(&(src_uuid.clone(), "rank".into()))
            .map(String::as_str),
        Some("_untyped")
    );
    assert_eq!(
        state
            .node_properties
            .get(&(src_uuid.clone(), "rank".into()))
            .map(|value| decode_graph_delta_value(value).unwrap()),
        Some(IrLiteral::Int(7))
    );
    assert_ne!(receipt.state_fingerprint, [0; 32]);

    let view = tempfile::tempdir().unwrap();
    let (_open, replay) = materialize_replayed_graph_tree(
        &reopened.graph_tree_root(),
        &inventory,
        view.path(),
        GraphDeltaJournalLimits::default(),
    )
    .unwrap();
    assert_eq!(replay.runs_replayed, 1);
    assert_eq!(
        read_nodes(view.path())
            .unwrap()
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        4
    );
    assert_eq!(
        read_edges(view.path(), "KNOWS", OntologyMode::Strict)
            .unwrap()
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );
    assert_small_write_property_evidence(view.path(), &src_uuid);
}

#[test]
fn entity_delete_rewrites_each_affected_property_route_with_tombstones() {
    let root = tempfile::tempdir().unwrap();
    publish_base(root.path());
    let node = Uuid::parse_str("00000000-0000-7000-8000-000000000001").unwrap();
    let edge = Uuid::parse_str("00000000-0000-7000-8000-000000000003").unwrap();
    publish_graph_delta(
        root.path(),
        &GraphDeltaPublishRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            run_uuid: Uuid::now_v7(),
            operations: vec![
                GraphDeltaOp {
                    operation_uuid: Uuid::now_v7(),
                    kind: GraphDeltaOpKind::DeleteEdge,
                    payload: GraphDeltaPayload::DeleteEdge {
                        edge_uuid: edge.hyphenated().to_string(),
                    },
                },
                GraphDeltaOp {
                    operation_uuid: Uuid::now_v7(),
                    kind: GraphDeltaOpKind::DeleteNode,
                    payload: GraphDeltaPayload::DeleteNode {
                        node_uuid: node.hyphenated().to_string(),
                    },
                },
            ],
            limits: GraphDeltaJournalLimits::default(),
        },
    )
    .unwrap();
    let generation = resolve_project_generation(root.path()).unwrap();
    let inventory = generation.graph_files_inventory().unwrap().unwrap();
    let view = tempfile::tempdir().unwrap();
    materialize_replayed_graph_tree(
        &generation.graph_tree_root(),
        &inventory,
        view.path(),
        GraphDeltaJournalLimits::default(),
    )
    .unwrap();

    let scratch = tempfile::tempdir().unwrap();
    for (kind, route) in [
        (PropertyRouteKind::Node, "Person"),
        (PropertyRouteKind::Edge, "KNOWS"),
    ] {
        let rows = authenticated_property_rows(view.path(), kind, route, scratch.path());
        assert!(
            rows.is_empty(),
            "deleted entity must not survive authenticated {kind:?}/{route} replay"
        );
        assert_eq!(
            enumerate_property_fragments(view.path(), kind, route)
                .unwrap()
                .len(),
            2,
            "copied base plus one authoritative tombstone fragment"
        );
    }
}

#[test]
fn remove_absent_property_key_does_not_emit_replay_fragment() {
    let root = tempfile::tempdir().unwrap();
    publish_base(root.path());
    let node = "00000000-0000-7000-8000-000000000001";
    let edge = "00000000-0000-7000-8000-000000000003";
    let operations = vec![
        GraphDeltaOp {
            operation_uuid: Uuid::now_v7(),
            kind: GraphDeltaOpKind::RemoveNodeProperty,
            payload: GraphDeltaPayload::RemoveNodeProperty {
                node_uuid: node.into(),
                property_stem: "Person".into(),
                key: "absent".into(),
            },
        },
        GraphDeltaOp {
            operation_uuid: Uuid::now_v7(),
            kind: GraphDeltaOpKind::RemoveEdgeProperty,
            payload: GraphDeltaPayload::RemoveEdgeProperty {
                edge_uuid: edge.into(),
                property_stem: "KNOWS".into(),
                key: "absent".into(),
            },
        },
    ];
    publish_graph_delta(
        root.path(),
        &GraphDeltaPublishRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            run_uuid: Uuid::now_v7(),
            operations,
            limits: GraphDeltaJournalLimits::default(),
        },
    )
    .unwrap();
    let generation = resolve_project_generation(root.path()).unwrap();
    let inventory = generation.graph_files_inventory().unwrap().unwrap();
    let view = tempfile::tempdir().unwrap();
    materialize_replayed_graph_tree(
        &generation.graph_tree_root(),
        &inventory,
        view.path(),
        GraphDeltaJournalLimits::default(),
    )
    .unwrap();

    assert_eq!(
        enumerate_property_fragments(view.path(), PropertyRouteKind::Node, "Person")
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        enumerate_property_fragments(view.path(), PropertyRouteKind::Edge, "KNOWS")
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn multi_chunk_large_property_values_are_charged_to_replay_memory() {
    let root = tempfile::tempdir().unwrap();
    publish_base(root.path());
    let value = encode_graph_delta_value(&IrLiteral::Str("x".repeat(8 * 1024))).unwrap();
    let mut operations = [
        "00000000-0000-7000-8000-000000000001",
        "00000000-0000-7000-8000-000000000002",
    ]
    .into_iter()
    .map(|node_uuid| GraphDeltaOp {
        operation_uuid: Uuid::now_v7(),
        kind: GraphDeltaOpKind::SetNodeProperty,
        payload: GraphDeltaPayload::SetNodeProperty {
            node_uuid: node_uuid.into(),
            property_stem: "Person".into(),
            key: "large".into(),
            value: value.clone(),
        },
    })
    .collect::<Vec<_>>();
    operations.push(GraphDeltaOp {
        operation_uuid: Uuid::now_v7(),
        kind: GraphDeltaOpKind::SetEdgeProperty,
        payload: GraphDeltaPayload::SetEdgeProperty {
            edge_uuid: "00000000-0000-7000-8000-000000000003".into(),
            property_stem: "KNOWS".into(),
            key: "large".into(),
            value: value.clone(),
        },
    });
    publish_graph_delta(
        root.path(),
        &GraphDeltaPublishRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            run_uuid: Uuid::now_v7(),
            operations,
            limits: GraphDeltaJournalLimits::default(),
        },
    )
    .unwrap();
    let generation = resolve_project_generation(root.path()).unwrap();
    let inventory = generation.graph_files_inventory().unwrap().unwrap();
    let rejected_view = tempfile::tempdir().unwrap();
    let error = materialize_replayed_graph_tree(
        &generation.graph_tree_root(),
        &inventory,
        rejected_view.path(),
        GraphDeltaJournalLimits {
            max_replay_memory_bytes: 256 * 1024,
            max_batch_rows: 1,
            ..GraphDeltaJournalLimits::default()
        },
    )
    .unwrap_err();
    assert_eq!(error.code(), "GF_RESOURCE_LIMIT");
    assert!(error.to_string().contains("Parquet writer memory"));

    let admitted_view = tempfile::tempdir().unwrap();
    materialize_replayed_graph_tree(
        &generation.graph_tree_root(),
        &inventory,
        admitted_view.path(),
        GraphDeltaJournalLimits {
            max_replay_memory_bytes: 512 * 1024,
            max_batch_rows: 1,
            ..GraphDeltaJournalLimits::default()
        },
    )
    .unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let nodes = authenticated_property_rows(
        admitted_view.path(),
        PropertyRouteKind::Node,
        "Person",
        scratch.path(),
    );
    assert_eq!(nodes.len(), 2);
    assert!(
        nodes
            .iter()
            .all(|row| { row.values.get("large") == Some(&IrLiteral::Str("x".repeat(8 * 1024))) })
    );
    let edges = authenticated_property_rows(
        admitted_view.path(),
        PropertyRouteKind::Edge,
        "KNOWS",
        scratch.path(),
    );
    assert_eq!(edges.len(), 1);
    assert_eq!(
        edges[0].values.get("large"),
        Some(&IrLiteral::Str("x".repeat(8 * 1024)))
    );
}

#[test]
fn exact_retry_transaction_is_idempotent_and_conflict_is_typed() {
    let root = tempfile::tempdir().unwrap();
    publish_base(root.path());
    let ops = sample_ops();
    let transaction_uuid = Uuid::now_v7();
    let generation_uuid = Uuid::now_v7();
    let run_uuid = Uuid::now_v7();
    let request = GraphDeltaPublishRequest {
        transaction_uuid,
        generation_uuid,
        run_uuid,
        operations: ops.clone(),
        limits: GraphDeltaJournalLimits::default(),
    };
    let first = publish_graph_delta(root.path(), &request).unwrap();
    assert!(!first.publication.idempotent_replay);
    let second = publish_graph_delta(root.path(), &request).unwrap();
    assert!(second.publication.idempotent_replay);
    assert_eq!(
        first.publication.generation_uuid,
        second.publication.generation_uuid
    );

    let mut conflicting = ops;
    if let GraphDeltaPayload::UpsertEdgeV2 { rel_type, .. } = &mut conflicting[2].payload {
        *rel_type = "OTHER".into();
    }
    let err = publish_graph_delta(
        root.path(),
        &GraphDeltaPublishRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            run_uuid: Uuid::now_v7(),
            operations: conflicting,
            limits: GraphDeltaJournalLimits::default(),
        },
    )
    .unwrap_err();
    assert_eq!(err.code(), "GF_IDEMPOTENCY_CONFLICT");
}

#[test]
fn missing_run_referenced_by_inventory_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    publish_base(root.path());
    publish_graph_delta(
        root.path(),
        &GraphDeltaPublishRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            run_uuid: Uuid::now_v7(),
            operations: sample_ops(),
            limits: GraphDeltaJournalLimits::default(),
        },
    )
    .unwrap();
    let resolved = resolve_project_generation(root.path()).unwrap();
    let inventory = resolved.graph_files_inventory().unwrap().unwrap();
    let run_path = resolved.graph_tree_root().join(delta_run_relative_path(1));
    fs::remove_file(&run_path).unwrap();
    let err = reconstruct_graph_state(
        &resolved.graph_tree_root(),
        &inventory,
        GraphDeltaJournalLimits::default(),
    )
    .unwrap_err();
    assert_eq!(err.code(), "GF_PROJECT_CORRUPT");
}

#[test]
fn resource_limits_reject_tiny_run_accumulation() {
    let limits = GraphDeltaJournalLimits {
        max_runs: 1,
        ..GraphDeltaJournalLimits::default()
    };
    let root = tempfile::tempdir().unwrap();
    publish_base(root.path());
    publish_graph_delta(
        root.path(),
        &GraphDeltaPublishRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            run_uuid: Uuid::now_v7(),
            operations: sample_ops(),
            limits,
        },
    )
    .unwrap();
    let err = publish_graph_delta(
        root.path(),
        &GraphDeltaPublishRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            run_uuid: Uuid::now_v7(),
            operations: sample_ops(),
            limits,
        },
    )
    .unwrap_err();
    assert_eq!(err.code(), "GF_RESOURCE_LIMIT");
}

#[test]
fn legacy_v1_project_without_deltas_remains_readable() {
    let root = tempfile::tempdir().unwrap();
    publish_base(root.path());
    let resolved = resolve_project_generation(root.path()).unwrap();
    let inventory = resolved.graph_files_inventory().unwrap().unwrap();
    assert!(
        list_delta_runs(&inventory, GraphDeltaJournalLimits::default())
            .unwrap()
            .is_empty()
    );
    let (state, evidence) = reconstruct_graph_state(
        &resolved.graph_tree_root(),
        &inventory,
        GraphDeltaJournalLimits::default(),
    )
    .unwrap();
    assert_eq!(evidence.runs_replayed, 0);
    assert!(
        state
            .nodes
            .contains_key("00000000-0000-7000-8000-000000000001")
    );
    assert_eq!(
        decode_graph_delta_value(
            state
                .node_properties
                .get(&(
                    "00000000-0000-7000-8000-000000000001".into(),
                    "score".into()
                ))
                .unwrap()
        )
        .unwrap(),
        IrLiteral::Int(42)
    );
    assert_eq!(state.node_ids.len(), 2);
    assert_eq!(state.node_timestamps.len(), 2);
    assert_eq!(state.edge_ids.len(), 1);
    assert_eq!(state.edge_created_at.len(), 1);
}

#[test]
fn gapped_run_sequence_in_inventory_fails_closed() {
    let inventory = GraphFilesInventory {
        format: "graphforge-graph-files".into(),
        format_version: 1,
        files: vec![
            GraphFileEntry {
                relative_path: delta_run_relative_path(1),
                byte_length: 1,
                content_sha256: "a".repeat(64),
                role: GraphFileRole::Delta,
            },
            GraphFileEntry {
                relative_path: delta_run_relative_path(3),
                byte_length: 2,
                content_sha256: "b".repeat(64),
                role: GraphFileRole::Delta,
            },
        ],
        file_count: 2,
        total_byte_length: 3,
    };
    let err = list_delta_runs(&inventory, GraphDeltaJournalLimits::default()).unwrap_err();
    assert_eq!(err.code(), "GF_PROJECT_CORRUPT");
}
