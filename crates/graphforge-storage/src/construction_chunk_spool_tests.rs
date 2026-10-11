// Chunk-API initial builds spool their chunks for the bulk builder.
//
// Included at the end of `bulk_builder`, whose helpers it shares. The same
// rows handed to the builder as registered sources (`build_checked`, which
// reads the generation back against the input) are the reference: a spool of
// those rows must publish the same artifacts.

fn spooled_session(root: &TempDir, budgets: GraphConstructionBudgets) -> GraphConstructionSession {
    let mut session = pinned_with(root, budgets);
    session.spool_chunks();
    session
}

/// Resubmit the chunks from the last one the session accepted onward. The last
/// accepted chunk replays; an earlier node chunk may not follow accepted edges,
/// on either route, so a caller resumes from its own record of acceptance.
fn resubmit(
    session: &mut GraphConstructionSession,
    nodes: &[RecordBatch],
    edges: &[RecordBatch],
) -> Result<(), GfError> {
    let accepted = usize::try_from(session.accepted_chunks()).unwrap();
    let ordered = nodes
        .iter()
        .enumerate()
        .map(|(index, batch)| (ConstructionChunkKind::Node, format!("n{index}"), batch))
        .chain(
            edges
                .iter()
                .enumerate()
                .map(|(index, batch)| (ConstructionChunkKind::Edge, format!("e{index}"), batch)),
        )
        .collect::<Vec<_>>();
    for (kind, id, batch) in ordered.into_iter().skip(accepted.saturating_sub(1)) {
        session.append(kind, &id, batch)?;
    }
    Ok(())
}

fn append_all(
    session: &mut GraphConstructionSession,
    nodes: &[RecordBatch],
    edges: &[RecordBatch],
) -> Result<(), GfError> {
    for (index, batch) in nodes.iter().enumerate() {
        session.append(ConstructionChunkKind::Node, &format!("n{index}"), batch)?;
    }
    for (index, batch) in edges.iter().enumerate() {
        session.append(ConstructionChunkKind::Edge, &format!("e{index}"), batch)?;
    }
    Ok(())
}

fn required_scratch_budget(error: &GfError) -> u64 {
    let message = error.to_string();
    message
        .split_once("scratch requires ")
        .and_then(|(_, tail)| tail.split_whitespace().next())
        .and_then(|number| number.parse().ok())
        .unwrap_or_else(|| panic!("expected the scratch admission refusal, got {message}"))
}

/// The spooled path's result or its first refusal.
fn spooled_with(
    budgets: GraphConstructionBudgets,
    nodes: &[RecordBatch],
    edges: &[RecordBatch],
) -> Result<Inventory, GfError> {
    let root = TempDir::new().unwrap();
    let mut session = spooled_session(&root, budgets);
    append_all(&mut session, nodes, edges)?;
    assert!(session.is_spooled());
    session.record_seal_route(SealRoute::Bulk)?;
    session
        .prepare_spooled_bulk_encoding(1, u64::MAX, || false)
        .map(|encoding| inventory(&encoding))
}

fn spooled(nodes: &[RecordBatch], edges: &[RecordBatch]) -> Inventory {
    spooled_with(GraphConstructionBudgets::default(), nodes, edges).unwrap()
}

/// A spooled build under `budget` with the generation read back and its counts
/// checked against the input.
fn spooled_checked(
    budgets: GraphConstructionBudgets,
    budget: u64,
    nodes: &[RecordBatch],
    edges: &[RecordBatch],
) -> (GraphConstructionSession, GraphConstructionEncoding, TempDir) {
    let root = TempDir::new().unwrap();
    let mut session = spooled_session(&root, budgets);
    append_all(&mut session, nodes, edges).unwrap();
    session.record_seal_route(SealRoute::Bulk).unwrap();
    let built = session
        .prepare_spooled_bulk_encoding(1, budget, || false)
        .unwrap();
    assert_counts(&session, &built, nodes, edges);
    assert_reads_back(nodes, edges, &read_back(&session, &built));
    (session, built, root)
}

#[test]
fn spooled_chunks_publish_the_registered_source_bytes_in_any_arrival_order() {
    for order in [identity_order, reversed, scattered] {
        let (nodes, edges) = graph(1_021, 3_001, 700, order);
        let expected = build_checked(&nodes, &edges, 1, 1);
        assert!(expected.len() > 20);
        assert_same(&expected, &spooled(&nodes, &edges));
        let (_, built, _) = spooled_checked(
            GraphConstructionBudgets::default(),
            u64::MAX,
            &nodes,
            &edges,
        );
        assert_same(&expected, &inventory(&built));
    }
}

#[test]
fn a_spooled_graph_larger_than_one_window_publishes_the_registered_source_bytes() {
    let (nodes, edges) = graph(70_001, 140_003, 20_000, scattered);
    assert_same(&build_checked(&nodes, &edges, 1, 1), &spooled(&nodes, &edges));
}

#[test]
fn spooled_nodes_without_edges_publish_the_registered_source_bytes() {
    let (nodes, _) = graph(50, 0, 25, identity_order);
    assert_same(&build_checked(&nodes, &[], 1, 1), &spooled(&nodes, &[]));
}

/// Property-bearing and mixed-schema chunks of both kinds.
fn property_bearing_inputs() -> (Vec<RecordBatch>, Vec<RecordBatch>) {
    let node_uuids = (0..600_u64).map(|i| uuid(0x10, i)).collect::<Vec<_>>();
    let edge_uuids = (0..900_u64).map(|i| uuid(0x20, i)).collect::<Vec<_>>();
    let nodes = vec![
        property_nodes(&node_uuids[0..200], true),
        node_batch_of(&node_uuids[200..350], &vec!["Person"; 150]),
        property_nodes(&node_uuids[350..600], false),
    ];
    let src = |range: std::ops::Range<usize>| {
        range.map(|i| node_uuids[(i * 3) % 600]).collect::<Vec<_>>()
    };
    let dst = |range: std::ops::Range<usize>| {
        range
            .map(|i| node_uuids[(i * 5 + 1) % 600])
            .collect::<Vec<_>>()
    };
    let edges = vec![
        property_edges(&edge_uuids[0..400], &src(0..400), &dst(0..400)),
        edge_batch_of(
            &edge_uuids[400..900],
            &vec!["OWNS"; 500],
            &src(400..900),
            &dst(400..900),
        ),
    ];
    (nodes, edges)
}

#[test]
fn spooled_property_bearing_and_mixed_schema_chunks_publish_the_registered_source_bytes() {
    let (nodes, edges) = property_bearing_inputs();
    let expected = build_checked(&nodes, &edges, 1, 1);
    assert!(
        expected
            .iter()
            .any(|entry| entry.0.starts_with("properties/"))
    );
    assert!(
        expected
            .iter()
            .any(|entry| entry.0.starts_with("edge_properties/"))
    );
    assert_same(&expected, &spooled(&nodes, &edges));
}

/// Tiny chunks are grouped into tasks, and a task is invisible in the output.
#[test]
fn spooled_output_is_independent_of_chunk_size_and_worker_count() {
    let (nodes, edges) = graph(2_003, 9_001, 9_001, scattered);
    let expected = build_checked(&nodes, &edges, 1, 1);
    for (chunk, workers) in [(37, 2), (700, 8), (30_000, 32)] {
        let (nodes, edges) = graph(2_003, 9_001, chunk, scattered);
        let root = TempDir::new().unwrap();
        let mut session = spooled_session(&root, GraphConstructionBudgets::default());
        session.set_cpu_admission(Some(Arc::new(
            cpu_admission::ConstructionCpuAdmission::new(
                std::num::NonZeroUsize::new(workers).unwrap(),
            ),
        )));
        append_all(&mut session, &nodes, &edges).unwrap();
        session.record_seal_route(SealRoute::Bulk).unwrap();
        let built = session
            .prepare_spooled_bulk_encoding(1, u64::MAX, || false)
            .unwrap();
        assert_same(&expected, &inventory(&built));
        assert_counts(&session, &built, &nodes, &edges);
        assert_reads_back(&nodes, &edges, &read_back(&session, &built));
    }
}

fn session_children(root: &TempDir) -> Vec<String> {
    let session = std::fs::read_dir(root.path().join(PRIVATE_ROOT))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut names = std::fs::read_dir(session)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();
    names.sort_unstable();
    names
}

/// A spooled initial build writes one file per chunk and none of the staged
/// artifacts; the bulk builder consumes them and the spool is gone afterwards.
#[test]
fn a_spooled_build_stages_nothing_and_retires_its_spool() {
    let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
    let chunks = nodes.len() + edges.len();
    let root = TempDir::new().unwrap();
    let mut session = spooled_session(&root, GraphConstructionBudgets::default());
    append_all(&mut session, &nodes, &edges).unwrap();
    assert_eq!(session.accepted_chunks(), chunks as u64);
    let spool = root
        .path()
        .join(PRIVATE_ROOT)
        .join(Uuid::from_u128(OPERATION).simple().to_string())
        .join("chunk-spool");
    assert_eq!(std::fs::read_dir(&spool).unwrap().count(), chunks);
    assert!(session_children(&root).iter().all(|name| {
        (name == "chunk-spool" || !name.starts_with("chunk-"))
            && !name.starts_with("receipt-")
            && !name.starts_with("key-")
    }));
    let evidence = session.chunk_spool_evidence();
    assert_eq!(evidence.chunks, chunks as u64);
    assert_eq!(evidence.rows, 1_021 + 3_001);

    session.record_seal_route(SealRoute::Bulk).unwrap();
    session
        .prepare_spooled_bulk_encoding(1, u64::MAX, || false)
        .unwrap();
    let report = session.bulk_build_report();
    assert_eq!((report.nodes, report.edges), (1_021, 3_001));
    assert!(report.passes.contains_key("nodes") && report.passes.contains_key("edges"));
    assert!(!spool.exists());
    assert_eq!(session.evidence().input_batches, 0);
    assert_eq!(session.evidence().parquet_shards, 0);
}

/// An append to a non-empty parent, or on a session that did not ask for the
/// spool, stages exactly as before.
#[test]
fn a_session_that_does_not_ask_for_the_spool_stages() {
    let (nodes, _) = graph(10, 0, 10, identity_order);
    let root = TempDir::new().unwrap();
    let mut session = pinned(&root);
    session
        .append(ConstructionChunkKind::Node, "n0", &nodes[0])
        .unwrap();
    assert!(!session.is_spooled());
    assert_eq!(session.evidence().input_batches, 1);
}

/// Every chunk-time refusal fires at the same call, with the same message,
/// whether the chunk is going to be staged or spooled.
#[test]
fn chunk_time_refusals_are_identical_on_both_routes() {
    let a = uuid(0x10, 1);
    let b = uuid(0x10, 2);
    let e = uuid(0x20, 1);
    let node = node_batch_of(&[a, b], &["Person", "Person"]);
    let edge = edge_batch_of(&[e], &["KNOWS"], &[a], &[b]);
    let duplicate = node_batch_of(&[a, a], &["Person", "Person"]);
    let bad_label = node_batch_of(&[a], &["not an identifier"]);
    let other = node_batch_of(&[a, b], &["Pet", "Pet"]);
    let long_label = "L".repeat(300);
    let too_long = node_batch_of(&[a], &[long_label.as_str()]);
    let empty = RecordBatch::new_empty(CONSTRUCTION_NODE_SCHEMA.clone());
    let cases: Vec<(&str, Vec<(ConstructionChunkKind, &str, &RecordBatch)>)> = vec![
        (
            "invalid chunk id",
            vec![(ConstructionChunkKind::Node, "bad id!", &node)],
        ),
        (
            "empty chunk",
            vec![(ConstructionChunkKind::Node, "n0", &empty)],
        ),
        (
            "duplicate in chunk",
            vec![(ConstructionChunkKind::Node, "n0", &duplicate)],
        ),
        (
            "bad label",
            vec![(ConstructionChunkKind::Node, "n0", &bad_label)],
        ),
        (
            "label too long",
            vec![(ConstructionChunkKind::Node, "n0", &too_long)],
        ),
        (
            "node after edge",
            vec![
                (ConstructionChunkKind::Node, "n0", &node),
                (ConstructionChunkKind::Edge, "e0", &edge),
                (ConstructionChunkKind::Node, "n1", &node),
            ],
        ),
        (
            "conflicting replay",
            vec![
                (ConstructionChunkKind::Node, "n0", &node),
                (ConstructionChunkKind::Node, "n0", &other),
            ],
        ),
        (
            "kind mismatch",
            vec![(ConstructionChunkKind::Edge, "e0", &node)],
        ),
    ];
    for (label, steps) in cases {
        let run = |spool: bool| {
            let root = TempDir::new().unwrap();
            let mut session = pinned(&root);
            if spool {
                session.spool_chunks();
            }
            let mut outcome = Vec::new();
            for (kind, id, batch) in &steps {
                outcome.push(
                    session
                        .append(*kind, id, batch)
                        .map(|receipt| (receipt.chunk_id, receipt.sequence, receipt.rows))
                        .map_err(|error| error.to_string()),
                );
            }
            (outcome, session.accepted_chunks())
        };
        let (staged_outcome, staged_chunks) = run(false);
        let (spooled_outcome, spooled_chunks) = run(true);
        assert!(
            staged_outcome.iter().any(Result::is_err),
            "{label}: the staged path accepted every step"
        );
        assert_eq!(staged_outcome, spooled_outcome, "{label}");
        assert_eq!(staged_chunks, spooled_chunks, "{label}");
    }
}

/// The resource windows are refused at the same call too.
#[test]
fn resource_windows_are_refused_at_the_same_call_on_both_routes() {
    let (nodes, _) = graph(400, 0, 100, identity_order);
    for budgets in [
        GraphConstructionBudgets {
            max_chunks: 2,
            ..GraphConstructionBudgets::default()
        },
        GraphConstructionBudgets {
            max_batch_rows: 50,
            max_run_records: 200,
            ..GraphConstructionBudgets::default()
        },
        GraphConstructionBudgets {
            max_schema_groups: 1,
            ..GraphConstructionBudgets::default()
        },
    ] {
        let run = |spool: bool| {
            let root = TempDir::new().unwrap();
            let mut session = pinned_with(&root, budgets);
            if spool {
                session.spool_chunks();
            }
            let mut batches = nodes.clone();
            batches.push(property_nodes(&[uuid(0x10, 9_001)], true));
            batches
                .iter()
                .enumerate()
                .map(|(index, batch)| {
                    session
                        .append(ConstructionChunkKind::Node, &format!("n{index}"), batch)
                        .map(|_| ())
                        .map_err(|error| error.to_string())
                })
                .collect::<Vec<_>>()
        };
        let staged_outcome = run(false);
        assert!(staged_outcome.iter().any(Result::is_err), "{budgets:?}");
        assert_eq!(staged_outcome, run(true), "{budgets:?}");
    }
}

/// Refusals that need global knowledge fire at seal, as for registered sources.
#[test]
fn global_refusals_fire_at_seal() {
    let a = uuid(0x10, 1);
    let b = uuid(0x10, 2);
    let e = uuid(0x20, 1);
    let node = |uuids: &[[u8; 16]]| node_batch_of(uuids, &vec!["Person"; uuids.len()]);
    let cases: Vec<(Vec<RecordBatch>, Vec<RecordBatch>, &str)> = vec![
        (
            vec![node(&[a, b]), node(&[a])],
            vec![],
            "duplicate identity across construction runs",
        ),
        (
            vec![node(&[a])],
            vec![edge_batch_of(&[e], &["KNOWS"], &[a], &[b])],
            "edge endpoint UUID does not exist",
        ),
        (
            vec![node(&[a])],
            vec![edge_batch_of(&[e], &["KNOWS"], &[a], &[e])],
            "edge endpoint is not a node UUID",
        ),
    ];
    for (nodes, edges, expected) in cases {
        let root = TempDir::new().unwrap();
        let mut session = spooled_session(&root, GraphConstructionBudgets::default());
        append_all(&mut session, &nodes, &edges).unwrap();
        session.record_seal_route(SealRoute::Bulk).unwrap();
        let message = session
            .prepare_spooled_bulk_encoding(1, u64::MAX, || false)
            .unwrap_err()
            .to_string();
        assert!(message.contains(expected), "{message}");
        // The registered-source build refuses the same input in the same words.
        assert_eq!(message, refusal(&nodes, &edges));
    }
}

/// An exact replay of an accepted chunk returns its receipt and accepts
/// nothing new, in the same process and after a reopen.
#[test]
fn an_accepted_chunk_replays_idempotently_across_a_reopen() {
    let (nodes, edges) = graph(300, 600, 200, scattered);
    let root = TempDir::new().unwrap();
    let mut session = spooled_session(&root, GraphConstructionBudgets::default());
    let receipt = session
        .append(ConstructionChunkKind::Node, "n0", &nodes[0])
        .unwrap();
    assert_eq!(
        session
            .append(ConstructionChunkKind::Node, "n0", &nodes[0])
            .unwrap(),
        receipt
    );
    assert_eq!(session.accepted_chunks(), 1);
    drop(session);

    let mut reopened = pinned(&root);
    assert!(reopened.is_spooled());
    assert_eq!(reopened.accepted_chunks(), 1);
    assert_eq!(
        reopened
            .append(ConstructionChunkKind::Node, "n0", &nodes[0])
            .unwrap(),
        receipt
    );
    assert_eq!(reopened.accepted_chunks(), 1);
    assert!(
        reopened
            .append(ConstructionChunkKind::Node, "n0", &nodes[1])
            .unwrap_err()
            .to_string()
            .contains("conflicting construction chunk replay")
    );
    for (index, batch) in nodes.iter().enumerate().skip(1) {
        reopened
            .append(ConstructionChunkKind::Node, &format!("n{index}"), batch)
            .unwrap();
    }
    for (index, batch) in edges.iter().enumerate() {
        reopened
            .append(ConstructionChunkKind::Edge, &format!("e{index}"), batch)
            .unwrap();
    }
    reopened.record_seal_route(SealRoute::Bulk).unwrap();
    let built = reopened
        .prepare_spooled_bulk_encoding(1, u64::MAX, || false)
        .unwrap();
    assert_same(&build_checked(&nodes, &edges, 1, 1), &inventory(&built));
    assert_counts(&reopened, &built, &nodes, &edges);
    assert_reads_back(&nodes, &edges, &read_back(&reopened, &built));
}

const SPOOL_CRASH_MODE: &str = "GF_SPOOL_CRASH_MODE";

/// The killed process. `append` accepts every chunk; `build` accepts them and
/// builds.
#[test]
fn spool_crash_child() {
    let Ok(path) = std::env::var(CRASH_ROOT) else {
        return;
    };
    let Ok(mode) = std::env::var(SPOOL_CRASH_MODE) else {
        return;
    };
    let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
    let mut session = GraphConstructionSession::open(
        Path::new(&path),
        Uuid::from_u128(OPERATION),
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    session.checkpoint.session_now_micros = CLOCK;
    session.spool_chunks();
    append_all(&mut session, &nodes, &edges).unwrap();
    match mode.as_str() {
        "build" => {
            session.record_seal_route(SealRoute::Bulk).unwrap();
            session
                .prepare_spooled_bulk_encoding(1, u64::MAX, || false)
                .unwrap();
        }
        _ => {}
    }
}

fn killed_at(root: &TempDir, mode: &str, failpoint: &str) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("graph_construction::tests::bulk_builder::spool_crash_child")
        .arg("--nocapture")
        .env(CRASH_ROOT, root.path())
        .env(SPOOL_CRASH_MODE, mode)
        .env(
            "GF_CONSTRUCTION_FAILPOINT_COOKIE",
            "graphforge-construction-test-v1",
        )
        .env("GF_CONSTRUCTION_FAILPOINT", failpoint)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(86), "{failpoint}");
}

fn spool_chunk_name(sequence: u64, kind: ConstructionChunkKind) -> String {
    format!("chunk-{sequence:020}-{}.arrow", kind.tag())
}

/// Kill after chunks were accepted, then reopen and continue per the contract:
/// every chunk acknowledged before the kill is still accepted, the chunk in
/// flight is accepted or absent but never torn, resubmitting everything by id
/// is idempotent, and the finished build equals an uninterrupted one.
#[test]
fn a_process_killed_while_accepting_chunks_resumes_and_continues() {
    let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
    let expected = build_checked(&nodes, &edges, 1, 1);
    let total = (nodes.len() + edges.len()) as u64;
    let kinds = |sequence: u64| {
        if sequence < nodes.len() as u64 {
            ConstructionChunkKind::Node
        } else {
            ConstructionChunkKind::Edge
        }
    };
    for sequence in [0_u64, 1, 2, 4, total - 1] {
        let name = spool_chunk_name(sequence, kinds(sequence));
        // (failpoint, chunks that must be accepted after the kill)
        for (failpoint, accepted) in [
            (format!("spool.after_temp_fsync.{name}"), sequence),
            (format!("spool.after_install.{name}"), sequence + 1),
        ] {
            let root = TempDir::new().unwrap();
            killed_at(&root, "append", &failpoint);
            let mut session = pinned(&root);
            assert!(session.is_spooled(), "{failpoint}");
            assert_eq!(session.accepted_chunks(), accepted, "{failpoint}");
            // No torn temporary survives a reopen.
            let spool = root
                .path()
                .join(PRIVATE_ROOT)
                .join(Uuid::from_u128(OPERATION).simple().to_string())
                .join("chunk-spool");
            assert!(std::fs::read_dir(&spool).unwrap().all(|entry| {
                let name = entry.unwrap().file_name().into_string().unwrap();
                name.starts_with("chunk-")
            }));
            // Resubmit everything: the accepted prefix replays, the rest is new.
            resubmit(&mut session, &nodes, &edges).unwrap();
            assert_eq!(session.accepted_chunks(), total, "{failpoint}");
            session.record_seal_route(SealRoute::Bulk).unwrap();
            let built = session
                .prepare_spooled_bulk_encoding(1, u64::MAX, || false)
                .unwrap();
            assert_same(&expected, &inventory(&built));
            assert_counts(&session, &built, &nodes, &edges);
            assert_reads_back(&nodes, &edges, &read_back(&session, &built));
        }
    }
}

/// A rename that survived a kill whose directory sync did not is a chunk that
/// was never acknowledged: it is accepted when present and rewritten when not.
#[test]
fn a_kill_between_rename_and_directory_sync_loses_nothing_acknowledged() {
    let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
    let expected = build_checked(&nodes, &edges, 1, 1);
    let name = spool_chunk_name(2, ConstructionChunkKind::Edge);
    let root = TempDir::new().unwrap();
    killed_at(&root, "append", &format!("spool.after_rename.{name}"));
    let mut session = pinned(&root);
    assert!(session.accepted_chunks() == 2 || session.accepted_chunks() == 3);
    resubmit(&mut session, &nodes, &edges).unwrap();
    session.record_seal_route(SealRoute::Bulk).unwrap();
    let built = session
        .prepare_spooled_bulk_encoding(1, u64::MAX, || false)
        .unwrap();
    assert_same(&expected, &inventory(&built));
}

/// Kill during the build, then rerun to identical bytes. The spool is the
/// input and survives; everything else the build wrote is scratch.
#[test]
fn a_process_killed_during_the_spooled_build_reruns_to_identical_artifacts() {
    let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
    let expected = build_checked(&nodes, &edges, 1, 1);
    for failpoint in [
        "bulk.after_nodes",
        "bulk.after_edges",
        "bulk.after_tables",
        "bulk.after_adjacency",
        "encode.after_inventory_pinned",
        "bulk.after_ordinal",
        "v4_publish.after_artifacts",
        "v4_publish.after_manifest_install",
        "bulk.before_inventory",
        "bulk.after_inventory_before_intent_removal",
    ] {
        let root = TempDir::new().unwrap();
        killed_at(&root, "build", failpoint);
        let mut session = pinned(&root);
        assert_eq!(session.seal_route(), Some(SealRoute::Bulk), "{failpoint}");
        // The route is read back, not recorded again.
        assert_eq!(
            session.record_seal_route(SealRoute::Bulk).unwrap(),
            SealRoute::Bulk
        );
        let rerun = session
            .prepare_spooled_bulk_encoding(1, u64::MAX, || false)
            .unwrap();
        assert_same(&expected, &inventory(&rerun));
        assert_reads_back(&nodes, &edges, &read_back(&session, &rerun));
        let report = session.bulk_build_report();
        assert_eq!((report.nodes, report.edges), (1_021, 3_001), "{failpoint}");
    }
}

/// Recorded before any work and read back: the recorded route survives a
/// reopen, closes the session to further chunks, and the storage-level staged
/// lifecycle refuses to seal a spooled session whose bulk route is not recorded.
#[test]
fn the_seal_route_is_durable_and_the_staged_lifecycle_refuses_a_spooled_session() {
    let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
    let expected = build_checked(&nodes, &edges, 1, 1);

    let root = TempDir::new().unwrap();
    let mut session = spooled_session(&root, GraphConstructionBudgets::default());
    append_all(&mut session, &nodes, &edges).unwrap();
    assert_eq!(session.seal_route(), None);
    // The staged lifecycle does not seal a spool.
    let refused = session.seal().unwrap_err().to_string();
    assert!(refused.contains("seals through the bulk builder"), "{refused}");
    assert_eq!(session.seal_route(), None);
    assert!(session.is_spooled());
    assert_eq!(
        session.record_seal_route(SealRoute::Bulk).unwrap(),
        SealRoute::Bulk
    );
    // Recorded, the session accepts no more chunks.
    let late = session
        .append(ConstructionChunkKind::Node, "late", &nodes[0])
        .unwrap_err()
        .to_string();
    assert!(late.contains("not accepting chunks"), "{late}");
    drop(session);
    let mut reopened = pinned(&root);
    assert_eq!(reopened.seal_route(), Some(SealRoute::Bulk));
    assert!(reopened.is_spooled());
    let built = reopened
        .prepare_spooled_bulk_encoding(1, u64::MAX, || false)
        .unwrap();
    assert_same(&expected, &inventory(&built));
    assert_counts(&reopened, &built, &nodes, &edges);
    assert_reads_back(&nodes, &edges, &read_back(&reopened, &built));
}

/// A spool file from another session, or out of sequence, is refused.
#[test]
fn a_spool_that_does_not_belong_to_its_session_is_refused() {
    let (nodes, _) = graph(300, 0, 100, identity_order);
    let root = TempDir::new().unwrap();
    let mut session = spooled_session(&root, GraphConstructionBudgets::default());
    append_all(&mut session, &nodes, &[]).unwrap();
    drop(session);
    let spool = root
        .path()
        .join(PRIVATE_ROOT)
        .join(Uuid::from_u128(OPERATION).simple().to_string())
        .join("chunk-spool");
    std::fs::remove_file(spool.join(spool_chunk_name(1, ConstructionChunkKind::Node))).unwrap();
    let error = GraphConstructionSession::open(
        root.path(),
        Uuid::from_u128(OPERATION),
        0,
        GraphConstructionBudgets::default(),
    )
    .err()
    .map(|error| error.to_string());
    assert!(
        error
            .as_deref()
            .is_some_and(|message| message.contains("not contiguous")),
        "{error:?}"
    );
}

/// A chunk admitted at exactly the byte window builds. Its decoded copy can
/// report more memory than the batch that was admitted, because the buffers of
/// one IPC body are shared; the builder must not refuse a batch the session
/// already accepted.
#[test]
fn a_chunk_admitted_at_its_exact_byte_window_builds() {
    let uuids = (0..64_u64).map(|i| uuid(0x10, i)).collect::<Vec<_>>();
    let batch = wide_nodes(&uuids, 7);
    let budgets = GraphConstructionBudgets {
        max_batch_bytes: batch.get_array_memory_size(),
        ..GraphConstructionBudgets::default()
    };
    let expected = build_checked_with(budgets, std::slice::from_ref(&batch), &[], 1, 1);
    let built = spooled_with(budgets, std::slice::from_ref(&batch), &[]).unwrap();
    assert_same(&expected, &built);
    // The node pass of a build with the node tables on scratch admits it too.
    let root = TempDir::new().unwrap();
    let mut session = spooled_session(&root, budgets);
    append_all(&mut session, std::slice::from_ref(&batch), &[]).unwrap();
    session.record_seal_route(SealRoute::Bulk).unwrap();
    let _forced =
        crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::set(2, 2);
    let built = session
        .prepare_spooled_bulk_encoding(1, 512 << 20, || false)
        .unwrap();
    assert_same(&expected, &inventory(&built));
    assert_counts(&session, &built, std::slice::from_ref(&batch), &[]);
    assert_reads_back(
        std::slice::from_ref(&batch),
        &[],
        &read_back(&session, &built),
    );
    assert!(session.bulk_build_report().node_partitions > 0);
}

// ----------------------------------------------------------------------
// The over-budget route (#1901 on #1912 and #1920): the same bytes, through
// scratch files, without staging the chunks.
// ----------------------------------------------------------------------

/// Property scratch reserves a larger decode workspace before it reads (#1920),
/// so property-bearing inputs need a budget above it that still falls below the
/// in-memory estimate.
const PROPERTY_SCRATCH_BUDGET: u64 = 960 << 20;

/// A spooled build whose estimate exceeds the budget runs the bulk builder's
/// scratch route over the spool: the scratch is used and removed, nothing is
/// staged, and the bytes are those of the resident build.
#[test]
fn an_over_budget_spooled_build_takes_the_scratch_route_and_publishes_the_resident_bytes() {
    let plain = graph(1_021, 3_001, 700, scattered);
    let property_bearing = property_bearing_inputs();
    for ((nodes, edges), budget) in [
        (plain, SCRATCH_BUDGET),
        (property_bearing, PROPERTY_SCRATCH_BUDGET),
    ] {
        let expected = build_checked(&nodes, &edges, 1, 1);
        let root = TempDir::new().unwrap();
        let mut session = spooled_session(&root, GraphConstructionBudgets::default());
        append_all(&mut session, &nodes, &edges).unwrap();
        session.record_seal_route(SealRoute::Bulk).unwrap();
        let built = session
            .prepare_spooled_bulk_encoding(1, budget, || false)
            .unwrap();
        assert_same(&expected, &inventory(&built));
        assert_counts(&session, &built, &nodes, &edges);
        assert_reads_back(&nodes, &edges, &read_back(&session, &built));
        let report = session.bulk_build_report();
        assert!(
            report.scratch_write_bytes > 0 && report.scratch_read_bytes > 0,
            "the build did not use scratch: {report:?}"
        );
        assert!(!scratch_dir(&session).exists());
        assert_eq!(session.evidence().input_batches, 0);
        assert_eq!(session.evidence().parquet_shards, 0);
    }
}

/// A budget below the node tables no longer replays the spool through the
/// staged path (#1929): the node tables go to scratch, and a budget below the
/// fixed workspace is refused before decoding, as for a registered source. The
/// spool stays intact, so a retry with room builds the resident build's bytes.
#[test]
fn a_budget_below_the_node_tables_keeps_the_bulk_route_for_a_spooled_build() {
    let (nodes, edges) = graph(300, 700, 200, scattered);
    let expected = build_checked(&nodes, &edges, 1, 1);
    let root = TempDir::new().unwrap();
    let mut session = spooled_session(&root, GraphConstructionBudgets::default());
    append_all(&mut session, &nodes, &edges).unwrap();
    session.record_seal_route(SealRoute::Bulk).unwrap();
    let error = session
        .prepare_spooled_bulk_encoding(1, 1, || false)
        .unwrap_err();
    assert!(
        matches!(
            error,
            GfError::Project {
                code: graphforge_core::ProjectErrorCode::ResourceLimit,
                ..
            }
        ),
        "{error}"
    );
    assert!(!scratch_dir(&session).exists());
    let built = session
        .prepare_spooled_bulk_encoding(1, SCRATCH_BUDGET, || false)
        .unwrap();
    assert_same(&expected, &inventory(&built));
    assert_counts(&session, &built, &nodes, &edges);
    assert_reads_back(&nodes, &edges, &read_back(&session, &built));
}

/// A spooled build whose node tables do not fit runs the node passes over the
/// spool: nodes, endpoints, degrees and CSR key ranges go through scratch, the
/// bytes are the resident build's, and nothing is staged.
#[test]
fn a_spooled_build_whose_node_tables_do_not_fit_runs_on_scratch_and_publishes_the_resident_bytes() {
    let plain = graph(1_021, 3_001, 700, scattered);
    let property_bearing = property_bearing_inputs();
    for (nodes, edges) in [plain, property_bearing] {
        let probe = plan(&nodes, &edges, 2);
        // The raw source's retained footer/schema bytes remain in this bound;
        // the spool itself drops those schemas after accepting the chunks.
        let budget = probe.scratch_floor_bytes() + 1;
        let expected = build_checked(&nodes, &edges, 1, 1);
        let root = TempDir::new().unwrap();
        let mut session = spooled_session(&root, GraphConstructionBudgets::default());
        append_all(&mut session, &nodes, &edges).unwrap();
        let _forced =
            crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::set(4, 3);
        session.record_seal_route(SealRoute::Bulk).unwrap();
        let property_bearing =
            nodes[0].num_columns() > 2 || edges.iter().any(|batch| batch.num_columns() > 4);
        let built = if property_bearing {
            let refusal = session
                .prepare_spooled_bulk_encoding(1, budget, || false)
                .unwrap_err();
            let required = required_scratch_budget(&refusal);
            assert!(required > budget, "{refusal}");
            session
                .prepare_spooled_bulk_encoding(1, required, || false)
                .unwrap()
        } else {
            session
                .prepare_spooled_bulk_encoding(1, budget, || false)
                .unwrap()
        };
        assert_same(&expected, &inventory(&built));
        assert_counts(&session, &built, &nodes, &edges);
        assert_reads_back(&nodes, &edges, &read_back(&session, &built));
        let report = session.bulk_build_report();
        assert!(report.node_partitions > 0, "{report:?}");
        assert!(report.node_scratch_write_bytes > 0, "{report:?}");
        assert!(report.endpoint_scratch_write_bytes > 0, "{report:?}");
        assert_eq!(
            report.node_scratch_read_bytes, report.node_scratch_write_bytes,
            "{report:?}"
        );
        assert_eq!(
            report.endpoint_scratch_read_bytes, report.endpoint_scratch_write_bytes,
            "{report:?}"
        );
        assert!(!scratch_dir(&session).exists());
        assert_eq!(session.evidence().input_batches, 0);
        assert_eq!(session.evidence().parquet_shards, 0);
    }
}

// ----------------------------------------------------------------------
// Authentication: a spooled chunk is replayed only if it is what was accepted.
// ----------------------------------------------------------------------

fn spool_directory(root: &TempDir) -> std::path::PathBuf {
    root.path()
        .join(PRIVATE_ROOT)
        .join(Uuid::from_u128(OPERATION).simple().to_string())
        .join("chunk-spool")
}

/// Replace each `from` with its `to` (same length) in the first spool file of
/// `kind` that holds the first, leaving a file of the same size that is still
/// valid Arrow IPC.
fn corrupt_in_place(root: &TempDir, kind: ConstructionChunkKind, edits: &[(Vec<u8>, Vec<u8>)]) {
    let (from, to) = &edits[0];
    assert_eq!(from.len(), to.len());
    let mut names = std::fs::read_dir(spool_directory(root))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(&format!("-{}.arrow", kind.tag())))
        })
        .collect::<Vec<_>>();
    names.sort();
    for path in names {
        let mut bytes = std::fs::read(&path).unwrap();
        if !bytes
            .windows(from.len())
            .any(|window| window == from.as_slice())
        {
            continue;
        }
        let length = bytes.len();
        for (from, to) in edits {
            assert_eq!(from.len(), to.len());
            let at = bytes
                .windows(from.len())
                .position(|window| window == from.as_slice())
                .expect("every edit's bytes are present");
            bytes[at..at + from.len()].copy_from_slice(to);
        }
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), length as u64);
        let reader =
            arrow::ipc::reader::FileReader::try_new(std::fs::File::open(&path).unwrap(), None)
                .unwrap();
        assert!(
            reader.into_iter().all(|batch| batch.is_ok()),
            "the corruption must leave valid Arrow IPC"
        );
        return;
    }
    panic!("no spool file holds the bytes to corrupt");
}

/// One corruption: the kind of chunk file it edits, the bytes it replaces and
/// the inputs it applies to.
struct Corruption {
    name: &'static str,
    kind: ConstructionChunkKind,
    edits: Vec<(Vec<u8>, Vec<u8>)>,
    inputs: (Vec<RecordBatch>, Vec<RecordBatch>),
}

fn corruptions() -> Vec<Corruption> {
    let node_only = graph(40, 0, 25, identity_order);
    let both = graph(40, 120, 25, identity_order);
    let property_bearing = property_bearing_inputs();
    // ["a, b", "c"] becomes ["a", "b, c"]: same bytes' length, same display text.
    let collision = (vec![list_nodes()], Vec::new());
    vec![
        Corruption {
            name: "list property that prints alike",
            kind: ConstructionChunkKind::Node,
            edits: vec![
                (b"a, bc".to_vec(), b"ab, c".to_vec()),
                (
                    [0, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0].to_vec(),
                    [0, 0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 0].to_vec(),
                ),
            ],
            inputs: collision,
        },
        // Valid but different: an unused identity replaces a node's.
        Corruption {
            name: "node identity",
            kind: ConstructionChunkKind::Node,
            edits: vec![(uuid(0x10, 3).to_vec(), uuid(0x99, 3).to_vec())],
            inputs: node_only,
        },
        // Valid but different: another label.
        Corruption {
            name: "node label",
            kind: ConstructionChunkKind::Node,
            edits: vec![(b"City".to_vec(), b"Cite".to_vec())],
            inputs: graph(40, 0, 25, identity_order),
        },
        // Valid but different: an edge now ends at another existing node.
        Corruption {
            name: "edge endpoint",
            kind: ConstructionChunkKind::Edge,
            edits: vec![(uuid(0x10, 1).to_vec(), uuid(0x10, 2).to_vec())],
            inputs: both,
        },
        // Valid but different: a property value.
        Corruption {
            name: "property value",
            kind: ConstructionChunkKind::Node,
            edits: vec![(b"n7n8".to_vec(), b"n7n9".to_vec())],
            inputs: property_bearing,
        },
    ]
}

/// A spooled chunk that changed after it was acknowledged is refused, even when
/// the file is the same size and still valid Arrow IPC, whether the session
/// accepted the chunks or reopened them.
#[test]
fn a_same_size_valid_ipc_corruption_of_a_spooled_chunk_is_refused() {
    // Every case runs, so a weakened check names each corruption it misses.
    let mut missed = Vec::new();
    for case in corruptions() {
        let (nodes, edges) = &case.inputs;
        for reopened in [false, true] {
            let label = format!("{} reopened={reopened}", case.name);
            let root = TempDir::new().unwrap();
            let mut session = spooled_session(&root, GraphConstructionBudgets::default());
            append_all(&mut session, nodes, edges).unwrap();
            corrupt_in_place(&root, case.kind, &case.edits);
            if reopened {
                drop(session);
                session = pinned_with(&root, GraphConstructionBudgets::default());
                assert!(session.is_spooled(), "{label}");
            }
            session.record_seal_route(SealRoute::Bulk).unwrap();
            match session
                .prepare_spooled_bulk_encoding(1, u64::MAX, || false)
                .err()
            {
                Some(error)
                    if error
                        .to_string()
                        .contains("differs from its acknowledged digest") => {}
                other => missed.push(format!("{label}: {other:?}")),
            }
            // Nothing was pinned from it.
            assert!(
                session.checkpoint.encoding_inventory_sha256.is_none(),
                "{label}"
            );
        }
    }
    assert!(missed.is_empty(), "corruptions not refused: {missed:#?}");
}

/// The same refusal on the scratch route, whose passes read the spool again.
#[test]
fn a_corrupted_spooled_chunk_is_refused_on_the_scratch_route() {
    let mut missed = Vec::new();
    for case in corruptions() {
        let (nodes, edges) = &case.inputs;
        let root = TempDir::new().unwrap();
        let mut session = spooled_session(&root, GraphConstructionBudgets::default());
        append_all(&mut session, nodes, edges).unwrap();
        corrupt_in_place(&root, case.kind, &case.edits);
        session.record_seal_route(SealRoute::Bulk).unwrap();
        let budget = if case.name.contains("property") {
            PROPERTY_SCRATCH_BUDGET
        } else {
            SCRATCH_BUDGET
        };
        match session
            .prepare_spooled_bulk_encoding(1, budget, || false)
            .err()
        {
            Some(error)
                if error
                    .to_string()
                    .contains("differs from its acknowledged digest") => {}
            other => missed.push(format!("{}: {other:?}", case.name)),
        }
        assert!(!scratch_dir(&session).exists(), "{}", case.name);
    }
    assert!(missed.is_empty(), "corruptions not refused: {missed:#?}");
}

/// One node whose `tags` property is the list `["a, b", "c"]`.
fn list_nodes() -> RecordBatch {
    use arrow::array::{ListBuilder, StringBuilder};
    let mut tags = ListBuilder::new(StringBuilder::new());
    tags.values().append_value("a, b");
    tags.values().append_value("c");
    tags.append(true);
    let tags = tags.finish();
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("tags", tags.data_type().clone(), true)));
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(fixed(&[uuid(0x10, 0)])),
            Arc::new(StringArray::from(vec!["Person"])),
            Arc::new(tags),
        ],
    )
    .unwrap()
}

/// The chunk digest is over typed values: nested values that print alike but
/// differ digest differently.
#[test]
fn the_chunk_digest_separates_values_that_print_alike() {
    use arrow::array::{ListBuilder, StringBuilder};
    let tagged = |first: &[&str], second: &[&str]| {
        let mut tags = ListBuilder::new(StringBuilder::new());
        for list in [first, second] {
            for value in list {
                tags.values().append_value(value);
            }
            tags.append(true);
        }
        let tags = tags.finish();
        let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
        fields.push(Arc::new(Field::new("tags", tags.data_type().clone(), true)));
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(fixed(&[uuid(0x10, 0), uuid(0x10, 1)])),
                Arc::new(StringArray::from(vec!["Person", "Person"])),
                Arc::new(tags),
            ],
        )
        .unwrap()
    };
    let digest = |batch: &RecordBatch| {
        super::intake::logical_batch_digest(ConstructionChunkKind::Node, batch).unwrap()
    };
    let base = digest(&tagged(&["a, b", "c"], &["d"]));
    // Same display text `[a, b, c]`, different values.
    assert_ne!(base, digest(&tagged(&["a", "b, c"], &["d"])));
    // A value moved across the row boundary.
    assert_ne!(base, digest(&tagged(&["a, b"], &["c", "d"])));
    // Equal values digest equally.
    assert_eq!(base, digest(&tagged(&["a, b", "c"], &["d"])));
}

/// The same chunks as registered sources (what an import session hands the
/// builder) and as a spool publish the same bytes, so the chunk API and an
/// import session are equivalent.
#[test]
fn spooled_chunks_publish_the_bytes_of_registered_sources() {
    for order in [identity_order, scattered] {
        let (nodes, edges) = graph(1_021, 3_001, 700, order);
        assert_same(&bulk(&nodes, &edges), &spooled(&nodes, &edges));
    }
    let (nodes, edges) = property_bearing_inputs();
    assert_same(&bulk(&nodes, &edges), &spooled(&nodes, &edges));
}
