use super::*;

// ------------------------------------------------------------------
// Node tables on scratch (#1929). With the budget just above the fixed
// workspace, no node table fits: node identities, endpoint resolution,
// degrees and CSR key ranges are built per node-UUID range partition.
// The resident build is the specification.

/// Above the fixed workspace and below any node table.
fn node_scratch_plan(
    nodes: &[RecordBatch],
    edges: &[RecordBatch],
    per_task: usize,
) -> BulkBuildPlan<'static> {
    let mut plan = plan(nodes, edges, per_task);
    plan.memory_budget = Some(plan.scratch_floor_bytes() + 1);
    // A build with no node at all has no node table to push over a budget.
    let has_nodes = nodes.iter().any(|batch| batch.num_rows() > 0);
    assert_eq!(
        plan.route(),
        if has_nodes {
            crate::BulkRoute::ScratchNodes
        } else {
            crate::BulkRoute::Scratch
        }
    );
    plan
}

fn node_scratch_run(
    nodes: &[RecordBatch],
    edges: &[RecordBatch],
    per_task: usize,
    workers: usize,
    (edge_partitions, csr_partitions): (usize, usize),
) -> Result<ScratchRun, GfError> {
    let _forced = crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::set(
        edge_partitions,
        csr_partitions,
    );
    let root = TempDir::new().unwrap();
    let mut session = pinned(&root);
    session.set_cpu_admission(Some(Arc::new(
        cpu_admission::ConstructionCpuAdmission::new(std::num::NonZeroUsize::new(workers).unwrap()),
    )));
    let encoding =
        session.prepare_bulk_encoding(1, &node_scratch_plan(nodes, edges, per_task), || false)?;
    Ok(ScratchRun {
        inventory: inventory(&encoding),
        report: session.bulk_build_report(),
        scratch_left: scratch_dir(&session).exists(),
    })
}

fn clustered_node_graph(
    node_count: usize,
    edge_count: usize,
) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
    let (nodes, edges, _) = skewed_node_graph(node_count, edge_count);
    (nodes, edges)
}

/// Put most rows in one task that the 64-task splitter sample omits. The
/// surrounding one-row tasks give the sampler a real UUID range while the
/// broad footer bounds still exercise adaptive refinement.
fn skewed_node_graph(
    node_count: usize,
    edge_count: usize,
) -> (Vec<RecordBatch>, Vec<RecordBatch>, Vec<[u8; 16]>) {
    const SENTINELS: usize = 257;
    assert!(node_count > SENTINELS);
    let cluster_rows = node_count - SENTINELS;
    let mut node_ids = (0..cluster_rows - 1)
        .map(|index| {
            let mut value = [0x80; 16];
            value[6] = 0x70;
            value[8] = 0x80;
            value[14..].copy_from_slice(&(index as u16).to_be_bytes());
            value
        })
        .collect::<Vec<_>>();
    node_ids.push([0xee; 16]);
    node_ids[cluster_rows - 1][6] = 0x70;
    node_ids[cluster_rows - 1][8] = 0x80;
    let mut nodes = Vec::with_capacity(SENTINELS + 1);
    let sentinel = |index: usize| {
        let mut value = [0; 16];
        value[..2].copy_from_slice(&u16::try_from(index + 1).unwrap().to_be_bytes());
        value[6] = 0x70;
        value[8] = 0x80;
        value
    };
    for index in 0..3 {
        nodes.push(node_batch_of(&[sentinel(index)], &["Person"]));
    }
    let cluster = &node_ids[..cluster_rows];
    nodes.push(node_batch_of(cluster, &vec!["Person"; cluster.len()]));
    for index in 3..SENTINELS {
        nodes.push(node_batch_of(&[sentinel(index)], &["Person"]));
    }
    node_ids.extend((0..SENTINELS).map(sentinel));
    let edge_ids = (0..edge_count as u64)
        .map(|index| uuid(0xf0, index))
        .collect::<Vec<_>>();
    let relationships = (0..edge_count)
        .map(|index| ["KNOWS", "LIVES_IN", "OWNS"][index % 3])
        .collect::<Vec<_>>();
    let edges = vec![edge_batch_of(
        &edge_ids,
        &relationships,
        &(0..edge_count)
            .map(|index| node_ids[(index * 7) % node_count])
            .collect::<Vec<_>>(),
        &(0..edge_count)
            .map(|index| node_ids[(index * 11 + 3) % node_count])
            .collect::<Vec<_>>(),
    )];
    (nodes, edges, node_ids)
}

fn scratch_refusal(nodes: &[RecordBatch], edges: &[RecordBatch]) -> String {
    let mut plan = plan(nodes, edges, 2);
    plan.memory_budget = Some(plan.node_tables_resident_bytes());
    assert_eq!(plan.route(), crate::BulkRoute::Scratch);
    let root = TempDir::new().unwrap();
    let mut session = pinned(&root);
    session.set_cpu_admission(Some(Arc::new(
        cpu_admission::ConstructionCpuAdmission::new(std::num::NonZeroUsize::new(4).unwrap()),
    )));
    session
        .prepare_bulk_encoding(1, &plan, || false)
        .unwrap_err()
        .to_string()
}

/// Every scratch block is written once and read once, and each family of
/// bytes is accounted separately: base payloads, then refinement and spools.
fn assert_node_scratch_traffic(report: &crate::BulkBuildReport, nodes: u64, edges: u64) {
    let within = |actual: u64, payload: u64, what: &str| {
        assert!(
            actual >= payload && actual <= payload + payload / 16 + 64 * 1024,
            "{what}: wrote {actual} for {payload} bytes of records: {report:?}"
        );
    };
    // A node record scatters once (20) and is written sorted (20).
    within(
        report.node_scratch_write_bytes - report.node_refinement_write_bytes,
        nodes * (20 + 20),
        "nodes",
    );
    // Per edge: two 33-byte references and one 16-byte identity probe to
    // the node leaves, and two 37-byte resolved endpoints back.
    within(
        report.endpoint_scratch_write_bytes,
        edges * (2 * 33 + 16 + 2 * 37),
        "endpoints",
    );
    // The edge side writes one 28-byte raw record and two 16-byte CSR
    // records per edge. Each record family uses framed blocks; when its
    // staging holds only one record, each record may add one 8-byte
    // header. Refinement and relation-spool writes are removed separately
    // below, so this bounds only the initial raw and CSR record families.
    let edge_payload = edges * (28 + 2 * 16);
    let maximum_headers = edges * 3 * 8;
    let edge_writes = report.scratch_write_bytes
        - report.edge_refinement_write_bytes
        - report.csr_spool_write_bytes
        - report.node_scratch_write_bytes
        - report.endpoint_scratch_write_bytes;
    assert!(
        edge_writes >= edge_payload && edge_writes <= edge_payload + maximum_headers,
        "edge record families wrote {edge_writes} bytes for {edge_payload} payload bytes +             and at most {maximum_headers} frame-header bytes: {report:?}"
    );
    assert_eq!(report.scratch_read_bytes, report.scratch_write_bytes);
    assert_eq!(
        report.node_scratch_read_bytes,
        report.node_scratch_write_bytes
    );
    assert_eq!(
        report.endpoint_scratch_read_bytes,
        report.endpoint_scratch_write_bytes
    );
    // Early reclamation keeps the occupied peak behind the cumulative
    // writes, on the skewed and refined builds too.
    assert!(report.scratch_peak_occupied_bytes > 0, "{report:?}");
    assert!(
        report.scratch_peak_occupied_bytes < report.scratch_write_bytes,
        "peak {} must sit below the {} cumulative bytes: {report:?}",
        report.scratch_peak_occupied_bytes,
        report.scratch_write_bytes
    );
    assert_eq!(
        report.node_refinement_read_bytes > 0,
        report.node_refinement_write_bytes > 0
    );
    assert_eq!(report.csr_spool_read_bytes, report.csr_spool_write_bytes);
}

#[test]
fn node_tables_on_scratch_publish_the_in_memory_bytes_at_budget_derived_partitions() {
    let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
    let expected = bulk_with(&nodes, &edges, 2, 4).unwrap();
    assert_same(&staged(&nodes, &edges), &expected);
    for (per_task, workers, partitions) in [
        (2, 1, (1, 1)),
        (2, 4, (2, 3)),
        (3, 2, (7, 5)),
        (1, 8, (16, 16)),
        (4, 3, (33, 2)),
        (5, 2, (1, 9)),
    ] {
        let run = node_scratch_run(&nodes, &edges, per_task, workers, partitions).unwrap();
        assert_eq!(
            expected, run.inventory,
            "edge/CSR partitions {partitions:?} workers {workers}"
        );
        assert!(!run.scratch_left, "scratch must be deleted on completion");
        let report = &run.report;
        assert!(report.node_partitions >= 1, "{report:?}");
        assert!(report.csr_partitions >= partitions.1 as u64);
        assert_eq!((report.nodes, report.edges), (1_021, 3_001));
        assert_node_scratch_traffic(report, 1_021, 3_001);
        // The endpoint pass ran and the edge pass resolved nothing itself.
        assert!(report.passes.contains_key("endpoints"), "{report:?}");
    }
}

#[test]
fn node_and_edge_windows_that_straddle_partitions_publish_the_in_memory_files() {
    let (nodes, edges) = graph(70_001, 140_003, 20_000, scattered);
    let expected = bulk_with(&nodes, &edges, 2, 4).unwrap();
    for prefix in ["topology/nodes/", "topology/edges/"] {
        let files = expected
            .iter()
            .filter(|entry| entry.0.starts_with(prefix))
            .count();
        assert!(files >= 2, "{prefix}: {files} files");
    }
    let _gate = crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::with_gate(
        32 << 10,
    );
    for partitions in [(2, 2), (5, 3), (64, 9)] {
        let run = node_scratch_run(&nodes, &edges, 2, 4, partitions).unwrap();
        assert_same(&expected, &run.inventory);
        assert!(run.report.node_partitions > 1, "{:?}", run.report);
        assert_node_scratch_traffic(&run.report, 70_001, 140_003);
    }
}

#[test]
fn csr_shards_over_scratch_node_tables_publish_the_in_memory_shards() {
    // Small shards: boundaries fall mid-node, across node leaves and across
    // CSR partitions, in every relation group and both directions.
    let _limits = ShardLimits::set(7, 3);
    let (nodes, edges) = graph(257, 2_000, 700, scattered);
    let expected = bulk_with(&nodes, &edges, 2, 4).unwrap();
    let shards = expected
        .iter()
        .filter(|entry| entry.0.ends_with(".csr"))
        .count();
    assert!(shards >= 200, "{shards} shards");
    let _gate =
        crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::with_gate(8 << 10);
    for partitions in [(1, 1), (3, 2), (6, 5), (9, 40)] {
        let run = node_scratch_run(&nodes, &edges, 2, 4, partitions).unwrap();
        assert_same(&expected, &run.inventory);
        assert!(run.report.node_partitions > 1, "{:?}", run.report);
    }
}

#[test]
fn a_hub_over_scratch_node_tables_splits_by_edge_id() {
    let _limits = ShardLimits::set(17, 100);
    let node_uuids = [uuid(0x10, 0), uuid(0x10, 1)];
    let nodes = vec![node_batch_of(&node_uuids, &["Person", "Person"])];
    let edge_uuids = (0..3001).map(|i| uuid(0x20, i)).collect::<Vec<_>>();
    let edges = vec![edge_batch_of(
        &edge_uuids,
        &vec!["KNOWS"; 3001],
        &vec![node_uuids[0]; 3001],
        &vec![node_uuids[1]; 3001],
    )];
    let expected = bulk_with(&nodes, &edges, 1, 2).unwrap();
    let _gate = crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::with_gate(
        32 << 10,
    );
    let run = node_scratch_run(&nodes, &edges, 1, 2, (16, 2)).unwrap();
    assert_same(&expected, &run.inventory);
    assert_node_scratch_traffic(&run.report, 2, 3001);
    assert_eq!(run.report.csr_spool_write_bytes, 0);
    assert!(!run.scratch_left);
}

#[test]
fn hubs_and_missing_sides_publish_the_in_memory_bytes() {
    let _limits = ShardLimits::set(5, 100);
    let node_uuids = (0..40_u64).map(|i| uuid(0x10, i)).collect::<Vec<_>>();
    let nodes = vec![node_batch_of(&node_uuids, &vec!["Person"; 40])];
    let edge_uuids = (0..60_u64).map(|i| uuid(0x20, i)).collect::<Vec<_>>();
    let src = (0..60).map(|i| {
        if i % 2 == 0 {
            node_uuids[0]
        } else {
            node_uuids[2 + i % 30]
        }
    });
    let dst = (0..60).map(|i| {
        if i % 2 == 0 {
            node_uuids[3 + i % 30]
        } else {
            node_uuids[1]
        }
    });
    let edges = vec![edge_batch_of(
        &edge_uuids,
        &vec!["KNOWS"; 60],
        &src.collect::<Vec<_>>(),
        &dst.collect::<Vec<_>>(),
    )];
    let expected = bulk_with(&nodes, &edges, 1, 2).unwrap();
    // Far more edge/CSR partitions than rows: most are empty.
    for partitions in [(2, 2), (64, 64), (200, 1)] {
        let run = node_scratch_run(&nodes, &edges, 1, 2, partitions).unwrap();
        assert_same(&expected, &run.inventory);
    }
    // Nodes and no edges.
    let expected = bulk_with(&nodes, &[], 1, 2).unwrap();
    let run = node_scratch_run(&nodes, &[], 1, 2, (4, 4)).unwrap();
    assert_same(&expected, &run.inventory);
    assert_eq!(run.report.edges, 0);
    assert_node_scratch_traffic(&run.report, 40, 0);
    // A single node with a self-loop.
    let one = vec![node_batch_of(&node_uuids[..1], &["Person"])];
    let loops = vec![edge_batch_of(
        &edge_uuids[..1],
        &["KNOWS"],
        &node_uuids[..1],
        &node_uuids[..1],
    )];
    let expected = bulk_with(&one, &loops, 1, 2).unwrap();
    let run = node_scratch_run(&one, &loops, 1, 2, (3, 3)).unwrap();
    assert_same(&expected, &run.inventory);
}

#[test]
fn oversized_node_ranges_refine_and_publish_the_in_memory_bytes() {
    // One unsampled task has a broad footer span around many clustered
    // identities, so both sampled and footer-derived splitters need refine.
    let _limits = ShardLimits::set(31, 100);
    let (nodes, edges, node_ids) = skewed_node_graph(3_001, 500);
    let expected = bulk_with(&nodes, &edges, 1, 2).unwrap();
    assert_same(&staged(&nodes, &edges), &expected);
    for bounded in [false, true] {
        for (gate, parts) in [(32 << 10, (4, 2)), (64 << 10, (1, 2))] {
            let _gate =
                crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::with_gate(
                    gate,
                );
            let _parts =
                crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::set(
                    parts.0, parts.1,
                );
            let mut plan = node_scratch_plan(&nodes, &edges, 1);
            if bounded {
                plan.nodes[0].reader = Arc::new(Bounded(Memory {
                    batches: nodes.clone(),
                    per_task: 1,
                }));
            }
            let root = TempDir::new().unwrap();
            let mut session = pinned(&root);
            let encoding = session.prepare_bulk_encoding(1, &plan, || false).unwrap();
            assert_same(&expected, &inventory(&encoding));
            let report = session.bulk_build_report();
            assert!(report.node_refinement_steps > 0, "{report:?}");
            assert!(report.node_refinement_write_bytes > 0, "{report:?}");
            // Every leaf fits the reservation its worker makes: 40 bytes per
            // node, and 144 per edge while its resolved endpoints are joined.
            let share = gate / (2 * report.scratch_concurrency);
            assert!(report.largest_node_partition * 40 <= share, "{report:?}");
            assert!(report.largest_edge_partition * 144 <= share, "{report:?}");
            assert_node_scratch_traffic(&report, 3001, 500);
            assert!(!scratch_dir(&session).exists());
        }
    }
    // A duplicate inside the large cluster is a duplicate node, not an overflow.
    let mut duplicate_nodes = nodes.clone();
    let mut duplicate_cluster = node_ids[..3_001 - 129].to_vec();
    duplicate_cluster[1] = duplicate_cluster[0];
    duplicate_nodes[1] =
        node_batch_of(&duplicate_cluster, &vec!["Person"; duplicate_cluster.len()]);
    let _gate = crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::with_gate(
        32 << 10,
    );
    let error = node_scratch_run(&duplicate_nodes, &[], 1, 1, (1, 2))
        .err()
        .unwrap();
    assert!(
        error
            .to_string()
            .contains("duplicate identity across construction runs (node)"),
        "{error}"
    );
}

#[test]
fn scratch_peak_occupancy_stays_below_cumulative_writes_on_a_refined_build() {
    // A skewed build whose node and edge ranges refine: leaves, probes and
    // refs leave the occupancy as soon as their reads complete, so the
    // build never holds everything its counters moved, and the relation
    // spools survive until the pass that consumes them.
    let (nodes, edges) = clustered_node_graph(1_021, 3_001);
    let expected = bulk_with(&nodes, &edges, 2, 4).unwrap();
    let _gate = crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::with_gate(
        32 << 10,
    );
    let run = node_scratch_run(&nodes, &edges, 2, 4, (7, 5)).unwrap();
    assert_same(&expected, &run.inventory);
    let report = &run.report;
    assert!(report.node_refinement_write_bytes > 0, "{report:?}");
    assert!(report.edge_refinement_write_bytes > 0, "{report:?}");
    assert_node_scratch_traffic(report, 1_021, 3_001);
    // The spool was written before it was read and the shards are the
    // canonical ones: the spool survived to its final read.
    assert_eq!(report.csr_spool_read_bytes, report.csr_spool_write_bytes);
    assert!(report.csr_spool_write_bytes > 0, "{report:?}");
    // The peak is a distinct figure: everything held at once, never the
    // cumulative traffic, and at least one whole edge partition at a time.
    assert!(
        report.scratch_peak_occupied_bytes >= report.largest_edge_partition * 28,
        "{report:?}"
    );
    assert!(
        report.scratch_peak_occupied_bytes < report.scratch_write_bytes,
        "peak {} must sit below the {} cumulative bytes: {report:?}",
        report.scratch_peak_occupied_bytes,
        report.scratch_write_bytes
    );
    assert!(!run.scratch_left);
}

#[test]
fn every_global_refusal_fires_identically_over_scratch_node_tables() {
    let a = uuid(0x10, 1);
    let b = uuid(0x10, 2);
    let c = uuid(0x10, 3);
    let e = uuid(0x20, 1);
    let f = uuid(0x20, 2);
    let node = |uuids: &[[u8; 16]]| node_batch_of(uuids, &vec!["Person"; uuids.len()]);
    let edge = |id: [u8; 16], from: [u8; 16], to: [u8; 16]| {
        edge_batch_of(&[id], &["KNOWS"], &[from], &[to])
    };
    let cases: Vec<(Vec<RecordBatch>, Vec<RecordBatch>)> = vec![
        // A node UUID repeated within and across batches.
        (vec![node(&[a, b]), node(&[a])], vec![]),
        (vec![node(&[a, a])], vec![]),
        // An edge UUID repeated across batches.
        (vec![node(&[a, b])], vec![edge(e, a, b), edge(e, b, a)]),
        // An edge UUID equal to a node UUID.
        (vec![node(&[a, b])], vec![edge(a, a, b)]),
        (vec![node(&[a, b, c])], vec![edge(e, a, b), edge(c, a, b)]),
        // A repeated edge UUID that is also a node UUID: the repeat is
        // reported first.
        (vec![node(&[a, b])], vec![edge(a, a, b), edge(a, b, a)]),
        // An endpoint that names no UUID at all, as source and as target.
        (vec![node(&[a])], vec![edge(e, a, b)]),
        (vec![node(&[a])], vec![edge(e, b, a)]),
        // An endpoint that names an edge, not a node.
        (vec![node(&[a])], vec![edge(e, a, e), edge(f, a, a)]),
        // An identity error outranks a missing endpoint, as in memory.
        (vec![node(&[a])], vec![edge(e, a, b), edge(e, a, a)]),
        (vec![node(&[a])], vec![edge(a, a, b)]),
        // Edges and no node at all.
        (vec![], vec![edge(e, a, b)]),
        // A label that is not an identifier, a relation that is not. (The
        // version of a UUID is checked where input is normalized; the API
        // suite runs that refusal on this route.)
        (vec![node_batch_of(&[a], &["not an identifier"])], vec![]),
        (
            vec![node(&[a, b])],
            vec![edge_batch_of(&[e], &["not an identifier"], &[a], &[b])],
        ),
        // Nothing at all.
        (vec![], vec![]),
    ];
    for (index, (nodes, edges)) in cases.iter().enumerate() {
        let in_memory = refusal(nodes, edges);
        if nodes.iter().all(|batch| batch.num_rows() == 0) {
            // With edges but no nodes, compare an actual ordinary-scratch
            // plan. An empty graph stays on the resident route.
            if edges.iter().any(|batch| batch.num_rows() > 0) {
                assert_eq!(in_memory, scratch_refusal(nodes, edges));
            } else {
                assert!(in_memory.contains("no identities"), "{in_memory}");
            }
            continue;
        }
        for partitions in [(1, 1), (4, 3)] {
            let error = node_scratch_run(nodes, edges, 2, 4, partitions)
                .err()
                .unwrap_or_else(|| panic!("case {index} was accepted"))
                .to_string();
            assert_eq!(in_memory, error, "case {index} partitions {partitions:?}");
        }
    }
}

#[test]
fn a_typed_ontology_refuses_unbound_owners_over_scratch_node_tables() {
    let authority = super::encoding_publication::tests::semantic_authority(
        graphforge_core::OntologyMode::Strict,
    );
    let open = |root: &TempDir| {
        let mut session = GraphConstructionSession::open_with_semantic_authority(
            root.path(),
            Uuid::from_u128(OPERATION),
            0,
            authority.clone(),
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session.checkpoint.session_now_micros = CLOCK;
        session
    };
    // A label the ontology does not bind.
    let ids = (0..4_u64).map(|i| uuid(0x10, i)).collect::<Vec<_>>();
    let unbound = [node_batch_of(&ids, &vec!["Unbound"; 4])];
    let root = TempDir::new().unwrap();
    let resident = open(&root)
        .prepare_bulk_encoding(1, &plan(&unbound, &[], 1), || false)
        .err()
        .unwrap()
        .to_string();
    let root = TempDir::new().unwrap();
    let mut session = open(&root);
    let on_scratch = session
        .prepare_bulk_encoding(1, &node_scratch_plan(&unbound, &[], 1), || false)
        .err()
        .unwrap()
        .to_string();
    assert_eq!(resident, on_scratch);
    assert!(!scratch_dir(&session).exists());
}

#[test]
fn a_cancelled_node_scratch_build_leaves_no_scratch_and_the_rerun_is_identical() {
    let _gate = crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::with_gate(
        32 << 10,
    );
    let (nodes, edges) = clustered_node_graph(1_021, 3_001);
    let expected = bulk_with(&nodes, &edges, 2, 4).unwrap();
    let _forced =
        crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::set(6, 4);
    for polls_before_cancel in [0_usize, 1, 40, 400, 2_000, 6_000] {
        let root = TempDir::new().unwrap();
        let mut session = pinned(&root);
        let calls = std::cell::Cell::new(0_usize);
        let cancelled =
            session.prepare_bulk_encoding(1, &node_scratch_plan(&nodes, &edges, 2), || {
                calls.set(calls.get() + 1);
                calls.get() > polls_before_cancel
            });
        if let Err(error) = cancelled {
            assert!(error.to_string().contains("cancelled"), "{error}");
            assert!(
                !scratch_dir(&session).exists(),
                "cancelled after {polls_before_cancel}"
            );
            // A budget below the fixed floor is refused before any decoding.
            let mut reduced = node_scratch_plan(&nodes, &edges, 2);
            reduced.memory_budget = Some(1);
            let error = session
                .prepare_bulk_encoding(1, &reduced, || false)
                .unwrap_err();
            assert!(matches!(
                error,
                GfError::Project {
                    code: graphforge_core::ProjectErrorCode::ResourceLimit,
                    ..
                }
            ));
            assert!(!scratch_dir(&session).exists());
        }
        let rerun = session
            .prepare_bulk_encoding(1, &node_scratch_plan(&nodes, &edges, 2), || false)
            .unwrap();
        assert_eq!(
            expected,
            inventory(&rerun),
            "cancelled after {polls_before_cancel} polls"
        );
        assert!(!scratch_dir(&session).exists());
    }
}

const NODE_SCRATCH_CRASH_PARTITIONS: &str = "GF_BULK_NODE_SCRATCH_PARTITIONS";

/// The killed process of the node scratch route.
#[test]
fn bulk_node_scratch_crash_child() {
    let (Ok(path), Ok(partitions)) = (
        std::env::var(CRASH_ROOT),
        std::env::var(NODE_SCRATCH_CRASH_PARTITIONS),
    ) else {
        return;
    };
    let parts = partitions
        .split(',')
        .map(|part| part.parse::<usize>().unwrap())
        .collect::<Vec<_>>();
    let _forced = crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::set(
        parts[0], parts[1],
    );
    let _gate = crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::with_gate(
        32 << 10,
    );
    let (nodes, edges) = clustered_node_graph(1_021, 3_001);
    let mut session = GraphConstructionSession::open(
        Path::new(&path),
        Uuid::from_u128(OPERATION),
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    session.checkpoint.session_now_micros = CLOCK;
    session
        .prepare_bulk_encoding(1, &node_scratch_plan(&nodes, &edges, 2), || false)
        .unwrap();
}

#[test]
fn a_process_killed_in_any_node_scratch_pass_leaves_scratch_and_the_rerun_is_identical() {
    let (nodes, edges) = clustered_node_graph(1_021, 3_001);
    let expected = bulk_with(&nodes, &edges, 2, 4).unwrap();
    // Every pass that exists only on this route, inside it and at its end,
    // and the passes it shares, whose scratch now includes node files.
    let mut left_scratch = 0;
    for failpoint in [
        "bulk.during_node_scatter",
        "bulk.during_node_refinement",
        "bulk.after_nodes",
        "bulk.during_edge_scatter",
        "bulk.during_edge_refinement",
        "bulk.after_edges",
        "bulk.during_endpoint_resolve",
        "bulk.after_endpoints",
        "bulk.after_ranks",
        "bulk.after_tables",
        "bulk.during_node_emit",
        "bulk.after_ordinal",
        "bulk.after_adjacency",
        "bulk.before_inventory",
    ] {
        let root = TempDir::new().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("graph_construction::tests::bulk_builder::node_scratch::bulk_node_scratch_crash_child")
            .arg("--nocapture")
            .env(CRASH_ROOT, root.path())
            .env(NODE_SCRATCH_CRASH_PARTITIONS, "7,5")
            .env(
                "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                "graphforge-construction-test-v1",
            )
            .env("GF_CONSTRUCTION_FAILPOINT", failpoint)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "{failpoint}");
        let leftover =
            root.path().join("bulk-scratch").exists() || walkdir_has(root.path(), "bulk-scratch");
        left_scratch += usize::from(leftover);
        // Recovery: opening the session deletes what the killed attempt left.
        let mut session = pinned(&root);
        assert!(
            !scratch_dir(&session).exists(),
            "recovery kept scratch after {failpoint}"
        );
        let _forced =
            crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::set(7, 5);
        let _gate =
            crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::with_gate(
                32 << 10,
            );
        let rerun = session
            .prepare_bulk_encoding(1, &node_scratch_plan(&nodes, &edges, 2), || false)
            .unwrap();
        assert_eq!(expected, inventory(&rerun), "killed at {failpoint}");
        let report = session.bulk_build_report();
        assert!(report.node_refinement_write_bytes > 0, "{report:?}");
        assert!(report.edge_refinement_write_bytes > 0, "{report:?}");
        assert_node_scratch_traffic(&report, 1_021, 3_001);
        assert!(!scratch_dir(&session).exists());
    }
    assert!(
        left_scratch >= 8,
        "killed attempts must have left scratch behind: {left_scratch}"
    );
}

#[test]
fn property_bearing_and_typed_input_over_scratch_node_tables_match_the_resident_build() {
    let (nodes, edges) = property_recovery_input();
    let expected = staged(&nodes, &edges);
    assert!(
        expected
            .iter()
            .any(|entry| entry.0.starts_with("properties/"))
    );
    let probe = plan(&nodes, &edges, 2);
    // Room for the property workspace but not for one more byte per node.
    let budget = probe.scratch_floor_bytes()
        + probe.property_floor_bytes(GraphConstructionBudgets::default())
        + 1;
    for partitions in [(1, 1), (4, 3)] {
        let _forced = crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::set(
            partitions.0,
            partitions.1,
        );
        let root = TempDir::new().unwrap();
        let mut session = pinned(&root);
        let mut plan = plan(&nodes, &edges, 2);
        plan.memory_budget = Some(budget);
        let encoding = session.prepare_bulk_encoding(1, &plan, || false).unwrap();
        assert_same(&expected, &inventory(&encoding));
        let report = session.bulk_build_report();
        assert!(report.node_partitions > 0, "{report:?}");
        assert!(report.property_scratch_write_bytes > 0, "{report:?}");
        // Property frames count toward the scratch occupancy peak.
        assert!(report.scratch_peak_occupied_bytes > 0, "{report:?}");
        // Property scans read more than they write, by design; the node and
        // endpoint scratch is still written once and read once.
        assert_eq!(
            report.node_scratch_read_bytes,
            report.node_scratch_write_bytes
        );
        assert_eq!(
            report.endpoint_scratch_read_bytes,
            report.endpoint_scratch_write_bytes
        );
        assert!(!scratch_dir(&session).exists());
    }
    // The same input under a strict ontology (qualified routes).
    let authority = super::encoding_publication::tests::semantic_authority(
        graphforge_core::OntologyMode::Strict,
    );
    let typed_nodes = [node_property_batch(1, 2), node_property_batch(3, 1)];
    let typed_edges = [edge_property_batch(100, 2)];
    let open = |root: &TempDir| {
        let mut session = GraphConstructionSession::open_with_semantic_authority(
            root.path(),
            Uuid::from_u128(OPERATION),
            0,
            authority.clone(),
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session.checkpoint.session_now_micros = CLOCK;
        session
    };
    let root = TempDir::new().unwrap();
    let resident = inventory(
        &open(&root)
            .prepare_bulk_encoding(1, &plan(&typed_nodes, &typed_edges, 1), || false)
            .unwrap(),
    );
    let probe = plan(&typed_nodes, &typed_edges, 1);
    let budget = probe.scratch_floor_bytes()
        + probe.property_floor_bytes(GraphConstructionBudgets::default())
        + 1;
    let root = TempDir::new().unwrap();
    let mut session = open(&root);
    let _frames =
        crate::graph_construction_encoding::bulk_test_support::ForcedPropertyFrames::set(1);
    let _forced =
        crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::set(2, 2);
    let mut typed = plan(&typed_nodes, &typed_edges, 1);
    typed.memory_budget = Some(budget);
    let encoding = session.prepare_bulk_encoding(1, &typed, || false).unwrap();
    assert_same(&resident, &inventory(&encoding));
    assert!(session.bulk_build_report().node_partitions > 0);
}

#[test]
fn budget_derived_node_scratch_builds_publish_the_in_memory_bytes() {
    // These are real builds on the naturally derived scratch plan. The
    // direct Ordered test covers the adverse worker schedule.
    let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
    let expected = bulk_with(&nodes, &edges, 2, 4).unwrap();
    for (gate, partitions) in [
        (48 << 20, (7, 5)),
        (96 << 10, (1, 3)),
        (256 << 10, (2, 2)),
        (64 << 10, (33, 2)),
    ] {
        let _gate =
            crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::with_gate(
                gate,
            );
        let run = node_scratch_run(&nodes, &edges, 1, 4, partitions).unwrap();
        assert_eq!(
            expected, run.inventory,
            "gate {gate}, edge/CSR partitions {partitions:?}"
        );
        assert_node_scratch_traffic(&run.report, 1_021, 3_001);
        assert!(!run.scratch_left);
    }
    // A hub and small shards through a real node-scratch build.
    let _limits = ShardLimits::set(17, 100);
    let ids = [uuid(0x10, 0), uuid(0x10, 1)];
    let nodes = vec![node_batch_of(&ids, &["Person", "Person"])];
    let edge_ids = (0..3001).map(|i| uuid(0x20, i)).collect::<Vec<_>>();
    let edges = vec![edge_batch_of(
        &edge_ids,
        &vec!["KNOWS"; 3001],
        &vec![ids[0]; 3001],
        &vec![ids[1]; 3001],
    )];
    let expected = bulk_with(&nodes, &edges, 1, 2).unwrap();
    // The hub's degrees must reach the key partitioner before its ranks
    // are used, or its 3,001 entries exceed the gate.
    let _gate = crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::with_gate(
        64 << 10,
    );
    let run = node_scratch_run(&nodes, &edges, 1, 4, (16, 4)).unwrap();
    assert_same(&expected, &run.inventory);
}

#[test]
fn nothing_extra_is_written_to_scratch_unless_the_node_tables_do_not_fit() {
    let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
    // In memory: no scratch at all.
    let root = TempDir::new().unwrap();
    let mut session = pinned(&root);
    session
        .prepare_bulk_encoding(1, &plan(&nodes, &edges, 2), || false)
        .unwrap();
    let report = session.bulk_build_report();
    assert_eq!(
        (
            report.scratch_write_bytes,
            report.scratch_read_bytes,
            report.scratch_peak_occupied_bytes,
            report.node_partitions,
            report.node_scratch_write_bytes,
            report.endpoint_scratch_write_bytes,
            report.node_refinement_steps,
        ),
        (0, 0, 0, 0, 0, 0, 0),
        "{report:?}"
    );
    assert!(!report.passes.contains_key("endpoints"));
    // The node tables fit: edges and adjacency go through scratch, nodes do not.
    let run = scratch_run(&nodes, &edges, 2, 4, (4, 3)).unwrap();
    let report = &run.report;
    assert!(report.scratch_write_bytes > 0, "{report:?}");
    assert_eq!(
        (
            report.node_partitions,
            report.largest_node_partition,
            report.node_scratch_write_bytes,
            report.node_scratch_read_bytes,
            report.endpoint_scratch_write_bytes,
            report.endpoint_scratch_read_bytes,
            report.node_refinement_write_bytes,
        ),
        (0, 0, 0, 0, 0, 0, 0),
        "{report:?}"
    );
    assert!(!report.passes.contains_key("endpoints"));
    // The node tables do not fit: exactly one scatter of the nodes, one of
    // the endpoint references, and one of the resolved endpoints.
    let run = node_scratch_run(&nodes, &edges, 2, 4, (4, 3)).unwrap();
    let report = &run.report;
    assert!(report.node_scratch_write_bytes > 0, "{report:?}");
    assert!(report.endpoint_scratch_write_bytes > 0, "{report:?}");
    assert_eq!(report.node_refinement_write_bytes, 0, "{report:?}");
    assert!(report.passes.contains_key("endpoints"));
}

#[test]
fn the_endpoint_reference_pass_is_metered_only_on_the_node_scratch_route() {
    let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
    // In memory: one edges pass, no reference pass.
    let root = TempDir::new().unwrap();
    let mut session = pinned(&root);
    session
        .prepare_bulk_encoding(1, &plan(&nodes, &edges, 2), || false)
        .unwrap();
    let report = session.bulk_build_report();
    assert!(report.passes.contains_key("edges"));
    assert!(!report.passes.contains_key("edge-refs"), "{report:?}");
    assert!(!report.passes.contains_key("endpoints"));
    // The node tables fit: the edges scatter, the nodes stay resident, and
    // the endpoints resolve during the edge pass.
    let run = scratch_run(&nodes, &edges, 2, 4, (4, 3)).unwrap();
    let report = &run.report;
    assert!(report.passes.contains_key("edges"));
    assert!(!report.passes.contains_key("edge-refs"), "{report:?}");
    // The node tables do not fit: the added real source read is its own
    // metered pass, between the edges and the endpoints.
    let run = node_scratch_run(&nodes, &edges, 2, 4, (4, 3)).unwrap();
    let report = &run.report;
    assert!(report.passes.contains_key("edges"), "{report:?}");
    assert!(report.passes.contains_key("edge-refs"), "{report:?}");
    assert!(report.passes.contains_key("endpoints"));
}

#[test]
fn two_edge_sources_over_scratch_node_tables_match_the_resident_bytes() {
    let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
    // The first source carries the first two batches, the second the rest.
    let resident = BulkBuildPlan {
        nodes: vec![source(&nodes, 2, 2)],
        edges: vec![source(&edges[..2], 4, 2), source(&edges[2..], 4, 2)],
        memory_budget: None,
    };
    let root = TempDir::new().unwrap();
    let mut session = pinned(&root);
    let expected = inventory(
        &session
            .prepare_bulk_encoding(1, &resident, || false)
            .unwrap(),
    );
    let mut plan = resident;
    plan.memory_budget = Some(plan.scratch_floor_bytes() + 1);
    assert_eq!(plan.route(), crate::BulkRoute::ScratchNodes);
    let _forced =
        crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::set(4, 3);
    let root = TempDir::new().unwrap();
    let mut session = pinned(&root);
    let encoding = session.prepare_bulk_encoding(1, &plan, || false).unwrap();
    assert_same(&expected, &inventory(&encoding));
    let report = session.bulk_build_report();
    assert_eq!((report.nodes, report.edges), (1_021, 3_001));
    assert_node_scratch_traffic(&report, 1_021, 3_001);
    assert!(report.passes.contains_key("edge-refs"), "{report:?}");
    assert!(!scratch_dir(&session).exists());
}
