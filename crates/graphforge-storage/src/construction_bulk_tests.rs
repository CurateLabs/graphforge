// The bulk builder builds exactly the generation the staged path builds (#1883).
//
// The staged path is the specification: every case ingests one logical input
// through staging, shaping and the staged encoder, then through the bulk
// builder, and compares the encoded inventory artifact by artifact. The only
// control excluded is the v4 ordinal receipt's random rebuild nonce.
mod bulk_builder {
    use super::*;
    use crate::graph_construction_encoding::{BulkBatchReader, BulkBuildPlan, BulkSource};
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use tempfile::TempDir;

    const NONCE_BEARING: &str = "topology/uuid-membership/ordinal-v4-receipt.json";
    const CLOCK: i64 = 1_789_000_000_000_000;
    const OPERATION: u128 = 0x5f4d_9c31_a20b_4e77_9d10_33c8_41ab_6e52;

    type Inventory = Vec<(String, u64, String)>;

    struct Memory {
        batches: Vec<RecordBatch>,
        per_task: usize,
    }

    impl BulkBatchReader for Memory {
        fn task_rows(&self, task: usize) -> usize {
            self.batches
                .iter()
                .skip(task * self.per_task)
                .take(self.per_task)
                .map(RecordBatch::num_rows)
                .sum()
        }

        fn read_task(
            &self,
            task: usize,
            sink: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
        ) -> Result<(), GfError> {
            let start = task * self.per_task;
            for batch in self.batches.iter().skip(start).take(self.per_task) {
                sink(batch.clone())?;
            }
            Ok(())
        }
    }

    fn source(batches: &[RecordBatch], required: usize, per_task: usize) -> BulkSource<'static> {
        BulkSource {
            tasks: batches.len().div_ceil(per_task),
            rows: batches.iter().map(|batch| batch.num_rows() as u64).sum(),
            property_free: batches.iter().all(|batch| batch.num_columns() == required),
            decoded_bytes: batches.iter().map(|batch| batch.get_array_memory_size() as u64).sum(),
            reader: Arc::new(Memory {
                batches: batches.to_vec(),
                per_task,
            }),
        }
    }

    fn plan(
        nodes: &[RecordBatch],
        edges: &[RecordBatch],
        per_task: usize,
    ) -> BulkBuildPlan<'static> {
        BulkBuildPlan {
            nodes: vec![source(nodes, 2, per_task)],
            edges: vec![source(edges, 4, per_task)],
            memory_budget: None,
        }
    }

    fn uuid(kind: u8, index: u64) -> [u8; 16] {
        let mut value = [0_u8; 16];
        value[0] = kind;
        value[6] = 0x70;
        value[8] = 0x80;
        value[8..].copy_from_slice(
            &(index.wrapping_mul(0x9e37_79b9_7f4a_7c15) | (1 << 63)).to_be_bytes(),
        );
        value
    }

    fn pinned(root: &TempDir) -> GraphConstructionSession {
        pinned_with(root, GraphConstructionBudgets::default())
    }

    fn pinned_with(root: &TempDir, budgets: GraphConstructionBudgets) -> GraphConstructionSession {
        let mut session =
            GraphConstructionSession::open(root.path(), Uuid::from_u128(OPERATION), 0, budgets)
                .unwrap();
        session.checkpoint.session_now_micros = CLOCK;
        session
    }

    fn inventory(encoding: &GraphConstructionEncoding) -> Inventory {
        encoding
            .artifacts
            .iter()
            .filter(|artifact| artifact.path != NONCE_BEARING)
            .map(|artifact| {
                (
                    artifact.path.clone(),
                    artifact.bytes,
                    artifact.sha256.clone(),
                )
            })
            .collect()
    }

    fn staged(nodes: &[RecordBatch], edges: &[RecordBatch]) -> Inventory {
        staged_with(GraphConstructionBudgets::default(), nodes, edges).unwrap()
    }

    /// The staged path's result or its first refusal.
    fn staged_with(
        budgets: GraphConstructionBudgets,
        nodes: &[RecordBatch],
        edges: &[RecordBatch],
    ) -> Result<Inventory, GfError> {
        let root = TempDir::new().unwrap();
        let mut session = pinned_with(&root, budgets);
        for (index, batch) in nodes.iter().enumerate() {
            session.append(ConstructionChunkKind::Node, &format!("n{index}"), batch)?;
        }
        for (index, batch) in edges.iter().enumerate() {
            session.append(ConstructionChunkKind::Edge, &format!("e{index}"), batch)?;
        }
        session.seal()?;
        let shape = session.shape_canonical_with_cancellation(|| false)?;
        Ok(inventory(&session.encode_canonical(&shape, 1)?))
    }

    fn bulk_budgeted(
        budgets: GraphConstructionBudgets,
        nodes: &[RecordBatch],
        edges: &[RecordBatch],
    ) -> Result<Inventory, GfError> {
        let root = TempDir::new().unwrap();
        let mut session = pinned_with(&root, budgets);
        session
            .prepare_bulk_encoding(1, &plan(nodes, edges, 2), || false)
            .map(|encoding| inventory(&encoding))
    }

    fn bulk_with(
        nodes: &[RecordBatch],
        edges: &[RecordBatch],
        per_task: usize,
        workers: usize,
    ) -> Result<Inventory, GfError> {
        let root = TempDir::new().unwrap();
        let mut session = pinned(&root);
        session.set_cpu_admission(Some(Arc::new(
            cpu_admission::ConstructionCpuAdmission::new(
                std::num::NonZeroUsize::new(workers).unwrap(),
            ),
        )));
        session
            .prepare_bulk_encoding(1, &plan(nodes, edges, per_task), || false)
            .map(|encoding| inventory(&encoding))
    }

    fn bulk(nodes: &[RecordBatch], edges: &[RecordBatch]) -> Inventory {
        bulk_with(nodes, edges, 2, 4).unwrap()
    }

    fn node_batch_of(uuids: &[[u8; 16]], labels: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            CONSTRUCTION_NODE_SCHEMA.clone(),
            vec![
                Arc::new(fixed(uuids)),
                Arc::new(StringArray::from(labels.to_vec())),
            ],
        )
        .unwrap()
    }

    fn edge_batch_of(
        uuids: &[[u8; 16]],
        rels: &[&str],
        src: &[[u8; 16]],
        dst: &[[u8; 16]],
    ) -> RecordBatch {
        RecordBatch::try_new(
            CONSTRUCTION_EDGE_SCHEMA.clone(),
            vec![
                Arc::new(fixed(uuids)),
                Arc::new(StringArray::from(rels.to_vec())),
                Arc::new(fixed(src)),
                Arc::new(fixed(dst)),
            ],
        )
        .unwrap()
    }

    /// `nodes` nodes over three labels and `edges` edges over three relation
    /// types, in `chunk`-row batches, presented in the order `order` picks.
    fn graph(
        nodes: usize,
        edges: usize,
        chunk: usize,
        order: fn(usize, usize) -> usize,
    ) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
        let node_uuids = (0..nodes as u64).map(|i| uuid(0x10, i)).collect::<Vec<_>>();
        let labels = ["Person", "City", "Pet"];
        let rels = ["KNOWS", "LIVES_IN", "OWNS"];
        let mut node_batches = Vec::new();
        let rows = (0..nodes).map(|i| order(i, nodes)).collect::<Vec<_>>();
        for window in rows.chunks(chunk) {
            let uuids = window.iter().map(|i| node_uuids[*i]).collect::<Vec<_>>();
            let names = window.iter().map(|i| labels[i % 3]).collect::<Vec<_>>();
            node_batches.push(node_batch_of(&uuids, &names));
        }
        let mut edge_batches = Vec::new();
        let rows = (0..edges).map(|i| order(i, edges)).collect::<Vec<_>>();
        for window in rows.chunks(chunk) {
            let uuids = window.iter().map(|i| uuid(0x20, *i as u64)).collect::<Vec<_>>();
            let names = window.iter().map(|i| rels[(i / 7) % 3]).collect::<Vec<_>>();
            let src = window
                .iter()
                .map(|i| node_uuids[(i * 7 + 1) % nodes])
                .collect::<Vec<_>>();
            let dst = window
                .iter()
                .map(|i| node_uuids[(i * 13 + 5) % nodes])
                .collect::<Vec<_>>();
            edge_batches.push(edge_batch_of(&uuids, &names, &src, &dst));
        }
        (node_batches, edge_batches)
    }

    fn identity_order(index: usize, _count: usize) -> usize {
        index
    }

    fn reversed(index: usize, count: usize) -> usize {
        count - 1 - index
    }

    fn scattered(index: usize, count: usize) -> usize {
        // A permutation of 0..count when `count` and 7919 are coprime.
        (index * 7919 + 3) % count
    }

    fn assert_same(expected: &Inventory, actual: &Inventory) {
        let missing = expected
            .iter()
            .filter(|entry| !actual.contains(entry))
            .collect::<Vec<_>>();
        let extra = actual
            .iter()
            .filter(|entry| !expected.contains(entry))
            .collect::<Vec<_>>();
        assert!(
            missing.is_empty() && extra.is_empty(),
            "missing from bulk: {missing:?}\nextra in bulk: {extra:?}"
        );
    }

    #[test]
    fn property_free_inputs_match_the_staged_encoder_in_any_arrival_order() {
        for order in [identity_order, reversed, scattered] {
            let (nodes, edges) = graph(1_021, 3_001, 700, order);
            let expected = staged(&nodes, &edges);
            assert!(expected.len() > 20);
            assert_same(&expected, &bulk(&nodes, &edges));
        }
    }

    #[test]
    fn a_graph_larger_than_one_window_matches_the_staged_encoder() {
        let (nodes, edges) = graph(70_001, 140_003, 20_000, scattered);
        assert_same(&staged(&nodes, &edges), &bulk(&nodes, &edges));
    }

    #[test]
    fn nodes_without_edges_match_the_staged_encoder() {
        let (nodes, _) = graph(50, 0, 25, identity_order);
        assert_same(&staged(&nodes, &[]), &bulk(&nodes, &[]));
    }

    #[test]
    fn output_is_independent_of_worker_count_and_task_shape() {
        let (nodes, edges) = graph(2_003, 9_001, 300, scattered);
        let expected = bulk_with(&nodes, &edges, 1, 1).unwrap();
        for (per_task, workers) in [(3, 2), (16, 8), (64, 32)] {
            assert_eq!(
                expected,
                bulk_with(&nodes, &edges, per_task, workers).unwrap(),
                "per_task={per_task} workers={workers}"
            );
        }
    }

    fn property_nodes(uuids: &[[u8; 16]], with_name: bool) -> RecordBatch {
        let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
        let mut columns: Vec<arrow::array::ArrayRef> = vec![
            Arc::new(fixed(uuids)),
            Arc::new(StringArray::from(
                (0..uuids.len())
                    .map(|i| if i % 2 == 0 { "Person" } else { "Pet" })
                    .collect::<Vec<_>>(),
            )),
        ];
        fields.push(Arc::new(Field::new("age", DataType::Int64, true)));
        columns.push(Arc::new(Int64Array::from(
            (0..uuids.len())
                .map(|i| (i % 5 != 0).then_some(i as i64))
                .collect::<Vec<_>>(),
        )));
        if with_name {
            fields.push(Arc::new(Field::new("name", DataType::Utf8, true)));
            columns.push(Arc::new(StringArray::from(
                (0..uuids.len())
                    .map(|i| (i % 3 != 0).then(|| format!("n{i}")))
                    .collect::<Vec<_>>(),
            )));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    fn property_edges(uuids: &[[u8; 16]], src: &[[u8; 16]], dst: &[[u8; 16]]) -> RecordBatch {
        let mut fields = CONSTRUCTION_EDGE_SCHEMA.fields().to_vec();
        fields.push(Arc::new(Field::new("weight", DataType::Float64, true)));
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(fixed(uuids)),
                Arc::new(StringArray::from(vec!["KNOWS"; uuids.len()])),
                Arc::new(fixed(src)),
                Arc::new(fixed(dst)),
                Arc::new(Float64Array::from(
                    (0..uuids.len())
                        .map(|i| (i % 4 != 0).then_some(i as f64 / 2.0))
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn property_bearing_and_mixed_schema_inputs_match_the_staged_encoder() {
        let node_uuids = (0..600_u64).map(|i| uuid(0x10, i)).collect::<Vec<_>>();
        let edge_uuids = (0..900_u64).map(|i| uuid(0x20, i)).collect::<Vec<_>>();
        // Three node schemas (bare, one property, two properties) and two edge
        // schemas, interleaved across chunks.
        let nodes = vec![
            property_nodes(&node_uuids[0..200], true),
            node_batch_of(&node_uuids[200..350], &vec!["Person"; 150]),
            property_nodes(&node_uuids[350..600], false),
        ];
        let src = |range: std::ops::Range<usize>| {
            range.map(|i| node_uuids[(i * 3) % 600]).collect::<Vec<_>>()
        };
        let dst = |range: std::ops::Range<usize>| {
            range.map(|i| node_uuids[(i * 5 + 1) % 600]).collect::<Vec<_>>()
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
        let expected = staged(&nodes, &edges);
        assert!(expected.iter().any(|entry| entry.0.starts_with("properties/")));
        assert!(expected
            .iter()
            .any(|entry| entry.0.starts_with("edge_properties/")));
        assert_same(&expected, &bulk(&nodes, &edges));
    }

    fn refusal(nodes: &[RecordBatch], edges: &[RecordBatch]) -> String {
        bulk_with(nodes, edges, 2, 4).unwrap_err().to_string()
    }

    #[test]
    fn every_global_refusal_of_the_staged_path_still_fires() {
        let a = uuid(0x10, 1);
        let b = uuid(0x10, 2);
        let e = uuid(0x20, 1);
        let node = |uuids: &[[u8; 16]]| node_batch_of(uuids, &vec!["Person"; uuids.len()]);
        let edge = |id: [u8; 16], from: [u8; 16], to: [u8; 16]| {
            edge_batch_of(&[id], &["KNOWS"], &[from], &[to])
        };

        // A node UUID repeated across batches.
        let message = refusal(&[node(&[a, b]), node(&[a])], &[]);
        assert!(message.contains("duplicate identity across construction runs"), "{message}");
        // An edge UUID repeated across batches.
        let message = refusal(&[node(&[a, b])], &[edge(e, a, b), edge(e, b, a)]);
        assert!(
            message.contains("duplicate identity across construction runs"),
            "{message}"
        );
        // An edge UUID equal to a node UUID.
        let message = refusal(&[node(&[a, b])], &[edge(a, a, b)]);
        assert!(message.contains("duplicate identity across construction runs"), "{message}");
        // An endpoint that names no UUID at all.
        let message = refusal(&[node(&[a])], &[edge(e, a, b)]);
        assert!(message.contains("edge endpoint UUID does not exist"), "{message}");
        // An endpoint that names an edge, not a node.
        let message = refusal(&[node(&[a])], &[edge(e, a, e)]);
        assert!(message.contains("edge endpoint is not a node UUID"), "{message}");
        // A label that is not an identifier.
        let message = refusal(&[node_batch_of(&[a], &["not an identifier"])], &[]);
        assert!(
            message.contains("invalid canonical label or relation"),
            "{message}"
        );
    }

    #[test]
    fn a_cancelled_build_leaves_nothing_and_the_rerun_is_identical() {
        let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
        let expected = bulk_with(&nodes, &edges, 2, 4).unwrap();
        for polls_before_cancel in [0_usize, 1, 40, 400] {
            let root = TempDir::new().unwrap();
            let mut session = pinned(&root);
            let calls = std::cell::Cell::new(0_usize);
            let cancelled = session.prepare_bulk_encoding(1, &plan(&nodes, &edges, 2), || {
                calls.set(calls.get() + 1);
                calls.get() > polls_before_cancel
            });
            if let Err(error) = cancelled {
                assert!(error.to_string().contains("cancelled"), "{error}");
            }
            // Restart: whatever the interrupted build left is scratch.
            let rerun = session
                .prepare_bulk_encoding(1, &plan(&nodes, &edges, 2), || false)
                .unwrap();
            assert_eq!(expected, inventory(&rerun), "cancelled after {polls_before_cancel} polls");
        }
    }

    const CRASH_ROOT: &str = "GF_BULK_CRASH_ROOT";

    /// The killed process: runs the build in a child that exits at the armed failpoint.
    #[test]
    fn bulk_crash_child() {
        let Ok(path) = std::env::var(CRASH_ROOT) else {
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
        session
            .prepare_bulk_encoding(1, &plan(&nodes, &edges, 2), || false)
            .unwrap();
    }

    #[test]
    fn a_process_killed_in_any_pass_reruns_to_identical_artifacts() {
        let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
        let expected = bulk_with(&nodes, &edges, 2, 4).unwrap();
        for failpoint in [
            "bulk.after_nodes",
            "bulk.after_edges",
            "bulk.after_tables",
            "bulk.after_adjacency",
            "encode.after_inventory_pinned",
            "uuid_encode.after_intent",
            "uuid_encode.after_delta_runs",
            "uuid_encode.after_manifest",
            "bulk.after_membership",
            "bulk.before_inventory",
            "bulk.after_inventory_before_intent_removal",
        ] {
            let root = TempDir::new().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("graph_construction::tests::bulk_builder::bulk_crash_child")
                .arg("--nocapture")
                .env(CRASH_ROOT, root.path())
                .env(
                    "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                    "graphforge-construction-test-v1",
                )
                .env("GF_CONSTRUCTION_FAILPOINT", failpoint)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "{failpoint}");
            let mut session = pinned(&root);
            let rerun = session
                .prepare_bulk_encoding(1, &plan(&nodes, &edges, 2), || false)
                .unwrap();
            assert_eq!(expected, inventory(&rerun), "killed at {failpoint}");
        }
    }

    /// The same typed (strict, qualified-route) input through both builds.
    #[test]
    fn a_typed_ontology_build_matches_the_staged_encoder() {
        let authority = super::encoding_publication::tests::semantic_authority(
            graphforge_core::OntologyMode::Strict,
        );
        let nodes = [node_property_batch(1, 2), node_property_batch(3, 1)];
        let edges = [edge_property_batch(100, 2)];
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

        let staged_root = TempDir::new().unwrap();
        let mut staged_session = open(&staged_root);
        for (index, batch) in nodes.iter().enumerate() {
            staged_session
                .append(ConstructionChunkKind::Node, &format!("n{index}"), batch)
                .unwrap();
        }
        for (index, batch) in edges.iter().enumerate() {
            staged_session
                .append(ConstructionChunkKind::Edge, &format!("e{index}"), batch)
                .unwrap();
        }
        staged_session.seal().unwrap();
        let shape = staged_session
            .shape_canonical_with_cancellation(|| false)
            .unwrap();
        let expected = inventory(&staged_session.encode_canonical(&shape, 1).unwrap());
        assert!(expected.iter().any(|entry| entry.0.starts_with("properties/")));
        assert!(expected
            .iter()
            .any(|entry| entry.0.starts_with("edge_properties/")));

        let bulk_root = TempDir::new().unwrap();
        let mut bulk_session = open(&bulk_root);
        let built = bulk_session
            .prepare_bulk_encoding(1, &plan(&nodes, &edges, 1), || false)
            .unwrap();
        assert_same(&expected, &inventory(&built));
    }

    /// Rows of `width` bytes of text no compressor can shrink.
    fn wide_text(start: u64, rows: usize, width: usize) -> StringArray {
        StringArray::from(
            (0..rows as u64)
                .map(|row| {
                    let mut state = (start + row).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
                    (0..width)
                        .map(|_| {
                            state ^= state << 13;
                            state ^= state >> 7;
                            state ^= state << 17;
                            char::from(b'a' + (state % 26) as u8)
                        })
                        .collect::<String>()
                })
                .collect::<Vec<_>>(),
        )
    }

    fn wide_nodes(uuids: &[[u8; 16]], first: u64) -> RecordBatch {
        let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
        fields.push(Arc::new(Field::new("bio", DataType::Utf8, true)));
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(fixed(uuids)),
                Arc::new(StringArray::from(vec!["Person"; uuids.len()])),
                Arc::new(wide_text(first, uuids.len(), 5_000)),
            ],
        )
        .unwrap()
    }

    fn wide_edges(
        uuids: &[[u8; 16]],
        src: &[[u8; 16]],
        dst: &[[u8; 16]],
        first: u64,
    ) -> RecordBatch {
        let mut fields = CONSTRUCTION_EDGE_SCHEMA.fields().to_vec();
        fields.push(Arc::new(Field::new("note", DataType::Utf8, true)));
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(fixed(uuids)),
                Arc::new(StringArray::from(vec!["KNOWS"; uuids.len()])),
                Arc::new(fixed(src)),
                Arc::new(fixed(dst)),
                Arc::new(wide_text(first, uuids.len(), 5_000)),
            ],
        )
        .unwrap()
    }

    /// Several `max_batch_rows` windows, each wider than one property fragment
    /// (4 MiB), so every overlay is split into fragments and the ordinals run on
    /// across windows and across batches.
    #[test]
    fn property_overlays_spanning_windows_and_fragments_match_the_staged_encoder() {
        let budgets = GraphConstructionBudgets {
            max_batch_rows: 1_024,
            max_run_records: 4 * 1_024,
            ..GraphConstructionBudgets::default()
        };
        let node_uuids = (0..3_000_u64).map(|i| uuid(0x10, i)).collect::<Vec<_>>();
        let edge_uuids = (0..2_500_u64).map(|i| uuid(0x20, i)).collect::<Vec<_>>();
        let nodes = node_uuids
            .chunks(1_000)
            .enumerate()
            .map(|(index, window)| wide_nodes(window, index as u64 * 1_000))
            .collect::<Vec<_>>();
        let edges = edge_uuids
            .chunks(1_000)
            .enumerate()
            .map(|(index, window)| {
                let from = |i: usize| node_uuids[(index * 1_000 + i) * 7 % 3_000];
                let to = |i: usize| node_uuids[(index * 1_000 + i) * 11 % 3_000];
                let src = (0..window.len()).map(from).collect::<Vec<_>>();
                let dst = (0..window.len()).map(to).collect::<Vec<_>>();
                wide_edges(window, &src, &dst, 50_000 + index as u64 * 1_000)
            })
            .collect::<Vec<_>>();
        let expected = staged_with(budgets, &nodes, &edges).unwrap();
        let fragments = |prefix: &str| {
            expected
                .iter()
                .filter(|entry| entry.0.starts_with(prefix))
                .count()
        };
        assert!(fragments("properties/") > 4, "{expected:?}");
        assert!(fragments("edge_properties/") > 4, "{expected:?}");
        assert_same(&expected, &bulk_budgeted(budgets, &nodes, &edges).unwrap());
    }

    /// Resets the thread's CSR shard limits when dropped.
    struct ShardLimits;

    impl ShardLimits {
        fn set(edges: usize, nodes: usize) -> Self {
            crate::adjacency::TEST_SHARD_LIMITS.with(|limits| limits.set(Some((edges, nodes))));
            Self
        }
    }

    impl Drop for ShardLimits {
        fn drop(&mut self) {
            crate::adjacency::TEST_SHARD_LIMITS.with(|limits| limits.set(None));
        }
    }

    /// A small graph on small shard limits: every CSR splits into shards by
    /// entries and by node span, and a high-degree node spans consecutive shards.
    #[test]
    fn a_graph_with_several_csr_shards_matches_the_staged_encoder() {
        let _limits = ShardLimits::set(300, 64);
        let (nodes, edges) = graph(257, 2_000, 700, scattered);
        let expected = staged(&nodes, &edges);
        let shards = expected
            .iter()
            .filter(|entry| entry.0.ends_with(".csr"))
            .count();
        // Eight CSRs (three relation groups and the union, each in both
        // directions), most of them in several shards.
        assert!(shards >= 40, "{shards} shards");
        assert_same(&expected, &bulk(&nodes, &edges));
    }

    /// The staged path's per-chunk and per-session admission, on the same input.
    #[test]
    fn the_staged_admission_budgets_refuse_on_the_bulk_path_too() {
        let (nodes, edges) = graph(100, 200, 100, identity_order);
        let two_properties = [property_nodes(
            &(0..10).map(|i| uuid(0x10, i)).collect::<Vec<_>>(),
            true,
        )];
        let defaults = GraphConstructionBudgets::default;
        let cases: Vec<(
            &str,
            GraphConstructionBudgets,
            &[RecordBatch],
            &[RecordBatch],
        )> = vec![
            (
                "construction property-column budget exhausted",
                GraphConstructionBudgets {
                    max_property_columns: 1,
                    ..defaults()
                },
                &two_properties,
                &[],
            ),
            (
                "construction resource window exhausted",
                GraphConstructionBudgets {
                    max_batch_bytes: 1_024,
                    ..defaults()
                },
                &nodes,
                &[],
            ),
            (
                "construction resource window exhausted",
                GraphConstructionBudgets {
                    max_batch_rows: 50,
                    max_run_records: 200,
                    ..defaults()
                },
                &nodes,
                &[],
            ),
            (
                "construction schema-group budget exhausted",
                GraphConstructionBudgets {
                    max_schema_groups: 1,
                    ..defaults()
                },
                &nodes,
                &edges,
            ),
        ];
        for (message, budgets, node_batches, edge_batches) in cases {
            let staged = staged_with(budgets, node_batches, edge_batches)
                .unwrap_err()
                .to_string();
            assert!(staged.contains(message), "staged: {staged}");
            let bulk = bulk_budgeted(budgets, node_batches, edge_batches)
                .unwrap_err()
                .to_string();
            assert!(bulk.contains(message), "bulk: {bulk}");
        }
    }

    /// A crash after the checkpoint pinned the inventory leaves nothing to build:
    /// the rerun reuses it and reports the rows it holds, not zeros.
    #[test]
    fn a_rerun_that_reuses_the_pinned_inventory_reports_its_rows() {
        let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
        let expected = bulk_with(&nodes, &edges, 2, 4).unwrap();
        let root = TempDir::new().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("graph_construction::tests::bulk_builder::bulk_crash_child")
            .arg("--nocapture")
            .env(CRASH_ROOT, root.path())
            .env(
                "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                "graphforge-construction-test-v1",
            )
            .env("GF_CONSTRUCTION_FAILPOINT", "encode.after_inventory_pinned")
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86));
        let mut session = pinned(&root);
        let reused = session
            .prepare_bulk_encoding(1, &plan(&nodes, &edges, 2), || false)
            .unwrap();
        assert_eq!(expected, inventory(&reused));
        let report = session.bulk_build_report();
        assert_eq!((report.nodes, report.edges), (1_021, 3_001));
    }

    // ------------------------------------------------------------------
    // The over-budget route (#1900): the same bytes, through scratch files.
    // ------------------------------------------------------------------

    /// Too small for the in-memory estimate of any test graph, large enough for
    /// its node tables: the plan routes to scratch.
    const SCRATCH_BUDGET: u64 = 800 << 20;

    fn scratch_plan(
        nodes: &[RecordBatch],
        edges: &[RecordBatch],
        per_task: usize,
    ) -> BulkBuildPlan<'static> {
        let mut plan = plan(nodes, edges, per_task);
        plan.memory_budget = Some(SCRATCH_BUDGET);
        assert_eq!(plan.route(), crate::BulkRoute::Scratch);
        plan
    }

    fn scratch_dir(session: &GraphConstructionSession) -> std::path::PathBuf {
        session.root.path().join("bulk-scratch")
    }

    /// One scratch build: its inventory, its report, and whether any scratch remained.
    struct ScratchRun {
        inventory: Inventory,
        report: crate::BulkBuildReport,
        scratch_left: bool,
    }

    fn scratch_run(
        nodes: &[RecordBatch],
        edges: &[RecordBatch],
        per_task: usize,
        workers: usize,
        (edge_partitions, csr_partitions): (usize, usize),
    ) -> Result<ScratchRun, GfError> {
        let _forced = crate::graph_construction_encoding::ForcedPartitions::set(
            edge_partitions,
            csr_partitions,
        );
        let root = TempDir::new().unwrap();
        let mut session = pinned(&root);
        session.set_cpu_admission(Some(Arc::new(
            cpu_admission::ConstructionCpuAdmission::new(
                std::num::NonZeroUsize::new(workers).unwrap(),
            ),
        )));
        let encoding =
            session.prepare_bulk_encoding(1, &scratch_plan(nodes, edges, per_task), || false)?;
        Ok(ScratchRun {
            inventory: inventory(&encoding),
            report: session.bulk_build_report(),
            scratch_left: scratch_dir(&session).exists(),
        })
    }

    /// Base scatter plus explicitly accounted bounded refinement/spools.
    /// Every successful scratch block is consumed exactly once.
    fn assert_scratch_traffic(report: &crate::BulkBuildReport, edges: u64) {
        // 28-byte edge records, two 16-byte adjacency entries and a 16-byte
        // identity per edge, plus an 8-byte header per block.
        let payload = edges * (28 + 2 * 16 + 16);
        let extra = report.edge_refinement_write_bytes + report.csr_spool_write_bytes;
        let base = report.scratch_write_bytes - extra;
        assert!(
            base >= payload && base <= payload + payload / 64 + 64 * 1024,
            "wrote {} for {payload} bytes of records",
            report.scratch_write_bytes
        );
        assert_eq!(report.scratch_read_bytes, report.scratch_write_bytes);
        // A refinement reads parent blocks and writes child blocks. Their
        // payloads match, but different block boundaries have different CRC
        // header counts; only total successful scratch traffic is identical.
        assert_eq!(
            report.edge_refinement_read_bytes > 0,
            report.edge_refinement_write_bytes > 0
        );
        assert_eq!(report.csr_spool_read_bytes, report.csr_spool_write_bytes);
    }

    #[test]
    fn the_over_budget_route_publishes_the_in_memory_bytes_at_any_partition_count() {
        let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
        let expected = bulk_with(&nodes, &edges, 2, 4).unwrap();
        assert_same(&staged(&nodes, &edges), &expected);
        for (partitions, per_task, workers) in [
            ((1, 1), 2, 1),
            ((2, 3), 2, 4),
            ((7, 5), 3, 2),
            ((16, 16), 1, 8),
            ((33, 2), 4, 3),
        ] {
            let run = scratch_run(&nodes, &edges, per_task, workers, partitions).unwrap();
            assert_eq!(expected, run.inventory, "partitions {partitions:?} workers {workers}");
            assert!(!run.scratch_left, "scratch must be deleted on completion");
            assert!(run.report.csr_partitions >= partitions.1 as u64);
            assert!(run.report.scratch_concurrency >= 1);
            if partitions.0 > 1 {
                assert!(run.report.edge_partitions > 1, "{:?}", run.report);
            }
            assert_scratch_traffic(&run.report, 3_001);
            // All three relations have distinct usable stems: each entry in
            // both directions goes through exactly one relation spool.
            assert!(run.report.csr_spool_write_bytes >= 3_001 * 32);
            assert!(
                run.report.csr_spool_write_bytes
                    <= 3_001 * 32 + run.report.csr_partitions * 3 * 2 * 8
            );
            assert_eq!((run.report.nodes, run.report.edges), (1_021, 3_001));
        }
    }

    #[test]
    fn edge_windows_that_straddle_partitions_publish_the_in_memory_files() {
        // Several canonical edge files, and partition boundaries that fall
        // inside them: the rows after a partition's last whole window carry on.
        let (nodes, edges) = graph(70_001, 140_003, 20_000, scattered);
        let expected = bulk_with(&nodes, &edges, 2, 4).unwrap();
        let files = expected
            .iter()
            .filter(|entry| entry.0.starts_with("topology/edges/"))
            .count();
        assert!(files >= 3, "{files} edge files");
        for partitions in [(2, 2), (5, 3), (64, 9)] {
            let run = scratch_run(&nodes, &edges, 2, 4, partitions).unwrap();
            assert_same(&expected, &run.inventory);
        }
    }

    #[test]
    fn csr_shards_that_span_partitions_publish_the_in_memory_shards() {
        // Small shards and a hub: shard boundaries fall mid-node and across
        // partition boundaries, in every relation group and both directions.
        let _limits = ShardLimits::set(7, 3);
        let (nodes, edges) = graph(257, 2_000, 700, scattered);
        let expected = bulk_with(&nodes, &edges, 2, 4).unwrap();
        let shards = expected
            .iter()
            .filter(|entry| entry.0.ends_with(".csr"))
            .count();
        assert!(shards >= 200, "{shards} shards");
        for partitions in [(1, 1), (3, 2), (6, 5), (9, 40)] {
            let run = scratch_run(&nodes, &edges, 2, 4, partitions).unwrap();
            assert_same(&expected, &run.inventory);
        }
        assert_same(&staged(&nodes, &edges), &expected);
    }

    #[test]
    fn a_hub_larger_than_the_gate_splits_by_edge_id_without_rewriting_scratch() {
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
        // All 3,001 entries at a key used to require 120,040 bytes from
        // this 32 KiB gate. Edge partitions themselves fit the gate.
        let _gate = crate::graph_construction_encoding::ForcedPartitions::with_gate(32 << 10);
        let run = scratch_run(&nodes, &edges, 1, 2, (16, 2)).unwrap();
        assert_same(&expected, &run.inventory);
        assert_scratch_traffic(&run.report, 3001);
        assert_eq!(run.report.edge_refinement_write_bytes, 0);
        assert_eq!(run.report.csr_spool_write_bytes, 0);
        assert!(!run.scratch_left);
    }

    #[test]
    fn a_hub_and_empty_partitions_publish_the_in_memory_bytes() {
        let _limits = ShardLimits::set(5, 100);
        let node_uuids = (0..40_u64).map(|i| uuid(0x10, i)).collect::<Vec<_>>();
        let nodes = vec![node_batch_of(&node_uuids, &vec!["Person"; 40])];
        // Every edge leaves node 0 or enters node 1: two keys hold all entries.
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
        // Far more partitions than edges: most are empty.
        for partitions in [(2, 2), (64, 64), (200, 1)] {
            let run = scratch_run(&nodes, &edges, 1, 2, partitions).unwrap();
            assert_same(&expected, &run.inventory);
        }
        // No edges at all.
        let expected = bulk_with(&nodes, &[], 1, 2).unwrap();
        let run = scratch_run(&nodes, &[], 1, 2, (4, 4)).unwrap();
        assert_same(&expected, &run.inventory);
        assert_eq!(run.report.edges, 0);
    }

    #[test]
    fn every_global_refusal_fires_identically_on_the_over_budget_route() {
        let a = uuid(0x10, 1);
        let b = uuid(0x10, 2);
        let e = uuid(0x20, 1);
        let f = uuid(0x20, 2);
        let node = |uuids: &[[u8; 16]]| node_batch_of(uuids, &vec!["Person"; uuids.len()]);
        let edge = |id: [u8; 16], from: [u8; 16], to: [u8; 16]| {
            edge_batch_of(&[id], &["KNOWS"], &[from], &[to])
        };
        let cases: Vec<(Vec<RecordBatch>, Vec<RecordBatch>)> = vec![
            // A node UUID repeated across batches.
            (vec![node(&[a, b]), node(&[a])], vec![]),
            // An edge UUID repeated across batches.
            (vec![node(&[a, b])], vec![edge(e, a, b), edge(e, b, a)]),
            // An edge UUID equal to a node UUID.
            (vec![node(&[a, b])], vec![edge(a, a, b)]),
            // An endpoint that names no UUID at all.
            (vec![node(&[a])], vec![edge(e, a, b)]),
            // An endpoint that names an edge, not a node.
            (vec![node(&[a])], vec![edge(e, a, e), edge(f, a, a)]),
            // An identity error outranks a missing endpoint, as in memory.
            (vec![node(&[a])], vec![edge(e, a, b), edge(e, a, a)]),
            (vec![node(&[a])], vec![edge(a, a, b)]),
            // A label that is not an identifier.
            (vec![node_batch_of(&[a], &["not an identifier"])], vec![]),
        ];
        for (index, (nodes, edges)) in cases.iter().enumerate() {
            let in_memory = refusal(nodes, edges);
            for partitions in [(1, 1), (4, 3)] {
                let error = scratch_run(nodes, edges, 2, 4, partitions)
                    .err()
                    .unwrap_or_else(|| panic!("case {index} was accepted"))
                    .to_string();
                assert_eq!(in_memory, error, "case {index} partitions {partitions:?}");
            }
        }
    }

    #[test]
    fn a_cancelled_over_budget_build_leaves_no_scratch_and_the_rerun_is_identical() {
        let _gate = crate::graph_construction_encoding::ForcedPartitions::with_gate(32 << 10);
        let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
        let expected = bulk_with(&nodes, &edges, 2, 4).unwrap();
        let _forced = crate::graph_construction_encoding::ForcedPartitions::set(6, 4);
        for polls_before_cancel in [0_usize, 1, 40, 400, 2_000] {
            let root = TempDir::new().unwrap();
            let mut session = pinned(&root);
            let calls = std::cell::Cell::new(0_usize);
            let cancelled =
                session.prepare_bulk_encoding(1, &scratch_plan(&nodes, &edges, 2), || {
                    calls.set(calls.get() + 1);
                    calls.get() > polls_before_cancel
                });
            if let Err(error) = cancelled {
                assert!(error.to_string().contains("cancelled"), "{error}");
                assert!(!scratch_dir(&session).exists(), "cancelled after {polls_before_cancel}");
                // A live budget drop cannot make this fixed bulk route build
                // resident tables it no longer has room for. Restoring the
                // budget below still completes the same valid input.
                let mut reduced = scratch_plan(&nodes, &edges, 2);
                reduced.memory_budget = Some(1);
                let error = session.prepare_bulk_encoding(1, &reduced, || false).unwrap_err();
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
                .prepare_bulk_encoding(1, &scratch_plan(&nodes, &edges, 2), || false)
                .unwrap();
            assert_eq!(expected, inventory(&rerun), "cancelled after {polls_before_cancel} polls");
            assert!(!scratch_dir(&session).exists());
        }
    }

    /// Whether a directory called `name` exists anywhere below `root`.
    fn walkdir_has(root: &Path, name: &str) -> bool {
        std::fs::read_dir(root).into_iter().flatten().flatten().any(|entry| {
            entry.file_name() == name
                || (entry.path().is_dir() && walkdir_has(&entry.path(), name))
        })
    }

    const CRASH_PARTITIONS: &str = "GF_BULK_CRASH_PARTITIONS";

    /// The killed process of the over-budget route.
    #[test]
    fn bulk_scratch_crash_child() {
        let (Ok(path), Ok(partitions)) = (std::env::var(CRASH_ROOT), std::env::var(CRASH_PARTITIONS))
        else {
            return;
        };
        let (edge, csr) = partitions.split_once(',').unwrap();
        let _forced = crate::graph_construction_encoding::ForcedPartitions::set(
            edge.parse().unwrap(),
            csr.parse().unwrap(),
        );
        let _gate = crate::graph_construction_encoding::ForcedPartitions::with_gate(32 << 10);
        let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
        let mut session = GraphConstructionSession::open(
            Path::new(&path),
            Uuid::from_u128(OPERATION),
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session.checkpoint.session_now_micros = CLOCK;
        session
            .prepare_bulk_encoding(1, &scratch_plan(&nodes, &edges, 2), || false)
            .unwrap();
    }

    #[test]
    fn a_process_killed_over_budget_leaves_scratch_that_recovery_deletes_and_the_rerun_is_identical()
    {
        let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
        let expected = bulk_with(&nodes, &edges, 2, 4).unwrap();
        // Scratch is live between the edge scatter and the end of the adjacency pass.
        let mut left_scratch = 0;
        for failpoint in [
            "bulk.after_nodes",
            "bulk.during_edge_refinement",
            "bulk.after_edges",
            "bulk.after_ranks",
            "bulk.after_tables",
            "bulk.after_membership",
            "bulk.after_adjacency",
            "bulk.before_inventory",
        ] {
            let root = TempDir::new().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("graph_construction::tests::bulk_builder::bulk_scratch_crash_child")
                .arg("--nocapture")
                .env(CRASH_ROOT, root.path())
                .env(CRASH_PARTITIONS, "7,5")
                .env(
                    "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                    "graphforge-construction-test-v1",
                )
                .env("GF_CONSTRUCTION_FAILPOINT", failpoint)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "{failpoint}");
            // Recovery: opening the session deletes what the killed attempt left.
            let leftover = root.path().join("bulk-scratch").exists()
                || walkdir_has(root.path(), "bulk-scratch");
            left_scratch += usize::from(leftover);
            let mut session = pinned(&root);
            assert!(!scratch_dir(&session).exists(), "recovery kept scratch after {failpoint}");
            let _forced = crate::graph_construction_encoding::ForcedPartitions::set(7, 5);
            let _gate = crate::graph_construction_encoding::ForcedPartitions::with_gate(32 << 10);
            let rerun = session
                .prepare_bulk_encoding(1, &scratch_plan(&nodes, &edges, 2), || false)
                .unwrap();
            assert_eq!(expected, inventory(&rerun), "killed at {failpoint}");
            let report = session.bulk_build_report();
            assert!(report.edge_refinement_write_bytes > 0, "{report:?}");
            assert_scratch_traffic(&report, 3001);
            assert!(!scratch_dir(&session).exists());
        }
        assert!(left_scratch >= 2, "a killed attempt must have left scratch behind");
    }

    #[test]
    fn routing_is_decided_from_the_footers_and_the_budget() {
        let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
        let mut plan = plan(&nodes, &edges, 2);
        // No budget, or one the estimate fits: in memory.
        assert_eq!(plan.route(), crate::BulkRoute::Memory);
        plan.memory_budget = Some(plan.estimated_resident_bytes());
        assert_eq!(plan.route(), crate::BulkRoute::Memory);
        // One byte short, with room for the node tables: scratch.
        plan.memory_budget = Some(plan.estimated_resident_bytes() - 1);
        assert_eq!(plan.route(), crate::BulkRoute::Scratch);
        // The node tables do not fit: staged, with the reason.
        plan.memory_budget = Some(plan.node_tables_resident_bytes() - 1);
        assert_eq!(
            plan.route(),
            crate::BulkRoute::Staged(crate::BulkStagedReason::NodeTablesExceedBudget)
        );
        plan.memory_budget = Some(1);
        assert_eq!(
            plan.route(),
            crate::BulkRoute::Staged(crate::BulkStagedReason::NodeTablesExceedBudget)
        );
        // Edge properties are retained in memory, so they cannot go to scratch.
        let node_uuids = (0..600_u64).map(|i| uuid(0x10, i)).collect::<Vec<_>>();
        let edge_uuids = (0..900_u64).map(|i| uuid(0x20, i)).collect::<Vec<_>>();
        let src = (0..900).map(|i| node_uuids[(i * 3) % 600]).collect::<Vec<_>>();
        let dst = (0..900).map(|i| node_uuids[(i * 5 + 1) % 600]).collect::<Vec<_>>();
        let with_properties = [property_edges(&edge_uuids, &src, &dst)];
        let mut plan = super::bulk_builder::plan(
            &[node_batch_of(&node_uuids, &vec!["Person"; 600])],
            &with_properties,
            2,
        );
        plan.memory_budget = Some(SCRATCH_BUDGET);
        assert_eq!(
            plan.route(),
            crate::BulkRoute::Staged(crate::BulkStagedReason::EdgePropertiesExceedBudget)
        );
        // The node tables are checked first.
        plan.memory_budget = Some(1);
        assert_eq!(
            plan.route(),
            crate::BulkRoute::Staged(crate::BulkStagedReason::NodeTablesExceedBudget)
        );
    }

    /// A reader that states its tasks' identity bounds, as a Parquet footer does.
    struct Bounded(Memory);

    impl BulkBatchReader for Bounded {
        fn task_rows(&self, task: usize) -> usize {
            self.0.task_rows(task)
        }

        fn read_task(
            &self,
            task: usize,
            sink: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
        ) -> Result<(), GfError> {
            self.0.read_task(task, sink)
        }

        fn uuid_bounds(&self, task: usize) -> Option<([u8; 16], [u8; 16])> {
            let mut all = Vec::new();
            for batch in self.0.batches.iter().skip(task * self.0.per_task).take(self.0.per_task) {
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                all.extend(
                    (0..column.len()).map(|row| <[u8; 16]>::try_from(column.value(row)).unwrap()),
                );
            }
            Some((*all.iter().min()?, *all.iter().max()?))
        }
    }

    /// An identity that grows with `index`, as a time-ordered UUID does.
    fn clustered(index: u64) -> [u8; 16] {
        let mut value = [0_u8; 16];
        value[..6].copy_from_slice(&index.to_be_bytes()[2..]);
        value[6] = 0x70;
        value[8] = 0x80;
        value[9] = 1;
        value
    }

    #[test]
    fn identities_that_arrive_in_order_split_evenly_with_or_without_footer_bounds() {
        let node_uuids = (0..500_u64).map(|i| uuid(0x10, i)).collect::<Vec<_>>();
        let nodes = vec![node_batch_of(&node_uuids, &vec!["Person"; 500])];
        let edges = (0..40_u64)
            .map(|chunk| {
                let ids = (chunk * 1_000..(chunk + 1) * 1_000).map(clustered).collect::<Vec<_>>();
                let src = (0..1_000).map(|i| node_uuids[(i * 7) % 500]).collect::<Vec<_>>();
                let dst = (0..1_000).map(|i| node_uuids[(i * 11 + 3) % 500]).collect::<Vec<_>>();
                edge_batch_of(&ids, &vec!["KNOWS"; 1_000], &src, &dst)
            })
            .collect::<Vec<_>>();
        let expected = bulk_with(&nodes, &edges, 2, 4).unwrap();
        for bounded in [false, true] {
            let _forced = crate::graph_construction_encoding::ForcedPartitions::set(8, 4);
            let mut plan = scratch_plan(&nodes, &edges, 2);
            if bounded {
                plan.edges[0].reader = Arc::new(Bounded(Memory {
                    batches: edges.clone(),
                    per_task: 2,
                }));
            }
            let root = TempDir::new().unwrap();
            let mut session = pinned(&root);
            let encoding = session.prepare_bulk_encoding(1, &plan, || false).unwrap();
            assert_eq!(expected, inventory(&encoding), "bounded={bounded}");
            let report = session.bulk_build_report();
            // 40,000 edges over eight partitions: 5,000 each when balanced.
            assert!(report.edge_partitions >= 6, "bounded={bounded} {report:?}");
            assert!(
                report.largest_edge_partition <= 8_000,
                "bounded={bounded}: largest partition {} of 40000",
                report.largest_edge_partition
            );
        }
    }

    #[test]
    fn clustered_uuid_ranges_refine_with_footer_bounds_or_sampling_and_publish_identical_bytes() {
        let _limits = ShardLimits::set(31, 100);
        let ids = [uuid(0x10, 0), uuid(0x10, 1)];
        let nodes = vec![node_batch_of(&ids, &["Person", "Person"])];
        let mut edge_ids = (0..3000_u16)
            .map(|index| {
                let mut value = [0x20; 16];
                value[14..].copy_from_slice(&index.to_be_bytes());
                value
            })
            .collect::<Vec<_>>();
        // Every task's exact min/max looks almost uniform across the UUID
        // space even though practically all identities share fourteen bytes.
        edge_ids.push([0xee; 16]);
        let edges = vec![edge_batch_of(
            &edge_ids,
            &vec!["KNOWS"; 3001],
            &vec![ids[0]; 3001],
            &vec![ids[1]; 3001],
        )];
        let expected = bulk_with(&nodes, &edges, 1, 2).unwrap();
        assert_same(&staged(&nodes, &edges), &expected);
        for bounded in [false, true] {
            for (gate, parts) in [(32 << 10, 4), (64 << 10, 1)] {
                let _gate = crate::graph_construction_encoding::ForcedPartitions::with_gate(gate);
                let _parts = crate::graph_construction_encoding::ForcedPartitions::set(parts, 2);
                let mut plan = scratch_plan(&nodes, &edges, 1);
                if bounded {
                    plan.edges[0].reader = Arc::new(Bounded(Memory {
                        batches: edges.clone(),
                        per_task: 1,
                    }));
                }
                let root = TempDir::new().unwrap();
                let mut session = pinned(&root);
                let encoding = session.prepare_bulk_encoding(1, &plan, || false).unwrap();
                assert_same(&expected, &inventory(&encoding));
                let report = session.bulk_build_report();
                assert!(report.edge_refinement_write_bytes > 0, "{report:?}");
                assert!(
                    report.largest_edge_partition * 44 <= gate / (2 * report.scratch_concurrency),
                    "{report:?}"
                );
                assert_eq!(report.csr_spool_write_bytes, 0);
                assert_scratch_traffic(&report, 3001);
                assert!(!scratch_dir(&session).exists());
            }
        }
        for endpoint in [edge_ids[1000], uuid(0x30, 0)] {
            let mut sources = vec![ids[0]; 3001];
            sources[0] = endpoint;
            let refused = vec![edge_batch_of(
                &edge_ids,
                &vec!["KNOWS"; 3001],
                &sources,
                &vec![ids[1]; 3001],
            )];
            let expected = bulk_with(&nodes, &refused, 1, 1).err().unwrap();
            let _gate = crate::graph_construction_encoding::ForcedPartitions::with_gate(32 << 10);
            let _parts = crate::graph_construction_encoding::ForcedPartitions::set(1, 2);
            let root = TempDir::new().unwrap();
            let mut session = pinned(&root);
            let actual = session
                .prepare_bulk_encoding(1, &scratch_plan(&nodes, &refused, 1), || false)
                .err()
                .unwrap();
            assert_eq!(expected.to_string(), actual.to_string());
            assert!(!scratch_dir(&session).exists());
        }
        // A duplicate inside the large cluster must keep the resident route's
        // identity refusal rather than being mistaken for partition overflow.
        edge_ids[2999] = edge_ids[0];
        let edges = vec![edge_batch_of(
            &edge_ids,
            &vec!["KNOWS"; 3001],
            &vec![ids[0]; 3001],
            &vec![ids[1]; 3001],
        )];
        let _gate = crate::graph_construction_encoding::ForcedPartitions::with_gate(32 << 10);
        let error = scratch_run(&nodes, &edges, 1, 1, (1, 2)).err().unwrap();
        assert!(error.to_string().contains("duplicate identity"), "{error}");
    }

    #[test]
    fn many_interleaved_relations_reuse_one_canonical_shard_carry() {
        let _limits = ShardLimits::set(1000, 100);
        let ids = [uuid(0x10, 0), uuid(0x10, 1)];
        let nodes = vec![node_batch_of(&ids, &["Person", "Person"])];
        let names = (0..32)
            .map(|index| format!("REL_{index:02}"))
            .collect::<Vec<_>>();
        let edge_ids = (0..6400).map(|index| uuid(0x20, index)).collect::<Vec<_>>();
        let rels = (0..6400)
            .map(|index| names[index % names.len()].as_str())
            .collect::<Vec<_>>();
        let edges = vec![edge_batch_of(
            &edge_ids,
            &rels,
            &vec![ids[0]; 6400],
            &vec![ids[1]; 6400],
        )];
        let expected = bulk_with(&nodes, &edges, 1, 4).unwrap();
        assert_same(&staged(&nodes, &edges), &expected);
        for (gate, parts, workers) in [(32 << 10, (16, 2), 1), (64 << 10, (7, 5), 4)] {
            // Previously each relation retained its 200-record tail while
            // partitions advanced: 102,400 bytes outside either sort gate.
            assert!(32 * 200 * 16 > gate);
            let _gate = crate::graph_construction_encoding::ForcedPartitions::with_gate(gate);
            let run = scratch_run(&nodes, &edges, 1, workers, parts).unwrap();
            assert_same(&expected, &run.inventory);
            assert_eq!(run.report.peak_csr_carry_entries, 1000);
            assert!(run.report.csr_spool_write_bytes >= 6400 * 32);
            assert!(
                run.report.csr_spool_write_bytes
                    <= 6400 * 32 + run.report.csr_partitions * 32 * 2 * 8
            );
            assert_scratch_traffic(&run.report, 6400);
            assert!(!run.scratch_left);
        }
    }
}
