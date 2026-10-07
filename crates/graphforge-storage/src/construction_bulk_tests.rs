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
            reader: Arc::new(Memory {
                batches: batches.to_vec(),
                per_task,
            }),
        }
    }

    fn plan(nodes: &[RecordBatch], edges: &[RecordBatch], per_task: usize) -> BulkBuildPlan<'static> {
        BulkBuildPlan {
            nodes: vec![source(nodes, 2, per_task)],
            edges: vec![source(edges, 4, per_task)],
        }
    }

    fn uuid(kind: u8, index: u64) -> [u8; 16] {
        let mut value = [0_u8; 16];
        value[0] = kind;
        value[6] = 0x70;
        value[8] = 0x80;
        value[8..].copy_from_slice(&(index.wrapping_mul(0x9e37_79b9_7f4a_7c15) | (1 << 63)).to_be_bytes());
        value
    }

    fn pinned(root: &TempDir) -> GraphConstructionSession {
        let mut session = GraphConstructionSession::open(
            root.path(),
            Uuid::from_u128(OPERATION),
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session.checkpoint.session_now_micros = CLOCK;
        session
    }

    fn inventory(encoding: &GraphConstructionEncoding) -> Inventory {
        encoding
            .artifacts
            .iter()
            .filter(|artifact| artifact.path != NONCE_BEARING)
            .map(|artifact| (artifact.path.clone(), artifact.bytes, artifact.sha256.clone()))
            .collect()
    }

    fn staged(nodes: &[RecordBatch], edges: &[RecordBatch]) -> Inventory {
        let root = TempDir::new().unwrap();
        let mut session = pinned(&root);
        for (index, batch) in nodes.iter().enumerate() {
            session
                .append(ConstructionChunkKind::Node, &format!("n{index}"), batch)
                .unwrap();
        }
        for (index, batch) in edges.iter().enumerate() {
            session
                .append(ConstructionChunkKind::Edge, &format!("e{index}"), batch)
                .unwrap();
        }
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        inventory(&session.encode_canonical(&shape, 1).unwrap())
    }

    fn bulk_with(
        nodes: &[RecordBatch],
        edges: &[RecordBatch],
        per_task: usize,
        workers: usize,
    ) -> Result<Inventory, GfError> {
        let root = TempDir::new().unwrap();
        let mut session = pinned(&root);
        session.set_cpu_admission(Some(Arc::new(cpu_admission::ConstructionCpuAdmission::new(
            std::num::NonZeroUsize::new(workers).unwrap(),
        ))));
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

    fn property_edges(
        uuids: &[[u8; 16]],
        src: &[[u8; 16]],
        dst: &[[u8; 16]],
    ) -> RecordBatch {
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
        let message = refusal(
            &[node(&[a, b])],
            &[edge(e, a, b), edge(e, b, a)],
        );
        assert!(message.contains("duplicate identity across construction runs"), "{message}");
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
        let message = refusal(
            &[node_batch_of(&[a], &["not an identifier"])],
            &[],
        );
        assert!(message.contains("invalid canonical label or relation"), "{message}");
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
}
