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
        fn schema_resident_bytes(&self) -> u64 {
            self.batches
                .iter()
                .map(|batch| {
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| field.size() as u64)
                        .sum::<u64>()
                        + batch
                            .schema()
                            .metadata()
                            .iter()
                            .map(|(key, value)| (key.capacity() + value.capacity() + 64) as u64)
                            .sum::<u64>()
                })
                .max()
                .unwrap_or(0)
        }

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
            decoded_bytes: batches
                .iter()
                .map(|batch| batch.get_array_memory_size() as u64)
                .sum(),
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
            let uuids = window
                .iter()
                .map(|i| uuid(0x20, *i as u64))
                .collect::<Vec<_>>();
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
        let expected = staged(&nodes, &edges);
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
        assert_same(&expected, &bulk(&nodes, &edges));
        for (budget, partitions) in [(920 << 20, (3, 4)), (944 << 20, (7, 5))] {
            let root = TempDir::new().unwrap();
            let mut session = pinned(&root);
            let _forced =
                crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::set(
                    partitions.0,
                    partitions.1,
                );
            let mut plan = plan(&nodes, &edges, 2);
            plan.memory_budget = Some(budget);
            assert_eq!(plan.route(), crate::BulkRoute::Scratch);
            let encoding = session.prepare_bulk_encoding(1, &plan, || false).unwrap();
            assert_same(&expected, &inventory(&encoding));
            let report = session.bulk_build_report();
            assert!(report.property_scratch_write_bytes > 0, "{report:?}");
            assert!(
                report.property_scratch_read_bytes > report.property_scratch_write_bytes,
                "catalog and property scans must count: {report:?}"
            );
            assert!(report.property_workspace_reserved_bytes >= 648 << 20);
            assert!(report.scratch_concurrency >= 1, "{report:?}");
            assert!(!scratch_dir(&session).exists());
        }
    }

    #[test]
    fn property_scratch_preserves_wide_fragments_nested_nulls_and_logical_windows() {
        use arrow::array::{ArrayRef, Int32Array, ListArray, StructArray, Time64NanosecondArray};
        let budgets = GraphConstructionBudgets {
            max_batch_rows: 257,
            max_run_records: 1028,
            ..GraphConstructionBudgets::default()
        };
        let temporal_fields = graphforge_ir::arrow_schema::datetime_struct_fields();
        let uuids = (0..521_u64)
            .rev()
            .map(|i| uuid(0x10, i))
            .collect::<Vec<_>>();
        let mut nodes = Vec::new();
        for ids in uuids.chunks(129) {
            let count = ids.len();
            let zone = StringArray::from(
                (0..count)
                    .map(|row| Some(format!("{row}:{}", "x".repeat(16_384))))
                    .collect::<Vec<_>>(),
            );
            let temporal = StructArray::new(
                temporal_fields.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![0; count])),
                    Arc::new(Time64NanosecondArray::from(vec![0; count])),
                    Arc::new(Int32Array::from(vec![0; count])),
                    Arc::new(zone),
                ],
                Some(
                    (0..count)
                        .map(|row| row % 3 != 0)
                        .collect::<Vec<_>>()
                        .into(),
                ),
            );
            let element = Arc::new(Field::new("item", DataType::Utf8, true));
            let values = Arc::new(StringArray::from(vec!["hidden-child"; count * 2])) as ArrayRef;
            let lists = ListArray::try_new(
                element.clone(),
                arrow::buffer::OffsetBuffer::new(
                    (0..=count)
                        .map(|row| i32::try_from(row * 2).unwrap())
                        .collect::<Vec<_>>()
                        .into(),
                ),
                values,
                Some(
                    (0..count)
                        .map(|row| row % 3 != 0)
                        .collect::<Vec<_>>()
                        .into(),
                ),
            )
            .unwrap();
            let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
            fields.push(Arc::new(Field::new(
                "when",
                DataType::Struct(temporal_fields.clone()),
                true,
            )));
            fields.push(Arc::new(Field::new("items", DataType::List(element), true)));
            nodes.push(
                RecordBatch::try_new(
                    Arc::new(Schema::new(fields)),
                    vec![
                        Arc::new(fixed(ids)),
                        Arc::new(StringArray::from(
                            (0..count)
                                .map(|row| if row % 2 == 0 { "Person" } else { "Pet" })
                                .collect::<Vec<_>>(),
                        )),
                        Arc::new(temporal),
                        Arc::new(lists),
                    ],
                )
                .unwrap(),
            );
        }
        let expected = staged_with(budgets, &nodes, &[]).unwrap();
        assert_same(&expected, &bulk_budgeted(budgets, &nodes, &[]).unwrap());
        for budget in [880 << 20, 944 << 20] {
            let root = TempDir::new().unwrap();
            let mut session = pinned_with(&root, budgets);
            let mut plan = plan(&nodes, &[], 2);
            plan.memory_budget = Some(budget);
            assert_eq!(plan.route(), crate::BulkRoute::Scratch);
            let encoded = session.prepare_bulk_encoding(1, &plan, || false).unwrap();
            assert_same(&expected, &inventory(&encoded));
            assert!(!scratch_dir(&session).exists());
        }
    }

    #[test]
    fn skewed_property_scratch_matches_staged_artifacts_at_natural_minimum() {
        let budgets = GraphConstructionBudgets {
            max_batch_rows: 1024,
            max_batch_bytes: 256 << 10,
            max_run_records: 4 * 1024,
            ..GraphConstructionBudgets::default()
        };
        let mut uuids = (0..32_768_u64)
            .map(|index| uuid(0x10, index))
            .collect::<Vec<_>>();
        uuids.sort_unstable();
        let large = "x".repeat(100 << 10);
        let mut nodes = Vec::with_capacity(32);
        for (task, ids) in uuids.chunks(1024).enumerate() {
            let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
            fields.push(Arc::new(Field::new("name", DataType::Utf8, false)));
            let names = (0..ids.len())
                .map(|row| if row == 0 { large.as_str() } else { "" })
                .collect::<Vec<_>>();
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(fields)),
                vec![
                    Arc::new(fixed(ids)),
                    Arc::new(StringArray::from(vec!["Person"; ids.len()])),
                    Arc::new(StringArray::from(names)),
                ],
            )
            .unwrap();
            assert!(
                batch.get_array_memory_size() <= budgets.max_batch_bytes,
                "source task {task} exceeded its byte window"
            );
            nodes.push(batch);
        }

        let expected = staged_with(budgets, &nodes, &[]).unwrap();
        let mut plan = plan(&nodes, &[], 1);
        let budget = crate::graph_construction_encoding::bulk_test_support::scratch_minimum_bytes(
            &plan, budgets,
        );
        plan.memory_budget = Some(budget);
        assert_eq!(plan.route(), crate::BulkRoute::Scratch);
        let root = TempDir::new().unwrap();
        let mut session = pinned_with(&root, budgets);
        session.set_cpu_admission(Some(Arc::new(
            cpu_admission::ConstructionCpuAdmission::new(std::num::NonZeroUsize::new(1).unwrap()),
        )));
        // Each source task completes one run; property rows are naturally
        // merged within the derived pool.
        let encoding = session.prepare_bulk_encoding(1, &plan, || false).unwrap();
        assert_same(&expected, &inventory(&encoding));
        let report = session.bulk_build_report();
        assert_eq!(report.scratch_concurrency, 1, "{report:?}");
        assert_eq!(
            report.property_retained_budget_bytes,
            24 << 20,
            "{report:?}"
        );
        assert!(!scratch_dir(&session).exists());
    }

    #[test]
    fn property_fields_active_in_later_frames_apply_to_earlier_owner_rows() {
        let ids = (0..521_u64)
            .map(|id| {
                let mut bytes = [0; 16];
                bytes[0] = 0x10;
                bytes[8..].copy_from_slice(&id.to_be_bytes());
                bytes
            })
            .collect::<Vec<_>>();
        let values = (0..521)
            .map(|row| (row % 257 >= 200).then(|| format!("value{row}")))
            .collect::<Vec<_>>();
        let base = node_batch_of(&ids, &vec!["Person"; 521]);
        let mut fields = base.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("late", DataType::Utf8, true)));
        let nodes = RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                base.column(0).clone(),
                base.column(1).clone(),
                Arc::new(StringArray::from(values)),
            ],
        )
        .unwrap();
        let nodes = (0..521)
            .step_by(101)
            .map(|offset| nodes.slice(offset, 101.min(521 - offset)))
            .collect::<Vec<_>>();
        let budgets = GraphConstructionBudgets {
            max_batch_rows: 257,
            max_run_records: 1028,
            ..GraphConstructionBudgets::default()
        };
        let expected = staged_with(budgets, &nodes, &[]).unwrap();
        let root = TempDir::new().unwrap();
        let mut session = pinned_with(&root, budgets);
        let _frames =
            crate::graph_construction_encoding::bulk_test_support::ForcedPropertyFrames::set(2048);
        let mut plan = plan(&nodes, &[], 1);
        plan.memory_budget = Some(920 << 20);
        let encoded = session.prepare_bulk_encoding(1, &plan, || false).unwrap();
        assert_same(&expected, &inventory(&encoded));
    }

    #[test]
    fn insufficient_property_workspace_refuses_before_source_decoding() {
        struct Never;
        impl BulkBatchReader for Never {
            fn task_rows(&self, _: usize) -> usize {
                3
            }
            fn read_task(
                &self,
                _: usize,
                _: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
            ) -> Result<(), GfError> {
                panic!("workspace refusal must precede source decoding")
            }
        }
        let plan = BulkBuildPlan {
            nodes: vec![BulkSource {
                reader: Arc::new(Never),
                tasks: 1,
                rows: 3,
                property_free: false,
                decoded_bytes: 100_000_000,
            }],
            edges: vec![],
            memory_budget: Some(800 << 20),
        };
        assert_eq!(plan.route(), crate::BulkRoute::Scratch);
        let root = TempDir::new().unwrap();
        let mut session = pinned(&root);
        let error = session
            .prepare_bulk_encoding(1, &plan, || false)
            .unwrap_err();
        assert!(matches!(
            error,
            GfError::Project {
                code: graphforge_core::ProjectErrorCode::ResourceLimit,
                ..
            }
        ));
        assert!(!scratch_dir(&session).exists());
        let historical: crate::BulkStagedReason =
            serde_json::from_str("\"edge_properties_exceed_budget\"").unwrap();
        assert_eq!(
            historical,
            crate::BulkStagedReason::EdgePropertiesExceedBudget
        );
    }

    fn property_recovery_input() -> (Vec<RecordBatch>, Vec<RecordBatch>) {
        let ids = (0..300_u64).map(|id| uuid(0x10, id)).collect::<Vec<_>>();
        let nodes = ids
            .chunks(37)
            .map(|ids| property_nodes(ids, true))
            .collect::<Vec<_>>();
        let edges = (0..200_u64)
            .collect::<Vec<_>>()
            .chunks(41)
            .map(|rows| {
                property_edges(
                    &rows.iter().map(|id| uuid(0x20, *id)).collect::<Vec<_>>(),
                    &rows
                        .iter()
                        .map(|id| ids[*id as usize % 300])
                        .collect::<Vec<_>>(),
                    &rows
                        .iter()
                        .map(|id| ids[(*id as usize + 1) % 300])
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        (nodes, edges)
    }

    #[test]
    fn cancelled_property_spools_are_discarded_and_rerun_bytes_match() {
        let (nodes, edges) = property_recovery_input();
        let expected = staged(&nodes, &edges);
        let root = TempDir::new().unwrap();
        let mut session = pinned(&root);
        let scratch_path = scratch_dir(&session);
        let mut plan = plan(&nodes, &edges, 2);
        plan.memory_budget = Some(920 << 20);
        let error = session
            .prepare_bulk_encoding(1, &plan, || {
                std::fs::read_dir(&scratch_path)
                    .into_iter()
                    .flatten()
                    .flatten()
                    .any(|entry| entry.file_name().to_string_lossy().contains("owner-"))
            })
            .unwrap_err();
        assert!(error.to_string().contains("cancelled"), "{error}");
        assert!(!scratch_dir(&session).exists());
        let rerun = session.prepare_bulk_encoding(1, &plan, || false).unwrap();
        assert_same(&expected, &inventory(&rerun));
    }

    #[test]
    fn property_scratch_crash_child() {
        let Ok(path) = std::env::var("GF_BULK_PROPERTY_CRASH_ROOT") else {
            return;
        };
        let (nodes, edges) = property_recovery_input();
        let mut session = GraphConstructionSession::open(
            Path::new(&path),
            Uuid::from_u128(OPERATION),
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session.checkpoint.session_now_micros = CLOCK;
        let mut plan = plan(&nodes, &edges, 2);
        plan.memory_budget = Some(920 << 20);
        session.prepare_bulk_encoding(1, &plan, || false).unwrap();
    }

    #[test]
    fn killed_property_windows_are_discarded_on_recovery_and_rerun_is_identical() {
        let (nodes, edges) = property_recovery_input();
        let expected = staged(&nodes, &edges);
        let root = TempDir::new().unwrap();
        let initial = pinned(&root);
        let scratch_path = scratch_dir(&initial);
        drop(initial);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("graph_construction::tests::bulk_builder::property_scratch_crash_child")
            .arg("--nocapture")
            .env("GF_BULK_PROPERTY_CRASH_ROOT", root.path())
            .env(
                "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                "graphforge-construction-test-v1",
            )
            .env("GF_CONSTRUCTION_FAILPOINT", "bulk.after_property_window")
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86));
        assert!(scratch_path.exists());
        let mut session = pinned(&root);
        assert!(!scratch_dir(&session).exists());
        let mut plan = plan(&nodes, &edges, 2);
        plan.memory_budget = Some(920 << 20);
        let rerun = session.prepare_bulk_encoding(1, &plan, || false).unwrap();
        assert_same(&expected, &inventory(&rerun));
        assert!(!scratch_dir(&session).exists());
    }

    // ------------------------------------------------------------------
    // Concurrent property scratch (#1938).
    // ------------------------------------------------------------------

    /// Property-bearing nodes in two schemas and edges in two, arriving in
    /// scattered order so that every task holds identities from the whole range.
    fn concurrent_property_graph(
        nodes: usize,
        edges: usize,
        chunk: usize,
    ) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
        let ids = (0..nodes as u64).map(|i| uuid(0x10, i)).collect::<Vec<_>>();
        let node_order = (0..nodes).map(|i| scattered(i, nodes)).collect::<Vec<_>>();
        let node_batches = node_order
            .chunks(chunk)
            .enumerate()
            .map(|(index, rows)| {
                property_nodes(
                    &rows.iter().map(|row| ids[*row]).collect::<Vec<_>>(),
                    index % 3 != 0,
                )
            })
            .collect::<Vec<_>>();
        let edge_order = (0..edges).map(|i| scattered(i, edges)).collect::<Vec<_>>();
        let edge_batches = edge_order
            .chunks(chunk)
            .enumerate()
            .map(|(index, rows)| {
                let uuids = rows
                    .iter()
                    .map(|row| uuid(0x20, *row as u64))
                    .collect::<Vec<_>>();
                let src = rows
                    .iter()
                    .map(|row| ids[(row * 7 + 1) % nodes])
                    .collect::<Vec<_>>();
                let dst = rows
                    .iter()
                    .map(|row| ids[(row * 13 + 5) % nodes])
                    .collect::<Vec<_>>();
                if index % 4 == 3 {
                    edge_batch_of(&uuids, &vec!["OWNS"; uuids.len()], &src, &dst)
                } else {
                    property_edges(&uuids, &src, &dst)
                }
            })
            .collect::<Vec<_>>();
        (node_batches, edge_batches)
    }

    fn with_padding(batch: RecordBatch, bytes: usize) -> RecordBatch {
        let mut fields = batch.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("padding", DataType::Utf8, false)));
        let mut columns = batch.columns().to_vec();
        let value = "p".repeat(bytes);
        columns.push(Arc::new(StringArray::from(vec![value; batch.num_rows()])));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    fn padded_concurrent_property_graph(
        nodes: usize,
        edges: usize,
        chunk: usize,
        padding_bytes: usize,
    ) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
        let (nodes, edges) = concurrent_property_graph(nodes, edges, chunk);
        let nodes = nodes
            .into_iter()
            .map(|batch| with_padding(batch, padding_bytes))
            .collect();
        let edges = edges
            .into_iter()
            .map(|batch| {
                if batch.num_columns() == 4 {
                    batch
                } else {
                    with_padding(batch, padding_bytes)
                }
            })
            .collect();
        (nodes, edges)
    }

    /// Sixteen lanes, whatever the host has, so the derived concurrency does
    /// not depend on the machine running the test.
    fn sixteen_lanes(session: &mut GraphConstructionSession) {
        lanes(session, 16);
    }

    fn lanes(session: &mut GraphConstructionSession, count: usize) {
        session.set_cpu_admission(Some(Arc::new(
            cpu_admission::ConstructionCpuAdmission::new(
                std::num::NonZeroUsize::new(count).unwrap(),
            ),
        )));
    }

    /// A reader that records how many tasks decode at once.
    struct Overlap {
        inner: Memory,
        in_flight: std::sync::atomic::AtomicUsize,
        peak: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl BulkBatchReader for Overlap {
        fn schema_resident_bytes(&self) -> u64 {
            self.inner.schema_resident_bytes()
        }

        fn task_rows(&self, task: usize) -> usize {
            self.inner.task_rows(task)
        }

        fn read_task(
            &self,
            task: usize,
            sink: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
        ) -> Result<(), GfError> {
            use std::sync::atomic::Ordering::SeqCst;
            let now = self.in_flight.fetch_add(1, SeqCst) + 1;
            self.peak.fetch_max(now, SeqCst);
            // Long enough that tasks claimed by different workers overlap.
            std::thread::sleep(std::time::Duration::from_millis(20));
            let result = self.inner.read_task(task, sink);
            self.in_flight.fetch_sub(1, SeqCst);
            result
        }
    }

    fn overlapping_source(
        batches: &[RecordBatch],
        required: usize,
        per_task: usize,
        peak: &Arc<std::sync::atomic::AtomicUsize>,
    ) -> BulkSource<'static> {
        let mut source = source(batches, required, per_task);
        source.reader = Arc::new(Overlap {
            inner: Memory {
                batches: batches.to_vec(),
                per_task,
            },
            in_flight: std::sync::atomic::AtomicUsize::new(0),
            peak: Arc::clone(peak),
        });
        source
    }

    fn overlapping_plan(
        nodes: &[RecordBatch],
        edges: &[RecordBatch],
        per_task: usize,
        peak: &Arc<std::sync::atomic::AtomicUsize>,
        budget: u64,
    ) -> BulkBuildPlan<'static> {
        BulkBuildPlan {
            nodes: vec![overlapping_source(nodes, 2, per_task, peak)],
            edges: vec![overlapping_source(edges, 4, per_task, peak)],
            memory_budget: Some(budget),
        }
    }

    /// The smallest budget, in 4 MiB steps, whose derived concurrency is at
    /// least `wanted`, and that concurrency.
    fn budget_admitting(
        nodes: &[RecordBatch],
        edges: &[RecordBatch],
        budgets: GraphConstructionBudgets,
        wanted: usize,
        lanes: usize,
    ) -> (u64, usize) {
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        budget_admitting_with(
            &|budget| overlapping_plan(nodes, edges, 2, &peak, budget),
            budgets,
            wanted,
            lanes,
        )
    }

    fn budget_admitting_with(
        plan_at: &dyn Fn(u64) -> BulkBuildPlan<'static>,
        budgets: GraphConstructionBudgets,
        wanted: usize,
        lanes: usize,
    ) -> (u64, usize) {
        let floor = crate::graph_construction_encoding::bulk_test_support::scratch_minimum_bytes(
            &plan_at(0),
            budgets,
        );
        let mut budget = floor.next_multiple_of(4 << 20);
        loop {
            let probe = plan_at(budget);
            let derived =
                crate::graph_construction_encoding::bulk_test_support::derived_concurrency(
                    &probe, budget, lanes, budgets,
                );
            if derived >= wanted {
                assert_eq!(probe.route(), crate::BulkRoute::Scratch, "budget {budget}");
                return (budget, derived);
            }
            budget += 4 << 20;
            assert!(budget < 16 << 30, "no budget admits {wanted} workers");
        }
    }

    fn small_property_budgets() -> GraphConstructionBudgets {
        // Small windows keep the property workspace, and so the smallest budget
        // that admits a scratch build, a few hundred MiB.
        GraphConstructionBudgets {
            max_batch_rows: 1_024,
            max_run_records: 4 * 1_024,
            max_batch_bytes: 32 << 20,
            max_catalog_identifier_bytes: 1 << 20,
            ..GraphConstructionBudgets::default()
        }
    }

    fn rss_property_budgets() -> GraphConstructionBudgets {
        // The RSS fixture creates 400-row UTF-8 batches with 5,000 bytes per
        // value. Eight MiB bounds those measured batches while avoiding the
        // unrelated 32 MiB default window in the route's shared workspace.
        GraphConstructionBudgets {
            max_batch_rows: 512,
            max_run_records: 4 * 1_024,
            max_batch_bytes: 8 << 20,
            max_catalog_identifier_bytes: 1 << 20,
            ..GraphConstructionBudgets::default()
        }
    }

    #[test]
    fn property_scratch_bytes_do_not_grow_with_the_number_of_runs() {
        let budgets = small_property_budgets();
        let (nodes, edges) = concurrent_property_graph(4_800, 9_600, 150);
        let expected = staged_with(budgets, &nodes, &edges).unwrap();
        let (budget, derived) = budget_admitting(&nodes, &edges, budgets, 1, 1);
        assert_eq!(derived, 1);
        let mut written = Vec::new();
        for per_task in [64, 2, 1] {
            let mut plan = plan(&nodes, &edges, per_task);
            plan.memory_budget = Some(budget);
            assert_eq!(plan.route(), crate::BulkRoute::Scratch);
            let root = TempDir::new().unwrap();
            let mut session = pinned_with(&root, budgets);
            lanes(&mut session, 1);
            let encoding = session.prepare_bulk_encoding(1, &plan, || false).unwrap();
            assert_same(&expected, &inventory(&encoding));
            let report = session.bulk_build_report();
            assert_eq!(report.scratch_concurrency, 1, "{report:?}");
            assert!(!scratch_dir(&session).exists());
            written.push((
                per_task,
                report.property_runs,
                report.property_scratch_write_bytes,
            ));
        }
        assert!(
            written[0].1 < written[1].1 && written[1].1 < written[2].1,
            "{written:?}"
        );
        let single = written[0].2;
        let plan = plan(&nodes, &edges, 1);
        let fan_in = crate::graph_construction_encoding::bulk_test_support::property_fan_in(
            &plan, budget, 1, budgets,
        ) as u64;
        for (per_task, runs, bytes) in &written[..2] {
            assert!(
                *bytes <= single * 3,
                "{per_task} batches per task ({runs} runs, fan-in {fan_in}) wrote {bytes}, one task wrote {single}"
            );
        }
        let (_, runs, bytes) = written[2];
        assert!(runs as u64 > fan_in, "{written:?}; natural fan-in {fan_in}");
        let mut levels = 0_u32;
        let mut reduced = runs as u64;
        while reduced > fan_in {
            reduced = reduced.div_ceil(fan_in);
            levels += 1;
        }
        assert!(bytes <= single * (u64::from(levels) + 2), "{written:?}");
    }

    const RSS_ROOT: &str = "GF_BULK_RSS_ROOT";
    const RSS_BUDGET: &str = "GF_BULK_RSS_BUDGET";
    const RSS_LANES: &str = "GF_BULK_RSS_LANES";
    const RSS_PER_TASK: &str = "GF_BULK_RSS_PER_TASK";

    // Keep the natural Memory estimate above the budget that admits 16 scratch
    // workers; otherwise the planner correctly takes the in-memory route.
    const WIDE_ROWS: usize = 40_000;
    const WIDE_BATCH: usize = 400;

    /// Batch `index` of the wide nodes: 400 identities from across the range.
    fn wide_node_batch(index: usize) -> RecordBatch {
        let rows = (index * WIDE_BATCH..(index + 1) * WIDE_BATCH)
            .map(|i| scattered(i, WIDE_ROWS))
            .collect::<Vec<_>>();
        wide_nodes(
            &rows
                .iter()
                .map(|row| uuid(0x10, *row as u64))
                .collect::<Vec<_>>(),
            (index * WIDE_BATCH) as u64,
        )
    }

    /// Batch `index` of the wide edges.
    fn wide_edge_batch(index: usize) -> RecordBatch {
        let rows = (index * WIDE_BATCH..(index + 1) * WIDE_BATCH)
            .map(|i| scattered(i, WIDE_ROWS))
            .collect::<Vec<_>>();
        wide_edges(
            &rows
                .iter()
                .map(|row| uuid(0x20, *row as u64))
                .collect::<Vec<_>>(),
            &rows
                .iter()
                .map(|row| uuid(0x10, ((row * 7 + 1) % WIDE_ROWS) as u64))
                .collect::<Vec<_>>(),
            &rows
                .iter()
                .map(|row| uuid(0x10, ((row * 13 + 5) % WIDE_ROWS) as u64))
                .collect::<Vec<_>>(),
            50_000 + (index * WIDE_BATCH) as u64,
        )
    }

    fn wide_property_graph() -> (Vec<RecordBatch>, Vec<RecordBatch>) {
        let batches = WIDE_ROWS / WIDE_BATCH;
        (
            (0..batches).map(wide_node_batch).collect(),
            (0..batches).map(wide_edge_batch).collect(),
        )
    }

    fn assert_wide_batches_fit(budgets: GraphConstructionBudgets) {
        let (nodes, edges) = wide_property_graph();
        for batch in nodes.iter().chain(&edges) {
            assert!(
                batch.num_rows() <= budgets.max_batch_rows,
                "{} rows",
                batch.num_rows()
            );
            assert!(
                batch.get_array_memory_size() <= budgets.max_batch_bytes,
                "{} source bytes exceed {}",
                batch.get_array_memory_size(),
                budgets.max_batch_bytes
            );
        }
    }

    /// Source batches made on demand, so that a build under test holds none of
    /// its input and its peak resident set is the builder's.
    struct Generated {
        batches: usize,
        per_task: usize,
        make: fn(usize) -> RecordBatch,
        in_flight: Arc<std::sync::atomic::AtomicUsize>,
        peak: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl BulkBatchReader for Generated {
        fn task_rows(&self, task: usize) -> usize {
            let first = task * self.per_task;
            (self.batches.min(first + self.per_task) - first.min(self.batches)) * WIDE_BATCH
        }

        fn read_task(
            &self,
            task: usize,
            sink: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
        ) -> Result<(), GfError> {
            use std::sync::atomic::Ordering::SeqCst;
            let first = task * self.per_task;
            let now = self.in_flight.fetch_add(1, SeqCst) + 1;
            self.peak.fetch_max(now, SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(20));
            for index in first..self.batches.min(first + self.per_task) {
                if let Err(error) = sink((self.make)(index)) {
                    self.in_flight.fetch_sub(1, SeqCst);
                    return Err(error);
                }
            }
            self.in_flight.fetch_sub(1, SeqCst);
            Ok(())
        }
    }

    fn generated_plan(
        budget: u64,
        per_task: usize,
        peak: Arc<std::sync::atomic::AtomicUsize>,
    ) -> BulkBuildPlan<'static> {
        let batches = WIDE_ROWS / WIDE_BATCH;
        let in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        assert_eq!(
            batches % per_task,
            0,
            "task groups must cover whole wide batches"
        );
        let source = |make: fn(usize) -> RecordBatch| BulkSource {
            reader: Arc::new(Generated {
                batches,
                per_task,
                make,
                in_flight: Arc::clone(&in_flight),
                peak: Arc::clone(&peak),
            }),
            tasks: batches.div_ceil(per_task),
            rows: WIDE_ROWS as u64,
            property_free: false,
            decoded_bytes: u64::try_from(
                (make(0).get_array_memory_size() as u128) * batches as u128,
            )
            .expect("generated source bytes fit u64"),
        };
        BulkBuildPlan {
            nodes: vec![source(wide_node_batch)],
            edges: vec![source(wide_edge_batch)],
            memory_budget: Some(budget),
        }
    }

    fn digest(inventory: &Inventory) -> String {
        use sha2::Digest as _;
        hex(&sha2::Sha256::digest(format!("{inventory:?}").as_bytes()))
    }

    fn peak_rss_bytes() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix("VmHWM:"))
            .and_then(|rest| rest.split_whitespace().next()?.parse::<u64>().ok())
            .map_or(0, |kib| kib * 1024)
    }

    /// One scratch build in a process of its own, so that its peak resident set
    /// is its own.
    #[test]
    fn property_scratch_rss_child() {
        let (Ok(path), Ok(budget), Ok(lane_count)) = (
            std::env::var(RSS_ROOT),
            std::env::var(RSS_BUDGET),
            std::env::var(RSS_LANES),
        ) else {
            return;
        };
        let budgets = rss_property_budgets();
        let before = peak_rss_bytes();
        let lane_count: usize = lane_count.parse().unwrap();
        let per_task = std::env::var(RSS_PER_TASK)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(2);
        let overlap = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut session = GraphConstructionSession::open(
            Path::new(&path),
            Uuid::from_u128(OPERATION),
            0,
            budgets,
        )
        .unwrap();
        session.checkpoint.session_now_micros = CLOCK;
        lanes(&mut session, lane_count);
        let budget: u64 = budget.parse().unwrap();
        let plan = generated_plan(budget, per_task, Arc::clone(&overlap));
        assert_eq!(
            plan.route(),
            crate::BulkRoute::Scratch,
            "RSS proof must use the natural production route"
        );
        assert!(
            plan.estimated_resident_bytes() > budget,
            "RSS route must be naturally admitted as Scratch"
        );
        let encoding = session.prepare_bulk_encoding(1, &plan, || false).unwrap();
        let report = session.bulk_build_report();
        assert_eq!(
            report.scratch_concurrency as usize, lane_count,
            "{report:?}"
        );
        assert!(report.property_runs > 8, "{report:?}");
        assert!(report.property_source_bytes > 0, "{report:?}");
        assert!(
            report.property_peak_retained_bytes <= report.property_retained_budget_bytes,
            "{report:?}"
        );
        assert!(
            report.decode_peak_bytes > 0 && report.decode_peak_bytes <= report.decode_pool_bytes,
            "{report:?}"
        );
        let overlapped = overlap.load(std::sync::atomic::Ordering::SeqCst);
        if per_task == 2 {
            assert!(
                overlapped >= lane_count.min(2),
                "{lane_count} workers overlapped only {overlapped}"
            );
        } else {
            let (min_request, max_request) =
                crate::graph_construction_encoding::bulk_test_support::task_decode_bytes_bounds(
                    &plan,
                );
            let expected_overlap = (report.decode_pool_bytes / min_request)
                .min(lane_count as u64)
                .min((WIDE_ROWS / WIDE_BATCH / per_task) as u64)
                as usize;
            assert!(
                min_request > report.decode_pool_bytes / 2
                    && max_request <= report.decode_pool_bytes,
                "task requests [{min_request},{max_request}] with {report:?}"
            );
            assert_eq!(overlapped, expected_overlap, "{report:?}");
        }
        assert!(
            overlapped <= lane_count,
            "{overlapped} tasks for {lane_count} workers"
        );
        assert!(!scratch_dir(&session).exists());
        println!(
            "RSS_RESULT before={before} peak={} concurrency={} overlap={} runs={} source_bytes={} retained={}/{} decode_pool={} decode_peak={} clean=1 digest={}",
            peak_rss_bytes(),
            report.scratch_concurrency,
            overlapped,
            report.property_runs,
            report.property_source_bytes,
            report.property_peak_retained_bytes,
            report.property_retained_budget_bytes,
            report.decode_pool_bytes,
            report.decode_peak_bytes,
            digest(&inventory(&encoding)),
        );
    }

    #[test]
    fn measured_peak_rss_stays_within_the_budget_at_every_concurrency() {
        let budgets = rss_property_budgets();
        let (nodes, edges) = wide_property_graph();
        assert_wide_batches_fit(budgets);
        let expected_inventory = staged_with(budgets, &nodes, &edges).unwrap();
        assert!(
            expected_inventory
                .iter()
                .any(|entry| entry.0.starts_with("edge_properties/"))
        );
        let expected = digest(&expected_inventory);
        let mut concurrencies = Vec::new();
        for wanted in [1, 2, 4, 8, 16] {
            let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let (budget, derived) = budget_admitting_with(
                &|budget| generated_plan(budget, 2, Arc::clone(&peak)),
                budgets,
                wanted,
                wanted,
            );
            let root = TempDir::new().unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("graph_construction::tests::bulk_builder::property_scratch_rss_child")
                .arg("--nocapture")
                .env(RSS_ROOT, root.path())
                .env(RSS_BUDGET, budget.to_string())
                .env(RSS_LANES, wanted.to_string())
                .env(RSS_PER_TASK, "2")
                .output()
                .unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                output.status.success(),
                "{stdout}\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let line = stdout
                .lines()
                .find(|line| line.starts_with("RSS_RESULT"))
                .unwrap_or_else(|| panic!("no result: {stdout}"));
            let field = |name: &str| {
                line.split_whitespace()
                    .find_map(|part| part.strip_prefix(&format!("{name}=")))
                    .unwrap()
                    .to_owned()
            };
            let peak: u64 = field("peak").parse().unwrap();
            assert_eq!(
                field("concurrency").parse::<usize>().unwrap(),
                derived,
                "{line}"
            );
            assert_eq!(field("digest"), expected, "{line}");
            let overlap = field("overlap").parse::<usize>().unwrap();
            assert!(overlap >= derived.min(2) && overlap <= derived, "{line}");
            assert!(field("runs").parse::<u64>().unwrap() > 8, "{line}");
            assert!(field("source_bytes").parse::<u64>().unwrap() > 0, "{line}");
            assert_eq!(field("clean"), "1", "{line}");
            assert!(
                peak <= budget,
                "peak RSS {peak} over budget {budget}: {line}"
            );
            let (retained, allowed) = field("retained")
                .split_once('/')
                .map(|(a, b)| (a.parse::<u64>().unwrap(), b.parse::<u64>().unwrap()))
                .unwrap();
            assert!(retained > 0 && retained <= allowed, "{line}");
            println!("budget {budget} concurrency {derived}: {line}");
            concurrencies.push(derived);
        }
        assert_eq!(concurrencies, vec![1, 2, 4, 8, 16]);

        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (budget, derived) = budget_admitting_with(
            &|budget| generated_plan(budget, 2, Arc::clone(&peak)),
            budgets,
            8,
            8,
        );
        assert_eq!(derived, 8);
        let probe = generated_plan(budget, 20, Arc::clone(&peak));
        let pool = crate::graph_construction_encoding::bulk_test_support::decode_pool(
            &probe, budget, 8, budgets,
        );
        let (large_min, large_max) =
            crate::graph_construction_encoding::bulk_test_support::task_decode_bytes_bounds(&probe);
        let small_plan = generated_plan(budget, 10, Arc::clone(&peak));
        let (small_min, small_max) =
            crate::graph_construction_encoding::bulk_test_support::task_decode_bytes_bounds(
                &small_plan,
            );
        assert!(large_min > pool / 2 && large_max <= pool);
        assert!(small_min > pool / 3 && small_max <= pool / 2);
        assert_eq!((pool / large_min).min(8).min(5), 1);
        assert_eq!((pool / small_min).min(8).min(10), 2);
        for (per_task, expected_overlap) in [(20, 1), (10, 2)] {
            let root = TempDir::new().unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("graph_construction::tests::bulk_builder::property_scratch_rss_child")
                .arg("--nocapture")
                .env(RSS_ROOT, root.path())
                .env(RSS_BUDGET, budget.to_string())
                .env(RSS_LANES, "8")
                .env(RSS_PER_TASK, per_task.to_string())
                .output()
                .unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                output.status.success(),
                "{stdout}\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let line = stdout
                .lines()
                .find(|line| line.starts_with("RSS_RESULT"))
                .unwrap_or_else(|| panic!("no result: {stdout}"));
            let field = |name: &str| {
                line.split_whitespace()
                    .find_map(|part| part.strip_prefix(&format!("{name}=")))
                    .unwrap()
                    .to_owned()
            };
            assert_eq!(field("concurrency"), "8", "{line}");
            assert_eq!(
                field("overlap").parse::<usize>().unwrap(),
                expected_overlap,
                "{line}"
            );
            assert_eq!(field("decode_pool").parse::<u64>().unwrap(), pool, "{line}");
            assert!(field("decode_peak").parse::<u64>().unwrap() > 0, "{line}");
            assert_eq!(field("digest"), expected, "{line}");
            assert_eq!(field("clean"), "1", "{line}");
        }
    }

    const CONCURRENT_CRASH_ROOT: &str = "GF_BULK_CONCURRENT_CRASH_ROOT";
    const CONCURRENT_CRASH_POINT: &str = "GF_BULK_CONCURRENT_CRASH_POINT";

    fn concurrent_crash_budget(nodes: &[RecordBatch], edges: &[RecordBatch]) -> u64 {
        let (budget, derived) = budget_admitting(nodes, edges, small_property_budgets(), 4, 4);
        assert_eq!(derived, 4);
        budget
    }

    /// The killed process: a concurrent scratch build that dies on the n-th
    /// time it reaches a failpoint, while other workers are mid-pass.
    #[test]
    fn property_scratch_concurrent_crash_child() {
        let (Ok(path), Ok(point)) = (
            std::env::var(CONCURRENT_CRASH_ROOT),
            std::env::var(CONCURRENT_CRASH_POINT),
        ) else {
            return;
        };
        let (name, occurrence) = point.split_once(':').unwrap();
        let (nodes, edges) = padded_concurrent_property_graph(4_800, 9_600, 150, 2 << 10);
        let budget = concurrent_crash_budget(&nodes, &edges);
        let mut session = GraphConstructionSession::open(
            Path::new(&path),
            Uuid::from_u128(OPERATION),
            0,
            small_property_budgets(),
        )
        .unwrap();
        session.checkpoint.session_now_micros = CLOCK;
        lanes(&mut session, 4);
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let plan = overlapping_plan(&nodes, &edges, 2, &peak, budget);
        assert!(plan.estimated_resident_bytes() > budget);
        assert_eq!(plan.route(), crate::BulkRoute::Scratch);
        *crate::graph_construction::ARMED_FAILPOINT.lock().unwrap() =
            Some((name.to_owned(), occurrence.parse().unwrap()));
        session.prepare_bulk_encoding(1, &plan, || false).unwrap();
    }

    #[test]
    fn a_kill_during_a_concurrent_scratch_pass_reruns_to_identical_bytes() {
        let budgets = small_property_budgets();
        let (nodes, edges) = padded_concurrent_property_graph(4_800, 9_600, 150, 2 << 10);
        let expected = staged_with(budgets, &nodes, &edges).unwrap();
        let budget = concurrent_crash_budget(&nodes, &edges);
        let mut left_scratch = 0;
        for point in [
            "bulk.during_property_run:3",
            "bulk.during_property_run:40",
            "bulk.during_property_merge:2",
            "bulk.after_property_window:1",
        ] {
            let root = TempDir::new().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("graph_construction::tests::bulk_builder::property_scratch_concurrent_crash_child")
                .arg("--nocapture")
                .env(CONCURRENT_CRASH_ROOT, root.path())
                .env(CONCURRENT_CRASH_POINT, point)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "{point}");
            left_scratch += usize::from(walkdir_has(root.path(), "bulk-scratch"));
            // Recovery: opening the session deletes what the killed attempt left.
            let mut session = pinned_with(&root, budgets);
            lanes(&mut session, 4);
            assert!(
                !scratch_dir(&session).exists(),
                "recovery kept scratch after {point}"
            );
            let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let plan = overlapping_plan(&nodes, &edges, 2, &peak, budget);
            assert!(plan.estimated_resident_bytes() > budget);
            assert_eq!(plan.route(), crate::BulkRoute::Scratch);
            let rerun = session.prepare_bulk_encoding(1, &plan, || false).unwrap();
            assert_eq!(expected, inventory(&rerun), "killed at {point}");
            assert_eq!(session.bulk_build_report().scratch_concurrency, 4);
            assert!(!scratch_dir(&session).exists());
        }
        assert!(
            left_scratch >= 3,
            "a killed attempt must leave scratch behind"
        );
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
        assert!(
            message.contains("duplicate identity across construction runs"),
            "{message}"
        );
        // An edge UUID repeated across batches.
        let message = refusal(&[node(&[a, b])], &[edge(e, a, b), edge(e, b, a)]);
        assert!(
            message.contains("duplicate identity across construction runs"),
            "{message}"
        );
        // An edge UUID equal to a node UUID.
        let message = refusal(&[node(&[a, b])], &[edge(a, a, b)]);
        assert!(
            message.contains("duplicate identity across construction runs"),
            "{message}"
        );
        // An endpoint that names no UUID at all.
        let message = refusal(&[node(&[a])], &[edge(e, a, b)]);
        assert!(
            message.contains("edge endpoint UUID does not exist"),
            "{message}"
        );
        // An endpoint that names an edge, not a node.
        let message = refusal(&[node(&[a])], &[edge(e, a, e)]);
        assert!(
            message.contains("edge endpoint is not a node UUID"),
            "{message}"
        );
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
            assert_eq!(
                expected,
                inventory(&rerun),
                "cancelled after {polls_before_cancel} polls"
            );
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
            "bulk.after_ordinal",
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

        let bulk_root = TempDir::new().unwrap();
        let mut bulk_session = open(&bulk_root);
        let built = bulk_session
            .prepare_bulk_encoding(1, &plan(&nodes, &edges, 1), || false)
            .unwrap();
        assert_same(&expected, &inventory(&built));
        for budget in [920 << 20, 944 << 20] {
            let root = TempDir::new().unwrap();
            let mut session = open(&root);
            let _frames =
                crate::graph_construction_encoding::bulk_test_support::ForcedPropertyFrames::set(1);
            let mut plan = plan(&nodes, &edges, 1);
            plan.memory_budget = Some(budget);
            let scratch = session.prepare_bulk_encoding(1, &plan, || false).unwrap();
            assert_same(&expected, &inventory(&scratch));
            assert!(session.bulk_build_report().property_scratch_write_bytes > 0);
        }
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
        let _forced = crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::set(
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
        // 28-byte edge records and two 16-byte adjacency entries per edge,
        // plus an 8-byte header per block. Edge UUIDs are not spilled: no
        // index is built from them (#1902).
        let payload = edges * (28 + 2 * 16);
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
            assert_eq!(
                expected, run.inventory,
                "partitions {partitions:?} workers {workers}"
            );
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

    /// The membership index duplicated the published Parquet UUID columns and
    /// is no longer produced (#1902). The ordinal node facet beside it is.
    #[test]
    fn neither_bulk_route_publishes_a_membership_index() {
        let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
        let memory = bulk_with(&nodes, &edges, 2, 4).unwrap();
        let scratch = scratch_run(&nodes, &edges, 2, 4, (7, 5)).unwrap().inventory;
        for (route, inventory) in [("memory", &memory), ("scratch", &scratch)] {
            let paths = inventory
                .iter()
                .map(|(path, _, _)| path.as_str())
                .collect::<Vec<_>>();
            assert!(
                paths.contains(&"topology/uuid-membership/ordinal-v4-manifest.json"),
                "{route}: the ordinal facet is the one artifact kept: {paths:?}"
            );
            let index = paths
                .iter()
                .filter(|path| {
                    path.starts_with("topology/uuid-membership/")
                        && (path.ends_with("/manifest.json")
                            || path.ends_with("/topology-receipt.json")
                            || path.contains("identities-v5")
                            || path.contains("node-surrogates-v5"))
                })
                .collect::<Vec<_>>();
            assert!(index.is_empty(), "{route} published {index:?}");
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
        let _gate =
            crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::with_gate(
                32 << 10,
            );
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
        let _gate =
            crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::with_gate(
                32 << 10,
            );
        let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
        let expected = bulk_with(&nodes, &edges, 2, 4).unwrap();
        let _forced =
            crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::set(6, 4);
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
                assert!(
                    !scratch_dir(&session).exists(),
                    "cancelled after {polls_before_cancel}"
                );
                // A live budget drop cannot make this fixed bulk route build
                // resident tables it no longer has room for. Restoring the
                // budget below still completes the same valid input.
                let mut reduced = scratch_plan(&nodes, &edges, 2);
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
                .prepare_bulk_encoding(1, &scratch_plan(&nodes, &edges, 2), || false)
                .unwrap();
            assert_eq!(
                expected,
                inventory(&rerun),
                "cancelled after {polls_before_cancel} polls"
            );
            assert!(!scratch_dir(&session).exists());
        }
    }

    /// Whether a directory called `name` exists anywhere below `root`.
    fn walkdir_has(root: &Path, name: &str) -> bool {
        std::fs::read_dir(root)
            .into_iter()
            .flatten()
            .flatten()
            .any(|entry| {
                entry.file_name() == name
                    || (entry.path().is_dir() && walkdir_has(&entry.path(), name))
            })
    }

    const CRASH_PARTITIONS: &str = "GF_BULK_CRASH_PARTITIONS";

    /// The killed process of the over-budget route.
    #[test]
    fn bulk_scratch_crash_child() {
        let (Ok(path), Ok(partitions)) =
            (std::env::var(CRASH_ROOT), std::env::var(CRASH_PARTITIONS))
        else {
            return;
        };
        let (edge, csr) = partitions.split_once(',').unwrap();
        let _forced = crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::set(
            edge.parse().unwrap(),
            csr.parse().unwrap(),
        );
        let _gate =
            crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::with_gate(
                32 << 10,
            );
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
            "bulk.after_ordinal",
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
                .prepare_bulk_encoding(1, &scratch_plan(&nodes, &edges, 2), || false)
                .unwrap();
            assert_eq!(expected, inventory(&rerun), "killed at {failpoint}");
            let report = session.bulk_build_report();
            assert!(report.edge_refinement_write_bytes > 0, "{report:?}");
            assert_scratch_traffic(&report, 3001);
            assert!(!scratch_dir(&session).exists());
        }
        assert!(
            left_scratch >= 2,
            "a killed attempt must have left scratch behind"
        );
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
        // Property payloads do not change the resident node identity footprint.
        let node_uuids = (0..600_u64).map(|i| uuid(0x10, i)).collect::<Vec<_>>();
        let edge_uuids = (0..900_u64).map(|i| uuid(0x20, i)).collect::<Vec<_>>();
        let src = (0..900)
            .map(|i| node_uuids[(i * 3) % 600])
            .collect::<Vec<_>>();
        let dst = (0..900)
            .map(|i| node_uuids[(i * 5 + 1) % 600])
            .collect::<Vec<_>>();
        let with_properties = [property_edges(&edge_uuids, &src, &dst)];
        let mut plan = super::bulk_builder::plan(
            &[node_batch_of(&node_uuids, &vec!["Person"; 600])],
            &with_properties,
            2,
        );
        plan.memory_budget = Some(SCRATCH_BUDGET);
        assert_eq!(plan.route(), crate::BulkRoute::Scratch);
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
            for batch in self
                .0
                .batches
                .iter()
                .skip(task * self.0.per_task)
                .take(self.0.per_task)
            {
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
                let ids = (chunk * 1_000..(chunk + 1) * 1_000)
                    .map(clustered)
                    .collect::<Vec<_>>();
                let src = (0..1_000)
                    .map(|i| node_uuids[(i * 7) % 500])
                    .collect::<Vec<_>>();
                let dst = (0..1_000)
                    .map(|i| node_uuids[(i * 11 + 3) % 500])
                    .collect::<Vec<_>>();
                edge_batch_of(&ids, &vec!["KNOWS"; 1_000], &src, &dst)
            })
            .collect::<Vec<_>>();
        let expected = bulk_with(&nodes, &edges, 2, 4).unwrap();
        for bounded in [false, true] {
            let _forced =
                crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::set(8, 4);
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
                let _gate = crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::with_gate(gate);
                let _parts =
                    crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::set(
                        parts, 2,
                    );
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
            let _gate =
                crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::with_gate(
                    32 << 10,
                );
            let _parts =
                crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::set(1, 2);
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
        let _gate =
            crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::with_gate(
                32 << 10,
            );
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
            let _gate =
                crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::with_gate(
                    gate,
                );
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

    /// Records the order in which tasks begin, and holds each long enough for
    /// the other workers to claim theirs.
    struct Claims {
        inner: Memory,
        log: Arc<std::sync::Mutex<Vec<(&'static str, usize)>>>,
        kind: &'static str,
    }

    impl BulkBatchReader for Claims {
        fn task_rows(&self, task: usize) -> usize {
            self.inner.task_rows(task)
        }

        fn read_task(
            &self,
            task: usize,
            sink: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
        ) -> Result<(), GfError> {
            self.log.lock().unwrap().push((self.kind, task));
            std::thread::sleep(std::time::Duration::from_millis(4));
            self.inner.read_task(task, sink)
        }
    }

    /// Tasks are claimed lowest first: a source that is read front to back, as a
    /// whole-file digest requires (#1898), is read in file order across workers,
    /// never from several far-apart positions at once.
    #[test]
    fn tasks_are_claimed_in_index_order_on_every_pass() {
        const WORKERS: usize = 4;
        let (nodes, edges) = graph(640, 1_280, 10, identity_order);
        let expected = bulk_with(&nodes, &edges, 1, WORKERS).unwrap();
        for scratch in [false, true] {
            let _forced = scratch.then(|| {
                crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::set(8, 4)
            });
            let log = Arc::new(std::sync::Mutex::new(Vec::new()));
            let mut plan = if scratch {
                scratch_plan(&nodes, &edges, 1)
            } else {
                plan(&nodes, &edges, 1)
            };
            for (sources, kind, batches) in [
                (&mut plan.nodes, "nodes", &nodes),
                (&mut plan.edges, "edges", &edges),
            ] {
                sources[0].reader = Arc::new(Claims {
                    inner: Memory {
                        batches: batches.clone(),
                        per_task: 1,
                    },
                    log: log.clone(),
                    kind,
                });
            }
            let root = TempDir::new().unwrap();
            let mut session = pinned(&root);
            session.set_cpu_admission(Some(Arc::new(
                cpu_admission::ConstructionCpuAdmission::new(
                    std::num::NonZeroUsize::new(WORKERS).unwrap(),
                ),
            )));
            let encoding = session.prepare_bulk_encoding(1, &plan, || false).unwrap();
            assert_same(&expected, &inventory(&encoding));
            let log = log.lock().unwrap();
            for kind in ["nodes", "edges"] {
                let started = log
                    .iter()
                    .filter(|(held, _)| *held == kind)
                    .map(|(_, task)| *task)
                    .collect::<Vec<_>>();
                let expected_tasks = if kind == "nodes" {
                    nodes.len()
                } else {
                    edges.len()
                };
                // The over-budget edge pass first samples 64 tasks to place its
                // splitters; the pass itself follows.
                let sampled = usize::from(scratch && kind == "edges") * 64;
                assert_eq!(
                    started.len(),
                    expected_tasks + sampled,
                    "scratch={scratch} {kind}"
                );
                let started = started[sampled..].to_vec();
                for (position, task) in started.iter().enumerate() {
                    assert!(
                        *task < position + WORKERS,
                        "scratch={scratch} {kind}: task {task} began as number {position}; \
                         workers claim the lowest unclaimed task, so none is more than {WORKERS} \
                         ahead: {started:?}"
                    );
                }
            }
        }
    }

    include!("construction_chunk_spool_tests.rs");
}
