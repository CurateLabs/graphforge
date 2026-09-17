// The determinism contract for range-partitioned shaping (#1387 §3).
//
// Range partitioning is what lets shaping stop merging, and it is also what a
// later concurrent step will schedule. These tests are the safety net for that
// step: they fix the contract now, while the partitions still run sequentially,
// so that a determinism failure after concurrency lands can only be concurrency.
//
// # What is being claimed, and what is not
//
// Shaped outputs are NOT byte-reproducible across sessions on unmodified main,
// and were not before this change. `ConstructionShape::runtime_catalog_now_micros`
// is `session_now_micros`, which is `SystemTime::now()` at session open, and it
// is written into `shaped-runtime-catalog.parquet` — a shaped output. Measured
// on `13632d4b` with the external merge tree still in place, two sessions over
// identical logical input produced identical bytes for every shaped artifact
// except that one:
//
//     shaped-identities.run          6ea5b846…  ==  6ea5b846…
//     node details                   d7eac2fd…  ==  d7eac2fd…
//     edge details                   4b9d9dd1…  ==  4b9d9dd1…
//     edge endpoints                 0b3c8415…  ==  0b3c8415…
//     shaped-rows-0-…                0592c6ed…  ==  0592c6ed…
//     shaped-rows-1-…                437d2b85…  ==  437d2b85…
//     shaped-runtime-catalog.parquet 195b24f7…  !=  ad153b93…
//
// Pinning `session_now_micros` removed that single difference and nothing else.
// That is a pre-existing durable-format defect, it is not introduced here, and
// fixing it is a separate concern.
//
// So the claim these tests make is precise:
//
//   * **Within a fixed set of recorded session parameters** — the session clock
//     and the operation UUID — identical logical input produces byte-identical
//     shaped and encoded artifacts, across separate sessions and separate
//     project directories, at every recorded partition count, and across an
//     interrupted-and-resumed run.
//   * **Range partitioning contributes no non-determinism of its own.** The
//     wall-clock artifact is isolated by
//     `unpinned_sessions_differ_only_in_the_wall_clock_runtime_catalog`, which
//     asserts that the *only* cross-session difference under an unpinned clock
//     is the runtime catalog — exactly the baseline measured above.
//
// Two session parameters are therefore pinned so the comparison measures what it
// claims to. Both are recorded inputs rather than derived state:
//
//   * `session_now_micros`. Beyond the shaped catalog, the encoded topology
//     carries `created_at`/`updated_at` stamped from the session clock.
//   * The operation UUID, which reaches the ordinal receipt.
//
// `shape_authority_sha256` is deliberately *not* compared across projects. It
// serializes `ArtifactReceipt.identity`, which carries volume serial and inode,
// so it is a local consistency authority and has never been reproducible across
// filesystems (design §9.4, confirmed here). The reproducibility contract covers
// artifact content digests and the encoded canonical inventory, compared in full
// below.
mod determinism {
    use super::*;
    use crate::graph_construction::shape::{SHAPED_RUNTIME_CATALOG, decode_splitters};
    use tempfile::TempDir;

    /// A UUIDv7-shaped identity: 48 bits of Unix-millisecond timestamp, then
    /// entropy. `graphforge-core/src/uuid.rs` mints this shape on every write
    /// path, so it is the distribution partitioning actually has to handle.
    /// Generated deterministically here because these tests compare digests.
    fn v7_like(millis: u64, entropy: u64) -> [u8; 16] {
        let mut key = [0_u8; 16];
        key[..6].copy_from_slice(&millis.to_be_bytes()[2..]);
        key[6] = 0x70;
        key[7] = (entropy >> 56) as u8;
        key[8] = 0x80 | ((entropy >> 50) as u8 & 0x3f);
        key[9..]
            .copy_from_slice(&entropy.wrapping_mul(0x9e37_79b9_7f4a_7c15).to_be_bytes()[1..]);
        key
    }

    fn ids(base_millis: u64, salt: u64, count: usize) -> Vec<[u8; 16]> {
        let mut ids = (0..count as u64)
            .map(|index| {
                v7_like(
                    base_millis + index / 512,
                    index
                        .wrapping_add(salt)
                        .wrapping_mul(0x2545_f491_4f6c_dd1d),
                )
            })
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    fn node_ids(count: usize) -> Vec<[u8; 16]> {
        ids(1_800_000_000_000, 0x51ed_2701, count)
    }

    fn edge_ids(count: usize) -> Vec<[u8; 16]> {
        ids(1_800_000_600_000, 0xa37f_00d5, count)
    }

    fn node_rows(uuids: &[[u8; 16]]) -> RecordBatch {
        RecordBatch::try_new(
            CONSTRUCTION_NODE_SCHEMA.clone(),
            vec![
                Arc::new(fixed(uuids)),
                Arc::new(StringArray::from(vec!["Person"; uuids.len()])),
            ],
        )
        .unwrap()
    }

    /// `start` is this window's offset into the full edge sequence (#1439).
    /// Every call site passes one chunk at a time, and indexing `nodes` from
    /// a per-call-local `0` on every chunk -- the previous behaviour --
    /// referenced only the first `chunk`-many nodes as an endpoint no matter
    /// how many nodes or chunks existed, concentrating every edge onto a
    /// narrow node-UUID band. That was invisible while endpoints were routed
    /// with the joint identity splitters, which never resolve node UUIDs
    /// finely enough to notice; routing them with node-only splitters
    /// surfaces it as a real (fixture-caused, not production) skew.
    fn edge_rows(start: usize, edges: &[[u8; 16]], nodes: &[[u8; 16]]) -> RecordBatch {
        let src = edges
            .iter()
            .enumerate()
            .map(|(index, _)| nodes[(start + index) % nodes.len()])
            .collect::<Vec<_>>();
        let dst = edges
            .iter()
            .enumerate()
            .map(|(index, _)| nodes[(start + index + 1) % nodes.len()])
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            CONSTRUCTION_EDGE_SCHEMA.clone(),
            vec![
                Arc::new(fixed(edges)),
                Arc::new(StringArray::from(vec!["R"; edges.len()])),
                Arc::new(fixed(&src)),
                Arc::new(fixed(&dst)),
            ],
        )
        .unwrap()
    }

    /// The one encoded artifact that is deliberately not reproducible: the v4
    /// ordinal receipt carries a freshly minted random rebuild nonce. It is a
    /// control file, not graph payload, and its non-determinism predates this
    /// work. Every other encoded artifact is compared byte for byte, and the
    /// comparison asserts this entry was actually produced so the exclusion
    /// cannot silently go stale.
    const NONCE_BEARING_CONTROLS: [&str; 1] =
        ["topology/uuid-membership/ordinal-v4-receipt.json"];

    /// Fixed session clock, so the encoder's `created_at`/`updated_at` stamps
    /// are a recorded parameter of the comparison rather than ambient time.
    const FIXED_NOW_MICROS: i64 = 1_789_000_000_000_000;

    /// One operation identity, reused across separate project directories so
    /// the ordinal receipt is comparable.
    const OPERATION: u128 = 0x5f4d_9c31_a20b_4e77_9d10_33c8_41ab_6e52;

    fn budgets(partition_count: u32) -> GraphConstructionBudgets {
        GraphConstructionBudgets {
            max_batch_rows: 512,
            max_run_records: 4 * 512,
            partition_count,
            ..GraphConstructionBudgets::default()
        }
    }

    /// Everything the reproducibility contract covers for one ingest.
    #[derive(Debug, PartialEq, Eq)]
    struct Fingerprint {
        shaped: Vec<(String, String)>,
        encoded: Vec<(String, u64, String)>,
        node_count: u64,
        edge_count: u64,
        max_node_surrogate: u64,
        max_edge_surrogate: u64,
    }

    /// Observations that are allowed to differ across partition counts.
    #[derive(Debug)]
    struct Layout {
        partitions: u64,
        recorded_partition_count: u64,
        splitters: Vec<String>,
        max_partition_identity_rows: u64,
        partitioned_identity_rows: u64,
        partition_outputs: u64,
        peak_partition_records: u64,
    }

    fn append_all(
        session: &mut GraphConstructionSession,
        nodes: &[[u8; 16]],
        edges: &[[u8; 16]],
        chunk: usize,
    ) {
        for (index, window) in nodes.chunks(chunk).enumerate() {
            session
                .append(
                    ConstructionChunkKind::Node,
                    &format!("nodes-{index}"),
                    &node_rows(window),
                )
                .unwrap();
        }
        for (index, window) in edges.chunks(chunk).enumerate() {
            session
                .append(
                    ConstructionChunkKind::Edge,
                    &format!("edges-{index}"),
                    &edge_rows(index * chunk, window, nodes),
                )
                .unwrap();
        }
    }

    fn fingerprint(
        session: &mut GraphConstructionSession,
        shape: &ConstructionShape,
    ) -> Fingerprint {
        let mut shaped = Vec::new();
        for name in std::iter::once(&shape.identities)
            .chain(shape.node_details.iter())
            .chain(shape.edge_details.iter())
            .chain(shape.node_rows.iter())
            .chain(shape.edge_rows.iter())
            .chain(shape.edge_endpoints.iter())
            .chain(std::iter::once(&shape.runtime_catalog))
        {
            let receipt = receipt_for_existing(&session.root, name).unwrap();
            shaped.push((name.clone(), receipt.sha256));
        }
        shaped.sort_unstable();
        let encoding = session.encode_canonical(shape, 1).unwrap();
        for control in NONCE_BEARING_CONTROLS {
            assert!(
                encoding
                    .artifacts
                    .iter()
                    .any(|artifact| artifact.path == control),
                "excluded control {control} was not produced"
            );
        }
        let mut encoded = encoding
            .artifacts
            .iter()
            .filter(|artifact| !NONCE_BEARING_CONTROLS.contains(&artifact.path.as_str()))
            .map(|artifact| {
                (
                    artifact.path.clone(),
                    artifact.bytes,
                    artifact.sha256.clone(),
                )
            })
            .collect::<Vec<_>>();
        encoded.sort_unstable();
        Fingerprint {
            shaped,
            encoded,
            node_count: shape.node_count,
            edge_count: shape.edge_count,
            max_node_surrogate: shape.max_node_surrogate,
            max_edge_surrogate: shape.max_edge_surrogate,
        }
    }

    fn layout(session: &GraphConstructionSession) -> Layout {
        let mut file = session
            .root
            .open_child_file(OsStr::new(SHAPE_INTENT))
            .unwrap();
        let intent: ShapeIntent = decode_shape_intent(&mut file).unwrap();
        assert!(intent.complete);
        let evidence = session.evidence();
        Layout {
            partitions: evidence.shape_partitions,
            recorded_partition_count: evidence.shape_partition_count,
            splitters: intent.splitters.clone(),
            max_partition_identity_rows: evidence.max_partition_identity_rows,
            partitioned_identity_rows: evidence.partitioned_identity_rows,
            partition_outputs: evidence.partition_outputs,
            peak_partition_records: evidence.peak_partition_records,
        }
    }

    /// A session whose recorded clock is pinned, so runs at different wall-clock
    /// instants remain comparable.
    fn pinned_session(root: &TempDir, partition_count: u32) -> GraphConstructionSession {
        let mut session = GraphConstructionSession::open(
            root.path(),
            Uuid::from_u128(OPERATION),
            0,
            budgets(partition_count),
        )
        .unwrap();
        session.checkpoint.session_now_micros = FIXED_NOW_MICROS;
        session
    }

    /// One complete ingest of the same logical rows under a recorded partition
    /// count, returning what must be identical and what may differ.
    fn ingest(
        root: &TempDir,
        partition_count: u32,
        nodes: &[[u8; 16]],
        edges: &[[u8; 16]],
        chunk: usize,
    ) -> (Fingerprint, Layout) {
        let mut session = pinned_session(root, partition_count);
        append_all(&mut session, nodes, edges, chunk);
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let fingerprint = fingerprint(&mut session, &shape);
        let layout = layout(&session);
        (fingerprint, layout)
    }

    #[test]
    fn same_input_twice_produces_identical_digests() {
        let nodes = node_ids(1_024);
        let edges = edge_ids(1_024);
        let first_root = TempDir::new().unwrap();
        let (first, first_layout) = ingest(&first_root, 64, &nodes, &edges, 128);
        let second_root = TempDir::new().unwrap();
        let (second, second_layout) = ingest(&second_root, 64, &nodes, &edges, 128);
        assert_eq!(first, second);
        assert_eq!(first_layout.splitters, second_layout.splitters);
        assert_eq!(first_layout.partitions, second_layout.partitions);
        assert!(first_layout.partitions > 1);
        println!(
            "DETERMINISM_SAME_INPUT {}",
            serde_json::json!({
                "partitions": first_layout.partitions,
                "shaped": first.shaped,
                "encoded": first.encoded,
            })
        );
    }

    /// A graph too small to fill one partition shapes into exactly one, and
    /// that is intended rather than a degenerate case to refuse.
    ///
    /// The cut is bounded by `identities / 16`, so below 32 identities the
    /// partitioned path *is* a plain sorted run: one spill, one concatenation
    /// that copies it, no splitters. That is the correct answer. A partition
    /// costs a durable spill with its own barrier and writer receipt in every
    /// family, and a two-thousand-row graph must not pay a sixty-seven-million
    /// row graph's durability price — which is exactly the regression the bound
    /// exists to prevent. Refusing the case instead would mean refusing small
    /// graphs outright.
    ///
    /// The property that must hold is that the degenerate path produces the
    /// same logical result as the partitioned one, which is asserted here
    /// against an explicit single-partition run.
    #[test]
    fn a_graph_below_one_partition_of_rows_shapes_into_exactly_one_partition() {
        let nodes = node_ids(12);
        let edges = edge_ids(8);
        assert!((nodes.len() + edges.len()) < 32);

        let root = TempDir::new().unwrap();
        let (fingerprint, layout) = ingest(&root, 256, &nodes, &edges, 8);
        assert_eq!(layout.recorded_partition_count, 256);
        assert_eq!(
            layout.partitions, 1,
            "a graph below the balance floor must not be cut into partitions \
             it cannot validate"
        );
        assert!(layout.splitters.is_empty());
        assert_eq!(
            layout.partitioned_identity_rows,
            (nodes.len() + edges.len()) as u64
        );
        assert_eq!(
            layout.max_partition_identity_rows,
            layout.partitioned_identity_rows
        );

        // The degenerate path is the partitioned path, not a second one: an
        // explicitly single-partition run produces the identical result.
        let explicit_root = TempDir::new().unwrap();
        let (explicit, explicit_layout) = ingest(&explicit_root, 1, &nodes, &edges, 8);
        assert_eq!(explicit_layout.partitions, 1);
        assert_eq!(fingerprint, explicit);
    }

    /// Characterize the one pre-existing cross-session difference, so that it
    /// cannot be mistaken for a partitioning defect and cannot silently grow.
    ///
    /// Everything range partitioning produces — the identity domain, both
    /// detail domains, the resolved endpoint domain and every shaped row
    /// Parquet — is byte-identical across two sessions with an ordinary,
    /// unpinned wall clock. Only `shaped-runtime-catalog.parquet` differs,
    /// because it embeds `runtime_catalog_now_micros`.
    #[test]
    fn unpinned_sessions_differ_only_in_the_wall_clock_runtime_catalog() {
        fn shaped_digests(nodes: &[[u8; 16]], edges: &[[u8; 16]]) -> Vec<(String, String)> {
            let root = TempDir::new().unwrap();
            // Deliberately NOT pinned: this is the ambient-clock behaviour.
            let mut session = GraphConstructionSession::open(
                root.path(),
                Uuid::from_u128(OPERATION),
                0,
                budgets(64),
            )
            .unwrap();
            append_all(&mut session, nodes, edges, 128);
            session.seal().unwrap();
            let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
            let mut digests = Vec::new();
            for name in std::iter::once(&shape.identities)
                .chain(shape.node_details.iter())
                .chain(shape.edge_details.iter())
                .chain(shape.node_rows.iter())
                .chain(shape.edge_rows.iter())
                .chain(shape.edge_endpoints.iter())
                .chain(std::iter::once(&shape.runtime_catalog))
            {
                let receipt = receipt_for_existing(&session.root, name).unwrap();
                digests.push((name.clone(), receipt.sha256));
            }
            digests.sort_unstable();
            digests
        }

        let nodes = node_ids(1_024);
        let edges = edge_ids(1_024);
        let first = shaped_digests(&nodes, &edges);
        std::thread::sleep(std::time::Duration::from_millis(5));
        let second = shaped_digests(&nodes, &edges);
        assert_eq!(
            first.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            second.iter().map(|(name, _)| name).collect::<Vec<_>>()
        );
        let differing = first
            .iter()
            .zip(second.iter())
            .filter(|(left, right)| left.1 != right.1)
            .map(|(left, _)| left.0.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            differing,
            vec![SHAPED_RUNTIME_CATALOG.to_owned()],
            "range partitioning must contribute no cross-session variance of its own; \
             the runtime catalog's wall-clock stamp is the only pre-existing one"
        );
        println!(
            "DETERMINISM_UNPINNED_CLOCK {}",
            serde_json::json!({
                "compared": first.len(),
                "differing": differing,
            })
        );
    }

    #[test]
    fn partition_count_changes_the_layout_but_not_the_logical_result() {
        let nodes = node_ids(1_024);
        let edges = edge_ids(1_024);
        let mut reference: Option<Fingerprint> = None;
        let mut observed = Vec::new();
        // #1439 extends the range up to MAX_PARTITION_COUNT, the new default
        // ceiling: at this fixture's small record count both 256 and 4_096
        // clip to the same recorded-count-bounded cut, so this proves raising
        // the ceiling all the way to its new default introduces no
        // logical-result variance either.
        for partition_count in [
            1_u32,
            4,
            64,
            256,
            crate::graph_construction::partition::MAX_PARTITION_COUNT,
        ] {
            let root = TempDir::new().unwrap();
            let (fingerprint, layout) = ingest(&root, partition_count, &nodes, &edges, 128);
            assert_eq!(layout.recorded_partition_count, u64::from(partition_count));
            assert_eq!(
                layout.splitters.len() as u64 + 1,
                layout.partitions,
                "P={partition_count}"
            );
            if partition_count == 1 {
                assert_eq!(layout.partitions, 1);
                assert!(layout.splitters.is_empty());
            } else {
                assert!(layout.partitions > 1, "P={partition_count}");
            }
            observed.push((
                partition_count,
                layout.partitions,
                layout.max_partition_identity_rows,
                layout.partitioned_identity_rows,
                layout.peak_partition_records,
            ));
            match reference.as_ref() {
                None => reference = Some(fingerprint),
                Some(expected) => assert_eq!(
                    *expected, fingerprint,
                    "partition count {partition_count} changed the logical result"
                ),
            }
        }
        // The layouts genuinely differ; the results do not.
        // The cut is bounded by the recorded record count on two independent
        // terms, `min`-combined (#1439): a small-scale floor of
        // `identities / 16` (unchanged since before #1439) and a data-driven
        // target floored at `DEFAULT_PARTITION_COUNT`. 2048 identities admit
        // at most 2048/16 = 128 partitions from the small-scale floor, which
        // is far below the data-driven target's floor of 256, so the
        // small-scale floor is what binds at this fixture's size for every
        // requested count above it -- including the raised MAX_PARTITION_COUNT
        // default.
        let partitions = observed
            .iter()
            .map(|(_, partitions, ..)| *partitions)
            .collect::<Vec<_>>();
        let identities = observed[0].3;
        assert_eq!(identities, (nodes.len() + edges.len()) as u64);
        let expected = [
            1_u32,
            4,
            64,
            256,
            crate::graph_construction::partition::MAX_PARTITION_COUNT,
        ]
        .into_iter()
        .map(|requested| u64::from(requested).min(identities / 16))
        .collect::<Vec<_>>();
        assert_eq!(partitions, expected);
        let peaks = observed
            .iter()
            .map(|(.., peak)| *peak)
            .collect::<Vec<_>>();
        println!(
            "DETERMINISM_PARTITION_COUNTS {}",
            serde_json::json!(observed)
        );
        // More partitions means a smaller sorted set held in memory at once,
        // which is the memory property the concurrent step depends on.
        assert!(
            peaks.windows(2).all(|pair| pair[0] >= pair[1]),
            "materialized partition size must not grow with the partition count: {peaks:?}"
        );
        assert!(
            peaks[peaks.len() - 1] * 8 <= peaks[0],
            "256 partitions must materialize far less at once than 1: {peaks:?}"
        );
    }

    #[test]
    fn interrupted_shaping_resumes_to_identical_digests() {
        let nodes = node_ids(1_024);
        let edges = edge_ids(1_024);
        let reference_root = TempDir::new().unwrap();
        let (reference, reference_layout) = ingest(&reference_root, 64, &nodes, &edges, 128);

        let root = TempDir::new().unwrap();
        let mut session = pinned_session(&root, 64);
        append_all(&mut session, &nodes, &edges, 128);
        session.seal().unwrap();
        let mut polls = 0;
        let interrupted = session.shape_canonical_with_cancellation(|| {
            polls += 1;
            polls > 24
        });
        assert!(interrupted.is_err(), "shaping was expected to be cancelled");
        drop(session);

        let mut resumed = GraphConstructionSession::open(
            root.path(),
            Uuid::from_u128(OPERATION),
            0,
            budgets(64),
        )
        .unwrap();
        assert_eq!(resumed.session_now_micros(), FIXED_NOW_MICROS);
        let shape = resumed.shape_canonical_with_cancellation(|| false).unwrap();
        let resumed_fingerprint = fingerprint(&mut resumed, &shape);
        let resumed_layout = layout(&resumed);
        assert_eq!(reference, resumed_fingerprint);
        // A resumed run partitions identically: the splitters are recorded, and
        // the sampling that produces them is a pure function of the staged
        // receipts rather than of how far the interrupted run got.
        assert_eq!(reference_layout.splitters, resumed_layout.splitters);
        assert_eq!(reference_layout.partitions, resumed_layout.partitions);
    }

    #[test]
    fn recorded_splitters_are_durable_canonical_and_strictly_increasing() {
        let nodes = node_ids(1_024);
        let edges = edge_ids(1_024);
        let root = TempDir::new().unwrap();
        let (_, layout) = ingest(&root, 64, &nodes, &edges, 128);
        assert_eq!(layout.splitters.len(), 63);
        assert!(
            layout
                .splitters
                .iter()
                .all(|splitter| is_canonical_lower_hex(splitter, 32))
        );
        assert!(layout.splitters.windows(2).all(|pair| pair[0] < pair[1]));
        let decoded = decode_splitters(&layout.splitters).unwrap();
        let plan = crate::graph_construction::partition::PartitionPlan::from_recorded(
            u32::try_from(layout.recorded_partition_count).unwrap(),
            decoded,
        )
        .unwrap();
        assert_eq!(plan.partitions() as u64, layout.partitions);
        // Every staged identity routes into the partition the recorded plan
        // names, so the recorded splitters describe the produced layout.
        let mut routed = vec![0_u64; plan.partitions()];
        for uuid in nodes.iter().chain(edges.iter()) {
            routed[plan.partition_of(uuid)] += 1;
        }
        assert_eq!(
            routed.iter().copied().max().unwrap(),
            layout.max_partition_identity_rows
        );
        assert_eq!(
            routed.iter().sum::<u64>(),
            layout.partitioned_identity_rows
        );
    }

    #[test]
    fn identity_partitioning_is_measured_and_balanced_end_to_end() {
        let nodes = node_ids(4_096);
        let edges = edge_ids(4_096);
        let root = TempDir::new().unwrap();
        let (_, layout) = ingest(&root, 256, &nodes, &edges, 512);
        assert_eq!(
            layout.partitioned_identity_rows,
            (nodes.len() + edges.len()) as u64
        );
        assert!(layout.partitions >= 250, "{}", layout.partitions);
        let mean = layout.partitioned_identity_rows / layout.partitions;
        assert!(mean >= 16, "balance assertion must be engaged: mean={mean}");
        assert!(
            layout.max_partition_identity_rows * layout.partitions
                <= layout.partitioned_identity_rows
                    * crate::graph_construction::partition::BALANCE_TOLERANCE,
            "max={} partitions={} total={}",
            layout.max_partition_identity_rows,
            layout.partitions,
            layout.partitioned_identity_rows
        );
        println!(
            "PARTITION_BALANCE {}",
            serde_json::json!({
                "partitions": layout.partitions,
                "identities": layout.partitioned_identity_rows,
                "mean": mean,
                "max": layout.max_partition_identity_rows,
                "max_over_mean_x1000":
                    layout.max_partition_identity_rows * 1_000 / mean.max(1),
                "partition_outputs": layout.partition_outputs,
                "peak_partition_records": layout.peak_partition_records,
            })
        );
    }

    /// The regression the balance assertion exists to catch, proved against the
    /// real identity generator rather than a synthetic distribution.
    ///
    /// A formula over the key's high bits is a perfectly valid *range*
    /// partition: order is preserved, every digest still reproduces, and every
    /// byte-equality test above still passes. It just routes an entire UUIDv7
    /// ingest into one partition and delivers nothing. Only the balance check
    /// tells the two apart.
    #[test]
    fn balance_assertion_refuses_a_formula_partitioning_of_minted_identities() {
        use crate::graph_construction::partition::{PartitionBalance, PartitionPlan};

        let minted = (0..4_096)
            .map(|_| *graphforge_core::uuid::new_v7().as_bytes())
            .collect::<Vec<_>>();
        let partition_count = 256_u32;
        let formula = (1..u128::from(partition_count))
            .map(|index| {
                (index * (u128::MAX / u128::from(partition_count))).to_be_bytes()
            })
            .collect::<Vec<_>>();
        let plan = PartitionPlan::from_recorded(partition_count, formula).unwrap();
        let mut balance = PartitionBalance::new(plan.partitions());
        for uuid in &minted {
            balance.record(plan.partition_of(uuid)).unwrap();
        }
        assert_eq!(
            balance.max_rows(),
            minted.len() as u64,
            "minted UUIDv7 identities were expected to collapse into one partition"
        );
        let refusal = balance
            .assert_balanced("minted formula")
            .expect_err("a collapsed partitioning must be refused")
            .to_string();
        assert!(refusal.contains("skewed"), "{refusal}");
        println!(
            "PARTITION_BALANCE_SKEW {}",
            serde_json::json!({
                "partitions": plan.partitions(),
                "identities": minted.len(),
                "max": balance.max_rows(),
                "refusal": refusal,
            })
        );
    }
}
