// Reading a bulk-built generation back against its input.
//
// Included at the end of `bulk_builder`, whose helpers it shares. The bulk
// builder has no second implementation to compare with, so these helpers read a
// generation through the ordinary readers and check it against the rows that
// were submitted.

/// What an encoded generation answers when it is read back through the
/// ordinary readers: node and edge rows with their surrogates, the
/// adjacency manifest, and every non-null property cell.
#[derive(Default)]
struct ReadBack {
    /// UUID to (surrogate, runtime type id).
    nodes: std::collections::BTreeMap<[u8; 16], (u64, u32)>,
    edges: std::collections::BTreeMap<[u8; 16], BackEdge>,
    adjacency: Vec<crate::adjacency::AdjacencyManifestRow>,
    /// (is edge, owner UUID, property) to the cell as text.
    cells: std::collections::BTreeMap<(bool, [u8; 16], String), String>,
}

struct BackEdge {
    relation: String,
    source: [u8; 16],
    target: [u8; 16],
    id: u64,
    source_id: u64,
    target_id: u64,
}

fn uuid_column(batch: &RecordBatch, name: &str) -> FixedSizeBinaryArray {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .unwrap_or_else(|| panic!("{name} is not a UUID column"))
        .clone()
}

fn uuid_at(column: &FixedSizeBinaryArray, row: usize) -> [u8; 16] {
    column.value(row).try_into().unwrap()
}

fn surrogates(batch: &RecordBatch, name: &str) -> arrow::array::UInt64Array {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<arrow::array::UInt64Array>())
        .unwrap_or_else(|| panic!("{name} is not a surrogate column"))
        .clone()
}

/// Every non-null property cell of `batch` past its `required` topology
/// columns, keyed by owner and column and rendered as text.
fn property_cells(
    batch: &RecordBatch,
    required: usize,
    is_edge: bool,
    cells: &mut std::collections::BTreeMap<(bool, [u8; 16], String), String>,
) {
    let owners = uuid_column(batch, if is_edge { "edge_uuid" } else { "node_uuid" });
    for (field, column) in batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .skip(required)
    {
        if matches!(
            field.name().as_str(),
            "node_uuid" | "edge_uuid" | "__gf_property_tombstone"
        ) {
            continue;
        }
        for row in (0..batch.num_rows()).filter(|row| column.is_valid(*row)) {
            let mut text = arrow::util::display::array_value_to_string(column, row).unwrap();
            if text.len() > 256 {
                // Wide values compare by length and digest.
                use std::hash::{Hash, Hasher};
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                text.hash(&mut hasher);
                text = format!("{} bytes, hash {:x}", text.len(), hasher.finish());
            }
            cells.insert((is_edge, uuid_at(&owners, row), field.name().clone()), text);
        }
    }
}

/// The encoded generation of `encoding`, read through the ordinary readers.
fn read_back(
    session: &GraphConstructionSession,
    encoding: &GraphConstructionEncoding,
) -> ReadBack {
    let graph = session.root.path().join(&encoding.root).join("graph");
    let mut back = ReadBack::default();
    for batch in crate::read_nodes(&graph).unwrap() {
        let uuids = uuid_column(&batch, "node_uuid");
        let ids = surrogates(&batch, "node_id");
        let types = batch
            .column_by_name("type_id")
            .and_then(|array| array.as_any().downcast_ref::<arrow::array::UInt32Array>())
            .unwrap();
        for row in 0..batch.num_rows() {
            back.nodes
                .insert(uuid_at(&uuids, row), (ids.value(row), types.value(row)));
        }
    }
    // Edge and property fragments are read as published, one file at a time.
    let fragments = |prefix: &str| {
        encoding
            .artifacts
            .iter()
            .filter(|artifact| {
                artifact.path.starts_with(prefix) && artifact.path.ends_with(".parquet")
            })
            .flat_map(|artifact| {
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                    std::fs::File::open(graph.join(&artifact.path)).unwrap(),
                )
                .unwrap()
                .build()
                .unwrap()
                .map(|batch| batch.unwrap())
            })
            .collect::<Vec<_>>()
    };
    for batch in fragments("topology/edges/") {
        let (uuids, sources, targets) = (
            uuid_column(&batch, "edge_uuid"),
            uuid_column(&batch, "src_uuid"),
            uuid_column(&batch, "dst_uuid"),
        );
        let (ids, source_ids, target_ids) = (
            surrogates(&batch, "edge_id"),
            surrogates(&batch, "src_id"),
            surrogates(&batch, "dst_id"),
        );
        let relations = batch
            .column_by_name("rel_type_name")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .unwrap();
        for row in 0..batch.num_rows() {
            let edge = BackEdge {
                relation: relations.value(row).to_owned(),
                source: uuid_at(&sources, row),
                target: uuid_at(&targets, row),
                id: ids.value(row),
                source_id: source_ids.value(row),
                target_id: target_ids.value(row),
            };
            assert!(back.edges.insert(uuid_at(&uuids, row), edge).is_none());
        }
    }
    back.adjacency = crate::adjacency::read_manifest(&graph).unwrap_or_default();
    for batch in fragments("properties/") {
        property_cells(&batch, 0, false, &mut back.cells);
    }
    for batch in fragments("edge_properties/") {
        property_cells(&batch, 0, true, &mut back.cells);
    }
    back
}

/// The generation reads back as exactly the input: the same nodes, with one
/// type id per label and distinct ids for distinct labels; the same edges
/// with their relation, endpoints and the endpoints' surrogates; the
/// adjacency manifest counts every edge once per direction and relation and
/// once in the union; and every non-null property cell is present.
fn assert_reads_back(nodes: &[RecordBatch], edges: &[RecordBatch], back: &ReadBack) {
    let mut labels = std::collections::BTreeMap::new();
    let mut expected_cells = std::collections::BTreeMap::new();
    for batch in nodes {
        let uuids = uuid_column(batch, "node_uuid");
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(
                labels
                    .insert(uuid_at(&uuids, row), names.value(row).to_owned())
                    .is_none()
            );
        }
        property_cells(batch, 2, false, &mut expected_cells);
    }
    assert_eq!(back.nodes.len(), labels.len(), "node count");
    let mut types = std::collections::BTreeMap::new();
    for (uuid, label) in &labels {
        let (_, type_id) = back.nodes.get(uuid).expect("an input node is missing");
        assert_eq!(
            *types.entry(label.clone()).or_insert(*type_id),
            *type_id,
            "label {label} has two type ids"
        );
    }
    assert_eq!(
        types.values().collect::<std::collections::BTreeSet<_>>().len(),
        types.len(),
        "two labels share a type id"
    );
    let node_ids = back
        .nodes
        .values()
        .map(|(id, _)| *id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(node_ids.len(), back.nodes.len(), "node surrogates repeat");

    let mut per_relation = std::collections::BTreeMap::<String, u64>::new();
    let mut total = 0_u64;
    for batch in edges {
        let uuids = uuid_column(batch, "edge_uuid");
        let relations = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let (sources, targets) = (uuid_column(batch, "source_uuid"), uuid_column(batch, "target_uuid"));
        for row in 0..batch.num_rows() {
            let edge = back
                .edges
                .get(&uuid_at(&uuids, row))
                .expect("an input edge is missing");
            assert_eq!(edge.relation, relations.value(row));
            assert_eq!(edge.source, uuid_at(&sources, row));
            assert_eq!(edge.target, uuid_at(&targets, row));
            assert_eq!(edge.source_id, back.nodes[&edge.source].0, "source surrogate");
            assert_eq!(edge.target_id, back.nodes[&edge.target].0, "target surrogate");
            *per_relation.entry(relations.value(row).to_owned()).or_default() += 1;
            total += 1;
        }
        property_cells(batch, 4, true, &mut expected_cells);
    }
    assert_eq!(back.edges.len() as u64, total, "edge count");
    let edge_ids = back
        .edges
        .values()
        .map(|edge| edge.id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(edge_ids.len(), back.edges.len(), "edge surrogates repeat");

    if total > 0 {
        for direction in [crate::adjacency::Direction::Out, crate::adjacency::Direction::In] {
            let counted = |relation: &str| {
                back.adjacency
                    .iter()
                    .filter(|row| row.relation_type == relation && row.direction == direction)
                    .map(|row| row.edge_count)
                    .sum::<u64>()
            };
            for (relation, count) in &per_relation {
                assert_eq!(counted(relation), *count, "{relation} {direction:?}");
            }
            assert_eq!(
                counted(crate::adjacency::ALL_RELATIONS_STEM),
                total,
                "union {direction:?}"
            );
        }
    }
    assert_eq!(back.cells, expected_cells, "property cells");
}

/// Build resident on `workers`, read the generation back and check it
/// against the input and its counts; returns the inventory.
fn build_checked(
    nodes: &[RecordBatch],
    edges: &[RecordBatch],
    per_task: usize,
    workers: usize,
) -> Inventory {
    build_checked_with(GraphConstructionBudgets::default(), nodes, edges, per_task, workers)
}

fn build_checked_with(
    budgets: GraphConstructionBudgets,
    nodes: &[RecordBatch],
    edges: &[RecordBatch],
    per_task: usize,
    workers: usize,
) -> Inventory {
    let root = TempDir::new().unwrap();
    let mut session = pinned_with(&root, budgets);
    session.set_cpu_admission(Some(Arc::new(
        cpu_admission::ConstructionCpuAdmission::new(
            std::num::NonZeroUsize::new(workers).unwrap(),
        ),
    )));
    let encoding = session
        .prepare_bulk_encoding(1, &plan(nodes, edges, per_task), || false)
        .unwrap();
    assert_counts(&session, &encoding, nodes, edges);
    assert_reads_back(nodes, edges, &read_back(&session, &encoding));
    inventory(&encoding)
}

/// The report, the encoding's evidence and the input agree on the counts.
fn assert_counts(
    session: &GraphConstructionSession,
    encoding: &GraphConstructionEncoding,
    nodes: &[RecordBatch],
    edges: &[RecordBatch],
) {
    let rows = |batches: &[RecordBatch]| {
        batches.iter().map(RecordBatch::num_rows).sum::<usize>() as u64
    };
    let report = session.bulk_build_report();
    assert_eq!((report.nodes, report.edges), (rows(nodes), rows(edges)));
    assert_eq!(encoding.evidence.edge_records, rows(edges));
    assert_eq!(encoding.evidence.ordinal_records, rows(nodes));
}

/// The bulk builder publishes the derived adjacency CSR as ordinary
/// SHA-256-declared artifacts of the generation (#1388): a query process
/// hydrates and opens it instead of rebuilding it into a temporary
/// directory. Pins presence, generation stamp, and that the published index
/// validates against the published topology, on the resident route and on
/// scratch.
#[test]
fn a_bulk_build_publishes_a_current_adjacency_index_that_hydrates_and_validates() {
    use crate::adjacency::{Direction, ShardedCsrIndex, csr_path};

    let (nodes, edges) = graph(256, 1_024, 128, scattered);
    let shard_manifest = |stem: &str, direction: Direction| {
        csr_path(std::path::Path::new(""), stem, direction)
            .with_extension("csr.json")
            .to_string_lossy()
            .into_owned()
    };
    for scratch in [false, true] {
        let root = TempDir::new().unwrap();
        let mut session = pinned(&root);
        let mut plan = plan(&nodes, &edges, 2);
        if scratch {
            plan.memory_budget = Some(SCRATCH_BUDGET);
        }
        let encoding = session.prepare_bulk_encoding(1, &plan, || false).unwrap();
        let published_index = encoding
            .artifacts
            .iter()
            .map(|artifact| artifact.path.as_str())
            .filter(|path| path.starts_with("indexes/adjacency/"))
            .collect::<Vec<_>>();
        let mut expected_manifests = vec![
            shard_manifest(crate::adjacency::ALL_RELATIONS_STEM, Direction::Out),
            shard_manifest(crate::adjacency::ALL_RELATIONS_STEM, Direction::In),
        ];
        for relation in ["KNOWS", "LIVES_IN", "OWNS"] {
            expected_manifests.push(shard_manifest(relation, Direction::Out));
            expected_manifests.push(shard_manifest(relation, Direction::In));
        }
        for expected in std::iter::once("indexes/adjacency/index_manifest.parquet")
            .chain(expected_manifests.iter().map(String::as_str))
        {
            assert!(
                published_index.contains(&expected),
                "scratch={scratch}: {expected} is not among {published_index:?}"
            );
        }
        assert!(encoding.evidence.adjacency.write_bytes > 0);

        let published = session
            .publish_canonical(&encoding, Uuid::from_u128(0x71), Uuid::from_u128(0x72))
            .unwrap();
        let generation = crate::resolve_project_generation(root.path()).unwrap();
        assert_eq!(generation.generation_uuid(), published.generation_uuid);
        let inventory = generation.graph_files_inventory().unwrap().unwrap();
        assert_eq!(
            inventory
                .files
                .iter()
                .filter(|entry| entry.role == crate::GraphFileRole::Index)
                .count(),
            published_index.len()
        );

        // Hydrate exactly as a query process does, then open presence-only.
        let workspace = TempDir::new().unwrap();
        crate::materialize_graph_objects(
            generation.container_root(),
            &inventory,
            workspace.path(),
        )
        .unwrap();
        let rows = crate::adjacency::read_manifest(workspace.path()).unwrap();
        assert!(!rows.is_empty());
        assert!(rows.iter().all(|row| row.topology_generation == 1));
        assert_eq!(
            crate::read_topology_generation(workspace.path()).unwrap(),
            1
        );
        let union = ShardedCsrIndex::open(&csr_path(
            workspace.path(),
            crate::adjacency::ALL_RELATIONS_STEM,
            Direction::Out,
        ))
        .unwrap();
        assert_eq!(union.edge_count(), 1_024);
        assert!(
            crate::adjacency::validate_adjacency_index(workspace.path())
                .unwrap()
                .is_empty(),
            "scratch={scratch}"
        );
    }
}

