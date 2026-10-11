// Reading a bulk-built generation back against its input.
//
// Included at the end of `bulk_builder`, whose helpers it shares. The bulk
// builder has no second implementation to compare with, so these helpers read a
// generation through the ordinary readers and check it against the rows that
// were submitted.

/// What an encoded generation answers when it is read back through the
/// ordinary readers: node and edge rows with their surrogates, the adjacency
/// manifest and CSR, and every non-null property cell.
#[derive(Clone, Default)]
struct ReadBack {
    /// The published graph tree the generation was read from.
    graph: PathBuf,
    /// UUID to (surrogate, runtime type id).
    nodes: std::collections::BTreeMap<[u8; 16], (u64, u32)>,
    edges: std::collections::BTreeMap<[u8; 16], BackEdge>,
    adjacency: Vec<crate::adjacency::AdjacencyManifestRow>,
    /// Every non-null cell of each node property, by column.
    node_columns: PropertyColumns,
    /// Every non-null cell of each edge property, by column.
    edge_columns: PropertyColumns,
}

/// The non-null cells of each property column as (owner UUIDs, values), both
/// in owner order, so a column compares with its typed Arrow values.
type PropertyColumns =
    std::collections::BTreeMap<String, (arrow::array::ArrayRef, arrow::array::ArrayRef)>;

#[derive(Clone)]
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

/// The non-null cells of every property column of `batches` past their
/// `required` topology columns, keyed by owner in the `owner` column. Values
/// keep their Arrow type, so integer 10 never equals the string "10".
fn property_columns(batches: &[RecordBatch], owner: &str, required: usize) -> PropertyColumns {
    use arrow::compute::{concat, filter, sort_to_indices, take};
    let mut parts = std::collections::BTreeMap::<
        String,
        Vec<(arrow::array::ArrayRef, arrow::array::ArrayRef)>,
    >::new();
    for batch in batches {
        let owners = batch.column_by_name(owner).unwrap();
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
            let valid = arrow::array::BooleanArray::from(
                (0..column.len())
                    .map(|row| column.is_valid(row))
                    .collect::<Vec<_>>(),
            );
            parts.entry(field.name().clone()).or_default().push((
                filter(owners, &valid).unwrap(),
                filter(column, &valid).unwrap(),
            ));
        }
    }
    parts
        .into_iter()
        .filter_map(|(name, parts)| {
            let owners = concat(&parts.iter().map(|part| part.0.as_ref()).collect::<Vec<_>>())
                .unwrap();
            if owners.is_empty() {
                // A column with no cell publishes nothing.
                return None;
            }
            let values = concat(&parts.iter().map(|part| part.1.as_ref()).collect::<Vec<_>>())
                .unwrap();
            let order = sort_to_indices(&owners, None, None).unwrap();
            Some((
                name,
                (
                    take(&owners, &order, None).unwrap(),
                    take(&values, &order, None).unwrap(),
                ),
            ))
        })
        .collect()
}

/// Two property column sets hold the same cells, owner for owner and type for
/// type.
fn assert_columns_equal(label: &str, expected: &PropertyColumns, actual: &PropertyColumns) {
    assert_eq!(
        expected.keys().collect::<Vec<_>>(),
        actual.keys().collect::<Vec<_>>(),
        "{label} columns"
    );
    for (name, (expected_owners, expected_values)) in expected {
        let (owners, values) = &actual[name];
        assert_eq!(expected_values.data_type(), values.data_type(), "{label} {name} type");
        assert!(expected_owners.to_data() == owners.to_data(), "{label} {name} owners");
        assert!(expected_values.to_data() == values.to_data(), "{label} {name} values");
    }
}

/// The encoded generation of `encoding`, read through the ordinary readers.
fn read_back(
    session: &GraphConstructionSession,
    encoding: &GraphConstructionEncoding,
) -> ReadBack {
    let graph = session.root.path().join(&encoding.root).join("graph");
    let mut back = ReadBack {
        graph: graph.clone(),
        ..ReadBack::default()
    };
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
    back.node_columns = property_columns(&fragments("properties/"), "node_uuid", 0);
    back.edge_columns = property_columns(&fragments("edge_properties/"), "edge_uuid", 0);
    back
}

/// The generation reads back as exactly the input: the same nodes, with one
/// type id per label and distinct ids for distinct labels; the same edges
/// with their relation, endpoints and the endpoints' surrogates; the
/// adjacency manifest counts every edge once per direction and relation and
/// once in the union; and every non-null property cell is present.
fn assert_reads_back(nodes: &[RecordBatch], edges: &[RecordBatch], back: &ReadBack) {
    let mut labels = std::collections::BTreeMap::new();
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
    // A surrogate is the rank of its UUID: ids rise with the UUID order.
    assert!(
        back.nodes
            .values()
            .zip(back.nodes.values().skip(1))
            .all(|(low, high)| low.0 < high.0),
        "node surrogates do not follow the UUID order"
    );

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
    }
    assert_eq!(back.edges.len() as u64, total, "edge count");
    let edge_ids = back
        .edges
        .values()
        .map(|edge| edge.id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(edge_ids.len(), back.edges.len(), "edge surrogates repeat");
    assert!(
        back.edges
            .values()
            .zip(back.edges.values().skip(1))
            .all(|(low, high)| low.id < high.id),
        "edge surrogates do not follow the UUID order"
    );

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
    assert_columns_equal(
        "node property",
        &property_columns(nodes, "node_uuid", 2),
        &back.node_columns,
    );
    assert_columns_equal(
        "edge property",
        &property_columns(edges, "edge_uuid", 4),
        &back.edge_columns,
    );
    assert_csr_matches(back);
}

/// Every published CSR (each relation and the union, both directions) holds,
/// for every node, exactly the (edge, neighbour) entries an adjacency computed
/// from the edges' endpoints holds, in edge order.
fn assert_csr_matches(back: &ReadBack) {
    use crate::adjacency::{ALL_RELATIONS_STEM, Direction, ShardedCsrIndex, csr_path};
    let mut by_relation = std::collections::BTreeMap::<String, Vec<(u64, u64, u64)>>::new();
    for edge in back.edges.values() {
        let entry = (edge.source_id, edge.target_id, edge.id);
        by_relation
            .entry(edge.relation.clone())
            .or_default()
            .push(entry);
        by_relation
            .entry(ALL_RELATIONS_STEM.to_owned())
            .or_default()
            .push(entry);
    }
    for (stem, entries) in &by_relation {
        for direction in [Direction::Out, Direction::In] {
            let mut expected = std::collections::BTreeMap::<u64, Vec<(u64, u64)>>::new();
            for &(source, target, edge) in entries {
                let (key, neighbour) = match direction {
                    Direction::Out => (source, target),
                    Direction::In => (target, source),
                };
                expected.entry(key).or_default().push((edge, neighbour));
            }
            for row in expected.values_mut() {
                row.sort_unstable();
            }
            let index = ShardedCsrIndex::open(&csr_path(&back.graph, stem, direction)).unwrap();
            assert_eq!(index.edge_count(), entries.len() as u64, "{stem} {direction:?}");
            for (node, _) in back.nodes.values() {
                assert_eq!(
                    index.row(*node).unwrap(),
                    expected.get(node).cloned().unwrap_or_default(),
                    "{stem} {direction:?} node {node}"
                );
            }
        }
    }
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


/// The oracle rejects what it exists to catch: a CSR whose entries disagree
/// with the edges' endpoints, surrogates that do not follow the UUID order, and
/// a value whose type differs although its text is the same.
#[test]
fn the_read_back_oracle_rejects_a_wrong_csr_wrong_surrogates_and_a_wrong_type() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    let (nodes, edges) = graph(64, 200, 64, scattered);
    let root = TempDir::new().unwrap();
    let mut session = pinned(&root);
    let encoding = session
        .prepare_bulk_encoding(1, &plan(&nodes, &edges, 2), || false)
        .unwrap();
    let back = read_back(&session, &encoding);
    assert_reads_back(&nodes, &edges, &back);

    // An edge whose recorded target is another node than the CSR holds.
    let mut wrong_csr = back.clone();
    wrong_csr.edges.values_mut().next().unwrap().target_id += 1;
    assert!(catch_unwind(AssertUnwindSafe(|| assert_csr_matches(&wrong_csr))).is_err());

    // Two nodes whose surrogates swap against their UUID order.
    let mut wrong_order = back.clone();
    let mut ids = wrong_order.nodes.values_mut().take(2);
    let (first, second) = (ids.next().unwrap(), ids.next().unwrap());
    std::mem::swap(&mut first.0, &mut second.0);
    assert!(
        catch_unwind(AssertUnwindSafe(|| assert_reads_back(
            &nodes,
            &edges,
            &wrong_order
        )))
        .is_err()
    );

    // Integer 10 against the string "10".
    let owner: arrow::array::ArrayRef = Arc::new(fixed(&[uuid(0x10, 0)]));
    let column = |values: arrow::array::ArrayRef| {
        PropertyColumns::from([("x".to_owned(), (owner.clone(), values))])
    };
    let integer = column(Arc::new(Int64Array::from(vec![10])));
    let text = column(Arc::new(StringArray::from(vec!["10"])));
    assert_columns_equal("same", &integer, &integer);
    assert!(
        catch_unwind(AssertUnwindSafe(|| assert_columns_equal(
            "different",
            &integer,
            &text
        )))
        .is_err()
    );
}
