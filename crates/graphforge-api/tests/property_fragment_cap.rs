//! Property fragments are cut at the fixed write-time cap, and the cut changes
//! no query answer (#1388, slice E2).
//!
//! A construction import whose property column is several times the cap
//! publishes several fragments per route. Queries over it are checked against
//! answers computed from the input alone, covering projection, a filter on a
//! property, ORDER BY a property and aggregation. The manifest's declared
//! lengths bound each physical object's authentication cost. A valid large
//! value spans bounded objects and remains byte-exact through public operations.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, RecordBatch, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_api::{CONSTRUCTION_NODE_SCHEMA, GraphConstructionBudgets, GraphForge};
use graphforge_storage::property_overlay::{MAX_PROPERTY_FRAGMENT_ROWS, MAX_PROPERTY_OBJECT_BYTES};
use graphforge_storage::resolve_project_generation;

const NODES: usize = 3_000;
const PAYLOAD_BYTES: usize = 4096;
const BUCKETS: i64 = 5;

fn payload(node: usize) -> String {
    let mut state = (node as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut out = String::with_capacity(PAYLOAD_BYTES + 16);
    while out.len() < PAYLOAD_BYTES {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push_str(&format!("{state:016x}"));
    }
    out.truncate(PAYLOAD_BYTES);
    out
}

/// A permutation of `0..NODES`, so ORDER BY and equality have one answer.
fn rank(node: usize) -> i64 {
    ((node * 1_237) % NODES) as i64
}

fn bucket(node: usize) -> i64 {
    node as i64 % BUCKETS
}

fn build(path: &std::path::Path) {
    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets::default())
        .unwrap();
    let mut fields = CONSTRUCTION_NODE_SCHEMA
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new("rank", DataType::Int64, true));
    fields.push(Field::new("bucket", DataType::Int64, true));
    fields.push(Field::new("payload", DataType::Utf8, true));
    let mut identities = FixedSizeBinaryBuilder::with_capacity(NODES, 16);
    for node in 0..NODES {
        identities
            .append_value((node as u128 + 1).to_be_bytes())
            .unwrap();
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(identities.finish()) as ArrayRef,
            Arc::new(StringArray::from(vec!["Entity"; NODES])),
            Arc::new(Int64Array::from_iter_values((0..NODES).map(rank))),
            Arc::new(Int64Array::from_iter_values((0..NODES).map(bucket))),
            Arc::new(StringArray::from_iter_values((0..NODES).map(payload))),
        ],
    )
    .unwrap();
    session.append_nodes("nodes", &batch).unwrap();
    session.seal_and_publish().unwrap();
}

fn column(batches: &[RecordBatch], name: &str, to: &DataType) -> ArrayRef {
    let arrays = batches
        .iter()
        .map(|batch| cast(batch.column_by_name(name).expect("column"), to).unwrap())
        .collect::<Vec<_>>();
    let refs = arrays.iter().map(AsRef::as_ref).collect::<Vec<_>>();
    arrow::compute::concat(&refs).unwrap()
}

fn ints(batches: &[RecordBatch], name: &str) -> Vec<i64> {
    let array = column(batches, name, &DataType::Int64);
    array
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .map(Option::unwrap)
        .collect()
}

fn strings(batches: &[RecordBatch], name: &str) -> Vec<String> {
    let array = column(batches, name, &DataType::Utf8);
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|value| value.unwrap().to_owned())
        .collect()
}

#[test]
fn split_property_fragments_answer_every_query_shape_unchanged() {
    let project = tempfile::tempdir().expect("project directory");
    let path = project.path().join("state");
    build(&path);

    // The import is split: several fragments, each within the cap by its
    // declared length, which is what first-touch admission reads.
    let inventory = resolve_project_generation(&path)
        .expect("project resolves")
        .unadmitted_graph_files_inventory()
        .expect("inventory reads")
        .expect("compact generation declares an inventory");
    let fragments = inventory
        .files
        .iter()
        .filter(|file| file.relative_path.starts_with("properties/"))
        .collect::<Vec<_>>();
    let declared = fragments.iter().map(|file| file.byte_length).sum::<u64>();
    assert!(
        fragments.len() >= 3,
        "{} fragments, {declared} bytes",
        fragments.len()
    );
    assert!(NODES <= MAX_PROPERTY_FRAGMENT_ROWS * fragments.len());
    for file in &fragments {
        assert!(
            file.byte_length <= MAX_PROPERTY_OBJECT_BYTES as u64,
            "{} is {} bytes",
            file.relative_path,
            file.byte_length
        );
    }

    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();

    // Projection of every property, ordered by one of them.
    let result = forge
        .execute("MATCH (n) RETURN n.rank AS rank, n.payload AS payload ORDER BY rank")
        .unwrap();
    let mut expected = (0..NODES)
        .map(|n| (rank(n), payload(n)))
        .collect::<Vec<_>>();
    expected.sort();
    let got = ints(&result.batches, "rank")
        .into_iter()
        .zip(strings(&result.batches, "payload"))
        .collect::<Vec<_>>();
    assert_eq!(got, expected);

    // Filter on a property, projecting a property from the same fragment.
    let target = 1_234_usize % NODES;
    let owner = (0..NODES).find(|n| rank(*n) == target as i64).unwrap();
    let result = forge
        .execute(&format!(
            "MATCH (n) WHERE n.rank = {target} RETURN n.payload AS payload"
        ))
        .unwrap();
    assert_eq!(strings(&result.batches, "payload"), vec![payload(owner)]);

    // ORDER BY a property with LIMIT.
    let result = forge
        .execute("MATCH (n) RETURN n.rank AS rank ORDER BY rank DESC LIMIT 5")
        .unwrap();
    let top = (NODES as i64 - 5..NODES as i64).rev().collect::<Vec<_>>();
    assert_eq!(ints(&result.batches, "rank"), top);

    // Aggregation across every fragment.
    let result = forge
        .execute(
            "MATCH (n) RETURN n.bucket AS bucket, count(n) AS c, sum(n.rank) AS s ORDER BY bucket",
        )
        .unwrap();
    let expected_counts = (0..BUCKETS)
        .map(|b| (0..NODES).filter(|n| bucket(*n) == b).count() as i64)
        .collect::<Vec<_>>();
    let expected_sums = (0..BUCKETS)
        .map(|b| {
            (0..NODES)
                .filter(|n| bucket(*n) == b)
                .map(rank)
                .sum::<i64>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        ints(&result.batches, "bucket"),
        (0..BUCKETS).collect::<Vec<_>>()
    );
    assert_eq!(ints(&result.batches, "c"), expected_counts);
    assert_eq!(ints(&result.batches, "s"), expected_sums);
}

/// Printable, poorly compressible text. One value is larger than a physical
/// object but remains below the existing default property-row read budget.
fn large_payload() -> String {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..7 * 1024 * 1024)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(33 + (state % 94) as u8)
        })
        .collect()
}

fn identities(values: &[u128]) -> ArrayRef {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
    for value in values {
        builder.append_value(value.to_be_bytes()).unwrap();
    }
    Arc::new(builder.finish())
}

fn build_large_properties(path: &std::path::Path, payload: &str) {
    let forge = GraphForge::new(path.to_str()).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets::default())
        .unwrap();
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("rank", DataType::Int64, true)));
    fields.push(Arc::new(Field::new("payload", DataType::Utf8, true)));
    session
        .append_nodes(
            "nodes",
            &RecordBatch::try_new(
                Arc::new(Schema::new(fields)),
                vec![
                    identities(&[1, 2]),
                    Arc::new(StringArray::from(vec!["Entity", "Entity"])),
                    Arc::new(Int64Array::from(vec![1, 2])),
                    Arc::new(StringArray::from(vec![Some(payload), None])),
                ],
            )
            .unwrap(),
        )
        .unwrap();
    let mut fields = graphforge_api::CONSTRUCTION_EDGE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("payload", DataType::Utf8, true)));
    session
        .append_edges(
            "edges",
            &RecordBatch::try_new(
                Arc::new(Schema::new(fields)),
                vec![
                    identities(&[3]),
                    Arc::new(StringArray::from(vec!["LINK"])),
                    identities(&[1]),
                    identities(&[2]),
                    Arc::new(StringArray::from(vec![payload])),
                ],
            )
            .unwrap(),
        )
        .unwrap();
    session.seal_and_publish().unwrap();
}

fn assert_large_answers(forge: &GraphForge, expected: &str) {
    for query in [
        "MATCH (n) WHERE n.rank = 1 RETURN n.payload AS payload",
        "MATCH ()-[r]->() RETURN r.payload AS payload",
    ] {
        let result = forge.execute(query).unwrap();
        assert_eq!(
            strings(&result.batches, "payload"),
            vec![expected.to_owned()]
        );
    }
    let result = forge
        .execute("MATCH (n) WHERE n.rank = 2 RETURN n.payload AS payload")
        .unwrap();
    let values = column(&result.batches, "payload", &DataType::Utf8);
    assert_eq!(values.len(), 1);
    assert!(values.is_null(0));
}

fn assert_bounded_objects(path: &std::path::Path) -> Vec<graphforge_storage::GraphFileEntry> {
    let files = resolve_project_generation(path)
        .unwrap()
        .unadmitted_graph_files_inventory()
        .unwrap()
        .unwrap()
        .files
        .into_iter()
        .filter(|entry| {
            entry.relative_path.starts_with("properties/")
                || entry.relative_path.starts_with("edge_properties/")
        })
        .collect::<Vec<_>>();
    for entry in &files {
        assert!(
            entry.byte_length <= MAX_PROPERTY_OBJECT_BYTES as u64,
            "{}: {} bytes",
            entry.relative_path,
            entry.byte_length
        );
    }
    for domain in ["properties/", "edge_properties/"] {
        assert!(
            files.iter().any(|entry| {
                entry.relative_path.starts_with(domain) && entry.relative_path.contains(".part-")
            }),
            "{domain} must exercise actual encoded segmentation"
        );
    }
    files
}

#[test]
fn oversized_property_values_round_trip_through_bounded_objects() {
    use graphforge_api::{
        OperationId, PortableSelection, PortableV2ExportRequest, PortableV2ImportRequest,
        PortableV2Limits, PortableV2Output, PortableV2SelectionProfile,
    };
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("source");
    let payload = large_payload();
    build_large_properties(&path, &payload);
    assert_bounded_objects(&path);
    let forge = GraphForge::new(path.to_str()).unwrap();
    assert_large_answers(&forge, &payload);
    forge
        .execute("MATCH (n) WHERE n.rank = 1 SET n.flag = true")
        .unwrap();
    forge.execute("MATCH ()-[r]->() SET r.flag = true").unwrap();
    drop(forge);
    let forge = GraphForge::new(path.to_str()).unwrap();
    assert_large_answers(&forge, &payload);
    assert_bounded_objects(&path);

    let package = root.path().join("large-values.gfpb");
    forge
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: package.clone(),
                representation: PortableV2Output::Bundle,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits: PortableV2Limits::default(),
            },
            None,
            |_| {},
        )
        .unwrap();
    let imported = root.path().join("imported");
    GraphForge::import_portable_v2(
        &imported,
        &PortableV2ImportRequest {
            input: package,
            operation_id: OperationId(uuid::Uuid::now_v7()),
            limits: PortableV2Limits::default(),
        },
        None,
    )
    .unwrap();
    assert_bounded_objects(&imported);
    assert_large_answers(&GraphForge::new(imported.to_str()).unwrap(), &payload);
}

#[test]
fn corrupted_property_part_is_refused_after_facade_open() {
    use std::io::{Read, Seek, SeekFrom, Write};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("source");
    build_large_properties(&path, &large_payload());
    let files = assert_bounded_objects(&path);
    let part = files
        .iter()
        .find(|entry| {
            entry.relative_path.starts_with("properties/") && entry.relative_path.contains(".part-")
        })
        .unwrap();
    let forge = GraphForge::new(path.to_str()).unwrap();
    let object = graphforge_storage::graph_object_path(&path, &part.content_sha256).unwrap();
    let permissions = std::fs::metadata(&object).unwrap().permissions();
    let mut writable = permissions.clone();
    writable.set_readonly(false);
    std::fs::set_permissions(&object, writable).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&object)
        .unwrap();
    let modified = file.metadata().unwrap().modified().unwrap();
    let offset = part.byte_length / 2;
    let mut byte = [0];
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.read_exact(&mut byte).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&[byte[0] ^ 0xff]).unwrap();
    file.set_modified(modified).unwrap();
    drop(file);
    std::fs::set_permissions(&object, permissions).unwrap();
    let error = forge
        .execute("MATCH (n) WHERE n.rank = 1 RETURN n.payload")
        .unwrap_err();
    assert_eq!(error.code(), "GF_PROJECT_CORRUPT", "{error}");
}
