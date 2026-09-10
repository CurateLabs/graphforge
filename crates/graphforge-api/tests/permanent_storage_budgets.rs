//! Reproducible permanent ownership and candidate-codec assessment (#1196).
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write,
    fs::File,
    path::Path,
    sync::Arc,
    time::Instant,
};

use arrow::{
    array::{Array, ArrayRef, FixedSizeBinaryArray, Int64Array, ListArray, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, GraphConstructionBudgets, GraphForge,
    OperationId, PortableSelection, PortableV2ExportRequest, PortableV2ImportRequest,
    PortableVerifyRequest, verify_portable_v2,
};
use graphforge_core::portable::{
    PortableV2Limits, PortableV2Mode, PortableV2Output, PortableV2SelectionProfile,
};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

type Node = (Uuid, String, Option<i64>);
type Edge = (Uuid, Uuid, Uuid, String, Option<i64>, Option<String>);

#[derive(Clone, Copy)]
enum Identifiers {
    Sequential,
    Random,
}

#[derive(Clone, Copy)]
struct Fixture {
    name: &'static str,
    nodes: usize,
    edges: usize,
    routes: usize,
    identifiers: Identifiers,
    properties: bool,
    adjacency: bool,
    heterogeneous: bool,
}

fn id(domain: u8, row: usize, random: bool) -> Uuid {
    if random {
        let digest = Sha256::digest(format!("graphforge-1196/{domain}/{row}"));
        Uuid::from_slice(&digest[..16]).unwrap()
    } else {
        Uuid::from_u128((u128::from(domain) << 120) | (row as u128 + 1))
    }
}

fn rows(f: Fixture) -> (Vec<Node>, Vec<Edge>) {
    let nodes = (0..f.nodes)
        .map(|i| {
            (
                id(1, i, matches!(f.identifiers, Identifiers::Random)),
                format!("Node{}", i % f.routes),
                (f.properties && i % 7 != 0 && (!f.heterogeneous || (i / 1024).is_multiple_of(2)))
                    .then_some(i64::try_from(i).unwrap() - 2048),
            )
        })
        .collect::<Vec<_>>();
    let edges = (0..f.edges)
        .map(|i| {
            (
                id(2, i, matches!(f.identifiers, Identifiers::Random)),
                nodes[i % f.nodes].0,
                nodes[(i * 7919 + i / f.nodes) % f.nodes].0,
                format!("REL{}", i % f.routes),
                (f.properties && i % 5 != 0).then_some(i64::try_from(i).unwrap() * 17),
                (f.properties && i % 11 != 0 && (!f.heterogeneous || (i / 1024).is_multiple_of(2)))
                    .then(|| {
                        if i % 2 == 0 {
                            format!("group-{}", i % 16)
                        } else {
                            format!("unique-value-{i:020}")
                        }
                    }),
            )
        })
        .collect();
    (nodes, edges)
}

fn uuids(ids: impl Iterator<Item = Uuid>) -> ArrayRef {
    Arc::new(FixedSizeBinaryArray::try_from_iter(ids.map(|id| *id.as_bytes())).unwrap())
}

fn construct(
    path: &Path,
    f: Fixture,
    nodes: &[Node],
    edges: &[Edge],
) -> graphforge_api::GraphConstructionEvidence {
    let graph = GraphForge::new(path.to_str()).unwrap();
    let mut session = graph
        .begin_graph_construction(GraphConstructionBudgets {
            max_batch_rows: 1024,
            max_run_records: 4096,
            merge_fan_in: 2,
            ..Default::default()
        })
        .unwrap();
    for (chunk, rows) in nodes.chunks(1024).enumerate() {
        let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
        let mut arrays = vec![
            uuids(rows.iter().map(|row| row.0)),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1.as_str()).collect::<Vec<_>>(),
            )) as ArrayRef,
        ];
        if f.properties && (!f.heterogeneous || chunk.is_multiple_of(2)) {
            fields.push(Arc::new(Field::new("score", DataType::Int64, true)));
            arrays.push(Arc::new(Int64Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )));
        }
        session
            .append_nodes(
                &format!("nodes-{chunk}"),
                &RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap(),
            )
            .unwrap();
    }
    for (chunk, rows) in edges.chunks(1024).enumerate() {
        let mut fields = CONSTRUCTION_EDGE_SCHEMA.fields().to_vec();
        let mut arrays = vec![
            uuids(rows.iter().map(|row| row.0)),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.3.as_str()).collect::<Vec<_>>(),
            )) as ArrayRef,
            uuids(rows.iter().map(|row| row.1)),
            uuids(rows.iter().map(|row| row.2)),
        ];
        if f.properties {
            fields.push(Arc::new(Field::new("weight", DataType::Int64, true)));
            arrays.push(Arc::new(Int64Array::from(
                rows.iter().map(|row| row.4).collect::<Vec<_>>(),
            )));
            if !f.heterogeneous || chunk.is_multiple_of(2) {
                fields.push(Arc::new(Field::new("text", DataType::Utf8, true)));
                arrays.push(Arc::new(StringArray::from(
                    rows.iter().map(|row| row.5.as_deref()).collect::<Vec<_>>(),
                )));
            }
        }
        session
            .append_edges(
                &format!("edges-{chunk}"),
                &RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap(),
            )
            .unwrap();
    }
    session.seal_and_publish().unwrap();
    session.progress().evidence.clone()
}

fn uuid_at(batch: &RecordBatch, column: usize, row: usize) -> Uuid {
    Uuid::from_slice(
        batch
            .column(column)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(row),
    )
    .unwrap()
}
fn int_at(batch: &RecordBatch, column: usize, row: usize) -> Option<i64> {
    (!batch
        .column(column)
        .logical_nulls()
        .is_some_and(|nulls| nulls.is_null(row)))
    .then(|| {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("expected Int64 at column {column}: {:?}", batch.schema()))
            .value(row)
    })
}

fn verify_graph(
    graph: &GraphForge,
    f: Fixture,
    expected_nodes: &[Node],
    expected_edges: &[Edge],
) -> String {
    let node_query = if f.properties {
        "MATCH (n) RETURN n.node_uuid, labels(n), n.score"
    } else {
        "MATCH (n) RETURN n.node_uuid, labels(n)"
    };
    let mut nodes = Vec::new();
    for batch in graph.execute(node_query).unwrap().batches {
        let labels = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let values = labels.value(row);
            let values = values.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(values.len(), 1);
            nodes.push((
                uuid_at(&batch, 0, row),
                values.value(0).to_owned(),
                if f.properties {
                    int_at(&batch, 2, row)
                } else {
                    None
                },
            ));
        }
    }
    let edge_query = if f.properties {
        "MATCH (a)-[r]->(b) RETURN r.edge_uuid, a.node_uuid, b.node_uuid, type(r), r.weight, r.text"
    } else {
        "MATCH (a)-[r]->(b) RETURN r.edge_uuid, a.node_uuid, b.node_uuid, type(r)"
    };
    let mut edges = Vec::new();
    for batch in graph.execute(edge_query).unwrap().batches {
        let routes = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let text = if f.properties
                && !batch
                    .column(5)
                    .logical_nulls()
                    .is_some_and(|nulls| nulls.is_null(row))
            {
                Some(
                    batch
                        .column(5)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(row)
                        .to_owned(),
                )
            } else {
                None
            };
            edges.push((
                uuid_at(&batch, 0, row),
                uuid_at(&batch, 1, row),
                uuid_at(&batch, 2, row),
                routes.value(row).to_owned(),
                if f.properties {
                    int_at(&batch, 4, row)
                } else {
                    None
                },
                text,
            ));
        }
    }
    nodes.sort();
    edges.sort();
    let mut expected_nodes = expected_nodes.to_vec();
    let mut expected_edges = expected_edges.to_vec();
    expected_nodes.sort();
    expected_edges.sort();
    assert_eq!(nodes, expected_nodes);
    assert_eq!(edges, expected_edges);
    let mut fingerprint = String::new();
    for byte in Sha256::digest(serde_json::to_vec(&(nodes, edges)).unwrap()) {
        write!(fingerprint, "{byte:02x}").unwrap();
    }
    fingerprint
}

fn round_trip(root: &Path, source: &Path, f: Fixture, nodes: &[Node], edges: &[Edge]) -> String {
    round_trip_checked(root, source, |graph| verify_graph(graph, f, nodes, edges))
}

fn round_trip_checked(
    root: &Path,
    source: &Path,
    verify: impl Fn(&GraphForge) -> String,
) -> String {
    let graph = GraphForge::new(source.to_str()).unwrap();
    let fingerprint = verify(&graph);
    let limits = PortableV2Limits::default();
    let package = root.join("graph.gfpb");
    let exported = graph
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: package.clone(),
                representation: PortableV2Output::Bundle,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits,
            },
            None,
            |_| {},
        )
        .unwrap();
    let verified = verify_portable_v2(
        &PortableVerifyRequest {
            input: package.clone(),
            mode: PortableV2Mode::Full,
            limits,
        },
        None,
    )
    .unwrap();
    assert_eq!(exported.package_digest, verified.package_digest);
    drop(graph);
    let imported_path = root.join("imported");
    let imported = GraphForge::import_portable_v2(
        &imported_path,
        &PortableV2ImportRequest {
            input: package,
            operation_id: OperationId(Uuid::from_u128(1196)),
            limits,
        },
        None,
    )
    .unwrap();
    assert_eq!(exported.package_digest, imported.package_digest);
    let imported = GraphForge::new(imported_path.to_str()).unwrap();
    assert_eq!(verify(&imported), fingerprint);
    fingerprint
}

fn parquet_experiment(source: &Path) -> Value {
    let selected = graphforge_storage::resolve_project_generation(source).unwrap();
    let inventory = selected.graph_files_inventory().unwrap().unwrap();
    let mut seen = BTreeSet::new();
    let mut sizes = [0_u64; 3];
    let mut encode_ns = [0_u128; 3];
    let mut decode_ns = [0_u128; 3];
    let mut original_bytes = 0_u64;
    let mut original_allocated = 0_u64;
    let mut normalized_allocated = [0_u64; 3];
    for entry in &inventory.files {
        if !entry.relative_path.ends_with(".parquet") || !seen.insert(entry.content_sha256.clone())
        {
            continue;
        }
        let path = graphforge_storage::graph_object_path(source, &entry.content_sha256).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
            .unwrap()
            .with_batch_size(4096);
        for group in reader.metadata().row_groups() {
            for column in group.columns() {
                assert!(
                    matches!(column.compression(), Compression::ZSTD(_)),
                    "permanent construction Parquet must use the selected Zstd codec"
                );
            }
        }
        original_allocated += graphforge_filesystem::file_space_usage(&File::open(&path).unwrap())
            .unwrap()
            .allocated_bytes;
        let schema = reader.schema().clone();
        let original = reader
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        original_bytes += entry.byte_length;
        for (slot, compression) in [
            Compression::UNCOMPRESSED,
            Compression::ZSTD(ZstdLevel::try_new(1).unwrap()),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        ]
        .into_iter()
        .enumerate()
        {
            let start = Instant::now();
            let mut bytes = Vec::new();
            let mut writer = ArrowWriter::try_new(
                &mut bytes,
                schema.clone(),
                Some(
                    WriterProperties::builder()
                        .set_compression(compression)
                        .build(),
                ),
            )
            .unwrap();
            for batch in &original {
                writer.write(batch).unwrap();
            }
            writer.close().unwrap();
            encode_ns[slot] += start.elapsed().as_nanos();
            sizes[slot] += bytes.len() as u64;
            normalized_allocated[slot] += (bytes.len() as u64).div_ceil(4096) * 4096;
            let start = Instant::now();
            let decoded = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
                .unwrap()
                .with_batch_size(4096)
                .build()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            decode_ns[slot] += start.elapsed().as_nanos();
            assert_eq!(
                decoded, original,
                "codec {compression:?} must preserve schema, UUIDs, values and nulls"
            );
        }
    }
    assert!(original_bytes > 0);
    json!({"production_parquet_bytes": original_bytes, "production_parquet_allocated_bytes":original_allocated, "normalized_allocated_bytes_at_4096":normalized_allocated, "normalized_bytes_uncompressed_zstd1_zstd3":sizes,
        "encode_elapsed_ns":encode_ns, "decode_elapsed_ns":decode_ns})
}

fn pack_identity_records(original: &[u8]) -> Vec<u8> {
    assert_eq!(original.len() % 32, 0);
    let mut packed = Vec::with_capacity(original.len() / 32 * 25);
    for record in original.chunks_exact(32) {
        assert_eq!(
            &record[17..24],
            &[0; 7],
            "only reserved zero padding is omitted"
        );
        packed.extend_from_slice(&record[..17]);
        packed.extend_from_slice(&record[24..32]);
    }
    packed
}

#[test]
fn packed_identity_candidate_preserves_full_width_and_tombstone_boundaries() {
    for uuid in [[0; 16], [255; 16]] {
        for kind in 0..=3_u8 {
            for surrogate in [0_u64, 1, u32::MAX.into(), u64::from(u32::MAX) + 1, u64::MAX] {
                let mut original = [0; 32];
                original[..16].copy_from_slice(&uuid);
                original[16] = kind;
                original[24..32].copy_from_slice(&surrogate.to_be_bytes());
                let packed = pack_identity_records(&original);
                assert_eq!(&packed[..16], &uuid);
                assert_eq!(packed[16], kind);
                assert_eq!(
                    u64::from_be_bytes(packed[17..25].try_into().unwrap()),
                    surrogate
                );
            }
        }
    }
}

fn adjacency_codec_experiment(source: &Path) -> Value {
    use arrow::ipc::{
        CompressionType,
        reader::FileReader,
        writer::{FileWriter, IpcWriteOptions},
    };
    let selected = graphforge_storage::resolve_project_generation(source).unwrap();
    selected.graph_files_inventory().unwrap().unwrap();
    let mut pending = vec![selected.graph_tree_root()];
    let mut sizes = [0_u64; 3];
    let mut allocations = [0_u64; 3];
    let mut encode_ns = [0_u128; 3];
    let mut decode_ns = [0_u128; 3];
    let mut source_bytes = 0_u64;
    let mut shards = 0_u64;
    let mut largest_decoded_batch = 0_usize;
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                pending.push(entry.path());
                continue;
            }
            if entry
                .path()
                .extension()
                .is_none_or(|extension| extension != "csr")
            {
                continue;
            }
            let reader = FileReader::try_new(File::open(entry.path()).unwrap(), None).unwrap();
            let schema = reader.schema();
            let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
            source_bytes += entry.metadata().unwrap().len();
            shards += 1;
            for batch in &batches {
                largest_decoded_batch = largest_decoded_batch.max(batch.get_array_memory_size());
            }
            for (slot, compression) in [
                None,
                Some(CompressionType::LZ4_FRAME),
                Some(CompressionType::ZSTD),
            ]
            .into_iter()
            .enumerate()
            {
                let started = Instant::now();
                let mut bytes = Vec::new();
                let options = IpcWriteOptions::default()
                    .try_with_compression(compression)
                    .unwrap();
                let mut writer =
                    FileWriter::try_new_with_options(&mut bytes, &schema, options).unwrap();
                for batch in &batches {
                    writer.write(batch).unwrap();
                }
                writer.finish().unwrap();
                drop(writer);
                encode_ns[slot] += started.elapsed().as_nanos();
                sizes[slot] += bytes.len() as u64;
                allocations[slot] += (bytes.len() as u64).div_ceil(4096) * 4096;
                let started = Instant::now();
                let decoded = FileReader::try_new(std::io::Cursor::new(bytes), None)
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert_eq!(
                    decoded, batches,
                    "IPC compression preserves every full-width value and schema"
                );
                decode_ns[slot] += started.elapsed().as_nanos();
            }
        }
    }
    assert!(shards > 0);
    json!({"shards":shards,"source_bytes":source_bytes,"normalized_bytes_none_lz4_zstd":sizes,
        "estimated_allocated_bytes_at_4096":allocations,"encode_elapsed_ns":encode_ns,"decode_elapsed_ns":decode_ns,
        "largest_decoded_batch_array_bytes":largest_decoded_batch,"production_format_changed":false})
}

fn identity_padding_experiment(source: &Path) -> Value {
    let selected = graphforge_storage::resolve_project_generation(source).unwrap();
    let inventory = selected.graph_files_inventory().unwrap().unwrap();
    let mut records = 0_u64;
    let mut edges = 0_u64;
    let mut runs = 0_u64;
    let mut current_bytes = 0_u64;
    for entry in &inventory.files {
        let name = Path::new(&entry.relative_path)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap();
        if !entry.relative_path.starts_with("topology/uuid-membership/")
            || !name.starts_with("identities-")
        {
            continue;
        }
        let bytes = std::fs::read(
            graphforge_storage::graph_object_path(source, &entry.content_sha256).unwrap(),
        )
        .unwrap();
        let mut cursor = bytes.as_slice();
        let mut prior: Option<[u8; 16]> = None;
        while !cursor.is_empty() {
            assert!(cursor.len() >= 17);
            let uuid: [u8; 16] = cursor[..16].try_into().unwrap();
            assert!(prior.is_none_or(|prior| prior < uuid));
            prior = Some(uuid);
            let kind = cursor[16];
            assert!(kind <= 3);
            let width = if kind == 1 { 17 } else { 25 };
            assert!(cursor.len() >= width);
            let surrogate = if kind == 1 {
                0
            } else {
                u64::from_be_bytes(cursor[17..25].try_into().unwrap())
            };
            // Expand only in the test to quantify the previous physical representation.
            // This is not a legacy reader or production migration path.
            let mut expanded = [0_u8; 32];
            expanded[..17].copy_from_slice(&cursor[..17]);
            expanded[24..].copy_from_slice(&surrogate.to_be_bytes());
            let packed = pack_identity_records(&expanded);
            assert_eq!(&packed[..width], &cursor[..width]);
            if kind == 1 {
                assert_eq!(&packed[17..], &[0; 8]);
                edges += 1;
            }
            records += 1;
            cursor = &cursor[width..];
        }
        current_bytes += bytes.len() as u64;
        runs += 1;
    }
    assert!(records > 0);
    assert_eq!(current_bytes, records * 25 - edges * 8);
    assert!(current_bytes <= records * 25);
    json!({"identity_runs":runs,"records":records,"live_edge_records":edges,
        "current_record_bytes":current_bytes,"previous_32_byte_baseline":records * 32,
        "reserved_padding_saved_bytes":records * 7,"defined_zero_edge_saved_bytes":edges * 8,
        "production_format_changed":true})
}

#[test]
fn permanent_sequential_single_route() {
    assess(Fixture {
        name: "sequential_single_route",
        nodes: 4097,
        edges: 65537,
        routes: 1,
        identifiers: Identifiers::Sequential,
        properties: false,
        adjacency: false,
        heterogeneous: false,
    });
}
#[test]
fn permanent_random_single_route() {
    assess(Fixture {
        name: "random_single_route",
        nodes: 8193,
        edges: 65537,
        routes: 1,
        identifiers: Identifiers::Random,
        properties: false,
        adjacency: false,
        heterogeneous: false,
    });
}
#[test]
fn permanent_random_properties_eight_routes() {
    assess(Fixture {
        name: "random_properties_eight_routes",
        nodes: 4097,
        edges: 65537,
        routes: 8,
        identifiers: Identifiers::Random,
        properties: true,
        adjacency: true,
        heterogeneous: false,
    });
}

#[test]
fn permanent_heterogeneous_property_schemas() {
    assess(Fixture {
        name: "heterogeneous_property_schemas",
        nodes: 4097,
        edges: 65537,
        routes: 4,
        identifiers: Identifiers::Random,
        properties: true,
        adjacency: false,
        heterogeneous: true,
    });
}

fn digest_hex(bytes: &[u8]) -> String {
    let mut result = String::new();
    for byte in Sha256::digest(bytes) {
        write!(result, "{byte:02x}").unwrap();
    }
    result
}

type ManifestEntry = graphforge_storage::GraphFileEntry;
fn build_manifest_candidate(
    entries: &[(String, ManifestEntry)],
    depth: usize,
    objects: &mut BTreeMap<String, Vec<u8>>,
) -> String {
    let mut end = depth;
    while end < 64
        && entries
            .iter()
            .all(|entry| entry.0.as_bytes()[end] == entries[0].0.as_bytes()[end])
    {
        end += 1;
    }
    let prefix = &entries[0].0[depth..end];
    assert!(
        entries.len() <= 8 || end < 64,
        "oversized hash collision bucket is unsupported"
    );
    let node = if entries.len() <= 8 {
        json!({"format":"graphforge-graph-manifest-radix-node","version":3,"depth":depth,"prefix":prefix,
            "kind":"bucket","entries":entries.iter().map(|entry| &entry.1).collect::<Vec<_>>()})
    } else {
        let mut groups = BTreeMap::<u8, Vec<(String, ManifestEntry)>>::new();
        for entry in entries {
            groups
                .entry(entry.0.as_bytes()[end])
                .or_default()
                .push(entry.clone());
        }
        let children = groups
            .into_iter()
            .map(|(nibble, group)| {
                (
                    char::from(nibble).to_string(),
                    build_manifest_candidate(&group, end + 1, objects),
                )
            })
            .collect::<BTreeMap<_, _>>();
        json!({"format":"graphforge-graph-manifest-radix-node","version":3,"depth":depth,"prefix":prefix,"kind":"branch","children":children})
    };
    let bytes = serde_json::to_vec(&node).unwrap();
    let digest = digest_hex(&bytes);
    objects.insert(digest.clone(), bytes);
    digest
}
fn lookup_manifest_candidate(
    root: &str,
    path: &str,
    objects: &BTreeMap<String, Vec<u8>>,
) -> Option<ManifestEntry> {
    let key = digest_hex(path.as_bytes());
    let mut current = root.to_owned();
    loop {
        let bytes = &objects[&current];
        assert_eq!(digest_hex(bytes), current);
        let node: Value = serde_json::from_slice(bytes).unwrap();
        let depth = usize::try_from(node["depth"].as_u64().unwrap()).unwrap();
        let prefix = node["prefix"].as_str().unwrap();
        if !key[depth..].starts_with(prefix) {
            return None;
        }
        if node["kind"] == "bucket" {
            let entries: Vec<ManifestEntry> =
                serde_json::from_value(node["entries"].clone()).unwrap();
            assert!(entries.len() <= 8);
            return entries
                .into_iter()
                .find(|entry| entry.relative_path == path);
        }
        node["children"][&key[depth + prefix.len()..=depth + prefix.len()]]
            .as_str()?
            .clone_into(&mut current);
    }
}
fn bucket_manifest_experiment(source: &Path) -> Value {
    let selected = graphforge_storage::resolve_project_generation(source).unwrap();
    let participant = selected
        .participant_snapshot(
            graphforge_storage::GRAPH_CAPABILITY_ID,
            graphforge_storage::GRAPH_FILES_FAMILY,
        )
        .unwrap()
        .unwrap();
    let root: Value = serde_json::from_slice(&participant.bytes).unwrap();
    let entries = selected.graph_files_inventory().unwrap().unwrap().files;
    let mut original = BTreeMap::new();
    let mut pending = vec![root["root_node_sha256"].as_str().unwrap().to_owned()];
    while let Some(digest) = pending.pop() {
        if original.contains_key(&digest) {
            continue;
        }
        assert!(original.len() <= 2 * entries.len() + 1);
        let bytes =
            std::fs::read(graphforge_storage::graph_object_path(source, &digest).unwrap()).unwrap();
        assert_eq!(digest_hex(&bytes), digest);
        let node: Value = serde_json::from_slice(&bytes).unwrap();
        match node["kind"].as_str().unwrap() {
            "branch" => pending.extend(
                node["children"]
                    .as_object()
                    .unwrap()
                    .values()
                    .map(|child| child.as_str().unwrap().to_owned()),
            ),
            "bucket" => {}
            other => panic!("unsupported assessment manifest node {other}"),
        }
        original.insert(digest, bytes);
    }
    let mut keyed = entries
        .iter()
        .map(|entry| (digest_hex(entry.relative_path.as_bytes()), entry.clone()))
        .collect::<Vec<_>>();
    keyed.sort_by(|a, b| a.0.cmp(&b.0));
    let mut candidate = BTreeMap::new();
    let root = build_manifest_candidate(&keyed, 0, &mut candidate);
    for entry in &entries {
        assert_eq!(
            lookup_manifest_candidate(&root, &entry.relative_path, &candidate),
            Some(entry.clone())
        );
    }
    assert!(lookup_manifest_candidate(&root, "absent-assessment-payload", &candidate).is_none());
    let original_allocated: u64 = original
        .keys()
        .map(|digest| {
            let file =
                File::open(graphforge_storage::graph_object_path(source, digest).unwrap()).unwrap();
            graphforge_filesystem::file_space_usage(&file)
                .unwrap()
                .allocated_bytes
        })
        .sum();
    let candidate_logical: usize = candidate.values().map(Vec::len).sum();
    let candidate_quantized: usize = candidate
        .values()
        .map(|bytes| bytes.len().div_ceil(4096) * 4096)
        .sum();
    json!({"source_manifest_objects":original.len(),"source_manifest_allocated_bytes":original_allocated,
        "candidate_bucket_capacity":8,"candidate_objects":candidate.len(),"candidate_logical_bytes":candidate_logical,
        "candidate_estimated_allocated_bytes_at_4096":candidate_quantized,"authenticated_lookups_checked":entries.len(),
        "production_format_changed":true,"source_manifest_logical_bytes":original.values().map(Vec::len).sum::<usize>()})
}

fn validate_storage_budgets(f: Fixture, storage: &graphforge_storage::StorageAttributionSnapshot) {
    use graphforge_storage::ArtifactCategory as Category;
    let nodes = f.nodes as u64;
    let edges = f.edges as u64;
    let routes = f.routes as u64;
    let node_shards = nodes.div_ceil(1024) * routes;
    let edge_shards = edges.div_ceil(1024) * routes;
    // Input batches bound fragment counts. Allow one footer/header block per
    // Parquet fragment, and one allocation block of EOF rounding per object.
    // These are payload/representation ceilings, not measured-value multipliers.
    for (category, object_limit, logical_limit) in [
        (
            Category::TopologyNodes,
            node_shards,
            48 * nodes + 2048 * node_shards,
        ),
        (
            Category::TopologyEdges,
            edge_shards,
            96 * edges + 2048 * edge_shards,
        ),
        (
            Category::Properties,
            if f.properties {
                node_shards + edge_shards
            } else {
                0
            },
            if f.properties {
                64 * (nodes + edges) + 2048 * (node_shards + edge_shards)
            } else {
                0
            },
        ),
        // 32-byte UUID/kind/ordinal rows plus up to 64 bytes/node of independent
        // forward/reverse ordinal/surrogate authority and bounded manifests.
        (
            Category::UuidAndSurrogates,
            32,
            32 * (nodes + edges) + 64 * nodes + 131_072,
        ),
        // Enabled sparse inbound/outbound CSR stores endpoints and edge identity
        // plus offsets/routes; absent adjacency must remain exactly absent.
        (
            Category::Adjacency,
            if f.adjacency { 8 * routes + 16 } else { 0 },
            if f.adjacency {
                80 * edges + 16 * nodes + 8192 * (routes + 1)
            } else {
                0
            },
        ),
    ] {
        let totals = &storage.categories[&category];
        assert!(
            totals.physical_objects <= object_limit,
            "{category:?} object budget: {totals:?}"
        );
        assert!(
            totals.physical_logical_bytes <= logical_limit,
            "{category:?} logical budget {logical_limit}: {totals:?}"
        );
        assert!(
            totals.allocated_bytes
                <= totals.physical_logical_bytes + 4096 * totals.physical_objects,
            "{category:?} allocation exceeds one 4-KiB EOF rounding block per object"
        );
    }
    let controls = &storage.categories[&Category::CatalogAndManifests];
    let payload_objects = storage.physical_objects - controls.physical_objects;
    assert!(controls.physical_objects <= 2 * payload_objects + 32);
    assert!(controls.physical_logical_bytes <= 2048 * payload_objects + 65536);
    assert!(
        controls.allocated_bytes
            <= controls.physical_logical_bytes + 4096 * controls.physical_objects
    );
}

fn whole_project_file_census(root: &Path) -> Value {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let mut pending = vec![root.to_path_buf()];
        let mut seen = BTreeSet::new();
        let (mut files, mut logical, mut allocated, mut directory_allocated) =
            (0_u64, 0_u64, 0_u64, 0_u64);
        while let Some(path) = pending.pop() {
            let metadata = std::fs::symlink_metadata(&path).unwrap();
            assert!(!metadata.file_type().is_symlink());
            if !seen.insert((metadata.dev(), metadata.ino())) {
                continue;
            }
            if metadata.is_dir() {
                directory_allocated += metadata.blocks() * 512;
                pending.extend(
                    std::fs::read_dir(path)
                        .unwrap()
                        .map(|entry| entry.unwrap().path()),
                );
            } else {
                assert!(metadata.is_file());
                files += 1;
                logical += metadata.len();
                allocated += metadata.blocks() * 512;
            }
        }
        json!({"unique_files":files,"file_logical_bytes":logical,"file_allocated_bytes":allocated,
            "directory_allocated_bytes":directory_allocated,"deduplication":"device/inode",
            "scope":"recursive project including retained generations, caches and private state; point-in-time, not peak"})
    }
    #[cfg(not(unix))]
    {
        let _ = root;
        json!({"measured":false,"reason":"native inode census requires Unix"})
    }
}

fn assess(f: Fixture) {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let (nodes, edges) = rows(f);
    let started = Instant::now();
    construct(&source, f, &nodes, &edges);
    let construction_ns = started.elapsed().as_nanos();
    let graph = GraphForge::new(source.to_str()).unwrap();
    let unindexed = graph.storage_attribution().unwrap();
    assert_eq!(
        unindexed.categories[&graphforge_storage::ArtifactCategory::Adjacency].allocated_bytes,
        0
    );
    validate_storage_budgets(
        Fixture {
            adjacency: false,
            ..f
        },
        &unindexed,
    );
    // Candidate codecs inspect construction's published payload generation.
    // Adjacency publication may switch to a generation-owned graph tree.
    let experiment = parquet_experiment(&source);
    if matches!(f.identifiers, Identifiers::Random) {
        let actual = experiment["production_parquet_bytes"].as_u64().unwrap();
        let uncompressed = experiment["normalized_bytes_uncompressed_zstd1_zstd3"][0]
            .as_u64()
            .unwrap();
        assert!(
            actual * 5 <= uncompressed * 4,
            "random-identity fixtures must retain the measured at-least-20% Parquet reduction"
        );
    }
    let identity_experiment = identity_padding_experiment(&source);
    let manifest_experiment = bucket_manifest_experiment(&source);
    if f.heterogeneous {
        assert_eq!(manifest_experiment["authenticated_lookups_checked"], 352);
        assert!(
            manifest_experiment["source_manifest_allocated_bytes"]
                .as_u64()
                .unwrap()
                <= 1_536_000,
            "production manifest must halve the 3,072,000-byte #1196 native allocation baseline: {manifest_experiment}"
        );
    }
    if f.adjacency {
        graph.rebuild_adjacency(None).unwrap();
    }
    let adjacency_experiment = f.adjacency.then(|| adjacency_codec_experiment(&source));
    let storage = graph.storage_attribution().unwrap();
    storage.validate_for_qualification().unwrap();
    validate_storage_budgets(f, &storage);
    assert_eq!(
        storage.categories[&graphforge_storage::ArtifactCategory::Adjacency].allocated_bytes > 0,
        f.adjacency,
        "unbuilt and built adjacency are distinct measured capabilities"
    );
    drop(graph);
    let whole_project = whole_project_file_census(&source);
    let fingerprint = round_trip(root.path(), &source, f, &nodes, &edges);
    println!(
        "PERMANENT_STORAGE_ASSESSMENT {}",
        json!({"fixture":f.name,"nodes":f.nodes,"edges":f.edges,"routes":f.routes,"random_ids":matches!(f.identifiers, Identifiers::Random),"properties":f.properties,"heterogeneous_schemas":f.heterogeneous,"adjacency_built":f.adjacency,"unindexed_allocated_bytes":unindexed.allocated_bytes,"unindexed_categories":unindexed.categories,
            "construction_elapsed_ns":construction_ns,"semantic_fingerprint":fingerprint,"whole_project":whole_project,
            "permanent": {"logical_bytes":storage.logical_bytes,"physical_logical_bytes":storage.physical_logical_bytes,"allocated_bytes":storage.allocated_bytes,"physical_objects":storage.physical_objects,"categories":storage.categories},
            "adjacency_codec_experiment":adjacency_experiment,"parquet_experiment":experiment,"identity_padding_experiment":identity_experiment,"manifest_bucket_experiment":manifest_experiment})
    );
}

#[test]
fn public_mutation_replaces_shared_constructed_payloads() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let fixture = Fixture {
        name: "shared_payload_mutation",
        nodes: 33,
        edges: 129,
        routes: 2,
        identifiers: Identifiers::Random,
        properties: true,
        adjacency: false,
        heterogeneous: true,
    };
    let (mut nodes, edges) = rows(fixture);
    construct(&source, fixture, &nodes, &edges);
    let graph = GraphForge::new(source.to_str()).unwrap();
    verify_graph(&graph, fixture, &nodes, &edges);
    let query =
        "MATCH (n) WHERE n.score IS NOT NULL RETURN n.node_uuid, n.score ORDER BY n.node_uuid";
    let expected = graph.execute(query).unwrap();
    let old_stream = graph.execute_stream(query).unwrap();
    graph
        .execute("MATCH (n) WHERE n.score IS NOT NULL SET n.score = n.score + 1")
        .unwrap();
    for node in &mut nodes {
        node.2 = node.2.map(|score| score + 1);
    }
    verify_graph(&graph, fixture, &nodes, &edges);
    use futures::TryStreamExt as _;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let old_batches: Vec<RecordBatch> = runtime.block_on(old_stream.try_collect()).unwrap();
    assert_eq!(
        old_batches
            .iter()
            .map(|batch| batch.columns())
            .collect::<Vec<_>>(),
        expected
            .batches
            .iter()
            .map(|batch| batch.columns())
            .collect::<Vec<_>>()
    );
    drop(graph);
    round_trip(root.path(), &source, fixture, &nodes, &edges);
    // Exercise the imported current-format manifest through a new publication,
    // then reopen and authenticate the subsequently mutated graph.
    let imported_path = root.path().join("imported");
    let imported = GraphForge::new(imported_path.to_str()).unwrap();
    imported
        .execute("MATCH (n) WHERE n.score IS NOT NULL SET n.score = n.score + 1")
        .unwrap();
    for node in &mut nodes {
        node.2 = node.2.map(|score| score + 1);
    }
    verify_graph(&imported, fixture, &nodes, &edges);
    drop(imported);
    verify_graph(
        &GraphForge::new(imported_path.to_str()).unwrap(),
        fixture,
        &nodes,
        &edges,
    );
}

#[test]
fn cas_construction_composite_mutation_and_compaction_preserve_values() {
    use graphforge_storage::{
        GraphDeltaCompactionLimits, GraphDeltaCompactionRequest, ProjectRetentionLimits,
        ProjectRetentionPolicy,
    };
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let fixture = Fixture {
        name: "publishing_policy",
        nodes: 66,
        edges: 0,
        routes: 1,
        identifiers: Identifiers::Random,
        properties: true,
        adjacency: false,
        heterogeneous: true,
    };
    let (mut nodes, edges) = rows(fixture);
    construct(&source, fixture, &nodes, &edges);
    let initial = graphforge_storage::resolve_project_generation(&source).unwrap();
    assert!(initial.declared_graph_files_inventory().unwrap().is_none());
    let base = initial.graph_files_inventory().unwrap().unwrap();

    let graph = GraphForge::new(source.to_str()).unwrap();
    verify_graph(&graph, fixture, &nodes, &edges);
    drop(graph);
    use graphforge_api::{
        COMPOSITE_TRANSACTION_CONTRACT_VERSION, CompositeGraphMutation,
        CompositeKnowledgeParticipants, CompositeTransactionRequest, PropValue, WriteContext,
    };
    let graph = GraphForge::new(source.to_str()).unwrap();
    let snapshot_query =
        "MATCH (n) WHERE n.score IS NOT NULL RETURN n.node_uuid, n.score ORDER BY n.node_uuid";
    let expected_snapshot = graph.execute(snapshot_query).unwrap();
    let old_stream = graph.execute_stream(snapshot_query).unwrap();
    graph
        .publish_composite_transaction(CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            graph_mutations: vec![CompositeGraphMutation::SetNodeProperty {
                node_uuid: nodes[0].0,
                property: "score".into(),
                value: PropValue::Int(123),
            }],
            knowledge: CompositeKnowledgeParticipants::default(),
        })
        .unwrap();
    use futures::TryStreamExt as _;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let old_batches: Vec<RecordBatch> = runtime.block_on(old_stream.try_collect()).unwrap();
    assert_eq!(
        old_batches
            .iter()
            .map(|batch| batch.columns())
            .collect::<Vec<_>>(),
        expected_snapshot
            .batches
            .iter()
            .map(|batch| batch.columns())
            .collect::<Vec<_>>()
    );
    drop(graph);
    nodes[0].2 = Some(123);
    let delta = graphforge_storage::resolve_project_generation(&source).unwrap();
    assert!(delta.declared_graph_files_inventory().unwrap().is_none());
    let delta_inventory = delta.graph_files_inventory().unwrap().unwrap();
    for entry in base
        .files
        .iter()
        .filter(|entry| entry.relative_path != "topology/runtime_catalog.parquet")
    {
        assert!(
            delta_inventory.files.contains(entry),
            "delta must preserve base entry {}",
            entry.relative_path
        );
    }
    assert_eq!(delta_inventory.file_count, base.file_count + 1);
    let participants = delta
        .participant_snapshots()
        .unwrap()
        .into_iter()
        .filter(|entry| !(entry.capability_id == "graph" && entry.record_family_id == "files"))
        .collect::<Vec<_>>();

    let mut graph = GraphForge::new(source.to_str()).unwrap();
    verify_graph(&graph, fixture, &nodes, &edges);
    let request = GraphDeltaCompactionRequest {
        transaction_uuid: Uuid::from_u128(121305),
        generation_uuid: Uuid::from_u128(121306),
        through_run_sequence: None,
        limits: GraphDeltaCompactionLimits::default(),
        cleanup_after_commit: false,
        cleanup_policy: ProjectRetentionPolicy::default(),
        cleanup_limits: ProjectRetentionLimits::default(),
    };
    let cancelled = graphforge_api::CancellationToken::new();
    cancelled.cancel();
    assert!(
        graph
            .compact_graph_delta(&request, Some(&cancelled))
            .is_err()
    );
    assert_eq!(
        graphforge_storage::resolve_project_generation(&source)
            .unwrap()
            .generation_uuid(),
        delta.generation_uuid()
    );
    let preview = graph
        .preview_graph_delta_compaction(&request, None)
        .unwrap();
    assert!(preview.dry_run);
    let compacted_report = graph.compact_graph_delta(&request, None).unwrap();
    assert_eq!(
        preview.state_fingerprint,
        compacted_report.state_fingerprint
    );
    let repeated = graph.compact_graph_delta(&request, None).unwrap();
    let replayed_receipt = repeated.publication.unwrap();
    let committed_receipt = compacted_report.publication.unwrap();
    assert!(replayed_receipt.idempotent_replay);
    assert!(!committed_receipt.idempotent_replay);
    assert_eq!(
        replayed_receipt.transaction_uuid,
        committed_receipt.transaction_uuid
    );
    assert_eq!(
        replayed_receipt.generation_uuid,
        committed_receipt.generation_uuid
    );
    assert_eq!(
        replayed_receipt.generation_manifest_sha256,
        committed_receipt.generation_manifest_sha256
    );
    drop(graph);
    let compacted = graphforge_storage::resolve_project_generation(&source).unwrap();
    assert!(
        compacted
            .declared_graph_files_inventory()
            .unwrap()
            .is_none()
    );
    assert_eq!(compacted.capabilities(), delta.capabilities());
    assert_eq!(
        compacted
            .participant_snapshots()
            .unwrap()
            .into_iter()
            .filter(|entry| !(entry.capability_id == "graph" && entry.record_family_id == "files"))
            .collect::<Vec<_>>(),
        participants
    );
    assert!(
        graphforge_storage::list_delta_runs(
            &compacted.graph_files_inventory().unwrap().unwrap(),
            graphforge_storage::GraphDeltaJournalLimits::default(),
        )
        .unwrap()
        .is_empty()
    );
    let portable = root.path().join("compaction-portable");
    std::fs::create_dir(&portable).unwrap();
    round_trip(&portable, &source, fixture, &nodes, &edges);
}

#[test]
fn cas_delta_preparation_reuses_payloads_with_bounded_private_allocation() {
    use graphforge_storage::{
        GraphDeltaJournalLimits, GraphDeltaOp, GraphDeltaOpKind, GraphDeltaPayload,
        GraphDeltaPublishRequest,
    };
    for node_count in [33, 4097] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let fixture = Fixture {
            name: "cas_delta_allocation",
            nodes: node_count,
            edges: 0,
            routes: 1,
            identifiers: Identifiers::Random,
            properties: true,
            adjacency: false,
            heterogeneous: false,
        };
        let (nodes, edges) = rows(fixture);
        construct(&source, fixture, &nodes, &edges);
        let parent = graphforge_storage::resolve_project_generation(&source).unwrap();
        let base = parent.graph_files_inventory().unwrap().unwrap();
        let request = GraphDeltaPublishRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            run_uuid: Uuid::now_v7(),
            operations: vec![GraphDeltaOp {
                operation_uuid: Uuid::now_v7(),
                kind: GraphDeltaOpKind::SetNodeProperty,
                payload: GraphDeltaPayload::SetNodeProperty {
                    node_uuid: nodes[0].0.to_string(),
                    property_stem: "_untyped".into(),
                    key: "score".into(),
                    value: graphforge_storage::encode_graph_delta_value(
                        &graphforge_ir::IrLiteral::Int(123),
                    )
                    .unwrap(),
                },
            }],
            limits: GraphDeltaJournalLimits {
                max_batch_rows: 7,
                max_replay_memory_bytes: 2 * 1024 * 1024,
                ..GraphDeltaJournalLimits::default()
            },
        };
        let prepared = graphforge_storage::prepare_graph_delta(&parent, &request).unwrap();
        assert!(prepared.graph_tree_source().is_none());
        assert!(prepared.preserved_base_parquet_digests);
        assert_eq!(prepared.unchanged_base_files, base.file_count);
        let mut shared_bytes = 0_u64;
        let mut private_allocated = 0_u64;
        for entry in &base.files {
            let object = File::open(
                graphforge_storage::graph_object_path(&source, &entry.content_sha256).unwrap(),
            )
            .unwrap();
            let private =
                File::open(prepared.graph_tree_root().join(&entry.relative_path)).unwrap();
            assert_eq!(private.metadata().unwrap().len(), entry.byte_length);
            if graphforge_filesystem::file_identity(&object).unwrap()
                == graphforge_filesystem::file_identity(&private).unwrap()
            {
                shared_bytes += entry.byte_length;
            } else {
                private_allocated += graphforge_filesystem::file_space_usage(&private)
                    .unwrap()
                    .allocated_bytes;
            }
            if entry.relative_path.starts_with("topology/nodes/")
                || entry.relative_path.starts_with("properties/")
            {
                assert_eq!(
                    graphforge_filesystem::file_identity(&object).unwrap(),
                    graphforge_filesystem::file_identity(&private).unwrap(),
                    "{} must reuse its CAS payload",
                    entry.relative_path
                );
            }
        }
        let run = File::open(
            prepared
                .graph_tree_root()
                .join(graphforge_storage::delta_run_relative_path(1)),
        )
        .unwrap();
        private_allocated += graphforge_filesystem::file_space_usage(&run)
            .unwrap()
            .allocated_bytes;
        assert!(shared_bytes > 0);
        assert!(
            private_allocated <= 256 * 1024,
            "private control/run allocation must stay bounded: {private_allocated}"
        );
        assert!(run.metadata().unwrap().len() <= 4096);
        assert_eq!(
            graphforge_storage::resolve_project_generation(&source)
                .unwrap()
                .generation_uuid(),
            parent.generation_uuid()
        );
        assert_eq!(parent.graph_files_inventory().unwrap().unwrap(), base);
        drop(run);
        drop(prepared);
        let receipt = graphforge_storage::publish_graph_delta(&source, &request).unwrap();
        assert!(receipt.preserved_base_parquet_digests);
        assert_eq!(receipt.unchanged_base_files, base.file_count);
        let repeated = graphforge_storage::publish_graph_delta(&source, &request).unwrap();
        assert!(repeated.publication.idempotent_replay);
        assert_eq!(repeated.state_fingerprint, receipt.state_fingerprint);
        let mut conflict = request.clone();
        conflict.generation_uuid = Uuid::now_v7();
        assert!(graphforge_storage::publish_graph_delta(&source, &conflict).is_err());
        let selected = graphforge_storage::resolve_project_generation(&source).unwrap();
        assert_eq!(selected.generation_uuid(), request.generation_uuid);
        assert!(selected.declared_graph_files_inventory().unwrap().is_none());
        let graph = GraphForge::new(source.to_str()).unwrap();
        let mut expected_nodes = nodes.clone();
        expected_nodes[0].2 = Some(123);
        verify_graph(&graph, fixture, &expected_nodes, &edges);
        println!(
            "CAS_DELTA_ALLOCATION {}",
            json!({"nodes":node_count,"base_bytes":base.total_byte_length,"shared_bytes":shared_bytes,"private_control_and_run_allocated_bytes":private_allocated,"replay_batch_rows":7,"replay_logical_memory_limit_bytes":2*1024*1024})
        );
    }
}

#[test]
fn workspace_publication_preserves_constructed_cas_payloads() {
    use graphforge_api::{AdoptOntologyRequest, ClearOntologyRequest, OntologyMode, WriteContext};
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let fixture = Fixture {
        name: "workspace_cas",
        nodes: 33,
        edges: 258,
        routes: 1,
        identifiers: Identifiers::Random,
        properties: false,
        adjacency: false,
        heterogeneous: false,
    };
    let (mut nodes, mut edges) = rows(fixture);
    construct(&source, fixture, &nodes, &edges[..129]);
    let selected = graphforge_storage::resolve_project_generation(&source).unwrap();
    assert!(selected.declared_graph_files_inventory().unwrap().is_none());
    let before = selected.graph_files_inventory().unwrap().unwrap();
    let identities = before
        .files
        .iter()
        .map(|entry| {
            let file = File::open(
                graphforge_storage::graph_object_path(&source, &entry.content_sha256).unwrap(),
            )
            .unwrap();
            (
                entry.relative_path.clone(),
                graphforge_filesystem::file_identity(&file).unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let ontology = root.path().join("routes.yaml");
    std::fs::write(&ontology, "ontology_id: https://example.test/mixed\nversion: \"1\"\nentity_types:\n  - name: NewNode\n    abstract: false\nrelation_types:\n  - name: NEW_TYPED\n    src: NewNode\n    dst: NewNode\n").unwrap();
    let mut graph = GraphForge::new(source.to_str()).unwrap();
    let context = WriteContext {
        operation_uuid: OperationId(Uuid::now_v7()),
        actor_uuid: None,
    };
    let request = AdoptOntologyRequest {
        context,
        path: ontology,
        mode: OntologyMode::Advisory,
    };
    graph.adopt_ontology(request.clone()).unwrap();
    let adopted = graphforge_storage::resolve_project_generation(&source).unwrap();
    assert!(adopted.declared_graph_files_inventory().unwrap().is_none());
    assert_eq!(before, adopted.graph_files_inventory().unwrap().unwrap());
    for entry in &before.files {
        let file = File::open(
            graphforge_storage::graph_object_path(&source, &entry.content_sha256).unwrap(),
        )
        .unwrap();
        assert_eq!(
            identities[&entry.relative_path],
            graphforge_filesystem::file_identity(&file).unwrap()
        );
    }
    assert_eq!(selected.capabilities(), adopted.capabilities());
    for participant in selected
        .participant_snapshots()
        .unwrap()
        .into_iter()
        .filter(|participant| participant.capability_id != "workspace")
    {
        assert_eq!(
            adopted
                .participant_snapshot(&participant.capability_id, &participant.record_family_id)
                .unwrap()
                .unwrap()
                .bytes,
            participant.bytes
        );
    }
    graph.adopt_ontology(request.clone()).unwrap();
    assert_eq!(
        adopted.generation_uuid(),
        graphforge_storage::resolve_project_generation(&source)
            .unwrap()
            .generation_uuid()
    );
    verify_graph(&graph, fixture, &nodes, &edges[..129]);
    graph
        .clear_ontology(ClearOntologyRequest {
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
        })
        .unwrap();
    drop(graph);
    let mut graph = GraphForge::new(source.to_str()).unwrap();
    verify_graph(&graph, fixture, &nodes, &edges[..129]);
    let mut readopt = request;
    readopt.context.operation_uuid = OperationId(Uuid::now_v7());
    graph.adopt_ontology(readopt).unwrap();
    drop(graph);
    let graph = GraphForge::new(source.to_str()).unwrap();
    assert_eq!(graph.ontology_mode(), OntologyMode::Advisory);
    verify_graph(&graph, fixture, &nodes, &edges[..129]);
    drop(graph);
    let mut graph = GraphForge::new(source.to_str()).unwrap();
    let candidate = graph.workspace_ontology_composition().unwrap().unwrap();
    let request = graphforge_api::CompositionChangeRequest {
        context: WriteContext {
            operation_uuid: OperationId(Uuid::now_v7()),
            actor_uuid: None,
        },
        expected_project_generation_uuid: graphforge_storage::resolve_project_generation(&source)
            .unwrap()
            .generation_uuid(),
        expected_composition_fingerprint: Some(candidate.composition_fingerprint.clone()),
        candidate,
        data_disposition: graphforge_api::CompositionDataDisposition::RequireConforming,
    };
    let preview = graph
        .preview_ontology_composition_change(&request, None)
        .unwrap();
    assert!(preview.diagnostics.is_empty(), "{:?}", preview.diagnostics);
    graph
        .publish_ontology_composition_change(&request, &preview, None)
        .unwrap();
    drop(graph);
    let child_nodes = (0..33)
        .map(|row| (id(3, row, true), "NewNode".to_owned(), None))
        .collect::<Vec<_>>();
    for (row, edge) in edges[129..].iter_mut().enumerate() {
        edge.3 = "NEW_TYPED".into();
        edge.1 = child_nodes[row % child_nodes.len()].0;
        edge.2 = child_nodes[(row + 1) % child_nodes.len()].0;
    }
    construct(&source, fixture, &child_nodes, &edges[129..]);
    nodes.extend(child_nodes.into_iter().map(|mut node| {
        node.1 = "mixed:entity:NewNode".into();
        node
    }));
    let current = graphforge_storage::resolve_project_generation(&source).unwrap();
    let bindings = graphforge_storage::semantic_storage_bindings(&current)
        .unwrap()
        .unwrap();
    let relation = bindings
        .bindings
        .iter()
        .find(|binding| {
            binding.route_kind == graphforge_storage::SemanticRouteKind::Relation
                && binding.symbol.local_id == "NEW_TYPED"
        })
        .unwrap();
    assert_eq!(relation.symbol.display(), "mixed:relation:NEW_TYPED");
    // The unqualified relationship projection exposes the stored route. Bind
    // it independently to the declared qualified symbol, not a query result.
    for edge in &mut edges[129..] {
        edge.3.clone_from(&relation.route);
    }
    let mut graph = GraphForge::new(source.to_str()).unwrap();
    verify_graph(&graph, fixture, &nodes, &edges);
    graph
        .set_graph_directedness(
            &WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            Some(graphforge_storage::GraphDirectedness::Directed),
        )
        .unwrap();
    drop(graph);
    round_trip(root.path(), &source, fixture, &nodes, &edges);
    println!(
        "WORKSPACE_CAS_REUSE {}",
        json!({"retained_files":before.files.len(),"retained_bytes":before.total_byte_length,"reencoded_graph_payload_bytes":0})
    );
}

#[test]
fn composite_qualified_property_publishing_preserves_values() {
    exercise_qualified_property_publication(true, 33, false);
}

#[test]
fn composite_qualified_undeclared_property_is_rejected_before_publication() {
    exercise_qualified_property_publication(false, 33, false);
}

#[test]
fn composite_qualified_property_large_base_reuses_topology() {
    exercise_qualified_property_publication(true, 4097, false);
}

#[test]
fn composite_qualified_mixed_graph_canonical_properties_preserve_values() {
    exercise_qualified_property_publication(true, 33, true);
}

fn exercise_qualified_property_publication(
    declared_property: bool,
    node_count: usize,
    mixed: bool,
) {
    use graphforge_api::{
        COMPOSITE_TRANSACTION_CONTRACT_VERSION, CompositeGraphMutation,
        CompositeKnowledgeParticipants, CompositeTransactionRequest, PropValue, WriteContext,
    };
    use graphforge_storage::{
        GraphDeltaCompactionLimits, GraphDeltaCompactionRequest, ProjectRetentionLimits,
        ProjectRetentionPolicy,
    };
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let fixture = Fixture {
        name: "mixed_semantic_routes",
        nodes: if mixed { 33 } else { 0 },
        edges: if mixed { 129 } else { 0 },
        routes: 1,
        identifiers: Identifiers::Random,
        properties: false,
        adjacency: false,
        heterogeneous: false,
    };
    let (mut nodes, mut edges) = rows(fixture);
    drop(GraphForge::new(source.to_str()).unwrap());
    if mixed {
        construct(&source, fixture, &nodes, &edges);
    }
    configure_composite_ontology(root.path(), &source, declared_property);
    let child_nodes = (0..node_count)
        .map(|row| (id(3, row, true), "NewNode".to_owned(), None))
        .collect::<Vec<_>>();
    let changed_node = child_nodes[0].0;
    let parent_edges = edges.len();
    let mut child_edges = edges.clone();
    for (row, edge) in child_edges.iter_mut().enumerate() {
        edge.0 = id(4, row, true);
        edge.3 = "NEW_TYPED".into();
        edge.1 = child_nodes[row % child_nodes.len()].0;
        edge.2 = child_nodes[(row + 1) % child_nodes.len()].0;
    }
    construct(&source, fixture, &child_nodes, &child_edges);
    edges.extend(child_edges);
    nodes.extend(child_nodes.into_iter().map(|mut node| {
        node.1 = "mixed:entity:NewNode".into();
        node
    }));
    let current = graphforge_storage::resolve_project_generation(&source).unwrap();
    let bindings = graphforge_storage::semantic_storage_bindings(&current)
        .unwrap()
        .unwrap();
    let relation = bindings
        .bindings
        .iter()
        .find(|binding| {
            binding.route_kind == graphforge_storage::SemanticRouteKind::Relation
                && binding.symbol.local_id == "NEW_TYPED"
        })
        .unwrap();
    assert_eq!(relation.symbol.display(), "mixed:relation:NEW_TYPED");
    for edge in &mut edges[parent_edges..] {
        edge.3.clone_from(&relation.route);
    }
    let options = graphforge_api::GraphForgeOptions {
        write_mode: if mixed {
            graphforge_api::ProjectWriteMode::OptimisticMultiWriter
        } else {
            graphforge_api::ProjectWriteMode::SingleWriter
        },
        ..Default::default()
    };
    let mut graph = GraphForge::new_with_options(source.to_str(), options).unwrap();
    verify_graph(&graph, fixture, &nodes, &edges);
    let base = current.graph_files_inventory().unwrap().unwrap();
    assert!(current.declared_graph_files_inventory().unwrap().is_none());
    let publication = graph.publish_composite_transaction(CompositeTransactionRequest {
        contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
        context: WriteContext {
            operation_uuid: OperationId(Uuid::now_v7()),
            actor_uuid: None,
        },
        graph_mutations: vec![CompositeGraphMutation::SetNodeProperty {
            node_uuid: changed_node,
            property: "score".into(),
            value: PropValue::Int(123),
        }],
        knowledge: CompositeKnowledgeParticipants::default(),
    });
    if !declared_property {
        let error = publication.unwrap_err();
        assert!(error.to_string().contains("WrongOwnerProperty"), "{error}");
        let unchanged = graphforge_storage::resolve_project_generation(&source).unwrap();
        assert_eq!(current.generation_uuid(), unchanged.generation_uuid());
        assert_eq!(base, unchanged.graph_files_inventory().unwrap().unwrap());
        verify_graph(&graph, fixture, &nodes, &edges);
        return;
    }
    publication.unwrap();
    let first = graphforge_storage::resolve_project_generation(&source).unwrap();
    assert!(
        first.declared_graph_files_inventory().unwrap().is_none(),
        "first property authority must retain CAS ownership"
    );
    let first_inventory = first.graph_files_inventory().unwrap().unwrap();
    for entry in base.files.iter().filter(|entry| {
        entry.relative_path.starts_with("topology/")
            && entry.relative_path.ends_with(".parquet")
            && entry.relative_path != "topology/runtime_catalog.parquet"
    }) {
        assert!(
            first_inventory.files.contains(entry),
            "property publication must reuse unchanged topology: {}",
            entry.relative_path
        );
    }
    assert!(
        graphforge_storage::list_delta_runs(&first_inventory, Default::default())
            .unwrap()
            .is_empty()
    );
    let changed_payload_bytes: u64 = first_inventory
        .files
        .iter()
        .filter(|entry| entry.relative_path.ends_with(".parquet") && !base.files.contains(entry))
        .map(|entry| entry.byte_length)
        .sum();
    assert!(
        changed_payload_bytes <= 64 * 1024,
        "one property must not copy or reencode the base: {changed_payload_bytes}"
    );
    println!(
        "COMPOSITE_OWNER_BUDGET {}",
        json!({
            "nodes": node_count, "base_bytes": base.total_byte_length,
            "first_property_changed_parquet_bytes": changed_payload_bytes,
            "unchanged_topology_reencoded_bytes": 0
        })
    );
    let immediate = graph
        .execute("MATCH (n:`mixed:NewNode`) WHERE n.score IS NOT NULL RETURN n.node_uuid, n.score")
        .unwrap();
    assert_eq!(
        immediate
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1,
        "qualified property must be visible immediately"
    );
    assert_eq!(uuid_at(&immediate.batches[0], 0, 0), changed_node);
    assert_eq!(int_at(&immediate.batches[0], 1, 0), Some(123));
    let snapshot_query =
        "MATCH (n:`mixed:NewNode`) WHERE n.score IS NOT NULL RETURN n.node_uuid, n.score";
    let (snapshot, _, _snapshot_guard) = graph
        .execute_stream_owned(snapshot_query, &Default::default())
        .unwrap();
    graph
        .publish_composite_transaction(CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            graph_mutations: vec![CompositeGraphMutation::SetNodeProperty {
                node_uuid: changed_node,
                property: "score".into(),
                value: PropValue::Int(124),
            }],
            knowledge: CompositeKnowledgeParticipants::default(),
        })
        .unwrap();
    let delta = graphforge_storage::resolve_project_generation(&source).unwrap();
    assert_eq!(
        graphforge_storage::list_delta_runs(
            &delta.graph_files_inventory().unwrap().unwrap(),
            Default::default()
        )
        .unwrap()
        .len(),
        usize::from(!mixed)
    );
    let latest = graph.execute(snapshot_query).unwrap();
    assert_eq!(int_at(&latest.batches[0], 1, 0), Some(124));
    let compaction = graph.compact_graph_delta(
        &GraphDeltaCompactionRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            through_run_sequence: None,
            limits: GraphDeltaCompactionLimits::default(),
            cleanup_after_commit: false,
            cleanup_policy: ProjectRetentionPolicy::default(),
            cleanup_limits: ProjectRetentionLimits::default(),
        },
        None,
    );
    if mixed {
        assert!(
            compaction
                .unwrap_err()
                .to_string()
                .contains("requires at least one verified run")
        );
        assert_eq!(
            graphforge_storage::resolve_project_generation(&source)
                .unwrap()
                .generation_uuid(),
            delta.generation_uuid()
        );
    } else {
        compaction.unwrap();
    }
    use futures::TryStreamExt as _;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let old: Vec<RecordBatch> = runtime.block_on(snapshot.try_collect()).unwrap();
    assert_eq!(
        old.iter().map(|batch| batch.columns()).collect::<Vec<_>>(),
        immediate
            .batches
            .iter()
            .map(|batch| batch.columns())
            .collect::<Vec<_>>()
    );
    drop(graph);
    let graph = GraphForge::new(source.to_str()).unwrap();
    verify_graph(&graph, fixture, &nodes, &edges);
    let score_query =
        "MATCH (n:`mixed:NewNode`) WHERE n.score IS NOT NULL RETURN n.node_uuid, n.score";
    let expected_score = graph.execute(score_query).unwrap();
    assert_eq!(
        expected_score
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    assert_eq!(uuid_at(&expected_score.batches[0], 0, 0), changed_node);
    assert_eq!(int_at(&expected_score.batches[0], 1, 0), Some(124));
    drop(graph);
    round_trip(root.path(), &source, fixture, &nodes, &edges);
    for path in [&source, &root.path().join("imported")] {
        let reopened = GraphForge::new_with_options(
            path.to_str(),
            graphforge_api::GraphForgeOptions {
                write_mode: if mixed {
                    graphforge_api::ProjectWriteMode::OptimisticMultiWriter
                } else {
                    graphforge_api::ProjectWriteMode::SingleWriter
                },
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            reopened
                .execute(score_query)
                .unwrap()
                .batches
                .iter()
                .map(|batch| batch.columns())
                .collect::<Vec<_>>(),
            expected_score
                .batches
                .iter()
                .map(|batch| batch.columns())
                .collect::<Vec<_>>()
        );
        reopened
            .publish_composite_transaction(CompositeTransactionRequest {
                contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
                context: WriteContext {
                    operation_uuid: OperationId(Uuid::now_v7()),
                    actor_uuid: None,
                },
                graph_mutations: vec![CompositeGraphMutation::RemoveNodeProperty {
                    node_uuid: changed_node,
                    property: "score".into(),
                }],
                knowledge: CompositeKnowledgeParticipants::default(),
            })
            .unwrap();
        assert_eq!(
            reopened
                .execute(score_query)
                .unwrap()
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
        drop(reopened);
        let removed = GraphForge::new(path.to_str()).unwrap();
        assert_eq!(
            removed
                .execute(score_query)
                .unwrap()
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
    }
}

fn configure_composite_ontology(root: &Path, source: &Path, declared_property: bool) {
    use graphforge_api::{AdoptOntologyRequest, OntologyMode, WriteContext};
    let ontology = root.join("routes.yaml");
    let mut ontology_yaml = "ontology_id: https://example.test/mixed\nversion: \"1\"\nentity_types:\n  - name: NewNode\n    abstract: false\nrelation_types:\n  - name: NEW_TYPED\n    src: NewNode\n    dst: NewNode\n".to_owned();
    if declared_property {
        ontology_yaml.push_str("properties:\n  - owner: NewNode\n    name: score\n    type: int64\n    nullable: true\n  - owner: NEW_TYPED\n    name: weight\n    type: int64\n    nullable: true\n");
    }
    std::fs::write(&ontology, ontology_yaml).unwrap();
    let mut graph = GraphForge::new(source.to_str()).unwrap();
    graph
        .adopt_ontology(AdoptOntologyRequest {
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            path: ontology,
            mode: OntologyMode::Advisory,
        })
        .unwrap();
    drop(graph);
    let mut graph = GraphForge::new(source.to_str()).unwrap();
    let candidate = graph.workspace_ontology_composition().unwrap().unwrap();
    let request = graphforge_api::CompositionChangeRequest {
        context: WriteContext {
            operation_uuid: OperationId(Uuid::now_v7()),
            actor_uuid: None,
        },
        expected_project_generation_uuid: graphforge_storage::resolve_project_generation(source)
            .unwrap()
            .generation_uuid(),
        expected_composition_fingerprint: Some(candidate.composition_fingerprint.clone()),
        candidate,
        data_disposition: graphforge_api::CompositionDataDisposition::RequireConforming,
    };
    let preview = graph
        .preview_ontology_composition_change(&request, None)
        .unwrap();
    assert!(preview.diagnostics.is_empty(), "{:?}", preview.diagnostics);
    graph
        .publish_ontology_composition_change(&request, &preview, None)
        .unwrap();
    drop(graph);
}

#[test]
fn composite_qualified_create_then_set_preserves_node_and_edge_owners() {
    exercise_qualified_create_then_set(0);
}

fn exercise_qualified_create_then_set(constructed_nodes: usize) {
    use graphforge_api::{
        COMPOSITE_TRANSACTION_CONTRACT_VERSION, CompositeGraphMutation as Mutation,
        CompositeKnowledgeParticipants, CompositeTransactionRequest, PropValue, WriteContext,
    };
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let fixture = Fixture {
        name: "qualified_composite_edge",
        nodes: constructed_nodes,
        edges: 129,
        routes: 1,
        identifiers: Identifiers::Random,
        properties: false,
        adjacency: false,
        heterogeneous: false,
    };
    let (mut nodes, mut edges) = if constructed_nodes == 0 {
        drop(GraphForge::new(source.to_str()).unwrap());
        (Vec::new(), Vec::new())
    } else {
        let (nodes, edges) = rows(fixture);
        construct(&source, fixture, &nodes, &edges);
        (nodes, edges)
    };
    let prior_objects = cas_uuid_parent_objects(&source);
    configure_composite_ontology(root.path(), &source, true);
    let graph = GraphForge::new(source.to_str()).unwrap();
    let a = Uuid::now_v7();
    let b = Uuid::now_v7();
    let edge = Uuid::now_v7();
    graph
        .publish_composite_transaction(CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            graph_mutations: vec![
                Mutation::CreateNode {
                    node_uuid: a,
                    label: "mixed:NewNode".into(),
                    properties: [("score".into(), PropValue::Int(1))].into(),
                },
                Mutation::CreateNode {
                    node_uuid: b,
                    label: "mixed:NewNode".into(),
                    properties: [("score".into(), PropValue::Int(2))].into(),
                },
                Mutation::CreateEdge {
                    edge_uuid: edge,
                    rel_type: "mixed:NEW_TYPED".into(),
                    source_uuid: a,
                    target_uuid: b,
                    properties: [("weight".into(), PropValue::Int(1))].into(),
                },
                Mutation::SetNodeProperty {
                    node_uuid: a,
                    property: "score".into(),
                    value: PropValue::Int(3),
                },
                Mutation::SetEdgeProperty {
                    edge_uuid: edge,
                    property: "weight".into(),
                    value: PropValue::Int(4),
                },
            ],
            knowledge: CompositeKnowledgeParticipants::default(),
        })
        .unwrap();
    drop(graph);
    let graph = GraphForge::new(source.to_str()).unwrap();
    let result = graph.execute("MATCH (a:`mixed:NewNode`)-[r:`mixed:NEW_TYPED`]->(b:`mixed:NewNode`) RETURN a.node_uuid, b.node_uuid, a.score, r.weight, b.score").unwrap();
    assert_eq!(
        result
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    let batch = &result.batches[0];
    assert_eq!(uuid_at(batch, 0, 0), a);
    assert_eq!(uuid_at(batch, 1, 0), b);
    assert_eq!(int_at(batch, 2, 0), Some(3));
    assert_eq!(int_at(batch, 3, 0), Some(4));
    assert_eq!(int_at(batch, 4, 0), Some(2));
    drop(graph);
    // Optimistic publication exercises the canonical lifecycle independently
    // of the typed-edge GFDR schema repair tracked in #1218.
    let graph = GraphForge::new_with_options(
        source.to_str(),
        graphforge_api::GraphForgeOptions {
            write_mode: graphforge_api::ProjectWriteMode::OptimisticMultiWriter,
            ..Default::default()
        },
    )
    .unwrap();
    graph
        .publish_composite_transaction(CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            graph_mutations: vec![Mutation::RemoveEdgeProperty {
                edge_uuid: edge,
                property: "weight".into(),
            }],
            knowledge: CompositeKnowledgeParticipants::default(),
        })
        .unwrap();
    drop(graph);
    let graph = GraphForge::new(source.to_str()).unwrap();
    let result = graph.execute("MATCH (a:`mixed:NewNode`)-[r:`mixed:NEW_TYPED`]->(b:`mixed:NewNode`) RETURN a.score, r.weight, b.score").unwrap();
    assert_eq!(
        result
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    assert_eq!(int_at(&result.batches[0], 0, 0), Some(3));
    assert_eq!(int_at(&result.batches[0], 1, 0), None);
    assert_eq!(int_at(&result.batches[0], 2, 0), Some(2));
    let current = graphforge_storage::resolve_project_generation(&source).unwrap();
    let bindings = graphforge_storage::semantic_storage_bindings(&current)
        .unwrap()
        .unwrap();
    let relation = bindings
        .bindings
        .iter()
        .find(|binding| {
            binding.route_kind == graphforge_storage::SemanticRouteKind::Relation
                && binding.symbol.local_id == "NEW_TYPED"
        })
        .unwrap();
    nodes.extend([
        (a, "mixed:entity:NewNode".into(), None),
        (b, "mixed:entity:NewNode".into(), None),
    ]);
    edges.push((edge, a, b, relation.route.clone(), None, None));
    cas_uuid_assert_parent_objects(&source, &prior_objects);
    drop(graph);
    round_trip(root.path(), &source, fixture, &nodes, &edges);
    let imported = GraphForge::new(root.path().join("imported").to_str()).unwrap();
    let imported_result = imported.execute("MATCH (a:`mixed:NewNode`)-[r:`mixed:NEW_TYPED`]->(b:`mixed:NewNode`) RETURN a.score, r.weight, b.score").unwrap();
    assert_eq!(
        imported_result
            .batches
            .iter()
            .map(|batch| batch.columns())
            .collect::<Vec<_>>(),
        result
            .batches
            .iter()
            .map(|batch| batch.columns())
            .collect::<Vec<_>>()
    );
}

fn assert_exploratory_edge_windows(source: &Path, expected_files: usize, expected_rows: usize) {
    let selected = graphforge_storage::resolve_project_generation(source).unwrap();
    let inventory = selected.graph_files_inventory().unwrap().unwrap();
    let entries = inventory
        .files
        .iter()
        .filter(|entry| entry.relative_path.starts_with("topology/edges/"))
        .collect::<Vec<_>>();
    assert_eq!(
        entries.len(),
        expected_files,
        "one fragment per physical route and bounded window"
    );
    let mut previous = 0_u64;
    let mut rows = 0;
    for entry in entries {
        let path = graphforge_storage::graph_object_path(source, &entry.content_sha256).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
        assert!(builder.metadata().file_metadata().num_rows() <= 1024);
        assert!(
            builder
                .metadata()
                .row_groups()
                .iter()
                .all(|group| group.num_rows() <= 1024)
        );
        let reader = builder.with_batch_size(7).build().unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            assert!(batch.num_rows() <= 7);
            assert!(batch.column_by_name("rel_type_name").is_some());
            let ids = batch
                .column_by_name("edge_id")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .unwrap();
            for id in ids.values() {
                assert!(
                    *id > previous,
                    "physical fragments must concatenate in strict surrogate order"
                );
                previous = *id;
                rows += 1;
            }
        }
    }
    assert_eq!(rows, expected_rows);
}

#[test]
fn exploratory_construction_groups_bounded_windows_by_physical_route() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let fixture = Fixture {
        name: "physical_route_windows",
        nodes: 66,
        edges: 4097,
        routes: 2,
        identifiers: Identifiers::Random,
        properties: true,
        adjacency: false,
        heterogeneous: true,
    };
    let (nodes, edges) = rows(fixture);
    let parent_evidence = construct(&source, fixture, &nodes, &edges[..2048]);
    assert_exploratory_edge_windows(&source, 2, 2048);
    let parent = graphforge_storage::resolve_project_generation(&source)
        .unwrap()
        .graph_files_inventory()
        .unwrap()
        .unwrap();
    let child_evidence = construct(&source, fixture, &[], &edges[2048..]);
    assert_exploratory_edge_windows(&source, 5, 4097);
    let child = graphforge_storage::resolve_project_generation(&source)
        .unwrap()
        .graph_files_inventory()
        .unwrap()
        .unwrap();
    for entry in parent
        .files
        .iter()
        .filter(|entry| entry.relative_path.starts_with("topology/edges/"))
    {
        assert!(
            child.files.contains(entry),
            "parent edge payloads remain byte-identical"
        );
    }
    for evidence in [&parent_evidence, &child_evidence] {
        assert!(evidence.peak_batch_rows <= 1024);
        assert!(evidence.peak_run_records <= 4096);
        assert!(evidence.peak_merge_inputs <= 2);
        assert_eq!(evidence.prior_topology_rows_decoded, 0);
        assert!(evidence.encode_application_read_bytes > 0);
        assert!(evidence.encode_application_write_bytes > 0);
        assert!(
            evidence.peak_batch_bytes <= GraphConstructionBudgets::default().max_batch_bytes as u64
        );
        assert!(evidence.peak_batch_bytes <= 128 * 1024);
        assert!(evidence.peak_accounted_live_bytes <= 4 * 1024 * 1024);
        assert!(evidence.encode_application_read_bytes <= 1536 * 1024);
        assert!(evidence.encode_application_write_bytes <= 512 * 1024);
        assert!(evidence.total_application_read_bytes().unwrap() <= 16 * 1024 * 1024);
        assert!(evidence.canonical_output_bytes <= 256 * 1024);
        assert!(evidence.staged_and_retained_disk_bytes <= 640 * 1024);
        assert!(evidence.storage_transient_peak_total_allocated_bytes <= 2 * 1024 * 1024);
    }
    let measured = |e: &graphforge_api::GraphConstructionEvidence| {
        json!({
            "input_rows": e.input_rows,
            "canonical_output_bytes": e.canonical_output_bytes,
            "encode_read_bytes": e.encode_application_read_bytes,
            "encode_write_bytes": e.encode_application_write_bytes,
            "total_read_bytes": e.total_application_read_bytes().unwrap(),
            "staged_and_retained_disk_bytes": e.staged_and_retained_disk_bytes,
            "peak_batch_rows": e.peak_batch_rows,
            "peak_batch_bytes": e.peak_batch_bytes,
            "peak_accounted_live_bytes": e.peak_accounted_live_bytes,
            "peak_merge_temporary_bytes": e.peak_merge_temporary_bytes,
            "temporary_allocation_peak_bytes": e.storage_transient_peak_total_allocated_bytes,
        })
    };
    println!(
        "PHYSICAL_ROUTE_WINDOW_RESOURCES {}",
        json!({"parent":measured(&parent_evidence),"child":measured(&child_evidence)})
    );
    let graph = GraphForge::new(source.to_str()).unwrap();
    verify_graph(&graph, fixture, &nodes, &edges);
    drop(graph);
    round_trip(root.path(), &source, fixture, &nodes, &edges);
}

#[test]
fn exploratory_parent_construction_replays_and_compacts_exact_routes() {
    use graphforge_storage::{
        GraphDeltaCompactionLimits, GraphDeltaCompactionRequest, ProjectRetentionLimits,
        ProjectRetentionPolicy,
    };
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let fixture = Fixture {
        name: "publishing_policy",
        nodes: 66,
        edges: 4097,
        routes: 2,
        identifiers: Identifiers::Random,
        properties: true,
        adjacency: false,
        heterogeneous: true,
    };
    let (mut nodes, edges) = rows(fixture);
    construct(&source, fixture, &nodes, &edges[..2048]);
    construct(&source, fixture, &[], &edges[2048..]);
    let graph = GraphForge::new(source.to_str()).unwrap();
    verify_graph(&graph, fixture, &nodes, &edges);
    drop(graph);
    use graphforge_api::{
        COMPOSITE_TRANSACTION_CONTRACT_VERSION, CompositeGraphMutation,
        CompositeKnowledgeParticipants, CompositeTransactionRequest, PropValue, WriteContext,
    };
    let graph = GraphForge::new(source.to_str()).unwrap();
    let snapshot_query = "MATCH (n) RETURN n.node_uuid, n.score ORDER BY n.node_uuid";
    let expected_snapshot = graph.execute(snapshot_query).unwrap();
    let (old_stream, _, _snapshot_guard) = graph
        .execute_stream_owned(snapshot_query, &Default::default())
        .unwrap();
    graph
        .publish_composite_transaction(CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            graph_mutations: vec![CompositeGraphMutation::SetNodeProperty {
                node_uuid: nodes[0].0,
                property: "score".into(),
                value: PropValue::Int(123),
            }],
            knowledge: CompositeKnowledgeParticipants::default(),
        })
        .unwrap();
    drop(graph);
    nodes[0].2 = Some(123);
    let mut graph = GraphForge::new(source.to_str()).unwrap();
    verify_graph(&graph, fixture, &nodes, &edges);
    graph
        .compact_graph_delta(
            &GraphDeltaCompactionRequest {
                transaction_uuid: Uuid::from_u128(121305),
                generation_uuid: Uuid::from_u128(121306),
                through_run_sequence: None,
                limits: GraphDeltaCompactionLimits::default(),
                cleanup_after_commit: false,
                cleanup_policy: ProjectRetentionPolicy::default(),
                cleanup_limits: ProjectRetentionLimits::default(),
            },
            None,
        )
        .unwrap();
    use futures::TryStreamExt as _;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let old_batches: Vec<RecordBatch> = runtime.block_on(old_stream.try_collect()).unwrap();
    assert_eq!(
        old_batches
            .iter()
            .map(|batch| batch.columns())
            .collect::<Vec<_>>(),
        expected_snapshot
            .batches
            .iter()
            .map(|batch| batch.columns())
            .collect::<Vec<_>>()
    );
    drop(graph);
    let graph = GraphForge::new(source.to_str()).unwrap();
    graph
        .execute("MATCH (n) WHERE n.score IS NOT NULL SET n.score = n.score + 1")
        .unwrap();
    for node in &mut nodes {
        node.2 = node.2.map(|score| score + 1);
    }
    verify_graph(&graph, fixture, &nodes, &edges);
    drop(graph);
    let portable = root.path().join("compaction-portable");
    std::fs::create_dir(&portable).unwrap();
    round_trip(&portable, &source, fixture, &nodes, &edges);
}

#[test]
fn exploratory_parent_and_qualified_child_replay_preserve_semantic_routes() {
    use graphforge_api::{
        AdoptOntologyRequest, COMPOSITE_TRANSACTION_CONTRACT_VERSION, CompositeGraphMutation,
        CompositeKnowledgeParticipants, CompositeTransactionRequest, OntologyMode, PropValue,
        WriteContext,
    };
    use graphforge_storage::{
        GraphDeltaCompactionLimits, GraphDeltaCompactionRequest, ProjectRetentionLimits,
        ProjectRetentionPolicy,
    };
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let fixture = Fixture {
        name: "mixed_semantic_routes",
        nodes: 33,
        edges: 258,
        routes: 1,
        identifiers: Identifiers::Random,
        properties: false,
        adjacency: false,
        heterogeneous: false,
    };
    let (mut nodes, mut edges) = rows(fixture);
    construct(&source, fixture, &nodes, &edges[..129]);
    let ontology = root.path().join("routes.yaml");
    std::fs::write(&ontology, "ontology_id: https://example.test/mixed\nversion: \"1\"\nentity_types:\n  - name: NewNode\n    abstract: false\nrelation_types:\n  - name: NEW_TYPED\n    src: NewNode\n    dst: NewNode\nproperties:\n  - owner: NewNode\n    name: score\n    type: int64\n    nullable: true\n").unwrap();
    let mut graph = GraphForge::new(source.to_str()).unwrap();
    graph
        .adopt_ontology(AdoptOntologyRequest {
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            path: ontology,
            mode: OntologyMode::Advisory,
        })
        .unwrap();
    drop(graph);
    let mut graph = GraphForge::new(source.to_str()).unwrap();
    let candidate = graph.workspace_ontology_composition().unwrap().unwrap();
    let request = graphforge_api::CompositionChangeRequest {
        context: WriteContext {
            operation_uuid: OperationId(Uuid::now_v7()),
            actor_uuid: None,
        },
        expected_project_generation_uuid: graphforge_storage::resolve_project_generation(&source)
            .unwrap()
            .generation_uuid(),
        expected_composition_fingerprint: Some(candidate.composition_fingerprint.clone()),
        candidate,
        data_disposition: graphforge_api::CompositionDataDisposition::RequireConforming,
    };
    let preview = graph
        .preview_ontology_composition_change(&request, None)
        .unwrap();
    assert!(preview.diagnostics.is_empty(), "{:?}", preview.diagnostics);
    graph
        .publish_ontology_composition_change(&request, &preview, None)
        .unwrap();
    drop(graph);
    let child_nodes = (0..33)
        .map(|row| (id(3, row, true), "NewNode".to_owned(), None))
        .collect::<Vec<_>>();
    let changed_node = child_nodes[0].0;
    for (row, edge) in edges[129..].iter_mut().enumerate() {
        edge.3 = "NEW_TYPED".into();
        edge.1 = child_nodes[row % child_nodes.len()].0;
        edge.2 = child_nodes[(row + 1) % child_nodes.len()].0;
    }
    construct(&source, fixture, &child_nodes, &edges[129..]);
    nodes.extend(child_nodes.into_iter().map(|mut node| {
        node.1 = "mixed:entity:NewNode".into();
        node
    }));
    let current = graphforge_storage::resolve_project_generation(&source).unwrap();
    let bindings = graphforge_storage::semantic_storage_bindings(&current)
        .unwrap()
        .unwrap();
    let relation = bindings
        .bindings
        .iter()
        .find(|binding| {
            binding.route_kind == graphforge_storage::SemanticRouteKind::Relation
                && binding.symbol.local_id == "NEW_TYPED"
        })
        .unwrap();
    assert_eq!(relation.symbol.display(), "mixed:relation:NEW_TYPED");
    for edge in &mut edges[129..] {
        edge.3.clone_from(&relation.route);
    }
    let mut graph = GraphForge::new(source.to_str()).unwrap();
    verify_graph(&graph, fixture, &nodes, &edges);
    let schemas = |source: &Path| {
        let selected = graphforge_storage::resolve_project_generation(source).unwrap();
        let generation_owned = selected.declared_graph_files_inventory().unwrap().is_some();
        let inventory = selected.graph_files_inventory().unwrap().unwrap();
        let mut schemas = BTreeMap::new();
        for entry in inventory
            .files
            .iter()
            .filter(|entry| entry.relative_path.starts_with("topology/edges/"))
        {
            let path = if generation_owned {
                selected.graph_tree_root().join(&entry.relative_path)
            } else {
                graphforge_storage::graph_object_path(source, &entry.content_sha256).unwrap()
            };
            let reader =
                ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
            let schema = reader.schema().clone();
            let key = if schema.index_of("rel_type_name").is_ok() {
                "exploratory".to_owned()
            } else {
                let route = schema
                    .metadata()
                    .get(graphforge_storage::SEMANTIC_ROUTE_METADATA_KEY)
                    .unwrap();
                assert!(
                    schema
                        .metadata()
                        .contains_key(graphforge_storage::SEMANTIC_COMPOSITION_METADATA_KEY)
                );
                route.clone()
            };
            if let Some(previous) = schemas.insert(key, schema.clone()) {
                assert_eq!(previous, schema);
            }
        }
        assert_eq!(schemas.len(), 2);
        assert!(schemas.contains_key("exploratory"));
        schemas
    };
    let before = schemas(&source);
    // First write establishes qualified schema authority; the next is real GFDR.
    for score in [122, 123] {
        graph
            .publish_composite_transaction(CompositeTransactionRequest {
                contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
                context: WriteContext {
                    operation_uuid: OperationId(Uuid::now_v7()),
                    actor_uuid: None,
                },
                graph_mutations: vec![CompositeGraphMutation::SetNodeProperty {
                    node_uuid: changed_node,
                    property: "score".into(),
                    value: PropValue::Int(score),
                }],
                knowledge: CompositeKnowledgeParticipants::default(),
            })
            .unwrap();
    }
    let delta = graphforge_storage::resolve_project_generation(&source).unwrap();
    assert_eq!(
        graphforge_storage::list_delta_runs(
            &delta.graph_files_inventory().unwrap().unwrap(),
            Default::default()
        )
        .unwrap()
        .len(),
        1
    );
    graph
        .compact_graph_delta(
            &GraphDeltaCompactionRequest {
                transaction_uuid: Uuid::now_v7(),
                generation_uuid: Uuid::now_v7(),
                through_run_sequence: None,
                limits: GraphDeltaCompactionLimits::default(),
                cleanup_after_commit: false,
                cleanup_policy: ProjectRetentionPolicy::default(),
                cleanup_limits: ProjectRetentionLimits::default(),
            },
            None,
        )
        .unwrap();
    assert_eq!(before, schemas(&source));
    graph
        .execute("MATCH (n:`mixed:NewNode`) WHERE n.score IS NOT NULL SET n.score = 124")
        .unwrap();
    let created = graph
        .execute("CREATE (n:`mixed:NewNode`) RETURN n.node_uuid")
        .unwrap();
    nodes.push((
        uuid_at(&created.batches[0], 0, 0),
        "mixed:entity:NewNode".into(),
        None,
    ));
    assert_eq!(before, schemas(&source));
    drop(graph);
    let graph = GraphForge::new(source.to_str()).unwrap();
    verify_graph(&graph, fixture, &nodes, &edges);
    let score_query =
        "MATCH (n:`mixed:NewNode`) WHERE n.score IS NOT NULL RETURN n.node_uuid, n.score";
    let expected_score = graph.execute(score_query).unwrap();
    assert_eq!(
        expected_score
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    assert_eq!(uuid_at(&expected_score.batches[0], 0, 0), changed_node);
    assert_eq!(int_at(&expected_score.batches[0], 1, 0), Some(124));
    drop(graph);
    round_trip(root.path(), &source, fixture, &nodes, &edges);
    for path in [&source, &root.path().join("imported")] {
        let reopened = GraphForge::new(path.to_str()).unwrap();
        assert_eq!(
            reopened
                .execute(score_query)
                .unwrap()
                .batches
                .iter()
                .map(|batch| batch.columns())
                .collect::<Vec<_>>(),
            expected_score
                .batches
                .iter()
                .map(|batch| batch.columns())
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn exploratory_coalesced_routes_reject_oversized_encoding_before_publication() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let fixture = Fixture {
        name: "long_routes",
        nodes: 2,
        edges: 64,
        routes: 2,
        identifiers: Identifiers::Random,
        properties: false,
        adjacency: false,
        heterogeneous: false,
    };
    let (nodes, mut edges) = rows(fixture);
    construct(&source, fixture, &nodes, &[]);
    let before = graphforge_storage::resolve_project_generation(&source).unwrap();
    let graph = GraphForge::new(source.to_str()).unwrap();
    let mut session = graph
        .begin_graph_construction(GraphConstructionBudgets {
            max_batch_rows: 64,
            max_batch_bytes: 16 * 1024,
            max_run_records: 256,
            merge_fan_in: 2,
            ..Default::default()
        })
        .unwrap();
    for (row, edge) in edges.iter_mut().enumerate() {
        edge.3 = format!("R{}_{}", row % 2, "x".repeat(197));
    }
    for (chunk, rows) in edges.chunks(4).enumerate() {
        let batch = RecordBatch::try_new(
            CONSTRUCTION_EDGE_SCHEMA.clone(),
            vec![
                uuids(rows.iter().map(|row| row.0)),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|row| row.3.as_str()),
                )),
                uuids(rows.iter().map(|row| row.1)),
                uuids(rows.iter().map(|row| row.2)),
            ],
        )
        .unwrap();
        session
            .append_edges(&format!("edges-{chunk}"), &batch)
            .unwrap();
    }
    let error = session.seal_and_publish().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("decoded canonical batch exceeds encoding budget"),
        "{error}"
    );
    let after = graphforge_storage::resolve_project_generation(&source).unwrap();
    assert_eq!(before.generation_uuid(), after.generation_uuid());
    assert_eq!(
        before.graph_files_inventory().unwrap(),
        after.graph_files_inventory().unwrap()
    );
    drop(session);
    drop(graph);
    let reopened = GraphForge::new(source.to_str()).unwrap();
    verify_graph(&reopened, fixture, &nodes, &[]);
}

fn publishing_parquet_inventory(source: &Path) -> Value {
    let selected = graphforge_storage::resolve_project_generation(source).unwrap();
    let inventory = selected.graph_files_inventory().unwrap().unwrap();
    let generation_owned = selected.declared_graph_files_inventory().unwrap().is_some();
    let mut files = Vec::new();
    let mut payload = 0_u64;
    let mut allocated = 0_u64;
    for entry in inventory
        .files
        .iter()
        .filter(|entry| entry.relative_path.ends_with(".parquet"))
    {
        let path = if generation_owned {
            selected.graph_tree_root().join(&entry.relative_path)
        } else {
            graphforge_storage::graph_object_path(source, &entry.content_sha256).unwrap()
        };
        let input = File::open(&path).unwrap();
        let physical = graphforge_filesystem::file_space_usage(&input)
            .unwrap()
            .allocated_bytes;
        let reader = ParquetRecordBatchReaderBuilder::try_new(input).unwrap();
        for group in reader.metadata().row_groups() {
            for column in group.columns() {
                assert!(
                    matches!(column.compression(), parquet::basic::Compression::ZSTD(_)),
                    "permanent payload lost shared compression: {}",
                    entry.relative_path
                );
            }
        }
        let groups = reader.metadata().row_groups().iter().map(|group| {
            json!({"rows":group.num_rows(),"columns":group.columns().iter().map(|column| {
                json!({"path":column.column_path().string(),"codec":format!("{:?}", column.compression()),
                    "encodings":format!("{:?}", column.encodings().collect::<Vec<_>>())})
            }).collect::<Vec<_>>()})
        }).collect::<Vec<_>>();
        let edge_order = if reader.schema().index_of("edge_id").is_ok() {
            let ids = reader
                .build()
                .unwrap()
                .flat_map(|batch| {
                    let batch = batch.unwrap();
                    batch
                        .column_by_name("edge_id")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<arrow::array::UInt64Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect::<Vec<_>>();
            json!({"rows":ids.len(), "first":ids.first(), "last":ids.last(),
                "first_inversion":ids.windows(2).position(|pair| pair[0] >= pair[1]),
                "prefix":ids.iter().take(12).collect::<Vec<_>>()})
        } else {
            Value::Null
        };
        let codec_pairs = if std::env::var_os("GF_PARQUET_CODEC_PAIRS").is_some() {
            let reader =
                ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
            let schema = reader.schema().clone();
            let batches = reader
                .build()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            Some(paired_codec_profiles(
                &arrow::compute::concat_batches(&schema, &batches).unwrap(),
            ))
        } else {
            None
        };
        payload += entry.byte_length;
        allocated += physical;
        files.push(json!({"path":entry.relative_path,"sha256":entry.content_sha256,"bytes":entry.byte_length,
            "allocated_bytes":physical,"row_groups":groups, "edge_id_order":edge_order,"codec_pairs":codec_pairs}));
    }
    json!({"ownership":if generation_owned { "generation_graph_tree" } else { "cas" }, "parquet_bytes":payload,"parquet_allocated_bytes":allocated,"files":files})
}

#[test]
fn permanent_publishing_policy_construction_mutation_and_compaction() {
    use graphforge_storage::{
        GraphDeltaCompactionLimits, GraphDeltaCompactionRequest, ProjectRetentionLimits,
        ProjectRetentionPolicy,
    };
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let fixture = Fixture {
        name: "publishing_policy",
        nodes: 1025,
        edges: 4097,
        routes: 2,
        identifiers: Identifiers::Random,
        properties: true,
        adjacency: false,
        heterogeneous: true,
    };
    let (mut nodes, edges) = rows(fixture);
    construct(&source, fixture, &nodes, &edges);
    let construction = publishing_parquet_inventory(&source);
    let graph = GraphForge::new(source.to_str()).unwrap();
    let before = verify_graph(&graph, fixture, &nodes, &edges);
    let started = Instant::now();
    graph
        .execute("MATCH (n) WHERE n.score IS NOT NULL SET n.score = n.score + 1")
        .unwrap();
    let mutation_ns = started.elapsed().as_nanos();
    for node in &mut nodes {
        node.2 = node.2.map(|score| score + 1);
    }
    let after = verify_graph(&graph, fixture, &nodes, &edges);
    assert_ne!(before, after);
    drop(graph);
    let mutation = publishing_parquet_inventory(&source);
    let portable = root.path().join("mutation-portable");
    std::fs::create_dir(&portable).unwrap();
    round_trip(&portable, &source, fixture, &nodes, &edges);

    println!(
        "PUBLISHING_PRE_DELTA {}",
        json!({"construction":construction, "mutation":mutation})
    );
    use graphforge_api::{
        COMPOSITE_TRANSACTION_CONTRACT_VERSION, CompositeGraphMutation,
        CompositeKnowledgeParticipants, CompositeTransactionRequest, PropValue, WriteContext,
    };
    let graph = GraphForge::new(source.to_str()).unwrap();
    graph
        .publish_composite_transaction(CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            graph_mutations: vec![CompositeGraphMutation::SetNodeProperty {
                node_uuid: nodes[0].0,
                property: "score".into(),
                value: PropValue::Int(123),
            }],
            knowledge: CompositeKnowledgeParticipants::default(),
        })
        .unwrap();
    drop(graph);
    nodes[0].2 = Some(123);
    let mut graph = GraphForge::new(source.to_str()).unwrap();
    verify_graph(&graph, fixture, &nodes, &edges);
    let before_compaction = graphforge_storage::resolve_project_generation(&source)
        .unwrap()
        .graph_files_inventory()
        .unwrap()
        .unwrap();
    let started = Instant::now();
    let report = graph
        .compact_graph_delta(
            &GraphDeltaCompactionRequest {
                transaction_uuid: Uuid::from_u128(121305),
                generation_uuid: Uuid::from_u128(121306),
                through_run_sequence: None,
                limits: GraphDeltaCompactionLimits::default(),
                cleanup_after_commit: false,
                cleanup_policy: ProjectRetentionPolicy::default(),
                cleanup_limits: ProjectRetentionLimits::default(),
            },
            None,
        )
        .unwrap();
    let compaction_ns = started.elapsed().as_nanos();
    drop(graph);
    let compaction = publishing_parquet_inventory(&source);
    let changed = compaction["files"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|file| {
            !before_compaction.files.iter().any(|before| {
                file["path"].as_str() == Some(before.relative_path.as_str())
                    && file["sha256"].as_str() == Some(before.content_sha256.as_str())
            })
        })
        .collect::<Vec<_>>();
    assert!(
        !changed.is_empty(),
        "compaction must encode permanent payloads"
    );
    for file in changed {
        for group in file["row_groups"].as_array().unwrap() {
            assert!(group["rows"].as_u64().unwrap() <= 8192);
            for column in group["columns"].as_array().unwrap() {
                assert!(
                    !column["encodings"]
                        .as_str()
                        .unwrap()
                        .contains("RLE_DICTIONARY")
                );
            }
        }
    }

    // Deterministic published-payload ceilings; CPU/RSS/OS I/O remain measured
    // observations, while replay admission and private-stream limits have
    // separate exact boundary regressions.
    for (name, inventory, logical_kib, allocated_kib) in [
        ("construction", &construction, 400, 448),
        ("mutation", &mutation, 416, 480),
        ("compaction", &compaction, 352, 400),
    ] {
        assert!(
            inventory["parquet_bytes"].as_u64().unwrap() <= logical_kib * 1024,
            "{name} payload budget"
        );
        assert!(
            inventory["parquet_allocated_bytes"].as_u64().unwrap() <= allocated_kib * 1024,
            "{name} allocation budget"
        );
    }
    let portable = root.path().join("compaction-portable");
    std::fs::create_dir(&portable).unwrap();
    round_trip(&portable, &source, fixture, &nodes, &edges);
    println!(
        "PERMANENT_PUBLISHING_POLICY {}",
        json!({"construction":construction,
        "mutation":mutation,"compaction":compaction,"mutation_ns":mutation_ns,
        "compaction_ns":compaction_ns,"compaction_report":format!("{report:?}")})
    );
}

// Codec-only comparisons keep schema, dictionary policy, row groups and write
// batches identical within each pair. Whole publishing costs are measured by
// the public fixture separately; ArrowWriter::memory_size excludes native Zstd.
fn paired_codec_profiles(batch: &RecordBatch) -> Value {
    let mut profiles = Vec::new();
    for (name, dictionary, row_group_rows, current) in [
        (
            "construction",
            true,
            1_048_576,
            Compression::ZSTD(ZstdLevel::try_new(1).unwrap()),
        ),
        ("canonical_staging", true, 65_536, Compression::UNCOMPRESSED),
        ("replay", false, 8_192, Compression::UNCOMPRESSED),
        (
            "other_permanent",
            true,
            1_048_576,
            Compression::UNCOMPRESSED,
        ),
    ] {
        let mut outputs = Vec::new();
        let mut retained = Vec::new();
        let mut allocated_peak = 0_u64;
        for codec in [current, Compression::ZSTD(ZstdLevel::try_new(1).unwrap())] {
            let file = tempfile::NamedTempFile::new().unwrap();
            let properties = WriterProperties::builder()
                .set_compression(codec)
                .set_dictionary_enabled(dictionary)
                .set_max_row_group_row_count(Some(row_group_rows))
                .build();
            let start = Instant::now();
            let mut writer =
                ArrowWriter::try_new(file.reopen().unwrap(), batch.schema(), Some(properties))
                    .unwrap();
            let mut arrow_writer_peak = 0;
            for offset in (0..batch.num_rows()).step_by(127) {
                writer
                    .write(&batch.slice(offset, 127.min(batch.num_rows() - offset)))
                    .unwrap();
                arrow_writer_peak = arrow_writer_peak.max(writer.memory_size());
            }
            writer.close().unwrap();
            let encode_ns = start.elapsed().as_nanos();
            let bytes = file.as_file().metadata().unwrap().len();
            let allocated = graphforge_filesystem::file_space_usage(file.as_file())
                .unwrap()
                .allocated_bytes;
            allocated_peak += allocated;
            let start = Instant::now();
            let reader = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap()).unwrap();
            assert_eq!(reader.schema().as_ref(), batch.schema().as_ref());
            let metadata = reader.metadata();
            assert_eq!(
                metadata.num_row_groups(),
                batch.num_rows().div_ceil(row_group_rows)
            );
            for group in metadata.row_groups() {
                assert!(group.num_rows() <= row_group_rows as i64);
                for column in group.columns() {
                    assert_eq!(column.compression(), codec);
                    if !dictionary {
                        assert!(
                            !column.encodings().any(
                                |encoding| encoding == parquet::basic::Encoding::RLE_DICTIONARY
                            )
                        );
                    }
                }
            }
            let decoded = reader
                .with_batch_size(127)
                .build()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let decoded = arrow::compute::concat_batches(&batch.schema(), &decoded).unwrap();
            let decode_ns = start.elapsed().as_nanos();
            assert_eq!(decoded, *batch);
            outputs.push(json!({"codec":format!("{codec:?}"),"bytes":bytes,"allocated_bytes":allocated,
                "encode_ns":encode_ns,"decode_ns":decode_ns,"arrow_writer_peak_bytes":arrow_writer_peak}));
            retained.push(file);
        }
        profiles.push(json!({"profile":name,"dictionary":dictionary,"row_group_rows":row_group_rows,
            "write_batch_rows":127,"outputs_current_candidate":outputs,"pair_temporary_allocated_peak_bytes":allocated_peak}));
    }
    json!({"rows":batch.num_rows(),"arrow_input_bytes":batch.get_array_memory_size(),"profiles":profiles})
}

#[test]
fn permanent_codec_pairs_cover_wide_nullable_and_full_width_values() {
    use arrow::array::{BooleanArray, Float64Array, StructArray, UInt8Array, UInt64Array};
    let rows = 257;
    let mut fields = vec![
        Field::new("uuid", DataType::FixedSizeBinary(16), false),
        Field::new("ordinal", DataType::UInt64, false),
    ];
    let mut arrays = vec![
        uuids((0..rows).map(|row| id(17, row, true))),
        Arc::new(UInt64Array::from(
            (0..rows)
                .map(|row| u64::MAX - row as u64)
                .collect::<Vec<_>>(),
        )) as ArrayRef,
    ];
    for column in 0..32 {
        fields.push(Field::new(
            format!("nullable_{column}"),
            DataType::Int64,
            true,
        ));
        arrays.push(Arc::new(Int64Array::from(
            (0..rows)
                .map(|row| (!row.is_multiple_of(3)).then_some((row * 37 + column) as i64))
                .collect::<Vec<_>>(),
        )));
    }
    let strings = (0..rows)
        .map(|row| {
            if row.is_multiple_of(3) {
                None
            } else {
                let length = if row == 1 { 256 * 1024 } else { 1024 };
                Some(
                    (0..length)
                        .map(|index| char::from(b'!' + ((index * 31 + row * 17) % 90) as u8))
                        .collect::<String>(),
                )
            }
        })
        .collect::<Vec<_>>();
    fields.push(Field::new("large_nullable_text", DataType::Utf8, true));
    arrays.push(Arc::new(StringArray::from(strings)));
    let tagged_fields: arrow::datatypes::Fields = vec![
        Field::new("tag", DataType::UInt8, false),
        Field::new("int", DataType::Int64, true),
        Field::new("float", DataType::Float64, true),
        Field::new("str", DataType::Utf8, true),
        Field::new("bool", DataType::Boolean, true),
    ]
    .into();
    let tagged = StructArray::new(
        tagged_fields.clone(),
        vec![
            Arc::new(UInt8Array::from(
                (0..rows).map(|row| (row % 4) as u8).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (0..rows)
                    .map(|row| (row % 4 == 0).then_some(row as i64))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..rows)
                    .map(|row| (row % 4 == 1).then_some(row as f64 / 3.0))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..rows)
                    .map(|row| (row % 4 == 2).then_some("tagged"))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                (0..rows)
                    .map(|row| (row % 4 == 3).then_some(true))
                    .collect::<Vec<_>>(),
            )),
        ],
        None,
    );
    fields.push(Field::new("tagged", DataType::Struct(tagged_fields), false));
    arrays.push(Arc::new(tagged));
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    let small = RecordBatch::try_new(
        batch.schema(),
        batch
            .columns()
            .iter()
            .map(|column| {
                arrow::compute::take(
                    column.as_ref(),
                    &arrow::array::UInt32Array::from(vec![0]),
                    None,
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap();
    let narrow_rows = 65_537;
    let narrow = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("uuid", DataType::FixedSizeBinary(16), false),
            Field::new("ordinal", DataType::UInt64, false),
        ])),
        vec![
            uuids((0..narrow_rows).map(|row| id(19, row, true))),
            Arc::new(UInt64Array::from(
                (0..narrow_rows)
                    .map(|row| u64::MAX - row as u64)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    for selected in [small, batch, narrow] {
        let result = paired_codec_profiles(&selected);
        for profile in result["profiles"].as_array().unwrap() {
            assert!(
                profile["pair_temporary_allocated_peak_bytes"]
                    .as_u64()
                    .unwrap()
                    <= 8 * 1024 * 1024
            );
            for output in profile["outputs_current_candidate"].as_array().unwrap() {
                assert!(output["arrow_writer_peak_bytes"].as_u64().unwrap() <= 8 * 1024 * 1024);
                assert!(output["bytes"].as_u64().unwrap() <= 2 * 1024 * 1024);
            }
        }
        println!("PERMANENT_CODEC_PAIRS {result}");
    }
}

type CasUuidParentObjects = Vec<(
    graphforge_storage::GraphFileEntry,
    graphforge_filesystem::FileIdentity,
)>;

fn cas_uuid_parent_objects(source: &Path) -> CasUuidParentObjects {
    let selected = graphforge_storage::resolve_project_generation(source).unwrap();
    let Some(inventory) = selected.graph_files_inventory().unwrap() else {
        return Vec::new();
    };
    if selected.declared_graph_files_inventory().unwrap().is_some() {
        return Vec::new();
    }
    inventory
        .files
        .into_iter()
        .map(|entry| {
            let file = File::open(
                graphforge_storage::graph_object_path(source, &entry.content_sha256).unwrap(),
            )
            .unwrap();
            let identity = graphforge_filesystem::file_identity(&file).unwrap();
            (entry, identity)
        })
        .collect()
}

fn cas_uuid_assert_parent_objects(source: &Path, objects: &CasUuidParentObjects) {
    for (entry, identity) in objects {
        let path = graphforge_storage::graph_object_path(source, &entry.content_sha256).unwrap();
        let file = File::open(&path).unwrap();
        assert_eq!(
            graphforge_filesystem::file_identity(&file).unwrap(),
            *identity
        );
        assert!(file.metadata().unwrap().permissions().readonly());
        assert_eq!(
            digest_hex(&std::fs::read(path).unwrap()),
            entry.content_sha256
        );
    }
}

fn cas_uuid_node_surrogates(source: &Path) -> BTreeMap<Uuid, u64> {
    use arrow::array::UInt64Array;
    let selected = graphforge_storage::resolve_project_generation(source).unwrap();
    let inventory = selected.graph_files_inventory().unwrap().unwrap();
    let owned = selected.declared_graph_files_inventory().unwrap().is_some();
    let mut ids = BTreeMap::new();
    for entry in inventory.files.iter().filter(|entry| {
        (entry.relative_path == "topology/nodes.parquet"
            || entry.relative_path.starts_with("topology/nodes/"))
            && entry.relative_path.ends_with(".parquet")
    }) {
        let path = if owned {
            selected.graph_tree_root().join(&entry.relative_path)
        } else {
            graphforge_storage::graph_object_path(source, &entry.content_sha256).unwrap()
        };
        for batch in ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap()
        {
            let batch = batch.unwrap();
            let uuids = batch
                .column_by_name("node_uuid")
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            let values = batch
                .column_by_name("node_id")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert!(
                    ids.insert(
                        Uuid::from_slice(uuids.value(row)).unwrap(),
                        values.value(row)
                    )
                    .is_none()
                );
            }
        }
    }
    ids
}

fn cas_uuid_hydration_budget(root: &Path, source: &Path) {
    let selected = graphforge_storage::resolve_project_generation(source).unwrap();
    let inventory = selected.graph_files_inventory().unwrap().unwrap();
    let owner = tempfile::tempdir_in(root).unwrap();
    let workspace = owner.path().join("hydration-probe");
    let evidence =
        graphforge_storage::materialize_graph_objects(source, &inventory, &workspace).unwrap();
    let mut control_bytes = 0;
    let mut control_allocated = 0;
    let mut shared_run_bytes = 0;
    for entry in inventory
        .files
        .iter()
        .filter(|entry| entry.relative_path.starts_with("topology/uuid-membership/"))
    {
        let name = Path::new(&entry.relative_path)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap();
        let file = File::open(workspace.join(&entry.relative_path)).unwrap();
        let object = File::open(
            graphforge_storage::graph_object_path(source, &entry.content_sha256).unwrap(),
        )
        .unwrap();
        if matches!(name, "manifest.json" | "topology-receipt.json") {
            assert_eq!(graphforge_filesystem::file_link_count(&file).unwrap(), 1);
            assert_ne!(
                graphforge_filesystem::file_identity(&file).unwrap(),
                graphforge_filesystem::file_identity(&object).unwrap()
            );
            control_bytes += entry.byte_length;
            control_allocated += graphforge_filesystem::file_space_usage(&file)
                .unwrap()
                .allocated_bytes;
        } else if name.starts_with("identities-v5") || name.starts_with("node-surrogates-v5") {
            assert!(file.metadata().unwrap().permissions().readonly());
            assert_eq!(
                graphforge_filesystem::file_identity(&file).unwrap(),
                graphforge_filesystem::file_identity(&object).unwrap()
            );
            shared_run_bytes += entry.byte_length;
        }
    }
    assert!(control_bytes > 0 && shared_run_bytes > 0);
    assert!(
        control_bytes <= 2 * 1024,
        "mutable control copy I/O: {control_bytes}"
    );
    assert!(
        evidence.application_write_bytes <= 192 * 1024,
        "all private hydration writes: {}",
        evidence.application_write_bytes
    );
    assert_eq!(graphforge_storage::GRAPH_OBJECT_IO_BUFFER_BYTES, 64 * 1024);
    assert!(
        control_allocated <= 8 * 1024,
        "two mutable controls: {control_allocated}"
    );
    println!(
        "CAS_UUID_HYDRATION {}",
        json!({"control_bytes":control_bytes,"control_allocated_bytes":control_allocated,"shared_immutable_run_bytes":shared_run_bytes,"materialization":format!("{evidence:?}")})
    );
}

#[test]
fn cas_uuid_canonical_create_after_construction_preserves_authorities() {
    for node_count in [33, 4097] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let fixture = Fixture {
            name: "cas_uuid_mutation",
            nodes: node_count,
            edges: 129,
            routes: 1,
            identifiers: Identifiers::Random,
            properties: true,
            adjacency: false,
            heterogeneous: false,
        };
        let (mut nodes, edges) = rows(fixture);
        construct(&source, fixture, &nodes, &edges);
        let prior_objects = cas_uuid_parent_objects(&source);
        assert!(!prior_objects.is_empty());
        cas_uuid_hydration_budget(root.path(), &source);
        let base_ids = cas_uuid_node_surrogates(&source);
        let mut high_water = *base_ids.values().max().unwrap();
        let graph = GraphForge::new(source.to_str()).unwrap();
        let snapshot_query = "MATCH (n) RETURN n.node_uuid, n.score ORDER BY n.node_uuid";
        let expected = graph.execute(snapshot_query).unwrap();
        let old_stream = graph.execute_stream(snapshot_query).unwrap();
        for step in 0..9 {
            let score = 9_000_000 + step;
            let result = graph
                .execute(&format!(
                    "CREATE (n:Node0 {{score: {score}}}) RETURN n.node_uuid"
                ))
                .unwrap();
            let uuid = uuid_at(&result.batches[0], 0, 0);
            let ids = cas_uuid_node_surrogates(&source);
            assert!(ids[&uuid] > high_water);
            high_water = ids[&uuid];
            for (uuid, id) in &base_ids {
                assert_eq!(ids[uuid], *id);
            }
            nodes.push((uuid, "Node0".into(), Some(score)));
        }
        graph
            .execute("MATCH (n:Node0 {score: 9000000}) DELETE n")
            .unwrap();
        nodes.retain(|node| node.2 != Some(9_000_000));
        let result = graph
            .execute("CREATE (n:Node0 {score: 9000100}) RETURN n.node_uuid")
            .unwrap();
        let uuid = uuid_at(&result.batches[0], 0, 0);
        assert!(cas_uuid_node_surrogates(&source)[&uuid] > high_water);
        nodes.push((uuid, "Node0".into(), Some(9_000_100)));
        verify_graph(&graph, fixture, &nodes, &edges);
        use futures::TryStreamExt as _;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = runtime.block_on(old_stream.try_collect()).unwrap();
        // Each query has its own query_id schema metadata. Compare the exact
        // fields and ordered values independently of execution batch boundaries.
        let actual = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let expected =
            arrow::compute::concat_batches(&expected.batches[0].schema(), &expected.batches)
                .unwrap();
        assert_eq!(actual.schema().fields(), expected.schema().fields());
        assert_eq!(actual.columns(), expected.columns());
        cas_uuid_assert_parent_objects(&source, &prior_objects);
        drop(graph);
        round_trip(root.path(), &source, fixture, &nodes, &edges);
    }
}

#[test]
fn cas_uuid_composite_create_after_construction_preserves_authorities() {
    exercise_qualified_create_then_set(33);
    exercise_qualified_create_then_set(4097);
}

#[test]
fn cas_uuid_publication_fault_child() {
    let Ok(source) = std::env::var("GF_CAS_UUID_FAULT_ROOT") else {
        return;
    };
    let hook = std::env::var("GRAPHFORGE_PROJECT_FAILPOINT").unwrap();
    let graph = GraphForge::new(Some(&source)).unwrap();
    let result = graph.execute("CREATE (n:Node0 {score: 9000000}) RETURN n.node_uuid");
    assert!(
        hook.ends_with(".error"),
        "crash hook was not reached: {hook}: {result:?}"
    );
    // A returned error after the private durable intent is reconciled by the
    // UUID transaction's authenticated roll-forward, then published normally.
    let reconciled = hook == "rewrite.after_durable_intent.error";
    if reconciled {
        result.unwrap();
    } else {
        let error = result.unwrap_err();
        assert_eq!(error.code(), "GF_PUBLICATION_FAILED", "{hook}: {error}");
    }
    let count = if reconciled || hook == "project.after_current_replace.error" {
        34
    } else {
        33
    };
    assert_eq!(graph.node_count("Node0").unwrap(), count);
}

#[test]
fn cas_uuid_publication_faults_recover_and_allow_retry() {
    for boundary in [
        "rewrite.before_intent",
        "rewrite.after_durable_intent",
        "project.before_current_replace",
        "project.after_current_replace",
    ] {
        for returned_error in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let source = root.path().join("source");
            let fixture = Fixture {
                name: "cas_uuid_recovery",
                nodes: 33,
                edges: 129,
                routes: 1,
                identifiers: Identifiers::Random,
                properties: true,
                adjacency: false,
                heterogeneous: false,
            };
            let (mut nodes, edges) = rows(fixture);
            construct(&source, fixture, &nodes, &edges);
            let prior_objects = cas_uuid_parent_objects(&source);
            let before = graphforge_storage::resolve_project_generation(&source)
                .unwrap()
                .generation_uuid();
            let hook = format!("{boundary}{}", if returned_error { ".error" } else { "" });
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "cas_uuid_publication_fault_child", "--nocapture"])
                .env("GF_CAS_UUID_FAULT_ROOT", &source)
                .env(
                    "GRAPHFORGE_PROJECT_FAILPOINTS",
                    "graphforge-internal-subprocess-v1",
                )
                .env("GRAPHFORGE_PROJECT_FAILPOINT", &hook)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(if returned_error { 0 } else { 86 }),
                "{hook}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let graph = GraphForge::new(source.to_str()).unwrap();
            let after = graphforge_storage::resolve_project_generation(&source)
                .unwrap()
                .generation_uuid();
            let published = boundary == "project.after_current_replace"
                || (returned_error && boundary == "rewrite.after_durable_intent");
            assert_eq!(before != after, published, "{hook}");
            let result = graph
                .execute("MATCH (n:Node0 {score: 9000000}) RETURN n.node_uuid")
                .unwrap();
            assert_eq!(
                result
                    .batches
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>(),
                usize::from(published)
            );
            if published {
                nodes.push((
                    uuid_at(&result.batches[0], 0, 0),
                    "Node0".into(),
                    Some(9_000_000),
                ));
            }
            verify_graph(&graph, fixture, &nodes, &edges);
            let result = graph
                .execute("CREATE (n:Node0 {score: 9000001}) RETURN n.node_uuid")
                .unwrap();
            nodes.push((
                uuid_at(&result.batches[0], 0, 0),
                "Node0".into(),
                Some(9_000_001),
            ));
            verify_graph(&graph, fixture, &nodes, &edges);
            cas_uuid_assert_parent_objects(&source, &prior_objects);
            drop(graph);
            round_trip(root.path(), &source, fixture, &nodes, &edges);
        }
    }
}

#[test]
fn bound_ontology_clear_refuses_before_publication_and_preserves_graph() {
    for node_count in [33, 4097] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let fixture = Fixture {
            name: "bound_ontology_clear",
            nodes: node_count,
            edges: 129,
            routes: 1,
            identifiers: Identifiers::Random,
            properties: false,
            adjacency: false,
            heterogeneous: false,
        };
        let (mut nodes, edges) = rows(fixture);
        construct(&source, fixture, &nodes, &edges);
        configure_composite_ontology(root.path(), &source, true);
        for populated in [false, true] {
            if populated {
                let children = vec![
                    (id(3, 0, true), "NewNode".into(), Some(17)),
                    (id(3, 1, true), "NewNode".into(), None),
                ];
                construct(
                    &source,
                    Fixture {
                        properties: true,
                        ..fixture
                    },
                    &children,
                    &[],
                );
                nodes.extend(children.into_iter().map(|mut node| {
                    node.1 = "mixed:entity:NewNode".into();
                    // Unlabelled projection excludes ontology properties; verify
                    // the qualified nullable score independently below.
                    node.2 = None;
                    node
                }));
            }
            let mut graph = GraphForge::new(source.to_str()).unwrap();
            let parent = graphforge_storage::resolve_project_generation(&source).unwrap();
            let bindings = graphforge_storage::semantic_storage_bindings(&parent)
                .unwrap()
                .unwrap();
            assert!(!bindings.bindings.is_empty());
            let participants = parent.participant_snapshots().unwrap();
            let ontology = graph.workspace_ontology().unwrap();
            let composition = graph.workspace_ontology_composition().unwrap();
            let configuration = graph.workspace_configuration().unwrap();
            let objects = cas_uuid_parent_objects(&source);
            let query = "MATCH (n) RETURN n.node_uuid ORDER BY n.node_uuid";
            let expected = graph.execute(query).unwrap();
            let stream = graph.execute_stream(query).unwrap();
            let files = clear_publication_files(&source);
            let request = graphforge_api::ClearOntologyRequest {
                context: graphforge_api::WriteContext {
                    operation_uuid: OperationId(Uuid::now_v7()),
                    actor_uuid: None,
                },
            };
            for _ in 0..2 {
                let error = graph.clear_ontology(request.clone()).unwrap_err();
                assert!(
                    matches!(error, graphforge_core::GfError::Validation(_)),
                    "{error:?}"
                );
                assert!(
                    error
                        .to_string()
                        .contains("semantic bindings require their persisted ontology composition"),
                    "{error}"
                );
                assert_eq!(clear_publication_files(&source), files);
                let selected = graphforge_storage::resolve_project_generation(&source).unwrap();
                assert_eq!(selected.generation_uuid(), parent.generation_uuid());
                assert_eq!(selected.participant_snapshots().unwrap(), participants);
                assert_eq!(graph.workspace_ontology().unwrap(), ontology);
                assert_eq!(graph.workspace_ontology_composition().unwrap(), composition);
                assert_eq!(graph.workspace_configuration().unwrap(), configuration);
                assert_eq!(
                    graph.ontology_mode(),
                    graphforge_api::OntologyMode::Advisory
                );
            }
            cas_uuid_assert_parent_objects(&source, &objects);
            use futures::TryStreamExt as _;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let retained: Vec<RecordBatch> = runtime.block_on(stream.try_collect()).unwrap();
            assert_eq!(
                retained
                    .iter()
                    .map(|batch| batch.columns())
                    .collect::<Vec<_>>(),
                expected
                    .batches
                    .iter()
                    .map(|batch| batch.columns())
                    .collect::<Vec<_>>()
            );
            verify_graph(&graph, fixture, &nodes, &edges);
            if populated {
                let values = graph.execute("MATCH (n:`mixed:NewNode`) RETURN n.node_uuid, n.score ORDER BY n.node_uuid").unwrap();
                assert_eq!(values.stats.rows_produced, 2);
                let scores = values
                    .batches
                    .iter()
                    .flat_map(|batch| {
                        (0..batch.num_rows())
                            .map(|row| (uuid_at(batch, 0, row), int_at(batch, 1, row)))
                    })
                    .collect::<BTreeMap<_, _>>();
                assert_eq!(
                    scores,
                    BTreeMap::from([(id(3, 0, true), Some(17)), (id(3, 1, true), None)])
                );
            }
            drop(graph);
            let portable = root.path().join(format!("portable-{populated}"));
            std::fs::create_dir(&portable).unwrap();
            round_trip(&portable, &source, fixture, &nodes, &edges);
            if populated {
                for path in [&source, &portable.join("imported")] {
                    let graph = GraphForge::new(path.to_str()).unwrap();
                    let values = graph
                        .execute("MATCH (n:`mixed:NewNode`) RETURN n.node_uuid, n.score")
                        .unwrap();
                    let scores = values
                        .batches
                        .iter()
                        .flat_map(|batch| {
                            (0..batch.num_rows())
                                .map(|row| (uuid_at(batch, 0, row), int_at(batch, 1, row)))
                        })
                        .collect::<BTreeMap<_, _>>();
                    assert_eq!(
                        scores,
                        BTreeMap::from([(id(3, 0, true), Some(17)), (id(3, 1, true), None)])
                    );
                }
            }
            println!(
                "BOUND_CLEAR_BUDGET {}",
                json!({"nodes":node_count,"qualified_rows_present":populated,"retained_files":files.len(),"new_files":0,"changed_file_bytes":0,"new_allocated_bytes":0})
            );
        }
    }
}

// Snapshot all durable files, including CURRENT and publication journals. A
// refused clear must not stage a candidate or allocate a replacement payload.
fn clear_publication_files(root: &Path) -> BTreeMap<std::path::PathBuf, (String, u64)> {
    let mut files = BTreeMap::new();
    let mut directories = vec![root.to_path_buf()];
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                directories.push(path);
            } else {
                let file = File::open(&path).unwrap();
                files.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    (
                        digest_hex(&std::fs::read(&path).unwrap()),
                        graphforge_filesystem::file_space_usage(&file)
                            .unwrap()
                            .allocated_bytes,
                    ),
                );
            }
        }
    }
    files
}

fn unbound_clear_request() -> graphforge_api::ClearOntologyRequest {
    graphforge_api::ClearOntologyRequest {
        context: graphforge_api::WriteContext {
            operation_uuid: OperationId(Uuid::from_u128(1230003)),
            actor_uuid: None,
        },
    }
}

#[test]
fn unbound_ontology_clear_fault_child() {
    let Ok(source) = std::env::var("GF_UNBOUND_CLEAR_ROOT") else {
        return;
    };
    let mut graph = GraphForge::new(Some(&source)).unwrap();
    let error = graph.clear_ontology(unbound_clear_request()).unwrap_err();
    assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
}

#[test]
fn unbound_ontology_clear_recovers_and_retries_without_rewriting_payloads() {
    for boundary in [
        "project.before_current_replace",
        "project.after_current_replace",
    ] {
        for returned_error in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let source = root.path().join("source");
            let fixture = Fixture {
                name: "unbound_clear_recovery",
                nodes: 33,
                edges: 129,
                routes: 1,
                identifiers: Identifiers::Random,
                properties: false,
                adjacency: false,
                heterogeneous: false,
            };
            let (nodes, edges) = rows(fixture);
            construct(&source, fixture, &nodes, &edges);
            let ontology = root.path().join("ontology.yaml");
            std::fs::write(&ontology, "ontology_id: clear\nversion: \"1\"\nentity_types:\n  - name: Unused\n    abstract: false\nrelation_types: []\n").unwrap();
            let mut graph = GraphForge::new(source.to_str()).unwrap();
            graph
                .adopt_ontology(graphforge_api::AdoptOntologyRequest {
                    context: graphforge_api::WriteContext {
                        operation_uuid: OperationId(Uuid::now_v7()),
                        actor_uuid: None,
                    },
                    path: ontology,
                    mode: graphforge_api::OntologyMode::Advisory,
                })
                .unwrap();
            drop(graph);
            let parent = graphforge_storage::resolve_project_generation(&source).unwrap();
            let inventory = parent.graph_files_inventory().unwrap();
            let objects = cas_uuid_parent_objects(&source);
            let hook = format!("{boundary}{}", if returned_error { ".error" } else { "" });
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "unbound_ontology_clear_fault_child",
                    "--nocapture",
                ])
                .env("GF_UNBOUND_CLEAR_ROOT", &source)
                .env(
                    "GRAPHFORGE_PROJECT_FAILPOINTS",
                    "graphforge-internal-subprocess-v1",
                )
                .env("GRAPHFORGE_PROJECT_FAILPOINT", &hook)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(if returned_error { 0 } else { 86 }),
                "{hook}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let mut graph = GraphForge::new(source.to_str()).unwrap();
            let recovered = graphforge_storage::resolve_project_generation(&source).unwrap();
            assert_eq!(
                recovered.generation_uuid() != parent.generation_uuid(),
                boundary == "project.after_current_replace"
            );
            assert_eq!(
                graph.ontology_mode(),
                if boundary == "project.after_current_replace" {
                    graphforge_api::OntologyMode::Exploratory
                } else {
                    graphforge_api::OntologyMode::Advisory
                }
            );
            verify_graph(&graph, fixture, &nodes, &edges);
            for _ in 0..2 {
                graph.clear_ontology(unbound_clear_request()).unwrap();
            }
            assert_eq!(
                graph.ontology_mode(),
                graphforge_api::OntologyMode::Exploratory
            );
            assert_eq!(
                graphforge_storage::resolve_project_generation(&source)
                    .unwrap()
                    .graph_files_inventory()
                    .unwrap(),
                inventory
            );
            cas_uuid_assert_parent_objects(&source, &objects);
            let created = graph
                .execute("CREATE (n:Node0) RETURN n.node_uuid")
                .unwrap();
            let mut expected = nodes.clone();
            expected.push((uuid_at(&created.batches[0], 0, 0), "Node0".into(), None));
            verify_graph(&graph, fixture, &expected, &edges);
            drop(graph);
            round_trip(root.path(), &source, fixture, &expected, &edges);
        }
    }
}

#[test]
fn facade_compaction_refreshes_authority_and_preserves_lazy_snapshot() {
    for nodes in [33, 4097] {
        exercise_facade_compaction_refresh(nodes, false);
    }
}

#[test]
fn facade_compaction_rejects_partial_chain_then_refreshes_full_chain() {
    for nodes in [33, 4097] {
        exercise_facade_compaction_refresh(nodes, true);
    }
}

fn exercise_facade_compaction_refresh(node_count: usize, multiple_deltas: bool) {
    use futures::TryStreamExt as _;
    use graphforge_api::{
        COMPOSITE_TRANSACTION_CONTRACT_VERSION, CompositeGraphMutation as Mutation,
        CompositeKnowledgeParticipants, CompositeTransactionRequest, PropValue, WriteContext,
    };
    use graphforge_storage::{
        GraphDeltaCompactionLimits, GraphDeltaCompactionRequest, ProjectRetentionLimits,
        ProjectRetentionPolicy,
    };
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let fixture = Fixture {
        name: "facade_compaction_authority",
        nodes: node_count,
        edges: 129,
        routes: 2,
        identifiers: Identifiers::Random,
        properties: true,
        adjacency: false,
        heterogeneous: false,
    };
    let (mut nodes, edges) = rows(fixture);
    construct(&source, fixture, &nodes, &edges);
    let parent_objects = cas_uuid_parent_objects(&source);
    let original_ids = cas_uuid_node_surrogates(&source);
    let mut graph = GraphForge::new_with_options(
        source.to_str(),
        graphforge_api::GraphForgeOptions {
            resource: graphforge_api::ExecutionResourcePolicy {
                batch_size: Some(7),
                target_partitions: Some(1),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    if multiple_deltas {
        graph.index_adjacency().unwrap();
    }
    graph
        .publish_composite_transaction(CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            graph_mutations: vec![
                Mutation::SetNodeProperty {
                    node_uuid: nodes[0].0,
                    property: "score".into(),
                    value: PropValue::Int(9),
                },
                Mutation::RemoveNodeProperty {
                    node_uuid: nodes[1].0,
                    property: "score".into(),
                },
            ],
            knowledge: CompositeKnowledgeParticipants::default(),
        })
        .unwrap();
    nodes[0].2 = Some(9);
    nodes[1].2 = None;
    verify_graph(&graph, fixture, &nodes, &edges);
    if multiple_deltas {
        graph
            .publish_composite_transaction(CompositeTransactionRequest {
                contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
                context: WriteContext {
                    operation_uuid: OperationId(Uuid::now_v7()),
                    actor_uuid: None,
                },
                graph_mutations: vec![Mutation::SetNodeProperty {
                    node_uuid: nodes[2].0,
                    property: "score".into(),
                    value: PropValue::Int(7),
                }],
                knowledge: CompositeKnowledgeParticipants::default(),
            })
            .unwrap();
        nodes[2].2 = Some(7);
    }
    let expected_snapshot = nodes
        .iter()
        .map(|row| (row.0, row.2))
        .collect::<BTreeMap<_, _>>();
    let mut snapshot = graph
        .execute_stream("MATCH (n) RETURN n.node_uuid, n.score")
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let first = runtime.block_on(snapshot.try_next()).unwrap().unwrap();
    assert!(first.num_rows() > 0 && first.num_rows() < node_count);
    let mut request = GraphDeltaCompactionRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid: Uuid::now_v7(),
        through_run_sequence: multiple_deltas.then_some(1),
        limits: GraphDeltaCompactionLimits::default(),
        cleanup_after_commit: false,
        cleanup_policy: ProjectRetentionPolicy::default(),
        cleanup_limits: ProjectRetentionLimits::default(),
    };
    let parent = graphforge_storage::resolve_project_generation(&source)
        .unwrap()
        .generation_uuid();
    let cancelled = graphforge_api::CancellationToken::new();
    cancelled.cancel();
    assert!(
        graph
            .compact_graph_delta(&request, Some(&cancelled))
            .is_err()
    );
    assert_eq!(
        graphforge_storage::resolve_project_generation(&source)
            .unwrap()
            .generation_uuid(),
        parent
    );
    if multiple_deltas {
        let error = graph.compact_graph_delta(&request, None).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("requires the full verified delta chain")
        );
        assert_eq!(
            graphforge_storage::resolve_project_generation(&source)
                .unwrap()
                .generation_uuid(),
            parent
        );
        verify_graph(&graph, fixture, &nodes, &edges);
        request.through_run_sequence = None;
    }
    let report = graph.compact_graph_delta(&request, None).unwrap();
    assert!(report.output_bytes <= 1024 * 1024, "{report:?}");
    // This report accounts logical compaction state, not total process RSS.
    assert!(report.peak_memory_bytes <= 4 * 1024, "{report:?}");
    assert_eq!(report.spill_bytes, 0);
    let compacted_parquet = publishing_parquet_inventory(&source);
    let hydration = graph.graph_open_evidence();
    let owned = compacted_parquet["ownership"] == "generation_graph_tree";
    if owned {
        // Generation-owned payloads cannot acquire extra immutable CAS links.
        // Refresh retains the existing streaming private-copy ownership rule.
        assert!(
            hydration.application_read_bytes <= 2 * 1024 * 1024,
            "{hydration:?}"
        );
        assert!(
            hydration.application_write_bytes <= 1024 * 1024,
            "{hydration:?}"
        );
        assert!(hydration.files_copied <= 64, "{hydration:?}");
        assert!(hydration.fsync_calls <= 128, "{hydration:?}");
    } else {
        assert!(hydration.files_reused > 0);
        assert!(
            hydration.application_read_bytes <= 1024 * 1024,
            "{hydration:?}"
        );
        assert!(
            hydration.application_write_bytes <= 192 * 1024,
            "{hydration:?}"
        );
        assert!(hydration.files_copied <= 8, "{hydration:?}");
        assert!(hydration.fsync_calls <= 16, "{hydration:?}");
    }
    println!(
        "FACADE_COMPACTION_REFRESH {}",
        json!({"nodes":node_count,"multiple_deltas":multiple_deltas,"owned":owned,"hydration":format!("{hydration:?}"),"compaction":format!("{report:?}"),"parquet_bytes":compacted_parquet["parquet_bytes"]})
    );
    verify_graph(&graph, fixture, &nodes, &edges);
    assert!(
        graph
            .compact_graph_delta(&request, None)
            .unwrap()
            .publication
            .unwrap()
            .idempotent_replay
    );
    graph.execute("MATCH (n:Node0) SET n.score = 11").unwrap();
    for node in &mut nodes {
        if node.1 == "Node0" {
            node.2 = Some(11);
        }
    }
    let created = graph
        .execute("CREATE (n:Node0 {score: 9000}) RETURN n.node_uuid")
        .unwrap();
    let retired_uuid = uuid_at(&created.batches[0], 0, 0);
    let retired_id = cas_uuid_node_surrogates(&source)[&retired_uuid];
    graph
        .execute("MATCH (n:Node0 {score: 9000}) DELETE n")
        .unwrap();
    let created = graph
        .execute("CREATE (n:Node0 {score: 9001}) RETURN n.node_uuid")
        .unwrap();
    let created_uuid = uuid_at(&created.batches[0], 0, 0);
    nodes.push((created_uuid, "Node0".into(), Some(9001)));
    let current_ids = cas_uuid_node_surrogates(&source);
    assert!(retired_id > *original_ids.values().max().unwrap());
    assert!(current_ids[&created_uuid] > retired_id);
    for (uuid, surrogate) in &original_ids {
        assert_eq!(current_ids[uuid], *surrogate);
    }
    let mut retained = vec![first];
    retained.extend(runtime.block_on(snapshot.try_collect::<Vec<_>>()).unwrap());
    let mut actual = BTreeMap::new();
    for batch in retained {
        for row in 0..batch.num_rows() {
            assert!(
                actual
                    .insert(uuid_at(&batch, 0, row), int_at(&batch, 1, row))
                    .is_none()
            );
        }
    }
    assert_eq!(actual, expected_snapshot);
    verify_graph(&graph, fixture, &nodes, &edges);
    let latest_parquet = publishing_parquet_inventory(&source);
    assert!(latest_parquet["parquet_bytes"].as_u64().unwrap() <= 1024 * 1024);
    cas_uuid_assert_parent_objects(&source, &parent_objects);
    drop(graph);
    round_trip(root.path(), &source, fixture, &nodes, &edges);
}

fn facade_compaction_fault_request() -> graphforge_storage::GraphDeltaCompactionRequest {
    graphforge_storage::GraphDeltaCompactionRequest {
        transaction_uuid: Uuid::from_u128(0x1231_001),
        generation_uuid: Uuid::from_u128(0x1231_002),
        through_run_sequence: None,
        limits: graphforge_storage::GraphDeltaCompactionLimits::default(),
        cleanup_after_commit: false,
        cleanup_policy: graphforge_storage::ProjectRetentionPolicy::default(),
        cleanup_limits: graphforge_storage::ProjectRetentionLimits::default(),
    }
}

#[test]
fn facade_compaction_fault_child() {
    use futures::TryStreamExt as _;
    let Ok(source) = std::env::var("GF_FACADE_COMPACTION_ROOT") else {
        return;
    };
    let mut graph = GraphForge::new(Some(&source)).unwrap();
    let query = "MATCH (n) RETURN n.node_uuid, n.score ORDER BY n.node_uuid";
    let exact_rows = |batches: Vec<RecordBatch>| {
        batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows()).map(|row| (uuid_at(batch, 0, row), int_at(batch, 1, row)))
            })
            .collect::<Vec<_>>()
    };
    let before = exact_rows(graph.execute(query).unwrap().batches);
    let stream = graph.execute_stream(query).unwrap();
    let error = graph
        .compact_graph_delta(&facade_compaction_fault_request(), None)
        .unwrap_err();
    assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
    assert_eq!(exact_rows(graph.execute(query).unwrap().batches), before);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert_eq!(
        exact_rows(runtime.block_on(stream.try_collect::<Vec<_>>()).unwrap()),
        before
    );
    if graphforge_storage::resolve_project_generation(Path::new(&source))
        .unwrap()
        .generation_uuid()
        == facade_compaction_fault_request().generation_uuid
    {
        assert!(
            graph
                .compact_graph_delta(&facade_compaction_fault_request(), None)
                .unwrap()
                .publication
                .unwrap()
                .idempotent_replay
        );
    }
    // This transaction is outside the injected operation's scope. Success
    // proves the same facade selected the committed compaction, when present.
    graph.execute("MATCH (n:Node0) SET n.score = 31").unwrap();
    graph.execute("CREATE (:Node0 {score: 9001})").unwrap();
}

#[test]
fn facade_compaction_faults_preserve_authority_and_allow_mutation() {
    for boundary in [
        "project.before_current_replace",
        "project.after_current_replace",
    ] {
        for returned_error in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let source = root.path().join("source");
            let fixture = Fixture {
                name: "facade_compaction_recovery",
                nodes: 33,
                edges: 129,
                routes: 2,
                identifiers: Identifiers::Random,
                properties: true,
                adjacency: false,
                heterogeneous: false,
            };
            let (mut nodes, edges) = rows(fixture);
            construct(&source, fixture, &nodes, &edges);
            let graph = GraphForge::new(source.to_str()).unwrap();
            graph
                .publish_composite_transaction(graphforge_api::CompositeTransactionRequest {
                    contract_version: graphforge_api::COMPOSITE_TRANSACTION_CONTRACT_VERSION,
                    context: graphforge_api::WriteContext {
                        operation_uuid: OperationId(Uuid::now_v7()),
                        actor_uuid: None,
                    },
                    graph_mutations: vec![
                        graphforge_api::CompositeGraphMutation::SetNodeProperty {
                            node_uuid: nodes[0].0,
                            property: "score".into(),
                            value: graphforge_api::PropValue::Int(9),
                        },
                    ],
                    knowledge: graphforge_api::CompositeKnowledgeParticipants::default(),
                })
                .unwrap();
            nodes[0].2 = Some(9);
            drop(graph);
            let parent = graphforge_storage::resolve_project_generation(&source)
                .unwrap()
                .generation_uuid();
            let objects = cas_uuid_parent_objects(&source);
            let hook = format!("{boundary}{}", if returned_error { ".error" } else { "" });
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "facade_compaction_fault_child", "--nocapture"])
                .env("GF_FACADE_COMPACTION_ROOT", &source)
                .env(
                    "GRAPHFORGE_PROJECT_FAILPOINTS",
                    "graphforge-internal-subprocess-v1",
                )
                .env("GRAPHFORGE_PROJECT_FAILPOINT", &hook)
                .env(
                    "GRAPHFORGE_PROJECT_FAILPOINT_TRANSACTION",
                    facade_compaction_fault_request()
                        .transaction_uuid
                        .to_string(),
                )
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(if returned_error { 0 } else { 86 }),
                "{hook}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let mut graph = GraphForge::new(source.to_str()).unwrap();
            if returned_error {
                for node in &mut nodes {
                    if node.1 == "Node0" {
                        node.2 = Some(31);
                    }
                }
                let created = graph
                    .execute("MATCH (n:Node0 {score: 9001}) RETURN n.node_uuid")
                    .unwrap();
                nodes.push((
                    uuid_at(&created.batches[0], 0, 0),
                    "Node0".into(),
                    Some(9001),
                ));
            } else {
                assert_eq!(
                    graphforge_storage::resolve_project_generation(&source)
                        .unwrap()
                        .generation_uuid()
                        != parent,
                    boundary == "project.after_current_replace"
                );
                for _ in 0..2 {
                    graph
                        .compact_graph_delta(&facade_compaction_fault_request(), None)
                        .unwrap();
                }
            }
            verify_graph(&graph, fixture, &nodes, &edges);
            cas_uuid_assert_parent_objects(&source, &objects);
            drop(graph);
            round_trip(root.path(), &source, fixture, &nodes, &edges);
        }
    }
}

// Both physical property owners contribute only by exact edge UUID. Keep the
// named traversal oracle independent of wildcard reads and publication routing.
fn verify_named_edge_property_owners(graph: &GraphForge, edges: &[Edge]) {
    for relation in ["REL0", "REL1"] {
        let mut paths = Vec::new();
        let query =
            format!("MATCH (a)-[r:{relation}*1..1]->(b) RETURN r, a.node_uuid, b.node_uuid");
        for batch in graph
            .execute(&query)
            .unwrap_or_else(|error| panic!("{query}: {error}"))
            .batches
        {
            let lists = batch
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let path = lists.value(row);
                let edge = path
                    .as_any()
                    .downcast_ref::<arrow::array::StructArray>()
                    .unwrap();
                assert_eq!(edge.len(), 1);
                let uuid = edge
                    .column_by_name("edge_uuid")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                let weight = edge
                    .column_by_name("weight")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let text = edge.column_by_name("text").unwrap();
                let text = if text.logical_nulls().is_some_and(|nulls| nulls.is_null(0)) {
                    None
                } else if let Some(values) = text.as_any().downcast_ref::<StringArray>() {
                    Some(values.value(0).to_owned())
                } else {
                    Some(
                        text.as_any()
                            .downcast_ref::<arrow::array::StringViewArray>()
                            .expect("typed string property")
                            .value(0)
                            .to_owned(),
                    )
                };
                paths.push((
                    Uuid::from_slice(uuid.value(0)).unwrap(),
                    uuid_at(&batch, 1, row),
                    uuid_at(&batch, 2, row),
                    relation.to_owned(),
                    (!weight.is_null(0)).then(|| weight.value(0)),
                    text,
                ));
            }
        }
        let mut expected = edges
            .iter()
            .filter(|edge| edge.3 == relation)
            .cloned()
            .collect::<Vec<_>>();
        paths.sort();
        expected.sort();
        assert_eq!(paths, expected, "{query}");
        for (pattern, edge, filtered) in [
            (format!("MATCH (a)-[r:{relation}]->(b)"), "r", false),
            (
                format!("MATCH (a)-[r:{relation}]->(b) WITH a, r, b MATCH (a)-[r:{relation}]->(b)"),
                "r",
                false,
            ),
            (
                format!(
                    "MATCH (a)-[r:{relation}]->(b) WHERE r.weight IS NOT NULL AND r.text IS NULL"
                ),
                "r",
                true,
            ),
        ] {
            let query = format!(
                "{pattern} RETURN {edge}.edge_uuid, a.node_uuid, b.node_uuid, {edge}.weight, {edge}.text"
            );
            let mut actual = Vec::new();
            for batch in graph
                .execute(&query)
                .unwrap_or_else(|error| panic!("{query}: {error}"))
                .batches
            {
                for row in 0..batch.num_rows() {
                    let text = batch.column(4);
                    let text = if text.logical_nulls().is_some_and(|nulls| nulls.is_null(row)) {
                        None
                    } else if let Some(values) = text.as_any().downcast_ref::<StringArray>() {
                        Some(values.value(row).to_owned())
                    } else {
                        Some(
                            text.as_any()
                                .downcast_ref::<arrow::array::StringViewArray>()
                                .expect("typed string property")
                                .value(row)
                                .to_owned(),
                        )
                    };
                    actual.push((
                        uuid_at(&batch, 0, row),
                        uuid_at(&batch, 1, row),
                        uuid_at(&batch, 2, row),
                        relation.to_owned(),
                        int_at(&batch, 3, row),
                        text,
                    ));
                }
            }
            let mut expected = edges
                .iter()
                .filter(|edge| {
                    edge.3 == relation && (!filtered || (edge.4.is_some() && edge.5.is_none()))
                })
                .cloned()
                .collect::<Vec<_>>();
            actual.sort();
            expected.sort();
            assert_eq!(actual, expected, "{query}");
        }
    }
}

#[test]
fn composite_constructed_edge_properties_preserve_authenticated_owner() {
    use futures::TryStreamExt;
    for node_count in [33, 4097] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let fixture = Fixture {
            name: "constructed_edge_property_owner",
            nodes: node_count,
            edges: 129,
            routes: 2,
            identifiers: Identifiers::Random,
            properties: true,
            adjacency: false,
            heterogeneous: false,
        };
        let (mut nodes, mut edges) = rows(fixture);
        construct(&source, fixture, &nodes, &edges);
        let mut graph = GraphForge::new(source.to_str()).unwrap();
        verify_named_edge_property_owners(&graph, &edges);
        for with_properties in [true, false] {
            let properties = if with_properties {
                " {weight: 3, text: 'ordinary'}"
            } else {
                ""
            };
            let created = graph.execute(&format!(
                "CREATE (a:Node0 {{score: 1}})-[r:REL0{properties}]->(b:Node1 {{score: 2}}) RETURN a.node_uuid, b.node_uuid, r.edge_uuid"
            )).unwrap();
            let batch = &created.batches[0];
            let source_uuid = uuid_at(batch, 0, 0);
            let target_uuid = uuid_at(batch, 1, 0);
            nodes.push((source_uuid, "Node0".into(), Some(1)));
            nodes.push((target_uuid, "Node1".into(), Some(2)));
            edges.push((
                uuid_at(batch, 2, 0),
                source_uuid,
                target_uuid,
                "REL0".into(),
                with_properties.then_some(3),
                with_properties.then(|| "ordinary".into()),
            ));
        }
        graph
            .execute("MATCH (a)-[r]->(b) RETURN r.edge_uuid, r.weight, r.text")
            .expect("mixed construction/ordinary properties before composite mutation");
        verify_named_edge_property_owners(&graph, &edges);
        let ordinary = edges[129].0;
        let empty = edges[130].0;
        graph
            .publish_composite_transaction(graphforge_api::CompositeTransactionRequest {
                contract_version: graphforge_api::COMPOSITE_TRANSACTION_CONTRACT_VERSION,
                context: graphforge_api::WriteContext {
                    operation_uuid: OperationId(Uuid::now_v7()),
                    actor_uuid: None,
                },
                graph_mutations: vec![
                    graphforge_api::CompositeGraphMutation::SetEdgeProperty {
                        edge_uuid: ordinary,
                        property: "weight".into(),
                        value: graphforge_api::PropValue::Int(23),
                    },
                    graphforge_api::CompositeGraphMutation::RemoveEdgeProperty {
                        edge_uuid: ordinary,
                        property: "text".into(),
                    },
                    graphforge_api::CompositeGraphMutation::SetEdgeProperty {
                        edge_uuid: empty,
                        property: "weight".into(),
                        value: graphforge_api::PropValue::Int(29),
                    },
                    graphforge_api::CompositeGraphMutation::SetEdgeProperty {
                        edge_uuid: edges[0].0,
                        property: "weight".into(),
                        value: graphforge_api::PropValue::Int(19),
                    },
                    graphforge_api::CompositeGraphMutation::RemoveEdgeProperty {
                        edge_uuid: edges[1].0,
                        property: "text".into(),
                    },
                ],
                knowledge: graphforge_api::CompositeKnowledgeParticipants::default(),
            })
            .unwrap();
        edges[129].4 = Some(23);
        edges[129].5 = None;
        edges[130].4 = Some(29);
        edges[0].4 = Some(19);
        edges[1].5 = None;
        verify_graph(&graph, fixture, &nodes, &edges);
        verify_named_edge_property_owners(&graph, &edges);
        let published = publishing_parquet_inventory(&source);
        assert!(
            published["parquet_bytes"].as_u64().unwrap() <= 1024 * 1024,
            "{published}"
        );
        assert!(
            published["parquet_allocated_bytes"].as_u64().unwrap() <= 2 * 1024 * 1024,
            "{published}"
        );
        let mut path_values = graph
            .execute("MATCH (a)-[r*1..1]->(b) RETURN r")
            .unwrap()
            .batches
            .into_iter()
            .flat_map(|batch| {
                let paths = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|row| {
                        let path = paths.value(row);
                        let edge = path
                            .as_any()
                            .downcast_ref::<arrow::array::StructArray>()
                            .unwrap();
                        assert_eq!(edge.len(), 1);
                        let uuid = edge
                            .column_by_name("edge_uuid")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<FixedSizeBinaryArray>()
                            .unwrap();
                        let weight = edge
                            .column_by_name("weight")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        let text = edge
                            .column_by_name("text")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        (
                            Uuid::from_slice(uuid.value(0)).unwrap(),
                            (!weight.is_null(0)).then(|| weight.value(0)),
                            (!text.is_null(0)).then(|| text.value(0).to_owned()),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        path_values.sort();
        let mut expected_values = edges
            .iter()
            .map(|edge| (edge.0, edge.4, edge.5.clone()))
            .collect::<Vec<_>>();
        expected_values.sort();
        assert_eq!(path_values, expected_values);
        if node_count == 4097 {
            let snapshot = graph
                .execute_stream("MATCH (a)-[r]->(b) RETURN r.edge_uuid, r.weight")
                .unwrap();
            let report = graph
                .compact_graph_delta(
                    &graphforge_storage::GraphDeltaCompactionRequest {
                        transaction_uuid: Uuid::now_v7(),
                        generation_uuid: Uuid::now_v7(),
                        through_run_sequence: None,
                        limits: graphforge_storage::GraphDeltaCompactionLimits::default(),
                        cleanup_after_commit: false,
                        cleanup_policy: graphforge_storage::ProjectRetentionPolicy::default(),
                        cleanup_limits: graphforge_storage::ProjectRetentionLimits::default(),
                    },
                    None,
                )
                .unwrap();
            assert!(report.output_bytes <= 1024 * 1024, "{report:?}");
            assert!(report.peak_memory_bytes <= 16 * 1024, "{report:?}");
            assert_eq!(report.spill_bytes, 0);
            let compacted = publishing_parquet_inventory(&source);
            assert!(
                compacted["parquet_bytes"].as_u64().unwrap() <= 1024 * 1024,
                "{compacted}"
            );
            assert!(
                compacted["parquet_allocated_bytes"].as_u64().unwrap() <= 2 * 1024 * 1024,
                "{compacted}"
            );
            verify_graph(&graph, fixture, &nodes, &edges);
            verify_named_edge_property_owners(&graph, &edges);
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let mut snapshot = snapshot;
            let mut old = BTreeMap::new();
            while let Some(batch) = runtime.block_on(snapshot.try_next()).unwrap() {
                for row in 0..batch.num_rows() {
                    assert!(
                        old.insert(uuid_at(&batch, 0, row), int_at(&batch, 1, row))
                            .is_none()
                    );
                }
            }
            assert_eq!(old, edges.iter().map(|edge| (edge.0, edge.4)).collect());
        }
        drop(graph);
        round_trip_checked(root.path(), &source, |graph| {
            verify_named_edge_property_owners(graph, &edges);
            verify_graph(graph, fixture, &nodes, &edges)
        });
        let imported_path = root.path().join("imported");
        let imported = GraphForge::new(imported_path.to_str()).unwrap();
        let request = graphforge_api::CompositeTransactionRequest {
            contract_version: graphforge_api::COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: graphforge_api::WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            graph_mutations: vec![
                graphforge_api::CompositeGraphMutation::SetEdgeProperty {
                    edge_uuid: edges[0].0,
                    property: "weight".into(),
                    value: graphforge_api::PropValue::Int(31),
                },
                graphforge_api::CompositeGraphMutation::RemoveEdgeProperty {
                    edge_uuid: edges[130].0,
                    property: "weight".into(),
                },
            ],
            knowledge: graphforge_api::CompositeKnowledgeParticipants::default(),
        };
        imported
            .publish_composite_transaction(request.clone())
            .unwrap();
        let published = graphforge_storage::resolve_project_generation(&imported_path)
            .unwrap()
            .generation_uuid();
        imported.publish_composite_transaction(request).unwrap();
        assert_eq!(
            graphforge_storage::resolve_project_generation(&imported_path)
                .unwrap()
                .generation_uuid(),
            published
        );
        edges[0].4 = Some(31);
        edges[130].4 = None;
        verify_graph(&imported, fixture, &nodes, &edges);
        verify_named_edge_property_owners(&imported, &edges);
        drop(imported);
        verify_graph(
            &GraphForge::new(imported_path.to_str()).unwrap(),
            fixture,
            &nodes,
            &edges,
        );
        verify_named_edge_property_owners(
            &GraphForge::new(imported_path.to_str()).unwrap(),
            &edges,
        );
    }
}

#[test]
fn composite_property_owner_scope_rejects_unnamed_public_edges() {
    let graph = GraphForge::new(None).unwrap();
    let error = graph
        .execute("CREATE (:Node)-[r {weight: 1}]->(:Node) RETURN r.edge_uuid")
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("a created relationship must have exactly one type")
    );
    let count = graph.execute("MATCH (n) RETURN count(n)").unwrap();
    assert_eq!(int_at(&count.batches[0], 0, 0), Some(0));
}

#[test]
fn named_edge_property_owners_refuse_incompatible_concrete_types() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let fixture = Fixture {
        name: "named_owner_type_conflict",
        nodes: 3,
        edges: 2,
        routes: 1,
        identifiers: Identifiers::Random,
        properties: true,
        adjacency: false,
        heterogeneous: false,
    };
    let (nodes, edges) = rows(fixture);
    construct(&source, fixture, &nodes, &edges);
    let graph = GraphForge::new(source.to_str()).unwrap();
    graph
        .execute("CREATE (:Node0)-[:REL0 {weight: 'incompatible'}]->(:Node0)")
        .unwrap();
    for query in [
        "MATCH ()-[r:REL0]->() RETURN r.weight",
        "MATCH ()-[r:REL0*1..1]->() RETURN r",
        "MATCH (a)-[r:REL0]->(b) WITH a, r, b MATCH (a)-[r:REL0]->(b) RETURN r.weight",
    ] {
        assert!(
            graph.execute(query).is_err(),
            "incompatible concrete owners must not be coerced: {query}"
        );
    }
}

#[test]
fn edge_property_union_preserves_null_positions_and_concrete_type_errors() {
    for removed_first in [0, 1] {
        let graph = GraphForge::new(None).unwrap();
        let mut ids = Vec::new();
        for relation in ["A", "z"] {
            let result = graph
                .execute(&format!(
                    "CREATE (:Node)-[r:{relation} {{value: 'kept'}}]->(:Node) RETURN r.edge_uuid"
                ))
                .unwrap();
            ids.push(uuid_at(&result.batches[0], 0, 0));
        }
        for removed in [removed_first, 1 - removed_first] {
            graph
                .publish_composite_transaction(graphforge_api::CompositeTransactionRequest {
                    contract_version: graphforge_api::COMPOSITE_TRANSACTION_CONTRACT_VERSION,
                    context: graphforge_api::WriteContext {
                        operation_uuid: OperationId(Uuid::now_v7()),
                        actor_uuid: None,
                    },
                    graph_mutations: vec![
                        graphforge_api::CompositeGraphMutation::RemoveEdgeProperty {
                            edge_uuid: ids[removed],
                            property: "value".into(),
                        },
                    ],
                    knowledge: graphforge_api::CompositeKnowledgeParticipants::default(),
                })
                .unwrap();
            let all_null = removed != removed_first;
            let result = graph
                .execute("MATCH ()-[r]->() RETURN r.edge_uuid, r.value")
                .unwrap();
            let mut observed = BTreeMap::new();
            for batch in result.batches {
                for row in 0..batch.num_rows() {
                    let uuid = uuid_at(&batch, 0, row);
                    let expected_null = all_null || uuid == ids[removed_first];
                    assert_eq!(
                        batch
                            .column(1)
                            .logical_nulls()
                            .is_some_and(|nulls| nulls.is_null(row)),
                        expected_null,
                        "removed_first={removed_first} removed={removed} type={:?} values={:?}",
                        batch.column(1).data_type(),
                        batch.column(1)
                    );
                    observed.insert(uuid, expected_null);
                }
            }
            assert_eq!(observed.len(), 2);
            let result = graph.execute("MATCH ()-[r*1..1]->() RETURN r").unwrap();
            let mut paths = BTreeMap::new();
            for batch in result.batches {
                let lists = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();
                for row in 0..lists.len() {
                    let path = lists.value(row);
                    let edge = path
                        .as_any()
                        .downcast_ref::<arrow::array::StructArray>()
                        .unwrap();
                    let uuid = edge
                        .column_by_name("edge_uuid")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .unwrap();
                    let value = edge.column_by_name("value");
                    assert_eq!(
                        value.is_none(),
                        all_null,
                        "relationship structs omit properties with no remaining live owner"
                    );
                    paths.insert(
                        Uuid::from_slice(uuid.value(0)).unwrap(),
                        value.is_none_or(|value| {
                            value.logical_nulls().is_some_and(|nulls| nulls.is_null(0))
                        }),
                    );
                }
            }
            assert_eq!(paths, observed);
        }
    }
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute("CREATE (:Node)-[:A {value: 1}]->(:Node)")
        .unwrap();
    graph
        .execute("CREATE (:Node)-[:z {value: 'text'}]->(:Node)")
        .unwrap();
    for query in [
        "MATCH ()-[r]->() RETURN r.value",
        "MATCH ()-[r*1..1]->() RETURN r",
    ] {
        assert!(
            graph.execute(query).is_err(),
            "incompatible concrete types must remain an error: {query}"
        );
    }
}

fn promotion_request(root: &Path) -> graphforge_api::AdoptOntologyRequest {
    let ontology = root.join("promotion.yaml");
    std::fs::write(&ontology, "ontology_id: https://example.test/promotion\nversion: \"1\"\nentity_types:\n  - name: Node0\n    abstract: false\nrelation_types:\n  - name: REL0\n    src: Node0\n    dst: Node0\nproperties:\n  - owner: Node0\n    name: score\n    type: int64\n    nullable: true\n  - owner: REL0\n    name: weight\n    type: int64\n    nullable: true\n  - owner: REL0\n    name: text\n    type: utf8\n    nullable: true\n").unwrap();
    graphforge_api::AdoptOntologyRequest {
        context: graphforge_api::WriteContext {
            operation_uuid: OperationId(Uuid::from_u128(1229001)),
            actor_uuid: None,
        },
        path: ontology,
        mode: graphforge_api::OntologyMode::Advisory,
    }
}

fn verify_promoted_graph(graph: &GraphForge, nodes: &[Node], edges: &[Edge]) {
    verify_promoted_route(graph, nodes, edges, "Node0", "REL0");
}

fn verify_promoted_route(
    graph: &GraphForge,
    nodes: &[Node],
    edges: &[Edge],
    label: &str,
    route: &str,
) {
    let mut actual = graph
        .execute(&format!("MATCH (n:{label}) RETURN n.node_uuid, n.score"))
        .unwrap()
        .batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows())
                .map(|row| (uuid_at(batch, 0, row), int_at(batch, 1, row)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut expected = nodes.iter().map(|n| (n.0, n.2)).collect::<Vec<_>>();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
    let mut actual_edges = Vec::new();
    for batch in graph.execute(&format!("MATCH (a)-[r:{route}]->(b) RETURN r.edge_uuid, a.node_uuid, b.node_uuid, r.weight, r.text")).unwrap_or_else(|error| panic!("route={route}: {error}")).batches {
        for row in 0..batch.num_rows() {
            let column = batch.column(4);
            let text = if column.data_type() == &DataType::Null || column.is_null(row) { None } else if let Some(text) = column.as_any().downcast_ref::<StringArray>() {
                Some(text.value(row).to_owned())
            } else {
                Some(column.as_any().downcast_ref::<arrow::array::StringViewArray>().expect("text must be an Arrow string").value(row).to_owned())
            };
            actual_edges.push((uuid_at(&batch, 0, row), uuid_at(&batch, 1, row), uuid_at(&batch, 2, row), route.to_owned(), int_at(&batch, 3, row), text));
        }
    }
    actual_edges.sort();
    let mut expected_edges = edges.to_vec();
    expected_edges.sort();
    assert_eq!(actual_edges, expected_edges);
}

#[test]
fn same_name_adoption_publishes_complete_constructed_authority() {
    use futures::TryStreamExt as _;
    for node_count in [33, 4097] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let fixture = Fixture {
            name: "same_name_promotion",
            nodes: node_count,
            edges: 129,
            routes: 1,
            identifiers: Identifiers::Random,
            properties: true,
            adjacency: false,
            heterogeneous: false,
        };
        let (mut nodes, mut edges) = rows(fixture);
        construct(&source, fixture, &nodes, &edges);
        let base_ids = cas_uuid_node_surrogates(&source);
        let objects = cas_uuid_parent_objects(&source);
        let mut graph = GraphForge::new(source.to_str()).unwrap();
        let query = "MATCH (n) RETURN n.node_uuid, n.score ORDER BY n.node_uuid";
        let expected_snapshot = graph.execute(query).unwrap();
        let stream = graph.execute_stream(query).unwrap();
        let mut request = promotion_request(root.path());
        if node_count == 4097 {
            request.mode = graphforge_api::OntologyMode::Strict;
        }
        graph.adopt_ontology(request.clone()).unwrap();
        verify_promoted_graph(&graph, &nodes, &edges);
        assert_eq!(cas_uuid_node_surrogates(&source), base_ids);
        let selected = graphforge_storage::resolve_project_generation(&source)
            .unwrap()
            .generation_uuid();
        graph.adopt_ontology(request).unwrap();
        assert_eq!(
            graphforge_storage::resolve_project_generation(&source)
                .unwrap()
                .generation_uuid(),
            selected
        );
        verify_promoted_graph(&graph, &nodes, &edges);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let snapshot = runtime.block_on(stream.try_collect::<Vec<_>>()).unwrap();
        let snapshot_rows = |batches: Vec<RecordBatch>| {
            batches
                .iter()
                .flat_map(|batch| {
                    (0..batch.num_rows())
                        .map(|row| (uuid_at(batch, 0, row), int_at(batch, 1, row)))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            snapshot_rows(snapshot),
            snapshot_rows(expected_snapshot.batches)
        );
        cas_uuid_assert_parent_objects(&source, &objects);
        let result = graph
            .execute("CREATE (n:Node0 {score: 9001229}) RETURN n.node_uuid")
            .unwrap();
        let new_uuid = uuid_at(&result.batches[0], 0, 0);
        assert!(cas_uuid_node_surrogates(&source)[&new_uuid] > *base_ids.values().max().unwrap());
        nodes.push((new_uuid, "Node0".into(), Some(9001229)));
        verify_promoted_graph(&graph, &nodes, &edges);
        drop(graph);
        let graph = GraphForge::new(source.to_str()).unwrap();
        verify_promoted_graph(&graph, &nodes, &edges);
        let package = root.path().join("promotion.gfpb");
        let limits = PortableV2Limits::default();
        let exported = graph
            .export_portable_v2(
                &PortableV2ExportRequest {
                    selection: PortableSelection::Current,
                    output_path: package.clone(),
                    representation: PortableV2Output::Bundle,
                    profile: PortableV2SelectionProfile::Complete,
                    subset: None,
                    limits,
                },
                None,
                |_| {},
            )
            .unwrap();
        let verified = verify_portable_v2(
            &PortableVerifyRequest {
                input: package.clone(),
                mode: PortableV2Mode::Full,
                limits,
            },
            None,
        )
        .unwrap();
        assert_eq!(exported.package_digest, verified.package_digest);
        let imported = root.path().join("imported");
        GraphForge::import_portable_v2(
            &imported,
            &PortableV2ImportRequest {
                input: package,
                operation_id: OperationId(Uuid::from_u128(1229002)),
                limits,
            },
            None,
        )
        .unwrap();
        let graph = GraphForge::new(imported.to_str()).unwrap();
        verify_promoted_graph(&graph, &nodes, &edges);
        graph
            .execute("MATCH (n:Node0 {score: 9001229}) SET n.score = 9001230")
            .unwrap();
        nodes.last_mut().unwrap().2 = Some(9001230);
        verify_promoted_graph(&graph, &nodes, &edges);
        graph
            .publish_composite_transaction(graphforge_api::CompositeTransactionRequest {
                contract_version: graphforge_api::COMPOSITE_TRANSACTION_CONTRACT_VERSION,
                context: graphforge_api::WriteContext {
                    operation_uuid: OperationId(Uuid::now_v7()),
                    actor_uuid: None,
                },
                graph_mutations: vec![
                    graphforge_api::CompositeGraphMutation::SetEdgeProperty {
                        edge_uuid: edges[0].0,
                        property: "weight".into(),
                        value: graphforge_api::PropValue::Int(1229),
                    },
                    graphforge_api::CompositeGraphMutation::RemoveEdgeProperty {
                        edge_uuid: edges[1].0,
                        property: "weight".into(),
                    },
                ],
                knowledge: graphforge_api::CompositeKnowledgeParticipants::default(),
            })
            .unwrap();
        edges[0].4 = Some(1229);
        edges[1].4 = None;
        verify_promoted_graph(&graph, &nodes, &edges);
        drop(graph);
        let graph = GraphForge::new(imported.to_str()).unwrap();
        verify_promoted_graph(&graph, &nodes, &edges);
    }
}

#[test]
fn ontology_promotion_fault_child() {
    use futures::TryStreamExt as _;
    let Ok(source) = std::env::var("GF_ONTOLOGY_PROMOTION_ROOT") else {
        return;
    };
    let source = Path::new(&source);
    let request = promotion_request(source.parent().unwrap());
    let mut graph = GraphForge::new(source.to_str()).unwrap();
    let query = "MATCH (n) RETURN n.node_uuid, n.score ORDER BY n.node_uuid";
    let before = graph.execute(query).unwrap();
    let stream = graph.execute_stream(query).unwrap();
    let error = graph.adopt_ontology(request.clone()).unwrap_err();
    assert_eq!(error.code(), "GF_PUBLICATION_FAILED", "{error}");
    let selected = graphforge_storage::resolve_project_generation(source).unwrap();
    let selected_query = if selected.transaction_uuid() == request.context.operation_uuid.0 {
        "MATCH (n:Node0) RETURN n.node_uuid, n.score ORDER BY n.node_uuid"
    } else {
        query
    };
    let exact = |batches: Vec<RecordBatch>| {
        batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows())
                    .map(|row| (uuid_at(batch, 0, row), int_at(batch, 1, row)))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        exact(graph.execute(selected_query).unwrap().batches),
        exact(before.batches.clone())
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert_eq!(
        exact(runtime.block_on(stream.try_collect::<Vec<_>>()).unwrap()),
        exact(before.batches)
    );
    if selected.transaction_uuid() == request.context.operation_uuid.0 {
        graph.adopt_ontology(request).unwrap();
    }
    graph.execute("CREATE (:Node0 {score: 9001229})").unwrap();
}

#[test]
fn ontology_promotion_faults_select_complete_authority_and_retry() {
    let executable = tempfile::tempdir().unwrap();
    let frozen = executable.path().join("promotion-fault-tests");
    std::fs::copy(std::env::current_exe().unwrap(), &frozen).unwrap();
    for boundary in [
        "project.before_current_replace",
        "project.after_current_replace",
    ] {
        for returned_error in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let source = root.path().join("source");
            let fixture = Fixture {
                name: "promotion_fault",
                nodes: 33,
                edges: 129,
                routes: 1,
                identifiers: Identifiers::Random,
                properties: true,
                adjacency: false,
                heterogeneous: false,
            };
            let (mut nodes, edges) = rows(fixture);
            construct(&source, fixture, &nodes, &edges);
            let prior = graphforge_storage::resolve_project_generation(&source)
                .unwrap()
                .generation_uuid();
            let request = promotion_request(root.path());
            let hook = format!("{boundary}{}", if returned_error { ".error" } else { "" });
            let output = std::process::Command::new(&frozen)
                .args(["--exact", "ontology_promotion_fault_child", "--nocapture"])
                .env("GF_ONTOLOGY_PROMOTION_ROOT", &source)
                .env(
                    "GRAPHFORGE_PROJECT_FAILPOINTS",
                    "graphforge-internal-subprocess-v1",
                )
                .env("GRAPHFORGE_PROJECT_FAILPOINT", &hook)
                .env(
                    "GRAPHFORGE_PROJECT_FAILPOINT_TRANSACTION",
                    request.context.operation_uuid.0.to_string(),
                )
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(if returned_error { 0 } else { 86 }),
                "{hook}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let mut graph = GraphForge::new(source.to_str()).unwrap();
            let current = graphforge_storage::resolve_project_generation(&source).unwrap();
            if !returned_error && boundary == "project.before_current_replace" {
                assert_eq!(current.generation_uuid(), prior);
                verify_graph(&graph, fixture, &nodes, &edges);
            }
            if returned_error {
                let batches = graph
                    .execute("MATCH (n:Node0 {score: 9001229}) RETURN n.node_uuid")
                    .unwrap()
                    .batches;
                let uuid = uuid_at(&batches[0], 0, 0);
                nodes.push((uuid, "Node0".into(), Some(9001229)));
            }
            graph.adopt_ontology(request).unwrap();
            verify_promoted_graph(&graph, &nodes, &edges);
        }
    }
}

#[test]
fn ontology_promotion_cancellation_and_conflict_preserve_prior_view() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let fixture = Fixture {
        name: "promotion_cancel",
        nodes: 33,
        edges: 129,
        routes: 1,
        identifiers: Identifiers::Random,
        properties: true,
        adjacency: false,
        heterogeneous: false,
    };
    let (nodes, edges) = rows(fixture);
    construct(&source, fixture, &nodes, &edges);
    let mut graph = GraphForge::new(source.to_str()).unwrap();
    let request = promotion_request(root.path());
    let before = clear_publication_files(&source);
    let cancellation = graphforge_api::CancellationToken::new();
    cancellation.cancel();
    assert!(
        graph
            .adopt_ontology_cancellable(request.clone(), Some(&cancellation))
            .is_err()
    );
    assert_eq!(clear_publication_files(&source), before);
    verify_graph(&graph, fixture, &nodes, &edges);
    let other = GraphForge::new(source.to_str()).unwrap();
    other.execute("CREATE (:Node0 {score: 9001229})").unwrap();
    let current = graphforge_storage::resolve_project_generation(&source)
        .unwrap()
        .generation_uuid();
    assert!(graph.adopt_ontology(request).is_err());
    assert_eq!(
        graphforge_storage::resolve_project_generation(&source)
            .unwrap()
            .generation_uuid(),
        current
    );
    verify_graph(&graph, fixture, &nodes, &edges);
}

#[test]
fn ontology_promotion_preserves_mixed_routes_and_reencoding_scope() {
    for (node_count, edge_count, routes) in [(33, 129, 2), (4097, 4097, 4)] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let fixture = Fixture {
            name: "promotion_mixed",
            nodes: node_count,
            edges: edge_count,
            routes,
            identifiers: Identifiers::Random,
            properties: true,
            adjacency: false,
            heterogeneous: false,
        };
        let (nodes, edges) = rows(fixture);
        let verify = |graph: &GraphForge| {
            for route in 0..routes {
                let label = format!("Node{route}");
                let relation = format!("REL{route}");
                verify_promoted_route(
                    graph,
                    &nodes
                        .iter()
                        .filter(|n| n.1 == label)
                        .cloned()
                        .collect::<Vec<_>>(),
                    &edges
                        .iter()
                        .filter(|e| e.3 == relation)
                        .cloned()
                        .collect::<Vec<_>>(),
                    &label,
                    &relation,
                );
            }
            Sha256::digest(serde_json::to_vec(&(&nodes, &edges)).unwrap())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        construct(&source, fixture, &nodes, &edges);
        let before = publishing_parquet_inventory(&source);
        let old_objects = cas_uuid_parent_objects(&source);
        let mut graph = GraphForge::new(source.to_str()).unwrap();
        let request = promotion_request(root.path());
        let started = Instant::now();
        graph.adopt_ontology(request.clone()).unwrap();
        let elapsed_ns = started.elapsed().as_nanos();
        verify(&graph);
        cas_uuid_assert_parent_objects(&source, &old_objects);
        let after = publishing_parquet_inventory(&source);
        let files = after["files"].as_array().unwrap();
        // Fixed-width identity/property payloads plus bounded shard framing.
        // This is a representative encoding ceiling, not a process-memory bound.
        let payload_limit =
            node_count as u64 * 64 + edge_count as u64 * 128 + files.len() as u64 * 8192 + 65536;
        assert!(after["parquet_bytes"].as_u64().unwrap() <= payload_limit);
        assert!(
            after["parquet_allocated_bytes"].as_u64().unwrap()
                <= after["parquet_bytes"].as_u64().unwrap() + files.len() as u64 * 4096
        );
        let changed = files
            .iter()
            .filter(|file| {
                !before["files"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|old| old["path"] == file["path"] && old["sha256"] == file["sha256"])
            })
            .collect::<Vec<_>>();
        let changed_bytes = changed
            .iter()
            .map(|file| file["bytes"].as_u64().unwrap())
            .sum::<u64>();
        println!(
            "ONTOLOGY_PROMOTION {}",
            json!({
                "nodes":node_count,"edges":edge_count,"routes":routes,
                "promotion_ns":elapsed_ns,"before":before,"after":after,
                "changed_parquet_files":changed.len(),"changed_parquet_bytes":changed_bytes,
                "parquet_payload_limit_bytes":payload_limit,
            })
        );
        // A second authored operation on the already promoted mixed graph must
        // preserve exact semantics and avoid repeating property ownership moves.
        let mut again = request;
        again.context.operation_uuid = OperationId(Uuid::from_u128(1229010));
        graph.adopt_ontology(again).unwrap();
        verify(&graph);
        assert_eq!(
            publishing_parquet_inventory(&source),
            after,
            "already promoted routes must not be reencoded"
        );
        drop(graph);
        round_trip_checked(root.path(), &source, verify);
    }
}

#[test]
fn ontology_promotion_bounds_changed_control_inventory() {
    for (node_count, edge_count, routes) in [(33, 129, 2), (4097, 4097, 4)] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let fixture = Fixture {
            name: "promotion_controls",
            nodes: node_count,
            edges: edge_count,
            routes,
            identifiers: Identifiers::Random,
            properties: true,
            adjacency: false,
            heterogeneous: false,
        };
        let (nodes, edges) = rows(fixture);
        construct(&source, fixture, &nodes, &edges);
        let inventory = || {
            graphforge_storage::resolve_project_generation(&source)
                .unwrap()
                .graph_files_inventory()
                .unwrap()
                .unwrap()
        };
        let before = inventory();
        let mut graph = GraphForge::new(source.to_str()).unwrap();
        graph
            .adopt_ontology(promotion_request(root.path()))
            .unwrap();
        let after = inventory();
        let controls = after
            .files
            .iter()
            .filter(|entry| {
                !entry.relative_path.ends_with(".parquet")
                    && !before.files.iter().any(|old| {
                        old.relative_path == entry.relative_path
                            && old.content_sha256 == entry.content_sha256
                    })
            })
            .collect::<Vec<_>>();
        let bytes = controls.iter().map(|entry| entry.byte_length).sum::<u64>();
        // Identity and ordinal records remain full-width; allow their complete
        // representation plus bounded control framing, without quadratic growth.
        let limit = (node_count + edge_count) as u64 * 80 + controls.len() as u64 * 1024 + 65536;
        assert!(bytes <= limit, "changed controls {bytes} exceed {limit}");
        println!(
            "ONTOLOGY_PROMOTION_CONTROLS {}",
            json!({
                "nodes":node_count,"edges":edge_count,"routes":routes,
                "changed_control_bytes":bytes,"changed_control_files":controls.len(),
                "control_byte_limit":limit,
                "controls":controls.iter().map(|entry| json!({"path":entry.relative_path,"bytes":entry.byte_length})).collect::<Vec<_>>()
            })
        );
    }
}

fn publishing_contract_verify(graph: &GraphForge, nodes: &[Node], edges: &[Edge]) -> String {
    for (query, expected) in [
        ("MATCH (n) RETURN count(n)", nodes.len()),
        ("MATCH ()-[r]->() RETURN count(r)", edges.len()),
    ] {
        let result = graph.execute(query).unwrap();
        assert_eq!(int_at(&result.batches[0], 0, 0), Some(expected as i64));
    }
    for route in 0..2 {
        let label = format!("Node{route}");
        let relation = format!("REL{route}");
        verify_promoted_route(
            graph,
            &nodes
                .iter()
                .filter(|n| n.1 == label)
                .cloned()
                .collect::<Vec<_>>(),
            &edges
                .iter()
                .filter(|e| e.3 == relation)
                .cloned()
                .collect::<Vec<_>>(),
            &label,
            &relation,
        );
    }
    digest_hex(&serde_json::to_vec(&(nodes, edges)).unwrap())
}

#[test]
fn publishing_contract_alternates_supported_paths_and_refuses_topology_journals() {
    exercise_publishing_contract(33, false);
}
#[test]
fn publishing_contract_flat_ontology() {
    exercise_publishing_contract(33, true);
}
#[test]
fn publishing_contract_sharded_exploratory() {
    exercise_publishing_contract(4097, false);
}
#[test]
fn publishing_contract_sharded_ontology() {
    exercise_publishing_contract(4097, true);
}

fn exercise_publishing_contract(count: usize, typed: bool) {
    use graphforge_api::{
        COMPOSITE_TRANSACTION_CONTRACT_VERSION, CompositeGraphMutation as Mutation,
        CompositeKnowledgeParticipants, CompositeTransactionRequest, PropValue, WriteContext,
    };
    use graphforge_storage::{
        GraphDeltaCompactionLimits, GraphDeltaCompactionRequest, GraphDeltaOp, GraphDeltaOpKind,
        GraphDeltaPayload, GraphDeltaPublishRequest, ProjectRetentionLimits,
        ProjectRetentionPolicy,
    };
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let fixture = Fixture {
        name: "publishing_contract",
        nodes: count,
        edges: 129,
        routes: 2,
        identifiers: Identifiers::Random,
        properties: true,
        adjacency: false,
        heterogeneous: false,
    };
    let (mut nodes, mut edges) = rows(fixture);
    if count > 4096 {
        construct(&source, fixture, &nodes, &edges);
    } else {
        // Empty-project composite CREATE publishes flat topology; construction
        // sessions publish shards even when the input fits in a single shard.
        // Composite CREATE requires UUIDv7. Preserve deterministic fixture bytes
        // apart from the version/variant bits, including every endpoint.
        let uuid7 = |uuid: Uuid| {
            let mut bytes = *uuid.as_bytes();
            bytes[6] = (bytes[6] & 0x0f) | 0x70;
            bytes[8] = (bytes[8] & 0x3f) | 0x80;
            Uuid::from_bytes(bytes)
        };
        for node in &mut nodes {
            node.0 = uuid7(node.0);
        }
        for edge in &mut edges {
            edge.0 = uuid7(edge.0);
            edge.1 = uuid7(edge.1);
            edge.2 = uuid7(edge.2);
        }
        let graph = GraphForge::new(source.to_str()).unwrap();
        let graph_mutations = nodes
            .iter()
            .map(|(uuid, label, score)| Mutation::CreateNode {
                node_uuid: *uuid,
                label: label.clone(),
                properties: score
                    .map(|value| ("score".into(), PropValue::Int(value)))
                    .into_iter()
                    .collect(),
            })
            .chain(
                edges
                    .iter()
                    .map(
                        |(uuid, source, target, route, weight, text)| Mutation::CreateEdge {
                            edge_uuid: *uuid,
                            source_uuid: *source,
                            target_uuid: *target,
                            rel_type: route.clone(),
                            properties: weight
                                .map(|value| ("weight".into(), PropValue::Int(value)))
                                .into_iter()
                                .chain(
                                    text.clone()
                                        .map(|value| ("text".into(), PropValue::Str(value))),
                                )
                                .collect(),
                        },
                    ),
            )
            .collect();
        graph
            .publish_composite_transaction(CompositeTransactionRequest {
                contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
                context: WriteContext {
                    operation_uuid: OperationId(Uuid::now_v7()),
                    actor_uuid: None,
                },
                graph_mutations,
                knowledge: CompositeKnowledgeParticipants::default(),
            })
            .unwrap();
    }
    let mut graph = GraphForge::new(source.to_str()).unwrap();
    if typed {
        graph
            .adopt_ontology(promotion_request(root.path()))
            .unwrap();
    }
    let initial = graphforge_storage::resolve_project_generation(&source).unwrap();
    let inventory = initial.graph_files_inventory().unwrap().unwrap();
    let node_shards = inventory
        .files
        .iter()
        .filter(|entry| entry.relative_path.starts_with("topology/nodes/"))
        .count();
    if count > 4096 {
        assert!(
            node_shards > 1,
            "large fixture must use sharded node authority"
        );
    } else {
        assert_eq!(node_shards, 0, "small fixture must use flat node authority");
        assert!(
            inventory
                .files
                .iter()
                .any(|entry| entry.relative_path == "topology/nodes.parquet")
        );
    }
    publishing_contract_verify(&graph, &nodes, &edges);
    let initial_ids = cas_uuid_node_surrogates(&source);
    let initial_edge_ids = publishing_edge_surrogates(&source);
    let before = clear_publication_files(&source);
    for payload in [
        GraphDeltaPayload::UpsertNodeV2 {
            node_uuid: nodes[0].0.to_string(),
            node_id: initial_ids[&nodes[0].0],
            type_ids: vec![],
            created_at_micros: 1,
            updated_at_micros: 2,
        },
        GraphDeltaPayload::UpsertEdgeV2 {
            edge_uuid: edges[0].0.to_string(),
            src_uuid: nodes[0].0.to_string(),
            dst_uuid: Uuid::now_v7().to_string(),
            rel_type: "REL1".into(),
            edge_id: 1,
            src_id: initial_ids[&nodes[0].0],
            dst_id: u64::MAX,
            created_at_micros: 1,
        },
        GraphDeltaPayload::DeleteNode {
            node_uuid: nodes[0].0.to_string(),
        },
        GraphDeltaPayload::DeleteEdge {
            edge_uuid: edges[0].0.to_string(),
        },
    ] {
        let kind = match payload {
            GraphDeltaPayload::UpsertNodeV2 { .. } => GraphDeltaOpKind::UpsertNode,
            GraphDeltaPayload::UpsertEdgeV2 { .. } => GraphDeltaOpKind::UpsertEdge,
            GraphDeltaPayload::DeleteNode { .. } => GraphDeltaOpKind::DeleteNode,
            _ => GraphDeltaOpKind::DeleteEdge,
        };
        let error = graphforge_storage::publish_graph_delta(
            &source,
            &GraphDeltaPublishRequest {
                transaction_uuid: Uuid::now_v7(),
                generation_uuid: Uuid::now_v7(),
                run_uuid: Uuid::now_v7(),
                operations: vec![GraphDeltaOp {
                    operation_uuid: Uuid::now_v7(),
                    kind,
                    payload,
                }],
                limits: Default::default(),
            },
        )
        .unwrap_err();
        assert_eq!(error.code(), "GF_UNSUPPORTED_PROJECT_FORMAT");
        assert_eq!(clear_publication_files(&source), before);
    }
    graph
        .publish_composite_transaction(CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            graph_mutations: vec![
                Mutation::SetNodeProperty {
                    node_uuid: nodes[0].0,
                    property: "score".into(),
                    value: PropValue::Int(1221),
                },
                Mutation::RemoveNodeProperty {
                    node_uuid: nodes[1].0,
                    property: "score".into(),
                },
                Mutation::SetEdgeProperty {
                    edge_uuid: edges[0].0,
                    property: "weight".into(),
                    value: PropValue::Int(1221),
                },
                Mutation::RemoveEdgeProperty {
                    edge_uuid: edges[1].0,
                    property: "text".into(),
                },
            ],
            knowledge: CompositeKnowledgeParticipants::default(),
        })
        .unwrap();
    assert!(
        graphforge_storage::resolve_project_generation(&source)
            .unwrap()
            .graph_files_inventory()
            .unwrap()
            .unwrap()
            .files
            .iter()
            .any(|entry| entry.relative_path.starts_with("deltas/")),
        "fixture must exercise actual GFDR"
    );
    nodes[0].2 = Some(1221);
    nodes[1].2 = None;
    edges[0].4 = Some(1221);
    edges[1].5 = None;
    publishing_contract_verify(&graph, &nodes, &edges);
    let report = graph
        .compact_graph_delta(
            &GraphDeltaCompactionRequest {
                transaction_uuid: Uuid::now_v7(),
                generation_uuid: Uuid::now_v7(),
                through_run_sequence: None,
                limits: GraphDeltaCompactionLimits::default(),
                cleanup_after_commit: false,
                cleanup_policy: ProjectRetentionPolicy::default(),
                cleanup_limits: ProjectRetentionLimits::default(),
            },
            None,
        )
        .unwrap();
    assert!(report.output_bytes <= 2 * 1024 * 1024);
    assert!(
        report.peak_memory_bytes <= 8 * 1024,
        "logical replay accounting: {report:?}"
    );
    publishing_contract_verify(&graph, &nodes, &edges);
    let created = graph
        .execute("CREATE (n:Node0 {score: 91221}) RETURN n.node_uuid")
        .unwrap();
    let retired_uuid = uuid_at(&created.batches[0], 0, 0);
    let retired_id = cas_uuid_node_surrogates(&source)[&retired_uuid];
    graph
        .execute("MATCH (n:Node0 {score: 91221}) DELETE n")
        .unwrap();
    drop(graph);
    let graph = GraphForge::new(source.to_str()).unwrap();
    let created = graph
        .execute("CREATE (n:Node0 {score: 92221}) RETURN n.node_uuid")
        .unwrap();
    let created_uuid = uuid_at(&created.batches[0], 0, 0);
    nodes.push((created_uuid, "Node0".into(), Some(92221)));
    let ids = cas_uuid_node_surrogates(&source);
    assert!(retired_id > *initial_ids.values().max().unwrap());
    assert!(ids[&created_uuid] > retired_id);
    for (uuid, id) in &initial_ids {
        assert_eq!(ids[uuid], *id);
    }
    let edge_query = "MATCH (a:Node0 {score: 1221}), (b:Node0 {score: 92221}) CREATE (a)-[e:REL0 {weight: 1221001}]->(b) RETURN e.edge_uuid";
    let result = graph.execute(edge_query).unwrap();
    assert_eq!(result.stats.rows_produced, 1);
    let retired_edge = uuid_at(&result.batches[0], 0, 0);
    let retired_edge_id = publishing_edge_surrogates(&source)[&retired_edge];
    graph
        .execute("MATCH ()-[e:REL0 {weight: 1221001}]->() DELETE e")
        .unwrap();
    drop(graph);
    let graph = GraphForge::new(source.to_str()).unwrap();
    let result = graph
        .execute(&edge_query.replace("1221001", "1221002"))
        .unwrap();
    let added_edge = uuid_at(&result.batches[0], 0, 0);
    edges.push((
        added_edge,
        nodes[0].0,
        created_uuid,
        "REL0".into(),
        Some(1221002),
        None,
    ));
    let edge_ids = publishing_edge_surrogates(&source);
    assert!(retired_edge_id > *initial_edge_ids.values().max().unwrap());
    assert!(edge_ids[&added_edge] > retired_edge_id);
    for (uuid, id) in &initial_edge_ids {
        assert_eq!(edge_ids[uuid], *id);
    }
    publishing_contract_verify(&graph, &nodes, &edges);
    drop(graph);
    round_trip_checked(root.path(), &source, |graph| {
        publishing_contract_verify(graph, &nodes, &edges)
    });
    let imported_path = root.path().join("imported");
    let imported = GraphForge::new(imported_path.to_str()).unwrap();
    let imported_before = cas_uuid_node_surrogates(&imported_path);
    assert_eq!(imported_before, ids);
    let created = imported
        .execute("CREATE (n:Node0 {score: 93221}) RETURN n.node_uuid")
        .unwrap();
    let created_uuid = uuid_at(&created.batches[0], 0, 0);
    nodes.push((created_uuid, "Node0".into(), Some(93221)));
    assert!(cas_uuid_node_surrogates(&imported_path)[&created_uuid] > *ids.values().max().unwrap());
    assert_eq!(publishing_edge_surrogates(&imported_path), edge_ids);
    let result = imported.execute("MATCH (a:Node0 {score: 1221}), (b:Node0 {score: 93221}) CREATE (a)-[e:REL0 {weight: 1221003}]->(b) RETURN e.edge_uuid").unwrap();
    let added_edge = uuid_at(&result.batches[0], 0, 0);
    edges.push((
        added_edge,
        nodes[0].0,
        created_uuid,
        "REL0".into(),
        Some(1221003),
        None,
    ));
    assert!(
        publishing_edge_surrogates(&imported_path)[&added_edge] > *edge_ids.values().max().unwrap()
    );
    publishing_contract_verify(&imported, &nodes, &edges);
    drop(imported);
    publishing_contract_verify(
        &GraphForge::new(imported_path.to_str()).unwrap(),
        &nodes,
        &edges,
    );
    println!(
        "PUBLISHING_CONTRACT {}",
        json!({"nodes":count,"typed":typed,"routes":2,"compaction_output_bytes":report.output_bytes,"logical_replay_peak_bytes":report.peak_memory_bytes})
    );
}

fn publishing_edge_surrogates(source: &Path) -> BTreeMap<Uuid, u64> {
    use arrow::array::UInt64Array;
    let selected = graphforge_storage::resolve_project_generation(source).unwrap();
    let inventory = selected.graph_files_inventory().unwrap().unwrap();
    let owned = selected.declared_graph_files_inventory().unwrap().is_some();
    let mut ids = BTreeMap::new();
    for entry in inventory.files.iter().filter(|entry| {
        entry.relative_path.starts_with("topology/edges/")
            && entry.relative_path.ends_with(".parquet")
    }) {
        let path = if owned {
            selected.graph_tree_root().join(&entry.relative_path)
        } else {
            graphforge_storage::graph_object_path(source, &entry.content_sha256).unwrap()
        };
        for batch in ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap()
        {
            let batch = batch.unwrap();
            let uuids = batch
                .column_by_name("edge_uuid")
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            let values = batch
                .column_by_name("edge_id")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert!(
                    ids.insert(
                        Uuid::from_slice(uuids.value(row)).unwrap(),
                        values.value(row)
                    )
                    .is_none()
                );
            }
        }
    }
    ids
}
