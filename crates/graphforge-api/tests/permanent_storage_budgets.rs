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
    (!batch.column(column).is_null(row)).then(|| {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
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
            let text = if f.properties && !batch.column(5).is_null(row) {
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
    let graph = GraphForge::new(source.to_str()).unwrap();
    let fingerprint = verify_graph(&graph, f, nodes, edges);
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
    assert_eq!(verify_graph(&imported, f, nodes, edges), fingerprint);
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
            "leaf" => {}
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
        "production_format_changed":false})
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
    let fingerprint = round_trip(root.path(), &source, f, &nodes, &edges);
    println!(
        "PERMANENT_STORAGE_ASSESSMENT {}",
        json!({"fixture":f.name,"nodes":f.nodes,"edges":f.edges,"routes":f.routes,"random_ids":matches!(f.identifiers, Identifiers::Random),"properties":f.properties,"heterogeneous_schemas":f.heterogeneous,"adjacency_built":f.adjacency,"unindexed_allocated_bytes":unindexed.allocated_bytes,"unindexed_categories":unindexed.categories,
            "construction_elapsed_ns":construction_ns,"semantic_fingerprint":fingerprint,
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

    let graph = GraphForge::new(source.to_str()).unwrap();
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
    let graph = GraphForge::new_with_options(source.to_str(), options).unwrap();
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
    use graphforge_api::{
        COMPOSITE_TRANSACTION_CONTRACT_VERSION, CompositeGraphMutation as Mutation,
        CompositeKnowledgeParticipants, CompositeTransactionRequest, PropValue, WriteContext,
    };
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    drop(GraphForge::new(source.to_str()).unwrap());
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
    let fixture = Fixture {
        name: "qualified_composite_edge",
        nodes: 2,
        edges: 1,
        routes: 1,
        identifiers: Identifiers::Random,
        properties: false,
        adjacency: false,
        heterogeneous: false,
    };
    let nodes = [
        (a, "mixed:entity:NewNode".into(), None),
        (b, "mixed:entity:NewNode".into(), None),
    ];
    let edges = [(edge, a, b, relation.route.clone(), None, None)];
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
    let graph = GraphForge::new(source.to_str()).unwrap();
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
    let graph = GraphForge::new(source.to_str()).unwrap();
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
    assert_eq!(int_at(&expected_score.batches[0], 1, 0), Some(123));
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
        payload += entry.byte_length;
        allocated += physical;
        files.push(json!({"path":entry.relative_path,"bytes":entry.byte_length,
            "allocated_bytes":physical,"row_groups":groups, "edge_id_order":edge_order}));
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
    let graph = GraphForge::new(source.to_str()).unwrap();
    verify_graph(&graph, fixture, &nodes, &edges);
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
