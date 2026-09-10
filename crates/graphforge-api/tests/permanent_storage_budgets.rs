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

fn construct(path: &Path, f: Fixture, nodes: &[Node], edges: &[Edge]) {
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
    let mut runs = 0_u64;
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
        let original = std::fs::read(
            graphforge_storage::graph_object_path(source, &entry.content_sha256).unwrap(),
        )
        .unwrap();
        let packed = pack_identity_records(&original);
        let mut restored = Vec::with_capacity(original.len());
        for record in packed.chunks_exact(25) {
            restored.extend_from_slice(&record[..17]);
            restored.extend_from_slice(&[0; 7]);
            restored.extend_from_slice(&record[17..25]);
        }
        assert_eq!(restored, original);
        // Fixed-width binary search retains exact UUID/kind ordering. Exercise
        // beginning, midpoint and last identifiers rather than surrogate-only keys.
        let packed_rows = packed.chunks_exact(25).collect::<Vec<_>>();
        for row in [
            0,
            packed_rows.len() / 2,
            packed_rows.len().saturating_sub(1),
        ] {
            if packed_rows.is_empty() {
                continue;
            }
            let key = &original[row * 32..row * 32 + 17];
            let found = packed_rows
                .binary_search_by(|record| record[..17].cmp(key))
                .unwrap();
            assert_eq!(
                &packed_rows[found][17..25],
                &original[row * 32 + 24..row * 32 + 32]
            );
        }
        records += (original.len() / 32) as u64;
        runs += 1;
    }
    assert!(records > 0);
    json!({"identity_runs":runs,"records":records,"current_record_bytes":records * 32,
        "packed_record_bytes":records * 25,"removable_reserved_padding_bytes":records * 7,
        "production_format_changed":false})
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
