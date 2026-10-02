//! Admission for the current bounded, single-batch CSR Arrow IPC format.

use std::io::Read as _;
use std::path::Path;

use arrow::ipc::{CompressionType, MetadataVersion};
use graphforge_core::GfError;

use super::{DEFAULT_CSR_SHARD_EDGES, DEFAULT_CSR_SHARD_NODES, storage_err};

// The fixed schema needs far less metadata. This is an admission limit, not an
// estimate of Arrow's parser or allocator overhead.
const METADATA_MAX_BYTES: usize = 16 * 1024;

/// A derived-index object that is present but fails its own integrity checks:
/// a checksum or length that disagrees with the manifest that names it, or
/// bytes that do not decode as what that manifest declares. This is never a
/// reason to rebuild. A rebuild would answer the query by scanning the edge
/// table, so the bounded query silently costs O(E) and the damage is never
/// reported. It surfaces as `GF_VALIDATION`, like every other refusal of a
/// corrupted graph payload.
pub(super) fn corrupt_index(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

/// Re-label a structural failure of an index object as corruption. Admission
/// code reports a malformed encoding as a storage error because a writer
/// producing one is a defect; a reader that meets one in bytes the manifest
/// names has found damage.
pub(super) fn corrupt_structure(error: GfError) -> GfError {
    match error {
        GfError::Storage(message) => corrupt_index(message),
        other => other,
    }
}

/// An I/O failure reading a shard the manifest declares. A declared shard that
/// is absent is damage (hydration and the builders guarantee presence); any
/// other failure is a genuine I/O error and stays one.
pub(super) fn shard_io_error(file: &str, error: &std::io::Error) -> GfError {
    if error.kind() == std::io::ErrorKind::NotFound {
        corrupt_index(format!("missing CSR shard {file}"))
    } else {
        GfError::Storage(format!("read CSR shard {file}: {error}"))
    }
}

pub(super) fn corrupt_index_from(error: impl std::fmt::Display) -> GfError {
    corrupt_index(format!("adjacency index object does not decode: {error}"))
}

/// Read a shard manifest. It is the authority for every shard checksum, so it
/// is admitted against the project's own manifest first: a flipped byte is
/// refused there, not read as a damaged or stale index.
pub(super) fn read_admitted_manifest(path: &Path) -> Result<Vec<u8>, GfError> {
    let mut file = crate::graph_admission::open_admitted(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(storage_err)?;
    Ok(bytes)
}

pub(super) fn shard_set_identity(
    node_count: u64,
    edge_count: u64,
    records: &[super::CsrShardRecord],
) -> String {
    use graphforge_core::hash_observation::ContractSha256;
    use sha2::Digest as _;
    use std::fmt::Write as _;

    let mut identity = ContractSha256::new();
    identity.update(b"graphforge/csr-shards/v2\0");
    identity.update(node_count.to_le_bytes());
    identity.update(edge_count.to_le_bytes());
    for record in records {
        identity.update(record.first_node.to_le_bytes());
        identity.update(record.node_count.to_le_bytes());
        identity.update(record.edge_count.to_le_bytes());
        identity.update(record.sha256.as_bytes());
    }
    ContractSha256::digest(identity.finalize()).iter().fold(
        String::with_capacity(64),
        |mut output, byte| {
            let _ = write!(output, "{byte:02x}");
            output
        },
    )
}

pub(super) fn decode_manifest(
    bytes: &[u8],
    path: &Path,
) -> Result<super::CsrShardManifest, GfError> {
    #[derive(serde::Deserialize)]
    struct Header {
        version: u32,
    }
    // The shard manifest is written whole and published by rename, so bytes
    // that do not parse as one are damaged, not a partial write.
    let header: Header = serde_json::from_slice(bytes)
        .map_err(|error| manifest_parse_error(error, path, bytes.len()))?;
    if header.version != super::SHARDED_CSR_VERSION {
        return Err(GfError::Storage(format!(
            "unsupported sharded CSR format version {}; recreate the index ({})",
            header.version,
            path.display()
        )));
    }
    let manifest: super::CsrShardManifest = serde_json::from_slice(bytes)
        .map_err(|error| manifest_parse_error(error, path, bytes.len()))?;
    if manifest.format != "graphforge.csr-shards" {
        return Err(GfError::Storage(format!(
            "unsupported sharded CSR manifest {}",
            path.display()
        )));
    }
    Ok(manifest)
}

fn manifest_parse_error(error: impl std::fmt::Display, path: &Path, bytes: usize) -> GfError {
    corrupt_index(format!(
        "CSR shard manifest {} ({} bytes) does not parse: {error}",
        path.display(),
        bytes
    ))
}

fn invalid() -> GfError {
    GfError::Storage("invalid or oversized current CSR shard encoding".into())
}

fn buffer_lengths(nodes: u64, edges: u64) -> Result<[u64; 7], GfError> {
    if nodes > DEFAULT_CSR_SHARD_NODES as u64 || edges > DEFAULT_CSR_SHARD_EDGES as u64 {
        return Err(invalid());
    }
    Ok([
        nodes.div_ceil(8),
        8 * (nodes + 1),
        edges.div_ceil(8),
        edges.div_ceil(8),
        8 * edges,
        edges.div_ceil(8),
        8 * edges,
    ])
}

pub(super) fn decoded_bytes(nodes: u64, edges: u64) -> Result<u64, GfError> {
    Ok(buffer_lengths(nodes, edges)?.iter().sum())
}

pub(super) fn encoded_limit(nodes: u64, edges: u64) -> Result<u64, GfError> {
    // Arrow stores a raw buffer if compression would expand it. Include IPC
    // prefixes, alignment, schema/message/footer and end markers separately.
    Ok(decoded_bytes(nodes, edges)? + METADATA_MAX_BYTES as u64)
}

/// Admission for a freshly encoded shard before any byte reaches the
/// filesystem: the same bound [`read`] enforces on the way back in.
pub(super) fn admit_encoded_len(bytes_len: u64, nodes: u64, edges: u64) -> Result<(), GfError> {
    if bytes_len > encoded_limit(nodes, edges)? {
        return Err(invalid());
    }
    Ok(())
}

/// Read one shard whose manifest declares `encoded_bytes`. Every disagreement
/// between the file and its declaration is corruption ([`corrupt_index`]); only
/// an I/O failure other than "not found" is a storage error.
pub(super) fn read(
    path: &Path,
    file_name: &str,
    encoded_bytes: u64,
    limit: u64,
) -> Result<Vec<u8>, GfError> {
    if encoded_bytes > limit {
        return Err(corrupt_index(format!(
            "CSR shard {file_name} declares more bytes than its encoding admits"
        )));
    }
    let mut file = std::fs::File::open(path).map_err(|error| shard_io_error(file_name, &error))?;
    let length_mismatch = || {
        corrupt_index(format!(
            "CSR shard {file_name} length does not match its manifest"
        ))
    };
    if file
        .metadata()
        .map_err(|error| shard_io_error(file_name, &error))?
        .len()
        != encoded_bytes
    {
        return Err(length_mismatch());
    }
    let mut bytes = vec![0; usize::try_from(encoded_bytes).map_err(storage_err)?];
    file.read_exact(&mut bytes).map_err(|error| {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            length_mismatch()
        } else {
            shard_io_error(file_name, &error)
        }
    })?;
    let mut trailing = [0_u8; 1];
    if file
        .read(&mut trailing)
        .map_err(|error| shard_io_error(file_name, &error))?
        != 0
    {
        return Err(length_mismatch());
    }
    Ok(bytes)
}

fn index(value: i64) -> Result<usize, GfError> {
    usize::try_from(value).map_err(storage_err)
}

fn slice(bytes: &[u8], offset: usize, len: usize) -> Result<&[u8], GfError> {
    bytes
        .get(offset..offset.checked_add(len).ok_or_else(invalid)?)
        .ok_or_else(invalid)
}

fn validate_zstd_frame(payload: &[u8], expected: u64) -> Result<(), GfError> {
    if payload.get(..4) != Some(&[0x28, 0xb5, 0x2f, 0xfd])
        || zstd::zstd_safe::find_frame_compressed_size(payload).map_err(storage_err)?
            != payload.len()
        || zstd::zstd_safe::get_frame_content_size(payload).map_err(storage_err)? != Some(expected)
    {
        return Err(invalid());
    }
    Ok(())
}

fn validate_schema(schema: arrow::ipc::Schema<'_>) -> Result<(), GfError> {
    if schema.endianness() != arrow::ipc::Endianness::Little {
        return Err(invalid());
    }
    let fields = schema.fields().ok_or_else(invalid)?;
    if fields.len() != 1 {
        return Err(invalid());
    }
    let adjacency = fields.get(0);
    validate_field(adjacency, "adjacency")?;
    if adjacency.type_as_large_list().is_none() {
        return Err(invalid());
    }
    let items = adjacency.children().ok_or_else(invalid)?;
    if items.len() != 1 {
        return Err(invalid());
    }
    let item = items.get(0);
    validate_field(item, "item")?;
    if item.type_as_struct_().is_none() {
        return Err(invalid());
    }
    let entries = item.children().ok_or_else(invalid)?;
    if entries.len() != 2 {
        return Err(invalid());
    }
    for (field, name) in entries.iter().zip(["edge_id", "neighbor_id"]) {
        validate_field(field, name)?;
        let integer = field.type_as_int().ok_or_else(invalid)?;
        if integer.bitWidth() != 64
            || integer.is_signed()
            || field
                .children()
                .is_some_and(|children| !children.is_empty())
        {
            return Err(invalid());
        }
    }
    Ok(())
}

fn validate_field(field: arrow::ipc::Field<'_>, name: &str) -> Result<(), GfError> {
    if field.name() != Some(name) || field.nullable() || field.dictionary().is_some() {
        return Err(invalid());
    }
    Ok(())
}

/// Check every allocation-bearing declaration before Arrow constructs arrays.
pub(super) fn preflight(bytes: &[u8], nodes: u64, edges: u64) -> Result<(), GfError> {
    let lengths = buffer_lengths(nodes, edges)?;
    if bytes.len() as u64 > encoded_limit(nodes, edges)? || bytes.get(..6) != Some(b"ARROW1") {
        return Err(invalid());
    }
    let trailer = bytes.len().checked_sub(10).ok_or_else(invalid)?;
    let footer_len =
        arrow::ipc::reader::read_footer_length(bytes[trailer..].try_into().map_err(storage_err)?)
            .map_err(storage_err)?;
    if footer_len > METADATA_MAX_BYTES {
        return Err(invalid());
    }
    let footer_start = trailer.checked_sub(footer_len).ok_or_else(invalid)?;
    let footer = arrow::ipc::root_as_footer(&bytes[footer_start..trailer]).map_err(storage_err)?;
    if footer.version() != MetadataVersion::V5
        || footer.dictionaries().is_some_and(|d| !d.is_empty())
    {
        return Err(invalid());
    }
    validate_schema(footer.schema().ok_or_else(invalid)?)?;
    let batches = footer.recordBatches().ok_or_else(invalid)?;
    if batches.len() != 1 {
        return Err(invalid());
    }
    let block = batches.get(0);
    let offset = index(block.offset())?;
    let metadata_len = usize::try_from(block.metaDataLength()).map_err(storage_err)?;
    let body_len = index(block.bodyLength())?;
    if metadata_len > METADATA_MAX_BYTES || offset < 8 {
        return Err(invalid());
    }
    let metadata = slice(&bytes[..footer_start], offset, metadata_len)?;
    if metadata.get(..4) != Some(&[255; 4]) {
        return Err(invalid());
    }
    let declared = i32::from_le_bytes(slice(metadata, 4, 4)?.try_into().map_err(storage_err)?);
    if usize::try_from(declared).map_err(storage_err)?
        != metadata_len.checked_sub(8).ok_or_else(invalid)?
    {
        return Err(invalid());
    }
    let message = arrow::ipc::root_as_message(&metadata[8..]).map_err(storage_err)?;
    if message.version() != MetadataVersion::V5 || index(message.bodyLength())? != body_len {
        return Err(invalid());
    }
    let batch = message.header_as_record_batch().ok_or_else(invalid)?;
    let compression = batch.compression().ok_or_else(invalid)?;
    if compression.codec() != CompressionType::ZSTD
        || compression.method() != arrow::ipc::BodyCompressionMethod::BUFFER
        || index(batch.length())? as u64 != nodes
    {
        return Err(invalid());
    }
    let fields = batch.nodes().ok_or_else(invalid)?;
    if fields.len() != 4 {
        return Err(invalid());
    }
    for (field, expected) in fields.iter().zip([nodes, edges, edges, edges]) {
        if index(field.length())? as u64 != expected || field.null_count() != 0 {
            return Err(invalid());
        }
    }
    let buffers = batch.buffers().ok_or_else(invalid)?;
    if buffers.len() != lengths.len() {
        return Err(invalid());
    }
    let body = slice(
        &bytes[..footer_start],
        offset.checked_add(metadata_len).ok_or_else(invalid)?,
        body_len,
    )?;
    let mut prior_end = 0;
    for (buffer, expected) in buffers.iter().zip(lengths) {
        let start = index(buffer.offset())?;
        let len = index(buffer.length())?;
        if start < prior_end {
            return Err(invalid());
        }
        let encoded = slice(body, start, len)?;
        prior_end = start.checked_add(len).ok_or_else(invalid)?;
        if expected == 0 && encoded.is_empty() {
            continue;
        }
        let declared = i64::from_le_bytes(slice(encoded, 0, 8)?.try_into().map_err(storage_err)?);
        let payload = &encoded[8..];
        if declared == -1 {
            if payload.len() as u64 != expected {
                return Err(invalid());
            }
        } else {
            if u64::try_from(declared).map_err(storage_err)? != expected {
                return Err(invalid());
            }
            validate_zstd_frame(payload, expected)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_set_identity_accounts_descriptor_contract_and_preserves_digest_bytes() {
        use graphforge_core::hash_observation::operation::{Capture, Snapshot};
        use sha2::{Digest, Sha256};
        let record = super::super::CsrShardRecord {
            first_node: 2,
            node_count: 3,
            edge_count: 4,
            file: "shard.csr".into(),
            sha256: "ab".repeat(32),
            xxh64: 0,
            encoded_bytes: 128,
            decoded_bytes: 64,
        };
        let mut preimage = b"graphforge/csr-shards/v2\0".to_vec();
        for value in [5_u64, 4, 2, 3, 4] {
            preimage.extend_from_slice(&value.to_le_bytes());
        }
        preimage.extend_from_slice(record.sha256.as_bytes());
        let expected = Sha256::digest(Sha256::digest(&preimage))
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let capture = Capture::start();
        let actual = shard_set_identity(5, 4, std::slice::from_ref(&record));
        assert_eq!(actual, expected);
        assert_eq!(
            capture.snapshot(),
            Snapshot {
                contract_identity_sha256_bytes: preimage.len() as u64 + 32,
                ..Snapshot::default()
            }
        );
        drop(capture);
        assert_ne!(actual, shard_set_identity(6, 4, &[record]));
    }

    #[test]
    fn checksum_csr_manifest_refuses_legacy_missing_and_malformed_metadata() {
        use super::super::{CsrIndex, ShardedCsrIndex, write_sharded_csr};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.csr");
        let csr = CsrIndex {
            offsets: vec![0, 1],
            edge_ids: vec![0],
            neighbor_ids: vec![0],
        };
        write_sharded_csr(&path, &csr, 1).unwrap();
        let manifest_path = path.with_extension("csr.json");
        let original: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        for mode in 0..4 {
            let mut changed = original.clone();
            match mode {
                0 => {
                    changed["version"] = serde_json::json!(2);
                    for shard in changed["shards"].as_array_mut().unwrap() {
                        shard.as_object_mut().unwrap().remove("xxh64");
                    }
                }
                1 => changed["version"] = serde_json::json!(4),
                2 => {
                    changed["shards"][0]
                        .as_object_mut()
                        .unwrap()
                        .remove("xxh64");
                }
                _ => changed["shards"][0]["xxh64"] = serde_json::json!("not-a-checksum"),
            }
            std::fs::write(&manifest_path, serde_json::to_vec(&changed).unwrap()).unwrap();
            let error = ShardedCsrIndex::open(&path).unwrap_err().to_string();
            assert_eq!(
                error.contains("unsupported sharded CSR format version"),
                mode < 2,
                "mode={mode}: {error}"
            );
            if mode < 2 {
                assert!(error.contains("recreate the index"), "{error}");
            }
        }
        std::fs::write(&manifest_path, serde_json::to_vec(&original).unwrap()).unwrap();
        ShardedCsrIndex::open(&path).unwrap();
    }

    fn fixture() -> (tempfile::TempDir, std::path::PathBuf, Vec<u8>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shard.csr");
        let csr = super::super::CsrIndex {
            offsets: vec![0, 128],
            edge_ids: vec![u64::MAX; 128],
            neighbor_ids: vec![u64::MAX - 1; 128],
        };
        let bytes = super::super::encode_csr_shard_bytes(&csr).unwrap();
        super::super::write_csr_shard_bytes(&path, &bytes).unwrap();
        (dir, path, bytes)
    }

    fn first_buffer_start(bytes: &[u8]) -> usize {
        let trailer = bytes.len() - 10;
        let len =
            arrow::ipc::reader::read_footer_length(bytes[trailer..].try_into().unwrap()).unwrap();
        let footer = arrow::ipc::root_as_footer(&bytes[trailer - len..trailer]).unwrap();
        let block = footer.recordBatches().unwrap().get(0);
        usize::try_from(block.offset()).unwrap() + usize::try_from(block.metaDataLength()).unwrap()
    }

    #[test]
    fn current_writer_passes_exact_predecode_admission() {
        let (_dir, path, bytes) = fixture();
        preflight(&bytes, 1, 128).unwrap();
        assert_eq!(
            read(
                &path,
                "shard.csr",
                bytes.len() as u64,
                encoded_limit(1, 128).unwrap()
            )
            .unwrap(),
            bytes
        );
        assert!(preflight(&bytes, 2, 128).is_err());
        assert!(preflight(&bytes, 1, 127).is_err());
        assert!(decoded_bytes(u64::MAX, 1).is_err());
        assert!(decoded_bytes(1, u64::MAX).is_err());
    }

    #[test]
    fn size_bomb_is_refused_before_arrow_decoding() {
        let (_dir, path, mut bytes) = fixture();
        let start = first_buffer_start(&bytes);
        bytes[start..start + 8].copy_from_slice(&i64::MAX.to_le_bytes());
        assert!(preflight(&bytes, 1, 128).is_err());
        assert!(read(&path, "shard.csr", u64::MAX, encoded_limit(1, 128).unwrap()).is_err());
        assert!(
            read(
                &path,
                "shard.csr",
                bytes.len() as u64 + 1,
                encoded_limit(1, 128).unwrap()
            )
            .is_err()
        );
    }

    #[test]
    fn malformed_footer_and_truncated_ipc_are_refused() {
        let (_dir, _path, bytes) = fixture();
        for len in [0, 5, 9, bytes.len() - 1] {
            assert!(preflight(&bytes[..len], 1, 128).is_err());
        }
        let mut oversized = bytes;
        let trailer = oversized.len() - 10;
        oversized[trailer..trailer + 4].copy_from_slice(&i32::MAX.to_le_bytes());
        assert!(preflight(&oversized, 1, 128).is_err());
    }

    #[test]
    fn zstd_requires_one_exact_current_frame_and_size() {
        let bytes = zstd::bulk::compress(&[7; 128], 1).unwrap();
        validate_zstd_frame(&bytes, 128).unwrap();
        assert!(validate_zstd_frame(&bytes, 127).is_err());
        assert!(validate_zstd_frame(&[bytes.clone(), bytes.clone()].concat(), 256).is_err());
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(validate_zstd_frame(&trailing, 128).is_err());
        assert!(validate_zstd_frame(&bytes[..bytes.len() - 1], 128).is_err());
        assert!(validate_zstd_frame(&[0x50, 0x2a, 0x4d, 0x18, 0, 0, 0, 0], 0).is_err());
    }

    #[test]
    fn bulk_decoder_context_does_not_grow_with_admitted_output() {
        let mut context = zstd::zstd_safe::DCtx::create();
        let context_bytes = context.sizeof();
        assert!(context_bytes <= 256 * 1024);
        for len in [1024, 1024 * 1024, 8 * 1024 * 1024] {
            let input = vec![23; len];
            let encoded = zstd::bulk::compress(&input, 1).unwrap();
            validate_zstd_frame(&encoded, u64::try_from(len).unwrap()).unwrap();
            let mut output = vec![0_u8; len];
            assert_eq!(
                context.decompress(output.as_mut_slice(), &encoded).unwrap(),
                len
            );
            assert_eq!(output, input);
            assert_eq!(context.sizeof(), context_bytes);
            assert!(
                context
                    .decompress(&mut output[..len - 1], &encoded)
                    .is_err()
            );
        }
        println!("CSR_ZSTD_FIXED_CONTEXT_BYTES {context_bytes}");
    }

    #[test]
    fn authenticated_size_bomb_is_refused_by_lookup_and_reuse() {
        use super::super::{
            CsrIndex, ShardedCsrIndex, sha256_hex, shard_set_matches, write_sharded_csr,
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.csr");
        let csr = CsrIndex {
            offsets: vec![0, 128],
            edge_ids: (0..128).collect(),
            neighbor_ids: vec![u64::MAX; 128],
        };
        write_sharded_csr(&path, &csr, 128).unwrap();
        let mut reader = ShardedCsrIndex::open(&path).unwrap();
        let record = &mut reader.manifest.shards[0];
        let payload = reader.root.join(&record.file);
        let mut bytes = std::fs::read(&payload).unwrap();
        let start = first_buffer_start(&bytes);
        bytes[start..start + 8].copy_from_slice(&i64::MAX.to_le_bytes());
        record.sha256 = sha256_hex(&bytes);
        record.xxh64 = crate::corruption_checksum::checksum(&bytes);
        std::fs::write(payload, bytes).unwrap();
        assert!(reader.row(0).is_err());
        assert!(reader.row_len(0).is_err());
        assert!(reader.row_chunk(0, 0, 1).is_err());
        assert!(reader.cache.lock().unwrap().entries.is_empty());
        assert!(!shard_set_matches(&reader.root, &reader.manifest.shards));
    }

    #[test]
    fn structurally_valid_incomplete_or_invalid_schema_is_refused_without_conversion() {
        let (_dir, _path, bytes) = fixture();
        let trailer = bytes.len() - 10;
        let len =
            arrow::ipc::reader::read_footer_length(bytes[trailer..].try_into().unwrap()).unwrap();
        let base = trailer - len;
        let footer_bytes = &bytes[base..trailer];
        let footer = arrow::ipc::root_as_footer(footer_bytes).unwrap();
        let schema = footer.schema().unwrap();
        let table = schema._tab.loc();
        let vtable_delta = i32::from_le_bytes(footer_bytes[table..table + 4].try_into().unwrap());
        let vtable =
            usize::try_from(i64::try_from(table).unwrap() - i64::from(vtable_delta)).unwrap();
        let slot = base + vtable + usize::from(arrow::ipc::Schema::VT_FIELDS);
        let mut missing_fields = bytes.clone();
        missing_fields[slot..slot + 2].fill(0);
        assert!(
            arrow::ipc::root_as_footer(&missing_fields[base..trailer])
                .unwrap()
                .schema()
                .unwrap()
                .fields()
                .is_none()
        );
        assert!(preflight(&missing_fields, 1, 128).is_err());

        let integer = schema
            .fields()
            .unwrap()
            .get(0)
            .children()
            .unwrap()
            .get(0)
            .children()
            .unwrap()
            .get(0)
            .type_as_int()
            .unwrap();
        let offset = integer._tab.vtable().get(arrow::ipc::Int::VT_BITWIDTH);
        assert_ne!(offset, 0);
        let width = base + integer._tab.loc() + usize::from(offset);
        let mut invalid_width = bytes;
        invalid_width[width..width + 4].copy_from_slice(&63_i32.to_le_bytes());
        assert!(arrow::ipc::root_as_footer(&invalid_width[base..trailer]).is_ok());
        assert!(preflight(&invalid_width, 1, 128).is_err());
    }
}
