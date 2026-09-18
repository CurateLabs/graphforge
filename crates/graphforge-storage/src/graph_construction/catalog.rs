//! Bounded construction runtime-catalog building and authenticated parent loading.
//!
//! Construction orchestration and shared I/O accounting stay in the parent.
//! Catalog admission, stable IDs/history, and retained/CAS catalog reads are
//! kept together so their resource and authority checks share one boundary.

use std::ffi::OsStr;
use std::io::{Read, Seek};
use std::path::Path;
use std::sync::atomic::Ordering;

use arrow::array::{Array, StringArray};
use graphforge_core::GfError;
use graphforge_filesystem::{file_identity, file_link_count};
use graphforge_ir::RuntimeCatalog;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};

use crate::construction_directory::ConstructionDirectory as StableDirectory;

use super::{
    BLOCK_BYTES, ConstructionChunkKind, ConstructionFileHandle, CountingChunkReader, DetailCodec,
    EDGE_DETAIL_WIDTH, GraphConstructionBudgets, GraphConstructionEvidence, IoCounter,
    NODE_DETAIL_WIDTH, ReadWork, account_cache_release, account_fixed_read_operations,
    account_merge_read, account_sequential_write, combine_cache_cleanup, hex, is_canonical_sha256,
    merge_cache_release_evidence, open_counted_fixed_reader, record_shape_artifact_install,
    reject_cancelled, release_counted_reader_cache, storage, write_parquet_with_properties,
};

/// Where one kind's runtime-catalog observations are read from (#1455).
///
/// The catalog interns one observation per staged row -- its label or route,
/// then any non-null property -- in row order, and the encoded bytes depend
/// on that order because entity and relation types share one id space. Both
/// sources below present the rows of one kind in the same order: the UUID
/// order the shaped families are concatenated in.
#[derive(Clone, Copy, Debug)]
pub(super) enum CatalogSource<'a> {
    /// The kind's exact-schema shaped row artifacts, scanned as Parquet. This
    /// is the path every property-bearing input takes: property observations
    /// exist only in the rows. It is also the path a kind keeps when its
    /// chunks mix property-free and property-bearing schemas, because the
    /// rows are then grouped by schema before UUID and the details family's
    /// plain UUID order would intern them differently.
    Rows(&'a [String]),
    /// The kind's shaped details family, when every staged chunk of that kind
    /// carried the bare canonical schema. Node details carry the label and
    /// edge details carry the route, sorted by UUID exactly as the single
    /// property-free row group would have been, so the intern sequence -- and
    /// the catalog bytes -- are identical to a scan of that row group. The
    /// row artifacts are not produced at all on this path.
    Details(Option<&'a str>),
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)] // One bounded streamed catalog pass; admission must surround each intern.
pub(super) fn build_runtime_catalog(
    mut catalog: RuntimeCatalog,
    root: &StableDirectory,
    node_source: CatalogSource<'_>,
    edge_source: CatalogSource<'_>,
    detail_codec: DetailCodec,
    now_micros: i64,
    budgets: GraphConstructionBudgets,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<String, GfError> {
    let mut catalog_entries = catalog.entry_count();
    let mut identifier_bytes = catalog.retained_identifier_bytes();
    if catalog_entries > budgets.max_catalog_entries
        || identifier_bytes > budgets.max_catalog_identifier_bytes
    {
        return Err(storage(
            "runtime catalog exceeds construction admission budget",
        ));
    }
    evidence.peak_catalog_entries = evidence.peak_catalog_entries.max(catalog_entries as u64);
    evidence.peak_catalog_identifier_bytes = evidence
        .peak_catalog_identifier_bytes
        .max(identifier_bytes as u64);
    for (kind, source) in [
        (ConstructionChunkKind::Node, node_source),
        (ConstructionChunkKind::Edge, edge_source),
    ] {
        let names = match source {
            CatalogSource::Rows(names) => names,
            CatalogSource::Details(None) => continue,
            CatalogSource::Details(Some(name)) => {
                let mut admission = CatalogAdmission {
                    catalog: &mut catalog,
                    entries: &mut catalog_entries,
                    identifier_bytes: &mut identifier_bytes,
                    now_micros,
                    budgets,
                };
                match kind {
                    ConstructionChunkKind::Node => intern_details::<NODE_DETAIL_WIDTH>(
                        &mut admission,
                        root,
                        name,
                        kind,
                        detail_codec,
                        cancelled,
                        evidence,
                    )?,
                    ConstructionChunkKind::Edge => intern_details::<EDGE_DETAIL_WIDTH>(
                        &mut admission,
                        root,
                        name,
                        kind,
                        detail_codec,
                        cancelled,
                        evidence,
                    )?,
                }
                continue;
            }
        };
        for name in names {
            let file = root.open_child_file(OsStr::new(name)).map_err(storage)?;
            let counter = IoCounter::default();
            let chunk_reader = CountingChunkReader::new(file, counter.clone());
            let cache_release = chunk_reader.cache_release_tracker();
            let scan = (|| -> Result<(), GfError> {
                let reader = ParquetRecordBatchReaderBuilder::try_new(chunk_reader)
                    .map_err(storage)?
                    .with_batch_size(4096)
                    .build()
                    .map_err(storage)?;
                for batch in reader {
                    reject_cancelled(cancelled)?;
                    let batch = batch.map_err(storage)?;
                    let decoded_bytes = batch.get_array_memory_size();
                    if decoded_bytes > budgets.max_catalog_decoded_bytes {
                        return Err(storage("runtime catalog decoded batch budget exhausted"));
                    }
                    evidence.peak_catalog_decoded_batch_bytes = evidence
                        .peak_catalog_decoded_batch_bytes
                        .max(decoded_bytes as u64);
                    let owner = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| storage("catalog owner column is not Utf8"))?;
                    let required = if kind == ConstructionChunkKind::Node {
                        2
                    } else {
                        4
                    };
                    for row in 0..batch.num_rows() {
                        let owner_name = owner.value(row);
                        match kind {
                            ConstructionChunkKind::Node => {
                                admit_catalog_identifier(
                                    !catalog.contains_entity_type(owner_name),
                                    owner_name.len(),
                                    &mut catalog_entries,
                                    &mut identifier_bytes,
                                    budgets,
                                    evidence,
                                )?;
                                catalog.intern_label_at(owner_name, now_micros)?;
                            }
                            ConstructionChunkKind::Edge => {
                                admit_catalog_identifier(
                                    !catalog.contains_relation_type(owner_name),
                                    owner_name.len(),
                                    &mut catalog_entries,
                                    &mut identifier_bytes,
                                    budgets,
                                    evidence,
                                )?;
                                catalog.intern_relation_type_at(owner_name, now_micros)?;
                            }
                        }
                        for (offset, field) in
                            batch.schema().fields()[required..].iter().enumerate()
                        {
                            if !batch.column(required + offset).is_null(row) {
                                admit_catalog_identifier(
                                    !catalog.contains_property(field.name(), Some(owner_name)),
                                    field
                                        .name()
                                        .len()
                                        .checked_add(owner_name.len())
                                        .ok_or_else(|| {
                                            storage("catalog identifier length overflows")
                                        })?,
                                    &mut catalog_entries,
                                    &mut identifier_bytes,
                                    budgets,
                                    evidence,
                                )?;
                                catalog.intern_property_at(
                                    field.name(),
                                    Some(owner_name),
                                    now_micros,
                                )?;
                            }
                        }
                    }
                }
                Ok(())
            })();
            let released = cache_release.check_error().map_err(storage);
            match (scan, released) {
                (Ok(()), Ok(())) => {}
                (Ok(()), Err(error)) => return Err(error),
                (Err(primary), Ok(())) => return Err(primary),
                (Err(primary), Err(release)) => {
                    return Err(storage(format!(
                        "{primary}; runtime catalog cache release also failed: {release}"
                    )));
                }
            }
            account_cache_release(cache_release.evidence(), evidence)?;
            counter.add_to(evidence)?;
        }
    }
    let output = "shaped-runtime-catalog.parquet";
    let receipt = write_parquet_with_properties(
        root,
        output,
        &catalog.to_record_batch(),
        Some(crate::permanent_parquet::writer_properties().build()),
        evidence,
    )?;
    record_shape_artifact_install(evidence, &receipt)?;
    evidence.merge_fsync_operations = evidence
        .merge_fsync_operations
        .checked_add(receipt.fsync_operations)
        .ok_or_else(|| storage("merge fsync count overflows"))?;
    evidence.parquet_write_bytes = evidence
        .parquet_write_bytes
        .checked_add(receipt.bytes)
        .ok_or_else(|| storage("Parquet write byte count overflows"))?;
    evidence.parquet_write_operations = evidence
        .parquet_write_operations
        .checked_add(receipt.write_operations)
        .ok_or_else(|| storage("Parquet write operation count overflows"))?;
    account_sequential_write(receipt.bytes, evidence)?;
    Ok(output.to_owned())
}

/// The admission state one catalog pass threads through every intern.
struct CatalogAdmission<'a> {
    catalog: &'a mut RuntimeCatalog,
    entries: &'a mut usize,
    identifier_bytes: &'a mut usize,
    now_micros: i64,
    budgets: GraphConstructionBudgets,
}

impl CatalogAdmission<'_> {
    /// Admit and intern one row's label or route observation.
    fn observe_owner(
        &mut self,
        kind: ConstructionChunkKind,
        owner_name: &str,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<(), GfError> {
        let is_new = match kind {
            ConstructionChunkKind::Node => !self.catalog.contains_entity_type(owner_name),
            ConstructionChunkKind::Edge => !self.catalog.contains_relation_type(owner_name),
        };
        admit_catalog_identifier(
            is_new,
            owner_name.len(),
            self.entries,
            self.identifier_bytes,
            self.budgets,
            evidence,
        )?;
        match kind {
            ConstructionChunkKind::Node => {
                self.catalog.intern_label_at(owner_name, self.now_micros)?;
            }
            ConstructionChunkKind::Edge => {
                self.catalog
                    .intern_relation_type_at(owner_name, self.now_micros)?;
            }
        }
        Ok(())
    }
}

/// Intern one kind's observations from its shaped details family (#1455).
///
/// The family is the same fixed-width run the encoder later consumes: UUID
/// sorted, one record per staged row, name bounded and padded by
/// [`DetailCodec`]. Node records carry the label after a 16-byte UUID; edge
/// records carry the route after the 48-byte UUID/endpoint prefix. The read
/// is charged as a merge read, like every other fixed-width shaping scan.
#[allow(clippy::too_many_arguments)]
fn intern_details<const N: usize>(
    admission: &mut CatalogAdmission<'_>,
    root: &StableDirectory,
    name: &str,
    kind: ConstructionChunkKind,
    codec: DetailCodec,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    // Both detail widths are `prefix + 1 + 255`: the name-length byte sits
    // immediately after the fixed prefix (`construction_detail_codec`).
    const { assert!(N == NODE_DETAIL_WIDTH || N == EDGE_DETAIL_WIDTH) };
    let prefix = N - 256;
    let (mut reader, counter) = open_counted_fixed_reader(root, name, evidence)?;
    let scanned = (|| -> Result<(), GfError> {
        let mut records = 0_u64;
        while let Some(record) = codec.read::<N>(&mut reader).map_err(storage)? {
            let wire = codec.bytes(&record).map_err(storage)?;
            let owner_name = std::str::from_utf8(&wire[prefix + 1..]).map_err(storage)?;
            admission.observe_owner(kind, owner_name, evidence)?;
            account_merge_read::<N>(evidence)?;
            records = records
                .checked_add(1)
                .ok_or_else(|| storage("catalog detail record count overflows"))?;
            if records.is_multiple_of(4096) {
                reject_cancelled(cancelled)?;
            }
        }
        account_fixed_read_operations(&counter, evidence)
    })();
    let released = release_counted_reader_cache(&mut reader, evidence);
    combine_cache_cleanup(scanned, released, "runtime catalog detail source")
}

#[allow(clippy::too_many_arguments)]
fn admit_catalog_identifier(
    is_new: bool,
    added_identifier_bytes: usize,
    entries: &mut usize,
    identifier_bytes: &mut usize,
    budgets: GraphConstructionBudgets,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    if !is_new {
        return Ok(());
    }
    let next_entries = entries
        .checked_add(1)
        .ok_or_else(|| storage("runtime catalog entry count overflow"))?;
    let next_identifier_bytes = identifier_bytes
        .checked_add(added_identifier_bytes)
        .ok_or_else(|| storage("runtime catalog identifier byte count overflow"))?;
    if next_entries > budgets.max_catalog_entries
        || next_identifier_bytes > budgets.max_catalog_identifier_bytes
    {
        return Err(storage("runtime catalog admission budget exhausted"));
    }
    *entries = next_entries;
    *identifier_bytes = next_identifier_bytes;
    evidence.peak_catalog_entries = evidence.peak_catalog_entries.max(next_entries as u64);
    evidence.peak_catalog_identifier_bytes = evidence
        .peak_catalog_identifier_bytes
        .max(next_identifier_bytes as u64);
    Ok(())
}

#[allow(clippy::too_many_lines)] // One retained-file pass couples digest and Parquet cache cleanup.
pub(super) fn load_parent_runtime_catalog(
    project: &StableDirectory,
    parent_generation: u64,
    budgets: GraphConstructionBudgets,
) -> Result<(RuntimeCatalog, Option<String>, ReadWork), GfError> {
    if parent_generation == 0 {
        return Ok((RuntimeCatalog::new(), None, ReadWork::default()));
    }
    let topology = match project.open_child_directory(OsStr::new("topology")) {
        Ok(topology) => topology,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((RuntimeCatalog::new(), None, ReadWork::default()));
        }
        Err(error) => return Err(storage(error)),
    };
    let mut file = match topology.open_child_file(OsStr::new("runtime_catalog.parquet")) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((RuntimeCatalog::new(), None, ReadWork::default()));
        }
        Err(error) => return Err(storage(error)),
    };
    if file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("parent runtime catalog has extra links"));
    }
    let identity = file_identity(&file).map_err(storage)?;
    let mut digest_reader =
        graphforge_filesystem::FileCacheReleasingReader::new(file.try_clone().map_err(storage)?)
            .map_err(storage)?;
    let mut digest = Sha256::new();
    let mut work = ReadWork::default();
    let mut block = vec![0_u8; BLOCK_BYTES];
    let hashed = (|| -> Result<(), GfError> {
        loop {
            let count = digest_reader.read(&mut block).map_err(storage)?;
            if count == 0 {
                break;
            }
            digest.update(&block[..count]);
            work.bytes = work
                .bytes
                .checked_add(count as u64)
                .ok_or_else(|| storage("bytes overflows"))?;
            work.operations = work
                .operations
                .checked_add(1)
                .ok_or_else(|| storage("operations overflows"))?;
        }
        Ok(())
    })();
    let released = digest_reader.finish().map_err(storage);
    work.cache_release = match (hashed, released) {
        (Ok(()), Ok(released)) => released,
        (Ok(()), Err(release)) => return Err(release),
        (Err(primary), Ok(_)) => return Err(primary),
        (Err(primary), Err(release)) => {
            return Err(storage(format!(
                "{primary}; parent catalog digest cache release also failed: {release}"
            )));
        }
    };
    file.rewind().map_err(storage)?;
    if file_identity(&file).map_err(storage)? != identity {
        return Err(storage("parent runtime catalog identity changed"));
    }
    let counter = IoCounter::default();
    let chunk_reader = CountingChunkReader::new(file, counter.clone());
    let cache_release = chunk_reader.cache_release_tracker();
    let mut catalog = RuntimeCatalog::new();
    let mut entries = 0_usize;
    let mut decoded_bytes = 0_usize;
    let decoded = (|| -> Result<(), GfError> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(chunk_reader)
            .map_err(storage)?
            .with_batch_size(4_096.min(budgets.max_catalog_entries))
            .build()
            .map_err(storage)?;
        for batch in reader {
            let batch = batch.map_err(storage)?;
            entries = entries
                .checked_add(batch.num_rows())
                .ok_or_else(|| storage("parent runtime catalog entry count overflow"))?;
            decoded_bytes = decoded_bytes
                .checked_add(batch.get_array_memory_size())
                .ok_or_else(|| storage("parent runtime catalog decoded size overflow"))?;
            if entries > budgets.max_catalog_entries
                || decoded_bytes > budgets.max_catalog_decoded_bytes
            {
                return Err(storage("parent runtime catalog admission budget exhausted"));
            }
            catalog.extend_from_record_batch(&batch).map_err(storage)?;
            if catalog.retained_identifier_bytes() > budgets.max_catalog_identifier_bytes {
                return Err(storage(
                    "parent runtime catalog identifier budget exhausted",
                ));
            }
        }
        Ok(())
    })();
    combine_cache_cleanup(
        decoded,
        cache_release.check_error().map_err(storage),
        "parent runtime catalog",
    )?;
    merge_cache_release_evidence(&mut work.cache_release, cache_release.evidence())?;
    work.bytes = work
        .bytes
        .checked_add(counter.bytes.load(Ordering::Relaxed))
        .ok_or_else(|| storage("parent catalog Parquet read bytes overflow"))?;
    work.operations = work
        .operations
        .checked_add(counter.operations.load(Ordering::Relaxed))
        .ok_or_else(|| storage("parent catalog Parquet read operations overflow"))?;
    let named = topology
        .open_child_file(OsStr::new("runtime_catalog.parquet"))
        .map_err(storage)?;
    if file_identity(&named).map_err(storage)? != identity
        || file_link_count(&named).map_err(storage)? != 1
    {
        return Err(storage("parent runtime catalog authority changed"));
    }
    topology.revalidate_named().map_err(storage)?;
    project.revalidate_named().map_err(storage)?;
    Ok((catalog, Some(hex(&digest.finalize())), work))
}

pub(super) fn load_parent_runtime_catalog_from_compact(
    container_root: &Path,
    inventory: &crate::GraphFilesInventory,
    parent_generation: u64,
    budgets: GraphConstructionBudgets,
) -> Result<(RuntimeCatalog, Option<String>, ReadWork), GfError> {
    if parent_generation == 0 {
        return Ok((RuntimeCatalog::new(), None, ReadWork::default()));
    }
    let Some(entry) = inventory
        .files
        .iter()
        .find(|entry| entry.relative_path == "topology/runtime_catalog.parquet")
    else {
        return Ok((RuntimeCatalog::new(), None, ReadWork::default()));
    };
    let file = crate::graph_object_store::open_graph_object_by_digest(
        container_root,
        &entry.content_sha256,
        entry.byte_length,
    )?;
    decode_parent_runtime_catalog_file(file, &entry.content_sha256, budgets)
}

fn decode_parent_runtime_catalog_file<R>(
    file: R,
    digest: &str,
    budgets: GraphConstructionBudgets,
) -> Result<(RuntimeCatalog, Option<String>, ReadWork), GfError>
where
    R: ConstructionFileHandle + 'static,
{
    let counter = IoCounter::default();
    let chunk_reader = CountingChunkReader::new(file, counter.clone());
    let cache_release = chunk_reader.cache_release_tracker();
    let mut catalog = RuntimeCatalog::new();
    let mut entries = 0_usize;
    let mut decoded_bytes = 0_usize;
    let decoded = (|| -> Result<(), GfError> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(chunk_reader)
            .map_err(storage)?
            .with_batch_size(4_096.min(budgets.max_catalog_entries))
            .build()
            .map_err(storage)?;
        for batch in reader {
            let batch = batch.map_err(storage)?;
            entries = entries
                .checked_add(batch.num_rows())
                .ok_or_else(|| storage("parent runtime catalog entry count overflow"))?;
            decoded_bytes = decoded_bytes
                .checked_add(batch.get_array_memory_size())
                .ok_or_else(|| storage("parent runtime catalog decoded size overflow"))?;
            if entries > budgets.max_catalog_entries
                || decoded_bytes > budgets.max_catalog_decoded_bytes
            {
                return Err(storage("parent runtime catalog admission budget exhausted"));
            }
            catalog.extend_from_record_batch(&batch).map_err(storage)?;
            if catalog.retained_identifier_bytes() > budgets.max_catalog_identifier_bytes {
                return Err(storage(
                    "parent runtime catalog identifier budget exhausted",
                ));
            }
        }
        Ok(())
    })();
    combine_cache_cleanup(
        decoded,
        cache_release.check_error().map_err(storage),
        "compact parent runtime catalog",
    )?;
    let bytes = counter.bytes.load(Ordering::Relaxed);
    let operations = counter.operations.load(Ordering::Relaxed);
    if !is_canonical_sha256(digest) {
        return Err(storage("parent runtime catalog CAS authority changed"));
    }
    Ok((
        catalog,
        Some(digest.to_owned()),
        ReadWork {
            detail_records: 0,
            bytes,
            operations,
            cache_release: cache_release.evidence(),
        },
    ))
}

#[cfg(test)]
mod tests;
