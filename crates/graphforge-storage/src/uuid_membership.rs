//! Persistent, bounded-memory UUID membership indexes used by bulk ingest.
//!
//! The canonical graph remains Parquet.  This derived format is deliberately
//! small: a manifest (published last with an atomic rename) names two immutable
//! sorted files containing 16-byte UUID records. Readers verify version,
//! topology generation, length, count, and SHA-256 before serving probes.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use arrow::array::{Array, FixedSizeBinaryArray};
use graphforge_core::GfError;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const FORMAT_VERSION: u32 = 1;
const RECORD_BYTES: u64 = 16;
const INDEX_DIR: &str = "indexes/uuid-membership";
const MANIFEST: &str = "manifest.json";

fn storage_err(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("UUID membership index: {error}"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Selects the canonical identity domain to probe.
pub enum UuidIndexKind {
    /// Canonical node UUIDs.
    Node,
    /// Canonical edge UUIDs.
    Edge,
}

#[derive(Clone, Copy, Debug)]
/// Hard work limits for a bounded index build.
pub struct UuidIndexBuildLimits {
    /// Maximum Parquet rows decoded per scan batch.
    pub scan_batch_rows: usize,
    /// Maximum UUID records held by one sort run.
    pub run_records: usize,
    /// Maximum run files opened by one merge group.
    pub merge_fan_in: usize,
}

impl Default for UuidIndexBuildLimits {
    fn default() -> Self {
        Self {
            scan_batch_rows: 8_192,
            run_records: 65_536,
            merge_fan_in: 32,
        }
    }
}

impl UuidIndexBuildLimits {
    fn validate(self) -> Result<Self, GfError> {
        if self.scan_batch_rows == 0 || self.run_records == 0 || self.merge_fan_in < 2 {
            return Err(storage_err(
                "build limits must be non-zero and merge_fan_in >= 2",
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// Aggregate-only build evidence; it never contains graph identities.
pub struct UuidIndexBuildMetrics {
    /// Unique node identities written.
    pub node_count: u64,
    /// Unique edge identities written.
    pub edge_count: u64,
    /// Maximum UUID records simultaneously held by the sorter.
    pub peak_buffered_records: usize,
    /// Number of temporary sort and merge runs produced.
    pub temporary_runs: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// Aggregate-only probe evidence; it never contains graph identities.
pub struct UuidProbeMetrics {
    /// Total requested identities, including duplicates.
    pub requested: u64,
    /// Distinct requested identities.
    pub unique_requested: u64,
    /// Distinct identities found.
    pub found: u64,
    /// Binary-search file seeks performed.
    pub file_seeks: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FileRecord {
    name: String,
    count: u64,
    sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Manifest {
    format_version: u32,
    topology_generation: u64,
    nodes: FileRecord,
    edges: FileRecord,
}

#[derive(Debug)]
/// An authenticated node-and-edge index snapshot pinned by one manifest.
pub struct UuidMembershipIndex {
    node: File,
    edge: File,
    manifest: Manifest,
}

impl UuidMembershipIndex {
    /// Open and fully authenticate the current immutable index snapshot.
    pub fn open(project_dir: &Path) -> Result<Self, GfError> {
        let root = project_dir.join(INDEX_DIR);
        let body = fs::read(root.join(MANIFEST)).map_err(storage_err)?;
        let manifest: Manifest = serde_json::from_slice(&body).map_err(storage_err)?;
        if manifest.format_version != FORMAT_VERSION {
            return Err(storage_err(format!(
                "unsupported format version {}",
                manifest.format_version
            )));
        }
        let generation = crate::read_topology_generation(project_dir)?;
        if manifest.topology_generation != generation {
            return Err(storage_err(format!(
                "stale index generation {} (graph generation {generation})",
                manifest.topology_generation
            )));
        }
        let node = open_verified(&root, &manifest.nodes)?;
        let edge = open_verified(&root, &manifest.edges)?;
        Ok(Self {
            node,
            edge,
            manifest,
        })
    }

    #[must_use]
    /// Return the authenticated unique-record count for one identity domain.
    pub fn count(&self, kind: UuidIndexKind) -> u64 {
        match kind {
            UuidIndexKind::Node => self.manifest.nodes.count,
            UuidIndexKind::Edge => self.manifest.edges.count,
        }
    }

    /// Probe a batch in caller order. Memory is O(unique requested UUIDs).
    pub fn probe(
        &mut self,
        kind: UuidIndexKind,
        requested: &[Uuid],
    ) -> Result<(Vec<bool>, UuidProbeMetrics), GfError> {
        let mut metrics = UuidProbeMetrics {
            requested: requested.len() as u64,
            ..Default::default()
        };
        let unique = requested.iter().copied().collect::<BTreeSet<_>>();
        metrics.unique_requested = unique.len() as u64;
        let (file, count) = match kind {
            UuidIndexKind::Node => (&mut self.node, self.manifest.nodes.count),
            UuidIndexKind::Edge => (&mut self.edge, self.manifest.edges.count),
        };
        let mut membership = std::collections::BTreeMap::new();
        for uuid in unique {
            let found = binary_search(file, count, uuid, &mut metrics.file_seeks)?;
            metrics.found += u64::from(found);
            membership.insert(uuid, found);
        }
        Ok((requested.iter().map(|u| membership[u]).collect(), metrics))
    }
}

fn open_verified(root: &Path, record: &FileRecord) -> Result<File, GfError> {
    if Path::new(&record.name).components().count() != 1 {
        return Err(storage_err("manifest contains a non-local index filename"));
    }
    let path = root.join(&record.name);
    let mut file = File::open(&path).map_err(storage_err)?;
    let expected_len = record
        .count
        .checked_mul(RECORD_BYTES)
        .ok_or_else(|| storage_err("record length overflow"))?;
    if file.metadata().map_err(storage_err)?.len() != expected_len {
        return Err(storage_err(format!(
            "length mismatch for {}",
            path.display()
        )));
    }
    let actual = sha256_reader(&mut file)?;
    if actual != record.sha256 {
        return Err(storage_err(format!(
            "checksum mismatch for {}",
            path.display()
        )));
    }
    file.seek(SeekFrom::Start(0)).map_err(storage_err)?;
    Ok(file)
}

fn binary_search(
    file: &mut File,
    count: u64,
    target: Uuid,
    seeks: &mut u64,
) -> Result<bool, GfError> {
    let mut lo = 0_u64;
    let mut hi = count;
    let target = *target.as_bytes();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        file.seek(SeekFrom::Start(mid * RECORD_BYTES))
            .map_err(storage_err)?;
        *seeks += 1;
        let mut current = [0_u8; 16];
        file.read_exact(&mut current).map_err(storage_err)?;
        match current.cmp(&target) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => return Ok(true),
        }
    }
    Ok(false)
}

/// Explicit bounded rebuild/migration path. Immutable data files are completed
/// and synced first; `manifest.json` is atomically replaced last.
pub fn rebuild_uuid_membership_indexes(
    project_dir: &Path,
    limits: UuidIndexBuildLimits,
) -> Result<UuidIndexBuildMetrics, GfError> {
    let limits = limits.validate()?;
    let root = project_dir.join(INDEX_DIR);
    fs::create_dir_all(&root).map_err(storage_err)?;
    let scratch = tempfile::Builder::new()
        .prefix("build-")
        .tempdir_in(&root)
        .map_err(storage_err)?;
    let mut metrics = UuidIndexBuildMetrics::default();
    let node_runs = scan_to_runs(
        &[project_dir.join("topology/nodes.parquet")],
        "node_uuid",
        scratch.path(),
        "node",
        limits,
        &mut metrics,
    )?;
    let mut edge_paths =
        crate::mutator::parquet_files_in(project_dir, "topology/edges").map_err(storage_err)?;
    edge_paths.sort();
    let edge_runs = scan_to_runs(
        &edge_paths,
        "edge_uuid",
        scratch.path(),
        "edge",
        limits,
        &mut metrics,
    )?;
    let node_tmp = merge_all(
        node_runs,
        scratch.path(),
        "nodes",
        limits.merge_fan_in,
        &mut metrics,
    )?;
    let edge_tmp = merge_all(
        edge_runs,
        scratch.path(),
        "edges",
        limits.merge_fan_in,
        &mut metrics,
    )?;
    let generation = crate::read_topology_generation(project_dir)?;
    let nodes = publish_data(&node_tmp, &root, "nodes", generation)?;
    let edges = publish_data(&edge_tmp, &root, "edges", generation)?;
    metrics.node_count = nodes.count;
    metrics.edge_count = edges.count;
    let manifest = Manifest {
        format_version: FORMAT_VERSION,
        topology_generation: generation,
        nodes,
        edges,
    };
    let mut tmp = tempfile::Builder::new()
        .prefix("manifest-")
        .suffix(".tmp")
        .tempfile_in(&root)
        .map_err(storage_err)?;
    serde_json::to_writer(&mut tmp, &manifest).map_err(storage_err)?;
    tmp.flush().map_err(storage_err)?;
    tmp.as_file().sync_all().map_err(storage_err)?;
    tmp.persist(root.join(MANIFEST))
        .map_err(|e| storage_err(e.error))?;
    sync_dir(&root)?;
    Ok(metrics)
}

fn scan_to_runs(
    paths: &[PathBuf],
    column: &str,
    scratch: &Path,
    prefix: &str,
    limits: UuidIndexBuildLimits,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<Vec<PathBuf>, GfError> {
    let mut buffer = Vec::<[u8; 16]>::with_capacity(limits.run_records);
    let mut runs = Vec::new();
    for path in paths {
        if !path.exists() {
            continue;
        }
        let file = File::open(path).map_err(storage_err)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(storage_err)?
            .with_batch_size(limits.scan_batch_rows)
            .build()
            .map_err(storage_err)?;
        for batch in reader {
            let batch = batch.map_err(storage_err)?;
            let array = batch
                .column_by_name(column)
                .ok_or_else(|| storage_err(format!("{} lacks {column}", path.display())))?
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| storage_err(format!("{column} is not FixedSizeBinary")))?;
            for row in 0..array.len() {
                if array.is_null(row) || array.value(row).len() != 16 {
                    return Err(storage_err(format!("invalid {column} at row {row}")));
                }
                buffer.push(array.value(row).try_into().expect("length checked"));
                metrics.peak_buffered_records = metrics.peak_buffered_records.max(buffer.len());
                if buffer.len() == limits.run_records {
                    flush_run(&mut buffer, scratch, prefix, &mut runs, metrics)?;
                }
            }
        }
    }
    if !buffer.is_empty() {
        flush_run(&mut buffer, scratch, prefix, &mut runs, metrics)?;
    }
    if runs.is_empty() {
        let path = scratch.join(format!("{prefix}-empty.run"));
        File::create(&path)
            .map_err(storage_err)?
            .sync_all()
            .map_err(storage_err)?;
        runs.push(path);
    }
    Ok(runs)
}

fn flush_run(
    buffer: &mut Vec<[u8; 16]>,
    scratch: &Path,
    prefix: &str,
    runs: &mut Vec<PathBuf>,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<(), GfError> {
    buffer.sort_unstable();
    buffer.dedup();
    let path = scratch.join(format!("{prefix}-{:08}.run", runs.len()));
    let mut out = BufWriter::new(File::create(&path).map_err(storage_err)?);
    for value in buffer.iter() {
        out.write_all(value).map_err(storage_err)?;
    }
    out.flush().map_err(storage_err)?;
    out.get_ref().sync_all().map_err(storage_err)?;
    buffer.clear();
    runs.push(path);
    metrics.temporary_runs += 1;
    Ok(())
}

fn merge_all(
    mut runs: Vec<PathBuf>,
    scratch: &Path,
    prefix: &str,
    fan_in: usize,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<PathBuf, GfError> {
    let mut round = 0;
    while runs.len() > 1 {
        let mut next = Vec::new();
        for (group, chunk) in runs.chunks(fan_in).enumerate() {
            let path = scratch.join(format!("{prefix}-merge-{round}-{group}.run"));
            merge_runs(chunk, &path)?;
            next.push(path);
            metrics.temporary_runs += 1;
        }
        for path in runs {
            let _ = fs::remove_file(path);
        }
        runs = next;
        round += 1;
    }
    Ok(runs.pop().expect("at least one run"))
}

fn merge_runs(inputs: &[PathBuf], output: &Path) -> Result<(), GfError> {
    let mut readers = inputs
        .iter()
        .map(|p| File::open(p).map(BufReader::new).map_err(storage_err))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::<Reverse<([u8; 16], usize)>>::new();
    for (idx, reader) in readers.iter_mut().enumerate() {
        if let Some(value) = read_record(reader)? {
            heap.push(Reverse((value, idx)));
        }
    }
    let mut out = BufWriter::new(File::create(output).map_err(storage_err)?);
    let mut previous = None;
    while let Some(Reverse((value, idx))) = heap.pop() {
        if previous != Some(value) {
            out.write_all(&value).map_err(storage_err)?;
            previous = Some(value);
        }
        if let Some(next) = read_record(&mut readers[idx])? {
            heap.push(Reverse((next, idx)));
        }
    }
    out.flush().map_err(storage_err)?;
    out.get_ref().sync_all().map_err(storage_err)?;
    Ok(())
}

fn read_record(reader: &mut BufReader<File>) -> Result<Option<[u8; 16]>, GfError> {
    let mut value = [0_u8; 16];
    match reader.read_exact(&mut value) {
        Ok(()) => Ok(Some(value)),
        Err(error)
            if error.kind() == std::io::ErrorKind::UnexpectedEof
                && reader.fill_buf().map_err(storage_err)?.is_empty() =>
        {
            Ok(None)
        }
        Err(error) => Err(storage_err(error)),
    }
}

fn publish_data(
    source: &Path,
    root: &Path,
    kind: &str,
    generation: u64,
) -> Result<FileRecord, GfError> {
    let length = source.metadata().map_err(storage_err)?.len();
    if length % RECORD_BYTES != 0 {
        return Err(storage_err("internal run has a partial UUID record"));
    }
    let mut input = File::open(source).map_err(storage_err)?;
    let sha256 = sha256_reader(&mut input)?;
    let name = format!("{kind}-{generation}-{}.uuidx", &sha256[..16]);
    let destination = root.join(&name);
    if !destination.exists() {
        let mut tmp = tempfile::Builder::new()
            .prefix(kind)
            .suffix(".tmp")
            .tempfile_in(root)
            .map_err(storage_err)?;
        let mut input = File::open(source).map_err(storage_err)?;
        std::io::copy(&mut input, &mut tmp).map_err(storage_err)?;
        tmp.flush().map_err(storage_err)?;
        tmp.as_file().sync_all().map_err(storage_err)?;
        tmp.persist(&destination)
            .map_err(|e| storage_err(e.error))?;
    }
    Ok(FileRecord {
        name,
        count: length / RECORD_BYTES,
        sha256,
    })
}

fn sync_dir(path: &Path) -> Result<(), GfError> {
    #[cfg(unix)]
    {
        File::open(path)
            .and_then(|f| f.sync_all())
            .map_err(storage_err)?;
    }
    Ok(())
}

fn sha256_reader(reader: &mut impl Read) -> Result<String, GfError> {
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer).map_err(storage_err)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    let mut encoded = String::with_capacity(64);
    for byte in digest.finalize() {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use arrow::array::FixedSizeBinaryArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    use super::*;

    fn write_uuid_parquet(path: &Path, column: &str, values: &[Uuid]) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            column,
            DataType::FixedSizeBinary(16),
            false,
        )]));
        let array = FixedSizeBinaryArray::try_from_iter(
            values.iter().map(|value| value.as_bytes().as_slice()),
        )
        .unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
        let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn fixture() -> (tempfile::TempDir, Vec<Uuid>, Vec<Uuid>) {
        let dir = tempfile::tempdir().unwrap();
        let nodes = vec![Uuid::from_u128(3), Uuid::from_u128(1), Uuid::from_u128(2)];
        let edges = vec![
            Uuid::from_u128(12),
            Uuid::from_u128(11),
            Uuid::from_u128(11),
        ];
        write_uuid_parquet(
            &dir.path().join("topology/nodes.parquet"),
            "node_uuid",
            &nodes,
        );
        write_uuid_parquet(
            &dir.path().join("topology/edges/R.parquet"),
            "edge_uuid",
            &edges,
        );
        (dir, nodes, edges)
    }

    #[test]
    fn bounded_build_reopens_and_probes_in_caller_order() {
        let (dir, nodes, _) = fixture();
        let metrics = rebuild_uuid_membership_indexes(
            dir.path(),
            UuidIndexBuildLimits {
                scan_batch_rows: 1,
                run_records: 1,
                merge_fan_in: 2,
            },
        )
        .unwrap();
        assert_eq!((metrics.node_count, metrics.edge_count), (3, 2));
        assert_eq!(metrics.peak_buffered_records, 1);
        let mut index = UuidMembershipIndex::open(dir.path()).unwrap();
        let missing = Uuid::from_u128(99);
        let (found, probe) = index
            .probe(UuidIndexKind::Node, &[nodes[1], missing, nodes[1]])
            .unwrap();
        assert_eq!(found, vec![true, false, true]);
        assert_eq!(
            (probe.requested, probe.unique_requested, probe.found),
            (3, 2, 1)
        );
    }

    #[test]
    fn corrupt_data_fails_closed_without_replacing_manifest() {
        let (dir, _, _) = fixture();
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        let root = dir.path().join(INDEX_DIR);
        let manifest: Manifest =
            serde_json::from_slice(&fs::read(root.join(MANIFEST)).unwrap()).unwrap();
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(root.join(manifest.nodes.name))
            .unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[0xff]).unwrap();
        assert!(
            UuidMembershipIndex::open(dir.path())
                .unwrap_err()
                .to_string()
                .contains("checksum mismatch")
        );
    }

    #[test]
    fn missing_and_stale_manifests_fail_closed() {
        let (dir, _, _) = fixture();
        assert!(UuidMembershipIndex::open(dir.path()).is_err());
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        crate::generation::bump_topology_generation(dir.path()).unwrap();
        assert!(
            UuidMembershipIndex::open(dir.path())
                .unwrap_err()
                .to_string()
                .contains("stale index generation")
        );
    }

    #[test]
    fn unpublished_build_artifacts_do_not_change_concurrent_readers() {
        let (dir, nodes, _) = fixture();
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let reader_root = dir.path().to_path_buf();
        let reader_barrier = barrier.clone();
        let expected = nodes[0];
        let reader = std::thread::spawn(move || {
            let mut index = UuidMembershipIndex::open(&reader_root).unwrap();
            reader_barrier.wait();
            index.probe(UuidIndexKind::Node, &[expected]).unwrap().0
        });
        let scratch = tempfile::Builder::new()
            .prefix("build-crash-")
            .tempdir_in(dir.path().join(INDEX_DIR))
            .unwrap();
        fs::write(scratch.path().join("nodes-unpublished.uuidx"), [7_u8; 16]).unwrap();
        barrier.wait();
        assert_eq!(reader.join().unwrap(), vec![true]);
        assert!(UuidMembershipIndex::open(dir.path()).is_ok());
    }
}
