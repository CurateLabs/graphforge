//! Staged Parquet writes — temp-file + atomic-rename commit (#790).
//!
//! Every Parquet write in this crate goes through a [`RewriteBatch`]: each
//! replacement file is staged as a sibling temp file without touching the
//! original, then [`commit`](RewriteBatch::commit) renames them into place in
//! insertion order. An I/O failure mid-write (disk full, encode error) can
//! never leave a truncated/torn file — the prior file survives
//! byte-identical. Callers encode safety ordering by insertion (deletes
//! commit `topology/nodes.parquet` last; appends commit it first). Dropping
//! an un-committed batch removes every temp (the abort path), leaving the
//! prior state fully intact.
//!
//! Several rewrites of the same file in one statement (#792) compose by
//! reading **through** the batch ([`staged_temp`](RewriteBatch::staged_temp)
//! gives the current staged content as the base) and replacing the entry in
//! place ([`restage`](RewriteBatch::restage)), so each file is committed
//! exactly once with the statement's net content.
//!
//! Temps are created in the destination's own directory (same filesystem, so
//! `rename` is atomic) with a `.tmp` extension — invisible to every reader in
//! this crate, which match on the `parquet` extension or exact file names.
//!
//! [`crate::generation::commit_topology_aware`] upgrades a batch into a durable
//! transaction: it records authenticated, deterministic recovery inputs before
//! replacing any destination and publishes generation authority last.  The
//! plain [`commit`](RewriteBatch::commit) remains for tests and explicitly
//! ephemeral callers; persistent graph mutations use the topology-aware path.

use std::path::{Path, PathBuf};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use std::collections::{BTreeMap, HashMap};
use tempfile::NamedTempFile;

use graphforge_core::GfError;

const STAGED_TEMP_DIRS: [&str; 3] = ["topology", "properties", "edge_properties"];

fn io_err(e: &std::io::Error) -> GfError {
    GfError::Storage(e.to_string())
}

fn pq_err(e: impl std::fmt::Display) -> GfError {
    GfError::Storage(e.to_string())
}

/// Remove abandoned sibling temps created by staged graph writes.
///
/// Only graph-owned directories are traversed, and only names matching the
/// exact `<parquet-file>.<random>.tmp` or generation-counter pattern are
/// removed. Missing directories are ignored.
///
/// # Errors
/// Returns [`GfError::Storage`] when a graph-owned directory cannot be read or
/// a recognized stale temp cannot be removed.
pub fn remove_stale_temps(project_dir: &Path) -> Result<usize, GfError> {
    // Recovery owns temp-looking durable inputs after intent. It must run
    // before the stale-temp sweep can remove any graph-owned file.
    let _ = crate::generation::read_topology_generation(project_dir)?;
    let mut removed = 0;
    for relative in STAGED_TEMP_DIRS {
        removed += remove_stale_temps_under(&project_dir.join(relative))?;
    }
    Ok(removed)
}

fn remove_stale_temps_under(dir: &Path) -> Result<usize, GfError> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(io_err(&error)),
    };
    let mut removed = 0;
    for entry in entries {
        let entry = entry.map_err(|error| io_err(&error))?;
        let file_type = entry.file_type().map_err(|error| io_err(&error))?;
        if file_type.is_dir() {
            removed += remove_stale_temps_under(&entry.path())?;
        } else if file_type.is_file() && is_staged_temp_name(&entry.file_name()) {
            std::fs::remove_file(entry.path()).map_err(|error| io_err(&error))?;
            removed += 1;
        }
    }
    Ok(removed)
}

pub(crate) fn is_staged_temp_name(name: &std::ffi::OsStr) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    let Some(without_tmp) = name.strip_suffix(".tmp") else {
        return false;
    };
    let Some((destination, random)) = without_tmp.rsplit_once('.') else {
        return false;
    };
    !random.is_empty()
        && random.bytes().all(|byte| byte.is_ascii_alphanumeric())
        && (destination.ends_with(".parquet") || destination == "generation.json")
}

/// A staged multi-file Parquet rewrite: nothing is visible until
/// [`commit`](Self::commit), which renames each staged file into place in
/// **insertion order**. Dropping the batch without committing removes all
/// temps and leaves every original untouched.
///
/// A given destination path may be staged **at most once** per batch: staging
/// reads committed state, so a second stage of the same file would silently
/// discard the first (guarded by a `debug_assert!`).
#[derive(Default)]
pub struct RewriteBatch {
    /// `(staged temp, final destination)` in insertion = commit order.
    staged: Vec<(NamedTempFile, PathBuf)>,
    property_windows: BTreeMap<PropertyWindowKey, PendingPropertyWindow>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct PropertyWindowKey {
    pub(crate) kind: crate::property_overlay::PropertyRouteKind,
    pub(crate) route: String,
}

#[derive(Debug, Clone)]
pub(crate) struct PendingPropertyWindow {
    pub(crate) project_root: PathBuf,
    pub(crate) rows: BTreeMap<[u8; 16], PendingPropertyRow>,
    pub(crate) metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PropertyWindowMode {
    Patch,
    Replace,
}

#[derive(Debug, Clone)]
pub(crate) struct PendingPropertyRow {
    pub(crate) snapshot: crate::property_overlay::PropertySnapshotRow,
    pub(crate) mode: PropertyWindowMode,
}

impl RewriteBatch {
    /// Creates an empty batch.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Stage `batch` as the replacement content for `final_path`.
    ///
    /// Writes a sibling temp file (creating the parent directory if needed);
    /// `final_path` itself is not touched until [`commit`](Self::commit).
    ///
    /// # Errors
    /// Returns [`GfError`] on I/O or Parquet-encode failure; the batch remains
    /// usable and the destination untouched.
    pub fn stage(
        &mut self,
        final_path: &Path,
        schema: SchemaRef,
        batch: &RecordBatch,
    ) -> Result<(), GfError> {
        if self.staged.iter().any(|(_, path)| path == final_path) {
            return Err(GfError::Storage(format!(
                "destination staged twice in one rewrite batch: {}",
                final_path.display()
            )));
        }
        let tmp = stage_parquet_temp(final_path, schema, batch)?;
        self.staged.push((tmp, final_path.to_path_buf()));
        Ok(())
    }

    /// Stage `batch` for `final_path`, **replacing** any prior staged content
    /// for the same destination at its original commit position; stages like
    /// [`stage`](Self::stage) when the destination is new to this batch.
    ///
    /// This is the read-modify-restage half of composing several rewrites of
    /// one file in a single statement (#792): a primitive reads the current
    /// content through [`staged_temp`](Self::staged_temp), applies its change,
    /// and restages the net result.
    ///
    /// # Errors
    /// Returns [`GfError`] on I/O or Parquet-encode failure.
    pub fn restage(
        &mut self,
        final_path: &Path,
        schema: SchemaRef,
        batch: &RecordBatch,
    ) -> Result<(), GfError> {
        let tmp = stage_parquet_temp(final_path, schema, batch)?;
        if let Some(entry) = self.staged.iter_mut().find(|(_, p)| p == final_path) {
            entry.0 = tmp; // the replaced NamedTempFile is removed on drop
        } else {
            self.staged.push((tmp, final_path.to_path_buf()));
        }
        Ok(())
    }

    /// Stage a fixed-schema append without materializing the existing Parquet
    /// file. Existing row groups are decoded and re-encoded one bounded batch
    /// at a time, followed by `batch`.
    ///
    /// Returns the number of existing rows copied into the replacement.
    ///
    /// # Errors
    /// Returns [`GfError`] on I/O, decode, schema, or encode failure. The prior
    /// destination remains untouched.
    pub fn restage_append(
        &mut self,
        final_path: &Path,
        schema: SchemaRef,
        batch: &RecordBatch,
    ) -> Result<u64, GfError> {
        self.restage_append_with(final_path, schema, batch, Ok)
    }

    /// Like [`Self::restage_append`], applying `normalize` to each bounded
    /// existing batch before it is written under the destination schema.
    pub fn restage_append_with(
        &mut self,
        final_path: &Path,
        schema: SchemaRef,
        batch: &RecordBatch,
        mut normalize: impl FnMut(RecordBatch) -> Result<RecordBatch, GfError>,
    ) -> Result<u64, GfError> {
        let read_path = self
            .staged_temp(final_path)
            .map_or_else(|| final_path.to_path_buf(), Path::to_path_buf);
        let parent = final_path.parent().ok_or_else(|| {
            GfError::Storage(format!(
                "staged path {} has no parent directory",
                final_path.display()
            ))
        })?;
        std::fs::create_dir_all(parent).map_err(|error| io_err(&error))?;
        let file_name = final_path.file_name().map_or_else(
            || "staged".to_owned(),
            |name| name.to_string_lossy().into_owned(),
        );
        let tmp = tempfile::Builder::new()
            .prefix(&format!("{file_name}."))
            .suffix(".tmp")
            .tempfile_in(parent)
            .map_err(|error| io_err(&error))?;
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(ROW_GROUP_SIZE))
            .build();
        let mut writer =
            ArrowWriter::try_new(tmp.as_file(), schema, Some(properties)).map_err(pq_err)?;
        let mut existing_rows = 0_u64;
        if read_path.exists() {
            let input = std::fs::File::open(&read_path).map_err(|error| io_err(&error))?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(input)
                .map_err(pq_err)?
                .with_batch_size(ROW_GROUP_SIZE)
                .build()
                .map_err(pq_err)?;
            for existing in reader {
                let existing = existing.map_err(pq_err)?;
                existing_rows = existing_rows.saturating_add(existing.num_rows() as u64);
                crate::io_stats::record_topology_rewrite_batch(existing.num_rows() as u64);
                let existing = normalize(existing)?;
                writer.write(&existing).map_err(pq_err)?;
            }
        }
        crate::io_stats::record_topology_rewrite_batch(batch.num_rows() as u64);
        writer.write(batch).map_err(pq_err)?;
        writer.close().map_err(pq_err)?;
        if let Some(entry) = self.staged.iter_mut().find(|(_, path)| path == final_path) {
            entry.0 = tmp;
        } else {
            self.staged.push((tmp, final_path.to_path_buf()));
        }
        Ok(existing_rows)
    }

    /// The staged temp file currently holding `final_path`'s replacement
    /// content, if any. Readers that must see this statement's
    /// already-staged effects (rather than the committed file) read through
    /// this path.
    #[must_use]
    pub fn staged_temp(&self, final_path: &Path) -> Option<&Path> {
        self.staged
            .iter()
            .find(|(_, p)| p == final_path)
            .map(|(tmp, _)| tmp.path())
    }

    /// Rename every staged file into place, in insertion order.
    ///
    /// # Errors
    /// Returns [`GfError`] on a rename failure; files renamed before the
    /// failure stay committed (see the module docs for the consistency bound),
    /// and the remaining temps are removed on drop.
    pub fn commit(self) -> Result<(), GfError> {
        if let Some(root) = self
            .property_windows
            .values()
            .next()
            .map(|window| window.project_root.clone())
        {
            crate::generation::commit_topology_aware(self, &root)?;
            return Ok(());
        }
        let non_empty = !self.staged.is_empty();
        for (tmp, final_path) in self.staged {
            tmp.persist(&final_path)
                .map_err(|e| io_err(&e.error))
                .map(|_| ())?;
        }
        if non_empty {
            crate::io_stats::record_rewrite_commit();
        }
        Ok(())
    }

    /// The staged destination paths, in insertion (= commit) order.
    pub fn staged_paths(&self) -> impl Iterator<Item = &Path> {
        self.staged.iter().map(|(_, p)| p.as_path())
    }

    /// Whether nothing has been staged.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.staged.is_empty() && self.property_windows.is_empty()
    }

    pub(crate) fn has_property_windows(&self) -> bool {
        !self.property_windows.is_empty()
    }

    pub(crate) fn has_node_property_windows(&self) -> bool {
        self.property_windows
            .keys()
            .any(|key| key.kind == crate::property_overlay::PropertyRouteKind::Node)
    }

    #[cfg(test)]
    pub(crate) fn property_window_count(&self) -> usize {
        self.property_windows.len()
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "window takes ownership of first-route metadata"
    )]
    pub(crate) fn accumulate_property_window(
        &mut self,
        project_root: &Path,
        kind: crate::property_overlay::PropertyRouteKind,
        route: &str,
        rows: impl IntoIterator<Item = crate::property_overlay::PropertySnapshotRow>,
        metadata: HashMap<String, String>,
        mode: PropertyWindowMode,
    ) -> Result<(), GfError> {
        let key = PropertyWindowKey {
            kind,
            route: route.to_owned(),
        };
        let window = self
            .property_windows
            .entry(key)
            .or_insert_with(|| PendingPropertyWindow {
                project_root: project_root.to_path_buf(),
                rows: BTreeMap::new(),
                metadata: metadata.clone(),
            });
        if window.project_root != project_root || window.metadata != metadata {
            return Err(GfError::Storage(
                "property window root or semantic metadata conflicts".into(),
            ));
        }
        for row in rows {
            let uuid = row.uuid;
            match (window.rows.get_mut(&uuid), mode, row.tombstone) {
                (Some(pending), PropertyWindowMode::Patch, false)
                    if !pending.snapshot.tombstone =>
                {
                    pending.snapshot.values.extend(row.values);
                }
                _ => {
                    window.rows.insert(
                        uuid,
                        PendingPropertyRow {
                            snapshot: row,
                            mode,
                        },
                    );
                }
            }
        }
        Ok(())
    }

    pub(crate) fn property_window_rows(
        &self,
        kind: crate::property_overlay::PropertyRouteKind,
        route: &str,
    ) -> Option<&BTreeMap<[u8; 16], PendingPropertyRow>> {
        self.property_windows
            .get(&PropertyWindowKey {
                kind,
                route: route.to_owned(),
            })
            .map(|window| &window.rows)
    }

    pub(crate) fn take_property_windows(
        &mut self,
    ) -> BTreeMap<PropertyWindowKey, PendingPropertyWindow> {
        std::mem::take(&mut self.property_windows)
    }

    pub(crate) fn into_staged(self) -> Vec<(NamedTempFile, PathBuf)> {
        self.staged
    }

    pub(crate) fn move_staged_destination_to_end(&mut self, destination: &Path) {
        if let Some(index) = self.staged.iter().position(|(_, path)| path == destination) {
            let entry = self.staged.remove(index);
            self.staged.push(entry);
        }
    }
}

/// Parquet row-group size for all staged files. Smaller than the 1 M-row
/// default so a localized filtered read (`read_edges_filtered` /
/// `read_nodes_filtered`, #830/#838) prunes to ~one row group by `edge_id` /
/// `node_id` min/max statistics — making a k-hop traversal's decode cost
/// proportional to its neighborhood rather than the whole table, independent of
/// total graph size. (The default would put a 625 k-node file in a single row
/// group, defeating pruning.) Bulk full-table scans pay only a little extra
/// row-group metadata; the engine targets small-to-medium graphs.
const ROW_GROUP_SIZE: usize = 64 * 1024;

/// Write `batch` to a fresh temp file in `final_path`'s directory and return
/// it. The temp lives next to its destination so the eventual rename is a
/// same-filesystem atomic replace; its `.tmp` extension keeps it invisible to
/// every Parquet reader in this crate.
fn stage_parquet_temp(
    final_path: &Path,
    schema: SchemaRef,
    batch: &RecordBatch,
) -> Result<NamedTempFile, GfError> {
    let parent = final_path.parent().ok_or_else(|| {
        GfError::Storage(format!(
            "staged path {} has no parent directory",
            final_path.display()
        ))
    })?;
    std::fs::create_dir_all(parent).map_err(|e| io_err(&e))?;
    let file_name = final_path
        .file_name()
        .map_or_else(|| "staged".to_owned(), |n| n.to_string_lossy().into_owned());
    let tmp = tempfile::Builder::new()
        .prefix(&format!("{file_name}."))
        .suffix(".tmp")
        .tempfile_in(parent)
        .map_err(|e| io_err(&e))?;

    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROW_GROUP_SIZE))
        .build();
    let mut writer = ArrowWriter::try_new(tmp.as_file(), schema, Some(props)).map_err(pq_err)?;
    writer.write(batch).map_err(pq_err)?;
    writer.close().map_err(pq_err)?;
    Ok(tmp)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use tempfile::TempDir;

    use super::*;

    fn int_batch(values: &[i64]) -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap();
        (schema, batch)
    }

    fn read_values(path: &Path) -> Vec<i64> {
        let file = std::fs::File::open(path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let mut out = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            out.extend(col.values().iter().copied());
        }
        out
    }

    fn tmp_entries(dir: &Path) -> usize {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.path().extension().is_some_and(|x| x == "tmp"))
            .count()
    }

    #[test]
    fn append_rewrite_copies_existing_topology_in_bounded_batches() {
        let _measurement = crate::io_stats::test_measurement_guard();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("topology/nodes.parquet");
        let initial_values = (0..(ROW_GROUP_SIZE * 2 + 17))
            .map(|value| i64::try_from(value).unwrap())
            .collect::<Vec<_>>();
        let (schema, initial) = int_batch(&initial_values);
        let mut first = RewriteBatch::new();
        first.stage(&path, Arc::clone(&schema), &initial).unwrap();
        first.commit().unwrap();

        crate::io_stats::reset();
        let (_, appended) = int_batch(&[900_001, 900_002, 900_003]);
        let mut rewrite = RewriteBatch::new();
        let existing = rewrite
            .restage_append(&path, Arc::clone(&schema), &appended)
            .unwrap();
        rewrite.commit().unwrap();

        assert_eq!(existing, initial_values.len() as u64);
        let io = crate::io_stats::snapshot();
        assert_eq!(io.topology_rewrite_peak_batch_rows, ROW_GROUP_SIZE as u64);
        let values = read_values(&path);
        assert_eq!(&values[..initial_values.len()], initial_values.as_slice());
        assert_eq!(
            &values[initial_values.len()..],
            &[900_001, 900_002, 900_003]
        );
    }

    #[test]
    fn stale_temp_cleanup_is_scoped_and_pattern_checked() {
        let dir = TempDir::new().unwrap();
        let topology = dir.path().join("topology");
        let edges = topology.join("edges");
        let properties = dir.path().join("properties");
        let unrelated = dir.path().join("notes");
        for path in [&edges, &properties, &unrelated] {
            std::fs::create_dir_all(path).unwrap();
        }

        let stale = [
            topology.join("nodes.parquet.Abc123.tmp"),
            topology.join("generation.json.Xyz789.tmp"),
            edges.join("KNOWS.parquet.Qwe456.tmp"),
            properties.join("Person.parquet.Rty012.tmp"),
        ];
        for path in &stale {
            std::fs::write(path, b"stale").unwrap();
        }
        let preserved = [
            topology.join("notes.tmp"),
            topology.join("nodes.parquet.bad-name.tmp"),
            properties.join("Person.parquet"),
            unrelated.join("Other.parquet.Abc123.tmp"),
        ];
        for path in &preserved {
            std::fs::write(path, b"keep").unwrap();
        }

        assert_eq!(remove_stale_temps(dir.path()).unwrap(), stale.len());
        assert!(stale.iter().all(|path| !path.exists()));
        assert!(preserved.iter().all(|path| path.exists()));
    }

    /// Single-file stage + commit, the test fixture writer.
    fn write_parquet(path: &Path, schema: SchemaRef, batch: &RecordBatch) -> Result<(), GfError> {
        let mut staged = RewriteBatch::new();
        staged.stage(path, schema, batch)?;
        staged.commit()
    }

    #[test]
    fn stage_leaves_target_untouched_until_commit() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("data.parquet");
        let (schema, before) = int_batch(&[1, 2, 3]);
        write_parquet(&path, Arc::clone(&schema), &before).unwrap();

        let (schema2, after) = int_batch(&[9]);
        let mut batch = RewriteBatch::new();
        batch.stage(&path, schema2, &after).unwrap();
        assert_eq!(read_values(&path), vec![1, 2, 3], "no change before commit");

        batch.commit().unwrap();
        assert_eq!(read_values(&path), vec![9], "replacement visible on commit");
    }

    #[test]
    fn commit_applies_all_files_in_insertion_order() {
        let dir = TempDir::new().unwrap();
        let a = dir.path().join("a.parquet");
        let b = dir.path().join("sub").join("b.parquet");
        let (schema, content) = int_batch(&[7]);

        let mut batch = RewriteBatch::new();
        batch.stage(&a, Arc::clone(&schema), &content).unwrap();
        batch.stage(&b, Arc::clone(&schema), &content).unwrap();
        // The insertion-order contract commit() consumes — callers encode
        // their safety ordering through it.
        let order: Vec<_> = batch.staged_paths().collect();
        assert_eq!(order, vec![a.as_path(), b.as_path()]);

        batch.commit().unwrap();
        assert_eq!(read_values(&a), vec![7]);
        assert_eq!(read_values(&b), vec![7]);
        assert_eq!(tmp_entries(dir.path()), 0, "no temp residue at root");
        assert_eq!(tmp_entries(&dir.path().join("sub")), 0, "none in subdir");
    }

    #[test]
    fn drop_without_commit_removes_temps_and_preserves_originals() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("data.parquet");
        let (schema, before) = int_batch(&[4, 5]);
        write_parquet(&path, Arc::clone(&schema), &before).unwrap();

        {
            let (schema2, after) = int_batch(&[6]);
            let mut batch = RewriteBatch::new();
            batch.stage(&path, schema2, &after).unwrap();
            assert_eq!(tmp_entries(dir.path()), 1, "temp exists while staged");
            // Dropped without commit — the abort path.
        }
        assert_eq!(tmp_entries(dir.path()), 0, "abort removed the temp");
        assert_eq!(read_values(&path), vec![4, 5], "original intact");
    }

    #[test]
    fn sequential_commits_replace_existing_and_leave_no_temp() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("data.parquet");
        let (schema, first) = int_batch(&[1]);
        write_parquet(&path, schema, &first).unwrap();
        let (schema, second) = int_batch(&[2, 3]);
        write_parquet(&path, schema, &second).unwrap();

        assert_eq!(read_values(&path), vec![2, 3]);
        assert_eq!(tmp_entries(dir.path()), 0);
    }

    #[test]
    fn restage_replaces_in_place_and_staged_temp_reads_through() {
        // The #792 composition contract: read the current staged content via
        // staged_temp, apply a change, restage — the entry keeps its commit
        // position and the old temp is cleaned up.
        let dir = TempDir::new().unwrap();
        let a = dir.path().join("a.parquet");
        let b = dir.path().join("b.parquet");
        let (schema, content) = int_batch(&[1]);

        let mut batch = RewriteBatch::new();
        batch.stage(&a, Arc::clone(&schema), &content).unwrap();
        batch.stage(&b, Arc::clone(&schema), &content).unwrap();

        // Read through: the staged temp for `a` holds [1].
        let tmp_a = batch.staged_temp(&a).expect("a is staged").to_path_buf();
        assert_eq!(read_values(&tmp_a), vec![1]);

        // Restage `a` with new content: position kept (still before b),
        // exactly one temp per destination.
        let (schema2, newer) = int_batch(&[5, 6]);
        batch.restage(&a, schema2, &newer).unwrap();
        let order: Vec<_> = batch.staged_paths().collect();
        assert_eq!(order, vec![a.as_path(), b.as_path()], "position kept");
        assert_eq!(tmp_entries(dir.path()), 2, "replaced temp was removed");

        batch.commit().unwrap();
        assert_eq!(read_values(&a), vec![5, 6]);
        assert_eq!(read_values(&b), vec![1]);
    }

    #[test]
    fn stage_creates_missing_parent_dir() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("properties").join("NewStem.parquet");
        let (schema, content) = int_batch(&[42]);

        let mut batch = RewriteBatch::new();
        batch.stage(&path, schema, &content).unwrap();
        batch.commit().unwrap();
        assert_eq!(read_values(&path), vec![42]);
    }
}
