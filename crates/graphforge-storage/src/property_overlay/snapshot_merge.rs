//! Snapshot merge for authenticated property overlays.

use super::{
    BTreeMap, BinaryHeap, BufRead, BufReader, BufWriter, Deserialize, File, GfError,
    IntoPropertySnapshotResult, IrLiteral, LiveByteBudget, Path, PathBuf, PropertyFragmentId,
    PropertyOverlayLimits, PropertyOverlayMetrics, PropertySnapshotRow, Read, Reverse, Serialize,
    Write, corrupt, fs,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SpoolRecord {
    uuid: [u8; 16],
    generation: u64,
    ordinal: u64,
    tombstone: bool,
    values: BTreeMap<String, IrLiteral>,
}

#[derive(Debug, Default)]
struct RunLevels {
    levels: Vec<Vec<PathBuf>>,
    next_ordinal: u64,
}

impl RunLevels {
    fn add<F>(
        &mut self,
        root: &Path,
        mut level: usize,
        mut path: PathBuf,
        fan_in: usize,
        budget: &LiveByteBudget,
        metrics: &mut PropertyOverlayMetrics,
    ) -> Result<(), GfError>
    where
        F: FnMut(PropertySnapshotRow) -> Result<(), GfError>,
    {
        loop {
            if self.levels.len() <= level {
                self.levels.resize_with(level + 1, Vec::new);
            }
            self.levels[level].push(path);
            metrics.peak_run_references = metrics.peak_run_references.max(
                u64::try_from(self.levels.iter().map(Vec::len).sum::<usize>()).unwrap_or(u64::MAX),
            );
            if self.levels[level].len() < fan_in {
                return Ok(());
            }
            let inputs = std::mem::take(&mut self.levels[level]);
            path = root.join(format!("level-{level}-{}.jsonl", self.next_ordinal));
            self.next_ordinal = self
                .next_ordinal
                .checked_add(1)
                .ok_or_else(|| corrupt("property run ordinal overflow"))?;
            merge_runs::<F>(&inputs, &path, None, budget, metrics)?;
            for input in inputs {
                fs::remove_file(input).map_err(io_error)?;
            }
            metrics.merge_passes = metrics.merge_passes.saturating_add(1);
            level = level
                .checked_add(1)
                .ok_or_else(|| corrupt("property run level overflow"))?;
        }
    }

    fn finish(self) -> Vec<PathBuf> {
        self.levels.into_iter().flatten().collect()
    }
}

impl SpoolRecord {
    fn sort_key(&self) -> ([u8; 16], Reverse<(u64, u64)>) {
        (self.uuid, Reverse((self.generation, self.ordinal)))
    }
}

impl IntoPropertySnapshotResult for PropertySnapshotRow {
    fn into_property_snapshot_result(self) -> Result<PropertySnapshotRow, GfError> {
        Ok(self)
    }
}

impl IntoPropertySnapshotResult for Result<PropertySnapshotRow, GfError> {
    fn into_property_snapshot_result(self) -> Result<PropertySnapshotRow, GfError> {
        self
    }
}

/// Bounded disk-backed newest-snapshot merge shared by property consumers.
///
/// Input rows may arrive in any fragment order. Runs are externally sorted by
/// UUID and descending numeric fragment authority. The final pass emits at
/// most one live row per UUID and suppresses a newest tombstone. No input path
/// is sought per record.
#[allow(
    clippy::too_many_lines,
    reason = "bounded merge accounting stays co-located"
)]
pub(crate) fn visit_newest_property_snapshots<I, R, F>(
    inputs: I,
    scratch: &Path,
    limits: PropertyOverlayLimits,
    budget: &LiveByteBudget,
    mut emit: F,
) -> Result<PropertyOverlayMetrics, GfError>
where
    I: IntoIterator<Item = (PropertyFragmentId, u64, u64, R)>,
    R: IntoIterator,
    R::Item: IntoPropertySnapshotResult,
    F: FnMut(PropertySnapshotRow) -> Result<(), GfError>,
{
    if limits.max_buffered_rows == 0
        || limits.max_open_runs < 2
        || limits.max_buffered_bytes == 0
        || limits.max_row_bytes == 0
        || limits.max_row_bytes > limits.max_buffered_bytes
    {
        return Err(corrupt("property overlay merge limits are invalid"));
    }
    fs::create_dir_all(scratch).map_err(io_error)?;
    let temp = tempfile::Builder::new()
        .prefix("property-overlay-")
        .tempdir_in(scratch)
        .map_err(io_error)?;
    let mut metrics = PropertyOverlayMetrics::default();
    let mut buffer = Vec::with_capacity(limits.max_buffered_rows);
    let mut runs = RunLevels::default();
    let mut next_run = 0_usize;
    let mut buffered_bytes = 0_u64;
    for (id, physical_bytes, read_calls, rows) in inputs {
        metrics.fragments_considered = metrics.fragments_considered.saturating_add(1);
        metrics.physical_bytes = metrics.physical_bytes.saturating_add(physical_bytes);
        metrics.read_calls = metrics.read_calls.saturating_add(read_calls);
        let mut prior = None;
        for row in rows {
            let row = row.into_property_snapshot_result()?;
            if row.tombstone && !row.values.is_empty() {
                return Err(corrupt("property tombstone carries live values"));
            }
            if prior.is_some_and(|uuid| uuid >= row.uuid) {
                return Err(corrupt("property fragment UUIDs are duplicate or unsorted"));
            }
            prior = Some(row.uuid);
            metrics.physical_rows = metrics.physical_rows.saturating_add(1);
            let record = SpoolRecord {
                uuid: row.uuid,
                generation: id.generation,
                ordinal: id.ordinal,
                tombstone: row.tombstone,
                values: row.values,
            };
            let charge = record_charge(&record);
            if charge > limits.max_row_bytes {
                return Err(corrupt("property snapshot row exceeds byte limit"));
            }
            if !buffer.is_empty()
                && (buffered_bytes
                    .checked_add(charge)
                    .is_none_or(|bytes| bytes > limits.max_buffered_bytes)
                    || !budget.can_charge(charge))
            {
                let run = write_sorted_run(temp.path(), next_run, &mut buffer, &mut metrics)?;
                budget.release(buffered_bytes);
                next_run = next_run
                    .checked_add(1)
                    .ok_or_else(|| corrupt("property run ordinal overflow"))?;
                runs.add::<F>(
                    temp.path(),
                    0,
                    run,
                    limits.max_open_runs,
                    budget,
                    &mut metrics,
                )?;
                buffered_bytes = 0;
            }
            buffered_bytes = buffered_bytes
                .checked_add(charge)
                .ok_or_else(|| corrupt("property snapshot byte charge overflows"))?;
            budget.charge(charge)?;
            buffer.push(record);
            metrics.peak_buffered_rows = metrics
                .peak_buffered_rows
                .max(u64::try_from(buffer.len()).unwrap_or(u64::MAX));
            metrics.peak_buffered_bytes = metrics.peak_buffered_bytes.max(buffered_bytes);
            if buffer.len() == limits.max_buffered_rows {
                let run = write_sorted_run(temp.path(), next_run, &mut buffer, &mut metrics)?;
                budget.release(buffered_bytes);
                next_run = next_run
                    .checked_add(1)
                    .ok_or_else(|| corrupt("property run ordinal overflow"))?;
                runs.add::<F>(
                    temp.path(),
                    0,
                    run,
                    limits.max_open_runs,
                    budget,
                    &mut metrics,
                )?;
                buffered_bytes = 0;
            }
        }
    }
    if !buffer.is_empty() {
        let run = write_sorted_run(temp.path(), next_run, &mut buffer, &mut metrics)?;
        budget.release(buffered_bytes);
        runs.add::<F>(
            temp.path(),
            0,
            run,
            limits.max_open_runs,
            budget,
            &mut metrics,
        )?;
    }
    let mut runs = runs.finish();
    while runs.len() > limits.max_open_runs {
        let mut next = Vec::new();
        for (group, chunk) in runs.chunks(limits.max_open_runs).enumerate() {
            let path = temp
                .path()
                .join(format!("pass-{}-{group}.jsonl", metrics.merge_passes));
            merge_runs::<F>(chunk, &path, None, budget, &mut metrics)?;
            next.push(path);
            for input in chunk {
                fs::remove_file(input).map_err(io_error)?;
            }
        }
        metrics.merge_passes = metrics.merge_passes.saturating_add(1);
        runs = next;
    }
    if !runs.is_empty() {
        merge_runs(
            &runs,
            &temp.path().join("final.jsonl"),
            Some(&mut emit),
            budget,
            &mut metrics,
        )?;
        metrics.merge_passes = metrics.merge_passes.saturating_add(1);
    }
    Ok(metrics)
}

fn write_sorted_run(
    root: &Path,
    ordinal: usize,
    rows: &mut Vec<SpoolRecord>,
    metrics: &mut PropertyOverlayMetrics,
) -> Result<PathBuf, GfError> {
    rows.sort_unstable_by_key(SpoolRecord::sort_key);
    let path = root.join(format!("run-{ordinal}.jsonl"));
    let mut writer = BufWriter::new(File::create(&path).map_err(io_error)?);
    for row in rows.drain(..) {
        serde_json::to_writer(&mut writer, &row).map_err(json_error)?;
        writer.write_all(b"\n").map_err(io_error)?;
    }
    writer.flush().map_err(io_error)?;
    let bytes = writer.get_ref().metadata().map_err(io_error)?.len();
    metrics.spill_runs = metrics.spill_runs.saturating_add(1);
    metrics.spill_bytes = metrics.spill_bytes.saturating_add(bytes);
    metrics.spool_input_bytes = metrics
        .spool_input_bytes
        .checked_add(bytes)
        .ok_or_else(|| corrupt("property spool input byte metric overflows"))?;
    Ok(path)
}

fn record_charge(record: &SpoolRecord) -> u64 {
    let values = serde_json::to_vec(&record.values).map_or(u64::MAX, |encoded| {
        u64::try_from(encoded.len()).unwrap_or(u64::MAX)
    });
    16_u64
        .saturating_add(8)
        .saturating_add(8)
        .saturating_add(1)
        .saturating_add(values)
}

#[cfg(test)]
thread_local! {
    // Count actual serialization work on this test thread, without changing
    // production counters or depending on elapsed time and host load.
    pub(super) static SNAPSHOT_CHARGE_CALLS: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

pub(crate) fn snapshot_charge(row: &PropertySnapshotRow) -> u64 {
    #[cfg(test)]
    SNAPSHOT_CHARGE_CALLS.with(|calls| {
        if let Some(count) = calls.get() {
            calls.set(Some(count + 1));
        }
    });
    let values = serde_json::to_vec(&row.values).map_or(u64::MAX, |encoded| {
        u64::try_from(encoded.len()).unwrap_or(u64::MAX)
    });
    16_u64.saturating_add(1).saturating_add(values)
}

fn merge_runs<F>(
    runs: &[PathBuf],
    output: &Path,
    mut emit: Option<&mut F>,
    budget: &LiveByteBudget,
    metrics: &mut PropertyOverlayMetrics,
) -> Result<(), GfError>
where
    F: FnMut(PropertySnapshotRow) -> Result<(), GfError>,
{
    let mut readers = runs
        .iter()
        .map(|path| File::open(path).map(BufReader::new).map_err(io_error))
        .collect::<Result<Vec<_>, _>>()?;
    let mut current = Vec::with_capacity(readers.len());
    let mut current_charges = Vec::with_capacity(readers.len());
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        current.push(read_spool(reader, budget.max)?);
        current_charges.push(current[index].as_ref().map_or(0, record_charge));
        if let Some(row) = &current[index] {
            heap.push(Reverse((row.sort_key(), index)));
        }
    }
    let mut cursor_bytes = current_charges
        .iter()
        .fold(0_u64, |total, charge| total.saturating_add(*charge));
    budget.charge(cursor_bytes)?;
    metrics.peak_buffered_rows = metrics
        .peak_buffered_rows
        .max(u64::try_from(current.iter().flatten().count()).unwrap_or(u64::MAX));
    metrics.peak_buffered_bytes = metrics.peak_buffered_bytes.max(cursor_bytes);
    let mut writer = (emit.is_none())
        .then(|| File::create(output).map(BufWriter::new).map_err(io_error))
        .transpose()?;
    // Every run is ordered by UUID and newest authority first. Resolve a UUID
    // in every merge, rather than carrying all of its shadowed history through
    // each level. This keeps intermediate I/O proportional to the live sparse
    // overlay instead of multiplying historical rows by the number of merge
    // levels.
    let mut resolved_uuid = None;
    while let Some(Reverse((_, index))) = heap.pop() {
        let row = current[index].take().expect("heap row exists");
        let prior_charge = std::mem::take(&mut current_charges[index]);
        budget.release(prior_charge);
        cursor_bytes = cursor_bytes.saturating_sub(prior_charge);
        let newest = resolved_uuid != Some(row.uuid);
        if newest {
            resolved_uuid = Some(row.uuid);
            if emit.is_some() {
                if row.tombstone {
                    metrics.tombstones = metrics.tombstones.saturating_add(1);
                } else if let Some(visitor) = emit.as_deref_mut() {
                    visitor(PropertySnapshotRow {
                        uuid: row.uuid,
                        tombstone: false,
                        values: row.values.clone(),
                    })?;
                    metrics.logical_rows = metrics.logical_rows.saturating_add(1);
                }
            }
        } else {
            metrics.shadowed_rows = metrics.shadowed_rows.saturating_add(1);
        }
        if newest && let Some(out) = writer.as_mut() {
            serde_json::to_writer(&mut *out, &row).map_err(json_error)?;
            out.write_all(b"\n").map_err(io_error)?;
        }
        current[index] = read_spool(&mut readers[index], budget.max)?;
        if let Some(next) = &current[index] {
            let next_charge = record_charge(next);
            budget.charge(next_charge)?;
            current_charges[index] = next_charge;
            cursor_bytes = cursor_bytes.saturating_add(next_charge);
        }
        metrics.peak_buffered_rows = metrics
            .peak_buffered_rows
            .max(u64::try_from(current.iter().flatten().count()).unwrap_or(u64::MAX));
        metrics.peak_buffered_bytes = metrics.peak_buffered_bytes.max(cursor_bytes);
        if let Some(next) = &current[index] {
            heap.push(Reverse((next.sort_key(), index)));
        }
    }
    if let Some(out) = writer.as_mut() {
        out.flush().map_err(io_error)?;
        let bytes = out.get_ref().metadata().map_err(io_error)?.len();
        metrics.spill_runs = metrics.spill_runs.saturating_add(1);
        metrics.spill_bytes = metrics.spill_bytes.saturating_add(bytes);
    }
    Ok(())
}

fn read_spool(
    reader: &mut BufReader<File>,
    max_encoded_bytes: u64,
) -> Result<Option<SpoolRecord>, GfError> {
    let mut line = String::new();
    let read = reader
        .take(max_encoded_bytes.saturating_add(1))
        .read_line(&mut line)
        .map_err(io_error)?;
    if read == 0 {
        return Ok(None);
    }
    if u64::try_from(read).unwrap_or(u64::MAX) > max_encoded_bytes || !line.ends_with('\n') {
        return Err(corrupt("property spill record exceeds byte limit"));
    }
    serde_json::from_str(&line).map(Some).map_err(json_error)
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "used directly as Result::map_err adapter"
)]
pub(super) fn io_error(error: std::io::Error) -> GfError {
    GfError::Storage(format!("property overlay I/O: {error}"))
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "used directly as Result::map_err adapter"
)]
pub(super) fn json_error(error: serde_json::Error) -> GfError {
    GfError::Storage(format!("property overlay spool: {error}"))
}

#[cfg(test)]
mod tests;
