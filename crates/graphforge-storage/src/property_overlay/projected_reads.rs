//! Projected reads for authenticated property overlays.

use super::{
    Arc, Array, AtomicU64, AuthenticatedPropertyFragment, AuthenticatedPropertyInventory, BTreeMap,
    BTreeSet, BooleanArray, CountingChunkReader, File, FragmentHandleGuard, GfError,
    LiveByteBudget, Mutex, Ordering, PROPERTY_TOMBSTONE_FIELD, ParquetError,
    ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder, Path, PropertyInventoryOpenMetrics,
    PropertyOverlayLimits, PropertyOverlayMetrics, PropertyRouteKind, PropertySnapshotRow,
    ReadCounts, RecordBatch, admitted_batch_rows, authenticated_property_inventory_for_route,
    corrupt, io_error, read_property_targets, snapshot_charge, validate_fragment_schema,
    validate_parquet_resource_admission, visit_newest_property_snapshots,
};

impl AuthenticatedPropertyInventory {
    /// Visit one authenticated route through the retained generation
    /// capability, opening and closing one Parquet decoder at a time.
    #[allow(
        clippy::too_many_lines,
        reason = "authenticated decoder admission and exact accounting stay co-located"
    )]
    pub fn visit_route<F>(
        &self,
        kind: PropertyRouteKind,
        route: &str,
        scratch: &Path,
        limits: PropertyOverlayLimits,
        emit: F,
    ) -> Result<PropertyOverlayMetrics, GfError>
    where
        F: FnMut(PropertySnapshotRow) -> Result<(), GfError>,
    {
        self.visit_route_projected(kind, route, scratch, limits, None, emit)
    }

    /// Visit one route while decoding only selected property columns plus the
    /// UUID and tombstone keys required by newest-wins overlay semantics.
    pub(crate) fn visit_route_projected<F>(
        &self,
        kind: PropertyRouteKind,
        route: &str,
        scratch: &Path,
        limits: PropertyOverlayLimits,
        selected_properties: Option<&BTreeSet<String>>,
        emit: F,
    ) -> Result<PropertyOverlayMetrics, GfError>
    where
        F: FnMut(PropertySnapshotRow) -> Result<(), GfError>,
    {
        let Some(fragments) = self.routes.get(&(kind, route.to_owned())) else {
            return Ok(PropertyOverlayMetrics::default());
        };
        let counts = Arc::new(ReadCounts::default());
        let authentication_bytes = Arc::new(AtomicU64::new(0));
        let authentication_block_equivalents = Arc::new(AtomicU64::new(0));
        let authentication_read_calls = Arc::new(AtomicU64::new(0));
        let budget = Arc::new(LiveByteBudget::new(limits.max_buffered_bytes));
        let decoded = Arc::new(Mutex::new(DecodedRetention::default()));
        let reader_context = ProjectedReaderContext {
            inventory: self,
            scratch,
            limits,
            kind,
            route,
            selected_properties,
            counts: &counts,
            budget: &budget,
            decoded: &decoded,
            authentication_bytes: &authentication_bytes,
            authentication_block_equivalents: &authentication_block_equivalents,
            authentication_read_calls: &authentication_read_calls,
        };
        let inputs = fragments.iter().map(|fragment| {
            let reader = open_projected_fragment(fragment, &reader_context);
            let (reader, pending_error, page_reservation_bytes, handle) = match reader {
                Ok((reader, page_reservation_bytes, _file, handle)) => {
                    (Some(reader), None, page_reservation_bytes, Some(handle))
                }
                Err(error) => (None, Some(error), 0, None),
            };
            (
                fragment.id,
                0,
                0,
                PropertyParquetRows {
                    reader,
                    current: Vec::new().into_iter(),
                    uuid_field: kind.uuid_field(),
                    pending_error,
                    decoded: Arc::clone(&decoded),
                    budget: Arc::clone(&budget),
                    max_row_bytes: limits.max_row_bytes,
                    page_reservation_bytes,
                    batch_reservation_bytes: limits.max_buffered_bytes / 4,
                    _handle: handle,
                    #[cfg(test)]
                    late_failure_row_countdown: Arc::clone(
                        &self.late_decoder_failure_row_countdown,
                    ),
                },
            )
        });
        let mut metrics =
            visit_newest_property_snapshots(inputs, scratch, limits, budget.as_ref(), emit)?;
        finalize_projected_metrics(
            &mut metrics,
            &ProjectedMetricSources {
                counts: &counts,
                authentication_bytes: &authentication_bytes,
                authentication_block_equivalents: &authentication_block_equivalents,
                authentication_read_calls: &authentication_read_calls,
                decoded: &decoded,
                budget: budget.as_ref(),
                authenticated_snapshot_peak_bytes: fragments
                    .iter()
                    .map(|fragment| fragment.entry.byte_length)
                    .max()
                    .unwrap_or(0),
            },
        );
        Ok(metrics)
    }
}

struct ProjectedReaderContext<'a> {
    inventory: &'a AuthenticatedPropertyInventory,
    scratch: &'a Path,
    limits: PropertyOverlayLimits,
    kind: PropertyRouteKind,
    route: &'a str,
    selected_properties: Option<&'a BTreeSet<String>>,
    counts: &'a Arc<ReadCounts>,
    budget: &'a Arc<LiveByteBudget>,
    decoded: &'a Arc<Mutex<DecodedRetention>>,
    authentication_bytes: &'a Arc<AtomicU64>,
    authentication_block_equivalents: &'a Arc<AtomicU64>,
    authentication_read_calls: &'a Arc<AtomicU64>,
}

fn open_projected_fragment(
    fragment: &AuthenticatedPropertyFragment,
    context: &ProjectedReaderContext<'_>,
) -> Result<
    (
        ParquetRecordBatchReader,
        u64,
        Arc<File>,
        FragmentHandleGuard,
    ),
    GfError,
> {
    let opened = context.inventory.open_fragment(fragment, context.scratch)?;
    context
        .authentication_bytes
        .fetch_add(opened.authentication_bytes, Ordering::Relaxed);
    context
        .authentication_block_equivalents
        .fetch_add(opened.authentication_block_equivalents, Ordering::Relaxed);
    context
        .authentication_read_calls
        .fetch_add(opened.authentication_read_calls, Ordering::Relaxed);
    let source = CountingChunkReader {
        length: fragment.entry.byte_length,
        file: Arc::clone(&opened.file),
        counts: Arc::clone(context.counts),
    };
    let builder = ParquetRecordBatchReaderBuilder::try_new(source).map_err(parquet_error)?;
    validate_fragment_schema(
        builder.schema().as_ref(),
        fragment.id,
        fragment.layout,
        context.kind,
        context.route,
    )?;
    let projected = projected_property_columns(
        builder.schema().as_ref(),
        context.kind,
        context.selected_properties,
    );
    let projection_mask = projected.as_ref().map(|roots| {
        parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), roots.iter().copied())
    });
    let projected_leaves = projection_mask.as_ref().map(|mask| {
        (0..builder.parquet_schema().num_columns())
            .filter(|index| mask.leaf_included(*index))
            .collect::<BTreeSet<_>>()
    });
    let page_reservation_bytes = validate_parquet_resource_admission(
        builder.metadata(),
        context.limits,
        opened.file.as_ref(),
        context.counts,
        projected_leaves.as_ref(),
    )?;
    context.budget.charge(page_reservation_bytes)?;
    {
        let mut retention = context.decoded.lock().expect("property retention lock");
        retention.page_peak = retention.page_peak.max(page_reservation_bytes);
    }
    let builder = if let Some(mask) = projection_mask {
        builder.with_projection(mask)
    } else {
        builder
    };
    let reader = builder
        .with_batch_size(admitted_batch_rows(context.limits))
        .build()
        .map_err(parquet_error)?;
    Ok((reader, page_reservation_bytes, opened.file, opened.handle))
}

struct ProjectedMetricSources<'a> {
    counts: &'a ReadCounts,
    authentication_bytes: &'a AtomicU64,
    authentication_block_equivalents: &'a AtomicU64,
    authentication_read_calls: &'a AtomicU64,
    decoded: &'a Mutex<DecodedRetention>,
    budget: &'a LiveByteBudget,
    authenticated_snapshot_peak_bytes: u64,
}

fn projected_property_columns(
    schema: &arrow::datatypes::Schema,
    kind: PropertyRouteKind,
    selected_properties: Option<&BTreeSet<String>>,
) -> Option<BTreeSet<usize>> {
    selected_properties.map(|selected| {
        schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                field.name() == kind.uuid_field()
                    || field.name() == PROPERTY_TOMBSTONE_FIELD
                    || selected.contains(field.name())
            })
            .map(|(index, _)| index)
            .collect()
    })
}

fn finalize_projected_metrics(
    metrics: &mut PropertyOverlayMetrics,
    sources: &ProjectedMetricSources<'_>,
) {
    metrics.authentication_bytes = sources.authentication_bytes.load(Ordering::Relaxed);
    metrics.authentication_block_equivalents = sources
        .authentication_block_equivalents
        .load(Ordering::Relaxed);
    metrics.authentication_read_calls = sources.authentication_read_calls.load(Ordering::Relaxed);
    metrics.property_authentication_bytes = metrics.authentication_bytes;
    metrics.authenticated_snapshot_bytes = metrics.authentication_bytes;
    metrics.authenticated_snapshot_peak_bytes = sources.authenticated_snapshot_peak_bytes;
    metrics.property_authentication_block_equivalents = metrics.authentication_block_equivalents;
    metrics.property_authentication_read_calls = metrics.authentication_read_calls;
    metrics.validation_bytes = sources.counts.bytes.load(Ordering::Relaxed);
    metrics.physical_bytes = metrics
        .authentication_bytes
        .saturating_add(metrics.validation_bytes);
    metrics.read_calls = sources.counts.blocks.load(Ordering::Relaxed);
    metrics.validation_read_calls = metrics.read_calls;
    metrics.physical_blocks = metrics
        .authentication_read_calls
        .saturating_add(metrics.read_calls);
    metrics.range_seeks = sources.counts.range_seeks.load(Ordering::Relaxed);
    let decoded = sources.decoded.lock().expect("property retention lock");
    metrics.decoder_peak_rows = decoded.peak_rows;
    metrics.decoder_peak_bytes = decoded.peak_bytes;
    metrics.decoder_page_reservation_bytes = decoded.page_peak;
    metrics.emitted_batches = decoded.batches;
    metrics.merge_peak_rows = metrics.peak_buffered_rows;
    metrics.merge_peak_bytes = metrics.peak_buffered_bytes;
    metrics.peak_buffered_rows = metrics
        .decoder_peak_rows
        .saturating_add(metrics.merge_peak_rows);
    metrics.peak_buffered_bytes = sources.budget.peak();
}

struct PropertyParquetRows {
    reader: Option<ParquetRecordBatchReader>,
    current: std::vec::IntoIter<PropertySnapshotRow>,
    uuid_field: &'static str,
    pending_error: Option<GfError>,
    decoded: Arc<Mutex<DecodedRetention>>,
    budget: Arc<LiveByteBudget>,
    max_row_bytes: u64,
    page_reservation_bytes: u64,
    batch_reservation_bytes: u64,
    _handle: Option<FragmentHandleGuard>,
    #[cfg(test)]
    late_failure_row_countdown: Arc<AtomicU64>,
}

impl Drop for PropertyParquetRows {
    fn drop(&mut self) {
        self.budget.release(self.page_reservation_bytes);
    }
}

#[derive(Debug, Default)]
struct DecodedRetention {
    current_rows: u64,
    current_bytes: u64,
    peak_rows: u64,
    peak_bytes: u64,
    batches: u64,
    page_peak: u64,
}

impl Iterator for PropertyParquetRows {
    type Item = Result<PropertySnapshotRow, GfError>;

    #[allow(
        clippy::too_many_lines,
        reason = "fallible decoder state, reservations, and release paths stay auditable together"
    )]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(error) = self.pending_error.take() {
                self.reader = None;
                return Some(Err(error));
            }
            if let Some(row) = self.current.next() {
                #[cfg(test)]
                if self
                    .late_failure_row_countdown
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        if remaining > 0 {
                            Some(remaining - 1)
                        } else {
                            None
                        }
                    })
                    .is_ok_and(|remaining| remaining == 1)
                {
                    self.budget.release(snapshot_charge(&row));
                    self.reader = None;
                    return Some(Err(corrupt(
                        "injected late authenticated property decoder failure",
                    )));
                }
                self.budget.release(snapshot_charge(&row));
                let mut decoded = self.decoded.lock().expect("property retention lock");
                decoded.current_rows = decoded.current_rows.saturating_sub(1);
                decoded.current_bytes = decoded.current_bytes.saturating_sub(snapshot_charge(&row));
                return Some(Ok(row));
            }
            self.reader.as_ref()?;
            if let Err(error) = self.budget.charge(self.batch_reservation_bytes) {
                self.reader = None;
                return Some(Err(error));
            }
            let Some(next_batch) = self.reader.as_mut().expect("reader checked").next() else {
                self.budget.release(self.batch_reservation_bytes);
                self.reader = None;
                return None;
            };
            let batch = match next_batch {
                Ok(batch) => batch,
                Err(error) => {
                    self.budget.release(self.batch_reservation_bytes);
                    self.reader = None;
                    return Some(Err(authenticated_arrow_error(error)));
                }
            };
            let arrow_bytes = u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX);
            if arrow_bytes > self.batch_reservation_bytes {
                self.budget.release(self.batch_reservation_bytes);
                self.reader = None;
                return Some(Err(corrupt(
                    "property Arrow batch exceeds pre-decode live-byte admission",
                )));
            }
            let decode_reservation = self.batch_reservation_bytes;
            if let Err(error) = self.budget.charge(decode_reservation) {
                self.budget.release(self.batch_reservation_bytes);
                self.reader = None;
                return Some(Err(error));
            }
            match decode_snapshot_batch(&batch, self.uuid_field) {
                Ok(rows) => {
                    if rows
                        .iter()
                        .any(|row| snapshot_charge(row) > self.max_row_bytes)
                    {
                        self.budget.release(decode_reservation);
                        self.budget.release(self.batch_reservation_bytes);
                        self.reader = None;
                        return Some(Err(corrupt("property snapshot row exceeds byte limit")));
                    }
                    let bytes = rows.iter().fold(0_u64, |total, row| {
                        total.saturating_add(snapshot_charge(row))
                    });
                    self.budget.release(decode_reservation);
                    if let Err(error) = self.budget.charge(bytes) {
                        self.budget.release(self.batch_reservation_bytes);
                        self.reader = None;
                        return Some(Err(error));
                    }
                    self.budget.release(self.batch_reservation_bytes);
                    let mut decoded = self.decoded.lock().expect("property retention lock");
                    decoded.current_rows = u64::try_from(rows.len()).unwrap_or(u64::MAX);
                    decoded.current_bytes = bytes;
                    decoded.peak_rows = decoded.peak_rows.max(decoded.current_rows);
                    decoded.peak_bytes = decoded.peak_bytes.max(decoded.current_bytes);
                    decoded.batches = decoded.batches.saturating_add(1);
                    drop(decoded);
                    self.current = rows.into_iter();
                }
                Err(error) => {
                    self.budget.release(decode_reservation);
                    self.budget.release(self.batch_reservation_bytes);
                    self.reader = None;
                    return Some(Err(error));
                }
            }
        }
    }
}

/// Scan a route through authenticated retained file handles and emit its
/// full-snapshot-v1 newest live rows. This is the production authority; the
/// generic external merge helper remains crate-internal.
pub fn visit_authenticated_property_snapshots<F>(
    project: &Path,
    kind: PropertyRouteKind,
    route: &str,
    scratch: &Path,
    limits: PropertyOverlayLimits,
    emit: F,
) -> Result<PropertyOverlayMetrics, GfError>
where
    F: FnMut(PropertySnapshotRow) -> Result<(), GfError>,
{
    let inventory = authenticated_property_inventory_for_route(project, kind, route)?;
    let open = inventory.open_metrics();
    let mut metrics = inventory.visit_route(kind, route, scratch, limits, emit)?;
    add_open_metrics(&mut metrics, open);
    Ok(metrics)
}

fn add_open_metrics(metrics: &mut PropertyOverlayMetrics, open: PropertyInventoryOpenMetrics) {
    metrics.authentication_bytes = metrics
        .authentication_bytes
        .saturating_add(open.authentication_bytes);
    metrics.authentication_block_equivalents = metrics
        .authentication_block_equivalents
        .saturating_add(open.authentication_block_equivalents);
    metrics.authentication_read_calls = metrics
        .authentication_read_calls
        .saturating_add(open.authentication_read_calls);
    metrics.authority_authentication_bytes = metrics
        .authority_authentication_bytes
        .saturating_add(open.authority_authentication_bytes);
    metrics.authority_authentication_block_equivalents = metrics
        .authority_authentication_block_equivalents
        .saturating_add(open.authority_authentication_block_equivalents);
    metrics.authority_authentication_read_calls = metrics
        .authority_authentication_read_calls
        .saturating_add(open.authority_authentication_read_calls);
    metrics.property_authentication_bytes = metrics
        .property_authentication_bytes
        .saturating_add(open.property_authentication_bytes);
    metrics.property_authentication_block_equivalents = metrics
        .property_authentication_block_equivalents
        .saturating_add(open.property_authentication_block_equivalents);
    metrics.property_authentication_read_calls = metrics
        .property_authentication_read_calls
        .saturating_add(open.property_authentication_read_calls);
    metrics.physical_bytes = metrics
        .physical_bytes
        .saturating_add(open.authentication_bytes);
    metrics.physical_blocks = metrics
        .physical_blocks
        .saturating_add(open.authentication_read_calls);
}

/// Resolve a bounded UUID batch newest-first without decoding unrelated row
/// groups. Caller order is restored by the returned map lookup.
#[allow(
    clippy::too_many_lines,
    reason = "validation and selected decode share exact counters"
)]
pub fn read_authenticated_property_snapshots_for(
    project: &Path,
    kind: PropertyRouteKind,
    route: &str,
    targets: &std::collections::BTreeSet<[u8; 16]>,
) -> Result<
    (
        BTreeMap<[u8; 16], PropertySnapshotRow>,
        PropertyOverlayMetrics,
    ),
    GfError,
> {
    let inventory = authenticated_property_inventory_for_route(project, kind, route)?;
    let open = inventory.open_metrics();
    let (rows, mut metrics) =
        read_authenticated_property_snapshots_for_inventory(&inventory, kind, route, targets)?;
    add_open_metrics(&mut metrics, open);
    Ok((rows, metrics))
}

/// Resolve a bounded UUID batch from an already authenticated generation
/// inventory. This is the mutation-baseline path used by a live session.
#[allow(
    clippy::too_many_lines,
    reason = "authenticated targeted admission and exact I/O accounting stay co-located"
)]
pub fn read_authenticated_property_snapshots_for_inventory(
    inventory: &AuthenticatedPropertyInventory,
    kind: PropertyRouteKind,
    route: &str,
    targets: &std::collections::BTreeSet<[u8; 16]>,
) -> Result<
    (
        BTreeMap<[u8; 16], PropertySnapshotRow>,
        PropertyOverlayMetrics,
    ),
    GfError,
> {
    let result = read_property_targets(inventory, kind, route, targets, None)?;
    Ok((result.rows, result.metrics))
}

pub(crate) fn decode_snapshot_batch(
    batch: &RecordBatch,
    uuid_field: &str,
) -> Result<Vec<PropertySnapshotRow>, GfError> {
    let uuid = batch
        .column_by_name(uuid_field)
        .ok_or_else(|| corrupt("property batch lacks UUID column"))?;
    if uuid.null_count() != 0 {
        return Err(corrupt("property UUID column contains null slots"));
    }
    let tombstones = batch
        .column_by_name(PROPERTY_TOMBSTONE_FIELD)
        .map(|column| {
            column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| corrupt("property tombstone column is not boolean"))
        })
        .transpose()?;
    if tombstones.is_some_and(|values| values.null_count() != 0) {
        return Err(corrupt("property tombstone column contains null slots"));
    }
    let mut rows = Vec::with_capacity(batch.num_rows());
    crate::writer::decode_property_batch(batch, uuid_field, |uuid, mut values| {
        values.remove(PROPERTY_TOMBSTONE_FIELD);
        let index = rows.len();
        rows.push(PropertySnapshotRow {
            uuid,
            tombstone: tombstones.is_some_and(|values| values.value(index)),
            values: values.into_iter().collect(),
        });
    })?;
    Ok(rows)
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "used directly as Result::map_err adapter"
)]
pub(super) fn parquet_error(error: ParquetError) -> GfError {
    GfError::Project {
        code: graphforge_core::ProjectErrorCode::ProjectCorrupt,
        message: format!("property overlay Parquet is corrupt: {error}"),
    }
}

pub(super) fn authenticated_arrow_error(error: arrow::error::ArrowError) -> GfError {
    match error {
        arrow::error::ArrowError::IoError(_, source) => io_error(source),
        other => corrupt(&format!(
            "authenticated property Arrow/Parquet data is corrupt: {other}"
        )),
    }
}

#[cfg(test)]
mod tests;
