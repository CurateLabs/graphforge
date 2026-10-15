//! Projected reads for authenticated property overlays.

use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};

use super::selective_reads::{RowGroupSelection, groups_fully_covered};
use super::uuid_pages::wanted_row_selection;
use super::{
    Arc, Array, AtomicU64, AuthenticatedPropertyFragment, AuthenticatedPropertyInventory, BTreeMap,
    BTreeSet, BooleanArray, FragmentHandleGuard, GfError, LiveByteBudget, Mutex, Ordering,
    PROPERTY_TOMBSTONE_FIELD, ParquetError, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder, Path, PropertyFile, PropertyInventoryOpenMetrics,
    PropertyOverlayLimits, PropertyOverlayMetrics, PropertyRouteKind, PropertySnapshotRow,
    ReadCounts, RecordBatch, SnapshotScratch, admitted_batch_rows,
    authenticated_property_inventory_for_route, corrupt, counting_chunk_reader, io_error,
    read_property_targets, snapshot_charge, validate_fragment_schema,
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
        self.visit_route_projected_collect(
            kind,
            route,
            scratch,
            limits,
            selected_properties,
            true,
            emit,
        )
    }

    /// Query execution requests observations only when its collector is active.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(crate) fn visit_route_projected_optional<F>(
        &self,
        kind: PropertyRouteKind,
        route: &str,
        scratch: &Path,
        limits: PropertyOverlayLimits,
        selected_properties: Option<&BTreeSet<String>>,
        emit: F,
    ) -> Result<Option<PropertyOverlayMetrics>, GfError>
    where
        F: FnMut(PropertySnapshotRow) -> Result<(), GfError>,
    {
        let collect = crate::lifecycle_io::is_active();
        self.visit_route_projected_collect(
            kind,
            route,
            scratch,
            limits,
            selected_properties,
            collect,
            emit,
        )
        .map(|metrics| collect.then_some(metrics))
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn visit_route_projected_collect<F>(
        &self,
        kind: PropertyRouteKind,
        route: &str,
        scratch: &Path,
        limits: PropertyOverlayLimits,
        selected_properties: Option<&BTreeSet<String>>,
        collect: bool,
        emit: F,
    ) -> Result<PropertyOverlayMetrics, GfError>
    where
        F: FnMut(PropertySnapshotRow) -> Result<(), GfError>,
    {
        let Some(fragments) = self.routes.get(&(kind, route.to_owned())) else {
            return Ok(PropertyOverlayMetrics::default());
        };
        // Resolve the route's footer summary before any fragment is trusted, so
        // a route that cannot be summarized refuses here rather than reading.
        self.route_summary(kind, route)?;
        let counts = ReadCounts::new(collect);
        let authentication_bytes = collect.then(|| Arc::new(AtomicU64::new(0)));
        let authentication_block_equivalents = collect.then(|| Arc::new(AtomicU64::new(0)));
        let authentication_read_calls = collect.then(|| Arc::new(AtomicU64::new(0)));
        let budget = Arc::new(LiveByteBudget::new(limits.max_buffered_bytes));
        let decoded = Arc::new(Mutex::new(DecodedRetention::default()));
        let reader_context = ProjectedReaderContext {
            inventory: self,
            scratch: SnapshotScratch::Directory(scratch),
            row_groups: RowGroupSelection::All,
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
            (
                fragment.id,
                0,
                0,
                projected_fragment_rows(fragment, &reader_context),
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
                    .flat_map(|fragment| fragment.parts.iter().map(|part| part.entry.byte_length))
                    .max()
                    .unwrap_or(0),
            },
        );
        Ok(metrics)
    }
}

pub(super) struct ProjectedReaderContext<'a> {
    pub(super) inventory: &'a AuthenticatedPropertyInventory,
    pub(super) scratch: SnapshotScratch<'a>,
    pub(super) row_groups: RowGroupSelection<'a>,
    pub(super) limits: PropertyOverlayLimits,
    pub(super) kind: PropertyRouteKind,
    pub(super) route: &'a str,
    pub(super) selected_properties: Option<&'a BTreeSet<String>>,
    pub(super) counts: &'a ReadCounts,
    pub(super) budget: &'a Arc<LiveByteBudget>,
    pub(super) decoded: &'a Arc<Mutex<DecodedRetention>>,
    pub(super) authentication_bytes: &'a Option<Arc<AtomicU64>>,
    pub(super) authentication_block_equivalents: &'a Option<Arc<AtomicU64>>,
    pub(super) authentication_read_calls: &'a Option<Arc<AtomicU64>>,
}

/// Open one fragment's decoder. A fragment that cannot be opened yields its
/// error from the first `next`, so a lazily consumed sequence reports it in
/// order.
pub(super) fn projected_fragment_rows(
    fragment: &AuthenticatedPropertyFragment,
    context: &ProjectedReaderContext<'_>,
) -> PropertyParquetRows {
    let reader = open_projected_fragment(fragment, context);
    let (reader, pending_error, page_reservation_bytes, source, handle, query_memory) = match reader
    {
        Ok((reader, page_reservation_bytes, file, handle, query_memory)) => (
            Some(reader),
            None,
            page_reservation_bytes,
            Some(file),
            Some(handle),
            query_memory,
        ),
        Err(error) => (None, Some(error), 0, None, None, QueryMemory::default()),
    };
    PropertyParquetRows {
        reader,
        row_memory: None,
        row_property_fields: 0,
        query_memory,
        current: Vec::new().into_iter().zip(Vec::new()),
        uuid_field: context.kind.uuid_field(),
        pending_error,
        decoded: Arc::clone(context.decoded),
        budget: Arc::clone(context.budget),
        max_row_bytes: context.limits.max_row_bytes,
        page_reservation_bytes,
        batch_reservation_bytes: context.limits.max_buffered_bytes / 4,
        source,
        counts: context.counts.clone(),
        authentication_bytes: context.authentication_bytes.clone(),
        authentication_block_equivalents: context.authentication_block_equivalents.clone(),
        authentication_read_calls: context.authentication_read_calls.clone(),
        _handle: handle,
        #[cfg(test)]
        late_failure_row_countdown: Arc::clone(
            &context.inventory.late_decoder_failure_row_countdown,
        ),
    }
}

type ProjectedFragment = (
    ParquetRecordBatchReader,
    u64,
    Arc<PropertyFile>,
    FragmentHandleGuard,
    QueryMemory,
);

#[derive(Default)]
struct QueryMemory {
    guards: Vec<crate::property_scan_memory::Guard>,
    initial_read: Option<crate::property_scan_memory::Guard>,
    read_bytes: usize,
}

impl QueryMemory {
    fn read_guard(
        &mut self,
        execution: Option<&Arc<crate::property_scan_memory::Budget>>,
    ) -> Result<Option<crate::property_scan_memory::Guard>, GfError> {
        if let Some(guard) = self.initial_read.take() {
            return Ok(Some(guard));
        }
        if self.read_bytes == 0 {
            return Ok(None);
        }
        execution
            .ok_or_else(|| crate::property_scan_memory::refused("missing decoder admission"))?
            .reserve(self.read_bytes)
            .map(Some)
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "one reader construction keeps its source, decoder and selection admissions together"
)]
fn open_projected_fragment(
    fragment: &AuthenticatedPropertyFragment,
    context: &ProjectedReaderContext<'_>,
) -> Result<ProjectedFragment, GfError> {
    let mut query_memory = QueryMemory::default();
    let mut decoder_memory = None;
    let mut read_bytes = 0;
    if let Some(budget) = &context.budget.execution {
        budget.check()?;
        let footer = fragment
            .footer
            .get()
            .and_then(|result| result.as_ref().ok())
            .ok_or_else(|| {
                crate::property_scan_memory::refused("cannot bound unadmitted property footer")
            })?;
        let object = fragment
            .object
            .get()
            .and_then(|result| result.as_ref().ok())
            .ok_or_else(|| {
                crate::property_scan_memory::refused("cannot bound unadmitted property source")
            })?;
        let source_bytes = if object.envelope.is_some() {
            2 * super::bounded_object::MAX_PROPERTY_OBJECT_BYTES
        } else if matches!(context.scratch, SnapshotScratch::Lazy(_))
            && fragment.entry.byte_length <= super::inventory::MAX_IN_MEMORY_SNAPSHOT_BYTES
        {
            usize::try_from(fragment.entry.byte_length)
                .map_err(|_| crate::property_scan_memory::refused("source size overflow"))?
        } else if matches!(context.scratch, SnapshotScratch::Lazy(_)) {
            usize::try_from(super::readonly_snapshot::reservation_bytes(
                fragment.entry.byte_length,
            )?)
            .map_err(|_| crate::property_scan_memory::refused("source size overflow"))?
        } else {
            0
        };
        let metadata_bytes = crate::property_scan_memory::mul(footer.metadata.memory_size(), 4)?;
        query_memory
            .guards
            .push(budget.reserve(crate::property_scan_memory::add(
                source_bytes,
                crate::property_scan_memory::add(metadata_bytes, 64 * 1024)?,
            )?)?);
    }
    let opened = context.inventory.open_fragment(fragment, context.scratch)?;
    if let Some(counter) = context.authentication_bytes {
        counter.fetch_add(opened.authentication_bytes, Ordering::Relaxed);
    }
    if let Some(counter) = context.authentication_block_equivalents {
        counter.fetch_add(opened.authentication_block_equivalents, Ordering::Relaxed);
    }
    if let Some(counter) = context.authentication_read_calls {
        counter.fetch_add(opened.authentication_read_calls, Ordering::Relaxed);
    }
    let source_reservation = opened.file.reservation_bytes();
    context.budget.charge(source_reservation)?;
    let source = counting_chunk_reader(&opened, context.counts.clone());
    let footer =
        ArrowReaderMetadata::load(&source, ArrowReaderOptions::new()).map_err(parquet_error)?;
    validate_fragment_schema(
        footer.schema().as_ref(),
        fragment.id,
        fragment.layout,
        context.kind,
        context.route,
    )?;
    let projected = projected_property_columns(
        footer.schema().as_ref(),
        context.kind,
        context.selected_properties,
    );
    let projection_mask = projected.as_ref().map(|roots| {
        parquet::arrow::ProjectionMask::roots(footer.parquet_schema(), roots.iter().copied())
    });
    let projected_leaves = projection_mask.as_ref().map(|mask| {
        (0..footer.parquet_schema().num_columns())
            .filter(|index| mask.leaf_included(*index))
            .collect::<BTreeSet<_>>()
    });
    let page_reservation_bytes =
        if let Some(budget) = &context.budget.execution {
            let memory = super::parquet_budget::parquet_resource_admission(
                footer.metadata(),
                context.limits,
                opened.file.as_ref(),
                context.counts,
                projected_leaves.as_ref(),
                admitted_batch_rows(context.limits),
                true,
                corrupt,
            )?;
            if memory.decoded > context.limits.max_buffered_bytes / 4 {
                return Err(corrupt(
                    "property projected pages exceed pre-decode live-byte admission",
                ));
            }
            // The full immutable Memory source was admitted before open_fragment.
            // Its compressed page Bytes are slices, with no configured decryptor;
            // other file modes retain separate compressed-page copy allowances.
            let (total, retained) = if matches!(opened.file.as_ref(), PropertyFile::Memory(_)) {
                (memory.memory_with_codec, memory.memory_retained)
            } else {
                (memory.with_codec, memory.retained)
            };
            decoder_memory =
                Some(budget.reserve(usize::try_from(total).map_err(|_| {
                    crate::property_scan_memory::refused("decoder size overflow")
                })?)?);
            read_bytes =
                usize::try_from(total.checked_sub(retained).ok_or_else(|| {
                    crate::property_scan_memory::refused("decoder size underflow")
                })?)
                .map_err(|_| crate::property_scan_memory::refused("decoder size overflow"))?;
            memory.decoded
        } else {
            validate_parquet_resource_admission(
                footer.metadata(),
                context.limits,
                opened.file.as_ref(),
                context.counts,
                projected_leaves.as_ref(),
            )?
        };
    context.budget.charge(page_reservation_bytes)?;
    let page_reservation_bytes = page_reservation_bytes.saturating_add(source_reservation);
    {
        let mut retention = context.decoded.lock().expect("property retention lock");
        retention.page_peak = retention.page_peak.max(page_reservation_bytes);
    }
    let selected_groups = context
        .row_groups
        .select(footer.metadata(), context.kind.uuid_field());
    // A nominated read decodes only the rows it names. The UUID column of the
    // selected row groups is read first (one narrow column, and only the pages
    // its page index leaves possible), and the full projection is then decoded
    // for the matching rows alone, instead of decoding every row of each
    // surviving row group.
    let wanted_rows = match (&context.row_groups, selected_groups.as_deref()) {
        // A partition's key range covers whole row groups of a fragment: every
        // row of them is wanted, so the UUID column need not be read to choose.
        (RowGroupSelection::Uuids(wanted), Some(groups))
            if !groups.is_empty()
                && !groups_fully_covered(
                    footer.metadata(),
                    context.kind.uuid_field(),
                    groups,
                    *wanted,
                ) =>
        {
            if let Some(budget) = &context.budget.execution {
                let mut index_bytes = 0usize;
                for group in footer.metadata().row_groups() {
                    for column in group.columns() {
                        for length in [column.column_index_length(), column.offset_index_length()]
                            .into_iter()
                            .flatten()
                        {
                            let length = usize::try_from(length).map_err(|_| {
                                crate::property_scan_memory::refused("page index size overflow")
                            })?;
                            index_bytes = crate::property_scan_memory::add(index_bytes, length)?;
                        }
                    }
                }
                let rows = usize::try_from(footer.metadata().file_metadata().num_rows()).map_err(
                    |_| crate::property_scan_memory::refused("row selection count overflow"),
                )?;
                // One encoded index byte can introduce at most one page entry.
                // Candidates and candidate-row selectors coexist with final
                // per-row decisions; RowSelection::from copies its old Vec.
                let pages = index_bytes;
                let selectors = crate::property_scan_memory::add(
                    rows,
                    crate::property_scan_memory::add(
                        crate::property_scan_memory::mul(pages, 4)?,
                        2,
                    )?,
                )?;
                let selection = crate::property_scan_memory::mul(
                    selectors,
                    4 * std::mem::size_of::<parquet::arrow::arrow_reader::RowSelector>(),
                )?;
                // Parsed indexes own byte vectors, null/count/histogram vectors
                // and PageLocation triples beside their encoded read buffers.
                let index = crate::property_scan_memory::mul(
                    index_bytes,
                    4 * std::mem::size_of::<Vec<u8>>() + 8 * std::mem::size_of::<u64>() + 64,
                )?;
                let candidates = crate::property_scan_memory::mul(
                    pages,
                    4 * std::mem::size_of::<super::uuid_pages::Candidate>(),
                )?;
                query_memory
                    .guards
                    .push(budget.reserve(crate::property_scan_memory::add(
                        selection,
                        crate::property_scan_memory::add(index, candidates)?,
                    )?)?);
            }
            Some(wanted_row_selection(
                &source,
                &footer,
                context.kind.uuid_field(),
                groups,
                *wanted,
                admitted_batch_rows(context.limits),
            )?)
        }
        _ => None,
    };
    let (row_selection, paged) = match wanted_rows {
        Some(rows) => (Some(rows.selection), rows.paged),
        None => (None, None),
    };
    // Selected/repeated/unknown shapes retain their original whole envelope.
    // For qualified flat bytes, R bounds idle decoder state and T is the
    // unchanged complete admission. R+(T-R)=T covers each read jointly; the
    // temporary difference need not bound returned payloads independently.
    if row_selection.is_some() {
        read_bytes = 0;
    }
    let initial_read = decoder_memory
        .as_mut()
        .filter(|_| read_bytes != 0)
        .map(|guard| guard.split_off(read_bytes))
        .transpose()?;
    if let Some(memory) = decoder_memory {
        query_memory.guards.push(memory);
    }
    query_memory.initial_read = initial_read;
    query_memory.read_bytes = read_bytes;
    let builder =
        ParquetRecordBatchReaderBuilder::new_with_metadata(source, paged.unwrap_or(footer));
    let builder = if let Some(mask) = projection_mask {
        builder.with_projection(mask)
    } else {
        builder
    };
    let builder = match selected_groups {
        Some(groups) => builder.with_row_groups(groups),
        None => builder,
    };
    let builder = match row_selection {
        Some(selection) => builder.with_row_selection(selection),
        None => builder,
    };
    let reader = builder
        .with_batch_size(admitted_batch_rows(context.limits))
        .build()
        .map_err(parquet_error)?;
    Ok((
        reader,
        page_reservation_bytes,
        opened.file,
        opened.handle,
        query_memory,
    ))
}

pub(super) struct ProjectedMetricSources<'a> {
    pub(super) counts: &'a ReadCounts,
    pub(super) authentication_bytes: &'a Option<Arc<AtomicU64>>,
    pub(super) authentication_block_equivalents: &'a Option<Arc<AtomicU64>>,
    pub(super) authentication_read_calls: &'a Option<Arc<AtomicU64>>,
    pub(super) decoded: &'a Mutex<DecodedRetention>,
    pub(super) budget: &'a LiveByteBudget,
    pub(super) authenticated_snapshot_peak_bytes: u64,
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

pub(super) fn finalize_projected_metrics(
    metrics: &mut PropertyOverlayMetrics,
    sources: &ProjectedMetricSources<'_>,
) {
    metrics.authentication_bytes = sources
        .authentication_bytes
        .as_ref()
        .map_or(0, |count| count.load(Ordering::Relaxed));
    metrics.authentication_block_equivalents = sources
        .authentication_block_equivalents
        .as_ref()
        .map_or(0, |count| count.load(Ordering::Relaxed));
    metrics.authentication_read_calls = sources
        .authentication_read_calls
        .as_ref()
        .map_or(0, |count| count.load(Ordering::Relaxed));
    metrics.property_authentication_bytes = metrics.authentication_bytes;
    metrics.authenticated_snapshot_bytes = metrics.authentication_bytes;
    metrics.authenticated_snapshot_peak_bytes = sources.authenticated_snapshot_peak_bytes;
    metrics.property_authentication_block_equivalents = metrics.authentication_block_equivalents;
    metrics.property_authentication_read_calls = metrics.authentication_read_calls;
    metrics.validation_bytes = sources.counts.values().0;
    metrics.physical_bytes = metrics
        .authentication_bytes
        .saturating_add(metrics.validation_bytes);
    metrics.read_calls = sources.counts.values().1;
    metrics.validation_read_calls = metrics.read_calls;
    metrics.physical_blocks = metrics
        .authentication_read_calls
        .saturating_add(metrics.read_calls);
    metrics.range_seeks = sources.counts.values().2;
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

pub(super) struct PropertyParquetRows {
    reader: Option<ParquetRecordBatchReader>,
    /// Decoded rows with the byte charge each was admitted at, computed once.
    current: std::iter::Zip<std::vec::IntoIter<PropertySnapshotRow>, std::vec::IntoIter<u64>>,
    uuid_field: &'static str,
    pending_error: Option<GfError>,
    decoded: Arc<Mutex<DecodedRetention>>,
    budget: Arc<LiveByteBudget>,
    max_row_bytes: u64,
    page_reservation_bytes: u64,
    batch_reservation_bytes: u64,
    source: Option<Arc<PropertyFile>>,
    counts: ReadCounts,
    authentication_bytes: Option<Arc<AtomicU64>>,
    authentication_block_equivalents: Option<Arc<AtomicU64>>,
    authentication_read_calls: Option<Arc<AtomicU64>>,
    _handle: Option<FragmentHandleGuard>,
    // Drop after the decoder/source/current rows, including partial consumption.
    row_memory: Option<crate::property_scan_memory::Guard>,
    row_property_fields: usize,
    query_memory: QueryMemory,
    #[cfg(test)]
    late_failure_row_countdown: Arc<AtomicU64>,
}

impl Drop for PropertyParquetRows {
    fn drop(&mut self) {
        if let Some(source) = &self.source {
            let (read_bytes, read_calls) = source.physical_reads();
            self.counts.record_physical(read_bytes, read_calls);
            let (bytes, blocks, calls) = source.authentication();
            if let Some(counter) = &self.authentication_bytes {
                counter.fetch_add(bytes, Ordering::Relaxed);
            }
            if let Some(counter) = &self.authentication_block_equivalents {
                counter.fetch_add(blocks, Ordering::Relaxed);
            }
            if let Some(counter) = &self.authentication_read_calls {
                counter.fetch_add(calls, Ordering::Relaxed);
            }
        }
        self.budget.release(self.page_reservation_bytes);
    }
}

#[derive(Debug, Default)]
pub(super) struct DecodedRetention {
    current_rows: u64,
    current_bytes: u64,
    peak_rows: u64,
    peak_bytes: u64,
    batches: u64,
    page_peak: u64,
}

impl Iterator for PropertyParquetRows {
    type Item = Result<PropertySnapshotRow, GfError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_admitted()
            .map(|result| result.map(|snapshot| snapshot.row))
    }
}

impl PropertyParquetRows {
    #[allow(
        clippy::too_many_lines,
        reason = "fallible decoder state, reservations, and release paths stay auditable together"
    )]
    pub(super) fn next_admitted(
        &mut self,
    ) -> Option<Result<crate::property_scan_memory::Snapshot, GfError>> {
        loop {
            if let Some(budget) = &self.budget.execution
                && let Err(error) = budget.check()
            {
                self.reader = None;
                return Some(Err(error));
            }
            if let Some(error) = self.pending_error.take() {
                self.reader = None;
                return Some(Err(error));
            }
            if let Some((row, charge)) = self.current.next() {
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
                    self.budget.release(charge);
                    self.reader = None;
                    return Some(Err(corrupt(
                        "injected late authenticated property decoder failure",
                    )));
                }
                self.budget.release(charge);
                let mut decoded = self.decoded.lock().expect("property retention lock");
                decoded.current_rows = decoded.current_rows.saturating_sub(1);
                decoded.current_bytes = decoded.current_bytes.saturating_sub(charge);
                let memory = match self.row_memory.as_mut() {
                    Some(memory) => {
                        match crate::property_scan_memory::retained_decoded_row(
                            &row,
                            self.row_property_fields,
                        )
                        .and_then(|bytes| memory.split_off(bytes))
                        {
                            Ok(memory) => Some(memory),
                            Err(error) => {
                                self.reader = None;
                                return Some(Err(error));
                            }
                        }
                    }
                    None => None,
                };
                return Some(Ok(crate::property_scan_memory::Snapshot { row, memory }));
            }
            let previous =
                std::mem::replace(&mut self.current, Vec::new().into_iter().zip(Vec::new()));
            drop(previous);
            self.row_memory = None;
            self.reader.as_ref()?;
            let read_memory = match self.query_memory.read_guard(self.budget.execution.as_ref()) {
                Ok(memory) => memory,
                Err(error) => {
                    self.reader = None;
                    return Some(Err(error));
                }
            };
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
            let mut row_memory = match &self.budget.execution {
                Some(budget) => {
                    match crate::property_scan_memory::decoded_rows(&batch, self.uuid_field)
                        .and_then(|bytes| budget.reserve(bytes))
                    {
                        Ok(guard) => Some(guard),
                        Err(error) => {
                            self.budget.release(decode_reservation);
                            self.budget.release(self.batch_reservation_bytes);
                            self.reader = None;
                            return Some(Err(error));
                        }
                    }
                }
                None => None,
            };
            match decode_snapshot_batch(&batch, self.uuid_field) {
                Ok(rows) => {
                    let charges = rows.iter().map(snapshot_charge).collect::<Vec<_>>();
                    if charges.iter().any(|charge| *charge > self.max_row_bytes) {
                        self.budget.release(decode_reservation);
                        self.budget.release(self.batch_reservation_bytes);
                        self.reader = None;
                        return Some(Err(corrupt("property snapshot row exceeds byte limit")));
                    }
                    let bytes = charges
                        .iter()
                        .fold(0_u64, |total, charge| total.saturating_add(*charge));
                    self.row_property_fields = batch
                        .schema()
                        .fields()
                        .iter()
                        .filter(|field| field.name() != self.uuid_field)
                        .count();
                    let retained = if row_memory.is_some() {
                        crate::property_scan_memory::retained_decoded_rows(
                            &rows,
                            rows.capacity(),
                            charges.capacity(),
                            self.row_property_fields,
                        )
                    } else {
                        Ok(0)
                    };
                    let retained = match retained {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            self.budget.release(decode_reservation);
                            self.budget.release(self.batch_reservation_bytes);
                            self.reader = None;
                            return Some(Err(error));
                        }
                    };
                    // Literal payloads now own their data. Keep the original
                    // admission through the source batch's last use, then stop
                    // retaining its dead Arrow buffers and dispatch headers.
                    drop(batch);
                    if let Some(memory) = row_memory.as_mut()
                        && let Err(error) = memory.shrink_to(retained)
                    {
                        self.budget.release(decode_reservation);
                        self.budget.release(self.batch_reservation_bytes);
                        self.reader = None;
                        return Some(Err(error));
                    }
                    drop(read_memory);
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
                    self.current = rows.into_iter().zip(charges);
                    self.row_memory = row_memory;
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
    let result = read_property_targets(inventory, kind, route, targets, None, true, false)?;
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
    crate::writer::decode_property_batch_into(
        batch,
        uuid_field,
        |uuid, mut values: BTreeMap<String, graphforge_ir::IrLiteral>| {
            values.remove(PROPERTY_TOMBSTONE_FIELD);
            let index = rows.len();
            rows.push(PropertySnapshotRow {
                uuid,
                tombstone: tombstones.is_some_and(|values| values.value(index)),
                values,
            });
        },
    )?;
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
