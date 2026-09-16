//! Targeted reads for authenticated property overlays.

use super::{
    Arc, Array, AuthenticatedPropertyFragment, AuthenticatedPropertyInventory, BTreeMap, BTreeSet,
    BooleanArray, FixedSizeBinaryArray, GfError, OpenPropertyFragment, Ordering,
    PROPERTY_TOMBSTONE_FIELD, PropertyOverlayLimits, PropertyOverlayMetrics, PropertyRouteKind,
    PropertySnapshotRow, ReadCounts, RecordBatch, TargetReadAdmission, Uuid, admit_target_footer,
    admitted_batch_rows, authenticated_arrow_error, charge_target_batch, corrupt,
    decode_snapshot_batch, io_error, open_counted_retained_property_builder, parquet_error,
    parquet_resource_admission, replay_decoder_limit, snapshot_charge, validate_fragment_schema,
    validate_parquet_resource_admission,
};

/// Cumulative probe I/O and serial decoder peaks. The identity containers are
/// reported separately: decoder counters are not a bound on process memory.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EdgeOwnerProbeWork {
    /// Candidate routes.
    pub candidate_routes: usize,
    /// Target memberships.
    pub target_memberships: usize,
    /// Resolved targets.
    pub resolved_targets: usize,
    /// Route name bytes.
    pub route_name_bytes: usize,
    /// Physical bytes.
    pub physical_bytes: u64,
    /// Authentication bytes.
    pub authentication_bytes: u64,
    /// Authenticated snapshot bytes.
    pub authenticated_snapshot_bytes: u64,
    /// Authenticated snapshot peak bytes.
    pub authenticated_snapshot_peak_bytes: u64,
    /// Physical rows.
    pub physical_rows: u64,
    /// Physical blocks.
    pub physical_blocks: u64,
    /// Fragments considered.
    pub fragments_considered: u64,
    /// Row groups considered.
    pub row_groups_considered: u64,
    /// Row groups selected.
    pub row_groups_selected: u64,
    /// Decoder peak bytes.
    pub decoder_peak_bytes: u64,
    /// Decoder page reservation bytes.
    pub decoder_page_reservation_bytes: u64,
}

/// Construction and ordinary mutation have different exploratory property
/// owners. Probe only the candidate routes for requested identities, once per
/// route, using authenticated row presence (including a newest tombstone).
pub fn resolve_existing_edge_property_owners(
    inventory: &crate::AuthenticatedPropertyInventory,
    owners: &mut BTreeMap<Uuid, String>,
) -> Result<EdgeOwnerProbeWork, GfError> {
    use crate::PropertyRouteKind;
    let mut candidates = BTreeMap::<String, BTreeSet<[u8; 16]>>::new();
    for (uuid, route) in owners.iter() {
        for candidate in [route.as_str(), "_exploratory"] {
            if inventory
                .route_schema(PropertyRouteKind::Edge, candidate)
                .is_some()
            {
                if let Some(targets) = candidates.get_mut(candidate) {
                    targets.insert(uuid.into_bytes());
                } else {
                    candidates.insert(candidate.to_owned(), BTreeSet::from([uuid.into_bytes()]));
                }
            }
        }
    }
    let mut resolved = BTreeMap::new();
    let mut total = EdgeOwnerProbeWork {
        candidate_routes: candidates.len(),
        target_memberships: candidates.values().map(BTreeSet::len).sum(),
        route_name_bytes: candidates.keys().map(String::len).sum(),
        ..EdgeOwnerProbeWork::default()
    };
    for (route, targets) in &candidates {
        let (present, work) = crate::read_authenticated_property_presence_for_inventory(
            inventory,
            PropertyRouteKind::Edge,
            route,
            targets,
        )?;
        total.physical_bytes += work.physical_bytes;
        total.authentication_bytes += work.authentication_bytes;
        total.authenticated_snapshot_bytes += work.authenticated_snapshot_bytes;
        total.authenticated_snapshot_peak_bytes = total
            .authenticated_snapshot_peak_bytes
            .max(work.authenticated_snapshot_peak_bytes);
        total.physical_rows += work.physical_rows;
        total.physical_blocks += work.physical_blocks;
        total.fragments_considered += work.fragments_considered;
        total.row_groups_considered += work.row_groups_considered;
        total.row_groups_selected += work.row_groups_selected;
        total.decoder_peak_bytes = total.decoder_peak_bytes.max(work.decoder_peak_bytes);
        total.decoder_page_reservation_bytes = total
            .decoder_page_reservation_bytes
            .max(work.decoder_page_reservation_bytes);
        for uuid in present {
            if resolved
                .insert(Uuid::from_bytes(uuid), route.as_str())
                .is_some()
            {
                return Err(GfError::Storage(
                    "composite edge property owner is ambiguous".into(),
                ));
            }
        }
    }
    // No property row means ordinary writer ownership, already derived from
    // topology. Same-request creations are added by the caller afterwards.
    total.resolved_targets = resolved.len();
    for (uuid, route) in resolved {
        owners.insert(uuid, route.to_owned());
    }
    Ok(total)
}

/// Selected logical values and tombstone-inclusive ownership from one read.
pub struct PropertyTargetSnapshots {
    /// Latest live snapshots for requested UUIDs only.
    pub rows: BTreeMap<[u8; 16], PropertySnapshotRow>,
    /// Requested UUIDs with a physical owner, including newest tombstones.
    pub present: BTreeSet<[u8; 16]>,
    /// Exact admission, authentication and decode work for this read.
    pub metrics: PropertyOverlayMetrics,
}

impl PropertyTargetSnapshots {
    /// Materialize selected live edge rows in their declared Arrow representation.
    /// Uses the canonical property encoder, including tagged and temporal values.
    pub fn edge_batch(&self, schema: &arrow::datatypes::Schema) -> Result<RecordBatch, GfError> {
        crate::writer::edge_property_snapshots_batch(schema, &self.rows)
    }
}

/// Read selected values and owner presence without a second fragment scan.
/// Uses the same authenticated decoder and resource limits as mutation reads.
pub fn read_authenticated_property_targets_for_inventory(
    inventory: &AuthenticatedPropertyInventory,
    kind: PropertyRouteKind,
    route: &str,
    targets: &BTreeSet<[u8; 16]>,
) -> Result<PropertyTargetSnapshots, GfError> {
    let result = read_property_targets(inventory, kind, route, targets, None)?;
    Ok(PropertyTargetSnapshots {
        present: targets.difference(&result.unresolved).copied().collect(),
        rows: result.rows,
        metrics: result.metrics,
    })
}

/// Resolve authenticated target row presence, including newest tombstones.
/// Unlike live snapshot reads, a removed row still proves its physical owner.
/// Uses the same schema, UUID ordering, tombstone-value and resource validation.
pub fn read_authenticated_property_presence_for_inventory(
    inventory: &AuthenticatedPropertyInventory,
    kind: PropertyRouteKind,
    route: &str,
    targets: &BTreeSet<[u8; 16]>,
) -> Result<(BTreeSet<[u8; 16]>, PropertyOverlayMetrics), GfError> {
    let result = read_property_targets(inventory, kind, route, targets, None)?;
    let present = targets.difference(&result.unresolved).copied().collect();
    Ok((present, result.metrics))
}

pub(crate) fn read_replay_property_targets(
    inventory: &AuthenticatedPropertyInventory,
    kind: PropertyRouteKind,
    route: &str,
    targets: &BTreeSet<[u8; 16]>,
    max_memory_bytes: usize,
) -> Result<
    (
        BTreeMap<[u8; 16], PropertySnapshotRow>,
        PropertyOverlayMetrics,
    ),
    GfError,
> {
    let result = read_property_targets(inventory, kind, route, targets, Some(max_memory_bytes))?;
    Ok((result.rows, result.metrics))
}

pub(super) struct TargetPropertyRows {
    pub(super) rows: BTreeMap<[u8; 16], PropertySnapshotRow>,
    unresolved: BTreeSet<[u8; 16]>,
    pub(super) metrics: PropertyOverlayMetrics,
}

#[allow(
    clippy::too_many_lines,
    reason = "authenticated targeted read and its resource accounting share one lifecycle"
)]
pub(super) fn read_property_targets(
    inventory: &AuthenticatedPropertyInventory,
    kind: PropertyRouteKind,
    route: &str,
    targets: &BTreeSet<[u8; 16]>,
    replay_budget: Option<usize>,
) -> Result<TargetPropertyRows, GfError> {
    let mut limits = PropertyOverlayLimits::default();
    if let Some(bytes) = replay_budget {
        limits.max_buffered_bytes = bytes as u64;
        limits.max_row_bytes = limits.max_row_bytes.min((bytes as u64 / 4).max(1));
        if bytes < 64 * 1024 {
            return Err(replay_decoder_limit(
                "property replay authentication buffer exceeds budget",
            ));
        }
    }
    let mut unresolved = targets.clone();
    let mut found = BTreeMap::new();
    let mut retained_bytes = 0;
    let mut metrics = PropertyOverlayMetrics::default();
    let Some(fragments) = inventory.routes.get(&(kind, route.to_owned())) else {
        return Ok(TargetPropertyRows {
            rows: found,
            unresolved,
            metrics,
        });
    };
    let root_path = inventory
        .root_path
        .as_deref()
        .ok_or_else(|| corrupt("property inventory lacks its retained root path"))?;
    let scratch_parent = root_path
        .parent()
        .ok_or_else(|| corrupt("property inventory root lacks a project-volume parent"))?;
    let targeted_scratch = tempfile::Builder::new()
        .prefix(".gf-property-targeted-")
        .tempdir_in(scratch_parent)
        .map_err(io_error)?;
    for fragment in fragments.iter().rev() {
        let counts = Arc::new(ReadCounts::default());
        let opened = inventory.open_fragment(fragment, targeted_scratch.path())?;
        metrics.authentication_bytes = metrics
            .authentication_bytes
            .saturating_add(opened.authentication_bytes);
        metrics.authentication_block_equivalents = metrics
            .authentication_block_equivalents
            .saturating_add(opened.authentication_block_equivalents);
        metrics.authentication_read_calls = metrics
            .authentication_read_calls
            .saturating_add(opened.authentication_read_calls);
        metrics.property_authentication_bytes = metrics
            .property_authentication_bytes
            .saturating_add(opened.authentication_bytes);
        metrics.authenticated_snapshot_bytes = metrics
            .authenticated_snapshot_bytes
            .saturating_add(opened.authentication_bytes);
        metrics.authenticated_snapshot_peak_bytes = metrics
            .authenticated_snapshot_peak_bytes
            .max(fragment.entry.byte_length);
        metrics.property_authentication_block_equivalents = metrics
            .property_authentication_block_equivalents
            .saturating_add(opened.authentication_block_equivalents);
        metrics.property_authentication_read_calls = metrics
            .property_authentication_read_calls
            .saturating_add(opened.authentication_read_calls);
        if let Some(bytes) = replay_budget {
            admit_target_footer(&opened.file, fragment.entry.byte_length, bytes)?;
        }
        let builder =
            open_counted_retained_property_builder(fragment, &opened, Arc::clone(&counts))?;
        validate_fragment_schema(
            builder.schema().as_ref(),
            fragment.id,
            fragment.layout,
            kind,
            route,
        )?;
        let targeted_batch_rows = admitted_batch_rows(limits);
        let page_reservation_bytes = if replay_budget.is_some() {
            let admission = parquet_resource_admission(
                builder.metadata(),
                limits,
                opened.file.as_ref(),
                &counts,
                None,
                targeted_batch_rows,
                true,
                replay_decoder_limit,
            )?;
            // Outer authority builder, validation builder, and active reader
            // may coexist; account metadata independently from page buffers.
            admission
                .with_codec_bytes
                .saturating_add((builder.metadata().memory_size() as u64).saturating_mul(3))
        } else {
            validate_parquet_resource_admission(
                builder.metadata(),
                limits,
                opened.file.as_ref(),
                &counts,
                None,
            )?
        };
        let admission = TargetReadAdmission {
            limits,
            page_reservation_bytes,
            replay: replay_budget.is_some(),
        };
        admission.check(0, retained_bytes)?;
        metrics.decoder_page_reservation_bytes = metrics
            .decoder_page_reservation_bytes
            .max(page_reservation_bytes);
        metrics.row_groups_considered = metrics
            .row_groups_considered
            .saturating_add(u64::try_from(builder.metadata().num_row_groups()).unwrap_or(u64::MAX));
        let row_groups = select_target_row_groups(
            fragment,
            &opened,
            kind,
            &unresolved,
            &counts,
            &mut metrics,
            admission,
            targeted_batch_rows,
            retained_bytes,
        )?;
        let validation_bytes = counts.bytes.load(Ordering::Relaxed);
        let validation_read_calls = counts.blocks.load(Ordering::Relaxed);
        if !row_groups.is_empty() {
            metrics.row_groups_selected = metrics
                .row_groups_selected
                .saturating_add(u64::try_from(row_groups.len()).unwrap_or(u64::MAX));
            decode_target_row_groups(
                TargetDecodeOptions {
                    fragment,
                    opened: &opened,
                    kind,
                    row_groups,
                    batch_rows: targeted_batch_rows,
                    admission,
                },
                &counts,
                &mut unresolved,
                &mut found,
                &mut retained_bytes,
                &mut metrics,
            )?;
        }
        let total_bytes = counts.bytes.load(Ordering::Relaxed);
        let total_read_calls = counts.blocks.load(Ordering::Relaxed);
        metrics.fragments_considered = metrics.fragments_considered.saturating_add(1);
        metrics.physical_bytes = metrics.physical_bytes.saturating_add(total_bytes);
        metrics.validation_bytes = metrics.validation_bytes.saturating_add(validation_bytes);
        metrics.selected_value_bytes = metrics
            .selected_value_bytes
            .saturating_add(total_bytes.saturating_sub(validation_bytes));
        metrics.read_calls = metrics.read_calls.saturating_add(total_read_calls);
        metrics.validation_read_calls = metrics
            .validation_read_calls
            .saturating_add(validation_read_calls);
        metrics.selected_value_read_calls = metrics
            .selected_value_read_calls
            .saturating_add(total_read_calls.saturating_sub(validation_read_calls));
        metrics.physical_blocks = metrics
            .physical_blocks
            .saturating_add(total_read_calls.saturating_add(opened.authentication_read_calls));
        metrics.range_seeks = metrics
            .range_seeks
            .saturating_add(counts.range_seeks.load(Ordering::Relaxed));
    }
    metrics.physical_bytes = metrics
        .physical_bytes
        .saturating_add(metrics.authentication_bytes);
    #[cfg(test)]
    assert_eq!(
        retained_bytes,
        found.values().map(snapshot_charge).sum::<u64>()
    );
    metrics.logical_rows = u64::try_from(found.len()).unwrap_or(u64::MAX);
    metrics.peak_buffered_rows = metrics.decoder_peak_rows;
    Ok(TargetPropertyRows {
        rows: found,
        unresolved,
        metrics,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "one authenticated capability and its bounded decode accounting are explicit"
)]
fn select_target_row_groups(
    fragment: &AuthenticatedPropertyFragment,
    opened: &OpenPropertyFragment,
    kind: PropertyRouteKind,
    unresolved: &std::collections::BTreeSet<[u8; 16]>,
    counts: &Arc<ReadCounts>,
    metrics: &mut PropertyOverlayMetrics,
    admission: TargetReadAdmission,
    targeted_batch_rows: usize,
    retained_bytes: u64,
) -> Result<Vec<usize>, GfError> {
    let builder = open_counted_retained_property_builder(fragment, opened, Arc::clone(counts))?;
    let mut selected_groups = Vec::new();
    let mut prior_uuid = None;
    for index in 0..builder.metadata().num_row_groups() {
        let validation =
            open_counted_retained_property_builder(fragment, opened, Arc::clone(counts))?
                .with_row_groups(vec![index])
                .with_batch_size(targeted_batch_rows)
                .build()
                .map_err(parquet_error)?;
        let mut selected = false;
        for batch in validation {
            let batch = batch.map_err(authenticated_arrow_error)?;
            charge_target_batch(metrics, &batch, admission, retained_bytes)?;
            let uuids = batch
                .column_by_name(kind.uuid_field())
                .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .ok_or_else(|| corrupt("property UUID column has wrong physical type"))?;
            let tombstones = batch
                .column_by_name(PROPERTY_TOMBSTONE_FIELD)
                .map(|column| {
                    column
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| corrupt("property tombstone column is not boolean"))
                })
                .transpose()?;
            if tombstones.is_none() && fragment.id.generation != 0 {
                return Err(corrupt("property snapshot fragment lacks tombstone field"));
            }
            if uuids.null_count() != 0 || tombstones.is_some_and(|values| values.null_count() != 0)
            {
                return Err(corrupt("property identity columns contain null slots"));
            }
            for row in 0..batch.num_rows() {
                let uuid: [u8; 16] = uuids
                    .value(row)
                    .try_into()
                    .map_err(|_| corrupt("property UUID value has wrong width"))?;
                if prior_uuid.is_some_and(|prior| prior >= uuid) {
                    return Err(corrupt(
                        "property fragment UUIDs are not strictly sorted and unique",
                    ));
                }
                prior_uuid = Some(uuid);
                selected |= !unresolved.is_empty() && unresolved.contains(&uuid);
                if tombstones.is_some_and(|values| values.value(row))
                    && batch
                        .columns()
                        .iter()
                        .skip(2)
                        .any(|column| !column.is_null(row))
                {
                    return Err(corrupt("property tombstone carries values"));
                }
            }
            metrics.physical_rows = metrics
                .physical_rows
                .saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
        }
        if selected {
            selected_groups.push(index);
        }
    }
    Ok(selected_groups)
}

struct TargetDecodeOptions<'a> {
    fragment: &'a AuthenticatedPropertyFragment,
    opened: &'a OpenPropertyFragment,
    kind: PropertyRouteKind,
    row_groups: Vec<usize>,
    batch_rows: usize,
    admission: TargetReadAdmission,
}

fn decode_target_row_groups(
    options: TargetDecodeOptions<'_>,
    counts: &Arc<ReadCounts>,
    unresolved: &mut std::collections::BTreeSet<[u8; 16]>,
    found: &mut BTreeMap<[u8; 16], PropertySnapshotRow>,
    retained_bytes: &mut u64,
    metrics: &mut PropertyOverlayMetrics,
) -> Result<(), GfError> {
    let reader = open_counted_retained_property_builder(
        options.fragment,
        options.opened,
        Arc::clone(counts),
    )?
    .with_row_groups(options.row_groups)
    .with_batch_size(options.batch_rows)
    .build()
    .map_err(parquet_error)?;
    for batch in reader {
        let batch = batch.map_err(authenticated_arrow_error)?;
        charge_target_batch(metrics, &batch, options.admission, *retained_bytes)?;
        let decoded = decode_snapshot_batch(&batch, options.kind.uuid_field())?;
        if decoded
            .iter()
            .any(|row| snapshot_charge(row) > options.admission.limits.max_row_bytes)
        {
            return Err(options
                .admission
                .error("property snapshot row exceeds byte limit"));
        }
        options.admission.check(
            batch.get_array_memory_size() as u64,
            decoded
                .iter()
                .map(snapshot_charge)
                .sum::<u64>()
                .checked_add(*retained_bytes)
                .ok_or_else(|| {
                    options
                        .admission
                        .error("property target memory charge overflow")
                })?,
        )?;
        metrics.decoder_peak_bytes = metrics
            .decoder_peak_bytes
            .max(decoded.iter().map(snapshot_charge).sum::<u64>());
        for row in decoded {
            metrics.physical_rows = metrics.physical_rows.saturating_add(1);
            if unresolved.remove(&row.uuid) {
                if row.tombstone {
                    metrics.tombstones = metrics.tombstones.saturating_add(1);
                } else {
                    // Unresolved UUIDs are removed once, so each retained live
                    // snapshot contributes exactly once across all fragments.
                    *retained_bytes = retained_bytes
                        .checked_add(snapshot_charge(&row))
                        .ok_or_else(|| {
                            options
                                .admission
                                .error("property target memory charge overflow")
                        })?;
                    found.insert(row.uuid, row);
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
