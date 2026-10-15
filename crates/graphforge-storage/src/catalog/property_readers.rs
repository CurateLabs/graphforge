//! Authenticated property overlay readers and bounded visitation.

use super::AdmittedSourceFile;
use super::admit_decoded_parquet;
use super::checksum_admitted_source;
use super::io_err;
use super::parquet_err;
use super::preflight_parquet_handle;
use super::property_relative_name;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

/// Read `properties/<stem>.parquet` for the project at `dir`, discovering its
/// (dynamic) schema from the file. Returns an **empty `Vec`** when the file is
/// absent — so a caller decoding rows sees zero pre-existing property rows.
///
/// Production reads go through the staged-batch read-through in `writer`
/// (#792); the node-hydration path (`nodes(p)`, #1024) also reads through it.
///
/// # Errors
/// Propagates Parquet / Arrow errors encountered while reading.
pub fn read_properties(dir: &Path, stem: &str) -> Result<Vec<RecordBatch>, DataFusionError> {
    read_property_overlay(dir, stem, false)
}

fn read_property_overlay(
    dir: &Path,
    stem: &str,
    is_edge: bool,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    read_property_overlay_projected(dir, stem, is_edge, None)
}

fn read_property_overlay_projected(
    dir: &Path,
    stem: &str,
    is_edge: bool,
    selected_properties: Option<&std::collections::BTreeSet<String>>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let mut batches = Vec::new();
    visit_property_overlay_batched_projected(
        dir,
        None,
        stem,
        is_edge,
        8_192,
        selected_properties,
        |batch| {
            batches.push(batch.clone());
            Ok(true)
        },
    )?;
    Ok(batches)
}

pub(crate) fn visit_property_overlay_batched<F>(
    dir: &Path,
    stem: &str,
    is_edge: bool,
    batch_size: usize,
    visit: F,
) -> Result<(), DataFusionError>
where
    F: FnMut(&RecordBatch) -> Result<bool, DataFusionError>,
{
    visit_property_overlay_batched_projected(dir, None, stem, is_edge, batch_size, None, visit)
        .map(|_| ())
}

pub(crate) fn visit_property_overlay_batched_with_inventory<F>(
    dir: &Path,
    inventory: Option<&crate::AuthenticatedPropertyInventory>,
    stem: &str,
    is_edge: bool,
    batch_size: usize,
    visit: F,
) -> Result<(), DataFusionError>
where
    F: FnMut(&RecordBatch) -> Result<bool, DataFusionError>,
{
    visit_property_overlay_batched_projected(dir, inventory, stem, is_edge, batch_size, None, visit)
        .map(|_| ())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn visit_property_overlay_batched_projected<F>(
    dir: &Path,
    inventory: Option<&crate::AuthenticatedPropertyInventory>,
    stem: &str,
    is_edge: bool,
    batch_size: usize,
    selected_properties: Option<&std::collections::BTreeSet<String>>,
    visit: F,
) -> Result<Option<crate::PropertyOverlayMetrics>, DataFusionError>
where
    F: FnMut(&RecordBatch) -> Result<bool, DataFusionError>,
{
    visit_property_overlay_batched_selected(
        dir,
        inventory,
        stem,
        is_edge,
        batch_size,
        selected_properties,
        None,
        &[],
        None,
        visit,
    )
}

/// The most rows a pushed-down equality may nominate before the read gives up
/// pruning and streams the route.
const MAX_EQUALITY_CANDIDATES: usize = 1 << 18;

/// Visit a route's newest live rows, optionally only those that can satisfy a
/// pushed-down equality, optionally only those whose key lies in `interval`.
/// The read streams the fragments and writes nothing.
///
/// An interval confines the whole merge to its keys, so a key's base, mutation
/// and tombstone rows are all read or none are: reading every interval of a
/// tiling visits each live row exactly once.
#[allow(clippy::too_many_arguments)]
pub(crate) fn visit_property_overlay_batched_selected<F>(
    dir: &Path,
    inventory: Option<&crate::AuthenticatedPropertyInventory>,
    stem: &str,
    is_edge: bool,
    batch_size: usize,
    selected_properties: Option<&std::collections::BTreeSet<String>>,
    equality: Option<&crate::property_overlay::PropertyEquality>,
    uuid_sets: &[&dyn crate::uuid_set::UuidMembership],
    interval: Option<crate::uuid_set::UuidInterval>,
    mut visit: F,
) -> Result<Option<crate::PropertyOverlayMetrics>, DataFusionError>
where
    F: FnMut(&RecordBatch) -> Result<bool, DataFusionError>,
{
    visit_property_overlay_batched_admitted(
        dir,
        inventory,
        stem,
        is_edge,
        batch_size,
        selected_properties,
        equality,
        uuid_sets,
        interval,
        None,
        |batch, _| visit(batch),
    )
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) fn visit_property_overlay_batched_admitted<F>(
    dir: &Path,
    inventory: Option<&crate::AuthenticatedPropertyInventory>,
    stem: &str,
    is_edge: bool,
    batch_size: usize,
    selected_properties: Option<&std::collections::BTreeSet<String>>,
    equality: Option<&crate::property_overlay::PropertyEquality>,
    uuid_sets: &[&dyn crate::uuid_set::UuidMembership],
    interval: Option<crate::uuid_set::UuidInterval>,
    execution: Option<&Arc<crate::property_scan_memory::Budget>>,
    mut visit: F,
) -> Result<Option<crate::PropertyOverlayMetrics>, DataFusionError>
where
    F: FnMut(
        &RecordBatch,
        Option<Arc<crate::property_scan_memory::Guard>>,
    ) -> Result<bool, DataFusionError>,
{
    if !dir.exists() {
        return Ok(crate::lifecycle_io::is_active().then(crate::PropertyOverlayMetrics::default));
    }
    let kind = if is_edge {
        crate::property_overlay::PropertyRouteKind::Edge
    } else {
        crate::property_overlay::PropertyRouteKind::Node
    };
    let captured;
    let inventory = if let Some(inventory) = inventory {
        inventory
    } else {
        captured =
            crate::property_overlay::authenticated_property_inventory_for_route(dir, kind, stem)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
        &captured
    };
    let schema = inventory
        .route_schema(kind, stem)
        .and_then(|schema| {
            schema
                .map(|schema| {
                    project_property_schema(schema, kind.uuid_field(), selected_properties)
                })
                .transpose()
        })
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let collect = crate::lifecycle_io::is_active();
    let limits = crate::property_overlay::PropertyOverlayLimits::default();
    // Query execution reuses the storage reader's bounded row window for
    // decoded-map handoff as well. This bounds conversion/source overlap;
    // every allocation still needs its own admission to the shared byte pool.
    let batch_size = if execution.is_some() {
        batch_size.max(1).min(limits.max_buffered_rows.max(1))
    } else {
        batch_size.max(1)
    };
    let mut metrics = crate::PropertyOverlayMetrics::default();
    let indexed = match equality.filter(|_| !is_edge) {
        Some(equality) => match execution {
            Some(execution) => crate::property_equality_index::lookup_admitted(
                dir,
                inventory,
                stem,
                equality,
                Some(execution),
            ),
            None => crate::property_equality_index::lookup(dir, inventory, stem, equality)
                .map(|value| value.map(|ids| (ids, None))),
        }
        .map_err(|error| DataFusionError::External(Box::new(error)))?,
        None => None,
    };
    // Declare admission before the data owner so all early returns also drop
    // the candidate set before releasing its reservation.
    let _indexed_memory;
    let indexed = if let Some((ids, memory)) = indexed {
        _indexed_memory = memory;
        Some(ids)
    } else {
        _indexed_memory = None;
        None
    };
    // The fallback's existing cap bounds its retained UUID set, including the
    // last row which makes pruning give up. Indexed reads use selected shards.
    let candidate_memory = if indexed.is_none() && equality.is_some() && !is_edge {
        execution
            .map(|budget| {
                let rows = inventory
                    .route_row_upper_bound(kind, stem)?
                    .min(MAX_EQUALITY_CANDIDATES + 1);
                crate::property_scan_memory::mul(
                    rows,
                    16 * (std::mem::size_of::<[u8; 16]>() + 4 * std::mem::size_of::<usize>()),
                )
                .and_then(|bytes| budget.reserve(bytes))
            })
            .transpose()
            .map_err(|error| DataFusionError::External(Box::new(error)))?
    } else {
        None
    };
    let candidates = match (indexed, equality.filter(|_| !is_edge)) {
        (Some(found), _) => Some((found, crate::PropertyOverlayMetrics::default())),
        (None, Some(equality)) => match execution {
            Some(_) => inventory.equality_candidates_admitted(
                kind,
                stem,
                equality,
                limits,
                MAX_EQUALITY_CANDIDATES,
                collect,
                execution.cloned(),
            ),
            None => inventory.equality_candidates(
                kind,
                stem,
                equality,
                limits,
                MAX_EQUALITY_CANDIDATES,
                collect,
            ),
        }
        .map_err(|error| DataFusionError::External(Box::new(error)))?,
        (None, None) => None,
    };
    let candidate_uuids = if let Some((candidates, work)) = candidates {
        metrics.absorb(&work);
        Some(candidates)
    } else {
        // An abandoned candidate pass retains no UUID set. Its envelope
        // must not compete with the subsequent unpruned read.
        drop(candidate_memory);
        None
    };
    let uuid_filter = (!uuid_sets.is_empty() || candidate_uuids.is_some()).then_some(
        crate::uuid_set::UuidFilter {
            nominations: uuid_sets,
            candidates: candidate_uuids.as_ref(),
        },
    );
    let ranged = interval.map(|interval| {
        crate::uuid_set::UuidRange::new(
            interval,
            uuid_filter
                .as_ref()
                .map(|uuids| uuids as &dyn crate::uuid_set::UuidMembership),
        )
    });
    // The compared column is needed to check the winner of each candidate.
    let streamed_properties = match (equality, selected_properties) {
        (Some(equality), Some(selected)) => {
            let mut selected = selected.clone();
            selected.insert(equality.column.clone());
            Some(selected)
        }
        _ => selected_properties.cloned(),
    };
    // Keep one accumulator allocation across emissions. Conversion has its own
    // admission below; draining avoids a fresh growing Vec after every batch.
    let _vectors = execution
        .map(|budget| {
            crate::property_scan_memory::mul(
                batch_size.max(1),
                std::mem::size_of::<crate::property_scan_memory::Snapshot>(),
            )
            .and_then(|bytes| budget.reserve(bytes))
        })
        .transpose()
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let emit_rows = |rows: &mut Vec<crate::property_scan_memory::Snapshot>| -> Result<
        (RecordBatch, Option<Arc<crate::property_scan_memory::Guard>>),
        crate::GfError,
    > {
        let mut guard = execution
            .map(|budget| {
                let empty = arrow::datatypes::Schema::empty();
                crate::property_scan_memory::codec(rows, schema.as_deref().unwrap_or(&empty))
                    .and_then(|bytes| budget.reserve(bytes))
            })
            .transpose()?;
        let batch = if let Some(budget) = execution {
            crate::writer::admitted_property_snapshots_to_batch(
                stem,
                is_edge,
                rows.drain(..),
                budget,
            )?
        } else {
            crate::writer::property_snapshots_to_batch(
                stem,
                is_edge,
                rows.drain(..).map(|snapshot| snapshot.row).collect(),
            )?
        }
        .ok_or_else(|| crate::GfError::Storage("property batch disappeared".into()))?;
        let batch = normalize_property_batch(batch, schema.as_ref())?;
        let batch = project_property_batch(batch, kind.uuid_field(), selected_properties)?;
        if let Some(guard) = guard.as_mut() {
            let bytes = crate::property_scan_memory::add(
                batch.get_array_memory_size(),
                crate::property_scan_memory::add(
                    1024,
                    crate::property_scan_memory::mul(batch.num_columns(), 128)?,
                )?,
            )?;
            guard.shrink_to(bytes)?;
        }
        let memory = guard.map(Arc::new);
        Ok((batch, memory))
    };
    let mut rows = Vec::with_capacity(batch_size.max(1));
    let mut stopped = false;
    let mut failure = None;
    let membership: Option<&dyn crate::uuid_set::UuidMembership> = match (&ranged, &uuid_filter) {
        (Some(ranged), _) => Some(ranged),
        (None, Some(uuids)) => Some(uuids),
        (None, None) => None,
    };
    if membership.is_none_or(|uuids| !uuids.is_empty()) {
        let read = crate::property_overlay::RouteRead {
            kind,
            route: stem,
            selected_properties: streamed_properties.as_ref(),
            uuids: membership,
            limits,
            collect,
        };
        let work = inventory
            .visit_route_streaming_admitted(&read, execution.cloned(), |row| {
                // A candidate's newest snapshot may differ from the value the
                // candidate scan saw; the predicate decides.
                if uuid_filter.is_some()
                    && equality.is_some_and(|equality| !equality.holds(&row.values))
                {
                    return Ok(true);
                }
                rows.push(row);
                // The accumulator's complete capacity is prepaid above. Its
                // inline Snapshot now has an owner independent of the reader.
                rows.last_mut().expect("just pushed").release_carrier()?;
                if rows.len() >= batch_size.max(1) {
                    if let Some(budget) = execution {
                        budget.check()?;
                    }
                    let (batch, memory) = emit_rows(&mut rows)?;
                    match visit(&batch, memory) {
                        Ok(true) => {}
                        Ok(false) => stopped = true,
                        Err(error) => {
                            failure = Some(error);
                            stopped = true;
                        }
                    }
                }
                Ok(!stopped)
            })
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        metrics.absorb(&work);
    }
    if let Some(error) = failure {
        return Err(error);
    }
    if !stopped && !rows.is_empty() {
        if let Some(budget) = execution {
            budget
                .check()
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
        }
        let (batch, memory) =
            emit_rows(&mut rows).map_err(|error| DataFusionError::External(Box::new(error)))?;
        let _ = visit(&batch, memory)?;
    }
    Ok(collect.then_some(metrics))
}

// Normalize decoded columns against their selected schema. Omitted required
// fields must not become null placeholders before projection removes them.
fn project_property_schema(
    schema: SchemaRef,
    uuid_field: &str,
    selected_properties: Option<&std::collections::BTreeSet<String>>,
) -> Result<SchemaRef, graphforge_core::GfError> {
    let Some(selected_properties) = selected_properties else {
        return Ok(schema);
    };
    let indices = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| {
            field.name() == uuid_field || selected_properties.contains(field.name())
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    schema
        .project(&indices)
        .map(Arc::new)
        .map_err(|error| graphforge_core::GfError::Storage(error.to_string()))
}

fn project_property_batch(
    batch: RecordBatch,
    uuid_field: &str,
    selected_properties: Option<&std::collections::BTreeSet<String>>,
) -> Result<RecordBatch, graphforge_core::GfError> {
    let Some(selected_properties) = selected_properties else {
        return Ok(batch);
    };
    let indices = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| {
            field.name() == uuid_field || selected_properties.contains(field.name())
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    batch
        .project(&indices)
        .map_err(|error| graphforge_core::GfError::Storage(error.to_string()))
}

fn normalize_property_batch(
    batch: RecordBatch,
    schema: Option<&SchemaRef>,
) -> Result<RecordBatch, graphforge_core::GfError> {
    let Some(schema) = schema else {
        return Ok(batch);
    };
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let column = if field.data_type() == &arrow::datatypes::DataType::Null {
                arrow::array::new_null_array(field.data_type(), batch.num_rows())
            } else {
                batch
                    .column_by_name(field.name())
                    .cloned()
                    .unwrap_or_else(|| {
                        arrow::array::new_null_array(field.data_type(), batch.num_rows())
                    })
            };
            normalize_property_column(column, field.data_type())
        })
        .collect::<Result<Vec<_>, graphforge_core::GfError>>()?;
    RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|error| graphforge_core::GfError::Storage(error.to_string()))
}

/// Rebuild a homogeneous scalar batch column in the route's authenticated
/// heterogeneous representation. A filtered read can contain only one of the
/// historical scalar types even though the complete route schema is tagged.
fn normalize_property_column(
    column: ArrayRef,
    expected: &arrow::datatypes::DataType,
) -> Result<ArrayRef, graphforge_core::GfError> {
    use graphforge_value::heterogeneous::{Scalar, encode_scalar};

    if expected != &arrow::datatypes::DataType::Struct(crate::writer::heterogeneous_scalar_fields())
    {
        return Ok(column);
    }
    macro_rules! encode_values {
        ($array_type:ty, $variant:ident) => {{
            let values = column
                .as_any()
                .downcast_ref::<$array_type>()
                .ok_or_else(|| {
                    graphforge_core::GfError::Storage(
                        "property scalar column does not match its Arrow type".into(),
                    )
                })?;
            Arc::new(encode_scalar((0..values.len()).map(|row| {
                (!values.is_null(row)).then(|| Scalar::$variant(values.value(row)))
            }))) as ArrayRef
        }};
    }
    let encoded = match column.data_type() {
        arrow::datatypes::DataType::Int64 => encode_values!(Int64Array, Int),
        arrow::datatypes::DataType::Float64 => encode_values!(Float64Array, Float),
        arrow::datatypes::DataType::Utf8 => encode_values!(StringArray, Str),
        arrow::datatypes::DataType::Boolean => encode_values!(BooleanArray, Bool),
        // Null-only columns are handled by the route-schema projection above;
        // the tagged array is already correct when a canonical tag is present.
        _ => return Ok(column),
    };
    Ok(encoded)
}

/// Stream `properties/<stem>.parquet` as bounded batches without concatenating
/// the complete property table into one `RecordBatch` (#341 enrichment).
///
/// Returns an empty `Vec` when the file is absent. When present, batches honor
/// `batch_size` (clamped to at least 1) and retain natural Parquet row-group
/// boundaries subject to that cap.
///
/// # Errors
/// Propagates Parquet / Arrow errors encountered while reading.
pub fn read_properties_batched(
    dir: &Path,
    stem: &str,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let mut out = Vec::new();
    visit_properties_batched(dir, stem, batch_size, |batch| {
        out.push(batch.clone());
        Ok(true)
    })?;
    Ok(out)
}

/// Visit `properties/<stem>.parquet` one bounded batch at a time (#706).
///
/// `visit` returns `Ok(true)` to continue or `Ok(false)` to stop early without
/// decoding the rest of the file. A missing file is a no-op (zero visits).
///
/// # Errors
/// Propagates Parquet / Arrow errors, or any error returned by `visit`.
pub fn visit_properties_batched<F>(
    dir: &Path,
    stem: &str,
    batch_size: usize,
    visit: F,
) -> Result<(), DataFusionError>
where
    F: FnMut(&RecordBatch) -> Result<bool, DataFusionError>,
{
    super::visit_property_overlay_batched(dir, stem, false, batch_size, visit)
}

/// Visit the authenticated newest-wins node-property overlay while retaining
/// exact physical-fragment evidence for source-byte admission and snapshots.
///
/// Every physical fragment is admitted and authenticated once by storage, but
/// `visit` receives only the newest live row for each UUID in a route. This
/// keeps consumers from mistaking superseded immutable snapshots for duplicate
/// logical rows.
///
/// `retained` is the session's already admitted inventory. Without it the
/// project's current authority is captured here, which checksums every payload
/// of a raw workspace; a caller that holds the inventory must pass it so the
/// read costs the property sources it touches, not the graph.
pub fn visit_node_property_overlay_admitted<F>(
    dir: &Path,
    retained: Option<&crate::AuthenticatedPropertyInventory>,
    batch_size: usize,
    byte_limit: u64,
    projected_columns: Option<&std::collections::BTreeSet<String>>,
    evidence: &mut Vec<AdmittedSourceFile>,
    mut visit: F,
) -> Result<u64, DataFusionError>
where
    F: FnMut(&str, &RecordBatch) -> Result<bool, DataFusionError>,
{
    let captured;
    let inventory = if let Some(retained) = retained {
        retained
    } else {
        captured = crate::property_overlay::authenticated_property_inventory(dir)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        &captured
    };
    let admitted =
        inventory.admitted_source_files(crate::property_overlay::PropertyRouteKind::Node);
    let total = admitted.iter().try_fold(0_u64, |sum, file| {
        sum.checked_add(file.byte_length).ok_or_else(|| {
            DataFusionError::ResourcesExhausted("property source bytes overflow".into())
        })
    })?;
    if total > byte_limit {
        return Err(DataFusionError::ResourcesExhausted(format!(
            "property source bytes exceed {byte_limit}"
        )));
    }

    let routes = inventory
        .routes(crate::property_overlay::PropertyRouteKind::Node)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let scratch = inventory
        .create_snapshot_scratch()
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let mut stopped = false;
    for route in routes {
        let mut rows = Vec::with_capacity(batch_size.max(1));
        inventory
            .visit_route_projected_optional(
                crate::property_overlay::PropertyRouteKind::Node,
                &route,
                scratch.path(),
                crate::property_overlay::PropertyOverlayLimits::default(),
                None,
                |mut row| {
                    if stopped {
                        return Ok(());
                    }
                    if let Some(columns) = projected_columns {
                        row.values.retain(|name, _| columns.contains(name));
                    }
                    rows.push(row);
                    if rows.len() >= batch_size.max(1) {
                        let batch = crate::writer::property_snapshots_to_batch(
                            &route,
                            false,
                            std::mem::take(&mut rows),
                        )?
                        .ok_or_else(|| {
                            graphforge_core::GfError::Storage("property batch disappeared".into())
                        })?;
                        stopped = !visit(&route, &batch).map_err(|error| {
                            graphforge_core::GfError::Storage(error.to_string())
                        })?;
                    }
                    Ok(())
                },
            )
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        if !stopped && !rows.is_empty() {
            let batch = crate::writer::property_snapshots_to_batch(&route, false, rows)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?
                .ok_or_else(|| DataFusionError::Execution("property batch disappeared".into()))?;
            stopped = !visit(&route, &batch)?;
        }
        if stopped {
            break;
        }
    }
    evidence.extend(admitted);
    Ok(total)
}

/// Visit an already-enumerated property source set through the same stable file
/// handles used for byte admission. This prevents pathname replacement between
/// accounting and Parquet decode and bounds each compressed column chunk before
/// Arrow allocation.
pub fn visit_property_fragments_admitted<F>(
    fragments: &[(String, PathBuf)],
    batch_size: usize,
    byte_limit: u64,
    projected_columns: Option<&std::collections::BTreeSet<String>>,
    evidence: &mut Vec<AdmittedSourceFile>,
    mut visit: F,
) -> Result<u64, DataFusionError>
where
    F: FnMut(&str, &RecordBatch) -> Result<bool, DataFusionError>,
{
    let mut total = 0_u64;
    for (stem, path) in fragments {
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        let mut file = options.open(path).map_err(|e| io_err(&e))?;
        let metadata = file.metadata().map_err(|e| io_err(&e))?;
        if !metadata.file_type().is_file() {
            return Err(DataFusionError::Execution(format!(
                "property source {} is not a regular file",
                path.display()
            )));
        }
        total = total.checked_add(metadata.len()).ok_or_else(|| {
            DataFusionError::ResourcesExhausted("property source bytes overflow".into())
        })?;
        if total > byte_limit {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "property source bytes exceed {byte_limit}"
            )));
        }
        preflight_parquet_handle(&mut file, metadata.len())?;
        evidence.push(checksum_admitted_source(
            property_relative_name(stem, path)?,
            &mut file,
            metadata.len(),
        )?);
        let mut builder =
            ParquetRecordBatchReaderBuilder::try_new(crate::catalog::admitted_path_file(file)?)
                .map_err(parquet_err)?;
        admit_decoded_parquet(&builder)?;
        if let Some(columns) = projected_columns {
            use parquet::arrow::ProjectionMask;
            let roots = builder
                .schema()
                .fields()
                .iter()
                .enumerate()
                .filter_map(|(index, field)| columns.contains(field.name()).then_some(index))
                .collect::<Vec<_>>();
            let projection = ProjectionMask::roots(builder.parquet_schema(), roots);
            builder = builder.with_projection(projection);
        }
        let reader = builder
            .with_batch_size(batch_size.max(1))
            .build()
            .map_err(parquet_err)?;
        for batch in reader {
            let batch = batch.map_err(parquet_err)?;
            if !visit(stem, &batch)? {
                return Ok(total);
            }
        }
    }
    Ok(total)
}

/// Edge analogue of [`read_properties`]: read `edge_properties/<stem>.parquet`
/// (keyed by `edge_uuid`), discovering its dynamic schema from the file. Returns
/// an **empty `Vec`** when the file is absent.
///
/// # Errors
/// Propagates Parquet / Arrow errors encountered while reading.
pub fn read_edge_properties(dir: &Path, stem: &str) -> Result<Vec<RecordBatch>, DataFusionError> {
    read_property_overlay(dir, stem, true)
}

/// Read edge properties using the caller's already admitted generation authority.
pub fn read_edge_properties_from_inventory(
    dir: &Path,
    inventory: &crate::AuthenticatedPropertyInventory,
    route: &str,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let mut batches = Vec::new();
    visit_property_overlay_batched_with_inventory(
        dir,
        Some(inventory),
        route,
        true,
        8_192,
        |batch| {
            batches.push(batch.clone());
            Ok(true)
        },
    )?;
    Ok(batches)
}

/// Read node properties using the caller's admitted route inventory.
pub fn read_properties_from_inventory(
    dir: &Path,
    inventory: &crate::AuthenticatedPropertyInventory,
    route: &str,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let mut batches = Vec::new();
    visit_property_overlay_batched_with_inventory(
        dir,
        Some(inventory),
        route,
        false,
        8_192,
        |batch| {
            batches.push(batch.clone());
            Ok(true)
        },
    )?;
    Ok(batches)
}

/// Read an authenticated newest-wins edge-property overlay while decoding
/// only the requested property names plus the mandatory edge UUID key.
#[doc(hidden)]
pub fn read_edge_properties_projected(
    dir: &Path,
    stem: &str,
    property_names: &[String],
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let selected = property_names.iter().cloned().collect();
    read_property_overlay_projected(dir, stem, true, Some(&selected))
}

/// Read selected edge properties through the caller's pinned route inventory.
pub fn read_edge_properties_projected_from_inventory(
    dir: &Path,
    inventory: &crate::AuthenticatedPropertyInventory,
    route: &str,
    property_names: &[String],
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let selected = property_names.iter().cloned().collect();
    let mut batches = Vec::new();
    visit_property_overlay_batched_projected(
        dir,
        Some(inventory),
        route,
        true,
        8_192,
        Some(&selected),
        |batch| {
            batches.push(batch.clone());
            Ok(true)
        },
    )?;
    Ok(batches)
}

#[cfg(test)]
mod tests;
