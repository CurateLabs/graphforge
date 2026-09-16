//! Authenticated property overlay readers and bounded visitation.

use super::AdmittedSourceFile;
use super::admit_decoded_parquet;
use super::hash_admitted_source;
use super::io_err;
use super::parquet_err;
use super::preflight_parquet_handle;
use super::property_relative_name;
use arrow::array::RecordBatch;
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
    mut visit: F,
) -> Result<crate::PropertyOverlayMetrics, DataFusionError>
where
    F: FnMut(&RecordBatch) -> Result<bool, DataFusionError>,
{
    if !dir.exists() {
        return Ok(crate::PropertyOverlayMetrics::default());
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
    let schema = inventory.route_schema(kind, stem);
    let scratch = tempfile::tempdir().map_err(|error| io_err(&error))?;
    let mut rows = Vec::with_capacity(batch_size.max(1));
    let mut stopped = false;
    let metrics = inventory
        .visit_route_projected(
            kind,
            stem,
            scratch.path(),
            crate::property_overlay::PropertyOverlayLimits::default(),
            selected_properties,
            |row| {
                if stopped {
                    return Ok(());
                }
                rows.push(row);
                if rows.len() >= batch_size.max(1) {
                    let batch = crate::writer::property_snapshots_to_batch(
                        stem,
                        is_edge,
                        std::mem::take(&mut rows),
                    )?
                    .ok_or_else(|| {
                        graphforge_core::GfError::Storage("property batch disappeared".into())
                    })?;
                    let batch = normalize_property_batch(batch, schema.as_ref())?;
                    let batch =
                        project_property_batch(batch, kind.uuid_field(), selected_properties)?;
                    stopped =
                        !visit(&batch).map_err(graphforge_core::GfError::from_execution_error)?;
                }
                Ok(())
            },
        )
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    if !stopped && !rows.is_empty() {
        let batch = crate::writer::property_snapshots_to_batch(stem, is_edge, rows)
            .map_err(|error| DataFusionError::External(Box::new(error)))?
            .ok_or_else(|| DataFusionError::Execution("property batch disappeared".into()))?;
        let batch = normalize_property_batch(batch, schema.as_ref())
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let batch = project_property_batch(batch, kind.uuid_field(), selected_properties)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let _ = visit(&batch)?;
    }
    Ok(metrics)
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
            if field.data_type() == &arrow::datatypes::DataType::Null {
                arrow::array::new_null_array(field.data_type(), batch.num_rows())
            } else {
                batch
                    .column_by_name(field.name())
                    .cloned()
                    .unwrap_or_else(|| {
                        arrow::array::new_null_array(field.data_type(), batch.num_rows())
                    })
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|error| graphforge_core::GfError::Storage(error.to_string()))
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
pub fn visit_node_property_overlay_admitted<F>(
    dir: &Path,
    batch_size: usize,
    byte_limit: u64,
    projected_columns: Option<&std::collections::BTreeSet<String>>,
    evidence: &mut Vec<AdmittedSourceFile>,
    mut visit: F,
) -> Result<u64, DataFusionError>
where
    F: FnMut(&str, &RecordBatch) -> Result<bool, DataFusionError>,
{
    let inventory = crate::property_overlay::authenticated_property_inventory(dir)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let admitted = inventory
        .admitted_source_files(crate::property_overlay::PropertyRouteKind::Node)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
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
    let scratch = tempfile::tempdir().map_err(|error| io_err(&error))?;
    let mut stopped = false;
    for route in routes {
        let mut rows = Vec::with_capacity(batch_size.max(1));
        inventory
            .visit_route(
                crate::property_overlay::PropertyRouteKind::Node,
                &route,
                scratch.path(),
                crate::property_overlay::PropertyOverlayLimits::default(),
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
        evidence.push(hash_admitted_source(
            property_relative_name(stem, path)?,
            &mut file,
            metadata.len(),
        )?);
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(parquet_err)?;
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
