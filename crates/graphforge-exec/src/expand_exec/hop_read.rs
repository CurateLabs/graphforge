//! Admission and ownership of single-hop read buffers.

use super::SingleHopConfig;
use crate::chunked_reservation::ChunkedReservation;
use arrow::array::RecordBatch;
use datafusion::error::DataFusionError;
use graphforge_core::GfError;

fn size_refused() -> GfError {
    GfError::from_execution_error(DataFusionError::ResourcesExhausted(
        "ExpandExec hop read: allocation size overflow".into(),
    ))
}

fn unsupported_arrow(kind: &arrow::datatypes::DataType) -> GfError {
    GfError::from_execution_error(DataFusionError::ResourcesExhausted(format!(
        "ExpandExec hop read: unsupported Arrow allocation bound for {kind}",
    )))
}

pub(super) fn checked_add(a: usize, b: usize) -> Result<usize, GfError> {
    a.checked_add(b).ok_or_else(size_refused)
}

pub(super) fn checked_mul(a: usize, b: usize) -> Result<usize, GfError> {
    a.checked_mul(b)
        .filter(|n| isize::try_from(*n).is_ok())
        .ok_or_else(size_refused)
}

/// Rust 1.96 std HashMap/HashSet use hashbrown: at least four buckets,
/// seven-eighths load, power-of-two buckets, and a trailing control group.
/// Includes alignment for the entry and the largest pinned SIMD group.
pub(super) fn table_bytes<T>(entries: usize) -> Result<usize, GfError> {
    if entries == 0 {
        return Ok(0);
    }
    let buckets = (checked_add(checked_mul(entries, 8)?, 6)? / 7)
        .max(4)
        .checked_next_power_of_two()
        .ok_or_else(size_refused)?;
    let alignment = 16usize.max(std::mem::align_of::<T>());
    let controls = checked_add(
        checked_mul(buckets, std::mem::size_of::<T>())?,
        alignment - 1,
    )? & !(alignment - 1);
    let bytes = checked_add(controls, checked_add(buckets, 16)?)?;
    if bytes > isize::MAX as usize - alignment {
        return Err(size_refused());
    }
    Ok(bytes)
}

/// Admission is deliberately outside the allocation closure. Tests can observe
/// the real constructor boundary without a process-global allocator hook.
pub(super) fn admitted_table<T, R>(
    charge: &mut HopReadCharge,
    entries: usize,
    allocate: impl FnOnce(usize) -> R,
) -> Result<R, GfError> {
    charge.charge(table_bytes::<T>(entries)?)?;
    Ok(allocate(entries))
}

pub(super) fn vector_capacity<T>(
    charge: &mut HopReadCharge,
    values: &mut Vec<T>,
    needed: usize,
) -> Result<(), GfError> {
    if needed <= values.capacity() {
        return Ok(());
    }
    let capacity = checked_mul(values.capacity().max(2), 2)?.max(needed);
    let old = checked_mul(values.capacity(), std::mem::size_of::<T>())?;
    charge.charge(checked_mul(capacity, std::mem::size_of::<T>())?)?;
    values.reserve_exact(capacity - values.len());
    charge.release(old);
    Ok(())
}

pub(super) fn set_insert(
    charge: &mut HopReadCharge,
    values: &mut std::collections::HashSet<u64>,
    value: u64,
) -> Result<bool, GfError> {
    if values.contains(&value) {
        return Ok(false);
    }
    if values.len() == values.capacity() {
        let capacity = checked_mul(values.capacity().max(2), 2)?;
        let old = table_bytes::<u64>(values.capacity())?;
        charge.charge(table_bytes::<u64>(capacity)?)?;
        values.reserve(capacity - values.len());
        charge.release(old);
    }
    Ok(values.insert(value))
}

fn view_buffers(array: &dyn arrow::array::Array) -> Option<&[arrow::buffer::Buffer]> {
    use arrow::array::AsArray;
    match array.data_type() {
        arrow::datatypes::DataType::Utf8View => Some(array.as_string_view().data_buffers()),
        arrow::datatypes::DataType::BinaryView => Some(array.as_binary_view().data_buffers()),
        _ => None,
    }
}

/// Pinned take_byte_view copies descriptors and every Buffer descriptor, then
/// converts the temporary Vec into a new Arc slice. Payloads remain aliases of
/// ALL source buffers, including unused ones. take_nulls either builds one
/// aligned bitmap or aliases the complete index-null backing.
fn view_take_bytes(
    array: &dyn arrow::array::Array,
    buffers: &[arrow::buffer::Buffer],
    indices: &arrow::array::UInt32Array,
) -> Result<usize, GfError> {
    use arrow::array::Array;
    let payload = buffers
        .iter()
        .try_fold(0, |bytes, buffer| checked_add(bytes, buffer.capacity()))?;
    let descriptors = checked_mul(indices.len(), 16)?;
    let bitmap = checked_add(indices.len().div_ceil(8), 64)?;
    let aliased_nulls = indices.nulls().map_or(0, |nulls| nulls.buffer().capacity());
    let containers = checked_add(
        checked_mul(
            buffers.len(),
            2 * std::mem::size_of::<arrow::buffer::Buffer>(),
        )?,
        2 * std::mem::size_of::<usize>(),
    )?;
    let headers = checked_add(
        checked_mul(2, std::mem::size_of_val(array))?,
        checked_add(2 * std::mem::size_of::<usize>(), 4 * 64)?,
    )?;
    checked_add(
        payload,
        checked_add(
            descriptors,
            checked_add(
                bitmap,
                checked_add(aliased_nulls, checked_add(containers, headers)?)?,
            )?,
        )?,
    )
}

/// Struct take invokes take recursively with the SAME row indexes, including
/// children beneath a null parent. Bound those actual selections rather than
/// multiplying an unrelated child's largest string by the output row count.
fn selected_take_bytes(
    array: &dyn arrow::array::Array,
    indices: &arrow::array::UInt32Array,
) -> Result<usize, GfError> {
    use arrow::array::{Array, AsArray};
    if let Some(buffers) = view_buffers(array) {
        return view_take_bytes(array, buffers, indices);
    }
    if matches!(array.data_type(), arrow::datatypes::DataType::Struct(_)) {
        let children = array
            .as_struct()
            .columns()
            .iter()
            .try_fold(0, |bytes, child| {
                checked_add(bytes, selected_take_bytes(child.as_ref(), indices)?)
            })?;
        // take_struct's child ArrayRef vectors, validity and aligned headers.
        return checked_add(
            children,
            checked_add(
                checked_mul(
                    array.as_struct().num_columns().max(4),
                    4 * std::mem::size_of::<(arrow::datatypes::FieldRef, arrow::array::ArrayRef)>(),
                )?,
                checked_add(
                    2048,
                    checked_add(
                        indices.len().div_ceil(8),
                        indices.nulls().map_or(0, |nulls| nulls.buffer().capacity()),
                    )?,
                )?,
            )?,
        );
    }
    crate::row_exec::hop_take_bytes(array, indices).map_err(GfError::from_execution_error)
}

/// Keep construction authority until the kernel has returned and dropped its
/// mutable builders. Only then transfer to the actual retained array, including
/// owned child/view containers omitted by Arrow's memory getter. This never
/// grows an envelope after allocation; errors leave its original authority live
/// while the local array drops before the caller's guard.
pub(super) fn construct_array(
    charge: &mut HopReadCharge,
    envelope: usize,
    construct: impl FnOnce() -> Result<arrow::array::ArrayRef, GfError>,
) -> Result<arrow::array::ArrayRef, GfError> {
    charge.charge(envelope)?;
    let array = construct()?;
    let retained = retained_array_bytes(array.as_ref())?;
    if retained > envelope {
        return Err(GfError::Execution(
            "Expand array exceeded its admitted construction envelope".into(),
        ));
    }
    charge.replace(envelope, retained)?;
    Ok(array)
}

pub(super) fn retained_array_bytes(array: &dyn arrow::array::Array) -> Result<usize, GfError> {
    checked_add(array.get_array_memory_size(), retained_containers(array)?)
}

pub(super) fn take(
    charge: &mut HopReadCharge,
    array: &arrow::array::ArrayRef,
    indices: &arrow::array::UInt32Array,
) -> Result<arrow::array::ArrayRef, GfError> {
    construct_array(
        charge,
        selected_take_bytes(array.as_ref(), indices)?,
        || arrow::compute::take(array, indices, None).map_err(GfError::from_execution_error),
    )
}

/// zip's MutableArrayData copies disjoint slices of these already-assembled
/// full-length columns. Byte offsets include payload stored beneath NULLs;
/// unrelated largest values must not be multiplied by the row count again.
pub(super) fn zip_copy_bytes(array: &dyn arrow::array::Array) -> Result<usize, GfError> {
    use arrow::array::AsArray;
    use arrow::datatypes::DataType;
    let (payload, width) = match array.data_type() {
        DataType::Utf8 => {
            let offsets = array.as_string::<i32>().value_offsets();
            (
                usize::try_from(offsets[offsets.len() - 1] - offsets[0])
                    .map_err(|_| size_refused())?,
                4,
            )
        }
        DataType::Binary => {
            let offsets = array.as_binary::<i32>().value_offsets();
            (
                usize::try_from(offsets[offsets.len() - 1] - offsets[0])
                    .map_err(|_| size_refused())?,
                4,
            )
        }
        DataType::LargeUtf8 => {
            let offsets = array.as_string::<i64>().value_offsets();
            (
                usize::try_from(offsets[offsets.len() - 1] - offsets[0])
                    .map_err(|_| size_refused())?,
                8,
            )
        }
        DataType::LargeBinary => {
            let offsets = array.as_binary::<i64>().value_offsets();
            (
                usize::try_from(offsets[offsets.len() - 1] - offsets[0])
                    .map_err(|_| size_refused())?,
                8,
            )
        }
        DataType::Struct(_) => {
            let children = array
                .as_struct()
                .columns()
                .iter()
                .try_fold(0, |bytes, child| {
                    checked_add(bytes, zip_copy_bytes(child.as_ref())?)
                })?;
            return checked_add(
                children,
                checked_add(
                    // MutableArrayData allocates parent validity even for an
                    // empty Struct whenever either input contains NULLs.
                    checked_add(
                        2048,
                        checked_mul(checked_add(array.len().div_ceil(8), 64)?, 4)?,
                    )?,
                    checked_mul(
                        array.as_struct().num_columns().max(4),
                        4 * std::mem::size_of::<(arrow::datatypes::FieldRef, arrow::array::ArrayRef)>(
                        ),
                    )?,
                )?,
            );
        }
        _ => {
            let bound = crate::row_exec::hop_copy_bytes(array, array.len())
                .map_err(GfError::from_execution_error)?;
            // Mutable nested copying may include populated NULL spans. Retain
            // the complete source buffer allowance alongside its initial-slot
            // bound, without dropping the established growth/header envelope.
            return checked_add(bound, checked_mul(array.get_buffer_memory_size(), 4)?);
        }
    };
    checked_add(
        checked_mul(
            checked_add(
                payload.max(array.len()),
                checked_add(
                    checked_mul(checked_add(array.len(), 1)?, width)?,
                    array.len().div_ceil(8),
                )?,
            )?,
            4,
        )?,
        2048,
    )
}

/// Filter copies a subset of each complete source span, never an arbitrary
/// multiplicity of its largest value. Full retained capacity is also admitted:
/// the all-selected fast path returns an alias, including unused backing.
pub(super) fn filter_workspace(
    charge: &mut HopReadCharge,
    columns: usize,
    rows: usize,
) -> Result<(), GfError> {
    // Result<Vec<ArrayRef>> grows geometrically from four slots. Admit its
    // old/new overlap and the cloned bool Vec plus aligned mask construction
    // before the caller constructs either one.
    let arrays = checked_mul(
        columns.max(4),
        3 * std::mem::size_of::<arrow::array::ArrayRef>(),
    )?;
    let mask = checked_add(
        rows,
        checked_add(checked_mul(checked_add(rows.div_ceil(8), 64)?, 4)?, 2048)?,
    )?;
    charge.charge(checked_add(arrays, mask)?)
}

/// Filter one column under the caller's already-admitted predicate mask.
pub(super) fn filter(
    charge: &mut HopReadCharge,
    array: &arrow::array::ArrayRef,
    mask: &arrow::array::BooleanArray,
) -> Result<arrow::array::ArrayRef, GfError> {
    use arrow::compute::kernels::filter::FilterBuilder;
    let retained = checked_add(
        array.get_array_memory_size(),
        retained_containers(array.as_ref())?,
    )?;
    let copies = zip_copy_bytes(array.as_ref())?.max(retained);
    // Struct filtering can optimize the predicate into Vec<usize> indexes or
    // Vec<(usize, usize)> ranges. Four full pair slots per input row cover
    // either growing Vec and its old/new overlap. Scalar filters iterate the
    // mask directly. A nullable mask additionally materializes one bitmap.
    let optimized = if FilterBuilder::is_optimize_beneficial(array.data_type()) {
        checked_mul(mask.len(), 4 * std::mem::size_of::<(usize, usize)>())?
    } else {
        0
    };
    let scratch = checked_add(optimized, checked_add(mask.len().div_ceil(8), 64)?)?;
    construct_array(charge, checked_add(copies, scratch)?, || {
        arrow::compute::filter(array, mask).map_err(GfError::from_execution_error)
    })
}

/// The target tree exists only while resolving the id-keyed property source.
/// Its nodes have dropped before any subsequent read/output workspace is used.
pub(super) fn with_property_targets<R>(
    charge: &mut HopReadCharge,
    traversed: &std::collections::HashSet<u64>,
    read: impl FnOnce(&std::collections::BTreeSet<u64>) -> Result<R, GfError>,
) -> Result<R, GfError> {
    // Unchanged conservative leaf/internal-node envelope.
    let bytes = checked_mul(
        traversed.len(),
        16 * (std::mem::size_of::<u64>() + 4 * std::mem::size_of::<usize>()),
    )?;
    charge.charge(bytes)?;
    let targets = traversed.iter().copied().collect();
    let result = read(&targets);
    drop(targets);
    charge.release(bytes);
    result
}

/// Buffers and growing builders used by unused_expand_column. Four copies
/// cover growing Vec/MutableBuffer old/new overlap; List<UInt32>'s default
/// primitive builder starts with 1024 values even for empty child lists.
pub(super) fn unused_bytes(
    kind: &arrow::datatypes::DataType,
    rows: usize,
) -> Result<usize, GfError> {
    use arrow::datatypes::DataType;
    let values = match kind {
        DataType::Null => 0,
        DataType::Boolean => rows.div_ceil(8),
        DataType::FixedSizeBinary(width) => checked_mul(
            checked_add(rows, 1)?,
            usize::try_from(*width).map_err(|_| size_refused())?,
        )?,
        DataType::Utf8 | DataType::Binary => checked_mul(checked_add(rows, 1)?, 32)?,
        DataType::LargeUtf8 | DataType::LargeBinary => checked_mul(checked_add(rows, 1)?, 40)?,
        DataType::Utf8View | DataType::BinaryView => checked_mul(rows, 16)?,
        DataType::List(child) | DataType::LargeList(child) => checked_add(
            checked_mul(checked_add(rows, 1)?, 8)?,
            checked_add(4096, unused_bytes(child.data_type(), 0)?)?,
        )?,
        DataType::Struct(fields) => fields.iter().try_fold(0, |n, field| {
            checked_add(n, unused_bytes(field.data_type(), rows)?)
        })?,
        DataType::FixedSizeList(child, width) => unused_bytes(
            child.data_type(),
            checked_mul(rows, usize::try_from(*width).map_err(|_| size_refused())?)?,
        )?,
        other => checked_mul(
            rows,
            other
                .primitive_width()
                .ok_or_else(|| unsupported_arrow(other))?,
        )?,
    };
    checked_add(
        checked_mul(checked_add(values, rows.div_ceil(8))?, 4)?,
        2048,
    )
}

pub(super) fn unused(
    charge: &mut HopReadCharge,
    field: &arrow::datatypes::Field,
    rows: usize,
) -> Result<arrow::array::ArrayRef, GfError> {
    construct_array(charge, unused_bytes(field.data_type(), rows)?, || {
        // Empty inline descriptors have no payload buffer. Nullable views use
        // Arrow's ordinary new_null_array path; nonnullable placeholders preserve
        // their declared type and nullability without a string builder.
        if !field.is_nullable() {
            let descriptors = || arrow::buffer::ScalarBuffer::from(vec![0u128; rows]);
            let buffers = || Vec::<arrow::buffer::Buffer>::new();
            match field.data_type() {
                arrow::datatypes::DataType::Utf8View => {
                    return Ok(std::sync::Arc::new(arrow::array::StringViewArray::new(
                        descriptors(),
                        buffers(),
                        None,
                    )));
                }
                arrow::datatypes::DataType::BinaryView => {
                    return Ok(std::sync::Arc::new(arrow::array::BinaryViewArray::new(
                        descriptors(),
                        buffers(),
                        None,
                    )));
                }
                _ => {}
            }
        }
        super::unused_expand_column(field, rows)
    })
}

/// Data precedes its guard so errors and cancellation release backing buffers
/// before their reservation. This follows the existing producer stream's
/// output lifetime for constructor metadata. Every shared backing separately
/// owns its exact immutable capacity and lease token.
pub(super) struct HopChunk {
    pub(super) batch: RecordBatch,
    pub(super) charge: std::sync::Arc<HopReadCharge>,
}

struct HopReadAdmission<'a>(&'a SingleHopConfig);
impl graphforge_storage::read_memory::ReadAdmission for HopReadAdmission<'_> {
    fn reserve(
        &self,
        bytes: usize,
    ) -> datafusion::error::Result<graphforge_storage::read_memory::ReadGrant> {
        self.check_cancelled()?;
        let mut reservation = self.0.read_reservation.lock().map_err(|_| {
            DataFusionError::Internal("ExpandExec hop read reservation poisoned".into())
        })?;
        reservation.try_grow(bytes)?;
        Ok(graphforge_storage::read_memory::ReadGrant::from_reserved(
            reservation.extract(bytes)?,
        ))
    }
    fn check_cancelled(&self) -> datafusion::error::Result<()> {
        if self
            .0
            .demand
            .as_ref()
            .is_some_and(|demand| demand.is_cancelled())
        {
            return Err(DataFusionError::Execution(
                "ExpandExec hop read cancelled".into(),
            ));
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn read_edge_batch(
    cfg: &SingleHopConfig,
    charge: &mut HopReadCharge,
    traversed: &std::collections::HashSet<u64>,
    projection: &[usize],
    projected: bool,
    width: usize,
    observer: Option<&std::sync::Arc<dyn graphforge_storage::io_stats::FilteredReadObserver>>,
) -> Result<RecordBatch, GfError> {
    let inventory = cfg.provider.admitted_inventory();
    if let Some(inventory) = inventory.as_deref().filter(|_| {
        projected
            && (cfg.rel_type_name != "*"
                || matches!(cfg.mode, graphforge_core::OntologyMode::Exploratory))
    }) {
        charge.charge(checked_mul(width + 1, std::mem::size_of::<usize>())?)?;
        let projection = if projected {
            projection.to_vec()
        } else {
            (0..width).collect()
        };
        let batch = graphforge_storage::read_edges_filtered_prepaid_from_inventory(
            inventory,
            &cfg.rel_type_name,
            cfg.mode,
            traversed,
            &projection,
            observer,
            &HopReadAdmission(cfg),
        )
        .map_err(GfError::from_execution_error)?;
        return charge.adopt_source(batch);
    }
    // Unmigrated wildcard union and legacy discovery retain their existing
    // authority and behavior; they cannot manufacture a prepaid capability.
    let decode = if let Some(inventory) = inventory.as_deref() {
        charge.charge(checked_mul(width + 1, std::mem::size_of::<usize>())?)?;
        let indices = if projected {
            projection.to_vec()
        } else {
            (0..width).collect()
        };
        graphforge_storage::edge_decode_admission_bytes(
            inventory,
            &cfg.rel_type_name,
            cfg.mode,
            traversed,
            &indices,
        )
        .map_err(GfError::from_execution_error)?
    } else {
        0
    };
    charge.charge(decode)?;
    let batches = if let Some(inventory) = inventory.as_deref().filter(|_| projected) {
        graphforge_storage::read_edges_filtered_projected_from_inventory(
            inventory,
            &cfg.rel_type_name,
            cfg.mode,
            traversed,
            projection,
            observer,
        )
    } else if projected {
        graphforge_storage::read_edges_filtered_projected_observed(
            &cfg.dir,
            &cfg.rel_type_name,
            cfg.mode,
            traversed,
            projection,
            observer,
        )
    } else if let Some(inventory) = inventory.as_deref() {
        graphforge_storage::read_edges_filtered_observed_from_inventory(
            inventory,
            &cfg.rel_type_name,
            cfg.mode,
            traversed,
            observer,
        )
    } else {
        graphforge_storage::read_edges_filtered_observed(
            &cfg.dir,
            &cfg.rel_type_name,
            cfg.mode,
            traversed,
            observer,
        )
    }
    .map_err(GfError::from_execution_error)?;
    charge.replace(decode, batches_bytes(&batches))?;
    concat_precharged(charge, batches)
}

/// What a single-hop chunk has charged to [`SingleHopConfig::read_reservation`],
/// released when the chunk ends, by whatever path it ends.
pub(super) struct HopReadCharge {
    reservation: std::sync::Arc<std::sync::Mutex<ChunkedReservation>>,
    held: usize,
    prepaid_sources: Vec<graphforge_storage::read_memory::PrepaidBatch>,
    schema_owners: Vec<graphforge_storage::read_memory::ReadSchemaOwner>,
}

impl HopReadCharge {
    pub(super) fn new(reservation: &std::sync::Arc<std::sync::Mutex<ChunkedReservation>>) -> Self {
        Self {
            reservation: std::sync::Arc::clone(reservation),
            held: 0,
            prepaid_sources: Vec::new(),
            schema_owners: Vec::new(),
        }
    }

    pub(super) fn adopt_source(
        &mut self,
        source: graphforge_storage::read_memory::PrepaidBatch,
    ) -> Result<RecordBatch, GfError> {
        // Pin original arrays through output identity checks; no address can
        // be reused while a known backing is classified. This is chunk-local
        // provenance, never a process-wide allocation registry.
        let needed = self.prepaid_sources.len() + 1;
        if needed > self.prepaid_sources.capacity() {
            self.charge(checked_mul(
                needed,
                std::mem::size_of::<graphforge_storage::read_memory::PrepaidBatch>(),
            )?)?;
            self.prepaid_sources
                .reserve_exact(needed - self.prepaid_sources.len());
        }
        if needed > self.schema_owners.capacity() {
            self.charge(checked_mul(
                needed,
                std::mem::size_of::<graphforge_storage::read_memory::ReadSchemaOwner>(),
            )?)?;
            self.schema_owners
                .reserve_exact(needed - self.schema_owners.len());
        }
        self.charge(checked_mul(
            source.batch().num_columns(),
            std::mem::size_of::<arrow::array::ArrayRef>(),
        )?)?;
        let batch = source.batch().clone();
        self.prepaid_sources.push(source);
        Ok(batch)
    }

    /// Charge `bytes` that are now live. Fails, naming [`HOP_READ_CONSUMER`],
    /// when the pool cannot hold them; the failed charge is not recorded.
    pub(super) fn charge(&mut self, bytes: usize) -> Result<(), GfError> {
        let held = checked_add(self.held, bytes)?;
        self.reservation
            .lock()
            .map_err(|_| GfError::Execution("ExpandExec hop read reservation poisoned".into()))?
            .try_grow(bytes)
            .map_err(GfError::from_execution_error)?;
        self.held = held;
        Ok(())
    }

    /// Transfer a completed decoder envelope to the exact returned buffers.
    /// Grow/shrink only the difference while holding one reservation lock.
    pub(super) fn replace(&mut self, old: usize, new: usize) -> Result<(), GfError> {
        if old > self.held {
            return Err(GfError::Execution(
                "ExpandExec invalid hop reservation transfer".into(),
            ));
        }
        let held = checked_add(self.held - old, new)?;
        let mut reservation = self
            .reservation
            .lock()
            .map_err(|_| GfError::Execution("ExpandExec hop read reservation poisoned".into()))?;
        if new > old {
            reservation
                .try_grow(new - old)
                .map_err(GfError::from_execution_error)?;
        } else {
            reservation.shrink(old - new);
        }
        self.held = held;
        Ok(())
    }

    fn extract(
        &mut self,
        bytes: usize,
    ) -> Result<datafusion::execution::memory_pool::MemoryReservation, GfError> {
        if bytes > self.held {
            return Err(GfError::Execution(
                "invalid hop output reservation transfer".into(),
            ));
        }
        let reservation = self
            .reservation
            .lock()
            .map_err(|_| GfError::Execution("ExpandExec hop read reservation poisoned".into()))?
            .extract(bytes)
            .map_err(GfError::from_execution_error)?;
        self.held -= bytes;
        Ok(reservation)
    }

    pub(super) fn held_bytes_internal(&self) -> usize {
        self.held
    }

    #[cfg(test)]
    pub(super) fn held_bytes(&self) -> usize {
        self.held
    }

    /// Release `bytes` that are no longer live.
    pub(super) fn release(&mut self, bytes: usize) {
        let bytes = bytes.min(self.held);
        if let Ok(mut reservation) = self.reservation.lock() {
            reservation.shrink(bytes);
            self.held -= bytes;
        }
    }
}

impl Drop for HopReadCharge {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.prepaid_sources));
        drop(std::mem::take(&mut self.schema_owners));
        self.release(self.held);
    }
}

/// The pool consumer a single-hop chunk's edge and destination-node reads
/// charge: the decoded batches, their concatenation, and the id-to-row tables.
pub(super) const HOP_READ_CONSUMER: &str = "ExpandExec hop read";

/// Bytes the decoded `batches` hold.
pub(super) fn batches_bytes(batches: &[RecordBatch]) -> usize {
    batches
        .iter()
        .map(RecordBatch::get_array_memory_size)
        .fold(0, usize::saturating_add)
}

/// The rows of `batches` as one batch, charging `charge` for every live copy.
/// A lone batch is shared, not copied, so its charge stays; otherwise the copy
/// is charged before it is allocated, so an over-budget read fails before the
/// copy, and the decoded batches' charge is released once they are dropped.
/// Fails, typed, when the pool cannot hold the charge.
#[cfg(test)]
pub(super) fn concat_charged(
    charge: &mut HopReadCharge,
    batches: Vec<RecordBatch>,
) -> Result<RecordBatch, GfError> {
    let decoded = batches_bytes(&batches);
    charge.charge(decoded)?;
    concat_precharged(charge, batches)
}

/// Concatenate batches whose decoded buffers are already charged.
pub(super) fn concat_precharged(
    charge: &mut HopReadCharge,
    batches: Vec<RecordBatch>,
) -> Result<RecordBatch, GfError> {
    let decoded = batches_bytes(&batches);
    if let [only] = batches.as_slice() {
        return Ok(only.clone());
    }
    let schema = batches
        .first()
        .map(RecordBatch::schema)
        .ok_or_else(|| GfError::Execution("Expand: scan returned no batches".into()))?;
    charge.charge(decoded)?;
    let joined = arrow::compute::concat_batches(&schema, &batches)
        .map_err(|error| GfError::Execution(error.to_string()))?;
    drop(batches);
    charge.release(decoded);
    Ok(joined)
}

impl SingleHopConfig {
    /// Charge the held input batch and the output batch to the session memory pool.
    pub(super) fn account(
        &self,
        held: Option<&RecordBatch>,
        output: &RecordBatch,
    ) -> Result<(), DataFusionError> {
        let bytes =
            held.map_or(0, RecordBatch::get_array_memory_size) + output.get_array_memory_size();
        self.reservation
            .lock()
            .map_err(|_| DataFusionError::Internal("ExpandExec reservation poisoned".into()))?
            .try_resize(bytes)
    }
}

/// Count concrete claim calls without materializing ArrayData or buffer lists.
/// In particular ByteView has a variable number of payload buffers. Pinned
/// Arrow 58.4's concrete claim overrides visit these borrowed buffers directly.
fn claim_slots(array: &dyn arrow::array::Array) -> Result<usize, GfError> {
    use arrow::array::AsArray;
    use arrow::datatypes::DataType;
    let nulls = usize::from(array.nulls().is_some());
    let values = match array.data_type() {
        DataType::Null => return Ok(0),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => 2,
        DataType::Utf8View => checked_add(1, array.as_string_view().data_buffers().len())?,
        DataType::BinaryView => checked_add(1, array.as_binary_view().data_buffers().len())?,
        DataType::List(_) => {
            checked_add(1, claim_slots(array.as_list::<i32>().values().as_ref())?)?
        }
        DataType::LargeList(_) => {
            checked_add(1, claim_slots(array.as_list::<i64>().values().as_ref())?)?
        }
        DataType::FixedSizeList(_, _) => claim_slots(array.as_fixed_size_list().values().as_ref())?,
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .try_fold(0, |count, child| {
                checked_add(count, claim_slots(child.as_ref())?)
            })?,
        DataType::Boolean | DataType::FixedSizeBinary(_) => 1,
        other if other.primitive_width().is_some() => 1,
        other => return Err(unsupported_arrow(other)),
    };
    checked_add(values, nulls)
}

/// Arrow's array getter omits StructArray's owned Vec<ArrayRef>. Both
/// supported producers (take_struct and new_null_array) collect one child per
/// field. Pinned StructArray::from unzips the exact-sized child iterator with
/// Vec::reserve(n), whose nonempty minimum is four ArrayRefs; new_null_array's
/// trusted collect reserves exactly n. Preserve that
/// allocation after construction scratch drops. Child arrays can themselves
/// contain such vectors, including beneath lists.
fn retained_containers(array: &dyn arrow::array::Array) -> Result<usize, GfError> {
    use arrow::array::AsArray;
    use arrow::datatypes::DataType;
    match array.data_type() {
        DataType::Utf8View | DataType::BinaryView => checked_add(
            checked_mul(
                view_buffers(array).ok_or_else(size_refused)?.len(),
                std::mem::size_of::<arrow::buffer::Buffer>(),
            )?,
            2 * std::mem::size_of::<usize>(),
        ),
        DataType::Struct(_) => {
            let children = array.as_struct().columns();
            let capacity = if children.is_empty() {
                0
            } else {
                children.len().max(4)
            };
            children.iter().try_fold(
                checked_mul(capacity, std::mem::size_of::<arrow::array::ArrayRef>())?,
                |bytes, child| checked_add(bytes, retained_containers(child.as_ref())?),
            )
        }
        DataType::List(_) => retained_containers(array.as_list::<i32>().values().as_ref()),
        DataType::LargeList(_) => retained_containers(array.as_list::<i64>().values().as_ref()),
        DataType::FixedSizeList(_, _) => {
            retained_containers(array.as_fixed_size_list().values().as_ref())
        }
        _ => Ok(0),
    }
}

#[derive(Default)]
struct ViewClaims {
    slots: usize,
    containers: usize,
    owners: usize,
}
fn view_claims(array: &dyn arrow::array::Array) -> Result<ViewClaims, GfError> {
    use arrow::array::AsArray;
    use arrow::datatypes::DataType;
    if view_buffers(array).is_some() {
        return Ok(ViewClaims {
            slots: claim_slots(array)?,
            containers: retained_containers(array)?,
            owners: 1,
        });
    }
    match array.data_type() {
        DataType::List(_) => view_claims(array.as_list::<i32>().values().as_ref()),
        DataType::LargeList(_) => view_claims(array.as_list::<i64>().values().as_ref()),
        DataType::FixedSizeList(_, _) => view_claims(array.as_fixed_size_list().values().as_ref()),
        DataType::Struct(_) => {
            array
                .as_struct()
                .columns()
                .iter()
                .try_fold(ViewClaims::default(), |total, child| {
                    let child = view_claims(child.as_ref())?;
                    Ok(ViewClaims {
                        slots: checked_add(total.slots, child.slots)?,
                        containers: checked_add(total.containers, child.containers)?,
                        owners: checked_add(total.owners, child.owners)?,
                    })
                })
        }
        _ => Ok(ViewClaims::default()),
    }
}

impl std::fmt::Debug for HopReadCharge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HopReadCharge")
            .field("held", &self.held)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct BackingLease {
    _reservation: datafusion::execution::memory_pool::MemoryReservation,
    size: usize,
}
impl arrow_buffer::MemoryReservation for BackingLease {
    fn size(&self) -> usize {
        self.size
    }
    fn resize(&mut self, size: usize) {
        // This boundary publishes immutable arrays. The source allocation's
        // owner remains conservative on shrink; any subsequent mutable
        // consumer is responsible for admission of its own new allocations.
        self.size = size;
    }
}

#[derive(Debug)]
struct PrepaidOutput(datafusion::execution::memory_pool::MemoryReservation);
impl arrow_buffer::MemoryPool for PrepaidOutput {
    fn reserve(&self, size: usize) -> Box<dyn arrow_buffer::MemoryReservation> {
        // Complete occurrence-counted capacities and every Box token were
        // checked and admitted before this infallible hook. Splitting moves
        // prepaid authority without releasing/re-admitting any source byte.
        // A duplicate claim replaces its prior owner only after this split.
        Box::new(BackingLease {
            _reservation: self.0.split(size + std::mem::size_of::<BackingLease>()),
            size,
        })
    }
    fn available(&self) -> isize {
        0
    }
    fn used(&self) -> usize {
        self.0.size()
    }
    fn capacity(&self) -> usize {
        self.0.size()
    }
}

#[derive(Debug)]
struct ViewBackingLease {
    _reservation: datafusion::execution::memory_pool::MemoryReservation,
    _metadata: std::sync::Arc<datafusion::execution::memory_pool::MemoryReservation>,
    size: usize,
}
impl arrow_buffer::MemoryReservation for ViewBackingLease {
    fn size(&self) -> usize {
        self.size
    }
    fn resize(&mut self, size: usize) {
        self.size = size;
    }
}
#[derive(Debug)]
struct ViewPool<'a> {
    prepaid: &'a PrepaidOutput,
    metadata: std::sync::Arc<datafusion::execution::memory_pool::MemoryReservation>,
}
impl arrow_buffer::MemoryPool for ViewPool<'_> {
    fn reserve(&self, size: usize) -> Box<dyn arrow_buffer::MemoryReservation> {
        Box::new(ViewBackingLease {
            _reservation: self
                .prepaid
                .0
                .split(size + std::mem::size_of::<ViewBackingLease>()),
            _metadata: std::sync::Arc::clone(&self.metadata),
            size,
        })
    }
    fn available(&self) -> isize {
        0
    }
    fn used(&self) -> usize {
        self.prepaid.0.size()
    }
    fn capacity(&self) -> usize {
        self.prepaid.0.size()
    }
}
fn view_owner_bytes() -> usize {
    std::mem::size_of::<datafusion::execution::memory_pool::MemoryReservation>()
        + 2 * std::mem::size_of::<usize>()
}
fn claim_new_buffer(
    buffer: &arrow::buffer::Buffer,
    prepaid: &PrepaidOutput,
    sources: &[graphforge_storage::read_memory::PrepaidBatch],
) {
    if !sources.iter().any(|source| source.funds_buffer(buffer)) {
        buffer.claim(prepaid);
    }
}

/// Concrete borrowed traversal preserves each view's variable buffer-list
/// owner independently of other columns, without ArrayData reconstruction.
fn claim_output(
    array: &dyn arrow::array::Array,
    prepaid: &PrepaidOutput,
    sources: &[graphforge_storage::read_memory::PrepaidBatch],
) {
    use arrow::array::AsArray;
    use arrow::datatypes::DataType;
    if view_buffers(array).is_some() {
        // These exact sums were checked by view_claims before extracting the
        // escrow; the hook performs no query-pool growth or fallible reserve.
        let bytes =
            retained_containers(array).expect("prechecked view metadata") + view_owner_bytes();
        let metadata = std::sync::Arc::new(prepaid.0.split(bytes));
        let pool = ViewPool { prepaid, metadata };
        graphforge_storage::read_memory::visit_buffers(array, &mut |buffer| {
            if !sources.iter().any(|source| source.funds_buffer(buffer)) {
                buffer.claim(&pool);
            }
            Ok(())
        })
        .expect("prechecked view buffers");
        return;
    }
    match array.data_type() {
        DataType::List(_) => {
            claim_new_buffer(
                array.as_list::<i32>().offsets().inner().inner(),
                prepaid,
                sources,
            );
            claim_output(array.as_list::<i32>().values().as_ref(), prepaid, sources);
        }
        DataType::LargeList(_) => {
            claim_new_buffer(
                array.as_list::<i64>().offsets().inner().inner(),
                prepaid,
                sources,
            );
            claim_output(array.as_list::<i64>().values().as_ref(), prepaid, sources);
        }
        DataType::FixedSizeList(_, _) => {
            claim_output(
                array.as_fixed_size_list().values().as_ref(),
                prepaid,
                sources,
            );
        }
        DataType::Struct(_) => {
            for child in array.as_struct().columns() {
                claim_output(child.as_ref(), prepaid, sources);
            }
        }
        _ => {
            graphforge_storage::read_memory::visit_buffers(array, &mut |buffer| {
                if !sources.iter().any(|source| source.funds_buffer(buffer)) {
                    buffer.claim(prepaid);
                }
                Ok(())
            })
            .expect("prechecked output buffers");
            return;
        }
    }
    if let Some(nulls) = array.nulls() {
        claim_new_buffer(nulls.buffer(), prepaid, sources);
    }
}

fn source_claims(
    array: &dyn arrow::array::Array,
    sources: &[graphforge_storage::read_memory::PrepaidBatch],
) -> Result<(usize, usize, usize), GfError> {
    // Preserve the operator's existing named shape refusal before consulting
    // storage provenance, from both construction validation and publication.
    claim_slots(array)?;
    let mut bytes = 0usize;
    let mut slots = 0usize;
    graphforge_storage::read_memory::visit_buffers(array, &mut |buffer| {
        if sources.iter().any(|source| source.funds_buffer(buffer)) {
            bytes = bytes.checked_add(buffer.capacity()).ok_or_else(|| {
                DataFusionError::ResourcesExhausted("hop shared backing size overflow".into())
            })?;
            slots = slots.checked_add(1).ok_or_else(|| {
                DataFusionError::ResourcesExhausted("hop shared claim count overflow".into())
            })?;
        }
        Ok(())
    })
    .map_err(GfError::from_execution_error)?;
    Ok((bytes, slots, shared_view_slots(array, sources)?))
}

fn shared_view_slots(
    array: &dyn arrow::array::Array,
    sources: &[graphforge_storage::read_memory::PrepaidBatch],
) -> Result<usize, GfError> {
    use arrow::array::AsArray;
    use arrow::datatypes::DataType;
    if view_buffers(array).is_some() {
        return source_claims_flat_view(array, sources);
    }
    match array.data_type() {
        DataType::List(_) => shared_view_slots(array.as_list::<i32>().values().as_ref(), sources),
        DataType::LargeList(_) => {
            shared_view_slots(array.as_list::<i64>().values().as_ref(), sources)
        }
        DataType::FixedSizeList(_, _) => {
            shared_view_slots(array.as_fixed_size_list().values().as_ref(), sources)
        }
        DataType::Struct(_) => array.as_struct().columns().iter().try_fold(0, |n, child| {
            checked_add(n, shared_view_slots(child.as_ref(), sources)?)
        }),
        _ => Ok(0),
    }
}
fn source_claims_flat_view(
    array: &dyn arrow::array::Array,
    sources: &[graphforge_storage::read_memory::PrepaidBatch],
) -> Result<usize, GfError> {
    let mut slots = 0;
    graphforge_storage::read_memory::visit_buffers(array, &mut |buffer| {
        slots += usize::from(sources.iter().any(|source| source.funds_buffer(buffer)));
        Ok(())
    })
    .map_err(GfError::from_execution_error)?;
    Ok(slots)
}

pub(super) fn lease_output(batch: RecordBatch, charge: HopReadCharge) -> Result<HopChunk, GfError> {
    // Declare the guard before its data so every fallible admission path drops
    // the batch first, independently of function-parameter drop ordering.
    let mut charge = charge;
    let sources = std::mem::take(&mut charge.prepaid_sources);
    let batch = batch;
    let mut shared_bytes = 0;
    let mut shared_slots = 0;
    let mut shared_view_slots = 0;
    for array in batch.columns() {
        let (bytes, slots, views) = source_claims(array.as_ref(), &sources)?;
        shared_bytes = checked_add(shared_bytes, bytes)?;
        shared_slots = checked_add(shared_slots, slots)?;
        shared_view_slots = checked_add(shared_view_slots, views)?;
    }
    let slots = batch
        .columns()
        .iter()
        .try_fold(0, |n, array| checked_add(n, claim_slots(array.as_ref())?))?;
    let views = batch
        .columns()
        .iter()
        .try_fold(ViewClaims::default(), |total, array| {
            let array = view_claims(array.as_ref())?;
            Ok::<_, GfError>(ViewClaims {
                slots: checked_add(total.slots, array.slots)?,
                containers: checked_add(total.containers, array.containers)?,
                owners: checked_add(total.owners, array.owners)?,
            })
        })?;
    let tokens = checked_add(
        checked_mul(
            slots - views.slots - (shared_slots - shared_view_slots),
            std::mem::size_of::<BackingLease>(),
        )?,
        checked_mul(
            views.slots - shared_view_slots,
            std::mem::size_of::<ViewBackingLease>(),
        )?,
    )?;
    let view_owners = checked_mul(views.owners, view_owner_bytes())?;
    let metadata = checked_add(
        tokens,
        checked_add(
            view_owners,
            checked_add(
                std::mem::size_of::<HopReadCharge>(),
                2 * std::mem::size_of::<usize>(),
            )?,
        )?,
    )?;
    // The final exact-sized column Vec was admitted before assembly and is
    // also omitted by RecordBatch's sum-of-arrays memory getter.
    let columns = checked_mul(
        batch.num_columns(),
        std::mem::size_of::<arrow::array::ArrayRef>(),
    )?;
    let containers = batch.columns().iter().try_fold(columns, |bytes, array| {
        checked_add(bytes, retained_containers(array.as_ref())?)
    })?;
    let retained = checked_add(
        checked_add(batch.get_array_memory_size() - shared_bytes, containers)?,
        metadata,
    )?;
    let buffers = batch.columns().iter().try_fold(0, |bytes, array| {
        checked_add(bytes, array.get_buffer_memory_size())
    })?;
    let immutable = checked_add(
        buffers - shared_bytes,
        checked_add(tokens, checked_add(views.containers, view_owners)?)?,
    )?;
    // Source maps and read/concat scratch have dropped. Constructor headers
    // and child/column Vecs remain admitted through producer handoff. Existing
    // Arrow output accounting thereafter owns generic wrappers/schema: Bytes
    // cannot observe their drops. Only real backing capacity plus its own Box
    // escapes here, never a whole chunk or projected-away sibling authority.
    let source_vector = checked_mul(
        sources.capacity(),
        std::mem::size_of::<graphforge_storage::read_memory::PrepaidBatch>(),
    )?;
    let schema_vector = checked_mul(
        charge.schema_owners.capacity(),
        std::mem::size_of::<graphforge_storage::read_memory::ReadSchemaOwner>(),
    )?;
    charge.replace(
        charge.held,
        checked_add(retained, checked_add(source_vector, schema_vector)?)?,
    )?;
    let prepaid = PrepaidOutput(charge.extract(immutable)?);
    for array in batch.columns() {
        claim_output(array.as_ref(), &prepaid, &sources);
    }
    for source in sources {
        // Array DataTypes can also clone source-owned timezone Arcs when the
        // output schema's FieldRefs differ. Transfer the small existing schema
        // credit through handoff; drop source arrays and unrelated payloads.
        charge.schema_owners.push(source.into_schema_owner());
    }
    charge.release(source_vector);
    let charge = std::sync::Arc::new(charge);
    drop(prepaid); // unused duplicate-occurrence credit is not live storage
    Ok(HopChunk { batch, charge })
}

/// Execute the adjacency-backed single-hop expansion: for every input row's
/// source node, emit one output row per adjacency entry, assembling input,
/// edge-topology, edge-property (nullable), and destination-node columns in
/// the [`graphforge_plan::ExpandNode`] schema order — the same rows the join chain
/// produces.
#[allow(clippy::too_many_lines)]
pub(super) fn expand_single_hop_chunk(
    cfg: &SingleHopConfig,
    input: &RecordBatch,
    position: &mut super::SingleHopPosition,
    max_output: usize,
) -> Result<HopChunk, GfError> {
    let mut charge = HopReadCharge::new(&cfg.read_reservation);
    let batch =
        super::expand_single_hop_chunk_inner(cfg, input, position, max_output, &mut charge)?;
    let shared = batch.columns().iter().try_fold(0, |bytes, array| {
        checked_add(
            bytes,
            source_claims(array.as_ref(), &charge.prepaid_sources)?.0,
        )
    })?;
    let actual = batch
        .get_array_memory_size()
        .checked_sub(shared)
        .ok_or_else(size_refused)?;
    if actual > charge.held_bytes_internal() {
        return Err(GfError::Execution(
            "Expand output exceeded its admitted construction envelope".into(),
        ));
    }
    lease_output(batch, charge)
}
