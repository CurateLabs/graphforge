//! Borrowed semantic validation and native allocation-request accounting for
//! Arrow IPC schemas. This component models `arrow-ipc` 58.4's
//! `convert::fb_to_schema` path; it does not construct an Arrow `Schema`.

use std::alloc::Layout;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use arrow::ipc::{
    DateUnit, Endianness, IntervalUnit, Precision, Schema, TimeUnit, Type, UnionMode,
};
use graphforge_core::GfError;

use super::inventory_budget::InventoryBudget;
use super::parquet_alloc::{self, Request};
use super::{cancelled, limit, storage};
use crate::CancellationToken;

const MAX_SCHEMA_DEPTH: usize = 64;
const HASH_GROUP_WIDTH: usize = 16;

/// Counted native allocation requests for one verified Arrow IPC schema.
///
/// The peak is a conservative sum of retained requests and source-derived
/// temporary vector requests; it is not an allocator-usable-size or RSS bound.
/// Metadata duplicate occurrences are charged as if all copied strings
/// remained live, and their count is an upper bound on map entries.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct IpcSchemaEnvelope {
    pub(super) field_occurrences: u64,
    pub(super) metadata_occurrences: u64,
    pub(super) field_name_copy_bytes: u64,
    pub(super) metadata_copy_bytes: u64,
    pub(super) timezone_copy_bytes: u64,
    pub(super) retained_request_bytes: u64,
    pub(super) peak_request_bytes: u64,
}

#[derive(Default)]
struct Requests {
    envelope: IpcSchemaEnvelope,
}

fn malformed() -> GfError {
    storage("Arrow IPC schema is not supported by the native schema converter")
}

fn overflow() -> GfError {
    limit("Arrow IPC schema allocation request exceeds the admitted workspace")
}

fn add(target: &mut u64, amount: u64) -> Result<(), GfError> {
    *target = target.checked_add(amount).ok_or_else(overflow)?;
    Ok(())
}

fn count(value: usize) -> Result<u64, GfError> {
    u64::try_from(value).map_err(|_| overflow())
}

fn layout_bytes(layout: Layout) -> Result<u64, GfError> {
    if layout.size() > isize::MAX as usize {
        return Err(overflow());
    }
    u64::try_from(layout.size()).map_err(|_| overflow())
}

fn add_request(requests: &mut Requests, request: Request) -> Result<(), GfError> {
    add(
        &mut requests.envelope.retained_request_bytes,
        request.retained_bytes,
    )?;
    add(
        &mut requests.envelope.peak_request_bytes,
        request.peak_bytes,
    )
}

fn add_exact_layout(requests: &mut Requests, bytes: u64) -> Result<(), GfError> {
    add_request(
        requests,
        Request {
            retained_bytes: bytes,
            peak_bytes: bytes,
        },
    )
}

fn add_temporary_request(requests: &mut Requests, request: Request) -> Result<(), GfError> {
    add(
        &mut requests.envelope.peak_request_bytes,
        request.peak_bytes,
    )
}

fn vector_growth<T>(requests: &mut Requests, elements: usize) -> Result<(), GfError> {
    add_temporary_request(
        requests,
        parquet_alloc::vector_envelope(0, elements, size_of::<T>())?,
    )
}

fn vector_exact<T>(requests: &mut Requests, elements: usize) -> Result<(), GfError> {
    add_temporary_request(
        requests,
        parquet_alloc::vector(0, elements, size_of::<T>(), true)?,
    )
}

fn arc_header() -> Result<Layout, GfError> {
    let (layout, _) = Layout::new::<AtomicUsize>()
        .extend(Layout::new::<AtomicUsize>())
        .map_err(|_| overflow())?;
    Ok(layout)
}

fn arc_sized<T>(requests: &mut Requests) -> Result<(), GfError> {
    let (layout, _) = arc_header()?
        .extend(Layout::new::<T>())
        .map_err(|_| overflow())?;
    add_exact_layout(requests, layout_bytes(layout.pad_to_align())?)
}

fn arc_slice<T>(requests: &mut Requests, length: usize) -> Result<(), GfError> {
    let elements = Layout::array::<T>(length).map_err(|_| overflow())?;
    let (layout, _) = arc_header()?.extend(elements).map_err(|_| overflow())?;
    add_exact_layout(requests, layout_bytes(layout.pad_to_align())?)
}

fn owned_string(requests: &mut Requests, value: &str) -> Result<(), GfError> {
    if !value.is_empty() {
        add_request(
            requests,
            parquet_alloc::vector(0, value.len(), size_of::<u8>(), true)?,
        )?;
    }
    Ok(())
}

macro_rules! metadata_occurrences {
    ($requests:expr, $metadata:expr, $cancellation:expr) => {{
        let mut occurrences = 0_u64;
        let mut copied = 0_u64;
        if let Some(metadata) = $metadata {
            for index in 0..metadata.len() {
                check_cancelled($cancellation)?;
                let key_value = metadata.get(index);
                if let (Some(key), Some(value)) = (key_value.key(), key_value.value()) {
                    occurrences = occurrences.checked_add(1).ok_or_else(overflow)?;
                    let key_bytes = count(key.len())?;
                    let value_bytes = count(value.len())?;
                    copied = copied
                        .checked_add(key_bytes)
                        .and_then(|total| total.checked_add(value_bytes))
                        .ok_or_else(overflow)?;
                    owned_string($requests, key)?;
                    owned_string($requests, value)?;
                }
            }
        }
        add_hash_map::<String, String>(
            $requests,
            usize::try_from(occurrences).map_err(|_| overflow())?,
        )?;
        Ok::<(u64, u64), GfError>((occurrences, copied))
    }};
}

fn arc_str(requests: &mut Requests, value: &str) -> Result<(), GfError> {
    let bytes = Layout::array::<u8>(value.len()).map_err(|_| overflow())?;
    let (layout, _) = arc_header()?.extend(bytes).map_err(|_| overflow())?;
    add_exact_layout(requests, layout_bytes(layout.pad_to_align())?)
}

/// Validate and admit the heap requests made by Arrow's native IPC schema
/// conversion. The supplied FlatBuffer view must already have passed Arrow's
/// verifier. No owned schema or converted Arrow data type is constructed here.
pub(super) fn preflight(
    schema: Schema<'_>,
    capacity: u64,
    cancellation: Option<&CancellationToken>,
) -> Result<IpcSchemaEnvelope, GfError> {
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err(cancelled());
    }
    let fields = schema.fields().ok_or_else(malformed)?;
    let mut requests = Requests::default();
    let field_count = fields.len();
    vector_growth::<arrow::datatypes::Field>(&mut requests, field_count)?;
    for index in 0..field_count {
        check_cancelled(cancellation)?;
        let field = fields.get(index);
        walk_field(
            &mut requests,
            field,
            0,
            true,
            schema.endianness(),
            cancellation,
        )?;
    }

    let (metadata_occurrences, metadata_bytes) =
        metadata_occurrences!(&mut requests, schema.custom_metadata(), cancellation)?;
    requests.envelope.metadata_occurrences = metadata_occurrences;
    requests.envelope.metadata_copy_bytes = metadata_bytes;

    // fb_to_schema moves Vec<Field> through a temporary Vec<FieldRef> into
    // Fields' Arc slice. The Vec<Field> push-growth was counted above.
    vector_exact::<Arc<arrow::datatypes::Field>>(&mut requests, field_count)?;
    arc_slice::<Arc<arrow::datatypes::Field>>(&mut requests, field_count)?;

    let mut budget = InventoryBudget::new(capacity);
    budget.admit(
        requests.envelope.peak_request_bytes,
        "the converted Arrow IPC schema",
    )?;
    Ok(requests.envelope)
}

fn check_cancelled(cancellation: Option<&CancellationToken>) -> Result<(), GfError> {
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        Err(cancelled())
    } else {
        Ok(())
    }
}

fn walk_field<'a>(
    requests: &mut Requests,
    field: arrow::ipc::Field<'a>,
    depth: usize,
    top_level: bool,
    endianness: Endianness,
    cancellation: Option<&CancellationToken>,
) -> Result<(), GfError> {
    check_cancelled(cancellation)?;
    if depth > MAX_SCHEMA_DEPTH {
        return Err(malformed());
    }
    add(&mut requests.envelope.field_occurrences, 1)?;
    let name = field.name().unwrap_or_default();
    let name_bytes = count(name.len())?;
    add(&mut requests.envelope.field_name_copy_bytes, name_bytes)?;
    owned_string(requests, name)?;
    // Every converted Field occurrence is wrapped once by its owner: a
    // top-level Fields value, list/map/run-end child Arc, struct Fields, or
    // union pair.
    arc_sized::<arrow::datatypes::Field>(requests)?;

    let (metadata_occurrences, metadata_bytes) =
        metadata_occurrences!(requests, field.custom_metadata(), cancellation)?;
    add(
        &mut requests.envelope.metadata_occurrences,
        metadata_occurrences,
    )?;
    add(&mut requests.envelope.metadata_copy_bytes, metadata_bytes)?;

    if let Some(dictionary) = field.dictionary() {
        let index = dictionary.indexType().ok_or_else(malformed)?;
        if !matches!((index.bitWidth(), index.is_signed()), (8 | 16 | 32 | 64, _)) {
            return Err(malformed());
        }
        add_exact_layout(
            requests,
            layout_bytes(Layout::new::<arrow::datatypes::DataType>())?,
        )?;
        add_exact_layout(
            requests,
            layout_bytes(Layout::new::<arrow::datatypes::DataType>())?,
        )?;
    }

    match field.type_type() {
        Type::Null | Type::Bool => {}
        Type::Int => {
            let int = field.type_as_int().ok_or_else(malformed)?;
            if !matches!(int.bitWidth(), 8 | 16 | 32 | 64) {
                return Err(malformed());
            }
        }
        Type::FloatingPoint => {
            let float = field.type_as_floating_point().ok_or_else(malformed)?;
            if !matches!(
                float.precision(),
                Precision::HALF | Precision::SINGLE | Precision::DOUBLE
            ) {
                return Err(malformed());
            }
        }
        Type::Binary
        | Type::BinaryView
        | Type::LargeBinary
        | Type::Utf8
        | Type::Utf8View
        | Type::LargeUtf8 => {}
        Type::FixedSizeBinary => {
            field.type_as_fixed_size_binary().ok_or_else(malformed)?;
        }
        Type::Date => {
            let date = field.type_as_date().ok_or_else(malformed)?;
            if !matches!(date.unit(), DateUnit::DAY | DateUnit::MILLISECOND) {
                return Err(malformed());
            }
        }
        Type::Time => {
            let time = field.type_as_time().ok_or_else(malformed)?;
            if !matches!(
                (time.bitWidth(), time.unit()),
                (32, TimeUnit::SECOND | TimeUnit::MILLISECOND)
                    | (64, TimeUnit::MICROSECOND | TimeUnit::NANOSECOND)
            ) {
                return Err(malformed());
            }
        }
        Type::Timestamp => {
            let timestamp = field.type_as_timestamp().ok_or_else(malformed)?;
            if !matches!(
                timestamp.unit(),
                TimeUnit::SECOND
                    | TimeUnit::MILLISECOND
                    | TimeUnit::MICROSECOND
                    | TimeUnit::NANOSECOND
            ) {
                return Err(malformed());
            }
            if let Some(timezone) = timestamp.timezone() {
                let bytes = count(timezone.len())?;
                add(&mut requests.envelope.timezone_copy_bytes, bytes)?;
                arc_str(requests, timezone)?;
            }
        }
        Type::Interval => {
            let interval = field.type_as_interval().ok_or_else(malformed)?;
            if !matches!(
                interval.unit(),
                IntervalUnit::YEAR_MONTH | IntervalUnit::DAY_TIME | IntervalUnit::MONTH_DAY_NANO
            ) {
                return Err(malformed());
            }
        }
        Type::Duration => {
            let duration = field.type_as_duration().ok_or_else(malformed)?;
            if !matches!(
                duration.unit(),
                TimeUnit::SECOND
                    | TimeUnit::MILLISECOND
                    | TimeUnit::MICROSECOND
                    | TimeUnit::NANOSECOND
            ) {
                return Err(malformed());
            }
        }
        Type::List
        | Type::LargeList
        | Type::ListView
        | Type::LargeListView
        | Type::FixedSizeList
        | Type::Map => {
            let children = field.children().ok_or_else(malformed)?;
            if children.len() != 1 {
                return Err(malformed());
            }
            if field.type_type() == Type::FixedSizeList {
                field.type_as_fixed_size_list().ok_or_else(malformed)?;
            } else if field.type_type() == Type::Map {
                field.type_as_map().ok_or_else(malformed)?;
            }
            // Native wraps the sole converted child directly in Arc<Field>.
            walk_field(
                requests,
                children.get(0),
                depth + 1,
                false,
                endianness,
                cancellation,
            )?;
        }
        Type::Struct_ => {
            let children = field.children();
            let child_count = children.map_or(0, |children| children.len());
            // Arrow first collects converted children into Vec<Field>, then
            // Fields::from_iter wraps each value in Arc and materializes the
            // shared Arc slice.
            vector_exact::<arrow::datatypes::Field>(requests, child_count)?;
            vector_exact::<Arc<arrow::datatypes::Field>>(requests, child_count)?;
            arc_slice::<Arc<arrow::datatypes::Field>>(requests, child_count)?;
            if let Some(children) = children {
                for index in 0..children.len() {
                    check_cancelled(cancellation)?;
                    walk_field(
                        requests,
                        children.get(index),
                        depth + 1,
                        false,
                        endianness,
                        cancellation,
                    )?;
                }
            }
        }
        Type::RunEndEncoded => {
            let children = field.children().ok_or_else(malformed)?;
            if children.len() != 2 {
                return Err(malformed());
            }
            for index in 0..2 {
                check_cancelled(cancellation)?;
                walk_field(
                    requests,
                    children.get(index),
                    depth + 1,
                    false,
                    endianness,
                    cancellation,
                )?;
            }
        }
        Type::Union => {
            let union = field.type_as_union().ok_or_else(malformed)?;
            if !matches!(union.mode(), UnionMode::Dense | UnionMode::Sparse) {
                return Err(malformed());
            }
            let children = field.children();
            let child_count = children.map_or(0, |children| children.len());
            let type_ids = union.typeIds();
            if let Some(ids) = type_ids {
                if ids.len() != child_count {
                    return Err(malformed());
                }
                let mut seen = 0_u128;
                for id_index in 0..ids.len() {
                    check_cancelled(cancellation)?;
                    let id = ids.get(id_index) as i8;
                    if id < 0 {
                        return Err(malformed());
                    }
                    let bit = 1_u128.checked_shl(id as u32).ok_or_else(malformed)?;
                    if seen & bit != 0 {
                        return Err(malformed());
                    }
                    seen |= bit;
                }
            } else if child_count > 128 {
                return Err(malformed());
            }
            vector_growth::<arrow::datatypes::Field>(requests, child_count)?;
            // `try_new` grows this tuple vector with push; `from_fields`
            // collects it from an exact-size iterator. The growth envelope
            // safely covers both source paths.
            vector_growth::<(i8, Arc<arrow::datatypes::Field>)>(requests, child_count)?;
            arc_slice::<(i8, Arc<arrow::datatypes::Field>)>(requests, child_count)?;
            if let Some(children) = children {
                for index in 0..children.len() {
                    check_cancelled(cancellation)?;
                    walk_field(
                        requests,
                        children.get(index),
                        depth + 1,
                        false,
                        endianness,
                        cancellation,
                    )?;
                }
            }
        }
        Type::Decimal => {
            if top_level && endianness == Endianness::Big {
                return Err(malformed());
            }
            let decimal = field.type_as_decimal().ok_or_else(malformed)?;
            u8::try_from(decimal.precision()).map_err(|_| malformed())?;
            i8::try_from(decimal.scale()).map_err(|_| malformed())?;
            if !matches!(decimal.bitWidth(), 32 | 64 | 128 | 256) {
                return Err(malformed());
            }
        }
        _ => return Err(malformed()),
    }
    Ok(())
}

fn add_hash_map<K, V>(requests: &mut Requests, entries: usize) -> Result<(), GfError> {
    if entries == 0 {
        return Ok(());
    }
    let (retained, peak) = hash_map_layouts::<K, V>(entries)?;
    add_request(
        requests,
        Request {
            retained_bytes: retained,
            peak_bytes: peak,
        },
    )
}

fn hash_map_layouts<K, V>(entries: usize) -> Result<(u64, u64), GfError> {
    let mut buckets = capacity_to_buckets::<K, V>(1)?;
    let first = table_layout::<K, V>(buckets)?;
    let mut retained = u64::try_from(first).map_err(|_| overflow())?;
    let mut peak = retained;
    let mut capacity = table_capacity(buckets)?;
    while entries > capacity {
        let needed = capacity.checked_add(1).ok_or_else(overflow)?;
        let next_buckets = capacity_to_buckets::<K, V>(needed)?;
        let next = table_layout::<K, V>(next_buckets)?;
        peak = peak.max(
            u64::try_from(table_layout::<K, V>(buckets)?)
                .map_err(|_| overflow())?
                .checked_add(u64::try_from(next).map_err(|_| overflow())?)
                .ok_or_else(overflow)?,
        );
        retained = u64::try_from(next).map_err(|_| overflow())?;
        buckets = next_buckets;
        capacity = table_capacity(buckets)?;
    }
    Ok((retained, peak))
}

fn table_capacity(buckets: usize) -> Result<usize, GfError> {
    if buckets < 16 {
        buckets.checked_sub(1).ok_or_else(overflow)
    } else {
        (buckets / 8).checked_mul(7).ok_or_else(overflow)
    }
}

fn capacity_to_buckets<K, V>(capacity: usize) -> Result<usize, GfError> {
    if capacity == 0 {
        return Err(overflow());
    }
    if capacity < 15 {
        let requested = capacity.max(3);
        return Ok(if requested < 4 {
            4
        } else if requested < 8 {
            8
        } else {
            16
        });
    }
    let adjusted = capacity.checked_mul(8).ok_or_else(overflow)? / 7;
    adjusted.checked_next_power_of_two().ok_or_else(overflow)
}

fn table_layout<K, V>(buckets: usize) -> Result<usize, GfError> {
    let element = Layout::new::<(K, V)>();
    let alignment = element.align().max(HASH_GROUP_WIDTH);
    let values = element.size().checked_mul(buckets).ok_or_else(overflow)?;
    let offset = values.checked_add(alignment - 1).ok_or_else(overflow)? & !(alignment - 1);
    let size = offset
        .checked_add(buckets)
        .and_then(|size| size.checked_add(HASH_GROUP_WIDTH))
        .ok_or_else(overflow)?;
    if size > isize::MAX as usize - (alignment - 1) {
        return Err(overflow());
    }
    Ok(size)
}

#[cfg(test)]
#[path = "ipc_schema_admission/tests.rs"]
mod tests;
