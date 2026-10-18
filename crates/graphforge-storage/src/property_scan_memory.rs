//! Execution-owned admission for property readers and their output handoffs.

use std::mem::size_of;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Schema};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use graphforge_core::{ApiErrorCode, GfError, SpatialCoordinates, SpatialCrs};
use graphforge_ir::IrLiteral;

pub(crate) fn refused(message: impl std::fmt::Display) -> GfError {
    GfError::Api {
        code: ApiErrorCode::ResourceLimit,
        message: format!("PropertyScan: {message}"),
    }
}

pub(crate) fn add(a: usize, b: usize) -> Result<usize, GfError> {
    a.checked_add(b)
        .ok_or_else(|| refused("allocation size overflow"))
}

pub(crate) fn mul(a: usize, b: usize) -> Result<usize, GfError> {
    a.checked_mul(b)
        .ok_or_else(|| refused("allocation size overflow"))
}

#[derive(Debug)]
pub(crate) struct Budget {
    reservation: Mutex<MemoryReservation>,
    cancelled: AtomicBool,
}

impl Budget {
    pub(crate) fn new(pool: &Arc<dyn MemoryPool>) -> Arc<Self> {
        Arc::new(Self {
            reservation: Mutex::new(MemoryConsumer::new("PropertyScan").register(pool)),
            cancelled: AtomicBool::new(false),
        })
    }

    pub(crate) fn check(&self) -> Result<(), GfError> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(GfError::Api {
                code: ApiErrorCode::Cancelled,
                message: "PropertyScan consumer closed".into(),
            });
        }
        Ok(())
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub(crate) fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Guard, GfError> {
        self.check()?;
        self.reservation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .try_grow(bytes)
            .map_err(refused)?;
        Ok(Guard {
            budget: Arc::clone(self),
            bytes,
        })
    }
}

#[derive(Debug)]
pub(crate) struct Guard {
    budget: Arc<Budget>,
    bytes: usize,
}

impl Guard {
    /// Admit additional capacity before its owner allocates it. A refusal
    /// leaves this guard and the pool's existing reservation unchanged.
    pub(crate) fn grow(&mut self, additional: usize) -> Result<(), GfError> {
        self.budget.check()?;
        let bytes = add(self.bytes, additional)?;
        self.budget
            .reservation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .try_grow(additional)
            .map_err(refused)?;
        self.bytes = bytes;
        Ok(())
    }

    fn release(&mut self, bytes: usize) -> Result<(), GfError> {
        let retained = self
            .bytes
            .checked_sub(bytes)
            .ok_or_else(|| refused("release exceeds admitted allocation"))?;
        self.shrink_to(retained)
    }

    /// Divide already-admitted authority without changing the shared pool sum.
    pub(crate) fn split_off(&mut self, bytes: usize) -> Result<Self, GfError> {
        if bytes > self.bytes {
            return Err(refused("split exceeds admitted allocation"));
        }
        self.bytes -= bytes;
        Ok(Self {
            budget: Arc::clone(&self.budget),
            bytes,
        })
    }

    pub(crate) fn shrink_to(&mut self, bytes: usize) -> Result<(), GfError> {
        if bytes > self.bytes {
            return Err(refused("allocation exceeded its admitted envelope"));
        }
        self.budget
            .reservation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .shrink(self.bytes - bytes);
        self.bytes = bytes;
        Ok(())
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.budget
            .reservation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .shrink(self.bytes);
    }
}

/// Rows drop before their independently owned map/payload admission. The reader
/// separately retains its complete vector capacity until its iterator drops.
pub(crate) struct Snapshot {
    pub(crate) row: crate::PropertySnapshotRow,
    pub(crate) memory: Option<Guard>,
}

/// The destination map must drop before either of these allocation owners.
pub(crate) struct MapMemory {
    _source: Option<Guard>,
    _table: Guard,
}

type AdmittedMap = (
    [u8; 16],
    std::collections::HashMap<String, IrLiteral>,
    MapMemory,
);

impl Snapshot {
    /// Called only after insertion into a separately prepaid Snapshot vector.
    pub(crate) fn release_carrier(&mut self) -> Result<(), GfError> {
        if let Some(memory) = self.memory.as_mut() {
            memory.release(size_of::<Self>())?;
        }
        Ok(())
    }

    pub(crate) fn into_admitted_map(
        mut self,
        budget: &Arc<Budget>,
    ) -> Result<AdmittedMap, GfError> {
        // Keep self intact through admission: on refusal its values drop before
        // their guard. collect consumes an exact-sized BTreeMap iterator and
        // frees its nodes, while moving the existing keys and literal heaps.
        let table = budget.reserve(hash_map_bytes(self.row.values.len())?)?;
        let nodes = decoded_map_bytes(self.row.values.len())?;
        let values = std::mem::take(&mut self.row.values).into_iter().collect();
        if let Some(memory) = self.memory.as_mut() {
            memory.release(nodes)?;
        }
        Ok((
            self.row.uuid,
            values,
            MapMemory {
                _source: self.memory.take(),
                _table: table,
            },
        ))
    }
}

impl std::ops::Deref for Snapshot {
    type Target = crate::PropertySnapshotRow;
    fn deref(&self) -> &Self::Target {
        &self.row
    }
}

// Rust's BTreeMap nodes hold eleven key/value slots and up to twelve edges.
// Sixteen slots per entry cover a singleton node and its edges/header.
const MAP_ENTRY: usize = 16 * (size_of::<(String, IrLiteral)>() + 4 * size_of::<usize>());
// Rust 1.96 alloc::collections::btree::node has eleven key/value slots per
// node. A fresh map with at most eleven inserts cannot split. The pair layout
// bounds the separate key/value arrays; 64 bytes cover the parent/length/index
// header and alignment, and twelve pointers also cover an internal-node tail.
const MAP_ROOT: usize = 11 * size_of::<(String, IrLiteral)>() + 64 + 12 * size_of::<usize>();
const ARRAY_NODE: usize = 4 * size_of::<arrow::array::ArrayData>() + 8 * 64;

fn decoded_map_bytes(property_fields: usize) -> Result<usize, GfError> {
    match property_fields {
        0 => Ok(0),
        1..=11 => Ok(MAP_ROOT),
        _ => mul(property_fields, MAP_ENTRY),
    }
}

/// Destination table allocation, excluding moved key/literal heaps and the
/// inline HashMap header. Rust 1.96's std HashMap::from_iter starts empty and
/// hashbrown 0.16.1 Extend reserves the full lower size_hint; BTreeMap IntoIter
/// is exact-sized, so property conversion needs one table and no growth copy.
/// hashbrown's raw::capacity_to_buckets / TableLayout::calculate_layout_for use
/// four minimum buckets, a 7/8 load, aligned buckets and trailing control bytes.
pub(crate) fn hash_map_bytes(entries: usize) -> Result<usize, GfError> {
    if entries == 0 {
        return Ok(0);
    }
    let buckets = (add(mul(entries, 8)?, 6)? / 7)
        .max(4)
        .checked_next_power_of_two()
        .ok_or_else(|| refused("property map bucket count overflow"))?;
    // The pinned hashbrown's SIMD groups are at most sixteen bytes; its generic
    // groups are smaller. Round to the larger control or entry alignment.
    let alignment = 16_usize.max(std::mem::align_of::<(String, IrLiteral)>());
    let controls = add(
        mul(buckets, size_of::<(String, IrLiteral)>())?,
        alignment - 1,
    )? & !(alignment - 1);
    let bytes = add(controls, add(buckets, 16)?)?;
    if bytes > isize::MAX as usize - (alignment - 1) {
        return Err(refused("property map allocation exceeds addressable size"));
    }
    Ok(bytes)
}

fn array_literals(array: &dyn Array, field: &arrow::datatypes::Field) -> Result<usize, GfError> {
    array_literals_inner(array, field, false)
}

fn array_literals_inner(
    array: &dyn Array,
    field: &arrow::datatypes::Field,
    nested: bool,
) -> Result<usize, GfError> {
    let mut bytes = add(
        array.get_array_memory_size(),
        mul(array.len(), size_of::<IrLiteral>())?,
    )?;
    // Scalar decoding borrows Arrow values and creates only inline literals or
    // their owned payloads, already charged above. Ordinary top-level scalars
    // create no Arrow headers. Preserve every existing container/descendant
    // and spatial allowance for child slices, shape dispatch and metadata.
    let headers = match array.data_type() {
        DataType::List(_) | DataType::Struct(_) => array.len().max(1),
        _ if nested
            || field.metadata().contains_key("ARROW:extension:name")
            || field.metadata().contains_key("ARROW:extension:metadata") =>
        {
            1
        }
        _ => 0,
    };
    bytes = add(bytes, mul(headers, ARRAY_NODE)?)?;
    if let Some(metadata) = field.metadata().get("ARROW:extension:metadata") {
        // Spatial metadata is parsed by serde_json for each row. Each encoded
        // byte can introduce at most one value/map slot; singleton BTree nodes
        // and parser temporaries are covered beside the preserved metadata.
        bytes = add(bytes, mul(array.len(), mul(metadata.len(), MAP_ENTRY)?)?)?;
    }
    match array.data_type() {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8 // Existing heterogeneous scalar tag, not a new stored value codec.
        | DataType::Int64
        | DataType::Float64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Utf8
        | DataType::FixedSizeBinary(_)
        | DataType::Int32 => {}
        DataType::List(inner) => {
            let values = array.as_list::<i32>().values();
            bytes = add(bytes, array_literals_inner(values.as_ref(), inner, true)?)?;
        }
        DataType::Struct(fields) => {
            for (child, field) in array.as_struct().columns().iter().zip(fields) {
                bytes = add(bytes, array_literals_inner(child.as_ref(), field, true)?)?;
            }
        }
        other => {
            return Err(refused(format!(
                "cannot bound property decoder for {other}"
            )));
        }
    }
    Ok(bytes)
}

/// Bound the existing Arrow-to-literal decoder before it constructs row maps.
/// String/list payloads come from actual retained Arrow buffers, recursively;
/// keys are cloned per row. Up to eleven entries occupy one BTreeMap root;
/// larger maps retain the conservative singleton allowance per entry.
pub(crate) fn decoded_rows(batch: &RecordBatch, uuid_field: &str) -> Result<usize, GfError> {
    let mut bytes = mul(
        batch.num_rows(),
        add(
            size_of::<crate::PropertySnapshotRow>(),
            size_of::<u64>() + size_of::<Snapshot>(),
        )?,
    )?;
    let property_fields = batch
        .schema()
        .fields()
        .iter()
        .filter(|field| field.name() != uuid_field)
        .count();
    // Rust 1.96's BTreeMap inserts into a single eleven-slot leaf before
    // splitting. Count nullable/all-null columns conservatively and include the
    // tombstone removed after decoding. Keep the per-entry bound above that root.
    bytes = add(
        bytes,
        mul(batch.num_rows(), decoded_map_bytes(property_fields)?)?,
    )?;
    for (array, field) in batch.columns().iter().zip(batch.schema().fields()) {
        // The decoder copies this key into PropertySnapshotRow's inline UUID
        // and skips map insertion. Every other field name is cloned, including
        // the tombstone inserted before decode_snapshot_batch removes it.
        if field.name() != uuid_field {
            bytes = add(bytes, mul(batch.num_rows(), field.name().len())?)?;
        }
        bytes = add(bytes, array_literals(array.as_ref(), field)?)?;
    }
    Ok(bytes)
}

/// After the source Arrow batch drops, retain only owned decoded rows. Map
/// capacity still uses the pre-removal schema, including the tombstone: removing
/// its key need not free the allocated BTreeMap root or split nodes.
pub(crate) fn retained_decoded_rows(
    rows: &[crate::PropertySnapshotRow],
    row_capacity: usize,
    charge_capacity: usize,
    property_fields: usize,
) -> Result<usize, GfError> {
    let mut bytes = add(
        mul(row_capacity, size_of::<crate::PropertySnapshotRow>())?,
        mul(charge_capacity, size_of::<u64>())?,
    )?;
    for row in rows {
        bytes = add(bytes, retained_decoded_row(row, property_fields)?)?;
    }
    Ok(bytes)
}

/// A decoded row owns its map and literal payloads; none borrow the source
/// Arrow batch or the reader's row vector. Move this charge with the row while
/// leaving the complete source vector capacities in the reader's reservation.
pub(crate) fn retained_decoded_row(
    row: &crate::PropertySnapshotRow,
    property_fields: usize,
) -> Result<usize, GfError> {
    let mut bytes = add(size_of::<Snapshot>(), decoded_map_bytes(property_fields)?)?;
    for (key, value) in &row.values {
        bytes = add(bytes, add(key.capacity(), literal_heap(value)?)?)?;
    }
    Ok(bytes)
}

fn spatial_heap(value: &graphforge_core::SpatialValue) -> Result<usize, GfError> {
    fn line(points: &Vec<[f64; 2]>) -> Result<usize, GfError> {
        mul(points.capacity(), size_of::<[f64; 2]>())
    }
    fn polygon(rings: &Vec<Vec<[f64; 2]>>) -> Result<usize, GfError> {
        let mut bytes = mul(rings.capacity(), size_of::<Vec<[f64; 2]>>())?;
        for ring in rings {
            bytes = add(bytes, line(ring)?)?;
        }
        Ok(bytes)
    }
    let mut bytes = match &value.coordinates {
        SpatialCoordinates::Point(_) => 0,
        SpatialCoordinates::LineString(points) | SpatialCoordinates::MultiPoint(points) => {
            line(points)?
        }
        SpatialCoordinates::Polygon(rings) | SpatialCoordinates::MultiLineString(rings) => {
            polygon(rings)?
        }
        SpatialCoordinates::MultiPolygon(polygons) => {
            let mut bytes = mul(polygons.capacity(), size_of::<Vec<Vec<[f64; 2]>>>())?;
            for polygon_value in polygons {
                bytes = add(bytes, polygon(polygon_value)?)?;
            }
            bytes
        }
    };
    if let SpatialCrs::Preserved(name) = &value.spatial_type.crs {
        bytes = add(bytes, name.capacity())?;
    }
    for text in [&value.extension_name, &value.extension_metadata]
        .into_iter()
        .flatten()
    {
        bytes = add(bytes, text.capacity())?;
    }
    Ok(bytes)
}

fn literal_heap(value: &IrLiteral) -> Result<usize, GfError> {
    match value {
        IrLiteral::Str(text) => Ok(text.capacity()),
        IrLiteral::ZonedDateTime { zone, .. } => Ok(zone.as_ref().map_or(0, String::capacity)),
        IrLiteral::List(values) => {
            let mut bytes = mul(values.capacity(), size_of::<IrLiteral>())?;
            for value in values {
                bytes = add(bytes, literal_heap(value)?)?;
            }
            Ok(bytes)
        }
        IrLiteral::Map(_) => Err(refused("maps are not stored property values")),
        IrLiteral::Spatial(value) => spatial_heap(value),
        _ => Ok(0),
    }
}

fn literal_codec(value: &IrLiteral) -> Result<usize, GfError> {
    // Admitted strings are cloned beside Arrow old/new buffer growth: four
    // payload copies cover those owners. Admitted numeric, temporal and spatial
    // paths use typed builders; 128 bytes cover scalar slots/offsets/validity,
    // not arbitrary numeric formatting. Lists clone a synthetic map per element.
    let mut bytes = add(mul(literal_heap(value)?, 4)?, 128)?;
    if let IrLiteral::List(values) = value {
        if values.len() > i32::MAX as usize {
            return Err(refused("property list offset overflow"));
        }
        for value in values {
            bytes = add(
                bytes,
                add(
                    add(
                        hash_map_bytes(1)?,
                        size_of::<[u8; 16]>()
                            + size_of::<std::collections::HashMap<String, IrLiteral>>()
                            + "item".len(),
                    )?,
                    literal_codec(value)?,
                )?,
            )?;
        }
    }
    Ok(bytes)
}

fn type_nodes(data_type: &DataType) -> Result<usize, GfError> {
    let mut nodes = 1;
    match data_type {
        DataType::List(field) => nodes = add(nodes, type_nodes(field.data_type())?)?,
        DataType::Struct(fields) => {
            for field in fields {
                nodes = add(nodes, type_nodes(field.data_type())?)?;
            }
        }
        _ => {}
    }
    Ok(nodes)
}

/// Arrow conversion admission. Source heaps remain separately charged; the
/// converter admits its row vectors and each destination table at allocation.
/// Recursion includes list synthetic rows and their cloned payloads; completed
/// columns and the active builder overlap.
pub(crate) fn codec(rows: &[Snapshot], schema: &Schema) -> Result<usize, GfError> {
    codec_rows(rows.iter().map(|row| &row.row), schema)
}

pub(crate) fn codec_rows<'a>(
    rows: impl IntoIterator<Item = &'a crate::PropertySnapshotRow>,
    schema: &Schema,
) -> Result<usize, GfError> {
    let mut bytes = 0;
    for field in schema.fields() {
        // Arrow's default primitive/byte builders start at1024 slots. The
        // largest native scalar slot is smaller than IrLiteral; account one
        // default buffer and its growth/finish copies per recursive node.
        bytes = add(
            bytes,
            mul(
                type_nodes(field.data_type())?,
                add(mul(1024, size_of::<IrLiteral>().max(64))?, ARRAY_NODE)?,
            )?,
        )?;
        bytes = add(bytes, mul(field.size(), 4)?)?;
    }
    let mut row_count = 0;
    for snapshot in rows {
        row_count = add(row_count, 1)?;
        for (key, value) in &snapshot.values {
            bytes = add(bytes, mul(key.capacity(), 8)?)?;
            bytes = add(bytes, literal_codec(value)?)?;
        }
    }
    // A row missing a field still appends a NULL slot to that field's typed
    // builder. Count slots from the complete destination schema, independently
    // of the literals present in older snapshots; this also funds UUID rows.
    // Vec/buffer builders double capacity, and offsets have rows + 1 entries.
    // The null constructor bound at that capacity covers retained slots and
    // growth copies, recursively including struct children and list offsets.
    // Empty list-child builders retain the default-node allowance above.
    let capacity = add(row_count, 1)?
        .checked_next_power_of_two()
        .ok_or_else(|| refused("property conversion row capacity overflow"))?;
    for field in schema.fields() {
        bytes = add(
            bytes,
            crate::read_memory::null_bytes(field.data_type(), capacity)
                .map_err(GfError::from_execution_error)?,
        )?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use std::collections::{BTreeMap, HashMap};

    #[test]
    fn guard_growth_refusal_and_cancellation_preserve_existing_authority() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
        let budget = Budget::new(&pool);
        let mut memory = budget.reserve(256).unwrap();
        memory.grow(512).unwrap();
        assert_eq!(pool.reserved(), 768);
        assert_eq!(memory.grow(512).unwrap_err().code(), "GF_RESOURCE_LIMIT");
        assert_eq!(memory.bytes, 768);
        assert_eq!(pool.reserved(), 768);
        assert_eq!(
            memory.grow(usize::MAX).unwrap_err().code(),
            "GF_RESOURCE_LIMIT"
        );
        assert_eq!(pool.reserved(), 768);
        budget.cancel();
        assert_eq!(memory.grow(128).unwrap_err().code(), "GF_CANCELLED");
        assert_eq!(memory.bytes, 768);
        assert_eq!(pool.reserved(), 768);
        drop(memory);
        assert_eq!(pool.reserved(), 0);
    }

    #[derive(Debug)]
    struct AdmissionPool {
        inner: GreedyMemoryPool,
        requests: Mutex<Vec<(usize, usize, bool)>>,
    }

    impl std::fmt::Display for AdmissionPool {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.inner.fmt(f)
        }
    }

    impl MemoryPool for AdmissionPool {
        fn grow(&self, reservation: &MemoryReservation, additional: usize) {
            self.inner.grow(reservation, additional);
        }

        fn shrink(&self, reservation: &MemoryReservation, bytes: usize) {
            self.inner.shrink(reservation, bytes);
        }

        fn try_grow(
            &self,
            reservation: &MemoryReservation,
            additional: usize,
        ) -> datafusion::error::Result<()> {
            let before = self.inner.reserved();
            let result = self.inner.try_grow(reservation, additional);
            self.requests
                .lock()
                .unwrap()
                .push((additional, before, result.is_ok()));
            result
        }

        fn reserved(&self) -> usize {
            self.inner.reserved()
        }
        fn name(&self) -> &str {
            "AdmissionPool"
        }
        fn memory_limit(&self) -> datafusion::execution::memory_pool::MemoryLimit {
            self.inner.memory_limit()
        }
    }

    fn scalar_rows(count: usize) -> Vec<crate::PropertySnapshotRow> {
        (0..count)
            .map(|index| crate::PropertySnapshotRow {
                uuid: (index as u128).to_le_bytes(),
                tombstone: false,
                values: BTreeMap::from([(
                    "name".into(),
                    IrLiteral::Str(format!("entity-{index}")),
                )]),
            })
            .collect()
    }

    fn own_rows(
        rows: Vec<crate::PropertySnapshotRow>,
        budget: &Arc<Budget>,
    ) -> (Guard, Vec<Snapshot>) {
        let accumulator = budget.reserve(rows.len() * size_of::<Snapshot>()).unwrap();
        let mut snapshots = Vec::with_capacity(rows.len());
        for row in rows {
            let memory = Some(
                budget
                    .reserve(retained_decoded_row(&row, 2).unwrap())
                    .unwrap(),
            );
            snapshots.push(Snapshot { row, memory });
            snapshots.last_mut().unwrap().release_carrier().unwrap();
        }
        (accumulator, snapshots)
    }

    #[test]
    fn flat_snapshot_decoder_fits_owned_payload_admission_without_new_arrow_headers() {
        use arrow::array::{
            ArrayRef, BooleanArray, FixedSizeBinaryArray, Float64Array, Int64Array, StringArray,
            Time64NanosecondArray, TimestampMicrosecondArray,
        };
        use arrow::datatypes::{Field, TimeUnit};

        let text = "owned utf8 ".repeat(512);
        let fields = vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new(crate::PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("count", DataType::Int64, true),
            Field::new("ratio", DataType::Float64, false),
            Field::new("enabled", DataType::Boolean, false),
            Field::new("time", DataType::Time64(TimeUnit::Nanosecond), false),
            Field::new(
                "at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ];
        let columns: Vec<ArrayRef> = vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([vec![1; 16], vec![2; 16]].into_iter())
                    .unwrap(),
            ),
            Arc::new(BooleanArray::from(vec![false, false])),
            Arc::new(StringArray::from(vec![text.as_str(), "second"])),
            Arc::new(Int64Array::from(vec![Some(7), None])),
            Arc::new(Float64Array::from(vec![1.25, 2.5])),
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(Time64NanosecondArray::from(vec![10, 20])),
            Arc::new(TimestampMicrosecondArray::from(vec![30, 40])),
        ];
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let bytes = decoded_rows(&batch, "node_uuid").unwrap();
        // The 5.6 KiB UTF8 source retains doubled backing capacity after its
        // second append. Actual buffers, decoded maps and payloads fit 24 KiB;
        // eight additional nonexistent scalar headers would exhaust this pool.
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(24 * 1024));
        let budget = Budget::new(&pool);
        let mut memory = budget.reserve(bytes).unwrap();
        let rows = crate::property_overlay::decode_snapshot_batch(&batch, "node_uuid").unwrap();
        for (index, row) in rows.iter().enumerate() {
            assert_eq!(row.uuid, [index as u8 + 1; 16]);
            assert!(!row.tombstone);
            let mut expected = BTreeMap::from([
                (
                    "name".into(),
                    IrLiteral::Str(if index == 0 {
                        text.clone()
                    } else {
                        "second".into()
                    }),
                ),
                (
                    "ratio".into(),
                    IrLiteral::Float(if index == 0 { 1.25 } else { 2.5 }),
                ),
                ("enabled".into(), IrLiteral::Bool(index == 0)),
                (
                    "time".into(),
                    IrLiteral::Time(if index == 0 { 10 } else { 20 }),
                ),
                (
                    "at".into(),
                    IrLiteral::DateTime(if index == 0 { 30 } else { 40 }),
                ),
            ]);
            if index == 0 {
                expected.insert("count".into(), IrLiteral::Int(7));
            }
            assert_eq!(row.values, expected);
        }
        drop(batch);
        memory
            .shrink_to(retained_decoded_rows(&rows, rows.capacity(), 0, 7).unwrap())
            .unwrap();
        assert!(
            pool.reserved() >= text.len(),
            "decoded UTF8 owns its payload after Arrow drops"
        );
        drop(rows);
        drop(memory);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn nested_list_decoder_still_accounts_for_real_child_array_slices() {
        use arrow::array::{ArrayRef, ListArray};
        use arrow::datatypes::{Field, Int64Type};

        let list = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(3), Some(5)]),
            Some(vec![Some(7)]),
        ]);
        // This is the exact ListArray::value operation used by decode_value:
        // a distinct child array header sharing the source backing allocation.
        let slice = list.value(0);
        assert!(!Arc::ptr_eq(&slice, list.values()));
        assert_eq!(slice.len(), 2);
        assert_eq!(
            slice.as_primitive::<Int64Type>().values().as_ptr(),
            list.values().as_primitive::<Int64Type>().values().as_ptr()
        );
        let field = Field::new("items", list.data_type().clone(), false);
        let array: ArrayRef = Arc::new(list);
        let bytes = array_literals(array.as_ref(), &field).unwrap();
        assert!(
            bytes >= array.get_array_memory_size() + 2 * ARRAY_NODE,
            "container header authority remains in addition to source buffers"
        );
        assert_eq!(
            crate::decode_property_value(&array, &field, 0).unwrap(),
            IrLiteral::List(vec![IrLiteral::Int(3), IrLiteral::Int(5)])
        );
        assert_eq!(
            crate::decode_property_value(&array, &field, 1).unwrap(),
            IrLiteral::List(vec![IrLiteral::Int(7)])
        );
    }

    #[test]
    fn property_arrow_workspace_is_admitted_after_source_maps_are_consumed() {
        let original = scalar_rows(256);
        let expected = crate::writer::property_snapshots_to_batch("route", false, original.clone())
            .unwrap()
            .unwrap();
        let estimates = original
            .into_iter()
            .map(|row| Snapshot { row, memory: None })
            .collect::<Vec<_>>();
        let arrow_bytes = codec(&estimates, expected.schema().as_ref()).unwrap();
        let source_bytes: usize = estimates
            .iter()
            .map(|snapshot| retained_decoded_row(&snapshot.row, 2).unwrap())
            .sum();
        let tables = estimates.len() * hash_map_bytes(1).unwrap();
        let vectors = estimates.len()
            * (size_of::<MapMemory>() + size_of::<([u8; 16], HashMap<String, IrLiteral>)>());
        let limit = (source_bytes + vectors + hash_map_bytes(1).unwrap()).max(
            source_bytes - estimates.len() * decoded_map_bytes(1).unwrap()
                + tables
                + vectors
                + arrow_bytes,
        );
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        let budget = Budget::new(&pool);
        let (accumulator, mut rows) = own_rows(
            estimates.into_iter().map(|snapshot| snapshot.row).collect(),
            &budget,
        );
        assert_eq!(
            budget.reserve(arrow_bytes).unwrap_err().code(),
            "GF_RESOURCE_LIMIT",
            "future Arrow workspace cannot coexist with all source nodes"
        );
        let arrow;
        let batch;
        (batch, arrow) = crate::writer::admitted_property_snapshots_to_batch(
            "route",
            false,
            rows.drain(..),
            &budget,
            arrow_bytes,
        )
        .unwrap()
        .unwrap();
        assert_eq!(batch, expected);
        assert_eq!(
            pool.reserved(),
            rows.capacity() * size_of::<Snapshot>() + arrow_bytes
        );
        drop(batch);
        assert!(
            pool.reserved() >= arrow_bytes,
            "returned Arrow authority remains live"
        );
        drop(arrow);
        drop(rows);
        drop(accumulator);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn property_arrow_refusal_after_map_transfer_releases_every_owner() {
        let original = scalar_rows(2);
        let expected = crate::writer::property_snapshots_to_batch("route", false, original.clone())
            .unwrap()
            .unwrap();
        let source_bytes: usize = original
            .iter()
            .map(|row| retained_decoded_row(row, 2).unwrap())
            .sum();
        let table_bytes = hash_map_bytes(1).unwrap();
        let observed = Arc::new(AdmissionPool {
            inner: GreedyMemoryPool::new(source_bytes + 4096),
            requests: Mutex::default(),
        });
        let pool: Arc<dyn MemoryPool> = observed.clone();
        let budget = Budget::new(&pool);
        let (accumulator, mut rows) = own_rows(original, &budget);
        let arrow_bytes = codec(&rows, expected.schema().as_ref()).unwrap();
        observed.requests.lock().unwrap().clear();
        let error = crate::writer::admitted_property_snapshots_to_batch(
            "route",
            false,
            rows.drain(..),
            &budget,
            arrow_bytes,
        )
        .unwrap_err();
        assert_eq!(error.code(), "GF_RESOURCE_LIMIT");
        let requests = observed.requests.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|(bytes, _, ok)| *bytes == table_bytes && *ok)
                .count(),
            2,
            "both real destination tables must precede Arrow admission"
        );
        let &(bytes, before, ok) = requests.last().unwrap();
        assert_eq!(bytes, arrow_bytes);
        assert!(!ok);
        assert!(
            before < source_bytes,
            "source nodes were freed before the Arrow request"
        );
        drop(requests);
        assert_eq!(pool.reserved(), rows.capacity() * size_of::<Snapshot>());
        drop(rows);
        drop(accumulator);
        assert_eq!(pool.reserved(), 0);
        assert!(
            crate::writer::admitted_property_snapshots_to_batch(
                "route",
                false,
                Vec::new().drain(..),
                &budget,
                arrow_bytes,
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(pool.reserved(), 0);
    }

    fn payload_row(index: u8) -> crate::PropertySnapshotRow {
        crate::PropertySnapshotRow {
            uuid: [index; 16],
            tombstone: false,
            values: BTreeMap::from([(
                "payload".into(),
                IrLiteral::List(vec![
                    IrLiteral::Str(char::from(b'a' + index).to_string().repeat(8192)),
                    IrLiteral::Int(i64::from(index)),
                ]),
            )]),
        }
    }

    #[test]
    fn decoded_property_transfer_preserves_tombstone_split_node_admission() {
        use arrow::array::{ArrayRef, BooleanArray, FixedSizeBinaryArray, StringArray};
        use arrow::datatypes::Field;

        let uuid_field = "node_uuid";
        for live_properties in [10, 11, 12] {
            let mut fields = vec![Field::new(uuid_field, DataType::FixedSizeBinary(16), false)];
            let mut columns: Vec<ArrayRef> = vec![Arc::new(
                FixedSizeBinaryArray::try_from_iter([vec![7_u8; 16]].into_iter()).unwrap(),
            )];
            let expected = (0..live_properties)
                .map(|index| {
                    (
                        format!("property_{index:02}"),
                        format!("value_{index}_").repeat(512),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            for (name, text) in &expected {
                fields.push(Field::new(name, DataType::Utf8, false));
                columns.push(Arc::new(StringArray::from(vec![text.as_str()])));
            }
            fields.push(Field::new(
                crate::PROPERTY_TOMBSTONE_FIELD,
                DataType::Boolean,
                false,
            ));
            columns.push(Arc::new(BooleanArray::from(vec![false])));
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
            let property_fields = batch.num_columns() - 1;
            assert_eq!(property_fields, live_properties + 1);
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
            let budget = Budget::new(&pool);
            let mut decoder = budget
                .reserve(decoded_rows(&batch, uuid_field).unwrap())
                .unwrap();
            let mut decoded =
                crate::property_overlay::decode_snapshot_batch(&batch, uuid_field).unwrap();
            drop(batch);
            decoder
                .shrink_to(
                    retained_decoded_rows(&decoded, decoded.capacity(), 0, property_fields)
                        .unwrap(),
                )
                .unwrap();
            let accumulator = budget.reserve(size_of::<Snapshot>()).unwrap();
            let mut rows = Vec::with_capacity(1);
            let row = decoded.pop().unwrap();
            assert!(!row.tombstone);
            assert_eq!(row.values.len(), live_properties);
            let memory = Some(
                decoder
                    .split_off(retained_decoded_row(&row, property_fields).unwrap())
                    .unwrap(),
            );
            rows.push(Snapshot { row, memory });
            rows.last_mut().unwrap().release_carrier().unwrap();
            drop(decoded);
            drop(decoder);
            let (uuid, values, memory) = rows.pop().unwrap().into_admitted_map(&budget).unwrap();
            assert_eq!(uuid, [7; 16]);
            assert_eq!(values.len(), expected.len());
            let mut moved_heaps = 0;
            for (name, text) in &expected {
                let (key, value) = values.get_key_value(name).unwrap();
                assert_eq!(value, &IrLiteral::Str(text.clone()));
                moved_heaps += key.capacity() + literal_heap(value).unwrap();
            }
            let source = memory._source.as_ref().unwrap().bytes;
            assert!(
                source >= moved_heaps,
                "all moved keys and values remain admitted"
            );
            if live_properties == 11 {
                assert!(
                    source > moved_heaps,
                    "the pre-removal twelve-field map retains conservative split-node residue"
                );
            }
            assert!(memory._table.bytes > 0);
            assert_eq!(
                pool.reserved(),
                size_of::<Snapshot>() + source + memory._table.bytes
            );
            drop(values);
            drop(memory);
            assert_eq!(pool.reserved(), size_of::<Snapshot>());
            drop(rows);
            drop(accumulator);
            assert_eq!(pool.reserved(), 0);
        }
    }

    #[test]
    fn property_map_transfer_releases_nodes_but_retains_moved_payloads() {
        let first = payload_row(1);
        let second = payload_row(2);
        let source_bytes =
            retained_decoded_row(&first, 2).unwrap() + retained_decoded_row(&second, 2).unwrap();
        let carriers = 2 * size_of::<Snapshot>();
        let table_bytes = hash_map_bytes(first.values.len()).unwrap();
        let pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(source_bytes + carriers + table_bytes));
        let budget = Budget::new(&pool);
        let accumulator = budget.reserve(carriers).unwrap();
        let mut rows = Vec::with_capacity(2);
        for row in [first, second] {
            let memory = Some(
                budget
                    .reserve(retained_decoded_row(&row, 2).unwrap())
                    .unwrap(),
            );
            rows.push(Snapshot { row, memory });
            rows.last_mut().unwrap().release_carrier().unwrap();
        }
        let before = pool.reserved();
        assert_eq!(
            budget.reserve(2 * table_bytes).unwrap_err().code(),
            "GF_RESOURCE_LIMIT",
            "both tables cannot be admitted while all source nodes remain"
        );
        let IrLiteral::List(items) = &rows[0].values["payload"] else {
            panic!("expected nested payload")
        };
        let IrLiteral::Str(text) = &items[0] else {
            panic!("expected string")
        };
        let payload_pointer = text.as_ptr();
        let (uuid, values, memory) = rows.remove(0).into_admitted_map(&budget).unwrap();
        assert_eq!(uuid, [1; 16]);
        assert!(
            pool.reserved() < before,
            "consumed BTree nodes must release"
        );
        let IrLiteral::List(items) = &values["payload"] else {
            panic!("expected nested payload")
        };
        let IrLiteral::Str(text) = &items[0] else {
            panic!("expected string")
        };
        assert_eq!(text.as_ptr(), payload_pointer, "payload heap must move");
        assert_eq!(text, &"b".repeat(8192));
        // Only one extra table fits beside all source allocations. The second
        // allocation succeeds by releasing the first map's consumed nodes.
        let (_, other_values, other_memory) = rows.remove(0).into_admitted_map(&budget).unwrap();
        drop(other_values);
        drop(other_memory);
        drop(rows);
        drop(accumulator);
        assert!(
            pool.reserved() >= text.capacity(),
            "moved heap remains owned"
        );
        let retained = pool.reserved();
        drop(values);
        assert_eq!(pool.reserved(), retained, "guard outlives destination map");
        drop(memory);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn admitted_property_conversion_preserves_node_and_edge_batches_and_cleans_refusals() {
        for is_edge in [false, true] {
            let original = vec![payload_row(1), payload_row(2)];
            let expected =
                crate::writer::property_snapshots_to_batch("route", is_edge, original.clone())
                    .unwrap()
                    .unwrap();
            for refuse in [false, true] {
                let source_bytes: usize = original
                    .iter()
                    .map(|row| retained_decoded_row(row, 2).unwrap())
                    .sum();
                let carriers = original.len() * size_of::<Snapshot>();
                let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(if refuse {
                    source_bytes + carriers
                } else {
                    16 * 1024 * 1024
                }));
                let budget = Budget::new(&pool);
                let accumulator = budget.reserve(carriers).unwrap();
                let mut rows = Vec::with_capacity(original.len());
                for row in original.clone() {
                    let memory = Some(
                        budget
                            .reserve(retained_decoded_row(&row, 2).unwrap())
                            .unwrap(),
                    );
                    rows.push(Snapshot { row, memory });
                    rows.last_mut().unwrap().release_carrier().unwrap();
                }
                let blocker = refuse.then(|| {
                    budget
                        .reserve(source_bytes + carriers - pool.reserved())
                        .unwrap()
                });
                let codec_bytes = codec(&rows, expected.schema().as_ref()).unwrap();
                let converted = crate::writer::admitted_property_snapshots_to_batch(
                    "route",
                    is_edge,
                    rows.drain(..),
                    &budget,
                    codec_bytes,
                );
                if refuse {
                    assert_eq!(converted.unwrap_err().code(), "GF_RESOURCE_LIMIT");
                } else {
                    let arrow;
                    let batch;
                    (batch, arrow) = converted.unwrap().unwrap();
                    assert_eq!(batch, expected);
                    assert!(pool.reserved() >= batch.get_array_memory_size());
                    drop(batch);
                    drop(arrow);
                }
                assert!(rows.is_empty());
                drop(rows);
                drop(blocker);
                assert_eq!(pool.reserved(), carriers);
                drop(accumulator);
                assert_eq!(pool.reserved(), 0);
            }
        }
    }

    #[test]
    fn property_codec_table_admission_covers_std_map_capacity_boundaries() {
        // Capacities come from the actual std maps below. Bucket counts are
        // the pinned hashbrown layout at each small-table/load transition.
        for (entries, capacity, buckets) in [
            (0, 0, 0),
            (1, 3, 4),
            (3, 3, 4),
            (4, 7, 8),
            (7, 7, 8),
            (8, 14, 16),
            (14, 14, 16),
            (15, 28, 32),
            (28, 28, 32),
            (29, 56, 64),
            (56, 56, 64),
            (57, 112, 128),
        ] {
            let source = (0..entries)
                .map(|index| {
                    (
                        format!("property_{index}"),
                        IrLiteral::Int(i64::try_from(index).unwrap()),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let values = source.into_iter();
            assert_eq!(values.size_hint(), (entries, Some(entries)));
            let converted: HashMap<_, _> = values.collect();
            assert_eq!(converted.len(), entries);
            assert_eq!(converted.capacity(), capacity);
            assert_eq!(
                converted.capacity(),
                HashMap::<String, IrLiteral>::with_capacity(entries).capacity(),
                "collect must reserve once from the exact iterator length"
            );
            let table = hash_map_bytes(entries).unwrap();
            if entries == 0 {
                assert_eq!(table, 0);
            } else {
                assert!(
                    table >= buckets * (size_of::<(String, IrLiteral)>() + 1) + 16,
                    "admit every bucket, its control byte and the trailing SIMD group"
                );
            }
            for index in 0..entries {
                assert_eq!(
                    converted[&format!("property_{index}")],
                    IrLiteral::Int(i64::try_from(index).unwrap())
                );
            }
        }
        assert_eq!(
            hash_map_bytes(usize::MAX).unwrap_err().code(),
            "GF_RESOURCE_LIMIT"
        );
    }
}
