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
fn hash_map_bytes(entries: usize) -> Result<usize, GfError> {
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
    let mut bytes = add(
        array.get_array_memory_size(),
        mul(array.len(), size_of::<IrLiteral>())?,
    )?;
    // Scalar decoding borrows Arrow values and creates only inline literals or
    // their owned payloads, already charged above. It creates no Arrow header
    // per row. Containers retain the existing allowance for sliced child arrays
    // and shape dispatch while recursively decoding their values.
    let headers = match array.data_type() {
        DataType::List(_) | DataType::Struct(_) => array.len().max(1),
        _ => 1,
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
            bytes = add(bytes, array_literals(values.as_ref(), inner)?)?;
        }
        DataType::Struct(fields) => {
            for (child, field) in array.as_struct().columns().iter().zip(fields) {
                bytes = add(bytes, array_literals(child.as_ref(), field)?)?;
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
    for snapshot in rows {
        for (key, value) in &snapshot.values {
            bytes = add(bytes, mul(key.capacity(), 8)?)?;
            bytes = add(bytes, literal_codec(value)?)?;
        }
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use std::collections::{BTreeMap, HashMap};

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
                // Real callers retain the Arrow envelope throughout conversion.
                let blocker = refuse.then(|| {
                    budget
                        .reserve(source_bytes + carriers - pool.reserved())
                        .unwrap()
                });
                let arrow = (!refuse).then(|| {
                    budget
                        .reserve(codec(&rows, expected.schema().as_ref()).unwrap())
                        .unwrap()
                });
                let converted = crate::writer::admitted_property_snapshots_to_batch(
                    "route",
                    is_edge,
                    rows.drain(..),
                    &budget,
                );
                if refuse {
                    assert_eq!(converted.unwrap_err().code(), "GF_RESOURCE_LIMIT");
                } else {
                    assert_eq!(converted.unwrap().unwrap(), expected);
                }
                assert!(rows.is_empty());
                drop(rows);
                drop(blocker);
                drop(arrow);
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
