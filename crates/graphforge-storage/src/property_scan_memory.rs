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

impl std::ops::Deref for Snapshot {
    type Target = crate::PropertySnapshotRow;
    fn deref(&self) -> &Self::Target {
        &self.row
    }
}

// Rust's BTreeMap nodes hold eleven key/value slots and up to twelve edges.
// Sixteen slots per entry cover a singleton node and its edges/header. The
// HashMap conversion's minimum buckets and old/new growth also fit this bound.
const MAP_ENTRY: usize = 16 * (size_of::<(String, IrLiteral)>() + 4 * size_of::<usize>());
const ARRAY_NODE: usize = 4 * size_of::<arrow::array::ArrayData>() + 8 * 64;

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
    let roots = if property_fields <= 11 {
        usize::from(property_fields != 0)
    } else {
        property_fields
    };
    bytes = add(bytes, mul(batch.num_rows(), mul(roots, MAP_ENTRY)?)?)?;
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
    let roots = if property_fields <= 11 {
        usize::from(property_fields != 0)
    } else {
        property_fields
    };
    let mut bytes = add(size_of::<Snapshot>(), mul(roots, MAP_ENTRY)?)?;
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
                    MAP_ENTRY
                        + (size_of::<[u8; 16]>()
                            + size_of::<std::collections::HashMap<String, IrLiteral>>()),
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

/// Admission before BTreeMap-to-HashMap and Arrow conversion. Source snapshots
/// remain separately charged. Recursion includes list synthetic rows and their
/// cloned payloads; completed columns and the active builder overlap.
pub(crate) fn codec(rows: &[Snapshot], schema: &Schema) -> Result<usize, GfError> {
    let mut bytes = mul(
        rows.len(),
        size_of::<crate::PropertySnapshotRow>()
            + (size_of::<[u8; 16]>() + size_of::<std::collections::HashMap<String, IrLiteral>>())
            + size_of::<Option<Guard>>(),
    )?;
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
            bytes = add(bytes, add(MAP_ENTRY, mul(key.capacity(), 8)?)?)?;
            bytes = add(bytes, literal_codec(value)?)?;
        }
    }
    Ok(bytes)
}
