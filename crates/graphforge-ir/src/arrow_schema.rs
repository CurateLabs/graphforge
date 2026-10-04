//! Shared Arrow value and graph-row schemas used by planning and storage.
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use std::sync::{Arc, LazyLock};

/// Arrow field metadata key marking an execution-internal surrogate identity
/// column (`node_id`, `edge_id`, `src_id`, `dst_id`, …). Public result shaping
/// drops fields that carry this marker; user projections that happen to reuse
/// the same spelling (e.g. `RETURN 42 AS node_id`) must not (#703).
pub const INTERNAL_SURROGATE_META_KEY: &str = "graphforge.internal_surrogate";

/// UUID column: `FixedSizeBinary(16)`, not nullable.
#[must_use]
pub fn uuid_field(name: &str) -> Field {
    Field::new(name, DataType::FixedSizeBinary(16), false)
}

/// Surrogate ID column: `UInt64`, not nullable, stamped with
/// [`INTERNAL_SURROGATE_META_KEY`] so public shaping can distinguish it from a
/// legal user alias of the same name (#703).
#[must_use]
pub fn id_field(name: &str) -> Field {
    Field::new(name, DataType::UInt64, false).with_metadata(
        [(INTERNAL_SURROGATE_META_KEY.to_owned(), "true".to_owned())]
            .into_iter()
            .collect(),
    )
}

/// True when `field` is an execution-internal surrogate identity column.
///
/// Dropping requires the **current public field name** to still be a scan-key
/// spelling (`node_id` / `edge_id` / `src_id` / `dst_id` / `neighbor_id`).
/// DataFusion preserves [`INTERNAL_SURROGATE_META_KEY`] across `AS` renames, so
/// a projection like `RETURN b.node_id AS id` must remain public under `id`
/// (#703 / fixed-hop LIMIT regressions).
///
/// Provenance is the stamped metadata from [`id_field`]. As a storage-contract
/// fallback, an unmarked top-level `node_id`/`edge_id` that is still `UInt64`
/// is treated as a surrogate (user Cypher projections of those names are never
/// bare `UInt64` scan keys). Name alone is never sufficient (#703).
#[must_use]
pub fn is_internal_surrogate_field(field: &Field) -> bool {
    let name = field.name().as_str();
    let is_scan_key_name = matches!(
        name,
        "node_id" | "edge_id" | "src_id" | "dst_id" | "neighbor_id"
    );
    if !is_scan_key_name {
        return false;
    }
    if field
        .metadata()
        .get(INTERNAL_SURROGATE_META_KEY)
        .is_some_and(|value| value == "true")
    {
        return true;
    }
    matches!(name, "node_id" | "edge_id") && *field.data_type() == DataType::UInt64
}

/// The Arrow fields of a typed Cypher `duration` value (ADR 0009): signed
/// `Struct{months: Int64, days: Int64, seconds: Int64, nanos: Int64}`. A struct
/// (not Arrow `Interval`) because Parquet cannot persist `Interval(MonthDayNano)`;
/// the single source of truth shared by storage and the graphforge-rel query value.
/// months/days/seconds are Int64 and seconds is split from nanos so billion-year
/// `duration.between`/`inSeconds` spans fit (#920/#1011); nanos is
/// nanoseconds-of-second, sharing the sign of seconds.
#[must_use]
pub fn duration_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("months", DataType::Int64, true),
        Field::new("days", DataType::Int64, true),
        Field::new("seconds", DataType::Int64, true),
        Field::new("nanos", DataType::Int64, true),
    ])
}

/// Whether an Arrow value can be normalized into GraphForge's ordinary
/// persisted property representation. This is the input acceptance rule;
/// [`canonical_property_data_type`] maps an accepted type to the form that is
/// persisted, and [`property_data_type_canonical`] recognizes that form.
#[must_use]
pub fn property_data_type_supported(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) => {
            property_data_type_supported(field.data_type())
        }
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond) => true,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, zone) => {
            zone.as_deref() == Some("UTC")
        }
        DataType::Struct(fields) => temporal_struct(fields),
        _ => false,
    }
}

/// Whether an Arrow property type is GraphForge's canonical persisted
/// representation: the only property types storage persists and its readers
/// decode. Integers are `Int64`, floats `Float64`, strings `Utf8`, and lists
/// `List` of a canonical element; booleans and temporal values are already
/// canonical on input.
#[must_use]
pub fn property_data_type_canonical(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(field) => property_data_type_canonical(field.data_type()),
        DataType::Boolean
        | DataType::Int64
        | DataType::Float64
        | DataType::Utf8
        | DataType::Time64(TimeUnit::Nanosecond) => true,
        DataType::Timestamp(TimeUnit::Microsecond, zone) => zone.as_deref() == Some("UTC"),
        DataType::Struct(fields) => temporal_struct(fields),
        _ => false,
    }
}

/// The canonical persisted type of an accepted property type, or `None` when
/// [`property_data_type_supported`] rejects it. Every accepted integer width
/// widens losslessly to `Int64`, `Float32` to `Float64`, `LargeUtf8` to
/// `Utf8`, and `LargeList` to `List`, recursively for list elements; a list
/// element keeps its name, nullability and metadata.
#[must_use]
pub fn canonical_property_data_type(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => Some(DataType::Int64),
        DataType::Float32 | DataType::Float64 => Some(DataType::Float64),
        DataType::Utf8 | DataType::LargeUtf8 => Some(DataType::Utf8),
        DataType::List(field) | DataType::LargeList(field) => {
            let element = canonical_property_data_type(field.data_type())?;
            Some(DataType::List(Arc::new(
                field.as_ref().clone().with_data_type(element),
            )))
        }
        other => property_data_type_supported(other).then(|| other.clone()),
    }
}

fn temporal_struct(fields: &Fields) -> bool {
    fields == &duration_struct_fields()
        || fields == &date_struct_fields()
        || fields == &localdatetime_struct_fields()
        || fields == &time_struct_fields()
        || fields == &datetime_struct_fields()
}

/// `Struct{epoch_day: Int64}` — a Cypher `date` typed value (ADR 0012): i64 days
/// since the Unix epoch, spanning the full openCypher year range
/// −999,999,999..+999,999,999. A self-describing one-field struct (a bare Int64
/// would be indistinguishable from an integer property on decode); orders
/// chronologically. The single source of truth shared by storage and graphforge-rel. (#1011)
#[must_use]
pub fn date_struct_fields() -> Fields {
    Fields::from(vec![Field::new("epoch_day", DataType::Int64, true)])
}

/// `Struct{date: Int64, time: Time64(ns)}` — a Cypher `localdatetime` typed
/// value (ADR 0009/0012). A two-field struct (not an epoch instant) so it spans
/// the full openCypher year range at nanosecond precision; `date` is i64 days
/// (#1011). The single source of truth shared by storage and the graphforge-rel value.
#[must_use]
pub fn localdatetime_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("date", DataType::Int64, true),
        Field::new("time", DataType::Time64(TimeUnit::Nanosecond), true),
    ])
}

/// `Struct{time: Time64(ns), offset: Int32}` — a Cypher `time` typed value: a
/// time-of-day plus its UTC offset in seconds (ADR 0009). Shared by storage and
/// graphforge-rel. (#920)
#[must_use]
pub fn time_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("time", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new("offset", DataType::Int32, true),
    ])
}

/// `Struct{date: Int64, time: Time64(ns), offset: Int32, zone: Utf8}` — a
/// Cypher `datetime` typed value: a date+time (date = i64 days, #1011), its UTC
/// offset in seconds, and an optional named IANA zone (null when offset-only)
/// (ADR 0009/0012). Shared by storage and graphforge-rel.
#[must_use]
pub fn datetime_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("date", DataType::Int64, true),
        Field::new("time", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new("offset", DataType::Int32, true),
        Field::new("zone", DataType::Utf8, true),
    ])
}

// ---------------------------------------------------------------------------
// TOPOLOGY_NODES_SCHEMA
// ---------------------------------------------------------------------------

/// Schema for `topology/nodes.parquet`.
///
/// Stores the identity and type of every node.  Property data lives in
/// per-entity-type files under `properties/`.
pub static TOPOLOGY_NODES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("node_uuid"),
        id_field("node_id"),
        // Immutable primary label retained for legacy files and property-stem
        // routing. `type_ids` is the authoritative full label set (#799).
        Field::new("type_id", DataType::UInt32, false),
        Field::new(
            "type_ids",
            DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
            false,
        ),
        ts_field("created_at"),
        ts_field("updated_at"),
    ]))
});

// ---------------------------------------------------------------------------
// TYPED_EDGE_SCHEMA
// ---------------------------------------------------------------------------

/// Schema for `topology/edges/TYPENAME.parquet`.
///
/// One file per relation type.  Joins against `topology/nodes.parquet` use
/// `src_id`/`dst_id` surrogates; `src_uuid`/`dst_uuid` are for API outputs.
pub static TYPED_EDGE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("edge_uuid"),
        uuid_field("src_uuid"),
        uuid_field("dst_uuid"),
        id_field("edge_id"),
        id_field("src_id"),
        id_field("dst_id"),
        ts_field("created_at"),
    ]))
});

// ---------------------------------------------------------------------------
// EXPLORATORY_EDGE_SCHEMA
// ---------------------------------------------------------------------------

/// Schema for `topology/edges/_exploratory.parquet`.
///
/// Catch-all bucket for edges whose relation type was not declared in a formal
/// ontology at write time.  Extends [`TYPED_EDGE_SCHEMA`] with a
/// `rel_type_name` string column so the executor can filter by relation name.
pub static EXPLORATORY_EDGE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let mut fields: Vec<Field> = TYPED_EDGE_SCHEMA
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new("rel_type_name", DataType::Utf8, false));
    Arc::new(Schema::new(fields))
});

// ---------------------------------------------------------------------------
// PROPERTY_BASE_SCHEMA
// ---------------------------------------------------------------------------

/// Minimal schema for `properties/ENTITY_TYPE.parquet` before per-type columns
/// are added.  Contains only the join key (`node_uuid`).
///
/// Storage extends this base with the per-entity-type property fields.
pub static PROPERTY_BASE_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(Schema::new(vec![uuid_field("node_uuid")])));

// ---------------------------------------------------------------------------
// EDGE_PROPERTY_BASE_SCHEMA
// ---------------------------------------------------------------------------

/// Minimal schema for `edge_properties/REL_TYPE.parquet` before per-relation
/// columns are added.  Contains only the join key (`edge_uuid`).
///
/// Edge properties live in a dedicated `edge_properties/` directory (keyed by
/// `edge_uuid`) so a relation type can never collide with a node label sharing
/// the same name in `properties/`.
pub static EDGE_PROPERTY_BASE_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(Schema::new(vec![uuid_field("edge_uuid")])));

/// Timestamp column: `Timestamp(Microsecond, UTC)`, not nullable.
#[must_use]
pub fn ts_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list(element: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("item", element, true)))
    }

    fn large_list(element: DataType) -> DataType {
        DataType::LargeList(Arc::new(Field::new("item", element, true)))
    }

    #[test]
    fn every_accepted_property_type_maps_to_a_canonical_type() {
        let scalars = [
            (DataType::Boolean, DataType::Boolean),
            (DataType::Int8, DataType::Int64),
            (DataType::Int16, DataType::Int64),
            (DataType::Int32, DataType::Int64),
            (DataType::Int64, DataType::Int64),
            (DataType::UInt8, DataType::Int64),
            (DataType::UInt16, DataType::Int64),
            (DataType::UInt32, DataType::Int64),
            (DataType::Float32, DataType::Float64),
            (DataType::Float64, DataType::Float64),
            (DataType::Utf8, DataType::Utf8),
            (DataType::LargeUtf8, DataType::Utf8),
            (
                DataType::Time64(TimeUnit::Nanosecond),
                DataType::Time64(TimeUnit::Nanosecond),
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            (
                DataType::Struct(duration_struct_fields()),
                DataType::Struct(duration_struct_fields()),
            ),
            (
                DataType::Struct(datetime_struct_fields()),
                DataType::Struct(datetime_struct_fields()),
            ),
        ];
        let mut cases = Vec::new();
        for (input, canonical) in scalars {
            cases.push((list(input.clone()), list(canonical.clone())));
            cases.push((large_list(input.clone()), list(canonical.clone())));
            cases.push((
                large_list(large_list(input.clone())),
                list(list(canonical.clone())),
            ));
            cases.push((input, canonical));
        }
        for (input, canonical) in cases {
            assert!(property_data_type_supported(&input), "{input}");
            assert_eq!(
                canonical_property_data_type(&input).as_ref(),
                Some(&canonical),
                "{input}"
            );
            assert!(property_data_type_canonical(&canonical), "{canonical}");
            assert_eq!(property_data_type_canonical(&input), input == canonical);
        }
    }

    #[test]
    fn unaccepted_property_types_have_no_canonical_form() {
        for input in [
            DataType::Binary,
            DataType::UInt64,
            DataType::Float16,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            list(DataType::UInt64),
            large_list(DataType::Binary),
        ] {
            assert!(!property_data_type_supported(&input), "{input}");
            assert_eq!(canonical_property_data_type(&input), None, "{input}");
            assert!(!property_data_type_canonical(&input), "{input}");
        }
    }

    #[test]
    fn canonical_list_element_keeps_name_nullability_and_metadata() {
        let element = Field::new("element", DataType::Int16, false).with_metadata(
            [("origin".to_owned(), "caller".to_owned())]
                .into_iter()
                .collect(),
        );
        let canonical =
            canonical_property_data_type(&DataType::LargeList(Arc::new(element.clone())));
        assert_eq!(
            canonical,
            Some(DataType::List(Arc::new(
                element.with_data_type(DataType::Int64)
            )))
        );
    }
}
