//! Canonical Arrow schemas for every Parquet file GraphForge reads or writes.
//!
//! These constants are the single source of truth for column names, types, and
//! nullability.  All `TableProvider` implementations, relational lowering, and
//! language bindings must reference these schemas rather than defining their own.
//!
//! # Dual-key identity
//!
//! Every node and edge row carries two identity columns:
//!
//! | Column | Type | Purpose |
//! |--------|------|---------|
//! | `*_uuid` | `FixedSizeBinary(16)` | Canonical UUIDv7 — stable, globally unique, never changes |
//! | `*_id` | `UInt64` | Surrogate assigned at scan time — used for DataFusion joins, never in API outputs |
//!
//! # File layout
//!
//! ```text
//! topology/nodes.parquet            → TOPOLOGY_NODES_SCHEMA
//! topology/edges/TYPENAME.parquet   → TYPED_EDGE_SCHEMA
//! topology/edges/_exploratory.parquet → EXPLORATORY_EDGE_SCHEMA
//! properties/ENTITY_TYPE.parquet    → property_schema(entity_type, defs)
//! indexes/adjacency/index_manifest.parquet → ADJACENCY_MANIFEST_SCHEMA
//! indexes/adjacency/REL.out.csr     → ADJACENCY_CSR_SCHEMA (Arrow IPC, not Parquet)
//! ```

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use graphforge_ontology::ontology::{PropertyDef, PropertyValueType};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Arrow field metadata key marking an execution-internal surrogate identity
/// column (`node_id`, `edge_id`, `src_id`, `dst_id`, …). Public result shaping
/// drops fields that carry this marker; user projections that happen to reuse
/// the same spelling (e.g. `RETURN 42 AS node_id`) must not (#703).
pub const INTERNAL_SURROGATE_META_KEY: &str = "graphforge.internal_surrogate";

/// UUID column: `FixedSizeBinary(16)`, not nullable.
pub(crate) fn uuid_field(name: &str) -> Field {
    Field::new(name, DataType::FixedSizeBinary(16), false)
}

/// Surrogate ID column: `UInt64`, not nullable, stamped with
/// [`INTERNAL_SURROGATE_META_KEY`] so public shaping can distinguish it from a
/// legal user alias of the same name (#703).
pub(crate) fn id_field(name: &str) -> Field {
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

/// Timestamp column: `Timestamp(Microsecond, UTC)`, not nullable.
pub(crate) fn ts_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    )
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
/// Use [`property_schema`] to build the full per-entity-type schema.
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

// ---------------------------------------------------------------------------
// ADJACENCY_CSR_SCHEMA
// ---------------------------------------------------------------------------

/// Fields of one adjacency entry: `{edge_id: UInt64, neighbor_id: UInt64}`.
///
/// Shared between [`ADJACENCY_CSR_SCHEMA`] and the array builders in the
/// [`adjacency`](crate::adjacency) module so the file schema and the arrays
/// written into it can never drift apart.
pub(crate) fn adjacency_entry_fields() -> Fields {
    Fields::from(vec![id_field("edge_id"), id_field("neighbor_id")])
}

/// Schema for `indexes/adjacency/<REL_TYPE>.<dir>.csr` (Arrow IPC, ADR 0005).
///
/// One non-nullable column with one row per surrogate `node_id` in
/// `0..node_count`:
///
/// ```text
/// adjacency: LargeList<Struct { edge_id: UInt64, neighbor_id: UInt64 }>
/// ```
///
/// This is the CSR (compressed sparse row) structure in its idiomatic Arrow
/// encoding: the list's offsets buffer **is** the CSR offsets array (length
/// `node_count + 1`, `Int64`), and the struct child **is** the targets array
/// (length `edge_count`). A node with no neighbors is an empty list; an empty
/// graph is a zero-row batch.
pub static ADJACENCY_CSR_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "adjacency",
        DataType::LargeList(Arc::new(Field::new(
            "item",
            DataType::Struct(adjacency_entry_fields()),
            false,
        ))),
        false,
    )]))
});

// ---------------------------------------------------------------------------
// ADJACENCY_MANIFEST_SCHEMA
// ---------------------------------------------------------------------------

/// Schema for `indexes/adjacency/index_manifest.parquet` (ADR 0005).
///
/// One row per CSR file. `topology_generation` records the topology counter
/// the CSR was built from; a mismatch against the project's current counter
/// marks the index stale. `relation_type` is a relation type name or the
/// reserved `_all` stem for the union index.
pub static ADJACENCY_MANIFEST_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("relation_type", DataType::Utf8, false),
        Field::new("direction", DataType::Utf8, false),
        Field::new("topology_generation", DataType::UInt64, false),
        ts_field("built_at"),
        Field::new("node_count", DataType::UInt64, false),
        Field::new("edge_count", DataType::UInt64, false),
    ]))
});

// ---------------------------------------------------------------------------
// ADJACENCY_DELTA_SCHEMA
// ---------------------------------------------------------------------------

/// Schema for `indexes/adjacency/deltas/<generation>.parquet` (#765).
///
/// One row per edge created by the topology commit that bumped the counter to
/// `<generation>`, in creation (ascending `edge_id`) order. The provider merges
/// a contiguous chain of these segments onto the base CSR to serve a fresh view
/// without a full rebuild. `rel_type_name` is the typed file stem (or the
/// exploratory row's relation) so a per-relation overlay can filter by it; the
/// union (`_all`) overlay takes every row.
pub static ADJACENCY_DELTA_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("rel_type_name", DataType::Utf8, false),
        Field::new("edge_id", DataType::UInt64, false),
        Field::new("src_id", DataType::UInt64, false),
        Field::new("dst_id", DataType::UInt64, false),
    ]))
});

// ---------------------------------------------------------------------------
// Runtime builders
// ---------------------------------------------------------------------------

/// Map a [`PropertyValueType`] to its Arrow [`DataType`].
///
/// `List` and `Map` are encoded as JSON in a `LargeUtf8` column because Arrow
/// does not have a single schema for heterogeneous or self-describing values.
#[must_use]
pub fn property_type_to_arrow(vt: &PropertyValueType) -> DataType {
    match vt {
        PropertyValueType::Utf8 => DataType::Utf8,
        PropertyValueType::Int64 => DataType::Int64,
        PropertyValueType::Float64 => DataType::Float64,
        PropertyValueType::Bool => DataType::Boolean,
        PropertyValueType::Duration => DataType::Struct(duration_struct_fields()),
        PropertyValueType::DateTime => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        PropertyValueType::List | PropertyValueType::Map => DataType::LargeUtf8,
        PropertyValueType::Spatial(spatial) => spatial.data_type(),
    }
}

/// Construct a canonical property field, including GeoArrow extension
/// metadata for spatial values.
#[must_use]
pub fn property_field(def: &PropertyDef) -> Field {
    match def.value_type {
        PropertyValueType::Spatial(spatial) => spatial.field(&def.name, def.nullable),
        _ => Field::new(
            &def.name,
            property_type_to_arrow(&def.value_type),
            def.nullable,
        ),
    }
}

/// Build a `properties/ENTITY_TYPE.parquet` schema for `entity_type`.
///
/// The schema is `PROPERTY_BASE_SCHEMA` (`node_uuid`) followed by one column
/// per entry in `property_defs`, with schema-level metadata identifying the
/// entity type.
#[must_use]
pub fn property_schema(entity_type: &str, property_defs: &[PropertyDef]) -> Schema {
    let mut fields = vec![uuid_field("node_uuid")];
    for def in property_defs {
        fields.push(property_field(def));
    }
    let meta: HashMap<String, String> =
        [("graphforge.entity_type".to_owned(), entity_type.to_owned())]
            .into_iter()
            .collect();
    Schema::new(fields).with_metadata(meta)
}

/// Attach authenticated generation-bound semantic routing metadata.
#[must_use]
pub fn with_semantic_route_metadata(
    schema: &Schema,
    route: &str,
    composition_fingerprint: &str,
) -> Schema {
    let mut metadata = schema.metadata().clone();
    metadata.insert(
        crate::SEMANTIC_ROUTE_METADATA_KEY.to_owned(),
        route.to_owned(),
    );
    metadata.insert(
        crate::SEMANTIC_COMPOSITION_METADATA_KEY.to_owned(),
        composition_fingerprint.to_owned(),
    );
    Schema::new_with_metadata(schema.fields().clone(), metadata)
}

/// Build a query result schema with GraphForge pipeline metadata attached.
///
/// The metadata keys `graphforge.query_id`, `graphforge.ontology_version`, and
/// `graphforge.ir_version` allow consumers to trace result rows back to the
/// exact pipeline that produced them.
///
/// # Panics (debug builds only)
///
/// Panics if any field is an internal surrogate ([`is_internal_surrogate_field`]).
/// Legal user aliases that reuse surrogate spellings (e.g. `RETURN id(n) AS
/// node_id`) are allowed (#703).
#[must_use]
pub fn result_schema(
    fields: Vec<Field>,
    query_id: &str,
    ontology_ver: &str,
    ir_ver: &str,
) -> Schema {
    debug_assert!(
        fields.iter().all(|f| !is_internal_surrogate_field(f)),
        "result_schema: internal surrogate columns must not appear in public API results \
         (offending fields: {:?})",
        fields
            .iter()
            .filter(|f| is_internal_surrogate_field(f))
            .map(arrow::datatypes::Field::name)
            .collect::<Vec<_>>()
    );
    let meta: HashMap<String, String> = [
        ("graphforge.query_id".to_owned(), query_id.to_owned()),
        (
            "graphforge.ontology_version".to_owned(),
            ontology_ver.to_owned(),
        ),
        ("graphforge.ir_version".to_owned(), ir_ver.to_owned()),
    ]
    .into_iter()
    .collect();
    Schema::new(fields).with_metadata(meta)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use graphforge_ontology::ontology::PropertyValueType;

    #[test]
    fn topology_nodes_schema_field_names() {
        let s = &*TOPOLOGY_NODES_SCHEMA;
        assert_eq!(s.fields().len(), 6);
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            [
                "node_uuid",
                "node_id",
                "type_id",
                "type_ids",
                "created_at",
                "updated_at"
            ]
        );
    }

    #[test]
    fn typed_edge_schema_field_types() {
        let s = &*TYPED_EDGE_SCHEMA;
        assert_eq!(s.fields().len(), 7);
        assert_eq!(
            s.fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            [
                "edge_uuid",
                "src_uuid",
                "dst_uuid",
                "edge_id",
                "src_id",
                "dst_id",
                "created_at",
            ]
        );

        // UUID fields are FixedSizeBinary(16)
        for name in ["edge_uuid", "src_uuid", "dst_uuid"] {
            let f = s.field_with_name(name).unwrap();
            assert_eq!(
                f.data_type(),
                &DataType::FixedSizeBinary(16),
                "{name} should be FixedSizeBinary(16)"
            );
        }

        // Surrogate fields are UInt64
        for name in ["edge_id", "src_id", "dst_id"] {
            let f = s.field_with_name(name).unwrap();
            assert_eq!(f.data_type(), &DataType::UInt64, "{name} should be UInt64");
        }
    }

    #[test]
    fn exploratory_edge_schema_has_rel_type_name() {
        let s = &*EXPLORATORY_EDGE_SCHEMA;
        // Must be one field longer than TYPED_EDGE_SCHEMA
        assert_eq!(s.fields().len(), TYPED_EDGE_SCHEMA.fields().len() + 1);

        let last = s.fields().last().unwrap();
        assert_eq!(last.name(), "rel_type_name");
        assert_eq!(last.data_type(), &DataType::Utf8);
        assert!(!last.is_nullable());
    }

    #[test]
    fn exploratory_edge_schema_extends_typed_edge_schema() {
        // The first N fields of EXPLORATORY_EDGE_SCHEMA must match TYPED_EDGE_SCHEMA exactly.
        let typed = &*TYPED_EDGE_SCHEMA;
        let exploratory = &*EXPLORATORY_EDGE_SCHEMA;
        for (i, typed_field) in typed.fields().iter().enumerate() {
            assert_eq!(
                exploratory.field(i).as_ref(),
                typed_field.as_ref(),
                "field {i} mismatch between TYPED and EXPLORATORY schemas"
            );
        }
    }

    #[test]
    fn property_type_to_arrow_all_variants() {
        use graphforge_ontology::{SpatialCrs, SpatialGeometryType, SpatialType};
        assert_eq!(
            property_type_to_arrow(&PropertyValueType::Utf8),
            DataType::Utf8
        );
        assert_eq!(
            property_type_to_arrow(&PropertyValueType::Int64),
            DataType::Int64
        );
        assert_eq!(
            property_type_to_arrow(&PropertyValueType::Float64),
            DataType::Float64
        );
        assert_eq!(
            property_type_to_arrow(&PropertyValueType::Bool),
            DataType::Boolean
        );
        assert_eq!(
            property_type_to_arrow(&PropertyValueType::Duration),
            DataType::Struct(duration_struct_fields())
        );
        assert_eq!(
            property_type_to_arrow(&PropertyValueType::DateTime),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            property_type_to_arrow(&PropertyValueType::List),
            DataType::LargeUtf8
        );
        assert_eq!(
            property_type_to_arrow(&PropertyValueType::Map),
            DataType::LargeUtf8
        );
        let spatial = SpatialType {
            geometry: SpatialGeometryType::Point,
            crs: SpatialCrs::Epsg4326,
        };
        assert_eq!(
            property_type_to_arrow(&PropertyValueType::Spatial(spatial)),
            spatial.data_type()
        );
        let definition = PropertyDef {
            owner: "Place".into(),
            name: "location".into(),
            value_type: PropertyValueType::Spatial(spatial),
            nullable: false,
            multivalued: false,
            default_json: None,
        };
        assert_eq!(
            property_field(&definition),
            spatial.field("location", false)
        );
    }

    #[test]
    fn property_schema_roundtrip() {
        use graphforge_ontology::ontology::PropertyDef;
        let defs = vec![
            PropertyDef {
                owner: "Person".into(),
                name: "name".into(),
                value_type: PropertyValueType::Utf8,
                nullable: false,
                multivalued: false,
                default_json: None,
            },
            PropertyDef {
                owner: "Person".into(),
                name: "age".into(),
                value_type: PropertyValueType::Int64,
                nullable: true,
                multivalued: false,
                default_json: None,
            },
        ];
        let schema = property_schema("Person", &defs);

        // node_uuid + 2 property fields
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "node_uuid");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert!(!schema.field(1).is_nullable());
        assert_eq!(schema.field(2).name(), "age");
        assert_eq!(schema.field(2).data_type(), &DataType::Int64);
        assert!(schema.field(2).is_nullable());

        // Metadata
        assert_eq!(
            schema.metadata().get("graphforge.entity_type"),
            Some(&"Person".to_owned())
        );
    }

    #[test]
    fn result_schema_metadata() {
        let schema = result_schema(
            vec![Field::new("n_name", DataType::Utf8, true)],
            "qid-123",
            "sha256:abc",
            "0.1.0",
        );
        let meta = schema.metadata();
        assert_eq!(meta.get("graphforge.query_id"), Some(&"qid-123".to_owned()));
        assert_eq!(
            meta.get("graphforge.ontology_version"),
            Some(&"sha256:abc".to_owned())
        );
        assert_eq!(meta.get("graphforge.ir_version"), Some(&"0.1.0".to_owned()));
        assert_eq!(schema.fields().len(), 1);
    }

    #[test]
    fn adjacency_csr_schema_shape() {
        let s = &*ADJACENCY_CSR_SCHEMA;
        assert_eq!(s.fields().len(), 1);
        let f = s.field(0);
        assert_eq!(f.name(), "adjacency");
        assert!(!f.is_nullable());
        let DataType::LargeList(item) = f.data_type() else {
            panic!("adjacency should be a LargeList, got {:?}", f.data_type());
        };
        assert!(!item.is_nullable());
        let DataType::Struct(entry) = item.data_type() else {
            panic!("list item should be a Struct, got {:?}", item.data_type());
        };
        let names: Vec<&str> = entry.iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["edge_id", "neighbor_id"]);
        for f in entry {
            assert_eq!(f.data_type(), &DataType::UInt64);
            assert!(!f.is_nullable());
        }
    }

    #[test]
    fn adjacency_manifest_schema_field_names_and_types() {
        let s = &*ADJACENCY_MANIFEST_SCHEMA;
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            [
                "relation_type",
                "direction",
                "topology_generation",
                "built_at",
                "node_count",
                "edge_count"
            ]
        );
        for name in ["relation_type", "direction"] {
            let f = s.field_with_name(name).unwrap();
            assert_eq!(f.data_type(), &DataType::Utf8);
        }
        for name in ["topology_generation", "node_count", "edge_count"] {
            let f = s.field_with_name(name).unwrap();
            assert_eq!(f.data_type(), &DataType::UInt64);
        }
        assert_eq!(
            s.field_with_name("built_at").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert!(s.fields().iter().all(|f| !f.is_nullable()));
    }

    #[test]
    fn result_schema_allows_uuid_fields() {
        // UUID columns (*_uuid) are allowed; surrogate *_id columns are not.
        let schema = result_schema(
            vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("name", DataType::Utf8, true),
            ],
            "q1",
            "v1",
            "0.1.0",
        );
        assert_eq!(schema.fields().len(), 2);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "internal surrogate columns must not appear")]
    fn result_schema_rejects_surrogate_id_field() {
        // In debug builds, a stamped / UInt64 surrogate field must panic.
        let _ = result_schema(vec![id_field("node_id")], "q1", "v1", "0.1.0");
    }

    #[test]
    fn result_schema_allows_user_alias_named_node_id() {
        // #703: a non-surrogate projection aliased `node_id` is public data.
        let schema = result_schema(
            vec![Field::new("node_id", DataType::Int64, false)],
            "q1",
            "v1",
            "0.1.0",
        );
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "node_id");
    }

    #[test]
    fn id_field_stamps_internal_surrogate_marker() {
        let field = id_field("node_id");
        assert!(is_internal_surrogate_field(&field));
        assert_eq!(
            field.metadata().get(INTERNAL_SURROGATE_META_KEY),
            Some(&"true".to_owned())
        );
        let user_alias = Field::new("node_id", DataType::Int64, false);
        assert!(!is_internal_surrogate_field(&user_alias));
        // Metadata may survive DataFusion rename; aliased-away scan keys stay public.
        let stamped = id_field("node_id");
        let renamed = Field::new("id", stamped.data_type().clone(), stamped.is_nullable())
            .with_metadata(stamped.metadata().clone());
        assert!(!is_internal_surrogate_field(&renamed));
    }
}
