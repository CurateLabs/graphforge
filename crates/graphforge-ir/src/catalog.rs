//! [`RuntimeCatalog`] — auto-growing registry of observed entity types,
//! relation types, and property names for GraphForge's progressive ontology model.
//!
//! In `exploratory` and `advisory` modes the binder delegates unknown label/type
//! resolution here.  The catalog auto-assigns integer IDs and records observations
//! so that cross-session stability is achieved by persisting to
//! `topology/runtime_catalog.parquet`.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::SystemTime;

use arrow::array::{
    Array, ArrayRef, RecordBatch, StringArray, StringBuilder, TimestampMicrosecondArray,
    TimestampMicrosecondBuilder, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use serde::{Deserialize, Serialize};

use graphforge_core::{GfError, TypeId};

// ---------------------------------------------------------------------------
// Public newtypes
// ---------------------------------------------------------------------------

/// Identifies an entity or relation type discovered at runtime.
///
/// Runtime IDs are local to a session (or persisted in
/// `topology/runtime_catalog.parquet` for cross-session stability).  They are
/// distinct from ontology [`TypeId`](graphforge_core::TypeId)s, which are assigned by
/// the compiler from a formal ontology definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RuntimeTypeId(pub u32);

/// Plan-space tag for runtime-catalog entity labels (`TypeId` bit 30).
///
/// Distinct from [`RUNTIME_RELATION_TYPE_TAG`] so entity and relation runtime
/// encodings never share a bit, and both remain disjoint from ontology IDs
/// (which occupy the untagged low range).
pub const RUNTIME_ENTITY_TYPE_TAG: u32 = 1 << 30;

/// Plan-space tag for runtime-catalog relation types (`TypeId` bit 31).
pub const RUNTIME_RELATION_TYPE_TAG: u32 = 1 << 31;

/// Maximum untagged runtime catalog ID that can be encoded into plan space.
const RUNTIME_TYPE_ID_PLAN_LIMIT: u32 = RUNTIME_ENTITY_TYPE_TAG;

/// Encode one runtime-catalog entity label ID in the plan/storage TypeId space.
///
/// Ontology and runtime IDs both begin at zero. Tagging runtime entity IDs
/// keeps advisory/exploratory labels disjoint from ontology entity type IDs
/// while the bound plan and persisted topology cross the IR/storage boundary.
#[must_use]
pub fn runtime_entity_type_id(id: RuntimeTypeId) -> TypeId {
    assert!(
        id.0 < RUNTIME_TYPE_ID_PLAN_LIMIT,
        "runtime entity type ID exceeds the plan encoding range"
    );
    TypeId(id.0 | RUNTIME_ENTITY_TYPE_TAG)
}

/// Returns true when `id` carries the runtime-entity plan tag.
#[must_use]
pub fn is_runtime_entity_type_id(id: TypeId) -> bool {
    id.0 & RUNTIME_ENTITY_TYPE_TAG != 0 && id.0 & RUNTIME_RELATION_TYPE_TAG == 0
}

/// Strip the runtime-entity plan tag, returning the catalog-local ID.
#[must_use]
pub fn runtime_type_id_from_entity_plan_id(id: TypeId) -> Option<RuntimeTypeId> {
    is_runtime_entity_type_id(id).then_some(RuntimeTypeId(id.0 & !RUNTIME_ENTITY_TYPE_TAG))
}

/// Encode one runtime-catalog relation ID in the plan TypeId space.
///
/// Ontology and runtime IDs both begin at zero. Tagging runtime relation IDs
/// keeps advisory/exploratory misses disjoint from ontology relation IDs while
/// the bound plan crosses the IR/lowering boundary.
#[must_use]
pub fn runtime_relation_type_id(id: RuntimeTypeId) -> TypeId {
    assert!(
        id.0 < RUNTIME_TYPE_ID_PLAN_LIMIT,
        "runtime relation type ID exceeds the plan encoding range"
    );
    TypeId(id.0 | RUNTIME_RELATION_TYPE_TAG)
}

/// Identifies a property discovered at runtime (not formally declared in an ontology).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RuntimePropId(pub u32);

// ---------------------------------------------------------------------------
// Arrow schema
// ---------------------------------------------------------------------------

/// Arrow schema for `topology/runtime_catalog.parquet`.
pub static RUNTIME_CATALOG_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("entry_kind", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("runtime_id", DataType::UInt32, false),
        Field::new("observation_count", DataType::UInt64, false),
        Field::new(
            "first_seen",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "last_seen",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("owner_label", DataType::Utf8, true),
    ]))
});

// ---------------------------------------------------------------------------
// Private internals
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryKind {
    EntityType,
    RelationType,
    Property,
}

#[derive(Debug, Clone)]
struct CatalogEntry {
    kind: EntryKind,
    name: String,
    runtime_id: u32,
    observation_count: u64,
    /// Microseconds since Unix epoch (UTC).
    first_seen: i64,
    /// Microseconds since Unix epoch (UTC).
    last_seen: i64,
    /// For `Property` entries: the label this property was observed on.
    owner_label: Option<String>,
}

/// Returns current time as microseconds since Unix epoch (UTC).
fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
}

// ---------------------------------------------------------------------------
// RuntimeCatalog
// ---------------------------------------------------------------------------

/// Auto-growing registry of observed entity types, relation types, and property names.
///
/// In `exploratory` and `advisory` ontology modes, the binder delegates
/// unknown label/type/property resolution here rather than returning an error.
/// The catalog auto-assigns stable integer IDs and tracks observation statistics.
///
/// The catalog is not internally synchronised — callers that share it across
/// threads must wrap it in `Arc<RwLock<RuntimeCatalog>>` (wired in issue #583).
#[derive(Debug, Clone, Default)]
pub struct RuntimeCatalog {
    /// name → index into `entries`
    entity_types: HashMap<String, usize>,
    /// name → index into `entries`
    relation_types: HashMap<String, usize>,
    /// (name, owner_label) → index into `entries`
    properties: HashMap<(String, Option<String>), usize>,
    /// All entries in insertion order.
    entries: Vec<CatalogEntry>,
    /// Next ID to assign for entity types and relation types (shared space).
    next_type_id: u32,
    /// Next ID to assign for properties.
    next_prop_id: u32,
}

impl RuntimeCatalog {
    /// Creates an empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Interns an entity label and returns a stable [`RuntimeTypeId`].
    ///
    /// If `name` has been seen before the observation count is incremented and
    /// the existing ID is returned unchanged.
    pub fn intern_label(&mut self, name: &str) -> RuntimeTypeId {
        self.intern_label_at(name, now_micros())
    }

    /// Interns an entity label at a caller-authoritative timestamp.
    pub fn intern_label_at(&mut self, name: &str, now: i64) -> RuntimeTypeId {
        if let Some(&idx) = self.entity_types.get(name) {
            let entry = &mut self.entries[idx];
            entry.observation_count += 1;
            entry.last_seen = now;
            return RuntimeTypeId(entry.runtime_id);
        }
        let id = self.next_type_id;
        self.next_type_id += 1;
        let idx = self.entries.len();
        self.entries.push(CatalogEntry {
            kind: EntryKind::EntityType,
            name: name.to_owned(),
            runtime_id: id,
            observation_count: 1,
            first_seen: now,
            last_seen: now,
            owner_label: None,
        });
        self.entity_types.insert(name.to_owned(), idx);
        RuntimeTypeId(id)
    }

    /// Interns a relation type name and returns a stable [`RuntimeTypeId`].
    pub fn intern_relation_type(&mut self, name: &str) -> RuntimeTypeId {
        self.intern_relation_type_at(name, now_micros())
    }

    /// Interns a relation type at a caller-authoritative timestamp.
    pub fn intern_relation_type_at(&mut self, name: &str, now: i64) -> RuntimeTypeId {
        if let Some(&idx) = self.relation_types.get(name) {
            let entry = &mut self.entries[idx];
            entry.observation_count += 1;
            entry.last_seen = now;
            return RuntimeTypeId(entry.runtime_id);
        }
        let id = self.next_type_id;
        self.next_type_id += 1;
        let idx = self.entries.len();
        self.entries.push(CatalogEntry {
            kind: EntryKind::RelationType,
            name: name.to_owned(),
            runtime_id: id,
            observation_count: 1,
            first_seen: now,
            last_seen: now,
            owner_label: None,
        });
        self.relation_types.insert(name.to_owned(), idx);
        RuntimeTypeId(id)
    }

    /// Interns a property name and returns a stable [`RuntimePropId`].
    ///
    /// Properties are keyed by `(name, owner_label)` so the same property name
    /// observed on different labels gets independent IDs.
    pub fn intern_property(&mut self, name: &str, owner_label: Option<&str>) -> RuntimePropId {
        self.intern_property_at(name, owner_label, now_micros())
    }

    /// Interns a property at a caller-authoritative timestamp.
    pub fn intern_property_at(
        &mut self,
        name: &str,
        owner_label: Option<&str>,
        now: i64,
    ) -> RuntimePropId {
        let key = (name.to_owned(), owner_label.map(str::to_owned));
        if let Some(&idx) = self.properties.get(&key) {
            let entry = &mut self.entries[idx];
            entry.observation_count += 1;
            entry.last_seen = now;
            return RuntimePropId(entry.runtime_id);
        }
        let id = self.next_prop_id;
        self.next_prop_id += 1;
        let idx = self.entries.len();
        self.entries.push(CatalogEntry {
            kind: EntryKind::Property,
            name: name.to_owned(),
            runtime_id: id,
            observation_count: 1,
            first_seen: now,
            last_seen: now,
            owner_label: owner_label.map(str::to_owned),
        });
        self.properties.insert(key, idx);
        RuntimePropId(id)
    }

    /// Returns `true` if `name` has been interned as an entity type.
    #[must_use]
    pub fn contains_entity_type(&self, name: &str) -> bool {
        self.entity_types.contains_key(name)
    }

    /// Returns all interned entity type names (order unspecified).
    #[must_use]
    pub fn entity_types(&self) -> Vec<&str> {
        self.entity_types.keys().map(String::as_str).collect()
    }

    /// Returns all interned relation type names (order unspecified).
    #[must_use]
    pub fn relation_types(&self) -> Vec<&str> {
        self.relation_types.keys().map(String::as_str).collect()
    }

    /// Returns all property names observed on `label` (order unspecified).
    #[must_use]
    pub fn properties_for(&self, label: &str) -> Vec<&str> {
        self.properties
            .iter()
            .filter(|((_, owner), _)| owner.as_deref() == Some(label))
            .map(|((name, _), _)| name.as_str())
            .collect()
    }

    /// Resolves a [`RuntimePropId`] back to the property name it was interned
    /// under, or `None` if no property entry carries that ID.
    ///
    /// Used by the relational lowering layer to turn a numeric `PropertyAccess`
    /// ID back into the real column name when reading exploratory property
    /// tables.
    #[must_use]
    pub fn property_name(&self, id: RuntimePropId) -> Option<&str> {
        self.entries
            .iter()
            .find(|e| e.kind == EntryKind::Property && e.runtime_id == id.0)
            .map(|e| e.name.as_str())
    }

    /// Returns `(RuntimePropId, name)` for every interned property (order
    /// unspecified). Convenient for building a `PropId → name` map in one pass.
    pub fn property_names(&self) -> impl Iterator<Item = (RuntimePropId, &str)> + '_ {
        self.entries
            .iter()
            .filter(|e| e.kind == EntryKind::Property)
            .map(|e| (RuntimePropId(e.runtime_id), e.name.as_str()))
    }

    /// Resolves a relation-type [`RuntimeTypeId`] back to the name it was
    /// interned under, or `None` if no relation-type entry carries that ID.
    ///
    /// Used by the relational lowering layer to resolve a `TypedEdgeScan`'s
    /// relation name when reading exploratory edge tables (no ontology present).
    #[must_use]
    pub fn relation_type_name(&self, id: RuntimeTypeId) -> Option<&str> {
        self.entries
            .iter()
            .find(|e| e.kind == EntryKind::RelationType && e.runtime_id == id.0)
            .map(|e| e.name.as_str())
    }

    /// Returns `(RuntimeTypeId, name)` for every interned relation type (order
    /// unspecified). Convenient for building a `TypeId → relation-name` map.
    pub fn relation_type_names_with_ids(&self) -> impl Iterator<Item = (RuntimeTypeId, &str)> + '_ {
        self.entries
            .iter()
            .filter(|e| e.kind == EntryKind::RelationType)
            .map(|e| (RuntimeTypeId(e.runtime_id), e.name.as_str()))
    }

    /// Resolves an entity-type (node label) [`RuntimeTypeId`] back to the name
    /// it was interned under, or `None` if no entity-type entry carries that ID.
    ///
    /// Mirror of [`relation_type_name`](Self::relation_type_name) for labels —
    /// used to render a real label name for an unlabelled `MATCH (n) RETURN n`
    /// in exploratory mode, where the ontology map is empty (#889).
    #[must_use]
    pub fn entity_type_name(&self, id: RuntimeTypeId) -> Option<&str> {
        self.entries
            .iter()
            .find(|e| e.kind == EntryKind::EntityType && e.runtime_id == id.0)
            .map(|e| e.name.as_str())
    }

    /// Returns `(RuntimeTypeId, name)` for every interned entity type (node
    /// label), order unspecified. Convenient for building a
    /// `TypeId → label-name` map.
    pub fn entity_type_names_with_ids(&self) -> impl Iterator<Item = (RuntimeTypeId, &str)> + '_ {
        self.entries
            .iter()
            .filter(|e| e.kind == EntryKind::EntityType)
            .map(|e| (RuntimeTypeId(e.runtime_id), e.name.as_str()))
    }

    /// Serialises the catalog to an Arrow [`RecordBatch`] using [`RUNTIME_CATALOG_SCHEMA`].
    ///
    /// The resulting batch can be written to `topology/runtime_catalog.parquet`
    /// and later restored via [`from_record_batch`](Self::from_record_batch).
    #[must_use]
    pub fn to_record_batch(&self) -> RecordBatch {
        let n = self.entries.len();
        let mut kind_b = StringBuilder::with_capacity(n, n * 12);
        let mut name_b = StringBuilder::with_capacity(n, n * 32);
        let mut id_b = UInt32Builder::with_capacity(n);
        let mut count_b = UInt64Builder::with_capacity(n);
        let mut first_b = TimestampMicrosecondBuilder::with_capacity(n);
        let mut last_b = TimestampMicrosecondBuilder::with_capacity(n);
        let mut owner_b = StringBuilder::with_capacity(n, n * 16);

        for entry in &self.entries {
            kind_b.append_value(match entry.kind {
                EntryKind::EntityType => "entity_type",
                EntryKind::RelationType => "relation_type",
                EntryKind::Property => "property",
            });
            name_b.append_value(&entry.name);
            id_b.append_value(entry.runtime_id);
            count_b.append_value(entry.observation_count);
            first_b.append_value(entry.first_seen);
            last_b.append_value(entry.last_seen);
            match &entry.owner_label {
                Some(label) => owner_b.append_value(label),
                None => owner_b.append_null(),
            }
        }

        let first_arr = first_b.finish().with_timezone_opt(Some(Arc::from("UTC")));
        let last_arr = last_b.finish().with_timezone_opt(Some(Arc::from("UTC")));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(kind_b.finish()),
            Arc::new(name_b.finish()),
            Arc::new(id_b.finish()),
            Arc::new(count_b.finish()),
            Arc::new(first_arr),
            Arc::new(last_arr),
            Arc::new(owner_b.finish()),
        ];

        RecordBatch::try_new(RUNTIME_CATALOG_SCHEMA.clone(), columns)
            .expect("schema and array lengths must be consistent")
    }

    /// Restores a `RuntimeCatalog` from an Arrow [`RecordBatch`] previously
    /// produced by [`to_record_batch`](Self::to_record_batch).
    ///
    /// # Errors
    /// Returns [`GfError::Storage`] if any column has the wrong type or an
    /// unknown `entry_kind` value is encountered.
    pub fn from_record_batch(batch: &RecordBatch) -> Result<Self, GfError> {
        Self::from_record_batches(std::iter::once(batch))
    }

    /// Restores a catalog from a bounded stream of persisted Arrow batches
    /// without concatenating or retaining those batches.
    #[allow(clippy::too_many_lines)] // One persisted schema decoder; splitting obscures row authority.
    pub fn from_record_batches<'a>(
        batches: impl IntoIterator<Item = &'a RecordBatch>,
    ) -> Result<Self, GfError> {
        let storage_err = |msg: &str| GfError::Storage(msg.to_owned());
        let mut catalog = Self::new();
        let mut max_type_id: u32 = 0;
        let mut max_prop_id: u32 = 0;
        for batch in batches {
            let kinds = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| storage_err("runtime_catalog col 0 (entry_kind) not Utf8"))?;
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| storage_err("runtime_catalog col 1 (name) not Utf8"))?;
            let ids = batch
                .column(2)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| storage_err("runtime_catalog col 2 (runtime_id) not UInt32"))?;
            let counts = batch
                .column(3)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    storage_err("runtime_catalog col 3 (observation_count) not UInt64")
                })?;
            let first_seens = batch
                .column(4)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    storage_err("runtime_catalog col 4 (first_seen) not TimestampMicrosecond")
                })?;
            let last_seens = batch
                .column(5)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    storage_err("runtime_catalog col 5 (last_seen) not TimestampMicrosecond")
                })?;
            let owners = batch
                .column(6)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| storage_err("runtime_catalog col 6 (owner_label) not Utf8"))?;

            for row in 0..batch.num_rows() {
                let kind = match kinds.value(row) {
                    "entity_type" => EntryKind::EntityType,
                    "relation_type" => EntryKind::RelationType,
                    "property" => EntryKind::Property,
                    other => {
                        return Err(GfError::Storage(format!(
                            "runtime_catalog: unknown entry_kind '{other}'"
                        )));
                    }
                };
                let name = names.value(row).to_owned();
                let runtime_id = ids.value(row);
                let observation_count = counts.value(row);
                let first_seen = first_seens.value(row);
                let last_seen = last_seens.value(row);
                let owner_label = if owners.is_null(row) {
                    None
                } else {
                    Some(owners.value(row).to_owned())
                };

                let idx = catalog.entries.len();
                catalog.entries.push(CatalogEntry {
                    kind,
                    name: name.clone(),
                    runtime_id,
                    observation_count,
                    first_seen,
                    last_seen,
                    owner_label: owner_label.clone(),
                });

                match kind {
                    EntryKind::EntityType => {
                        if catalog.entity_types.insert(name, idx).is_some() {
                            return Err(storage_err(
                                "runtime_catalog contains duplicate entity type",
                            ));
                        }
                        max_type_id = max_type_id.max(runtime_id + 1);
                    }
                    EntryKind::RelationType => {
                        if catalog.relation_types.insert(name, idx).is_some() {
                            return Err(storage_err(
                                "runtime_catalog contains duplicate relation type",
                            ));
                        }
                        max_type_id = max_type_id.max(runtime_id + 1);
                    }
                    EntryKind::Property => {
                        if catalog
                            .properties
                            .insert((name, owner_label), idx)
                            .is_some()
                        {
                            return Err(storage_err("runtime_catalog contains duplicate property"));
                        }
                        max_prop_id = max_prop_id.max(runtime_id + 1);
                    }
                }
            }
        }

        catalog.next_type_id = max_type_id;
        catalog.next_prop_id = max_prop_id;
        Ok(catalog)
    }

    /// Appends one persisted catalog batch while preserving its stable IDs and
    /// observations. The caller may therefore decode a Parquet catalog in
    /// bounded windows rather than concatenating it in memory.
    pub fn extend_from_record_batch(&mut self, batch: &RecordBatch) -> Result<(), GfError> {
        let incoming = Self::from_record_batch(batch)?;
        for entry in &incoming.entries {
            let duplicate = match entry.kind {
                EntryKind::EntityType => self.entity_types.contains_key(&entry.name),
                EntryKind::RelationType => self.relation_types.contains_key(&entry.name),
                EntryKind::Property => self
                    .properties
                    .contains_key(&(entry.name.clone(), entry.owner_label.clone())),
            };
            if duplicate {
                return Err(GfError::Storage(
                    "runtime_catalog contains a duplicate persisted entry".to_owned(),
                ));
            }
            entry.runtime_id.checked_add(1).ok_or_else(|| {
                GfError::Storage("runtime_catalog persisted ID overflow".to_owned())
            })?;
        }
        for entry in incoming.entries {
            let idx = self.entries.len();
            match entry.kind {
                EntryKind::EntityType => {
                    self.entity_types.insert(entry.name.clone(), idx);
                    self.next_type_id = self.next_type_id.max(entry.runtime_id + 1);
                }
                EntryKind::RelationType => {
                    self.relation_types.insert(entry.name.clone(), idx);
                    self.next_type_id = self.next_type_id.max(entry.runtime_id + 1);
                }
                EntryKind::Property => {
                    let key = (entry.name.clone(), entry.owner_label.clone());
                    self.properties.insert(key, idx);
                    self.next_prop_id = self.next_prop_id.max(entry.runtime_id + 1);
                }
            }
            self.entries.push(entry);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn make_catalog() -> RuntimeCatalog {
        let mut cat = RuntimeCatalog::new();
        cat.intern_label("Person");
        cat.intern_label("Company");
        cat.intern_relation_type("KNOWS");
        cat.intern_property("name", Some("Person"));
        cat.intern_property("founded", Some("Company"));
        cat
    }

    #[test]
    fn runtime_entity_type_id_tags_disjoint_from_ontology_zero() {
        let tagged = runtime_entity_type_id(RuntimeTypeId(0));
        assert_ne!(tagged, TypeId(0));
        assert!(is_runtime_entity_type_id(tagged));
        assert_eq!(
            runtime_type_id_from_entity_plan_id(tagged),
            Some(RuntimeTypeId(0))
        );
        assert!(!is_runtime_entity_type_id(TypeId(0)));
        assert_ne!(
            runtime_entity_type_id(RuntimeTypeId(0)),
            runtime_relation_type_id(RuntimeTypeId(0))
        );
    }

    #[test]
    fn intern_label_same_id_for_same_name() {
        let mut cat = RuntimeCatalog::new();
        let id1 = cat.intern_label("Person");
        let id2 = cat.intern_label("Person");
        assert_eq!(id1, id2);
    }

    #[test]
    fn caller_authoritative_timestamp_is_preserved_for_every_catalog_kind() {
        let mut catalog = RuntimeCatalog::new();
        catalog.intern_label_at("Person", 42);
        catalog.intern_relation_type_at("KNOWS", 42);
        catalog.intern_property_at("score", Some("Person"), 42);
        let batch = catalog.to_record_batch();
        let first = batch
            .column(4)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let last = batch
            .column(5)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(first.values(), &[42, 42, 42]);
        assert_eq!(last.values(), &[42, 42, 42]);
    }

    #[test]
    fn property_name_reverse_lookup() {
        let mut cat = RuntimeCatalog::new();
        let name_id = cat.intern_property("name", Some("Person"));
        let founded_id = cat.intern_property("founded", Some("Company"));
        assert_eq!(cat.property_name(name_id), Some("name"));
        assert_eq!(cat.property_name(founded_id), Some("founded"));
        // An unknown ID resolves to None.
        assert_eq!(cat.property_name(RuntimePropId(9999)), None);
    }

    #[test]
    fn property_names_lists_all_properties() {
        let cat = make_catalog();
        let names: HashSet<&str> = cat.property_names().map(|(_, n)| n).collect();
        assert_eq!(names, HashSet::from(["name", "founded"]));
    }

    #[test]
    fn intern_label_distinct_ids_for_distinct_names() {
        let mut cat = RuntimeCatalog::new();
        let person = cat.intern_label("Person");
        let company = cat.intern_label("Company");
        assert_ne!(person, company);
    }

    #[test]
    fn intern_relation_type_same_id() {
        let mut cat = RuntimeCatalog::new();
        let id1 = cat.intern_relation_type("KNOWS");
        let id2 = cat.intern_relation_type("KNOWS");
        assert_eq!(id1, id2);
    }

    #[test]
    fn intern_relation_type_distinct_from_entity_type() {
        // Entity "Person" (id=0) and relation "KNOWS" (id=1) share the type ID
        // counter, so they get different IDs.
        let mut cat = RuntimeCatalog::new();
        let person_id = cat.intern_label("Person");
        let knows_id = cat.intern_relation_type("KNOWS");
        assert_ne!(person_id, knows_id);
    }

    #[test]
    fn type_id_and_prop_id_are_independent() {
        let mut cat = RuntimeCatalog::new();
        // First entity type gets type ID 0.
        let type_id = cat.intern_label("Person");
        // First property gets prop ID 0 — independent counter.
        let prop_id = cat.intern_property("name", Some("Person"));
        assert_eq!(type_id.0, 0);
        assert_eq!(prop_id.0, 0);
    }

    #[test]
    fn contains_entity_type() {
        let mut cat = RuntimeCatalog::new();
        cat.intern_label("Person");
        assert!(cat.contains_entity_type("Person"));
        assert!(!cat.contains_entity_type("Unknown"));
    }

    #[test]
    fn entity_types_returns_all_interned() {
        let cat = make_catalog();
        let types: HashSet<&str> = cat.entity_types().into_iter().collect();
        assert!(types.contains("Person"));
        assert!(types.contains("Company"));
        assert_eq!(types.len(), 2);
    }

    #[test]
    fn properties_for_returns_correct_props() {
        let cat = make_catalog();
        let props: HashSet<&str> = cat.properties_for("Person").into_iter().collect();
        assert!(props.contains("name"));
        assert!(!props.contains("founded"));
    }

    #[test]
    fn observation_count_increments() {
        let mut cat = RuntimeCatalog::new();
        cat.intern_label("Person");
        cat.intern_label("Person");
        cat.intern_label("Person");
        let batch = cat.to_record_batch();
        let counts = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(counts.value(0), 3);
    }

    #[test]
    fn empty_catalog_to_record_batch_has_correct_schema() {
        let cat = RuntimeCatalog::new();
        let batch = cat.to_record_batch();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), *RUNTIME_CATALOG_SCHEMA);
    }

    #[test]
    fn roundtrip_to_from_record_batch() {
        let mut cat = make_catalog();
        // Intern "Person" twice more so observation_count = 3.
        cat.intern_label("Person");
        cat.intern_label("Person");

        let batch = cat.to_record_batch();
        let restored = RuntimeCatalog::from_record_batch(&batch).unwrap();

        // Entity types and relation types preserved.
        let et: HashSet<&str> = restored.entity_types().into_iter().collect();
        assert!(et.contains("Person"));
        assert!(et.contains("Company"));

        let rt: HashSet<&str> = restored.relation_types().into_iter().collect();
        assert!(rt.contains("KNOWS"));

        // Properties preserved with owner.
        let pp: HashSet<&str> = restored.properties_for("Person").into_iter().collect();
        assert!(pp.contains("name"));

        // IDs are preserved (stable after round-trip).
        let person_id_orig = cat.intern_label("Person");
        let person_id_rest = {
            let mut r = restored.clone();
            r.intern_label("Person")
        };
        assert_eq!(person_id_orig, person_id_rest);

        // Observation count for "Person" should be 3 (original 1 + 2 extra interns).
        let restored_batch = restored.to_record_batch();
        let kinds = restored_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = restored_batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let person_row = (0..restored_batch.num_rows())
            .find(|&r| kinds.value(r) == "entity_type")
            .unwrap();
        assert_eq!(counts.value(person_row), 3);
    }

    #[test]
    fn roundtrip_empty_catalog() {
        let cat = RuntimeCatalog::new();
        let batch = cat.to_record_batch();
        let restored = RuntimeCatalog::from_record_batch(&batch).unwrap();
        assert_eq!(restored.entity_types().len(), 0);
        assert_eq!(restored.relation_types().len(), 0);
    }

    #[test]
    fn from_record_batch_unknown_entry_kind_returns_error() {
        // Build a single-row catalog with a valid entry first, then manually
        // produce a batch via to_record_batch and overwrite the entry_kind column
        // with an invalid value.  We do this by constructing a fresh batch from
        // scratch using the schema-aware builders.
        let mut cat = RuntimeCatalog::new();
        cat.intern_label("X");
        let good_batch = cat.to_record_batch();
        assert_eq!(good_batch.num_rows(), 1);

        // Replace the entry_kind column with an invalid value while keeping all
        // other columns (and their types) intact.
        let bad_kinds = Arc::new(StringArray::from(vec!["bogus_kind"])) as ArrayRef;
        let mut cols: Vec<ArrayRef> = good_batch.columns().to_vec();
        cols[0] = bad_kinds;

        let batch = RecordBatch::try_new(RUNTIME_CATALOG_SCHEMA.clone(), cols).unwrap();
        let result = RuntimeCatalog::from_record_batch(&batch);
        assert!(
            matches!(result, Err(GfError::Storage(_))),
            "expected Storage error for unknown entry_kind"
        );
    }
}
