//! Persisted runtime catalog schema, checked identities, and atomic batch merging.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, ArrayRef, RecordBatch, StringArray, StringBuilder, TimestampMicrosecondArray,
    TimestampMicrosecondBuilder, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::{RuntimeEntityId, RuntimePropId, RuntimeRelationId};
use graphforge_core::GfError;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CatalogIdentity {
    Entity(RuntimeEntityId),
    Relation(RuntimeRelationId),
    Property(RuntimePropId),
}

impl CatalogIdentity {
    fn checked(kind: EntryKind, raw: u32) -> Result<Self, GfError> {
        match kind {
            EntryKind::EntityType => RuntimeEntityId::new(raw).map(Self::Entity),
            EntryKind::RelationType => RuntimeRelationId::new(raw).map(Self::Relation),
            EntryKind::Property => RuntimePropId::new(raw).map(Self::Property),
        }
        .map_err(|error| GfError::Storage(format!("runtime_catalog invalid identity: {error}")))
    }

    fn kind(self) -> EntryKind {
        match self {
            Self::Entity(_) => EntryKind::EntityType,
            Self::Relation(_) => EntryKind::RelationType,
            Self::Property(_) => EntryKind::Property,
        }
    }

    fn get(self) -> u32 {
        match self {
            Self::Entity(id) => id.get(),
            Self::Relation(id) => id.get(),
            Self::Property(id) => id.get(),
        }
    }
}

#[derive(Debug, Clone)]
struct CatalogEntry {
    identity: CatalogIdentity,
    name: String,
    observation_count: u64,
    /// Microseconds since Unix epoch (UTC).
    first_seen: i64,
    /// Microseconds since Unix epoch (UTC).
    last_seen: i64,
    /// For `Property` entries: the label this property was observed on.
    owner_label: Option<String>,
}

// ---------------------------------------------------------------------------
// RuntimeCatalogData
// ---------------------------------------------------------------------------

/// Checked runtime catalog carrier and its encoding-preserving Arrow codec.
///
/// The caller chooses observation timestamps and whether a name may be interned.
/// Entries preserve their stable local identity and insertion order across batches.
#[derive(Debug, Clone, Default)]
pub struct RuntimeCatalogData {
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

impl RuntimeCatalogData {
    /// Creates an empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of retained catalog entries.
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Exact UTF-8 bytes retained by entry names and property owners.
    #[must_use]
    pub fn retained_identifier_bytes(&self) -> usize {
        self.entries.iter().fold(0_usize, |bytes, entry| {
            bytes
                .saturating_add(entry.name.len())
                .saturating_add(entry.owner_label.as_ref().map_or(0, String::len))
        })
    }

    /// Interns an entity label at a caller-authoritative timestamp.
    pub fn intern_label_at(&mut self, name: &str, now: i64) -> Result<RuntimeEntityId, GfError> {
        if let Some(&idx) = self.entity_types.get(name) {
            let entry = &mut self.entries[idx];
            entry.observation_count = entry.observation_count.checked_add(1).ok_or_else(|| {
                GfError::Storage("runtime_catalog observation count overflow".to_owned())
            })?;
            entry.last_seen = now;
            let CatalogIdentity::Entity(id) = entry.identity else {
                unreachable!("catalog index matches identity kind")
            };
            return Ok(id);
        }
        let id = RuntimeEntityId::new(self.next_type_id).map_err(|error| {
            GfError::Storage(format!("runtime_catalog exhausted ID range: {error}"))
        })?;
        self.next_type_id += 1;
        let idx = self.entries.len();
        self.entries.push(CatalogEntry {
            identity: CatalogIdentity::Entity(id),
            name: name.to_owned(),
            observation_count: 1,
            first_seen: now,
            last_seen: now,
            owner_label: None,
        });
        self.entity_types.insert(name.to_owned(), idx);
        Ok(id)
    }

    /// Interns a relation type at a caller-authoritative timestamp.
    pub fn intern_relation_type_at(
        &mut self,
        name: &str,
        now: i64,
    ) -> Result<RuntimeRelationId, GfError> {
        if let Some(&idx) = self.relation_types.get(name) {
            let entry = &mut self.entries[idx];
            entry.observation_count = entry.observation_count.checked_add(1).ok_or_else(|| {
                GfError::Storage("runtime_catalog observation count overflow".to_owned())
            })?;
            entry.last_seen = now;
            let CatalogIdentity::Relation(id) = entry.identity else {
                unreachable!("catalog index matches identity kind")
            };
            return Ok(id);
        }
        let id = RuntimeRelationId::new(self.next_type_id).map_err(|error| {
            GfError::Storage(format!("runtime_catalog exhausted ID range: {error}"))
        })?;
        self.next_type_id += 1;
        let idx = self.entries.len();
        self.entries.push(CatalogEntry {
            identity: CatalogIdentity::Relation(id),
            name: name.to_owned(),
            observation_count: 1,
            first_seen: now,
            last_seen: now,
            owner_label: None,
        });
        self.relation_types.insert(name.to_owned(), idx);
        Ok(id)
    }

    /// Interns a property at a caller-authoritative timestamp.
    pub fn intern_property_at(
        &mut self,
        name: &str,
        owner_label: Option<&str>,
        now: i64,
    ) -> Result<RuntimePropId, GfError> {
        let key = (name.to_owned(), owner_label.map(str::to_owned));
        if let Some(&idx) = self.properties.get(&key) {
            let entry = &mut self.entries[idx];
            entry.observation_count = entry.observation_count.checked_add(1).ok_or_else(|| {
                GfError::Storage("runtime_catalog observation count overflow".to_owned())
            })?;
            entry.last_seen = now;
            let CatalogIdentity::Property(id) = entry.identity else {
                unreachable!("catalog index matches identity kind")
            };
            return Ok(id);
        }
        let id = RuntimePropId::new(self.next_prop_id).map_err(|error| {
            GfError::Storage(format!("runtime_catalog exhausted ID range: {error}"))
        })?;
        self.next_prop_id += 1;
        let idx = self.entries.len();
        self.entries.push(CatalogEntry {
            identity: CatalogIdentity::Property(id),
            name: name.to_owned(),
            observation_count: 1,
            first_seen: now,
            last_seen: now,
            owner_label: owner_label.map(str::to_owned),
        });
        self.properties.insert(key, idx);
        Ok(id)
    }

    /// Returns `true` if `name` has been interned as an entity type.
    #[must_use]
    pub fn contains_entity_type(&self, name: &str) -> bool {
        self.entity_types.contains_key(name)
    }

    /// Returns `true` if `name` has been interned as a relation type.
    #[must_use]
    pub fn contains_relation_type(&self, name: &str) -> bool {
        self.relation_types.contains_key(name)
    }

    /// Returns `true` if the owner-scoped property has been interned.
    #[must_use]
    pub fn contains_property(&self, name: &str, owner_label: Option<&str>) -> bool {
        self.properties
            .contains_key(&(name.to_owned(), owner_label.map(str::to_owned)))
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
            .find(|e| e.identity.kind() == EntryKind::Property && e.identity.get() == id.get())
            .map(|e| e.name.as_str())
    }

    /// Returns `(RuntimePropId, name)` for every interned property (order
    /// unspecified). Convenient for building a `PropId → name` map in one pass.
    pub fn property_names(&self) -> impl Iterator<Item = (RuntimePropId, &str)> + '_ {
        self.entries.iter().filter_map(|e| match e.identity {
            CatalogIdentity::Property(id) => Some((id, e.name.as_str())),
            _ => None,
        })
    }

    /// Resolves a relation-type [`RuntimeRelationId`] back to the name it was
    /// interned under, or `None` if no relation-type entry carries that ID.
    ///
    /// Used by the relational lowering layer to resolve a `TypedEdgeScan`'s
    /// relation name when reading exploratory edge tables (no ontology present).
    #[must_use]
    pub fn relation_type_name(&self, id: RuntimeRelationId) -> Option<&str> {
        self.entries
            .iter()
            .find(|e| e.identity.kind() == EntryKind::RelationType && e.identity.get() == id.get())
            .map(|e| e.name.as_str())
    }

    /// Returns `(RuntimeRelationId, name)` for every interned relation type (order
    /// unspecified). Convenient for building a `TypeId → relation-name` map.
    pub fn relation_type_names_with_ids(
        &self,
    ) -> impl Iterator<Item = (RuntimeRelationId, &str)> + '_ {
        self.entries.iter().filter_map(|e| match e.identity {
            CatalogIdentity::Relation(id) => Some((id, e.name.as_str())),
            _ => None,
        })
    }

    /// Resolves an entity-type (node label) [`RuntimeEntityId`] back to the name
    /// it was interned under, or `None` if no entity-type entry carries that ID.
    ///
    /// Mirror of [`relation_type_name`](Self::relation_type_name) for labels —
    /// used to render a real label name for an unlabelled `MATCH (n) RETURN n`
    /// in exploratory mode, where the ontology map is empty (#889).
    #[must_use]
    pub fn entity_type_name(&self, id: RuntimeEntityId) -> Option<&str> {
        self.entries
            .iter()
            .find(|e| e.identity.kind() == EntryKind::EntityType && e.identity.get() == id.get())
            .map(|e| e.name.as_str())
    }

    /// Returns `(RuntimeEntityId, name)` for every interned entity type (node
    /// label), order unspecified. Convenient for building a
    /// `TypeId → label-name` map.
    pub fn entity_type_names_with_ids(&self) -> impl Iterator<Item = (RuntimeEntityId, &str)> + '_ {
        self.entries.iter().filter_map(|e| match e.identity {
            CatalogIdentity::Entity(id) => Some((id, e.name.as_str())),
            _ => None,
        })
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
            kind_b.append_value(match entry.identity.kind() {
                EntryKind::EntityType => "entity_type",
                EntryKind::RelationType => "relation_type",
                EntryKind::Property => "property",
            });
            name_b.append_value(&entry.name);
            id_b.append_value(entry.identity.get());
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

    /// Restores a `RuntimeCatalogData` from an Arrow [`RecordBatch`] previously
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
        Self::decode_record_batches_at(batches, 0, 0)
    }

    #[allow(clippy::too_many_lines)]
    fn decode_record_batches_at<'a>(
        batches: impl IntoIterator<Item = &'a RecordBatch>,
        initial_type_id: u32,
        initial_prop_id: u32,
    ) -> Result<Self, GfError> {
        let storage_err = |msg: &str| GfError::Storage(msg.to_owned());
        let mut catalog = Self::new();
        let mut next_type_id = initial_type_id;
        let mut next_prop_id = initial_prop_id;
        for batch in batches {
            if batch.schema().as_ref() != RUNTIME_CATALOG_SCHEMA.as_ref() {
                return Err(storage_err("runtime_catalog schema is not canonical"));
            }
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

            if kinds.null_count() != 0
                || names.null_count() != 0
                || ids.null_count() != 0
                || counts.null_count() != 0
                || first_seens.null_count() != 0
                || last_seens.null_count() != 0
            {
                return Err(storage_err("runtime_catalog required column contains null"));
            }

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

                if name.is_empty()
                    || observation_count == 0
                    || first_seen > last_seen
                    || (kind != EntryKind::Property && owner_label.is_some())
                {
                    return Err(storage_err("runtime_catalog row is not canonical"));
                }
                let expected_id = match kind {
                    EntryKind::EntityType | EntryKind::RelationType => &mut next_type_id,
                    EntryKind::Property => &mut next_prop_id,
                };
                if runtime_id != *expected_id {
                    return Err(storage_err(
                        "runtime_catalog IDs are not unique and contiguous in insertion order",
                    ));
                }
                *expected_id = expected_id.checked_add(1).ok_or_else(|| {
                    storage_err("runtime_catalog persisted ID exceeds supported range")
                })?;

                let idx = catalog.entries.len();
                catalog.entries.push(CatalogEntry {
                    identity: CatalogIdentity::checked(kind, runtime_id)?,
                    name: name.clone(),
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
                    }
                    EntryKind::RelationType => {
                        if catalog.relation_types.insert(name, idx).is_some() {
                            return Err(storage_err(
                                "runtime_catalog contains duplicate relation type",
                            ));
                        }
                    }
                    EntryKind::Property => {
                        if catalog
                            .properties
                            .insert((name, owner_label), idx)
                            .is_some()
                        {
                            return Err(storage_err("runtime_catalog contains duplicate property"));
                        }
                    }
                }
            }
        }

        catalog.next_type_id = next_type_id;
        catalog.next_prop_id = next_prop_id;
        Ok(catalog)
    }

    /// Appends one persisted catalog batch while preserving its stable IDs and
    /// observations. The caller may therefore decode a Parquet catalog in
    /// bounded windows rather than concatenating it in memory.
    pub fn extend_from_record_batch(&mut self, batch: &RecordBatch) -> Result<(), GfError> {
        let incoming = Self::decode_record_batches_at(
            std::iter::once(batch),
            self.next_type_id,
            self.next_prop_id,
        )?;
        for entry in &incoming.entries {
            let duplicate = match entry.identity.kind() {
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
        }
        self.next_type_id = incoming.next_type_id;
        self.next_prop_id = incoming.next_prop_id;
        for entry in incoming.entries {
            let idx = self.entries.len();
            match entry.identity.kind() {
                EntryKind::EntityType => {
                    self.entity_types.insert(entry.name.clone(), idx);
                }
                EntryKind::RelationType => {
                    self.relation_types.insert(entry.name.clone(), idx);
                }
                EntryKind::Property => {
                    let key = (entry.name.clone(), entry.owner_label.clone());
                    self.properties.insert(key, idx);
                }
            }
            self.entries.push(entry);
        }
        Ok(())
    }
}
