//! Runtime observation policy over the compiler-independent catalog carrier.

use std::ops::{Deref, DerefMut};
use std::time::SystemTime;

use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
pub use graphforge_value::{
    RUNTIME_CATALOG_SCHEMA, RuntimeCatalogData, RuntimeEntityId, RuntimePropId, RuntimeRelationId,
};

fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
        })
}

/// Runtime observation policy; durable schema, identities and validation are shared.
#[derive(Debug, Clone, Default)]
pub struct RuntimeCatalog(RuntimeCatalogData);

impl RuntimeCatalog {
    /// Create an empty catalog without assigning identities.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Observe an entity label at the runtime clock.
    pub fn intern_label(&mut self, name: &str) -> Result<RuntimeEntityId, GfError> {
        self.0.intern_label_at(name, now_micros())
    }

    /// Observe a relation type at the runtime clock.
    pub fn intern_relation_type(&mut self, name: &str) -> Result<RuntimeRelationId, GfError> {
        self.0.intern_relation_type_at(name, now_micros())
    }

    /// Observe an owner-scoped property at the runtime clock.
    pub fn intern_property(
        &mut self,
        name: &str,
        owner_label: Option<&str>,
    ) -> Result<RuntimePropId, GfError> {
        self.0.intern_property_at(name, owner_label, now_micros())
    }

    /// Decode the unchanged persisted catalog schema through its shared owner.
    pub fn from_record_batch(batch: &RecordBatch) -> Result<Self, GfError> {
        RuntimeCatalogData::from_record_batch(batch).map(Self)
    }

    /// Decode bounded persisted batches without concatenating them.
    pub fn from_record_batches<'a>(
        batches: impl IntoIterator<Item = &'a RecordBatch>,
    ) -> Result<Self, GfError> {
        RuntimeCatalogData::from_record_batches(batches).map(Self)
    }
}

impl Deref for RuntimeCatalog {
    type Target = RuntimeCatalogData;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RuntimeCatalog {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn pre_refactor_catalog_and_tagged_id_golden_vectors() {
        use arrow::array::{
            Array, StringArray, TimestampMicrosecondArray, UInt32Array, UInt64Array,
        };
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../graphforge-value/tests/fixtures/catalog_ids_v1.json"
        ))
        .unwrap();
        for vector in fixture["valid"].as_array().unwrap() {
            let local = u32::try_from(vector["local"].as_u64().unwrap()).unwrap();
            let expected = u32::try_from(vector["encoded"].as_u64().unwrap()).unwrap();
            let actual = match vector["kind"].as_str().unwrap() {
                "ontology" => TaggedTypeId::ontology(TypeId(local)).unwrap(),
                "runtime_entity" => {
                    EntityTypeId::runtime(RuntimeEntityId::new(local).unwrap()).tagged()
                }
                "runtime_relation" => {
                    RelationTypeId::runtime(RuntimeRelationId::new(local).unwrap()).tagged()
                }
                other => panic!("unexpected fixture domain {other}"),
            };
            assert_eq!(actual.encode(), expected);
            assert_eq!(serde_json::to_value(actual).unwrap(), vector["encoded"]);
        }
        let mut catalog = RuntimeCatalog::new();
        catalog.intern_label_at("Person", 100).unwrap();
        catalog.intern_relation_type_at("KNOWS", 101).unwrap();
        catalog
            .intern_property_at("name", Some("Person"), 102)
            .unwrap();
        catalog.intern_label_at("Person", 103).unwrap();
        let batch = catalog.to_record_batch();
        let kinds = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let counts = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
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
        let owners = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let actual: Vec<_> = (0..batch.num_rows()).map(|row| serde_json::json!({
            "entry_kind": kinds.value(row), "name": names.value(row), "runtime_id": ids.value(row),
            "observation_count": counts.value(row), "first_seen": first.value(row),
            "last_seen": last.value(row),
            "owner_label": if owners.is_null(row) { None } else { Some(owners.value(row)) },
        })).collect();
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            fixture["catalog_rows"]
        );
        assert_eq!(
            RuntimeCatalog::from_record_batch(&batch)
                .unwrap()
                .to_record_batch(),
            batch
        );
    }

    use std::collections::HashSet;

    use super::*;
    use arrow::array::{
        Array, ArrayRef, StringArray, TimestampMicrosecondArray, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::Schema;
    use graphforge_core::TypeId;
    use graphforge_value::{EntityTypeId, RelationTypeId, TaggedTypeId};
    use std::sync::Arc;

    fn make_catalog() -> RuntimeCatalog {
        let mut cat = RuntimeCatalog::new();
        cat.intern_label("Person").unwrap();
        cat.intern_label("Company").unwrap();
        cat.intern_relation_type("KNOWS").unwrap();
        cat.intern_property("name", Some("Person")).unwrap();
        cat.intern_property("founded", Some("Company")).unwrap();
        cat
    }

    #[test]
    fn checked_runtime_domains_remain_disjoint_from_ontology_zero() {
        let entity = EntityTypeId::runtime(RuntimeEntityId::new(0).unwrap()).tagged();
        let relation = RelationTypeId::runtime(RuntimeRelationId::new(0).unwrap()).tagged();
        let ontology = TaggedTypeId::ontology(TypeId(0)).unwrap();
        assert_ne!(entity, ontology);
        assert_ne!(entity, relation);
        assert_eq!(
            entity.runtime_entity_id(),
            Some(RuntimeEntityId::new(0).unwrap())
        );
        assert_eq!(ontology.runtime_entity_id(), None);
    }

    #[test]
    fn intern_label_same_id_for_same_name() {
        let mut cat = RuntimeCatalog::new();
        let id1 = cat.intern_label("Person").unwrap();
        let id2 = cat.intern_label("Person").unwrap();
        assert_eq!(id1, id2);
    }

    #[test]
    fn caller_authoritative_timestamp_is_preserved_for_every_catalog_kind() {
        let mut catalog = RuntimeCatalog::new();
        catalog.intern_label_at("Person", 42).unwrap();
        catalog.intern_relation_type_at("KNOWS", 42).unwrap();
        catalog
            .intern_property_at("score", Some("Person"), 42)
            .unwrap();
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
        let name_id = cat.intern_property("name", Some("Person")).unwrap();
        let founded_id = cat.intern_property("founded", Some("Company")).unwrap();
        assert_eq!(cat.property_name(name_id), Some("name"));
        assert_eq!(cat.property_name(founded_id), Some("founded"));
        // An unknown ID resolves to None.
        assert_eq!(cat.property_name(RuntimePropId::new(9999).unwrap()), None);
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
        let person = cat.intern_label("Person").unwrap();
        let company = cat.intern_label("Company").unwrap();
        assert_ne!(person, company);
    }

    #[test]
    fn intern_relation_type_same_id() {
        let mut cat = RuntimeCatalog::new();
        let id1 = cat.intern_relation_type("KNOWS").unwrap();
        let id2 = cat.intern_relation_type("KNOWS").unwrap();
        assert_eq!(id1, id2);
    }

    #[test]
    fn intern_relation_type_distinct_from_entity_type() {
        // Entity "Person" (id=0) and relation "KNOWS" (id=1) share the type ID
        // counter, so they get different IDs.
        let mut cat = RuntimeCatalog::new();
        let person_id = cat.intern_label("Person").unwrap();
        let knows_id = cat.intern_relation_type("KNOWS").unwrap();
        assert_ne!(person_id.get(), knows_id.get());
    }

    #[test]
    fn type_id_and_prop_id_are_independent() {
        let mut cat = RuntimeCatalog::new();
        // First entity type gets type ID 0.
        let type_id = cat.intern_label("Person").unwrap();
        // First property gets prop ID 0 — independent counter.
        let prop_id = cat.intern_property("name", Some("Person")).unwrap();
        assert_eq!(type_id.get(), 0);
        assert_eq!(prop_id.get(), 0);
    }

    #[test]
    fn contains_entity_type() {
        let mut cat = RuntimeCatalog::new();
        cat.intern_label("Person").unwrap();
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
        cat.intern_label("Person").unwrap();
        cat.intern_label("Person").unwrap();
        cat.intern_label("Person").unwrap();
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
        cat.intern_label("Person").unwrap();
        cat.intern_label("Person").unwrap();

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
        let person_id_orig = cat.intern_label("Person").unwrap();
        let person_id_rest = {
            let mut r = restored.clone();
            r.intern_label("Person").unwrap()
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
    fn roundtrip_preserves_global_unowned_properties() {
        let mut catalog = RuntimeCatalog::new();
        let expected = catalog.intern_property("score", None).unwrap();

        let restored = RuntimeCatalog::from_record_batch(&catalog.to_record_batch()).unwrap();
        assert!(restored.contains_property("score", None));
        assert_eq!(restored.property_name(expected), Some("score"));
        assert_eq!(
            restored
                .to_record_batch()
                .column(6)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .null_count(),
            1
        );
    }

    #[test]
    fn persisted_catalog_rejects_owner_on_non_property_rows() {
        let mut catalog = RuntimeCatalog::new();
        catalog.intern_label("Person").unwrap();
        let good = catalog.to_record_batch();
        let mut columns = good.columns().to_vec();
        columns[6] = Arc::new(StringArray::from(vec![Some("invalid-owner")]));
        let malformed = RecordBatch::try_new(RUNTIME_CATALOG_SCHEMA.clone(), columns).unwrap();

        assert!(RuntimeCatalog::from_record_batch(&malformed).is_err());
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
        cat.intern_label("X").unwrap();
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

    #[test]
    fn persisted_catalog_rejects_noncanonical_schema_without_panicking() {
        let mut cat = RuntimeCatalog::new();
        cat.intern_label("X").unwrap();
        let good = cat.to_record_batch();
        let shortened = RecordBatch::try_new(
            Arc::new(Schema::new(RUNTIME_CATALOG_SCHEMA.fields()[..6].to_vec())),
            good.columns()[..6].to_vec(),
        )
        .unwrap();
        assert!(RuntimeCatalog::from_record_batch(&shortened).is_err());
    }

    #[test]
    fn persisted_catalog_rejects_maximum_id_before_overflow() {
        let mut cat = RuntimeCatalog::new();
        cat.intern_label("X").unwrap();
        let good = cat.to_record_batch();
        let mut columns = good.columns().to_vec();
        columns[2] = Arc::new(UInt32Array::from(vec![u32::MAX]));
        let malformed = RecordBatch::try_new(RUNTIME_CATALOG_SCHEMA.clone(), columns).unwrap();
        assert!(RuntimeCatalog::from_record_batch(&malformed).is_err());
    }
}
