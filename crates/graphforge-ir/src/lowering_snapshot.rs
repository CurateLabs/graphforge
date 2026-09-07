//! Immutable schema and catalog facts consumed by relational lowering.
//!
//! This value contains no filesystem paths, providers, or graph rows. Storage
//! constructs it before compilation; execution independently admits resources.
use arrow::datatypes::SchemaRef;
use graphforge_value::{
    EntityTypeId, PropertyId, RelationTypeId, RuntimeEntityId, RuntimeRelationId,
};
use std::collections::{BTreeMap, HashMap, HashSet};

/// Schema/catalog data for one compilation snapshot.
#[derive(Debug, Clone, Default)]
pub struct LoweringSnapshot {
    /// Checked property identity to display name.
    pub property_names: HashMap<PropertyId, String>,
    /// Checked runtime entity identities, separate from ontology IDs.
    pub runtime_labels: HashMap<RuntimeEntityId, String>,
    /// Checked runtime relation identities, separate from ontology IDs.
    pub runtime_relations: HashMap<RuntimeRelationId, String>,
    /// Semantic relation identities to opaque route stems.
    pub semantic_relations: HashMap<RelationTypeId, String>,
    /// Semantic entity identities to opaque property route stems.
    pub semantic_labels: HashMap<EntityTypeId, String>,
    /// Semantic entity identities to qualified display names.
    pub semantic_display_labels: HashMap<EntityTypeId, String>,
    /// Semantic composition fingerprint, when one is bound.
    pub composition: Option<String>,
    /// Dataset topology schema; absent in schema-only snapshots.
    pub node_schema: Option<SchemaRef>,
    /// Ordered discovered node routes used by wildcard/path schema unions.
    pub node_property_stems: Vec<String>,
    /// Ordered discovered edge routes used by wildcard schema unions.
    pub edge_property_stems: Vec<String>,
    /// Node property schemas, including catalog-backed selected routes.
    pub node_properties: BTreeMap<String, SchemaRef>,
    /// Edge property schemas, including catalog-backed selected routes.
    pub edge_properties: BTreeMap<String, SchemaRef>,
    /// Authenticated semantic edge schemas by checked relation identity.
    pub semantic_edges: HashMap<RelationTypeId, SchemaRef>,
    /// Authenticated semantic property schemas by relation identity.
    pub semantic_edge_properties: HashMap<RelationTypeId, SchemaRef>,
    /// Names of registered typed edge tables.
    pub typed_edge_tables: HashSet<String>,
}

impl LoweringSnapshot {
    /// Borrow or clone the corresponding immutable catalog fact.
    #[must_use]
    pub fn prop_names(&self) -> &HashMap<PropertyId, String> {
        &self.property_names
    }
    /// Borrow or clone the corresponding immutable catalog fact.
    #[must_use]
    pub fn label_names(&self) -> &HashMap<RuntimeEntityId, String> {
        &self.runtime_labels
    }
    /// Borrow or clone the corresponding immutable catalog fact.
    #[must_use]
    pub fn rel_names(&self) -> &HashMap<RuntimeRelationId, String> {
        &self.runtime_relations
    }
    /// Borrow or clone the corresponding immutable catalog fact.
    #[must_use]
    pub fn semantic_rel_routes(&self) -> &HashMap<RelationTypeId, String> {
        &self.semantic_relations
    }
    /// Borrow or clone the corresponding immutable catalog fact.
    #[must_use]
    pub fn semantic_label_routes(&self) -> &HashMap<EntityTypeId, String> {
        &self.semantic_labels
    }
    /// Borrow or clone the corresponding immutable catalog fact.
    #[must_use]
    pub fn semantic_label_names(&self) -> &HashMap<EntityTypeId, String> {
        &self.semantic_display_labels
    }
    /// Borrow or clone the corresponding immutable catalog fact.
    #[must_use]
    pub fn semantic_composition_fingerprint(&self) -> Option<&str> {
        self.composition.as_deref()
    }
    /// Borrow or clone the corresponding immutable catalog fact.
    #[must_use]
    pub fn semantic_edge_schema(&self, id: RelationTypeId) -> Option<SchemaRef> {
        self.semantic_edges.get(&id).cloned()
    }
    /// Borrow or clone the corresponding immutable catalog fact.
    #[must_use]
    pub fn semantic_edge_property_schema(&self, id: RelationTypeId) -> Option<SchemaRef> {
        self.semantic_edge_properties.get(&id).cloned()
    }
}
