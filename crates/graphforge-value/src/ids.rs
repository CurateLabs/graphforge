//! Checked owner of the existing tagged type-ID integer encoding.

use graphforge_core::{PropId, TypeId};
use serde::{Deserialize, Serialize};

/// Exclusive upper bound of ontology/semantic and runtime-local type IDs.
pub const TYPE_LOCAL_ID_LIMIT: u32 = 1 << 30;
const ENTITY_TAG: u32 = 1 << 30;
const RELATION_TAG: u32 = 1 << 31;
const TAG_MASK: u32 = ENTITY_TAG | RELATION_TAG;
const LOCAL_MASK: u32 = TYPE_LOCAL_ID_LIMIT - 1;

/// Identity domain encoded by a persisted type ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TypeIdKind {
    /// Untagged declared identity, interpreted against its ontology/semantic authority.
    Ontology,
    /// Entity label observed by the runtime catalog.
    RuntimeEntity,
    /// Relation type observed by the runtime catalog.
    RuntimeRelation,
    /// Owner-scoped runtime property identity (not stored in tagged type columns).
    RuntimeProperty,
    /// Declared ontology or composition property identity.
    OntologyProperty,
}

/// A malformed identity or an attempted use in the wrong identity domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CatalogIdError {
    /// A local identity does not fit its existing serialized domain.
    #[error("{kind:?} local ID {value} exceeds its supported range")]
    OutOfRange {
        /// Requested identity domain.
        kind: TypeIdKind,
        /// Rejected raw identity.
        value: u32,
    },
    /// Both reserved type-domain bits are set.
    #[error("type ID {encoded} has conflicting runtime entity and relation tags")]
    ConflictingTags {
        /// Rejected persisted integer.
        encoded: u32,
    },
    /// An entity/relation-specific field received the opposite runtime domain.
    #[error("expected {expected:?} type domain, found {actual:?}")]
    WrongDomain {
        /// Runtime domain allowed by the destination field.
        expected: TypeIdKind,
        /// Domain supplied by the input.
        actual: TypeIdKind,
    },
}

macro_rules! runtime_id {
    ($name:ident, $kind:ident, $limit:expr, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(try_from = "u32", into = "u32")]
        pub struct $name(u32);

        impl $name {
            /// Check a catalog-local integer without interpreting any type tag.
            pub const fn new(value: u32) -> Result<Self, CatalogIdError> {
                if value < $limit {
                    Ok(Self(value))
                } else {
                    Err(CatalogIdError::OutOfRange {
                        kind: TypeIdKind::$kind,
                        value,
                    })
                }
            }

            /// Return the unchanged catalog-local integer.
            #[must_use]
            pub const fn get(self) -> u32 {
                self.0
            }
        }

        impl TryFrom<u32> for $name {
            type Error = CatalogIdError;

            fn try_from(value: u32) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for u32 {
            fn from(value: $name) -> Self {
                value.get()
            }
        }
    };
}

runtime_id!(
    RuntimeEntityId,
    RuntimeEntity,
    TYPE_LOCAL_ID_LIMIT,
    "Checked runtime-catalog entity identity; distinct from relation and ontology IDs."
);
runtime_id!(
    RuntimeRelationId,
    RuntimeRelation,
    TYPE_LOCAL_ID_LIMIT,
    "Checked runtime-catalog relation identity; distinct from entity and ontology IDs."
);
// Existing catalog decoding requires a representable next-property counter.
runtime_id!(
    RuntimePropId,
    RuntimeProperty,
    u32::MAX,
    "Checked owner-scoped runtime property identity, retaining its untagged integer encoding."
);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TypeIdentity {
    Ontology(TypeId),
    RuntimeEntity(RuntimeEntityId),
    RuntimeRelation(RuntimeRelationId),
}

/// Checked carrier for the persisted ontology/runtime type-ID integer space.
///
/// Construction and serde decoding reject invalid identities. Entity and relation
/// consumers use [`EntityTypeId`] and [`RelationTypeId`] to enforce their context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct TaggedTypeId(TypeIdentity);

impl TaggedTypeId {
    /// Check an untagged ontology or generation-bound semantic identity.
    pub const fn ontology(id: TypeId) -> Result<Self, CatalogIdError> {
        if id.0 < TYPE_LOCAL_ID_LIMIT {
            Ok(Self(TypeIdentity::Ontology(id)))
        } else {
            Err(CatalogIdError::OutOfRange {
                kind: TypeIdKind::Ontology,
                value: id.0,
            })
        }
    }

    /// Decode the existing UInt32 type-column representation.
    pub const fn decode(encoded: u32) -> Result<Self, CatalogIdError> {
        let local = encoded & LOCAL_MASK;
        match encoded & TAG_MASK {
            0 => Ok(Self(TypeIdentity::Ontology(TypeId(local)))),
            ENTITY_TAG => Ok(Self(TypeIdentity::RuntimeEntity(RuntimeEntityId(local)))),
            RELATION_TAG => Ok(Self(TypeIdentity::RuntimeRelation(RuntimeRelationId(
                local,
            )))),
            _ => Err(CatalogIdError::ConflictingTags { encoded }),
        }
    }

    /// Encode without changing the existing persisted integer.
    #[must_use]
    pub const fn encode(self) -> u32 {
        match self.0 {
            TypeIdentity::Ontology(id) => id.0,
            TypeIdentity::RuntimeEntity(id) => ENTITY_TAG | id.get(),
            TypeIdentity::RuntimeRelation(id) => RELATION_TAG | id.get(),
        }
    }

    /// Inspect the checked identity domain without interpreting tag bits again.
    #[must_use]
    pub const fn kind(self) -> TypeIdKind {
        match self.0 {
            TypeIdentity::Ontology(_) => TypeIdKind::Ontology,
            TypeIdentity::RuntimeEntity(_) => TypeIdKind::RuntimeEntity,
            TypeIdentity::RuntimeRelation(_) => TypeIdKind::RuntimeRelation,
        }
    }

    /// Get an ontology/semantic ID only when the input belongs to that domain.
    #[must_use]
    pub const fn ontology_id(self) -> Option<TypeId> {
        match self.0 {
            TypeIdentity::Ontology(id) => Some(id),
            _ => None,
        }
    }

    /// Get a runtime entity ID only when the input belongs to that domain.
    #[must_use]
    pub const fn runtime_entity_id(self) -> Option<RuntimeEntityId> {
        match self.0 {
            TypeIdentity::RuntimeEntity(id) => Some(id),
            _ => None,
        }
    }

    /// Get a runtime relation ID only when the input belongs to that domain.
    #[must_use]
    pub const fn runtime_relation_id(self) -> Option<RuntimeRelationId> {
        match self.0 {
            TypeIdentity::RuntimeRelation(id) => Some(id),
            _ => None,
        }
    }
}

impl TryFrom<u32> for TaggedTypeId {
    type Error = CatalogIdError;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Self::decode(value)
    }
}

impl From<TaggedTypeId> for u32 {
    fn from(value: TaggedTypeId) -> Self {
        value.encode()
    }
}

macro_rules! contextual_id {
    ($name:ident, $runtime:ident, $variant:ident, $kind:ident, $other:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(try_from = "u32", into = "u32")]
        pub struct $name(TaggedTypeId);

        impl $name {
            /// Check an ontology or generation-bound semantic identity.
            pub const fn ontology(id: TypeId) -> Result<Self, CatalogIdError> {
                match TaggedTypeId::ontology(id) {
                    Ok(id) => Ok(Self(id)),
                    Err(error) => Err(error),
                }
            }

            /// Carry an already checked runtime identity of the correct kind.
            #[must_use]
            pub const fn runtime(id: $runtime) -> Self {
                Self(TaggedTypeId(TypeIdentity::$variant(id)))
            }

            /// Validate persisted encoding and its entity/relation context.
            pub const fn decode(encoded: u32) -> Result<Self, CatalogIdError> {
                let decoded = match TaggedTypeId::decode(encoded) {
                    Ok(id) => id,
                    Err(error) => return Err(error),
                };
                match decoded.kind() {
                    TypeIdKind::$other => Err(CatalogIdError::WrongDomain {
                        expected: TypeIdKind::$kind,
                        actual: TypeIdKind::$other,
                    }),
                    _ => Ok(Self(decoded)),
                }
            }

            /// Return the unchanged integer for a typed serialization boundary.
            #[must_use]
            pub const fn encode(self) -> u32 {
                self.0.encode()
            }

            /// Inspect identity domain through the shared checked carrier.
            #[must_use]
            pub const fn tagged(self) -> TaggedTypeId {
                self.0
            }
        }

        impl TryFrom<u32> for $name {
            type Error = CatalogIdError;
            fn try_from(value: u32) -> Result<Self, Self::Error> {
                Self::decode(value)
            }
        }
        impl From<$name> for u32 {
            fn from(value: $name) -> Self {
                value.encode()
            }
        }
    };
}

contextual_id!(
    EntityTypeId,
    RuntimeEntityId,
    RuntimeEntity,
    RuntimeEntity,
    RuntimeRelation,
    "Checked entity-label carrier, which cannot contain a runtime relation ID."
);
contextual_id!(
    RelationTypeId,
    RuntimeRelationId,
    RuntimeRelation,
    RuntimeRelation,
    RuntimeEntity,
    "Checked relation-type carrier, which cannot contain a runtime entity ID."
);

/// Entity-label selection without conflating an unknown label with all labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntityTypeSelection {
    /// No label restriction.
    All,
    /// Select one checked declared or runtime entity label.
    Known(EntityTypeId),
    /// A requested label is absent; preserve the empty-result behavior.
    Missing,
}

impl EntityTypeSelection {
    /// Inspect a selected identity without treating missing as unrestricted.
    #[must_use]
    pub const fn known_id(self) -> Option<EntityTypeId> {
        match self {
            Self::Known(id) => Some(id),
            Self::All | Self::Missing => None,
        }
    }
}

/// Relation selection with distinct wildcard and unresolved-name states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RelationTypeSelection {
    /// Expand every relation type.
    All,
    /// Expand the checked declared or runtime relation type.
    Known(RelationTypeId),
    /// An explicitly requested type is absent.
    Missing,
}

impl RelationTypeSelection {
    /// Inspect a selected identity without treating missing as a wildcard.
    #[must_use]
    pub const fn known_id(self) -> Option<RelationTypeId> {
        match self {
            Self::Known(id) => Some(id),
            Self::All | Self::Missing => None,
        }
    }
}

/// Immutable primary property-routing label, independent of current membership.
///
/// The existing topology scalar uses `u32::MAX` for an originally unlabelled
/// node. Label changes can leave that absence beside a nonempty membership list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct PrimaryEntityTypeId(Option<EntityTypeId>);

impl PrimaryEntityTypeId {
    /// Preserve the original absence of a primary routing label.
    #[must_use]
    pub const fn absent() -> Self {
        Self(None)
    }

    /// Preserve a checked original primary routing label.
    #[must_use]
    pub const fn known(id: EntityTypeId) -> Self {
        Self(Some(id))
    }

    /// Decode the existing primary-routing scalar, including its absence value.
    pub const fn decode(encoded: u32) -> Result<Self, CatalogIdError> {
        if encoded == u32::MAX {
            Ok(Self::absent())
        } else {
            match EntityTypeId::decode(encoded) {
                Ok(id) => Ok(Self::known(id)),
                Err(error) => Err(error),
            }
        }
    }

    /// Return the original scalar without inferring it from current membership.
    #[must_use]
    pub const fn encode(self) -> u32 {
        match self.0 {
            Some(id) => id.encode(),
            None => u32::MAX,
        }
    }

    /// Inspect the original routing label independently of current membership.
    #[must_use]
    pub const fn label(self) -> Option<EntityTypeId> {
        self.0
    }
}

impl TryFrom<u32> for PrimaryEntityTypeId {
    type Error = CatalogIdError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Self::decode(value)
    }
}

impl From<PrimaryEntityTypeId> for u32 {
    fn from(value: PrimaryEntityTypeId) -> Self {
        value.encode()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum PropertyIdentity {
    Ontology(PropId),
    Runtime(RuntimePropId),
}

/// Compiler property reference with explicit ontology/runtime ownership.
///
/// Property catalog integers have no tag bits. Their domain must stay attached
/// in compiler carriers and be resolved against the matching authority; it
/// cannot be reconstructed from a bare numeric property ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "PropertyWire", into = "PropertyWire")]
pub struct PropertyId(PropertyIdentity);

impl PropertyId {
    /// Carry a declared property identity without substituting a runtime ID.
    pub const fn ontology(id: PropId) -> Result<Self, CatalogIdError> {
        if id.0 == u32::MAX {
            Err(CatalogIdError::OutOfRange {
                kind: TypeIdKind::OntologyProperty,
                value: id.0,
            })
        } else {
            Ok(Self(PropertyIdentity::Ontology(id)))
        }
    }

    /// Carry an already checked owner-scoped runtime property identity.
    #[must_use]
    pub const fn runtime(id: RuntimePropId) -> Self {
        Self(PropertyIdentity::Runtime(id))
    }

    /// Resolve only declared property references through an ontology map.
    #[must_use]
    pub const fn ontology_id(self) -> Option<PropId> {
        match self.0 {
            PropertyIdentity::Ontology(id) => Some(id),
            PropertyIdentity::Runtime(_) => None,
        }
    }

    /// Resolve only runtime property references through the runtime catalog.
    #[must_use]
    pub const fn runtime_id(self) -> Option<RuntimePropId> {
        match self.0 {
            PropertyIdentity::Runtime(id) => Some(id),
            PropertyIdentity::Ontology(_) => None,
        }
    }
}

impl std::fmt::Display for PropertyId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            PropertyIdentity::Ontology(id) => write!(f, "ontology_{}", id.0),
            PropertyIdentity::Runtime(id) => write!(f, "runtime_{}", id.get()),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum PropertyWire {
    Ontology(u32),
    Runtime(u32),
}

impl TryFrom<PropertyWire> for PropertyId {
    type Error = CatalogIdError;

    fn try_from(value: PropertyWire) -> Result<Self, Self::Error> {
        match value {
            PropertyWire::Ontology(id) => Self::ontology(PropId(id)),
            PropertyWire::Runtime(id) => RuntimePropId::new(id).map(Self::runtime),
        }
    }
}

impl From<PropertyId> for PropertyWire {
    fn from(value: PropertyId) -> Self {
        match value.0 {
            PropertyIdentity::Ontology(id) => Self::Ontology(id.0),
            PropertyIdentity::Runtime(id) => Self::Runtime(id.get()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn original_integer_and_serde_golden_vectors() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/catalog_ids_v1.json")).unwrap();
        let mut unique = HashSet::new();
        for vector in fixture["valid"].as_array().unwrap() {
            let raw = u32::try_from(vector["encoded"].as_u64().unwrap()).unwrap();
            let local = u32::try_from(vector["local"].as_u64().unwrap()).unwrap();
            let decoded = TaggedTypeId::decode(raw).unwrap();
            let expected = match vector["kind"].as_str().unwrap() {
                "ontology" => TaggedTypeId::ontology(TypeId(local)).unwrap(),
                "runtime_entity" => {
                    EntityTypeId::runtime(RuntimeEntityId::new(local).unwrap()).tagged()
                }
                "runtime_relation" => {
                    RelationTypeId::runtime(RuntimeRelationId::new(local).unwrap()).tagged()
                }
                other => panic!("unknown frozen fixture domain {other}"),
            };
            assert_eq!(decoded, expected);
            assert_eq!(decoded.encode(), raw);
            assert_eq!(serde_json::to_value(decoded).unwrap(), vector["encoded"]);
            assert_eq!(
                serde_json::from_value::<TaggedTypeId>(vector["encoded"].clone()).unwrap(),
                decoded
            );
            assert_eq!(
                serde_json::to_value(raw.to_le_bytes()).unwrap(),
                vector["little_endian"]
            );
            assert!(unique.insert(decoded.encode()));
        }
        for raw in fixture["invalid_tagged"].as_array().unwrap() {
            let encoded = u32::try_from(raw.as_u64().unwrap()).unwrap();
            assert!(TaggedTypeId::decode(encoded).is_err());
            assert!(EntityTypeId::decode(encoded).is_err());
            assert!(RelationTypeId::decode(encoded).is_err());
            assert!(serde_json::from_value::<TaggedTypeId>(raw.clone()).is_err());
        }
        for raw in fixture["invalid_type_locals"].as_array().unwrap() {
            let local = u32::try_from(raw.as_u64().unwrap()).unwrap();
            assert!(TaggedTypeId::ontology(TypeId(local)).is_err());
            assert!(RuntimeEntityId::new(local).is_err());
            assert!(RuntimeRelationId::new(local).is_err());
        }
    }

    #[test]
    fn contextual_types_reject_the_opposite_runtime_domain() {
        let entity = EntityTypeId::runtime(RuntimeEntityId::new(0).unwrap());
        let relation = RelationTypeId::runtime(RuntimeRelationId::new(0).unwrap());
        assert!(EntityTypeId::decode(relation.encode()).is_err());
        assert!(RelationTypeId::decode(entity.encode()).is_err());
        assert!(
            serde_json::from_value::<EntityTypeId>(serde_json::json!(relation.encode())).is_err()
        );
        assert!(
            serde_json::from_value::<RelationTypeId>(serde_json::json!(entity.encode())).is_err()
        );
        assert_eq!(entity.tagged().runtime_relation_id(), None);
        assert_eq!(relation.tagged().runtime_entity_id(), None);
        assert_eq!(entity.tagged().ontology_id(), None);
        assert_eq!(relation.tagged().ontology_id(), None);
    }

    #[test]
    fn primary_absence_does_not_admit_a_membership_or_relation_sentinel() {
        let primary = PrimaryEntityTypeId::decode(u32::MAX).unwrap();
        assert_eq!(primary, PrimaryEntityTypeId::absent());
        assert_eq!(primary.label(), None);
        assert_eq!(
            serde_json::to_value(primary).unwrap(),
            serde_json::json!(4294967295_u32)
        );
        assert!(EntityTypeId::decode(u32::MAX).is_err());
        assert!(RelationTypeId::decode(u32::MAX).is_err());
        let membership = EntityTypeId::runtime(RuntimeEntityId::new(7).unwrap());
        assert_eq!(PrimaryEntityTypeId::known(membership).encode(), 1073741831);
        assert!(PrimaryEntityTypeId::decode(2147483648).is_err());
        assert!(PrimaryEntityTypeId::decode(3221225472).is_err());
    }

    #[test]
    fn property_ir_domains_do_not_collide_or_accept_ambiguous_numeric_input() {
        let declared = PropertyId::ontology(PropId(7)).unwrap();
        let runtime = PropertyId::runtime(RuntimePropId::new(7).unwrap());
        assert_ne!(declared, runtime);
        assert_eq!(HashSet::from([declared, runtime]).len(), 2);
        assert_eq!(declared.runtime_id(), None);
        assert_eq!(runtime.ontology_id(), None);
        for (id, wire) in [
            (declared, serde_json::json!({"Ontology": 7})),
            (runtime, serde_json::json!({"Runtime": 7})),
        ] {
            assert_eq!(serde_json::to_value(id).unwrap(), wire);
            assert_eq!(serde_json::from_value::<PropertyId>(wire).unwrap(), id);
        }
        for invalid in [
            serde_json::json!(7),
            serde_json::json!({"Runtime": u32::MAX}),
            serde_json::json!({"Ontology": u32::MAX}),
        ] {
            assert!(serde_json::from_value::<PropertyId>(invalid).is_err());
        }
    }

    #[test]
    fn missing_and_all_are_distinct_wire_selections() {
        assert_ne!(EntityTypeSelection::All, EntityTypeSelection::Missing);
        assert_ne!(RelationTypeSelection::All, RelationTypeSelection::Missing);
        assert_eq!(
            serde_json::to_value(EntityTypeSelection::All).unwrap(),
            serde_json::json!("All")
        );
        assert_eq!(
            serde_json::to_value(EntityTypeSelection::Missing).unwrap(),
            serde_json::json!("Missing")
        );
        assert_eq!(
            serde_json::from_value::<EntityTypeSelection>(serde_json::json!("Missing")).unwrap(),
            EntityTypeSelection::Missing
        );
        assert!(
            serde_json::from_value::<EntityTypeSelection>(serde_json::json!({"Known": u32::MAX}))
                .is_err()
        );
    }

    #[test]
    fn property_ids_remain_untagged_and_counter_range_checked() {
        for raw in [0, TYPE_LOCAL_ID_LIMIT, u32::MAX - 1] {
            let id = RuntimePropId::new(raw).unwrap();
            assert_eq!(id.get(), raw);
            assert_eq!(serde_json::to_value(id).unwrap(), serde_json::json!(raw));
            assert_eq!(
                serde_json::from_value::<RuntimePropId>(serde_json::json!(raw)).unwrap(),
                id
            );
        }
        assert!(RuntimePropId::new(u32::MAX).is_err());
    }
}
