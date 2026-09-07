//! Compiler-independent checked identities and persisted value contracts.
//!
//! The integer encodings here are project compatibility contracts. Compiler
//! policy, ontology resolution, physical I/O, and execution belong to consumers.
#![forbid(unsafe_code)]

mod catalog;
pub use catalog::{RUNTIME_CATALOG_SCHEMA, RuntimeCatalogData};

mod ids;
pub use ids::{
    CatalogIdError, EntityTypeId, EntityTypeSelection, PrimaryEntityTypeId, PropertyId,
    RelationTypeId, RelationTypeSelection, RuntimeEntityId, RuntimePropId, RuntimeRelationId,
    TYPE_LOCAL_ID_LIMIT, TaggedTypeId, TypeIdKind,
};

mod literal;
pub use literal::Literal;
