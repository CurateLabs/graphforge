//! GraphForge runtime-loadable ontology model, validation, and migration.
//!
//! # Milestone status
//!
//! - phase-10 #557 — Serde types (`ontology` module) ✓
//! - phase-10 #558 — YAML/JSON loader (`loader` module) ✓
//! - phase-10 #559 — Load-time validator (`validator` module) ✓
//! - phase-10 #560 — Arrow compiler (`compiler` module) ✓
//! - phase-10 #561 — `OntologyHandle` / `OntologyRegistry` ✓
//! - phase-10 #562 — Parquet persistence (`persistence` module) ✓
//! - phase-10 #563 — Migration engine (`migration` module) ← **this issue**
#![forbid(unsafe_code)]

pub mod compiler;
pub mod error;
pub mod handle;
pub mod loader;
pub mod migration;
pub mod ontology;
pub mod persistence;
pub mod registry;
pub mod schemas;
pub mod spatial;
pub mod validator;

pub use compiler::{OntologyCompiler, OntologyRuntime, PropertyOwnerKind};
pub use error::{OntologyError, OntologyValidationError, ValidationErrorKind};
pub use graphforge_core::{PropId, TypeId};
pub use handle::{OntologyFormat, OntologyHandle};
pub use loader::OntologyLoader;
pub use migration::{MigrationEngine, MigrationStep, TransformKind};
pub use ontology::{
    ConstraintDef, ConstraintKind, EntityTypeDef, MigrationDef, OntologyDoc, PropertyDef,
    PropertyValueType, RelationTypeDef, SemanticFlags,
};
pub use persistence::{load_parquet, save_parquet};
pub use registry::OntologyRegistry;
pub use spatial::{
    SpatialCrs, SpatialGeometryType, SpatialType, SpatialValidationError, SpatialValidationLimits,
};
pub use validator::OntologyValidator;
