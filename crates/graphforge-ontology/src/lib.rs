//! GraphForge runtime-loadable ontology model, validation, and migration.
//!
//! # Milestone status
//!
//! - phase-10 ontology load/compile/persist/migrate ✓
//! - M9 #836 — deterministic inventory composition (`composition` module) ✓
//! - M9 #837 — ergonomic inventory CRUD / import-export (`inventory` module) ✓
//! - M9 #838 — provenance-bearing bridge sets (`bridge` module) ✓
#![forbid(unsafe_code)]

pub mod bridge;
pub mod compiler;
pub mod composition;
pub mod error;
pub mod handle;
pub mod inventory;
pub mod loader;
pub mod migration;
pub mod ontology;
pub mod persistence;
pub mod registry;
pub mod schemas;
pub mod spatial;
pub mod validator;

pub use bridge::{
    BridgeAssertion, BridgeDeletePreview, BridgeDocument, BridgeExportFormat,
    BridgeImportFormatHint, BridgeInspect, BridgeInventory, BridgeLifecycleStatus, BridgeListEntry,
    BridgeMutationReceipt, BridgePredicate, BridgeProvenance, BridgeSelector, BridgeSnapshot,
    BridgeUpdatePreview, MappingConfidence, MappingMethod, ModuleSymbolTable, SharedSurfaceHint,
    validate_bridge_document,
};
pub use compiler::{OntologyCompiler, OntologyRuntime, PropertyOwnerKind};
pub use composition::{
    ActivationMode, ActivationRecord, ActivationScope, AuthoredModule, BridgeSetId,
    CompiledComposition, CompiledModule, CompositionDiagnostic, CompositionError,
    CompositionLimits, DiagnosticCode, DiagnosticLimit, InventoryCompileRequest, OntologyModuleId,
    QualifiedSymbol, ResolutionOutcome, ResolveRequest, SymbolKind, bridge_document_digest,
    compile_inventory, compile_legacy_single_ontology, module_document_digest,
};
pub use error::{OntologyError, OntologyValidationError, ValidationErrorKind};
pub use graphforge_core::{PropId, TypeId};
pub use handle::{OntologyFormat, OntologyHandle};
pub use inventory::{
    DeletePreview, ExportFormat, ImportFormatHint, InventoryMetadata, InventoryMutationReceipt,
    InventorySnapshot, ModuleInspect, ModuleLifecycleStatus, ModuleListEntry, ModuleSelector,
    OntologyInventory, UpdatePreview,
};
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
