//! Durable multi-ontology module inventory lifecycle (#837).
//!
//! Session staging (import/create candidates) is distinct from adopted
//! authority. Mutations publish one generation with idempotency receipts.
//! Bridge-set lifecycle remains #838; this module only tracks empty/stub bridge
//! identities for composition fingerprint stability.

mod store;

pub use store::{
    DeletePreview, ExportFormat, ImportFormatHint, InventoryMetadata, InventoryMutationReceipt,
    InventorySnapshot, ModuleInspect, ModuleLifecycleStatus, ModuleListEntry, ModuleSelector,
    OntologyInventory, UpdatePreview,
};
