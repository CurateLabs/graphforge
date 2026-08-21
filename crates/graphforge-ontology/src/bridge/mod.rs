//! Provenance-bearing bridge-set lifecycle (#838).
//!
//! Bridge sets connect exact qualified symbols across ontology modules without
//! mutating either source module. Suggested/inferred mappings stay
//! non-authoritative until explicitly adopted. Equal human names never create a
//! bridge automatically.

mod store;
mod types;
mod validate;

pub use store::{
    BridgeDeletePreview, BridgeExportFormat, BridgeImportFormatHint, BridgeInspect,
    BridgeInventory, BridgeListEntry, BridgeMutationReceipt, BridgeSelector, BridgeSnapshot,
    BridgeUpdatePreview, ModuleSymbolTable,
};
pub use types::{
    BridgeAssertion, BridgeDocument, BridgeLifecycleStatus, BridgePredicate, BridgeProvenance,
    MappingConfidence, MappingMethod, SharedSurfaceHint,
};
pub use validate::validate_bridge_document;
