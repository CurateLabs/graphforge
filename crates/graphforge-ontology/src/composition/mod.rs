//! Deterministic multi-ontology inventory composition (#836).
//!
//! Compiles independently identified ontology modules into a closure-ordered
//! inventory with qualified-symbol lookups and a contract-approved composition
//! fingerprint. Bridge *identities* participate in the fingerprint; full bridge
//! set lifecycle is owned by the sibling `bridge` module (#838).

mod canonical;
mod compile;
mod diagnostic;
mod identity;
mod resolve;

pub use canonical::{bridge_document_digest, canonical_json, module_document_digest};
pub use compile::{
    AuthoredModule, CompiledComposition, CompiledModule, CompositionLimits,
    InventoryCompileRequest, compile_inventory, compile_legacy_single_ontology,
};
pub use diagnostic::{
    CompositionDiagnostic, CompositionError, CompositionPhase, DiagnosticCode, DiagnosticLimit,
};
pub use identity::{
    ActivationMode, ActivationRecord, ActivationScope, BRIDGE_DIGEST_DOMAIN, BridgeSetId,
    DIGEST_HEX_LEN, MODULE_DIGEST_DOMAIN, OntologyModuleId, QualifiedSymbol, SymbolKind,
};
pub use resolve::{ResolutionOutcome, ResolveRequest};
