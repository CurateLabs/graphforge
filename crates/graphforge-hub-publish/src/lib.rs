//! Provider-neutral GraphForge Hub publish wire contract.
//!
//! This crate owns the `graphforge-hub-publish/1` documents, the stable error
//! classification and its HTTP projection, the request commitment, and
//! the native-only `ReferenceHub`, a temporary-file-backed implementation of
//! publish and read mapping used for conformance. It contains no HTTP client and no project
//! I/O; ADR 0053 documents the mapping.

#![forbid(unsafe_code)]

mod canonical;
mod commit;
mod error;
mod exchange;
mod history;
mod item_admission;
mod item_source;
#[cfg(not(target_arch = "wasm32"))]
mod reference;
mod wire;

pub use canonical::{canonical_json, parse_unique_json, sha256_digest, validate_digest};
pub use commit::check_commit_manifest;
pub use error::{
    GF_IDEMPOTENCY_CONFLICT, HubErrorBody, HubPublishError, HubPublishErrorCode, ID_TAKEN_MESSAGE,
    IdTakenItem, MAX_ID_TAKEN_ITEMS,
};
pub use exchange::{HubExchange, HubMethod, HubRequest, HubResponse};
pub use history::{
    prune_unreferenced_history, require_history_capability, require_history_requirement,
    validate_history_transition,
};
pub use item_admission::{
    ItemAdmissionInput, ItemAdmissionPlan, ItemClaimAction, ItemClaimDecision, ItemClaimFact,
    ItemClaimRequirement, ItemClaimState, ItemExactTuple, ItemTupleFact, MAX_ITEM_ADMISSION_BYTES,
    StableAccountId, item_admission_requirements, plan_item_admission,
};
pub use item_source::{
    ItemSourceChange, ItemSourceLifecycle, ItemSourceMutationFact, ItemSourcePrecondition,
    ItemSourceRecord, ItemSourceTransitionInput, ItemSourceTransitionPlan,
    plan_item_source_transition,
};
#[cfg(not(target_arch = "wasm32"))]
pub use reference::{ReferenceHub, ReferenceHubConfig};
pub use wire::{
    Capability, CommitRequest, DEVICE_CODE_GRANT_TYPE, DIGEST_PLACEHOLDER,
    DeviceAuthorizationResponse, DevicePoll, HEX_PLACEHOLDER, HISTORY_MUTATION_CAPABILITY,
    HUB_PUBLISH_FORMAT, LARGE_OBJECT_PUBLISH_CAPABILITY, OAuthErrorBody, ObjectDeclaration,
    OpenSessionRequest, OperationStatus, PUBLISH_NEGOTIATION_CAPABILITY, PUBLISH_TOKEN_ENV,
    PublishAuthorization, PublishCapabilities, PublishIntent, PublishLimits, PublishReceipt,
    PublishSessionId, PublishToken, SUPPORTED_CAPABILITIES, SessionResponse, TokenResponse,
    UPLOAD_LENGTH_HEADER, UPLOAD_OFFSET_HEADER, UploadStatus, UploadTarget, check_format,
    check_requirements, content_range, parse_content_range, publish_scope, validate_https_url,
    validate_inventory, validate_ref_list,
};

/// Reusable Rust image admission API shared by publisher and Hub.
pub use graphforge_discovery::preview;
