//! Provider-neutral GraphForge Hub publish wire contract.
//!
//! This crate owns the `graphforge-hub-publish/1` documents, the stable error
//! classification and its HTTP projection, the request commitment, and
//! [`ReferenceHub`], an in-memory implementation of the whole publish and read
//! mapping used for conformance. It contains no HTTP client and no project
//! I/O; ADR 0053 documents the mapping.

#![forbid(unsafe_code)]

mod canonical;
mod error;
mod exchange;
mod reference;
mod wire;

pub use canonical::{canonical_json, parse_unique_json, sha256_digest, validate_digest};
pub use error::{GF_IDEMPOTENCY_CONFLICT, HubErrorBody, HubPublishError, HubPublishErrorCode};
pub use exchange::{HubExchange, HubMethod, HubRequest, HubResponse};
pub use reference::{ReferenceHub, ReferenceHubConfig};
pub use wire::{
    Capability, CommitRequest, DEVICE_CODE_GRANT_TYPE, DIGEST_PLACEHOLDER,
    DeviceAuthorizationResponse, DevicePoll, HUB_PUBLISH_FORMAT, OAuthErrorBody, ObjectDeclaration,
    OpenSessionRequest, PUBLISH_TOKEN_ENV, PublishAuthorization, PublishCapabilities,
    PublishIntent, PublishLimits, PublishReceipt, PublishSessionId, PublishToken,
    SUPPORTED_CAPABILITIES, SessionResponse, TokenResponse, UPLOAD_LENGTH_HEADER,
    UPLOAD_OFFSET_HEADER, UploadStatus, UploadTarget, check_format, check_requirements,
    content_range, parse_content_range, publish_scope, validate_https_url, validate_inventory,
    validate_ref_list,
};
