//! Stable publish error classification and its HTTP projection.

use serde::{Deserialize, Serialize};
use std::fmt;

/// GraphForge-wide idempotency conflict code for changed content under a stable operation identity.
pub const GF_IDEMPOTENCY_CONFLICT: &str = "GF_IDEMPOTENCY_CONFLICT";

/// Stable machine-readable publish failure classification.
///
/// The serialized form (`auth_denied`, ...) is the `code` member of every Hub
/// error body.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HubPublishErrorCode {
    /// Publish credential missing, expired, or not scoped to the repository.
    AuthDenied,
    /// Entitlement or quota denied publication.
    EntitlementDenied,
    /// Same operation identity with a conflicting request.
    IdempotencyConflict,
    /// Ref advance rejected by an expected-revision precondition.
    RefConflict,
    /// Required future protocol format or capability is unsupported.
    UnsupportedFuture,
    /// Transported or stored bytes disagree with the declared digest or length.
    IntegrityFailure,
    /// Input failed structural validation.
    InvalidInput,
    /// Transport or Hub internal failure.
    Internal,
}

impl HubPublishErrorCode {
    /// Every code, in declaration order.
    pub const ALL: [Self; 8] = [
        Self::AuthDenied,
        Self::EntitlementDenied,
        Self::IdempotencyConflict,
        Self::RefConflict,
        Self::UnsupportedFuture,
        Self::IntegrityFailure,
        Self::InvalidInput,
        Self::Internal,
    ];

    /// Snake-case token used as the `code` member of Hub error bodies.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AuthDenied => "auth_denied",
            Self::EntitlementDenied => "entitlement_denied",
            Self::IdempotencyConflict => "idempotency_conflict",
            Self::RefConflict => "ref_conflict",
            Self::UnsupportedFuture => "unsupported_future",
            Self::IntegrityFailure => "integrity_failure",
            Self::InvalidInput => "invalid_input",
            Self::Internal => "internal",
        }
    }

    /// Default HTTP status for this code.
    ///
    /// The mapping in ADR 0053 lists the few request-specific overrides
    /// (`404` unknown resource, `413` declared size over a limit, `416` upload
    /// offset mismatch, `403` insufficient token scope).
    #[must_use]
    pub const fn http_status(self) -> u16 {
        match self {
            Self::AuthDenied => 401,
            Self::EntitlementDenied => 403,
            Self::IdempotencyConflict => 409,
            Self::RefConflict => 412,
            Self::UnsupportedFuture | Self::IntegrityFailure => 422,
            Self::InvalidInput => 400,
            Self::Internal => 500,
        }
    }
}

/// Sanitized publish error suitable for CLI, Hub, and telemetry projection.
///
/// `detail` is always a static string chosen by GraphForge code, never request
/// input, so it can never echo a token or an upload URL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubPublishError {
    /// Stable machine-readable code.
    pub code: HubPublishErrorCode,
    detail: &'static str,
}

impl HubPublishError {
    /// Construct a publish error with a stable detail string (never untrusted input).
    #[must_use]
    pub const fn new(code: HubPublishErrorCode, detail: &'static str) -> Self {
        Self { code, detail }
    }

    /// Sanitized, non-input-bearing diagnostic text.
    #[must_use]
    pub const fn detail(&self) -> &'static str {
        self.detail
    }

    /// Wire-level token used in logs and telemetry (`hub.publish.*`).
    #[must_use]
    pub const fn wire_code(&self) -> &'static str {
        match self.code {
            HubPublishErrorCode::AuthDenied => "hub.publish.auth_denied",
            HubPublishErrorCode::EntitlementDenied => "hub.publish.entitlement_denied",
            HubPublishErrorCode::IdempotencyConflict => "hub.publish.idempotency_conflict",
            HubPublishErrorCode::RefConflict => "hub.publish.ref_conflict",
            HubPublishErrorCode::UnsupportedFuture => "hub.publish.unsupported_future",
            HubPublishErrorCode::IntegrityFailure => "hub.publish.integrity_failure",
            HubPublishErrorCode::InvalidInput => "hub.publish.invalid_input",
            HubPublishErrorCode::Internal => "hub.publish.internal",
        }
    }

    /// Native GraphForge error code projected to callers.
    ///
    /// Idempotency conflicts use the GraphForge-wide [`GF_IDEMPOTENCY_CONFLICT`];
    /// every other code projects its wire token unchanged.
    #[must_use]
    pub const fn gf_code(&self) -> &'static str {
        match self.code {
            HubPublishErrorCode::IdempotencyConflict => GF_IDEMPOTENCY_CONFLICT,
            _ => self.wire_code(),
        }
    }

    /// Hub error body for this error.
    #[must_use]
    pub fn body(&self) -> HubErrorBody {
        HubErrorBody {
            code: self.code,
            message: self.detail.to_owned(),
        }
    }
}

impl fmt::Display for HubPublishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} ({}): {}",
            self.gf_code(),
            self.wire_code(),
            self.detail
        )
    }
}

impl std::error::Error for HubPublishError {}

/// JSON error body `{code, message}` returned by every Hub publish endpoint
/// except the OAuth token endpoint (which uses RFC 6749 error bodies).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubErrorBody {
    /// Stable machine-readable code.
    pub code: HubPublishErrorCode,
    /// Sanitized human-readable text. Clients display it but never branch on it.
    pub message: String,
}

pub(crate) const fn invalid(detail: &'static str) -> HubPublishError {
    HubPublishError::new(HubPublishErrorCode::InvalidInput, detail)
}

pub(crate) const fn unsupported(detail: &'static str) -> HubPublishError {
    HubPublishError::new(HubPublishErrorCode::UnsupportedFuture, detail)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotency_conflict_projects_the_graphforge_code() {
        let error = HubPublishError::new(HubPublishErrorCode::IdempotencyConflict, "conflict");
        assert_eq!(error.gf_code(), GF_IDEMPOTENCY_CONFLICT);
        assert_eq!(error.wire_code(), "hub.publish.idempotency_conflict");
    }

    #[test]
    fn serialized_code_equals_the_documented_token() {
        for code in HubPublishErrorCode::ALL {
            assert_eq!(
                serde_json::to_value(code).unwrap(),
                serde_json::Value::String(code.as_str().to_owned())
            );
            let error = HubPublishError::new(code, "x");
            assert_eq!(error.wire_code(), format!("hub.publish.{}", code.as_str()));
        }
    }
}
