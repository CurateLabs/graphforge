//! Negotiated visibility and typed, bounded refusal semantics.
use graphforge_discovery::RepositoryIdentity;
use graphforge_hub_publish::{
    Capability, HubErrorBody, HubPublishErrorCode, PUBLISH_VISIBILITY_CAPABILITY,
    PublishErrorReason, PublishVisibility, ReferenceHub, require_visibility_capability,
};
use serde_json::json;

#[test]
fn explicit_visibility_requires_the_exact_offered_major() {
    let hub = ReferenceHub::new();
    let mut capabilities = hub.capabilities(&RepositoryIdentity::parse("alice/graph").unwrap());
    require_visibility_capability(&capabilities, None).unwrap();
    for visibility in [PublishVisibility::Public, PublishVisibility::Private] {
        for major in [None, Some(2), Some(1)] {
            capabilities
                .capabilities
                .retain(|c| c.capability != PUBLISH_VISIBILITY_CAPABILITY);
            if let Some(major) = major {
                capabilities
                    .capabilities
                    .push(Capability::new(PUBLISH_VISIBILITY_CAPABILITY, major));
            }
            let result = require_visibility_capability(&capabilities, Some(visibility));
            if major == Some(1) {
                result.unwrap();
            } else {
                assert_eq!(
                    result.unwrap_err().code,
                    HubPublishErrorCode::UnsupportedFuture
                );
            }
        }
    }
}

#[test]
fn reasons_are_typed_and_belong_to_one_error_code() {
    for (reason, code, expected) in [
        (
            "private_requires_plan",
            "entitlement_denied",
            PublishErrorReason::PrivateRequiresPlan,
        ),
        (
            "visibility_required",
            "entitlement_denied",
            PublishErrorReason::VisibilityRequired,
        ),
        (
            "repository_is_public",
            "visibility_mismatch",
            PublishErrorReason::RepositoryIsPublic,
        ),
        (
            "repository_is_private",
            "visibility_mismatch",
            PublishErrorReason::RepositoryIsPrivate,
        ),
        (
            "invalid_visibility",
            "invalid_input",
            PublishErrorReason::InvalidVisibility,
        ),
        (
            "storage_cap",
            "entitlement_denied",
            PublishErrorReason::StorageCap,
        ),
        (
            "session_cap",
            "entitlement_denied",
            PublishErrorReason::SessionCap,
        ),
    ] {
        let mut body = json!({"code":code, "reason":reason, "message":"untrusted server prose"});
        let parsed: HubErrorBody = serde_json::from_value(body.clone()).unwrap();
        assert_eq!(parsed.reason, Some(expected));
        assert_eq!(parsed.code, expected.code());
        body["code"] = json!("internal");
        assert!(serde_json::from_value::<HubErrorBody>(body).is_err());
    }
    for reason in [json!(null), json!("unknown_reason"), json!({}), json!(1)] {
        assert!(
            serde_json::from_value::<HubErrorBody>(json!({
                "code":"entitlement_denied", "message":"x", "reason":reason,
            }))
            .is_err()
        );
    }
}

#[test]
fn absent_reason_preserves_legacy_wire_bytes() {
    let body = br#"{"code":"entitlement_denied","message":"legacy"}"#;
    let parsed: HubErrorBody = serde_json::from_slice(body).unwrap();
    assert_eq!(parsed.reason, None);
    assert_eq!(serde_json::to_vec(&parsed).unwrap(), body);
}
