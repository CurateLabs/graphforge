use super::*;
use crate::auth::control::exchange_control_from;
use graphforge_discovery::hub::*;
use graphforge_discovery::{Sha256Digest, StableRepositoryId};
use serde_json::json;

const REQUEST: &str = "00000000-0000-4000-8000-000000000001";
const OPERATION: &str = "00000000-0000-4000-8000-000000000002";
fn control(operation: serde_json::Value) -> HubControlRequest {
    let mut document = json!({"format":HUB_CONTROL_FORMAT, "request_uuid":REQUEST});
    document["operation"] = operation;
    HubControlRequest::from_json(&serde_json::to_vec(&document).unwrap()).unwrap()
}
fn envelope(result: serde_json::Value) -> serde_json::Value {
    let mut document = json!({"format":HUB_CONTROL_FORMAT, "request_uuid":REQUEST});
    document["result"] = result;
    document
}
fn saved() -> MemoryStore {
    MemoryStore {
        value: RefCell::new(Some(
            serde_json::to_string(&Session {
                access_token: "human-session-secret".into(),
                refresh_token: "refresh-secret".into(),
                expires_at: 500,
            })
            .unwrap(),
        )),
        ..MemoryStore::default()
    }
}
fn accounts() -> HubControlRequest {
    control(json!({"kind":"accounts","page":{"limit":10,"cursor":null}}))
}
fn prepared() -> HubManagementPlan {
    HubManagementPlan::new(
        DEFAULT_HUB.into(),
        OPERATION.into(),
        HubMutation::Rename {
            repository_id: StableRepositoryId::new("repository-key").unwrap(),
            name: "renamed".into(),
        },
        HubManagementPrecondition {
            authority_revision: "lease-1".into(),
            object_revision: "revision-1".into(),
        },
        200_000,
    )
    .unwrap()
}

#[test]
fn control_saved_session_is_process_only_and_account_calls_never_downgrade_to_public() {
    let hub = origin(DEFAULT_HUB).unwrap();
    for (anonymous, attached, store) in [
        (false, false, saved()),
        (true, true, saved()),
        (false, true, MemoryStore::default()),
    ] {
        let transport = Mock::new(vec![]);
        let error = exchange_control_from(
            &store,
            &transport,
            &hub,
            &accounts(),
            anonymous,
            attached,
            100,
        )
        .unwrap_err();
        assert!(error.to_string().contains("hub.auth_denied"));
        assert!(transport.requests.borrow().is_empty());
        assert!(!error.to_string().contains("human-session-secret"));
    }
    let transport = Mock::new(vec![(
        200,
        envelope(json!({"kind":"accounts","accounts":[],
        "page":{"next_cursor":null,"coverage":"complete","total":{"kind":"exact","value":0}}})),
    )]);
    exchange_control_from(&saved(), &transport, &hub, &accounts(), false, true, 100).unwrap();
    let requests = transport.requests.borrow();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].url, format!("{DEFAULT_HUB}{HUB_CONTROL_PATH}"));
    assert_eq!(
        requests[0].header("authorization"),
        Some("Bearer human-session-secret")
    );
    assert!(!format!("{:?}", requests[0]).contains("human-session-secret"));
}

#[test]
fn control_capabilities_never_read_vault_and_refresh_stays_on_selected_origin() {
    let hub = origin("https://preview.example").unwrap();
    let transport = Mock::new(vec![(
        200,
        envelope(json!({"kind":"capabilities","capabilities":["public_search"]})),
    )]);
    exchange_control_from(
        &FailedStore,
        &transport,
        &hub,
        &control(json!({"kind":"capabilities"})),
        false,
        true,
        100,
    )
    .unwrap();
    assert!(
        transport.requests.borrow()[0]
            .header("authorization")
            .is_none()
    );
    let transport = Mock::new(vec![
        (200, tokens("rotated-secret", "rotated-refresh")),
        (
            200,
            envelope(
                json!({"kind":"accounts","accounts":[],"page":{"next_cursor":null,"coverage":"complete","total":{"kind":"exact","value":0}}}),
            ),
        ),
    ]);
    let store = saved();
    exchange_control_from(&store, &transport, &hub, &accounts(), false, true, 500).unwrap();
    let requests = transport.requests.borrow();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].url, "https://preview.example/api/cli/token");
    assert_eq!(
        requests[1].url,
        "https://preview.example/api/hub/v1/control"
    );
    assert_eq!(
        requests[1].header("authorization"),
        Some("Bearer rotated-secret")
    );
    assert_eq!(store.writes.get(), 1);
}

#[test]
fn control_refuses_cross_hub_and_expired_review_before_credentials_or_network() {
    let hub = origin(DEFAULT_HUB).unwrap();
    let plan = prepared();
    let apply = control(json!({"kind":"apply","plan":plan}));
    for (selected, now, code) in [
        (
            origin("https://other.example").unwrap(),
            100,
            "hub.control_origin_mismatch",
        ),
        (hub, 201, "hub.control_plan_expired"),
    ] {
        let transport = Mock::new(vec![]);
        let error = exchange_control_from(
            &FailedStore,
            &transport,
            &selected,
            &apply,
            false,
            true,
            now,
        )
        .unwrap_err();
        assert!(error.to_string().contains(code));
        assert!(transport.requests.borrow().is_empty());
    }
}

#[test]
fn interrupted_or_redirected_apply_is_uncertain_never_retried_or_reported_success() {
    let hub = origin(DEFAULT_HUB).unwrap();
    let apply = control(json!({"kind":"apply","plan":prepared()}));
    for (status, result) in [
        (
            302,
            envelope(json!({"kind":"failure","code":"service_unavailable","retry_after_ms":null})),
        ),
        (200, json!({"private":"DO_NOT_ECHO"})),
        (
            503,
            envelope(
                json!({"kind":"receipt","receipt":{"operation_uuid":OPERATION,
            "intent_digest":prepared().intent_digest,"state":"applied","refusal":null,"record":null}}),
            ),
        ),
    ] {
        let transport = Mock::new(vec![(status, result)]);
        let error = exchange_control_from(&saved(), &transport, &hub, &apply, false, true, 100)
            .unwrap_err();
        assert!(error.to_string().contains("hub.control_outcome_uncertain"));
        assert!(!error.to_string().contains("DO_NOT_ECHO"));
        assert_eq!(transport.requests.borrow().len(), 1);
    }
    let transport = Mock::new(vec![(
        409,
        envelope(json!({"kind":"failure","code":"stale_plan","retry_after_ms":null})),
    )]);
    let response =
        exchange_control_from(&saved(), &transport, &hub, &apply, false, true, 100).unwrap();
    assert!(matches!(
        response.result,
        HubControlResult::Failure {
            code: HubControlErrorCode::StalePlan,
            ..
        }
    ));
}

#[test]
fn expired_session_and_exact_status_refusals_do_not_leak_or_start_new_operations() {
    let hub = origin(DEFAULT_HUB).unwrap();
    let transport = Mock::new(vec![(400, json!({"error":"invalid_grant"}))]);
    let error = exchange_control_from(&saved(), &transport, &hub, &accounts(), false, true, 500)
        .unwrap_err();
    assert!(error.to_string().contains("hub.auth_denied"));
    assert_eq!(transport.requests.borrow().len(), 1);
    let status = control(json!({"kind":"status","operation_uuid":OPERATION,
        "intent_digest":Sha256Digest(format!("sha256:{}","f".repeat(64)))}));
    let transport = Mock::new(vec![(
        404,
        envelope(json!({"kind":"failure","code":"not_found","retry_after_ms":null})),
    )]);
    assert!(matches!(
        exchange_control_from(&saved(), &transport, &hub, &status, false, true, 100)
            .unwrap()
            .result,
        HubControlResult::Failure {
            code: HubControlErrorCode::NotFound,
            ..
        }
    ));
    assert_eq!(transport.requests.borrow().len(), 1);
}

#[path = "control_http.rs"]
mod http;
