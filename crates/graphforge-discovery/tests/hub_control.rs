use graphforge_discovery::hub::*;
use graphforge_discovery::{
    ItemRepositoryFact, RepositoryIdentity, Sha256Digest, StableRepositoryId,
};
use serde_json::{Value, json};

const REQUEST: &str = "00000000-0000-4000-8000-000000000001";
const OPERATION: &str = "00000000-0000-4000-8000-000000000002";

fn request(operation: Value) -> HubControlRequest {
    HubControlRequest::from_json(
        &serde_json::to_vec(&json!({
            "format": HUB_CONTROL_FORMAT, "request_uuid": REQUEST, "operation": operation
        }))
        .unwrap(),
    )
    .unwrap()
}
fn response(
    request: &HubControlRequest,
    result: Value,
) -> Result<HubControlResponse, graphforge_discovery::DiscoveryError> {
    HubControlResponse::from_json(
        &serde_json::to_vec(&json!({
            "format": HUB_CONTROL_FORMAT, "request_uuid": REQUEST, "result": result
        }))
        .unwrap(),
        request,
    )
}
fn plan() -> HubManagementPlan {
    HubManagementPlan::new(
        "https://graphforge.sh".into(),
        OPERATION.into(),
        HubMutation::Rename {
            repository_id: StableRepositoryId::new("repository-key").unwrap(),
            name: "renamed".into(),
        },
        HubManagementPrecondition {
            authority_revision: "lease-1".into(),
            object_revision: "revision-1".into(),
        },
        2_000_000_000_000,
    )
    .unwrap()
}
fn record() -> HubRecord {
    HubRecord {
        target: HubTarget::Project {
            repository_id: StableRepositoryId::new("repository-key").unwrap(),
        },
        repository: ItemRepositoryFact {
            repository_id: StableRepositoryId::new("repository-key").unwrap(),
            repository: RepositoryIdentity::parse("publisher/project").unwrap(),
            revision: "revision-1".into(),
        },
        account_id: None,
        title: "A <literal> title".into(),
        description: None,
        license: None,
        tags: vec![],
        publisher_kind: HubAccountKind::Personal,
        visibility: HubVisibility::Public,
        actions: vec![HubAction::Inspect],
        ontology: None,
        producing_core_version: Some("0.6.0-rc.7".into()),
        starred: false,
    }
}
fn search(scope: Value) -> HubControlRequest {
    request(
        json!({"kind":"search", "query":{"scope":scope,"text":"","object_kind":null,"tags":[],"visibility":null},
        "page":{"limit":2,"cursor":null}}),
    )
}
fn page() -> Value {
    json!({"next_cursor":null,"coverage":"partial","total":{"kind":"unknown"}})
}

#[test]
fn request_boundary_is_closed_duplicate_free_bounded_and_diagnostic_safe() {
    let valid = request(json!({"kind":"capabilities"}));
    let value = serde_json::to_value(valid).unwrap();
    for changed in [
        json!({"format":HUB_CONTROL_FORMAT,"request_uuid":REQUEST,"operation":{"kind":"shell","command":"DO_NOT_ECHO"}}),
        json!({"format":HUB_CONTROL_FORMAT,"request_uuid":REQUEST,"operation":{"kind":"capabilities","token":"DO_NOT_ECHO"}}),
        json!({"format":"future/2","request_uuid":REQUEST,"operation":{"kind":"capabilities"}}),
    ] {
        let error =
            HubControlRequest::from_json(&serde_json::to_vec(&changed).unwrap()).unwrap_err();
        assert!(!error.to_string().contains("DO_NOT_ECHO"));
    }
    let duplicate = format!(
        "{{\"format\":\"DO_NOT_ECHO\",\"format\":\"{HUB_CONTROL_FORMAT}\",\"request_uuid\":\"{REQUEST}\",\"operation\":{{\"kind\":\"capabilities\"}}}}"
    );
    assert!(HubControlRequest::from_json(duplicate.as_bytes()).is_err());
    let mut trailing = serde_json::to_vec(&value).unwrap();
    trailing.extend_from_slice(b" {}");
    assert!(HubControlRequest::from_json(&trailing).is_err());
    assert!(HubControlRequest::from_json(&vec![b' '; MAX_HUB_REQUEST_BYTES + 1]).is_err());
    for uuid in [
        "",
        "00000000-0000-0000-0000-000000000000",
        "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF",
    ] {
        let mut value = value.clone();
        value["request_uuid"] = uuid.into();
        assert!(HubControlRequest::from_json(&serde_json::to_vec(&value).unwrap()).is_err());
    }
}

#[test]
fn public_and_private_search_have_explicit_scope_and_finite_continuations() {
    let public = search(json!({"kind":"public"}));
    assert!(!public.requires_session());
    assert!(search(json!({"kind":"account","account_id":"account-key"})).requires_session());
    assert!(search(json!({"kind":"starred"})).requires_session());
    let mut value = serde_json::to_value(public).unwrap();
    for limit in [0, 51] {
        value["operation"]["page"]["limit"] = limit.into();
        assert!(HubControlRequest::from_json(&serde_json::to_vec(&value).unwrap()).is_err());
    }
    value["operation"]["page"]["limit"] = 2.into();
    value["operation"]["page"]["cursor"] = "c".repeat(4097).into();
    assert!(HubControlRequest::from_json(&serde_json::to_vec(&value).unwrap()).is_err());
    value["operation"]["page"]["cursor"] = Value::Null;
    value["operation"]["query"]["visibility"] = "private".into();
    assert!(HubControlRequest::from_json(&serde_json::to_vec(&value).unwrap()).is_err());
}

#[test]
fn response_cannot_change_family_request_exact_target_or_privacy_scope() {
    let public = search(json!({"kind":"public"}));
    assert!(
        response(
            &public,
            json!({"kind":"search","records":[record()],"page":page()})
        )
        .is_ok()
    );
    let mut private = record();
    private.visibility = HubVisibility::Private;
    assert!(
        response(
            &public,
            json!({"kind":"search","records":[private],"page":page()})
        )
        .is_err()
    );
    let mut internal = record();
    internal.account_id = Some("account-key".into());
    assert!(
        response(
            &public,
            json!({"kind":"search","records":[internal],"page":page()})
        )
        .is_err()
    );
    let owned = search(json!({"kind":"account","account_id":"account-key"}));
    assert!(
        response(
            &owned,
            json!({"kind":"search","records":[record()],"page":page()})
        )
        .is_err()
    );
    assert!(
        response(
            &public,
            json!({"kind":"accounts","accounts":[],"page":page()})
        )
        .is_err()
    );
    assert!(
        response(
            &public,
            json!({"kind":"search","records":[record(),record(),record()],"page":page()})
        )
        .is_err()
    );
    let inspect =
        request(json!({"kind":"inspect","target":{"kind":"project","repository_id":"other"}}));
    assert!(response(&inspect, json!({"kind":"inspect","record":record()})).is_err());
    let mut bytes = serde_json::to_value(HubControlResponse {
        format: HUB_CONTROL_FORMAT.into(),
        request_uuid: OPERATION.into(),
        result: HubControlResult::Capabilities {
            capabilities: vec![],
        },
    })
    .unwrap();
    assert!(
        HubControlResponse::from_json(
            &serde_json::to_vec(&bytes).unwrap(),
            &request(json!({"kind":"capabilities"}))
        )
        .is_err()
    );
    bytes["request_uuid"] = REQUEST.into();
    assert!(
        HubControlResponse::from_json(
            &serde_json::to_vec(&bytes).unwrap(),
            &request(json!({"kind":"capabilities"}))
        )
        .is_ok()
    );
}

#[test]
fn native_plan_binds_every_reviewed_field_and_is_deterministic() {
    let plan = plan();
    assert_eq!(plan, plan.clone());
    let input = json!({"hub_origin":plan.hub_origin,"operation_uuid":plan.operation_uuid,
        "mutation":plan.mutation,"precondition":plan.precondition,"expires_at_ms":plan.expires_at_ms});
    assert_eq!(
        plan,
        HubManagementPlan::prepare_json(&serde_json::to_vec(&input).unwrap()).unwrap()
    );
    let value = serde_json::to_value(&plan).unwrap();
    for (path, changed) in [
        (vec!["hub_origin"], json!("https://other.example")),
        (vec!["operation_uuid"], json!(REQUEST)),
        (vec!["mutation", "name"], json!("other-name")),
        (vec!["precondition", "authority_revision"], json!("lease-2")),
        (vec!["precondition", "object_revision"], json!("revision-2")),
        (vec!["expires_at_ms"], json!(2_000_000_000_001_u64)),
    ] {
        let mut changed_plan = value.clone();
        let mut slot = &mut changed_plan;
        for key in path {
            slot = &mut slot[key];
        }
        *slot = changed;
        let parsed: HubManagementPlan = serde_json::from_value(changed_plan).unwrap();
        assert!(parsed.validate().is_err());
    }
    let mut input = input;
    input["hub_origin"] = "https://graphforge.sh/".into();
    assert!(HubManagementPlan::prepare_json(&serde_json::to_vec(&input).unwrap()).is_err());
}

#[test]
fn prepared_intent_and_reconciled_receipts_cannot_change_identity_or_success_state() {
    let plan = plan();
    let prepare =
        request(json!({"kind":"prepare","operation_uuid":OPERATION,"mutation":plan.mutation}));
    assert!(response(&prepare, json!({"kind":"prepared","plan":plan})).is_ok());
    let mut wrong = plan.clone();
    wrong.mutation = HubMutation::SetVisibility {
        repository_id: StableRepositoryId::new("repository-key").unwrap(),
        visibility: HubVisibility::Private,
    };
    let wrong = HubManagementPlan::new(
        wrong.hub_origin,
        wrong.operation_uuid,
        wrong.mutation,
        wrong.precondition,
        wrong.expires_at_ms,
    )
    .unwrap();
    assert!(response(&prepare, json!({"kind":"prepared","plan":wrong})).is_err());
    let apply = request(json!({"kind":"apply","plan":plan}));
    let status = request(
        json!({"kind":"status","operation_uuid":OPERATION,"intent_digest":plan.intent_digest}),
    );
    for state in ["prepared", "pending", "applied"] {
        let receipt = json!({"kind":"receipt","receipt":{"operation_uuid":OPERATION,"intent_digest":plan.intent_digest,
            "state":state,"refusal":null,"record":null}});
        assert!(response(&apply, receipt.clone()).is_ok());
        assert!(response(&status, receipt).is_ok());
    }
    let bad = json!({"kind":"receipt","receipt":{"operation_uuid":OPERATION,"intent_digest":plan.intent_digest,
        "state":"applied","refusal":"stale_plan","record":null}});
    assert!(response(&apply, bad).is_err());
    let conflict = json!({"kind":"failure","code":"idempotency_conflict","retry_after_ms":null});
    assert!(response(&apply, conflict).is_ok());
    let wrong_digest = Sha256Digest(format!("sha256:{}", "f".repeat(64)));
    assert!(response(&status, json!({"kind":"receipt","receipt":{"operation_uuid":OPERATION,"intent_digest":wrong_digest,
        "state":"applied","refusal":null,"record":null}})).is_err());
}

#[test]
fn metadata_permissions_are_not_package_authority_or_version_compatibility_gates() {
    let mut record = record();
    record.producing_core_version = Some("99.0.0".into());
    record.validate().unwrap();
    record.target = HubTarget::Project {
        repository_id: StableRepositoryId::new("different").unwrap(),
    };
    assert!(record.validate().is_err());
    record.target = HubTarget::Ontology {
        id: "ont_invalid".into(),
        version: "latest".into(),
        digest: Sha256Digest(format!("sha256:{}", "a".repeat(64))),
    };
    assert!(record.validate().is_err());
}

#[test]
fn constructed_replies_and_duplicate_lists_obey_wire_limits_and_count_semantics() {
    let capabilities = request(json!({"kind":"capabilities"}));
    assert!(
        response(
            &capabilities,
            json!({"kind":"capabilities","capabilities":["inspect","inspect"]})
        )
        .is_err()
    );
    let accounts = request(json!({"kind":"accounts","page":{"limit":2,"cursor":null}}));
    let account = json!({"account_id":"account-key","handle":null,"label":"Account","account_kind":"personal","role":"owner"});
    assert!(
        response(
            &accounts,
            json!({"kind":"accounts","accounts":[account,account],"page":page()})
        )
        .is_err()
    );
    let public = search(json!({"kind":"public"}));
    let mut item = record();
    item.actions.push(HubAction::Inspect);
    assert!(item.validate().is_err());
    assert!(response(&public, json!({"kind":"search","records":[record()],"page":{"next_cursor":null,"coverage":"complete","total":{"kind":"exact","value":0}}})).is_err());
    let inspect = request(json!({"kind":"inspect","target":record().target}));
    let mut oversized = record();
    oversized.description = Some("x".repeat(MAX_HUB_RESPONSE_BYTES));
    let constructed = HubControlResponse {
        format: HUB_CONTROL_FORMAT.into(),
        request_uuid: REQUEST.into(),
        result: HubControlResult::Inspect {
            record: oversized,
            detail: None,
        },
    };
    assert!(constructed.validate_for(&inspect).is_err());
    assert!(constructed.to_json(&inspect).is_err());
}

#[test]
fn project_detail_reuses_native_snapshot_and_exact_content_bindings() {
    use graphforge_discovery::{DiscoveryLimits, DiscoveryManifest, RefSet};
    let mut item = record();
    let manifest = DiscoveryManifest::from_json(
        include_bytes!("../../../tests/fixtures/hub/generated/v1/manifest.json"),
        DiscoveryLimits::default(),
    )
    .unwrap();
    let refs = RefSet::from_json(
        include_bytes!("../../../tests/fixtures/hub/generated/v1/refs.json"),
        DiscoveryLimits::default(),
    )
    .unwrap();
    item.repository.repository = manifest.repository.clone();
    let inspect = request(json!({"kind":"inspect","target":item.target}));
    let detail = HubProjectDetail {
        manifest,
        refs,
        lineage: None,
    };
    assert!(
        response(
            &inspect,
            json!({"kind":"inspect","record":item,"detail":detail})
        )
        .is_ok()
    );
    let mut wrong = detail;
    wrong.refs.repository = RepositoryIdentity::parse("other/repository").unwrap();
    assert!(
        response(
            &inspect,
            json!({"kind":"inspect","record":item,"detail":wrong})
        )
        .is_err()
    );
}

#[test]
fn completed_receipts_cannot_contradict_review_and_status_has_no_unbound_metadata() {
    let plan = plan();
    let apply = request(json!({"kind":"apply","plan":plan}));
    let receipt = |state, record: HubRecord| json!({"kind":"receipt","receipt":{"operation_uuid":OPERATION,"intent_digest":plan.intent_digest,"state":state,"refusal":null,"record":record}});
    assert!(response(&apply, receipt("pending", record())).is_ok());
    assert!(
        response(&apply, receipt("applied", record())).is_err(),
        "old canonical name cannot describe completed rename"
    );
    let mut renamed = record();
    renamed.repository.repository.repository = "renamed".into();
    assert!(response(&apply, receipt("applied", renamed.clone())).is_ok());
    let status = request(
        json!({"kind":"status","operation_uuid":OPERATION,"intent_digest":plan.intent_digest}),
    );
    assert!(
        response(&status, receipt("applied", renamed)).is_err(),
        "status must use explicit inspect to refresh target metadata"
    );
}
