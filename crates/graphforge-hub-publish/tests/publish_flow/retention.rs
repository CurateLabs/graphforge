//! Real reference transport retention and resume, using the ordinary wire/session owners.
use super::*;
use graphforge_hub_publish::{Capability, HISTORY_MUTATION_CAPABILITY};

fn open(
    client: &Client<'_>,
    plan: &Plan,
    prune: bool,
    refs: &[&str],
) -> (String, UploadTarget, CommitRequest) {
    let mut manifest = client.manifest(plan);
    manifest["resolved_ref"] = json!(refs[0]);
    let manifest_digest = DiscoveryManifest::from_json(
        &serde_json::to_vec(&manifest).unwrap(),
        DiscoveryLimits::default(),
    )
    .unwrap()
    .canonical_digest()
    .unwrap()
    .0;
    let refs: Vec<_> = refs.iter().map(|s| (*s).to_owned()).collect();
    let request = CommitRequest {
        rewrite: false,
        prune,
        manifest,
        refs: refs.clone(),
        expected_revision: plan.expected_revision.clone(),
    };
    let intent = PublishIntent {
        rewrite: false,
        prune,
        repository: client.repository.clone(),
        intent_digest: plan.intent_digest.clone(),
        objects: vec![plan.declaration()],
        manifest_validator: manifest_digest,
        refs,
        expected_revision: plan.expected_revision.clone(),
    };
    let response = client.open_with(&OpenSessionRequest {
        format: HUB_PUBLISH_FORMAT.into(),
        operation_uuid: plan.operation,
        request_commitment: intent.request_commitment(),
        intent_digest: plan.intent_digest.clone(),
        repository: client.repository.clone(),
        objects: vec![plan.declaration()],
        requirements: vec![Capability::new(HISTORY_MUTATION_CAPABILITY, 1)],
    });
    assert_eq!(response.status, 201);
    let SessionResponse::Open {
        session_id,
        mut uploads,
    } = serde_json::from_slice(&response.body).unwrap()
    else {
        panic!("expected open");
    };
    (session_id.as_str().into(), uploads.remove(0), request)
}
fn commit(client: &Client<'_>, session: &str, request: &CommitRequest) -> HubResponse {
    client.hub.handle(
        &HubRequest::new(
            HubMethod::Post,
            format!(
                "{}/.gf/publish/sessions/{session}/commit",
                client.hub.repository_url(&client.repository)
            ),
        )
        .with_header("authorization", client.token.authorization())
        .with_json(request),
    )
}
fn publish(client: &Client<'_>, plan: &Plan, prune: bool, refs: &[&str]) -> PublishReceipt {
    let (session, upload, request) = open(client, plan, prune, refs);
    if upload.received < upload.length {
        assert_eq!(
            client
                .put(
                    &upload,
                    upload.received as usize,
                    &plan.bytes[upload.received as usize..]
                )
                .status,
            200
        );
    }
    receipt(&commit(client, &session, &request))
}
fn object(client: &Client<'_>, plan: &Plan) -> HubResponse {
    client.hub.handle(&HubRequest::new(
        HubMethod::Get,
        client
            .capabilities
            .object_location(&plan.declaration().digest),
    ))
}
#[test]
fn fixed_grace_collects_unreferenced_objects_without_destroying_session_bytes() {
    let hub = ReferenceHub::new();
    let client = Client::new(&hub, demo());
    let first = Plan::new("old bytes", 'a', None);
    let old = client.publish(&first);
    let pending = Plan::new("old bytes", 'b', Some(old.revision.clone()));
    let (session, upload, request) = open(&client, &pending, false, &["main"]);
    assert_eq!(upload.received, upload.length);
    let next = Plan::new("new bytes", 'c', Some(old.revision));
    let changed = publish(&client, &next, true, &["main"]);
    assert_eq!(object(&client, &first).status, 200);
    hub.advance_clock(899);
    assert_eq!(object(&client, &first).status, 200);
    hub.advance_clock(1);
    assert_eq!(object(&client, &first).status, 404);
    assert_eq!(object(&client, &next).status, 200);
    // Refresh only the expired transport credential; the owned upload retains
    // its actual bytes, reaches CAS, and refuses its stale original lease.
    let fresh = Client::new(&hub, demo());
    assert_eq!(
        hub.handle(&HubRequest::new(HubMethod::Head, &upload.upload_url))
            .header("upload-offset"),
        Some(upload.length.to_string().as_str())
    );
    let rejected = commit(&fresh, &session, &request);
    assert_eq!(rejected.status, 412);
    assert_eq!(error_code(&rejected), HubPublishErrorCode::RefConflict);
    assert_eq!(sha256_digest(&fresh.refs().unwrap()), changed.revision);
}
#[test]
fn every_surviving_ref_keeps_its_shared_objects_live_after_prune_grace() {
    let hub = ReferenceHub::new();
    let client = Client::new(&hub, demo());
    let first = Plan::new("shared bytes", 'a', None);
    let old = client.publish(&first);
    let kept = publish(
        &client,
        &Plan::new("shared bytes", 'a', Some(old.revision)),
        false,
        &["keep"],
    );
    let next = Plan::new("new main", 'b', Some(kept.revision));
    publish(&client, &next, true, &["main"]);
    hub.advance_clock(900);
    assert_eq!(object(&client, &first).status, 200);
    assert_eq!(object(&client, &next).status, 200);
}
