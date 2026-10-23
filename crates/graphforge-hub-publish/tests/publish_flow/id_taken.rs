use super::*;
use graphforge_discovery::ItemId;

const MODULE: &str = "https://graphforge.sh/ont_aaaaaaaaaaaaaaaa";
const OTHER: &str = "https://graphforge.sh/ont_bbbbbbbbbbbbbbbb";
const BRIDGE: &str = "https://graphforge.sh/brg_aaaaaaaaaaaaaaaa";

fn plan(modules: &[(&str, char)], bridges: &[(&str, char)]) -> Plan {
    let mut plan = Plan::new("permanent item fixture", 'a', None);
    let descriptor = |(id, digest): &(&str, char)| {
        json!({
            "id": id, "version": "1", "content_digest": format!("sha256:{}", digest.to_string().repeat(64))
        })
    };
    plan.ontology = Some(json!({
        "composition_digest": format!("sha256:{}", "c".repeat(64)),
        "modules": modules.iter().map(descriptor).collect::<Vec<_>>(),
        "bridge_sets": bridges.iter().map(descriptor).collect::<Vec<_>>(),
    }));
    plan
}

fn client<'a>(hub: &'a ReferenceHub, owner: &str, repository: &str) -> Client<'a> {
    Client::new(
        hub,
        RepositoryIdentity::parse(&format!("{owner}/{repository}")).unwrap(),
    )
}

fn admit(client: &Client<'_>, plan: &Plan) -> HubResponse {
    let (session, upload) = client.opened(plan);
    assert_eq!(client.put(&upload, 0, &plan.bytes).status, 200);
    let response = client.commit(plan, &session);
    if response.status != 202 {
        return response;
    }
    for _ in 0..4 {
        let status = client.operation(plan.operation);
        if status.status != 200 {
            return status;
        }
        if OperationStatus::from_json(&status.body)
            .unwrap()
            .receipt
            .is_some()
        {
            return status;
        }
    }
    panic!("small reference publication must finish within bounded polls")
}

#[test]
fn permanent_claims_allow_owner_updates_and_exact_foreign_reuse_without_transfer() {
    for large_objects in [false, true] {
        let hub = ReferenceHub::with_config(ReferenceHubConfig {
            large_objects,
            limits: graphforge_hub_publish::PublishLimits {
                preferred_chunk_bytes: large_objects.then_some(1024 * 1024),
                ..ReferenceHubConfig::default().limits
            },
            ..ReferenceHubConfig::default()
        })
        .unwrap();
        assert_eq!(
            admit(
                &client(&hub, "alice", "original"),
                &plan(&[(MODULE, 'a')], &[(BRIDGE, 'b')])
            )
            .status,
            200
        );
        assert_eq!(
            admit(
                &client(&hub, "bob", "reuse"),
                &plan(&[(MODULE, 'a')], &[(BRIDGE, 'b')])
            )
            .status,
            200
        );
        let bob = client(&hub, "bob", "changed");
        let refusal = admit(&bob, &plan(&[(MODULE, 'c')], &[(BRIDGE, 'd')]));
        assert_eq!(refusal.status, 409);
        let body: HubErrorBody = serde_json::from_slice(&refusal.body).unwrap();
        assert_eq!(body.code, HubPublishErrorCode::IdTaken);
        assert_eq!(body.message, graphforge_hub_publish::ID_TAKEN_MESSAGE);
        let items = body.items.unwrap();
        assert_eq!(
            items
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            vec!["brg_aaaaaaaaaaaaaaaa", "ont_aaaaaaaaaaaaaaaa"]
        );
        assert!(
            items
                .iter()
                .all(|item| item.owner.is_none() && item.publish_as_owner.is_none())
        );
        assert!(bob.refs().is_none());
        assert_eq!(
            admit(
                &client(&hub, "alice", "updated"),
                &plan(&[(MODULE, 'c')], &[(BRIDGE, 'd')])
            )
            .status,
            200
        );
    }
}

#[test]
fn permanent_refusals_filter_visibility_and_only_offer_authorized_organization_publication() {
    let hub = ReferenceHub::with_config(ReferenceHubConfig::default()).unwrap();
    assert_eq!(
        admit(
            &client(&hub, "organization", "original"),
            &plan(&[(MODULE, 'a')], &[(BRIDGE, 'b')])
        )
        .status,
        200
    );
    hub.set_item_visibility("alice", &ItemId::parse_reference(MODULE).unwrap(), true)
        .unwrap();
    hub.set_organization_publish_permission("alice", "organization", true)
        .unwrap();
    let refusal = admit(
        &client(&hub, "alice", "changed"),
        &plan(&[(MODULE, 'c')], &[(BRIDGE, 'd')]),
    );
    assert_eq!(refusal.status, 409);
    let body: HubErrorBody = serde_json::from_slice(&refusal.body).unwrap();
    let items = body.items.unwrap();
    assert_eq!(items[0].owner, None);
    assert_eq!(items[1].owner.as_deref(), Some("organization"));
    assert!(items.iter().all(|item| item.publish_as_owner == Some(true)));
}

#[test]
fn failed_multi_item_admission_claims_nothing_and_legacy_identifiers_are_out_of_scope() {
    let hub = ReferenceHub::with_config(ReferenceHubConfig::default()).unwrap();
    assert_eq!(
        admit(
            &client(&hub, "alice", "original"),
            &plan(&[(MODULE, 'a')], &[])
        )
        .status,
        200
    );
    let bob = client(&hub, "bob", "changed");
    let refused = plan(&[(MODULE, 'b'), (OTHER, 'a')], &[]);
    assert_eq!(admit(&bob, &refused).status, 409);
    assert!(bob.refs().is_none());
    assert!(
        OperationStatus::from_json(&bob.operation(refused.operation).body)
            .unwrap()
            .receipt
            .is_none()
    );
    assert_eq!(
        admit(&client(&hub, "carol", "first"), &plan(&[(OTHER, 'b')], &[])).status,
        200
    );
    for (owner, digest) in [("alice", 'a'), ("bob", 'b')] {
        assert_eq!(
            admit(
                &client(&hub, owner, "legacy"),
                &plan(&[("https://example.org/ontology", digest)], &[])
            )
            .status,
            200
        );
    }
}

#[test]
fn competing_new_claims_commit_only_one_owner() {
    let hub = ReferenceHub::with_config(ReferenceHubConfig::default()).unwrap();
    let barrier = std::sync::Barrier::new(2);
    let results = std::thread::scope(|scope| {
        let tasks = [("alice", 'a'), ("bob", 'b')].map(|(owner, digest)| {
            let hub = &hub;
            let barrier = &barrier;
            scope.spawn(move || {
                let client = client(hub, owner, "competing");
                let plan = plan(&[(MODULE, digest)], &[]);
                let (session, upload) = client.opened(&plan);
                assert_eq!(client.put(&upload, 0, &plan.bytes).status, 200);
                barrier.wait();
                client.commit(&plan, &session).status
            })
        });
        tasks.map(|task| task.join().unwrap())
    });
    let mut sorted = results;
    sorted.sort_unstable();
    assert_eq!(sorted, [200, 409]);
}

#[test]
fn reassigned_namespace_cannot_finish_or_replay_an_old_accounts_session() {
    use graphforge_hub_publish::StableAccountId;
    for large_objects in [false, true] {
        let hub = ReferenceHub::with_config(ReferenceHubConfig {
            large_objects,
            limits: graphforge_hub_publish::PublishLimits {
                preferred_chunk_bytes: large_objects.then_some(1024 * 1024),
                ..ReferenceHubConfig::default().limits
            },
            ..ReferenceHubConfig::default()
        })
        .unwrap();
        hub.set_publication_account(
            "alice",
            &StableAccountId::new("account-A").unwrap(),
            "alice",
        )
        .unwrap();
        let original = client(&hub, "alice", "pending");
        let pending = plan(&[(MODULE, 'a')], &[]);
        let (session, upload) = original.opened(&pending);
        assert_eq!(original.put(&upload, 0, &pending.bytes).status, 200);
        if large_objects {
            assert_eq!(original.commit(&pending, &session).status, 202);
        }
        hub.set_publication_account(
            "alice",
            &StableAccountId::new("account-B").unwrap(),
            "alice",
        )
        .unwrap();
        assert_eq!(original.commit(&pending, &session).status, 403);
        assert_eq!(original.operation(pending.operation).status, 403);
        let replacement = client(&hub, "alice", "pending");
        assert_eq!(replacement.commit(&pending, &session).status, 404);
        assert_eq!(replacement.operation(pending.operation).status, 404);
        assert!(replacement.refs().is_none());
        // The pending transaction applied no claim, including after async polls.
        assert_eq!(
            admit(&replacement, &plan(&[(MODULE, 'b')], &[])).status,
            200
        );
    }
}

#[test]
fn issued_device_grant_cannot_become_a_reassigned_namespaces_account() {
    use graphforge_hub_publish::{DeviceAuthorizationResponse, StableAccountId};
    let hub = ReferenceHub::new();
    let account = StableAccountId::new("device-account-A").unwrap();
    hub.set_publication_account("alice", &account, "alice")
        .unwrap();
    let response = hub.handle(
        &HubRequest::new(
            HubMethod::Post,
            "https://hub.example/.gf/oauth/device_authorization",
        )
        .with_body(b"client_id=graphforge-cli&scope=publish%3Aalice%2Fdemo".to_vec()),
    );
    assert_eq!(response.status, 200);
    let grant: DeviceAuthorizationResponse = serde_json::from_slice(&response.body).unwrap();
    assert!(hub.approve_device(&grant.user_code));
    hub.set_publication_account(
        "alice",
        &StableAccountId::new("device-account-B").unwrap(),
        "alice",
    )
    .unwrap();
    let body = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs([
            ("client_id", "graphforge-cli"),
            ("grant_type", graphforge_hub_publish::DEVICE_CODE_GRANT_TYPE),
            ("device_code", &grant.device_code),
        ])
        .finish()
        .into_bytes();
    let response = hub.handle(
        &HubRequest::new(HubMethod::Post, "https://hub.example/.gf/oauth/token").with_body(body),
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).unwrap(),
        json!({"error":"access_denied"})
    );
}

#[test]
fn repository_rehome_preserves_claims_and_foreign_reuse_commits_a_new_witness() {
    let hub = ReferenceHub::new();
    let original = client(&hub, "alice", "original");
    assert_eq!(admit(&original, &plan(&[(MODULE, 'a')], &[])).status, 200);
    let moved = RepositoryIdentity::parse("bob/moved").unwrap();
    hub.move_repository(&original.repository, &moved).unwrap();
    let foreign = Client::new(&hub, moved);
    let before = foreign.refs().unwrap();
    let mut reuse = plan(&[(MODULE, 'a')], &[]);
    reuse.bytes = b"a new foreign publication witness".to_vec();
    reuse.expected_revision = Some(sha256_digest(&before));
    let reused = admit(&foreign, &reuse);
    assert_eq!(reused.status, 200);
    let receipt = receipt(&reused);
    assert_eq!(receipt.repository, foreign.repository);
    assert_ne!(before, foreign.refs().unwrap());
    let changed = admit(
        &client(&hub, "bob", "changed"),
        &plan(&[(MODULE, 'b')], &[]),
    );
    assert_eq!(changed.status, 409);
    assert_eq!(error_code(&changed), HubPublishErrorCode::IdTaken);
    assert_eq!(
        admit(
            &client(&hub, "alice", "still-owned"),
            &plan(&[(MODULE, 'b')], &[])
        )
        .status,
        200
    );
}
