//! Publish behaviors driven through the typed wire documents against
//! [`ReferenceHub`], asserting Hub state as well as responses.

use graphforge_discovery::{
    DISCOVERY_FORMAT, DiscoveryLimits, DiscoveryManifest, PORTABLE_V2_MEDIA_TYPE, RefSet,
    RepositoryIdentity,
};
use graphforge_hub_publish::{
    CommitRequest, DevicePoll, HUB_PUBLISH_FORMAT, HubErrorBody, HubMethod, HubPublishErrorCode,
    HubRequest, HubResponse, ObjectDeclaration, OpenSessionRequest, OperationStatus,
    PublishCapabilities, PublishIntent, PublishReceipt, PublishToken, ReferenceHub,
    ReferenceHubConfig, SessionResponse, UploadTarget, content_range, sha256_digest,
};
use serde_json::{Value, json};
use uuid::Uuid;

fn demo() -> RepositoryIdentity {
    RepositoryIdentity::parse("openalex/demo").unwrap()
}

fn error_code(response: &HubResponse) -> HubPublishErrorCode {
    serde_json::from_slice::<HubErrorBody>(&response.body)
        .unwrap_or_else(|_| panic!("not an error body: {}", response.status))
        .code
}

/// A publication as a client would assemble it.
struct Client<'a> {
    hub: &'a ReferenceHub,
    repository: RepositoryIdentity,
    token: PublishToken,
    capabilities: PublishCapabilities,
}

struct Plan {
    operation: Uuid,
    intent_digest: String,
    bytes: Vec<u8>,
    package_digest: String,
    expected_revision: Option<String>,
}

impl Plan {
    fn new(text: &str, package: char, expected_revision: Option<String>) -> Self {
        Self {
            operation: Uuid::now_v7(),
            intent_digest: sha256_digest(format!("intent of {text}").as_bytes()),
            bytes: text.as_bytes().to_vec(),
            package_digest: format!("sha256:{}", package.to_string().repeat(64)),
            expected_revision,
        }
    }

    fn declaration(&self) -> ObjectDeclaration {
        ObjectDeclaration {
            digest: sha256_digest(&self.bytes),
            length: self.bytes.len() as u64,
            media_type: PORTABLE_V2_MEDIA_TYPE.to_owned(),
        }
    }
}

impl<'a> Client<'a> {
    fn new(hub: &'a ReferenceHub, repository: RepositoryIdentity) -> Self {
        let token = PublishToken::new(hub.issue_token(
            &format!("publish:{}/{}", repository.owner, repository.repository),
            900,
        ));
        let response = hub.handle(&HubRequest::new(
            HubMethod::Get,
            format!("{}/.gf/publish", hub.repository_url(&repository)),
        ));
        assert_eq!(response.status, 200);
        let capabilities = PublishCapabilities::from_json(&response.body).unwrap();
        Self {
            hub,
            repository,
            token,
            capabilities,
        }
    }

    fn manifest(&self, plan: &Plan) -> serde_json::Value {
        let object = plan.declaration();
        json!({
            "format": DISCOVERY_FORMAT,
            "version": {"major": 1, "minor": 1},
            "repository": self.repository,
            "default_ref": "main",
            "resolved_ref": "main",
            "immutable_version": plan.package_digest,
            "package": {"format": "graphforge-project/2", "package_digest": plan.package_digest, "object_digest": object.digest},
            "requirements": [{"capability": "portable-v2", "major": 1}],
            "capabilities": [],
            "objects": [{"digest": object.digest, "length": object.length, "media_type": object.media_type,
                "locations": [self.capabilities.object_location(&object.digest)]}],
        })
    }

    fn intent(&self, plan: &Plan) -> PublishIntent {
        let manifest = DiscoveryManifest::from_json(
            &serde_json::to_vec(&self.manifest(plan)).unwrap(),
            DiscoveryLimits::default(),
        )
        .unwrap();
        PublishIntent {
            repository: self.repository.clone(),
            intent_digest: plan.intent_digest.clone(),
            objects: vec![plan.declaration()],
            manifest_validator: manifest.canonical_digest().unwrap().0,
            refs: vec!["main".into()],
            expected_revision: plan.expected_revision.clone(),
        }
    }

    fn open(&self, plan: &Plan) -> HubResponse {
        self.open_with(&OpenSessionRequest {
            format: HUB_PUBLISH_FORMAT.into(),
            operation_uuid: plan.operation,
            request_commitment: self.intent(plan).request_commitment(),
            intent_digest: plan.intent_digest.clone(),
            repository: self.repository.clone(),
            objects: vec![plan.declaration()],
            requirements: self
                .capabilities
                .requirements
                .iter()
                .filter(|capability| {
                    matches!(
                        capability.capability.as_str(),
                        graphforge_hub_publish::PUBLISH_NEGOTIATION_CAPABILITY
                            | "large-object-publish"
                    )
                })
                .cloned()
                .collect(),
        })
    }

    fn open_with(&self, request: &OpenSessionRequest) -> HubResponse {
        self.hub.handle(
            &HubRequest::new(
                HubMethod::Post,
                format!(
                    "{}/.gf/publish/sessions",
                    self.hub.repository_url(&self.repository)
                ),
            )
            .with_header("authorization", self.token.authorization())
            .with_json(request),
        )
    }

    fn put(&self, upload: &UploadTarget, start: usize, chunk: &[u8]) -> HubResponse {
        self.hub.handle(
            &HubRequest::new(HubMethod::Put, &upload.upload_url)
                .with_header(
                    "content-range",
                    content_range(start as u64, chunk.len() as u64, upload.length),
                )
                .with_body(chunk.to_vec()),
        )
    }

    fn commit(&self, plan: &Plan, session_id: &str) -> HubResponse {
        self.hub.handle(
            &HubRequest::new(
                HubMethod::Post,
                format!(
                    "{}/.gf/publish/sessions/{session_id}/commit",
                    self.hub.repository_url(&self.repository)
                ),
            )
            .with_header("authorization", self.token.authorization())
            .with_json(&CommitRequest {
                manifest: self.manifest(plan),
                refs: vec!["main".into()],
                expected_revision: plan.expected_revision.clone(),
            }),
        )
    }

    fn opened(&self, plan: &Plan) -> (String, UploadTarget) {
        let response = self.open(plan);
        assert_eq!(
            response.status,
            201,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        match serde_json::from_slice(&response.body).unwrap() {
            SessionResponse::Open {
                session_id,
                mut uploads,
            } => (session_id.as_str().to_owned(), uploads.remove(0)),
            SessionResponse::Complete { .. } | SessionResponse::Pending { .. } => {
                panic!("expected an open session")
            }
        }
    }

    fn publish(&self, plan: &Plan) -> PublishReceipt {
        let (session, upload) = self.opened(plan);
        assert_eq!(self.put(&upload, 0, &plan.bytes).status, 200);
        receipt(&self.commit(plan, &session))
    }

    fn operation(&self, operation: Uuid) -> HubResponse {
        self.hub.handle(
            &HubRequest::new(
                HubMethod::Get,
                format!(
                    "{}/.gf/publish/operations/{operation}",
                    self.hub.repository_url(&self.repository)
                ),
            )
            .with_header("authorization", self.token.authorization()),
        )
    }

    fn refs(&self) -> Option<Vec<u8>> {
        let response = self.hub.handle(&HubRequest::new(
            HubMethod::Get,
            format!("{}/.gf/refs", self.hub.repository_url(&self.repository)),
        ));
        (response.status == 200).then_some(response.body)
    }
}

fn receipt(response: &HubResponse) -> PublishReceipt {
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    match serde_json::from_slice(&response.body).unwrap() {
        SessionResponse::Complete { receipt } => receipt,
        SessionResponse::Open { .. } | SessionResponse::Pending { .. } => {
            panic!("expected a receipt")
        }
    }
}

#[test]
fn same_publication_twice_yields_one_version_and_the_original_receipt() {
    let hub = ReferenceHub::new();
    let client = Client::new(&hub, demo());
    let plan = Plan::new("package v1", 'b', None);
    let (session, upload) = client.opened(&plan);
    assert_eq!(client.put(&upload, 0, &plan.bytes).status, 200);
    let first = receipt(&client.commit(&plan, &session));
    let refs = client.refs().unwrap();

    // Reopen with the same operation and commitment: original receipt, no upload.
    assert_eq!(receipt(&client.open(&plan)), first);
    // Recommit the same session (lost response): original receipt.
    assert_eq!(receipt(&client.commit(&plan, &session)), first);

    assert_eq!(hub.revisions(&demo()), vec![first.revision.clone()]);
    assert_eq!(client.refs().unwrap(), refs);
    assert_eq!(sha256_digest(&refs), first.revision);
    assert_eq!(first.previous_revision, None);
}

#[test]
fn operation_status_reports_intent_and_original_receipt_without_a_session() {
    let hub = ReferenceHub::new();
    let client = Client::new(&hub, demo());
    let plan = Plan::new("package v1", 'b', None);
    let unknown = client.operation(plan.operation);
    assert_eq!(unknown.status, 404);
    assert_eq!(error_code(&unknown), HubPublishErrorCode::InvalidInput);

    let (session, upload) = client.opened(&plan);
    let open = OperationStatus::from_json(&client.operation(plan.operation).body).unwrap();
    assert_eq!(open.intent_digest, plan.intent_digest);
    assert_eq!(open.receipt, None);

    assert_eq!(client.put(&upload, 0, &plan.bytes).status, 200);
    let first = receipt(&client.commit(&plan, &session));
    let done = OperationStatus::from_json(&client.operation(plan.operation).body).unwrap();
    assert_eq!(done.receipt, Some(first));
    assert_eq!(done.operation_uuid, plan.operation);

    // The lookup is authorized and repository-scoped.
    let anonymous = hub.handle(&HubRequest::new(
        HubMethod::Get,
        format!(
            "{}/.gf/publish/operations/{}",
            hub.repository_url(&demo()),
            plan.operation
        ),
    ));
    assert_eq!(error_code(&anonymous), HubPublishErrorCode::AuthDenied);
    let other = Client::new(&hub, RepositoryIdentity::parse("openalex/other").unwrap());
    assert_eq!(other.operation(plan.operation).status, 404);
}

#[test]
fn changed_content_under_the_same_operation_is_an_idempotency_conflict() {
    let hub = ReferenceHub::new();
    let client = Client::new(&hub, demo());
    let plan = Plan::new("package v1", 'b', None);
    let first = client.publish(&plan);
    let mut changed = Plan::new("package v2", 'c', Some(first.revision.clone()));
    changed.operation = plan.operation;
    let response = client.open(&changed);
    assert_eq!(response.status, 409);
    assert_eq!(
        error_code(&response),
        HubPublishErrorCode::IdempotencyConflict
    );
    assert_eq!(hub.revisions(&demo()).len(), 1);
}

#[test]
fn interrupted_upload_resumes_from_the_reported_offset() {
    let hub = ReferenceHub::new();
    let client = Client::new(&hub, demo());
    let plan = Plan::new("a package long enough to split in two", 'b', None);
    let (session, upload) = client.opened(&plan);
    assert_eq!(client.put(&upload, 0, &plan.bytes[..7]).status, 200);

    // The client lost track of progress: HEAD reports what the Hub retained.
    let head = hub.handle(&HubRequest::new(HubMethod::Head, &upload.upload_url));
    assert_eq!(head.status, 200);
    let offset: usize = head.header("upload-offset").unwrap().parse().unwrap();
    assert_eq!(offset, 7);
    // Resending from zero is refused with the retained offset, not appended.
    let restart = client.put(&upload, 0, &plan.bytes);
    assert_eq!(restart.status, 416);
    assert_eq!(restart.header("upload-offset"), Some("7"));

    assert_eq!(
        client.put(&upload, offset, &plan.bytes[offset..]).status,
        200
    );
    let receipt = receipt(&client.commit(&plan, &session));
    assert_eq!(hub.revisions(&demo()), vec![receipt.revision]);
    let object = hub.handle(&HubRequest::new(
        HubMethod::Get,
        client
            .capabilities
            .object_location(&plan.declaration().digest),
    ));
    assert_eq!(object.body, plan.bytes);
}

#[test]
fn corrupt_object_is_refused_and_refs_do_not_move() {
    let hub = ReferenceHub::new();
    let client = Client::new(&hub, demo());
    let first = client.publish(&Plan::new("package v1", 'b', None));
    let refs = client.refs().unwrap();

    let plan = Plan::new("package v2", 'c', Some(first.revision.clone()));
    let (session, upload) = client.opened(&plan);
    let mut corrupt = plan.bytes.clone();
    corrupt[0] ^= 1;
    let put = client.put(&upload, 0, &corrupt);
    assert_eq!(put.status, 422);
    assert_eq!(error_code(&put), HubPublishErrorCode::IntegrityFailure);
    let commit = client.commit(&plan, &session);
    assert_eq!(commit.status, 422);
    assert_eq!(error_code(&commit), HubPublishErrorCode::IntegrityFailure);
    assert_eq!(client.refs().unwrap(), refs);

    // Corruption after a verified upload is caught by the commit's re-verification.
    let retry = Plan::new("package v2", 'c', Some(first.revision));
    let (session, upload) = client.opened(&retry);
    assert_eq!(client.put(&upload, 0, &retry.bytes).status, 200);
    assert!(hub.corrupt_object(&upload.digest) > 0);
    let commit = client.commit(&retry, &session);
    assert_eq!(error_code(&commit), HubPublishErrorCode::IntegrityFailure);
    assert_eq!(client.refs().unwrap(), refs);
    assert_eq!(hub.revisions(&demo()).len(), 1);
}

#[test]
fn a_new_attempt_of_the_same_intent_supersedes_the_open_session() {
    let hub = ReferenceHub::new();
    let client = Client::new(&hub, demo());
    let first = client.publish(&Plan::new("package v1", 'b', None));

    // The attempt read revision one and uploaded its object, then lost a race:
    // another publication moved the repository.
    let mut attempt = Plan::new(
        "package v2 long enough to split",
        'c',
        Some(first.revision.clone()),
    );
    let (old_session, upload) = client.opened(&attempt);
    assert_eq!(client.put(&upload, 0, &attempt.bytes).status, 200);
    let racer = client.publish(&Plan::new("package v3", 'd', Some(first.revision.clone())));
    assert_eq!(
        error_code(&client.commit(&attempt, &old_session)),
        HubPublishErrorCode::RefConflict
    );

    // The rerun reads the new revision: same operation and intent, new request.
    attempt.expected_revision = Some(racer.revision.clone());
    let response = client.open(&attempt);
    assert_eq!(
        response.status,
        201,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    let SessionResponse::Open {
        session_id,
        uploads,
    } = serde_json::from_slice(&response.body).unwrap()
    else {
        panic!("expected an open session");
    };
    assert_ne!(session_id.as_str(), old_session);
    assert_eq!(
        uploads[0].received,
        attempt.bytes.len() as u64,
        "verified bytes carry over by digest and are not sent again"
    );
    assert_eq!(
        hub.handle(&HubRequest::new(HubMethod::Head, &upload.upload_url))
            .status,
        404,
        "the superseded session's upload location is closed"
    );
    let landed = receipt(&client.commit(&attempt, session_id.as_str()));
    assert_eq!(landed.previous_revision, Some(racer.revision.clone()));
    assert_eq!(hub.revisions(&demo()).len(), 3);
    // A committed operation replays; a different intent still conflicts.
    assert_eq!(receipt(&client.open(&attempt)), landed);
    let mut other = Plan::new("package v4", 'e', Some(landed.revision.clone()));
    other.operation = attempt.operation;
    assert_eq!(
        error_code(&client.open(&other)),
        HubPublishErrorCode::IdempotencyConflict
    );
}

#[test]
fn an_object_corrupted_at_rest_must_be_uploaded_again() {
    let hub = ReferenceHub::new();
    let client = Client::new(&hub, demo());
    let plan = Plan::new("package v1", 'b', None);
    let first = client.publish(&plan);
    let digest = plan.declaration().digest;
    assert_eq!(hub.corrupt_object(&digest), 1);

    // A later publication that lists the same object is not told it is
    // complete; re-uploading verified bytes repairs the stored copy.
    let mut again = Plan::new("package v1", 'b', Some(first.revision.clone()));
    again.intent_digest = sha256_digest(b"republish");
    let (session, upload) = client.opened(&again);
    assert_eq!(upload.received, 0);
    assert_eq!(client.put(&upload, 0, &again.bytes).status, 200);
    receipt(&client.commit(&again, &session));
    let object = hub.handle(&HubRequest::new(
        HubMethod::Get,
        client.capabilities.object_location(&digest),
    ));
    assert_eq!(object.body, plan.bytes);
}

#[test]
fn stale_expected_revision_is_a_ref_conflict() {
    let hub = ReferenceHub::new();
    let client = Client::new(&hub, demo());
    let first = client.publish(&Plan::new("package v1", 'b', None));
    let second = client.publish(&Plan::new("package v2", 'c', Some(first.revision.clone())));
    assert_eq!(
        second.previous_revision.as_deref(),
        Some(first.revision.as_str())
    );
    let refs = client.refs().unwrap();

    let stale = Plan::new("package v3", 'd', Some(first.revision));
    let (session, upload) = client.opened(&stale);
    assert_eq!(client.put(&upload, 0, &stale.bytes).status, 200);
    let response = client.commit(&stale, &session);
    assert_eq!(response.status, 412);
    assert_eq!(error_code(&response), HubPublishErrorCode::RefConflict);
    assert_eq!(client.refs().unwrap(), refs);
    assert_eq!(hub.revisions(&demo()).len(), 2);
}

#[test]
fn fork_creates_a_new_identity_and_create_if_absent_never_overwrites() {
    let hub = ReferenceHub::new();
    let origin = Client::new(&hub, demo());
    origin.publish(&Plan::new("package v1", 'b', None));
    let origin_refs = origin.refs().unwrap();

    let fork = Client::new(&hub, RepositoryIdentity::parse("alice/demo").unwrap());
    let receipt = fork.publish(&Plan::new("package v1", 'b', None));
    assert_eq!(receipt.repository.canonical_name(), "alice/demo");
    let fork_refs = RefSet::from_json(&fork.refs().unwrap(), DiscoveryLimits::default()).unwrap();
    assert_eq!(fork_refs.repository.canonical_name(), "alice/demo");

    let again = Plan::new("package fork attempt", 'e', None);
    let (session, upload) = origin.opened(&again);
    assert_eq!(origin.put(&upload, 0, &again.bytes).status, 200);
    let response = origin.commit(&again, &session);
    assert_eq!(response.status, 412);
    assert_eq!(error_code(&response), HubPublishErrorCode::RefConflict);
    assert_eq!(origin.refs().unwrap(), origin_refs);
}

#[test]
fn unknown_required_capability_fails_before_any_state_change() {
    let hub = ReferenceHub::new();
    let client = Client::new(&hub, demo());
    let plan = Plan::new("package v1", 'b', None);
    let mut request = OpenSessionRequest {
        format: HUB_PUBLISH_FORMAT.into(),
        operation_uuid: plan.operation,
        request_commitment: client.intent(&plan).request_commitment(),
        intent_digest: plan.intent_digest.clone(),
        repository: demo(),
        objects: vec![plan.declaration()],
        requirements: vec![graphforge_hub_publish::Capability::new(
            "server-side-merge",
            1,
        )],
    };
    let response = client.open_with(&request);
    assert_eq!(response.status, 422);
    assert_eq!(
        error_code(&response),
        HubPublishErrorCode::UnsupportedFuture
    );
    // No session was recorded under the operation: the valid request opens fresh.
    request.requirements.clear();
    assert_eq!(client.open_with(&request).status, 201);

    // Client side: a capabilities document requiring an unknown capability.
    let mut document = serde_json::to_value(&client.capabilities).unwrap();
    document["requirements"] = json!([{"capability": "server-side-merge", "major": 1}]);
    let error =
        PublishCapabilities::from_json(&serde_json::to_vec(&document).unwrap()).unwrap_err();
    assert_eq!(error.code, HubPublishErrorCode::UnsupportedFuture);
}

#[test]
fn expired_or_misscoped_tokens_are_auth_denied() {
    let hub = ReferenceHub::new();
    let mut client = Client::new(&hub, demo());
    let plan = Plan::new("package v1", 'b', None);

    client.token = PublishToken::new(hub.issue_token("publish:openalex/other", 900));
    let response = client.open(&plan);
    assert_eq!(
        (response.status, error_code(&response)),
        (403, HubPublishErrorCode::AuthDenied)
    );

    client.token = PublishToken::new(hub.issue_token("publish:openalex/demo", 30));
    hub.advance_clock(30);
    let response = client.open(&plan);
    assert_eq!(
        (response.status, error_code(&response)),
        (401, HubPublishErrorCode::AuthDenied)
    );
    // The error body never echoes the credential.
    assert!(!String::from_utf8_lossy(&response.body).contains(client.token.expose()));
    assert!(client.refs().is_none());
}

#[test]
fn oversized_declared_length_is_refused_at_open() {
    let config = ReferenceHubConfig::default();
    let max = config.limits.max_object_bytes;
    let hub = ReferenceHub::with_config(config).unwrap();
    let client = Client::new(&hub, demo());
    let plan = Plan::new("package v1", 'b', None);
    let mut declaration = plan.declaration();
    declaration.length = max + 1;
    let response = client.open_with(&OpenSessionRequest {
        format: HUB_PUBLISH_FORMAT.into(),
        operation_uuid: plan.operation,
        request_commitment: client.intent(&plan).request_commitment(),
        intent_digest: plan.intent_digest.clone(),
        repository: demo(),
        objects: vec![declaration],
        requirements: Vec::new(),
    });
    assert_eq!(response.status, 413);
    assert_eq!(error_code(&response), HubPublishErrorCode::InvalidInput);
    assert_eq!(client.open(&plan).status, 201, "nothing was recorded");
}

#[test]
fn device_flow_issues_a_scoped_short_lived_token() {
    let hub = ReferenceHub::new();
    let capabilities = Client::new(&hub, demo()).capabilities;
    let form = |pairs: &[(&str, &str)]| {
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        serializer.extend_pairs(pairs);
        serializer.finish().into_bytes()
    };
    let response = hub.handle(
        &HubRequest::new(
            HubMethod::Post,
            &capabilities.authorization.device_authorization_endpoint,
        )
        .with_body(form(&[
            ("client_id", "graphforge-cli"),
            ("scope", &capabilities.authorization.scope),
        ])),
    );
    let device: graphforge_hub_publish::DeviceAuthorizationResponse =
        serde_json::from_slice(&response.body).unwrap();
    let poll = || {
        let response = hub.handle(
            &HubRequest::new(HubMethod::Post, &capabilities.authorization.token_endpoint)
                .with_body(form(&[
                    ("grant_type", graphforge_hub_publish::DEVICE_CODE_GRANT_TYPE),
                    ("device_code", &device.device_code),
                    ("client_id", "graphforge-cli"),
                ])),
        );
        DevicePoll::from_response(response.status, &response.body)
    };
    assert_eq!(poll().unwrap(), DevicePoll::Pending);
    assert_eq!(poll().unwrap(), DevicePoll::SlowDown);
    assert!(hub.approve_device(&device.user_code));
    let DevicePoll::Granted(token) = poll().unwrap() else {
        panic!("expected a token");
    };
    assert_eq!(poll().unwrap_err().code, HubPublishErrorCode::AuthDenied);

    let mut client = Client::new(&hub, demo());
    client.token = token;
    let plan = Plan::new("package v1", 'b', None);
    client.publish(&plan);
    hub.advance_clock(900);
    let response = client.open(&Plan::new("package v2", 'c', None));
    assert_eq!(error_code(&response), HubPublishErrorCode::AuthDenied);
}

#[test]
fn negotiated_capabilities_validate_client_chunk_and_hex_without_ambiguous_templates() {
    let mut config = ReferenceHubConfig {
        authorization_client_id: Some("client_workos_example".into()),
        object_location_hex: true,
        ..ReferenceHubConfig::default()
    };
    config.limits.preferred_chunk_bytes = Some(8 * 1024 * 1024);
    let hub = ReferenceHub::with_config(config).unwrap();
    let caps = hub.capabilities(&demo());
    caps.validate().unwrap();
    let digest = sha256_digest(b"object");
    assert!(
        caps.object_location(&digest)
            .ends_with(&format!("/objects/sha256/{}", &digest[7..]))
    );
    for bad in ["", "with space", "line\nfeed", &"x".repeat(129)] {
        let mut value = caps.clone();
        value.authorization.client_id = Some(bad.into());
        assert!(value.validate().is_err());
    }
    for n in [0, 8 * 1024 * 1024 + 1] {
        let mut value = caps.clone();
        value.limits.preferred_chunk_bytes = Some(n);
        assert!(value.validate().is_err());
    }
    for template in [
        "https://hub.example/{digest}/{hex}",
        "https://hub.example/{hex}/{hex}",
        "https://hub.example/{hex}/{unknown}",
        "https://user@hub.example/{hex}",
    ] {
        let mut value = caps.clone();
        value.object_location_template = template.into();
        assert!(value.validate().is_err());
    }
    let mut value = serde_json::to_value(&caps).unwrap();
    value["requirements"] = json!([]);
    assert_eq!(
        PublishCapabilities::from_json(&serde_json::to_vec(&value).unwrap())
            .unwrap_err()
            .code,
        HubPublishErrorCode::InvalidInput
    );
    value["requirements"] = json!([{"capability":"publish-negotiation","major":2}]);
    value["authorization"]["unknown_field"] = json!(true);
    assert_eq!(
        PublishCapabilities::from_json(&serde_json::to_vec(&value).unwrap())
            .unwrap_err()
            .code,
        HubPublishErrorCode::UnsupportedFuture
    );
}

#[test]
fn optional_negotiation_members_are_nonnullable_and_legacy_omission_stays_valid() {
    let legacy = serde_json::to_value(ReferenceHub::new().capabilities(&demo())).unwrap();
    PublishCapabilities::from_json(&serde_json::to_vec(&legacy).unwrap()).unwrap();
    for pointer in ["/authorization", "/limits"] {
        let key = if pointer == "/authorization" {
            "client_id"
        } else {
            "preferred_chunk_bytes"
        };
        for required in [false, true] {
            let mut value = legacy.clone();
            value.pointer_mut(pointer).unwrap()[key] = Value::Null;
            if required {
                value["requirements"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"capability":"publish-negotiation","major":1}));
            }
            assert_eq!(
                PublishCapabilities::from_json(&serde_json::to_vec(&value).unwrap())
                    .unwrap_err()
                    .code,
                HubPublishErrorCode::InvalidInput
            );
        }
    }
}

#[test]
fn sessions_select_per_object_chunks_and_preserve_them_on_resume() {
    let mut config = ReferenceHubConfig::default();
    config.limits.preferred_chunk_bytes = Some(8);
    let hub = ReferenceHub::with_config(config).unwrap();
    let client = Client::new(&hub, demo());
    for text in ["abc", "abcdefghijkl"] {
        let plan = Plan::new(text, 'a', None);
        let (session, upload) = client.opened(&plan);
        let selected = (text.len() as u64).min(8);
        assert_eq!(upload.chunk_bytes, Some(selected));
        assert_eq!(
            client.capabilities.upload_chunk_bytes(&upload).unwrap(),
            Some(selected)
        );
        assert_eq!(client.put(&upload, 0, &plan.bytes[..2]).status, 200);
        let response = client.open(&plan);
        assert_eq!(response.status, 200);
        let SessionResponse::Open {
            session_id,
            uploads,
        } = serde_json::from_slice(&response.body).unwrap()
        else {
            panic!("expected a resumed session")
        };
        let resumed = &uploads[0];
        assert_eq!(session_id.as_str(), session);
        assert_eq!(resumed.received, 2);
        assert_eq!(resumed.chunk_bytes, Some(selected));
    }

    // A client that has not opted in still receives the legacy response shape.
    let plan = Plan::new("legacy session", 'b', None);
    let response = client.open_with(&OpenSessionRequest {
        format: HUB_PUBLISH_FORMAT.into(),
        operation_uuid: plan.operation,
        request_commitment: client.intent(&plan).request_commitment(),
        intent_digest: plan.intent_digest.clone(),
        repository: demo(),
        objects: vec![plan.declaration()],
        requirements: vec![],
    });
    assert_eq!(response.status, 201);
    let legacy: Value = serde_json::from_slice(&response.body).unwrap();
    assert!(legacy["uploads"][0].get("chunk_bytes").is_none());
    let mut invalid = legacy.clone();
    invalid["uploads"][0]["chunk_bytes"] = Value::Null;
    assert!(serde_json::from_value::<SessionResponse>(invalid).is_err());

    let (_, mut upload) = client.opened(&Plan::new("validate chunks", 'c', None));
    for bytes in [0, client.capabilities.limits.max_chunk_bytes + 1] {
        upload.chunk_bytes = Some(bytes);
        assert!(client.capabilities.upload_chunk_bytes(&upload).is_err());
    }
    upload.chunk_bytes = Some(4);
    let legacy_caps = ReferenceHub::new().capabilities(&demo());
    assert!(legacy_caps.upload_chunk_bytes(&upload).is_err());
    upload.chunk_bytes = None;
    assert_eq!(
        client.capabilities.upload_chunk_bytes(&upload).unwrap(),
        Some(8)
    );
    assert_eq!(legacy_caps.upload_chunk_bytes(&upload).unwrap(), None);
}

fn async_hub(chunk: u64, object_limit: u64) -> ReferenceHub {
    let mut config = ReferenceHubConfig {
        large_objects: true,
        ..ReferenceHubConfig::default()
    };
    config.limits.max_chunk_bytes = chunk;
    config.limits.preferred_chunk_bytes = Some(chunk);
    config.limits.max_object_bytes = object_limit;
    config.limits.max_session_bytes = object_limit;
    ReferenceHub::with_config(config).unwrap()
}

#[test]
fn async_commit_rechecks_retained_bytes_and_never_advances_unverified_refs() {
    let hub = async_hub(4, 1024);
    let client = Client::new(&hub, demo());
    let plan = Plan::new("abcdefgh", 'd', None);
    let (session, upload) = client.opened(&plan);
    assert_eq!(client.put(&upload, 0, b"abc").status, 400);
    assert_eq!(client.put(&upload, 0, b"abcd").status, 200);
    let resumed: SessionResponse = serde_json::from_slice(&client.open(&plan).body).unwrap();
    assert!(matches!(resumed, SessionResponse::Open { uploads, .. } if uploads[0].received == 4));
    assert_eq!(client.put(&upload, 4, b"efgh").status, 200);
    assert_eq!(hub.corrupt_object(&upload.digest), 1);
    assert_eq!(client.commit(&plan, &session).status, 202);
    assert!(client.refs().is_none());
    assert_eq!(
        error_code(&client.operation(plan.operation)),
        HubPublishErrorCode::IntegrityFailure
    );
    assert!(client.refs().is_none());
    let retry = client.open(&plan);
    assert_eq!(retry.status, 200);
    let SessionResponse::Open {
        session_id,
        mut uploads,
    } = serde_json::from_slice(&retry.body).unwrap()
    else {
        panic!("expected reopened session")
    };
    let retry_session = session_id.as_str().to_owned();
    let retry_upload = uploads.remove(0);
    assert_eq!(
        retry_upload.received, 0,
        "same request discards known-corrupt bytes"
    );
    for (index, part) in plan.bytes.chunks(4).enumerate() {
        assert_eq!(client.put(&retry_upload, index * 4, part).status, 200);
    }
    // A different request commitment for the same intent supersedes this attempt;
    // known-corrupt carried bytes must not survive that path either.
    assert_eq!(hub.corrupt_object(&retry_upload.digest), 1);
    let mut next = Plan::new("abcdefgh", 'e', None);
    next.operation = plan.operation;
    let (next_session, next_upload) = client.opened(&next);
    assert_ne!(next_session, retry_session);
    assert_eq!(next_upload.received, 0);
    for (index, part) in next.bytes.chunks(4).enumerate() {
        assert_eq!(client.put(&next_upload, index * 4, part).status, 200);
    }
    assert_eq!(client.commit(&next, &next_session).status, 202);
    assert!(
        OperationStatus::from_json(&client.operation(next.operation).body)
            .unwrap()
            .receipt
            .is_some()
    );
    assert_eq!(hub.revisions(&demo()).len(), 1);
}

#[test]
fn async_commit_replay_and_final_compare_and_swap_remain_authoritative() {
    let hub = async_hub(4, 1024);
    let client = Client::new(&hub, demo());
    let first = Plan::new("abcdefgh", 'c', None);
    let second = Plan::new("ijklmnop", 'd', None);
    let (a, au) = client.opened(&first);
    let (b, bu) = client.opened(&second);
    for (upload, bytes) in [(&au, &first.bytes), (&bu, &second.bytes)] {
        for (index, part) in bytes.chunks(4).enumerate() {
            assert_eq!(client.put(upload, index * 4, part).status, 200);
        }
    }
    assert_eq!(client.commit(&first, &a).status, 202);
    assert_eq!(client.commit(&second, &b).status, 202);
    assert!(client.refs().is_none());
    let status = OperationStatus::from_json(&client.operation(first.operation).body).unwrap();
    let original = status.receipt.unwrap();
    assert_eq!(receipt(&client.commit(&first, &a)), original);
    assert_eq!(
        error_code(&client.operation(second.operation)),
        HubPublishErrorCode::RefConflict
    );
    assert_eq!(hub.revisions(&demo()).len(), 1);
}

#[test]
fn async_verification_requires_fresh_authorization_and_enforces_part_ceiling() {
    let hub = async_hub(4, 40_000);
    let client = Client::new(&hub, demo());
    let plan = Plan::new("abcdefgh", 'c', None);
    let (session, target) = client.opened(&plan);
    for (index, part) in plan.bytes.chunks(4).enumerate() {
        assert_eq!(client.put(&target, index * 4, part).status, 200);
    }
    assert_eq!(client.commit(&plan, &session).status, 202);
    hub.advance_clock(901);
    assert_eq!(
        error_code(&client.operation(plan.operation)),
        HubPublishErrorCode::AuthDenied
    );
    assert!(client.refs().is_none());
    let client = Client::new(&hub, demo());
    let completed = OperationStatus::from_json(&client.operation(plan.operation).body).unwrap();
    assert!(
        completed.receipt.is_some(),
        "fresh bearer completes the same pending operation"
    );
    assert!(client.refs().is_some());
    let mut too_many = Plan::new("a", 'e', None);
    too_many.bytes = vec![0; 40_001];
    assert_eq!(client.open(&too_many).status, 413);
}

/// Actual bytes traverse the ordinary upload, sparse-file storage, and async
/// SHA-256 re-verification paths. This is intentionally opt-in, not a 200-GiB
/// allocation or a replacement hasher/counter-only simulation.
#[test]
#[ignore = "operator-scale real 200 GiB traversal; run optimized explicitly"]
#[allow(clippy::too_many_lines)]
fn actual_200_gib_object_publishes_in_uniform_64_mib_parts() {
    use sha2::{Digest as _, Sha256};
    use std::fmt::Write as _;
    use std::time::{Duration, Instant};
    const PART: u64 = 64 * 1024 * 1024;
    const TOTAL: u64 = 200 * 1024 * 1024 * 1024;
    let body = vec![0; usize::try_from(PART).unwrap()];
    let started = Instant::now();
    let mut hash = Sha256::new();
    for _ in 0..TOTAL / PART {
        hash.update(&body);
    }
    let mut digest = String::from("sha256:");
    for byte in hash.finalize() {
        write!(&mut digest, "{byte:02x}").unwrap();
    }
    let hub = async_hub(PART, TOTAL);
    let client = Client::new(&hub, demo());
    let plan = Plan::new("generated zero extent", 'a', None);
    let object = ObjectDeclaration {
        digest: digest.clone(),
        length: TOTAL,
        media_type: PORTABLE_V2_MEDIA_TYPE.into(),
    };
    let mut manifest = client.manifest(&plan);
    manifest["package"]["object_digest"] = json!(digest);
    manifest["objects"][0]["digest"] = json!(digest);
    manifest["objects"][0]["length"] = json!(TOTAL);
    manifest["objects"][0]["locations"] = json!([client.capabilities.object_location(&digest)]);
    let validated = DiscoveryManifest::from_json(
        &serde_json::to_vec(&manifest).unwrap(),
        DiscoveryLimits {
            max_cumulative_object_bytes: TOTAL,
            ..DiscoveryLimits::default()
        },
    )
    .unwrap();
    let intent = PublishIntent {
        repository: demo(),
        intent_digest: plan.intent_digest.clone(),
        objects: vec![object.clone()],
        manifest_validator: validated.canonical_digest().unwrap().0,
        refs: vec!["main".into()],
        expected_revision: None,
    };
    let opened = client.open_with(&OpenSessionRequest {
        format: HUB_PUBLISH_FORMAT.into(),
        operation_uuid: plan.operation,
        request_commitment: intent.request_commitment(),
        intent_digest: plan.intent_digest.clone(),
        repository: demo(),
        objects: vec![object],
        requirements: client.capabilities.requirements.clone(),
    });
    assert_eq!(opened.status, 201);
    let SessionResponse::Open {
        session_id,
        uploads,
    } = serde_json::from_slice(&opened.body).unwrap()
    else {
        panic!("open")
    };
    let target = &uploads[0];
    let mut max_request = Duration::ZERO;
    for offset in (0..TOTAL).step_by(usize::try_from(PART).unwrap()) {
        let request = HubRequest::new(HubMethod::Put, &target.upload_url)
            .with_header("content-range", content_range(offset, PART, TOTAL))
            .with_body(body.clone());
        let now = Instant::now();
        assert_eq!(hub.handle(&request).status, 200);
        max_request = max_request.max(now.elapsed());
    }
    let commit = HubRequest::new(
        HubMethod::Post,
        format!(
            "{}/.gf/publish/sessions/{session_id}/commit",
            hub.repository_url(&demo())
        ),
    )
    .with_header("authorization", client.token.authorization())
    .with_json(&CommitRequest {
        manifest,
        refs: vec!["main".into()],
        expected_revision: None,
    });
    let now = Instant::now();
    assert_eq!(hub.handle(&commit).status, 202);
    max_request = max_request.max(now.elapsed());
    assert!(client.refs().is_none());
    let mut polls = 0;
    loop {
        let now = Instant::now();
        let response = client.operation(plan.operation);
        max_request = max_request.max(now.elapsed());
        assert_eq!(response.status, 200);
        polls += 1;
        if OperationStatus::from_json(&response.body)
            .unwrap()
            .receipt
            .is_some()
        {
            break;
        }
        assert!(polls <= TOTAL / PART);
    }
    assert_eq!(polls, TOTAL / PART);
    assert_eq!(hub.revisions(&demo()).len(), 1);
    assert!(client.refs().is_some());
    // Existing five-minute request budget, not a Cloudflare CPU claim.
    assert!(
        max_request < Duration::from_mins(5),
        "max request {max_request:?}"
    );
    eprintln!(
        "actual_bytes={TOTAL} upload_parts={} verification_polls={polls} max_request_ms={} elapsed_s={}",
        TOTAL / PART,
        max_request.as_millis(),
        started.elapsed().as_secs()
    );
}
