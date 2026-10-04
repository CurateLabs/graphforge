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
use serde_json::json;
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
            requirements: Vec::new(),
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
            SessionResponse::Complete { .. } => panic!("expected an open session"),
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
        SessionResponse::Open { .. } => panic!("expected a receipt"),
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
