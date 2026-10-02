//! Deterministic generator and replay test for the public Hub publish artifacts.
//!
//! The JSON Schemas and the conformance corpus under
//! `docs/reference/hub-publish/v1/` are generated from this file. Without
//! `GRAPHFORGE_UPDATE_HUB_PUBLISH_ARTIFACTS` the test compares the checked-in
//! bytes with the generated ones, then replays every corpus case through a
//! fresh [`ReferenceHub`].
#![allow(
    clippy::large_enum_variant,
    clippy::needless_pass_by_value,
    clippy::too_many_lines
)]

use graphforge_discovery::{
    DISCOVERY_FORMAT, DiscoveryLimits, DiscoveryManifest, PORTABLE_V2_MEDIA_TYPE, ProtocolVersion,
    RefSet, RepositoryIdentity, RepositoryRef, Sha256Digest,
};
use graphforge_hub_publish::{
    DEVICE_CODE_GRANT_TYPE, HubMethod, HubRequest, HubResponse, ObjectDeclaration, PublishIntent,
    ReferenceHub, sha256_digest,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

const HUB: &str = "https://hub.example";
const ARTIFACTS: [(&str, &str); 8] = [
    (
        "capabilities.schema.json",
        include_str!("../../../docs/reference/hub-publish/v1/capabilities.schema.json"),
    ),
    (
        "session-request.schema.json",
        include_str!("../../../docs/reference/hub-publish/v1/session-request.schema.json"),
    ),
    (
        "session-response.schema.json",
        include_str!("../../../docs/reference/hub-publish/v1/session-response.schema.json"),
    ),
    (
        "commit-request.schema.json",
        include_str!("../../../docs/reference/hub-publish/v1/commit-request.schema.json"),
    ),
    (
        "operation-status.schema.json",
        include_str!("../../../docs/reference/hub-publish/v1/operation-status.schema.json"),
    ),
    (
        "upload-status.schema.json",
        include_str!("../../../docs/reference/hub-publish/v1/upload-status.schema.json"),
    ),
    (
        "error.schema.json",
        include_str!("../../../docs/reference/hub-publish/v1/error.schema.json"),
    ),
    (
        "conformance.json",
        include_str!("../../../docs/reference/hub-publish/v1/conformance.json"),
    ),
];

// ---------------------------------------------------------------- corpus model

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Corpus {
    format: String,
    hub: String,
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Case {
    name: String,
    description: String,
    steps: Vec<Step>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum Step {
    Request { request: Request, expect: Expect },
    Action { action: Action },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Request {
    method: HubMethod,
    url: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    json: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    form: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    body_text: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Expect {
    status: u16,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    headers: BTreeMap<String, String>,
    /// Exact JSON body after variable substitution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    json: Option<Value>,
    /// Error body `{code, message}` with this code and any message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    /// RFC 6749 error body `{error}` with this value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    oauth_error: Option<String>,
    /// Exact UTF-8 body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    body_text: Option<String>,
    /// Variables captured from the JSON body by JSON pointer, before matching.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    capture: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum Action {
    IssueToken {
        scope: String,
        ttl_seconds: u64,
        capture: String,
    },
    AdvanceClock {
        seconds: u64,
    },
    ApproveDevice {
        user_code: String,
    },
    DenyDevice {
        user_code: String,
    },
    SetQuota {
        owner: String,
        bytes: u64,
    },
    CorruptObject {
        digest: String,
    },
}

// ------------------------------------------------------------ corpus builders

fn identity(owner: &str, repository: &str) -> RepositoryIdentity {
    RepositoryIdentity {
        owner: owner.to_owned(),
        repository: repository.to_owned(),
    }
}

fn repo_url(repository: &RepositoryIdentity) -> String {
    format!("{HUB}/{}/{}", repository.owner, repository.repository)
}

fn operation(n: u128) -> Uuid {
    Uuid::from_u128(0x0000_0000_0000_7000_8000_0000_0000_0000 | n)
}

fn digest_of(c: char) -> String {
    format!("sha256:{}", c.to_string().repeat(64))
}

/// One object to publish.
#[derive(Clone)]
struct Object {
    media_type: &'static str,
    bytes: Vec<u8>,
}

impl Object {
    fn package(text: &str) -> Self {
        Self {
            media_type: PORTABLE_V2_MEDIA_TYPE,
            bytes: text.as_bytes().to_vec(),
        }
    }

    fn other(media_type: &'static str, text: &str) -> Self {
        Self {
            media_type,
            bytes: text.as_bytes().to_vec(),
        }
    }

    fn digest(&self) -> String {
        sha256_digest(&self.bytes)
    }

    fn text(&self) -> String {
        String::from_utf8(self.bytes.clone()).unwrap()
    }
}

/// Everything one publication sends, modelled independently of the Hub.
#[derive(Clone)]
struct Publication {
    repository: RepositoryIdentity,
    operation: Uuid,
    objects: Vec<Object>,
    package_digest: String,
    refs: Vec<String>,
    resolved_ref: String,
    expected_revision: Option<String>,
    /// Object location prefix; `None` uses this Hub's object locations.
    location_base: Option<String>,
}

impl Publication {
    fn new(repository: &RepositoryIdentity, operation: Uuid, objects: Vec<Object>) -> Self {
        let mut objects = objects;
        objects.sort_by_key(Object::digest);
        Self {
            repository: repository.clone(),
            operation,
            objects,
            package_digest: digest_of('b'),
            refs: vec!["main".into()],
            resolved_ref: "main".into(),
            expected_revision: None,
            location_base: None,
        }
    }

    fn expecting(mut self, revision: Option<String>) -> Self {
        self.expected_revision = revision;
        self
    }

    fn declarations(&self) -> Vec<ObjectDeclaration> {
        self.objects
            .iter()
            .map(|object| ObjectDeclaration {
                digest: object.digest(),
                length: object.bytes.len() as u64,
                media_type: object.media_type.to_owned(),
            })
            .collect()
    }

    fn package_object(&self) -> &Object {
        self.objects
            .iter()
            .find(|object| object.media_type == PORTABLE_V2_MEDIA_TYPE)
            .unwrap()
    }

    fn manifest(&self) -> Value {
        json!({
            "format": DISCOVERY_FORMAT,
            "version": {"major": 1, "minor": 1},
            "repository": self.repository,
            "default_ref": "main",
            "resolved_ref": self.resolved_ref,
            "immutable_version": self.package_digest,
            "package": {
                "format": "graphforge-project/2",
                "package_digest": self.package_digest,
                "object_digest": self.package_object().digest(),
            },
            "requirements": [{"capability": "portable-v2", "major": 1}],
            "capabilities": [{"capability": "range-requests", "major": 1}],
            "objects": self.objects.iter().map(|object| json!({
                "digest": object.digest(),
                "length": object.bytes.len(),
                "media_type": object.media_type,
                "locations": [format!(
                    "{}/{}",
                    self.location_base.clone().unwrap_or_else(|| format!("{}/.gf/objects", repo_url(&self.repository))),
                    object.digest(),
                )],
            })).collect::<Vec<_>>(),
        })
    }

    fn manifest_document(&self) -> Vec<u8> {
        DiscoveryManifest::from_json(
            &serde_json::to_vec(&self.manifest()).unwrap(),
            DiscoveryLimits::default(),
        )
        .unwrap()
        .to_canonical_json()
        .unwrap()
    }

    fn manifest_validator(&self) -> String {
        sha256_digest(&self.manifest_document())
    }

    fn commitment(&self) -> String {
        PublishIntent {
            repository: self.repository.clone(),
            intent_digest: self.intent_digest(),
            objects: self.declarations(),
            manifest_validator: self.manifest_validator(),
            refs: self.refs.clone(),
            expected_revision: self.expected_revision.clone(),
        }
        .request_commitment()
    }

    /// The corpus models one intent per operation identity.
    fn intent_digest(&self) -> String {
        sha256_digest(format!("publish intent {}", self.operation).as_bytes())
    }

    fn open_body(&self) -> Value {
        json!({
            "format": "graphforge-hub-publish/1",
            "operation_uuid": self.operation,
            "request_commitment": self.commitment(),
            "intent_digest": self.intent_digest(),
            "repository": self.repository,
            "objects": self.declarations(),
        })
    }

    fn commit_body(&self) -> Value {
        json!({
            "manifest": self.manifest(),
            "refs": self.refs,
            "expected_revision": self.expected_revision,
        })
    }

    fn receipt(&self, revision: &str) -> Value {
        json!({
            "format": "graphforge-hub-publish/1",
            "operation_uuid": self.operation,
            "request_commitment": self.commitment(),
            "repository": self.repository,
            "manifest_validator": self.manifest_validator(),
            "refs": self.refs,
            "previous_revision": self.expected_revision,
            "revision": revision,
        })
    }
}

/// The refs a repository holds, modelled with discovery types.
#[derive(Clone, Default)]
struct RepoModel {
    refs: BTreeMap<String, RepositoryRef>,
}

impl RepoModel {
    fn advance(&mut self, publication: &Publication) {
        for name in &publication.refs {
            self.refs.insert(
                name.clone(),
                RepositoryRef {
                    name: name.clone(),
                    target: Sha256Digest(publication.package_digest.clone()),
                    validator: Sha256Digest(publication.manifest_validator()),
                },
            );
        }
    }

    fn document(&self, repository: &RepositoryIdentity) -> Vec<u8> {
        RefSet {
            format: DISCOVERY_FORMAT.to_owned(),
            version: ProtocolVersion::CURRENT,
            repository: repository.clone(),
            default_ref: "main".to_owned(),
            refs: self.refs.values().cloned().collect(),
            extensions: BTreeMap::new(),
        }
        .to_canonical_json()
        .unwrap()
    }

    fn revision(&self, repository: &RepositoryIdentity) -> String {
        sha256_digest(&self.document(repository))
    }
}

fn request(method: HubMethod, url: impl Into<String>) -> Request {
    Request {
        method,
        url: url.into(),
        headers: BTreeMap::new(),
        json: None,
        form: None,
        body_text: None,
    }
}

impl Request {
    fn bearer(mut self, variable: &str) -> Self {
        self.headers
            .insert("authorization".into(), format!("Bearer ${{{variable}}}"));
        self
    }

    fn json(mut self, value: Value) -> Self {
        self.headers
            .insert("content-type".into(), "application/json".into());
        self.json = Some(value);
        self
    }

    fn header(mut self, name: &str, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    fn text(mut self, value: impl Into<String>) -> Self {
        self.body_text = Some(value.into());
        self
    }

    fn form(mut self, pairs: &[(&str, &str)]) -> Self {
        self.headers.insert(
            "content-type".into(),
            "application/x-www-form-urlencoded".into(),
        );
        self.form = Some(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        );
        self
    }
}

fn status(code: u16) -> Expect {
    Expect {
        status: code,
        ..Expect::default()
    }
}

fn error(code: u16, error: &str) -> Expect {
    Expect {
        status: code,
        error: Some(error.to_owned()),
        ..Expect::default()
    }
}

fn oauth(error: &str) -> Expect {
    Expect {
        status: 400,
        oauth_error: Some(error.to_owned()),
        ..Expect::default()
    }
}

impl Expect {
    fn json(mut self, value: Value) -> Self {
        self.json = Some(value);
        self
    }

    fn header(mut self, name: &str, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    fn capture(mut self, variable: &str, pointer: &str) -> Self {
        self.capture.insert(variable.into(), pointer.into());
        self
    }

    fn text(mut self, value: impl Into<String>) -> Self {
        self.body_text = Some(value.into());
        self
    }
}

fn send(request: Request, expect: Expect) -> Step {
    Step::Request { request, expect }
}

fn act(action: Action) -> Step {
    Step::Action { action }
}

fn token(repository: &RepositoryIdentity, variable: &str) -> Step {
    act(Action::IssueToken {
        scope: format!("publish:{}/{}", repository.owner, repository.repository),
        ttl_seconds: 900,
        capture: variable.into(),
    })
}

fn open_new(publication: &Publication, token: &str, prefix: &str) -> Step {
    let mut expect = status(201)
        .json(json!({
            "state": "open",
            "session_id": format!("${{{prefix}_session}}"),
            "uploads": publication.objects.iter().enumerate().map(|(index, object)| json!({
                "digest": object.digest(),
                "length": object.bytes.len(),
                "upload_url": format!("${{{prefix}_upload_{index}}}"),
                "received": 0,
            })).collect::<Vec<_>>(),
        }))
        .capture(&format!("{prefix}_session"), "/session_id");
    for index in 0..publication.objects.len() {
        expect = expect.capture(
            &format!("{prefix}_upload_{index}"),
            &format!("/uploads/{index}/upload_url"),
        );
    }
    send(
        request(
            HubMethod::Post,
            format!("{}/.gf/publish/sessions", repo_url(&publication.repository)),
        )
        .bearer(token)
        .json(publication.open_body()),
        expect,
    )
}

fn put_chunk(
    publication: &Publication,
    prefix: &str,
    index: usize,
    start: usize,
    end: usize,
) -> Step {
    let object = &publication.objects[index];
    let total = object.bytes.len();
    send(
        request(HubMethod::Put, format!("${{{prefix}_upload_{index}}}"))
            .header(
                "content-range",
                format!("bytes {start}-{}/{total}", end - 1),
            )
            .text(String::from_utf8(object.bytes[start..end].to_vec()).unwrap()),
        status(200)
            .header("upload-offset", end.to_string())
            .json(json!({"digest": object.digest(), "length": total, "received": end})),
    )
}

fn upload_all(publication: &Publication, prefix: &str) -> Vec<Step> {
    (0..publication.objects.len())
        .map(|index| {
            put_chunk(
                publication,
                prefix,
                index,
                0,
                publication.objects[index].bytes.len(),
            )
        })
        .collect()
}

fn commit_url(publication: &Publication, prefix: &str) -> String {
    format!(
        "{}/.gf/publish/sessions/${{{prefix}_session}}/commit",
        repo_url(&publication.repository)
    )
}

fn commit(publication: &Publication, token: &str, prefix: &str, expect: Expect) -> Step {
    send(
        request(HubMethod::Post, commit_url(publication, prefix))
            .bearer(token)
            .json(publication.commit_body()),
        expect,
    )
}

fn complete(publication: &Publication, revision: &str) -> Expect {
    status(200).json(json!({"state": "complete", "receipt": publication.receipt(revision)}))
}

/// Open, upload every object whole, and commit; updates `model`.
fn publish(
    publication: &Publication,
    token: &str,
    prefix: &str,
    model: &mut RepoModel,
) -> Vec<Step> {
    let mut steps = vec![open_new(publication, token, prefix)];
    steps.extend(upload_all(publication, prefix));
    model.advance(publication);
    let revision = model.revision(&publication.repository);
    steps.push(commit(
        publication,
        token,
        prefix,
        complete(publication, &revision),
    ));
    steps
}

fn get_refs(repository: &RepositoryIdentity, model: &RepoModel) -> Step {
    let document = model.document(repository);
    send(
        request(HubMethod::Get, format!("{}/.gf/refs", repo_url(repository))),
        status(200)
            .header("etag", format!("\"{}\"", sha256_digest(&document)))
            .text(String::from_utf8(document).unwrap()),
    )
}

fn refs_absent(repository: &RepositoryIdentity) -> Step {
    send(
        request(HubMethod::Get, format!("{}/.gf/refs", repo_url(repository))),
        error(404, "invalid_input"),
    )
}

fn case(name: &str, description: &str, steps: Vec<Step>) -> Case {
    Case {
        name: name.into(),
        description: description.into(),
        steps,
    }
}

fn demo() -> RepositoryIdentity {
    identity("openalex", "demo")
}

fn v1(operation_id: u128) -> Publication {
    Publication::new(
        &demo(),
        operation(operation_id),
        vec![Object::package(
            "graphforge project package fixture: version one",
        )],
    )
}

fn v2(operation_id: u128, expected: Option<String>) -> Publication {
    let mut publication = Publication::new(
        &demo(),
        operation(operation_id),
        vec![Object::package(
            "graphforge project package fixture: version two",
        )],
    )
    .expecting(expected);
    publication.package_digest = digest_of('c');
    publication
}

fn corpus() -> Corpus {
    let repository = demo();
    let mut cases = Vec::new();

    // Capabilities.
    cases.push(case(
        "capabilities-document",
        "The capabilities document names the format, required capabilities, device-flow endpoints, object locations, and limits.",
        vec![send(
            request(HubMethod::Get, format!("{}/.gf/publish", repo_url(&repository))),
            status(200).json(json!({
                "format": "graphforge-hub-publish/1",
                "repository": repository,
                "requirements": [
                    {"capability": "publish-session", "major": 1},
                    {"capability": "resumable-upload", "major": 1},
                ],
                "capabilities": [{"capability": "device-authorization", "major": 1}],
                "authorization": {
                    "device_authorization_endpoint": format!("{HUB}/.gf/oauth/device_authorization"),
                    "token_endpoint": format!("{HUB}/.gf/oauth/token"),
                    "scope": "publish:openalex/demo",
                },
                "object_location_template": format!("{}/.gf/objects/{{digest}}", repo_url(&repository)),
                "limits": {
                    "max_object_bytes": 67_108_864,
                    "max_objects": 1024,
                    "max_session_bytes": 268_435_456,
                    "max_chunk_bytes": 8_388_608,
                    "max_document_bytes": 4_194_304,
                },
            })),
        )],
    ));

    // Create, then read back the way `gf clone` does.
    {
        let publication = v1(1);
        let mut model = RepoModel::default();
        let mut steps = vec![token(&repository, "token")];
        steps.extend(publish(&publication, "token", "first", &mut model));
        steps.push(get_refs(&repository, &model));
        let manifest = publication.manifest_document();
        steps.push(send(
            request(
                HubMethod::Get,
                format!("{}/.gf/manifest", repo_url(&repository)),
            ),
            status(200)
                .header("etag", format!("\"{}\"", sha256_digest(&manifest)))
                .text(String::from_utf8(manifest).unwrap()),
        ));
        let package = publication.package_object();
        let object_url = format!("{}/.gf/objects/{}", repo_url(&repository), package.digest());
        steps.push(send(
            request(HubMethod::Get, &object_url),
            status(200)
                .header("etag", format!("\"{}\"", package.digest()))
                .header("accept-ranges", "bytes")
                .text(package.text()),
        ));
        steps.push(send(
            request(HubMethod::Get, &object_url)
                .header("range", "bytes=10-")
                .header("if-range", format!("\"{}\"", package.digest())),
            status(206)
                .header(
                    "content-range",
                    format!(
                        "bytes 10-{}/{}",
                        package.bytes.len() - 1,
                        package.bytes.len()
                    ),
                )
                .text(String::from_utf8(package.bytes[10..].to_vec()).unwrap()),
        ));
        steps.push(send(
            request(HubMethod::Get, &object_url)
                .header("range", "bytes=10-")
                .header("if-range", "\"stale\""),
            status(200).text(package.text()),
        ));
        cases.push(case(
            "create-and-read-back",
            "Create-if-absent publication; refs, manifest, and object reads (with resumable Range and If-Range) serve exactly the published bytes.",
            steps,
        ));
    }

    // Same session twice.
    {
        let publication = v1(2);
        let mut model = RepoModel::default();
        let mut steps = vec![token(&repository, "token")];
        steps.extend(publish(&publication, "token", "first", &mut model));
        let revision = model.revision(&repository);
        steps.push(send(
            request(
                HubMethod::Post,
                format!("{}/.gf/publish/sessions", repo_url(&repository)),
            )
            .bearer("token")
            .json(publication.open_body()),
            complete(&publication, &revision),
        ));
        steps.push(commit(
            &publication,
            "token",
            "first",
            complete(&publication, &revision),
        ));
        steps.push(get_refs(&repository, &model));
        cases.push(case(
            "same-publication-twice-returns-original-receipt",
            "Reopening and recommitting the same operation with the same commitment returns the original receipt before any upload; the repository revision does not change.",
            steps,
        ));
    }

    // Operation status before and after commit.
    {
        let publication = v1(21);
        let mut model = RepoModel::default();
        let status_url = format!(
            "{}/.gf/publish/operations/{}",
            repo_url(&repository),
            publication.operation
        );
        let mut steps = vec![
            token(&repository, "token"),
            send(
                request(HubMethod::Get, &status_url).bearer("token"),
                error(404, "invalid_input"),
            ),
        ];
        steps.extend(publish(&publication, "token", "first", &mut model));
        let revision = model.revision(&repository);
        steps.push(send(
            request(HubMethod::Get, &status_url).bearer("token"),
            status(200).json(json!({
                "format": "graphforge-hub-publish/1",
                "operation_uuid": publication.operation,
                "repository": repository,
                "intent_digest": publication.intent_digest(),
                "receipt": publication.receipt(&revision),
            })),
        ));
        steps.push(send(
            request(HubMethod::Get, &status_url),
            error(401, "auth_denied"),
        ));
        cases.push(case(
            "operation-status-returns-intent-and-original-receipt",
            "An authorized operation lookup is 404 before the operation opens and afterwards returns its intent digest and, once committed, the original receipt, so a retry is classified before any package is derived.",
            steps,
        ));
    }

    // Changed content under the same operation identity.
    {
        let publication = v1(3);
        let mut model = RepoModel::default();
        let mut steps = vec![token(&repository, "token")];
        steps.extend(publish(&publication, "token", "first", &mut model));
        let mut changed = v2(3, Some(model.revision(&repository)));
        changed.operation = publication.operation;
        steps.push(send(
            request(
                HubMethod::Post,
                format!("{}/.gf/publish/sessions", repo_url(&repository)),
            )
            .bearer("token")
            .json(changed.open_body()),
            error(409, "idempotency_conflict"),
        ));
        steps.push(get_refs(&repository, &model));
        cases.push(case(
            "changed-content-same-operation-conflicts",
            "A different request commitment under a committed operation identity fails idempotency_conflict (GF_IDEMPOTENCY_CONFLICT) and changes nothing.",
            steps,
        ));
    }

    // Commit content that does not match the declared commitment.
    {
        let publication = v1(4);
        let mut drifted = publication.clone();
        drifted.package_digest = digest_of('d');
        cases.push(case(
            "commit-content-must-match-commitment",
            "A commit whose manifest differs from the one the request commitment bound fails idempotency_conflict; the repository is not created.",
            vec![
                token(&repository, "token"),
                open_new(&publication, "token", "first"),
                upload_all(&publication, "first").remove(0),
                send(
                    request(HubMethod::Post, commit_url(&publication, "first"))
                        .bearer("token")
                        .json(drifted.commit_body()),
                    error(409, "idempotency_conflict"),
                ),
                refs_absent(&repository),
            ],
        ));
    }

    // Interrupted upload resumes.
    {
        let publication = v1(5);
        let object = &publication.objects[0];
        let total = object.bytes.len();
        let mut model = RepoModel::default();
        model.advance(&publication);
        let revision = model.revision(&repository);
        cases.push(case(
            "interrupted-upload-resumes",
            "After a partial PUT, a retry at the wrong offset is refused with the retained offset, HEAD reports it, the remaining bytes append, and the commit succeeds.",
            vec![
                token(&repository, "token"),
                open_new(&publication, "token", "first"),
                put_chunk(&publication, "first", 0, 0, 10),
                send(
                    request(HubMethod::Put, "${first_upload_0}")
                        .header("content-range", format!("bytes 0-{}/{total}", total - 1))
                        .text(object.text()),
                    error(416, "invalid_input").header("upload-offset", "10"),
                ),
                send(
                    request(HubMethod::Head, "${first_upload_0}"),
                    status(200)
                        .header("upload-offset", "10")
                        .header("upload-length", total.to_string())
                        .text(""),
                ),
                send(
                    request(
                        HubMethod::Post,
                        format!("{}/.gf/publish/sessions", repo_url(&repository)),
                    )
                    .bearer("token")
                    .json(publication.open_body()),
                    status(200).json(json!({
                        "state": "open",
                        "session_id": "${first_session}",
                        "uploads": [{"digest": object.digest(), "length": total, "upload_url": "${first_upload_0}", "received": 10}],
                    })),
                ),
                put_chunk(&publication, "first", 0, 10, total),
                commit(&publication, "token", "first", complete(&publication, &revision)),
                get_refs(&repository, &model),
            ],
        ));
    }

    // Corrupt upload refused; refs unchanged.
    {
        let first = v1(6);
        let mut model = RepoModel::default();
        let mut steps = vec![token(&repository, "token")];
        steps.extend(publish(&first, "token", "first", &mut model));
        let second = v2(7, Some(model.revision(&repository)));
        let object = &second.objects[0];
        let mut corrupt = object.text().into_bytes();
        corrupt[0] ^= 0x01;
        steps.push(open_new(&second, "token", "second"));
        steps.push(send(
            request(HubMethod::Put, "${second_upload_0}")
                .header(
                    "content-range",
                    format!("bytes 0-{}/{}", corrupt.len() - 1, corrupt.len()),
                )
                .text(String::from_utf8(corrupt).unwrap()),
            error(422, "integrity_failure").header("upload-offset", "0"),
        ));
        steps.push(send(
            request(HubMethod::Head, "${second_upload_0}"),
            status(200).header("upload-offset", "0").text(""),
        ));
        steps.push(commit(
            &second,
            "token",
            "second",
            error(422, "integrity_failure"),
        ));
        steps.push(get_refs(&repository, &model));
        cases.push(case(
            "corrupt-upload-refused-before-ref-moves",
            "Bytes that do not hash to the declared digest are discarded at finalize; the commit fails integrity_failure and refs keep the prior revision.",
            steps,
        ));
    }

    // Corruption at rest after finalize.
    {
        let publication = v1(8);
        let digest = publication.objects[0].digest();
        let mut steps = vec![
            token(&repository, "token"),
            open_new(&publication, "token", "first"),
        ];
        steps.extend(upload_all(&publication, "first"));
        steps.push(act(Action::CorruptObject { digest }));
        steps.push(commit(
            &publication,
            "token",
            "first",
            error(422, "integrity_failure"),
        ));
        steps.push(refs_absent(&repository));
        cases.push(case(
            "object-corrupted-after-upload-refuses-commit",
            "The commit re-verifies every inventory object; one corrupted after upload fails integrity_failure and no repository is created.",
            steps,
        ));
    }

    // Stale expected revision.
    {
        let first = v1(9);
        let mut model = RepoModel::default();
        let mut steps = vec![token(&repository, "token")];
        steps.extend(publish(&first, "token", "first", &mut model));
        let stale = model.revision(&repository);
        let second = v2(10, Some(stale.clone()));
        steps.extend(publish(&second, "token", "second", &mut model));
        let mut third = Publication::new(
            &repository,
            operation(11),
            vec![Object::package(
                "graphforge project package fixture: version three",
            )],
        )
        .expecting(Some(stale));
        third.package_digest = digest_of('e');
        steps.push(open_new(&third, "token", "third"));
        steps.extend(upload_all(&third, "third"));
        steps.push(commit(&third, "token", "third", error(412, "ref_conflict")));
        steps.push(get_refs(&repository, &model));
        cases.push(case(
            "stale-expected-revision-conflicts",
            "A commit whose expected_revision is no longer current fails ref_conflict and leaves refs at the newer revision.",
            steps,
        ));
    }

    // Fork: create-if-absent of a new repository identity.
    {
        let origin = v1(12);
        let fork_repository = identity("alice", "demo-fork");
        let citation = format!(
            "{{\"origin\":{{\"repository\":\"openalex/demo\",\"revision\":\"{}\"}}}}",
            {
                let mut model = RepoModel::default();
                model.advance(&origin);
                model.revision(&repository)
            }
        );
        let lineage = Object::other(
            "application/vnd.graphforge.research-lineage+json",
            &citation,
        );
        let fork = Publication::new(
            &fork_repository,
            operation(13),
            vec![
                Object::package("graphforge project package fixture: version one"),
                lineage.clone(),
            ],
        );
        let mut origin_model = RepoModel::default();
        let mut fork_model = RepoModel::default();
        let mut steps = vec![
            token(&repository, "token"),
            token(&fork_repository, "fork_token"),
        ];
        steps.extend(publish(&origin, "token", "origin", &mut origin_model));
        steps.extend(publish(&fork, "fork_token", "fork", &mut fork_model));
        steps.push(get_refs(&fork_repository, &fork_model));
        steps.push(send(
            request(
                HubMethod::Get,
                format!(
                    "{}/.gf/objects/{}",
                    repo_url(&fork_repository),
                    lineage.digest()
                ),
            ),
            status(200).text(citation.clone()),
        ));
        let again = Publication::new(
            &repository,
            operation(14),
            vec![Object::package(
                "graphforge project package fixture: fork attempt",
            )],
        );
        steps.push(open_new(&again, "token", "again"));
        steps.extend(upload_all(&again, "again"));
        steps.push(commit(&again, "token", "again", error(412, "ref_conflict")));
        steps.push(get_refs(&repository, &origin_model));
        cases.push(case(
            "fork-is-create-if-absent",
            "A Fork publishes a new repository identity with expected_revision null and its origin citation object stored verbatim; create-if-absent on an existing repository fails ref_conflict.",
            steps,
        ));
    }

    // Unsupported future.
    {
        let publication = v1(15);
        let mut future_requirement = publication.open_body();
        future_requirement["requirements"] =
            json!([{"capability": "server-side-merge", "major": 1}]);
        let mut future_format = publication.open_body();
        future_format["format"] = json!("graphforge-hub-publish/2");
        future_format["unknown_future_member"] = json!(true);
        let sessions = format!("{}/.gf/publish/sessions", repo_url(&repository));
        cases.push(case(
            "unknown-required-capability-is-unsupported-future",
            "An unknown required capability or format major fails unsupported_future before any state change: the same operation then opens normally.",
            vec![
                token(&repository, "token"),
                send(
                    request(HubMethod::Post, &sessions).bearer("token").json(future_requirement),
                    error(422, "unsupported_future"),
                ),
                send(
                    request(HubMethod::Post, &sessions).bearer("token").json(future_format),
                    error(422, "unsupported_future"),
                ),
                open_new(&publication, "token", "first"),
                refs_absent(&repository),
            ],
        ));
    }

    // Credentials.
    {
        let publication = v1(16);
        let sessions = format!("{}/.gf/publish/sessions", repo_url(&repository));
        cases.push(case(
            "credentials-are-scoped-and-short-lived",
            "A missing, unknown, expired, or other-repository token fails auth_denied; nothing is opened.",
            vec![
                send(
                    request(HubMethod::Post, &sessions).json(publication.open_body()),
                    error(401, "auth_denied")
                        .header("www-authenticate", "Bearer realm=\"graphforge-hub\""),
                ),
                send(
                    request(HubMethod::Post, &sessions)
                        .header("authorization", "Bearer not-a-token")
                        .json(publication.open_body()),
                    error(401, "auth_denied")
                        .header("www-authenticate", "Bearer error=\"invalid_token\""),
                ),
                token(&identity("alice", "demo"), "other"),
                send(
                    request(HubMethod::Post, &sessions).bearer("other").json(publication.open_body()),
                    error(403, "auth_denied")
                        .header("www-authenticate", "Bearer error=\"insufficient_scope\""),
                ),
                act(Action::IssueToken {
                    scope: "publish:openalex/demo".into(),
                    ttl_seconds: 60,
                    capture: "short".into(),
                }),
                act(Action::AdvanceClock { seconds: 60 }),
                send(
                    request(HubMethod::Post, &sessions).bearer("short").json(publication.open_body()),
                    error(401, "auth_denied"),
                ),
                token(&repository, "token"),
                open_new(&publication, "token", "first"),
            ],
        ));
    }

    // Declared length bounds.
    {
        let publication = v1(17);
        let mut oversized = publication.open_body();
        oversized["objects"][0]["length"] = json!(67_108_865_u64);
        let sessions = format!("{}/.gf/publish/sessions", repo_url(&repository));
        cases.push(case(
            "oversized-declared-length-refused-at-open",
            "A declared object length above the advertised max_object_bytes fails at session open with 413, before any upload location exists.",
            vec![
                token(&repository, "token"),
                send(
                    request(HubMethod::Post, &sessions).bearer("token").json(oversized),
                    error(413, "invalid_input"),
                ),
                open_new(&publication, "token", "first"),
            ],
        ));
    }

    // Entitlement.
    {
        let publication = v1(18);
        cases.push(case(
            "entitlement-denied",
            "A publication beyond the owner's storage entitlement fails entitlement_denied at session open.",
            vec![
                token(&repository, "token"),
                act(Action::SetQuota {
                    owner: "openalex".into(),
                    bytes: 8,
                }),
                send(
                    request(
                        HubMethod::Post,
                        format!("{}/.gf/publish/sessions", repo_url(&repository)),
                    )
                    .bearer("token")
                    .json(publication.open_body()),
                    error(403, "entitlement_denied"),
                ),
                refs_absent(&repository),
            ],
        ));
    }

    // Manifest must list exactly the admitted objects at this Hub's locations.
    {
        let mut publication = v1(19);
        publication.location_base = Some("https://elsewhere.example/objects".into());
        cases.push(case(
            "manifest-must-reference-admitted-objects",
            "A manifest listing an object at a location other than this Hub's is refused with invalid_input even when the commitment binds it; the repository is not created.",
            vec![
                token(&repository, "token"),
                open_new(&publication, "token", "first"),
                upload_all(&publication, "first").remove(0),
                commit(&publication, "token", "first", error(400, "invalid_input")),
                refs_absent(&repository),
            ],
        ));
    }

    // Device authorization.
    {
        let device = format!("{HUB}/.gf/oauth/device_authorization");
        let token_url = format!("{HUB}/.gf/oauth/token");
        let authorize = |prefix: &str| {
            send(
                request(HubMethod::Post, &device).form(&[
                    ("client_id", "graphforge-cli"),
                    ("scope", "publish:openalex/demo"),
                ]),
                status(200)
                    .capture(&format!("{prefix}_device"), "/device_code")
                    .capture(&format!("{prefix}_user"), "/user_code")
                    .json(json!({
                        "device_code": format!("${{{prefix}_device}}"),
                        "user_code": format!("${{{prefix}_user}}"),
                        "verification_uri": format!("{HUB}/device"),
                        "verification_uri_complete": format!("{HUB}/device?user_code=${{{prefix}_user}}"),
                        "expires_in": 600,
                        "interval": 5,
                    })),
            )
        };
        let poll = |prefix: &str, expect: Expect| {
            let code = format!("${{{prefix}_device}}");
            send(
                request(HubMethod::Post, &token_url).form(&[
                    ("grant_type", DEVICE_CODE_GRANT_TYPE),
                    ("device_code", code.as_str()),
                    ("client_id", "graphforge-cli"),
                ]),
                expect,
            )
        };
        let publication = v1(20);
        cases.push(case(
            "device-authorization-flow",
            "RFC 8628: pending, slow_down, approval yielding a scoped short-lived token that opens a session, single use, expiry, and denial.",
            vec![
                authorize("a"),
                poll("a", oauth("authorization_pending")),
                poll("a", oauth("slow_down")),
                act(Action::AdvanceClock { seconds: 10 }),
                act(Action::ApproveDevice {
                    user_code: "${a_user}".into(),
                }),
                poll(
                    "a",
                    status(200).capture("token", "/access_token").json(json!({
                        "access_token": "${token}",
                        "token_type": "Bearer",
                        "expires_in": 900,
                        "scope": "publish:openalex/demo",
                    })),
                ),
                poll("a", oauth("invalid_grant")),
                open_new(&publication, "token", "first"),
                authorize("b"),
                act(Action::AdvanceClock { seconds: 600 }),
                poll("b", oauth("expired_token")),
                authorize("c"),
                act(Action::DenyDevice {
                    user_code: "${c_user}".into(),
                }),
                poll("c", oauth("access_denied")),
                send(
                    request(HubMethod::Post, &token_url).form(&[
                        ("grant_type", "password"),
                        ("client_id", "graphforge-cli"),
                    ]),
                    oauth("unsupported_grant_type"),
                ),
            ],
        ));
    }

    Corpus {
        format: "graphforge-hub-publish-conformance/1".into(),
        hub: HUB.into(),
        cases,
    }
}

// ------------------------------------------------------------------- schemas

fn schema(title: &str, body: Value) -> Value {
    let mut schema = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": format!("https://graphforge.sh/schemas/hub-publish/v1/{title}.schema.json"),
        "title": title,
    });
    schema
        .as_object_mut()
        .unwrap()
        .extend(body.as_object().unwrap().clone());
    schema
}

fn defs() -> Value {
    json!({
        "digest": {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"},
        "slug": {"type": "string", "minLength": 1, "maxLength": 100, "pattern": "^[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?$"},
        "identity": {"type": "object", "additionalProperties": false, "required": ["owner", "repository"], "properties": {
            "owner": {"$ref": "#/$defs/slug"}, "repository": {"$ref": "#/$defs/slug"}}},
        "capability": {"type": "object", "additionalProperties": false, "required": ["capability", "major"], "properties": {
            "capability": {"type": "string", "minLength": 1, "maxLength": 128}, "major": {"type": "integer", "minimum": 0, "maximum": 65535}}},
        "https_url": {"type": "string", "format": "uri", "pattern": "^https://"},
        "object": {"type": "object", "additionalProperties": false, "required": ["digest", "length", "media_type"], "properties": {
            "digest": {"$ref": "#/$defs/digest"}, "length": {"type": "integer", "minimum": 1},
            "media_type": {"type": "string", "minLength": 1, "maxLength": 255}}},
        "receipt": {"type": "object", "additionalProperties": false,
            "required": ["format", "operation_uuid", "request_commitment", "repository", "manifest_validator", "refs", "previous_revision", "revision"],
            "properties": {
                "format": {"const": "graphforge-hub-publish/1"},
                "operation_uuid": {"type": "string", "format": "uuid"},
                "request_commitment": {"$ref": "#/$defs/digest"},
                "repository": {"$ref": "#/$defs/identity"},
                "manifest_validator": {"$ref": "#/$defs/digest"},
                "refs": {"type": "array", "minItems": 1, "maxItems": 64, "items": {"type": "string", "minLength": 1, "maxLength": 4096}},
                "previous_revision": {"oneOf": [{"$ref": "#/$defs/digest"}, {"type": "null"}]},
                "revision": {"$ref": "#/$defs/digest"}}}
    })
}

fn capabilities_schema() -> Value {
    schema(
        "capabilities",
        json!({
            "type": "object", "additionalProperties": false,
            "required": ["format", "repository", "requirements", "capabilities", "authorization", "object_location_template", "limits"],
            "properties": {
                "format": {"const": "graphforge-hub-publish/1"},
                "repository": {"$ref": "#/$defs/identity"},
                "requirements": {"type": "array", "maxItems": 64, "items": {"$ref": "#/$defs/capability"}},
                "capabilities": {"type": "array", "maxItems": 64, "items": {"$ref": "#/$defs/capability"}},
                "authorization": {"type": "object", "additionalProperties": false,
                    "required": ["device_authorization_endpoint", "token_endpoint", "scope"],
                    "properties": {
                        "device_authorization_endpoint": {"$ref": "#/$defs/https_url"},
                        "token_endpoint": {"$ref": "#/$defs/https_url"},
                        "scope": {"type": "string", "pattern": "^publish:[a-z0-9._-]+/[a-z0-9._-]+$"}}},
                "object_location_template": {"type": "string", "pattern": "^https://[^{}]*\\{digest\\}[^{}]*$"},
                "limits": {"type": "object", "additionalProperties": false,
                    "required": ["max_object_bytes", "max_objects", "max_session_bytes", "max_chunk_bytes", "max_document_bytes"],
                    "properties": {
                        "max_object_bytes": {"type": "integer", "minimum": 1},
                        "max_objects": {"type": "integer", "minimum": 1},
                        "max_session_bytes": {"type": "integer", "minimum": 1},
                        "max_chunk_bytes": {"type": "integer", "minimum": 1},
                        "max_document_bytes": {"type": "integer", "minimum": 1}}}
            },
            "$defs": defs(),
        }),
    )
}

fn session_request_schema() -> Value {
    schema(
        "session-request",
        json!({
            "type": "object", "additionalProperties": false,
            "required": ["format", "operation_uuid", "request_commitment", "intent_digest", "repository", "objects"],
            "properties": {
                "format": {"const": "graphforge-hub-publish/1"},
                "operation_uuid": {"type": "string", "format": "uuid"},
                "request_commitment": {"$ref": "#/$defs/digest"},
                "intent_digest": {"$ref": "#/$defs/digest"},
                "repository": {"$ref": "#/$defs/identity"},
                "objects": {"type": "array", "minItems": 1, "items": {"$ref": "#/$defs/object"}},
                "requirements": {"type": "array", "maxItems": 64, "items": {"$ref": "#/$defs/capability"}}
            },
            "$defs": defs(),
        }),
    )
}

fn session_response_schema() -> Value {
    schema(
        "session-response",
        json!({
            "oneOf": [
                {"type": "object", "additionalProperties": false, "required": ["state", "session_id", "uploads"], "properties": {
                    "state": {"const": "open"},
                    "session_id": {"type": "string", "pattern": "^[A-Za-z0-9_-]{1,128}$"},
                    "uploads": {"type": "array", "minItems": 1, "items": {"type": "object", "additionalProperties": false,
                        "required": ["digest", "length", "upload_url", "received"], "properties": {
                            "digest": {"$ref": "#/$defs/digest"}, "length": {"type": "integer", "minimum": 1},
                            "upload_url": {"$ref": "#/$defs/https_url"}, "received": {"type": "integer", "minimum": 0}}}}}},
                {"type": "object", "additionalProperties": false, "required": ["state", "receipt"], "properties": {
                    "state": {"const": "complete"}, "receipt": {"$ref": "#/$defs/receipt"}}}
            ],
            "$defs": defs(),
        }),
    )
}

fn commit_request_schema() -> Value {
    schema(
        "commit-request",
        json!({
            "type": "object", "additionalProperties": false,
            "required": ["manifest", "refs", "expected_revision"],
            "properties": {
                "manifest": {"$ref": "https://graphforge.sh/schemas/discovery/v1/manifest.schema.json"},
                "refs": {"type": "array", "minItems": 1, "maxItems": 64, "uniqueItems": true,
                    "items": {"type": "string", "minLength": 1, "maxLength": 4096}},
                "expected_revision": {"oneOf": [{"$ref": "#/$defs/digest"}, {"type": "null"}]}
            },
            "$defs": defs(),
        }),
    )
}

fn operation_status_schema() -> Value {
    schema(
        "operation-status",
        json!({
            "type": "object", "additionalProperties": false,
            "required": ["format", "operation_uuid", "repository", "intent_digest", "receipt"],
            "properties": {
                "format": {"const": "graphforge-hub-publish/1"},
                "operation_uuid": {"type": "string", "format": "uuid"},
                "repository": {"$ref": "#/$defs/identity"},
                "intent_digest": {"$ref": "#/$defs/digest"},
                "receipt": {"oneOf": [{"$ref": "#/$defs/receipt"}, {"type": "null"}]}
            },
            "$defs": defs(),
        }),
    )
}

fn upload_status_schema() -> Value {
    schema(
        "upload-status",
        json!({
            "type": "object", "additionalProperties": false,
            "required": ["digest", "length", "received"],
            "properties": {
                "digest": {"$ref": "#/$defs/digest"},
                "length": {"type": "integer", "minimum": 1},
                "received": {"type": "integer", "minimum": 0}
            },
            "$defs": defs(),
        }),
    )
}

fn error_schema() -> Value {
    schema(
        "error",
        json!({
            "type": "object", "additionalProperties": false,
            "required": ["code", "message"],
            "properties": {
                "code": {"enum": ["auth_denied", "entitlement_denied", "idempotency_conflict", "ref_conflict",
                    "unsupported_future", "integrity_failure", "invalid_input", "internal"]},
                "message": {"type": "string", "maxLength": 512}
            }
        }),
    )
}

// ------------------------------------------------------------------ harness

fn pretty(value: &impl Serialize) -> Vec<u8> {
    let mut bytes = serde_json::to_vec_pretty(value).unwrap();
    bytes.push(b'\n');
    bytes
}

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

fn generated() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("capabilities.schema.json", pretty(&capabilities_schema())),
        (
            "session-request.schema.json",
            pretty(&session_request_schema()),
        ),
        (
            "session-response.schema.json",
            pretty(&session_response_schema()),
        ),
        (
            "commit-request.schema.json",
            pretty(&commit_request_schema()),
        ),
        (
            "operation-status.schema.json",
            pretty(&operation_status_schema()),
        ),
        ("upload-status.schema.json", pretty(&upload_status_schema())),
        ("error.schema.json", pretty(&error_schema())),
        ("conformance.json", pretty(&corpus())),
    ]
}

#[test]
fn checked_in_contract_artifacts_match_rust_authority() {
    let expected = generated();
    if std::env::var_os("GRAPHFORGE_UPDATE_HUB_PUBLISH_ARTIFACTS").is_some() {
        let dir = root().join("docs/reference/hub-publish/v1");
        fs::create_dir_all(&dir).unwrap();
        for (name, bytes) in &expected {
            fs::write(dir.join(name), bytes).unwrap();
        }
        return;
    }
    assert_eq!(expected.len(), ARTIFACTS.len());
    for ((name, expected), (checked_name, actual)) in expected.iter().zip(ARTIFACTS) {
        assert_eq!(name, &checked_name);
        assert_eq!(
            actual.as_bytes(),
            expected.as_slice(),
            "{name} is stale; regenerate with GRAPHFORGE_UPDATE_HUB_PUBLISH_ARTIFACTS=1 cargo test -p graphforge-hub-publish --test contract_artifacts"
        );
    }
}

fn substitute(text: &str, variables: &BTreeMap<String, String>, case: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let end = rest[start..]
            .find('}')
            .unwrap_or_else(|| panic!("{case}: unterminated variable in {text}"));
        let name = &rest[start + 2..start + end];
        out.push_str(
            variables
                .get(name)
                .unwrap_or_else(|| panic!("{case}: variable {name} is not captured")),
        );
        rest = &rest[start + end + 1..];
    }
    out.push_str(rest);
    out
}

fn substitute_json(value: &Value, variables: &BTreeMap<String, String>, case: &str) -> Value {
    match value {
        Value::String(text) => Value::String(substitute(text, variables, case)),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| substitute_json(item, variables, case))
                .collect(),
        ),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, item)| (key.clone(), substitute_json(item, variables, case)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn build(request: &Request, variables: &BTreeMap<String, String>, case: &str) -> HubRequest {
    let mut built = HubRequest::new(request.method, substitute(&request.url, variables, case));
    for (name, value) in &request.headers {
        built = built.with_header(name, substitute(value, variables, case));
    }
    if let Some(json) = &request.json {
        built.body = serde_json::to_vec(&substitute_json(json, variables, case)).unwrap();
    }
    if let Some(form) = &request.form {
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        for (name, value) in form {
            serializer.append_pair(name, &substitute(value, variables, case));
        }
        built.body = serializer.finish().into_bytes();
    }
    if let Some(text) = &request.body_text {
        built.body = text.as_bytes().to_vec();
    }
    built
}

fn check(
    response: &HubResponse,
    expect: &Expect,
    variables: &mut BTreeMap<String, String>,
    label: &str,
) {
    assert_eq!(
        response.status,
        expect.status,
        "{label}: status; body {}",
        String::from_utf8_lossy(&response.body)
    );
    let body: Option<Value> = serde_json::from_slice(&response.body).ok();
    for (variable, pointer) in &expect.capture {
        let value = body
            .as_ref()
            .and_then(|body| body.pointer(pointer))
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("{label}: nothing to capture at {pointer}"));
        variables.insert(variable.clone(), value.to_owned());
    }
    for (name, value) in &expect.headers {
        assert_eq!(
            response.header(name),
            Some(substitute(value, variables, label).as_str()),
            "{label}: header {name}"
        );
    }
    if let Some(json) = &expect.json {
        assert_eq!(
            body.as_ref(),
            Some(&substitute_json(json, variables, label)),
            "{label}: body"
        );
    }
    if let Some(code) = &expect.error {
        let object = body
            .as_ref()
            .and_then(Value::as_object)
            .unwrap_or_else(|| panic!("{label}: error body is not a JSON object"));
        assert_eq!(object.len(), 2, "{label}: error body members");
        assert_eq!(object["code"], Value::String(code.clone()), "{label}: code");
        assert!(object["message"].is_string(), "{label}: message");
    }
    if let Some(error) = &expect.oauth_error {
        assert_eq!(body, Some(json!({"error": error})), "{label}: oauth error");
    }
    if let Some(text) = &expect.body_text {
        assert_eq!(
            String::from_utf8_lossy(&response.body),
            substitute(text, variables, label),
            "{label}: body text"
        );
    }
}

#[test]
fn every_conformance_case_replays_through_the_reference_hub() {
    let corpus: Corpus = serde_json::from_str(ARTIFACTS[ARTIFACTS.len() - 1].1).unwrap();
    assert_eq!(corpus.format, "graphforge-hub-publish-conformance/1");
    assert_eq!(corpus.hub, HUB);
    assert!(corpus.cases.len() >= 15, "corpus lost cases");
    for case in &corpus.cases {
        let hub = ReferenceHub::new();
        let mut variables = BTreeMap::new();
        for (index, step) in case.steps.iter().enumerate() {
            let label = format!("{} step {index}", case.name);
            match step {
                Step::Request { request, expect } => {
                    let response = hub.handle(&build(request, &variables, &label));
                    check(&response, expect, &mut variables, &label);
                }
                Step::Action { action } => match action {
                    Action::IssueToken {
                        scope,
                        ttl_seconds,
                        capture,
                    } => {
                        let token = hub.issue_token(scope, *ttl_seconds);
                        variables.insert(capture.clone(), token);
                    }
                    Action::AdvanceClock { seconds } => hub.advance_clock(*seconds),
                    Action::ApproveDevice { user_code } => {
                        assert!(hub.approve_device(&substitute(user_code, &variables, &label)));
                    }
                    Action::DenyDevice { user_code } => {
                        assert!(hub.deny_device(&substitute(user_code, &variables, &label)));
                    }
                    Action::SetQuota { owner, bytes } => hub.set_quota(owner, *bytes),
                    Action::CorruptObject { digest } => {
                        assert!(hub.corrupt_object(digest) > 0, "{label}: nothing corrupted");
                    }
                },
            }
        }
    }
}
