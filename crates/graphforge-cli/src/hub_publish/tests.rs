//! End-to-end `gf publish`: real research Projects through the Rust facade, the
//! real publish code path over HTTP to an in-process server wrapping
//! [`ReferenceHub`], and the real `gf clone` path back.
//!
//! The test transport only re-targets `https://hub.test` to the loopback
//! server; every URL still passes the product validators unchanged.

use super::*;
use crate::hub_clone::{CloneArgs, run_clone_with};
use crate::hub_http::{
    HttpResponse, HttpTransport, HubTransport, WriteTimeouts, write_agent_config,
};
use graphforge_api::{
    BranchSource, CreateResearchBranchRequest, ExecuteResearchBranchRequest, ForkResearchRequest,
    OperationId, ResearchReference, UpdateResearchMetadataRequest, WorkspaceResearchMetadata,
    WriteContext,
};
use graphforge_discovery::ProjectSummary;
use graphforge_hub_publish::{ReferenceHub, ReferenceHubConfig};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead as _, BufReader};
use std::net::{TcpListener, TcpStream};
use std::rc::Rc;
use std::sync::{Arc, Mutex};

mod ancestor_publication;
mod remote_fetch;

const HUB: &str = "https://hub.test";

// ------------------------------------------------------------ loopback Hub

/// One request the loopback server received.
#[derive(Clone, Debug)]
struct Logged {
    method: String,
    path: String,
    bearer: Option<String>,
    bypass: Option<String>,
    content_range: Option<String>,
    body_len: usize,
    /// Object a successful `PUT` appended to.
    digest: Option<String>,
}

/// Server-side fault injected into the next matching request.
enum Fault {
    /// Retain only the first half of the next `PUT` body, then drop the
    /// connection without a response.
    CutPut,
    /// Retain one whole uniform part and drop only its successful reply.
    DropPutReply(usize),
    /// Flip one byte of the next `PUT` body in transit.
    TamperPut,
    /// Corrupt the retained copy of the most recently uploaded object just
    /// before the next commit.
    CorruptBeforeCommit,
    /// Answer the next `GET` of each listed path with these bytes instead.
    Serve(BTreeMap<String, Vec<u8>>),
    /// Delay the response to the next `PUT`.
    SlowPut(Duration),
}

struct LoopbackHub {
    hub: Arc<ReferenceHub>,
    port: u16,
    log: Arc<Mutex<Vec<Logged>>>,
    fault: Arc<Mutex<Option<Fault>>>,
}

#[derive(Clone, Copy, PartialEq)]
enum ContextMode {
    Legacy,
    Private,
    Public,
}

/// Server state shared with the accept thread.
struct ServerState {
    private_context: ContextMode,
    hub: Arc<ReferenceHub>,
    log: Arc<Mutex<Vec<Logged>>>,
    fault: Arc<Mutex<Option<Fault>>>,
    last_upload: Mutex<Option<String>>,
}

impl LoopbackHub {
    fn start() -> Self {
        Self::with_config(ReferenceHubConfig {
            base_url: HUB.into(),
            ..ReferenceHubConfig::default()
        })
    }

    fn with_config(config: ReferenceHubConfig) -> Self {
        Self::configured(config, ContextMode::Legacy)
    }

    fn private() -> Self {
        Self::configured(
            ReferenceHubConfig {
                base_url: HUB.into(),
                ..ReferenceHubConfig::default()
            },
            ContextMode::Private,
        )
    }

    fn configured(config: ReferenceHubConfig, private_context: ContextMode) -> Self {
        let hub = Arc::new(ReferenceHub::with_config(config).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let log = Arc::new(Mutex::new(Vec::new()));
        let fault = Arc::new(Mutex::new(None));
        let state = ServerState {
            private_context,
            hub: hub.clone(),
            log: log.clone(),
            fault: fault.clone(),
            last_upload: Mutex::new(None),
        };
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                serve(&state, stream.unwrap());
            }
        });
        Self {
            hub,
            port,
            log,
            fault,
        }
    }

    fn transport(&self) -> LoopbackTransport {
        self.transport_with(Duration::from_secs(30), &crate::hub_http::WRITE_TIMEOUTS)
    }

    /// The product transport over plain loopback HTTP: reads keep a global
    /// deadline of `read_deadline`; writes use the product write configuration.
    fn transport_with(&self, read_deadline: Duration, writes: &WriteTimeouts) -> LoopbackTransport {
        let agent = |config| {
            ureq::Agent::with_parts(
                config,
                ureq::unversioned::transport::DefaultConnector::new(),
                ureq::unversioned::resolver::DefaultResolver::default(),
            )
        };
        let read = ureq::Agent::config_builder()
            .https_only(false)
            .http_status_as_error(false)
            .max_redirects(0)
            .proxy(None)
            .timeout_global(Some(read_deadline))
            .build();
        LoopbackTransport {
            inner: HubTransport::with_agents(
                HttpTransport { agent: agent(read) },
                agent(write_agent_config(false, writes)),
            ),
            port: self.port,
        }
    }

    fn inject(&self, fault: Fault) {
        *self.fault.lock().unwrap() = Some(fault);
    }

    fn log(&self) -> Vec<Logged> {
        self.log.lock().unwrap().clone()
    }

    fn get(&self, url: &str) -> HubResponse {
        self.hub.handle(&HubRequest::new(HubMethod::Get, url))
    }
}

#[allow(clippy::too_many_lines)] // one in-process HTTP exchange with its fault hooks
fn serve(state: &ServerState, stream: TcpStream) {
    let (hub, log, fault) = (&state.hub, &state.log, &state.fault);
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut line = String::new();
    if reader.read_line(&mut line).unwrap_or(0) == 0 {
        return;
    }
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap().to_owned();
    let path = parts.next().unwrap().to_owned();
    let mut headers = Vec::new();
    loop {
        let mut header = String::new();
        reader.read_line(&mut header).unwrap();
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        let (name, value) = header.split_once(':').unwrap();
        headers.push((name.trim().to_ascii_lowercase(), value.trim().to_owned()));
    }
    let header = |name: &str| {
        headers
            .iter()
            .find(|(candidate, _)| candidate == name)
            .map(|(_, value)| value.clone())
    };
    let length: usize = header("content-length").map_or(0, |value| value.parse().unwrap());
    let hub_method = match method.as_str() {
        "GET" => HubMethod::Get,
        "HEAD" => HubMethod::Head,
        "POST" => HubMethod::Post,
        "PUT" => HubMethod::Put,
        other => panic!("unexpected method {other}"),
    };
    let mut injected = fault.lock().unwrap();
    let cut = hub_method == HubMethod::Put && matches!(*injected, Some(Fault::CutPut));
    let mut body = vec![0; if cut { length / 2 } else { length }];
    reader.read_exact(&mut body).unwrap();
    let mut request = HubRequest {
        method: hub_method,
        url: format!("{HUB}{path}"),
        headers: headers.clone(),
        body,
    };
    log.lock().unwrap().push(Logged {
        method: method.clone(),
        path: path.clone(),
        bearer: header("authorization"),
        bypass: header("x-vercel-protection-bypass"),
        content_range: header("content-range"),
        body_len: request.body.len(),
        digest: None,
    });
    if cut {
        // The Hub retains the prefix that arrived; the client never hears back.
        injected.take();
        let (start, _, total) =
            graphforge_hub_publish::parse_content_range(&header("content-range").unwrap()).unwrap();
        let range = content_range(start, request.body.len() as u64, total);
        request.headers.retain(|(name, _)| name != "content-range");
        request.headers.push(("content-range".into(), range));
        hub.handle(&request);
        drop(reader);
        stream.shutdown(std::net::Shutdown::Both).ok();
        return;
    }
    if hub_method == HubMethod::Put && matches!(*injected, Some(Fault::TamperPut)) {
        injected.take();
        request.body[0] ^= 0x01;
    }
    if hub_method == HubMethod::Post
        && path.ends_with("/commit")
        && matches!(*injected, Some(Fault::CorruptBeforeCommit))
    {
        injected.take();
        let digest = state.last_upload.lock().unwrap().clone().unwrap();
        assert!(hub.corrupt_object(&digest) > 0, "corrupted a retained copy");
    }
    let replayed_body = match injected.as_mut() {
        Some(Fault::Serve(documents)) if hub_method == HubMethod::Get => documents.remove(&path),
        _ => None,
    };
    if matches!(injected.as_ref(), Some(Fault::Serve(documents)) if documents.is_empty()) {
        injected.take();
    }
    let delay = match injected.as_ref() {
        Some(Fault::SlowPut(delay)) if hub_method == HubMethod::Put => Some(*delay),
        _ => None,
    };
    if delay.is_some() {
        injected.take();
    }
    let drop_reply = hub_method == HubMethod::Put
        && matches!(*injected, Some(Fault::DropPutReply(size)) if request.body.len() == size);
    if drop_reply {
        injected.take();
    }
    drop(injected);
    let response = match replayed_body {
        Some(body) => HubResponse {
            status: 200,
            headers: vec![("etag".into(), format!("\"{}\"", sha256_digest(&body)))],
            body,
        },
        None if state.private_context != ContextMode::Legacy => {
            private_context_response(hub, &request, state.private_context == ContextMode::Private)
        }
        None => hub.handle(&request),
    };
    if hub_method == HubMethod::Put && response.status == 200 {
        let status: UploadStatus = serde_json::from_slice(&response.body).unwrap();
        log.lock().unwrap().last_mut().unwrap().digest = Some(status.digest.clone());
        *state.last_upload.lock().unwrap() = Some(status.digest);
    }
    if drop_reply && response.status == 200 {
        stream.shutdown(std::net::Shutdown::Both).ok();
        return;
    }
    if let Some(delay) = delay {
        std::thread::sleep(delay);
    }
    let mut stream = stream;
    let body: &[u8] = if hub_method == HubMethod::Head {
        &[]
    } else {
        &response.body
    };
    let mut head = format!("HTTP/1.1 {} Hub\r\n", response.status);
    for (name, value) in &response.headers {
        head = head + name + ": " + value + "\r\n";
    }
    head = head + "content-length: " + &body.len().to_string() + "\r\nconnection: close\r\n\r\n";
    stream.write_all(head.as_bytes()).unwrap();
    stream.write_all(body).unwrap();
}

/// Emulates the Hub context contract over the real HTTP transport while hiding
/// all anonymous discovery/object reads. ReferenceHub remains admission authority.
fn private_context_response(
    hub: &ReferenceHub,
    request: &HubRequest,
    hide_discovery: bool,
) -> HubResponse {
    let mut response = hub.handle(request);
    if request.method != HubMethod::Get {
        return response;
    }
    if request.url.ends_with("/.gf/publish") {
        let mut document: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        document["capabilities"]
            .as_array_mut()
            .unwrap()
            .push(json!({"capability":"publish-read-context", "major":1}));
        response.body = serde_json::to_vec(&document).unwrap();
    } else if let Some(base) = request.url.strip_suffix("/.gf/publish/context") {
        let probe = HubRequest {
            url: format!("{base}/.gf/publish/operations/{}", Uuid::nil()),
            ..request.clone()
        };
        let authorization = hub.handle(&probe);
        if authorization.status != 404 {
            return authorization;
        }
        let get = |suffix: &str| {
            hub.handle(&HubRequest::new(
                HubMethod::Get,
                format!("{base}/.gf/{suffix}"),
            ))
        };
        let refs = get("refs");
        let value = if refs.status == 404 {
            json!({"refs_json":null,"manifest_json":null,"lineage_json":null})
        } else {
            let manifest = get("manifest");
            let parsed =
                DiscoveryManifest::from_json(&manifest.body, DiscoveryLimits::default()).unwrap();
            let lineage = hub.handle(&HubRequest::new(
                HubMethod::Get,
                &parsed.lineage_object().unwrap().locations[0],
            ));
            json!({"refs_json":String::from_utf8(refs.body).unwrap(),"manifest_json":String::from_utf8(manifest.body).unwrap(),"lineage_json":String::from_utf8(lineage.body).unwrap()})
        };
        response = HubResponse {
            status: 200,
            headers: vec![],
            body: serde_json::to_vec(&value).unwrap(),
        };
    } else if hide_discovery && !request.url.contains("/.gf/publish/") {
        response = HubResponse {
            status: 404,
            headers: vec![],
            body: vec![],
        };
    }
    response
}

/// Routes `https://hub.test` to the loopback server after product validation.
struct LoopbackTransport {
    inner: HubTransport,
    port: u16,
}

impl LoopbackTransport {
    fn route(&self, url: &str) -> Result<Url, GfError> {
        let url = Url::parse(url).map_err(|_| network("test URL is invalid"))?;
        if url.scheme() != "https" || url.host_str() != Some("hub.test") {
            return Err(network("the test transport routes only https://hub.test"));
        }
        let mut routed = Url::parse(&format!("http://127.0.0.1:{}", self.port)).unwrap();
        routed.set_path(url.path());
        routed.set_query(url.query());
        Ok(routed)
    }
}

impl Transport for LoopbackTransport {
    fn get(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
    ) -> Result<HttpResponse, GfError> {
        self.inner
            .get(&self.route(url.as_str())?, range, if_range, limit)
    }

    fn get_discovery(
        &self,
        url: &Url,
        headers: &[(&str, ureq::http::HeaderValue)],
        limit: u64,
    ) -> Result<HttpResponse, GfError> {
        self.inner
            .get_discovery(&self.route(url.as_str())?, headers, limit)
    }

    fn exchange(&self, request: &HubRequest, limit: usize) -> Result<HubResponse, GfError> {
        let mut routed = request.clone();
        routed.url = self.route(&request.url)?.to_string();
        self.inner.exchange(&routed, limit)
    }
}

// ------------------------------------------------------------ research Projects

fn generation(graph: &GraphForge) -> Uuid {
    graph
        .committed_generation_identity()
        .unwrap()
        .generation_uuid
}

fn create_branch(graph: &mut GraphForge, label: &str, source: BranchSource) -> (Uuid, Uuid) {
    let branch_uuid = Uuid::now_v7();
    let version_uuid = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                author: None,
                committer: None,
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: generation(graph),
                branch_uuid,
                version_uuid,
                source,
                creator_uuid: Uuid::now_v7(),
                created_at: 1,
                label: label.into(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    (branch_uuid, version_uuid)
}

fn current() -> BranchSource {
    BranchSource::Current {
        origin_version_uuid: Uuid::now_v7(),
        context_uuid: Uuid::now_v7(),
    }
}

fn edit(graph: &mut GraphForge, branch_uuid: Uuid, score: i64) -> Uuid {
    let version_uuid = Uuid::now_v7();
    graph
        .execute_research_branch(
            &ExecuteResearchBranchRequest {
                author: None,
                committer: None,
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: generation(graph),
                branch_uuid,
                version_uuid,
                created_at: 2,
                query: format!("MATCH (n:Item) SET n.score={score}"),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    version_uuid
}

fn reference(graph: &GraphForge, version_uuid: Uuid) -> ResearchReference {
    graph
        .research_reference(
            &ResearchReferenceTarget::Version { version_uuid },
            &CancellationToken::new(),
        )
        .unwrap()
}

/// A durable Project with Branch `main` (base and one edited head).
struct History {
    root: tempfile::TempDir,
    graph: GraphForge,
    branch: Uuid,
    base: Uuid,
    head: Uuid,
}

fn history() -> History {
    let root = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(root.path().join("project").to_str()).unwrap();
    graph.execute("CREATE (:Item {score:0})").unwrap();
    let (branch, base) = create_branch(&mut graph, "main", current());
    let head = edit(&mut graph, branch, 1);
    History {
        root,
        graph,
        branch,
        base,
        head,
    }
}

// ------------------------------------------------------------ publish driver

fn repository_url(repository: &str) -> String {
    format!("{HUB}/{repository}")
}

fn args(repository: &str) -> PublishArgs {
    PublishArgs {
        repository: repository_url(repository),
        git_ref: None,
        version_uuid: None,
        fork_of: None,
        operation_uuid: None,
        protection_bypass_env: None,
        staging_directory: None,
        previews: Vec::new(),
    }
}

fn on_ref(repository: &str, name: &str) -> PublishArgs {
    PublishArgs {
        git_ref: Some(name.into()),
        ..args(repository)
    }
}

fn token(hub: &LoopbackHub, repository: &str) -> PublishToken {
    PublishToken::new(hub.hub.issue_token(&format!("publish:{repository}"), 900))
}

/// Captured stdout and stderr of one publish, rendered as the CLI renders them.
struct Run {
    result: Result<PublishOutcome, GfError>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

fn run_publish_with(
    hub: &LoopbackHub,
    graph: &GraphForge,
    args: &PublishArgs,
    credential: Credential,
) -> Run {
    run_publish_with_bypass(hub, graph, args, credential, None)
}
fn run_publish_with_bypass(
    hub: &LoopbackHub,
    graph: &GraphForge,
    args: &PublishArgs,
    credential: Credential,
    bypass: Option<&crate::hub_http::DiscoveryCredentials>,
) -> Run {
    let mut stderr = Vec::new();
    let mut stdout = Vec::new();
    let result = {
        let mut environment = PublishEnvironment {
            bypass,
            credential,
            wait: &|_| panic!("no device flow expected"),
            notices: &mut stderr,
            resolution: None,
        };
        publish(&hub.transport(), graph, args, &mut environment)
    };
    match &result {
        Ok(outcome) => write_outcome(outcome, true, &mut stdout).unwrap(),
        Err(error) => crate::write_error(error, true, &mut stderr).unwrap(),
    }
    Run {
        result,
        stdout,
        stderr,
    }
}

fn publish_ok(hub: &LoopbackHub, graph: &GraphForge, args: &PublishArgs) -> Run {
    let repository = args
        .repository
        .trim_start_matches(&format!("{HUB}/"))
        .to_owned();
    let run = run_publish_with(hub, graph, args, Credential::Token(token(hub, &repository)));
    if let Err(error) = &run.result {
        panic!("publish failed: {error}");
    }
    run
}

fn error_code(error: &GfError) -> String {
    let mut rendered = Vec::new();
    crate::write_error(error, true, &mut rendered).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&rendered).unwrap();
    value["error"]["details"]["semantic_code"]
        .as_str()
        .map_or_else(
            || value["error"]["code"].as_str().unwrap().to_owned(),
            str::to_owned,
        )
}

fn refs_document(hub: &LoopbackHub, repository: &str) -> HubResponse {
    hub.get(&format!("{}/.gf/refs", repository_url(repository)))
}

/// The lineage a consumer reads from the Hub's current documents.
fn served_lineage(hub: &LoopbackHub, repository: &str) -> (DiscoveryManifest, ResearchLineage) {
    let limits = DiscoveryLimits::default();
    let refs = RefSet::from_json(&refs_document(hub, repository).body, limits).unwrap();
    let manifest = DiscoveryManifest::from_json(
        &hub.get(&format!("{}/.gf/manifest", repository_url(repository)))
            .body,
        limits,
    )
    .unwrap();
    refs.validate_manifest(&manifest).unwrap();
    let object = manifest.lineage_object().unwrap();
    let bytes = hub.get(&object.locations[0]).body;
    assert_eq!(digest_bytes(&bytes), object.digest.0);
    let lineage = ResearchLineage::from_json(&bytes, limits).unwrap();
    manifest.bind_lineage(&refs, &lineage).unwrap();
    (manifest, lineage)
}

fn clone_into(
    hub: &LoopbackHub,
    repository: &str,
    destination: &Path,
    git_ref: Option<&str>,
    version_uuid: Option<Uuid>,
) -> serde_json::Value {
    let mut output = Vec::new();
    run_clone_with(
        &hub.transport(),
        CloneArgs {
            hub: None,
            token_env: None,
            protection_bypass_env: None,
            repository: repository_url(repository),
            destination: Some(destination.to_path_buf()),
            telemetry_endpoint: None,
            git_ref: git_ref.map(str::to_owned),
            version_uuid: version_uuid.map(|id| id.to_string()),
        },
        true,
        &mut output,
    )
    .unwrap();
    serde_json::from_slice(&output).unwrap()
}

fn puts(log: &[Logged]) -> Vec<&Logged> {
    log.iter().filter(|entry| entry.method == "PUT").collect()
}

// ------------------------------------------------------------ acceptance

#[test]
fn publishing_the_same_version_twice_returns_the_original_receipt() {
    let history = history();
    let hub = LoopbackHub::start();
    let first = publish_ok(&hub, &history.graph, &on_ref("curate/claims", "main"));
    let first_outcome = first.result.as_ref().unwrap();
    assert!(!first_outcome.replayed);
    assert!(
        !puts(&hub.log()).is_empty(),
        "the first publication uploads"
    );
    let refs_before = refs_document(&hub, "curate/claims");
    let (_, lineage) = served_lineage(&hub, "curate/claims");
    assert_eq!(lineage.branches.len(), 1);
    assert_eq!(
        lineage.branches[0].head_version_uuid,
        history.head.to_string()
    );

    // The same Version again: the original receipt, before any derivation or upload.
    let logged = hub.log().len();
    let second = publish_ok(&hub, &history.graph, &on_ref("curate/claims", "main"));
    assert!(second.result.as_ref().unwrap().replayed);
    assert_eq!(second.stdout, first.stdout, "receipts are byte-equal");
    let rerun = hub.log()[logged..].to_vec();
    assert!(puts(&rerun).is_empty(), "zero data-plane PUTs: {rerun:?}");
    assert!(
        rerun
            .iter()
            .all(|entry| entry.method == "GET" && !entry.path.ends_with("/sessions")),
        "the rerun only reads capabilities and the operation status: {rerun:?}"
    );
    let receipt = &first_outcome.receipt;
    let identity = RepositoryIdentity::parse("curate/claims").unwrap();
    assert_eq!(hub.hub.revisions(&identity), vec![receipt.revision.clone()]);
    let refs_after = refs_document(&hub, "curate/claims");
    assert_eq!(refs_after.body, refs_before.body);
    assert_eq!(refs_after.header("etag"), refs_before.header("etag"));
    let (_, lineage) = served_lineage(&hub, "curate/claims");
    assert_eq!(
        lineage
            .versions
            .iter()
            .filter(|version| version.package.is_some())
            .map(|version| version.version_uuid.clone())
            .collect::<Vec<_>>(),
        [history.head.to_string()],
        "the Hub holds one Version payload, independently of ancestry metadata"
    );

    let ancestor = lineage.version(&history.base.to_string()).unwrap();
    assert!(ancestor.package.is_none());
    assert_eq!(
        lineage.versions.len(),
        2,
        "one head and one metadata-only Branch ancestor"
    );

    // The same operation identity with a different Version is refused before
    // any derivation, and refs do not move.
    let logged = hub.log().len();
    let conflicting = PublishArgs {
        version_uuid: Some(history.base.to_string()),
        operation_uuid: Some(receipt.operation_uuid.to_string()),
        ..args("curate/claims")
    };
    let run = run_publish_with(
        &hub,
        &history.graph,
        &conflicting,
        Credential::Token(token(&hub, "curate/claims")),
    );
    let error = run.result.unwrap_err();
    assert_eq!(error.code(), "GF_IDEMPOTENCY_CONFLICT", "{error}");
    assert!(puts(&hub.log()[logged..]).is_empty());
    assert_eq!(refs_document(&hub, "curate/claims").body, refs_before.body);
    assert_eq!(hub.hub.revisions(&identity).len(), 1);
}

#[test]
fn interrupted_upload_resumes_from_hub_offset() {
    let history = history();
    let hub = LoopbackHub::start();
    hub.inject(Fault::CutPut);
    let interrupted = run_publish_with(
        &hub,
        &history.graph,
        &on_ref("curate/claims", "main"),
        Credential::Token(token(&hub, "curate/claims")),
    );
    let error = interrupted.result.unwrap_err();
    assert!(error.to_string().contains("hub.network"), "{error}");
    assert_eq!(refs_document(&hub, "curate/claims").status, 404);
    let first_run = hub.log();
    let cut = puts(&first_run)[0].clone();
    let total = graphforge_hub_publish::parse_content_range(cut.content_range.as_deref().unwrap())
        .unwrap()
        .2;
    let retained = cut.body_len as u64;
    assert!(retained > 0 && retained < total);

    // The rerun reopens the same session, asks the Hub what it retained, and
    // sends only the remainder of the interrupted object.
    let resumed = publish_ok(&hub, &history.graph, &on_ref("curate/claims", "main"));
    assert!(!resumed.result.as_ref().unwrap().replayed);
    let rerun = hub.log()[first_run.len()..].to_vec();
    assert!(
        rerun
            .iter()
            .any(|entry| entry.method == "HEAD" && entry.path == cut.path),
        "the rerun HEADs the interrupted upload"
    );
    let resumed_puts: Vec<_> = puts(&rerun)
        .into_iter()
        .filter(|entry| entry.path == cut.path)
        .collect();
    assert_eq!(resumed_puts.len(), 1);
    assert_eq!(
        resumed_puts[0].content_range.as_deref(),
        Some(content_range(retained, total - retained, total).as_str()),
        "the remainder starts at the Hub's offset"
    );
    assert_eq!(resumed_puts[0].body_len as u64, total - retained);
    let resent: u64 = puts(&rerun)
        .iter()
        .filter(|entry| entry.path == cut.path)
        .map(|entry| entry.body_len as u64)
        .sum();
    assert_eq!(resent, total - retained, "retained bytes are not resent");

    // The committed result clones back through the real clone path.
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("clone");
    let cloned = clone_into(&hub, "curate/claims", &destination, None, None);
    assert_eq!(cloned["research_version_uuid"], history.head.to_string());
    let reopened = GraphForge::new(destination.to_str()).unwrap();
    assert_eq!(
        reference(&reopened, history.head).identity_sha256,
        reference(&history.graph, history.head).identity_sha256
    );
}

#[test]
fn corrupt_object_is_refused_before_ref_moves() {
    let mut history = history();
    let hub = LoopbackHub::start();
    let identity = RepositoryIdentity::parse("curate/claims").unwrap();

    // Bytes tampered in transit: the Hub refuses them and nothing is published.
    hub.inject(Fault::TamperPut);
    let tampered = run_publish_with(
        &hub,
        &history.graph,
        &on_ref("curate/claims", "main"),
        Credential::Token(token(&hub, "curate/claims")),
    );
    let error = tampered.result.unwrap_err();
    assert_eq!(
        error_code(&error),
        "hub.publish.integrity_failure",
        "{error}"
    );
    assert!(tampered.stdout.is_empty(), "no receipt");
    assert_eq!(refs_document(&hub, "curate/claims").status, 404);
    assert!(hub.hub.revisions(&identity).is_empty());

    // An untampered rerun publishes; then bytes corrupted at rest before the
    // next commit are refused by commit-time verification and refs stay put.
    let published = publish_ok(&hub, &history.graph, &on_ref("curate/claims", "main"));
    let refs = refs_document(&hub, "curate/claims").body;
    edit(&mut history.graph, history.branch, 2);
    hub.inject(Fault::CorruptBeforeCommit);
    let corrupted = run_publish_with(
        &hub,
        &history.graph,
        &on_ref("curate/claims", "main"),
        Credential::Token(token(&hub, "curate/claims")),
    );
    let error = corrupted.result.unwrap_err();
    assert_eq!(
        error_code(&error),
        "hub.publish.integrity_failure",
        "{error}"
    );
    assert!(corrupted.stdout.is_empty(), "no receipt");
    assert_eq!(refs_document(&hub, "curate/claims").body, refs);
    assert_eq!(
        hub.hub.revisions(&identity),
        vec![published.result.unwrap().receipt.revision]
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn published_fork_clones_back_with_origin_citation() {
    let cancellation = CancellationToken::new();
    let origin = history();
    let hub = LoopbackHub::configured(
        ReferenceHubConfig {
            base_url: HUB.into(),
            ..ReferenceHubConfig::default()
        },
        ContextMode::Public,
    );
    publish_ok(&hub, &origin.graph, &on_ref("curate/claims", "main"));

    // Fork the published origin head into an independent Project.
    let fork_project = Uuid::now_v7();
    let fork_dir = origin.root.path().join("fork");
    origin
        .graph
        .fork_research(
            &ForkResearchRequest {
                operation_uuid: Uuid::now_v7(),
                project_uuid: fork_project,
                version_uuid: origin.head,
                projection: None,
                target: fork_dir.clone(),
                actor_uuid: Uuid::now_v7(),
                governance: "Independent local review".into(),
                adopt_selected_ontology: true,
                metadata: WorkspaceResearchMetadata::empty(),
            },
            &cancellation,
        )
        .unwrap();
    let mut fork = GraphForge::new(fork_dir.to_str()).unwrap();
    let (fork_branch, _) = create_branch(&mut fork, "main", current());
    let fork_head = edit(&mut fork, fork_branch, 7);

    // Citing an origin repository that does not publish the origin Version fails closed.
    let mut wrong_origin = on_ref("curate/claims-fork", "main");
    wrong_origin.fork_of = Some(repository_url("curate/elsewhere"));
    let refused = run_publish_with(
        &hub,
        &fork,
        &wrong_origin,
        Credential::Token(token(&hub, "curate/claims-fork")),
    );
    assert_eq!(
        error_code(&refused.result.unwrap_err()),
        "hub.publish.integrity_failure"
    );
    assert_eq!(refs_document(&hub, "curate/claims-fork").status, 404);

    // An origin repository that publishes the cited Version UUID with another
    // identity digest fails closed too. Serve consistent documents whose
    // lineage differs from the published one only in that identity.
    hub.inject(Fault::Serve(tampered_origin(
        &hub,
        "curate/claims",
        origin.head,
    )));
    let mut tampered = on_ref("curate/claims-fork", "main");
    tampered.fork_of = Some(repository_url("curate/claims"));
    let refused = run_publish_with(
        &hub,
        &fork,
        &tampered,
        Credential::Token(token(&hub, "curate/claims-fork")),
    );
    assert_eq!(
        error_code(&refused.result.unwrap_err()),
        "hub.publish.integrity_failure"
    );
    assert!(
        hub.fault.lock().unwrap().is_none(),
        "the tampered documents were read"
    );
    assert_eq!(refs_document(&hub, "curate/claims-fork").status, 404);

    let mut fork_args = on_ref("curate/claims-fork", "main");
    fork_args.fork_of = Some(repository_url("curate/claims"));
    let published = publish_ok(&hub, &fork, &fork_args);
    let receipt = published.result.unwrap().receipt;
    assert_eq!(
        receipt.previous_revision, None,
        "a Fork creates a new repository"
    );

    // The served lineage cites the origin Project, Version, and identity.
    assert!(
        hub.log()
            .iter()
            .filter(|request| request.method == "GET"
                && request.path.starts_with("/curate/claims/.gf/")
                && !request.path.contains("/publish"))
            .all(|request| request.bearer.is_none()),
        "Fork origin reads must never borrow destination credentials, even on the same Hub"
    );
    assert_eq!(
        hub.log()
            .iter()
            .filter(|request| request.path == "/curate/claims/.gf/publish/context")
            .count(),
        1,
        "only the origin own publication may read its authenticated context"
    );
    let (_, lineage) = served_lineage(&hub, "curate/claims-fork");
    let cited = lineage.fork.as_ref().expect("Fork origin is published");
    let origin_reference = reference(&origin.graph, origin.head);
    assert_eq!(
        cited.origin_repository,
        RepositoryIdentity::parse("curate/claims").unwrap()
    );
    assert_eq!(
        cited.origin_project_uuid,
        origin_reference.project_uuid.to_string()
    );
    assert_eq!(cited.origin_version_uuid, origin.head.to_string());
    assert_eq!(
        cited.origin_version_identity,
        hex_digest(&origin_reference.identity_sha256)
    );
    assert_eq!(lineage.project_uuid, fork_project.to_string());

    // A Fork never overwrites an existing repository.
    let again = run_publish_with(
        &hub,
        &fork,
        &PublishArgs {
            operation_uuid: Some(Uuid::now_v7().to_string()),
            ..fork_args
        },
        Credential::Token(token(&hub, "curate/claims-fork")),
    );
    assert_eq!(
        error_code(&again.result.unwrap_err()),
        "hub.publish.ref_conflict"
    );

    // Clone the Fork head back by Version and by ref into fresh destinations.
    let root = tempfile::tempdir().unwrap();
    for (name, git_ref, version) in [
        ("by-version", None, Some(fork_head)),
        ("by-ref", Some("main"), None),
    ] {
        let destination = root.path().join(name);
        let cloned = clone_into(&hub, "curate/claims-fork", &destination, git_ref, version);
        assert_eq!(cloned["research_version_uuid"], fork_head.to_string());
        let reopened = GraphForge::new(destination.to_str()).unwrap();
        let cloned_reference = reference(&reopened, fork_head);
        let source = reference(&fork, fork_head);
        assert_eq!(cloned_reference.identity_sha256, source.identity_sha256);
        assert_eq!(
            hex_digest(&cloned_reference.identity_sha256),
            lineage
                .version(&fork_head.to_string())
                .unwrap()
                .identity_digest
        );
        assert_eq!(cloned_reference.genealogy, source.genealogy);
        assert_eq!(
            cloned_reference.origin_project_uuid,
            source.origin_project_uuid
        );
    }
    // A citation remains the original published identity, while resolution may
    // follow its verified one-hop move only for that exact Version identity.
    let citation = lineage.fork.as_ref().unwrap().clone();
    hub.hub
        .move_repository(
            &citation.origin_repository,
            &RepositoryIdentity::parse("research/moved-origin").unwrap(),
        )
        .unwrap();
    let transport = hub.transport();
    let source_base = Url::parse(&repository_url("curate/claims")).unwrap();
    let bypass = crate::hub_http::DiscoveryCredentials::load(
        &Url::parse(HUB).unwrap(),
        None,
        Some("PREVIEW_SECRET"),
        |_| Ok("fork-preview".into()),
    )
    .unwrap();
    let reader = Hub {
        transport: &transport,
        base: source_base.clone(),
        bypass: bypass.as_ref(),
    };
    let before = hub.log().len();
    verify_fork_origin(&reader, &source_base, &citation).unwrap();
    let log = hub.log();
    assert!(
        log[before..]
            .iter()
            .any(|r| r.path == "/research/moved-origin/.gf/refs")
    );
    for request in &log[before..] {
        assert!(request.bearer.is_none());
        assert_eq!(
            request.bypass.as_deref(),
            if request.path.ends_with("/.gf/refs") || request.path.ends_with("/.gf/manifest") {
                Some("fork-preview")
            } else {
                None
            }
        );
    }
    let mut wrong = citation.clone();
    wrong.origin_version_identity = Sha256Digest(sha256_digest(b"another identity"));
    assert!(verify_fork_origin(&reader, &source_base, &wrong).is_err());
    let (_, retained) = served_lineage(&hub, "curate/claims-fork");
    assert_eq!(retained.fork.as_ref(), Some(&citation));
}

/// Consistent refs, manifest, and lineage for `repository` whose lineage lists
/// `version` with a different identity digest, keyed by request path.
fn tampered_origin(
    hub: &LoopbackHub,
    repository: &str,
    version: Uuid,
) -> BTreeMap<String, Vec<u8>> {
    let (mut manifest, mut lineage) = served_lineage(hub, repository);
    let entry = lineage
        .versions
        .iter_mut()
        .find(|entry| entry.version_uuid == version.to_string())
        .unwrap();
    assert_ne!(entry.identity_digest.0, sha256_digest(b"another Version"));
    entry.identity_digest = Sha256Digest(sha256_digest(b"another Version"));
    let lineage_bytes = lineage.to_canonical_json().unwrap();
    let lineage_object = digest_bytes(&lineage_bytes);
    let old_object = manifest.lineage.as_ref().unwrap().object_digest.clone();
    manifest
        .objects
        .retain(|object| object.digest != old_object);
    manifest.objects.push(object_descriptor(
        lineage_object.clone(),
        lineage_bytes.len() as u64,
        RESEARCH_LINEAGE_MEDIA_TYPE,
        format!(
            "{}/.gf/objects/{lineage_object}",
            repository_url(repository)
        ),
    ));
    manifest
        .objects
        .sort_by(|left, right| left.digest.0.cmp(&right.digest.0));
    manifest.lineage = Some(ResearchLineageReference {
        format: RESEARCH_LINEAGE_FORMAT.into(),
        lineage_digest: lineage.canonical_digest().unwrap(),
        object_digest: Sha256Digest(lineage_object.clone()),
    });
    let manifest_bytes = manifest.to_canonical_json().unwrap();
    let mut refs = RefSet::from_json(
        &refs_document(hub, repository).body,
        DiscoveryLimits::default(),
    )
    .unwrap();
    for item in &mut refs.refs {
        item.validator = Sha256Digest(digest_bytes(&manifest_bytes));
    }
    BTreeMap::from([
        (
            format!("/{repository}/.gf/refs"),
            refs.to_canonical_json().unwrap(),
        ),
        (format!("/{repository}/.gf/manifest"), manifest_bytes),
        (
            format!("/{repository}/.gf/objects/{lineage_object}"),
            lineage_bytes,
        ),
    ])
}

// ------------------------------------------------------------ supporting

fn tree_contains(root: &Path, needle: &[u8]) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            let metadata = std::fs::symlink_metadata(&path).unwrap();
            if metadata.is_dir() {
                pending.push(path);
            } else if metadata.is_file() {
                let bytes = std::fs::read(&path).unwrap();
                if bytes.windows(needle.len()).any(|window| window == needle) {
                    found.push(path);
                }
            }
        }
    }
    found
}

#[test]
fn published_research_repository_summary_carries_project_metadata() {
    let root = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(root.path().join("project").to_str()).unwrap();
    graph.execute("CREATE (:Item {score:0})").unwrap();
    let mut metadata = WorkspaceResearchMetadata::empty();
    metadata.title = Some("Claims".into());
    metadata.license = Some("CC0-1.0".into());
    graph
        .update_research_metadata(UpdateResearchMetadataRequest {
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            metadata,
        })
        .unwrap();
    let (branch, _) = create_branch(&mut graph, "main", current());
    edit(&mut graph, branch, 1);
    let hub = LoopbackHub::start();
    publish_ok(&hub, &graph, &on_ref("curate/claims", "main"));

    let (manifest, _) = served_lineage(&hub, "curate/claims");
    let object = manifest.summary_object().unwrap();
    let bytes = hub.get(&object.locations[0]).body;
    assert_eq!(digest_bytes(&bytes), object.digest.0);
    let summary = ProjectSummary::from_json(&bytes, DiscoveryLimits::default()).unwrap();
    manifest.bind_summary(&summary).unwrap();
    // The summary describes the Project, not only its graph data.
    assert_eq!(summary.metadata.title.as_deref(), Some("Claims"));
    assert_eq!(summary.metadata.license.as_deref(), Some("CC0-1.0"));
    assert_eq!(summary.facts.components.get("settings"), Some(&2));
    // Research travels in the Version packages; the repository's lineage, not
    // the Project package, says it has research.
    assert!(!summary.facts.research_present);
    assert!(manifest.lineage.is_some());
}

#[test]
fn publish_token_never_leaves_the_control_plane() {
    let mut history = history();
    let hub = LoopbackHub::start();
    let canary = token(&hub, "curate/claims");
    let secret = canary.expose().to_owned();
    let mut outputs = Vec::new();
    let published = run_publish_with(
        &hub,
        &history.graph,
        &on_ref("curate/claims", "main"),
        Credential::Token(canary.clone()),
    );
    assert!(published.result.is_ok());
    outputs.push(published.stdout);
    outputs.push(published.stderr);
    // A refused publication renders an error; it must not carry the token either.
    edit(&mut history.graph, history.branch, 3);
    let refused = run_publish_with(
        &hub,
        &history.graph,
        &PublishArgs {
            fork_of: Some(repository_url("curate/origin")),
            ..on_ref("curate/claims", "main")
        },
        Credential::Token(canary),
    );
    assert!(refused.result.is_err());
    assert!(!refused.stderr.is_empty());
    outputs.push(refused.stdout);
    outputs.push(refused.stderr);
    for output in &outputs {
        assert!(
            !output
                .windows(secret.len())
                .any(|window| window == secret.as_bytes()),
            "token leaked into CLI output"
        );
    }
    assert_eq!(
        tree_contains(history.root.path(), secret.as_bytes()),
        Vec::<PathBuf>::new(),
        "token written into the project tree"
    );
    // The bearer goes only to the repository's control plane, never to upload
    // capability URLs or public reads.
    let log = hub.log();
    assert!(log.iter().any(|entry| entry.bearer.is_some()));
    for entry in &log {
        let control = entry.path.starts_with("/curate/claims/.gf/publish/")
            && entry.method != "PUT"
            && entry.method != "HEAD";
        assert_eq!(
            entry.bearer.is_some(),
            control,
            "{} {}",
            entry.method,
            entry.path
        );
        if let Some(bearer) = &entry.bearer {
            assert_eq!(bearer, &format!("Bearer {secret}"));
        }
    }
    assert!(
        log.iter()
            .any(|entry| entry.path.starts_with("/.gf/uploads/") && entry.bearer.is_none())
    );
}

#[test]
fn stale_expected_revision_is_a_ref_conflict_and_a_rerun_lands_on_top() {
    let mut history = history();
    let hub = LoopbackHub::start();
    let identity = RepositoryIdentity::parse("curate/claims").unwrap();
    publish_ok(&hub, &history.graph, &on_ref("curate/claims", "main"));
    let stale_refs = refs_document(&hub, "curate/claims").body;
    let stale_manifest = hub
        .get(&format!("{}/.gf/manifest", repository_url("curate/claims")))
        .body;
    let main_head = edit(&mut history.graph, history.branch, 5);
    publish_ok(&hub, &history.graph, &on_ref("curate/claims", "main"));
    let current = refs_document(&hub, "curate/claims").body;
    assert_ne!(current, stale_refs);

    // Another publisher moved the repository after this client read it.
    let (feature, _) = create_branch(
        &mut history.graph,
        "feature",
        BranchSource::Branch {
            branch_uuid: history.branch,
        },
    );
    hub.inject(Fault::Serve(BTreeMap::from([
        ("/curate/claims/.gf/refs".to_owned(), stale_refs),
        ("/curate/claims/.gf/manifest".to_owned(), stale_manifest),
    ])));
    let logged = hub.log().len();
    let run = run_publish_with(
        &hub,
        &history.graph,
        &on_ref("curate/claims", "feature"),
        Credential::Token(token(&hub, "curate/claims")),
    );
    let error = run.result.unwrap_err();
    assert_eq!(error_code(&error), "hub.publish.ref_conflict", "{error}");
    assert_eq!(refs_document(&hub, "curate/claims").body, current);
    assert_eq!(hub.hub.revisions(&identity).len(), 2);
    let conflicted: BTreeSet<String> = puts(&hub.log()[logged..])
        .iter()
        .filter_map(|entry| entry.digest.clone())
        .collect();
    assert!(!conflicted.is_empty());

    // A plain rerun reads the new revision and lands on top of it, carrying
    // both earlier publications forward; bytes the conflicted attempt already
    // sent are not sent again.
    let logged = hub.log().len();
    let rerun = publish_ok(&hub, &history.graph, &on_ref("curate/claims", "feature"));
    let receipt = rerun.result.unwrap().receipt;
    assert_eq!(receipt.previous_revision, Some(sha256_digest(&current)));
    assert_eq!(hub.hub.revisions(&identity).len(), 3);
    let rerun_log = hub.log();
    let resent: Vec<_> = puts(&rerun_log[logged..])
        .into_iter()
        .filter(|entry| {
            entry
                .digest
                .as_ref()
                .is_some_and(|digest| conflicted.contains(digest))
        })
        .collect();
    assert!(resent.is_empty(), "{resent:?}");
    let (_, lineage) = served_lineage(&hub, "curate/claims");
    let heads: BTreeMap<_, _> = lineage
        .branches
        .iter()
        .map(|branch| (branch.ref_name.clone(), branch.head_version_uuid.clone()))
        .collect();
    assert_eq!(heads["main"], main_head.to_string());
    assert!(heads.contains_key("feature"));
    assert_eq!(
        lineage
            .branches
            .iter()
            .find(|branch| branch.ref_name == "feature")
            .unwrap()
            .branch_uuid,
        feature.to_string()
    );
    for version in [history.head, main_head] {
        assert!(lineage.version(&version.to_string()).is_some());
    }
}

#[test]
fn a_changed_project_publishes_a_new_snapshot_and_conflicts_under_a_reused_operation() {
    let history = history();
    let hub = LoopbackHub::start();
    let identity = RepositoryIdentity::parse("curate/claims").unwrap();
    let first = publish_ok(&hub, &history.graph, &on_ref("curate/claims", "main"));
    let first = first.result.unwrap().receipt;
    let (first_manifest, _) = served_lineage(&hub, "curate/claims");

    // New data in the Project, same Branch head: the next publication is new.
    history.graph.execute("CREATE (:Item {score:9})").unwrap();
    let second = publish_ok(&hub, &history.graph, &on_ref("curate/claims", "main"));
    let second = second.result.unwrap();
    assert!(
        !second.replayed,
        "a changed Project never replays a stale receipt"
    );
    assert_ne!(second.receipt.operation_uuid, first.operation_uuid);
    assert_eq!(hub.hub.revisions(&identity).len(), 2);
    let (second_manifest, lineage) = served_lineage(&hub, "curate/claims");
    assert_ne!(
        second_manifest.package.package_digest, first_manifest.package.package_digest,
        "the Hub received the new Project package"
    );
    assert_eq!(
        lineage.branches[0].head_version_uuid,
        history.head.to_string()
    );

    // Reusing an operation explicitly with changed content conflicts.
    history.graph.execute("CREATE (:Item {score:10})").unwrap();
    let refs = refs_document(&hub, "curate/claims").body;
    let reused = run_publish_with(
        &hub,
        &history.graph,
        &PublishArgs {
            operation_uuid: Some(second.receipt.operation_uuid.to_string()),
            ..on_ref("curate/claims", "main")
        },
        Credential::Token(token(&hub, "curate/claims")),
    );
    assert_eq!(reused.result.unwrap_err().code(), "GF_IDEMPOTENCY_CONFLICT");
    assert_eq!(refs_document(&hub, "curate/claims").body, refs);
}

#[test]
fn a_slow_but_progressing_upload_succeeds_in_bounded_chunks() {
    let history = history();
    let hub = LoopbackHub::with_config(ReferenceHubConfig {
        base_url: HUB.into(),
        limits: graphforge_hub_publish::PublishLimits {
            max_chunk_bytes: 4096,
            ..ReferenceHubConfig::default().limits
        },
        ..ReferenceHubConfig::default()
    });
    // Scaled down: reads keep a one-second whole-request deadline, a write
    // waits two seconds for its response, and writes bound phases instead.
    let transport = hub.transport_with(
        Duration::from_secs(1),
        &WriteTimeouts {
            send_body: Duration::from_secs(10),
            response: Duration::from_secs(10),
        },
    );
    hub.inject(Fault::SlowPut(Duration::from_secs(2)));
    let mut notices = Vec::new();
    let mut environment = PublishEnvironment {
        bypass: None,
        credential: Credential::Token(token(&hub, "curate/claims")),
        wait: &|_| panic!("no device flow expected"),
        notices: &mut notices,
        resolution: None,
    };
    let outcome = publish(
        &transport,
        &history.graph,
        &on_ref("curate/claims", "main"),
        &mut environment,
    )
    .unwrap();
    assert!(!outcome.replayed);
    let log = hub.log();
    let chunks = puts(&log);
    assert!(chunks.iter().all(|entry| entry.body_len <= 4096));
    assert!(
        chunks.iter().any(|entry| !entry
            .content_range
            .as_deref()
            .unwrap()
            .starts_with("bytes 0-")),
        "an object larger than one chunk uploads in several"
    );
}

#[test]
fn device_flow_waits_are_bounded_whatever_the_hub_advertises() {
    let history = history();
    let hub = LoopbackHub::with_config(ReferenceHubConfig {
        base_url: HUB.into(),
        device_code_ttl_seconds: 1_000_000,
        device_poll_interval_seconds: 1_000,
        ..ReferenceHubConfig::default()
    });
    let (result, waits, _) = device_flow(&hub, &history.graph, |_, hub, _| {
        hub.advance_clock(60);
    });
    let error = result.unwrap_err();
    assert_eq!(error_code(&error), "hub.publish.auth_denied", "{error}");
    assert!(error.to_string().contains("fifteen minutes"), "{error}");
    assert!(waits.iter().all(|wait| *wait <= Duration::from_mins(1)));
    assert_eq!(waits.iter().sum::<Duration>(), Duration::from_mins(15));
}

#[test]
fn a_status_without_a_publish_error_body_is_a_hub_failure() {
    for status in [401, 409, 412] {
        let error = hub_failure(&HubResponse {
            status,
            headers: vec![("content-type".into(), "text/html".into())],
            body: b"<html>proxy error</html>".to_vec(),
        });
        assert_ne!(error.code(), "GF_IDEMPOTENCY_CONFLICT");
        assert!(error.to_string().contains("hub.network"), "{error}");
    }
    let conflict = hub_failure(&HubResponse {
        status: 409,
        headers: Vec::new(),
        body: br#"{"code":"idempotency_conflict","message":"x"}"#.to_vec(),
    });
    assert_eq!(conflict.code(), "GF_IDEMPOTENCY_CONFLICT");
}

/// A `Write` the device-flow waiter can read while the publish holds it.
#[derive(Clone, Default)]
struct Shared(Rc<RefCell<Vec<u8>>>);

impl Write for Shared {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn user_code(notices: &Shared) -> String {
    let text = String::from_utf8(notices.0.borrow().clone()).unwrap();
    text.rsplit_once("enter code ").unwrap().1.trim().to_owned()
}

/// Run the device flow; `decide(poll_index, hub, user_code)` acts before each poll.
fn device_flow(
    hub: &LoopbackHub,
    graph: &GraphForge,
    decide: impl Fn(usize, &ReferenceHub, &str),
) -> (Result<PublishOutcome, GfError>, Vec<Duration>, String) {
    let notices = Shared::default();
    let waits = RefCell::new(Vec::new());
    let wait = |duration: Duration| {
        let index = waits.borrow().len();
        waits.borrow_mut().push(duration);
        decide(index, &hub.hub, &user_code(&notices));
    };
    let mut writer = notices.clone();
    let result = {
        let mut environment = PublishEnvironment {
            bypass: None,
            credential: Credential::DeviceFlow,
            wait: &wait,
            notices: &mut writer,
            resolution: None,
        };
        publish(
            &hub.transport(),
            graph,
            &on_ref("curate/claims", "main"),
            &mut environment,
        )
    };
    let text = String::from_utf8(notices.0.borrow().clone()).unwrap();
    (result, waits.into_inner(), text)
}

#[test]
fn device_flow_publishes_after_pending_slow_down_and_approval() {
    let history = history();
    let hub = LoopbackHub::start();
    let (result, waits, notices) = device_flow(&hub, &history.graph, |poll, hub, code| {
        match poll {
            // First poll on time: pending. Second poll without the Hub clock
            // advancing: slow_down. Third: approved.
            0 => hub.advance_clock(5),
            1 => {}
            _ => {
                assert!(hub.approve_device(code));
                hub.advance_clock(10);
            }
        }
    });
    let outcome = result.unwrap();
    assert!(!outcome.replayed);
    assert_eq!(
        waits,
        [5, 5, 10].map(Duration::from_secs),
        "slow_down adds five seconds"
    );
    assert!(notices.contains("https://hub.test/device"), "{notices}");
    // The device-issued token reached only the control plane and was never shown.
    let bearer = hub
        .log()
        .into_iter()
        .find_map(|entry| entry.bearer)
        .unwrap();
    let token = bearer.trim_start_matches("Bearer ");
    assert!(!notices.contains(token));
    let refs = RefSet::from_json(
        &refs_document(&hub, "curate/claims").body,
        DiscoveryLimits::default(),
    )
    .unwrap();
    assert_eq!(refs.refs[0].validator.0, outcome.receipt.manifest_validator);
}

#[test]
fn denied_device_authorization_is_auth_denied() {
    let history = history();
    let hub = LoopbackHub::start();
    let (result, waits, _) = device_flow(&hub, &history.graph, |poll, hub, code| {
        hub.advance_clock(5);
        if poll == 1 {
            assert!(hub.deny_device(code));
        }
    });
    let error = result.unwrap_err();
    assert_eq!(error_code(&error), "hub.publish.auth_denied", "{error}");
    assert_eq!(waits.len(), 2);
    assert!(hub.log().iter().all(|entry| entry.bearer.is_none()));
    assert_eq!(refs_document(&hub, "curate/claims").status, 404);
}

#[test]
fn quota_denial_is_entitlement_denied() {
    let history = history();
    let hub = LoopbackHub::start();
    hub.hub.set_quota("curate", 16);
    let run = run_publish_with(
        &hub,
        &history.graph,
        &on_ref("curate/claims", "main"),
        Credential::Token(token(&hub, "curate/claims")),
    );
    assert_eq!(
        error_code(&run.result.unwrap_err()),
        "hub.publish.entitlement_denied"
    );
    assert!(puts(&hub.log()).is_empty());
}

#[test]
fn upload_urls_are_validated_as_capabilities() {
    let ok = |url: &str| validate_upload_url(&Url::parse(url).unwrap()).is_ok();
    assert!(ok("https://uploads.example/u/1?signature=abc"));
    assert!(!ok("http://uploads.example/u/1"));
    assert!(!ok("https://user:secret@uploads.example/u/1"));
    assert!(!ok("https://uploads.example/u/1#fragment"));
    assert!(!ok("https://127.0.0.1/u/1"));
    assert!(!ok("https://10.0.0.1/u/1"));
    // Control-plane URLs never carry a query.
    assert!(validate_url(&Url::parse("https://hub.example/a/b?x=1").unwrap()).is_err());
}

#[test]
fn repository_cli_parity_cases_match_the_rust_cli() {
    let fixtures: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/contracts/repository-cli-parity.json"
    ))
    .unwrap();
    let cases = fixtures["cases"].as_array().unwrap();
    assert!(cases.iter().any(|case| {
        case["args"]
            .as_array()
            .unwrap()
            .iter()
            .any(|arg| arg == "publish")
    }));
    for case in cases {
        let args: Vec<String> = case["args"]
            .as_array()
            .unwrap()
            .iter()
            .map(|arg| arg.as_str().unwrap().to_owned())
            .collect();
        let execution = crate::execute(std::iter::once("gf".to_owned()).chain(args));
        let name = case["name"].as_str().unwrap();
        assert_eq!(i64::from(execution.exit_code), case["exitCode"], "{name}");
        assert_eq!(
            String::from_utf8(execution.stdout).unwrap(),
            case["stdout"],
            "{name}"
        );
        assert_eq!(
            String::from_utf8(execution.stderr).unwrap(),
            case["stderr"],
            "{name}"
        );
    }
}

#[test]
fn saved_query_definition_and_parameterized_aggregate_survive_hub_publish_and_clone() {
    use graphforge_api::{IrLiteral, SavedQuery, SavedQueryParameterType, SavedQuerySource};
    let root = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(root.path().join("project").to_str()).unwrap();
    graph
        .execute("CREATE (:Item {score:1}), (:Item {score:3})")
        .unwrap();
    let saved = SavedQuery {
        query_uuid: Uuid::now_v7(),
        name: "Items above threshold".into(),
        description: Some("Reusable published aggregate".into()),
        query: "MATCH (n:Item) WHERE n.score >= $minimum RETURN count(n) AS total".into(),
        parameters: BTreeMap::from([("minimum".into(), SavedQueryParameterType::Integer)]),
    };
    graph.create_saved_query(saved.clone()).unwrap();
    // A portable definition may need parameters and must never run on publish or clone.
    let failing = SavedQuery {
        query_uuid: Uuid::now_v7(),
        name: "Explicit execution only".into(),
        description: None,
        query: "RETURN 1 / 0 AS unavailable".into(),
        parameters: BTreeMap::new(),
    };
    graph.create_saved_query(failing.clone()).unwrap();
    let (_, version) = create_branch(&mut graph, "main", current());
    let hub = LoopbackHub::start();
    publish_ok(&hub, &graph, &on_ref("curate/claims", "main"));
    let destination = root.path().join("clone");
    let cloned = clone_into(&hub, "curate/claims", &destination, Some("main"), None);
    assert_eq!(cloned["research_version_uuid"], version.to_string());
    let reopened = GraphForge::new(destination.to_str()).unwrap();
    assert_eq!(reopened.saved_query(saved.query_uuid).unwrap(), saved);
    assert_eq!(reopened.saved_query(failing.query_uuid).unwrap(), failing);
    assert!(
        reopened
            .execute_saved_query(
                failing.query_uuid,
                &std::collections::HashMap::new(),
                &SavedQuerySource::Current,
                None
            )
            .is_err()
    );
    let result = reopened
        .execute_saved_query(
            saved.query_uuid,
            &std::collections::HashMap::from([("minimum".into(), IrLiteral::Int(2))]),
            &SavedQuerySource::Current,
            None,
        )
        .unwrap();
    assert_eq!(
        result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        reference(&reopened, version).identity_sha256,
        reference(&graph, version).identity_sha256
    );
}

#[test]
fn captured_publish_credentials_never_consult_the_vault_or_network() {
    let error = select_credential(None, false, false, || {
        panic!("captured publish must not consult saved credentials")
    })
    .err()
    .unwrap();
    assert_eq!(
        error.to_string(),
        "validation error: hub.publish.auth_denied: no publish credential: set GRAPHFORGE_HUB_PUBLISH_TOKEN or run gf publish in a terminal"
    );
    let credential = select_credential(
        Some(PublishToken::new("explicit-token".into())),
        false,
        false,
        || panic!("explicit credentials take precedence"),
    )
    .unwrap();
    assert!(matches!(credential, Credential::Token(token) if token.expose() == "explicit-token"));
}

#[test]
fn streaming_publish_can_reuse_saved_credentials_without_a_terminal() {
    let credential = select_credential(None, true, false, || {
        Ok(Some(PublishToken::new("saved-token".into())))
    })
    .unwrap();
    assert!(matches!(credential, Credential::Token(token) if token.expose() == "saved-token"));
    assert!(matches!(
        select_credential(None, true, true, || Ok(None)).unwrap(),
        Credential::DeviceFlow
    ));
}

#[test]
fn preview_publish_keeps_bypass_on_control_requests_and_off_upload_retries() {
    let history = history();
    let hub = LoopbackHub::start();
    let bypass = crate::hub_http::DiscoveryCredentials::load(
        &Url::parse(HUB).unwrap(),
        None,
        Some("PREVIEW_SECRET"),
        |_| Ok("preview-secret".into()),
    )
    .unwrap();
    hub.inject(Fault::CutPut);
    let interrupted = run_publish_with_bypass(
        &hub,
        &history.graph,
        &on_ref("curate/claims", "main"),
        Credential::Token(token(&hub, "curate/claims")),
        bypass.as_ref(),
    );
    assert!(
        interrupted
            .result
            .unwrap_err()
            .to_string()
            .contains("hub.network")
    );
    let run = run_publish_with_bypass(
        &hub,
        &history.graph,
        &on_ref("curate/claims", "main"),
        Credential::Token(token(&hub, "curate/claims")),
        bypass.as_ref(),
    );
    run.result.unwrap();
    let old = RepositoryIdentity::parse("curate/claims").unwrap();
    let new = RepositoryIdentity::parse("research/renamed").unwrap();
    hub.hub.move_repository(&old, &new).unwrap();
    let moved = run_publish_with_bypass(
        &hub,
        &history.graph,
        &on_ref("curate/claims", "main"),
        Credential::Token(token(&hub, "research/renamed")),
        bypass.as_ref(),
    )
    .result
    .unwrap();
    assert_eq!(moved.receipt.repository, new);
    assert_eq!(moved.moved.unwrap().moved_from, old);
    let log = hub.log();
    assert!(log.iter().any(|entry| entry.method == "HEAD"));
    assert!(log.iter().filter(|entry| entry.method == "PUT").count() >= 2);
    for entry in log {
        if entry.method == "HEAD"
            || entry.method == "PUT"
            || (entry.method == "GET" && entry.path.contains("/.gf/objects/"))
        {
            assert!(
                entry.bypass.is_none(),
                "upload purpose must exclude preview credentials"
            );
            assert!(
                entry.bearer.is_none(),
                "upload purpose must exclude publish bearer"
            );
        } else {
            assert_eq!(
                entry.bypass.as_deref(),
                Some("preview-secret"),
                "{} {}",
                entry.method,
                entry.path
            );
        }
    }
    assert!(
        !String::from_utf8(run.stdout)
            .unwrap()
            .contains("preview-secret")
    );
    assert!(
        !String::from_utf8(run.stderr)
            .unwrap()
            .contains("preview-secret")
    );
}

#[test]
fn private_context_first_publish_republish_and_replay_use_only_authorized_hub_metadata() {
    let mut history = history();
    let hub = LoopbackHub::private();
    let credential = || Credential::Token(token(&hub, "curate/claims"));
    let bypass = crate::hub_http::DiscoveryCredentials::load(
        &Url::parse(HUB).unwrap(),
        None,
        Some("BYPASS"),
        |_| Ok("context-preview".into()),
    )
    .unwrap()
    .unwrap();
    let first_args = on_ref("curate/claims", "main");
    let first = run_publish_with_bypass(
        &hub,
        &history.graph,
        &first_args,
        credential(),
        Some(&bypass),
    );
    assert!(first.result.is_ok(), "{:?}", first.result.err());
    edit(&mut history.graph, history.branch, 9);
    let second_args = on_ref("curate/claims", "main");
    let second = run_publish_with_bypass(
        &hub,
        &history.graph,
        &second_args,
        credential(),
        Some(&bypass),
    );
    assert!(second.result.is_ok(), "{:?}", second.result.err());
    let replay = run_publish_with_bypass(
        &hub,
        &history.graph,
        &second_args,
        credential(),
        Some(&bypass),
    );
    assert!(replay.result.unwrap().replayed);
    let log = hub.log();
    let contexts: Vec<_> = log
        .iter()
        .filter(|r| r.path.ends_with("/publish/context"))
        .collect();
    assert_eq!(contexts.len(), 2);
    assert!(
        contexts
            .iter()
            .all(|r| r.bearer.is_some() && r.bypass.as_deref() == Some("context-preview"))
    );
    assert!(!log.iter().any(|r| r.method == "GET"
        && (r.path.ends_with("/.gf/refs")
            || r.path.ends_with("/.gf/manifest")
            || r.path.contains("/objects/"))));
    assert!(
        log.iter()
            .filter(|r| r.method == "PUT" || r.method == "HEAD")
            .all(|r| r.bearer.is_none() && r.bypass.is_none())
    );
    let transport = hub.transport();
    let control = Hub {
        transport: &transport,
        bypass: Some(&bypass),
        base: Url::parse(&repository_url("curate/claims")).unwrap(),
    };
    let identity = RepositoryIdentity::parse("curate/claims").unwrap();
    assert!(read_private_context(&control, &identity, &token(&hub, "curate/another")).is_err());
    let expired = token(&hub, "curate/claims");
    hub.hub.advance_clock(901);
    assert!(read_private_context(&control, &identity, &expired).is_err());
}

fn changed_lineage_binding(
    parsed: &DiscoveryManifest,
    lineage: &[u8],
    base: &Url,
) -> (Vec<u8>, Vec<u8>) {
    // Keep outer descriptors and canonical digests correct so this reaches
    // the semantic ref/Version binding check instead of an earlier hash failure.
    let mut document: serde_json::Value = serde_json::from_slice(lineage).unwrap();
    document["branches"][0]["ref_name"] = json!("unpublished");
    let changed_binding = serde_json::to_vec(&document).unwrap();
    let changed_binding_lineage =
        ResearchLineage::from_json(&changed_binding, DiscoveryLimits::default()).unwrap();
    let mut changed_binding_manifest = parsed.clone();
    let lineage_digest = parsed.lineage.as_ref().unwrap().object_digest.clone();
    let reference = changed_binding_manifest.lineage.as_mut().unwrap();
    reference.lineage_digest = changed_binding_lineage.canonical_digest().unwrap();
    reference.object_digest = Sha256Digest(digest_bytes(&changed_binding));
    let object = changed_binding_manifest
        .objects
        .iter_mut()
        .find(|object| object.digest == lineage_digest)
        .unwrap();
    object.digest = reference.object_digest.clone();
    object.length = changed_binding.len() as u64;
    object.locations = vec![format!(
        "{}/.gf/objects/{}",
        base.as_str().trim_end_matches('/'),
        object.digest.0
    )];
    changed_binding_manifest
        .objects
        .sort_by(|a, b| a.digest.cmp(&b.digest));
    let changed_binding_manifest = serde_json::to_vec(&changed_binding_manifest).unwrap();
    (changed_binding_manifest, changed_binding)
}

fn refs_for_manifest(refs: &[u8], bytes: &[u8]) -> Vec<u8> {
    let manifest = DiscoveryManifest::from_json(bytes, DiscoveryLimits::default()).unwrap();
    let mut refs = RefSet::from_json(refs, DiscoveryLimits::default()).unwrap();
    for reference in &mut refs.refs {
        reference.validator = manifest.canonical_digest().unwrap();
    }
    serde_json::to_vec(&refs).unwrap()
}

fn mutate_context_document(bytes: &[u8], pointer: &str, value: serde_json::Value) -> Vec<u8> {
    let mut document: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    *document.pointer_mut(pointer).unwrap() = value;
    serde_json::to_vec(&document).unwrap()
}

#[test]
fn private_context_checks_exact_revision_and_raw_lineage_integrity_and_bindings() {
    let history = history();
    let server = LoopbackHub::start();
    publish_ok(&server, &history.graph, &on_ref("curate/claims", "main"));
    let base = Url::parse(&repository_url("curate/claims")).unwrap();
    let refs = server.get(endpoint(&base, "refs").as_str()).body;
    let manifest = server.get(endpoint(&base, "manifest").as_str()).body;
    let parsed = DiscoveryManifest::from_json(&manifest, DiscoveryLimits::default()).unwrap();
    let lineage = server
        .get(&parsed.lineage_object().unwrap().locations[0])
        .body;
    let context = |r: &[u8], m: &[u8], l: &[u8]| {
        serde_json::to_vec(&json!({"refs_json":std::str::from_utf8(r).unwrap(),"manifest_json":std::str::from_utf8(m).unwrap(),"lineage_json":std::str::from_utf8(l).unwrap()})).unwrap()
    };
    let transport = server.transport();
    let hub = Hub {
        transport: &transport,
        bypass: None,
        base: base.clone(),
    };
    let identity = RepositoryIdentity::parse("curate/claims").unwrap();
    let credential = token(&server, "curate/claims");
    let path = endpoint(&base, "publish/context").path().to_owned();
    let mut exact_refs = refs.clone();
    exact_refs.push(b'\n');
    server.inject(Fault::Serve(BTreeMap::from([(
        path.clone(),
        context(&exact_refs, &manifest, &lineage),
    )])));
    let state = read_private_context(&hub, &identity, &credential)
        .unwrap()
        .unwrap();
    assert_eq!(state.revision, sha256_digest(&exact_refs));
    let wrong_repo = mutate_context_document(&refs, "/repository/owner", json!("other"));
    let wrong_validator = mutate_context_document(
        &refs,
        "/refs/0/validator",
        json!(format!("sha256:{}", "0".repeat(64))),
    );
    let wrong_ref = mutate_context_document(
        &refs,
        "/refs/0/target",
        json!(format!("sha256:{}", "0".repeat(64))),
    );
    let wrong_package = mutate_context_document(
        &manifest,
        "/package/package_digest",
        json!(format!("sha256:{}", "0".repeat(64))),
    );
    let mut wrong_length = parsed.clone();
    let lineage_digest = wrong_length.lineage.as_ref().unwrap().object_digest.clone();
    wrong_length
        .objects
        .iter_mut()
        .find(|object| object.digest == lineage_digest)
        .unwrap()
        .length += 1;
    let wrong_length = serde_json::to_vec(&wrong_length).unwrap();
    let wrong_canonical = mutate_context_document(
        &manifest,
        "/lineage/lineage_digest",
        json!(format!("sha256:{}", "0".repeat(64))),
    );
    let (changed_binding_manifest, changed_binding) =
        changed_lineage_binding(&parsed, &lineage, &base);
    let mut changed_lineage = lineage.clone();
    changed_lineage.push(b' ');
    let malformed_refs = br#"{"repository":{},"repository":{}}"#.to_vec();
    for (index, body) in [
        context(&wrong_repo, &manifest, &lineage),
        context(&wrong_validator, &manifest, &lineage),
        context(&wrong_ref, &manifest, &lineage),
        context(&refs, &wrong_package, &lineage),
        context(
            &refs_for_manifest(&refs, &wrong_length),
            &wrong_length,
            &lineage,
        ),
        context(
            &refs_for_manifest(&refs, &wrong_canonical),
            &wrong_canonical,
            &lineage,
        ),
        context(
            &refs_for_manifest(&refs, &changed_binding_manifest),
            &changed_binding_manifest,
            &changed_binding,
        ),
        context(&refs, &manifest, &changed_lineage),
        context(&malformed_refs, &manifest, &lineage),
    ]
    .into_iter()
    .enumerate()
    {
        server.inject(Fault::Serve(BTreeMap::from([(path.clone(), body)])));
        assert!(
            read_private_context(&hub, &identity, &credential).is_err(),
            "corruption case {index} must fail"
        );
    }
}

#[test]
fn negotiated_workos_client_eight_mib_chunks_and_hex_locations_publish_real_project() {
    negotiated_native_project(8 * 1024 * 1024, 12 * 1024 * 1024, false);
}

#[test]
fn native_publish_uniform_64_mib_parts_resume_and_async_commit_real_project() {
    negotiated_native_project(64 * 1024 * 1024, 72 * 1024 * 1024, true);
}

#[allow(clippy::too_many_lines)]
fn negotiated_native_project(chunk_bytes: u64, artifact_bytes: usize, uniform: bool) {
    let root = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(root.path().join("project").to_str()).unwrap();
    // A native owned Artifact carries real incompressible content while the
    // historical graph validator retains its normal small semantic footprint.
    // This avoids changing native graph capture/query admission for this test.
    use graphforge_api::{
        ArtifactKind, ArtifactPayloadRequest, CapabilityId, EnableCapabilityRequest,
        RegisterArtifactRequest, RegisterSourceRequest, SourceKind,
    };
    let context = || WriteContext {
        operation_uuid: OperationId(Uuid::now_v7()),
        actor_uuid: None,
    };
    for capability_id in [CapabilityId::Provenance, CapabilityId::Knowledge] {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: context(),
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }
    let source_uuid = Uuid::now_v7();
    graph
        .register_source(RegisterSourceRequest {
            context: context(),
            source_uuid,
            label: "Negotiated upload evidence".into(),
            source_kind: SourceKind::Manuscript,
            identity_uri: None,
        })
        .unwrap();
    let mut state = 0x1234_5678_u32;
    let bytes: Vec<u8> = (0..artifact_bytes)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect();
    graph
        .register_artifact(RegisterArtifactRequest {
            context: context(),
            source_uuid,
            artifact_uuid: Uuid::now_v7(),
            artifact_kind: ArtifactKind::RawScan,
            media_type: "application/octet-stream".into(),
            payload: ArtifactPayloadRequest::LocalBytes(bytes),
            derivation_inputs: vec![],
            run_uuid: None,
        })
        .unwrap();
    create_branch(&mut graph, "main", current());
    let mut config = ReferenceHubConfig {
        base_url: HUB.into(),
        authorization_client_id: Some("client_workos_example".into()),
        object_location_hex: true,
        ..ReferenceHubConfig::default()
    };
    config.limits.preferred_chunk_bytes = Some(chunk_bytes);
    config.limits.max_chunk_bytes = chunk_bytes;
    if uniform {
        config.large_objects = true;
        config.limits.max_object_bytes = 256 * 1024 * 1024;
        config.limits.max_session_bytes = 512 * 1024 * 1024;
    }
    let hub = LoopbackHub::with_config(config);
    hub.inject(if uniform {
        Fault::DropPutReply(chunk_bytes as usize)
    } else {
        Fault::CutPut
    });
    let (interrupted, _, _) = device_flow(&hub, &graph, |index, hub, code| {
        if index == 0 {
            assert!(hub.approve_device(code));
            hub.advance_clock(5);
        }
    });
    let error = interrupted.unwrap_err();
    assert!(
        error.to_string().contains("hub.network"),
        "expected the injected connection cut, got {error}"
    );
    let first_run = hub.log();
    let cut = if uniform {
        puts(&first_run)
            .into_iter()
            .find(|request| request.body_len as u64 == chunk_bytes)
            .unwrap()
            .clone()
    } else {
        puts(&first_run)[0].clone()
    };
    let (_, _, total) =
        graphforge_hub_publish::parse_content_range(cut.content_range.as_deref().unwrap()).unwrap();
    let retained = cut.body_len as u64;
    assert!(retained > 0 && retained < total);
    if uniform {
        assert_eq!(retained, chunk_bytes);
    } else {
        assert!(retained < chunk_bytes);
    }
    let (result, _, _) = device_flow(&hub, &graph, |index, hub, code| {
        if index == 0 {
            assert!(hub.approve_device(code));
            hub.advance_clock(5);
        }
    });
    result.unwrap();
    let log = hub.log();
    let resumed = puts(&log[first_run.len()..])
        .into_iter()
        .find(|x| x.path == cut.path)
        .unwrap();
    let (start, end, advertised_total) =
        graphforge_hub_publish::parse_content_range(resumed.content_range.as_deref().unwrap())
            .unwrap();
    assert_eq!(
        start, retained,
        "resume starts at exact native retained offset"
    );
    assert_eq!(advertised_total, total);
    assert_eq!(resumed.body_len as u64, end - start + 1);

    assert!(
        log.iter()
            .any(|x| x.method == "PUT" && x.body_len as u64 == chunk_bytes),
        "a real negotiated full-size upload must reach the Hub"
    );
    assert!(
        log.iter()
            .filter(|x| x.method == "PUT")
            .all(|x| x.body_len as u64 <= chunk_bytes && x.bearer.is_none() && x.bypass.is_none())
    );
    let response = hub.hub.handle(&HubRequest::new(
        HubMethod::Get,
        format!("{HUB}/curate/claims/.gf/manifest"),
    ));
    let manifest =
        DiscoveryManifest::from_json(&response.body, DiscoveryLimits::default()).unwrap();
    assert!(
        manifest
            .objects
            .iter()
            .any(|object| object.length > chunk_bytes)
    );
    for object in manifest.objects {
        let stored = hub
            .hub
            .handle(&HubRequest::new(HubMethod::Get, &object.locations[0]));
        assert_eq!(stored.status, 200);
        assert_eq!(stored.body.len() as u64, object.length);
        assert_eq!(sha256_digest(&stored.body), object.digest.0);
        assert_eq!(
            object.locations,
            vec![format!(
                "{HUB}/curate/claims/.gf/objects/sha256/{}",
                &object.digest.0[7..]
            )]
        );
    }
}

#[test]
fn negotiated_chunk_budget_refuses_hostile_preferences_without_clamping() {
    let mut limits = ReferenceHubConfig::default().limits;
    assert_eq!(negotiated_chunk_size(&limits).unwrap(), 1024 * 1024);
    limits.preferred_chunk_bytes = Some(8 * 1024 * 1024);
    assert_eq!(negotiated_chunk_size(&limits).unwrap(), 8 * 1024 * 1024);
    limits.preferred_chunk_bytes = Some(u64::MAX);
    assert_eq!(
        error_code(&negotiated_chunk_size(&limits).unwrap_err()),
        "hub.publish.invalid_input"
    );
    assert!(crate::hub_http::chunk_send_timeout(Duration::from_secs(u64::MAX), u64::MAX).is_err());
    assert_eq!(
        crate::hub_http::chunk_send_timeout(Duration::from_secs(300), 8 * 1024 * 1024).unwrap(),
        Duration::from_secs(2400)
    );
}

#[test]
fn per_object_chunk_selection_overrides_the_hub_preference_with_the_same_budget() {
    let mut config = ReferenceHubConfig::default();
    config.limits.preferred_chunk_bytes = Some(8 * 1024 * 1024);
    config.limits.max_chunk_bytes = u64::MAX;
    let capabilities = ReferenceHub::with_config(config)
        .unwrap()
        .capabilities(&RepositoryIdentity::parse("curate/claims").unwrap());
    let mut target = UploadTarget {
        digest: sha256_digest(b"object"),
        length: 128 * 1024 * 1024,
        upload_url: format!("{HUB}/.gf/uploads/test"),
        received: 0,
        chunk_bytes: Some(4 * 1024 * 1024),
    };
    assert_eq!(
        upload_chunk_size(&capabilities, &target).unwrap(),
        4 * 1024 * 1024
    );
    target.chunk_bytes = None;
    assert_eq!(
        upload_chunk_size(&capabilities, &target).unwrap(),
        8 * 1024 * 1024
    );
    target.chunk_bytes = Some(95 * 1024 * 1024 + 1);
    assert_eq!(
        error_code(&upload_chunk_size(&capabilities, &target).unwrap_err()),
        "hub.publish.invalid_input"
    );
}

mod staging;

#[test]
fn native_publish_polls_async_verification_and_replays_the_original_receipt() {
    let history = history();
    let mut config = ReferenceHubConfig {
        base_url: HUB.into(),
        large_objects: true,
        ..ReferenceHubConfig::default()
    };
    config.limits.preferred_chunk_bytes = Some(64 * 1024 * 1024);
    config.limits.max_chunk_bytes = 64 * 1024 * 1024;
    let hub = LoopbackHub::with_config(config);
    let selected = on_ref("curate/async-publication", "main");
    let first = publish_ok(&hub, &history.graph, &selected).result.unwrap();
    let second = publish_ok(&hub, &history.graph, &selected).result.unwrap();
    assert_eq!(first.receipt, second.receipt);
    assert!(second.replayed);
    let logs = hub.log.lock().unwrap();
    assert!(
        logs.iter()
            .filter(|request| request.path.contains("/publish/operations/"))
            .count()
            >= 3
    );
    assert_eq!(
        logs.iter()
            .filter(|request| request.method == "POST" && request.path.ends_with("/commit"))
            .count(),
        1
    );
    assert_eq!(
        hub.hub
            .revisions(&RepositoryIdentity::parse("curate/async-publication").unwrap())
            .len(),
        1
    );
}

fn assert_rehomed_clones(hub: &LoopbackHub, history: &History) {
    let root = tempfile::tempdir().unwrap();
    for (name, repository) in [
        ("old", "curate/claims"),
        ("short", "curate/claims"),
        ("new", "research/renamed"),
    ] {
        let destination = root.path().join(name);
        let cloned = if name == "short" {
            let mut output = Vec::new();
            run_clone_with(
                &hub.transport(),
                CloneArgs {
                    hub: Some(HUB.into()),
                    token_env: None,
                    protection_bypass_env: None,
                    repository: repository.into(),
                    destination: Some(destination.clone()),
                    telemetry_endpoint: None,
                    git_ref: None,
                    version_uuid: None,
                },
                true,
                &mut output,
            )
            .unwrap();
            serde_json::from_slice(&output).unwrap()
        } else {
            clone_into(&hub, repository, &destination, None, None)
        };
        assert_eq!(cloned["repository"], "research/renamed");
        if name != "new" {
            assert_eq!(cloned["moved_from"], "curate/claims");
            assert_eq!(cloned["moved_to"], "research/renamed");
        }
        let reopened = GraphForge::new(destination.to_str()).unwrap();
        assert_eq!(
            reference(&reopened, history.head).identity_sha256,
            reference(&history.graph, history.head).identity_sha256
        );
        let query = reopened
            .execute("MATCH (n:Item) RETURN count(n) AS total, sum(n.score) AS score")
            .unwrap();
        for (column, expected) in [(0, 1), (1, 1)] {
            assert_eq!(
                query.batches[0]
                    .column(column)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .value(0),
                expected
            );
        }
    }
    let mut human_clone = Vec::new();
    run_clone_with(
        &hub.transport(),
        CloneArgs {
            hub: Some(HUB.into()),
            token_env: None,
            protection_bypass_env: None,
            repository: "curate/claims".into(),
            destination: Some(root.path().join("human")),
            telemetry_endpoint: None,
            git_ref: None,
            version_uuid: None,
        },
        false,
        &mut human_clone,
    )
    .unwrap();
    assert!(
        String::from_utf8(human_clone)
            .unwrap()
            .contains("Repository moved from curate/claims to research/renamed")
    );
}

#[test]
fn moved_repository_clones_both_names_and_publishes_only_with_current_scope() {
    let mut history = history();
    let hub = LoopbackHub::start();
    publish_ok(&hub, &history.graph, &on_ref("curate/claims", "main"));
    let old = RepositoryIdentity::parse("curate/claims").unwrap();
    let new = RepositoryIdentity::parse("research/renamed").unwrap();
    let (_, before) = served_lineage(&hub, "curate/claims");
    hub.hub.move_repository(&old, &new).unwrap();
    let (_, after) = served_lineage(&hub, "research/renamed");
    assert_eq!(after.versions, before.versions);
    assert_rehomed_clones(&hub, &history);
    edit(&mut history.graph, history.branch, 3);
    let before_log = hub.log().len();
    let stale = run_publish_with(
        &hub,
        &history.graph,
        &on_ref("curate/claims", "main"),
        Credential::Token(token(&hub, "curate/claims")),
    );
    assert!(stale.result.is_err());
    assert!(puts(&hub.log()[before_log..]).is_empty());
    let current = run_publish_with(
        &hub,
        &history.graph,
        &on_ref("curate/claims", "main"),
        Credential::Token(token(&hub, "research/renamed")),
    );
    let outcome = current.result.unwrap();
    assert_eq!(outcome.receipt.repository, new);
    assert_eq!(outcome.moved.as_ref().unwrap().moved_from, old);
    let mut human = Vec::new();
    write_outcome(&outcome, false, &mut human).unwrap();
    assert!(
        String::from_utf8(human)
            .unwrap()
            .contains("Repository moved from curate/claims to research/renamed")
    );
    let json: serde_json::Value = serde_json::from_slice(&current.stdout).unwrap();
    assert_eq!(json["moved_to"], "research/renamed");
    assert!(
        hub.log()[before_log..]
            .iter()
            .filter(|entry| entry.method == "POST")
            .all(|entry| entry.path.starts_with("/research/renamed/"))
    );
}

#[test]
fn selected_png_svg_previews_are_version_bound_but_not_native_package_data() {
    selected_previews_preserve_native_data_and_moves(false);
}

#[test]
fn async_selected_previews_preserve_native_data_and_moves() {
    selected_previews_preserve_native_data_and_moves(true);
}

fn selected_previews_preserve_native_data_and_moves(large_objects: bool) {
    let history = history();
    let mut config = ReferenceHubConfig {
        base_url: HUB.into(),
        authorization_client_id: Some("client_workos_preview".into()),
        object_location_hex: true,
        large_objects,
        ..ReferenceHubConfig::default()
    };
    config.limits.preferred_chunk_bytes = Some(8 * 1024 * 1024);
    let plain = LoopbackHub::with_config(config);
    publish_ok(&plain, &history.graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let (plain_manifest, _) = served_lineage(&plain, "curate/claims");
    let root = tempfile::tempdir().unwrap();
    let cases: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../docs/reference/hub-publish/v1/previews.json"
    ))
    .unwrap();
    let hex = cases[0]["hex"].as_str().unwrap();
    let png: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|n| u8::from_str_radix(&hex[n..n + 2], 16).unwrap())
        .collect();
    let svg = cases[1]["utf8"].as_str().unwrap().as_bytes();
    let paths = [
        root.path().join("selected.png"),
        root.path().join("selected.svg"),
    ];
    std::fs::write(&paths[0], &png).unwrap();
    std::fs::write(&paths[1], svg).unwrap();
    // Adjacent unsafe/private workbench files are never inferred as selections.
    std::fs::write(root.path().join("unselected.svg"), b"<script/>").unwrap();
    let selected = &plain;
    let mut args = on_ref("curate/claims", "main");
    args.previews = paths.to_vec();
    let first = publish_ok(selected, &history.graph, &args).result.unwrap();
    assert!(!first.replayed);
    assert!(
        publish_ok(selected, &history.graph, &args)
            .result
            .unwrap()
            .replayed
    );
    let (manifest, _) = served_lineage(&selected, "curate/claims");
    assert_eq!(plain_manifest.package, manifest.package);
    let plain_package = plain.hub.handle(&HubRequest::new(
        HubMethod::Get,
        &plain_manifest.package_object().unwrap().locations[0],
    ));
    let selected_package = selected.hub.handle(&HubRequest::new(
        HubMethod::Get,
        &manifest.package_object().unwrap().locations[0],
    ));
    assert_eq!(
        plain_package.body, selected_package.body,
        "preview channel never alters native package bytes"
    );
    let previews = manifest.preview_descriptors().unwrap();
    assert_eq!(previews.len(), 2);
    for (item, expected) in previews.iter().zip([png.as_slice(), svg]) {
        assert_eq!(item.version_uuid, history.head.to_string());
        let object = manifest
            .objects
            .iter()
            .find(|object| object.digest.0 == item.digest)
            .unwrap();
        let response = selected
            .hub
            .handle(&HubRequest::new(HubMethod::Get, &object.locations[0]));
        assert_eq!(response.status, 200);
        assert_eq!(response.body, expected);
        graphforge_hub_publish::preview::validate_preview(item, &response.body).unwrap();
    }
    // New validated image selections get new default identities. An explicit
    // reused operation remains a conflict, preserving caller-stable CAS intent.
    std::fs::write(&paths[1],br#"<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16"><rect width="2" height="2"/></svg>"#).unwrap();
    let explicit = PublishArgs {
        operation_uuid: Some(first.receipt.operation_uuid.to_string()),
        previews: args.previews.clone(),
        ..on_ref("curate/claims", "main")
    };
    let refused = run_publish_with(
        selected,
        &history.graph,
        &explicit,
        Credential::Token(token(selected, "curate/claims")),
    );
    assert_eq!(
        refused.result.unwrap_err().code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
    let changed = publish_ok(selected, &history.graph, &args).result.unwrap();
    assert!(!changed.replayed);
    assert_ne!(changed.receipt.operation_uuid, first.receipt.operation_uuid);
    assert!(
        publish_ok(selected, &history.graph, &args)
            .result
            .unwrap()
            .replayed
    );
    let (final_manifest, _) = served_lineage(selected, "curate/claims");
    let previews = final_manifest.preview_descriptors().unwrap();
    let before = selected.log().len();
    clone_into(
        &selected,
        "curate/claims",
        &root.path().join("clone"),
        Some("main"),
        None,
    );
    let reads = selected.log();
    for item in &previews {
        assert!(
            !reads[before..]
                .iter()
                .any(|r| r.method == "GET" && r.path.contains(&item.digest[7..]))
        );
    }

    // Re-homing preserves the selected presentation channel, while publication
    // through either name derives the same current-identity operation.
    let old = RepositoryIdentity::parse("curate/claims").unwrap();
    let new = RepositoryIdentity::parse("research/renamed").unwrap();
    selected.hub.move_repository(&old, &new).unwrap();
    let (moved_manifest, _) = served_lineage(selected, "research/renamed");
    assert_eq!(moved_manifest.preview_descriptors().unwrap(), previews);
    assert_eq!(moved_manifest.package, final_manifest.package);
    for (item, path) in previews.iter().zip(&paths) {
        let object = moved_manifest
            .objects
            .iter()
            .find(|object| object.digest.0 == item.digest)
            .unwrap();
        let response = selected
            .hub
            .handle(&HubRequest::new(HubMethod::Get, &object.locations[0]));
        assert_eq!(response.status, 200);
        assert_eq!(response.body, std::fs::read(path).unwrap());
    }
    let before = selected.log().len();
    let moved = run_publish_with(
        selected,
        &history.graph,
        &args,
        Credential::Token(token(selected, "research/renamed")),
    )
    .result
    .unwrap();
    assert_eq!(moved.receipt.repository, new);
    assert_eq!(moved.moved.as_ref().unwrap().moved_from, old);
    args.repository = repository_url("research/renamed");
    let before_replay = selected.log().len();
    let canonical = publish_ok(selected, &history.graph, &args).result.unwrap();
    assert!(canonical.replayed);
    assert_eq!(canonical.receipt, moved.receipt);
    assert!(puts(&selected.log()[before_replay..]).is_empty());
    assert!(
        selected.log()[before..]
            .iter()
            .filter(|entry| entry.method == "POST")
            .all(|entry| entry.path.starts_with("/research/renamed/"))
    );
}

#[test]
fn unsafe_or_oversize_selected_preview_fails_before_any_upload() {
    let history = history();
    let root = tempfile::tempdir().unwrap();
    for (name, bytes) in [
        (
            "unsafe.svg",
            br#"<svg xmlns="http://www.w3.org/2000/svg" width="1" height="1"><script/></svg>"#
                .to_vec(),
        ),
        (
            "oversize.png",
            vec![0; graphforge_hub_publish::preview::MAX_PREVIEW_BYTES + 1],
        ),
        ("not-image.html", b"<html/>".to_vec()),
    ] {
        let path = root.path().join(name);
        std::fs::write(&path, bytes).unwrap();
        let hub = LoopbackHub::start();
        let mut args = on_ref("curate/claims", "main");
        args.previews = vec![path];
        let run = run_publish_with(
            &hub,
            &history.graph,
            &args,
            Credential::Token(token(&hub, "curate/claims")),
        );
        let error = run.result.unwrap_err();
        assert!(error.to_string().contains("preview."));
        assert!(puts(&hub.log()).is_empty());
        assert!(
            hub.log().is_empty(),
            "invalid selected images fail before networking"
        );
    }
}

#[test]
fn reference_hub_revalidates_unsafe_images_even_when_client_admission_is_bypassed() {
    use graphforge_hub_publish::preview::{PREVIEW_EXTENSION, PreviewDescriptor, PreviewSet};
    use graphforge_hub_publish::{Capability, content_range};
    let history = history();
    let hub = LoopbackHub::start();
    publish_ok(&hub, &history.graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let (original, _) = served_lineage(&hub, "curate/claims");
    let bytes=br#"<svg xmlns="http://www.w3.org/2000/svg" width="1" height="1"><script>alert(1)</script></svg>"#.to_vec();
    let valid =
        br#"<svg xmlns="http://www.w3.org/2000/svg" width="1" height="1"><path d="M0 0"/></svg>"#
            .to_vec();
    for (bytes, version, detail) in [
        (bytes, history.head.to_string(), "Core image admission"),
        (
            valid,
            Uuid::now_v7().to_string(),
            "absent from native lineage",
        ),
    ] {
        let digest = sha256_digest(&bytes);
        let mut manifest = original.clone();
        manifest.objects.push(object_descriptor(
            digest.clone(),
            bytes.len() as u64,
            "image/svg+xml",
            format!("{HUB}/curate/claims/.gf/objects/{digest}"),
        ));
        manifest.objects.sort_by(|a, b| a.digest.cmp(&b.digest));
        manifest.extensions.insert(
            PREVIEW_EXTENSION.into(),
            serde_json::to_value(PreviewSet {
                format: "graphforge-previews/1".into(),
                previews: vec![PreviewDescriptor {
                    version_uuid: version,
                    digest: digest.clone(),
                    media_type: "image/svg+xml".into(),
                    length: bytes.len() as u64,
                    width: 1,
                    height: 1,
                    caption: None,
                    alt: None,
                }],
            })
            .unwrap(),
        );
        manifest.validate(DiscoveryLimits::default()).unwrap(); // Metadata alone is valid.
        let objects: Vec<_> = manifest
            .objects
            .iter()
            .map(|o| ObjectDeclaration {
                digest: o.digest.0.clone(),
                length: o.length,
                media_type: o.media_type.clone(),
            })
            .collect();
        let expected_revision = Some(sha256_digest(&refs_document(&hub, "curate/claims").body));
        let intent = PublishIntent {
            repository: manifest.repository.clone(),
            intent_digest: sha256_digest(b"untrusted preview caller"),
            objects: objects.clone(),
            manifest_validator: manifest.canonical_digest().unwrap().0,
            refs: vec!["main".into()],
            expected_revision: expected_revision.clone(),
        };
        let token = token(&hub, "curate/claims");
        let response = hub.hub.handle(
            &HubRequest::new(
                HubMethod::Post,
                format!("{HUB}/curate/claims/.gf/publish/sessions"),
            )
            .with_header("authorization", token.authorization())
            .with_json(&OpenSessionRequest {
                format: HUB_PUBLISH_FORMAT.into(),
                operation_uuid: Uuid::now_v7(),
                request_commitment: intent.request_commitment(),
                intent_digest: intent.intent_digest.clone(),
                repository: manifest.repository.clone(),
                objects,
                requirements: vec![Capability::new("version-previews", 1)],
            }),
        );
        assert_eq!(response.status, 201);
        let SessionResponse::Open {
            session_id,
            uploads,
        } = serde_json::from_slice(&response.body).unwrap()
        else {
            panic!("new session")
        };
        for upload in uploads {
            if upload.received == upload.length {
                continue;
            }
            let body = if upload.digest == digest {
                bytes.clone()
            } else {
                let location = manifest
                    .objects
                    .iter()
                    .find(|o| o.digest.0 == upload.digest)
                    .unwrap()
                    .locations[0]
                    .clone();
                hub.hub
                    .handle(&HubRequest::new(HubMethod::Get, location))
                    .body
            };
            let response = hub.hub.handle(
                &HubRequest::new(HubMethod::Put, upload.upload_url)
                    .with_header(
                        "content-range",
                        content_range(0, body.len() as u64, body.len() as u64),
                    )
                    .with_body(body),
            );
            assert_eq!(response.status, 200);
        }
        let before = refs_document(&hub, "curate/claims").body;
        let response = hub.hub.handle(
            &HubRequest::new(
                HubMethod::Post,
                format!("{HUB}/curate/claims/.gf/publish/sessions/{session_id}/commit"),
            )
            .with_header("authorization", token.authorization())
            .with_json(&CommitRequest {
                manifest: serde_json::to_value(manifest).unwrap(),
                refs: vec!["main".into()],
                expected_revision,
            }),
        );
        assert_eq!(response.status, 400);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["code"], "invalid_input");
        assert!(body["message"].as_str().unwrap().contains(detail));
        assert_eq!(refs_document(&hub, "curate/claims").body, before);
    }
}

#[test]
fn opaque_workbench_file_cannot_be_selected_as_native_package_data() {
    use graphforge_api::{
        PortableV2ErrorCode, PortableV2Limits, PortableV2ParticipantId, PortableV2SelectionProfile,
        PortableV2SelectionRequest,
    };
    let history = history();
    std::fs::write(
        history.root.path().join("project/notebook.ipynb"),
        b"{\"cells\":[]}",
    )
    .unwrap();
    let error = history
        .graph
        .preview_portable_v2_selection(&graphforge_api::PortableV2SelectionPreviewRequest {
            selection: graphforge_api::PortableSelection::Current,
            request: PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::Custom(vec![PortableV2ParticipantId {
                    capability_id: "opaque-files".into(),
                    record_family_id: "notebook.ipynb".into(),
                }]),
                strict: true,
            },
            limits: PortableV2Limits::default(),
        })
        .unwrap_err();
    assert_eq!(error.code, PortableV2ErrorCode::Incompatible);
    assert!(
        error
            .to_string()
            .contains("custom selector is missing or ambiguous")
    );
}

fn edit_by(graph: &mut GraphForge, branch: Uuid, score: i64, name: &str) -> Uuid {
    let version_uuid = Uuid::now_v7();
    let signature = graphforge_storage::research_versions::ResearchSignature {
        name: name.into(),
        email: None,
        orcid: None,
    };
    graph
        .execute_research_branch(
            &ExecuteResearchBranchRequest {
                author: Some(signature.clone()),
                committer: Some(signature),
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: generation(graph),
                branch_uuid: branch,
                version_uuid,
                created_at: 3,
                query: format!("MATCH (n:Item) SET n.score={score}"),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    version_uuid
}

#[test]
fn cloned_branch_continues_with_second_committer_and_fast_forwards_without_payload_history() {
    let mut history = history();
    let alice = edit_by(&mut history.graph, history.branch, 2, "Alice");
    let original_branch =
        history.graph.research_version_retention().unwrap().branches[&history.branch].clone();
    let hub = LoopbackHub::start();
    publish_ok(&hub, &history.graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let destinations = tempfile::tempdir().unwrap();
    let bob_path = destinations.path().join("bob");
    let eve_path = destinations.path().join("eve");
    clone_into(&hub, "curate/claims", &bob_path, Some("main"), None);
    clone_into(&hub, "curate/claims", &eve_path, Some("main"), None);
    let mut bob = GraphForge::new(bob_path.to_str()).unwrap();
    let imported = bob.research_version_retention().unwrap();
    assert_eq!(imported.branches[&history.branch], original_branch);
    assert_eq!(imported.heads[&history.branch], alice);
    assert_eq!(imported.heads.len(), 1);
    assert!(!imported.versions.contains_key(&history.base));
    assert!(imported.historical_version(history.base).is_some());
    let bob_version = edit_by(&mut bob, history.branch, 3, "Bob");
    assert_eq!(
        bob.research_version(bob_version).unwrap().parents,
        vec![alice]
    );
    publish_ok(&hub, &bob, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let (_, lineage) = served_lineage(&hub, "curate/claims");
    assert!(lineage.descends_from(&bob_version.to_string(), &alice.to_string()));
    let ancestor = lineage.version(&alice.to_string()).unwrap();
    // Historical records travel independently of native payload retention.
    assert_eq!(ancestor.parents, vec![history.head.to_string()]);
    let final_path = destinations.path().join("final");
    clone_into(&hub, "curate/claims", &final_path, Some("main"), None);
    let reopened = GraphForge::new(final_path.to_str()).unwrap();
    let query = reopened
        .execute("MATCH (n:Item) RETURN count(n), sum(n.score)")
        .unwrap();
    for (column, expected) in [(0, 1), (1, 3)] {
        assert_eq!(
            query.batches[0]
                .column(column)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            expected
        );
    }
    for (version, name) in [(alice, "Alice"), (bob_version, "Bob")] {
        let record = reopened.research_version(version).unwrap();
        assert_eq!(record.author.as_ref().unwrap().name, name);
        assert_eq!(record.committer.as_ref().unwrap().name, name);
    }
    let before = refs_document(&hub, "curate/claims");
    let mut eve = GraphForge::new(eve_path.to_str()).unwrap();
    edit_by(&mut eve, history.branch, 4, "Eve");
    let log = hub.log().len();
    let refused = run_publish_with(
        &hub,
        &eve,
        &on_ref("curate/claims", "main"),
        Credential::Token(token(&hub, "curate/claims")),
    );
    assert_eq!(
        error_code(&refused.result.unwrap_err()),
        "hub.publish.non_fast_forward"
    );
    assert!(puts(&hub.log()[log..]).is_empty());
    let after = refs_document(&hub, "curate/claims");
    assert_eq!(before, after);
}

#[test]
fn unavailable_released_intermediate_metadata_refuses_before_upload() {
    let mut history = history();
    edit(&mut history.graph, history.branch, 2);
    let generation = history
        .graph
        .committed_generation_identity()
        .unwrap()
        .generation_uuid;
    history
        .graph
        .commit_research_version_operation(
            graphforge_api::ResearchOperation {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: generation,
                mutation: graphforge_api::ResearchMutation::DeleteVersion {
                    version_uuid: history.head,
                },
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let state = history.graph.research_version_retention().unwrap();
    assert!(!state.versions.contains_key(&history.head));
    assert!(state.identities.contains_key(&history.head));
    let hub = LoopbackHub::start();
    let refused = run_publish_with(
        &hub,
        &history.graph,
        &on_ref("curate/claims", "main"),
        Credential::Token(token(&hub, "curate/claims")),
    );
    assert!(refused.result.is_err());
    assert!(puts(&hub.log()).is_empty());
    assert_eq!(refs_document(&hub, "curate/claims").status, 404);
}
