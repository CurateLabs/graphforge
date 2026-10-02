//! `gf publish`: derive one repository snapshot client-side and publish it to a
//! Hub over the `graphforge-hub-publish/1` contract (ADR 0053).
//!
//! The client exports and verifies every package, derives the summary,
//! ontology, lineage, and manifest documents, uploads object bytes only to the
//! data-plane URLs the Hub issues, and advances refs only on the repository's
//! expected revision. The publish token is held in memory, sent only to the
//! repository's control-plane origin, and never written, logged, or echoed.

use crate::hub_http::{
    MAX_METADATA_BYTES, MAX_REDIRECTS, Transport, endpoint, network, parse_input,
    validate_upload_url, validate_url, validation,
};
use clap::{ArgGroup, Args};
use graphforge_api::{
    BuildResearchLineageRequest, CancellationToken, ExportResearchRequest, GfError, GraphForge,
    PortableV2SelectionProfile, ProjectErrorCode, ResearchReferenceTarget,
};
use graphforge_discovery::{
    DiscoveryLimits, DiscoveryManifest, ObjectDescriptor, RESEARCH_LINEAGE_FORMAT,
    RESEARCH_LINEAGE_MEDIA_TYPE, RefSet, RepositoryIdentity, ResearchLineage,
    ResearchLineageReference, Sha256Digest,
};
use graphforge_hub_publish::{
    CommitRequest, DEVICE_CODE_GRANT_TYPE, DeviceAuthorizationResponse, DevicePoll,
    HUB_PUBLISH_FORMAT, HubErrorBody, HubMethod, HubPublishErrorCode, HubRequest, HubResponse,
    ObjectDeclaration, OpenSessionRequest, OperationStatus, PublishCapabilities, PublishIntent,
    PublishReceipt, PublishToken, SessionResponse, UPLOAD_OFFSET_HEADER, UploadStatus,
    UploadTarget, canonical_json, content_range, publish_scope, sha256_digest,
};
use publication::{
    ManifestInputs, PACKAGE_MEDIA_TYPE, SUMMARY_MEDIA_TYPE, VerifiedBundle, build_manifest,
    derive_summary, digest_bytes, export_module_packages, export_verified_bundle,
    object_descriptor, summary_reference, verify_exported_bundle,
};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use url::Url;
use uuid::Uuid;

pub(crate) mod publication;

/// Set once by the streaming `gf` process; captured invocations (bindings,
/// tests) never prompt.
static DEVICE_FLOW_ALLOWED: AtomicBool = AtomicBool::new(false);

/// Allow the device flow when this process is attached to a terminal.
pub(crate) fn allow_device_flow() {
    DEVICE_FLOW_ALLOWED.store(true, Ordering::Relaxed);
}

/// OAuth client identifier the CLI presents to device authorization.
const CLIENT_ID: &str = "graphforge-cli";
/// Largest upload chunk the client sends, whatever the Hub allows.
const MAX_CLIENT_CHUNK_BYTES: u64 = 8 * 1024 * 1024;
/// Largest control-plane response the client reads.
const MAX_CONTROL_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
/// Format of the canonical intent whose digest identifies what an operation publishes.
const INTENT_FORMAT: &str = "graphforge-hub-publish-intent/1";

/// Publish a research Branch head or exact Version to a GraphForge Hub.
#[derive(Args)]
#[command(group(ArgGroup::new("selection").required(true).args(["git_ref", "version_uuid"])))]
pub(crate) struct PublishArgs {
    /// Canonical owner/repository name or an HTTPS Hub repository URL.
    pub repository: String,
    /// Research Branch to publish, named by its label; its head becomes this
    /// discovery ref.
    #[arg(long = "ref")]
    pub git_ref: Option<String>,
    /// Exact immutable research Version UUID to publish into the repository.
    #[arg(long)]
    pub version_uuid: Option<String>,
    /// Origin repository of a Fork (owner/repository or HTTPS URL). Publishes a
    /// new repository that cites its origin Version.
    #[arg(long)]
    pub fork_of: Option<String>,
    /// Caller-stable operation UUID; defaults to one derived from the
    /// repository, ref, and Version, so a rerun replays the original receipt.
    #[arg(long)]
    pub operation_uuid: Option<String>,
}

/// How the CLI obtains its publish token.
pub(crate) enum Credential {
    /// A token handed over by the environment (CI).
    Token(PublishToken),
    /// Run the RFC 8628 device flow against the Hub's advertised endpoints.
    DeviceFlow,
}

/// Process-side effects a publish needs besides the Hub transport.
pub(crate) struct PublishEnvironment<'a> {
    /// Credential source.
    pub credential: Credential,
    /// Waits between device-flow polls.
    pub wait: &'a dyn Fn(Duration),
    /// Human instructions (device flow); never receives secrets.
    pub notices: &'a mut dyn Write,
}

/// Published result written to stdout.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct PublishOutcome {
    /// Original Hub receipt.
    pub receipt: PublishReceipt,
    /// Whether this run replayed a committed operation without any upload.
    pub replayed: bool,
}

/// Dispatch the Hub commands, `gf clone` and `gf publish`.
pub(crate) fn run_hub_command(
    command: crate::Command,
    project: Option<PathBuf>,
    project_dir: Option<PathBuf>,
    json: bool,
    output: &mut dyn Write,
) -> Result<i32, crate::CliRuntimeError> {
    match command {
        crate::Command::Clone(args) => {
            Ok(crate::hub_clone::run_clone(args, json, output).map(|()| 0)?)
        }
        crate::Command::Publish(args) => run(&args, project, project_dir, json, output),
        _ => unreachable!("only Hub commands are dispatched here"),
    }
}

/// `gf publish` entry point used by the CLI dispatcher.
///
/// The device flow runs only in the streaming `gf` process with standard input
/// and standard error on a terminal; otherwise a missing token is `auth_denied`
/// before any network request.
fn run(
    args: &PublishArgs,
    project: Option<PathBuf>,
    project_dir: Option<PathBuf>,
    json: bool,
    output: &mut dyn Write,
) -> Result<i32, crate::CliRuntimeError> {
    run_command(args, project, project_dir, json, output)
        .map(|()| 0)
        .map_err(Into::into)
}

fn run_command(
    args: &PublishArgs,
    project: Option<PathBuf>,
    project_dir: Option<PathBuf>,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    use std::io::IsTerminal as _;
    let interactive = DEVICE_FLOW_ALLOWED.load(Ordering::Relaxed)
        && std::io::stdin().is_terminal()
        && std::io::stderr().is_terminal();
    // Locations and arguments are validated before any credential or network use.
    parse_input(&args.repository)?;
    if let Some(origin) = &args.fork_of {
        parse_input(origin)?;
    }
    let credential = match PublishToken::from_env() {
        Some(token) => Credential::Token(token),
        None if interactive => Credential::DeviceFlow,
        None => {
            return Err(publish_error(
                HubPublishErrorCode::AuthDenied,
                "no publish credential: set GRAPHFORGE_HUB_PUBLISH_TOKEN or run gf publish in a terminal",
            ));
        }
    };
    let path = crate::resolve_project_path(project, project_dir)?;
    let path = path
        .to_str()
        .ok_or_else(|| GfError::Validation("--project must be valid UTF-8".into()))?;
    let graph = GraphForge::new(Some(path))?;
    let stderr = std::io::stderr();
    let mut notices = stderr.lock();
    let mut environment = PublishEnvironment {
        credential,
        wait: &std::thread::sleep,
        notices: &mut notices,
    };
    let outcome = publish(
        &crate::hub_http::HttpTransport::new(),
        &graph,
        args,
        &mut environment,
    )?;
    write_outcome(&outcome, json, output)
}

pub(crate) fn write_outcome(
    outcome: &PublishOutcome,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let io = |error: std::io::Error| GfError::Storage(error.to_string());
    if json {
        serde_json::to_writer(&mut *output, &outcome.receipt)
            .map_err(|error| GfError::Execution(error.to_string()))?;
        writeln!(output).map_err(io)
    } else {
        writeln!(
            output,
            "Published {} ({}) at revision {}",
            outcome.receipt.repository.canonical_name(),
            outcome.receipt.refs.join(", "),
            outcome.receipt.revision
        )
        .map_err(io)
    }
}

// ------------------------------------------------------------------- errors

/// Project a publish failure onto the CLI's stable error codes.
///
/// Idempotency conflicts use `GF_IDEMPOTENCY_CONFLICT`; transport and Hub
/// internal failures are storage errors; everything else is a validation error
/// whose message starts with the stable `hub.publish.*` code. Messages are
/// fixed client text: Hub-supplied text, tokens, and upload URLs never appear.
fn publish_error(code: HubPublishErrorCode, detail: &str) -> GfError {
    let wire = graphforge_hub_publish::HubPublishError::new(code, "").wire_code();
    match code {
        HubPublishErrorCode::IdempotencyConflict => GfError::Project {
            code: ProjectErrorCode::TransactionConflict,
            message: format!("{wire}: {detail}"),
        },
        HubPublishErrorCode::Internal => GfError::Storage(format!("{wire}: {detail}")),
        _ => GfError::Validation(format!("{wire}: {detail}")),
    }
}

const fn code_detail(code: HubPublishErrorCode) -> &'static str {
    match code {
        HubPublishErrorCode::AuthDenied => "the Hub refused the publish credential",
        HubPublishErrorCode::EntitlementDenied => "the Hub refused the publication entitlement",
        HubPublishErrorCode::IdempotencyConflict => {
            "the operation identity was already used for a different publication"
        }
        HubPublishErrorCode::RefConflict => {
            "the repository changed since its revision was read; publish again"
        }
        HubPublishErrorCode::UnsupportedFuture => {
            "the Hub requires an unsupported protocol version or capability"
        }
        HubPublishErrorCode::IntegrityFailure => {
            "the Hub refused object bytes that disagree with their digest or length"
        }
        HubPublishErrorCode::InvalidInput => "the Hub refused the request as invalid",
        HubPublishErrorCode::Internal => "the Hub failed internally",
    }
}

/// Classify an unsuccessful Hub response from its `{code, message}` body, or
/// from its status when the body is not a publish error.
fn hub_failure(response: &HubResponse) -> GfError {
    let code = serde_json::from_slice::<HubErrorBody>(&response.body)
        .map(|body| body.code)
        .ok()
        .or(match response.status {
            401 | 403 => Some(HubPublishErrorCode::AuthDenied),
            409 => Some(HubPublishErrorCode::IdempotencyConflict),
            412 => Some(HubPublishErrorCode::RefConflict),
            _ => None,
        });
    match code {
        Some(code) => publish_error(code, code_detail(code)),
        None => network("Hub returned an unsuccessful status"),
    }
}

fn protocol(error: &graphforge_hub_publish::HubPublishError) -> GfError {
    publish_error(error.code, error.detail())
}

fn derivation(error: &str) -> GfError {
    GfError::Validation(format!("hub.publish.derivation: {error}"))
}

fn discovery(error: &graphforge_discovery::DiscoveryError) -> GfError {
    let code = match error.code {
        graphforge_discovery::DiscoveryErrorCode::UnsupportedFuture => {
            HubPublishErrorCode::UnsupportedFuture
        }
        graphforge_discovery::DiscoveryErrorCode::IntegrityFailure => {
            HubPublishErrorCode::IntegrityFailure
        }
        _ => HubPublishErrorCode::InvalidInput,
    };
    publish_error(code, error.detail())
}

// --------------------------------------------------------------- transport

/// A fetched document body and its `ETag`.
type Document = (Vec<u8>, Option<String>);

/// Control-plane and data-plane exchange over one transport.
struct Hub<'a> {
    transport: &'a dyn Transport,
    base: Url,
}

impl Hub<'_> {
    /// Public, redirect-bounded read of a repository document or object.
    /// `None` on 404. Every hop is validated; no credential is sent.
    fn read(&self, url: &Url, limit: usize) -> Result<Option<Document>, GfError> {
        let mut url = url.clone();
        for hop in 0..=MAX_REDIRECTS {
            self.transport.validate(&url)?;
            let response = self
                .transport
                .exchange(&HubRequest::new(HubMethod::Get, url.as_str()), limit)?;
            match response.status {
                301 | 302 | 303 | 307 | 308 if hop < MAX_REDIRECTS => {
                    let location = response
                        .header("location")
                        .ok_or_else(|| network("redirect is missing Location"))?;
                    url = url
                        .join(location)
                        .map_err(|_| validation("hub.unsafe_location", "invalid redirect URL"))?;
                }
                301 | 302 | 303 | 307 | 308 => return Err(network("redirect limit exceeded")),
                404 => return Ok(None),
                200..=299 => {
                    let etag = response.header("etag").map(str::to_owned);
                    return Ok(Some((response.body, etag)));
                }
                _ => return Err(hub_failure(&response)),
            }
        }
        unreachable!("the redirect loop returns on its last hop")
    }

    /// Bearer-authorized control-plane request. The token is attached only when
    /// the URL is on the repository's own origin; writes follow no redirects.
    fn control(&self, request: HubRequest, token: &PublishToken) -> Result<HubResponse, GfError> {
        let url = Url::parse(&request.url)
            .map_err(|_| validation("hub.unsafe_location", "control-plane URL is invalid"))?;
        self.transport.validate(&url)?;
        if url.origin() != self.base.origin() {
            return Err(validation(
                "hub.unsafe_location",
                "control-plane URL is not on the repository origin",
            ));
        }
        self.transport.exchange(
            &request.with_header("authorization", token.authorization()),
            MAX_CONTROL_RESPONSE_BYTES,
        )
    }

    /// Unauthenticated data-plane request to a Hub-issued upload capability URL.
    fn data(&self, request: &HubRequest) -> Result<HubResponse, GfError> {
        let url = Url::parse(&request.url)
            .map_err(|_| validation("hub.unsafe_location", "upload URL is invalid"))?;
        validate_upload_url(&url)?;
        debug_assert!(request.header("authorization").is_none());
        self.transport.exchange(request, MAX_CONTROL_RESPONSE_BYTES)
    }

    /// Unauthenticated form POST to an OAuth endpoint the capabilities named.
    fn oauth(&self, endpoint: &str, form: &[(&str, &str)]) -> Result<HubResponse, GfError> {
        let url = Url::parse(endpoint)
            .map_err(|_| validation("hub.unsafe_location", "OAuth endpoint is invalid"))?;
        self.transport.validate(&url)?;
        let body = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(form)
            .finish();
        self.transport.exchange(
            &HubRequest::new(HubMethod::Post, endpoint)
                .with_header("content-type", "application/x-www-form-urlencoded")
                .with_body(body.into_bytes()),
            MAX_CONTROL_RESPONSE_BYTES,
        )
    }
}

// ------------------------------------------------------------- credentials

fn obtain_token(
    hub: &Hub<'_>,
    capabilities: &PublishCapabilities,
    environment: &mut PublishEnvironment<'_>,
) -> Result<PublishToken, GfError> {
    if let Credential::Token(token) = &environment.credential {
        return Ok(token.clone());
    }
    let authorization = &capabilities.authorization;
    let response = hub.oauth(
        &authorization.device_authorization_endpoint,
        &[("client_id", CLIENT_ID), ("scope", &authorization.scope)],
    )?;
    if response.status != 200 {
        return Err(publish_error(
            HubPublishErrorCode::AuthDenied,
            "device authorization was refused",
        ));
    }
    let device: DeviceAuthorizationResponse =
        serde_json::from_slice(&response.body).map_err(|_| {
            publish_error(
                HubPublishErrorCode::InvalidInput,
                "device authorization response is malformed",
            )
        })?;
    let display_safe = |text: &str| {
        !text.is_empty() && text.len() <= 512 && !text.bytes().any(|byte| byte.is_ascii_control())
    };
    if !display_safe(&device.user_code) || !display_safe(&device.verification_uri) {
        return Err(publish_error(
            HubPublishErrorCode::InvalidInput,
            "device authorization response is malformed",
        ));
    }
    let io = |error: std::io::Error| GfError::Storage(error.to_string());
    writeln!(
        environment.notices,
        "To publish {}, open {} and enter code {}",
        capabilities.repository.canonical_name(),
        device.verification_uri,
        device.user_code
    )
    .map_err(io)?;
    environment.notices.flush().map_err(io)?;
    let mut interval = device.interval.max(1);
    let mut elapsed = 0_u64;
    loop {
        if elapsed >= device.expires_in {
            return Err(publish_error(
                HubPublishErrorCode::AuthDenied,
                "device authorization expired before approval",
            ));
        }
        (environment.wait)(Duration::from_secs(interval));
        elapsed = elapsed.saturating_add(interval);
        let response = hub.oauth(
            &authorization.token_endpoint,
            &[
                ("grant_type", DEVICE_CODE_GRANT_TYPE),
                ("device_code", &device.device_code),
                ("client_id", CLIENT_ID),
            ],
        )?;
        match DevicePoll::from_response(response.status, &response.body)
            .map_err(|error| protocol(&error))?
        {
            DevicePoll::Granted(token) => return Ok(token),
            DevicePoll::Pending => {}
            DevicePoll::SlowDown => interval = interval.saturating_add(5),
        }
    }
}

// ------------------------------------------------------------ project side

/// The research Version a publication selects, with its registry identity.
struct Selection {
    git_ref: Option<String>,
    branch_uuid: Option<Uuid>,
    version_uuid: Uuid,
    identity: Sha256Digest,
    project_uuid: Uuid,
}

fn hex_digest(bytes: &[u8; 32]) -> Sha256Digest {
    Sha256Digest(
        bytes
            .iter()
            .fold(String::from("sha256:"), |mut output, byte| {
                use std::fmt::Write as _;
                write!(output, "{byte:02x}").expect("writing to a string cannot fail");
                output
            }),
    )
}

fn select(graph: &GraphForge, args: &PublishArgs) -> Result<Selection, GfError> {
    let invalid = |detail: &str| publish_error(HubPublishErrorCode::InvalidInput, detail);
    let registry = graph.research_version_retention()?;
    let (branch_uuid, version_uuid) = match (&args.git_ref, &args.version_uuid) {
        (Some(name), None) => {
            check_ref_name(name)?;
            let mut named = registry
                .branches
                .iter()
                .filter(|(_, record)| record.label == *name);
            let (branch_uuid, _) = named
                .next()
                .ok_or_else(|| invalid("no research Branch is labelled with this ref"))?;
            if named.next().is_some() {
                return Err(invalid("more than one research Branch has this label"));
            }
            let head = registry
                .heads
                .get(branch_uuid)
                .ok_or_else(|| invalid("the research Branch has no head Version"))?;
            (Some(*branch_uuid), *head)
        }
        (None, Some(version)) => (
            None,
            Uuid::parse_str(version).map_err(|_| invalid("version UUID is invalid"))?,
        ),
        _ => return Err(invalid("specify exactly one of --ref and --version-uuid")),
    };
    let identity = registry
        .identities
        .get(&version_uuid)
        .filter(|_| registry.versions.contains_key(&version_uuid))
        .ok_or_else(|| invalid("the research Version is not retained in this Project"))?;
    let reference = graph.research_reference(
        &ResearchReferenceTarget::Version { version_uuid },
        &CancellationToken::new(),
    )?;
    Ok(Selection {
        git_ref: args.git_ref.clone(),
        branch_uuid,
        version_uuid,
        identity: hex_digest(identity),
        project_uuid: reference.project_uuid,
    })
}

fn check_ref_name(name: &str) -> Result<(), GfError> {
    // Reuse the discovery ref grammar through a one-ref document.
    let probe = RefSet {
        format: graphforge_discovery::DISCOVERY_FORMAT.into(),
        version: graphforge_discovery::ProtocolVersion::CURRENT,
        repository: RepositoryIdentity {
            owner: "probe".into(),
            repository: "probe".into(),
        },
        default_ref: name.into(),
        refs: vec![graphforge_discovery::RepositoryRef {
            name: name.into(),
            target: Sha256Digest(sha256_digest(b"")),
            validator: Sha256Digest(sha256_digest(b"")),
        }],
        extensions: BTreeMap::new(),
    };
    probe.validate(DiscoveryLimits::default()).map_err(|_| {
        publish_error(
            HubPublishErrorCode::InvalidInput,
            "ref name is not a valid discovery ref",
        )
    })
}

/// Canonical digest of what an operation publishes, independent of bytes.
fn intent_digest(
    repository: &RepositoryIdentity,
    selection: &Selection,
    fork_of: Option<&RepositoryIdentity>,
) -> String {
    sha256_digest(&canonical_json(&json!({
        "format": INTENT_FORMAT,
        "repository": repository.canonical_name(),
        "ref": selection.git_ref,
        "version_uuid": selection.version_uuid.to_string(),
        "version_identity": selection.identity.0,
        "project_uuid": selection.project_uuid.to_string(),
        "fork_of": fork_of.map(RepositoryIdentity::canonical_name),
    })))
}

// ---------------------------------------------------------- repository side

/// Published state of a repository, read before building the next snapshot.
struct Published {
    refs: RefSet,
    revision: String,
    manifest: DiscoveryManifest,
    lineage: ResearchLineage,
}

fn read_published(
    hub: &Hub<'_>,
    base: &Url,
    identity: &RepositoryIdentity,
) -> Result<Option<Published>, GfError> {
    let limits = DiscoveryLimits {
        max_response_bytes: MAX_METADATA_BYTES,
        ..DiscoveryLimits::default()
    };
    let Some((refs_bytes, etag)) = hub.read(&endpoint(base, "refs"), MAX_METADATA_BYTES)? else {
        return Ok(None);
    };
    let revision = sha256_digest(&refs_bytes);
    if etag.is_some_and(|etag| etag.trim_matches('"') != revision) {
        return Err(publish_error(
            HubPublishErrorCode::IntegrityFailure,
            "refs validator does not match the refs document",
        ));
    }
    let refs = RefSet::from_json(&refs_bytes, limits).map_err(|error| discovery(&error))?;
    let (manifest_bytes, _) = hub
        .read(&endpoint(base, "manifest"), MAX_METADATA_BYTES)?
        .ok_or_else(|| {
            publish_error(
                HubPublishErrorCode::InvalidInput,
                "repository refs exist without a manifest",
            )
        })?;
    let manifest =
        DiscoveryManifest::from_json(&manifest_bytes, limits).map_err(|error| discovery(&error))?;
    if refs.repository != *identity || manifest.repository != *identity {
        return Err(publish_error(
            HubPublishErrorCode::IntegrityFailure,
            "discovery documents name another repository",
        ));
    }
    refs.validate_manifest(&manifest)
        .map_err(|error| discovery(&error))?;
    let object = manifest.lineage_object().map_err(|_| {
        publish_error(
            HubPublishErrorCode::InvalidInput,
            "the repository has no research lineage to extend",
        )
    })?;
    let lineage_bytes = read_object(hub, object, limits.max_lineage_bytes)?;
    let lineage =
        ResearchLineage::from_json(&lineage_bytes, limits).map_err(|error| discovery(&error))?;
    manifest
        .bind_lineage(&refs, &lineage)
        .map_err(|error| discovery(&error))?;
    Ok(Some(Published {
        refs,
        revision,
        manifest,
        lineage,
    }))
}

fn read_object(hub: &Hub<'_>, object: &ObjectDescriptor, limit: usize) -> Result<Vec<u8>, GfError> {
    let location = object.locations.first().ok_or_else(|| {
        publish_error(HubPublishErrorCode::InvalidInput, "object has no location")
    })?;
    let url = Url::parse(location)
        .map_err(|_| validation("hub.unsafe_location", "object location is invalid"))?;
    validate_url(&url)?;
    let (bytes, _) = hub.read(&url, limit)?.ok_or_else(|| {
        publish_error(HubPublishErrorCode::InvalidInput, "object is not published")
    })?;
    if digest_bytes(&bytes) != object.digest.0 || bytes.len() as u64 != object.length {
        return Err(publish_error(
            HubPublishErrorCode::IntegrityFailure,
            "object bytes disagree with their descriptor",
        ));
    }
    Ok(bytes)
}

/// Fail closed unless the origin repository publishes the cited origin Version
/// of `fork` with the same Project and identity.
fn verify_fork_origin(
    hub: &Hub<'_>,
    origin_base: &Url,
    fork: &graphforge_discovery::LineageForkOrigin,
) -> Result<(), GfError> {
    let refused = || {
        publish_error(
            HubPublishErrorCode::IntegrityFailure,
            "the origin repository does not publish the cited origin Version with its identity",
        )
    };
    let origin = read_published(hub, origin_base, &fork.origin_repository)
        .map_err(|_| refused())?
        .ok_or_else(refused)?;
    let version = origin
        .lineage
        .version(&fork.origin_version_uuid)
        .ok_or_else(refused)?;
    if origin.lineage.project_uuid != fork.origin_project_uuid
        || version.identity_digest != fork.origin_version_identity
    {
        return Err(refused());
    }
    Ok(())
}

// --------------------------------------------------------------- snapshot

/// Where the bytes of one inventory object come from.
enum Source {
    File(PathBuf),
    Bytes(Vec<u8>),
    /// Already admitted to the repository by an earlier publication.
    Carried,
}

/// Everything one commit publishes.
struct Snapshot {
    manifest: DiscoveryManifest,
    refs: Vec<String>,
    objects: Vec<ObjectDeclaration>,
    sources: BTreeMap<String, Source>,
}

/// Carry the published lineage forward and add this publication's entries.
///
/// Earlier Versions, Branches, and Proposals stay exactly as published; the
/// new Branch entry replaces only its own earlier entry.
fn merge_lineage(
    previous: Option<&ResearchLineage>,
    mut next: ResearchLineage,
) -> Result<ResearchLineage, GfError> {
    let Some(previous) = previous else {
        return Ok(next);
    };
    let invalid = |detail: &str| publish_error(HubPublishErrorCode::InvalidInput, detail);
    if previous.project_uuid != next.project_uuid {
        return Err(invalid(
            "the repository publishes a different research Project",
        ));
    }
    if previous.fork.is_some() && previous.fork != next.fork {
        return Err(invalid("the repository cites a different Fork origin"));
    }
    let mut versions: BTreeMap<String, _> = previous
        .versions
        .iter()
        .map(|version| (version.version_uuid.clone(), version.clone()))
        .collect();
    for version in next.versions.drain(..) {
        match versions.get(&version.version_uuid) {
            Some(published) if published.identity_digest != version.identity_digest => {
                return Err(publish_error(
                    HubPublishErrorCode::IntegrityFailure,
                    "a published Version has a different identity than the local Version",
                ));
            }
            Some(_) => {}
            None => {
                versions.insert(version.version_uuid.clone(), version);
            }
        }
    }
    let mut branches: BTreeMap<String, _> = previous
        .branches
        .iter()
        .map(|branch| (branch.branch_uuid.clone(), branch.clone()))
        .collect();
    for branch in next.branches.drain(..) {
        if branches.values().any(|published| {
            (published.ref_name == branch.ref_name) != (published.branch_uuid == branch.branch_uuid)
        }) {
            return Err(invalid(
                "the ref already names another Branch, or the Branch is published under another ref",
            ));
        }
        branches.insert(branch.branch_uuid.clone(), branch);
    }
    next.versions = versions.into_values().collect();
    next.branches = branches.into_values().collect();
    next.proposals.clone_from(&previous.proposals);
    next.validate(DiscoveryLimits::default())
        .map_err(|error| discovery(&error))?;
    Ok(next)
}

#[allow(clippy::too_many_lines)]
fn build_snapshot(
    graph: &GraphForge,
    identity: &RepositoryIdentity,
    capabilities: &PublishCapabilities,
    selection: &Selection,
    published: Option<&Published>,
    fork_origin_repository: Option<RepositoryIdentity>,
    scratch: &Path,
) -> Result<(Snapshot, ResearchLineage), GfError> {
    let cancellation = CancellationToken::new();
    let location = |digest: &str| capabilities.object_location(digest);
    let mut sources = BTreeMap::new();
    let mut objects = Vec::new();

    // The selected Version's research package, unless already published.
    let carried_version = published.and_then(|published| {
        published
            .lineage
            .version(&selection.version_uuid.to_string())
            .and_then(|version| version.package.clone())
    });
    let version_package = if let Some(package) = carried_version {
        package
    } else {
        let path = scratch.join(format!("version-{}.gfpb", selection.version_uuid));
        let receipt = graph
            .export_research(
                &ExportResearchRequest {
                    version_uuid: selection.version_uuid,
                    output: path.clone(),
                    bundled: true,
                    projection: None,
                },
                &cancellation,
            )
            .map_err(|error| GfError::Validation(format!("hub.publish.derivation: {error}")))?;
        let bundle =
            verify_exported_bundle(&path, &receipt.package_digest, &receipt.transport_digest)
                .map_err(|error| derivation(&error))?;
        add_bundle(&mut objects, &mut sources, &bundle, &location);
        bundle.package_reference()
    };

    // The Project package. A Project with research Branches carries research
    // only through the research interchange, so the Project package is its
    // graph data components (ADR 0053).
    let project = export_verified_bundle(
        graph,
        PortableV2SelectionProfile::DataComponents,
        &scratch.join("project.gfpb"),
    )
    .map_err(|error| derivation(&error))?;
    add_bundle(&mut objects, &mut sources, &project, &location);
    let immutable_version = Sha256Digest(project.package_digest.clone());

    let summary = derive_summary(identity, &immutable_version, &project.path)
        .map_err(|error| derivation(&error))?;
    let summary_object = object_descriptor(
        digest_bytes(&summary.bytes),
        summary.bytes.len() as u64,
        SUMMARY_MEDIA_TYPE,
        location(&digest_bytes(&summary.bytes)),
    );
    sources.insert(
        summary_object.digest.0.clone(),
        Source::Bytes(summary.bytes.clone()),
    );
    objects.push(summary_object.clone());

    let (ontology, modules) = export_module_packages(graph, &summary.summary, |digest| {
        Ok(scratch.join(format!(
            "module-{}.gfpb",
            digest.trim_start_matches("sha256:")
        )))
    })
    .map_err(|error| derivation(&error))?;
    for module in &modules {
        add_bundle(&mut objects, &mut sources, &module.bundle, &location);
    }

    let lineage = graph.build_research_lineage_for_discovery(
        &BuildResearchLineageRequest {
            repository: identity.clone(),
            immutable_version: immutable_version.clone(),
            project_uuid: selection.project_uuid,
            branch_ref_names: selection
                .branch_uuid
                .zip(selection.git_ref.clone())
                .into_iter()
                .collect(),
            version_packages: BTreeMap::from([(selection.version_uuid, version_package)]),
            fork_origin_repository,
            published_proposals: BTreeSet::new(),
        },
        &cancellation,
    )?;
    let lineage = merge_lineage(published.map(|published| &published.lineage), lineage)?;

    // Earlier published Version and Proposal packages are carried unchanged.
    let carried: BTreeSet<&Sha256Digest> = lineage
        .versions
        .iter()
        .filter_map(|version| version.package.as_ref())
        .chain(lineage.proposals.iter().map(|proposal| &proposal.package))
        .map(|package| &package.object_digest)
        .collect();
    for object_digest in carried {
        if sources.contains_key(&object_digest.0) {
            continue;
        }
        let previous = published
            .and_then(|published| {
                published
                    .manifest
                    .objects
                    .iter()
                    .find(|object| object.digest == *object_digest)
            })
            .ok_or_else(|| {
                publish_error(
                    HubPublishErrorCode::IntegrityFailure,
                    "a published package object is absent from the repository manifest",
                )
            })?;
        objects.push(object_descriptor(
            previous.digest.0.clone(),
            previous.length,
            &previous.media_type,
            location(&previous.digest.0),
        ));
        sources.insert(previous.digest.0.clone(), Source::Carried);
    }

    let lineage_bytes = lineage
        .to_canonical_json()
        .map_err(|error| discovery(&error))?;
    let lineage_digest = lineage
        .canonical_digest()
        .map_err(|error| discovery(&error))?;
    let lineage_object = object_descriptor(
        digest_bytes(&lineage_bytes),
        lineage_bytes.len() as u64,
        RESEARCH_LINEAGE_MEDIA_TYPE,
        location(&digest_bytes(&lineage_bytes)),
    );
    sources.insert(
        lineage_object.digest.0.clone(),
        Source::Bytes(lineage_bytes),
    );
    objects.push(lineage_object.clone());

    let default_ref = match (published, &selection.git_ref) {
        (Some(published), _) => published.refs.default_ref.clone(),
        (None, Some(name)) => name.clone(),
        (None, None) => {
            return Err(publish_error(
                HubPublishErrorCode::InvalidInput,
                "creating a repository needs --ref to name its default Branch",
            ));
        }
    };
    let resolved_ref = selection
        .git_ref
        .clone()
        .unwrap_or_else(|| default_ref.clone());
    let manifest = build_manifest(ManifestInputs {
        repository: identity.clone(),
        default_ref: default_ref.clone(),
        resolved_ref: resolved_ref.clone(),
        package: project.package_reference(),
        summary: Some(summary_reference(&summary, &summary_object)),
        ontology,
        lineage: Some(ResearchLineageReference {
            format: RESEARCH_LINEAGE_FORMAT.into(),
            lineage_digest,
            object_digest: lineage_object.digest.clone(),
        }),
        objects,
    })
    .map_err(|error| derivation(&error))?;
    manifest
        .bind_summary(&summary.summary)
        .map_err(|error| discovery(&error))?;

    // Every Branch ref the lineage describes must select this snapshot.
    let refs: BTreeSet<String> = lineage
        .branches
        .iter()
        .map(|branch| branch.ref_name.clone())
        .chain([default_ref, resolved_ref])
        .collect();
    let declarations = manifest
        .objects
        .iter()
        .map(|object| ObjectDeclaration {
            digest: object.digest.0.clone(),
            length: object.length,
            media_type: object.media_type.clone(),
        })
        .collect();
    Ok((
        Snapshot {
            manifest,
            refs: refs.into_iter().collect(),
            objects: declarations,
            sources,
        },
        lineage,
    ))
}

fn add_bundle(
    objects: &mut Vec<ObjectDescriptor>,
    sources: &mut BTreeMap<String, Source>,
    bundle: &VerifiedBundle,
    location: &dyn Fn(&str) -> String,
) {
    objects.push(object_descriptor(
        bundle.object_digest.clone(),
        bundle.length,
        PACKAGE_MEDIA_TYPE,
        location(&bundle.object_digest),
    ));
    sources.insert(
        bundle.object_digest.clone(),
        Source::File(bundle.path.clone()),
    );
}

// ------------------------------------------------------------------ upload

fn read_chunk(source: &Source, offset: u64, length: u64) -> Result<Vec<u8>, GfError> {
    let io = |error: std::io::Error| GfError::Storage(error.to_string());
    match source {
        Source::Bytes(bytes) => {
            let start = usize::try_from(offset).map_err(|_| network("offset overflow"))?;
            let end = usize::try_from(offset + length).map_err(|_| network("offset overflow"))?;
            bytes
                .get(start..end)
                .map(<[u8]>::to_vec)
                .ok_or_else(|| network("upload range is outside the object"))
        }
        Source::File(path) => {
            let mut file = File::open(path).map_err(io)?;
            file.seek(SeekFrom::Start(offset)).map_err(io)?;
            let mut buffer =
                vec![0; usize::try_from(length).map_err(|_| network("chunk overflow"))?];
            file.read_exact(&mut buffer).map_err(io)?;
            Ok(buffer)
        }
        Source::Carried => Err(publish_error(
            HubPublishErrorCode::InvalidInput,
            "the Hub has not admitted an object an earlier publication carried",
        )),
    }
}

fn upload_offset(response: &HubResponse, length: u64) -> Result<u64, GfError> {
    response
        .header(UPLOAD_OFFSET_HEADER)
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|offset| *offset <= length)
        .ok_or_else(|| {
            publish_error(
                HubPublishErrorCode::InvalidInput,
                "upload offset is missing or out of range",
            )
        })
}

/// Upload whatever the Hub has not retained, resuming at its reported offset.
fn upload(
    hub: &Hub<'_>,
    target: &UploadTarget,
    source: &Source,
    max_chunk: u64,
) -> Result<(), GfError> {
    if target.received >= target.length {
        return Ok(());
    }
    let head = hub.data(&HubRequest::new(HubMethod::Head, &target.upload_url))?;
    if head.status != 200 {
        return Err(hub_failure(&head));
    }
    let mut offset = upload_offset(&head, target.length)?;
    let mut resynchronized = false;
    while offset < target.length {
        let chunk = max_chunk.min(target.length - offset);
        let bytes = read_chunk(source, offset, chunk)?;
        let response = hub.data(
            &HubRequest::new(HubMethod::Put, &target.upload_url)
                .with_header("content-type", "application/octet-stream")
                .with_header("content-range", content_range(offset, chunk, target.length))
                .with_body(bytes),
        )?;
        match response.status {
            200 => {
                let status: UploadStatus =
                    serde_json::from_slice(&response.body).map_err(|_| {
                        publish_error(
                            HubPublishErrorCode::InvalidInput,
                            "upload status is malformed",
                        )
                    })?;
                if status.digest != target.digest
                    || status.length != target.length
                    || status.received != offset + chunk
                {
                    return Err(publish_error(
                        HubPublishErrorCode::IntegrityFailure,
                        "upload status disagrees with the bytes sent",
                    ));
                }
                offset = status.received;
                resynchronized = false;
            }
            416 if !resynchronized => {
                offset = upload_offset(&response, target.length)?;
                resynchronized = true;
            }
            _ => return Err(hub_failure(&response)),
        }
    }
    Ok(())
}

// ------------------------------------------------------------------ publish

fn session_response(response: &HubResponse) -> Result<SessionResponse, GfError> {
    if !(200..300).contains(&response.status) {
        return Err(hub_failure(response));
    }
    serde_json::from_slice(&response.body).map_err(|_| {
        publish_error(
            HubPublishErrorCode::InvalidInput,
            "session response is malformed",
        )
    })
}

fn check_receipt(
    receipt: &PublishReceipt,
    identity: &RepositoryIdentity,
    operation_uuid: Uuid,
) -> Result<(), GfError> {
    if receipt.format != HUB_PUBLISH_FORMAT
        || &receipt.repository != identity
        || receipt.operation_uuid != operation_uuid
    {
        return Err(publish_error(
            HubPublishErrorCode::IntegrityFailure,
            "the receipt does not describe this operation",
        ));
    }
    Ok(())
}

/// Publish one research Version (or Branch head) of `graph`.
#[allow(clippy::too_many_lines)]
pub(crate) fn publish(
    transport: &dyn Transport,
    graph: &GraphForge,
    args: &PublishArgs,
    environment: &mut PublishEnvironment<'_>,
) -> Result<PublishOutcome, GfError> {
    let (identity, base) = parse_input(&args.repository)?;
    let fork = args.fork_of.as_deref().map(parse_input).transpose()?;
    if fork.as_ref().is_some_and(|(origin, _)| *origin == identity) {
        return Err(publish_error(
            HubPublishErrorCode::InvalidInput,
            "a Fork must publish a repository other than its origin",
        ));
    }
    let operation_uuid = args
        .operation_uuid
        .as_deref()
        .map(Uuid::parse_str)
        .transpose()
        .map_err(|_| {
            publish_error(
                HubPublishErrorCode::InvalidInput,
                "operation UUID is invalid",
            )
        })?;
    let selection = select(graph, args)?;
    let operation_uuid = operation_uuid.unwrap_or_else(|| {
        graphforge_api::hub_publish_operation(
            &identity.canonical_name(),
            selection.git_ref.as_deref(),
            &selection.version_uuid.to_string(),
        )
    });
    let intent_digest = intent_digest(&identity, &selection, fork.as_ref().map(|(id, _)| id));
    let hub = Hub {
        transport,
        base: base.clone(),
    };

    // Capabilities first: an unsupported future Hub fails before any credential use.
    let (capabilities_bytes, _) = hub
        .read(&endpoint(&base, "publish"), MAX_CONTROL_RESPONSE_BYTES)?
        .ok_or_else(|| {
            publish_error(
                HubPublishErrorCode::InvalidInput,
                "the Hub does not accept publications for this repository",
            )
        })?;
    let capabilities =
        PublishCapabilities::from_json(&capabilities_bytes).map_err(|error| protocol(&error))?;
    if capabilities.repository != identity
        || capabilities.authorization.scope != publish_scope(&identity)
    {
        return Err(publish_error(
            HubPublishErrorCode::InvalidInput,
            "capabilities describe another repository",
        ));
    }
    let token = obtain_token(&hub, &capabilities, environment)?;

    // A retry is classified before any package is derived.
    let status_url = endpoint(&base, &format!("publish/operations/{operation_uuid}"));
    let status = hub.control(HubRequest::new(HubMethod::Get, status_url.as_str()), &token)?;
    match status.status {
        200 => {
            let status =
                OperationStatus::from_json(&status.body).map_err(|error| protocol(&error))?;
            if status.repository != identity || status.operation_uuid != operation_uuid {
                return Err(publish_error(
                    HubPublishErrorCode::IntegrityFailure,
                    "operation status describes another operation",
                ));
            }
            if status.intent_digest != intent_digest {
                return Err(publish_error(
                    HubPublishErrorCode::IdempotencyConflict,
                    code_detail(HubPublishErrorCode::IdempotencyConflict),
                ));
            }
            if let Some(receipt) = status.receipt {
                check_receipt(&receipt, &identity, operation_uuid)?;
                return Ok(PublishOutcome {
                    receipt,
                    replayed: true,
                });
            }
        }
        404 => {}
        _ => return Err(hub_failure(&status)),
    }

    let published = read_published(&hub, &base, &identity)?;
    let fork_origin_repository = match (&fork, &published) {
        (Some(_), Some(_)) => {
            return Err(publish_error(
                HubPublishErrorCode::RefConflict,
                "a Fork publishes a new repository, and this repository already exists",
            ));
        }
        (Some((origin, _)), None) => Some(origin.clone()),
        (None, Some(published)) => published
            .lineage
            .fork
            .as_ref()
            .map(|fork| fork.origin_repository.clone()),
        (None, None) => None,
    };
    let scratch = tempfile::tempdir().map_err(|error| GfError::Storage(error.to_string()))?;
    let (snapshot, lineage) = build_snapshot(
        graph,
        &identity,
        &capabilities,
        &selection,
        published.as_ref(),
        fork_origin_repository,
        scratch.path(),
    )?;
    if let Some((_, origin_base)) = &fork {
        let cited = lineage.fork.as_ref().ok_or_else(|| {
            publish_error(
                HubPublishErrorCode::InvalidInput,
                "this Project is not a Fork",
            )
        })?;
        verify_fork_origin(&hub, origin_base, cited)?;
    }

    let manifest_validator = snapshot
        .manifest
        .canonical_digest()
        .map_err(|error| discovery(&error))?
        .0;
    let expected_revision = published
        .as_ref()
        .map(|published| published.revision.clone());
    let request_commitment = PublishIntent {
        repository: identity.clone(),
        intent_digest: intent_digest.clone(),
        objects: snapshot.objects.clone(),
        manifest_validator: manifest_validator.clone(),
        refs: snapshot.refs.clone(),
        expected_revision: expected_revision.clone(),
    }
    .request_commitment();
    let open = OpenSessionRequest {
        format: HUB_PUBLISH_FORMAT.into(),
        operation_uuid,
        request_commitment: request_commitment.clone(),
        intent_digest,
        repository: identity.clone(),
        objects: snapshot.objects.clone(),
        requirements: Vec::new(),
    };
    let sessions = endpoint(&base, "publish/sessions");
    let opened = session_response(&hub.control(
        HubRequest::new(HubMethod::Post, sessions.as_str()).with_json(&open),
        &token,
    )?)?;
    let (session_id, uploads) = match opened {
        SessionResponse::Complete { receipt } => {
            check_receipt(&receipt, &identity, operation_uuid)?;
            return Ok(PublishOutcome {
                receipt,
                replayed: true,
            });
        }
        SessionResponse::Open {
            session_id,
            uploads,
        } => (session_id, uploads),
    };
    if uploads.len() != snapshot.objects.len()
        || uploads
            .iter()
            .zip(&snapshot.objects)
            .any(|(target, object)| {
                target.digest != object.digest || target.length != object.length
            })
    {
        return Err(publish_error(
            HubPublishErrorCode::InvalidInput,
            "upload locations do not match the declared inventory",
        ));
    }
    let max_chunk = capabilities
        .limits
        .max_chunk_bytes
        .min(MAX_CLIENT_CHUNK_BYTES);
    for target in &uploads {
        let source = snapshot.sources.get(&target.digest).ok_or_else(|| {
            publish_error(
                HubPublishErrorCode::InvalidInput,
                "the Hub issued an upload for an undeclared object",
            )
        })?;
        upload(&hub, target, source, max_chunk)?;
    }

    let commit_url = endpoint(&base, &format!("publish/sessions/{session_id}/commit"));
    let manifest_value = serde_json::to_value(&snapshot.manifest)
        .map_err(|error| GfError::Execution(error.to_string()))?;
    let committed = session_response(&hub.control(
        HubRequest::new(HubMethod::Post, commit_url.as_str()).with_json(&CommitRequest {
            manifest: manifest_value,
            refs: snapshot.refs.clone(),
            expected_revision,
        }),
        &token,
    )?)?;
    let SessionResponse::Complete { receipt } = committed else {
        return Err(publish_error(
            HubPublishErrorCode::InvalidInput,
            "commit did not complete the publication",
        ));
    };
    check_receipt(&receipt, &identity, operation_uuid)?;
    if receipt.request_commitment != request_commitment
        || receipt.manifest_validator != manifest_validator
    {
        return Err(publish_error(
            HubPublishErrorCode::IntegrityFailure,
            "the receipt does not bind the committed publication",
        ));
    }
    Ok(PublishOutcome {
        receipt,
        replayed: false,
    })
}

#[cfg(test)]
mod tests;
