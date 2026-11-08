//! Shared Hub catalog and account-management control contract.
//!
//! Metadata never authorizes a mutation or verifies native package bytes. Hosts
//! authenticate before reading private state, recheck authority transactionally
//! on apply, and persist operation identity before acknowledging a mutation.
//! Existing discovery/package verifiers remain the native content authority.

use crate::{
    DiscoveryError, DiscoveryErrorCode, DiscoveryLimits, ItemDescriptor, ItemId,
    ItemRepositoryFact, Sha256Digest, StableRepositoryId, canonical_json, parse_unique_json,
    sha256_digest,
};
use serde::{Deserialize, Serialize};

/// Version shared by native clients and Hub services.
pub const HUB_CONTROL_FORMAT: &str = "graphforge-hub-control/1";
/// Same-origin HTTPS POST; redirects must never be followed.
pub const HUB_CONTROL_PATH: &str = "/api/hub/v1/control";
/// Request size before JSON allocation.
pub const MAX_HUB_REQUEST_BYTES: usize = 64 * 1024;
/// Response size before JSON allocation.
pub const MAX_HUB_RESPONSE_BYTES: usize = 1024 * 1024;
/// Maximum records or accounts per page.
pub const MAX_HUB_PAGE_SIZE: u32 = 50;

/// Closed operation envelope; request identity is distinct from mutation identity.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct HubControlRequest {
    /// Exactly [`HUB_CONTROL_FORMAT`].
    pub format: String,
    /// Canonical nonnil UUID echoed by the response.
    pub request_uuid: String,
    /// Only the selected operation is accepted.
    pub operation: HubControlOperation,
}

/// Reads never initiate login or mutate visit/star state implicitly.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HubControlOperation {
    /// Discover supported operations without a session.
    Capabilities {},
    /// Page fresh, verified authorized account context.
    Accounts {
        /// Bounded page size or continuation for this exact scope.
        page: HubPageRequest,
    },
    /// Public catalog, authorized account inventory, or the caller's stars.
    Search {
        /// Complete query retained across every continuation.
        query: HubSearch,
        /// Bounded page size or continuation for this exact scope.
        page: HubPageRequest,
    },
    /// Hub metadata for one stable Project or exact ontology.
    Inspect {
        /// Stable Project or exact ontology selected by the caller.
        target: HubTarget,
    },
    /// Prepare a reviewable mutation. Reusing UUID with different intent conflicts.
    Prepare {
        /// Durable caller-selected idempotency UUID.
        operation_uuid: String,
        /// Exact desired hosted change; contains no credentials.
        mutation: HubMutation,
    },
    /// Apply exactly the reviewed native commitment; never refresh it silently.
    Apply {
        /// Complete reviewed native commitment; never renewed implicitly.
        plan: HubManagementPlan,
    },
    /// Reconcile an interrupted apply without submitting another mutation.
    Status {
        /// Durable caller-selected idempotency UUID.
        operation_uuid: String,
        /// Commitment of the exact prepared operation.
        intent_digest: Sha256Digest,
    },
}

/// Server-signed/opaque continuation, bound to actor, scope and full query.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct HubPageRequest {
    /// 1..=50.
    pub limit: u32,
    /// Opaque continuation; absent for the first page.
    pub cursor: Option<String>,
}

/// Private inventory is a separate authorized scope, never public search plus auth.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HubSearchScope {
    /// Visible public objects only; works signed out.
    Public {},
    /// Fresh server authorization is required, including private metadata/counts.
    Account {
        /// Stable authorized account key, never a provider organization prompt.
        account_id: String,
    },
    /// The caller's stars, reauthorized against current object visibility.
    Starred {},
}

/// Supported object families. Ontology identity is independent of its Project.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum HubObjectKind {
    /// A native Project identified by its stable hosting repository.
    Project,
    /// An independent exact ontology module.
    Ontology,
}

/// Text is a literal query, never executable syntax or a regex.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct HubSearch {
    /// Public, explicit authorized account, or starred inventory.
    pub scope: HubSearchScope,
    /// UTF-8 query of at most 512 bytes; empty means browse.
    pub text: String,
    /// Absent means both families.
    pub object_kind: Option<HubObjectKind>,
    /// At most 16 exact tag filters.
    pub tags: Vec<String>,
    /// Absent means all visibility allowed by scope and current authorization.
    pub visibility: Option<HubVisibility>,
}

/// Exact targets never silently resolve to latest/default.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HubTarget {
    /// Host stable repository key, never display owner/name.
    Project {
        /// Stable host repository identity, preserved across rename.
        repository_id: StableRepositoryId,
    },
    /// Permanent bare item ID plus opaque authored version and canonical digest.
    Ontology {
        /// Canonical permanent ontology ID, independent of its publishing Project.
        id: String,
        /// Opaque exact authored version, without SemVer inference.
        version: String,
        /// Exact canonical module digest, distinct from package identity.
        digest: Sha256Digest,
    },
}

/// Current server policy, including entitlement, remains authoritative on apply.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum HubVisibility {
    /// Visible in the public catalog.
    Public,
    /// Visible only after current server authorization.
    Private,
}

/// An available action is UI guidance, never a reusable authorization grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum HubAction {
    /// Inspect accessible metadata and exact native identities.
    Inspect,
    /// Clone a native verified Project selection.
    Clone,
    /// Fetch verified native module bytes without adopting.
    Download,
    /// Prepare native adoption into an existing Project.
    Adopt,
    /// Prepare a native Project containing this ontology.
    CreateProject,
    /// Publish native content through the existing Core channel.
    Publish,
    /// Edit native metadata locally and republish.
    EditMetadata,
    /// Review a visibility change under current entitlement.
    SetVisibility,
    /// Review a rename preserving native aliases.
    Rename,
    /// Review deletion restricted to empty Projects.
    DeleteEmpty,
    /// Review ontology lifecycle under native dependency policy.
    Tombstone,
    /// Set explicit public star state.
    Star,
    /// Explicitly advance the caller seen baseline.
    Visit,
}

/// Exact publication proving an ontology star/visit target is publicly visible.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct HubOntologyWitness {
    /// Stable publishing repository, not the ontology identity.
    pub repository_id: StableRepositoryId,
    /// Exact retained publication. Opaque, not a research Version UUID.
    pub immutable_version: Sha256Digest,
}

/// Closed hosted-management intents. Native semantic edits use edit-and-republish.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HubMutation {
    /// Idempotent desired state, not toggle-on-retry.
    Star {
        /// Stable Project or exact ontology selected by the caller.
        target: HubTarget,
        /// Required exact public publication for an ontology star or visit.
        witness: Option<HubOntologyWitness>,
        /// Explicit desired star state, so retries never toggle it.
        starred: bool,
    },
    /// Explicitly advance seen baseline; reads do not do this.
    Visit {
        /// Stable Project or exact ontology selected by the caller.
        target: HubTarget,
        /// Required exact public publication for an ontology star or visit.
        witness: Option<HubOntologyWitness>,
    },
    /// Preserve private entitlement checks and search/audit refresh.
    SetVisibility {
        /// Stable host repository identity, preserved across rename.
        repository_id: StableRepositoryId,
        /// Explicit desired hosted visibility; entitlement is rechecked on apply.
        visibility: HubVisibility,
    },
    /// Current namespace remains the authority; old names become native aliases.
    Rename {
        /// Stable host repository identity, preserved across rename.
        repository_id: StableRepositoryId,
        /// Desired canonical repository name within the existing namespace.
        name: String,
    },
    /// Never means arbitrary deletion of published content.
    DeleteEmpty {
        /// Stable host repository identity, preserved across rename.
        repository_id: StableRepositoryId,
        /// Reviewed current repository name; only empty repositories may be deleted.
        confirm_name: String,
    },
    /// Ontology lifecycle, distinct from deleting its publishing Project.
    Tombstone {
        /// Stable authorized account key, never a provider organization prompt.
        account_id: String,
        /// Canonical permanent ontology ID, independent of its publishing Project.
        id: String,
    },
}

/// Host CAS facts include an authority epoch to prevent ABA/revocation replay.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct HubManagementPrecondition {
    /// Opaque actor/account authority epoch, checked again by the host.
    pub authority_revision: String,
    /// Opaque current object/policy epoch, changes on rename/visibility/lifecycle.
    pub object_revision: String,
}

/// Fresh authorized host facts from which Rust constructs the review commitment.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct HubManagementPlanInput {
    /// Explicit selected bare HTTPS Hub origin.
    pub hub_origin: String,
    /// Caller-supplied replay identity.
    pub operation_uuid: String,
    /// Exact desired change.
    pub mutation: HubMutation,
    /// Fresh host facts; never supplied as authority by an untrusted caller.
    pub precondition: HubManagementPrecondition,
    /// Expiration fixed by the host, in Unix milliseconds.
    #[cfg_attr(feature = "typescript", tsify(type = "bigint"))]
    pub expires_at_ms: u64,
}

/// Review commitment. A hash is not authorization: the host stores and rechecks it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct HubManagementPlan {
    /// Bare HTTPS origin; prevents cross-Hub application.
    pub hub_origin: String,
    /// Durable idempotency identity, distinct from a request UUID.
    pub operation_uuid: String,
    /// Full reviewed intent.
    pub mutation: HubMutation,
    /// Host-supplied current-state commitment.
    pub precondition: HubManagementPrecondition,
    /// Unix milliseconds, exact JSON-safe integer; apply must not renew it.
    #[cfg_attr(feature = "typescript", tsify(type = "bigint"))]
    pub expires_at_ms: u64,
    /// Domain-separated canonical SHA-256 of every preceding plan field.
    pub intent_digest: Sha256Digest,
}

/// Terminal and pending effects must remain distinguishable.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum HubManagementState {
    /// Prepared without applying an effect.
    Prepared,
    /// Accepted but still running; never terminal success.
    Pending,
    /// Committed and completed.
    Applied,
    /// No requested effect was applied; inspect the refusal code.
    Refused,
}

/// No untrusted prose in native errors/diagnostics; clients supply recovery copy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum HubControlErrorCode {
    /// A saved human session on this execution host is required.
    AuthenticationRequired,
    /// Current actor lacks authority; no hidden object existence is implied.
    AccessDenied,
    /// Absent or hidden to the current actor.
    NotFound,
    /// The exact requested identity cannot be resolved; no fallback is permitted.
    ExactVersionUnavailable,
    /// The selected operation is not supported by this Hub.
    UnsupportedOperation,
    /// The request violates the closed native contract.
    InvalidRequest,
    /// Authority, object state or policy changed since review.
    StalePlan,
    /// Prepare and review again; do not renew a plan silently.
    ExpiredPlan,
    /// Operation UUID was already bound to different intent.
    IdempotencyConflict,
    /// Current account entitlement does not permit the operation.
    EntitlementRequired,
    /// Retry only after the bounded server delay.
    RateLimited,
    /// An incompatible operation is in progress.
    Busy,
    /// Published or other content prevents empty-only deletion.
    NotEmpty,
    /// Native references or incomplete dependency proof prevent tombstoning.
    OntologyInUse,
    /// Bounded evaluation could not establish the requested fact.
    Incomplete,
    /// The service cannot currently establish an authoritative result.
    ServiceUnavailable,
}

/// Durable, actor-bound result for the exact prepared intent.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct HubManagementReceipt {
    /// Same identity as the prepared plan.
    pub operation_uuid: String,
    /// Same commitment as the reviewed plan.
    pub intent_digest: Sha256Digest,
    /// Pending is not successful completion.
    pub state: HubManagementState,
    /// Present exactly for a refused operation.
    pub refusal: Option<HubControlErrorCode>,
    /// Current authorized object if still present/visible; absence proves nothing.
    pub record: Option<HubRecord>,
}

/// A namespace/publisher kind; never a provider organization selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum HubAccountKind {
    /// Personal publishing namespace.
    Personal,
    /// Organization publishing namespace; current membership is verified.
    Organization,
}

/// Authorized publication namespace obtained from the Hub, not guessed locally.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct HubAccount {
    /// Host-owned key used only in native calls, not a normal UI input.
    pub account_id: String,
    /// Public publisher/namespace handle; may be unclaimed.
    pub handle: Option<String>,
    /// Accessible display label.
    pub label: String,
    /// Personal or organization namespace.
    pub account_kind: HubAccountKind,
    /// Fresh verified membership, not database enumeration of historical roles.
    pub role: HubAccountRole,
}

/// Current account role; every private operation reauthorizes it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum HubAccountRole {
    /// Authorized account member.
    Member,
    /// Authorized account owner.
    Owner,
}

/// Catalog metadata is not a package/module verification receipt.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct HubRecord {
    /// Stable Project or exact ontology identity.
    pub target: HubTarget,
    /// Current canonical path bound to stable repository identity/revision.
    pub repository: ItemRepositoryFact,
    /// Authorized account inventory binding; omitted from public catalog results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    /// Accessible title, plain untrusted text.
    pub title: String,
    /// Plain untrusted text; never trusted Markdown/command links.
    pub description: Option<String>,
    /// Declared license, not an inferred permission.
    pub license: Option<String>,
    /// Bounded exact tags.
    pub tags: Vec<String>,
    /// Personal or organization publisher namespace.
    pub publisher_kind: HubAccountKind,
    /// Current visibility; private reads/counts require server authorization.
    pub visibility: HubVisibility,
    /// Permitted UI intents; server must recheck before effects.
    pub actions: Vec<HubAction>,
    /// Exact descriptor for ontologies only. Fetch/verify native bytes separately.
    pub ontology: Option<ItemDescriptor>,
    /// Advisory producing version; never a universal read refusal.
    pub producing_core_version: Option<String>,
    /// Current caller's explicit star state; signed-out reads use false.
    pub starred: bool,
}

/// Explicitly bounded discovery coverage; no cursor never implies completeness.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum HubCoverage {
    /// The reported scope was evaluated completely, separately from page continuation.
    Complete,
    /// Only part of the scope was evaluated; no cursor does not prove absence.
    Partial,
    /// Coverage or count could not be established.
    Unknown,
}

/// Facet/count semantics must survive the native boundary without false zeros.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HubCount {
    /// An exact authorized count.
    Exact {
        /// Count with the accuracy guarantee named by this variant.
        #[cfg_attr(feature = "typescript", tsify(type = "bigint"))]
        value: u64,
    },
    /// At least this many authorized objects.
    LowerBound {
        /// Count with the accuracy guarantee named by this variant.
        #[cfg_attr(feature = "typescript", tsify(type = "bigint"))]
        value: u64,
    },
    /// An explicitly estimated authorized count.
    Approximate {
        /// Count with the accuracy guarantee named by this variant.
        #[cfg_attr(feature = "typescript", tsify(type = "bigint"))]
        value: u64,
    },
    /// No authoritative count is available; never displayed as zero.
    Unknown {},
}

/// Page state is scoped to precisely the query and current authorization context.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct HubPageInfo {
    /// Continue only with the original complete query/account context.
    pub next_cursor: Option<String>,
    /// Scope coverage, separate from resumable pagination.
    pub coverage: HubCoverage,
    /// Count for the same visibility-filtered scope.
    pub total: HubCount,
}

/// Existing discovery documents carry Project selectors, lineage and previews.
/// These are metadata bindings, never proof that native package bytes are valid.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct HubProjectDetail {
    /// Exact current metadata snapshot, including preview descriptors if advertised.
    pub manifest: crate::DiscoveryManifest,
    /// Explicit ordered refs, default ref and immutable snapshot selectors.
    pub refs: crate::RefSet,
    /// Branch/Version/tag/Fork/Proposal metadata; required when advertised.
    pub lineage: Option<crate::ResearchLineage>,
}
impl HubProjectDetail {
    fn validate(&self, record: &HubRecord) -> Result<(), DiscoveryError> {
        let limits = DiscoveryLimits::default();
        self.manifest.validate(limits)?;
        self.refs.validate(limits)?;
        self.refs.validate_manifest(&self.manifest)?;
        if !matches!(record.target, HubTarget::Project { .. })
            || self.manifest.repository != record.repository.repository
        {
            return Err(mismatch());
        }
        match (&self.manifest.lineage, &self.lineage) {
            (Some(_), Some(lineage)) => {
                lineage.validate(limits)?;
                self.manifest.bind_lineage(&self.refs, lineage)?;
            }
            (None, None) => (),
            _ => return Err(mismatch()),
        }
        Ok(())
    }
}

/// Native control response. Non-200 errors use the same closed envelope.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct HubControlResponse {
    /// Exactly [`HUB_CONTROL_FORMAT`].
    pub format: String,
    /// Echo of the request UUID; rejects late/mismatched responses.
    pub request_uuid: String,
    /// Closed result family.
    pub result: HubControlResult,
}

/// Capability names correspond to operation families, not promises of authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum HubControlCapability {
    /// Fresh authorized account context.
    Accounts,
    /// Signed-out public catalog discovery.
    PublicSearch,
    /// Authorized account inventory including private objects.
    PrivateSearch,
    /// Current caller starred inventory, with fresh visibility checks.
    StarredSearch,
    /// Inspect accessible metadata and exact native identities.
    Inspect,
    /// Reviewed hosted management with durable operation reconciliation.
    Manage,
}

/// Discriminated operation results prevent a response being used for another task.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HubControlResult {
    /// Supported operation families.
    Capabilities {
        /// Supported operation families; conveys no account or object authority.
        capabilities: Vec<HubControlCapability>,
    },
    /// Fresh authorized account context.
    Accounts {
        /// Only freshly authorized account contexts for the current human session.
        accounts: Vec<HubAccount>,
        /// Bounded page size or continuation for this exact scope.
        page: HubPageInfo,
    },
    /// Bounded visibility-filtered metadata results.
    Search {
        /// Bounded records from the exact visibility-filtered query.
        records: Vec<HubRecord>,
        /// Bounded page size or continuation for this exact scope.
        page: HubPageInfo,
    },
    /// Inspect accessible metadata and exact native identities.
    Inspect {
        /// Current authorized metadata; never native content verification.
        record: HubRecord,
        /// Project discovery/lineage/preview metadata, absent when unavailable or for ontologies.
        detail: Option<Box<HubProjectDetail>>,
    },
    /// Prepared without applying an effect.
    Prepared {
        /// Complete reviewed native commitment; never renewed implicitly.
        plan: HubManagementPlan,
    },
    /// Durable prepared, pending, applied or refused management result.
    Receipt {
        /// Actor-bound durable result for the exact prepared operation.
        receipt: HubManagementReceipt,
    },
    /// Closed error response; does not imply success or object existence.
    Failure {
        /// Closed sanitized failure; callers supply actionable recovery text.
        code: HubControlErrorCode,
        /// Optional server delay, bounded to one hour; never an automatic mutation retry.
        retry_after_ms: Option<u32>,
    },
}

fn invalid() -> DiscoveryError {
    DiscoveryError::new(
        DiscoveryErrorCode::MalformedResponse,
        Some("hub_control"),
        "invalid Hub control document; refresh capabilities or review the request",
    )
}
fn mismatch() -> DiscoveryError {
    DiscoveryError::new(
        DiscoveryErrorCode::IntegrityFailure,
        Some("hub_control"),
        "Hub control response does not match the reviewed request",
    )
}
fn text(value: &str, max: usize, empty: bool) -> Result<(), DiscoveryError> {
    if (!empty && value.is_empty())
        || value.len() > max
        || value
            .chars()
            .any(|c| c.is_control() && c != '\n' && c != '\t')
    {
        return Err(invalid());
    }
    Ok(())
}
fn key(value: &str) -> Result<(), DiscoveryError> {
    text(value, 512, false)?;
    if value.chars().any(char::is_control) {
        return Err(invalid());
    }
    Ok(())
}
fn uuid(value: &str) -> Result<(), DiscoveryError> {
    if value.len() != 36
        || !value.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
        || !value.bytes().any(|b| b.is_ascii_hexdigit() && b != b'0')
    {
        return Err(invalid());
    }
    Ok(())
}
fn cursor(value: Option<&str>) -> Result<(), DiscoveryError> {
    if let Some(value) = value {
        text(value, 4096, false)?;
    }
    Ok(())
}
fn unique<T: PartialEq>(values: &[T]) -> bool {
    values
        .iter()
        .enumerate()
        .all(|(i, value)| !values[..i].contains(value))
}
fn tags(values: &[String]) -> Result<(), DiscoveryError> {
    if values.len() > 16 || !unique(values) {
        return Err(invalid());
    }
    for value in values {
        key(value)?;
    }
    Ok(())
}
fn item_id(value: &str) -> Result<(), DiscoveryError> {
    let id = ItemId::parse(value).map_err(|_| invalid())?;
    if id.as_str() != value || id.kind() != crate::ItemKind::OntologyModule {
        return Err(invalid());
    }
    Ok(())
}
fn origin(value: &str) -> Result<(), DiscoveryError> {
    text(value, 2048, false)?;
    let url = url::Url::parse(value).map_err(|_| invalid())?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || value != url.origin().ascii_serialization()
    {
        return Err(invalid());
    }
    Ok(())
}
fn parse<T: for<'a> Deserialize<'a>>(bytes: &[u8], limit: usize) -> Result<T, DiscoveryError> {
    if bytes.len() > limit {
        return Err(DiscoveryError::new(
            DiscoveryErrorCode::LimitExceeded,
            Some("hub_control"),
            "Hub control byte limit exceeded",
        ));
    }
    parse_unique_json(bytes).map_err(|_| invalid())
}
fn page_request(page: &HubPageRequest) -> Result<(), DiscoveryError> {
    if page.limit == 0 || page.limit > MAX_HUB_PAGE_SIZE {
        return Err(invalid());
    }
    cursor(page.cursor.as_deref())
}
fn page_info(
    page: &HubPageInfo,
    count: usize,
    request: &HubPageRequest,
) -> Result<(), DiscoveryError> {
    if page.next_cursor.is_some() && page.next_cursor == request.cursor {
        return Err(invalid());
    }
    if let HubCount::Exact { value } | HubCount::LowerBound { value } = page.total
        && value < count as u64
    {
        return Err(invalid());
    }
    cursor(page.next_cursor.as_deref())?;
    match page.total {
        HubCount::Exact { value }
        | HubCount::LowerBound { value }
        | HubCount::Approximate { value }
            if value > 9_007_199_254_740_991 =>
        {
            Err(invalid())
        }
        _ => Ok(()),
    }
}

impl HubTarget {
    /// Intrinsic identity validation, not host authority.
    pub fn validate(&self) -> Result<(), DiscoveryError> {
        match self {
            Self::Project { repository_id } => repository_id.validate(DiscoveryLimits::default()),
            Self::Ontology {
                id,
                version,
                digest,
            } => {
                item_id(id)?;
                key(version)?;
                digest.validate()
            }
        }
    }
}
impl HubMutation {
    /// Validate closed intent and exact witness shape before preparing.
    pub fn validate(&self) -> Result<(), DiscoveryError> {
        match self {
            Self::Star {
                target, witness, ..
            }
            | Self::Visit { target, witness } => {
                target.validate()?;
                if matches!(target, HubTarget::Ontology { .. }) != witness.is_some() {
                    return Err(invalid());
                }
                if let Some(witness) = witness {
                    witness.repository_id.validate(DiscoveryLimits::default())?;
                    witness.immutable_version.validate()?;
                }
                Ok(())
            }
            Self::SetVisibility { repository_id, .. } => {
                repository_id.validate(DiscoveryLimits::default())
            }
            Self::Rename {
                repository_id,
                name,
            }
            | Self::DeleteEmpty {
                repository_id,
                confirm_name: name,
            } => {
                repository_id.validate(DiscoveryLimits::default())?;
                crate::RepositoryIdentity {
                    owner: "owner".into(),
                    repository: name.clone(),
                }
                .validate()
            }
            Self::Tombstone { account_id, id } => {
                key(account_id)?;
                item_id(id)
            }
        }
    }
}
impl HubManagementPlan {
    /// Bounded host adapter over the same constructor used by native clients.
    pub fn prepare_json(bytes: &[u8]) -> Result<Self, DiscoveryError> {
        let input: HubManagementPlanInput = parse(bytes, MAX_HUB_REQUEST_BYTES)?;
        Self::new(
            input.hub_origin,
            input.operation_uuid,
            input.mutation,
            input.precondition,
            input.expires_at_ms,
        )
    }
    /// Build the same reviewed commitment in every client and host implementation.
    pub fn new(
        hub_origin: String,
        operation_uuid: String,
        mutation: HubMutation,
        precondition: HubManagementPrecondition,
        expires_at_ms: u64,
    ) -> Result<Self, DiscoveryError> {
        let mut plan = Self {
            hub_origin,
            operation_uuid,
            mutation,
            precondition,
            expires_at_ms,
            intent_digest: sha256_digest(b""),
        };
        plan.intent_digest = plan.commitment()?;
        plan.validate()?;
        Ok(plan)
    }
    fn commitment(&self) -> Result<Sha256Digest, DiscoveryError> {
        let bytes = canonical_json(&(
            &self.hub_origin,
            &self.operation_uuid,
            &self.mutation,
            &self.precondition,
            self.expires_at_ms,
        ))?;
        let mut domain = b"graphforge-hub-management-plan/1\0".to_vec();
        domain.extend(bytes);
        Ok(sha256_digest(&domain))
    }
    /// Reject changed review content; host must additionally enforce expiry/authority.
    pub fn validate(&self) -> Result<(), DiscoveryError> {
        origin(&self.hub_origin)?;
        uuid(&self.operation_uuid)?;
        self.mutation.validate()?;
        key(&self.precondition.authority_revision)?;
        key(&self.precondition.object_revision)?;
        if self.expires_at_ms == 0 || self.expires_at_ms > 9_007_199_254_740_991 {
            return Err(invalid());
        }
        self.intent_digest.validate()?;
        if self.intent_digest != self.commitment()? {
            return Err(mismatch());
        }
        Ok(())
    }
}
impl HubRecord {
    /// Metadata must retain exact target and current repository bindings.
    pub fn validate(&self) -> Result<(), DiscoveryError> {
        self.target.validate()?;
        self.repository.validate(DiscoveryLimits::default())?;
        if let Some(account_id) = &self.account_id {
            key(account_id)?;
        }
        text(&self.title, 1024, false)?;
        if let Some(v) = &self.description {
            text(v, 16 * 1024, true)?;
        }
        if let Some(v) = &self.license {
            text(v, 512, false)?;
        }
        if let Some(v) = &self.producing_core_version {
            key(v)?;
        }
        tags(&self.tags)?;
        if self.actions.len() > 13 || !unique(&self.actions) {
            return Err(invalid());
        }
        match (&self.target, &self.ontology) {
            (HubTarget::Project { repository_id }, None)
                if repository_id == &self.repository.repository_id =>
            {
                Ok(())
            }
            (
                HubTarget::Ontology {
                    id,
                    version,
                    digest,
                },
                Some(descriptor),
            ) => {
                descriptor.validate(DiscoveryLimits::default())?;
                if &descriptor.id != id
                    || &descriptor.module.version != version
                    || &descriptor.module.content_digest != digest
                    || descriptor.repository != self.repository.repository
                {
                    return Err(mismatch());
                }
                Ok(())
            }
            _ => Err(mismatch()),
        }
    }
}
impl HubControlRequest {
    /// Bounded duplicate-free closed JSON parsing.
    pub fn from_json(bytes: &[u8]) -> Result<Self, DiscoveryError> {
        let request: Self = parse(bytes, MAX_HUB_REQUEST_BYTES)?;
        request.validate()?;
        Ok(request)
    }
    /// Validate constructed requests under the same rules as parsed ones.
    pub fn validate(&self) -> Result<(), DiscoveryError> {
        if canonical_json(self)?.len() > MAX_HUB_REQUEST_BYTES {
            return Err(invalid());
        }
        if self.format != HUB_CONTROL_FORMAT {
            return Err(invalid());
        }
        uuid(&self.request_uuid)?;
        match &self.operation {
            HubControlOperation::Capabilities {} => Ok(()),
            HubControlOperation::Accounts { page } => page_request(page),
            HubControlOperation::Search { query, page } => {
                page_request(page)?;
                text(&query.text, 512, true)?;
                tags(&query.tags)?;
                if let HubSearchScope::Account { account_id } = &query.scope {
                    key(account_id)?;
                }
                if matches!(query.scope, HubSearchScope::Public {})
                    && query.visibility == Some(HubVisibility::Private)
                {
                    return Err(invalid());
                }
                Ok(())
            }
            HubControlOperation::Inspect { target } => target.validate(),
            HubControlOperation::Prepare {
                operation_uuid,
                mutation,
            } => {
                uuid(operation_uuid)?;
                mutation.validate()
            }
            HubControlOperation::Apply { plan } => plan.validate(),
            HubControlOperation::Status {
                operation_uuid,
                intent_digest,
            } => {
                uuid(operation_uuid)?;
                intent_digest.validate()
            }
        }
    }
    /// Public reads work signed out. Other calls may not silently become public.
    #[must_use]
    pub fn requires_session(&self) -> bool {
        !matches!(
            &self.operation,
            HubControlOperation::Capabilities {}
                | HubControlOperation::Inspect { .. }
                | HubControlOperation::Search {
                    query: HubSearch {
                        scope: HubSearchScope::Public {},
                        ..
                    },
                    ..
                }
        )
    }
    /// Canonical validated request bytes with a finite encoded size.
    pub fn to_json(&self) -> Result<Vec<u8>, DiscoveryError> {
        self.validate()?;
        let bytes = canonical_json(self)?;
        if bytes.len() > MAX_HUB_REQUEST_BYTES {
            return Err(invalid());
        }
        Ok(bytes)
    }
}
fn validate_account_page(
    accounts: &[HubAccount],
    page: &HubPageInfo,
    request_page: &HubPageRequest,
) -> Result<(), DiscoveryError> {
    if accounts.len() > request_page.limit as usize {
        return Err(invalid());
    }
    if !unique(&accounts.iter().map(|a| &a.account_id).collect::<Vec<_>>()) {
        return Err(invalid());
    }
    page_info(page, accounts.len(), request_page)?;
    for account in accounts {
        key(&account.account_id)?;
        text(&account.label, 1024, false)?;
        if let Some(handle) = &account.handle {
            crate::RepositoryIdentity {
                owner: handle.clone(),
                repository: "repository".into(),
            }
            .validate()?;
        }
    }
    Ok(())
}
fn validate_search_page(
    query: &HubSearch,
    records: &[HubRecord],
    page: &HubPageInfo,
    request_page: &HubPageRequest,
) -> Result<(), DiscoveryError> {
    if records.len() > request_page.limit as usize {
        return Err(invalid());
    }
    if !unique(&records.iter().map(|r| &r.target).collect::<Vec<_>>()) {
        return Err(invalid());
    }
    page_info(page, records.len(), request_page)?;
    for record in records {
        record.validate()?;
        let kind = if matches!(record.target, HubTarget::Project { .. }) {
            HubObjectKind::Project
        } else {
            HubObjectKind::Ontology
        };
        if query.object_kind.is_some_and(|v| v != kind)
            || (matches!(query.scope, HubSearchScope::Public {})
                && (record.visibility != HubVisibility::Public || record.account_id.is_some()))
            || matches!(&query.scope, HubSearchScope::Account {account_id} if record.account_id.as_ref() != Some(account_id))
            || query.visibility.is_some_and(|v| v != record.visibility)
            || query.tags.iter().any(|tag| !record.tags.contains(tag))
            || (matches!(query.scope, HubSearchScope::Starred {}) && !record.starred)
        {
            return Err(mismatch());
        }
    }
    Ok(())
}
impl HubControlResponse {
    /// Validate and encode a host reply under the same finite wire bound.
    pub fn to_json(&self, request: &HubControlRequest) -> Result<Vec<u8>, DiscoveryError> {
        self.validate_for(request)?;
        canonical_json(self)
    }
    /// Parse and bind every result to the initiating request before exposure.
    pub fn from_json(bytes: &[u8], request: &HubControlRequest) -> Result<Self, DiscoveryError> {
        let response: Self = parse(bytes, MAX_HUB_RESPONSE_BYTES)?;
        response.validate_for(request)?;
        Ok(response)
    }
    /// Revalidate constructed host results using the native client rules.
    pub fn validate_for(&self, request: &HubControlRequest) -> Result<(), DiscoveryError> {
        request.validate()?;
        if canonical_json(self)?.len() > MAX_HUB_RESPONSE_BYTES {
            return Err(invalid());
        }
        if self.format != HUB_CONTROL_FORMAT || self.request_uuid != request.request_uuid {
            return Err(mismatch());
        }
        match (&request.operation, &self.result) {
            (_, HubControlResult::Failure { retry_after_ms, .. }) => {
                if retry_after_ms.is_some_and(|v| v > 3_600_000) {
                    return Err(invalid());
                }
                Ok(())
            }
            (
                HubControlOperation::Capabilities {},
                HubControlResult::Capabilities { capabilities },
            ) => {
                if capabilities.len() > 6 || !unique(capabilities) {
                    return Err(invalid());
                }
                Ok(())
            }
            (
                HubControlOperation::Accounts { page: request_page },
                HubControlResult::Accounts { accounts, page },
            ) => validate_account_page(accounts, page, request_page),
            (
                HubControlOperation::Search {
                    query,
                    page: request_page,
                },
                HubControlResult::Search { records, page },
            ) => validate_search_page(query, records, page, request_page),
            (
                HubControlOperation::Inspect { target },
                HubControlResult::Inspect { record, detail },
            ) => {
                if let Some(detail) = detail {
                    detail.validate(record)?;
                }
                record.validate()?;
                if target != &record.target {
                    return Err(mismatch());
                }
                Ok(())
            }
            (
                HubControlOperation::Prepare {
                    operation_uuid,
                    mutation,
                },
                HubControlResult::Prepared { plan },
            ) => {
                plan.validate()?;
                if operation_uuid != &plan.operation_uuid || mutation != &plan.mutation {
                    return Err(mismatch());
                }
                Ok(())
            }
            (HubControlOperation::Apply { plan }, HubControlResult::Receipt { receipt }) => {
                receipt.validate_plan(plan)
            }
            (
                HubControlOperation::Status {
                    operation_uuid,
                    intent_digest,
                },
                HubControlResult::Receipt { receipt },
            ) => {
                receipt.validate(operation_uuid, intent_digest)?;
                // Status carries only replay identity, not a reviewed target.
                // Fetch current metadata through an explicit inspect request.
                if receipt.record.is_some() {
                    return Err(mismatch());
                }
                Ok(())
            }
            _ => Err(mismatch()),
        }
    }
}
impl HubManagementReceipt {
    fn validate_plan(&self, plan: &HubManagementPlan) -> Result<(), DiscoveryError> {
        self.validate(&plan.operation_uuid, &plan.intent_digest)?;
        if let Some(record) = &self.record {
            let matches = match &plan.mutation {
                HubMutation::Star { target, .. } | HubMutation::Visit { target, .. } => {
                    &record.target == target
                }
                HubMutation::SetVisibility { repository_id, .. }
                | HubMutation::Rename { repository_id, .. }
                | HubMutation::DeleteEmpty { repository_id, .. } => {
                    record.target
                        == (HubTarget::Project {
                            repository_id: repository_id.clone(),
                        })
                }
                HubMutation::Tombstone { account_id, id } => {
                    record.account_id.as_ref() == Some(account_id)
                        && matches!(&record.target, HubTarget::Ontology { id: selected, .. } if selected == id)
                }
            };
            if !matches {
                return Err(mismatch());
            }
            if self.state == HubManagementState::Applied {
                let consistent = match &plan.mutation {
                    HubMutation::Star { starred, .. } => record.starred == *starred,
                    HubMutation::SetVisibility { visibility, .. } => {
                        record.visibility == *visibility
                    }
                    HubMutation::Rename { name, .. } => {
                        &record.repository.repository.repository == name
                    }
                    HubMutation::DeleteEmpty { .. } => false,
                    HubMutation::Visit { .. } | HubMutation::Tombstone { .. } => true,
                };
                if !consistent {
                    return Err(mismatch());
                }
            }
        }
        Ok(())
    }

    fn validate(
        &self,
        operation_uuid: &str,
        intent_digest: &Sha256Digest,
    ) -> Result<(), DiscoveryError> {
        if self.operation_uuid != operation_uuid
            || &self.intent_digest != intent_digest
            || (self.state == HubManagementState::Refused) != self.refusal.is_some()
        {
            return Err(mismatch());
        }
        if let Some(record) = &self.record {
            record.validate()?;
        }
        Ok(())
    }
}
