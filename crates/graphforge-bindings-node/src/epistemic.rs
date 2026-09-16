//! Epistemic bindings and native task ownership.

use crate::AbortSignal;
use crate::AnalyzeOptions;
use crate::Arc;
use crate::AsyncTask;
use crate::BeliefProjectionPolicyV1;
use crate::BeliefSubjectV1;
use crate::BigInt;
use crate::Buffer;
use crate::Env;
use crate::GfError;
use crate::GraphForge;
use crate::InvocationDescriptorHandle;
use crate::NodeSelectorInput;
use crate::OperationId;
use crate::PathsOptions;
use crate::ResolveBeliefProjectionRequest;
use crate::ResolveBeliefSubjectRequest;
use crate::ResolvedBeliefProjection;
use crate::ResolvedBeliefSubject;
use crate::Result;
use crate::RwLock;
use crate::SimilarOptions;
use crate::StatuslessPolicyV1;
use crate::SupersessionBranchPolicyV1;
use crate::Task;
use crate::WriteContext;
use crate::assertion_status;
use crate::cancelled_error;
use crate::canonical_operation_id;
use crate::hex_bytes;
use crate::napi;
use crate::napi_validation;
use crate::node_page;
use crate::node_selector_from_input;
use crate::optional_uuid;
use crate::parse_seed;
use crate::parse_terminal_uuids;
use crate::result_to_ipc;
use crate::to_napi_deferred_err;
use crate::to_napi_err;
use crate::to_napi_invocation_err;

/// Thin Node request for one immutable assertion-validity event.
#[napi(object)]
pub struct RecordAssertionValidityInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 event identity.
    pub validity_event_uuid: String,
    /// Existing assertion UUID.
    pub assertion_uuid: String,
    /// Inclusive lower valid-time bound in Unix microseconds.
    pub valid_from_micros: Option<i64>,
    /// Exclusive upper valid-time bound in Unix microseconds.
    pub valid_to_micros: Option<i64>,
    /// Optional existing reasoning UUID.
    pub reasoning_uuid: Option<String>,
    /// Existing producing provenance UUID.
    pub provenance_uuid: String,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin assertion-validity history filter.
#[napi(object, object_to_js = false)]
pub struct ListAssertionValidityInput {
    /// Optional assertion UUID filter.
    pub assertion_uuid: Option<String>,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin Node request for valid-time evaluation after a transaction-time cutoff.
#[napi(object)]
pub struct ApplyValidTimeInput {
    /// Mandatory transaction-time cutoff in Unix microseconds.
    pub transaction_cutoff_micros: i64,
    /// Valid time to evaluate in Unix microseconds.
    pub valid_time_micros: i64,
}

/// Thin Node request for one atomic assertion supersession.
#[napi(object)]
pub struct SupersedeAssertionInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 relation identity.
    pub supersession_uuid: String,
    /// Existing assertion that becomes superseded.
    pub prior_assertion_uuid: String,
    /// Existing replacement assertion.
    pub replacement_assertion_uuid: String,
    /// Caller-supplied UUIDv7 paired status-event identity.
    pub status_event_uuid: String,
    /// Existing reasoning record for the prior assertion.
    pub reasoning_uuid: String,
    /// Existing producing provenance event.
    pub provenance_uuid: String,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin branch-preserving supersession-history filter.
#[napi(object, object_to_js = false)]
pub struct ListAssertionSupersessionsInput {
    /// Optional prior-assertion UUID filter.
    pub prior_assertion_uuid: Option<String>,
    /// Optional replacement-assertion UUID filter.
    pub replacement_assertion_uuid: Option<String>,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin Node request for one immutable hypothesis group.
#[napi(object)]
pub struct CreateHypothesisGroupInput {
    /// Idempotency UUID.
    pub operation_uuid: String,
    /// Group UUID.
    pub group_uuid: String,
    /// Canonical question key.
    pub question_key: String,
    /// Producing provenance UUID.
    pub provenance_uuid: String,
    /// Optional actor UUID.
    pub actor_uuid: Option<String>,
}

/// Thin Node request for one hypothesis-membership event.
#[napi(object)]
pub struct RecordHypothesisMembershipInput {
    /// Idempotency UUID.
    pub operation_uuid: String,
    /// Event UUID.
    pub membership_event_uuid: String,
    /// Group UUID.
    pub group_uuid: String,
    /// Assertion UUID.
    pub assertion_uuid: String,
    /// `added` or `removed`.
    pub action: String,
    /// Reasoning UUID.
    pub reasoning_uuid: String,
    /// Producing provenance UUID.
    pub provenance_uuid: String,
    /// Optional actor UUID.
    pub actor_uuid: Option<String>,
}

/// Thin Node request for one explicit hypothesis selection or clear.
#[napi(object)]
pub struct RecordHypothesisSelectionInput {
    /// Idempotency UUID.
    pub operation_uuid: String,
    /// Event UUID.
    pub selection_event_uuid: String,
    /// Group UUID.
    pub group_uuid: String,
    /// Selected assertion, or absent to clear.
    pub selected_assertion_uuid: Option<String>,
    /// Reasoning UUID.
    pub reasoning_uuid: String,
    /// Producing provenance UUID.
    pub provenance_uuid: String,
    /// Optional actor UUID.
    pub actor_uuid: Option<String>,
}

/// Thin Node request for atomic selected-member removal.
#[napi(object)]
pub struct RemoveHypothesisMemberInput {
    /// Idempotency UUID.
    pub operation_uuid: String,
    /// Removal event UUID.
    pub membership_event_uuid: String,
    /// Paired selection event UUID.
    pub selection_event_uuid: String,
    /// Group UUID.
    pub group_uuid: String,
    /// Removed assertion UUID.
    pub assertion_uuid: String,
    /// Replacement selection, or absent to clear.
    pub selected_assertion_uuid: Option<String>,
    /// Reasoning UUID.
    pub reasoning_uuid: String,
    /// Producing provenance UUID.
    pub provenance_uuid: String,
    /// Optional actor UUID.
    pub actor_uuid: Option<String>,
}

/// Thin hypothesis-group history filter.
#[napi(object, object_to_js = false)]
pub struct ListHypothesisGroupsInput {
    /// Optional exact question key.
    pub question_key: Option<String>,
    /// Page size.
    pub limit: Option<u32>,
    /// Opaque page cursor.
    pub after: Option<String>,
    /// Optional abort signal.
    pub signal: Option<AbortSignal>,
}

/// Thin hypothesis-membership history filter.
#[napi(object, object_to_js = false)]
pub struct ListHypothesisMembershipInput {
    /// Optional group UUID.
    pub group_uuid: Option<String>,
    /// Optional assertion UUID.
    pub assertion_uuid: Option<String>,
    /// Page size.
    pub limit: Option<u32>,
    /// Opaque page cursor.
    pub after: Option<String>,
    /// Optional abort signal.
    pub signal: Option<AbortSignal>,
}

/// Thin hypothesis-selection history filter.
#[napi(object, object_to_js = false)]
pub struct ListHypothesisSelectionInput {
    /// Optional group UUID.
    pub group_uuid: Option<String>,
    /// Page size.
    pub limit: Option<u32>,
    /// Opaque page cursor.
    pub after: Option<String>,
    /// Optional abort signal.
    pub signal: Option<AbortSignal>,
}

/// Complete version-1 belief-resolution policy. Every field is mandatory.
#[napi(object)]
pub struct BeliefProjectionPolicyInput {
    /// Statuses eligible for projection.
    pub included_statuses: Vec<String>,
    /// Explicit statusless behavior.
    pub statusless: String,
    /// Explicit supersession-branch behavior.
    pub supersession_branches: String,
    /// Explicit hypothesis-selection behavior.
    pub hypotheses: String,
}

/// Resolve an immutable graph-only projection at one epistemic cutoff.
#[napi(object, object_to_js = false)]
pub struct ResolveBeliefProjectionInput {
    /// Mandatory transaction-time cutoff.
    pub transaction_cutoff_micros: i64,
    /// Optional valid-time intersection.
    pub valid_time_micros: Option<i64>,
    /// Complete version-1 policy.
    pub policy: BeliefProjectionPolicyInput,
    /// Optional abort signal.
    pub signal: Option<AbortSignal>,
}

/// Version-1 policy input whose omissions receive GraphForge validation codes.
#[napi(object)]
pub struct BeliefSubjectPolicyInput {
    /// Statuses eligible for projection.
    pub included_statuses: Option<Vec<String>>,
    /// Explicit statusless behavior.
    pub statusless: Option<String>,
    /// Explicit supersession-branch behavior.
    pub supersession_branches: Option<String>,
    /// Explicit hypothesis-selection behavior.
    pub hypotheses: Option<String>,
}

/// Resolve one explicitly addressed belief subject and its graph projection.
#[napi(object, object_to_js = false)]
pub struct ResolveBeliefSubjectInput {
    /// Exactly one of this assertion UUID or `hypothesisQuestionKey` is required.
    pub assertion_uuid: Option<String>,
    /// Exactly one of this question key or `assertionUuid` is required.
    pub hypothesis_question_key: Option<String>,
    /// Mandatory transaction-time cutoff.
    pub transaction_cutoff_micros: i64,
    /// Optional valid-time intersection.
    pub valid_time_micros: Option<i64>,
    /// Complete version-1 policy.
    pub policy: Option<BeliefSubjectPolicyInput>,
    /// Optional abort signal.
    pub signal: Option<AbortSignal>,
}

/// Same-generation opaque graph projection and canonical subject evidence.
#[napi(js_name = "ResolvedBeliefSubjectOutput")]
pub struct ResolvedBeliefSubjectOutput {
    projection: Arc<ResolvedBeliefProjection>,
    evidence: Vec<u8>,
}

fn belief_projection_policy(
    input: BeliefProjectionPolicyInput,
) -> Result<BeliefProjectionPolicyV1> {
    let included_statuses = input
        .included_statuses
        .iter()
        .map(|value| assertion_status(value))
        .collect::<Result<Vec<graphforge_api::AssertionStatus>>>()?;
    let statusless = match input.statusless.as_str() {
        "reject" => StatuslessPolicyV1::Reject,
        "exclude" => StatuslessPolicyV1::Exclude,
        "include" => StatuslessPolicyV1::Include,
        _ => {
            return Err(napi_validation(
                "statusless must be reject, exclude, or include",
            ));
        }
    };
    let supersession_branches = match input.supersession_branches.as_str() {
        "reject" => SupersessionBranchPolicyV1::Reject,
        "include_all_leaves" => SupersessionBranchPolicyV1::IncludeAllLeaves,
        _ => {
            return Err(napi_validation(
                "supersessionBranches must be reject or include_all_leaves",
            ));
        }
    };
    let hypotheses = match input.hypotheses.as_str() {
        "require_selected" => graphforge_api::HypothesisSelectionPolicyV1::RequireSelected,
        "exclude_unselected_group" => {
            graphforge_api::HypothesisSelectionPolicyV1::ExcludeUnselectedGroup
        }
        "include_all_current_members" => {
            graphforge_api::HypothesisSelectionPolicyV1::IncludeAllCurrentMembers
        }
        _ => {
            return Err(napi_validation(
                "hypotheses must be require_selected, exclude_unselected_group, or include_all_current_members",
            ));
        }
    };
    Ok(BeliefProjectionPolicyV1 {
        included_statuses,
        statusless,
        supersession_branches,
        hypotheses,
    })
}

fn belief_subject_policy(
    input: Option<BeliefSubjectPolicyInput>,
) -> Result<BeliefProjectionPolicyV1> {
    let input = input.ok_or_else(|| napi_validation("policy is required"))?;
    belief_projection_policy(BeliefProjectionPolicyInput {
        included_statuses: input
            .included_statuses
            .ok_or_else(|| napi_validation("policy.includedStatuses is required"))?,
        statusless: input
            .statusless
            .ok_or_else(|| napi_validation("policy.statusless is required"))?,
        supersession_branches: input
            .supersession_branches
            .ok_or_else(|| napi_validation("policy.supersessionBranches is required"))?,
        hypotheses: input
            .hypotheses
            .ok_or_else(|| napi_validation("policy.hypotheses is required"))?,
    })
}

/// Opaque Rust-owned graph projection resolved from explicit epistemic policy.
#[napi(js_name = "ResolvedBeliefProjection")]
pub struct ResolvedBeliefProjectionHandle {
    pub(super) inner: Arc<ResolvedBeliefProjection>,
}

#[napi]
impl ResolvedBeliefProjectionHandle {
    /// Source project generation pinned during resolution.
    #[napi(getter)]
    #[must_use]
    pub fn source_generation_uuid(&self) -> String {
        self.inner.source_generation_uuid().to_string()
    }

    /// Universal graph-content fingerprint as lowercase hex.
    #[napi(getter)]
    #[must_use]
    pub fn graph_content_fingerprint(&self) -> String {
        hex_bytes(&self.inner.graph_content_fingerprint())
    }

    /// Canonical versioned policy bytes.
    #[napi(getter)]
    #[must_use]
    pub fn policy_bytes(&self) -> Buffer {
        Buffer::from(self.inner.policy_bytes().to_vec())
    }

    /// Policy fingerprint as lowercase hex.
    #[napi(getter)]
    #[must_use]
    pub fn policy_fingerprint(&self) -> String {
        hex_bytes(&self.inner.policy_fingerprint())
    }

    /// Transaction snapshot fingerprint as lowercase hex.
    #[napi(getter)]
    #[must_use]
    pub fn snapshot_fingerprint(&self) -> String {
        hex_bytes(&self.inner.snapshot_fingerprint())
    }

    /// Transaction cutoff used by resolution.
    #[napi(getter)]
    #[must_use]
    pub fn transaction_cutoff_micros(&self) -> i64 {
        self.inner.transaction_cutoff_micros()
    }

    /// Optional valid-time intersection.
    #[napi(getter)]
    #[must_use]
    pub fn valid_time_micros(&self) -> Option<i64> {
        self.inner.valid_time_micros()
    }

    /// Optional valid-time result fingerprint as lowercase hex.
    #[napi(getter)]
    #[must_use]
    pub fn valid_time_fingerprint(&self) -> Option<String> {
        self.inner
            .valid_time_fingerprint()
            .map(|value| hex_bytes(&value))
    }

    /// Sorted epistemic source-record UUIDs used by resolution.
    #[napi(getter)]
    #[must_use]
    pub fn source_record_uuids(&self) -> Vec<String> {
        self.inner
            .source_record_uuids()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    /// Prepare rank without executing it.
    #[napi]
    pub fn prepare_rank_invocation(
        &self,
        label: String,
        by: String,
        via: Option<String>,
        directed: Option<bool>,
    ) -> Result<InvocationDescriptorHandle> {
        let options = graphforge_api::RankOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            via,
            directed: directed.unwrap_or(true),
            write_property: None,
        };
        self.inner
            .prepare_rank_invocation(&label, &options)
            .map(|inner| InvocationDescriptorHandle { inner })
            .map_err(|error| to_napi_invocation_err(&error))
    }

    /// Prepare clustering without executing it.
    #[napi]
    pub fn prepare_cluster_invocation(
        &self,
        label: String,
        by: String,
        via: Option<String>,
        directed: Option<bool>,
        vector_property: Option<String>,
    ) -> Result<InvocationDescriptorHandle> {
        let options = graphforge_api::ClusterOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            vector_property,
            via,
            directed: directed.unwrap_or(false),
            write_property: None,
        };
        self.inner
            .prepare_cluster_invocation(&label, &options)
            .map(|inner| InvocationDescriptorHandle { inner })
            .map_err(|error| to_napi_invocation_err(&error))
    }

    /// Prepare paths without executing it.
    #[napi(
        ts_args_type = "source: string | NodeHandle | { label: string; property: string; value: any } | null | undefined, target: string | NodeHandle | { label: string; property: string; value: any } | null | undefined, by: string, via?: string | null, directed?: boolean | null, k?: number | null, weight?: string | null, heuristic?: string | null, walkLength?: number | null, seed?: bigint | null, terminalUuids?: string[] | null, prizeProperty?: string | null, capacityProperty?: string | null, costProperty?: string | null"
    )]
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_paths_invocation(
        &self,
        source: Option<NodeSelectorInput<'_>>,
        target: Option<NodeSelectorInput<'_>>,
        by: String,
        via: Option<String>,
        directed: Option<bool>,
        k: Option<u32>,
        weight: Option<String>,
        heuristic: Option<String>,
        walk_length: Option<u32>,
        seed: Option<BigInt>,
        terminal_uuids: Option<Vec<String>>,
        prize_property: Option<String>,
        capacity_property: Option<String>,
        cost_property: Option<String>,
    ) -> Result<InvocationDescriptorHandle> {
        let seed = seed.map(parse_seed).transpose()?;
        let options = PathsOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            via,
            directed: directed.unwrap_or(true),
            k: k.unwrap_or(1) as usize,
            weight,
            capacity_property,
            cost_property,
            heuristic,
            walk_length: walk_length.map(|value| value as usize),
            seed,
            terminal_uuids: parse_terminal_uuids(terminal_uuids.as_deref().unwrap_or_default())?,
            prize_property,
        };
        let source = source.map(node_selector_from_input).transpose()?;
        let target = target.map(node_selector_from_input).transpose()?;
        self.inner
            .prepare_paths_invocation(source.as_ref(), target.as_ref(), &options)
            .map(|inner| InvocationDescriptorHandle { inner })
            .map_err(|error| to_napi_invocation_err(&error))
    }

    /// Prepare analysis without executing it.
    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_analyze_invocation(
        &self,
        by: String,
        label: Option<String>,
        via: Option<String>,
        directed: Option<bool>,
        weight: Option<String>,
        partition_property: Option<String>,
        k: Option<u32>,
    ) -> Result<InvocationDescriptorHandle> {
        let options = AnalyzeOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            via,
            directed: directed.unwrap_or(true),
            weight,
            k: k.map(|value| value as usize),
            partition_property,
        };
        self.inner
            .prepare_analyze_invocation(label.as_deref(), &options)
            .map(|inner| InvocationDescriptorHandle { inner })
            .map_err(|error| to_napi_invocation_err(&error))
    }

    /// Prepare similarity without executing it.
    #[napi]
    pub fn prepare_similar_invocation(
        &self,
        label: String,
        by: String,
        k: Option<u32>,
        vector_property: Option<String>,
        via: Option<String>,
    ) -> Result<InvocationDescriptorHandle> {
        let options = SimilarOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            k: k.unwrap_or(10) as usize,
            vector_property,
            via,
        };
        self.inner
            .prepare_similar_invocation(&label, &options)
            .map(|inner| InvocationDescriptorHandle { inner })
            .map_err(|error| to_napi_invocation_err(&error))
    }
}

#[napi]
impl ResolvedBeliefSubjectOutput {
    /// Opaque Rust-owned graph projection.
    #[napi(getter)]
    #[must_use]
    pub fn projection(&self) -> ResolvedBeliefProjectionHandle {
        ResolvedBeliefProjectionHandle {
            inner: Arc::clone(&self.projection),
        }
    }

    /// Canonical subject-evidence Arrow IPC stream.
    #[napi(getter)]
    #[must_use]
    pub fn evidence(&self) -> Buffer {
        Buffer::from(self.evidence.clone())
    }
}

/// Worker task for one assertion-validity append.
pub struct RecordAssertionValidityTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::RecordAssertionValidityRequest,
}

impl Task for RecordAssertionValidityTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.record_assertion_validity(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one assertion-validity history page.
pub struct ListAssertionValidityTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::ListAssertionValidityRequest,
}

impl Task for ListAssertionValidityTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.list_assertion_validity(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one valid-time projection.
pub struct ApplyValidTimeTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::ApplyValidTimeRequest,
}

impl Task for ApplyValidTimeTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.apply_valid_time(self.request)?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one atomic assertion supersession.
pub struct SupersedeAssertionTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::SupersedeAssertionRequest,
}

impl Task for SupersedeAssertionTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.supersede_assertion(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for a deterministic supersession-history page.
pub struct ListAssertionSupersessionsTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::ListAssertionSupersessionsRequest,
}

impl Task for ListAssertionSupersessionsTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.list_assertion_supersessions(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

enum HypothesisOperation {
    Create(graphforge_api::CreateHypothesisGroupRequest),
    Membership(graphforge_api::RecordHypothesisMembershipRequest),
    Selection(graphforge_api::RecordHypothesisSelectionRequest),
    Remove(graphforge_api::RemoveHypothesisMemberRequest),
    ListGroups(graphforge_api::ListHypothesisGroupsRequest),
    ListMembership(graphforge_api::ListHypothesisMembershipRequest),
    ListSelection(graphforge_api::ListHypothesisSelectionRequest),
    Members(OperationId),
    CurrentSelection(OperationId),
}

/// Worker task for hypothesis mutation and projection operations.
pub struct HypothesisTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    operation: HypothesisOperation,
}

impl Task for HypothesisTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            let result = match &self.operation {
                HypothesisOperation::Create(request) => {
                    graph.create_hypothesis_group(request.clone())
                }
                HypothesisOperation::Membership(request) => {
                    graph.record_hypothesis_membership(request)
                }
                HypothesisOperation::Selection(request) => {
                    graph.record_hypothesis_selection(request)
                }
                HypothesisOperation::Remove(request) => graph.remove_hypothesis_member(request),
                HypothesisOperation::ListGroups(request) => graph.list_hypothesis_groups(request),
                HypothesisOperation::ListMembership(request) => {
                    graph.list_hypothesis_membership(request)
                }
                HypothesisOperation::ListSelection(request) => {
                    graph.list_hypothesis_selection(request)
                }
                HypothesisOperation::Members(group_uuid) => graph.hypothesis_members(group_uuid.0),
                HypothesisOperation::CurrentSelection(group_uuid) => {
                    graph.hypothesis_selection(group_uuid.0)
                }
            }?;
            result_to_ipc(&result)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one deterministic epistemic transaction-time snapshot.
pub struct EpistemicSnapshotTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    transaction_cutoff: i64,
}

impl Task for EpistemicSnapshotTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.epistemic_snapshot(self.transaction_cutoff)?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one resolved epistemic projection.
pub struct ResolveBeliefProjectionTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: ResolveBeliefProjectionRequest,
    cancellation: graphforge_api::CancellationToken,
}

/// Worker task for one same-generation subject evidence and graph projection.
pub struct ResolveBeliefSubjectTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: ResolveBeliefSubjectRequest,
    cancellation: graphforge_api::CancellationToken,
}

impl Task for ResolveBeliefSubjectTask {
    type Output = std::result::Result<ResolvedBeliefSubject, GfError>;
    type JsValue = ResolvedBeliefSubjectOutput;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            if self.cancellation.is_cancelled() {
                return Err(cancelled_error());
            }
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            graph.resolve_belief_subject(&self.request)
        })())
    }

    fn resolve(
        &mut self,
        env: Env,
        output: Self::Output,
    ) -> napi::Result<ResolvedBeliefSubjectOutput> {
        output
            .and_then(|resolved| {
                Ok(ResolvedBeliefSubjectOutput {
                    projection: Arc::new(resolved.projection),
                    evidence: result_to_ipc(&resolved.evidence)?,
                })
            })
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

impl Task for ResolveBeliefProjectionTask {
    type Output = std::result::Result<ResolvedBeliefProjection, GfError>;
    type JsValue = ResolvedBeliefProjectionHandle;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            if self.cancellation.is_cancelled() {
                return Err(cancelled_error());
            }
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            graph.resolve_belief_projection(self.request.clone())
        })())
    }

    fn resolve(
        &mut self,
        env: Env,
        output: Self::Output,
    ) -> napi::Result<ResolvedBeliefProjectionHandle> {
        output
            .map(|inner| ResolvedBeliefProjectionHandle {
                inner: Arc::new(inner),
            })
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

#[napi]
impl GraphForge {
    /// Append one immutable assertion-validity event.
    #[napi]
    pub fn record_assertion_validity(
        &self,
        request: RecordAssertionValidityInput,
    ) -> Result<AsyncTask<RecordAssertionValidityTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(RecordAssertionValidityTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::RecordAssertionValidityRequest {
                context: WriteContext {
                    operation_uuid: canonical_operation_id(&request.operation_uuid)?,
                    actor_uuid: optional_uuid(request.actor_uuid.as_deref())?,
                },
                validity_event_uuid: canonical_operation_id(&request.validity_event_uuid)?.0,
                assertion_uuid: canonical_operation_id(&request.assertion_uuid)?.0,
                valid_from_micros: request.valid_from_micros,
                valid_to_micros: request.valid_to_micros,
                reasoning_uuid: optional_uuid(request.reasoning_uuid.as_deref())?,
                provenance_uuid: canonical_operation_id(&request.provenance_uuid)?.0,
            },
        }))
    }

    /// Return deterministic immutable assertion-validity history as Arrow IPC.
    #[napi]
    pub fn list_assertion_validity(
        &self,
        request: Option<ListAssertionValidityInput>,
    ) -> Result<AsyncTask<ListAssertionValidityTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListAssertionValidityInput {
            assertion_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let page = node_page(request.limit, request.after.as_deref(), request.signal)?;
        Ok(AsyncTask::new(ListAssertionValidityTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::ListAssertionValidityRequest {
                assertion_uuid: optional_uuid(request.assertion_uuid.as_deref())?,
                page,
            },
        }))
    }

    /// Apply valid time after resolving the mandatory transaction-time cutoff.
    #[napi]
    pub fn apply_valid_time(
        &self,
        request: ApplyValidTimeInput,
    ) -> Result<AsyncTask<ApplyValidTimeTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(ApplyValidTimeTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::ApplyValidTimeRequest {
                transaction_cutoff_micros: request.transaction_cutoff_micros,
                valid_time_micros: request.valid_time_micros,
            },
        }))
    }

    /// Atomically append one assertion supersession and paired terminal status.
    #[napi]
    pub fn supersede_assertion(
        &self,
        request: SupersedeAssertionInput,
    ) -> Result<AsyncTask<SupersedeAssertionTask>> {
        self.ensure_open()?;
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        Ok(AsyncTask::new(SupersedeAssertionTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::SupersedeAssertionRequest {
                context: WriteContext {
                    operation_uuid: canonical_operation_id(&request.operation_uuid)?,
                    actor_uuid,
                },
                supersession_uuid: canonical_operation_id(&request.supersession_uuid)?.0,
                prior_assertion_uuid: canonical_operation_id(&request.prior_assertion_uuid)?.0,
                replacement_assertion_uuid: canonical_operation_id(
                    &request.replacement_assertion_uuid,
                )?
                .0,
                status_event_uuid: canonical_operation_id(&request.status_event_uuid)?.0,
                reasoning_uuid: canonical_operation_id(&request.reasoning_uuid)?.0,
                provenance_uuid: canonical_operation_id(&request.provenance_uuid)?.0,
            },
        }))
    }

    /// Return deterministic branch-preserving supersession history as Arrow IPC.
    #[napi]
    pub fn list_assertion_supersessions(
        &self,
        request: Option<ListAssertionSupersessionsInput>,
    ) -> Result<AsyncTask<ListAssertionSupersessionsTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListAssertionSupersessionsInput {
            prior_assertion_uuid: None,
            replacement_assertion_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let parse_optional = |value: Option<&str>| {
            value
                .map(canonical_operation_id)
                .transpose()
                .map(|value| value.map(|id| id.0))
        };
        let after = request
            .after
            .as_deref()
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ListAssertionSupersessionsTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::ListAssertionSupersessionsRequest {
                prior_assertion_uuid: parse_optional(request.prior_assertion_uuid.as_deref())?,
                replacement_assertion_uuid: parse_optional(
                    request.replacement_assertion_uuid.as_deref(),
                )?,
                page: graphforge_api::PageRequest {
                    limit: request.limit.unwrap_or(graphforge_api::DEFAULT_PAGE_LIMIT),
                    after,
                    cancellation: Some(cancellation),
                },
            },
        }))
    }

    /// Create one immutable hypothesis group.
    #[napi]
    pub fn create_hypothesis_group(
        &self,
        request: CreateHypothesisGroupInput,
    ) -> Result<AsyncTask<HypothesisTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(HypothesisTask {
            engine: Arc::clone(&self.inner),
            operation: HypothesisOperation::Create(graphforge_api::CreateHypothesisGroupRequest {
                context: WriteContext {
                    operation_uuid: canonical_operation_id(&request.operation_uuid)?,
                    actor_uuid: optional_uuid(request.actor_uuid.as_deref())?,
                },
                group_uuid: canonical_operation_id(&request.group_uuid)?.0,
                question_key: request.question_key,
                provenance_uuid: canonical_operation_id(&request.provenance_uuid)?.0,
            }),
        }))
    }

    /// Append one explicit hypothesis-membership event.
    #[napi]
    pub fn record_hypothesis_membership(
        &self,
        request: RecordHypothesisMembershipInput,
    ) -> Result<AsyncTask<HypothesisTask>> {
        self.ensure_open()?;
        let action = match request.action.as_str() {
            "added" => graphforge_api::HypothesisMembershipAction::Added,
            "removed" => graphforge_api::HypothesisMembershipAction::Removed,
            _ => return Err(napi_validation("action must be 'added' or 'removed'")),
        };
        Ok(AsyncTask::new(HypothesisTask {
            engine: Arc::clone(&self.inner),
            operation: HypothesisOperation::Membership(
                graphforge_api::RecordHypothesisMembershipRequest {
                    context: WriteContext {
                        operation_uuid: canonical_operation_id(&request.operation_uuid)?,
                        actor_uuid: optional_uuid(request.actor_uuid.as_deref())?,
                    },
                    membership_event_uuid: canonical_operation_id(&request.membership_event_uuid)?
                        .0,
                    group_uuid: canonical_operation_id(&request.group_uuid)?.0,
                    assertion_uuid: canonical_operation_id(&request.assertion_uuid)?.0,
                    action,
                    reasoning_uuid: canonical_operation_id(&request.reasoning_uuid)?.0,
                    provenance_uuid: canonical_operation_id(&request.provenance_uuid)?.0,
                },
            ),
        }))
    }

    /// Append one explicit hypothesis selection or clear.
    #[napi]
    pub fn record_hypothesis_selection(
        &self,
        request: RecordHypothesisSelectionInput,
    ) -> Result<AsyncTask<HypothesisTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(HypothesisTask {
            engine: Arc::clone(&self.inner),
            operation: HypothesisOperation::Selection(
                graphforge_api::RecordHypothesisSelectionRequest {
                    context: WriteContext {
                        operation_uuid: canonical_operation_id(&request.operation_uuid)?,
                        actor_uuid: optional_uuid(request.actor_uuid.as_deref())?,
                    },
                    selection_event_uuid: canonical_operation_id(&request.selection_event_uuid)?.0,
                    group_uuid: canonical_operation_id(&request.group_uuid)?.0,
                    selected_assertion_uuid: optional_uuid(
                        request.selected_assertion_uuid.as_deref(),
                    )?,
                    reasoning_uuid: canonical_operation_id(&request.reasoning_uuid)?.0,
                    provenance_uuid: canonical_operation_id(&request.provenance_uuid)?.0,
                },
            ),
        }))
    }

    /// Atomically remove one member and explicitly change or clear selection.
    #[napi]
    pub fn remove_hypothesis_member(
        &self,
        request: RemoveHypothesisMemberInput,
    ) -> Result<AsyncTask<HypothesisTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(HypothesisTask {
            engine: Arc::clone(&self.inner),
            operation: HypothesisOperation::Remove(graphforge_api::RemoveHypothesisMemberRequest {
                context: WriteContext {
                    operation_uuid: canonical_operation_id(&request.operation_uuid)?,
                    actor_uuid: optional_uuid(request.actor_uuid.as_deref())?,
                },
                membership_event_uuid: canonical_operation_id(&request.membership_event_uuid)?.0,
                selection_event_uuid: canonical_operation_id(&request.selection_event_uuid)?.0,
                group_uuid: canonical_operation_id(&request.group_uuid)?.0,
                assertion_uuid: canonical_operation_id(&request.assertion_uuid)?.0,
                selected_assertion_uuid: optional_uuid(request.selected_assertion_uuid.as_deref())?,
                reasoning_uuid: canonical_operation_id(&request.reasoning_uuid)?.0,
                provenance_uuid: canonical_operation_id(&request.provenance_uuid)?.0,
            }),
        }))
    }

    /// Return deterministic hypothesis-group history.
    #[napi]
    pub fn list_hypothesis_groups(
        &self,
        request: Option<ListHypothesisGroupsInput>,
    ) -> Result<AsyncTask<HypothesisTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListHypothesisGroupsInput {
            question_key: None,
            limit: None,
            after: None,
            signal: None,
        });
        let page = node_page(request.limit, request.after.as_deref(), request.signal)?;
        Ok(AsyncTask::new(HypothesisTask {
            engine: Arc::clone(&self.inner),
            operation: HypothesisOperation::ListGroups(
                graphforge_api::ListHypothesisGroupsRequest {
                    question_key: request.question_key,
                    page,
                },
            ),
        }))
    }

    /// Return deterministic hypothesis-membership history.
    #[napi]
    pub fn list_hypothesis_membership(
        &self,
        request: Option<ListHypothesisMembershipInput>,
    ) -> Result<AsyncTask<HypothesisTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListHypothesisMembershipInput {
            group_uuid: None,
            assertion_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let page = node_page(request.limit, request.after.as_deref(), request.signal)?;
        Ok(AsyncTask::new(HypothesisTask {
            engine: Arc::clone(&self.inner),
            operation: HypothesisOperation::ListMembership(
                graphforge_api::ListHypothesisMembershipRequest {
                    group_uuid: optional_uuid(request.group_uuid.as_deref())?,
                    assertion_uuid: optional_uuid(request.assertion_uuid.as_deref())?,
                    page,
                },
            ),
        }))
    }

    /// Return deterministic hypothesis-selection history.
    #[napi]
    pub fn list_hypothesis_selection(
        &self,
        request: Option<ListHypothesisSelectionInput>,
    ) -> Result<AsyncTask<HypothesisTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListHypothesisSelectionInput {
            group_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let page = node_page(request.limit, request.after.as_deref(), request.signal)?;
        Ok(AsyncTask::new(HypothesisTask {
            engine: Arc::clone(&self.inner),
            operation: HypothesisOperation::ListSelection(
                graphforge_api::ListHypothesisSelectionRequest {
                    group_uuid: optional_uuid(request.group_uuid.as_deref())?,
                    page,
                },
            ),
        }))
    }

    /// Return current hypothesis members.
    #[napi]
    pub fn hypothesis_members(&self, group_uuid: String) -> Result<AsyncTask<HypothesisTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(HypothesisTask {
            engine: Arc::clone(&self.inner),
            operation: HypothesisOperation::Members(canonical_operation_id(&group_uuid)?),
        }))
    }

    /// Return the current explicit hypothesis selection.
    #[napi]
    pub fn hypothesis_selection(&self, group_uuid: String) -> Result<AsyncTask<HypothesisTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(HypothesisTask {
            engine: Arc::clone(&self.inner),
            operation: HypothesisOperation::CurrentSelection(canonical_operation_id(&group_uuid)?),
        }))
    }

    /// Reconstruct one deterministic epistemic transaction-time snapshot as Arrow IPC.
    #[napi]
    pub fn epistemic_snapshot(
        &self,
        transaction_cutoff: i64,
    ) -> Result<AsyncTask<EpistemicSnapshotTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(EpistemicSnapshotTask {
            engine: Arc::clone(&self.inner),
            transaction_cutoff,
        }))
    }

    /// Resolve an immutable graph-only projection from explicit epistemic policy.
    #[napi(ts_return_type = "Promise<ResolvedBeliefProjection>")]
    pub fn resolve_belief_projection(
        &self,
        request: ResolveBeliefProjectionInput,
    ) -> Result<AsyncTask<ResolveBeliefProjectionTask>> {
        self.ensure_open()?;
        let signal = request.signal;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ResolveBeliefProjectionTask {
            engine: Arc::clone(&self.inner),
            request: ResolveBeliefProjectionRequest {
                transaction_cutoff_micros: request.transaction_cutoff_micros,
                valid_time_micros: request.valid_time_micros,
                policy: belief_projection_policy(request.policy)?,
            },
            cancellation,
        }))
    }

    /// Resolve one explicit belief subject and its projection from one generation.
    #[napi(
        ts_args_type = "request: ({ assertionUuid: string; hypothesisQuestionKey?: never } | { assertionUuid?: never; hypothesisQuestionKey: string }) & { transactionCutoffMicros: number; validTimeMicros?: number; policy: Required<BeliefSubjectPolicyInput>; signal?: AbortSignal }",
        ts_return_type = "Promise<ResolvedBeliefSubjectOutput>"
    )]
    pub fn resolve_belief_subject(
        &self,
        request: ResolveBeliefSubjectInput,
    ) -> Result<AsyncTask<ResolveBeliefSubjectTask>> {
        self.ensure_open()?;
        let subject = match (
            request.assertion_uuid.as_deref(),
            request.hypothesis_question_key,
        ) {
            (Some(assertion_uuid), None) => {
                BeliefSubjectV1::Assertion(canonical_operation_id(assertion_uuid)?.0)
            }
            (None, Some(question_key)) => BeliefSubjectV1::HypothesisQuestionKey(question_key),
            _ => {
                return Err(napi_validation(
                    "exactly one of assertionUuid or hypothesisQuestionKey is required",
                ));
            }
        };
        let signal = request.signal;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ResolveBeliefSubjectTask {
            engine: Arc::clone(&self.inner),
            request: ResolveBeliefSubjectRequest {
                subject,
                projection: ResolveBeliefProjectionRequest {
                    transaction_cutoff_micros: request.transaction_cutoff_micros,
                    valid_time_micros: request.valid_time_micros,
                    policy: belief_subject_policy(request.policy)?,
                },
            },
            cancellation,
        }))
    }
}
