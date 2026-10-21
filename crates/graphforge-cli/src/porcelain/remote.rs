//! The porcelain verbs that talk to a Hub: `push`, `pull` and `log --remote`.
use super::local::{upstream_of, write_log};
use super::render::LogEntry;
use super::{
    BranchSelect, HubFlags, LogArgs, PullArgs, PushArgs, created_at, derived_uuid, emit,
    operation_uuid, receipt_json, resolve_branch, short,
};
use crate::hub_clone::remote_read::{RemoteRepository, RemoteRequest, read_remote};
use crate::hub_http::Transport;
use crate::hub_publish::PublishArgs;
use arrow::array::{Array as _, StringArray};
use graphforge_api::{
    CancellationToken, DiscoveryResearchVersionRequest, FetchResearchBranchRequest, GfError,
    GraphForge, PortableLimits, PortableMode, PreviewResearchUpstreamRequest, ResearchRegistry,
    ResearchUpstreamScope, ResearchUpstreamSelection, SetResearchTagRequest,
    UpdateResearchBranchRequest,
};
use graphforge_hub_publish::PublishToken;
use serde_json::json;
use std::collections::BTreeSet;
use std::io::Write;
use std::path::PathBuf;
use uuid::Uuid;

/// Dispatch a Hub verb for the production process: open the Project and use the
/// real transport and credential sources.
pub(crate) fn run_remote(
    command: crate::Command,
    project: Option<PathBuf>,
    project_dir: Option<PathBuf>,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let path = crate::resolve_project_path(project, project_dir)?;
    let path = path
        .to_str()
        .ok_or_else(|| GfError::Validation("--project must be valid UTF-8".into()))?;
    let mut graph = GraphForge::new(Some(path))?;
    run_remote_with(
        &crate::hub_http::HubTransport::new(),
        &|hub: &HubFlags| crate::hub_publish::publish_token(hub.token_env.as_deref()),
        &mut graph,
        command,
        json_output,
        output,
    )
}

/// Dispatch a Hub verb over a given transport and publish-credential source.
pub(crate) fn run_remote_with(
    transport: &dyn Transport,
    publish_token: &dyn Fn(&HubFlags) -> Result<Option<PublishToken>, GfError>,
    graph: &mut GraphForge,
    command: crate::Command,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    match command {
        crate::Command::Push(args) => {
            let token = publish_token(&args.hub)?;
            push(transport, graph, &args, token, json_output, output)
        }
        crate::Command::Pull(args) => pull(transport, graph, &args, json_output, output),
        crate::Command::Log(args) => log_remote(transport, graph, &args, json_output, output),
        _ => unreachable!("only Hub porcelain verbs are dispatched here"),
    }
}

/// `--ref` names a Branch by label (ADR 0061), so it selects the Branch just as
/// `--branch` does; naming two different Branches is an error.
fn branch_flag<'a>(
    select: &'a BranchSelect,
    git_ref: Option<&'a str>,
) -> Result<Option<&'a str>, GfError> {
    match (select.branch.as_deref(), git_ref) {
        (Some(branch), Some(reference)) if branch != reference => Err(GfError::Validation(
            "--ref names a Branch by its label; it must equal --branch".into(),
        )),
        (branch, reference) => Ok(branch.or(reference)),
    }
}

fn repository_of(
    registry: &ResearchRegistry,
    branch: Uuid,
    given: Option<&str>,
    hub: &HubFlags,
) -> Result<String, GfError> {
    let repository = match given {
        Some(repository) => repository.to_owned(),
        None => upstream_of(registry, branch)
            .map(|remote| remote.repository_url.clone())
            .ok_or_else(|| {
                GfError::Validation(
                    "GF_RESEARCH_UPSTREAM_UNSET: the Branch has no upstream; name the repository, for example `gf push owner/repository`"
                        .into(),
                )
            })?,
    };
    Ok(match &hub.hub {
        Some(hub) if !repository.contains("://") => {
            format!("{}/{repository}", hub.trim_end_matches('/'))
        }
        _ => repository,
    })
}

/// The arguments `gf publish` would take for this push.
pub(super) fn push_args(
    registry: &ResearchRegistry,
    args: &PushArgs,
) -> Result<PublishArgs, GfError> {
    let branch = resolve_branch(
        registry,
        branch_flag(&args.select, args.git_ref.as_deref())?,
    )?;
    // A Branch label is its ref: publish names the Branch by it.
    let label = registry.branches[&branch].label.clone();
    Ok(PublishArgs {
        rewrite: args.rewrite,
        prune: args.prune,
        check_fork_origin: None,
        repository: repository_of(registry, branch, args.repository.as_deref(), &args.hub)?,
        git_ref: Some(label),
        version_uuid: None,
        fork_of: None,
        operation_uuid: args.operation_uuid.clone(),
        staging_directory: args.staging_directory.clone(),
        protection_bypass_env: args.hub.protection_bypass_env.clone(),
        previews: Vec::new(),
    })
}

/// `gf push`: `gf publish` with the Branch's upstream as its defaults.
pub(crate) fn push(
    transport: &dyn Transport,
    graph: &GraphForge,
    args: &PushArgs,
    token: Option<PublishToken>,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let publish = push_args(&graph.research_version_retention()?, args)?;
    let (_, base) = crate::hub_http::parse_input(&publish.repository)?;
    let bypass = crate::hub_http::DiscoveryCredentials::load(
        &base,
        None,
        publish.protection_bypass_env.as_deref(),
        |name| std::env::var(name),
    )?;
    crate::hub_publish::publish_with_credentials(
        transport,
        graph,
        &publish,
        token,
        bypass.as_ref(),
        json_output,
        output,
    )
}

/// A served tag whose name a local tag holds for another Version refuses the pull.
fn check_tags(registry: &ResearchRegistry, remote: &RemoteRepository) -> Result<(), GfError> {
    for tag in &remote.lineage.tags {
        if let (Some(local), Ok(served)) = (
            registry.tags.get(&tag.name),
            Uuid::parse_str(&tag.version_uuid),
        ) && *local != served
        {
            return Err(GfError::Validation(format!(
                "GF_RESEARCH_TAG_CONFLICT: the Hub serves tag '{}' on another Version than the local tag; delete the local tag (`gf tag -d {}`) and pull again",
                tag.name, tag.name
            )));
        }
    }
    Ok(())
}

/// Install the served tags whose Versions this Project now knows.
fn install_tags(graph: &mut GraphForge, remote: &RemoteRepository) -> Result<(), GfError> {
    for tag in &remote.lineage.tags {
        let registry = graph.research_version_retention()?;
        let Ok(version) = Uuid::parse_str(&tag.version_uuid) else {
            continue;
        };
        if registry.tags.contains_key(&tag.name) || !registry.identities.contains_key(&version) {
            continue;
        }
        graph.set_research_tag(
            &SetResearchTagRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: graph.committed_generation_identity()?.generation_uuid,
                name: tag.name.clone(),
                version_uuid: Some(version),
            },
            &CancellationToken::new(),
        )?;
    }
    Ok(())
}

fn column<'a>(
    batch: &'a arrow::record_batch::RecordBatch,
    name: &str,
) -> Result<&'a StringArray, GfError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| GfError::Execution(format!("the upstream preview has no {name} column")))
}

/// `gf pull`: fetch the Hub's head of the Branch's ref, preview it as an upstream
/// and record a Version that incorporates every compatible change.
#[allow(clippy::too_many_lines)]
pub(crate) fn pull(
    transport: &dyn Transport,
    graph: &mut GraphForge,
    args: &PullArgs,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let registry = graph.research_version_retention()?;
    let branch = resolve_branch(
        &registry,
        branch_flag(&args.select, args.git_ref.as_deref())?,
    )?;
    let label = registry.branches[&branch].label.clone();
    let repository = repository_of(&registry, branch, args.repository.as_deref(), &args.hub)?;
    let operation = operation_uuid(args.operation_uuid.as_deref())?;
    let scratch = tempfile::tempdir()
        .map_err(|_| GfError::Storage("cannot create a download directory".into()))?;
    let request = RemoteRequest {
        repository: &repository,
        hub: args.hub.hub.as_deref(),
        token_env: args.hub.token_env.as_deref(),
        protection_bypass_env: args.hub.protection_bypass_env.as_deref(),
    };
    let (remote, head) = read_remote(transport, &request, Some((&label, scratch.path())))?;
    let head = head.ok_or_else(|| GfError::Execution("the remote head was not read".into()))?;
    let remote_head = Uuid::parse_str(&head.version_uuid)
        .map_err(|_| GfError::Validation("the remote head is not a Version UUID".into()))?;
    let local_head = registry.heads[&branch];
    check_tags(&registry, &remote)?;
    if local_head == remote_head || registry.ancestors(local_head).contains(&remote_head) {
        install_tags(graph, &remote)?;
        return emit(
            output,
            json_output,
            &json!({"up_to_date": true, "branch_uuid": branch, "head_version_uuid": local_head}),
            "Already up to date",
        );
    }
    let cancel = CancellationToken::new();
    let fetched = graph.fetch_research_branch(
        &FetchResearchBranchRequest {
            operation_uuid: derived_uuid(operation, "fetch"),
            expected_generation_uuid: graph.committed_generation_identity()?.generation_uuid,
            branch_uuid: branch,
            repository_url: &remote.repository_url,
            discovery: DiscoveryResearchVersionRequest {
                manifest_json: &remote.manifest_bytes,
                refs_json: &remote.refs_bytes,
                lineage_json: &remote.lineage_bytes,
                expected_repository: &remote.identity,
                version_uuid: &head.version_uuid,
                package: &head.package,
                discovery_limits: remote.limits,
                portable_limits: PortableLimits::default(),
                mode: PortableMode::Full,
                cancelled: None,
                scratch: None,
            },
        },
        &cancel,
    )?;
    let preview_request = PreviewResearchUpstreamRequest {
        branch_uuid: branch,
        scope: ResearchUpstreamScope::Branch,
        remote: Some(fetched.binding.clone()),
    };
    let preview = graph.preview_research_upstream(&preview_request, &cancel)?;
    let batch = &preview.batches[0];
    let (kinds, fields, changes, evidence) = (
        column(batch, "object_kind")?,
        column(batch, "field")?,
        column(batch, "change")?,
        column(batch, "required_evidence_acknowledgements")?,
    );
    let mut blocking = Vec::new();
    let mut adoptable = 0_usize;
    let mut acknowledge = BTreeSet::new();
    for row in 0..batch.num_rows() {
        match changes.value(row) {
            change @ ("conflict" | "dependency_unavailable") => {
                blocking.push(format!(
                    "{}:{} ({change})",
                    kinds.value(row),
                    fields.value(row)
                ));
            }
            "upstream" | "equivalent" => {
                adoptable += 1;
                let needed: Vec<Uuid> = serde_json::from_str(evidence.value(row))
                    .map_err(|_| GfError::Execution("malformed evidence requirements".into()))?;
                acknowledge.extend(needed);
            }
            _ => {}
        }
    }
    if !blocking.is_empty() {
        let shown = blocking
            .iter()
            .take(10)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        return Err(GfError::Validation(format!(
            "the upstream has {} change(s) that cannot be adopted automatically: {shown}; the fetch is recorded, so resolve them explicitly with `gf research upstream update --file`",
            blocking.len()
        )));
    }
    if adoptable == 0 {
        return Err(GfError::Validation(
            "the upstream head changes no field; incorporate it explicitly with `gf research upstream update --file`"
                .into(),
        ));
    }
    let digest_hex = batch
        .schema()
        .metadata()
        .get("graphforge.upstream.preview_sha256")
        .cloned()
        .ok_or_else(|| GfError::Execution("the upstream preview has no digest".into()))?;
    let mut preview_sha256 = [0_u8; 32];
    for (index, byte) in preview_sha256.iter_mut().enumerate() {
        *byte = digest_hex
            .get(index * 2..index * 2 + 2)
            .and_then(|pair| u8::from_str_radix(pair, 16).ok())
            .ok_or_else(|| GfError::Execution("malformed upstream preview digest".into()))?;
    }
    let (author, committer) = crate::identity::resolve_from_process(&args.identity)?;
    let version_uuid = derived_uuid(operation, "version");
    let update = UpdateResearchBranchRequest {
        operation_uuid: operation,
        expected_generation_uuid: graph.committed_generation_identity()?.generation_uuid,
        version_uuid,
        preview: preview_request,
        preview_sha256,
        selection: ResearchUpstreamSelection::AllCompatible,
        acknowledge_evidence: acknowledge,
        actor_uuid: derived_uuid(operation, "actor"),
        created_at: created_at(operation),
        explanation: format!("gf pull {repository} {label}"),
        author: Some(author),
        committer: Some(committer),
        label: None,
        description: Some(format!("Merge {label} of {repository}")),
    };
    let receipt = graph.update_research_branch(&update, &cancel)?;
    install_tags(graph, &remote)?;
    let mut value = receipt_json(&receipt, false);
    value["parents"] = json!([local_head, remote_head]);
    value["incorporated"] = json!(adoptable);
    value["conflicts"] = json!([]);
    emit(
        output,
        json_output,
        &value,
        &format!(
            "Merged {} into {label}: {}",
            short(&remote_head),
            short(&version_uuid)
        ),
    )
}

/// `gf log --remote`: walk the Hub's published lineage from the ref's head.
fn log_remote(
    transport: &dyn Transport,
    graph: &GraphForge,
    args: &LogArgs,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let registry = graph.research_version_retention()?;
    let branch = resolve_branch(
        &registry,
        branch_flag(&args.select, args.git_ref.as_deref())?,
    )?;
    let label = registry.branches[&branch].label.clone();
    let given = args.remote.as_deref().filter(|text| !text.is_empty());
    let repository = repository_of(&registry, branch, given, &args.hub)?;
    let (remote, _) = read_remote(
        transport,
        &RemoteRequest {
            repository: &repository,
            hub: args.hub.hub.as_deref(),
            token_env: args.hub.token_env.as_deref(),
            protection_bypass_env: args.hub.protection_bypass_env.as_deref(),
        },
        None,
    )?;
    let lineage = &remote.lineage;
    let start = match &args.version {
        Some(text) => lineage
            .tags
            .iter()
            .find(|tag| tag.name == *text)
            .map_or_else(|| text.clone(), |tag| tag.version_uuid.clone()),
        None => lineage
            .branches
            .iter()
            .find(|candidate| candidate.ref_name == label)
            .map(|candidate| candidate.head_version_uuid.clone())
            .ok_or_else(|| {
                GfError::Validation(format!("the repository publishes no ref '{label}'"))
            })?,
    };
    let mut entries = Vec::new();
    let mut seen = BTreeSet::new();
    let mut pending = vec![start];
    while let Some(version) = pending.pop() {
        if args.max_count.is_some_and(|limit| entries.len() >= limit) {
            break;
        }
        if !seen.insert(version.clone()) {
            continue;
        }
        let Some(record) = lineage.version(&version) else {
            break;
        };
        entries.push(LogEntry {
            version_uuid: record.version_uuid.clone(),
            parents: record.parents.clone(),
            author: record
                .author
                .as_ref()
                .and_then(|person| serde_json::to_value(person).ok()),
            committer: record
                .committer
                .as_ref()
                .and_then(|person| serde_json::to_value(person).ok()),
            created_at: None,
            label: None,
            description: None,
            tags: lineage
                .tags
                .iter()
                .filter(|tag| tag.version_uuid == record.version_uuid)
                .map(|tag| tag.name.clone())
                .collect(),
            kind: record.kind.clone(),
        });
        let next: Vec<String> = if args.all {
            record.parents.clone()
        } else {
            record.parents.iter().take(1).cloned().collect()
        };
        pending.extend(next.into_iter().rev());
    }
    write_log(&entries, json_output, output)
}
