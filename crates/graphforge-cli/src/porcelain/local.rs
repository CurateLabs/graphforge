//! The porcelain verbs that read or write only the local Project.
use super::render::{LogEntry, history_order, log_line};
use super::{
    BranchArgs, CommitArgs, LogArgs, StatusArgs, SwitchArgs, TagArgs, bound_uuid, created_at, emit,
    hex, idempotency_conflict, intent_digest, receipt_json, report_operation, resolve_branch,
    resolve_version, short,
};
use graphforge_api::{
    BranchSource, CancellationToken, CreateResearchBranchRequest, ExecuteResearchBranchRequest,
    GfError, GraphForge, ResearchRegistry, ResearchSignature, SelectResearchBranchRequest,
    SetResearchTagRequest,
};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::io::{Read, Write};
use uuid::Uuid;

const MAX_QUERY_BYTES: u64 = 1024 * 1024;

/// Dispatch a purely local verb.
pub(crate) fn run_local(
    graph: &mut GraphForge,
    command: crate::Command,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    match command {
        crate::Command::Status(args) => status(graph, &args, json, output),
        crate::Command::Log(args) => log(graph, &args, json, output),
        crate::Command::Commit(args) => commit(graph, &args, json, output),
        crate::Command::Branch(args) => branch(graph, &args, json, output),
        crate::Command::Switch(args) => switch(graph, &args, json, output),
        crate::Command::Tag(args) => tag(graph, &args, json, output),
        _ => unreachable!("only local porcelain verbs are dispatched here"),
    }
}

/// The recorded upstream of a Branch: the remote citation whose ref is the
/// Branch label if there is one, otherwise the first by repository and ref.
pub(super) fn upstream_of(
    registry: &ResearchRegistry,
    branch: Uuid,
) -> Option<&graphforge_api::ResearchRemoteBranch> {
    let label = registry.branches.get(&branch).map(|record| &record.label);
    registry
        .remote_branches
        .values()
        .filter(|remote| remote.branch_uuid == branch)
        .min_by_key(|remote| {
            (
                Some(&remote.ref_name) != label,
                remote.repository_url.clone(),
                remote.ref_name.clone(),
            )
        })
}

fn tags_of(registry: &ResearchRegistry, version: Uuid) -> Vec<String> {
    registry
        .tags
        .iter()
        .filter(|(_, target)| **target == version)
        .map(|(name, _)| name.clone())
        .collect()
}

fn head_of(registry: &ResearchRegistry, branch: Uuid) -> Result<Uuid, GfError> {
    registry
        .heads
        .get(&branch)
        .copied()
        .ok_or_else(|| GfError::Validation("the Branch has no head Version".into()))
}

fn current_generation(graph: &GraphForge) -> Result<Uuid, GfError> {
    Ok(graph.committed_generation_identity()?.generation_uuid)
}

fn lineage_of(registry: &ResearchRegistry, version: Uuid) -> BTreeSet<Uuid> {
    let mut all: BTreeSet<Uuid> = registry.ancestors(version).into_iter().collect();
    all.insert(version);
    all
}

fn status(
    graph: &GraphForge,
    args: &StatusArgs,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let registry = graph.research_version_retention()?;
    let branch = resolve_branch(&registry, args.select.branch.as_deref())?;
    let record = &registry.branches[&branch];
    let head = head_of(&registry, branch)?;
    let upstream = upstream_of(&registry, branch);
    let ahead = upstream.map_or(0, |remote| {
        let local = lineage_of(&registry, head);
        let published = if registry.identities.contains_key(&remote.head_version_uuid) {
            lineage_of(&registry, remote.head_version_uuid)
        } else {
            BTreeSet::new()
        };
        local.difference(&published).count()
    });
    let mut warnings = Vec::new();
    if graphforge_discovery::check_ref_name(&record.label).is_err() {
        warnings.push("unpublishable label".to_owned());
    }
    if registry
        .current_branch
        .is_some_and(|current| !registry.branches.contains_key(&current))
    {
        warnings.push("the current Branch no longer exists; run `gf switch <ref>`".to_owned());
    }
    let tags = tags_of(&registry, head);
    let mut value = json!({
        "branch_uuid": branch,
        "label": record.label,
        "head_version_uuid": head,
        "ahead": ahead,
        "tags": tags,
        "warnings": warnings,
    });
    let mut lines = vec![format!(
        "On branch {} (head {})",
        record.label,
        short(&head)
    )];
    if let Some(remote) = upstream {
        value["upstream"] = json!({
            "repository_url": remote.repository_url,
            "ref": remote.ref_name,
            "head_version_uuid": remote.head_version_uuid,
        });
        lines.push(format!(
            "Upstream {} {} at {}, ahead {ahead}",
            remote.repository_url,
            remote.ref_name,
            short(&remote.head_version_uuid)
        ));
    }
    if !tags.is_empty() {
        lines.push(format!("Tags on head: {}", tags.join(", ")));
    }
    lines.extend(warnings.iter().map(|warning| format!("warning: {warning}")));
    let text = lines.join("\n");
    emit(output, json_output, &value, &text)
}

fn log(
    graph: &GraphForge,
    args: &LogArgs,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let registry = graph.research_version_retention()?;
    let start = if let Some(text) = &args.version {
        resolve_version(&registry, text)?
    } else {
        let branch = resolve_branch(&registry, args.select.branch.as_deref())?;
        head_of(&registry, branch)?
    };
    let entries = walk(&registry, start, args.all, args.max_count);
    write_log(&entries, json_output, output)
}

pub(super) fn write_log(
    entries: &[LogEntry],
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let io = |error: std::io::Error| GfError::Execution(error.to_string());
    if json_output {
        serde_json::to_writer(&mut *output, entries)
            .map_err(|error| GfError::Execution(error.to_string()))?;
        writeln!(output).map_err(io)
    } else {
        for entry in entries {
            writeln!(output, "{}", log_line(entry)).map_err(io)?;
        }
        Ok(())
    }
}

/// Walk history from `start`, newest first with every child before its
/// parents: first parent only, or every parent.
fn walk(
    registry: &ResearchRegistry,
    start: Uuid,
    all: bool,
    limit: Option<usize>,
) -> Vec<LogEntry> {
    let parents_of = |version: &Uuid| {
        registry.versions.get(version).map_or_else(
            || registry.ancestry.get(version).cloned().unwrap_or_default(),
            |record| record.parents.clone(),
        )
    };
    let order = history_order(
        start,
        all,
        |version| Some(parents_of(version)),
        |version| {
            registry
                .versions
                .get(version)
                .map(|record| record.created_at)
        },
    );
    order
        .into_iter()
        .take(limit.unwrap_or(usize::MAX))
        .map(|version| {
            let record = registry.versions.get(&version);
            LogEntry {
                version_uuid: version.to_string(),
                parents: parents_of(&version).iter().map(Uuid::to_string).collect(),
                author: record.and_then(|record| serde_json::to_value(&record.author).ok()),
                committer: record.and_then(|record| serde_json::to_value(&record.committer).ok()),
                created_at: record.map(|record| record.created_at),
                label: record.and_then(|record| record.label.clone()),
                description: record.and_then(|record| record.description.clone()),
                tags: tags_of(registry, version),
                kind: record
                    .map_or("released", |record| {
                        if record.content.source_version.is_some() {
                            "projection"
                        } else {
                            "complete"
                        }
                    })
                    .to_owned(),
            }
        })
        .collect()
}

/// A durable receipt for `operation`, when it was already published by `branch`.
fn replayed(
    registry: &ResearchRegistry,
    operation: Uuid,
) -> Option<graphforge_api::ResearchOperationReceipt> {
    registry.receipts.get(&operation).cloned()
}

fn read_query(args: &CommitArgs) -> Result<String, GfError> {
    match (&args.query, &args.file) {
        (Some(query), None) => Ok(query.clone()),
        (None, Some(path)) => {
            let mut text = String::new();
            std::fs::File::open(path)
                .map_err(|_| GfError::Validation("cannot open the query file".into()))?
                .take(MAX_QUERY_BYTES + 1)
                .read_to_string(&mut text)
                .map_err(|_| GfError::Validation("the query file is not UTF-8 text".into()))?;
            if text.len() as u64 > MAX_QUERY_BYTES {
                return Err(GfError::Validation("the query file exceeds 1 MiB".into()));
            }
            Ok(text)
        }
        _ => Err(GfError::Validation(
            "specify exactly one of --query and --file".into(),
        )),
    }
}

/// Everything `gf commit` was asked to do: the Branch, message, the exact query
/// and who is credited.
pub(super) fn commit_intent(
    branch: Uuid,
    args: &CommitArgs,
    signatures: &(ResearchSignature, ResearchSignature),
    query: &str,
) -> [u8; 32] {
    use sha2::Digest as _;
    let query_sha256 = hex(&sha2::Sha256::digest(query.as_bytes()));
    intent_digest(
        "commit",
        &json!({
            "branch": branch,
            "message": args.message,
            "query_sha256": query_sha256,
            "author": signatures.0,
            "committer": signatures.1,
        }),
    )
}

/// The native request `gf commit` publishes. The new Version's identity is bound
/// to the complete intent, so only that exact request can have produced it.
pub(super) fn commit_request(
    branch: Uuid,
    args: &CommitArgs,
    operation: Uuid,
    expected_generation: Uuid,
    signatures: (ResearchSignature, ResearchSignature),
    query: String,
) -> ExecuteResearchBranchRequest {
    let intent = commit_intent(branch, args, &signatures, &query);
    ExecuteResearchBranchRequest {
        author: Some(signatures.0),
        committer: Some(signatures.1),
        operation_uuid: operation,
        expected_generation_uuid: expected_generation,
        branch_uuid: branch,
        version_uuid: bound_uuid(operation, "version", &intent),
        query,
        created_at: created_at(operation),
        label: None,
        description: Some(args.message.clone()),
    }
}

fn commit(
    graph: &mut GraphForge,
    args: &CommitArgs,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let registry = graph.research_version_retention()?;
    let branch = resolve_branch(&registry, args.select.branch.as_deref())?;
    let operation = report_operation(args.operation_uuid.as_deref(), json_output, output)?;
    let signatures = crate::identity::resolve_from_process(&args.identity)?;
    let query = read_query(args)?;
    let request = commit_request(
        branch,
        args,
        operation,
        current_generation(graph)?,
        signatures,
        query,
    );
    // An earlier run of this identity replays only when it was this exact request:
    // the Version it recorded carries the identity bound to the complete intent.
    let (receipt, was_replayed) = if let Some(receipt) = replayed(&registry, operation) {
        if receipt.version_uuid != Some(request.version_uuid) {
            return Err(idempotency_conflict());
        }
        (receipt, true)
    } else {
        (
            graph.execute_research_branch(&request, &CancellationToken::new())?,
            false,
        )
    };
    let registry = graph.research_version_retention()?;
    let version = receipt
        .version_uuid
        .ok_or_else(|| GfError::Execution("the commit produced no Version".into()))?;
    let parents: Vec<Uuid> = registry
        .versions
        .get(&version)
        .map(|record| record.parents.clone())
        .unwrap_or_default();
    let mut value = receipt_json(&receipt, was_replayed);
    value["parents"] = json!(parents);
    let subject = args.message.lines().next().unwrap_or_default();
    let label = &registry.branches[&branch].label;
    emit(
        output,
        json_output,
        &value,
        &format!("[{label} {}] {subject}", short(&version)),
    )
}

fn list_branches(
    registry: &ResearchRegistry,
    args: &BranchArgs,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let current = resolve_branch(registry, args.select.branch.as_deref()).ok();
    let mut rows: Vec<(&String, Uuid)> = registry
        .branches
        .iter()
        .map(|(uuid, record)| (&record.label, *uuid))
        .collect();
    rows.sort();
    let list: Vec<Value> = rows
        .iter()
        .map(|(label, uuid)| {
            let mut row = json!({
                "branch_uuid": uuid,
                "label": label,
                "head_version_uuid": registry.heads.get(uuid),
                "current": Some(*uuid) == current,
            });
            if let Some(remote) = upstream_of(registry, *uuid) {
                row["upstream"] = json!({
                    "repository_url": remote.repository_url,
                    "ref": remote.ref_name,
                });
            }
            row
        })
        .collect();
    let text = rows
        .iter()
        .map(|(label, uuid)| {
            let mark = if Some(*uuid) == current { '*' } else { ' ' };
            let head = registry.heads.get(uuid).map_or_else(String::new, short);
            match upstream_of(registry, *uuid) {
                Some(remote) => format!(
                    "{mark} {label} {head} -> {} {}",
                    remote.repository_url, remote.ref_name
                ),
                None => format!("{mark} {label} {head}"),
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    emit(output, json_output, &Value::Array(list), &text)
}

fn branch(
    graph: &mut GraphForge,
    args: &BranchArgs,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let registry = graph.research_version_retention()?;
    let Some(name) = &args.name else {
        return list_branches(&registry, args, json_output, output);
    };
    let operation = report_operation(args.operation_uuid.as_deref(), json_output, output)?;
    let signatures = crate::identity::resolve_from_process(&args.identity)?;
    // The request as it was typed: a Branch asked for from the head, or from a
    // named Version, from a named Branch's head, or as the Project's first.
    let intent = intent_digest(
        "branch",
        &json!({
            "name": name,
            "from": args.from,
            "branch": args.select.branch,
            "author": signatures.0,
            "committer": signatures.1,
        }),
    );
    let version_uuid = bound_uuid(operation, "version", &intent);
    let (receipt, was_replayed) = if let Some(receipt) = replayed(&registry, operation) {
        if receipt.version_uuid != Some(version_uuid) {
            return Err(idempotency_conflict());
        }
        (receipt, true)
    } else {
        // The first Branch of a Project starts from the Project's current research.
        let source = match &args.from {
            Some(text) => BranchSource::Version {
                version_uuid: resolve_version(&registry, text)?,
            },
            None if registry.branches.is_empty() => BranchSource::Current {
                origin_version_uuid: bound_uuid(operation, "origin", &intent),
                context_uuid: bound_uuid(operation, "project", &intent),
            },
            None => BranchSource::Version {
                version_uuid: head_of(
                    &registry,
                    resolve_branch(&registry, args.select.branch.as_deref())?,
                )?,
            },
        };
        let request = CreateResearchBranchRequest {
            author: Some(signatures.0),
            committer: Some(signatures.1),
            operation_uuid: operation,
            expected_generation_uuid: current_generation(graph)?,
            branch_uuid: bound_uuid(operation, "branch", &intent),
            version_uuid,
            source,
            creator_uuid: bound_uuid(operation, "creator", &intent),
            created_at: created_at(operation),
            label: name.clone(),
            description: Some(format!("Create branch {name}")),
        };
        (
            graph.create_research_branch(&request, &CancellationToken::new())?,
            false,
        )
    };
    let mut value = receipt_json(&receipt, was_replayed);
    value["label"] = json!(name);
    emit(
        output,
        json_output,
        &value,
        &format!("Created branch '{name}'"),
    )
}

fn switch(
    graph: &mut GraphForge,
    args: &SwitchArgs,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let registry = graph.research_version_retention()?;
    let target = resolve_branch(&registry, Some(&args.reference))?;
    let operation = report_operation(args.operation_uuid.as_deref(), json_output, output)?;
    // Without an identity, selecting the current Branch is a no-op.
    // A supplied operation identity always goes through the receipted operation, a
    // no-op selection included, so the identity is bound to this request.
    if args.operation_uuid.is_some()
        || registry.current_branch != Some(target)
        || replayed(&registry, operation).is_some()
    {
        let request = SelectResearchBranchRequest {
            operation_uuid: operation,
            expected_generation_uuid: current_generation(graph)?,
            branch_uuid: target,
        };
        graph.select_research_branch(&request, &CancellationToken::new())?;
    }
    emit(
        output,
        json_output,
        &json!({"operation_uuid": operation, "branch_uuid": target, "label": args.reference}),
        &format!("Switched to branch '{}'", args.reference),
    )
}

fn tag(
    graph: &mut GraphForge,
    args: &TagArgs,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let registry = graph.research_version_retention()?;
    let (name, version, text) = if let Some(name) = &args.delete {
        (name.clone(), None, format!("Deleted tag {name}"))
    } else if let Some(name) = &args.name {
        let target = match &args.version {
            Some(text) => resolve_version(&registry, text)?,
            None => head_of(
                &registry,
                resolve_branch(&registry, args.select.branch.as_deref())?,
            )?,
        };
        (
            name.clone(),
            Some(target),
            format!("Tagged {name} {}", short(&target)),
        )
    } else {
        let list: Vec<Value> = registry
            .tags
            .iter()
            .map(|(name, version)| json!({"name": name, "version_uuid": version}))
            .collect();
        let text = registry
            .tags
            .iter()
            .map(|(name, version)| format!("{name} {}", short(version)))
            .collect::<Vec<_>>()
            .join("\n");
        return emit(output, json_output, &Value::Array(list), &text);
    };
    let operation = report_operation(args.operation_uuid.as_deref(), json_output, output)?;
    // The native operation replays a rerun of the identity and refuses a different request.
    let was_replayed = replayed(&registry, operation).is_some();
    let request = SetResearchTagRequest {
        operation_uuid: operation,
        expected_generation_uuid: current_generation(graph)?,
        name: name.clone(),
        version_uuid: version,
    };
    let receipt = (
        graph.set_research_tag(&request, &CancellationToken::new())?,
        was_replayed,
    );
    let mut value = receipt_json(&receipt.0, receipt.1);
    value["name"] = json!(name);
    value["version_uuid"] = json!(version);
    emit(output, json_output, &value, &text)
}
