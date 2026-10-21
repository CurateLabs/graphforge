//! The porcelain verbs that read or write only the local Project.
use super::render::{LogEntry, log_line};
use super::{
    BranchArgs, CommitArgs, LogArgs, StatusArgs, SwitchArgs, TagArgs, created_at, derived_uuid,
    emit, operation_uuid, receipt_json, resolve_branch, resolve_version, short,
};
use graphforge_api::{
    BranchSource, CancellationToken, CreateResearchBranchRequest, ExecuteResearchBranchRequest,
    GfError, GraphForge, ResearchRegistry, SelectResearchBranchRequest, SetResearchTagRequest,
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
    let mut text = format!("On branch {} (head {})", record.label, short(&head));
    if let Some(remote) = upstream {
        value["upstream"] = json!({
            "repository_url": remote.repository_url,
            "ref": remote.ref_name,
            "head_version_uuid": remote.head_version_uuid,
        });
        text.push_str(&format!(
            "\nUpstream {} {} at {}, ahead {ahead}",
            remote.repository_url,
            remote.ref_name,
            short(&remote.head_version_uuid)
        ));
    }
    if !tags.is_empty() {
        text.push_str(&format!("\nTags on head: {}", tags.join(", ")));
    }
    for warning in &warnings {
        text.push_str(&format!("\nwarning: {warning}"));
    }
    emit(output, json_output, &value, &text)
}

fn log(
    graph: &GraphForge,
    args: &LogArgs,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let registry = graph.research_version_retention()?;
    let branch = resolve_branch(&registry, args.select.branch.as_deref())?;
    let start = match &args.version {
        Some(text) => resolve_version(&registry, text)?,
        None => head_of(&registry, branch)?,
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

/// Walk history from `start`: first parent only, or every parent.
fn walk(
    registry: &ResearchRegistry,
    start: Uuid,
    all: bool,
    limit: Option<usize>,
) -> Vec<LogEntry> {
    let mut entries = Vec::new();
    let mut seen = BTreeSet::new();
    let mut pending = vec![start];
    while let Some(version) = pending.pop() {
        if limit.is_some_and(|limit| entries.len() >= limit) {
            break;
        }
        if !seen.insert(version) {
            continue;
        }
        let record = registry.versions.get(&version);
        let parents = record.map_or_else(
            || registry.ancestry.get(&version).cloned().unwrap_or_default(),
            |record| record.parents.clone(),
        );
        entries.push(LogEntry {
            version_uuid: version.to_string(),
            parents: parents.iter().map(Uuid::to_string).collect(),
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
        });
        let next: Vec<Uuid> = if all {
            parents
        } else {
            parents.into_iter().take(1).collect()
        };
        // The first parent is visited first.
        pending.extend(next.into_iter().rev());
    }
    entries
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

fn commit(
    graph: &mut GraphForge,
    args: &CommitArgs,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let registry = graph.research_version_retention()?;
    let branch = resolve_branch(&registry, args.select.branch.as_deref())?;
    let operation = operation_uuid(args.operation_uuid.as_deref())?;
    let (receipt, was_replayed) = if let Some(receipt) = replayed(&registry, operation) {
        // The same operation identity is the same commit, or it conflicts.
        let version = receipt
            .version_uuid
            .and_then(|uuid| registry.versions.get(&uuid));
        if version.is_none_or(|version| {
            version.context_uuid != branch
                || version.description.as_deref() != Some(args.message.as_str())
        }) {
            return Err(GfError::Validation(
                "GF_IDEMPOTENCY_CONFLICT: this operation UUID recorded a different commit".into(),
            ));
        }
        (receipt, true)
    } else {
        let (author, committer) = crate::identity::resolve_from_process(&args.identity)?;
        let request = ExecuteResearchBranchRequest {
            author: Some(author),
            committer: Some(committer),
            operation_uuid: operation,
            expected_generation_uuid: current_generation(graph)?,
            branch_uuid: branch,
            version_uuid: derived_uuid(operation, "version"),
            query: read_query(args)?,
            created_at: created_at(operation),
            label: None,
            description: Some(args.message.clone()),
        };
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

fn branch(
    graph: &mut GraphForge,
    args: &BranchArgs,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let registry = graph.research_version_retention()?;
    let Some(name) = &args.name else {
        let current = resolve_branch(&registry, args.select.branch.as_deref()).ok();
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
                if let Some(remote) = upstream_of(&registry, *uuid) {
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
                match upstream_of(&registry, *uuid) {
                    Some(remote) => format!(
                        "{mark} {label} {head} -> {} {}",
                        remote.repository_url, remote.ref_name
                    ),
                    None => format!("{mark} {label} {head}"),
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        return emit(output, json_output, &Value::Array(list), &text);
    };
    let operation = operation_uuid(args.operation_uuid.as_deref())?;
    let (receipt, was_replayed) = if let Some(receipt) = replayed(&registry, operation) {
        let created = receipt
            .version_uuid
            .and_then(|uuid| registry.versions.get(&uuid))
            .and_then(|version| registry.branches.get(&version.context_uuid));
        if created.is_none_or(|record| record.label != *name) {
            return Err(GfError::Validation(
                "GF_IDEMPOTENCY_CONFLICT: this operation UUID created a different Branch".into(),
            ));
        }
        (receipt, true)
    } else {
        let base = match &args.from {
            Some(text) => resolve_version(&registry, text)?,
            None => head_of(
                &registry,
                resolve_branch(&registry, args.select.branch.as_deref())?,
            )?,
        };
        let (author, committer) = crate::identity::resolve_from_process(&args.identity)?;
        let request = CreateResearchBranchRequest {
            author: Some(author),
            committer: Some(committer),
            operation_uuid: operation,
            expected_generation_uuid: current_generation(graph)?,
            branch_uuid: derived_uuid(operation, "branch"),
            version_uuid: derived_uuid(operation, "version"),
            source: BranchSource::Version { version_uuid: base },
            creator_uuid: derived_uuid(operation, "creator"),
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
    let operation = operation_uuid(args.operation_uuid.as_deref())?;
    if registry.current_branch != Some(target) && replayed(&registry, operation).is_none() {
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
        &json!({"branch_uuid": target, "label": args.reference}),
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
    let operation = operation_uuid(args.operation_uuid.as_deref())?;
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
    let receipt = if let Some(receipt) = replayed(&registry, operation) {
        (receipt, true)
    } else {
        let request = SetResearchTagRequest {
            operation_uuid: operation,
            expected_generation_uuid: current_generation(graph)?,
            name: name.clone(),
            version_uuid: version,
        };
        (
            graph.set_research_tag(&request, &CancellationToken::new())?,
            false,
        )
    };
    let mut value = receipt_json(&receipt.0, receipt.1);
    value["name"] = json!(name);
    value["version_uuid"] = json!(version);
    emit(output, json_output, &value, &text)
}
