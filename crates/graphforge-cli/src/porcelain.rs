//! Git-style research porcelain: `status`, `log`, `commit`, `branch`, `switch`,
//! `tag`, `push` and `pull` (ADR 0061).
//!
//! Every verb is one native operation (or, for `pull`, the existing fetch,
//! preview and update sequence): replay, conflict and credit behaviour are the
//! native operations'. This module resolves defaults, maps arguments to native
//! requests and renders the result; it holds no history semantics of its own.
use crate::identity::IdentityFlags;
use clap::{ArgGroup, Args};
use graphforge_api::{GfError, ResearchRegistry};
use serde_json::{Value, json};
use std::io::Write;
use std::path::PathBuf;
use uuid::Uuid;

mod local;
pub(crate) mod remote;
mod render;
#[cfg(test)]
mod tests;

/// Act on this Branch instead of the current one.
#[derive(Args, Default, Clone)]
pub(crate) struct BranchSelect {
    /// Branch ref (its label); defaults to the current Branch, then the only Branch.
    #[arg(long)]
    pub branch: Option<String>,
}

/// Where the Hub is and how to authenticate to it.
#[derive(Args, Default, Clone)]
pub(crate) struct HubFlags {
    /// Hub base URL for an owner/repository name.
    #[arg(long)]
    pub hub: Option<String>,
    /// Read a Hub bearer token from this environment variable.
    #[arg(long)]
    pub token_env: Option<String>,
    /// Read the x-vercel-protection-bypass secret from this environment variable.
    #[arg(long)]
    pub protection_bypass_env: Option<String>,
}

/// Show the current Branch, its head, its upstream and what is unpublished.
///
/// Example: `gf status`
#[derive(Args)]
pub(crate) struct StatusArgs {
    #[command(flatten)]
    pub select: BranchSelect,
}

/// Show history, newest first: the local Branch, or a repository with --remote.
///
/// Example: `gf log -n 10`, `gf log v1`, `gf log --remote`
#[derive(Args)]
pub(crate) struct LogArgs {
    /// Start from this tag or Version UUID instead of the Branch head.
    pub version: Option<String>,
    #[command(flatten)]
    pub select: BranchSelect,
    /// Read the Hub's published history instead; optionally name the repository.
    #[arg(long, num_args = 0..=1, default_missing_value = "")]
    pub remote: Option<String>,
    /// Remote ref to walk; defaults to the Branch's upstream ref.
    #[arg(long = "ref")]
    pub git_ref: Option<String>,
    /// Stop after this many Versions.
    #[arg(short = 'n', long = "max-count")]
    pub max_count: Option<usize>,
    /// Follow every parent, not only the first.
    #[arg(long)]
    pub all: bool,
    #[command(flatten)]
    pub hub: HubFlags,
}

/// Record a new Version of the Branch by running a query, with a message.
///
/// Example: `gf commit -m "Raise scores" --query "MATCH (n:Item) SET n.score = 2"`
#[derive(Args)]
#[command(group(ArgGroup::new("change").required(true).args(["query", "file"])))]
pub(crate) struct CommitArgs {
    /// Commit message; the first line is the subject.
    #[arg(short = 'm', long = "message")]
    pub message: String,
    /// Cypher mutation to apply to the Branch.
    #[arg(long)]
    pub query: Option<String>,
    /// File holding the Cypher mutation.
    #[arg(long)]
    pub file: Option<PathBuf>,
    /// Stable operation UUIDv7; rerunning with it returns the original receipt.
    #[arg(long)]
    pub operation_uuid: Option<String>,
    #[command(flatten)]
    pub select: BranchSelect,
    #[command(flatten)]
    pub identity: IdentityFlags,
}

/// List Branches, or create one at the current head or at `--from`.
///
/// Example: `gf branch`, `gf branch feature/score`, `gf branch fix --from v1`
#[derive(Args)]
pub(crate) struct BranchArgs {
    /// Name of the Branch to create; it must be a valid ref and unused.
    pub name: Option<String>,
    /// Create the Branch from this tag or Version UUID instead of the head.
    #[arg(long)]
    pub from: Option<String>,
    /// Stable operation UUIDv7 for exact replay.
    #[arg(long)]
    pub operation_uuid: Option<String>,
    #[command(flatten)]
    pub select: BranchSelect,
    #[command(flatten)]
    pub identity: IdentityFlags,
}

/// Select the Branch the other verbs act on.
///
/// Example: `gf switch feature/score`
#[derive(Args)]
pub(crate) struct SwitchArgs {
    /// Branch ref (its label).
    pub reference: String,
    /// Stable operation UUIDv7 for exact replay.
    #[arg(long)]
    pub operation_uuid: Option<String>,
}

/// List tags, create one on a Version, or delete one.
///
/// Example: `gf tag`, `gf tag v1`, `gf tag v1 <version-uuid>`, `gf tag -d v1`
#[derive(Args)]
pub(crate) struct TagArgs {
    /// Tag to create.
    pub name: Option<String>,
    /// Tag or Version UUID the new tag names; defaults to the Branch head.
    pub version: Option<String>,
    /// Delete this tag.
    #[arg(short = 'd', long = "delete", conflicts_with_all = ["name", "version"])]
    pub delete: Option<String>,
    /// Stable operation UUIDv7 for exact replay.
    #[arg(long)]
    pub operation_uuid: Option<String>,
    #[command(flatten)]
    pub select: BranchSelect,
}

/// Publish the Branch head to its Hub repository.
///
/// Example: `gf push`, `gf push curate/claims --ref main`
#[derive(Args)]
pub(crate) struct PushArgs {
    /// Repository (owner/repository or HTTPS URL); defaults to the Branch's upstream.
    pub repository: Option<String>,
    /// Ref to publish; defaults to the upstream ref, then the Branch label.
    #[arg(long = "ref")]
    pub git_ref: Option<String>,
    /// Replace a non-descendant head under the revision lease.
    #[arg(long)]
    pub rewrite: bool,
    /// Remove now-unreferenced published history.
    #[arg(long)]
    pub prune: bool,
    /// Stable operation UUID; a rerun replays the original receipt.
    #[arg(long)]
    pub operation_uuid: Option<String>,
    /// Existing local directory for publication staging.
    #[arg(long)]
    pub staging_directory: Option<PathBuf>,
    #[command(flatten)]
    pub select: BranchSelect,
    #[command(flatten)]
    pub hub: HubFlags,
}

/// Incorporate the Hub's head of the Branch's ref into the local Branch.
///
/// Example: `gf pull`
#[derive(Args)]
pub(crate) struct PullArgs {
    /// Repository (owner/repository or HTTPS URL); defaults to the Branch's upstream.
    pub repository: Option<String>,
    /// Ref to pull; defaults to the upstream ref, then the Branch label.
    #[arg(long = "ref")]
    pub git_ref: Option<String>,
    /// Stable operation UUIDv7 for exact replay.
    #[arg(long)]
    pub operation_uuid: Option<String>,
    #[command(flatten)]
    pub select: BranchSelect,
    #[command(flatten)]
    pub identity: IdentityFlags,
    #[command(flatten)]
    pub hub: HubFlags,
}

pub(crate) use local::run_local;
pub(crate) use remote::run_remote;

/// The Branch a verb acts on, resolved flag, then current, then the only Branch.
pub(crate) fn resolve_branch(
    registry: &ResearchRegistry,
    flag: Option<&str>,
) -> Result<Uuid, GfError> {
    if let Some(reference) = flag {
        let mut named = registry
            .branches
            .iter()
            .filter(|(_, record)| record.label == reference);
        let (uuid, _) = named
            .next()
            .ok_or_else(|| GfError::Validation(format!("no Branch is labelled '{reference}'")))?;
        return if named.next().is_some() {
            Err(GfError::Validation(format!(
                "more than one Branch is labelled '{reference}'"
            )))
        } else {
            Ok(*uuid)
        };
    }
    if let Some(current) = registry
        .current_branch
        .filter(|uuid| registry.branches.contains_key(uuid))
    {
        return Ok(current);
    }
    let mut all = registry.branches.keys();
    match (all.next(), all.next()) {
        (Some(only), None) => Ok(*only),
        _ => Err(GfError::Validation(
            "GF_RESEARCH_BRANCH_UNSELECTED: no Branch is selected; run `gf switch <ref>`".into(),
        )),
    }
}

/// Resolve a tag name, then a Version UUID, to a Version the Project knows.
pub(crate) fn resolve_version(registry: &ResearchRegistry, text: &str) -> Result<Uuid, GfError> {
    let version = match registry.tags.get(text) {
        Some(tagged) => *tagged,
        None => Uuid::parse_str(text).map_err(|_| {
            GfError::Validation(format!("'{text}' is neither a tag nor a Version UUID"))
        })?,
    };
    if registry.identities.contains_key(&version) {
        Ok(version)
    } else {
        Err(GfError::Validation(format!(
            "'{text}' names a Version this Project does not know"
        )))
    }
}

/// The caller's operation UUID, or a fresh UUIDv7. A caller-supplied identity
/// must be a UUIDv7 so that everything derived from it is stable on a rerun.
pub(crate) fn operation_uuid(given: Option<&str>) -> Result<Uuid, GfError> {
    let Some(text) = given else {
        return Ok(Uuid::now_v7());
    };
    let uuid = Uuid::parse_str(text)
        .map_err(|_| GfError::Validation("operation UUID is invalid".into()))?;
    if uuid.get_version_num() != 7 {
        return Err(GfError::Validation(
            "an operation UUID must be a UUIDv7".into(),
        ));
    }
    Ok(uuid)
}

/// Print the retry identity before a mutation can fail, including a refusal after
/// a durable fetch. JSON success results carry the identity in their receipt.
pub(crate) fn report_operation(
    given: Option<&str>,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<Uuid, GfError> {
    let operation = operation_uuid(given)?;
    if !json_output {
        writeln!(output, "Operation UUID: {operation}")
            .and_then(|()| output.flush())
            .map_err(|error| GfError::Execution(error.to_string()))?;
    }
    Ok(operation)
}

/// UTC microseconds carried by a UUIDv7, so a rerun records the same time.
pub(crate) fn created_at(operation: Uuid) -> i64 {
    operation.get_timestamp().map_or(0, |timestamp| {
        let (seconds, nanos) = timestamp.to_unix();
        i64::try_from(seconds).unwrap_or(i64::MAX / 2_000_000) * 1_000_000
            + i64::from(nanos / 1_000)
    })
}

/// Commitment to everything a verb was asked to do, independent of the Project's
/// CURRENT. Two runs under one operation identity are the same request exactly
/// when their intents are equal.
pub(crate) fn intent_digest(kind: &str, fields: &Value) -> [u8; 32] {
    use sha2::Digest as _;
    let mut digest = sha2::Sha256::new();
    digest.update(b"graphforge-porcelain-intent/1");
    digest.update(kind.as_bytes());
    // `serde_json` keeps object keys sorted, so equal fields encode equally.
    digest.update(fields.to_string().as_bytes());
    digest.finalize().into()
}

/// Lower-case hexadecimal of `bytes`.
pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut text, byte| {
        let _ = write!(text, "{byte:02x}");
        text
    })
}

/// An identity bound to the operation, its role and the complete intent: a Version
/// or Branch that carries it can only have come from that exact request.
pub(crate) fn bound_uuid(operation: Uuid, role: &str, intent: &[u8; 32]) -> Uuid {
    derived_uuid(operation, &format!("{role}/{}", hex(intent)))
}

/// The idempotency conflict a native operation reports when its identity was
/// used for another request.
pub(crate) fn idempotency_conflict() -> GfError {
    GfError::Project {
        code: graphforge_api::ProjectErrorCode::TransactionConflict,
        message: "operation identity has conflicting request content".into(),
    }
}

/// A stable identity derived from the operation and the role it plays.
pub(crate) fn derived_uuid(operation: Uuid, role: &str) -> Uuid {
    use sha2::Digest as _;
    let mut digest = sha2::Sha256::new();
    digest.update(b"graphforge-porcelain/1");
    digest.update(operation.as_bytes());
    digest.update(role.as_bytes());
    let bytes: [u8; 32] = digest.finalize().into();
    let mut first = [0_u8; 16];
    first.copy_from_slice(&bytes[..16]);
    uuid::Builder::from_custom_bytes(first).into_uuid()
}

/// Write `value` as JSON, or `human` as text.
pub(crate) fn emit(
    output: &mut dyn Write,
    json_output: bool,
    value: &Value,
    human: &str,
) -> Result<(), GfError> {
    let io = |error: std::io::Error| GfError::Execution(error.to_string());
    if json_output {
        serde_json::to_writer(&mut *output, value)
            .map_err(|error| GfError::Execution(error.to_string()))?;
        writeln!(output).map_err(io)
    } else {
        writeln!(output, "{human}").map_err(io)
    }
}

/// Abbreviated Version identity for human output.
pub(crate) fn short(uuid: &impl std::fmt::Display) -> String {
    uuid.to_string().chars().take(8).collect()
}

/// The receipt fields every mutating verb reports.
pub(crate) fn receipt_json(
    receipt: &graphforge_api::ResearchOperationReceipt,
    replayed: bool,
) -> Value {
    json!({
        "operation_uuid": receipt.operation_uuid,
        "generation_uuid": receipt.generation_uuid,
        "version_uuid": receipt.version_uuid,
        "replayed": replayed,
    })
}
