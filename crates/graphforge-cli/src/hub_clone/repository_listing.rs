//! `gf hub list`: show one published Hub repository's refs and research
//! lineage without cloning it.
//!
//! The refs, manifest, and lineage documents go through the same reads and
//! the same verifier as `gf clone` (repository move confirmation, transport
//! digests, document bindings, and required semantics), so a repository that
//! clone refuses is refused here with the same code. No package object is read.

use super::{
    HttpTransport, RETRY_POLICY, RetryPolicy, Transport, canonical_name, discovery_limits,
    endpoint, parse_input_at, read_bounded, read_lineage, storage, verify_discovery,
};
use crate::hub_http::{MAX_METADATA_BYTES, RepositoryMoveTransport, fetch_with_attempts};
use clap::{Args, Subcommand};
use graphforge_discovery::{DiscoveryManifest, RefSet, ResearchLineage};
use serde::Serialize;
use std::io::Write;
use url::Url;

const LISTING_CONTRACT: &str = "graphforge-hub-listing/1";

/// Read published GraphForge Hub repositories without cloning them.
#[derive(Args)]
pub(crate) struct HubArgs {
    #[command(subcommand)]
    pub command: HubCommand,
}

#[derive(Subcommand)]
pub(crate) enum HubCommand {
    /// List a repository's refs, Branches, Versions, Fork origin, and Proposals.
    #[command(visible_alias = "ls")]
    List(ListArgs),
}

#[derive(Args)]
pub(crate) struct ListArgs {
    /// Canonical owner/repository name or an HTTPS Hub repository URL.
    pub repository: String,
    /// Hub base URL for an owner/repository name; explicit repository URLs name their own Hub.
    #[arg(long)]
    pub hub: Option<String>,
}

#[derive(Serialize)]
struct Listing {
    contract: &'static str,
    repository: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    moved_from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    moved_to: Option<String>,
    default_ref: String,
    immutable_version: String,
    /// Semantic digest of the Project package a selection-free clone of a
    /// repository without a default lineage Branch downloads.
    package_digest: String,
    refs: Vec<ListedRef>,
    /// Absent when the repository advertises no research lineage.
    lineage: Option<ListedLineage>,
}

#[derive(Serialize)]
struct ListedRef {
    name: String,
    target: String,
}

#[derive(Serialize)]
struct ListedLineage {
    project_uuid: String,
    fork: Option<ListedFork>,
    branches: Vec<ListedBranch>,
    versions: Vec<ListedVersion>,
    proposals: Vec<ListedProposal>,
}

/// The origin a Fork cites: its repository, Project, and Version.
#[derive(Serialize)]
struct ListedFork {
    repository: String,
    project_uuid: String,
    version_uuid: String,
    version_identity: String,
}

#[derive(Serialize)]
struct ListedBranch {
    #[serde(rename = "ref")]
    ref_name: String,
    branch_uuid: String,
    label: String,
    head_version_uuid: String,
    base_version_uuid: String,
    parent_branch_uuid: Option<String>,
}

#[derive(Serialize)]
struct ListedVersion {
    version_uuid: String,
    kind: String,
    branch_uuid: String,
    identity_digest: String,
    /// The Version a projection was derived from.
    source_version_uuid: Option<String>,
    /// Absent when no package is published for this Version.
    package_digest: Option<String>,
}

#[derive(Serialize)]
struct ListedProposal {
    proposal_uuid: String,
    source_branch_uuid: String,
    source_version_uuid: String,
    payload_version_uuid: String,
    package_digest: String,
}

pub(crate) fn run_hub(
    args: &HubArgs,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    match &args.command {
        HubCommand::List(list) => {
            run_list_with(&HttpTransport::new(), list, json, output, &RETRY_POLICY)
        }
    }
}

pub(crate) fn run_list_with(
    transport: &dyn Transport,
    args: &ListArgs,
    json: bool,
    output: &mut dyn Write,
    retry: &RetryPolicy,
) -> Result<(), graphforge_api::GfError> {
    let listing = list_repository(transport, args, retry)?;
    if json {
        serde_json::to_writer(&mut *output, &listing)
            .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
        writeln!(output).map_err(storage)
    } else {
        write_human(&listing, output).map_err(storage)
    }
}

fn fetch_document(
    transport: &dyn Transport,
    url: &Url,
    retry: &RetryPolicy,
) -> Result<Vec<u8>, graphforge_api::GfError> {
    let mut attempts = 0;
    read_bounded(
        fetch_with_attempts(
            transport,
            url,
            None,
            None,
            MAX_METADATA_BYTES as u64,
            &mut attempts,
            retry,
        )?,
        MAX_METADATA_BYTES,
    )
}

fn list_repository(
    transport: &dyn Transport,
    args: &ListArgs,
    retry: &RetryPolicy,
) -> Result<Listing, graphforge_api::GfError> {
    let (identity, base) = parse_input_at(&args.repository, args.hub.as_deref())?;
    let move_transport = RepositoryMoveTransport::new(transport, &identity, &base);
    let transport = &move_transport as &dyn Transport;
    let refs_bytes = fetch_document(transport, &endpoint(&base, "refs"), retry)?;
    // Both old markers are verified before the first moved read, as in clone.
    let (_, manifest_base, _) = move_transport.resolved(false)?;
    let manifest_bytes = fetch_document(transport, &endpoint(&manifest_base, "manifest"), retry)?;
    let (identity, _, moved) = move_transport.resolved(false)?;
    let limits = discovery_limits();
    let (refs, manifest) = verify_discovery(&identity, &refs_bytes, &manifest_bytes, limits)?;
    let lineage = if manifest.lineage.is_some() {
        Some(read_lineage(transport, &manifest, &refs, limits, retry)?.1)
    } else {
        None
    };
    let mut listing = listing(&refs, &manifest, lineage.as_ref());
    listing.moved_from = moved.as_ref().map(|m| m.moved_from.canonical_name());
    listing.moved_to = moved.as_ref().map(|m| m.moved_to.canonical_name());
    Ok(listing)
}

fn listing(
    refs: &RefSet,
    manifest: &DiscoveryManifest,
    lineage: Option<&ResearchLineage>,
) -> Listing {
    Listing {
        contract: LISTING_CONTRACT,
        repository: canonical_name(&manifest.repository),
        moved_from: None,
        moved_to: None,
        default_ref: refs.default_ref.clone(),
        immutable_version: manifest.immutable_version.0.clone(),
        package_digest: manifest.package.package_digest.0.clone(),
        refs: refs
            .refs
            .iter()
            .map(|item| ListedRef {
                name: item.name.clone(),
                target: item.target.0.clone(),
            })
            .collect(),
        lineage: lineage.map(|lineage| ListedLineage {
            project_uuid: lineage.project_uuid.clone(),
            fork: lineage.fork.as_ref().map(|fork| ListedFork {
                repository: canonical_name(&fork.origin_repository),
                project_uuid: fork.origin_project_uuid.clone(),
                version_uuid: fork.origin_version_uuid.clone(),
                version_identity: fork.origin_version_identity.0.clone(),
            }),
            branches: lineage
                .branches
                .iter()
                .map(|branch| ListedBranch {
                    ref_name: branch.ref_name.clone(),
                    branch_uuid: branch.branch_uuid.clone(),
                    label: branch.label.clone(),
                    head_version_uuid: branch.head_version_uuid.clone(),
                    base_version_uuid: branch.base_version_uuid.clone(),
                    parent_branch_uuid: branch.parent_branch_uuid.clone(),
                })
                .collect(),
            versions: lineage
                .versions
                .iter()
                .map(|version| ListedVersion {
                    version_uuid: version.version_uuid.clone(),
                    kind: version.kind.clone(),
                    branch_uuid: version.branch_uuid.clone(),
                    identity_digest: version.identity_digest.0.clone(),
                    source_version_uuid: version.source_version_uuid.clone(),
                    package_digest: version
                        .package
                        .as_ref()
                        .map(|package| package.package_digest.0.clone()),
                })
                .collect(),
            proposals: lineage
                .proposals
                .iter()
                .map(|proposal| ListedProposal {
                    proposal_uuid: proposal.proposal_uuid.clone(),
                    source_branch_uuid: proposal.source_branch_uuid.clone(),
                    source_version_uuid: proposal.source_version_uuid.clone(),
                    payload_version_uuid: proposal.payload_version_uuid.clone(),
                    package_digest: proposal.package.package_digest.0.clone(),
                })
                .collect(),
        }),
    }
}

/// One line per entry; Branches are named by their ref, other UUIDs in full.
fn write_human(listing: &Listing, output: &mut dyn Write) -> std::io::Result<()> {
    if let (Some(from), Some(to)) = (&listing.moved_from, &listing.moved_to) {
        writeln!(output, "Repository moved from {from} to {to}")?;
    }
    writeln!(
        output,
        "{} (default ref {})",
        listing.repository, listing.default_ref
    )?;
    let names: Vec<&str> = listing.refs.iter().map(|item| item.name.as_str()).collect();
    writeln!(output, "Refs: {}", names.join(", "))?;
    let Some(lineage) = &listing.lineage else {
        return writeln!(
            output,
            "No research lineage; clone downloads the Project package."
        );
    };
    let branch_name = |uuid: &str| {
        lineage
            .branches
            .iter()
            .find(|branch| branch.branch_uuid == uuid)
            .map_or_else(|| uuid.to_owned(), |branch| branch.ref_name.clone())
    };
    if let Some(fork) = &lineage.fork {
        writeln!(
            output,
            "Forked from {} Version {}",
            fork.repository, fork.version_uuid
        )?;
    }
    writeln!(output, "Branches:")?;
    for branch in &lineage.branches {
        write!(
            output,
            "  {}  head {}  base {}",
            branch.ref_name, branch.head_version_uuid, branch.base_version_uuid
        )?;
        match &branch.parent_branch_uuid {
            Some(parent) => writeln!(output, "  parent {}", branch_name(parent))?,
            None => writeln!(output)?,
        }
    }
    writeln!(output, "Versions:")?;
    for version in &lineage.versions {
        write!(
            output,
            "  {}  {}  {}",
            version.version_uuid,
            version.kind,
            branch_name(&version.branch_uuid)
        )?;
        if let Some(source) = &version.source_version_uuid {
            write!(output, "  of {source}")?;
        }
        let package = if version.package_digest.is_some() {
            "package"
        } else {
            "no package"
        };
        writeln!(output, "  {package}")?;
    }
    if !lineage.proposals.is_empty() {
        writeln!(output, "Proposals:")?;
        for proposal in &lineage.proposals {
            writeln!(
                output,
                "  {}  from {} Version {}  payload {}",
                proposal.proposal_uuid,
                branch_name(&proposal.source_branch_uuid),
                proposal.source_version_uuid,
                proposal.payload_version_uuid
            )?;
        }
    }
    Ok(())
}
