//! One repository's verified discovery state, and optionally the package of one
//! Branch head, for `gf pull` and `gf log --remote` (ADR 0061).
//!
//! The reads, the repository-move confirmation and the verification are the ones
//! `gf clone` performs; nothing here is a second implementation of them.
use super::{
    DiscoveryCredentials, DownloadControl, DownloadReport, MAX_METADATA_BYTES, RETRY_POLICY,
    RetryPolicy, Transport, discovery_limits, download_with_progress, endpoint,
    fetch_discovery_with_attempts, parse_input_at, protocol_error, read_bounded, read_lineage,
    validation, verify_discovery,
};
use graphforge_api::GfError;
use graphforge_discovery::{
    DiscoveryLimits, DiscoveryManifest, RefSet, RepositoryIdentity, ResearchLineage,
};
use std::path::{Path, PathBuf};

/// Which repository to read and how to authenticate the read.
pub(crate) struct RemoteRequest<'a> {
    pub(crate) repository: &'a str,
    pub(crate) hub: Option<&'a str>,
    pub(crate) token_env: Option<&'a str>,
    pub(crate) protection_bypass_env: Option<&'a str>,
}

/// Verified documents of one repository snapshot.
pub(crate) struct RemoteRepository {
    pub(crate) identity: RepositoryIdentity,
    /// Canonical repository URL, the key of the local remote Branch citation.
    pub(crate) repository_url: String,
    pub(crate) refs_bytes: Vec<u8>,
    pub(crate) manifest_bytes: Vec<u8>,
    pub(crate) lineage_bytes: Vec<u8>,
    pub(crate) lineage: ResearchLineage,
    pub(crate) limits: DiscoveryLimits,
}

/// A downloaded, not yet imported, Branch head package.
pub(crate) struct RemoteHead {
    pub(crate) version_uuid: String,
    pub(crate) package: PathBuf,
}

/// Read and verify the repository's refs, manifest and lineage. With `head`,
/// also download the package of that ref's head Version into `scratch`.
pub(crate) fn read_remote(
    transport: &dyn Transport,
    request: &RemoteRequest<'_>,
    head: Option<(&str, &Path)>,
) -> Result<(RemoteRepository, Option<RemoteHead>), GfError> {
    let (identity, base) = parse_input_at(request.repository, request.hub)?;
    let credentials = DiscoveryCredentials::load(
        &base,
        request.token_env,
        request.protection_bypass_env,
        |name| std::env::var(name),
    )?;
    let moves = crate::hub_http::RepositoryMoveTransport::new(transport, &identity, &base)
        .with_credentials(credentials.as_ref());
    let transport = &moves as &dyn Transport;
    let retry: &RetryPolicy = &RETRY_POLICY;
    let fetch = |url: url::Url| {
        let mut attempts = 0;
        read_bounded(
            fetch_discovery_with_attempts(
                transport,
                &url,
                credentials.as_ref(),
                MAX_METADATA_BYTES as u64,
                &mut attempts,
                retry,
            )?,
            MAX_METADATA_BYTES,
        )
    };
    let refs_bytes = fetch(endpoint(&base, "refs"))?;
    // Both old markers are verified before the first moved read, as in clone.
    let (_, manifest_base, _) = moves.resolved(false)?;
    let manifest_bytes = fetch(endpoint(&manifest_base, "manifest"))?;
    let (identity, _, _) = moves.resolved(false)?;
    let limits = discovery_limits();
    let (refs, manifest) = verify_discovery(&identity, &refs_bytes, &manifest_bytes, limits)?;
    if manifest.lineage.is_none() {
        return Err(validation(
            "hub.missing_object",
            "the repository advertises no research lineage",
        ));
    }
    let (lineage_bytes, lineage) = read_lineage(transport, &manifest, &refs, limits, retry)?;
    let repository_url = format!(
        "{}/{}",
        manifest_base.origin().ascii_serialization(),
        identity.canonical_name()
    );
    let downloaded = head
        .map(|(ref_name, scratch)| {
            download_head(
                transport, &manifest, &lineage, &refs, ref_name, scratch, retry,
            )
        })
        .transpose()?;
    Ok((
        RemoteRepository {
            identity,
            repository_url,
            refs_bytes,
            manifest_bytes,
            lineage_bytes,
            lineage,
            limits,
        },
        downloaded,
    ))
}

fn download_head(
    transport: &dyn Transport,
    manifest: &DiscoveryManifest,
    lineage: &ResearchLineage,
    refs: &RefSet,
    ref_name: &str,
    scratch: &Path,
    retry: &RetryPolicy,
) -> Result<RemoteHead, GfError> {
    if !refs.refs.iter().any(|reference| reference.name == ref_name) {
        return Err(validation(
            "hub.missing_ref",
            "the repository has no such ref",
        ));
    }
    let branch = lineage
        .branches
        .iter()
        .find(|branch| branch.ref_name == ref_name)
        .ok_or_else(|| {
            validation(
                "hub.missing_ref",
                "branch ref is absent from research lineage",
            )
        })?;
    let version_uuid = branch.head_version_uuid.clone();
    let (_, object) = manifest
        .research_version_object(lineage, &version_uuid)
        .map_err(|error| protocol_error(&error))?;
    let package = scratch.join(format!("remote-{version_uuid}.gfpb"));
    let cancelled = std::sync::atomic::AtomicBool::new(false);
    download_with_progress(
        transport,
        object,
        &package,
        &mut DownloadReport::default(),
        &mut DownloadControl {
            retry,
            cancelled: &cancelled,
            progress: None,
        },
    )?;
    Ok(RemoteHead {
        version_uuid,
        package,
    })
}
