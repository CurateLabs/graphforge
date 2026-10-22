//! Existing discovery/package and native import owners authenticate all fetch inputs.
use super::{FetchResearchBranchRequest, invalid};
use crate::{CancellationToken, GfError, GraphForge, OperationId, PortableImportRequest};
use graphforge_core::portable::PortableMode;
use graphforge_discovery::ResearchLineage;
use graphforge_storage::research_versions::{
    ResearchRemoteBranch, ResearchRemoteBranchFetch, ResearchRemoteFetchBinding,
};
use sha2::{Digest as _, Sha256};
use std::path::Path;
use uuid::Uuid;

pub(super) fn prepare(
    request: &FetchResearchBranchRequest<'_>,
    scratch: &Path,
    cancel: &CancellationToken,
) -> Result<(GraphForge, ResearchRemoteBranchFetch), GfError> {
    let discovery = &request.discovery;
    let accepted = verify(request, scratch, cancel)?;
    let lineage = ResearchLineage::from_json(discovery.lineage_json, discovery.discovery_limits)
        .map_err(|error| invalid(&error.to_string()))?;
    let branch = lineage
        .branches
        .iter()
        .find(|branch| {
            branch.branch_uuid == request.branch_uuid.to_string()
                && branch.head_version_uuid == accepted.version_uuid
        })
        .ok_or_else(|| invalid("fetch must select the advertised head of the existing Branch"))?;
    let package = accepted
        .version
        .package
        .as_ref()
        .ok_or_else(|| invalid("fetch requires a retained remote package"))?;
    if accepted.version.kind != "complete"
        || accepted.report.transport_digest.as_deref() != Some(package.object_digest.0.as_str())
    {
        return Err(invalid(
            "fetch requires the exact complete downloaded transport object",
        ));
    }
    let source = scratch.join("source");
    let imported = GraphForge::import_portable(
        &source,
        &PortableImportRequest {
            input: discovery.package.to_path_buf(),
            operation_id: OperationId(request.operation_uuid),
            limits: discovery.portable_limits,
        },
        Some(cancel.flag()),
    )
    .map_err(|error| invalid(&format!("remote native import failed: {error}")))?;
    if imported.package_digest != accepted.report.package_digest
        || imported.transport_digest != accepted.report.transport_digest
    {
        return Err(invalid(
            "remote package changed between verification and import",
        ));
    }
    let graph = GraphForge::new(source.to_str())?;
    let registry = graph.research_version_retention()?;
    let head =
        Uuid::parse_str(&accepted.version_uuid).map_err(|_| invalid("invalid fetched head"))?;
    let archive = registry
        .interchange
        .get(&head)
        .cloned()
        .ok_or_else(|| invalid("verified fetch lacks its native archive"))?;
    crate::research_versions::require_discovered_branch(
        branch,
        archive
            .genealogy
            .get(&request.branch_uuid)
            .ok_or_else(|| invalid("fetched Branch creation is unavailable"))?,
        archive.source_project_uuid,
    )?;
    let archive_identities = archive.identities.clone();
    let reference = ResearchRemoteBranch {
        repository_url: request.repository_url.into(),
        project_uuid: Uuid::parse_str(&lineage.project_uuid)
            .map_err(|_| invalid("invalid fetched Project"))?,
        ref_name: branch.ref_name.clone(),
        branch_uuid: request.branch_uuid,
        head_version_uuid: head,
        identity_sha256: digest(&accepted.version.identity_digest.0)?,
    };
    if !request
        .repository_url
        .ends_with(&format!("/{}", lineage.repository.canonical_name()))
    {
        return Err(invalid(
            "fetch repository URL differs from verified discovery",
        ));
    }
    reference.key()?;
    let fetch = ResearchRemoteBranchFetch {
        binding: ResearchRemoteFetchBinding {
            operation_uuid: request.operation_uuid,
            expected_generation_uuid: request.expected_generation_uuid,
            reference,
            lineage_sha256: Sha256::digest(
                lineage
                    .to_canonical_json()
                    .map_err(|error| invalid(&error.to_string()))?,
            )
            .into(),
            package_sha256: digest(&package.package_digest.0)?,
            object_sha256: digest(&package.object_digest.0)?,
            // A served tag is installed with the Version it names, here the ones
            // this fetch admits; a tag on any other Version stays on the Hub.
            tags: lineage
                .tags
                .iter()
                .filter_map(|tag| {
                    let version = Uuid::parse_str(&tag.version_uuid).ok()?;
                    archive_identities
                        .contains_key(&version)
                        .then(|| (tag.name.clone(), version))
                })
                .collect(),
        },
        archive: Box::new(archive),
    };
    Ok((graph, fetch))
}

fn verify(
    request: &FetchResearchBranchRequest<'_>,
    scratch: &Path,
    cancel: &CancellationToken,
) -> Result<crate::DiscoveredResearchVersion, GfError> {
    let discovery = &request.discovery;
    crate::verify_discovered_research_version(&crate::DiscoveryResearchVersionRequest {
        manifest_json: discovery.manifest_json,
        refs_json: discovery.refs_json,
        lineage_json: discovery.lineage_json,
        expected_repository: discovery.expected_repository,
        version_uuid: discovery.version_uuid,
        package: discovery.package,
        discovery_limits: discovery.discovery_limits,
        portable_limits: discovery.portable_limits,
        mode: PortableMode::Full,
        cancelled: Some(cancel.flag()),
        scratch: Some(scratch),
    })
    .map_err(|error| {
        invalid(&format!(
            "remote discovery/package verification failed: {error}"
        ))
    })
}

fn digest(value: &str) -> Result<[u8; 32], GfError> {
    let hex = value
        .strip_prefix("sha256:")
        .ok_or_else(|| invalid("invalid fetch digest"))?;
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid("invalid fetch digest"));
    }
    let mut output = [0; 32];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)
            .map_err(|_| invalid("invalid fetch digest"))?;
    }
    Ok(output)
}
