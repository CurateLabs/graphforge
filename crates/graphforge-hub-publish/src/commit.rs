//! Commit-time checks a Hub applies to the manifest it is asked to publish.

use crate::error::{HubPublishError, invalid};
use crate::wire::{ObjectDeclaration, PublishCapabilities};
use graphforge_discovery::{DiscoveryManifest, RepositoryIdentity};

/// Check that `manifest` names `repository` and lists exactly the session
/// `inventory` at the Hub's template locations, and that the advanced `refs`
/// include the manifest's resolved ref (ADR 0053).
pub fn check_commit_manifest(
    manifest: &DiscoveryManifest,
    repository: &RepositoryIdentity,
    capabilities: &PublishCapabilities,
    inventory: &[ObjectDeclaration],
    refs: &[String],
) -> Result<(), HubPublishError> {
    if &manifest.repository != repository {
        return Err(invalid("manifest repository does not match the URL"));
    }
    let exact = manifest.objects.len() == inventory.len()
        && manifest
            .objects
            .iter()
            .zip(inventory)
            .all(|(listed, admitted)| {
                listed.digest.0 == admitted.digest
                    && listed.length == admitted.length
                    && listed.media_type == admitted.media_type
                    && listed.locations == [capabilities.object_location(&admitted.digest)]
            });
    if !exact {
        return Err(invalid(
            "manifest objects must equal the session inventory at this hub's locations",
        ));
    }
    if !refs.contains(&manifest.resolved_ref) {
        return Err(invalid(
            "advanced refs must include the manifest's resolved ref",
        ));
    }
    Ok(())
}
