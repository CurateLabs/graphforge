//! Resolve writer qualification only from a publication receipt's authenticated manifest.

use super::{DiscoveryManifest, GfError, HubPublishErrorCode, PublishReceipt, publish_error};

#[derive(Debug, PartialEq)]
pub(crate) enum PublishedDurability {
    Qualified,
    Unverified(graphforge_api::UnverifiedDurability),
    Unavailable,
}

pub(super) fn receipt_binds_manifest(
    receipt: &PublishReceipt,
    manifest: &DiscoveryManifest,
) -> bool {
    manifest.repository == receipt.repository
        && manifest
            .canonical_digest()
            .is_ok_and(|digest| digest.0 == receipt.manifest_validator)
}

pub(super) fn published_durability(
    receipt: &PublishReceipt,
    manifests: &[&DiscoveryManifest],
) -> Result<PublishedDurability, GfError> {
    let Some(manifest) = manifests
        .iter()
        .find(|manifest| receipt_binds_manifest(receipt, manifest))
    else {
        return Ok(PublishedDurability::Unavailable);
    };
    graphforge_api::manifest_source_unverified_durability(manifest)
        .map(|stamp| match stamp {
            Some(stamp) => PublishedDurability::Unverified(stamp),
            None => PublishedDurability::Qualified,
        })
        .map_err(|_| {
            publish_error(
                HubPublishErrorCode::IntegrityFailure,
                "the publication manifest has invalid source durability metadata",
            )
        })
}
