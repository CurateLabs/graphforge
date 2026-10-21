//! Pure repository re-homing over the complete current ref closure.
use crate::{
    DiscoveryError, DiscoveryErrorCode, DiscoveryLimits, DiscoveryManifest, ObjectLocationTemplate,
    ProjectSummary, RefSet, RepositoryIdentity, ResearchLineage, Sha256Digest, sha256_digest,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// One exact stored manifest and the raw control objects it advertises.
/// Native package/module bytes are deliberately absent from this API.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositorySnapshot {
    /// Parsed original manifest; its canonical digest is a ref validator.
    pub manifest: DiscoveryManifest,
    /// Exact summary transport bytes, iff advertised by the manifest.
    pub summary: Option<Vec<u8>>,
    /// Exact lineage transport bytes, iff advertised by the manifest.
    pub lineage: Option<Vec<u8>>,
}
/// Complete deterministic replacement set, suitable for one Hub transaction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RehomedRepository {
    /// Re-homed refs with freshly derived manifest validators.
    pub refs: RefSet,
    /// Snapshots ordered by their new canonical manifest digest.
    pub snapshots: Vec<RepositorySnapshot>,
    /// Canonical digest of the new refs document.
    pub revision: Sha256Digest,
}
fn mismatch() -> DiscoveryError {
    DiscoveryError::new(
        DiscoveryErrorCode::IntegrityFailure,
        Some("rehome"),
        "repository re-homing bindings disagree",
    )
}

// Count encoded typed documents without first allocating their serialization.
struct Budget(usize);
impl std::io::Write for Budget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_sub(bytes.len())
            .ok_or_else(|| std::io::Error::other("document budget"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn account<T: Serialize>(budget: &mut Budget, value: &T) -> Result<(), DiscoveryError> {
    serde_json::to_writer(budget, value).map_err(|_| crate::limit("rehome.input"))
}
fn account_bytes(budget: &mut Budget, bytes: &[u8]) -> Result<(), DiscoveryError> {
    budget.0 = budget
        .0
        .checked_sub(bytes.len())
        .ok_or_else(|| crate::limit("rehome.input"))?;
    Ok(())
}
fn bind_raw(
    manifest: &DiscoveryManifest,
    digest: &Sha256Digest,
    bytes: &[u8],
) -> Result<(), DiscoveryError> {
    let object = manifest
        .objects
        .iter()
        .find(|object| &object.digest == digest)
        .ok_or_else(mismatch)?;
    if object.length != bytes.len() as u64 || sha256_digest(bytes) != *digest {
        return Err(mismatch());
    }
    Ok(())
}
fn replace_control(
    manifest: &mut DiscoveryManifest,
    old: &Sha256Digest,
    bytes: &[u8],
    template: &ObjectLocationTemplate,
) -> Result<Sha256Digest, DiscoveryError> {
    let object = manifest
        .objects
        .iter_mut()
        .find(|object| &object.digest == old)
        .ok_or_else(mismatch)?;
    let digest = sha256_digest(bytes);
    object.digest = digest.clone();
    object.length = bytes.len() as u64;
    object.locations = vec![template.expand(&digest)?];
    Ok(digest)
}
fn validate_snapshot(
    snapshot: &RepositorySnapshot,
    refs: &RefSet,
    limits: DiscoveryLimits,
) -> Result<(), DiscoveryError> {
    let manifest = &snapshot.manifest;
    manifest.validate(limits)?;
    if manifest.repository != refs.repository {
        return Err(mismatch());
    }
    let summary = match (&manifest.summary, &snapshot.summary) {
        (Some(reference), Some(bytes)) => {
            bind_raw(manifest, &reference.object_digest, bytes)?;
            let summary = ProjectSummary::from_json(bytes, limits)?;
            manifest.bind_summary(&summary)?;
            Some(summary)
        }
        (None, None) => None,
        _ => return Err(mismatch()),
    };
    let lineage = match (&manifest.lineage, &snapshot.lineage) {
        (Some(reference), Some(bytes)) => {
            bind_raw(manifest, &reference.object_digest, bytes)?;
            let lineage = ResearchLineage::from_json(bytes, limits)?;
            manifest.bind_lineage_document(&lineage)?;
            Some(lineage)
        }
        (None, None) => None,
        _ => return Err(mismatch()),
    };
    summary
        .as_ref()
        .map_or(Ok(()), |summary| summary.bind_lineage(lineage.as_ref()))
}

fn derive_snapshot(
    snapshot: &RepositorySnapshot,
    new_identity: &RepositoryIdentity,
    control_locations: &ObjectLocationTemplate,
    limits: DiscoveryLimits,
) -> Result<RepositorySnapshot, DiscoveryError> {
    let mut next = snapshot.clone();
    next.manifest.repository = new_identity.clone();
    if let Some(bytes) = &snapshot.summary {
        let mut summary = ProjectSummary::from_json(bytes, limits)?;
        summary.repository = new_identity.clone();
        let bytes = summary.to_canonical_json()?;
        let old = snapshot
            .manifest
            .summary
            .as_ref()
            .ok_or_else(mismatch)?
            .object_digest
            .clone();
        let digest = replace_control(&mut next.manifest, &old, &bytes, control_locations)?;
        let reference = next.manifest.summary.as_mut().ok_or_else(mismatch)?;
        reference.object_digest = digest;
        reference.summary_digest = summary.canonical_digest()?;
        next.summary = Some(bytes);
    }
    if let Some(bytes) = &snapshot.lineage {
        let mut lineage = ResearchLineage::from_json(bytes, limits)?;
        // The repository changes; Fork origin citations are immutable facts.
        lineage.repository = new_identity.clone();
        let bytes = lineage.to_canonical_json()?;
        let old = snapshot
            .manifest
            .lineage
            .as_ref()
            .ok_or_else(mismatch)?
            .object_digest
            .clone();
        let digest = replace_control(&mut next.manifest, &old, &bytes, control_locations)?;
        let reference = next.manifest.lineage.as_mut().ok_or_else(mismatch)?;
        reference.object_digest = digest;
        reference.lineage_digest = lineage.canonical_digest()?;
        next.lineage = Some(bytes);
    }
    next.manifest
        .objects
        .sort_by(|a, b| a.digest.cmp(&b.digest));
    next.manifest.validate(limits)?;
    Ok(next)
}

/// Re-home every snapshot referenced by `refs`, without I/O or package changes.
///
/// The input must be exactly the current ref closure: no missing or unreferenced
/// snapshots. Each current ref binds a supplied canonical manifest validator and
/// immutable target. Historical lineage is validated intrinsically, not against
/// a fabricated historical ref set. All original bindings are checked before
/// deriving any replacement. Aggregate encoded input is bounded by
/// `limits.max_response_bytes`, independently of each document's existing limit.
pub fn rehome_repository(
    refs: &RefSet,
    snapshots: &[RepositorySnapshot],
    new_identity: &RepositoryIdentity,
    control_locations: &ObjectLocationTemplate,
    limits: DiscoveryLimits,
) -> Result<RehomedRepository, DiscoveryError> {
    refs.validate(limits)?;
    new_identity.validate()?;
    if new_identity == &refs.repository
        || snapshots.is_empty()
        || snapshots.len() > refs.refs.len()
        || snapshots.len() > limits.max_refs
    {
        return Err(mismatch());
    }
    let mut budget = Budget(limits.max_response_bytes);
    account(&mut budget, refs)?;
    let mut original = BTreeMap::new();
    for snapshot in snapshots {
        account(&mut budget, &snapshot.manifest)?;
        for bytes in [&snapshot.summary, &snapshot.lineage].into_iter().flatten() {
            account_bytes(&mut budget, bytes)?;
        }
        validate_snapshot(snapshot, refs, limits)?;
        if original
            .insert(snapshot.manifest.canonical_digest()?, snapshot)
            .is_some()
        {
            return Err(mismatch());
        }
    }
    let mut referenced = BTreeSet::new();
    for reference in &refs.refs {
        let snapshot = original.get(&reference.validator).ok_or_else(mismatch)?;
        if reference.target != snapshot.manifest.immutable_version {
            return Err(mismatch());
        }
        referenced.insert(reference.validator.clone());
    }
    let default = refs
        .refs
        .iter()
        .find(|item| item.name == refs.default_ref)
        .ok_or_else(mismatch)?;
    if original
        .get(&default.validator)
        .ok_or_else(mismatch)?
        .manifest
        .default_ref
        != refs.default_ref
    {
        return Err(mismatch());
    }
    let default_snapshot = original.get(&default.validator).ok_or_else(mismatch)?;
    refs.validate_manifest(&default_snapshot.manifest)?;
    if let Some(bytes) = &default_snapshot.lineage {
        default_snapshot
            .manifest
            .bind_lineage(refs, &ResearchLineage::from_json(bytes, limits)?)?;
    }
    if referenced.len() != original.len() {
        return Err(mismatch());
    }
    let mut replacements = BTreeMap::new();
    let mut output = BTreeMap::new();
    for (old_validator, snapshot) in original {
        let next = derive_snapshot(snapshot, new_identity, control_locations, limits)?;
        let validator = next.manifest.canonical_digest()?;
        replacements.insert(old_validator, validator.clone());
        if output.insert(validator, next).is_some() {
            return Err(mismatch());
        }
    }
    let mut next_refs = refs.clone();
    next_refs.repository = new_identity.clone();
    for reference in &mut next_refs.refs {
        reference.validator = replacements
            .get(&reference.validator)
            .ok_or_else(mismatch)?
            .clone();
    }
    next_refs.validate(limits)?;
    for snapshot in output.values() {
        validate_snapshot(snapshot, &next_refs, limits)?;
    }
    let mut output_budget = Budget(limits.max_response_bytes);
    account(&mut output_budget, &next_refs)?;
    for snapshot in output.values() {
        account(&mut output_budget, &snapshot.manifest)?;
        for bytes in [&snapshot.summary, &snapshot.lineage].into_iter().flatten() {
            account_bytes(&mut output_budget, bytes)?;
        }
    }
    let revision = sha256_digest(&next_refs.to_canonical_json()?);
    Ok(RehomedRepository {
        refs: next_refs,
        snapshots: output.into_values().collect(),
        revision,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ObjectDescriptor, PROJECT_SUMMARY_MEDIA_TYPE, ProjectSummaryReference, ProtocolVersion,
        RESEARCH_LINEAGE_FORMAT, RESEARCH_LINEAGE_MEDIA_TYPE, RepositoryRef,
        ResearchLineageReference,
    };
    fn corpus(name: &str) -> String {
        let corpus: serde_json::Value = serde_json::from_str(include_str!(
            "../../../docs/reference/discovery/v1/conformance.json"
        ))
        .unwrap();
        corpus["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == name)
            .unwrap()["json"]
            .as_str()
            .unwrap()
            .into()
    }
    fn snapshot(marker: char) -> RepositorySnapshot {
        let limits = DiscoveryLimits::default();
        let mut manifest = DiscoveryManifest::from_json(
            corpus("manifest-minor-compatible-and-canonical").as_bytes(),
            limits,
        )
        .unwrap();
        manifest.immutable_version =
            Sha256Digest(format!("sha256:{}", marker.to_string().repeat(64)));
        let mut summary =
            ProjectSummary::from_json(corpus("summary-canonical").as_bytes(), limits).unwrap();
        summary.repository = manifest.repository.clone();
        summary.immutable_version = manifest.immutable_version.clone();
        summary.package.package_digest = manifest.package.package_digest.clone();
        summary.facts.ontology_composition = None;
        summary.facts.ontology_mode = "none".into();
        summary.facts.components.remove("ontology");
        // Keep the per-kind byte totals consistent with the histogram and the
        // payload total: drop the dropped kind's bytes from both.
        if let Some(bytes) = summary.facts.bytes_by_kind.remove("ontology") {
            summary.facts.payload_bytes -= bytes;
        }
        let summary = summary.to_canonical_json().unwrap();
        let summary_digest = sha256_digest(&summary);
        manifest.summary = Some(ProjectSummaryReference {
            format: crate::PROJECT_SUMMARY_FORMAT.into(),
            summary_digest: summary_digest.clone(),
            object_digest: summary_digest.clone(),
        });
        manifest.objects.push(ObjectDescriptor {
            digest: summary_digest.clone(),
            length: summary.len() as u64,
            media_type: PROJECT_SUMMARY_MEDIA_TYPE.into(),
            locations: vec![format!(
                "https://control.example/opaque/{}",
                summary_digest.0
            )],
        });
        let mut lineage = ResearchLineage::from_json(
            corpus("lineage-fork-two-branches-proposal").as_bytes(),
            limits,
        )
        .unwrap();
        lineage.repository = manifest.repository.clone();
        lineage.immutable_version = manifest.immutable_version.clone();
        lineage.proposals.clear();
        for version in &mut lineage.versions {
            version.package = None;
        }
        let lineage = lineage.to_canonical_json().unwrap();
        let lineage_digest = sha256_digest(&lineage);
        manifest.lineage = Some(ResearchLineageReference {
            format: RESEARCH_LINEAGE_FORMAT.into(),
            lineage_digest: lineage_digest.clone(),
            object_digest: lineage_digest.clone(),
        });
        manifest.objects.push(ObjectDescriptor {
            digest: lineage_digest.clone(),
            length: lineage.len() as u64,
            media_type: RESEARCH_LINEAGE_MEDIA_TYPE.into(),
            locations: vec![format!(
                "https://control.example/opaque/{}",
                lineage_digest.0
            )],
        });
        manifest.objects.sort_by(|a, b| a.digest.cmp(&b.digest));
        manifest.validate(limits).unwrap();
        RepositorySnapshot {
            manifest,
            summary: Some(summary),
            lineage: Some(lineage),
        }
    }
    fn refs(current: &RepositorySnapshot, historical: Option<&RepositorySnapshot>) -> RefSet {
        let reference = |name: &str, snapshot: &RepositorySnapshot| RepositoryRef {
            name: name.into(),
            target: snapshot.manifest.immutable_version.clone(),
            validator: snapshot.manifest.canonical_digest().unwrap(),
        };
        let mut refs = vec![
            reference("feature/claims", current),
            reference("main", current),
        ];
        if let Some(snapshot) = historical {
            refs.push(reference("stable", snapshot));
        }
        RefSet {
            format: crate::DISCOVERY_FORMAT.into(),
            version: ProtocolVersion::CURRENT,
            repository: current.manifest.repository.clone(),
            default_ref: "main".into(),
            refs,
            extensions: BTreeMap::new(),
        }
    }
    fn template() -> ObjectLocationTemplate {
        ObjectLocationTemplate::parse("https://control.example/new/sha256/{hex}").unwrap()
    }
    #[test]
    fn rehomes_complete_current_and_historical_ref_closure_without_native_rewrites() {
        let current = snapshot('a');
        let historical = snapshot('d');
        let refs = refs(&current, Some(&historical));
        let destination = RepositoryIdentity::parse("curate/renamed").unwrap();
        let originals = [current, historical];
        let result = rehome_repository(
            &refs,
            &originals,
            &destination,
            &template(),
            DiscoveryLimits::default(),
        )
        .unwrap();
        assert_eq!(result.refs.repository, destination);
        assert_ne!(
            result.revision,
            sha256_digest(&refs.to_canonical_json().unwrap())
        );
        for old in &originals {
            let new = result
                .snapshots
                .iter()
                .find(|snapshot| {
                    snapshot.manifest.immutable_version == old.manifest.immutable_version
                })
                .unwrap();
            assert_eq!(old.manifest.package, new.manifest.package);
            assert_eq!(
                old.manifest.package_object().unwrap(),
                new.manifest.package_object().unwrap()
            );
            assert_ne!(
                old.manifest.canonical_digest().unwrap(),
                new.manifest.canonical_digest().unwrap()
            );
            let before = ResearchLineage::from_json(
                old.lineage.as_ref().unwrap(),
                DiscoveryLimits::default(),
            )
            .unwrap();
            let after = ResearchLineage::from_json(
                new.lineage.as_ref().unwrap(),
                DiscoveryLimits::default(),
            )
            .unwrap();
            assert_eq!(before.fork, after.fork);
            assert_eq!(before.versions, after.versions);
            assert_eq!(before.branches, after.branches);
            assert_eq!(after.repository, destination);
        }
        let reversed = [originals[1].clone(), originals[0].clone()];
        assert_eq!(
            result,
            rehome_repository(
                &refs,
                &reversed,
                &destination,
                &template(),
                DiscoveryLimits::default()
            )
            .unwrap()
        );
    }
    #[test]
    fn refuses_partial_cross_bound_or_oversized_originals_before_derivation() {
        let current = snapshot('a');
        let historical = snapshot('d');
        let refs = refs(&current, Some(&historical));
        let target = RepositoryIdentity::parse("curate/new").unwrap();
        assert!(
            rehome_repository(
                &refs,
                std::slice::from_ref(&current),
                &target,
                &template(),
                DiscoveryLimits::default()
            )
            .is_err()
        );
        let snapshots = [current, historical];
        for index in 0..2 {
            let mut bad = refs.clone();
            bad.refs[index].target = Sha256Digest(format!("sha256:{}", "f".repeat(64)));
            assert!(
                rehome_repository(
                    &bad,
                    &snapshots,
                    &target,
                    &template(),
                    DiscoveryLimits::default()
                )
                .is_err()
            );
        }
        let mut bad = snapshots.clone();
        bad[0].summary.as_mut().unwrap().push(b' ');
        assert!(
            rehome_repository(
                &refs,
                &bad,
                &target,
                &template(),
                DiscoveryLimits::default()
            )
            .is_err()
        );
        let limits = DiscoveryLimits {
            max_response_bytes: 100,
            ..DiscoveryLimits::default()
        };
        assert_eq!(
            rehome_repository(&refs, &snapshots, &target, &template(), limits)
                .unwrap_err()
                .code,
            DiscoveryErrorCode::LimitExceeded
        );
    }
}
