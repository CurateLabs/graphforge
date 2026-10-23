//! Item resolution binds actual retained discovery snapshots, not current refs.

use graphforge_discovery::{
    DiscoveryErrorCode, DiscoveryLimits, DiscoveryManifest, ItemDescriptor, ItemDiscoverySnapshot,
    ItemId, RefSet, Sha256Digest,
};
use sha2::{Digest, Sha256};

fn revision(refs: &RefSet) -> Sha256Digest {
    let hex: String = Sha256::digest(refs.to_canonical_json().unwrap())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Sha256Digest(format!("sha256:{hex}"))
}

fn fixture() -> (ItemDescriptor, RefSet, DiscoveryManifest) {
    let limits = DiscoveryLimits::default();
    let refs = RefSet::from_json(
        include_bytes!("../../../tests/fixtures/hub/generated/v1/refs.json"),
        limits,
    )
    .unwrap();
    let manifest = DiscoveryManifest::from_json(
        include_bytes!("../../../tests/fixtures/hub/generated/v1/manifest.json"),
        limits,
    )
    .unwrap();
    let module = manifest.ontology.as_ref().unwrap().modules[0].identity();
    let id = ItemId::parse_reference(&module.id).unwrap();
    let descriptor = ItemDescriptor {
        format: "graphforge-item/1".into(),
        id: id.as_str().into(),
        kind: id.kind(),
        repository: manifest.repository.clone(),
        module,
        discovery: ItemDiscoverySnapshot {
            refs_revision: revision(&refs),
            manifest_validator: manifest.canonical_digest().unwrap(),
        },
    };
    (descriptor, refs, manifest)
}

fn assert_integrity(
    descriptor: &ItemDescriptor,
    refs: &RefSet,
    manifest: &DiscoveryManifest,
    field: &str,
) {
    let error = descriptor.bind_snapshot(refs, manifest).unwrap_err();
    assert_eq!(error.code, DiscoveryErrorCode::IntegrityFailure);
    assert_eq!(error.field, Some(field));
}

#[test]
fn retained_snapshot_stays_verifiable_after_every_current_ref_advances() {
    let (descriptor, retained_refs, retained_manifest) = fixture();
    descriptor
        .bind_snapshot(&retained_refs, &retained_manifest)
        .unwrap();
    let mut current_refs = retained_refs.clone();
    let mut current_manifest = retained_manifest.clone();
    current_manifest.immutable_version = Sha256Digest(format!("sha256:{}", "e".repeat(64)));
    let validator = current_manifest.canonical_digest().unwrap();
    for reference in &mut current_refs.refs {
        reference.target = current_manifest.immutable_version.clone();
        reference.validator = validator.clone();
    }
    current_refs.validate_manifest(&current_manifest).unwrap();
    assert_integrity(
        &descriptor,
        &current_refs,
        &current_manifest,
        "discovery.refs_revision",
    );
    descriptor
        .bind_snapshot(&retained_refs, &retained_manifest)
        .unwrap();
}

#[test]
fn both_canonical_hashes_are_required_even_when_repository_and_targets_match() {
    let (descriptor, refs, manifest) = fixture();
    let mut changed_refs = refs.clone();
    changed_refs
        .extensions
        .insert("x-changed".into(), serde_json::json!(true));
    assert_integrity(
        &descriptor,
        &changed_refs,
        &manifest,
        "discovery.refs_revision",
    );
    let mut changed_manifest = manifest.clone();
    changed_manifest
        .extensions
        .insert("x-changed".into(), serde_json::json!(true));
    assert_integrity(
        &descriptor,
        &refs,
        &changed_manifest,
        "discovery.manifest_validator",
    );
}

#[test]
fn resolved_ref_must_list_both_the_manifest_validator_and_immutable_target() {
    let (mut descriptor, mut refs, manifest) = fixture();
    refs.refs
        .iter_mut()
        .find(|r| r.name == manifest.resolved_ref)
        .unwrap()
        .validator = Sha256Digest(format!("sha256:{}", "f".repeat(64)));
    descriptor.discovery.refs_revision = revision(&refs);
    assert_integrity(
        &descriptor,
        &refs,
        &manifest,
        "discovery.manifest_validator",
    );
    let (mut descriptor, mut refs, manifest) = fixture();
    refs.refs
        .iter_mut()
        .find(|r| r.name == manifest.resolved_ref)
        .unwrap()
        .target = Sha256Digest(format!("sha256:{}", "f".repeat(64)));
    descriptor.discovery.refs_revision = revision(&refs);
    assert_integrity(&descriptor, &refs, &manifest, "immutable_version");
}

#[test]
fn exact_module_tuple_must_be_present_in_the_bound_manifest() {
    let (mut descriptor, refs, manifest) = fixture();
    descriptor.module.version = "absent-version".into();
    assert_integrity(&descriptor, &refs, &manifest, "module");
    descriptor.module.version = manifest.ontology.as_ref().unwrap().modules[0]
        .version
        .clone();
    descriptor.module.content_digest = Sha256Digest(format!("sha256:{}", "d".repeat(64)));
    assert_integrity(&descriptor, &refs, &manifest, "module");
}

#[test]
fn repository_moves_require_new_pins_for_the_actual_rehomed_documents() {
    let (mut descriptor, refs, manifest) = fixture();
    descriptor.repository.repository = "renamed".into();
    assert_integrity(&descriptor, &refs, &manifest, "repository");
    let mut rehomed_refs = refs.clone();
    let mut rehomed_manifest = manifest.clone();
    rehomed_refs.repository = descriptor.repository.clone();
    rehomed_manifest.repository = descriptor.repository.clone();
    let validator = rehomed_manifest.canonical_digest().unwrap();
    for reference in &mut rehomed_refs.refs {
        reference.validator = validator.clone();
    }
    assert_integrity(
        &descriptor,
        &rehomed_refs,
        &rehomed_manifest,
        "discovery.refs_revision",
    );
    descriptor.discovery = ItemDiscoverySnapshot {
        refs_revision: revision(&rehomed_refs),
        manifest_validator: validator,
    };
    descriptor
        .bind_snapshot(&rehomed_refs, &rehomed_manifest)
        .unwrap();
}
