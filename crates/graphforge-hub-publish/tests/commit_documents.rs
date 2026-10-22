//! Receipt validation and the commit-time manifest check, on the openalex Hub
//! fixture (the same inputs the WebAssembly package replays).

use graphforge_discovery::{DiscoveryLimits, DiscoveryManifest};
use graphforge_hub_publish::{
    HubPublishErrorCode, ObjectDeclaration, PublishCapabilities, PublishReceipt,
    check_commit_manifest,
};
use serde_json::{Value, json};

const MANIFEST: &str = include_str!("../../../tests/fixtures/hub/generated/v1/manifest.json");

fn capabilities() -> PublishCapabilities {
    let document = json!({
        "format": "graphforge-hub-publish/1",
        "repository": {"owner": "openalex", "repository": "openalex"},
        "requirements": [
            {"capability": "publish-session", "major": 1},
            {"capability": "publish-negotiation", "major": 1},
        ],
        "capabilities": [],
        "authorization": {
            "device_authorization_endpoint": "https://data.graphforge.sh/.gf/oauth/device_authorization",
            "token_endpoint": "https://data.graphforge.sh/.gf/oauth/token",
            "scope": "publish:openalex/openalex",
        },
        "object_location_template": "https://data.graphforge.sh/objects/sha256/{hex}",
        "limits": {
            "max_chunk_bytes": 8_388_608,
            "max_document_bytes": 4_194_304,
            "max_object_bytes": 67_108_864,
            "max_objects": 1024,
            "max_session_bytes": 268_435_456,
        },
    });
    PublishCapabilities::from_json(document.to_string().as_bytes()).unwrap()
}

fn inventory(manifest: &DiscoveryManifest) -> Vec<ObjectDeclaration> {
    manifest
        .objects
        .iter()
        .map(|object| ObjectDeclaration {
            digest: object.digest.0.clone(),
            length: object.length,
            media_type: object.media_type.clone(),
        })
        .collect()
}

#[test]
fn commit_manifest_must_name_the_repository_the_inventory_and_a_resolved_ref() {
    let manifest =
        DiscoveryManifest::from_json(MANIFEST.as_bytes(), DiscoveryLimits::default()).unwrap();
    let capabilities = capabilities();
    let repository = manifest.repository.clone();
    let inventory = inventory(&manifest);
    let refs = vec![manifest.resolved_ref.clone()];
    check_commit_manifest(&manifest, &repository, &capabilities, &inventory, &refs).unwrap();

    let other = graphforge_discovery::RepositoryIdentity::parse("openalex/other").unwrap();
    let mut short = inventory.clone();
    short.pop();
    let mut moved = inventory.clone();
    moved[0].length += 1;
    for (repository, inventory, refs) in [
        (&other, &inventory, &refs),
        (&repository, &short, &refs),
        (&repository, &moved, &refs),
        (&repository, &inventory, &vec!["other".to_string()]),
    ] {
        let error = check_commit_manifest(&manifest, repository, &capabilities, inventory, refs)
            .unwrap_err();
        assert_eq!(error.code, HubPublishErrorCode::InvalidInput);
    }
}

#[test]
fn receipts_are_validated_like_every_other_publish_document() {
    let digest = |c: char| format!("sha256:{}", c.to_string().repeat(64));
    let receipt: Value = json!({
        "format": "graphforge-hub-publish/1",
        "operation_uuid": "00000000-0000-4000-8000-000000000001",
        "request_commitment": digest('a'),
        "repository": {"owner": "openalex", "repository": "openalex"},
        "manifest_validator": digest('b'),
        "refs": ["main"],
        "previous_revision": null,
        "revision": digest('c'),
    });
    PublishReceipt::from_json(receipt.to_string().as_bytes()).unwrap();

    // The member is required even when `null`; an explicit `null` is accepted.
    let mut omitted = receipt.clone();
    omitted.as_object_mut().unwrap().remove("previous_revision");
    let error = PublishReceipt::from_json(omitted.to_string().as_bytes()).unwrap_err();
    assert_eq!(error.code, HubPublishErrorCode::InvalidInput);
    assert_eq!(error.detail(), "receipt is malformed");

    let mut bad_digest = receipt.clone();
    bad_digest["revision"] = json!("sha256:ABC");
    let mut future = receipt.clone();
    future["format"] = json!("graphforge-hub-publish/2");
    let mut unordered = receipt;
    unordered["refs"] = json!(["main", "alpha"]);
    for (document, code) in [
        (bad_digest, HubPublishErrorCode::InvalidInput),
        (future, HubPublishErrorCode::UnsupportedFuture),
        (unordered, HubPublishErrorCode::InvalidInput),
    ] {
        let error = PublishReceipt::from_json(document.to_string().as_bytes()).unwrap_err();
        assert_eq!(error.code, code);
    }
}
