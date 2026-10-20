use super::tests::{RecordingTransport, Scripted, response};
use super::{ListArgs, TEST_RETRY_POLICY, run_list_with};
use graphforge_discovery::{DiscoveryLimits, DiscoveryManifest};
use std::sync::Mutex;

const FIXTURE_REFS: &[u8] = include_bytes!("../../../../tests/fixtures/hub/generated/v1/refs.json");
const FIXTURE_MANIFEST: &[u8] =
    include_bytes!("../../../../tests/fixtures/hub/generated/v1/manifest.json");

fn list(json: bool) -> (Vec<u8>, Vec<String>) {
    let transport = RecordingTransport {
        inner: Scripted::new(vec![
            response(200, None, FIXTURE_REFS),
            response(200, None, FIXTURE_MANIFEST),
        ]),
        requested: Mutex::new(Vec::new()),
    };
    let mut output = Vec::new();
    run_list_with(
        &transport,
        &ListArgs {
            repository: "openalex/openalex".into(),
            hub: None,
        },
        json,
        &mut output,
        &TEST_RETRY_POLICY,
    )
    .unwrap();
    let requested = transport.requested.lock().unwrap().clone();
    (output, requested)
}

#[test]
fn a_repository_without_lineage_lists_its_refs_and_reads_no_object() {
    let manifest =
        DiscoveryManifest::from_json(FIXTURE_MANIFEST, DiscoveryLimits::default()).unwrap();
    assert!(manifest.lineage.is_none());
    let (output, requested) = list(true);
    assert_eq!(
        requested,
        [
            "https://graphforge.sh/openalex/openalex/.gf/refs",
            "https://graphforge.sh/openalex/openalex/.gf/manifest",
        ],
        "discovery documents only"
    );
    let listing: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(
        listing,
        serde_json::json!({
            "contract": "graphforge-hub-listing/1",
            "repository": "openalex/openalex",
            "default_ref": manifest.default_ref,
            "immutable_version": manifest.immutable_version.0,
            "package_digest": manifest.package.package_digest.0,
            "refs": [{"name": "main", "target": manifest.immutable_version.0}],
            "lineage": null,
        })
    );
    let (text, _) = list(false);
    assert_eq!(
        String::from_utf8(text).unwrap(),
        "openalex/openalex (default ref main)\nRefs: main\n\
         No research lineage; clone downloads the Project package.\n"
    );
}
