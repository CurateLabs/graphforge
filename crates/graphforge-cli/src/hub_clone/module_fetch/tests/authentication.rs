//! Real HTTP evidence for opt-in protected ontology discovery.
use super::*;
use crate::hub_clone::auth_tests::{Reply, server};

const BYPASS: &str = "ontology-preview-test-secret";

fn arguments(output: &Path, item: bool) -> ModuleFetchArgs {
    let mut args = if item {
        item_ids::item_args(output, &item_ids::descriptor().id)
    } else {
        fixture_module_args(output)
    };
    args.hub = Some("https://hub.example".into());
    args.protection_bypass_env = Some("ONTOLOGY_PREVIEW_SECRET".into());
    args
}

fn replies(item: bool) -> Vec<Reply> {
    let mut replies = Vec::new();
    if item {
        replies.push(Reply::ok(
            &item_ids::descriptor().to_canonical_json().unwrap(),
        ));
    }
    replies.extend([
        Reply::ok(FIXTURE_REFS),
        Reply::ok(FIXTURE_MANIFEST),
        Reply::ok(&fixture_module_package()),
    ]);
    replies
}

fn lookup(name: &str) -> Result<String, std::env::VarError> {
    assert_eq!(name, "ONTOLOGY_PREVIEW_SECRET");
    Ok(BYPASS.into())
}

fn assert_headers(request: &str, bypass: bool, accept: bool) {
    let request = request.to_ascii_lowercase();
    assert!(!request.contains("\r\nauthorization:"));
    assert_eq!(request.contains("\r\nx-vercel-protection-bypass:"), bypass);
    assert_eq!(request.contains(BYPASS), bypass);
    assert_eq!(
        request.contains(graphforge_discovery::ITEM_DESCRIPTOR_MEDIA_TYPE),
        accept
    );
}

#[test]
fn protected_repository_and_item_reads_authenticate_metadata_but_not_package_downloads() {
    for item in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let args = arguments(&root.path().join("module.json"), item);
        let (transport, server) = server(replies(item));
        let result = resolve_module_with_lookup(&transport, &args, &TEST_RETRY_POLICY, lookup);
        let requests = server.join().unwrap();
        let (resolved, moved) = result.unwrap();
        assert!(moved.is_none());
        assert_eq!(
            resolved.module().ontology_id,
            item_ids::descriptor().module.id
        );
        assert_eq!(resolved.repository().canonical_name(), "opendata/openalex");
        assert!(!resolved.document().is_empty());
        assert!(
            directory_entries(root.path()).is_empty(),
            "no staging survives resolution"
        );
        assert_eq!(requests.len(), if item { 4 } else { 3 });
        for (index, request) in requests.iter().enumerate() {
            assert_headers(request, index + 1 < requests.len(), item && index == 0);
        }
        if item {
            assert!(requests[1].contains("/.gf/refs?revision=sha256%3A"));
            assert!(requests[2].contains("/.gf/manifest?validator=sha256%3A"));
        }
    }
}

#[test]
fn protected_ontology_reads_strip_bypass_on_same_and_foreign_origin_redirects() {
    for item in [false, true] {
        for origin in ["https://hub.example", "https://other.example"] {
            let root = tempfile::tempdir().unwrap();
            let args = arguments(&root.path().join("module.json"), item);
            let path = if item {
                format!("/{}", item_ids::descriptor().id)
            } else {
                "/opendata/openalex/.gf/refs".into()
            };
            let mut responses = replies(item);
            responses.insert(0, Reply::redirect(302, &format!("{origin}{path}"), b""));
            let (transport, server) = server(responses);
            let result = resolve_module_with_lookup(&transport, &args, &TEST_RETRY_POLICY, lookup);
            let requests = server.join().unwrap();
            result.unwrap();
            assert_eq!(requests.len(), if item { 5 } else { 4 });
            for (index, request) in requests.iter().enumerate() {
                let authenticated = index != 1 && index + 1 < requests.len();
                assert_headers(request, authenticated, item && index <= 1);
            }
            assert!(directory_entries(root.path()).is_empty());
        }
    }
}

#[test]
fn absent_or_invalid_explicit_bypass_stops_before_network_and_never_leaks_its_value() {
    for item in [false, true] {
        for value in [None, Some(""), Some("DO_NOT_LEAK\n")] {
            let root = tempfile::tempdir().unwrap();
            let args = arguments(&root.path().join("module.json"), item);
            let transport = Scripted::new(Vec::new());
            let error = resolve_module_with_lookup(&transport, &args, &TEST_RETRY_POLICY, |_| {
                value
                    .map(str::to_owned)
                    .ok_or(std::env::VarError::NotPresent)
            })
            .err()
            .expect("explicit invalid credential must fail")
            .to_string();
            assert!(error.contains("hub.auth_denied"), "{error}");
            assert!(!error.contains("DO_NOT_LEAK"));
            assert!(directory_entries(root.path()).is_empty());
        }
    }
}

#[test]
fn protected_ontology_move_reauthenticates_only_after_both_old_markers_agree() {
    let root = tempfile::tempdir().unwrap();
    let args = arguments(&root.path().join("module.json"), false);
    let mut manifest =
        DiscoveryManifest::from_json(FIXTURE_MANIFEST, DiscoveryLimits::default()).unwrap();
    let old = manifest.repository.clone();
    let new = RepositoryIdentity::parse("research/renamed").unwrap();
    manifest.repository = new.clone();
    let mut refs = RefSet::from_json(FIXTURE_REFS, DiscoveryLimits::default()).unwrap();
    refs.repository = new.clone();
    for reference in &mut refs.refs {
        reference.validator = manifest.canonical_digest().unwrap();
    }
    let marker = serde_json::to_vec(&graphforge_discovery::RepositoryMoved {
        format: "graphforge-repository-moved/1".into(),
        moved_from: old,
        moved_to: new.clone(),
    })
    .unwrap();
    let (transport, server) = server(vec![
        Reply::redirect(
            308,
            "https://hub.example/research/renamed/.gf/refs",
            &marker,
        ),
        Reply::redirect(
            308,
            "https://hub.example/research/renamed/.gf/manifest",
            &marker,
        ),
        Reply::ok(&refs.to_canonical_json().unwrap()),
        Reply::ok(&manifest.to_canonical_json().unwrap()),
        Reply::ok(&fixture_module_package()),
    ]);
    let result = resolve_module_with_lookup(&transport, &args, &TEST_RETRY_POLICY, lookup);
    let requests = server.join().unwrap();
    let (resolved, moved) = result.unwrap();
    assert_eq!(resolved.repository(), &new);
    assert_eq!(moved.unwrap().moved_to, new);
    assert_eq!(requests.len(), 5);
    for (index, request) in requests.iter().enumerate() {
        assert_headers(request, index < 4, false);
    }
    assert!(requests[0].starts_with("GET /opendata/openalex/.gf/refs "));
    assert!(requests[1].starts_with("GET /opendata/openalex/.gf/manifest "));
    assert!(requests[2].starts_with("GET /research/renamed/.gf/refs "));
    assert!(requests[3].starts_with("GET /research/renamed/.gf/manifest "));
    assert!(directory_entries(root.path()).is_empty());
}
