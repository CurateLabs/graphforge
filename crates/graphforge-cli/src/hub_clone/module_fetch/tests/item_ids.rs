use super::*;
use graphforge_discovery::{ITEM_DESCRIPTOR_MEDIA_TYPE, ItemDescriptor, ItemDiscoverySnapshot};
use graphforge_hub_publish::sha256_digest;

struct ItemTransport(RecordingTransport);

impl Transport for ItemTransport {
    fn get(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
    ) -> Result<HttpResponse, graphforge_api::GfError> {
        self.0.get(url, range, if_range, limit)
    }

    fn get_discovery(
        &self,
        url: &Url,
        headers: &[(&str, ureq::http::HeaderValue)],
        limit: u64,
    ) -> Result<HttpResponse, graphforge_api::GfError> {
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].0, "accept");
        assert_eq!(headers[0].1, ITEM_DESCRIPTOR_MEDIA_TYPE);
        self.get(url, None, None, limit)
    }
}

fn descriptor() -> ItemDescriptor {
    let limits = DiscoveryLimits::default();
    let refs = RefSet::from_json(FIXTURE_REFS, limits).unwrap();
    let manifest = DiscoveryManifest::from_json(FIXTURE_MANIFEST, limits).unwrap();
    let module = manifest.ontology.as_ref().unwrap().modules[0].identity();
    let id = ItemId::parse_reference(&module.id).unwrap();
    ItemDescriptor {
        format: "graphforge-item/1".into(),
        id: id.as_str().into(),
        kind: id.kind(),
        repository: manifest.repository.clone(),
        module,
        discovery: ItemDiscoverySnapshot {
            refs_revision: Sha256Digest(sha256_digest(&refs.to_canonical_json().unwrap())),
            manifest_validator: manifest.canonical_digest().unwrap(),
        },
    }
}

fn item_args(output: &Path, reference: &str) -> ModuleFetchArgs {
    let mut args = fixture_module_args(output);
    args.repository = reference.into();
    args.ontology_id = None;
    args.version = None;
    args.digest = None;
    args
}

fn transport(descriptor: &[u8], refs: &[u8], manifest: &[u8]) -> ItemTransport {
    ItemTransport(RecordingTransport {
        inner: Scripted::new(vec![
            response(200, None, descriptor),
            response(200, None, refs),
            response(200, None, manifest),
            response(200, None, &fixture_module_package()),
        ]),
        requested: Mutex::new(Vec::new()),
    })
}

#[test]
fn bare_canonical_uri_and_authored_version_inputs_use_the_same_verified_module_path() {
    let descriptor = descriptor();
    let refs_revision = &descriptor.discovery.refs_revision.0;
    let manifest_validator = &descriptor.discovery.manifest_validator.0;
    for reference in [
        descriptor.id.clone(),
        descriptor.id.to_ascii_uppercase(),
        descriptor.module.id.clone(),
        format!("{}/{}", descriptor.id, descriptor.module.version),
    ] {
        let root = tempfile::tempdir().unwrap();
        let args = item_args(&root.path().join("module.json"), &reference);
        let transport = transport(
            &descriptor.to_canonical_json().unwrap(),
            FIXTURE_REFS,
            FIXTURE_MANIFEST,
        );
        let receipt = fetch_module(&transport, &args).unwrap();
        assert_eq!(receipt["module"]["ontology_id"], descriptor.module.id);
        assert_eq!(
            receipt["module"]["authored_version"],
            descriptor.module.version
        );
        assert_eq!(receipt["repository"], "opendata/openalex");
        assert!(args.output.is_file());
        assert_eq!(transport.0.inner.remaining(), 0);
        let requests = transport.0.requested.lock().unwrap();
        assert_eq!(requests.len(), 4);
        let exact = reference == format!("{}/{}", descriptor.id, descriptor.module.version);
        assert_eq!(
            requests[0],
            format!(
                "https://graphforge.sh/{}{}",
                descriptor.id,
                if exact {
                    format!("/{}", descriptor.module.version)
                } else {
                    String::new()
                }
            )
        );
        let refs_url = Url::parse(&requests[1]).unwrap();
        let manifest_url = Url::parse(&requests[2]).unwrap();
        assert_eq!(refs_url.path(), "/opendata/openalex/.gf/refs");
        assert_eq!(
            refs_url.query_pairs().collect::<Vec<_>>(),
            vec![("revision".into(), refs_revision.as_str().into())]
        );
        assert_eq!(manifest_url.path(), "/opendata/openalex/.gf/manifest");
        assert_eq!(
            manifest_url.query_pairs().collect::<Vec<_>>(),
            vec![("validator".into(), manifest_validator.as_str().into())]
        );
    }
}

#[test]
fn exact_version_uses_retained_snapshots_after_current_refs_have_advanced() {
    let descriptor = descriptor();
    let limits = DiscoveryLimits::default();
    let mut current_refs = RefSet::from_json(FIXTURE_REFS, limits).unwrap();
    let mut current_manifest = DiscoveryManifest::from_json(FIXTURE_MANIFEST, limits).unwrap();
    current_manifest.immutable_version = Sha256Digest(format!("sha256:{}", "e".repeat(64)));
    current_manifest.ontology = None;
    for reference in &mut current_refs.refs {
        reference.target = current_manifest.immutable_version.clone();
        reference.validator = current_manifest.canonical_digest().unwrap();
    }
    current_refs.validate_manifest(&current_manifest).unwrap();

    struct Retained {
        inner: ItemTransport,
        current_refs: Vec<u8>,
        current_manifest: Vec<u8>,
        current_reads: std::cell::Cell<usize>,
    }
    impl Transport for Retained {
        fn get(
            &self,
            url: &Url,
            range: Option<u64>,
            if_range: Option<&str>,
            limit: u64,
        ) -> Result<HttpResponse, graphforge_api::GfError> {
            if url.query().is_none() && url.path().ends_with("/.gf/refs") {
                self.current_reads.set(self.current_reads.get() + 1);
                return Ok(response(200, None, &self.current_refs));
            }
            if url.query().is_none() && url.path().ends_with("/.gf/manifest") {
                self.current_reads.set(self.current_reads.get() + 1);
                return Ok(response(200, None, &self.current_manifest));
            }
            self.inner.get(url, range, if_range, limit)
        }
        fn get_discovery(
            &self,
            url: &Url,
            headers: &[(&str, ureq::http::HeaderValue)],
            limit: u64,
        ) -> Result<HttpResponse, graphforge_api::GfError> {
            self.inner.get_discovery(url, headers, limit)
        }
    }
    let transport = Retained {
        inner: transport(
            &descriptor.to_canonical_json().unwrap(),
            FIXTURE_REFS,
            FIXTURE_MANIFEST,
        ),
        current_refs: current_refs.to_canonical_json().unwrap(),
        current_manifest: current_manifest.to_canonical_json().unwrap(),
        current_reads: std::cell::Cell::new(0),
    };
    let root = tempfile::tempdir().unwrap();
    let args = item_args(
        &root.path().join("module.json"),
        &format!("{}/{}", descriptor.id, descriptor.module.version),
    );
    fetch_module(&transport, &args).unwrap();
    assert_eq!(transport.current_reads.get(), 0);
    assert_eq!(transport.inner.0.inner.remaining(), 0);
}

#[test]
fn descriptor_or_snapshot_mismatches_fail_before_package_download_or_output() {
    for change in [
        Box::new(|d: &mut ItemDescriptor| d.module.version = "wrong-version".into())
            as Box<dyn Fn(&mut ItemDescriptor)>,
        Box::new(|d| d.module.content_digest = Sha256Digest(format!("sha256:{}", "f".repeat(64)))),
        Box::new(|d| d.repository.repository = "other".into()),
        Box::new(|d| {
            d.discovery.refs_revision = Sha256Digest(format!("sha256:{}", "f".repeat(64)))
        }),
        Box::new(|d| {
            d.discovery.manifest_validator = Sha256Digest(format!("sha256:{}", "f".repeat(64)))
        }),
    ] {
        let mut descriptor = descriptor();
        let root = tempfile::tempdir().unwrap();
        let args = item_args(&root.path().join("module.json"), &descriptor.id);
        change(&mut descriptor);
        let transport = transport(
            &descriptor.to_canonical_json().unwrap(),
            FIXTURE_REFS,
            FIXTURE_MANIFEST,
        );
        let error = fetch_module(&transport, &args).unwrap_err().to_string();
        assert!(error.contains("hub.integrity_failure"), "{error}");
        assert_eq!(
            transport.0.inner.remaining(),
            1,
            "module package was never requested"
        );
        assert!(directory_entries(root.path()).is_empty());
    }
}

#[test]
fn digest_and_authored_version_pins_are_checked_before_discovery_reads() {
    for (version, digest) in [(Some("wrong-version"), None), (None, Some("0".repeat(64)))] {
        let descriptor = descriptor();
        let root = tempfile::tempdir().unwrap();
        let mut args = item_args(&root.path().join("module.json"), &descriptor.id);
        args.version = version.map(str::to_owned);
        args.digest = digest;
        let transport = transport(
            &descriptor.to_canonical_json().unwrap(),
            FIXTURE_REFS,
            FIXTURE_MANIFEST,
        );
        let error = fetch_module(&transport, &args).unwrap_err().to_string();
        assert!(error.contains("hub.integrity_failure"), "{error}");
        assert_eq!(transport.0.requested.lock().unwrap().len(), 1);
        assert!(directory_entries(root.path()).is_empty());
    }
}

#[test]
fn missing_retained_snapshot_never_falls_back_to_current_metadata() {
    let descriptor = descriptor();
    let transport = ItemTransport(RecordingTransport {
        inner: Scripted::new(vec![
            response(200, None, &descriptor.to_canonical_json().unwrap()),
            response(404, None, b"missing"),
        ]),
        requested: Mutex::new(Vec::new()),
    });
    let root = tempfile::tempdir().unwrap();
    let args = item_args(&root.path().join("module.json"), &descriptor.id);
    assert!(fetch_module(&transport, &args).is_err());
    assert_eq!(transport.0.requested.lock().unwrap().len(), 2);
    assert!(directory_entries(root.path()).is_empty());
}

#[test]
fn bridge_kind_and_unknown_prefix_fail_without_network_access() {
    let root = tempfile::tempdir().unwrap();
    for reference in ["brg_7k2m9x4q8r3t5v1w", "abc_0000000000000000"] {
        let args = item_args(&root.path().join("module.json"), reference);
        let transport = transport(b"unused", FIXTURE_REFS, FIXTURE_MANIFEST);
        assert!(
            fetch_module(&transport, &args)
                .unwrap_err()
                .to_string()
                .contains("hub.invalid_identity")
        );
        assert!(transport.0.requested.lock().unwrap().is_empty());
    }
}

#[test]
fn authored_versions_never_normalize_to_a_current_item_request() {
    let root = tempfile::tempdir().unwrap();
    for version in [".", ".."] {
        let descriptor = descriptor();
        let args = item_args(
            &root.path().join("module.json"),
            &format!("{}/{version}", descriptor.id),
        );
        let transport = transport(b"unused", FIXTURE_REFS, FIXTURE_MANIFEST);
        assert!(fetch_module(&transport, &args).is_err());
        assert!(transport.0.requested.lock().unwrap().is_empty());
    }
}

#[test]
fn repository_input_accepts_bare_ontology_id_and_an_optional_digest_pin() {
    let root = tempfile::tempdir().unwrap();
    let mut args = fixture_module_args(&root.path().join("module.json"));
    args.ontology_id = Some(descriptor().id);
    args.digest = None;
    let transport = module_transport(FIXTURE_MANIFEST, &fixture_module_package());
    fetch_module(&transport, &args).unwrap();
    assert_eq!(transport.requested.lock().unwrap().len(), 3);
    assert!(args.output.is_file());
}

#[test]
fn repository_input_requires_selectors_and_digest_resolves_ambiguity() {
    let root = tempfile::tempdir().unwrap();
    let mut args = fixture_module_args(&root.path().join("module.json"));
    args.ontology_id = None;
    let transport = module_transport(FIXTURE_MANIFEST, &fixture_module_package());
    assert!(
        fetch_module(&transport, &args)
            .unwrap_err()
            .to_string()
            .contains("hub.invalid_identity")
    );
    assert!(transport.requested.lock().unwrap().is_empty());
    let manifest = changed_fixture_manifest(|manifest| {
        let mut other = manifest["ontology"]["modules"][0].clone();
        other["content_digest"] = serde_json::json!(format!("sha256:{}", "f".repeat(64)));
        manifest["ontology"]["modules"]
            .as_array_mut()
            .unwrap()
            .push(other);
    });
    let mut args = fixture_module_args(&root.path().join("module.json"));
    args.digest = None;
    let transport = module_transport(&manifest, &fixture_module_package());
    assert!(
        fetch_module(&transport, &args)
            .unwrap_err()
            .to_string()
            .contains("ambiguous")
    );
    assert_eq!(transport.inner.remaining(), 1);
    assert!(directory_entries(root.path()).is_empty());
}

#[test]
fn snapshot_redirects_fail_closed_and_general_url_queries_stay_forbidden() {
    let descriptor = descriptor();
    let mut redirect = response(302, None, b"");
    redirect.location = Some("https://other.example/current".into());
    let transport = ItemTransport(RecordingTransport {
        inner: Scripted::new(vec![
            response(200, None, &descriptor.to_canonical_json().unwrap()),
            redirect,
        ]),
        requested: Mutex::new(Vec::new()),
    });
    let root = tempfile::tempdir().unwrap();
    let args = item_args(&root.path().join("module.json"), &descriptor.id);
    let error = fetch_module(&transport, &args).unwrap_err().to_string();
    assert!(error.contains("hub.integrity_failure"), "{error}");
    assert_eq!(transport.0.requested.lock().unwrap().len(), 2);
    assert!(
        crate::hub_http::validate_url(
            &Url::parse("https://hub.example/a/b/.gf/refs?revision=anything").unwrap()
        )
        .is_err()
    );
}

#[test]
fn real_http_sends_accept_and_exact_selectors_only_on_their_metadata_reads() {
    use crate::hub_clone::auth_tests::{Reply, server};
    let descriptor = descriptor();
    let (transport, server) = server(vec![
        Reply::ok(&descriptor.to_canonical_json().unwrap()),
        Reply::ok(FIXTURE_REFS),
        Reply::ok(FIXTURE_MANIFEST),
        Reply::ok(&fixture_module_package()),
    ]);
    let root = tempfile::tempdir().unwrap();
    let mut args = item_args(&root.path().join("module.json"), &descriptor.id);
    args.hub = Some("https://hub.example".into());
    let result = fetch_module(&transport, &args);
    let requests = server.join().unwrap();
    result.unwrap();
    assert!(requests[0].starts_with(&format!("GET /{} HTTP/1.1", descriptor.id)));
    assert!(
        requests[0]
            .to_ascii_lowercase()
            .contains(&format!("accept: {ITEM_DESCRIPTOR_MEDIA_TYPE}\r\n"))
    );
    assert!(requests[1].starts_with("GET /opendata/openalex/.gf/refs?revision=sha256%3A"));
    assert!(requests[2].starts_with("GET /opendata/openalex/.gf/manifest?validator=sha256%3A"));
    for request in &requests[1..] {
        assert!(!request.contains(ITEM_DESCRIPTOR_MEDIA_TYPE));
        assert!(!request.to_ascii_lowercase().contains("authorization:"));
    }
}

#[test]
fn canonical_ambiguity_is_sanitized_and_stops_before_metadata_packages_or_output() {
    let descriptor = descriptor();
    let root = tempfile::tempdir().unwrap();
    let mut args = item_args(
        &root.path().join("module.json"),
        &format!("{}/{}", descriptor.id, descriptor.module.version),
    );
    args.digest = Some(descriptor.module.content_digest.0.clone());
    let transport = ItemTransport(RecordingTransport {
        inner: Scripted::new(vec![
            response(409, None, br#"{"code":"ambiguous_identity"}"#),
            response(200, None, FIXTURE_REFS),
            response(200, None, FIXTURE_MANIFEST),
            response(200, None, &fixture_module_package()),
        ]),
        requested: Mutex::new(Vec::new()),
    });
    let mut output = Vec::new();
    let error = run_module_fetch_with(&transport, &args, true, &mut output, &TEST_RETRY_POLICY)
        .unwrap_err();
    let mut stderr = Vec::new();
    crate::write_error(&error, true, &mut stderr, &[]).unwrap();
    let document: serde_json::Value = serde_json::from_slice(&stderr).unwrap();
    assert_eq!(document["error"]["code"], "GF_VALIDATION");
    assert_eq!(
        document["error"]["details"]["semantic_code"],
        "hub.ambiguous_identity"
    );
    assert_eq!(
        crate::error_exit_code(&crate::CliRuntimeError::Core(error)),
        2
    );
    assert!(output.is_empty());
    assert!(directory_entries(root.path()).is_empty());
    assert_eq!(transport.0.requested.lock().unwrap().len(), 1);
    assert_eq!(
        transport.0.inner.remaining(),
        3,
        "no retained metadata or package request, even with --digest"
    );
}

#[test]
fn untrusted_conflict_bodies_never_become_ambiguity_or_leak_details() {
    let descriptor = descriptor();
    for body in [
        br#"{"code":"missing_object"}"#.to_vec(),
        br#"{"code":"ambiguous_identity","owners":["SECRET"]}"#.to_vec(),
        br#"{"code":"ambiguous_identity","code":"ambiguous_identity"}"#.to_vec(),
        b"SECRET proxy failure".to_vec(),
        vec![b'S'; 1025],
    ] {
        let root = tempfile::tempdir().unwrap();
        let args = item_args(&root.path().join("module.json"), &descriptor.id);
        let transport = ItemTransport(RecordingTransport {
            inner: Scripted::new(vec![response(409, None, &body)]),
            requested: Mutex::new(Vec::new()),
        });
        let mut output = Vec::new();
        let error = run_module_fetch_with(&transport, &args, true, &mut output, &TEST_RETRY_POLICY)
            .unwrap_err()
            .to_string();
        assert!(!error.contains("hub.ambiguous_identity"));
        assert!(!error.contains("SECRET"));
        assert!(!error.contains("SSSS"));
        assert!(
            error.contains("hub.malformed_response") || error.contains("hub.limit_exceeded"),
            "{error}"
        );
        assert_eq!(transport.0.requested.lock().unwrap().len(), 1);
        assert!(output.is_empty());
        assert!(directory_entries(root.path()).is_empty());
    }
}

#[test]
fn canonical_ambiguity_is_recognized_through_the_real_http_read_transport() {
    use crate::hub_clone::status_tests::{Routed, StatusServer};
    struct ItemRouted(Routed);
    impl Transport for ItemRouted {
        fn get(
            &self,
            url: &Url,
            range: Option<u64>,
            if_range: Option<&str>,
            limit: u64,
        ) -> Result<HttpResponse, graphforge_api::GfError> {
            self.0.get(url, range, if_range, limit)
        }
        fn get_discovery(
            &self,
            url: &Url,
            _headers: &[(&str, ureq::http::HeaderValue)],
            limit: u64,
        ) -> Result<HttpResponse, graphforge_api::GfError> {
            self.0.get(url, None, None, limit)
        }
    }
    let server = StatusServer::start(vec![(
        409,
        "Content-Type: application/json\r\n".into(),
        br#"{"code":"ambiguous_identity"}"#.to_vec(),
    )]);
    let descriptor = descriptor();
    let root = tempfile::tempdir().unwrap();
    let mut args = item_args(&root.path().join("module.json"), &descriptor.id);
    args.hub = Some("https://hub.test".into());
    let mut output = Vec::new();
    let error = run_module_fetch_with(
        &ItemRouted(Routed::new(server.address)),
        &args,
        true,
        &mut output,
        &TEST_RETRY_POLICY,
    )
    .unwrap_err();
    assert!(error.to_string().contains("hub.ambiguous_identity"));
    assert!(output.is_empty());
    assert!(directory_entries(root.path()).is_empty());
    assert_eq!(server.paths(), vec![format!("/{}", descriptor.id)]);
}
