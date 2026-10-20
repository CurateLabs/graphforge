use super::*;

/// Read `url` over the loopback socket through the product transport.
fn fetch_over_socket(hub: &LoopbackHub, url: &str) -> Vec<u8> {
    let response = hub
        .transport()
        .get(&Url::parse(url).unwrap(), None, None, 1 << 20)
        .unwrap();
    assert_eq!(response.status, 200);
    let mut bytes = Vec::new();
    response.body.take(1 << 20).read_to_end(&mut bytes).unwrap();
    bytes
}

fn location_path(url: &str) -> String {
    Url::parse(url).unwrap().path().to_owned()
}

#[test]
fn a_published_version_declares_its_producing_core_version_without_a_package_fetch() {
    let root = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(root.path().join("project").to_str()).unwrap();
    graph.execute("CREATE (:Item {score:0})").unwrap();
    let (branch, _) = create_branch(&mut graph, "main", current());
    edit(&mut graph, branch, 1);
    let hub = LoopbackHub::start();
    publish_ok(&hub, &graph, &on_ref("curate/claims", "main"));

    // A consumer reads only the discovery documents, over the socket.
    let before = hub.log().len();
    let limits = DiscoveryLimits::default();
    let base = repository_url("curate/claims");
    let refs = RefSet::from_json(
        &fetch_over_socket(&hub, &format!("{base}/.gf/refs")),
        limits,
    )
    .unwrap();
    let manifest = DiscoveryManifest::from_json(
        &fetch_over_socket(&hub, &format!("{base}/.gf/manifest")),
        limits,
    )
    .unwrap();
    refs.validate_manifest(&manifest).unwrap();
    let summary_object = manifest.summary_object().unwrap();
    let summary_bytes = fetch_over_socket(&hub, &summary_object.locations[0]);
    assert_eq!(digest_bytes(&summary_bytes), summary_object.digest.0);
    let summary = ProjectSummary::from_json(&summary_bytes, limits).unwrap();
    manifest.bind_summary(&summary).unwrap();

    // Both documents name the Core that wrote the package: the build's own version.
    assert_eq!(
        manifest.package.producing_core_version,
        graphforge_api::GRAPHFORGE_VERSION
    );
    assert_eq!(
        summary.package.producing_core_version,
        manifest.package.producing_core_version
    );

    // The requests served for that read were exactly the three documents; the
    // Project package (the graph payload) was never requested.
    let package_path = location_path(&manifest.package_object().unwrap().locations[0]);
    let served: Vec<_> = hub.log()[before..]
        .iter()
        .map(|entry| (entry.method.clone(), entry.path.clone()))
        .collect();
    assert_eq!(
        served,
        vec![
            ("GET".to_owned(), location_path(&format!("{base}/.gf/refs"))),
            (
                "GET".to_owned(),
                location_path(&format!("{base}/.gf/manifest"))
            ),
            (
                "GET".to_owned(),
                location_path(&summary_object.locations[0])
            ),
        ]
    );
    assert!(served.iter().all(|(_, path)| *path != package_path));

    // The accounting can see a package fetch: reading the package shows up.
    let before = hub.log().len();
    fetch_over_socket(&hub, &manifest.package_object().unwrap().locations[0]);
    assert!(
        hub.log()[before..]
            .iter()
            .any(|entry| entry.path == package_path)
    );
}
