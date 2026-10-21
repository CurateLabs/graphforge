use super::*;

/// Refs, manifest, and lineage object for `repository` that advertise
/// `lineage_bytes` with `lineage_digest` as its research lineage and are
/// otherwise consistent with the published snapshot, keyed by request path.
pub(super) fn serve_lineage(
    hub: &LoopbackHub,
    repository: &str,
    lineage_bytes: Vec<u8>,
    lineage_digest: Sha256Digest,
) -> BTreeMap<String, Vec<u8>> {
    let (mut manifest, _) = served_lineage(hub, repository);
    let lineage_object = digest_bytes(&lineage_bytes);
    let old_object = manifest.lineage.as_ref().unwrap().object_digest.clone();
    manifest
        .objects
        .retain(|object| object.digest != old_object);
    manifest.objects.push(object_descriptor(
        lineage_object.clone(),
        lineage_bytes.len() as u64,
        RESEARCH_LINEAGE_MEDIA_TYPE,
        format!(
            "{}/.gf/objects/{lineage_object}",
            repository_url(repository)
        ),
    ));
    manifest
        .objects
        .sort_by(|left, right| left.digest.0.cmp(&right.digest.0));
    manifest.lineage = Some(ResearchLineageReference {
        format: RESEARCH_LINEAGE_FORMAT.into(),
        lineage_digest,
        object_digest: Sha256Digest(lineage_object.clone()),
    });
    let manifest_bytes = manifest.to_canonical_json().unwrap();
    let mut refs = RefSet::from_json(
        &refs_document(hub, repository).body,
        DiscoveryLimits::default(),
    )
    .unwrap();
    for item in &mut refs.refs {
        item.validator = Sha256Digest(digest_bytes(&manifest_bytes));
    }
    BTreeMap::from([
        (
            format!("/{repository}/.gf/refs"),
            refs.to_canonical_json().unwrap(),
        ),
        (format!("/{repository}/.gf/manifest"), manifest_bytes),
        (
            format!("/{repository}/.gf/objects/{lineage_object}"),
            lineage_bytes,
        ),
    ])
}

#[test]
fn clone_without_a_selection_follows_the_default_ref_to_its_branch_head() {
    let history = history();
    let hub = LoopbackHub::start();
    publish_ok(&hub, &history.graph, &on_ref("curate/claims", "main"));
    let (manifest, lineage) = served_lineage(&hub, "curate/claims");
    assert_eq!(manifest.default_ref, "main");
    let listed = lineage.version(&history.head.to_string()).unwrap();

    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("claims");
    let before = hub.log().len();
    let cloned = clone_into(&hub, "curate/claims", &destination, None, None);
    assert_eq!(cloned["resolved_ref"], "main");
    assert_eq!(cloned["research_branch_uuid"], history.branch.to_string());
    assert_eq!(cloned["research_version_uuid"], history.head.to_string());
    assert_eq!(cloned["research_version_kind"], "complete");
    assert_eq!(
        cloned["research_version_identity"],
        listed.identity_digest.0
    );
    assert_eq!(
        cloned["package_digest"],
        listed.package.as_ref().unwrap().package_digest.0
    );
    // The Branch head's package, never the component-selective Project
    // package that import refuses.
    let fetched: Vec<_> = hub.log()[before..]
        .iter()
        .map(|entry| entry.path.clone())
        .collect();
    let head_object = &listed.package.as_ref().unwrap().object_digest.0;
    assert!(
        fetched
            .iter()
            .any(|path| path.ends_with(head_object.as_str()))
    );
    assert!(
        !fetched
            .iter()
            .any(|path| path.ends_with(manifest.package.object_digest.0.as_str())),
        "{fetched:?}"
    );
    let reopened = GraphForge::new(destination.to_str()).unwrap();
    assert_eq!(
        hex_digest(&reference(&reopened, history.head).identity_sha256),
        listed.identity_digest
    );

    // An explicit Version reports its Branch and identity but no ref.
    let by_version = clone_into(
        &hub,
        "curate/claims",
        &root.path().join("by-version"),
        None,
        Some(history.head),
    );
    assert!(by_version.get("resolved_ref").is_none(), "{by_version}");
    assert_eq!(
        by_version["research_branch_uuid"],
        history.branch.to_string()
    );
    assert_eq!(
        by_version["research_version_identity"],
        listed.identity_digest.0
    );
}

// ------------------------------------------------------------ listing

/// `gf hub list` of `repository` through the loopback Hub: JSON or human text.
fn list(hub: &LoopbackHub, repository: &str, json: bool) -> Result<Vec<u8>, GfError> {
    let mut output = Vec::new();
    crate::hub_clone::repository_listing::run_list_with(
        &hub.transport(),
        &crate::hub_clone::repository_listing::ListArgs {
            repository: repository_url(repository),
            hub: None,
        },
        json,
        &mut output,
        &crate::hub_http::RETRY_POLICY,
    )?;
    Ok(output)
}

fn listed(hub: &LoopbackHub, repository: &str) -> serde_json::Value {
    serde_json::from_slice(&list(hub, repository, true).unwrap()).unwrap()
}

/// A selection-free `gf clone` of `repository` that must fail; its error.
fn clone_error(hub: &LoopbackHub, repository: &str, destination: &Path) -> GfError {
    run_clone_with(
        &hub.transport(),
        CloneArgs {
            hub: None,
            token_env: None,
            protection_bypass_env: None,
            repository: repository_url(repository),
            destination: Some(destination.to_path_buf()),
            telemetry_endpoint: None,
            git_ref: None,
            version_uuid: None,
        },
        true,
        &mut Vec::new(),
    )
    .unwrap_err()
}

#[test]
#[allow(clippy::too_many_lines)]
fn listing_shows_branches_versions_fork_origin_and_proposals() {
    let cancellation = CancellationToken::new();
    let origin = history();
    let hub = LoopbackHub::start();
    publish_ok(&hub, &origin.graph, &on_ref("curate/claims", "main"));
    let fork_project = Uuid::now_v7();
    let fork_dir = origin.root.path().join("fork");
    origin
        .graph
        .fork_research(
            &ForkResearchRequest {
                operation_uuid: Uuid::now_v7(),
                project_uuid: fork_project,
                version_uuid: origin.head,
                projection: None,
                target: fork_dir.clone(),
                actor_uuid: Uuid::now_v7(),
                governance: "Independent local review".into(),
                adopt_selected_ontology: true,
                metadata: WorkspaceResearchMetadata::empty(),
            },
            &cancellation,
        )
        .unwrap();
    let mut fork = GraphForge::new(fork_dir.to_str()).unwrap();
    let (main, main_base) = create_branch(&mut fork, "main", current());
    let main_head = edit(&mut fork, main, 7);
    let (feature, feature_base) = create_branch(
        &mut fork,
        "feature",
        BranchSource::Branch { branch_uuid: main },
    );
    let feature_head = edit(&mut fork, feature, 8);
    let mut fork_args = on_ref("curate/claims-fork", "main");
    fork_args.fork_of = Some(repository_url("curate/claims"));
    publish_ok(&hub, &fork, &fork_args);
    publish_ok(&hub, &fork, &on_ref("curate/claims-fork", "feature"));
    let (manifest, lineage) = served_lineage(&hub, "curate/claims-fork");

    let listing = listed(&hub, "curate/claims-fork");
    assert_eq!(listing["contract"], "graphforge-hub-listing/1");
    assert_eq!(listing["repository"], "curate/claims-fork");
    assert_eq!(listing["default_ref"], "main");
    assert_eq!(listing["immutable_version"], manifest.immutable_version.0);
    assert_eq!(listing["package_digest"], manifest.package.package_digest.0);
    let refs: Vec<_> = listing["refs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["name"].as_str().unwrap())
        .collect();
    assert_eq!(refs, ["feature", "main"]);
    let research = &listing["lineage"];
    assert_eq!(research["project_uuid"], fork_project.to_string());
    let origin_identity = hex_digest(&reference(&origin.graph, origin.head).identity_sha256);
    assert_eq!(
        research["fork"],
        serde_json::json!({
            "repository": "curate/claims",
            "project_uuid": reference(&origin.graph, origin.head).project_uuid.to_string(),
            "version_uuid": origin.head.to_string(),
            "version_identity": origin_identity.0,
        })
    );
    let branches: BTreeMap<_, _> = research["branches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|branch| (branch["ref"].as_str().unwrap().to_owned(), branch.clone()))
        .collect();
    assert_eq!(branches.len(), 2);
    assert_eq!(branches["main"]["branch_uuid"], main.to_string());
    assert_eq!(branches["main"]["head_version_uuid"], main_head.to_string());
    assert_eq!(branches["main"]["base_version_uuid"], main_base.to_string());
    assert_eq!(
        branches["main"]["parent_branch_uuid"],
        serde_json::Value::Null
    );
    assert_eq!(branches["feature"]["branch_uuid"], feature.to_string());
    assert_eq!(
        branches["feature"]["head_version_uuid"],
        feature_head.to_string()
    );
    assert_eq!(
        branches["feature"]["base_version_uuid"],
        feature_base.to_string()
    );
    assert_eq!(branches["feature"]["parent_branch_uuid"], main.to_string());
    let versions = research["versions"].as_array().unwrap();
    assert_eq!(versions.len(), lineage.versions.len());
    for (entry, version) in versions.iter().zip(&lineage.versions) {
        assert_eq!(entry["version_uuid"], version.version_uuid);
        assert_eq!(entry["kind"], version.kind);
        assert_eq!(entry["branch_uuid"], version.branch_uuid);
        assert_eq!(entry["identity_digest"], version.identity_digest.0);
        assert_eq!(
            entry["package_digest"].as_str(),
            version
                .package
                .as_ref()
                .map(|package| package.package_digest.0.as_str())
        );
    }
    for head in [main_head, feature_head] {
        let entry = versions
            .iter()
            .find(|entry| entry["version_uuid"] == head.to_string())
            .unwrap();
        assert_eq!(entry["kind"], "complete");
        assert!(entry["package_digest"].is_string(), "{entry}");
        assert_eq!(entry["source_version_uuid"], serde_json::Value::Null);
    }
    // `gf publish` publishes no Proposals.
    assert_eq!(research["proposals"], serde_json::json!([]));

    // A Hub that also lists a Proposal of the feature head: its payload is a
    // projection of that head.
    let mut proposed = lineage.clone();
    let payload = Uuid::now_v7();
    let proposal = Uuid::now_v7();
    let head_entry = proposed.version(&feature_head.to_string()).unwrap().clone();
    let package = head_entry.package.clone().unwrap();
    proposed
        .versions
        .push(graphforge_discovery::LineageVersion {
            version_uuid: payload.to_string(),
            parents: Vec::new(),
            identity_digest: Sha256Digest(sha256_digest(b"payload projection")),
            kind: "projection".into(),
            branch_uuid: feature.to_string(),
            source_version_uuid: Some(feature_head.to_string()),
            author: None,
            committer: None,
            package: Some(package.clone()),
        });
    proposed
        .versions
        .sort_by(|left, right| left.version_uuid.cmp(&right.version_uuid));
    proposed
        .proposals
        .push(graphforge_discovery::LineageProposal {
            proposal_uuid: proposal.to_string(),
            source_branch_uuid: feature.to_string(),
            source_version_uuid: feature_head.to_string(),
            payload_version_uuid: payload.to_string(),
            package: package.clone(),
        });
    hub.inject(Fault::Serve(serve_lineage(
        &hub,
        "curate/claims-fork",
        proposed.to_canonical_json().unwrap(),
        proposed.canonical_digest().unwrap(),
    )));
    let listing = listed(&hub, "curate/claims-fork");
    assert!(
        hub.fault.lock().unwrap().is_none(),
        "the served documents were read"
    );
    assert_eq!(
        listing["lineage"]["proposals"],
        serde_json::json!([{
            "proposal_uuid": proposal.to_string(),
            "source_branch_uuid": feature.to_string(),
            "source_version_uuid": feature_head.to_string(),
            "payload_version_uuid": payload.to_string(),
            "package_digest": package.package_digest.0,
        }])
    );
    let projection = listing["lineage"]["versions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["version_uuid"] == payload.to_string())
        .unwrap()
        .clone();
    assert_eq!(projection["kind"], "projection");
    assert_eq!(projection["source_version_uuid"], feature_head.to_string());

    // The human listing names Branches by ref.
    hub.inject(Fault::Serve(serve_lineage(
        &hub,
        "curate/claims-fork",
        proposed.to_canonical_json().unwrap(),
        proposed.canonical_digest().unwrap(),
    )));
    let text = String::from_utf8(list(&hub, "curate/claims-fork", false).unwrap()).unwrap();
    for line in [
        "curate/claims-fork (default ref main)".to_owned(),
        "Refs: feature, main".to_owned(),
        format!("Forked from curate/claims Version {}", origin.head),
        format!("  feature  head {feature_head}  base {feature_base}  parent main"),
        format!("  {payload}  projection  feature  of {feature_head}  package"),
        format!("  {proposal}  from feature Version {feature_head}  payload {payload}"),
    ] {
        assert!(
            text.lines().any(|candidate| candidate == line),
            "{line}\n{text}"
        );
    }
}

#[test]
fn listing_fails_exactly_as_clone_on_a_tampered_or_future_lineage() {
    let history = history();
    let hub = LoopbackHub::start();
    publish_ok(&hub, &history.graph, &on_ref("curate/claims", "main"));
    let (manifest, lineage) = served_lineage(&hub, "curate/claims");
    let lineage_path = format!(
        "/curate/claims/.gf/objects/{}",
        manifest.lineage.as_ref().unwrap().object_digest.0
    );
    // Bytes that disagree with the advertised lineage object digest.
    let mut tampered = lineage.to_canonical_json().unwrap();
    let last = tampered.len() - 2;
    tampered[last] ^= 0x01;
    // A digest-consistent lineage bound to another repository snapshot.
    let mut rebound = lineage.clone();
    rebound.immutable_version = Sha256Digest(sha256_digest(b"another snapshot"));
    // A digest-consistent lineage that requires an unknown capability major.
    let mut future: serde_json::Value =
        serde_json::from_slice(&lineage.to_canonical_json().unwrap()).unwrap();
    future["requirements"] = serde_json::json!([{"capability":"research-lineage","major":3}]);
    let future = serde_json::to_vec(&future).unwrap();
    let cases = [
        (
            "hub.integrity_failure",
            BTreeMap::from([(lineage_path, tampered)]),
        ),
        (
            "hub.integrity_failure",
            serve_lineage(
                &hub,
                "curate/claims",
                rebound.to_canonical_json().unwrap(),
                rebound.canonical_digest().unwrap(),
            ),
        ),
        (
            "hub.unsupported_future",
            serve_lineage(
                &hub,
                "curate/claims",
                future,
                manifest.lineage.as_ref().unwrap().lineage_digest.clone(),
            ),
        ),
    ];
    let root = tempfile::tempdir().unwrap();
    for (index, (expected, documents)) in cases.into_iter().enumerate() {
        hub.inject(Fault::Serve(documents.clone()));
        let destination = root.path().join(format!("clone-{index}"));
        let cloned = clone_error(&hub, "curate/claims", &destination);
        assert!(
            hub.fault.lock().unwrap().is_none(),
            "clone read every served document"
        );
        assert!(!destination.exists());
        hub.inject(Fault::Serve(documents));
        let listed = list(&hub, "curate/claims", true).unwrap_err();
        assert!(
            hub.fault.lock().unwrap().is_none(),
            "listing read every served document"
        );
        assert_eq!(error_code(&cloned), expected, "{cloned}");
        assert_eq!(error_code(&listed), expected, "{listed}");
        assert_eq!(listed.to_string(), cloned.to_string());
    }
}
