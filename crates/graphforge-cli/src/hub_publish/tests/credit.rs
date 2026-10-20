//! Published credit through the real publish path and the reference Hub (ADR 0055).
//! Authors and committers recorded in research history are always published; the
//! summary's people list is derived from the lineage, never entered by hand.
use super::*;

pub(super) fn signature(name: &str) -> graphforge_storage::research_versions::ResearchSignature {
    graphforge_storage::research_versions::ResearchSignature {
        name: name.into(),
        email: None,
        orcid: None,
    }
}

pub(super) fn edit_by(graph: &mut GraphForge, branch: Uuid, score: i64, name: &str) -> Uuid {
    edit_credited(graph, branch, score, signature(name), signature(name))
}

pub(super) fn edit_credited(
    graph: &mut GraphForge,
    branch: Uuid,
    score: i64,
    author: graphforge_storage::research_versions::ResearchSignature,
    committer: graphforge_storage::research_versions::ResearchSignature,
) -> Uuid {
    let version_uuid = Uuid::now_v7();
    graph
        .execute_research_branch(
            &ExecuteResearchBranchRequest {
                author: Some(author),
                committer: Some(committer),
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: generation(graph),
                branch_uuid: branch,
                version_uuid,
                created_at: 3,
                query: format!("MATCH (n:Item) SET n.score={score}"),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    version_uuid
}

/// The summary a consumer reads from the Hub's current documents.
pub(super) fn served_summary(hub: &LoopbackHub, repository: &str) -> ProjectSummary {
    let (manifest, lineage) = served_lineage(hub, repository);
    let object = manifest.summary_object().unwrap();
    let bytes = hub.get(&object.locations[0]).body;
    assert_eq!(digest_bytes(&bytes), object.digest.0);
    let summary = ProjectSummary::from_json(&bytes, DiscoveryLimits::default()).unwrap();
    manifest.bind_summary(&summary).unwrap();
    summary.bind_lineage(Some(&lineage)).unwrap();
    summary
}

fn person(summary: &ProjectSummary, name: &str) -> Vec<String> {
    summary
        .people
        .iter()
        .find(|person| person.name == name)
        .unwrap_or_else(|| panic!("{name} is not credited in the summary"))
        .roles
        .clone()
}

fn hex(bytes: &[u8; 32]) -> String {
    hex_digest(bytes).0
}

#[test]
fn second_committers_are_credited_in_the_summary_across_a_fast_forward_round_trip() {
    let mut history = history();
    let alice = edit_credited(
        &mut history.graph,
        history.branch,
        2,
        signature("Alice"),
        signature("Carol"),
    );
    let hub = LoopbackHub::start();
    let arguments = on_ref("curate/claims", "main");
    assert!(!arguments.rewrite && !arguments.prune);
    publish_ok(&hub, &history.graph, &arguments).result.unwrap();

    let first = served_summary(&hub, "curate/claims");
    assert_eq!(person(&first, "Alice"), ["author"]);
    assert_eq!(person(&first, "Carol"), ["committer"]);
    assert_eq!(first.people.len(), 2);
    let (_, first_lineage) = served_lineage(&hub, "curate/claims");
    let published = first_lineage.version(&alice.to_string()).unwrap();
    assert_eq!(published.author.as_ref().unwrap().name, "Alice");
    assert_eq!(published.committer.as_ref().unwrap().name, "Carol");

    let destinations = tempfile::tempdir().unwrap();
    let bob_path = destinations.path().join("bob");
    clone_into(&hub, "curate/claims", &bob_path, Some("main"), None);
    let mut bob = GraphForge::new(bob_path.to_str()).unwrap();
    let bob_version = edit_by(&mut bob, history.branch, 3, "Bob");
    assert_eq!(
        bob.research_version(bob_version).unwrap().parents,
        vec![alice]
    );

    // The descendant update is a fast-forward and needs neither flag.
    let log = hub.log().len();
    publish_ok(&hub, &bob, &arguments).result.unwrap();
    assert!(!puts(&hub.log()[log..]).is_empty());

    let summary = served_summary(&hub, "curate/claims");
    assert_eq!(person(&summary, "Alice"), ["author"]);
    assert_eq!(person(&summary, "Bob"), ["author", "committer"]);
    assert_eq!(person(&summary, "Carol"), ["committer"]);
    assert_eq!(summary.people.len(), 3);

    let (_, lineage) = served_lineage(&hub, "curate/claims");
    assert_eq!(
        lineage.branches[0].head_version_uuid,
        bob_version.to_string()
    );
    assert!(lineage.descends_from(&bob_version.to_string(), &alice.to_string()));
    let registry = bob.research_version_retention().unwrap();
    for version in [alice, bob_version] {
        let entry = lineage.version(&version.to_string()).unwrap();
        assert_eq!(entry.identity_digest.0, hex(&registry.identities[&version]));
        let record = bob.research_version(version).or_else(|_| {
            registry
                .historical_version(version)
                .cloned()
                .ok_or(GfError::Validation("missing".into()))
        });
        let record = record.unwrap();
        assert_eq!(
            entry.parents,
            record
                .parents
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            entry.author.as_ref().map(|a| a.name.clone()),
            record.author.map(|a| a.name)
        );
        assert_eq!(
            entry.committer.as_ref().map(|a| a.name.clone()),
            record.committer.map(|a| a.name)
        );
    }

    let final_path = destinations.path().join("final");
    clone_into(&hub, "curate/claims", &final_path, Some("main"), None);
    let reopened = GraphForge::new(final_path.to_str()).unwrap();
    let query = reopened
        .execute("MATCH (n:Item) RETURN count(n), sum(n.score)")
        .unwrap();
    for (column, expected) in [(0, 1), (1, 3)] {
        assert_eq!(
            query.batches[0]
                .column(column)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            expected
        );
    }
    let reopened_registry = reopened.research_version_retention().unwrap();
    assert_eq!(reopened_registry.heads[&history.branch], bob_version);
    assert_eq!(
        reopened_registry.identities[&bob_version],
        registry.identities[&bob_version]
    );
    assert_eq!(
        reopened
            .research_version(bob_version)
            .unwrap()
            .committer
            .unwrap()
            .name,
        "Bob"
    );
}

#[test]
fn fork_citation_carries_the_origin_versions_credit() {
    let cancellation = CancellationToken::new();
    let mut origin = history();
    let cited = edit_credited(
        &mut origin.graph,
        origin.branch,
        2,
        signature("Alice"),
        signature("Carol"),
    );
    let hub = LoopbackHub::configured(
        ReferenceHubConfig {
            base_url: HUB.into(),
            ..ReferenceHubConfig::default()
        },
        ContextMode::Public,
    );
    publish_ok(&hub, &origin.graph, &on_ref("curate/claims", "main"));
    let fork_dir = origin.root.path().join("fork");
    origin
        .graph
        .fork_research(
            &ForkResearchRequest {
                operation_uuid: Uuid::now_v7(),
                project_uuid: Uuid::now_v7(),
                version_uuid: cited,
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
    let (fork_branch, _) = create_branch(&mut fork, "main", current());
    edit_by(&mut fork, fork_branch, 7, "Dana");
    let mut fork_args = on_ref("curate/claims-fork", "main");
    fork_args.fork_of = Some(repository_url("curate/claims"));
    publish_ok(&hub, &fork, &fork_args).result.unwrap();

    let (_, lineage) = served_lineage(&hub, "curate/claims-fork");
    let citation = lineage.fork.as_ref().expect("Fork origin is published");
    assert_eq!(citation.origin_version_uuid, cited.to_string());
    assert_eq!(citation.origin_author.as_ref().unwrap().name, "Alice");
    assert_eq!(citation.origin_committer.as_ref().unwrap().name, "Carol");
    // The Fork's own people are its own history's credit, not the cited origin's.
    let summary = served_summary(&hub, "curate/claims-fork");
    assert_eq!(person(&summary, "Dana"), ["author", "committer"]);
    assert!(
        summary
            .people
            .iter()
            .all(|p| p.name != "Alice" && p.name != "Carol")
    );
}

fn full_lineage(history: &History, head: Uuid) -> ResearchLineage {
    let digest = |marker: char| Sha256Digest(format!("sha256:{}", marker.to_string().repeat(64)));
    let package = |marker: char| graphforge_discovery::PortablePackageReference {
        format: graphforge_discovery::PORTABLE_V2_FORMAT.into(),
        package_digest: digest(marker),
        object_digest: digest(marker),
    };
    history
        .graph
        .build_research_lineage_for_discovery(
            &BuildResearchLineageRequest {
                repository: RepositoryIdentity::parse("curate/claims").unwrap(),
                immutable_version: digest('a'),
                project_uuid: reference(&history.graph, history.head).project_uuid,
                branch_ref_names: BTreeMap::from([(history.branch, "main".to_owned())]),
                version_packages: BTreeMap::from([(head, package('b'))]),
                fork_origin_repository: None,
                published_proposals: BTreeSet::new(),
            },
            &CancellationToken::new(),
        )
        .unwrap()
}

#[test]
fn republishing_a_pre_credit_lineage_adopts_the_verified_credit_and_keeps_every_identity() {
    let mut history = history();
    let alice = edit_credited(
        &mut history.graph,
        history.branch,
        2,
        signature("Alice"),
        signature("Carol"),
    );
    let local = full_lineage(&history, alice);
    assert_eq!(
        local
            .version(&alice.to_string())
            .unwrap()
            .author
            .as_ref()
            .unwrap()
            .name,
        "Alice"
    );
    // The lineage as a publisher that predates credit display wrote it: same
    // identities and parents, no author/committer on any entry.
    let mut published = local.clone();
    for version in &mut published.versions {
        version.author = None;
        version.committer = None;
    }
    assert!(published.people().is_empty());

    let merged = merge_lineage(Some(&published), local.clone(), false, false).unwrap();
    assert_eq!(merged, local);
    assert_eq!(
        merged
            .versions
            .iter()
            .map(|v| &v.identity_digest)
            .collect::<Vec<_>>(),
        published
            .versions
            .iter()
            .map(|v| &v.identity_digest)
            .collect::<Vec<_>>()
    );
    // The summary's people are derived from the complete merged lineage.
    let people = merged.people();
    assert_eq!(people.len(), 2);
    assert_eq!(people[0].name, "Alice");
    assert_eq!(people[0].roles, ["author"]);
    assert_eq!(people[1].name, "Carol");
    assert_eq!(people[1].roles, ["committer"]);
}

#[test]
fn a_published_credit_that_contradicts_the_local_record_is_refused() {
    let mut history = history();
    let alice = edit_credited(
        &mut history.graph,
        history.branch,
        2,
        signature("Alice"),
        signature("Carol"),
    );
    let local = full_lineage(&history, alice);
    for tamper in [
        |v: &mut graphforge_discovery::LineageVersion| {
            v.author.as_mut().unwrap().name = "Mallory".into();
        },
        |v: &mut graphforge_discovery::LineageVersion| {
            v.committer.as_mut().unwrap().name = "Mallory".into();
        },
    ] {
        let mut published = local.clone();
        tamper(
            published
                .versions
                .iter_mut()
                .find(|v| v.version_uuid == alice.to_string())
                .unwrap(),
        );
        let error = merge_lineage(Some(&published), local.clone(), false, false).unwrap_err();
        assert_eq!(error_code(&error), "hub.publish.integrity_failure");
    }
}

fn origin(
    author: Option<&str>,
    committer: Option<&str>,
) -> graphforge_discovery::LineageForkOrigin {
    let credit = |name: &str| graphforge_discovery::CreditSignature {
        name: name.into(),
        email: None,
        orcid: None,
    };
    graphforge_discovery::LineageForkOrigin {
        origin_repository: RepositoryIdentity::parse("curate/origin").unwrap(),
        origin_project_uuid: Uuid::from_u128(7).to_string(),
        origin_version_uuid: Uuid::from_u128(8).to_string(),
        origin_version_identity: Sha256Digest(format!("sha256:{}", "7".repeat(64))),
        origin_author: author.map(credit),
        origin_committer: committer.map(credit),
    }
}

fn forked(
    history: &History,
    head: Uuid,
    citation: graphforge_discovery::LineageForkOrigin,
) -> ResearchLineage {
    let mut lineage = full_lineage(history, head);
    lineage.fork = Some(citation);
    lineage
}

#[test]
fn republishing_a_pre_credit_fork_adopts_the_verified_origin_credit_and_keeps_every_identity() {
    let history = history();
    let head = history.head;
    let published = forked(&history, head, origin(None, None));
    let local = forked(&history, head, origin(Some("Ada"), Some("Grace")));
    let merged = merge_lineage(Some(&published), local.clone(), false, false).unwrap();
    let cited = merged.fork.as_ref().unwrap();
    let before = published.fork.as_ref().unwrap();
    assert_eq!(cited.origin_author.as_ref().unwrap().name, "Ada");
    assert_eq!(cited.origin_committer.as_ref().unwrap().name, "Grace");
    assert_eq!(cited.origin_repository, before.origin_repository);
    assert_eq!(cited.origin_project_uuid, before.origin_project_uuid);
    assert_eq!(cited.origin_version_uuid, before.origin_version_uuid);
    assert_eq!(
        cited.origin_version_identity,
        before.origin_version_identity
    );
    assert_eq!(merged, local);
    // Credit already published is carried unchanged on a later republish.
    assert_eq!(
        merge_lineage(Some(&merged), local, false, false).unwrap(),
        merged
    );
}

#[test]
fn a_fork_whose_published_origin_credit_differs_or_whose_origin_differs_is_refused() {
    let history = history();
    let head = history.head;
    let published = forked(&history, head, origin(Some("Ada"), Some("Grace")));
    let mut other_origin = origin(Some("Ada"), Some("Grace"));
    other_origin.origin_version_uuid = Uuid::from_u128(9).to_string();
    for (name, proposed) in [
        ("changed", origin(Some("Mallory"), Some("Grace"))),
        ("removed", origin(Some("Ada"), None)),
        ("different origin", other_origin),
    ] {
        let local = forked(&history, head, proposed);
        let error = merge_lineage(Some(&published), local, false, false).expect_err(name);
        assert_eq!(error_code(&error), "hub.publish.invalid_input", "{name}");
    }
    // Filling credit does not excuse a different origin identity either.
    let pre_credit = forked(&history, head, origin(None, None));
    let mut shifted = origin(Some("Ada"), Some("Grace"));
    shifted.origin_project_uuid = Uuid::from_u128(10).to_string();
    let local = forked(&history, head, shifted);
    assert!(merge_lineage(Some(&pre_credit), local, false, false).is_err());
}
