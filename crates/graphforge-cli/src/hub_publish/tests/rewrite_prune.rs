//! Genuine divergent Branch publication, lease refusal, retention and native reopen.
mod cross_hub;
use super::*;
use arrow::array::Int64Array;

#[test]
fn explicit_rewrite_prune_preserves_lease_and_removes_real_orphan_packages_after_grace() {
    let mut alice = history();
    let hub = LoopbackHub::start();
    publish_ok(&hub, &alice.graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let bob_path = root.path().join("bob");
    clone_into(&hub, "curate/claims", &bob_path, Some("main"), None);
    let mut bob = GraphForge::new(bob_path.to_str()).unwrap();
    edit_by(&mut bob, alice.branch, 3, "Bob");
    edit_by(&mut alice.graph, alice.branch, 2, "Alice");
    publish_ok(&hub, &alice.graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let stale_refs = refs_document(&hub, "curate/claims").body;
    let stale_manifest = hub
        .get(&format!("{}/.gf/manifest", repository_url("curate/claims")))
        .body;
    let orphan = edit_by(&mut alice.graph, alice.branch, 4, "Alice");
    publish_ok(&hub, &alice.graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let (manifest, lineage) = served_lineage(&hub, "curate/claims");
    let orphan_url = manifest
        .research_version_object(&lineage, &orphan.to_string())
        .unwrap()
        .1
        .locations[0]
        .clone();
    let before = refs_document(&hub, "curate/claims").body;
    let rejected = run_publish_with(
        &hub,
        &bob,
        &on_ref("curate/claims", "main"),
        Credential::Token(token(&hub, "curate/claims")),
    );
    assert_eq!(
        error_code(&rejected.result.unwrap_err()),
        "hub.publish.non_fast_forward"
    );
    let mut flags = on_ref("curate/claims", "main");
    flags.rewrite = true;
    flags.prune = true;
    hub.inject(Fault::Serve(BTreeMap::from([
        ("/curate/claims/.gf/refs".into(), stale_refs),
        ("/curate/claims/.gf/manifest".into(), stale_manifest),
    ])));
    let rejected = run_publish_with(
        &hub,
        &bob,
        &flags,
        Credential::Token(token(&hub, "curate/claims")),
    );
    assert_eq!(
        error_code(&rejected.result.unwrap_err()),
        "hub.publish.ref_conflict"
    );
    assert_eq!(refs_document(&hub, "curate/claims").body, before);
    let run = publish_ok(&hub, &bob, &flags);
    let receipt = run.result.unwrap().receipt;
    let (_, lineage) = served_lineage(&hub, "curate/claims");
    assert!(lineage.version(&orphan.to_string()).is_none());
    assert_eq!(hub.get(&orphan_url).status, 200);
    hub.hub.advance_clock(899);
    assert_eq!(hub.get(&orphan_url).status, 200);
    hub.hub.advance_clock(1);
    assert_eq!(hub.get(&orphan_url).status, 404);
    assert_eq!(
        publish_ok(&hub, &bob, &flags).result.unwrap().receipt,
        receipt
    );
    let destination = root.path().join("rewritten");
    clone_into(&hub, "curate/claims", &destination, Some("main"), None);
    let reopened = GraphForge::new(destination.to_str()).unwrap();
    let result = reopened
        .open_research_branch(alice.branch)
        .unwrap()
        .graph()
        .execute("MATCH (n:Item) RETURN sum(n.score)")
        .unwrap();
    assert_eq!(
        result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
}

fn independent_fork(origin: &History, version: Uuid) -> (GraphForge, Uuid) {
    let path = origin.root.path().join("independent-fork");
    origin
        .graph
        .fork_research(
            &ForkResearchRequest {
                operation_uuid: Uuid::now_v7(),
                project_uuid: Uuid::now_v7(),
                version_uuid: version,
                projection: None,
                target: path.clone(),
                actor_uuid: Uuid::now_v7(),
                governance: "Independent retained research copy".into(),
                adopt_selected_ontology: true,
                metadata: WorkspaceResearchMetadata::empty(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    let (branch, _) = create_branch(&mut graph, "main", current());
    edit_by(&mut graph, branch, 7, "Fork author");
    (graph, branch)
}
#[test]
fn established_fork_warns_on_verified_erased_origin_and_retains_its_independent_copy() {
    let mut origin = history();
    let hub = LoopbackHub::start();
    publish_ok(&hub, &origin.graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let replacement = root.path().join("replacement");
    clone_into(&hub, "curate/claims", &replacement, Some("main"), None);
    let mut replacement = GraphForge::new(replacement.to_str()).unwrap();
    edit_by(&mut replacement, origin.branch, 3, "Other author");
    let cited = edit_by(&mut origin.graph, origin.branch, 2, "Origin author");
    publish_ok(&hub, &origin.graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let (mut fork, branch) = independent_fork(&origin, cited);
    let mut initial = on_ref("curate/claims-fork", "main");
    initial.fork_of = Some(repository_url("curate/claims"));
    publish_ok(&hub, &fork, &initial).result.unwrap();
    let (_, before) = served_lineage(&hub, "curate/claims-fork");
    edit_by(&mut fork, branch, 8, "Fork author");
    let ordinary = on_ref("curate/claims-fork", "main");
    let updated = publish_ok(&hub, &fork, &ordinary).result.unwrap().receipt;
    let fork_writes = || {
        hub.log()
            .iter()
            .filter(|r| {
                r.path.starts_with("/curate/claims-fork/")
                    && matches!(r.method.as_str(), "POST" | "PUT")
            })
            .count()
    };
    let writes_before = fork_writes();
    let replay = publish_ok(&hub, &fork, &ordinary).result.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, updated);
    let mut checked = ordinary;
    checked.check_fork_origin = Some(repository_url("curate/claims"));
    let mut mismatch = on_ref("curate/claims-fork", "main");
    mismatch.check_fork_origin = Some(repository_url("curate/wrong-origin"));
    let rejected = run_publish_with(
        &hub,
        &fork,
        &mismatch,
        Credential::Token(token(&hub, "curate/claims-fork")),
    );
    assert_eq!(
        error_code(&rejected.result.unwrap_err()),
        "hub.publish.integrity_failure"
    );
    hub.inject(Fault::Serve(tampered_origin(&hub, "curate/claims", cited)));
    let rejected = run_publish_with(
        &hub,
        &fork,
        &checked,
        Credential::Token(token(&hub, "curate/claims-fork")),
    );
    assert_eq!(
        error_code(&rejected.result.unwrap_err()),
        "hub.publish.integrity_failure"
    );
    let mut flags = on_ref("curate/claims", "main");
    flags.rewrite = true;
    flags.prune = true;
    publish_ok(&hub, &replacement, &flags).result.unwrap();
    hub.hub.advance_clock(900);
    let run = publish_ok(&hub, &fork, &checked);
    let replay = run.result.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, updated);
    assert_eq!(fork_writes(), writes_before);
    assert!(
        String::from_utf8(run.stderr)
            .unwrap()
            .contains("Fork origin Version was erased")
    );
    let (_, after) = served_lineage(&hub, "curate/claims-fork");
    assert_eq!(before.fork, after.fork);
    let destination = root.path().join("fork-reopened");
    clone_into(&hub, "curate/claims-fork", &destination, Some("main"), None);
    let reopened = GraphForge::new(destination.to_str()).unwrap();
    let query = reopened
        .open_research_branch(branch)
        .unwrap()
        .graph()
        .execute("MATCH (n:Item) RETURN sum(n.score)")
        .unwrap();
    assert_eq!(
        query.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        8
    );
    assert!(
        hub.log()
            .iter()
            .filter(|r| r.method == "GET"
                && r.path.starts_with("/curate/claims/.gf/")
                && !r.path.contains("publish"))
            .all(|r| r.bearer.is_none())
    );
}

#[test]
fn ordinary_operation_identity_is_unchanged_and_each_flag_is_committed() {
    let mut args = args("curate/claims");
    let original = "existing canonical operation identity".to_owned();
    assert_eq!(
        super::super::history::bind_flags(original.clone(), &args),
        original
    );
    args.rewrite = true;
    let rewrite = super::super::history::bind_flags(original.clone(), &args);
    args.rewrite = false;
    args.prune = true;
    let prune = super::super::history::bind_flags(original.clone(), &args);
    args.rewrite = true;
    let both = super::super::history::bind_flags(original.clone(), &args);
    assert_ne!(rewrite, original);
    assert_ne!(rewrite, prune);
    assert_ne!(both, rewrite);
    assert_ne!(both, prune);
}
