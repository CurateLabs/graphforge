//! A local revert is compared with the shared remote history, not its original parent.
use super::*;

fn incorporate(
    bob: &mut GraphForge,
    hub: &LoopbackHub,
    root: &Path,
    branch: Uuid,
    remote: Uuid,
    base: Uuid,
) -> Uuid {
    let package = Package::download(hub, root, remote);
    let before = generation(bob);
    let fetched = package.fetch(bob, branch, Uuid::now_v7(), before).unwrap();
    let request = review(bob, &fetched);
    let result = bob
        .preview_research_upstream(&request.preview, &CancellationToken::new())
        .unwrap();
    let batch = &result.batches[0];
    assert_eq!(
        batch.schema().metadata()["graphforge.upstream.remote_base_version_uuid"],
        base.to_string()
    );
    let strings = |name| {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
    };
    let row = (0..batch.num_rows())
        .find(|i| strings("field").value(*i) == "property:score")
        .unwrap();
    assert_eq!(strings("change").value(row), "conflict");
    assert_eq!(generation(bob), request.expected_generation_uuid);
    bob.update_research_branch(&request, &CancellationToken::new())
        .unwrap();
    let state = bob.research_version_retention().unwrap();
    assert_eq!(state.versions[&request.version_uuid].parents[1], remote);
    request.version_uuid
}

#[test]
fn local_reverts_and_repeated_remote_incorporation_use_shared_native_history() {
    let mut alice = history();
    let hub = LoopbackHub::start();
    publish_ok(&hub, &alice.graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let bob_path = root.path().join("bob");
    clone_into(&hub, "curate/claims", &bob_path, Some("main"), None);
    let mut bob = GraphForge::new(bob_path.to_str()).unwrap();
    let local = edit_by(&mut bob, alice.branch, 0, "Bob");
    let remote = edit_by(&mut alice.graph, alice.branch, 2, "Alice");
    publish_ok(&hub, &alice.graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let merged = incorporate(
        &mut bob,
        &hub,
        root.path(),
        alice.branch,
        remote,
        alice.head,
    );
    assert_eq!(
        bob.research_version_retention().unwrap().versions[&merged].parents,
        vec![local, remote]
    );
    publish_ok(&hub, &bob, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let next_path = root.path().join("alice-next");
    clone_into(&hub, "curate/claims", &next_path, Some("main"), None);
    let mut next = GraphForge::new(next_path.to_str()).unwrap();
    let remote = edit_by(&mut next, alice.branch, 3, "Alice");
    publish_ok(&hub, &next, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let local = edit_by(&mut bob, alice.branch, 0, "Bob");
    let merged_again = incorporate(&mut bob, &hub, root.path(), alice.branch, remote, merged);
    assert_eq!(
        bob.research_version_retention().unwrap().versions[&merged_again].parents,
        vec![local, remote]
    );
    drop(bob);
    let bob = GraphForge::new(bob_path.to_str()).unwrap();
    assert_eq!(
        score(
            bob.open_research_branch(alice.branch)
                .unwrap()
                .graph()
                .execute("MATCH (n:Item) RETURN sum(n.score)")
                .unwrap()
        ),
        3
    );
}

/// A collaborator pushed and the local Branch made no new Version: the local
/// head is the shared base, and `AllCompatible` adopts the remote value.
#[test]
fn remote_head_descending_from_the_local_head_incorporates_as_a_two_parent_version() {
    let mut alice = history();
    let hub = LoopbackHub::start();
    publish_ok(&hub, &alice.graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let bob_path = root.path().join("bob");
    clone_into(&hub, "curate/claims", &bob_path, Some("main"), None);
    let mut bob = GraphForge::new(bob_path.to_str()).unwrap();
    let remote = edit_by(&mut alice.graph, alice.branch, 2, "Alice");
    publish_ok(&hub, &alice.graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let package = Package::download(&hub, root.path(), remote);
    let before = generation(&bob);
    let fetched = package
        .fetch(&mut bob, alice.branch, Uuid::now_v7(), before)
        .unwrap();
    let mut request = review(&bob, &fetched);
    request.selection = ResearchUpstreamSelection::AllCompatible;
    bob.update_research_branch(&request, &CancellationToken::new())
        .unwrap();
    let state = bob.research_version_retention().unwrap();
    assert_eq!(
        state.versions[&request.version_uuid].parents,
        vec![alice.head, remote]
    );
}
