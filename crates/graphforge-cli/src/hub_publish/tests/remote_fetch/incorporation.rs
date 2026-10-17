//! Composed Alice/Bob divergence uses real discovery, native fetch/review and FF publication.
use super::*;
use graphforge_api::{
    PreviewResearchUpstreamRequest, ResearchFieldIdentity, ResearchUpstreamDecision,
    ResearchUpstreamResolution, ResearchUpstreamScope, ResearchUpstreamSelection,
    UpdateResearchBranchRequest,
};

fn review(graph: &GraphForge, fetched: &FetchedResearchBranch) -> UpdateResearchBranchRequest {
    let preview = PreviewResearchUpstreamRequest {
        branch_uuid: fetched.binding.reference.branch_uuid,
        scope: ResearchUpstreamScope::Branch,
        remote: Some(fetched.binding.clone()),
    };
    let result = graph
        .preview_research_upstream(&preview, &CancellationToken::new())
        .unwrap();
    let text = result.batches[0].schema().metadata()["graphforge.upstream.preview_sha256"].clone();
    let mut digest = [0; 32];
    for (i, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).unwrap();
    }
    let batch = &result.batches[0];
    let fields = batch
        .column_by_name("field")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let row = (0..batch.num_rows())
        .find(|i| fields.value(*i) == "property:score")
        .unwrap();
    let ids = batch
        .column_by_name("object_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .unwrap();
    UpdateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: generation(graph),
        version_uuid: Uuid::now_v7(),
        preview,
        preview_sha256: digest,
        selection: ResearchUpstreamSelection::Selected {
            decisions: vec![ResearchUpstreamDecision {
                unit: ResearchFieldIdentity {
                    object_kind: "node".into(),
                    object_uuid: Uuid::from_slice(ids.value(row)).unwrap(),
                    field: "property:score".into(),
                },
                resolution: ResearchUpstreamResolution::AdoptUpstream,
            }],
        },
        acknowledge_evidence: Default::default(),
        actor_uuid: Uuid::now_v7(),
        created_at: 4,
        explanation: "Bob reviewed Alice's divergent remote score".into(),
        author: Some(signature()),
        committer: Some(signature()),
    }
}
fn signature() -> graphforge_storage::research_versions::ResearchSignature {
    graphforge_storage::research_versions::ResearchSignature {
        name: "Bob".into(),
        email: None,
        orcid: None,
    }
}

#[test]
fn divergent_remote_fetch_review_incorporation_fast_forwards_and_reopens() {
    let mut alice = history();
    let hub = LoopbackHub::start();
    publish_ok(&hub, &alice.graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let bob_path = root.path().join("bob");
    clone_into(&hub, "curate/claims", &bob_path, Some("main"), None);
    let mut bob = GraphForge::new(bob_path.to_str()).unwrap();
    edit_by(&mut bob, alice.branch, 2, "Bob");
    let remote = edit_by(&mut alice.graph, alice.branch, 3, "Alice");
    publish_ok(&hub, &alice.graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let package = Package::download(&hub, root.path(), remote);
    let before = generation(&bob);
    let fetched = package
        .fetch(&mut bob, alice.branch, Uuid::now_v7(), before)
        .unwrap();
    let stale = review(&bob, &fetched);
    let local = edit_by(&mut bob, alice.branch, 4, "Bob");
    let current = generation(&bob);
    let error = bob
        .update_research_branch(&stale, &CancellationToken::new())
        .unwrap_err();
    assert!(matches!(
        error,
        GfError::Project {
            code: graphforge_core::ProjectErrorCode::WriteConflict,
            ..
        }
    ));
    assert_eq!(generation(&bob), current);
    reject_forged_review(&bob, &fetched);
    let request = review(&bob, &fetched);
    let receipt = bob
        .update_research_branch(&request, &CancellationToken::new())
        .unwrap();
    let state = bob.research_version_retention().unwrap();
    assert_eq!(
        state.versions[&request.version_uuid].parents,
        vec![local, remote]
    );
    assert_eq!(
        state.upstream.reviews[&request.operation_uuid].remote,
        Some(fetched.binding.clone())
    );
    assert_eq!(
        score(
            bob.open_research_version(request.version_uuid)
                .unwrap()
                .execute("MATCH (n:Item) RETURN sum(n.score)")
                .unwrap()
        ),
        3
    );
    publish_ok(&hub, &bob, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let destination = root.path().join("merged-clone");
    clone_into(&hub, "curate/claims", &destination, Some("main"), None);
    let reopened = GraphForge::new(destination.to_str()).unwrap();
    let registry = reopened.research_version_retention().unwrap();
    assert_eq!(registry.heads[&alice.branch], request.version_uuid);
    assert_eq!(
        registry.versions[&request.version_uuid].parents,
        vec![local, remote]
    );
    assert_eq!(
        registry.versions[&request.version_uuid].committer,
        Some(signature())
    );
    assert_eq!(
        score(
            reopened
                .open_research_branch(alice.branch)
                .unwrap()
                .graph()
                .execute("MATCH (n:Item) RETURN sum(n.score)")
                .unwrap()
        ),
        3
    );
    historical_review_remains_pinned(&mut bob, &hub, &root, &fetched, &request, receipt);
}

fn reject_forged_review(graph: &GraphForge, fetched: &FetchedResearchBranch) {
    let before = generation(graph);
    let mut forged = fetched.clone();
    forged.binding.lineage_sha256[0] ^= 1;
    let request = PreviewResearchUpstreamRequest {
        branch_uuid: fetched.binding.reference.branch_uuid,
        scope: ResearchUpstreamScope::Branch,
        remote: Some(forged.binding),
    };
    assert!(
        graph
            .preview_research_upstream(&request, &CancellationToken::new())
            .is_err()
    );
    assert_eq!(generation(graph), before);
}

fn historical_review_remains_pinned(
    bob: &mut GraphForge,
    hub: &LoopbackHub,
    root: &tempfile::TempDir,
    old: &FetchedResearchBranch,
    request: &UpdateResearchBranchRequest,
    receipt: graphforge_storage::research_versions::ResearchOperationReceipt,
) {
    let package = Package::download(hub, root.path(), request.version_uuid);
    let expected = generation(bob);
    package
        .fetch(bob, request.preview.branch_uuid, Uuid::now_v7(), expected)
        .unwrap();
    let current = generation(bob);
    let registry = bob.research_version_retention().unwrap();
    registry.validate().unwrap();
    old.binding.authenticate(&registry).unwrap();
    let error = bob
        .preview_research_upstream(&request.preview, &CancellationToken::new())
        .unwrap_err();
    assert!(matches!(
        error,
        GfError::Project {
            code: graphforge_core::ProjectErrorCode::WriteConflict,
            ..
        }
    ));
    assert_eq!(
        bob.update_research_branch(request, &CancellationToken::new())
            .unwrap(),
        receipt
    );
    assert_eq!(generation(bob), current);
}
