//! Upstream adoption and its projection compose reviewed state in private
//! workspaces and write there, never through an alias of a published
//! generation's graph tree (#1709).
use super::*;
use crate::pinned_workspace_tests::{
    assert_published_trees_untouched, assert_tree_backed_owner, published_generations,
};
use arrow::array::Array;

fn current(graph: &GraphForge) -> Uuid {
    graph.generation_for_read().unwrap().generation_uuid()
}

/// `(x, y, secret)` of every `Item` in the Branch, ordered by `x`.
fn branch_items(
    graph: &GraphForge,
    branch: Uuid,
) -> Vec<(Option<i64>, Option<i64>, Option<String>)> {
    let branch = graph.open_research_branch(branch).unwrap();
    let rows = branch
        .graph()
        .execute("MATCH (n:Item) RETURN n.x AS x, n.y AS y, n.secret AS secret ORDER BY x")
        .unwrap();
    let mut out = Vec::new();
    for batch in &rows.batches {
        let cell = |name: &str, row: usize| {
            let column = batch.column_by_name(name).unwrap();
            (*column.data_type() != arrow::datatypes::DataType::Null && !column.is_null(row))
                .then(|| arrow::util::display::array_value_to_string(column, row).unwrap())
        };
        for row in 0..batch.num_rows() {
            out.push((
                cell("x", row).map(|value| value.parse().unwrap()),
                cell("y", row).map(|value| value.parse().unwrap()),
                cell("secret", row),
            ));
        }
    }
    out
}

/// One tree-backed owner whose Branch differs from CURRENT by a changed field,
/// an unrelated changed field and a new object carrying an unselected property.
fn owner_with_pending_upstream() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    GraphForge,
    PreviewResearchUpstreamRequest,
) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Item {x:0, y:0})").unwrap();
    let branch_uuid = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&graph),
                branch_uuid,
                version_uuid: Uuid::now_v7(),
                source: BranchSource::Current {
                    origin_version_uuid: Uuid::now_v7(),
                    context_uuid: Uuid::now_v7(),
                },
                creator_uuid: Uuid::now_v7(),
                created_at: 1,
                label: "Pinned study".into(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    graph
        .execute("MATCH (n:Item {x:0}) SET n.x=1, n.y=2")
        .unwrap();
    graph
        .execute("CREATE (:Item {x:5, secret:'unselected'})")
        .unwrap();
    // Review every field of every Item, including the new object's.
    let fields = crate::branches::fields::read(&graph, &CancellationToken::new())
        .unwrap()
        .keys()
        .filter(|key| key.0 == "node")
        .map(|key| ResearchFieldIdentity {
            object_kind: key.0.clone(),
            object_uuid: key.1,
            field: key.2.clone(),
        })
        .collect();
    let request = PreviewResearchUpstreamRequest {
        branch_uuid,
        scope: ResearchUpstreamScope::Fields { fields },
    };
    (temp, root, graph, request)
}

fn update(
    graph: &GraphForge,
    preview_request: &PreviewResearchUpstreamRequest,
    keep: impl Fn(&crate::branches::fields::Key) -> bool,
) -> UpdateResearchBranchRequest {
    let review = preview::load(graph, preview_request, &CancellationToken::new()).unwrap();
    let decisions: Vec<_> = review
        .rows
        .iter()
        .filter(|row| matches!(row.change, "upstream" | "equivalent") && keep(&row.key))
        .map(|row| ResearchUpstreamDecision {
            unit: ResearchFieldIdentity {
                object_kind: row.key.0.clone(),
                object_uuid: row.key.1,
                field: row.key.2.clone(),
            },
            resolution: ResearchUpstreamResolution::AdoptUpstream,
        })
        .collect();
    assert!(!decisions.is_empty(), "the preview offers fields to adopt");
    UpdateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(graph),
        version_uuid: Uuid::now_v7(),
        preview: preview_request.clone(),
        preview_sha256: review.digest,
        selection: ResearchUpstreamSelection::Selected { decisions },
        acknowledge_evidence: Default::default(),
        actor_uuid: Uuid::now_v7(),
        created_at: 2,
        explanation: "Adopt the reviewed fields".into(),
    }
}

/// Projection redacts every property of the selected upstream objects that was
/// not reviewed, with a real mutation, before adoption reads the projection.
/// The new upstream object's unreviewed `secret` is observable only if that
/// mutation succeeded in the projection's facade.
#[test]
fn projection_redacts_unreviewed_properties_and_adoption_applies_only_reviewed_fields() {
    let (_temp, root, mut graph, preview_request) = owner_with_pending_upstream();
    assert_tree_backed_owner(&graph);
    let before = published_generations(&root);
    let request = update(&graph, &preview_request, |key| {
        key.2 != "property:y" && key.2 != "property:secret"
    });

    graph
        .update_research_branch(&request, &CancellationToken::new())
        .expect("adoption must write its private workspaces");

    assert_eq!(
        branch_items(&graph, preview_request.branch_uuid),
        [(Some(1), Some(0), None), (Some(5), None, None)],
        "reviewed x is adopted, unreviewed y stays local and secret is never adopted"
    );
    assert_published_trees_untouched(&before, "update_research_branch");
}

/// Adoption's own facade (the one it writes last) is a private view over the
/// composed Branch content. Its real write is the native append-only Source
/// preference event, so adopting a changed preferred Artifact exercises it.
#[test]
fn adopting_a_source_preference_writes_the_private_view_and_no_published_tree() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    let cancel = CancellationToken::new();
    // A graph with content, so the owner's generations hold graph trees.
    graph.execute("CREATE (:Item {x:0, y:0})").unwrap();
    let context = || WriteContext {
        operation_uuid: OperationId(Uuid::now_v7()),
        actor_uuid: None,
    };
    for capability_id in [CapabilityId::Provenance, CapabilityId::Knowledge] {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: context(),
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }
    let source_uuid = Uuid::now_v7();
    graph
        .register_source(RegisterSourceRequest {
            context: context(),
            source_uuid,
            label: "Manuscript".into(),
            source_kind: SourceKind::Manuscript,
            identity_uri: None,
        })
        .unwrap();
    let artifact = |graph: &GraphForge, artifact_uuid: Uuid, bytes: &[u8]| {
        graph
            .register_artifact(RegisterArtifactRequest {
                context: context(),
                artifact_uuid,
                source_uuid,
                artifact_kind: ArtifactKind::RawScan,
                media_type: "text/plain".into(),
                payload: ArtifactPayloadRequest::LocalBytes(bytes.to_vec()),
                derivation_inputs: vec![],
                run_uuid: None,
            })
            .unwrap();
    };
    let prefer = |graph: &GraphForge, artifact_uuid: Uuid| {
        graph
            .set_preferred_artifact(SetPreferredArtifactRequest {
                context: context(),
                preference_event_uuid: Uuid::now_v7(),
                source_uuid,
                artifact_uuid,
                reason: "Reviewed representation".into(),
            })
            .unwrap();
    };
    let (first, second) = (Uuid::now_v7(), Uuid::now_v7());
    artifact(&graph, first, b"first reading");
    prefer(&graph, first);
    let branch_uuid = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&graph),
                branch_uuid,
                version_uuid: Uuid::now_v7(),
                source: BranchSource::Current {
                    origin_version_uuid: Uuid::now_v7(),
                    context_uuid: Uuid::now_v7(),
                },
                creator_uuid: Uuid::now_v7(),
                created_at: 1,
                label: "Source review".into(),
            },
            &cancel,
        )
        .unwrap();
    artifact(&graph, second, b"second reading");
    prefer(&graph, second);
    assert_tree_backed_owner(&graph);

    let preview_request = PreviewResearchUpstreamRequest {
        branch_uuid,
        scope: ResearchUpstreamScope::Sources,
    };
    let review = preview::load(&graph, &preview_request, &cancel).unwrap();
    let preference = (
        "source".to_owned(),
        source_uuid,
        "$preferred_artifact".to_owned(),
    );
    // The new preference is adoptable only with the Artifact it names.
    let decisions = std::iter::once(&preference)
        .chain(&review.requirements[&preference].fields)
        .map(|key| ResearchUpstreamDecision {
            unit: ResearchFieldIdentity {
                object_kind: key.0.clone(),
                object_uuid: key.1,
                field: key.2.clone(),
            },
            resolution: ResearchUpstreamResolution::AdoptUpstream,
        })
        .collect();
    let request = UpdateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        version_uuid: Uuid::now_v7(),
        preview: preview_request,
        preview_sha256: review.digest,
        selection: ResearchUpstreamSelection::Selected { decisions },
        acknowledge_evidence: Default::default(),
        actor_uuid: Uuid::now_v7(),
        created_at: 2,
        explanation: "Adopt the reviewed representation".into(),
    };
    let before = published_generations(&root);

    graph
        .update_research_branch(&request, &cancel)
        .expect("adoption must write its private view");

    let branch = graph.open_research_branch(branch_uuid).unwrap();
    let preferences = crate::knowledge::ledger::read_preference_ledger(
        &branch.graph().generation_for_read().unwrap(),
    )
    .unwrap();
    assert_eq!(
        preferences.current_preferred_artifact(source_uuid),
        Some(second),
        "the adopted preference event was appended in the private view"
    );
    assert_eq!(preferences.events.len(), 2);
    drop(branch);
    assert_published_trees_untouched(&before, "update_research_branch preference adoption");
}
