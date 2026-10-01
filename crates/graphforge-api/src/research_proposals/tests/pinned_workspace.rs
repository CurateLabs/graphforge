//! Research flows that need a writable facade must write a private copy, never
//! a published generation's own graph tree (#1709).
use super::*;
use crate::pinned_workspace_tests::{
    TreeStamp, assert_published_trees_untouched, assert_tree_backed_owner, has_compact_graph_root,
    published_generations, tree_drift,
};
use graphforge_storage::research_versions::{ResearchMutation, ResearchOperation};

fn project_with_branch() -> (tempfile::TempDir, std::path::PathBuf, GraphForge, Uuid) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Character {score:0})").unwrap();
    let branch = branch(&mut graph);
    (temp, root, graph, branch)
}

fn score_sum(graph: &GraphForge) -> i64 {
    let rows = graph
        .execute("MATCH (n:Character) RETURN sum(n.score) AS total")
        .unwrap();
    rows.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[test]
fn branch_edit_leaves_every_published_generation_tree_byte_identical() {
    let (_temp, root, mut graph, branch) = project_with_branch();
    let before = published_generations(&root);

    let version = edit(&mut graph, branch, "MATCH (n:Character) SET n.score=1");

    assert_published_trees_untouched(&before, "execute_research_branch");
    // The edit really happened, in the new Version and not in CURRENT.
    assert_eq!(score_sum(&graph), 0);
    let edited = graph.open_research_version(version).unwrap();
    let rows = edited
        .execute("MATCH (n:Character) RETURN sum(n.score) AS total")
        .unwrap();
    assert_eq!(
        rows.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
}

/// The private project the edit runs in is ephemeral, so its parent generation
/// is only observable between preparation and publication.
#[test]
fn branch_edit_private_parent_generation_matches_its_recorded_inventory() {
    let (_temp, _root, graph, branch) = project_with_branch();
    let request = ExecuteResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: branch,
        version_uuid: Uuid::now_v7(),
        created_at: 2,
        query: "MATCH (n:Character) SET n.score=1".into(),
    };
    let token = CancellationToken::new();
    let command = crate::branches::publication::begin(
        &graph,
        request.operation_uuid,
        request.expected_generation_uuid,
        &request,
        &token,
    )
    .unwrap();
    let (prepared, _version) =
        crate::branches::edit::prepare(&graph, &command, request.branch_uuid).unwrap();
    let parent = prepared.generation_for_read().unwrap();
    let before = TreeStamp::capture(&parent.graph_tree_root());
    assert_eq!(tree_drift(&parent, &before), Vec::<String>::new());

    prepared.execute(&request.query).unwrap();

    assert_eq!(
        tree_drift(&parent, &before),
        Vec::<String>::new(),
        "the edit must run in a private copy of the parent generation's tree"
    );
}

#[test]
fn project_restore_then_write_publishes_without_touching_any_published_tree() {
    let (_temp, root, mut graph, _branch) = project_with_branch();
    let baseline = Uuid::now_v7();
    let owner = Uuid::now_v7();
    let capture = graph
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid: baseline,
            context_uuid: owner,
            label: None,
            description: None,
            created_at: 1,
            required_versions: Default::default(),
        })
        .unwrap();
    let token = CancellationToken::new();
    graph
        .commit_research_version_operation(capture, &token)
        .unwrap();
    graph.execute("MATCH (n:Character) SET n.score=5").unwrap();
    assert_eq!(score_sum(&graph), 5);

    graph
        .commit_research_version_operation(
            ResearchOperation {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&graph),
                mutation: ResearchMutation::RestoreProject {
                    context_uuid: owner,
                    source_version: baseline,
                    version_uuid: Uuid::now_v7(),
                    created_at: 2,
                },
            },
            &token,
        )
        .unwrap();
    assert_eq!(score_sum(&graph), 0, "restore returns the captured graph");
    let before = published_generations(&root);
    // The restore materialized the source Version into a temporary project;
    // that project's generations are published trees too.
    let materialized = graph
        .research_materialization
        .as_ref()
        .expect("restore keeps the materialized source alive")
        .path()
        .to_path_buf();
    let materialized_before = published_generations(&materialized);

    graph.execute("CREATE (:Character {score:7})").unwrap();

    assert_eq!(score_sum(&graph), 7);
    assert_published_trees_untouched(&before, "a write after RestoreProject");
    assert_published_trees_untouched(
        &materialized_before,
        "a write after RestoreProject (materialized source)",
    );
}

/// Field selection redacts every unselected property from a private view of the
/// selected content with a real mutation, before freezing it. The unselected
/// `private_note` is gone from the frozen payload only if that write succeeded.
#[test]
fn proposal_field_selection_redacts_a_private_view_and_leaves_published_trees_untouched() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph
        .execute("CREATE (:Character {score:0, private_note:'unselected'})")
        .unwrap();
    let node = node(&mut graph);
    let branch = branch(&mut graph);
    let version = edit(&mut graph, branch, "MATCH (n:Character) SET n.score=1");
    assert_tree_backed_owner(&graph);
    let before = published_generations(&root);

    let request = submit(&mut graph, branch, version, node, &["property:score"]);

    let registry = graph.research_version_retention().unwrap();
    let payload = &registry.versions
        [&registry.proposals.proposals[&request.proposal_uuid].payload_version_uuid];
    let payload = crate::research_versions::materialize_version(&graph, payload).unwrap();
    let fields = crate::branches::fields::read(&payload, &CancellationToken::new()).unwrap();
    assert!(
        fields.contains_key(&("node".into(), node, "property:score".into())),
        "the selected field is retained"
    );
    assert!(
        !fields.contains_key(&("node".into(), node, "property:private_note".into())),
        "the unselected field was redacted by a successful write"
    );
    assert_published_trees_untouched(&before, "submit_research_proposal");
}

/// Field selection, adoption and projection all open private views over
/// prepared Branch content. Prepared content records its graph as a compact
/// root, which always hydrates into a private workspace, so the view is never a
/// pinned alias of a tree even when the owner project is tree-backed, and
/// writing it leaves every owner generation untouched.
#[test]
fn prepared_branch_content_never_opens_a_pinned_alias_over_a_tree_backed_owner() {
    let (_temp, root, graph, branch) = project_with_branch();
    assert_tree_backed_owner(&graph);
    let request = ExecuteResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: branch,
        version_uuid: Uuid::now_v7(),
        created_at: 2,
        query: "MATCH (n:Character) SET n.score=1".into(),
    };
    let token = CancellationToken::new();
    let command = crate::branches::publication::begin(
        &graph,
        request.operation_uuid,
        request.expected_generation_uuid,
        &request,
        &token,
    )
    .unwrap();
    let (edited, version) = crate::branches::edit::prepare(&graph, &command, branch).unwrap();
    let prepared = graphforge_storage::research_versions::prepare_branch_content(
        &command.root,
        &edited.generation_for_read().unwrap(),
        version,
        token.flag(),
    )
    .unwrap();
    let before = published_generations(&root);

    let read_only = crate::branches::private_view::open(&graph, &prepared).unwrap();
    let writable = crate::branches::private_view::open_writable(&graph, &prepared).unwrap();

    for (name, view) in [("read-only", &read_only), ("writable", &writable)] {
        let generation = view.generation_for_read().unwrap();
        assert!(
            has_compact_graph_root(&generation),
            "{name}: prepared content must record its graph as a compact root"
        );
        assert!(
            !view.dir().is_pinned_alias(),
            "{name}: a private view of prepared content must not alias a published tree"
        );
        assert_ne!(view.dir().path(), generation.graph_tree_root(), "{name}");
    }
    writable
        .execute("CREATE (:Character {score:9})")
        .expect("a private view of prepared content must accept a write");
    assert_published_trees_untouched(&before, "a write through a private prepared view");
}
