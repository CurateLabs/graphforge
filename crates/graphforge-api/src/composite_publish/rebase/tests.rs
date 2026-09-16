use super::super::tests::{publish_request, uuid7};
use super::*;

#[test]
fn rebase_compatibility_rejects_administrative_drift_and_removed_targets() {
    let graph = GraphForge::new(None).unwrap();
    let parent =
        graphforge_storage::resolve_project_generation(graph.resolved_generation.container_root())
            .unwrap();
    let request = publish_request();
    let mut baseline = RebaseBaseline {
        fields: BTreeMap::new(),
        node_targets: BTreeSet::new(),
        edge_targets: BTreeSet::new(),
        administrative_contract: Vec::new(),
        non_mergeable: false,
    };
    assert_eq!(
        ensure_rebase_compatible(&graph, &request, &parent, &baseline)
            .unwrap_err()
            .code(),
        "GF_WRITE_CONFLICT"
    );
    baseline.administrative_contract = administrative_contract(&parent).unwrap();
    baseline.node_targets.insert(uuid7(250));
    assert_eq!(
        ensure_rebase_compatible(&graph, &request, &parent, &baseline)
            .unwrap_err()
            .code(),
        "GF_WRITE_CONFLICT"
    );
}
