//! Local tags travel in the published lineage and never move on a Hub without
//! an explicit rewrite (ADR 0061).
use super::*;
use graphforge_api::SetResearchTagRequest;

fn tag(graph: &mut GraphForge, name: &str, version: Option<Uuid>) {
    let request = SetResearchTagRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: generation(graph),
        name: name.into(),
        version_uuid: version,
    };
    graph
        .set_research_tag(&request, &CancellationToken::new())
        .unwrap();
}

fn served_tags(hub: &LoopbackHub) -> Vec<(String, String)> {
    let (_, lineage) = served_lineage(hub, "curate/claims");
    lineage
        .tags
        .iter()
        .map(|tag| (tag.name.clone(), tag.version_uuid.clone()))
        .collect()
}

#[test]
fn push_publishes_local_tags_and_refuses_a_moved_served_tag_without_rewrite() {
    let mut alice = history();
    let hub = LoopbackHub::start();
    let first = alice.head;
    tag(&mut alice.graph, "v1", Some(first));
    publish_ok(&hub, &alice.graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    assert_eq!(served_tags(&hub), [("v1".to_owned(), first.to_string())]);

    // A later publish carries the served tag even though the local one is gone.
    let second = edit_by(&mut alice.graph, alice.branch, 2, "Alice");
    tag(&mut alice.graph, "v1", None);
    tag(&mut alice.graph, "v2", Some(second));
    publish_ok(&hub, &alice.graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    assert_eq!(
        served_tags(&hub),
        [
            ("v1".to_owned(), first.to_string()),
            ("v2".to_owned(), second.to_string())
        ]
    );

    // Moving a served tag is a non-fast-forward without --rewrite.
    let third = edit_by(&mut alice.graph, alice.branch, 3, "Alice");
    tag(&mut alice.graph, "v1", Some(third));
    let refused = run_publish_with(
        &hub,
        &alice.graph,
        &on_ref("curate/claims", "main"),
        Credential::Token(token(&hub, "curate/claims")),
    );
    assert!(
        refused
            .result
            .unwrap_err()
            .to_string()
            .contains("GF_HUB_TAG_CONFLICT")
    );
    assert_eq!(served_tags(&hub)[0].1, first.to_string());

    let mut rewrite = on_ref("curate/claims", "main");
    rewrite.rewrite = true;
    publish_ok(&hub, &alice.graph, &rewrite).result.unwrap();
    assert_eq!(served_tags(&hub)[0].1, third.to_string());
}
