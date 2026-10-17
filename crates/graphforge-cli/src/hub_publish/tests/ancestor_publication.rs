use super::*;

#[test]
fn descendant_first_then_explicit_ancestor_publish_is_clonable() {
    let history = history();
    let hub = LoopbackHub::start();
    publish_ok(&hub, &history.graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let (_, before) = served_lineage(&hub, "curate/claims");
    let original_ancestor = before.version(&history.base.to_string()).unwrap().clone();
    assert!(original_ancestor.package.is_none());
    let head_package = before
        .version(&history.head.to_string())
        .unwrap()
        .package
        .clone();
    let arguments = PublishArgs {
        version_uuid: Some(history.base.to_string()),
        ..args("curate/claims")
    };
    publish_ok(&hub, &history.graph, &arguments).result.unwrap();
    let (_, after) = served_lineage(&hub, "curate/claims");
    let mut materialized = after.version(&history.base.to_string()).unwrap().clone();
    let package = materialized
        .package
        .take()
        .expect("explicit ancestor has a payload");
    assert_eq!(materialized, original_ancestor);
    assert_eq!(
        after.version(&history.head.to_string()).unwrap().package,
        head_package
    );
    assert_eq!(after.branches, before.branches);
    let destinations = tempfile::tempdir().unwrap();
    let destination = destinations.path().join("ancestor");
    let cloned = clone_into(
        &hub,
        "curate/claims",
        &destination,
        None,
        Some(history.base),
    );
    assert_eq!(cloned["research_version_uuid"], history.base.to_string());
    let reopened = GraphForge::new(destination.to_str()).unwrap();
    assert_eq!(
        reopened
            .research_version(history.base)
            .unwrap()
            .version_uuid,
        history.base
    );
    let query = reopened
        .execute("MATCH (n:Item) RETURN sum(n.score)")
        .unwrap();
    assert_eq!(
        query.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0),
        0
    );
    publish_ok(&hub, &history.graph, &arguments).result.unwrap();
    let (_, replayed) = served_lineage(&hub, "curate/claims");
    assert_eq!(
        replayed
            .version(&history.base.to_string())
            .unwrap()
            .package
            .as_ref(),
        Some(&package)
    );
}
