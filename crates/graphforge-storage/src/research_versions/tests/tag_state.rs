//! Tags and the current Branch are local registry state (ADR 0061).
use super::*;

fn set_tag(root: &Path, name: &str, version: Option<Uuid>) -> ResearchOperation {
    ResearchOperation {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(root),
        mutation: ResearchMutation::SetTag {
            name: name.into(),
            version_uuid: version,
            intent_sha256: [0; 32],
        },
    }
}

fn refusal(root: &Path, operation: &ResearchOperation) -> String {
    publish_research_operation(root, operation, &AtomicBool::new(false))
        .unwrap_err()
        .to_string()
}

#[test]
fn tags_are_bounded_and_name_known_identities() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fixture(root, "content");
    let known = execute(root, &register(root, Uuid::now_v7()))
        .version_uuid
        .unwrap();
    let before = state(root);
    for operation in [
        set_tag(root, "v1", Some(Uuid::now_v7())),
        set_tag(root, "", Some(known)),
        set_tag(root, "has space", Some(known)),
        set_tag(root, "never-created", None),
    ] {
        refusal(root, &operation);
    }
    assert_eq!(state(root), before);
    execute(root, &set_tag(root, "v1", Some(known)));
    assert_eq!(state(root).tags, BTreeMap::from([("v1".to_owned(), known)]));

    // Capacity is a registry bound, checked on publication and on read.
    let mut registry = ResearchRegistry::default();
    registry.identities.insert(known, [0; 32]);
    registry.tags = (0..MAX_RECEIPTS)
        .map(|index| (format!("tag-{index}"), known))
        .collect();
    super::super::local_state::set_tag(&mut registry, "one-more", Some(known)).unwrap_err();
    registry.validate().unwrap();
    registry.tags.insert("one-more".into(), known);
    registry.validate().unwrap_err();
}

#[test]
fn registry_without_tags_or_current_branch_keeps_its_bytes() {
    let registry = ResearchRegistry::default();
    let encoded = json(&registry).unwrap();
    let text = String::from_utf8(encoded.clone()).unwrap();
    assert!(
        !text.contains("tags") && !text.contains("current_branch"),
        "{text}"
    );
    let decoded: ResearchRegistry = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(json(&decoded).unwrap(), encoded);

    // A real registry written before the fields existed decodes and re-encodes
    // to the same bytes, so its participant digest and every identity are unchanged.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    super::revision_six::legacy_project(root);
    let generation = crate::resolve_project_generation(root).unwrap();
    let raw = generation
        .participant_snapshot(RESEARCH_CAPABILITY, RESEARCH_REGISTRY)
        .unwrap()
        .unwrap()
        .bytes;
    let existing: ResearchRegistry = serde_json::from_slice(&raw).unwrap();
    assert!(existing.tags.is_empty() && existing.current_branch.is_none());
    assert_eq!(json(&existing).unwrap(), raw);
}

#[test]
fn set_tag_refuses_a_moved_target_and_replays_exactly() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fixture(root, "content");
    let context = Uuid::now_v7();
    let first = execute(root, &register(root, context))
        .version_uuid
        .unwrap();
    let second = execute(root, &register(root, context))
        .version_uuid
        .unwrap();

    let create = set_tag(root, "v1", Some(first));
    let receipt = execute(root, &create);
    let after_create = state(root);
    assert_eq!(after_create.tags["v1"], first);
    assert_eq!(
        execute(root, &create),
        receipt,
        "exact replay returns the receipt"
    );
    assert_eq!(state(root), after_create);

    // The same name and target again is idempotent; another target is refused.
    execute(root, &set_tag(root, "v1", Some(first)));
    let moved = set_tag(root, "v1", Some(second));
    assert!(
        refusal(root, &moved).contains("GF_RESEARCH_TAG_EXISTS"),
        "a tag never moves"
    );
    assert_eq!(state(root).tags["v1"], first);

    // Delete and create again is the only way to move it.
    execute(root, &set_tag(root, "v1", None));
    assert!(state(root).tags.is_empty());
    execute(root, &set_tag(root, "v1", Some(second)));
    assert_eq!(state(root).tags["v1"], second);

    let missing = ResearchOperation {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(root),
        mutation: ResearchMutation::SelectBranch {
            branch_uuid: Uuid::now_v7(),
            intent_sha256: [0; 32],
        },
    };
    refusal(root, &missing);
}
