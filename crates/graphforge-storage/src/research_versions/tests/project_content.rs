//! Private complete preparation is not an authoritative intermediate capture.
use super::*;

#[test]
fn project_draft_keeps_current_and_history_unchanged_until_publication() {
    let project = tempfile::tempdir().unwrap();
    let root = project.path();
    fixture(root, "original complete Project");
    execute(root, &register(root, Uuid::now_v7()));
    let before = current(root);
    let registry = state(root);
    let ResearchMutation::Register(spec) = register(root, Uuid::now_v7()).mutation else {
        unreachable!()
    };
    let draft = prepare_project_draft(root, &spec, &AtomicBool::new(false)).unwrap();
    assert_eq!(draft.parent_generation_uuid(), before);
    assert_eq!(current(root), before);
    assert_eq!(state(root), registry);
    assert_eq!(state(draft.path()), ResearchRegistry::default());
    let initial = crate::resolve_project_generation(draft.path()).unwrap();
    assert_eq!(
        initial
            .participant_snapshot("workspace", "research_fixture")
            .unwrap()
            .unwrap()
            .bytes,
        json(&"original complete Project").unwrap()
    );
    fixture(draft.path(), "privately edited Project");
    assert!(draft.finish(vec![], &AtomicBool::new(true)).is_err());
    assert_eq!(current(root), before);
    let prepared = draft.finish(vec![], &AtomicBool::new(false)).unwrap();
    assert_eq!(prepared.version.version_uuid, spec.version_uuid);
    assert_eq!(prepared.version.context_uuid, spec.context_uuid);
    assert!(prepared.version.content.source_version.is_none());
    drop(initial);
    drop(draft);
    // The final lease and CAS content suffice after the private workspace closes.
    let snapshots = retained_content::inspect(root, &prepared.version, None).unwrap();
    assert_eq!(
        snapshots[0].bytes,
        json(&"privately edited Project").unwrap()
    );
    crate::project_recovery::recover_project_on_open(root).unwrap();
    assert_eq!(current(root), before);
    assert_eq!(state(root), registry);
}

#[test]
fn project_draft_refuses_projection_stale_source_and_cancellation() {
    let project = tempfile::tempdir().unwrap();
    let root = project.path();
    fixture(root, "original");
    let ResearchMutation::Register(spec) = register(root, Uuid::now_v7()).mutation else {
        unreachable!()
    };
    let before = current(root);
    assert!(prepare_project_draft(root, &spec, &AtomicBool::new(true)).is_err());
    let mut selected = spec.clone();
    selected.source_version = Some(Uuid::now_v7());
    assert!(prepare_project_draft(root, &selected, &AtomicBool::new(false)).is_err());
    selected.source_version = None;
    selected.selection = Some(BTreeSet::new());
    assert!(prepare_project_draft(root, &selected, &AtomicBool::new(false)).is_err());
    assert_eq!(current(root), before);
    fixture(root, "advanced parent");
    assert!(prepare_project_draft(root, &spec, &AtomicBool::new(false)).is_err());
    assert!(state(root).identities.is_empty());
}
