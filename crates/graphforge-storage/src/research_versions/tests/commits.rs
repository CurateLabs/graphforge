//! Versions are commits: parents, signatures and the permanent ancestry ledger.
use super::*;

/// A real research/6 record, exactly as its producer encoded it, and the identity
/// digest that producer committed (graphforge-storage 0.5.2; captured in
/// `tests/fixtures/analyst-journey-v1/output/selected-reference.json`).
const V6_RECORD: &str = include_str!("v6_record.json");
const V6_IDENTITY: &str = "e1d962bbd23be52884d0a90accbcec9486fe922566c5b5d7ccf3faa70002b6ec";

fn v6_record() -> (String, ResearchVersionRecord, String) {
    let record = serde_json::from_str(V6_RECORD).unwrap();
    (V6_RECORD.to_owned(), record, V6_IDENTITY.to_owned())
}

fn signature(name: &str) -> ResearchSignature {
    ResearchSignature {
        name: name.into(),
        email: Some(format!("{}@example.org", name.to_lowercase())),
        orcid: None,
    }
}

fn mutate(root: &Path, mutation: ResearchMutation) -> ResearchOperationReceipt {
    execute(
        root,
        &ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(root),
            mutation,
        },
    )
}

fn refused(root: &Path, mutation: ResearchMutation) -> GfError {
    let before = current(root);
    let error = publish_research_operation(
        root,
        &ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: before,
            mutation,
        },
        &AtomicBool::new(false),
    )
    .unwrap_err();
    assert_eq!(current(root), before);
    error
}

#[test]
fn v6_record_identity_digest_is_unchanged() {
    let (encoded, record, golden) = v6_record();
    assert!(record.content.producer.ends_with(";research/6"));
    assert!(record.parents.is_empty());
    assert!(record.author.is_none() && record.committer.is_none());
    assert!(record.provenance.is_none());
    // Absent commit fields are omitted, so the v6 bytes re-encode exactly.
    assert_eq!(json(&record).unwrap(), encoded.as_bytes());
    assert_eq!(hex(&identity_digest(&record).unwrap()), golden);
}

#[test]
fn identity_commits_to_parents_author_committer_and_provenance() {
    let (_, base, golden) = v6_record();
    let parent = Uuid::from_u128(1);
    let merged = Uuid::from_u128(2);
    let mut variants = vec![base.clone()];
    let mut push = |change: &dyn Fn(&mut ResearchVersionRecord)| {
        let mut variant = base.clone();
        change(&mut variant);
        variants.push(variant);
    };
    push(&|v| v.parents = vec![parent]);
    push(&|v| v.parents = vec![parent, merged]);
    push(&|v| v.parents = vec![merged, parent]);
    push(&|v| v.author = Some(signature("Ada")));
    push(&|v| v.author = Some(signature("Grace")));
    push(&|v| {
        v.author = Some(ResearchSignature {
            orcid: Some("0000-0002-1825-0097".into()),
            ..signature("Ada")
        });
    });
    push(&|v| v.committer = Some(signature("Ada")));
    push(&|v| {
        v.provenance = Some(ResearchVersionProvenance::Restored {
            version_uuid: parent,
        });
    });
    push(&|v| {
        v.provenance = Some(ResearchVersionProvenance::Brought {
            version_uuid: parent,
        });
    });
    let digests: BTreeSet<_> = variants
        .iter()
        .map(|v| hex(&identity_digest(v).unwrap()))
        .collect();
    assert_eq!(digests.len(), variants.len(), "each field changes identity");
    assert!(digests.contains(&golden));
}

#[test]
fn project_capture_and_restore_record_prior_head_signatures_and_provenance() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fixture(root, "first");
    let context = Uuid::now_v7();
    let first = execute(root, &register(root, context))
        .version_uuid
        .unwrap();
    assert!(state(root).versions[&first].parents.is_empty());
    fixture(root, "second");
    let mut capture = register(root, context);
    if let ResearchMutation::Register(spec) = &mut capture.mutation {
        spec.author = Some(signature("Ada"));
        spec.committer = Some(signature("Grace"));
    }
    let second = execute(root, &capture).version_uuid.unwrap();
    let registry = state(root);
    let record = &registry.versions[&second];
    assert_eq!(record.parents, vec![first]);
    assert_eq!(record.author, Some(signature("Ada")));
    assert_eq!(record.committer, Some(signature("Grace")));
    assert_eq!(registry.ancestry[&second], vec![first]);
    for project in [false, true] {
        let head = state(root).heads[&context];
        let restored = Uuid::now_v7();
        let (author, committer) = (Some(signature("Ada")), Some(signature("Linus")));
        mutate(
            root,
            if project {
                ResearchMutation::RestoreProject {
                    context_uuid: context,
                    source_version: first,
                    version_uuid: restored,
                    created_at: 3,
                    author,
                    committer,
                }
            } else {
                ResearchMutation::Restore {
                    context_uuid: context,
                    source_version: first,
                    version_uuid: restored,
                    created_at: 3,
                    author,
                    committer,
                }
            },
        );
        let record = state(root).versions[&restored].clone();
        assert_eq!(
            record.parents,
            vec![head],
            "restore descends from prior head"
        );
        assert_eq!(
            record.provenance,
            Some(ResearchVersionProvenance::Restored {
                version_uuid: first
            })
        );
        assert_eq!(record.author, Some(signature("Ada")));
        assert_eq!(record.committer, Some(signature("Linus")));
    }
    let invalid = ResearchSignature {
        name: " padded".into(),
        email: None,
        orcid: None,
    };
    let error = refused(
        root,
        ResearchMutation::Restore {
            context_uuid: context,
            source_version: first,
            version_uuid: Uuid::now_v7(),
            created_at: 4,
            author: Some(invalid),
            committer: None,
        },
    );
    assert!(error.to_string().contains("signature"), "{error}");
}

fn compacted_origin(root: &Path) -> ResearchVersionRecord {
    let origin = execute(root, &register(root, Uuid::now_v7()))
        .version_uuid
        .unwrap();
    mutate(
        root,
        ResearchMutation::Compact {
            versions: BTreeSet::from([origin]),
        },
    );
    state(root).versions[&origin].clone()
}

fn publish_branch(
    creation: Option<ResearchBranchRecord>,
    version: &ResearchVersionRecord,
) -> ResearchMutation {
    ResearchMutation::PublishBranch {
        intent_sha256: [0; 32],
        origin_capture: None,
        creation,
        version: Box::new(version.clone()),
    }
}

#[test]
fn branch_heads_descend_from_origin_then_prior_head() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fixture(root, "origin");
    let origin = compacted_origin(root);
    let mut base = origin.clone();
    base.version_uuid = Uuid::now_v7();
    base.context_uuid = Uuid::now_v7();
    base.content.source_version = Some(origin.version_uuid);
    let creation = ResearchBranchRecord {
        branch_uuid: base.context_uuid,
        project_uuid: Uuid::now_v7(),
        parent_branch_uuid: None,
        origin_version_uuid: origin.version_uuid,
        base_version_uuid: base.version_uuid,
        creator_uuid: Uuid::now_v7(),
        created_at: 1,
        label: "commits".into(),
        selection_sha256: [0; 32],
    };
    for wrong in [vec![], vec![Uuid::now_v7()]] {
        base.parents = wrong;
        let error = refused(root, publish_branch(Some(creation.clone()), &base));
        assert!(error.to_string().contains("parents"), "{error}");
    }
    base.parents = vec![origin.version_uuid];
    base.author = Some(signature("Ada"));
    mutate(root, publish_branch(Some(creation), &base));
    assert_eq!(
        state(root).ancestry[&base.version_uuid],
        vec![origin.version_uuid]
    );
    let mut edit = base.clone();
    edit.version_uuid = Uuid::now_v7();
    for wrong in [
        vec![],
        vec![origin.version_uuid],
        vec![base.version_uuid, origin.version_uuid],
    ] {
        edit.parents = wrong;
        let error = refused(root, publish_branch(None, &edit));
        assert!(error.to_string().contains("parents"), "{error}");
    }
    edit.parents = vec![base.version_uuid];
    mutate(root, publish_branch(None, &edit));
    let registry = state(root);
    assert_eq!(registry.heads[&base.context_uuid], edit.version_uuid);
    assert_eq!(
        registry.ancestors(edit.version_uuid),
        vec![base.version_uuid, origin.version_uuid]
    );
}

#[test]
fn ancestry_of_released_versions_remains_walkable_after_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let context = Uuid::now_v7();
    let mut chain = Vec::new();
    for content in ["one", "two", "three", "four"] {
        fixture(root, content);
        chain.push(
            execute(root, &register(root, context))
                .version_uuid
                .unwrap(),
        );
    }
    for released in &chain[..3] {
        mutate(
            root,
            ResearchMutation::DeleteVersion {
                version_uuid: *released,
            },
        );
    }
    crate::project_recovery::recover_project_on_open(root).unwrap();
    let registry = state(root);
    assert!(
        chain[..3]
            .iter()
            .all(|id| !registry.versions.contains_key(id))
    );
    assert_eq!(
        registry.ancestors(chain[3]),
        vec![chain[2], chain[1], chain[0]]
    );
    assert_eq!(registry.ancestry[&chain[1]], vec![chain[0]]);
    assert!(!registry.ancestry.contains_key(&chain[0]), "a root commit");
}

#[test]
fn parent_signature_and_ledger_bounds_fail_closed() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fixture(root, "one");
    let context = Uuid::now_v7();
    let first = execute(root, &register(root, context))
        .version_uuid
        .unwrap();
    fixture(root, "two");
    let second = execute(root, &register(root, context))
        .version_uuid
        .unwrap();
    let valid = state(root);
    valid.validate().unwrap();
    let rewrite = |change: &dyn Fn(&mut ResearchRegistry)| {
        let mut registry = valid.clone();
        change(&mut registry);
        let record = registry.versions.get_mut(&second).unwrap().clone();
        registry
            .identities
            .insert(second, identity_digest(&record).unwrap());
        registry.validate().unwrap_err()
    };
    let known: Vec<_> = (0..=MAX_PARENTS).map(|_| Uuid::now_v7()).collect();
    for (case, parents) in [
        ("too many", known.clone()),
        ("duplicate", vec![first, first]),
        ("self", vec![second]),
        ("unknown", vec![Uuid::now_v7()]),
        ("nil", vec![Uuid::nil()]),
    ] {
        let error = rewrite(&|registry| {
            for id in &known {
                registry.identities.insert(*id, [1; 32]);
            }
            registry.versions.get_mut(&second).unwrap().parents = parents.clone();
            registry.ancestry.insert(second, parents.clone());
        });
        assert!(error.to_string().contains("parents"), "{case}: {error}");
    }
    let error = rewrite(&|registry| {
        registry.versions.get_mut(&second).unwrap().author = Some(ResearchSignature {
            name: "n".repeat(MAX_SIGNATURE_NAME_BYTES + 1),
            email: None,
            orcid: None,
        });
    });
    assert!(error.to_string().contains("signature"), "{error}");
    let error = rewrite(&|registry| {
        registry.ancestry.remove(&second);
    });
    assert!(error.to_string().contains("ancestry ledger"), "{error}");
    let error = rewrite(&|registry| {
        registry.ancestry.insert(first, vec![second]);
        registry.versions.remove(&first);
    });
    assert!(error.to_string().contains("cycle"), "{error}");
    let mut oversized = valid.clone();
    for _ in 0..MAX_RECEIPTS {
        oversized.ancestry.insert(Uuid::now_v7(), vec![first]);
    }
    assert_eq!(
        oversized.validate().unwrap_err().code(),
        "GF_RESOURCE_LIMIT"
    );
}

#[test]
fn ordinary_publication_cannot_erase_or_rewrite_ancestry() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let context = Uuid::now_v7();
    fixture(root, "one");
    let first = execute(root, &register(root, context))
        .version_uuid
        .unwrap();
    fixture(root, "two");
    let second = execute(root, &register(root, context))
        .version_uuid
        .unwrap();
    fixture(root, "three");
    execute(root, &register(root, context));
    mutate(
        root,
        ResearchMutation::DeleteVersion {
            version_uuid: second,
        },
    );
    let before = current(root);
    for erase in [true, false] {
        let mut registry = state(root);
        if erase {
            registry.ancestry.remove(&second);
        } else {
            registry.ancestry.insert(second, vec![Uuid::now_v7()]);
            registry
                .identities
                .insert(registry.ancestry[&second][0], [2; 32]);
        }
        let participant = registry.participant().unwrap();
        let request = ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            capabilities: vec![
                ProjectCapability {
                    capability_id: "graph".into(),
                    capability_version: 1,
                },
                ProjectCapability {
                    capability_id: RESEARCH_CAPABILITY.into(),
                    capability_version: RESEARCH_VERSION,
                },
            ],
            participants: vec![participant],
        };
        let ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation(root, &request).unwrap()
        else {
            panic!("unexpected replay")
        };
        let Err(error) = staged.validate(|_| Ok(()), |_, _| Ok(())) else {
            panic!("ancestry rewrite validated")
        };
        assert!(error.to_string().contains("ancestry"), "{error}");
        assert_eq!(current(root), before);
    }
    assert_eq!(state(root).ancestry[&second], vec![first]);
}
