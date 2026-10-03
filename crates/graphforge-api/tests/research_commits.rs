//! Versions are commits: real facade operations record their parents and signatures.
use arrow::ipc::writer::StreamWriter;
use graphforge_api::*;
use uuid::Uuid;

fn current(graph: &GraphForge) -> Uuid {
    graph
        .research_project_summary()
        .unwrap()
        .identity
        .generation_uuid
}

fn signature(name: &str) -> ResearchSignature {
    ResearchSignature {
        name: name.into(),
        email: Some(format!("{}@example.org", name.to_lowercase())),
        orcid: None,
    }
}

fn head(graph: &GraphForge, branch: Uuid) -> Uuid {
    graph.open_research_branch(branch).unwrap().version_uuid()
}

fn record(graph: &GraphForge, version: Uuid) -> ResearchVersionRecord {
    graph.research_version(version).unwrap()
}

fn capture(graph: &mut GraphForge, context: Uuid, author: Option<ResearchSignature>) -> Uuid {
    let version_uuid = Uuid::now_v7();
    let operation = graph
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid,
            context_uuid: context,
            label: None,
            description: None,
            created_at: 1,
            required_versions: Default::default(),
            author,
            committer: Some(signature("Committer")),
        })
        .unwrap();
    graph
        .commit_research_version_operation(operation, &CancellationToken::new())
        .unwrap();
    version_uuid
}

fn frozen(graph: &GraphForge, version_uuid: Uuid, query: &str) -> Vec<u8> {
    let result = graph
        .freeze_slice(
            &SliceRequest {
                request_uuid: Uuid::now_v7(),
                source: SliceSource::Version { version_uuid },
                selector: SliceSelector::Query {
                    query: query.into(),
                },
                include: Default::default(),
                exclude: Default::default(),
                limits: Default::default(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, result.schema.as_ref()).unwrap();
    for batch in &result.batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    bytes
}

#[test]
fn project_capture_and_restore_descend_from_the_prior_head() {
    let directory = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(directory.path().join("p").to_str()).unwrap();
    graph.execute("CREATE (:Item {n: 1})").unwrap();
    let context = Uuid::now_v7();
    let first = capture(&mut graph, context, Some(signature("Ada")));
    let first_record = record(&graph, first);
    assert!(first_record.parents.is_empty(), "a context's first Version");
    assert_eq!(first_record.author, Some(signature("Ada")));
    assert_eq!(first_record.committer, Some(signature("Committer")));
    graph.execute("CREATE (:Item {n: 2})").unwrap();
    let second = capture(&mut graph, context, None);
    let second_record = record(&graph, second);
    assert_eq!(second_record.parents, vec![first]);
    assert_eq!(second_record.author, None);
    let restored = Uuid::now_v7();
    graph
        .commit_research_version_operation(
            ResearchOperation {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&graph),
                mutation: ResearchMutation::RestoreProject {
                    context_uuid: context,
                    source_version: first,
                    version_uuid: restored,
                    created_at: 3,
                    author: Some(signature("Grace")),
                    committer: None,
                },
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let restored_record = record(&graph, restored);
    assert_eq!(restored_record.parents, vec![second]);
    assert_eq!(
        restored_record.provenance,
        Some(ResearchVersionProvenance::Restored {
            version_uuid: first
        })
    );
    assert_eq!(restored_record.author, Some(signature("Grace")));
    assert_eq!(restored_record.committer, None);
    let retention = graph.research_version_retention().unwrap();
    assert_eq!(retention.ancestors(restored), vec![second, first]);
}

#[test]
fn branch_operations_record_their_specified_parents() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("p");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph
        .execute("CREATE (:Character {name:'Shared'}), (:Story {name:'Outside'})")
        .unwrap();
    let cancel = CancellationToken::new();
    // Create from current research: the base descends from the captured origin.
    let origin = Uuid::now_v7();
    let create = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Current {
            origin_version_uuid: origin,
            context_uuid: Uuid::now_v7(),
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 1,
        label: "commits".into(),
        author: Some(signature("Ada")),
        committer: Some(signature("Grace")),
    };
    graph.create_research_branch(&create, &cancel).unwrap();
    let branch = create.branch_uuid;
    let base = record(&graph, create.version_uuid);
    assert_eq!(base.parents, vec![origin]);
    assert_eq!(base.author, Some(signature("Ada")));
    assert_eq!(base.committer, Some(signature("Grace")));
    // Execute: the prior head; signatures are the request's, never inherited.
    let execute = ExecuteResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: branch,
        version_uuid: Uuid::now_v7(),
        query: "MATCH (c:Character) SET c.mood = 'calm'".into(),
        created_at: 2,
        author: None,
        committer: Some(signature("Linus")),
    };
    graph.execute_research_branch(&execute, &cancel).unwrap();
    let edited = record(&graph, execute.version_uuid);
    assert_eq!(edited.parents, vec![create.version_uuid]);
    assert_eq!(edited.author, None);
    assert_eq!(edited.committer, Some(signature("Linus")));
    assert_eq!(edited.provenance, None);
    // Reference: the prior head.
    graph.execute("CREATE (:Story {name:'New'})").unwrap();
    let source = capture(&mut graph, Uuid::now_v7(), None);
    let reference = ReferenceResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: branch,
        version_uuid: Uuid::now_v7(),
        reference_uuid: Uuid::now_v7(),
        source_version_uuid: source,
        label: "cited".into(),
        created_at: 3,
        author: Some(signature("Ada")),
        committer: None,
    };
    graph
        .reference_research_branch(&reference, &cancel)
        .unwrap();
    let referenced = record(&graph, reference.version_uuid);
    assert_eq!(referenced.parents, vec![execute.version_uuid]);
    assert_eq!(referenced.author, Some(signature("Ada")));
    // Bring: the prior head, with the Slice source as provenance.
    let bring = BringResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: branch,
        version_uuid: Uuid::now_v7(),
        frozen_ipc: frozen(
            &graph,
            source,
            "MATCH (s:Story {name:'New'}) RETURN s.node_uuid AS node_uuid",
        ),
        created_at: 4,
        author: None,
        committer: None,
    };
    graph.bring_research_branch(&bring, &cancel).unwrap();
    let brought = record(&graph, bring.version_uuid);
    assert_eq!(brought.parents, vec![reference.version_uuid]);
    assert_eq!(
        brought.provenance,
        Some(ResearchVersionProvenance::Brought {
            version_uuid: source
        })
    );
    // Restore: the prior head, with the restored source as provenance; the
    // restored record's own parents and signatures are not copied.
    let restore = RestoreResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: branch,
        source_version_uuid: create.version_uuid,
        version_uuid: Uuid::now_v7(),
        created_at: 5,
        author: None,
        committer: Some(signature("Linus")),
    };
    graph.restore_research_branch(&restore, &cancel).unwrap();
    let restored = record(&graph, restore.version_uuid);
    assert_eq!(restored.parents, vec![bring.version_uuid]);
    assert_eq!(restored.author, None);
    assert_eq!(restored.committer, Some(signature("Linus")));
    assert_eq!(
        restored.provenance,
        Some(ResearchVersionProvenance::Restored {
            version_uuid: create.version_uuid
        })
    );
    assert_eq!(head(&graph, branch), restore.version_uuid);
    // The whole descent survives reopen.
    drop(graph);
    let graph = GraphForge::new(root.to_str()).unwrap();
    assert_eq!(
        graph
            .research_version_retention()
            .unwrap()
            .ancestors(restore.version_uuid),
        vec![
            bring.version_uuid,
            reference.version_uuid,
            execute.version_uuid,
            create.version_uuid,
            origin
        ]
    );
}

#[test]
fn invalid_signatures_are_refused_before_publication() {
    let mut graph = GraphForge::new(None).unwrap();
    graph.execute("CREATE (:Item)").unwrap();
    let before = current(&graph);
    let padded = ResearchSignature {
        name: "Ada ".into(),
        email: None,
        orcid: None,
    };
    let error = graph
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
            label: None,
            description: None,
            created_at: 1,
            required_versions: Default::default(),
            author: Some(padded.clone()),
            committer: None,
        })
        .unwrap_err();
    assert!(error.to_string().contains("signature name"), "{error}");
    let bad_orcid = ResearchSignature {
        orcid: Some("0000-0002-1825-0098".into()),
        ..signature("Ada")
    };
    let error = graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: before,
                branch_uuid: Uuid::now_v7(),
                version_uuid: Uuid::now_v7(),
                source: BranchSource::Current {
                    origin_version_uuid: Uuid::now_v7(),
                    context_uuid: Uuid::now_v7(),
                },
                creator_uuid: Uuid::now_v7(),
                created_at: 1,
                label: "refused".into(),
                author: None,
                committer: Some(bad_orcid),
            },
            &CancellationToken::new(),
        )
        .unwrap_err();
    assert!(error.to_string().contains("ORCID"), "{error}");
    assert_eq!(current(&graph), before);
    assert!(
        graph
            .research_version_retention()
            .unwrap()
            .identities
            .is_empty()
    );
}

#[test]
fn requests_without_signatures_keep_their_serialized_intent() {
    // Absent signatures are omitted, so existing callers' request bytes (and
    // their replay intent digests) are unchanged.
    let request = ExecuteResearchBranchRequest {
        operation_uuid: Uuid::nil(),
        expected_generation_uuid: Uuid::nil(),
        branch_uuid: Uuid::nil(),
        version_uuid: Uuid::nil(),
        query: String::new(),
        created_at: 0,
        author: None,
        committer: None,
    };
    let json = serde_json::to_value(&request).unwrap();
    assert!(json.get("author").is_none() && json.get("committer").is_none());
    let parsed: ExecuteResearchBranchRequest = serde_json::from_value(serde_json::json!({
        "operation_uuid": Uuid::nil(),
        "expected_generation_uuid": Uuid::nil(),
        "branch_uuid": Uuid::nil(),
        "version_uuid": Uuid::nil(),
        "query": "",
        "created_at": 0,
        "author": {"name": "Ada", "orcid": "0000-0002-1825-0097"},
    }))
    .unwrap();
    assert_eq!(parsed.author.unwrap().orcid.unwrap(), "0000-0002-1825-0097");
}

#[test]
fn exported_research_carries_the_walkable_descent_of_released_versions() {
    let directory = tempfile::tempdir().unwrap();
    let mut source = GraphForge::new(directory.path().join("source").to_str()).unwrap();
    source.execute("CREATE (:Item {n: 0})").unwrap();
    let cancel = CancellationToken::new();
    let origin = Uuid::now_v7();
    let create = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&source),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Current {
            origin_version_uuid: origin,
            context_uuid: Uuid::now_v7(),
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 1,
        label: "exported".into(),
        author: None,
        committer: None,
    };
    source.create_research_branch(&create, &cancel).unwrap();
    let mut edits = Vec::new();
    for n in 1..=2 {
        let request = ExecuteResearchBranchRequest {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(&source),
            branch_uuid: create.branch_uuid,
            version_uuid: Uuid::now_v7(),
            query: format!("MATCH (i:Item) SET i.n = {n}"),
            created_at: 2,
            author: Some(signature("Ada")),
            committer: None,
        };
        source.execute_research_branch(&request, &cancel).unwrap();
        edits.push(request.version_uuid);
    }
    // Release the intermediate Version's payload; its ancestry stays.
    source
        .commit_research_version_operation(
            ResearchOperation {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&source),
                mutation: ResearchMutation::DeleteVersion {
                    version_uuid: edits[0],
                },
            },
            &cancel,
        )
        .unwrap();
    let descent = vec![edits[0], create.version_uuid, origin];
    let registry = source.research_version_retention().unwrap();
    assert!(!registry.versions.contains_key(&edits[0]));
    assert_eq!(registry.ancestors(edits[1]), descent);
    let package = directory.path().join("package");
    source
        .export_research(
            &ExportResearchRequest {
                version_uuid: edits[1],
                output: package.clone(),
                bundled: false,
                projection: None,
            },
            &cancel,
        )
        .unwrap();
    let target = directory.path().join("imported");
    GraphForge::import_portable_v2(
        &target,
        &PortableV2ImportRequest {
            input: package,
            operation_id: OperationId(Uuid::now_v7()),
            limits: Default::default(),
        },
        None,
    )
    .unwrap();
    let imported = GraphForge::new(target.to_str())
        .unwrap()
        .research_version_retention()
        .unwrap();
    let archive = &imported.interchange[&edits[1]];
    assert_eq!(archive.ancestry, registry.ancestry);
    assert_eq!(imported.ancestors(edits[1]), descent);
    assert_eq!(imported.versions[&edits[1]].author, Some(signature("Ada")));
    for id in &descent {
        assert_eq!(imported.identities[id], registry.identities[id]);
    }
}
