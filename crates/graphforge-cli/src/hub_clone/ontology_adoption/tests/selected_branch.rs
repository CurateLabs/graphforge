use super::*;

pub(super) fn selected_main(graph: &mut GraphForge) -> Uuid {
    let branch = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: graph
                    .committed_generation_identity()
                    .unwrap()
                    .generation_uuid,
                branch_uuid: branch,
                version_uuid: Uuid::now_v7(),
                source: BranchSource::Current {
                    origin_version_uuid: Uuid::now_v7(),
                    context_uuid: Uuid::now_v7(),
                },
                creator_uuid: Uuid::now_v7(),
                created_at: 1,
                label: "main".into(),
                author: None,
                committer: None,
                description: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    graph
        .select_research_branch(
            &SelectResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: graph
                    .committed_generation_identity()
                    .unwrap()
                    .generation_uuid,
                branch_uuid: branch,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    branch
}

#[test]
fn adopt_command_retains_existing_selected_branch_authority_and_source() {
    let published = Published::new();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("project");
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    graph.execute("CREATE (:Retained {value:42})").unwrap();
    let row = retained_row(&graph);
    let branch = selected_main(&mut graph);
    let before = graph.research_version_retention().unwrap();
    drop(graph);
    let command = published.command("adopt", &path);
    let receipt = run(&published.transport(), &command).unwrap();
    let graph = published.assert_project(&path);
    let after = graph.research_version_retention().unwrap();
    assert_ne!(after.heads[&branch], before.heads[&branch]);
    assert_eq!(after.current_branch, Some(branch));
    assert_eq!(after.branches, before.branches);
    assert_eq!(retained_row(&graph), row);
    let head = graph.open_research_branch(branch).unwrap();
    assert_eq!(head.version_uuid(), after.heads[&branch]);
    assert_eq!(
        head.graph().workspace_ontology_composition().unwrap(),
        graph.workspace_ontology_composition().unwrap()
    );
    assert_eq!(
        head.graph().research_project_metadata().unwrap(),
        graph.research_project_metadata().unwrap()
    );
    assert_eq!(retained_row(head.graph()), row);
    assert_eq!(
        receipt["adoption"]["project_generation_uuid"],
        json!(
            graph
                .committed_generation_identity()
                .unwrap()
                .generation_uuid
        )
    );
    drop(head);
    drop(graph);
    let retry = run(&published.transport(), &command).unwrap();
    assert_eq!(retry["adoption"]["already_adopted"], true);
    assert_eq!(
        retry["adoption"]["project_generation_uuid"],
        receipt["adoption"]["project_generation_uuid"]
    );
    assert_eq!(
        GraphForge::new(path.to_str())
            .unwrap()
            .research_version_retention()
            .unwrap()
            .heads,
        after.heads
    );
}

fn resolve(published: &Published, root: &Path) -> graphforge_api::ResolvedOntologyModule {
    module_fetch::resolve_module_with(
        &published.transport(),
        &ModuleFetchArgs {
            repository: "opendata/openalex".into(),
            ontology_id: Some(published.module.ontology_id.clone()),
            version: Some(published.module.authored_version.clone()),
            digest: Some(format!("sha256:{}", published.module.canonical_digest)),
            hub: None,
            protection_bypass_env: None,
            output: root.join(Uuid::now_v7().to_string()),
        },
        &TEST_RETRY_POLICY,
    )
    .unwrap()
    .0
}

fn context() -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(Uuid::now_v7()),
        actor_uuid: None,
    }
}

#[test]
fn legacy_workspace_only_adoption_recovers_head_and_keeps_earliest_source_on_exact_retries() {
    let published = Published::new();
    let republished = Published::new();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("project");
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    graph.execute("CREATE (:Retained {value:42})").unwrap();
    let branch = selected_main(&mut graph);
    let before = graph.research_version_retention().unwrap();
    let first = resolve(&published, root.path());
    let second = resolve(&republished, root.path());
    assert_ne!(first.package_digest(), second.package_digest());
    let original = graph
        .adopt_resolved_ontology_module(&first, context(), None)
        .unwrap();
    // Programmatic workspace-only semantics are deliberately unchanged.
    assert_eq!(
        graph.research_version_retention().unwrap().heads,
        before.heads
    );
    drop(graph);
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    let operation = context();
    let completed = graph
        .adopt_resolved_ontology_module_for_publication(&second, operation.clone(), None)
        .unwrap();
    assert!(!completed.already_adopted);
    assert_eq!(completed.package_digest, original.package_digest);
    assert_eq!(completed.repository, original.repository);
    let after = graph.research_version_retention().unwrap();
    assert_ne!(after.heads[&branch], before.heads[&branch]);
    let head = graph.open_research_branch(branch).unwrap();
    assert_eq!(
        head.graph().workspace_ontology_composition().unwrap(),
        graph.workspace_ontology_composition().unwrap()
    );
    assert_eq!(
        head.graph().research_project_metadata().unwrap(),
        graph.research_project_metadata().unwrap()
    );
    drop(head);
    drop(graph);
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    for retry in [operation, context()] {
        let receipt = graph
            .adopt_resolved_ontology_module_for_publication(&second, retry, None)
            .unwrap();
        assert!(receipt.already_adopted);
        assert_eq!(
            receipt.project_generation_uuid,
            completed.project_generation_uuid
        );
        assert_eq!(receipt.package_digest, original.package_digest);
        assert_eq!(
            graph.research_version_retention().unwrap().heads,
            after.heads
        );
    }
    // Return the present authority, even after a later independent workspace edit.
    graph.execute("CREATE (:Another)").unwrap();
    let current = graph
        .committed_generation_identity()
        .unwrap()
        .generation_uuid;
    let receipt = graph
        .adopt_resolved_ontology_module_for_publication(&second, context(), None)
        .unwrap();
    assert!(receipt.already_adopted);
    assert_eq!(receipt.project_generation_uuid, current);
    assert_eq!(
        graph.research_version_retention().unwrap().heads,
        after.heads
    );
}

#[test]
fn stale_cancelled_and_conflicting_branch_adoptions_preserve_all_authority() {
    let published = Published::new();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("project");
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    graph.execute("CREATE (:Retained {value:42})").unwrap();
    let branch = selected_main(&mut graph);
    let mut stale = GraphForge::new(path.to_str()).unwrap();
    let resolved = resolve(&published, root.path());
    graph.execute("CREATE (:Another)").unwrap();
    let generation = graph.committed_generation_identity().unwrap();
    let registry = graph.research_version_retention().unwrap();
    assert!(
        stale
            .adopt_resolved_ontology_module_for_publication(&resolved, context(), None)
            .unwrap_err()
            .to_string()
            .contains("reopen")
    );
    let token = CancellationToken::new();
    token.cancel();
    assert_eq!(
        graph
            .adopt_resolved_ontology_module_for_publication(&resolved, context(), Some(&token))
            .unwrap_err()
            .code(),
        "GF_CANCELLED"
    );
    assert_eq!(graph.committed_generation_identity().unwrap(), generation);
    assert_eq!(
        graph.research_version_retention().unwrap().heads,
        registry.heads
    );
    assert!(graph.workspace_ontology_composition().unwrap().is_none());

    let head = graph.open_research_branch(branch).unwrap();
    let candidate = head
        .graph()
        .create_ontology_module(
            document("https://graphforge.sh/ont_cccccccccccccccc", "BranchOnly"),
            vec![],
            None,
        )
        .unwrap();
    let composition = graphforge_storage::WorkspaceOntologyComposition::from_authored(
        vec![graphforge_storage::WorkspaceCompositionModule {
            id: candidate.id,
            document: candidate.document,
            dependencies: candidate.dependencies,
            allow_projected_identity: false,
        }],
        vec![],
        vec![],
        graphforge_api::ActivationMode::Exploratory,
    )
    .unwrap();
    graph
        .change_research_branch_ontology(
            &graphforge_api::ChangeResearchBranchOntologyRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: generation.generation_uuid,
                branch_uuid: branch,
                expected_composition_fingerprint: None,
                version_uuid: Uuid::now_v7(),
                created_at: 2,
                candidate: composition,
                data_disposition: graphforge_api::CompositionDataDisposition::RequireConforming,
                author: None,
                committer: None,
                label: None,
                description: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let generation = graph.committed_generation_identity().unwrap();
    let before = graph.research_version_retention().unwrap();
    assert!(
        graph
            .adopt_resolved_ontology_module_for_publication(&resolved, context(), None)
            .unwrap_err()
            .to_string()
            .contains("conflict")
    );
    assert_eq!(graph.committed_generation_identity().unwrap(), generation);
    assert_eq!(
        graph.research_version_retention().unwrap().heads,
        before.heads
    );
    assert!(graph.workspace_ontology_composition().unwrap().is_none());
    assert!(
        graph
            .research_project_metadata()
            .unwrap()
            .extensions
            .is_empty()
    );
    assert_eq!(retained_row(&graph).1, 42);
}

#[test]
fn adoption_preserves_independent_rows_metadata_modules_sibling_and_version_lineage() {
    let published = Published::new();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("project");
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    let unrelated = adopt(
        &mut graph,
        serde_json::from_value(json!({"ontology_id":"https://graphforge.sh/ont_cccccccccccccccc", "version":"1", "entity_types":[{"name":"Retained"}], "properties":[{"owner":"Retained", "name":"value", "type":"int64"}]})).unwrap(),
        vec![],
    );
    graph
        .change_ontology_activation_profile(
            &graphforge_api::ActivationProfileChangeRequest {
                authority: authority(&graph),
                profile_default: graphforge_api::ActivationMode::Exploratory,
                activation: vec![graphforge_api::ActivationRecord {
                    scope: graphforge_api::ActivationScope::Module,
                    subject: unrelated.display_ref(),
                    mode: graphforge_api::ActivationMode::Strict,
                }],
            },
            None,
        )
        .unwrap();
    let enforcement = graph.ontology_activation_profile().unwrap();
    graph.execute("CREATE (:Retained {value:42})").unwrap();
    let mut metadata = graph.research_project_metadata().unwrap();
    metadata
        .extensions
        .insert("user.annotation".into(), json!("retain this"));
    graph
        .update_research_metadata(graphforge_api::UpdateResearchMetadataRequest {
            context: context(),
            metadata,
        })
        .unwrap();
    let branch = selected_main(&mut graph);
    let sibling = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: graph
                    .committed_generation_identity()
                    .unwrap()
                    .generation_uuid,
                branch_uuid: sibling,
                version_uuid: Uuid::now_v7(),
                source: BranchSource::Branch {
                    branch_uuid: branch,
                },
                creator_uuid: Uuid::now_v7(),
                created_at: 2,
                label: "sibling".into(),
                author: None,
                committer: None,
                description: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    // A Branch edit must survive adoption without replacing the workspace rows.
    graph
        .execute_research_branch(
            &graphforge_api::ExecuteResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: graph
                    .committed_generation_identity()
                    .unwrap()
                    .generation_uuid,
                branch_uuid: branch,
                version_uuid: Uuid::now_v7(),
                created_at: 3,
                query: "MATCH (n:Retained) SET n.value = 43".into(),
                author: None,
                committer: None,
                label: None,
                description: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let before = graph.research_version_retention().unwrap();
    let original = graph.research_version(before.heads[&branch]).unwrap();
    let workspace_row = retained_row(&graph);
    let head_row = retained_row(graph.open_research_branch(branch).unwrap().graph());
    assert_eq!(workspace_row.1, 42);
    assert_eq!(head_row.1, 43);
    drop(graph);
    let receipt = run(&published.transport(), &published.command("adopt", &path)).unwrap();
    let graph = GraphForge::new(path.to_str()).unwrap();
    let after = graph.research_version_retention().unwrap();
    assert_eq!(after.current_branch, Some(branch));
    assert_eq!(after.branches, before.branches);
    assert_eq!(after.heads[&sibling], before.heads[&sibling]);
    for (id, prior) in &before.versions {
        assert_eq!(&graph.research_version(*id).unwrap(), prior);
    }
    let version = graph.research_version(after.heads[&branch]).unwrap();
    assert_eq!(version.parents, [original.version_uuid]);
    assert_eq!(
        version.content.source_version,
        original.content.source_version
    );
    assert_eq!(
        version.content.required_versions,
        original.content.required_versions
    );
    assert_eq!(retained_row(&graph), workspace_row);
    assert_eq!(
        retained_row(graph.open_research_branch(sibling).unwrap().graph()),
        workspace_row
    );
    let head = graph.open_research_branch(branch).unwrap();
    assert_eq!(retained_row(head.graph()), head_row);
    assert_eq!(
        head.graph().workspace_ontology_composition().unwrap(),
        graph.workspace_ontology_composition().unwrap()
    );
    assert_eq!(
        head.graph().research_project_metadata().unwrap(),
        graph.research_project_metadata().unwrap()
    );
    let composition = graph.workspace_ontology_composition().unwrap().unwrap();
    assert_eq!(composition.profile_default, enforcement.0);
    assert!(
        enforcement
            .1
            .iter()
            .all(|record| composition.activation.contains(record))
    );
    assert_eq!(composition.modules.len(), 3);
    assert!(
        composition
            .modules
            .iter()
            .any(|module| module.id == unrelated)
    );
    assert!(
        composition
            .modules
            .iter()
            .any(|module| module.id == published.module
                && module.dependencies == [published.dependency.clone()])
    );
    assert_eq!(composition.bridges, [published.bridge]);
    assert_eq!(
        receipt["adoption"]["composition_fingerprint"],
        composition.composition_fingerprint
    );
    let metadata = graph.research_project_metadata().unwrap();
    assert_eq!(metadata.extensions["user.annotation"], "retain this");
    assert_eq!(receipt["adoption"]["modules"].as_array().unwrap().len(), 2);
    // The child reopens independently, exports this exact Version and fully verifies
    // its composition/provenance by importing the portable package and reopening.
    let expected = root.path().join("expected.json");
    std::fs::write(
        &expected,
        serde_json::to_vec(&json!({
            "version": version.version_uuid, "branch": branch, "composition": composition,
            "metadata": metadata, "row": head_row,
        }))
        .unwrap(),
    )
    .unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "hub_clone::ontology_adoption::tests::selected_branch::selected_adoption_reopen_child",
            "--nocapture",
        ])
        .env("GF_ADOPTION_VERIFY_PROJECT", &path)
        .env("GF_ADOPTION_VERIFY_EXPECTED", expected)
        .status()
        .unwrap();
    assert!(status.success());
}

#[test]
fn selected_adoption_reopen_child() {
    let Ok(path) = std::env::var("GF_ADOPTION_VERIFY_PROJECT") else {
        return;
    };
    let expected: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("GF_ADOPTION_VERIFY_EXPECTED").unwrap()).unwrap(),
    )
    .unwrap();
    let graph = GraphForge::new(Some(&path)).unwrap();
    let branch = serde_json::from_value(expected["branch"].clone()).unwrap();
    let version = serde_json::from_value(expected["version"].clone()).unwrap();
    assert_eq!(
        graph.research_version_retention().unwrap().heads[&branch],
        version
    );
    let output = Path::new(&path).parent().unwrap().join("selected.gfpb");
    graph
        .export_research(
            &graphforge_api::ExportResearchRequest {
                version_uuid: version,
                output: output.clone(),
                bundled: true,
                projection: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let report = graphforge_api::verify_portable(
        &graphforge_api::PortableVerifyRequest {
            input: output.clone(),
            mode: graphforge_api::PortableMode::Full,
            limits: PortableLimits::default(),
        },
        None,
    )
    .unwrap();
    assert_eq!(
        report.ontology_composition,
        graph
            .research_version_portable_ontology_composition(version, PortableLimits::default())
            .unwrap()
    );
    let imported = Path::new(&path).parent().unwrap().join("imported");
    GraphForge::import_portable(
        &imported,
        &graphforge_api::PortableImportRequest {
            input: output,
            operation_id: OperationId(Uuid::now_v7()),
            limits: PortableLimits::default(),
        },
        None,
    )
    .unwrap();
    let target = GraphForge::new(imported.to_str()).unwrap();
    assert_eq!(
        json!(target.workspace_ontology_composition().unwrap().unwrap()),
        expected["composition"]
    );
    assert_eq!(
        json!(target.research_project_metadata().unwrap()),
        expected["metadata"]
    );
    assert_eq!(json!(retained_row(&target)), expected["row"]);
}

#[test]
fn selected_adoption_fault_child() {
    let Ok(path) = std::env::var("GF_ADOPTION_FAULT_PROJECT") else {
        return;
    };
    let root = PathBuf::from(std::env::var("GF_ADOPTION_FAULT_FIXTURE").unwrap());
    let published = load_published(&root);
    let resolved = resolve(&published, root.parent().unwrap());
    let operation = WriteContext {
        operation_uuid: OperationId(Uuid::from_u128(0xad0_710)),
        actor_uuid: None,
    };
    let mut graph = GraphForge::new(Some(&path)).unwrap();
    let result =
        graph.adopt_resolved_ontology_module_for_publication(&resolved, operation.clone(), None);
    let phase = std::env::var("GRAPHFORGE_PROJECT_FAILPOINT").unwrap();
    assert!(
        phase.ends_with(".error"),
        "crash failpoint must terminate the process"
    );
    assert_eq!(result.unwrap_err().code(), "GF_PUBLICATION_FAILED");
    let committed = phase.contains("after_current");
    let registry = graph.research_version_retention().unwrap();
    let head = graph
        .open_research_branch(registry.current_branch.unwrap())
        .unwrap();
    assert_eq!(
        graph.workspace_ontology_composition().unwrap().is_some(),
        committed
    );
    assert_eq!(
        head.graph()
            .workspace_ontology_composition()
            .unwrap()
            .is_some(),
        committed
    );
    assert_eq!(
        head.graph().research_project_metadata().unwrap(),
        graph.research_project_metadata().unwrap()
    );
    assert_eq!(retained_row(&graph).1, 42);
    // One-shot failure permits an exact same-context retry in this live facade.
    let receipt = graph
        .adopt_resolved_ontology_module_for_publication(&resolved, operation, None)
        .unwrap();
    assert_eq!(receipt.already_adopted, committed);
}

#[test]
fn final_current_failures_keep_workspace_provenance_and_head_atomic_and_retryable() {
    let published = Published::new();
    for phase in [
        "project.before_current_replace",
        "project.after_current_replace",
        "project.before_current_replace.error",
        "project.after_current_replace.error",
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("project");
        let mut graph = GraphForge::new(path.to_str()).unwrap();
        graph.execute("CREATE (:Retained {value:42})").unwrap();
        let branch = selected_main(&mut graph);
        let before = graph.research_version_retention().unwrap();
        drop(graph);
        let fixture = root.path().join("fixture");
        save_published(&published, &fixture);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "hub_clone::ontology_adoption::tests::selected_branch::selected_adoption_fault_child", "--nocapture"])
            .env("GF_ADOPTION_FAULT_PROJECT", &path).env("GF_ADOPTION_FAULT_FIXTURE", fixture)
            .env("GRAPHFORGE_PROJECT_FAILPOINTS", "graphforge-internal-subprocess-v1")
            .env("GRAPHFORGE_PROJECT_FAILPOINT", phase)
            .env("GRAPHFORGE_PROJECT_FAILPOINT_ONCE", "1")
            .env("GRAPHFORGE_PROJECT_FAILPOINT_TRANSACTION", Uuid::from_u128(0xad0_710).to_string())
            .status().unwrap();
        assert_eq!(
            status.code(),
            Some(if phase.ends_with(".error") { 0 } else { 86 }),
            "{phase}"
        );
        let graph = GraphForge::new(path.to_str()).unwrap();
        let committed = phase.contains("after_current") || phase.ends_with(".error");
        assert_eq!(
            graph.workspace_ontology_composition().unwrap().is_some(),
            committed,
            "{phase}"
        );
        let after = graph.research_version_retention().unwrap();
        assert_eq!(after.heads[&branch] != before.heads[&branch], committed);
        assert_eq!(after.branches, before.branches);
        let head = graph.open_research_branch(branch).unwrap();
        assert_eq!(
            head.graph().workspace_ontology_composition().unwrap(),
            graph.workspace_ontology_composition().unwrap()
        );
        assert_eq!(
            head.graph().research_project_metadata().unwrap(),
            graph.research_project_metadata().unwrap()
        );
        assert_eq!(retained_row(&graph).1, 42);
        assert_eq!(retained_row(head.graph()).1, 42);
        drop(head);
        drop(graph);
        let command = published.command("adopt", &path);
        let retry = run(&published.transport(), &command).unwrap();
        assert_eq!(retry["adoption"]["already_adopted"], committed);
        let final_retry = run(&published.transport(), &command).unwrap();
        assert!(
            final_retry["adoption"]["already_adopted"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(
            retry["adoption"]["project_generation_uuid"],
            final_retry["adoption"]["project_generation_uuid"]
        );
    }
}

#[test]
fn cancellation_stale_current_and_native_failure_during_preparation_never_publish_adoption() {
    let published = Published::new();
    for fault in ["cancel", "stale", "failure"] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("project");
        let mut graph = GraphForge::new(path.to_str()).unwrap();
        graph.execute("CREATE (:Retained {value:42})").unwrap();
        let branch = selected_main(&mut graph);
        let before = graph.research_version_retention().unwrap();
        let generation = graph.committed_generation_identity().unwrap();
        let resolved = resolve(&published, root.path());
        let token = CancellationToken::new();
        let cancelling = token.clone();
        let competing = path.clone();
        // The existing write-clock boundary runs after the head is privately
        // prepared and before the complete workspace draft is captured.
        graph = graph.with_write_clock(move || match fault {
            "cancel" => {
                cancelling.cancel();
                Ok(9)
            }
            "stale" => {
                GraphForge::new(competing.to_str())?.execute("CREATE (:Concurrent)")?;
                Ok(9)
            }
            "failure" => Err(GfError::Execution(
                "injected native write-clock failure".into(),
            )),
            _ => unreachable!(),
        });
        let error = graph
            .adopt_resolved_ontology_module_for_publication(&resolved, context(), Some(&token))
            .unwrap_err();
        if fault == "cancel" {
            assert_eq!(error.code(), "GF_CANCELLED");
        }
        drop(graph);
        let reopened = GraphForge::new(path.to_str()).unwrap();
        assert_eq!(
            reopened.research_version_retention().unwrap().heads,
            before.heads
        );
        assert_eq!(
            reopened.research_version_retention().unwrap().branches,
            before.branches
        );
        assert_eq!(
            reopened.committed_generation_identity().unwrap() != generation,
            fault == "stale"
        );
        assert!(reopened.workspace_ontology_composition().unwrap().is_none());
        assert!(
            reopened
                .research_project_metadata()
                .unwrap()
                .extensions
                .is_empty()
        );
        assert!(
            reopened
                .open_research_branch(branch)
                .unwrap()
                .graph()
                .workspace_ontology_composition()
                .unwrap()
                .is_none()
        );
        assert_eq!(retained_row(&reopened).1, 42);
    }
}

#[test]
fn corrupt_fetched_closure_cannot_change_an_existing_selected_head() {
    let published = Published::new();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("project");
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    graph.execute("CREATE (:Retained {value:42})").unwrap();
    selected_main(&mut graph);
    let before = graph.research_version_retention().unwrap();
    let generation = graph.committed_generation_identity().unwrap();
    drop(graph);
    let transport = PublicTransport {
        inner: Scripted::new(vec![
            response(200, None, REFS),
            response(200, None, &published.manifest),
            response(200, None, b"corrupt package"),
        ]),
        requested: Mutex::new(vec![]),
    };
    assert!(run(&transport, &published.command("adopt", &path)).is_err());
    let graph = GraphForge::new(path.to_str()).unwrap();
    assert_eq!(graph.committed_generation_identity().unwrap(), generation);
    assert_eq!(
        graph.research_version_retention().unwrap().heads,
        before.heads
    );
    assert!(graph.workspace_ontology_composition().unwrap().is_none());
    assert!(
        graph
            .research_project_metadata()
            .unwrap()
            .extensions
            .is_empty()
    );
    assert_eq!(retained_row(&graph).1, 42);
}
