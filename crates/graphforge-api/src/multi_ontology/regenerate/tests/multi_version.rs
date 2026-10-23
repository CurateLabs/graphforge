//! A refusal authorizes a permanent ID, including every adopted exact identity.
use super::*;

fn create_module(graph: &GraphForge, version: &str, name: &str) -> crate::ModuleCandidate {
    let document = serde_json::from_value(serde_json::json!({
        "ontology_id":uri(OLD),"version":version,
        "entity_types":[{"name":name}],
        "relation_types":[{"name":format!("{name}Link"),"src":name,"dst":name}],
        "properties":[{"owner":name,"name":"value","type":"utf8"},
            {"owner":format!("{name}Link"),"name":"weight","type":"int64"}]
    }))
    .unwrap();
    graph
        .create_ontology_module(document, vec![], Some(ActivationMode::Advisory))
        .unwrap()
}

fn adopt_bridge(graph: &mut GraphForge, document: BridgeDocument) -> BridgeSetId {
    let candidate = graph.create_ontology_bridge(document).unwrap();
    let id = candidate.id.clone();
    graph
        .adopt_ontology_bridge(
            &BridgeAdoptionRequest {
                authority: authority(graph),
                candidate,
            },
            None,
        )
        .unwrap();
    let (profile_default, mut activation) = graph.ontology_activation_profile().unwrap();
    activation.push(graphforge_ontology::ActivationRecord {
        scope: graphforge_ontology::ActivationScope::Bridge,
        subject: id.display_ref(),
        mode: ActivationMode::Advisory,
    });
    graph
        .change_ontology_activation_profile(
            &crate::ActivationProfileChangeRequest {
                authority: authority(graph),
                profile_default,
                activation,
            },
            None,
        )
        .unwrap();
    id
}

fn assert_history(
    graph: &GraphForge,
    prior: &graphforge_storage::research_versions::ResearchRegistry,
    branch: Uuid,
    result: &ItemIdRegeneration,
) {
    let registry = graph.research_version_retention().unwrap();
    assert_eq!(registry.versions.len(), prior.versions.len() + 1);
    assert_eq!(registry.heads[&branch], result.version_uuid);
    assert_eq!(registry.current_branch, prior.current_branch);
    for (id, head) in &prior.heads {
        if *id != branch {
            assert_eq!(registry.heads[id], *head);
        }
    }
    for (id, version) in &prior.versions {
        assert_eq!(registry.versions[id], *version);
    }
    assert_eq!(
        registry.versions[&result.version_uuid].parents,
        vec![prior.heads[&branch]]
    );
}

fn assert_field_credit(
    graph: &GraphForge,
    base: Uuid,
    branch: Uuid,
    identities: &[(&str, String, String)],
) {
    let old = crate::research_versions::materialize_version(
        graph,
        &graph.research_version(base).unwrap(),
    )
    .unwrap();
    let old_rows = crate::branches::baseline::read(&old).unwrap();
    let view = graph.open_research_branch(branch).unwrap();
    let new_rows = crate::branches::baseline::read(view.graph()).unwrap();
    for row in old_rows.values() {
        let mut key = row.key.clone();
        for (kind, old, new) in identities {
            if key.0 == *kind
                && key.1 == crate::branches::context_fields::context_identity(kind, old)
            {
                key.1 = crate::branches::context_fields::context_identity(kind, new);
                break;
            }
        }
        let next = &new_rows[&key];
        assert_eq!(next.origin, row.origin);
        assert_eq!(next.incorporated, row.incorporated);
        assert_eq!(next.original, row.original);
        assert_eq!(next.baseline, row.baseline);
        assert_eq!(next.contribution, row.contribution);
    }
}

#[test]
fn rewritten_module_dependencies_use_target_digest_order_in_both_directions() {
    let graph = GraphForge::new(None).unwrap();
    let mut source = crate::multi_ontology::candidate::empty_composition().unwrap();
    let mut ids = Vec::new();
    for name in ["Beta", "Gamma"] {
        let mut document = create_module(&graph, "2", name).document;
        document.ontology_id = uri(OTHER);
        let candidate = graph
            .create_ontology_module(document, vec![], None)
            .unwrap();
        ids.push(candidate.id.clone());
        source
            .modules
            .push(graphforge_storage::WorkspaceCompositionModule {
                id: candidate.id,
                document: candidate.document,
                dependencies: vec![],
                allow_projected_identity: false,
            });
    }
    let candidate = graph.create_ontology_module(serde_json::from_value(serde_json::json!({
        "ontology_id":"https://example.org/dependent","version":"1","entity_types":[{"name":"Other"}]
    })).unwrap(), ids.clone(), None).unwrap();
    let dependent = candidate.id.clone();
    source
        .modules
        .push(graphforge_storage::WorkspaceCompositionModule {
            id: candidate.id,
            document: candidate.document,
            dependencies: candidate.dependencies,
            allow_projected_identity: false,
        });
    let compiled = compile_candidate(&source).unwrap();
    let source = WorkspaceOntologyComposition::from_compiled(&compiled, source.bridges);
    // Fixed document/ID inputs exercise both orders without depending on the
    // random ID produced by the durable facade test. The second ID is the CI
    // case whose target digest ordering reverses the prior dependency order.
    for (next, order) in [
        (OLD, std::cmp::Ordering::Greater),
        ("ont_9pnk3pntnkskn0ja", std::cmp::Ordering::Less),
    ] {
        let rewritten = graph
            .regenerate_composition(
                &source,
                &ItemId::parse_reference(OTHER).unwrap(),
                next,
                Uuid::from_u128(1),
            )
            .unwrap();
        let replacements = ids
            .iter()
            .map(|id| {
                rewritten
                    .modules
                    .iter()
                    .find(|(old, _)| old == id)
                    .unwrap()
                    .1
                    .clone()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            replacements[0]
                .canonical_digest
                .cmp(&replacements[1].canonical_digest),
            order
        );
        let mut expected = replacements;
        expected.sort_by_key(crate::OntologyModuleId::sort_key);
        let actual = &rewritten
            .composition
            .modules
            .iter()
            .find(|module| module.id == dependent)
            .unwrap()
            .dependencies;
        assert_eq!(actual, &expected);
        assert_eq!(actual.len(), 2);
        assert!(actual[0].sort_key() < actual[1].sort_key());
    }
}

#[test]
fn all_refused_module_identities_rewrite_exact_references_and_reopen_data() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("project");
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    // Module identities include the digest even when authored versions coincide.
    let versions = [("1", "Alpha"), ("2", "Beta"), ("2", "Gamma")];
    let mut candidates = versions
        .iter()
        .map(|(version, name)| create_module(&graph, version, name))
        .collect::<Vec<_>>();
    for index in 1..candidates.len() {
        candidates[index].dependencies = vec![candidates[index - 1].id.clone()];
    }
    let modules = candidates
        .iter()
        .map(|module| module.id.clone())
        .collect::<Vec<_>>();
    let other_candidate = graph
        .create_ontology_module(
            serde_json::from_value(serde_json::json!({
                "ontology_id":uri(OTHER),"version":"1","entity_types":[{"name":"Other"}]
            }))
            .unwrap(),
            modules.clone(),
            Some(ActivationMode::Advisory),
        )
        .unwrap();
    let other = other_candidate.id.clone();
    let mut candidate = crate::multi_ontology::candidate::empty_composition().unwrap();
    candidate.modules = candidates
        .into_iter()
        .chain([other_candidate])
        .map(|module| graphforge_storage::WorkspaceCompositionModule {
            id: module.id,
            document: module.document,
            dependencies: module.dependencies,
            allow_projected_identity: false,
        })
        .collect();
    for module in &candidate.modules {
        candidate
            .activation
            .push(graphforge_ontology::ActivationRecord {
                scope: graphforge_ontology::ActivationScope::Module,
                subject: module.id.display_ref(),
                mode: ActivationMode::Advisory,
            });
    }
    for (index, old) in modules.iter().enumerate() {
        let name = versions[index].1;
        let doc: BridgeDocument = serde_json::from_value(serde_json::json!({
            "bridge_id":uri(BRIDGE),"authored_version":index.to_string(),
            "source_modules":[old],"target_modules":[other],"dependencies":candidate.bridges.last().map(bridge_id).transpose().unwrap().into_iter().collect::<Vec<_>>(),
            "shared_surfaces":["research_actions"],"enforcement":"advisory",
            "assertions":[{"source":{"module":old,"kind":"entity","local_id":name},
                "target":{"module":other,"kind":"entity","local_id":"Other"},
                "predicate":"related","directional":true,
                "provenance":{"method":"authored","justification":"version-specific symbols","evidence_refs":[]}}]
        })).unwrap();
        candidate
            .activation
            .push(graphforge_ontology::ActivationRecord {
                scope: graphforge_ontology::ActivationScope::Bridge,
                subject: bridge_id(&doc).unwrap().display_ref(),
                mode: ActivationMode::Advisory,
            });
        candidate.bridges.push(doc);
    }
    let compiled = compile_candidate(&candidate).unwrap();
    let candidate = WorkspaceOntologyComposition::from_compiled(&compiled, candidate.bridges);
    let request = crate::CompositionChangeRequest {
        context: WriteContext {
            operation_uuid: OperationId(Uuid::now_v7()),
            actor_uuid: None,
        },
        expected_project_generation_uuid: current(&graph),
        expected_composition_fingerprint: graph
            .ontology_authority_state()
            .unwrap()
            .composition_fingerprint,
        candidate,
        data_disposition: crate::CompositionDataDisposition::RequireConforming,
    };
    let preview = graph
        .preview_ontology_composition_change(&request, None)
        .unwrap();
    graph
        .publish_ontology_composition_change(&request, &preview, None)
        .unwrap();
    for (_, name) in versions {
        graph.execute(&format!(
            "CREATE (a:`{name}` {{value:'{name} value'}}),(b:`{name}` {{value:'target'}}),(a)-[:`{name}Link` {{weight:17}}]->(b)"
        )).unwrap();
    }
    let mut metadata = graph.research_project_metadata().unwrap();
    metadata.ontologies = vec![uri(OLD), uri(OTHER)];
    graph
        .update_research_metadata(UpdateResearchMetadataRequest {
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            metadata,
        })
        .unwrap();
    let (selected, base) = branch(&mut graph, "main");
    let (sibling, _) = branch(&mut graph, "sibling");
    graph
        .select_research_branch(
            &SelectResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&graph),
                branch_uuid: selected,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    refusal(&graph, OLD, base, false);
    let before = graph.workspace_ontology_composition().unwrap().unwrap();
    let prior = graph.research_version_retention().unwrap();
    let operation = Uuid::now_v7();
    let result = graph.regenerate_item_id(OLD, operation, None).unwrap();
    assert_history(&graph, &prior, selected, &result);
    let after = graph.workspace_ontology_composition().unwrap().unwrap();
    assert!(!serde_json::to_string(&after).unwrap().contains(&uri(OLD)));
    assert_ne!(
        before.composition_fingerprint,
        after.composition_fingerprint
    );
    let replacement = ItemId::parse_reference(&result.id).unwrap();
    assert_eq!(replacement.kind(), ItemKind::OntologyModule);
    let mut expected_ids = Vec::new();
    let mut module_pairs = Vec::new();
    let mut identities = vec![("ontology_module", uri(OLD), replacement.to_uri())];
    for old in &before.modules {
        let mut expected = old.document.clone();
        if old.id.ontology_id == uri(OLD) {
            expected.ontology_id = replacement.to_uri();
        }
        let new_id = crate::OntologyModuleId {
            ontology_id: expected.ontology_id.clone(),
            authored_version: old.id.authored_version.clone(),
            canonical_digest: graphforge_ontology::module_document_digest(&expected).unwrap(),
        };
        let new = after
            .modules
            .iter()
            .find(|module| module.id == new_id)
            .unwrap();
        assert_eq!(new.document, expected);
        module_pairs.push((old.id.clone(), new_id.clone()));
        if old.id.ontology_id == uri(OLD) {
            expected_ids.push(new_id.clone());
            identities.push((
                "ontology_activation",
                format!("Module:{}", old.id.display_ref()),
                format!("Module:{}", new_id.display_ref()),
            ));
            assert!(
                after
                    .activation
                    .iter()
                    .any(|record| record.subject == new_id.display_ref())
            );
        }
    }
    for (old, new) in &module_pairs {
        let source = before
            .modules
            .iter()
            .find(|module| module.id == *old)
            .unwrap();
        let target = after
            .modules
            .iter()
            .find(|module| module.id == *new)
            .unwrap();
        let mut expected = source
            .dependencies
            .iter()
            .map(|id| {
                module_pairs
                    .iter()
                    .find(|(prior, _)| prior == id)
                    .unwrap()
                    .1
                    .clone()
            })
            .collect::<Vec<_>>();
        // Compilation canonicalizes the rewritten identities. Changing the ID
        // can reverse the digest order of modules sharing an authored version.
        expected.sort_by_key(crate::OntologyModuleId::sort_key);
        assert_eq!(target.dependencies, expected);
        assert!(
            target
                .dependencies
                .windows(2)
                .all(|pair| pair[0].sort_key() < pair[1].sort_key())
        );
    }
    let mut bridge_pairs: Vec<(BridgeSetId, BridgeSetId)> = Vec::new();
    for old in &before.bridges {
        let mut expected = old.clone();
        let new_module = &module_pairs
            .iter()
            .find(|(id, _)| *id == old.source_modules[0])
            .unwrap()
            .1;
        expected.source_modules = vec![new_module.clone()];
        expected.assertions[0].source.module = new_module.clone();
        expected.dependencies = old
            .dependencies
            .iter()
            .map(|id| {
                bridge_pairs
                    .iter()
                    .find(|(prior, _)| prior == id)
                    .unwrap()
                    .1
                    .clone()
            })
            .collect();
        assert!(after.bridges.contains(&expected));
        let new_id = bridge_id(&expected).unwrap();
        assert!(
            after
                .activation
                .iter()
                .any(|record| record.subject == new_id.display_ref())
        );
        identities.push((
            "ontology_activation",
            format!("Bridge:{}", bridge_id(old).unwrap().display_ref()),
            format!("Bridge:{}", new_id.display_ref()),
        ));
        bridge_pairs.push((bridge_id(old).unwrap(), new_id));
    }
    assert_field_credit(&graph, base, selected, &identities);
    let mut metadata = graph.research_project_metadata().unwrap().ontologies;
    metadata.sort();
    let mut expected_metadata = vec![replacement.to_uri(), uri(OTHER)];
    expected_metadata.sort();
    assert_eq!(metadata, expected_metadata);
    let dependent = after
        .modules
        .iter()
        .find(|module| module.id == other)
        .unwrap();
    expected_ids.sort_by_key(crate::OntologyModuleId::sort_key);
    assert_eq!(dependent.dependencies, expected_ids);
    assert_eq!(
        graph
            .open_research_branch(sibling)
            .unwrap()
            .graph()
            .workspace_ontology_composition()
            .unwrap()
            .unwrap(),
        before
    );
    drop(graph);
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    assert_eq!(
        graph.workspace_ontology_composition().unwrap().unwrap(),
        after
    );
    for (_, name) in versions {
        let values = graph
            .execute(&format!(
                "MATCH (a:`{name}`)-[r:`{name}Link`]->() RETURN a.value,r.weight"
            ))
            .unwrap();
        assert_eq!(
            values.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            format!("{name} value")
        );
        assert_eq!(
            values.batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            17
        );
        graph.execute_research_branch(&ExecuteResearchBranchRequest {
            operation_uuid:Uuid::now_v7(),expected_generation_uuid:current(&graph),branch_uuid:selected,version_uuid:Uuid::now_v7(),
            query:format!("MATCH (a:`{name}`)-[r:`{name}Link`]->() SET a.value='{name} edited',r.weight=23"),
            created_at:4,label:None,description:None,author:None,committer:None,
        }, &CancellationToken::new()).unwrap();
    }
    let generation = current(&graph);
    assert_eq!(
        graph.regenerate_item_id(OLD, operation, None).unwrap(),
        result
    );
    assert_eq!(current(&graph), generation);
    drop(graph);
    let graph = GraphForge::new(path.to_str()).unwrap();
    let view = graph.open_research_branch(selected).unwrap();
    let graph = view.graph();
    for (_, name) in versions {
        let values = graph
            .execute(&format!(
                "MATCH (a:`{name}`)-[r:`{name}Link`]->() RETURN a.value,r.weight"
            ))
            .unwrap();
        assert_eq!(
            values.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            format!("{name} edited")
        );
        assert_eq!(
            values.batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            23
        );
    }
}

#[test]
fn all_refused_bridge_identities_rewrite_exact_dependencies_and_activation() {
    let mut fixture = Fixture::new(true);
    let graph = &mut fixture.graph;
    let before = graph.workspace_ontology_composition().unwrap().unwrap();
    let original = before
        .bridges
        .iter()
        .find(|bridge| bridge.bridge_id == uri(BRIDGE))
        .unwrap();
    let mut variants = vec![bridge_id(original).unwrap()];
    for (version, justification) in [("2", "second version"), ("3", "third version")] {
        let mut document = original.clone();
        document.authored_version = version.into();
        document.assertions[0].provenance.justification = justification.into();
        variants.push(adopt_bridge(graph, document));
    }
    let mut dependent = original.clone();
    dependent.bridge_id = uri("brg_dddddddddddddddd");
    dependent.dependencies = variants.clone();
    adopt_bridge(graph, dependent);
    let (selected, base) = branch(graph, "bridge-versions");
    refusal(graph, BRIDGE, base, false);
    let before = graph.workspace_ontology_composition().unwrap().unwrap();
    let prior = graph.research_version_retention().unwrap();
    let operation = Uuid::now_v7();
    let result = graph.regenerate_item_id(BRIDGE, operation, None).unwrap();
    assert_history(graph, &prior, selected, &result);
    let after = graph.workspace_ontology_composition().unwrap().unwrap();
    let next = ItemId::parse_reference(&result.id).unwrap();
    assert_eq!(next.kind(), ItemKind::BridgeSet);
    assert_eq!(after.modules, before.modules);
    assert!(
        !serde_json::to_string(&after)
            .unwrap()
            .contains(&uri(BRIDGE))
    );
    let mut expected_ids = Vec::new();
    let mut identities = vec![("ontology_bridge", uri(BRIDGE), next.to_uri())];
    for old in before
        .bridges
        .iter()
        .filter(|bridge| bridge.bridge_id == uri(BRIDGE))
    {
        let mut expected = old.clone();
        expected.bridge_id = next.to_uri();
        assert!(after.bridges.contains(&expected));
        let id = bridge_id(&expected).unwrap();
        identities.push((
            "ontology_activation",
            format!("Bridge:{}", bridge_id(old).unwrap().display_ref()),
            format!("Bridge:{}", id.display_ref()),
        ));
        assert!(
            after
                .activation
                .iter()
                .any(|record| record.subject == id.display_ref())
        );
        expected_ids.push(id);
    }
    let dependent = after
        .bridges
        .iter()
        .find(|bridge| bridge.bridge_id == uri("brg_dddddddddddddddd"))
        .unwrap();
    expected_ids.sort_by_key(BridgeSetId::sort_key);
    let mut dependencies = dependent.dependencies.clone();
    dependencies.sort_by_key(BridgeSetId::sort_key);
    assert_eq!(dependencies, expected_ids);
    for bridge in &after.bridges {
        assert!(
            after
                .activation
                .iter()
                .any(|record| record.subject == bridge_id(bridge).unwrap().display_ref())
        );
        for dep in &bridge.dependencies {
            assert!(
                after
                    .bridges
                    .iter()
                    .any(|doc| bridge_id(doc).unwrap() == *dep)
            );
        }
    }
    for old in before
        .bridges
        .iter()
        .filter(|bridge| bridge.bridge_id != uri(BRIDGE))
    {
        let new = after
            .bridges
            .iter()
            .find(|bridge| {
                bridge.bridge_id == old.bridge_id && bridge.authored_version == old.authored_version
            })
            .unwrap();
        identities.push((
            "ontology_activation",
            format!("Bridge:{}", bridge_id(old).unwrap().display_ref()),
            format!("Bridge:{}", bridge_id(new).unwrap().display_ref()),
        ));
    }
    assert_field_credit(graph, base, selected, &identities);
    let path = fixture.path();
    drop(fixture.graph);
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    assert_eq!(
        graph.workspace_ontology_composition().unwrap().unwrap(),
        after
    );
    data(&graph);
    let generation = current(&graph);
    assert_eq!(
        graph.regenerate_item_id(BRIDGE, operation, None).unwrap(),
        result
    );
    assert_eq!(current(&graph), generation);
    let edited = Uuid::now_v7();
    graph
        .execute_research_branch(
            &ExecuteResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&graph),
                branch_uuid: selected,
                version_uuid: edited,
                query: "MATCH ()-[r:LINK]->() SET r.weight=23".into(),
                created_at: 4,
                label: None,
                description: None,
                author: None,
                committer: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    assert_eq!(
        graph.regenerate_item_id(BRIDGE, operation, None).unwrap(),
        result
    );
    assert_eq!(
        graph.research_version_retention().unwrap().heads[&selected],
        edited
    );
    drop(graph);
    let graph = GraphForge::new(path.to_str()).unwrap();
    let view = graph.open_research_branch(selected).unwrap();
    let values = view
        .graph()
        .execute("MATCH ()-[r:LINK]->() RETURN r.weight")
        .unwrap();
    assert_eq!(
        values.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        23
    );
}
