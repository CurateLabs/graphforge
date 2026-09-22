use super::fixture::*;
use graphforge_api::*;
use serde_json::json;
use uuid::Uuid;
impl Journey {
    pub fn share_and_reopen(&mut self, accepted: &super::review::Accepted) {
        let version = self.head(self.a);
        let reference = self
            .graph
            .research_reference(
                &ResearchReferenceTarget::Version {
                    version_uuid: version,
                },
                &cancel(),
            )
            .unwrap();
        self.output.json("export-source-reference", &reference);
        let ontology = self
            .graph
            .open_research_version(version)
            .unwrap()
            .workspace_ontology()
            .unwrap();
        let package = self.owner.path().join("complete");
        let export = self
            .graph
            .export_research(
                &ExportResearchRequest {
                    version_uuid: version,
                    output: package.clone(),
                    bundled: false,
                    projection: None,
                },
                &cancel(),
            )
            .unwrap();
        let verified = verify_portable_v2(
            &PortableVerifyRequest {
                input: package.clone(),
                mode: PortableV2Mode::Full,
                limits: Default::default(),
            },
            None,
        )
        .unwrap();
        assert_eq!(verified.package_digest, export.package_digest);
        assert!(verified.research_interchange);
        self.output.json("complete-verification", &verified);
        let target = self.owner.path().join("imported");
        let import = PortableV2ImportRequest {
            input: package,
            operation_id: OperationId(Uuid::now_v7()),
            limits: Default::default(),
        };
        let first = GraphForge::import_portable_v2(&target, &import, None).unwrap();
        let replay = GraphForge::import_portable_v2(&target, &import, None).unwrap();
        assert!(replay.idempotent_replay);
        assert_eq!(first.generation_uuid, replay.generation_uuid);
        graphforge_storage::execute_project_cleanup(
            &target,
            graphforge_storage::ProjectRetentionPolicy {
                retained_ancestors: 0,
            },
            Default::default(),
        )
        .unwrap();
        let imported = GraphForge::new(target.to_str()).unwrap();
        let after = imported
            .research_reference(
                &ResearchReferenceTarget::Version {
                    version_uuid: version,
                },
                &cancel(),
            )
            .unwrap();
        assert_eq!(imported.workspace_ontology().unwrap(), ontology);
        assert_eq!(after.version, reference.version);
        assert_eq!(after.genealogy, reference.genealogy);
        let historical = imported.open_research_version(version).unwrap();
        assert_eq!(
            historical
                .artifact_payload(self.external)
                .unwrap_err()
                .code(),
            "GF_RESULT_NOT_RETAINED"
        );
        self.output.arrow(
            "imported-external-artifact",
            &historical.artifact(self.external).unwrap(),
        );
        assert_eq!(
            payload(&historical.artifact_payload(self.ocr).unwrap()),
            self.text("ocr_text").as_bytes()
        );
        assert_eq!(
            payload(&historical.artifact_payload(self.scan).unwrap()),
            self.text("scan_bytes").as_bytes()
        );
        assert_eq!(
            integer(
                &imported
                    .execute("MATCH(n:Character) RETURN n.score AS score")
                    .unwrap(),
                "score"
            ),
            1
        );
        assert!(
            imported
                .research_reference(
                    &ResearchReferenceTarget::Branch {
                        branch_uuid: self.a
                    },
                    &cancel()
                )
                .is_err()
        );
        self.output.json("imported-reference", &after);
        drop(historical);
        drop(imported);
        self.project_and_fork(version);
        // Reopen the original durable Project after compaction, retaining both heads.
        let registry = self.graph.research_version_retention().unwrap();
        self.graph
            .commit_research_version_operation(
                ResearchOperation {
                    operation_uuid: Uuid::now_v7(),
                    expected_generation_uuid: self.generation(),
                    mutation: ResearchMutation::Compact {
                        versions: registry.heads.values().copied().collect(),
                    },
                },
                &cancel(),
            )
            .unwrap();
        let ontology_before = self.graph.workspace_ontology().unwrap();
        graphforge_storage::execute_project_cleanup(
            &self.root,
            graphforge_storage::ProjectRetentionPolicy {
                retained_ancestors: 0,
            },
            Default::default(),
        )
        .unwrap();
        let mut reopened = GraphForge::new(self.root.to_str()).unwrap();
        assert_eq!(reopened.workspace_ontology().unwrap(), ontology_before);
        let before_retry = reopened
            .research_project_summary()
            .unwrap()
            .identity
            .generation_uuid;
        assert_eq!(
            reopened
                .review_research_proposal(&accepted.review, &cancel())
                .unwrap(),
            accepted.receipt
        );
        assert_eq!(
            reopened
                .research_project_summary()
                .unwrap()
                .identity
                .generation_uuid,
            before_retry
        );
        let mut changed = accepted.review.clone();
        changed.explanation = "Changed request after cleanup".into();
        assert_eq!(
            reopened
                .review_research_proposal(&changed, &cancel())
                .unwrap_err()
                .code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );
        assert_eq!(
            reopened
                .research_project_summary()
                .unwrap()
                .identity
                .generation_uuid,
            before_retry
        );
        for (branch, score) in [(self.a, 1), (self.b, 73)] {
            let view = reopened.open_research_branch(branch).unwrap();
            assert_eq!(
                reopened
                    .open_research_version(view.version_uuid())
                    .unwrap()
                    .artifact_payload(self.external)
                    .unwrap_err()
                    .code(),
                "GF_RESULT_NOT_RETAINED"
            );
            assert_eq!(
                payload(
                    &reopened
                        .open_research_version(view.version_uuid())
                        .unwrap()
                        .artifact_payload(self.scan)
                        .unwrap()
                ),
                self.text("scan_bytes").as_bytes()
            );
            assert_eq!(
                integer(
                    &view
                        .graph()
                        .execute("MATCH(n:Character) RETURN n.score AS score")
                        .unwrap(),
                    "score"
                ),
                score
            );
            assert_eq!(
                payload(
                    &reopened
                        .open_research_version(view.version_uuid())
                        .unwrap()
                        .artifact_payload(self.ocr)
                        .unwrap()
                ),
                self.text("ocr_text").as_bytes()
            );
        }
        assert_eq!(
            integer(
                &reopened
                    .execute("MATCH(n:Character) RETURN n.score AS score")
                    .unwrap(),
                "score"
            ),
            99
        );
    }
    fn project_and_fork(&mut self, version: Uuid) {
        let projected = Uuid::now_v7();
        let output = self.owner.path().join("selected");
        self.graph
            .export_research(
                &ExportResearchRequest {
                    version_uuid: version,
                    output: output.clone(),
                    bundled: false,
                    projection: Some(ResearchExportProjection {
                        version_uuid: projected,
                        frozen_ipc: self.node_capsule(version),
                        fields: vec![ResearchFieldIdentity {
                            object_kind: "node".into(),
                            object_uuid: self.ada,
                            field: "property:score".into(),
                        }],
                        created_at: 8,
                    }),
                },
                &cancel(),
            )
            .unwrap();
        let target = self.owner.path().join("selected-import");
        GraphForge::import_portable_v2(
            &target,
            &PortableV2ImportRequest {
                input: output,
                operation_id: OperationId(Uuid::now_v7()),
                limits: Default::default(),
            },
            None,
        )
        .unwrap();
        let selected = GraphForge::new(target.to_str()).unwrap();
        let citation = selected
            .research_reference(
                &ResearchReferenceTarget::Version {
                    version_uuid: projected,
                },
                &cancel(),
            )
            .unwrap();
        assert_ne!(citation.version.version_uuid, version);
        assert_eq!(citation.version.content.source_version, Some(version));
        self.output.json("selected-reference", &citation);
        let values = selected
            .execute("MATCH(n:Character) RETURN n.score AS score,n.private_note IS NULL AS private_omitted")
            .unwrap();
        assert_eq!(integer(&values, "score"), 1);
        assert!(
            values.batches[0]
                .column_by_name("private_omitted")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .unwrap()
                .value(0)
        );
        drop(selected);
        let mut metadata = self.graph.research_project_metadata().unwrap();
        metadata.title = Some(self.text("fork_title").into());
        metadata.access.visibility = Some("private".into());
        metadata.access.access_policy = Some("Independent review".into());
        let fork: ForkResearchRequest = request(
            json!({"operation_uuid":Uuid::now_v7(),"project_uuid":Uuid::now_v7(),"version_uuid":version,"projection":null,"target":self.owner.path().join("fork"),"actor_uuid":self.actor,"governance":"Independent review","adopt_selected_ontology":true,"metadata":metadata}),
        );
        let first = self.graph.fork_research(&fork, &cancel()).unwrap();
        let replay = self.graph.fork_research(&fork, &cancel()).unwrap();
        assert!(replay.idempotent_replay);
        assert_eq!(first.generation_uuid, replay.generation_uuid);
        let independent = GraphForge::new(fork.target.to_str()).unwrap();
        assert_eq!(independent.research_project_metadata().unwrap(), metadata);
        let citation = independent
            .research_reference(
                &ResearchReferenceTarget::Version {
                    version_uuid: version,
                },
                &cancel(),
            )
            .unwrap();
        assert_eq!(citation.project_uuid, fork.project_uuid);
        assert_ne!(citation.origin_project_uuid, citation.project_uuid);
        self.output.json("fork-reference", &citation);
        independent
            .execute("MATCH(n:Character) SET n.score=77")
            .unwrap();
        assert_eq!(
            integer(
                &independent
                    .execute("MATCH(n:Character) RETURN n.score AS score")
                    .unwrap(),
                "score"
            ),
            77
        );
        assert_eq!(
            integer(
                &self
                    .graph
                    .open_research_branch(self.a)
                    .unwrap()
                    .graph()
                    .execute("MATCH(n:Character) RETURN n.score AS score")
                    .unwrap(),
                "score"
            ),
            1
        );
    }
}
