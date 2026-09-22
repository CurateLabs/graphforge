use arrow::{
    array::{BinaryArray, FixedSizeBinaryArray, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use graphforge_api::*;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{cell::RefCell, path::PathBuf};
use uuid::Uuid;

pub fn cancel() -> CancellationToken {
    CancellationToken::new()
}
pub fn request<T: DeserializeOwned>(value: Value) -> T {
    serde_json::from_value(value).unwrap()
}
pub fn context() -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(Uuid::now_v7()),
        actor_uuid: None,
    }
}
pub fn string(batch: &RecordBatch, name: &str, row: usize) -> String {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(row)
        .to_owned()
}
pub fn binary_uuid(batch: &RecordBatch, name: &str, row: usize) -> Uuid {
    Uuid::from_slice(
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(row),
    )
    .unwrap()
}
pub fn integer(result: &ExecutionResult, name: &str) -> i64 {
    result.batches[0]
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}
pub fn payload(result: &ExecutionResult) -> Vec<u8> {
    result.batches[0]
        .column_by_name("payload")
        .unwrap()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .value(0)
        .to_vec()
}
pub fn hex_bytes(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}
pub fn ipc(result: &ExecutionResult) -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &result.schema).unwrap();
        for b in &result.batches {
            writer.write(b).unwrap();
        }
        writer.finish().unwrap();
    }
    bytes
}

pub struct Output {
    root: Option<PathBuf>,
    entries: RefCell<Vec<Value>>,
}
impl Output {
    fn new() -> Self {
        let root = std::env::var_os("GRAPHFORGE_JOURNEY_CAPTURE_DIR").map(PathBuf::from);
        if let Some(root) = &root {
            std::fs::create_dir_all(root).unwrap();
        }
        Self {
            root,
            entries: RefCell::new(vec![]),
        }
    }
    fn write(&self, name: &str, extension: &str, bytes: &[u8]) {
        use sha2::{Digest, Sha256};
        if let Some(root) = &self.root {
            let file = format!("{name}.{extension}");
            std::fs::write(root.join(&file), bytes).unwrap();
            self.entries.borrow_mut().push(json!({"file":file,"bytes":bytes.len(),"sha256":Sha256::digest(bytes).iter().map(|byte| format!("{byte:02x}")).collect::<String>()}));
        }
    }
    pub fn arrow(&self, name: &str, result: &ExecutionResult) {
        self.write(name, "arrow", &ipc(result));
    }
    pub fn json(&self, name: &str, value: &impl Serialize) {
        self.write(name, "json", &serde_json::to_vec_pretty(value).unwrap());
    }
    pub fn finish(&self, j: &Journey) {
        if let Some(root) = &self.root {
            let manifest = json!({"contract":"graphforge-analyst-journey-output/1","reader":{"crate_version":env!("CARGO_PKG_VERSION"),"research_registry":6,"interchange":1,"portable":2},"identities":{"character":j.ada,"mystery_branch":j.a,"voyage_branch":j.b,"source":j.source,"scan":j.scan,"ocr":j.ocr,"external":j.external,"claim":j.claim},"files":self.entries.borrow().clone(),"qualification":"Native Arrow IPC and serialized control outputs; cancelled-reference and restoration-outcome are derived assertion summaries, not native response schemas. Generated identities are preserved; the identity map names fixture roles. Synthetic scan/OCR inputs do not claim OCR execution or human comprehension."});
            std::fs::write(
                root.join("manifest.json"),
                serde_json::to_vec_pretty(&manifest).unwrap(),
            )
            .unwrap();
        }
    }
}

pub struct Journey {
    pub owner: tempfile::TempDir,
    pub root: PathBuf,
    pub graph: GraphForge,
    pub corpus: Value,
    pub a: Uuid,
    pub b: Uuid,
    pub ada: Uuid,
    pub actor: Uuid,
    pub source: Uuid,
    pub scan: Uuid,
    pub ocr: Uuid,
    pub external: Uuid,
    pub claim: Uuid,
    pub origin: Uuid,
    pub output: Output,
}
impl Journey {
    pub fn new() -> Self {
        let corpus: Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/analyst-journey-v1/corpus.json"
        ))
        .unwrap();
        let owner = tempfile::tempdir().unwrap();
        let root = owner.path().join("research");
        let graph = GraphForge::new(root.to_str()).unwrap();
        graph
            .execute(corpus["initial_graph"].as_str().unwrap())
            .unwrap();
        let ada = binary_uuid(
            &graph
                .execute("MATCH(n:Character) RETURN n.node_uuid AS id")
                .unwrap()
                .batches[0],
            "id",
            0,
        );
        let mut j = Self {
            owner,
            root,
            graph,
            corpus,
            a: Uuid::now_v7(),
            b: Uuid::now_v7(),
            ada,
            actor: Uuid::now_v7(),
            source: Uuid::now_v7(),
            scan: Uuid::now_v7(),
            ocr: Uuid::now_v7(),
            external: Uuid::now_v7(),
            claim: Uuid::now_v7(),
            origin: Uuid::now_v7(),
            output: Output::new(),
        };
        j.evidence();
        j
    }
    pub fn text(&self, key: &str) -> &str {
        self.corpus[key].as_str().unwrap()
    }
    pub fn generation(&self) -> Uuid {
        self.graph
            .research_project_summary()
            .unwrap()
            .identity
            .generation_uuid
    }
    pub fn head(&self, branch: Uuid) -> Uuid {
        self.graph
            .open_research_branch(branch)
            .unwrap()
            .version_uuid()
    }
    pub fn edit(&mut self, branch: Uuid, query: impl Into<String>) -> Uuid {
        let version = Uuid::now_v7();
        self.graph
            .execute_research_branch(
                &ExecuteResearchBranchRequest {
                    operation_uuid: Uuid::now_v7(),
                    expected_generation_uuid: self.generation(),
                    branch_uuid: branch,
                    version_uuid: version,
                    query: query.into(),
                    created_at: 4,
                },
                &cancel(),
            )
            .unwrap();
        version
    }
    fn evidence(&mut self) {
        for capability_id in [
            CapabilityId::Provenance,
            CapabilityId::Knowledge,
            CapabilityId::Epistemic,
        ] {
            self.graph
                .enable_capability(EnableCapabilityRequest {
                    context: context(),
                    capability_id,
                    capability_version: 1,
                })
                .unwrap();
        }
        self.graph
            .register_source(RegisterSourceRequest {
                context: context(),
                source_uuid: self.source,
                label: self.text("source_label").into(),
                source_kind: SourceKind::Manuscript,
                identity_uri: None,
            })
            .unwrap();
        for (id, kind, bytes, inputs) in [
            (
                self.scan,
                ArtifactKind::RawScan,
                self.text("scan_bytes").as_bytes().to_vec(),
                vec![],
            ),
            (
                self.ocr,
                ArtifactKind::OcrText,
                self.text("ocr_text").as_bytes().to_vec(),
                vec![DerivationInput {
                    input_uuid: self.scan,
                    input_kind: DerivationSubjectKind::Artifact,
                }],
            ),
        ] {
            self.graph
                .register_artifact(RegisterArtifactRequest {
                    context: context(),
                    source_uuid: self.source,
                    artifact_uuid: id,
                    artifact_kind: kind,
                    media_type: "text/plain".into(),
                    payload: ArtifactPayloadRequest::LocalBytes(bytes),
                    derivation_inputs: inputs,
                    run_uuid: None,
                })
                .unwrap();
        }
        self.graph
            .register_artifact(RegisterArtifactRequest {
                context: context(),
                source_uuid: self.source,
                artifact_uuid: self.external,
                artifact_kind: ArtifactKind::Other,
                media_type: "text/plain".into(),
                payload: ArtifactPayloadRequest::ExternalReference {
                    uri: self.text("external_reference").into(),
                    fingerprint: None,
                },
                derivation_inputs: vec![],
                run_uuid: None,
            })
            .unwrap();
        self.graph
            .set_preferred_artifact(SetPreferredArtifactRequest {
                context: context(),
                preference_event_uuid: Uuid::now_v7(),
                source_uuid: self.source,
                artifact_uuid: self.ocr,
                reason: "Reviewed transcription".into(),
            })
            .unwrap();
        self.graph.create_research_claim(&request(json!({"operation_uuid":Uuid::now_v7(),"expected_generation_uuid":self.generation(),"assertion_uuid":self.claim,"claim":self.text("claim"),"graph_refs":[{"graph_uuid":self.ada,"graph_kind":"node","role":"subject","ordinal":0}],"category":"analyst_assertion","creator_uuid":self.actor,"run_uuid":null,"created_at":1})),&cancel()).unwrap();
        self.graph
            .attach_evidence(AttachEvidenceRequest {
                context: context(),
                evidence_uuid: Uuid::now_v7(),
                assertion_uuid: self.claim,
                source_uuid: self.ocr,
                source_kind: EvidenceSourceKind::Artifact,
                role: EvidenceRole::Supports,
                weight: None,
            })
            .unwrap();
    }
    pub fn node_capsule(&self, version: Uuid) -> Vec<u8> {
        ipc(&self.graph.freeze_slice(&request(json!({"request_uuid":Uuid::now_v7(),"source":{"kind":"version","version_uuid":version},"selector":{"kind":"direct","members":{"nodes":[self.ada]}}})),&cancel()).unwrap())
    }
    pub fn explore_and_focus(&mut self) {
        let mut metadata = self.graph.research_project_metadata().unwrap();
        metadata.title = Some(self.text("title").into());
        self.graph
            .update_research_metadata(UpdateResearchMetadataRequest {
                context: context(),
                metadata,
            })
            .unwrap();
        self.output.json(
            "project-metadata",
            &self.graph.research_project_metadata().unwrap(),
        );
        let discovered = GraphForge::discover_research_projects(&DiscoverResearchProjectsRequest {
            project_roots: vec![self.root.clone()],
            query: Default::default(),
            limits: Default::default(),
        })
        .unwrap();
        assert_eq!(discovered.batches[0].num_rows(), 1);
        assert_eq!(
            string(&discovered.batches[0], "title", 0),
            self.text("title")
        );
        self.output.arrow("discovery", &discovered);
        self.output
            .arrow("source", &self.graph.source(self.source).unwrap());
        self.output
            .arrow("ocr-artifact", &self.graph.artifact(self.ocr).unwrap());
        self.output.arrow(
            "assertion",
            &self.graph.assertion(self.claim, None).unwrap(),
        );
        self.output.arrow(
            "claim-context",
            &self
                .graph
                .inspect_research_claims(&InspectResearchClaimsRequest {
                    context: ResearchContext::Project,
                    community_uuid: None,
                    include_suppressed: false,
                })
                .unwrap(),
        );
        self.output.arrow(
            "claim-evidence",
            &self
                .graph
                .research_claim_history(&ResearchClaimHistoryRequest {
                    context: ResearchContext::Project,
                    family: ResearchClaimHistoryKind::Evidence,
                    assertion_uuid: Some(self.claim),
                })
                .unwrap(),
        );
        let prepared = self
            .graph
            .prepare_research_version(PrepareResearchVersionRequest {
                operation_uuid: Uuid::now_v7(),
                version_uuid: self.origin,
                context_uuid: Uuid::now_v7(),
                label: Some("Two-story source".into()),
                description: None,
                created_at: 1,
                required_versions: Default::default(),
            })
            .unwrap();
        self.graph
            .commit_research_version_operation(prepared, &cancel())
            .unwrap();
        for (name, branch) in [("Mystery", self.a), ("Voyage", self.b)] {
            let node = binary_uuid(
                &self
                    .graph
                    .execute(&format!(
                        "MATCH(n:Story {{name:'{name}'}}) RETURN n.node_uuid AS id"
                    ))
                    .unwrap()
                    .batches[0],
                "id",
                0,
            );
            let slice: SliceRequest = request(
                json!({"request_uuid":Uuid::now_v7(),"source":{"kind":"version","version_uuid":self.origin},"selector":{"kind":"traverse","seeds":[node],"direction":"both","max_depth":1,"relationship_types":[]},"include":{"assertions":[self.claim],"artifacts":[self.external]}}),
            );
            let before = self.generation();
            for (kind, label) in [
                (SlicePageKind::Included, "included"),
                (SlicePageKind::Boundary, "boundary"),
                (SlicePageKind::Dependencies, "dependencies"),
                (SlicePageKind::Explanations, "explanations"),
            ] {
                let result = self
                    .graph
                    .preview_slice(&slice, kind, PageRequest::default())
                    .unwrap();
                if name == "Mystery" {
                    self.output.arrow(label, &result);
                }
                if kind == SlicePageKind::Included {
                    assert_eq!(
                        result
                            .batches
                            .iter()
                            .map(|b| (0..b.num_rows())
                                .filter(|i| ["node", "edge"]
                                    .contains(&string(b, "object_kind", *i).as_str()))
                                .count())
                            .sum::<usize>(),
                        3
                    );
                }
                if kind == SlicePageKind::Explanations {
                    let batch = &result.batches[0];
                    let row = (0..batch.num_rows())
                        .find(|i| string(batch, "object_uuid", *i) == self.ada.to_string())
                        .unwrap();
                    assert_eq!(string(batch, "reason", row), "traversal");
                    assert_eq!(string(batch, "root_uuid", row), node.to_string());
                    assert_eq!(string(batch, "predecessor_uuid", row), node.to_string());
                }
                if kind == SlicePageKind::Boundary {
                    let other = if name == "Mystery" {
                        "Voyage"
                    } else {
                        "Mystery"
                    };
                    let outside = binary_uuid(
                        &self
                            .graph
                            .execute(&format!(
                                "MATCH(n:Story {{name:'{other}'}}) RETURN n.node_uuid AS id"
                            ))
                            .unwrap()
                            .batches[0],
                        "id",
                        0,
                    );
                    assert!(
                        result
                            .batches
                            .iter()
                            .any(|b| (0..b.num_rows())
                                .any(|i| string(b, "object_uuid", i) == outside.to_string()))
                    );
                }
                if kind == SlicePageKind::Dependencies {
                    assert!(
                        result
                            .batches
                            .iter()
                            .any(|b| (0..b.num_rows())
                                .any(|i| string(b, "object_uuid", i) == self.ocr.to_string()))
                    );
                }
            }
            assert_eq!(self.generation(), before);
            let frozen = self.graph.freeze_slice(&slice, &cancel()).unwrap();
            self.graph
                .create_research_branch(
                    &CreateResearchBranchRequest {
                        operation_uuid: Uuid::now_v7(),
                        expected_generation_uuid: self.generation(),
                        branch_uuid: branch,
                        version_uuid: Uuid::now_v7(),
                        source: BranchSource::Slice {
                            frozen_ipc: ipc(&frozen),
                        },
                        creator_uuid: self.actor,
                        created_at: 2,
                        label: name.into(),
                    },
                    &cancel(),
                )
                .unwrap();
            let view = self.graph.open_research_branch(branch).unwrap();
            let external_view = self
                .graph
                .open_research_version(view.version_uuid())
                .unwrap();
            assert_eq!(
                external_view
                    .artifact_payload(self.external)
                    .unwrap_err()
                    .code(),
                "GF_RESULT_NOT_RETAINED"
            );
            if name == "Mystery" {
                self.output.arrow(
                    "external-artifact",
                    &external_view.artifact(self.external).unwrap(),
                );
            }

            assert_eq!(
                integer(
                    &view
                        .graph()
                        .execute("MATCH(n:Story) RETURN count(n) AS count")
                        .unwrap(),
                    "count"
                ),
                1
            );
            assert_eq!(
                payload(
                    &self
                        .graph
                        .open_research_version(view.version_uuid())
                        .unwrap()
                        .artifact_payload(self.ocr)
                        .unwrap()
                ),
                self.text("ocr_text").as_bytes()
            );
        }
        let before = self.generation();
        let token = cancel();
        token.cancel();
        let failure = self
            .graph
            .research_reference(
                &ResearchReferenceTarget::Branch {
                    branch_uuid: self.a,
                },
                &token,
            )
            .unwrap_err();
        assert_eq!(failure.code(), "GF_CANCELLED");
        assert_eq!(self.generation(), before);
        self.output.json(
            "cancelled-reference",
            &json!({"code":failure.code(),"authoritative_generation_unchanged":true}),
        );
    }
}
