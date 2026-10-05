//! Real facade derivation validation; bytes/rows are scoped to Parquet decoding.
use std::collections::{BTreeMap, HashMap};

use arrow::array::{FixedSizeBinaryArray, StringArray, UInt32Array};
use graphforge_api::{
    ArtifactKind, ArtifactPayloadRequest, AssertionGraphRefInput, AssertionGraphRole,
    AttachEvidenceRequest, CapabilityId, CreateAssertionRequest, DerivationInput,
    DerivationSubjectKind, EnableCapabilityRequest, EvidenceRole, EvidenceSourceKind, GraphForge,
    GraphObjectKind, LineageDirection, OperationId, PageRequest, RankAlgorithm, RankOptions,
    RecordedAlgorithmRequest, RegisterArtifactRequest, RegisterSourceRequest,
    ResearchLineageRequest, SourceKind, WriteContext,
};
use graphforge_storage::concurrency_attribution::RegionCapture;
use uuid::Uuid;

fn context() -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(Uuid::now_v7()),
        actor_uuid: None,
    }
}

fn request(source_uuid: Uuid, derivation_inputs: Vec<DerivationInput>) -> RegisterArtifactRequest {
    RegisterArtifactRequest {
        context: context(),
        artifact_uuid: Uuid::now_v7(),
        source_uuid,
        artifact_kind: ArtifactKind::OcrText,
        media_type: "text/plain".into(),
        payload: ArtifactPayloadRequest::Absent,
        derivation_inputs,
        run_uuid: None,
    }
}

struct Fixture {
    root: tempfile::TempDir,
    graph: GraphForge,
    source: Uuid,
    subjects: HashMap<DerivationSubjectKind, Vec<Uuid>>,
}

impl Fixture {
    fn new(rows: usize) -> Self {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        for capability_id in [CapabilityId::Provenance, CapabilityId::Knowledge] {
            graph
                .enable_capability(EnableCapabilityRequest {
                    context: context(),
                    capability_id,
                    capability_version: 1,
                })
                .unwrap();
        }
        let a = graph.add_node("Person", &HashMap::new()).unwrap();
        let b = graph.add_node("Person", &HashMap::new()).unwrap();
        let edge = graph.add_edge(&a, "KNOWS", &b, &HashMap::new()).unwrap();
        let mut subjects = HashMap::from([
            (DerivationSubjectKind::Node, vec![a.uuid, b.uuid]),
            (DerivationSubjectKind::Edge, vec![edge.uuid]),
        ]);
        for _ in 0..rows {
            let source = Uuid::now_v7();
            graph
                .register_source(RegisterSourceRequest {
                    context: context(),
                    source_uuid: source,
                    label: "Manuscript".into(),
                    source_kind: SourceKind::Manuscript,
                    identity_uri: None,
                })
                .unwrap();
            let artifact = request(source, vec![]);
            let artifact_id = artifact.artifact_uuid;
            graph.register_artifact(artifact).unwrap();
            let assertion = Uuid::now_v7();
            graph
                .create_assertion(CreateAssertionRequest {
                    context: context(),
                    assertion_uuid: assertion,
                    claim: "observed".into(),
                    graph_refs: vec![AssertionGraphRefInput {
                        graph_uuid: a.uuid,
                        graph_kind: GraphObjectKind::Node,
                        role: AssertionGraphRole::Subject,
                        ordinal: 0,
                    }],
                })
                .unwrap();
            let evidence = Uuid::now_v7();
            graph
                .attach_evidence(AttachEvidenceRequest {
                    context: context(),
                    evidence_uuid: evidence,
                    assertion_uuid: assertion,
                    source_uuid: source,
                    source_kind: EvidenceSourceKind::Source,
                    role: EvidenceRole::Supports,
                    weight: None,
                })
                .unwrap();
            let run = Uuid::now_v7();
            let descriptor = graph
                .prepare_rank_invocation(
                    "Person",
                    &RankOptions {
                        by: RankAlgorithm::Degree,
                        ..RankOptions::default()
                    },
                )
                .unwrap();
            graph
                .invoke_recorded(RecordedAlgorithmRequest {
                    context: context(),
                    run_uuid: run,
                    descriptor,
                    cancellation: None,
                })
                .unwrap();
            for (kind, id) in [
                (DerivationSubjectKind::Source, source),
                (DerivationSubjectKind::Artifact, artifact_id),
                (DerivationSubjectKind::Assertion, assertion),
                (DerivationSubjectKind::EvidenceLink, evidence),
                (DerivationSubjectKind::AlgorithmRun, run),
            ] {
                subjects.entry(kind).or_default().push(id);
            }
        }
        let source = subjects[&DerivationSubjectKind::Source][0];
        Self {
            root,
            graph,
            source,
            subjects,
        }
    }

    fn inputs(&self, kind: DerivationSubjectKind, count: usize) -> Vec<DerivationInput> {
        self.subjects[&kind]
            .iter()
            .cycle()
            .take(count)
            .map(|&input_uuid| DerivationInput {
                input_uuid,
                input_kind: kind,
            })
            .collect()
    }

    fn ledger_work(&self, kind: DerivationSubjectKind) -> (u64, u64) {
        let families: &[&str] = match kind {
            // The mandatory source precheck supplies the validation snapshot.
            DerivationSubjectKind::Source
            | DerivationSubjectKind::Node
            | DerivationSubjectKind::Edge => &[],
            DerivationSubjectKind::Artifact => &["artifacts"],
            DerivationSubjectKind::Assertion => &["assertions", "assertion_graph_refs"],
            DerivationSubjectKind::EvidenceLink => &["evidence"],
            DerivationSubjectKind::AlgorithmRun => &["algorithm_runs", "algorithm_run_events"],
        };
        let generation = graphforge_storage::resolve_project_generation(self.root.path()).unwrap();
        families
            .iter()
            .map(|family| {
                let snapshot = generation
                    .participant_snapshot("knowledge", family)
                    .unwrap()
                    .unwrap();
                if snapshot.row_count == 0 {
                    (0, 0)
                } else {
                    (snapshot.bytes.len() as u64, snapshot.row_count)
                }
            })
            .fold((0, 0), |a, b| (a.0 + b.0, a.1 + b.1))
    }
}

fn measured_register(graph: &GraphForge, request: RegisterArtifactRequest) -> (u64, u64) {
    let nodes = request
        .derivation_inputs
        .iter()
        .any(|input| input.input_kind == DerivationSubjectKind::Node);
    let edges = request
        .derivation_inputs
        .iter()
        .any(|input| input.input_kind == DerivationSubjectKind::Edge);
    let capture = RegionCapture::start("artifact_registration");
    assert_eq!(
        graph
            .register_artifact(request)
            .unwrap()
            .stats
            .rows_produced,
        1
    );
    let snapshot = capture.finish();
    let region = &snapshot.regions["artifact_registration/derivation_input_validation"];
    assert_eq!(region.calls, 1);
    assert_eq!(
        snapshot
            .regions
            .get("artifact_registration/derivation_input_validation/participant_materialization")
            .and_then(|row| row.work.get("participant_materialized_bytes"))
            .copied()
            .unwrap_or_default(),
        region.work.get("bytes").copied().unwrap_or_default(),
        "each nonempty validation snapshot is read and decoded exactly once"
    );
    assert_eq!(
        region.work.get("nodes").copied().unwrap_or_default(),
        if nodes { 2 } else { 0 }
    );
    assert_eq!(
        region.work.get("edges").copied().unwrap_or_default(),
        u64::from(edges)
    );
    (
        region.work.get("bytes").copied().unwrap_or_default(),
        region.work.get("rows").copied().unwrap_or_default(),
    )
}

#[test]
fn derivation_validation_decodes_each_required_kind_once() {
    let mut observations = Vec::new();
    for ledger_rows in [2, 8] {
        let fixture = Fixture::new(ledger_rows);
        for kind in [
            DerivationSubjectKind::Source,
            DerivationSubjectKind::Artifact,
            DerivationSubjectKind::Assertion,
            DerivationSubjectKind::EvidenceLink,
            DerivationSubjectKind::AlgorithmRun,
        ] {
            for input_count in [1, 16] {
                let expected = fixture.ledger_work(kind);
                let actual = measured_register(
                    &fixture.graph,
                    request(fixture.source, fixture.inputs(kind, input_count)),
                );
                eprintln!(
                    "ledger_rows={ledger_rows} kind={kind:?} inputs={input_count} decoded_bytes_rows={actual:?} single_load={expected:?}"
                );
                observations.push((kind, input_count, actual, expected));
            }
        }
        assert_eq!(
            measured_register(&fixture.graph, request(fixture.source, vec![])),
            (0, 0)
        );
    }
    for (kind, inputs, actual, expected) in observations {
        assert_eq!(
            actual, expected,
            "{kind:?}, {inputs} inputs must load only one required ledger"
        );
    }
}

fn assert_lineage(graph: &GraphForge, output: Uuid, inputs: &[DerivationInput]) {
    let result = graph
        .research_lineage(ResearchLineageRequest {
            subject_uuid: output,
            subject_kind: DerivationSubjectKind::Artifact,
            direction: LineageDirection::Backward,
            max_depth: 1,
            page: PageRequest::default(),
        })
        .unwrap();
    let mut rows = BTreeMap::new();
    for batch in result.batches {
        let ids = batch
            .column_by_name("input_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let kinds = batch
            .column_by_name("input_kind")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ordinals = batch
            .column_by_name("ordinal")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(
                rows.insert(
                    ordinals.value(row),
                    (
                        Uuid::from_slice(ids.value(row)).unwrap(),
                        kinds.value(row).to_owned()
                    )
                )
                .is_none()
            );
        }
    }
    assert_eq!(rows.len(), inputs.len());
    for (ordinal, input) in inputs.iter().enumerate() {
        assert_eq!(
            rows[&(ordinal as u32)],
            (input.input_uuid, input.input_kind.as_str().to_owned())
        );
    }
}

#[test]
fn mixed_derivation_inputs_preserve_order_duplicates_and_reopen() {
    let fixture = Fixture::new(2);
    let mut inputs = Vec::new();
    let mut expected = (0, 0);
    for kind in [
        DerivationSubjectKind::Edge,
        DerivationSubjectKind::Source,
        DerivationSubjectKind::Assertion,
        DerivationSubjectKind::Node,
        DerivationSubjectKind::EvidenceLink,
        DerivationSubjectKind::Artifact,
        DerivationSubjectKind::AlgorithmRun,
    ] {
        inputs.extend(fixture.inputs(kind, 3));
        let work = fixture.ledger_work(kind);
        expected = (expected.0 + work.0, expected.1 + work.1);
    }
    inputs.reverse();
    let request = request(fixture.source, inputs.clone());
    let artifact = request.artifact_uuid;
    assert_eq!(measured_register(&fixture.graph, request), expected);
    assert_lineage(&fixture.graph, artifact, &inputs);
    drop(fixture.graph);
    let reopened = GraphForge::new(fixture.root.path().to_str()).unwrap();
    assert_lineage(&reopened, artifact, &inputs);
}

#[test]
fn missing_invalid_and_stale_derivations_refuse_publication() {
    let fixture = Fixture::new(2);
    for kind in [
        DerivationSubjectKind::Source,
        DerivationSubjectKind::Artifact,
        DerivationSubjectKind::Assertion,
        DerivationSubjectKind::EvidenceLink,
        DerivationSubjectKind::AlgorithmRun,
        DerivationSubjectKind::Node,
        DerivationSubjectKind::Edge,
    ] {
        let before = graphforge_storage::resolve_project_generation(fixture.root.path())
            .unwrap()
            .generation_uuid();
        let mut inputs = fixture.inputs(kind, 3);
        inputs.push(DerivationInput {
            input_uuid: Uuid::now_v7(),
            input_kind: kind,
        });
        let error = fixture
            .graph
            .register_artifact(request(fixture.source, inputs))
            .unwrap_err();
        assert_eq!(error.code(), "GF_NOT_FOUND");
        assert!(
            error
                .to_string()
                .contains("derivation input subject was not found")
        );
        assert_eq!(
            graphforge_storage::resolve_project_generation(fixture.root.path())
                .unwrap()
                .generation_uuid(),
            before
        );
    }
    let stale = GraphForge::new(fixture.root.path().to_str()).unwrap();
    fixture
        .graph
        .register_artifact(request(fixture.source, vec![]))
        .unwrap();
    let before = graphforge_storage::resolve_project_generation(fixture.root.path())
        .unwrap()
        .generation_uuid();
    let error = stale
        .register_artifact(request(
            fixture.source,
            fixture.inputs(DerivationSubjectKind::Node, 2),
        ))
        .unwrap_err();
    assert_eq!(error.code(), "GF_IDEMPOTENCY_CONFLICT");
    let mut invalid = request(fixture.source, vec![]);
    invalid.artifact_uuid = Uuid::nil();
    assert_eq!(
        fixture.graph.register_artifact(invalid).unwrap_err().code(),
        "GF_VALIDATION"
    );
    assert_eq!(
        graphforge_storage::resolve_project_generation(fixture.root.path())
            .unwrap()
            .generation_uuid(),
        before
    );
}

#[test]
fn input_order_preserves_missing_subject_before_later_corrupt_kind() {
    use std::io::{Read, Seek, SeekFrom, Write};

    let fixture = Fixture::new(2);
    let generation = graphforge_storage::resolve_project_generation(fixture.root.path()).unwrap();
    let path = generation
        .participant_path("knowledge", "assertions")
        .unwrap();
    let metadata = std::fs::metadata(&path).unwrap();
    let identity = graphforge_filesystem::path_identity(&path).unwrap();
    let permissions = metadata.permissions();
    let mut writable = permissions.clone();
    writable.set_readonly(false);
    std::fs::set_permissions(&path, writable).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let offset = metadata.len() / 2;
    let mut byte = [0];
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.read_exact(&mut byte).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&[byte[0] ^ 1]).unwrap();
    file.set_modified(metadata.modified().unwrap()).unwrap();
    assert_eq!(file.metadata().unwrap().len(), metadata.len());
    assert_eq!(
        graphforge_filesystem::path_identity(&path).unwrap(),
        identity
    );

    let missing = DerivationInput {
        input_uuid: Uuid::now_v7(),
        input_kind: DerivationSubjectKind::Source,
    };
    let assertion = fixture
        .inputs(DerivationSubjectKind::Assertion, 1)
        .remove(0);
    let before_corruption = fixture.graph.register_artifact(request(
        fixture.source,
        vec![missing.clone(), assertion.clone()],
    ));
    let corruption_first = fixture
        .graph
        .register_artifact(request(fixture.source, vec![assertion, missing]));
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.set_modified(metadata.modified().unwrap()).unwrap();
    drop(file);
    std::fs::set_permissions(path, permissions).unwrap();

    assert_eq!(before_corruption.unwrap_err().code(), "GF_NOT_FOUND");
    assert_eq!(corruption_first.unwrap_err().code(), "GF_PROJECT_CORRUPT");
    assert_eq!(
        graphforge_storage::resolve_project_generation(fixture.root.path())
            .unwrap()
            .generation_uuid(),
        generation.generation_uuid()
    );
}
