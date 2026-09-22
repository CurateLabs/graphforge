//! Same-build CLI prepare/commit/query/restore over real durable research.
use arrow::array::{BinaryArray, Int64Array};
use arrow::ipc::reader::StreamReader;
use graphforge_api::*;
use serde_json::{Value, json};
use std::io::Cursor;
use std::path::Path;
use std::process::Command;
use uuid::Uuid;

fn run(root: &Path, arguments: &[&str]) -> Vec<u8> {
    let result = Command::new(env!("CARGO_BIN_EXE_gf"))
        .arg("--project")
        .arg(root)
        .args(["research", "version"])
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    result.stdout
}
fn commit(root: &Path, file: &Path, operation: &Value) -> Value {
    std::fs::write(file, serde_json::to_vec(operation).unwrap()).unwrap();
    serde_json::from_slice(&run(root, &["commit", "--file", file.to_str().unwrap()])).unwrap()
}
fn count(bytes: Vec<u8>) -> i64 {
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
    reader
        .next()
        .unwrap()
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[test]
fn cli_freezes_queries_compacts_and_restores_new_research_identity() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Person)").unwrap();
    let context = || WriteContext {
        operation_uuid: OperationId(Uuid::now_v7()),
        actor_uuid: None,
    };
    let ontology = directory.path().join("ontology.yaml");
    std::fs::write(&ontology, "ontology_id: historical\nversion: \"1\"\nentity_types:\n  - name: Person\n    abstract: false\nrelation_types: []\n").unwrap();
    graph
        .adopt_ontology(AdoptOntologyRequest {
            context: context(),
            path: ontology,
            mode: OntologyMode::Advisory,
        })
        .unwrap();
    let frozen_ontology = serde_json::to_value(graph.workspace_ontology().unwrap()).unwrap();
    for capability_id in [CapabilityId::Provenance, CapabilityId::Knowledge] {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: context(),
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }
    let source = Uuid::now_v7();
    let artifact = Uuid::now_v7();
    graph
        .register_source(RegisterSourceRequest {
            context: context(),
            source_uuid: source,
            label: "Original".into(),
            source_kind: SourceKind::Manuscript,
            identity_uri: None,
        })
        .unwrap();
    graph
        .register_artifact(RegisterArtifactRequest {
            context: context(),
            source_uuid: source,
            artifact_uuid: artifact,
            artifact_kind: ArtifactKind::RawScan,
            media_type: "application/octet-stream".into(),
            payload: ArtifactPayloadRequest::LocalBytes(b"frozen CLI bytes".to_vec()),
            derivation_inputs: vec![],
            run_uuid: None,
        })
        .unwrap();
    drop(graph);
    let owner = Uuid::now_v7();
    let version = Uuid::now_v7();
    let request = PrepareResearchVersionRequest {
        operation_uuid: Uuid::now_v7(),
        version_uuid: version,
        context_uuid: owner,
        label: Some("CLI citation".into()),
        description: None,
        created_at: 1,
        required_versions: Default::default(),
    };
    let file = directory.path().join("request.json");
    std::fs::write(&file, serde_json::to_vec(&request).unwrap()).unwrap();
    let prepared: Value =
        serde_json::from_slice(&run(&root, &["prepare", "--file", file.to_str().unwrap()]))
            .unwrap();
    let receipt = commit(&root, &file, &prepared);
    GraphForge::new(root.to_str())
        .unwrap()
        .execute("CREATE (:Person)")
        .unwrap();
    let current = || {
        graphforge_storage::resolve_project_generation(&root)
            .unwrap()
            .generation_uuid()
    };
    commit(
        &root,
        &file,
        &json!({"operation_uuid":Uuid::now_v7(), "expected_generation_uuid":current(), "mutation":{"operation":"compact", "versions":[version]}}),
    );
    graphforge_storage::execute_project_cleanup(
        &root,
        graphforge_storage::ProjectRetentionPolicy {
            retained_ancestors: 0,
        },
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        count(run(
            &root,
            &[
                "query",
                "--version",
                &version.to_string(),
                "--query",
                "MATCH (n) RETURN count(n)"
            ]
        )),
        1
    );
    let ontology: Value = serde_json::from_slice(&run(
        &root,
        &["ontology", "--version", &version.to_string()],
    ))
    .unwrap();
    assert_eq!(ontology, frozen_ontology);
    let bytes = run(
        &root,
        &[
            "artifact-payload",
            "--version",
            &version.to_string(),
            "--artifact",
            &artifact.to_string(),
        ],
    );
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
    assert_eq!(
        reader
            .next()
            .unwrap()
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        b"frozen CLI bytes"
    );
    let restore = json!({"operation_uuid":Uuid::now_v7(), "expected_generation_uuid":current(), "mutation":{"operation":"restore_project", "source_version":version,"context_uuid":owner,"version_uuid":Uuid::now_v7(),"created_at":2}});
    let restored = commit(&root, &file, &restore);
    assert_ne!(restored["version_uuid"], receipt["version_uuid"]);
    let graph = GraphForge::new(root.to_str()).unwrap();
    assert_eq!(
        graph.execute("MATCH (n) RETURN count(n)").unwrap().batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    graph.execute("CREATE (:Person)").unwrap();
    drop(graph);
    assert_eq!(commit(&root, &file, &restore), restored);
    assert_eq!(commit(&root, &file, &prepared), receipt);
}

#[test]
fn cli_request_diagnostics_do_not_echo_private_values_or_fields() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    drop(GraphForge::new(root.to_str()).unwrap());
    let file = directory.path().join("request.json");
    let sentinel = "PRIVATE_SOURCE_SENTINEL";
    let valid = json!({"operation_uuid": Uuid::now_v7(), "version_uuid": Uuid::now_v7(), "context_uuid": Uuid::now_v7(), "created_at": 1, "required_versions": []});
    for field in ["created_at", sentinel] {
        let mut invalid = valid.clone();
        invalid[field] = json!(sentinel);
        std::fs::write(&file, serde_json::to_vec(&invalid).unwrap()).unwrap();
        let result = Command::new(env!("CARGO_BIN_EXE_gf"))
            .arg("--project")
            .arg(&root)
            .args(["research", "version", "prepare", "--file"])
            .arg(&file)
            .output()
            .unwrap();
        assert!(!result.status.success());
        let error = String::from_utf8_lossy(&result.stderr);
        assert!(!error.contains(sentinel));
        assert!(error.contains("invalid research Version JSON contract"));
        assert!(result.stdout.is_empty());
    }
}
