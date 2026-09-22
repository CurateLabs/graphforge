//! Composed two-story journey through the same-build CLI and native Arrow/JSON.
//! Every command opens durable research afresh; unsupported filesystems fail.
use arrow::{
    array::{BinaryArray, BooleanArray, FixedSizeBinaryArray, Int64Array, StringArray},
    ipc::reader::StreamReader,
    record_batch::RecordBatch,
};
use graphforge_api::*;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    io::Cursor,
    path::{Path, PathBuf},
    process::Command,
};
use uuid::Uuid;

struct Journey {
    root: PathBuf,
    file: PathBuf,
}
impl Journey {
    fn run(&self, args: &[&str]) -> Vec<u8> {
        let result = Command::new(env!("CARGO_BIN_EXE_gf"))
            .arg("--project")
            .arg(&self.root)
            .args(args)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "gf {args:?}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        result.stdout
    }
    fn request(&self, args: &[&str], request: Value) -> Vec<u8> {
        std::fs::write(&self.file, serde_json::to_vec(&request).unwrap()).unwrap();
        let mut args = args.to_vec();
        args.extend(["--file", self.file.to_str().unwrap()]);
        self.run(&args)
    }
    fn control(&self, args: &[&str], request: Value) -> Value {
        decode(&self.request(args, request))
    }
    fn generation(&self) -> Value {
        decode(&self.run(&["research", "metadata", "show"]))["identity"]["generation_uuid"].clone()
    }
    fn branch_edit(&self, branch: Uuid, query: &str) -> Uuid {
        let version = Uuid::now_v7();
        self.control(&["research", "branch", "execute"], json!({"operation_uuid":Uuid::now_v7(),"expected_generation_uuid":self.generation(),"branch_uuid":branch,"version_uuid":version,"query":query,"created_at":2}));
        version
    }
    fn query(&self, kind: &str, id: Uuid, query: &str) -> RecordBatch {
        batch(self.run(&[
            "research",
            kind,
            "query",
            if kind == "branch" {
                "--branch-uuid"
            } else {
                "--version"
            },
            &id.to_string(),
            "--query",
            query,
        ]))
    }
    fn parent_query(&self, query: &str) -> RecordBatch {
        let sink = self
            .file
            .with_file_name(format!("query-{}.arrow", Uuid::now_v7()));
        self.run(&[
            "query",
            "--cypher",
            query,
            "--output",
            sink.to_str().unwrap(),
            "--format",
            "arrow-ipc",
        ]);
        batch(std::fs::read(sink).unwrap())
    }
    fn parent_execute(&self, query: &str) {
        self.run(&[
            "transaction",
            "commit",
            "--operation-uuid",
            &Uuid::now_v7().to_string(),
            "--cypher",
            query,
        ]);
    }
    fn reference(&self, kind: &str, id: Uuid) -> Value {
        let key = if kind == "branch" {
            "branch_uuid"
        } else {
            "version_uuid"
        };
        self.control(
            &["research", "interchange", "reference"],
            json!({"kind":kind,key:id}),
        )
    }
}
fn decode(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).unwrap()
}
fn batch(bytes: Vec<u8>) -> RecordBatch {
    StreamReader::try_new(Cursor::new(bytes), None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
}
fn text<'a>(b: &'a RecordBatch, name: &str, row: usize) -> &'a str {
    b.column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(row)
}
fn integer(b: &RecordBatch, name: &str) -> i64 {
    b.column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}
fn id(b: &RecordBatch, name: &str, row: usize) -> Uuid {
    Uuid::from_slice(
        b.column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(row),
    )
    .unwrap()
}
fn digest(hash: &str) -> Vec<u8> {
    (0..32)
        .map(|i| u8::from_str_radix(&hash[2 * i..2 * i + 2], 16).unwrap())
        .collect()
}
fn context() -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(Uuid::now_v7()),
        actor_uuid: None,
    }
}
fn fields(node: Uuid) -> Value {
    json!([{"object_kind":"node","object_uuid":node,"field":"property:score"},{"object_kind":"node","object_uuid":node,"field":"property:ending"}])
}
fn freeze(j: &Journey, version: Uuid, node: Uuid) -> Vec<u8> {
    j.request(&["research","slice","freeze"],json!({"request_uuid":Uuid::now_v7(),"source":{"kind":"version","version_uuid":version},"selector":{"kind":"direct","members":{"nodes":[node]}}}))
}
const VALUES: &str = "MATCH (n:Character) RETURN n.score AS score,n.ending AS ending,n.x AS x,n.y AS y,n.private_note AS private_note";

#[test]
#[allow(clippy::too_many_lines)]
fn two_story_cli_research_survives_partial_review_restore_and_interchange() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/analyst-journey-v1/corpus.json"
    ))
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let j = Journey {
        root: directory.path().join("project"),
        file: directory.path().join("request.json"),
    };
    drop(GraphForge::new(j.root.to_str()).unwrap());
    j.parent_execute(corpus["initial_graph"].as_str().unwrap());
    let nodes = j.parent_query("MATCH (n) RETURN n.node_uuid AS id,n.name AS name");
    let find = |name| {
        id(
            &nodes,
            "id",
            (0..nodes.num_rows())
                .find(|r| text(&nodes, "name", *r) == name)
                .unwrap(),
        )
    };
    let ada = find("Ada");
    let mystery = find("Mystery");
    let voyage = find("Voyage");
    for capability in ["provenance", "knowledge", "epistemic"] {
        j.run(&[
            "source-artifact",
            "enable-capability",
            "--operation-uuid",
            &Uuid::now_v7().to_string(),
            "--capability-id",
            capability,
        ]);
    }
    let source = Uuid::now_v7();
    let scan = Uuid::now_v7();
    let ocr = Uuid::now_v7();
    let claim = Uuid::now_v7();
    j.run(&[
        "source-artifact",
        "register-source",
        "--operation-uuid",
        &Uuid::now_v7().to_string(),
        "--source-uuid",
        &source.to_string(),
        "--label",
        corpus["source_label"].as_str().unwrap(),
        "--source-kind",
        "manuscript",
    ]);
    for (artifact, kind, media, payload) in [
        (scan, "raw_scan", "image/tiff", "scan_bytes"),
        (ocr, "ocr_text", "text/plain", "ocr_text"),
    ] {
        let path = directory.path().join(payload);
        std::fs::write(&path, corpus[payload].as_str().unwrap()).unwrap();
        let operation = Uuid::now_v7().to_string();
        let artifact_id = artifact.to_string();
        let source_id = source.to_string();
        let derivation = format!("{scan}:artifact");
        let mut args = vec![
            "source-artifact",
            "register-artifact",
            "--operation-uuid",
            &operation,
            "--artifact-uuid",
            &artifact_id,
            "--source-uuid",
            &source_id,
            "--artifact-kind",
            kind,
            "--media-type",
            media,
            "--payload-file",
            path.to_str().unwrap(),
        ];
        if artifact == ocr {
            args.extend(["--derivation-input", &derivation]);
        }
        j.run(&args);
    }
    j.request(&["research","claim","create"],json!({"operation_uuid":Uuid::now_v7(),"expected_generation_uuid":j.generation(),"assertion_uuid":claim,"claim":corpus["claim"],"graph_refs":[{"graph_uuid":ada,"graph_kind":"node","role":"subject","ordinal":0}],"category":"analyst_assertion","creator_uuid":Uuid::now_v7(),"run_uuid":null,"created_at":1}));
    // Evidence attachment has no CLI constructor; the actual Rust facade owns it.
    let graph = GraphForge::new(j.root.to_str()).unwrap();
    graph
        .attach_evidence(AttachEvidenceRequest {
            context: context(),
            evidence_uuid: Uuid::now_v7(),
            assertion_uuid: claim,
            source_uuid: ocr,
            source_kind: EvidenceSourceKind::Artifact,
            role: EvidenceRole::Supports,
            weight: None,
        })
        .unwrap();
    let mut metadata = graph.research_project_metadata().unwrap();
    metadata.title = Some(corpus["title"].as_str().unwrap().into());
    drop(graph);
    std::fs::write(&j.file, metadata.to_canonical_json().unwrap()).unwrap();
    j.run(&[
        "research",
        "metadata",
        "update",
        "--file",
        j.file.to_str().unwrap(),
        "--operation-uuid",
        &Uuid::now_v7().to_string(),
    ]);
    let before = j.generation();
    let discovered = batch(j.run(&[
        "research",
        "discover",
        "--root",
        j.root.to_str().unwrap(),
        "--free-text",
        "Two stories",
    ]));
    assert_eq!(discovered.num_rows(), 1);
    assert_eq!(j.generation(), before);

    let origin = Uuid::now_v7();
    let prepared=j.control(&["research","version","prepare"],json!({"operation_uuid":Uuid::now_v7(),"version_uuid":origin,"context_uuid":Uuid::now_v7(),"created_at":1,"required_versions":[]}));
    j.control(&["research", "version", "commit"], prepared);
    let a = Uuid::now_v7();
    let b = Uuid::now_v7();
    for (branch, seed, label) in [(a, mystery, "Mystery"), (b, voyage, "Voyage")] {
        let selection = json!({"request_uuid":Uuid::now_v7(),"source":{"kind":"version","version_uuid":origin},"selector":{"kind":"traverse","seeds":[seed],"direction":"both","max_depth":1,"relationship_types":["FEATURES"]},"include":{"assertions":[claim]}});
        let included = batch(j.request(&["research", "slice", "preview"], selection.clone()));
        let selected_nodes: BTreeSet<_> = (0..included.num_rows())
            .filter(|r| text(&included, "object_kind", *r) == "node")
            .map(|r| text(&included, "object_uuid", r).to_owned())
            .collect();
        assert_eq!(selected_nodes, [seed.to_string(), ada.to_string()].into());
        let boundary = batch(j.request(
            &["research", "slice", "preview", "--kind", "boundary"],
            selection.clone(),
        ));
        assert!(
            (0..boundary.num_rows()).any(|r| text(&boundary, "object_uuid", r)
                == if branch == a {
                    voyage.to_string()
                } else {
                    mystery.to_string()
                })
        );
        let explanations = batch(j.request(
            &["research", "slice", "preview", "--kind", "explanations"],
            selection.clone(),
        ));
        assert!(
            (0..explanations.num_rows())
                .any(|r| text(&explanations, "object_uuid", r) == ada.to_string()
                    && text(&explanations, "reason", r) == "traversal")
        );
        let dependencies = batch(j.request(
            &["research", "slice", "preview", "--kind", "dependencies"],
            selection.clone(),
        ));
        for dependency in [source, scan, ocr] {
            assert!(
                (0..dependencies.num_rows())
                    .any(|r| text(&dependencies, "object_uuid", r) == dependency.to_string())
            );
        }
        let frozen = j.request(&["research", "slice", "freeze"], selection);
        j.control(&["research","branch","create"],json!({"operation_uuid":Uuid::now_v7(),"expected_generation_uuid":j.generation(),"branch_uuid":branch,"version_uuid":Uuid::now_v7(),"source":{"kind":"slice","frozen_ipc":frozen},"creator_uuid":Uuid::now_v7(),"created_at":1,"label":label}));
    }
    j.branch_edit(a, corpus["local_edit"].as_str().unwrap());
    assert_eq!(integer(&j.query("branch", a, VALUES), "score"), 1);
    assert_eq!(integer(&j.parent_query(VALUES), "score"), 0);
    j.parent_execute(corpus["upstream_edit"].as_str().unwrap());
    let before = j.generation();
    let upstream = json!({"branch_uuid":a,"scope":{"kind":"branch"}});
    let bytes = j.request(&["research", "upstream", "preview"], upstream.clone());
    let reader = StreamReader::try_new(Cursor::new(&bytes), None).unwrap();
    let hash = digest(&reader.schema().metadata()["graphforge.upstream.preview_sha256"]);
    let changes = batch(bytes);
    let row = (0..changes.num_rows())
        .find(|r| text(&changes, "field", *r) == "property:x")
        .unwrap();
    assert_eq!(id(&changes, "object_uuid", row), ada);
    assert_eq!(j.generation(), before);
    let accepted_source = Uuid::now_v7();
    j.control(&["research","upstream","update"],json!({"operation_uuid":Uuid::now_v7(),"expected_generation_uuid":before,"version_uuid":accepted_source,"preview":upstream,"preview_sha256":hash,"selection":{"kind":"selected","decisions":[{"unit":{"object_kind":"node","object_uuid":ada,"field":"property:x"},"resolution":{"kind":"adopt_upstream"}}]},"acknowledge_evidence":[],"actor_uuid":Uuid::now_v7(),"created_at":3,"explanation":"Adopt x only"}));
    let local = j.query("branch", a, VALUES);
    assert_eq!(integer(&local, "x"), 1);
    assert_eq!(integer(&local, "y"), 0);
    let before = j.generation();
    let comparison=batch(j.request(&["research","compare"],json!({"left":{"kind":"branch","branch_uuid":a},"right":{"kind":"project"},"detail":"changes","max_fields":40000,"max_bytes":67108864,"page_size":100})));
    assert!(comparison.num_rows() > 0);
    assert_eq!(j.generation(), before);
    let selected = freeze(&j, accepted_source, ada);
    let proposal = Uuid::now_v7();
    j.control(&["research","proposal","submit"],json!({"operation_uuid":Uuid::now_v7(),"expected_generation_uuid":j.generation(),"proposal_uuid":proposal,"source_branch_uuid":a,"source_version_uuid":accepted_source,"frozen_ipc":selected,"fields":fields(ada),"actor_uuid":Uuid::now_v7(),"created_at":4,"motivation":"Review score and ending","policy":""}));
    let preview_bytes = j.request(
        &["research", "proposal", "preview"],
        json!({"proposal_uuid":proposal}),
    );
    assert!(!String::from_utf8_lossy(&preview_bytes).contains("PRIVATE_LOCAL"));
    assert!(!String::from_utf8_lossy(&preview_bytes).contains("private_note"));
    let preview = batch(preview_bytes);
    assert_eq!(preview.num_rows(), 2);
    let mut decisions = serde_json::Map::new();
    for row in 0..preview.num_rows() {
        let decision = match text(&preview, "field", row) {
            "property:score" => "accept",
            "property:ending" => "defer",
            other => panic!("unselected field {other}"),
        };
        decisions.insert(text(&preview, "item_uuid", row).into(), json!(decision));
    }
    let review = json!({"operation_uuid":Uuid::now_v7(),"expected_generation_uuid":text(&preview,"generation_uuid",0),"proposal_uuid":proposal,"preview_sha256":digest(text(&preview,"preview_sha256",0)),"decisions":decisions,"resolve_conflicts":[],"acknowledge_evidence":[],"promotions":[],"actor_uuid":Uuid::now_v7(),"created_at":5,"explanation":"Accept score, defer ending","policy":""});
    let receipt = j.control(&["research", "proposal", "review"], review.clone());
    let parent = j.parent_query(VALUES);
    assert_eq!(integer(&parent, "score"), 1);
    assert_eq!(text(&parent, "ending", 0), "undecided");
    assert_eq!(text(&parent, "private_note", 0), "PRIVATE_JOURNEY");
    let accepted = batch(j.request(
        &["research", "proposal", "history"],
        json!({"proposal_uuid":proposal,"detail":"accepted","page_size":100}),
    ));
    assert_eq!(accepted.num_rows(), 1);
    let citation = j.reference("version", accepted_source);
    let continued = j.branch_edit(a, "MATCH (n:Character) SET n.score=2");
    assert_eq!(
        j.reference("branch", a)["version"]["version_uuid"],
        continued.to_string()
    );
    assert_eq!(
        j.reference("version", accepted_source)["version"],
        citation["version"]
    );
    assert_eq!(
        integer(&j.query("version", accepted_source, VALUES), "score"),
        1
    );
    j.branch_edit(b, "MATCH (n:Character) SET n.score=73");
    j.parent_execute("MATCH (n:Character) SET n.score=99");
    let restored = Uuid::now_v7();
    j.control(&["research","branch","restore"],json!({"operation_uuid":Uuid::now_v7(),"expected_generation_uuid":j.generation(),"branch_uuid":a,"source_version_uuid":accepted_source,"version_uuid":restored,"created_at":6}));
    assert_eq!(
        j.control(&["research", "proposal", "review"], review.clone()),
        receipt
    );
    for (branch, score) in [(a, 1), (b, 73)] {
        assert_eq!(integer(&j.query("branch", branch, VALUES), "score"), score);
    }
    assert_eq!(integer(&j.parent_query(VALUES), "score"), 99);
    // Fresh reproposal of an accepted field preserves accepted-mapping evidence.
    let reproposal = Uuid::now_v7();
    let score_field = json!([{"object_kind":"node","object_uuid":ada,"field":"property:score"}]);
    j.control(&["research","proposal","submit"],json!({"operation_uuid":Uuid::now_v7(),"expected_generation_uuid":j.generation(),"proposal_uuid":reproposal,"source_branch_uuid":a,"source_version_uuid":restored,"frozen_ipc":freeze(&j,restored,ada),"fields":score_field,"actor_uuid":Uuid::now_v7(),"created_at":7,"motivation":"Revisit accepted score","policy":""}));
    let preview = batch(j.request(
        &["research", "proposal", "preview"],
        json!({"proposal_uuid":reproposal}),
    ));
    assert_eq!(preview.num_rows(), 1);
    assert!(
        preview
            .column_by_name("already_accepted")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    let repeated = j.control(&["research","proposal","review"],json!({"operation_uuid":Uuid::now_v7(),"expected_generation_uuid":text(&preview,"generation_uuid",0),"proposal_uuid":reproposal,"preview_sha256":digest(text(&preview,"preview_sha256",0)),"decisions":{text(&preview,"item_uuid",0):"accept"},"resolve_conflicts":[],"acknowledge_evidence":[],"promotions":[],"actor_uuid":Uuid::now_v7(),"created_at":8,"explanation":"Already accepted","policy":""}));
    assert!(repeated["version_uuid"].is_null());
    assert_eq!(integer(&j.parent_query(VALUES), "score"), 99);
    let retention = decode(&j.run(&["research", "version", "retention"]));
    let mut retained: Vec<Value> = retention["heads"]
        .as_object()
        .unwrap()
        .values()
        .cloned()
        .collect();
    retained.push(json!(accepted_source));
    j.control(&["research","version","commit"],json!({"operation_uuid":Uuid::now_v7(),"expected_generation_uuid":j.generation(),"mutation":{"operation":"compact","versions":retained}}));
    j.run(&[
        "maintenance",
        "cleanup-execute",
        "--retained-ancestors",
        "0",
        "--yes",
    ]);
    assert_eq!(
        j.control(&["research", "proposal", "review"], review),
        receipt
    );
    for (branch, score) in [(a, 1), (b, 73)] {
        assert_eq!(integer(&j.query("branch", branch, VALUES), "score"), score);
    }
    assert_eq!(integer(&j.parent_query(VALUES), "score"), 99);
    assert_eq!(
        j.reference("version", accepted_source)["version"],
        citation["version"]
    );
    interchange(
        &j,
        directory.path(),
        accepted_source,
        ada,
        ocr,
        &corpus,
        &citation,
    );
}

#[allow(clippy::too_many_lines)]
fn interchange(
    j: &Journey,
    directory: &Path,
    version: Uuid,
    ada: Uuid,
    ocr: Uuid,
    corpus: &Value,
    citation: &Value,
) {
    let package = directory.join("complete.gfpb");
    let before = j.generation();
    let exported = j.control(
        &["research", "interchange", "export"],
        json!({"version_uuid":version,"output":package,"bundled":true,"projection":null}),
    );
    let verified = decode(&j.run(&[
        "--json",
        "portable",
        "verify",
        "--mode",
        "full",
        "--input",
        package.to_str().unwrap(),
    ]));
    assert_eq!(exported["package_digest"], verified["package_digest"]);
    let imported = Journey {
        root: directory.join("imported"),
        file: directory.join("imported-request.json"),
    };
    let operation = Uuid::now_v7().to_string();
    imported.run(&[
        "--json",
        "portable",
        "import",
        "--input",
        package.to_str().unwrap(),
        "--idempotency-key",
        &operation,
    ]);
    assert_eq!(
        imported.reference("version", version)["version"],
        citation["version"]
    );
    assert_eq!(
        imported.reference("version", version)["genealogy"],
        citation["genealogy"]
    );
    assert_eq!(
        integer(&imported.query("version", version, VALUES), "score"),
        1
    );
    let imported_graph = imported.query("version", version, VALUES);
    let source_graph = j.query("version", version, VALUES);
    assert_eq!(imported_graph.num_rows(), source_graph.num_rows());
    assert_eq!(imported_graph.columns(), source_graph.columns());
    assert_eq!(
        imported_graph.schema().fields(),
        source_graph.schema().fields()
    );
    // Each native execution gets its own query identity; all other schema
    // metadata and every field/value must survive the interchange unchanged.
    let mut imported_metadata = imported_graph.schema().metadata().clone();
    let mut source_metadata = source_graph.schema().metadata().clone();
    assert!(imported_metadata.remove("graphforge.query_id").is_some());
    assert!(source_metadata.remove("graphforge.query_id").is_some());
    assert_eq!(imported_metadata, source_metadata);
    let ontology_args = ["research", "version", "ontology", "--version"];
    let version_text = version.to_string();
    let mut ontology_args = ontology_args.to_vec();
    ontology_args.push(&version_text);
    assert_eq!(
        decode(&imported.run(&ontology_args)),
        decode(&j.run(&ontology_args))
    );
    let payload = batch(imported.run(&[
        "research",
        "version",
        "artifact-payload",
        "--version",
        &version.to_string(),
        "--artifact",
        &ocr.to_string(),
    ]));
    assert_eq!(
        payload
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        corpus["ocr_text"].as_str().unwrap().as_bytes()
    );
    let projection = Uuid::now_v7();
    let redacted = directory.join("selected.gfpb");
    j.control(&["research","interchange","export"],json!({"version_uuid":version,"output":redacted,"bundled":true,"projection":{"version_uuid":projection,"frozen_ipc":freeze(j,version,ada),"fields":[{"object_kind":"node","object_uuid":ada,"field":"property:score"}],"created_at":8}}));
    let selected = Journey {
        root: directory.join("selected"),
        file: directory.join("selected-request.json"),
    };
    selected.run(&[
        "--json",
        "portable",
        "import",
        "--input",
        redacted.to_str().unwrap(),
        "--idempotency-key",
        &Uuid::now_v7().to_string(),
    ]);
    let reference = selected.reference("version", projection);
    assert_eq!(
        reference["version"]["content"]["source_version"],
        version.to_string()
    );
    assert_ne!(reference["version"]["version_uuid"], version.to_string());
    let data = selected.query(
        "version",
        projection,
        "MATCH (n) RETURN n.score AS score,n.private_note IS NULL AS private_omitted",
    );
    assert_eq!(integer(&data, "score"), 1);
    assert!(
        data.column_by_name("private_omitted")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    let mut metadata = decode(&j.run(&["research", "metadata", "show"]))["metadata"].clone();
    metadata["title"] = corpus["fork_title"].clone();
    metadata["access"]["access_policy"] = json!("Independent local review");
    let fork = Journey {
        root: directory.join("fork"),
        file: directory.join("fork-request.json"),
    };
    let project = Uuid::now_v7();
    let request = json!({"operation_uuid":Uuid::now_v7(),"project_uuid":project,"version_uuid":version,"projection":null,"target":fork.root,"actor_uuid":Uuid::now_v7(),"governance":"Independent review","adopt_selected_ontology":true,"metadata":metadata});
    let first = j.control(&["research", "interchange", "fork"], request.clone());
    let replay = j.control(&["research", "interchange", "fork"], request);
    assert_eq!(first["generation_uuid"], replay["generation_uuid"]);
    assert_eq!(replay["idempotent_replay"], true);
    let fork_ref = fork.reference("version", version);
    assert_eq!(fork_ref["project_uuid"], project.to_string());
    assert_eq!(
        fork_ref["origin_project_uuid"],
        citation["origin_project_uuid"]
    );
    assert_ne!(fork_ref["project_uuid"], fork_ref["origin_project_uuid"]);
    assert_eq!(fork_ref["version"], citation["version"]);
    let replay = decode(&imported.run(&[
        "--json",
        "portable",
        "import",
        "--input",
        package.to_str().unwrap(),
        "--idempotency-key",
        &operation,
    ]));
    assert_eq!(replay["idempotent_replay"], true);
    assert_eq!(j.generation(), before);
}
