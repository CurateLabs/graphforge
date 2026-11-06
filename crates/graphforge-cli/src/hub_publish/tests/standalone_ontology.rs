//! Standalone authored ontology publication through the existing native pipeline.
use super::*;
use clap::Parser as _;
use graphforge_api::{
    DiscoveryOntologyModuleRequest, ModuleSelector, OntologyDoc, OntologyModuleId, PortableMode,
    PortableVerifyRequest, resolve_discovered_ontology_module, verify_portable,
};
use graphforge_discovery::{ItemDescriptor, ItemDiscoverySnapshot, ItemId};
use serde_json::Value;

const REPOSITORY: &str = "curate/claims";
const AUTHOR: &str = "Ontology Author <author@example.org>";
const EMPTY_QUERIES: [&str; 2] = [
    "MATCH (n) RETURN count(n)",
    "MATCH ()-[r]->() RETURN count(r)",
];
const SOURCE: &str = r#"{
  "version": "1.0",
  "label": "Scholarly works",
  "description": "People author papers; this module contains schema, not a corpus.",
  "entity_types": [{"name": "Person"}, {"name": "Paper"}],
  "relation_types": [{"name": "AUTHORED", "src": "Person", "dst": "Paper"}]
}"#;

fn local(project: &Path, arguments: &[&str]) -> Value {
    let mut argv = vec!["gf", "--json", "--project", project.to_str().unwrap()];
    argv.extend_from_slice(arguments);
    let result = crate::execute(argv);
    assert_eq!(
        result.exit_code,
        0,
        "gf {arguments:?}: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    serde_json::from_slice(&result.stdout).unwrap()
}

fn author(project: &Path, root: &Path) -> (OntologyModuleId, OntologyDoc, Uuid) {
    let source = root.join("scholarly-source.json");
    let document = root.join("scholarly.json");
    std::fs::write(&source, SOURCE).unwrap();
    // Create fills the permanent ID, but neither create nor import adopts it.
    let candidate = local(
        project,
        &[
            "ontology",
            "module",
            "create",
            "--input",
            source.to_str().unwrap(),
            "--output",
            document.to_str().unwrap(),
        ],
    );
    let imported = local(
        project,
        &[
            "ontology",
            "module",
            "import",
            "--input",
            document.to_str().unwrap(),
        ],
    );
    assert_eq!(candidate["id"], imported["id"]);
    let graph = GraphForge::new(project.to_str()).unwrap();
    let state = graph.ontology_authority_state().unwrap();
    assert!(state.composition_fingerprint.is_none());
    assert_empty(&graph);
    drop(graph);
    local(
        project,
        &[
            "ontology",
            "module",
            "adopt",
            "--input",
            document.to_str().unwrap(),
            "--operation-uuid",
            &Uuid::now_v7().to_string(),
            "--expected-generation",
            &state.project_generation_uuid.to_string(),
        ],
    );
    // Creating the first Branch commits a retained Version of current research,
    // including the adopted ontology; no synthetic graph mutation is needed.
    let branch = local(project, &["branch", "main", "--author", AUTHOR]);
    let version = branch["version_uuid"].as_str().unwrap().parse().unwrap();
    (
        serde_json::from_value(candidate["id"].clone()).unwrap(),
        serde_json::from_slice(&std::fs::read(document).unwrap()).unwrap(),
        version,
    )
}

fn assert_empty(graph: &GraphForge) {
    for query in EMPTY_QUERIES {
        assert_eq!(porcelain::sum(graph.execute(query).unwrap()), 0, "{query}");
    }
}

fn get(hub: &LoopbackHub, url: &str) -> Vec<u8> {
    let response = hub
        .transport()
        .exchange(&HubRequest::new(HubMethod::Get, url), 64 * 1024 * 1024)
        .unwrap();
    assert_eq!(response.status, 200, "{url}");
    response.body
}

fn download(hub: &LoopbackHub, object: &ObjectDescriptor, path: &Path) {
    let bytes = get(hub, &object.locations[0]);
    assert_eq!(bytes.len() as u64, object.length);
    assert_eq!(digest_bytes(&bytes), object.digest.0);
    std::fs::write(path, bytes).unwrap();
}

struct Snapshot {
    refs: Vec<u8>,
    manifest_bytes: Vec<u8>,
    manifest: DiscoveryManifest,
    summary: ProjectSummary,
}

fn discover(hub: &LoopbackHub, id: &OntologyModuleId) -> Snapshot {
    let limits = DiscoveryLimits::default();
    let refs_bytes = get(hub, &format!("{}/.gf/refs", repository_url(REPOSITORY)));
    let manifest_bytes = get(hub, &format!("{}/.gf/manifest", repository_url(REPOSITORY)));
    let refs = RefSet::from_json(&refs_bytes, limits).unwrap();
    let manifest = DiscoveryManifest::from_json(&manifest_bytes, limits).unwrap();
    refs.validate_manifest(&manifest).unwrap();
    let object = manifest.summary_object().unwrap();
    let bytes = get(hub, &object.locations[0]);
    assert_eq!(digest_bytes(&bytes), object.digest.0);
    let summary = ProjectSummary::from_json(&bytes, limits).unwrap();
    manifest.bind_summary(&summary).unwrap();
    let modules = &summary.facts.ontology_composition.as_ref().unwrap().modules;
    assert_eq!(modules.len(), 1);
    assert_eq!(modules[0].id, id.ontology_id);
    assert_eq!(modules[0].version, id.authored_version);
    assert_eq!(
        modules[0].content_digest.0,
        format!("sha256:{}", id.canonical_digest)
    );
    assert_eq!(modules[0].label.as_deref(), Some("Scholarly works"));
    assert_eq!(
        modules[0].description.as_deref(),
        Some("People author papers; this module contains schema, not a corpus.")
    );
    // Package class describes selection, not whether graph rows exist. Publish
    // uses component-selective for both the Project and exact module packages.
    assert_eq!(summary.package.package_class, "component-selective");
    assert!(!summary.facts.research_present);
    assert!(summary.metadata.corpus_size.is_none());
    let module = manifest.ontology.as_ref().unwrap().modules[0].identity();
    let item = ItemId::parse_reference(&id.ontology_id).unwrap();
    let descriptor = ItemDescriptor {
        format: graphforge_discovery::ITEM_DESCRIPTOR_FORMAT.into(),
        id: item.as_str().into(),
        kind: item.kind(),
        repository: manifest.repository.clone(),
        module,
        discovery: ItemDiscoverySnapshot {
            refs_revision: Sha256Digest(digest_bytes(&refs.to_canonical_json().unwrap())),
            manifest_validator: manifest.canonical_digest().unwrap(),
        },
    };
    // Reuse the descriptor contract without implementing Hub content negotiation.
    let descriptor =
        ItemDescriptor::from_json(&descriptor.to_canonical_json().unwrap(), limits).unwrap();
    descriptor.bind_snapshot(&refs, &manifest).unwrap();
    let mut wrong = descriptor;
    wrong.module.version = "another-version".into();
    assert!(wrong.bind_snapshot(&refs, &manifest).is_err());
    Snapshot {
        refs: refs_bytes,
        manifest_bytes,
        manifest,
        summary,
    }
}

fn verify_downloads(hub: &LoopbackHub, root: &Path, id: &OntologyModuleId, document: &OntologyDoc) {
    let Snapshot {
        refs,
        manifest_bytes,
        manifest,
        summary,
    } = discover(hub, id);
    let project_package = root.join("project.gfpb");
    download(hub, manifest.package_object().unwrap(), &project_package);
    let report = verify_portable(
        &PortableVerifyRequest {
            input: project_package,
            mode: PortableMode::Full,
            limits: PortableLimits::default(),
        },
        None,
    )
    .unwrap();
    assert_eq!(report.package_digest, manifest.package.package_digest.0);
    assert_eq!(report.bytes_by_kind, summary.facts.bytes_by_kind);
    assert_eq!(report.payload_bytes, summary.facts.payload_bytes);
    let rows = report.plausibility.parquet.unwrap();
    assert_eq!(
        (rows.node_rows, rows.edge_rows, rows.property_rows),
        (0, 0, 0)
    );

    let module = manifest.ontology.as_ref().unwrap().modules[0].identity();
    let (_, _, object) = manifest.ontology_module_selection(&module).unwrap();
    let module_package = root.join("module.gfpb");
    download(hub, object, &module_package);
    let resolved = resolve_discovered_ontology_module(&DiscoveryOntologyModuleRequest {
        manifest_json: &manifest_bytes,
        refs_json: &refs,
        expected_repository: &RepositoryIdentity::parse(REPOSITORY).unwrap(),
        module: &module,
        package: &module_package,
        discovery_limits: DiscoveryLimits::default(),
        portable_limits: PortableLimits::default(),
        cancelled: None,
    })
    .unwrap();
    assert_eq!(&resolved.module, id);
    assert_eq!(
        serde_json::from_slice::<OntologyDoc>(&resolved.document).unwrap(),
        *document
    );
    assert!(!resolved.report.bytes_by_kind.contains_key("graph-data"));
}

#[test]
fn standalone_ontology_publishes_without_graph_rows_and_clones_exact_schema() {
    for public in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let project = root.path().join("author");
        let (id, document, version) = author(&project, root.path());
        let hub = LoopbackHub::start();
        // Existing capability fixture proves the public session-header path,
        // not account entitlements or production visibility enforcement.
        let transport = visibility::VisibilityTransport {
            hub: &hub,
            major: public.then_some(1),
            refusal: None,
            requests: RefCell::new(Vec::new()),
        };
        let mut argv = vec![
            "gf",
            "publish",
            "https://hub.test/curate/claims",
            "--ref",
            "main",
        ];
        if public {
            argv.push("--public");
        }
        let crate::Command::Publish(arguments) =
            crate::Cli::try_parse_from(argv).unwrap().command.unwrap()
        else {
            panic!("publish command");
        };
        run_command_with_transport(
            &transport,
            || Ok(Some(token(&hub, REPOSITORY))),
            &arguments,
            Some(project.clone()),
            None,
            true,
            &mut Vec::new(),
            &mut crate::Diagnostics::new(true, &mut Vec::new()),
        )
        .unwrap();
        let requests = transport.requests.borrow();
        let open = requests
            .iter()
            .find(|request| request.method == HubMethod::Post && request.url.ends_with("/sessions"))
            .unwrap();
        assert_eq!(
            open.header(graphforge_hub_publish::PUBLISH_VISIBILITY_HEADER),
            public.then_some("public")
        );
        drop(requests);
        assert!(!puts(&hub.log()).is_empty());
        verify_downloads(&hub, root.path(), &id, &document);
        let destination = root.path().join("reader");
        let receipt = clone_into(&hub, REPOSITORY, &destination, Some("main"), None);
        assert_eq!(receipt["research_version_uuid"], version.to_string());
        let reopened = GraphForge::new(destination.to_str()).unwrap();
        assert_empty(&reopened);
        assert_eq!(reopened.ontology_modules().unwrap().len(), 1);
        let inspected = reopened
            .inspect_ontology_module(&ModuleSelector::Exact(id.clone()))
            .unwrap();
        assert_eq!(inspected.doc, document);
        let retained = reopened.open_research_version(version).unwrap();
        for query in EMPTY_QUERIES {
            // No graph domain was retained in this graphless Version. Historical
            // access must keep refusing expansion rather than inventing payload.
            assert!(matches!(
                retained.execute(query),
                Err(GfError::Api {
                    code: graphforge_api::ApiErrorCode::ResultNotRetained,
                    ..
                })
            ));
            let result = local(
                &destination,
                &[
                    "research",
                    "branch",
                    "query",
                    "--branch-uuid",
                    receipt["research_branch_uuid"].as_str().unwrap(),
                    "--query",
                    query,
                ],
            );
            assert_eq!(result["rows"], json!([[0]]));
        }
    }
}
