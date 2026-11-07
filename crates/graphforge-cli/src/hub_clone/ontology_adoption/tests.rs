use super::*;
use crate::hub_clone::tests::{Scripted, response};
use crate::hub_clone::{HttpResponse, TEST_RETRY_POLICY};
use clap::Parser;
use graphforge_api::{
    BridgeAdoptionRequest, ModuleAdoptionRequest, OntologyAuthorityExpectation, OntologyDoc,
    OntologyModuleId, PortableExactIdentity, PortableExportRequest, PortableLimits, PortableOutput,
    PortableSelection, PortableSelectionProfile,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Mutex;
use url::Url;

const REFS: &[u8] = include_bytes!("../../../../../tests/fixtures/hub/generated/v1/refs.json");
const MANIFEST: &[u8] =
    include_bytes!("../../../../../tests/fixtures/hub/generated/v1/manifest.json");
const CONTRACT: &str = include_str!("../../../../../tests/fixtures/hub/ontology-adoption.json");

fn authority(graph: &GraphForge) -> OntologyAuthorityExpectation {
    let state = graph.ontology_authority_state().unwrap();
    OntologyAuthorityExpectation {
        context: WriteContext {
            operation_uuid: OperationId(Uuid::now_v7()),
            actor_uuid: None,
        },
        expected_project_generation_uuid: state.project_generation_uuid,
        expected_composition_fingerprint: state.composition_fingerprint,
    }
}

fn document(id: &str, entity: &str) -> OntologyDoc {
    serde_json::from_value(
        json!({"ontology_id": id, "version": "1", "entity_types": [{"name": entity}]}),
    )
    .unwrap()
}

fn adopt(
    graph: &mut GraphForge,
    document: OntologyDoc,
    dependencies: Vec<OntologyModuleId>,
) -> OntologyModuleId {
    let candidate = graph
        .create_ontology_module(document, dependencies, None)
        .unwrap();
    let id = candidate.id.clone();
    graph
        .adopt_ontology_module(
            &ModuleAdoptionRequest {
                authority: authority(graph),
                candidate,
            },
            None,
        )
        .unwrap();
    id
}

struct Published {
    module: OntologyModuleId,
    dependency: OntologyModuleId,
    bridge: graphforge_api::BridgeDocument,
    manifest: Vec<u8>,
    package: Vec<u8>,
}

impl Published {
    fn new() -> Self {
        Self::with_mode(graphforge_api::ActivationMode::Exploratory)
    }

    fn with_mode(mode: graphforge_api::ActivationMode) -> Self {
        let root = tempfile::tempdir().unwrap();
        let mut graph = GraphForge::new(root.path().join("source").to_str()).unwrap();
        let dependency = adopt(
            &mut graph,
            document("https://graphforge.sh/ont_bbbbbbbbbbbbbbbb", "Base"),
            vec![],
        );
        let module = adopt(
            &mut graph,
            document("https://graphforge.sh/ont_aaaaaaaaaaaaaaaa", "Thing"),
            vec![dependency.clone()],
        );
        let bridge: graphforge_api::BridgeDocument = serde_json::from_value(json!({
            "bridge_id": "https://graphforge.sh/brg_aaaaaaaaaaaaaaaa", "authored_version": "1",
            "source_modules": [module], "target_modules": [dependency],
            "assertions": [{"source": {"module": module, "kind": "entity", "local_id": "Thing"},
                "target": {"module": dependency, "kind": "entity", "local_id": "Base"},
                "predicate": "equivalent", "directional": true,
                "provenance": {"method": "authored", "justification": "curated mapping", "evidence_refs": ["evidence:review"]}}]
        })).unwrap();
        let candidate = graph.create_ontology_bridge(bridge.clone()).unwrap();
        let bridge_id = candidate.id.clone();
        graph
            .adopt_ontology_bridge(
                &BridgeAdoptionRequest {
                    authority: authority(&graph),
                    candidate,
                },
                None,
            )
            .unwrap();
        graph
            .change_ontology_activation_profile(
                &graphforge_api::ActivationProfileChangeRequest {
                    authority: authority(&graph),
                    profile_default: mode,
                    activation: Vec::new(),
                },
                None,
            )
            .unwrap();
        let path = root.path().join("module.gfpb");
        let exported = graph
            .export_portable(
                &PortableExportRequest {
                    selection: PortableSelection::Current,
                    output_path: path.clone(),
                    representation: PortableOutput::Bundle,
                    profile: PortableSelectionProfile::OntologyComposition(vec![
                        PortableExactIdentity {
                            id: module.ontology_id.clone(),
                            version: module.authored_version.clone(),
                            content_digest: format!("sha256:{}", module.canonical_digest),
                        },
                        PortableExactIdentity {
                            id: bridge_id.bridge_id,
                            version: bridge_id.authored_version,
                            content_digest: format!("sha256:{}", bridge_id.canonical_digest),
                        },
                    ]),
                    subset: None,
                    limits: PortableLimits::default(),
                },
                None,
                |_| {},
            )
            .unwrap();
        let package = std::fs::read(path).unwrap();
        let object_digest = format!(
            "sha256:{}",
            crate::porcelain::hex(&Sha256::digest(&package))
        );
        let mut manifest: Value = serde_json::from_slice(MANIFEST).unwrap();
        let prior = manifest["ontology"]["modules"][0]["package"]["object_digest"].clone();
        let entry = &mut manifest["ontology"]["modules"][0];
        entry["id"] = json!(module.ontology_id);
        entry["version"] = json!(module.authored_version);
        entry["content_digest"] = json!(format!("sha256:{}", module.canonical_digest));
        entry["package"]["object_digest"] = json!(object_digest);
        entry["package"]["package_digest"] = json!(exported.receipt().package_digest);
        let objects = manifest["objects"].as_array_mut().unwrap();
        let object = objects
            .iter_mut()
            .find(|object| object["digest"] == prior)
            .unwrap();
        object["digest"] = json!(object_digest);
        object["length"] = json!(package.len());
        object["locations"] = json!(["https://data.example.org/module.gfpb"]);
        objects.sort_by_key(|object| object["digest"].as_str().unwrap().to_owned());
        Self {
            module,
            dependency,
            bridge,
            manifest: serde_json::to_vec(&manifest).unwrap(),
            package,
        }
    }

    fn transport(&self) -> PublicTransport {
        PublicTransport {
            inner: Scripted::new(vec![
                response(200, None, REFS),
                response(200, None, &self.manifest),
                response(200, None, &self.package),
            ]),
            requested: Mutex::new(vec![]),
        }
    }

    fn command(&self, flow: &str, destination: &Path) -> Vec<String> {
        let fixture: Value = serde_json::from_str(CONTRACT).unwrap();
        fixture[flow]["argv"]
            .as_array()
            .unwrap()
            .iter()
            .map(|arg| match arg.as_str().unwrap() {
                "{project}" => destination.display().to_string(),
                "{ontology_id}" => self.module.ontology_id.clone(),
                "{version}" => self.module.authored_version.clone(),
                "{digest}" => format!("sha256:{}", self.module.canonical_digest),
                other => other.into(),
            })
            .collect()
    }

    fn assert_project(&self, path: &Path) -> GraphForge {
        let graph = GraphForge::new(path.to_str()).unwrap();
        let composition = graph.workspace_ontology_composition().unwrap().unwrap();
        assert_eq!(composition.modules.len(), 2);
        assert_eq!(composition.bridges, [self.bridge.clone()]);
        assert_eq!(
            composition
                .modules
                .iter()
                .find(|module| module.id == self.module)
                .unwrap()
                .dependencies,
            [self.dependency.clone()]
        );
        let metadata = graph.research_project_metadata().unwrap();
        let record: Value = serde_json::from_str(
            metadata.extensions[&format!(
                "graphforge.ontology_adoption:{}",
                self.module.canonical_digest
            )]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(record["repository"], "opendata/openalex");
        assert_eq!(record["module"], json!(self.module));
        assert!(
            record["package_digest"]
                .as_str()
                .unwrap()
                .starts_with("sha256:")
        );
        graph
    }
}

/// Product URL policy still runs. Any authorization-capable path is forbidden;
/// ordinary GET is the only transport operation these commands may invoke.
struct PublicTransport {
    inner: Scripted,
    requested: Mutex<Vec<String>>,
}
impl Transport for PublicTransport {
    fn get(
        &self,
        url: &Url,
        range: Option<u64>,
        validator: Option<&str>,
        limit: u64,
    ) -> Result<HttpResponse, GfError> {
        self.requested.lock().unwrap().push(url.to_string());
        self.inner.get(url, range, validator, limit)
    }
    fn get_authorized(
        &self,
        _: &Url,
        _: Option<u64>,
        _: Option<&str>,
        _: u64,
        _: &[(&str, ureq::http::HeaderValue)],
    ) -> Result<HttpResponse, GfError> {
        panic!("ontology adoption must never send credentials")
    }
}

fn run(transport: &dyn Transport, argv: &[String]) -> Result<Value, GfError> {
    let cli = crate::Cli::try_parse_from(argv).unwrap();
    let mut output = Vec::new();
    let result = dispatch_with(
        transport,
        cli.command.unwrap(),
        cli.project,
        cli.project_dir,
        cli.json,
        &mut output,
        &TEST_RETRY_POLICY,
        |name| std::env::var(name),
    )?;
    assert!(matches!(result, ControlFlow::Break(())));
    Ok(serde_json::from_slice(&output).unwrap())
}

#[test]
fn protected_adopt_and_start_verify_http_packages_preserve_provenance_and_reopen() {
    use crate::hub_clone::auth_tests::{Reply, server};
    const BYPASS: &str = "ontology-adoption-preview-secret";
    for new_project in [false, true] {
        let published = Published::new();
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("project");
        if !new_project {
            let graph = GraphForge::new(path.to_str()).unwrap();
            graph.execute("CREATE (:Retained {value:42})").unwrap();
        }
        let mut args = published.command(if new_project { "start_from" } else { "adopt" }, &path);
        args.extend(
            [
                "--hub",
                "https://hub.example",
                "--protection-bypass-env",
                "PREVIEW_SECRET",
            ]
            .map(String::from),
        );
        let cli = crate::Cli::try_parse_from(args).unwrap();
        let (transport, server) = server(vec![
            Reply::ok(REFS),
            Reply::ok(&published.manifest),
            Reply::ok(&published.package),
        ]);
        let mut output = Vec::new();
        let result = dispatch_with(
            &transport,
            cli.command.unwrap(),
            cli.project,
            cli.project_dir,
            true,
            &mut output,
            &TEST_RETRY_POLICY,
            |name| {
                assert_eq!(name, "PREVIEW_SECRET");
                Ok(BYPASS.into())
            },
        );
        let requests = server.join().unwrap();
        assert!(matches!(result.unwrap(), ControlFlow::Break(())));
        let receipt: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(receipt["adoption"]["module"], json!(published.module));
        assert_eq!(receipt["adoption"]["already_adopted"], false);
        assert_eq!(requests.len(), 3);
        for (index, request) in requests.iter().enumerate() {
            let request = request.to_ascii_lowercase();
            assert!(!request.contains("\r\nauthorization:"));
            assert_eq!(
                request.contains("\r\nx-vercel-protection-bypass:"),
                index < 2
            );
            assert_eq!(request.contains(BYPASS), index < 2);
        }
        let graph = published.assert_project(&path);
        if !new_project {
            assert_eq!(retained_row(&graph).1, 42);
        } else {
            assert!(
                graph
                    .research_version_retention()
                    .unwrap()
                    .current_branch
                    .is_some()
            );
        }
    }
}

fn retained_row(graph: &GraphForge) -> (Vec<u8>, i64) {
    let result = graph
        .execute("MATCH (n:Retained) RETURN n.node_uuid, n.value")
        .unwrap();
    assert_eq!(
        result
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    let batch = result
        .batches
        .iter()
        .find(|batch| batch.num_rows() == 1)
        .unwrap();
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .unwrap();
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    (ids.value(0).to_vec(), values.value(0))
}

#[test]
fn adopt_command_preserves_data_exact_dependency_bridge_provenance_and_replays_after_reopen() {
    let published = Published::new();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("project");
    let graph = GraphForge::new(path.to_str()).unwrap();
    graph.execute("CREATE (:Retained {value:42})").unwrap();
    let expected_row = retained_row(&graph);
    assert_eq!(expected_row.1, 42);
    let before = graph
        .committed_generation_identity()
        .unwrap()
        .generation_uuid;
    drop(graph);
    let transport = published.transport();
    let command = published.command("adopt", &path);
    let receipt = run(&transport, &command).unwrap();
    assert_eq!(receipt["contract"], "graphforge-ontology-adopt/1");
    assert_eq!(receipt["adoption"]["module"], json!(published.module));
    assert_eq!(transport.requested.lock().unwrap().len(), 3);
    let graph = published.assert_project(&path);
    assert_eq!(retained_row(&graph), expected_row);
    let after = graph
        .committed_generation_identity()
        .unwrap()
        .generation_uuid;
    assert_ne!(after, before);
    drop(graph);
    let retry = run(&published.transport(), &command).unwrap();
    assert_eq!(retry["adoption"]["already_adopted"], true);
    assert_eq!(
        published
            .assert_project(&path)
            .committed_generation_identity()
            .unwrap()
            .generation_uuid,
        after
    );
    assert_eq!(
        std::fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| !name.starts_with(".graphforge-admission-") || !name.ends_with(".lock"))
            .collect::<Vec<_>>(),
        vec!["project"]
    );
}

#[test]
fn project_new_command_publishes_absent_destination_with_a_publishable_selected_branch() {
    let published = Published::new();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("project");
    assert!(!path.exists());
    let receipt = run(
        &published.transport(),
        &published.command("start_from", &path),
    )
    .unwrap();
    assert_eq!(receipt["contract"], "graphforge-project-from-ontology/1");
    let graph = published.assert_project(&path);
    let registry = graph.research_version_retention().unwrap();
    let branch = registry.current_branch.unwrap();
    assert_eq!(registry.branches[&branch].label, "main");
    let view = graph.open_research_branch(branch).unwrap();
    assert_eq!(
        view.graph().workspace_ontology_composition().unwrap(),
        graph.workspace_ontology_composition().unwrap()
    );
    assert_eq!(
        std::fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| !name.starts_with(".graphforge-admission-") || !name.ends_with(".lock"))
            .collect::<Vec<_>>(),
        vec!["project"]
    );
}

#[test]
fn verification_failure_and_cancellation_never_publish_or_half_adopt() {
    let published = Published::new();
    for flow in ["adopt", "start_from"] {
        for cancellation in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("project");
            let before = if flow == "adopt" {
                Some(
                    GraphForge::new(path.to_str())
                        .unwrap()
                        .committed_generation_identity()
                        .unwrap()
                        .generation_uuid,
                )
            } else {
                None
            };
            let transport = PublicTransport {
                inner: Scripted::new(vec![
                    response(200, None, REFS),
                    response(200, None, &published.manifest),
                    response(200, None, b"corrupt package"),
                ]),
                requested: Mutex::new(vec![]),
            };
            let mut command = published.command(flow, &path);
            if cancellation {
                command.push("--cancel-before-start".into());
            }
            assert!(run(&transport, &command).is_err());
            if let Some(before) = before {
                let graph = GraphForge::new(path.to_str()).unwrap();
                assert_eq!(
                    graph
                        .committed_generation_identity()
                        .unwrap()
                        .generation_uuid,
                    before
                );
                assert!(graph.workspace_ontology_composition().unwrap().is_none());
                assert!(
                    !graph
                        .research_project_metadata()
                        .unwrap()
                        .extensions
                        .keys()
                        .any(|key| key.starts_with("graphforge.ontology_adoption:"))
                );
            } else {
                assert!(!path.exists());
            }
            if cancellation {
                assert!(transport.requested.lock().unwrap().is_empty());
            }
            if flow == "start_from" && !cancellation {
                // The failed transfer left no destination or poisoned staging;
                // the exact same command can complete on the next attempt.
                run(&published.transport(), &command).unwrap();
                published.assert_project(&path);
            }
        }
    }
}

#[test]
fn exact_pin_destination_and_private_object_refusals_do_not_mutate() {
    let published = Published::new();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("project");
    let mut command = published.command("start_from", &path);
    let digest = command.iter().position(|arg| arg == "--digest").unwrap() + 1;
    command[digest] = format!("sha256:{}", "0".repeat(64));
    assert!(run(&published.transport(), &command).is_err());
    assert!(!path.exists());
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("keep"), "unrelated").unwrap();
    let transport = published.transport();
    assert!(run(&transport, &published.command("start_from", &path)).is_err());
    assert!(transport.requested.lock().unwrap().is_empty());
    assert_eq!(
        std::fs::read_to_string(path.join("keep")).unwrap(),
        "unrelated"
    );
    std::fs::remove_dir_all(&path).unwrap();
    let mut manifest: Value = serde_json::from_slice(&published.manifest).unwrap();
    for object in manifest["objects"].as_array_mut().unwrap() {
        object["locations"] = json!(["https://127.0.0.1/private"]);
    }
    let transport = PublicTransport {
        inner: Scripted::new(vec![
            response(200, None, REFS),
            response(200, None, &serde_json::to_vec(&manifest).unwrap()),
        ]),
        requested: Mutex::new(vec![]),
    };
    assert!(run(&transport, &published.command("start_from", &path)).is_err());
    assert!(!path.exists());
    assert!(
        transport
            .requested
            .lock()
            .unwrap()
            .iter()
            .all(|url| !url.contains("127.0.0.1"))
    );
}

struct PublicationHub(graphforge_hub_publish::ReferenceHub);
impl Transport for PublicationHub {
    fn get(
        &self,
        url: &Url,
        _: Option<u64>,
        _: Option<&str>,
        _: u64,
    ) -> Result<HttpResponse, GfError> {
        let response = self.0.handle(&graphforge_hub_publish::HubRequest::new(
            graphforge_hub_publish::HubMethod::Get,
            url.as_str(),
        ));
        Ok(HttpResponse {
            status: response.status,
            location: response.header("location").map(str::to_owned),
            content_range: response.header("content-range").map(str::to_owned),
            etag: response.header("etag").map(str::to_owned),
            content_length: Some(response.body.len() as u64),
            retry_after: None,
            body: Box::new(std::io::Cursor::new(response.body)),
        })
    }
    fn exchange(
        &self,
        request: &graphforge_hub_publish::HubRequest,
        _: usize,
    ) -> Result<graphforge_hub_publish::HubResponse, GfError> {
        let mut response = self.0.handle(request);
        if request.method == graphforge_hub_publish::HubMethod::Get
            && request.url.ends_with("/.gf/publish")
        {
            let mut body: Value = serde_json::from_slice(&response.body).unwrap();
            body["capabilities"]
                .as_array_mut()
                .unwrap()
                .push(json!({"capability":"publish-visibility", "major":1}));
            response.body = serde_json::to_vec(&body).unwrap();
        }
        Ok(response)
    }
}

fn publish_public(hub: &PublicationHub, graph: &mut GraphForge, repository: &str) -> Value {
    let credential = graphforge_hub_publish::PublishToken::new(
        hub.0.issue_token(&format!("publish:{repository}"), 900),
    );
    let url = format!("https://hub.test/{repository}");
    let cli = crate::Cli::try_parse_from(["gf", "publish", &url, "--public"]).unwrap();
    let crate::Command::Publish(mut args) = cli.command.unwrap() else {
        panic!("publish command")
    };
    args.resolve_defaults(&graph.research_version_retention().unwrap())
        .unwrap();
    let mut output = Vec::new();
    crate::hub_publish::publish_with_credentials(
        hub,
        &args,
        Some(credential),
        None,
        |environment| {
            crate::hub_publish::publish_and_report(
                hub,
                graph,
                &args,
                environment,
                true,
                &mut output,
            )
        },
    )
    .unwrap();
    serde_json::from_slice(&output).unwrap()
}

#[test]
fn project_from_ontology_can_run_the_existing_public_publish_command_without_a_ref_flag() {
    let published = Published::new();
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    run(
        &published.transport(),
        &published.command("start_from", &source),
    )
    .unwrap();
    let mut origin = published.assert_project(&source);
    let hub = PublicationHub(
        graphforge_hub_publish::ReferenceHub::with_config(
            graphforge_hub_publish::ReferenceHubConfig {
                base_url: "https://hub.test".into(),
                ..Default::default()
            },
        )
        .unwrap(),
    );
    // Establish the source owner's exact IDs in the real Hub admission registry.
    publish_public(&hub, &mut origin, "opendata/openalex");
    let path = root.path().join("consumer");
    let mut command = published.command("start_from", &path);
    command.extend(["--hub".into(), "https://hub.test".into()]);
    let receipt = run(&hub, &command).unwrap();
    assert_eq!(receipt["adoption"]["module"], json!(published.module));
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    let composition = graph.workspace_ontology_composition().unwrap().unwrap();
    assert_eq!(composition.modules.len(), 2);
    assert_eq!(
        composition
            .modules
            .iter()
            .find(|module| module.id == published.module)
            .unwrap()
            .dependencies,
        [published.dependency]
    );
    // The real advertised module package contains only its dependency closure;
    // unrelated source bridges do not enter the consumer Project.
    assert!(composition.bridges.is_empty());
    let result = publish_public(&hub, &mut graph, "curate/adoption");
    assert!(result.is_object());
    let manifest = hub.0.handle(&graphforge_hub_publish::HubRequest::new(
        graphforge_hub_publish::HubMethod::Get,
        "https://hub.test/curate/adoption/.gf/manifest",
    ));
    assert_eq!(manifest.status, 200);
    let manifest: Value = serde_json::from_slice(&manifest.body).unwrap();
    assert!(
        manifest["ontology"]["modules"]
            .as_array()
            .unwrap()
            .iter()
            .any(|module| module["id"] == published.module.ontology_id
                && module["content_digest"]
                    == format!("sha256:{}", published.module.canonical_digest))
    );
}

#[test]
fn conflicting_local_module_fails_after_full_verification_without_adopting_any_dependency() {
    let published = Published::new();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("project");
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    adopt(
        &mut graph,
        document(&published.module.ontology_id, "Different"),
        vec![],
    );
    let before = graph.workspace_ontology_composition().unwrap();
    let generation = graph.committed_generation_identity().unwrap();
    drop(graph);
    let transport = published.transport();
    assert!(run(&transport, &published.command("adopt", &path)).is_err());
    assert_eq!(transport.requested.lock().unwrap().len(), 3);
    let graph = GraphForge::new(path.to_str()).unwrap();
    assert_eq!(graph.committed_generation_identity().unwrap(), generation);
    assert_eq!(graph.workspace_ontology_composition().unwrap(), before);
    assert!(
        !graph
            .research_project_metadata()
            .unwrap()
            .extensions
            .keys()
            .any(|key| key.starts_with("graphforge.ontology_adoption:"))
    );
}

#[test]
fn adoption_publication_child() {
    let Ok(root) = std::env::var("GF_ONTOLOGY_ADOPTION_PROJECT") else {
        return;
    };
    let fixture = PathBuf::from(std::env::var("GF_ONTOLOGY_ADOPTION_FIXTURE").unwrap());
    let published = load_published(&fixture);
    run(
        &published.transport(),
        &published.command("adopt", Path::new(&root)),
    )
    .expect("configured publication failpoint must terminate the process");
}

fn save_published(published: &Published, directory: &Path) {
    std::fs::create_dir(directory).unwrap();
    std::fs::write(directory.join("manifest.json"), &published.manifest).unwrap();
    std::fs::write(directory.join("package.gfpb"), &published.package).unwrap();
    std::fs::write(directory.join("identities.json"), serde_json::to_vec(&json!({
        "module": published.module, "dependency": published.dependency, "bridge": published.bridge})).unwrap()).unwrap();
}

fn load_published(directory: &Path) -> Published {
    let identities: Value =
        serde_json::from_slice(&std::fs::read(directory.join("identities.json")).unwrap()).unwrap();
    Published {
        module: serde_json::from_value(identities["module"].clone()).unwrap(),
        dependency: serde_json::from_value(identities["dependency"].clone()).unwrap(),
        bridge: serde_json::from_value(identities["bridge"].clone()).unwrap(),
        manifest: std::fs::read(directory.join("manifest.json")).unwrap(),
        package: std::fs::read(directory.join("package.gfpb")).unwrap(),
    }
}

#[test]
fn interrupted_adoption_reopens_old_or_complete_closure_and_source_record_together() {
    let published = Published::new();
    for (failpoint, adopted) in [
        ("project.before_current_replace", false),
        ("project.after_current_replace", true),
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("project");
        let graph = GraphForge::new(path.to_str()).unwrap();
        graph.execute("CREATE (:Retained {value:42})").unwrap();
        let expected_row = retained_row(&graph);
        assert_eq!(expected_row.1, 42);
        drop(graph);
        let fixture = root.path().join("fixture");
        save_published(&published, &fixture);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "hub_clone::ontology_adoption::tests::adoption_publication_child",
                "--nocapture",
            ])
            .env("GF_ONTOLOGY_ADOPTION_PROJECT", &path)
            .env("GF_ONTOLOGY_ADOPTION_FIXTURE", fixture)
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINTS",
                "graphforge-internal-subprocess-v1",
            )
            .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "{failpoint}");
        let graph = GraphForge::new(path.to_str()).unwrap();
        assert_eq!(
            graph.workspace_ontology_composition().unwrap().is_some(),
            adopted
        );
        assert_eq!(
            graph
                .research_project_metadata()
                .unwrap()
                .extensions
                .keys()
                .any(|key| key.starts_with("graphforge.ontology_adoption:")),
            adopted
        );
        assert_eq!(retained_row(&graph), expected_row);
        drop(graph);
        let retry = run(&published.transport(), &published.command("adopt", &path)).unwrap();
        assert_eq!(retry["adoption"]["already_adopted"], adopted);
        published.assert_project(&path);
    }
}

#[test]
fn strict_source_enforcement_is_preserved_and_cannot_admit_incompatible_retained_data() {
    let published = Published::with_mode(graphforge_api::ActivationMode::Strict);
    let root = tempfile::tempdir().unwrap();
    let fresh = root.path().join("fresh");
    run(
        &published.transport(),
        &published.command("start_from", &fresh),
    )
    .unwrap();
    let graph = published.assert_project(&fresh);
    assert_eq!(
        graph.ontology_activation_profile().unwrap().0,
        graphforge_api::ActivationMode::Strict
    );
    assert!(graph.execute("CREATE (:Unknown)").is_err());
    drop(graph);

    let existing = root.path().join("existing");
    let graph = GraphForge::new(existing.to_str()).unwrap();
    graph.execute("CREATE (:Retained {value:42})").unwrap();
    let before = graph.committed_generation_identity().unwrap();
    let row = retained_row(&graph);
    drop(graph);
    assert!(
        run(
            &published.transport(),
            &published.command("adopt", &existing)
        )
        .is_err()
    );
    let graph = GraphForge::new(existing.to_str()).unwrap();
    assert_eq!(graph.committed_generation_identity().unwrap(), before);
    assert_eq!(retained_row(&graph), row);
    assert!(graph.workspace_ontology_composition().unwrap().is_none());
}
