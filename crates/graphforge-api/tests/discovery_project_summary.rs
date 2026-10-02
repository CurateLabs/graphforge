//! Project summary derivation from verified portable-v2 packages, over real
//! durable Projects and real exports.

use graphforge_api::{
    ActivationMode, ActivationProfileChangeRequest, AdoptOntologyRequest, DiscoveryPortableV2Error,
    GraphForge, ModuleAdoptionRequest, OntologyAuthorityExpectation, OntologyMode,
    PortableSelection, PortableV2ExportRequest, PortableV2Limits, PortableV2Mode, PortableV2Output,
    PortableV2PackageIndex, PortableV2ParticipantId, PortableV2SelectionProfile,
    PortableVerifyRequest, ProjectSummaryRequest, UpdateResearchMetadataRequest,
    WorkspaceResearchMetadata, WriteContext, summarize_verified_portable_v2, verify_portable_v2,
};
use graphforge_discovery::{
    BridgeSetDescriptor, DISCOVERY_FORMAT, DiscoveryErrorCode, DiscoveryLimits, DiscoveryManifest,
    ObjectDescriptor, OntologyInventory, OntologyModuleDescriptor, PORTABLE_V2_FORMAT,
    PORTABLE_V2_MEDIA_TYPE, PROJECT_SUMMARY_FORMAT, PROJECT_SUMMARY_MEDIA_TYPE,
    PortablePackageReference, ProjectSummary, ProjectSummaryReference, ProtocolRequirement,
    ProtocolVersion, RepositoryIdentity, Sha256Digest,
};
use graphforge_ontology::OntologyDoc;
use graphforge_storage::{
    ResearchAccessPolicyMetadata, ResearchCorpusSize, ResearchDiscoveryFacets,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

const BAGIT: &[u8] = b"BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n";
const BAG_INFO: &[u8] = b"Bag-Software-Agent: GraphForge portable-v2\nBagging-Date: 1970-01-01\n";
const MANIFEST_PATH: &str = "data/graphforge-project.json";

const PRIVATE_COLLABORATOR: &str = "private-collaborator@example.org";
const PRIVATE_EXTENSION: &str = "private-extension-value";

fn hex(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .fold(String::new(), |mut output, byte| {
            write!(output, "{byte:02x}").unwrap();
            output
        })
}

fn marker(character: char) -> Sha256Digest {
    Sha256Digest(format!("sha256:{}", character.to_string().repeat(64)))
}

fn repository() -> RepositoryIdentity {
    RepositoryIdentity::parse("example/summary-project").unwrap()
}

fn module_document() -> OntologyDoc {
    serde_json::from_value(serde_json::json!({
        "ontology_id": "https://example.org/ontology/summary-module",
        "version": "1",
        "entity_types": [{"name": "Person", "abstract": false, "parent": null}],
        "relation_types": [],
        "properties": [],
        "constraints": [],
        "migrations": []
    }))
    .unwrap()
}

fn rich_metadata() -> WorkspaceResearchMetadata {
    let mut metadata = WorkspaceResearchMetadata::empty();
    metadata.title = Some("Summary Corpus".into());
    metadata.description = Some("A corpus used to test public summaries.".into());
    metadata.authors = vec!["Ada Lovelace".into(), "Grace Hopper".into()];
    metadata.subjects = vec!["history".into(), "linguistics".into()];
    metadata.languages = vec!["en".into()];
    metadata.license = Some("CC0-1.0".into());
    metadata.ontologies = vec!["https://example.org/ontology/summary-module".into()];
    metadata.corpus_size = Some(ResearchCorpusSize {
        node_count: Some(1_000),
        relationship_count: Some(2_500),
        source_count: Some(1),
        artifact_count: None,
    });
    metadata.access = ResearchAccessPolicyMetadata {
        visibility: Some("public".into()),
        access_policy: Some("open".into()),
        collaborators: vec![PRIVATE_COLLABORATOR.into()],
    };
    metadata.extensions = BTreeMap::from([("x-private".into(), Value::from(PRIVATE_EXTENSION))]);
    metadata.discovery_facets = ResearchDiscoveryFacets {
        text_entry_points: 7,
        ..ResearchDiscoveryFacets::default()
    };
    metadata.created_at = Some("2026-01-01T00:00:00.000000Z".into());
    metadata.updated_at = Some("2026-01-02T00:00:00.000000Z".into());
    metadata
}

struct Exported {
    _root: tempfile::TempDir,
    bundle: PathBuf,
    expanded: PathBuf,
}

/// Create a real durable Project with rich research metadata and one adopted
/// module, then export the complete Project as both a bundle and a directory.
fn exported_project() -> Exported {
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    fs::create_dir(&project).unwrap();
    let mut graph = GraphForge::new(project.to_str()).expect("durable Project");
    graph
        .update_research_metadata(UpdateResearchMetadataRequest {
            context: WriteContext {
                operation_uuid: graphforge_api::OperationId(Uuid::from_u128(9_100)),
                actor_uuid: None,
            },
            metadata: rich_metadata(),
        })
        .expect("write metadata");
    let candidate = graph
        .create_ontology_module(module_document(), Vec::new(), None)
        .expect("validate module");
    let state = graph.ontology_authority_state().expect("authority state");
    graph
        .adopt_ontology_module(
            &ModuleAdoptionRequest {
                authority: OntologyAuthorityExpectation {
                    context: WriteContext {
                        operation_uuid: graphforge_api::OperationId(Uuid::from_u128(9_200)),
                        actor_uuid: None,
                    },
                    expected_project_generation_uuid: state.project_generation_uuid,
                    expected_composition_fingerprint: state.composition_fingerprint,
                },
                candidate,
            },
            None,
        )
        .expect("adopt module");
    let bundle = root.path().join("project.gfpb");
    let expanded = root.path().join("project.gfproject");
    for (path, representation) in [
        (&bundle, PortableV2Output::Bundle),
        (&expanded, PortableV2Output::Expanded),
    ] {
        graph
            .export_portable_v2(
                &PortableV2ExportRequest {
                    selection: PortableSelection::Current,
                    output_path: path.clone(),
                    representation,
                    profile: PortableV2SelectionProfile::Complete,
                    subset: None,
                    limits: PortableV2Limits::default(),
                },
                None,
                |_| {},
            )
            .expect("export complete Project");
    }
    Exported {
        _root: root,
        bundle,
        expanded,
    }
}

fn summarize(package: &Path) -> Result<ProjectSummary, DiscoveryPortableV2Error> {
    summarize_verified_portable_v2(&ProjectSummaryRequest {
        repository: &repository(),
        immutable_version: &marker('a'),
        package,
        discovery_limits: DiscoveryLimits::default(),
        portable_limits: PortableV2Limits::default(),
        cancelled: None,
    })
}

fn report(package: &Path) -> graphforge_core::portable::PortableV2Report {
    verify_portable_v2(
        &PortableVerifyRequest {
            input: package.to_path_buf(),
            mode: PortableV2Mode::Full,
            limits: PortableV2Limits::default(),
        },
        None,
    )
    .unwrap()
}

fn keys(value: &Value, found: &mut BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                found.insert(key.clone());
                keys(value, found);
            }
        }
        Value::Array(values) => values.iter().for_each(|value| keys(value, found)),
        _ => {}
    }
}

#[test]
fn summary_bytes_are_identical_for_bundle_and_expanded_and_facts_are_verified() {
    let exported = exported_project();
    let from_bundle = summarize(&exported.bundle).unwrap();
    let from_directory = summarize(&exported.expanded).unwrap();
    assert_eq!(from_bundle, from_directory);
    let bytes = from_bundle.to_canonical_json().unwrap();
    assert_eq!(bytes, from_directory.to_canonical_json().unwrap());
    from_bundle.validate(DiscoveryLimits::default()).unwrap();
    assert_eq!(
        ProjectSummary::from_json(&bytes, DiscoveryLimits::default()).unwrap(),
        from_bundle
    );

    let report = report(&exported.bundle);
    assert_eq!(from_bundle.package.package_digest.0, report.package_digest);
    assert_eq!(from_bundle.package.package_class, "complete");
    assert_eq!(from_bundle.facts.payload_bytes, report.payload_bytes);

    // Component histogram, derived independently from the authenticated manifest.
    let manifest: Value =
        serde_json::from_slice(&fs::read(exported.expanded.join(MANIFEST_PATH)).unwrap()).unwrap();
    let mut expected = BTreeMap::<String, u64>::new();
    for component in manifest["components"].as_array().unwrap() {
        *expected
            .entry(component["kind"].as_str().unwrap().to_owned())
            .or_default() += 1;
    }
    assert_eq!(from_bundle.facts.components, expected);
    assert_eq!(
        from_bundle.facts.components.values().sum::<u64>(),
        report.component_count
    );
    assert_eq!(
        from_bundle.facts.research_present,
        report.research_interchange
    );
    assert!(!from_bundle.facts.evidence_present);

    // Ontology composition mirrors the verified report, in `sha256:` form.
    let composition = report.ontology_composition.as_ref().unwrap();
    let summarized = from_bundle.facts.ontology_composition.as_ref().unwrap();
    assert_eq!(
        summarized.composition_digest.0,
        composition.composition_digest
    );
    assert!(summarized.composition_digest.0.starts_with("sha256:"));
    assert_eq!(summarized.modules.len(), composition.modules.len());
    for (summary, verified) in summarized.modules.iter().zip(&composition.modules) {
        assert_eq!(summary.id, verified.ontology_id);
        assert_eq!(summary.version, verified.version);
        assert_eq!(summary.content_digest.0, verified.content_digest);
        assert!(summary.content_digest.0.starts_with("sha256:"));
        assert_eq!(summary.dialect, verified.dialect);
        assert_eq!(summary.profile, verified.profile);
    }
    let bridges: Vec<_> = composition
        .bridge_sets
        .iter()
        .map(|bridge| BridgeSetDescriptor {
            id: bridge.bridge_id.clone(),
            version: bridge.version.clone(),
            content_digest: Sha256Digest(bridge.content_digest.clone()),
        })
        .collect();
    assert_eq!(summarized.bridge_sets, bridges);
    assert_eq!(summarized.modules.len(), 1);
    assert_eq!(
        summarized.modules[0].id,
        "https://example.org/ontology/summary-module"
    );
    // Adoption writes `configuration.ontology_mode` from the composition's
    // profile default, which is Exploratory unless changed, and Exploratory is
    // reported as `none`. The activation-profile test below covers the others.
    assert_eq!(from_bundle.facts.ontology_mode, "none");
}

#[test]
fn public_projection_excludes_collaborators_extensions_and_discovery_facets() {
    let exported = exported_project();
    let summary = summarize(&exported.bundle).unwrap();
    let bytes = summary.to_canonical_json().unwrap();
    let text = String::from_utf8(bytes.clone()).unwrap();

    let mut found = BTreeSet::new();
    keys(
        &serde_json::from_slice::<Value>(&bytes).unwrap(),
        &mut found,
    );
    for excluded in ["collaborators", "discovery_facets", "extensions"] {
        assert!(!found.contains(excluded), "{excluded} leaked into summary");
    }
    assert!(!text.contains(PRIVATE_COLLABORATOR));
    assert!(!text.contains(PRIVATE_EXTENSION));
    assert!(!text.contains("x-private"));
    assert!(!text.contains("text_entry_points"));
    // Local paths and Project identity are never consulted.
    assert!(!text.contains(exported.bundle.parent().unwrap().to_str().unwrap()));

    // Public fields survive, including consumer access metadata.
    assert_eq!(summary.metadata.title.as_deref(), Some("Summary Corpus"));
    assert_eq!(summary.metadata.license.as_deref(), Some("CC0-1.0"));
    assert_eq!(summary.metadata.authors, ["Ada Lovelace", "Grace Hopper"]);
    assert_eq!(
        summary.metadata.access.visibility.as_deref(),
        Some("public")
    );
    assert_eq!(
        summary.metadata.access.access_policy.as_deref(),
        Some("open")
    );
    assert_eq!(
        summary
            .metadata
            .corpus_size
            .and_then(|size| size.node_count),
        Some(1_000)
    );
    assert_eq!(
        summary.metadata.created_at.as_deref(),
        Some("2026-01-01T00:00:00.000000Z")
    );
}

#[test]
fn summary_binds_to_a_manifest_built_with_discovery_types() {
    let exported = exported_project();
    let summary = summarize(&exported.expanded).unwrap();
    let bytes = summary.to_canonical_json().unwrap();
    let object_digest = Sha256Digest(format!("sha256:{}", hex(Sha256::digest(&bytes))));
    let composition = summary.facts.ontology_composition.clone().unwrap();
    let manifest = DiscoveryManifest {
        format: DISCOVERY_FORMAT.into(),
        version: ProtocolVersion::CURRENT,
        repository: repository(),
        default_ref: "main".into(),
        resolved_ref: "main".into(),
        immutable_version: marker('a'),
        package: PortablePackageReference {
            format: PORTABLE_V2_FORMAT.into(),
            package_digest: summary.package.package_digest.clone(),
            object_digest: marker('c'),
        },
        summary: Some(ProjectSummaryReference {
            format: PROJECT_SUMMARY_FORMAT.into(),
            summary_digest: summary.canonical_digest().unwrap(),
            object_digest: object_digest.clone(),
        }),
        ontology: Some(OntologyInventory {
            composition_digest: composition.composition_digest.clone(),
            modules: composition
                .modules
                .iter()
                .map(|module| OntologyModuleDescriptor {
                    id: module.id.clone(),
                    version: module.version.clone(),
                    content_digest: module.content_digest.clone(),
                    package: None,
                })
                .collect(),
            bridge_sets: composition.bridge_sets.clone(),
        }),
        requirements: vec![ProtocolRequirement {
            capability: "portable-v2".into(),
            major: 1,
        }],
        capabilities: vec![],
        objects: {
            let mut objects = vec![
                ObjectDescriptor {
                    digest: marker('c'),
                    length: 1,
                    media_type: PORTABLE_V2_MEDIA_TYPE.into(),
                    locations: vec!["https://data.example.org/project".into()],
                },
                ObjectDescriptor {
                    digest: object_digest,
                    length: bytes.len() as u64,
                    media_type: PROJECT_SUMMARY_MEDIA_TYPE.into(),
                    locations: vec!["https://data.example.org/summary".into()],
                },
            ];
            objects.sort_by(|left, right| left.digest.0.cmp(&right.digest.0));
            objects
        },
        extensions: BTreeMap::default(),
    };
    let manifest = DiscoveryManifest::from_json(
        &serde_json::to_vec(&manifest).unwrap(),
        DiscoveryLimits::default(),
    )
    .unwrap();
    assert_eq!(
        manifest.summary_object().unwrap().length,
        bytes.len() as u64
    );
    let parsed = ProjectSummary::from_json(&bytes, DiscoveryLimits::default()).unwrap();
    manifest.bind_summary(&parsed).unwrap();

    // A different immutable version is a different summary.
    let mut other = parsed;
    other.immutable_version = marker('b');
    assert_eq!(
        manifest.bind_summary(&other).unwrap_err().code,
        DiscoveryErrorCode::IntegrityFailure
    );
}

/// A minimal valid ontology-only package with no runtime map and therefore no
/// workspace participants.
fn participantless_package() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    let mut value: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/portable-v2/ontology-only.manifest.json"
    ))
    .unwrap();
    value.as_object_mut().unwrap().remove("package_digest");
    let semantic = serde_json::to_vec(&value).unwrap();
    let package_digest = format!(
        "sha256:{}",
        hex(Sha256::digest(
            [b"graphforge-project/2\0".as_slice(), semantic.as_slice()].concat()
        ))
    );
    value["package_digest"] = Value::String(package_digest);
    let manifest = serde_json::to_vec(&value).unwrap();
    let payload_path = "data/components/ontology/core-ontology/ontology.json";
    let manifest_path = root.path().join(MANIFEST_PATH);
    fs::create_dir_all(manifest_path.parent().unwrap()).unwrap();
    fs::write(&manifest_path, &manifest).unwrap();
    let payload = root.path().join(payload_path);
    fs::create_dir_all(payload.parent().unwrap()).unwrap();
    fs::write(payload, b"{}").unwrap();
    fs::write(root.path().join("bagit.txt"), BAGIT).unwrap();
    fs::write(root.path().join("bag-info.txt"), BAG_INFO).unwrap();
    let data_manifest = format!(
        "{}  {}\n{}  {}\n",
        hex(Sha256::digest(b"{}")),
        payload_path,
        hex(Sha256::digest(&manifest)),
        MANIFEST_PATH
    );
    fs::write(root.path().join("manifest-sha256.txt"), &data_manifest).unwrap();
    let tag_manifest = format!(
        "{}  bag-info.txt\n{}  bagit.txt\n{}  manifest-sha256.txt\n",
        hex(Sha256::digest(BAG_INFO)),
        hex(Sha256::digest(BAGIT)),
        hex(Sha256::digest(data_manifest.as_bytes()))
    );
    fs::write(root.path().join("tagmanifest-sha256.txt"), tag_manifest).unwrap();
    root
}

#[test]
fn absent_participants_yield_empty_metadata_and_ontology_mode_none() {
    let package = participantless_package();
    let summary = summarize(package.path()).unwrap();
    assert_eq!(summary.facts.ontology_mode, "none");
    assert_eq!(
        summary.facts.components,
        BTreeMap::from([("ontology".to_owned(), 1)])
    );
    assert!(!summary.facts.research_present);
    assert!(summary.facts.ontology_composition.is_none());
    let metadata = serde_json::to_value(&summary.metadata).unwrap();
    let mut populated = BTreeSet::new();
    for (key, value) in metadata.as_object().unwrap() {
        let empty = value.is_null()
            || value.as_array().is_some_and(Vec::is_empty)
            || (key == "access" && value.as_object().unwrap().values().all(Value::is_null));
        if !empty {
            populated.insert(key.clone());
        }
    }
    assert!(populated.is_empty(), "unexpected metadata: {populated:?}");
    summary.validate(DiscoveryLimits::default()).unwrap();
}

#[test]
fn unknown_required_capability_fails_before_any_package_io() {
    let exported = exported_project();
    let summary = summarize(&exported.bundle).unwrap();
    let mut document: Value =
        serde_json::from_slice(&summary.to_canonical_json().unwrap()).unwrap();
    document["requirements"] = serde_json::json!([{"capability": "future-summary", "major": 1}]);
    let future = serde_json::to_vec(&document).unwrap();
    let never_created = exported.bundle.with_file_name("does-not-exist.gfpb");

    // A consumer parses the summary before it touches any package path.
    let consume = |summary: &[u8]| -> Result<(), String> {
        ProjectSummary::from_json(summary, DiscoveryLimits::default())
            .map_err(|error| format!("{:?}", error.code))?;
        verify_portable_v2(
            &PortableVerifyRequest {
                input: never_created.clone(),
                mode: PortableV2Mode::Full,
                limits: PortableV2Limits::default(),
            },
            None,
        )
        .map(|_| ())
        .map_err(|error| format!("package:{:?}", error.code))
    };
    assert_eq!(consume(&future).unwrap_err(), "UnsupportedFuture");
    // `ProjectSummary::from_json` rejects the future summary before the
    // package call below is made. The control: a supported summary proceeds to
    // that call, which fails on the missing path.
    let supported = summary.to_canonical_json().unwrap();
    assert_eq!(consume(&supported).unwrap_err(), "package:Io");
}

#[test]
fn ontology_mode_follows_the_persisted_configuration() {
    for (mode, label) in [
        (OntologyMode::Advisory, "advisory"),
        (OntologyMode::Strict, "strict"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let project = root.path().join("project");
        fs::create_dir(&project).unwrap();
        let document = root.path().join("ontology.json");
        fs::write(&document, serde_json::to_vec(&module_document()).unwrap()).unwrap();
        let mut graph = GraphForge::new(project.to_str()).expect("durable Project");
        graph
            .adopt_ontology(AdoptOntologyRequest {
                context: WriteContext {
                    operation_uuid: graphforge_api::OperationId(Uuid::from_u128(9_300)),
                    actor_uuid: None,
                },
                path: document,
                mode,
            })
            .expect("adopt ontology");
        let bundle = root.path().join("project.gfpb");
        graph
            .export_portable_v2(
                &PortableV2ExportRequest {
                    selection: PortableSelection::Current,
                    output_path: bundle.clone(),
                    representation: PortableV2Output::Bundle,
                    profile: PortableV2SelectionProfile::Complete,
                    subset: None,
                    limits: PortableV2Limits::default(),
                },
                None,
                |_| {},
            )
            .expect("export complete Project");
        let summary = summarize(&bundle).unwrap();
        assert_eq!(summary.facts.ontology_mode, label);
        summary.validate(DiscoveryLimits::default()).unwrap();
    }
}

#[test]
fn multi_module_activation_profile_sets_the_summary_mode() {
    for (profile_default, label) in [
        (ActivationMode::Exploratory, "none"),
        (ActivationMode::Advisory, "advisory"),
        (ActivationMode::Strict, "strict"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let project = root.path().join("project");
        fs::create_dir(&project).unwrap();
        let mut graph = GraphForge::new(project.to_str()).expect("durable Project");
        let candidate = graph
            .create_ontology_module(module_document(), Vec::new(), None)
            .expect("validate module");
        let authority = |graph: &GraphForge, operation: u128| {
            let state = graph.ontology_authority_state().expect("authority state");
            OntologyAuthorityExpectation {
                context: WriteContext {
                    operation_uuid: graphforge_api::OperationId(Uuid::from_u128(operation)),
                    actor_uuid: None,
                },
                expected_project_generation_uuid: state.project_generation_uuid,
                expected_composition_fingerprint: state.composition_fingerprint,
            }
        };
        graph
            .adopt_ontology_module(
                &ModuleAdoptionRequest {
                    authority: authority(&graph, 9_400),
                    candidate,
                },
                None,
            )
            .expect("adopt module");
        let (_, activation) = graph.ontology_activation_profile().unwrap();
        graph
            .change_ontology_activation_profile(
                &ActivationProfileChangeRequest {
                    authority: authority(&graph, 9_401),
                    profile_default,
                    activation,
                },
                None,
            )
            .expect("change activation profile");
        let bundle = root.path().join("project.gfpb");
        graph
            .export_portable_v2(
                &PortableV2ExportRequest {
                    selection: PortableSelection::Current,
                    output_path: bundle.clone(),
                    representation: PortableV2Output::Bundle,
                    profile: PortableV2SelectionProfile::Complete,
                    subset: None,
                    limits: PortableV2Limits::default(),
                },
                None,
                |_| {},
            )
            .expect("export complete Project");
        let summary = summarize(&bundle).unwrap();
        assert_eq!(summary.facts.ontology_mode, label);
        assert_eq!(
            summary
                .facts
                .ontology_composition
                .as_ref()
                .unwrap()
                .modules
                .len(),
            1
        );
    }
}

/// Rewrite one manifest-listed file of an expanded package and re-seal every
/// digest that covers it, so the package stays fully verifiable.
fn replace_package_file(package: &Path, path: &str, bytes: &[u8]) {
    let manifest_file = package.join(MANIFEST_PATH);
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_file).unwrap()).unwrap();
    let mut listed = false;
    for component in manifest["components"].as_array_mut().unwrap() {
        for file in component["files"].as_array_mut().unwrap() {
            if file["path"] == path {
                file["length"] = Value::from(bytes.len());
                file["sha256"] = Value::from(hex(Sha256::digest(bytes)));
                listed = true;
            }
        }
    }
    assert!(listed, "{path} is not listed by the manifest");
    manifest.as_object_mut().unwrap().remove("package_digest");
    let semantic = serde_json::to_vec(&manifest).unwrap();
    manifest["package_digest"] = Value::from(format!(
        "sha256:{}",
        hex(Sha256::digest(
            [b"graphforge-project/2\0".as_slice(), semantic.as_slice()].concat()
        ))
    ));
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    fs::write(package.join(path), bytes).unwrap();
    fs::write(&manifest_file, &manifest_bytes).unwrap();

    let reseal = |inventory: &Path, replacements: &[(&str, String)]| -> Vec<u8> {
        let text = fs::read_to_string(inventory).unwrap();
        let mut rewritten = String::new();
        for line in text.lines() {
            let (original, name) = line.split_once("  ").unwrap();
            let digest = replacements
                .iter()
                .find(|(candidate, _)| *candidate == name)
                .map_or(original, |(_, replacement)| replacement.as_str());
            writeln!(rewritten, "{digest}  {name}").unwrap();
        }
        fs::write(inventory, &rewritten).unwrap();
        rewritten.into_bytes()
    };
    let data = reseal(
        &package.join("manifest-sha256.txt"),
        &[
            (path, hex(Sha256::digest(bytes))),
            (MANIFEST_PATH, hex(Sha256::digest(&manifest_bytes))),
        ],
    );
    reseal(
        &package.join("tagmanifest-sha256.txt"),
        &[("manifest-sha256.txt", hex(Sha256::digest(&data)))],
    );
}

#[test]
fn malformed_research_metadata_participant_is_a_participant_error() {
    let exported = exported_project();
    let digest = report(&exported.expanded).package_digest;
    let record = PortableV2PackageIndex::open(
        &exported.expanded,
        &digest,
        PortableV2Limits::default(),
        None,
    )
    .unwrap()
    .participant_file(&PortableV2ParticipantId {
        capability_id: "workspace".into(),
        record_family_id: "research_metadata".into(),
    })
    .unwrap()
    .unwrap();
    replace_package_file(&exported.expanded, &record.path, b"{}\n");

    // The rewritten package still verifies: only the participant record is bad.
    report(&exported.expanded);
    let error = summarize(&exported.expanded).unwrap_err();
    assert!(
        matches!(
            &error,
            DiscoveryPortableV2Error::Participant { participant, .. }
                if *participant == "workspace/research_metadata"
        ),
        "{error:?}"
    );
}
