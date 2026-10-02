//! The public Hub fixture: drift gate, and the Project page facts a Hub renders
//! from the checked-in `generated/v1` artifacts alone.

use graphforge_api::{
    DiscoveryOntologyModuleRequest, PortableV2Limits, resolve_discovered_ontology_module,
};
use graphforge_cli::hub_fixture_artifacts::{DEFAULT_LOCATION_BASE, check, generate};
use graphforge_discovery::{
    DiscoveryLimits, DiscoveryManifest, ExactIdentity, ProjectSummary, RefSet, RepositoryIdentity,
};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn source() -> PathBuf {
    root().join("tests/fixtures/hub/openalex-source")
}

fn generated() -> PathBuf {
    root().join("tests/fixtures/hub/generated/v1")
}

fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::from("sha256:"), |mut output, byte| {
            write!(output, "{byte:02x}").unwrap();
            output
        })
}

#[test]
fn checked_in_fixture_has_no_drift() {
    // Regenerates every artifact from the checked-in source and compares bytes.
    // On failure: `cargo run -p graphforge-cli --example generate_hub_fixture -- --update`.
    check(&source(), &generated()).unwrap();
}

#[test]
fn project_page_facts_come_from_the_checked_in_artifacts_alone() {
    let manifest_bytes = fs::read(generated().join("manifest.json")).unwrap();
    let refs_bytes = fs::read(generated().join("refs.json")).unwrap();
    let manifest =
        DiscoveryManifest::from_json(&manifest_bytes, DiscoveryLimits::default()).unwrap();
    RefSet::from_json(&refs_bytes, DiscoveryLimits::default())
        .unwrap()
        .validate_manifest(&manifest)
        .unwrap();

    // Select the summary object the way a consumer does: by manifest descriptor.
    let descriptor = manifest.summary_object().unwrap();
    let summary_bytes =
        fs::read(generated().join("objects/openalex-openalex.summary.json")).unwrap();
    assert_eq!(digest(&summary_bytes), descriptor.digest.0);
    assert_eq!(summary_bytes.len() as u64, descriptor.length);
    let summary = ProjectSummary::from_json(&summary_bytes, DiscoveryLimits::default()).unwrap();
    manifest.bind_summary(&summary).unwrap();

    assert_eq!(summary.repository.owner, "openalex");
    assert_eq!(summary.repository.repository, "openalex");
    assert_eq!(summary.metadata.title.as_deref(), Some("OpenAlex"));
    assert_eq!(summary.metadata.license.as_deref(), Some("CC0-1.0"));
    assert_eq!(summary.metadata.authors, ["OurResearch"]);
    assert_eq!(summary.metadata.languages, ["en"]);
    assert_eq!(summary.metadata.subjects, ["scholarly-communication"]);
    assert_eq!(summary.metadata.source_types, ["bibliographic-database"]);
    assert_eq!(
        summary.metadata.ontologies,
        ["https://openalex.org/ontology/works"]
    );
    let size = summary.metadata.corpus_size.as_ref().unwrap();
    assert_eq!(size.node_count, Some(1_000));
    assert_eq!(size.relationship_count, Some(2_500));
    assert_eq!(size.source_count, Some(1));
    assert_eq!(size.artifact_count, Some(0));
    assert_eq!(
        summary.metadata.access.visibility.as_deref(),
        Some("public")
    );
    assert_eq!(
        summary.metadata.access.access_policy.as_deref(),
        Some("open")
    );
    assert_eq!(summary.facts.ontology_mode, "advisory");

    let composition = summary.facts.ontology_composition.as_ref().unwrap();
    assert_eq!(composition.modules.len(), 1);
    let module = &composition.modules[0];
    assert_eq!(module.id, "https://openalex.org/ontology/works");
    assert_eq!(module.version, "2026.01");

    // The manifest inventory advertises the same exact module with its own
    // package object, which is not the Project package.
    let inventory = manifest.ontology.as_ref().unwrap();
    assert_eq!(inventory.modules.len(), 1);
    let identity = ExactIdentity {
        id: module.id.clone(),
        version: module.version.clone(),
        content_digest: module.content_digest.clone(),
    };
    let (advertised, module_object) = manifest.ontology_module_object(&identity).unwrap();
    assert_eq!(advertised.content_digest, module.content_digest);
    assert_ne!(module_object.digest, manifest.package.object_digest);

    // The advertised module package resolves to exactly that module without
    // any Project graph data.
    let module_package = generated().join(format!(
        "objects/ontology-module-{}.gfpb",
        module.content_digest.0.strip_prefix("sha256:").unwrap()
    ));
    assert_eq!(
        digest(&fs::read(&module_package).unwrap()),
        module_object.digest.0
    );
    let resolved = resolve_discovered_ontology_module(&DiscoveryOntologyModuleRequest {
        manifest_json: &manifest_bytes,
        refs_json: &refs_bytes,
        expected_repository: &RepositoryIdentity::parse("openalex/openalex").unwrap(),
        module: &identity,
        package: &module_package,
        discovery_limits: DiscoveryLimits::default(),
        portable_limits: PortableV2Limits::default(),
        cancelled: None,
    })
    .unwrap();
    assert_eq!(resolved.module.ontology_id, module.id);
    assert_eq!(resolved.module.authored_version, module.version);
    assert!(!resolved.document.is_empty());
}

#[test]
fn summary_and_descriptors_survive_a_different_object_location_base() {
    let regenerated = tempfile::tempdir().unwrap();
    generate(
        &source(),
        regenerated.path(),
        "https://mirror.example.org/content/",
    )
    .unwrap();
    assert_ne!(DEFAULT_LOCATION_BASE, "https://mirror.example.org/content/");
    let relative = "objects/openalex-openalex.summary.json";
    assert_eq!(
        fs::read(regenerated.path().join(relative)).unwrap(),
        fs::read(generated().join(relative)).unwrap()
    );
    let manifest = |dir: &Path| -> serde_json::Value {
        serde_json::from_slice(&fs::read(dir.join("manifest.json")).unwrap()).unwrap()
    };
    let (mirrored, checked_in) = (manifest(regenerated.path()), manifest(&generated()));
    for field in ["summary", "ontology"] {
        assert!(!checked_in[field].is_null(), "{field}");
        assert_eq!(mirrored[field], checked_in[field], "{field}");
    }
    assert_ne!(mirrored["objects"], checked_in["objects"]);
}
