//! The public Hub fixture: drift gate, and the Project page facts a Hub renders
//! from the checked-in `generated/v1` artifacts alone.

use graphforge_api::{
    DiscoveryOntologyModuleRequest, GraphForge, OperationId, PortableImportRequest, PortableLimits,
    resolve_discovered_ontology_module,
};
use graphforge_cli::hub_fixture_artifacts::{
    DEFAULT_LOCATION_BASE, check, generate, rebuild_source,
};
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
    check(&source(), &generated(), DEFAULT_LOCATION_BASE).unwrap();
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
        fs::read(generated().join("objects/opendata-openalex.summary.json")).unwrap();
    assert_eq!(digest(&summary_bytes), descriptor.digest.0);
    assert_eq!(summary_bytes.len() as u64, descriptor.length);
    let summary = ProjectSummary::from_json(&summary_bytes, DiscoveryLimits::default()).unwrap();
    manifest.bind_summary(&summary).unwrap();

    assert_eq!(summary.repository.owner, "opendata");
    assert_eq!(summary.repository.repository, "openalex");
    assert_eq!(
        summary.metadata.title.as_deref(),
        Some("Synthetic OpenAlex-themed GraphForge example")
    );
    let description = summary.metadata.description.as_deref().unwrap();
    assert!(description.contains("invented authors and works"));
    assert!(description.contains("not affiliated with OpenAlex or OurResearch"));
    assert_eq!(summary.metadata.license.as_deref(), Some("CC0-1.0"));
    assert_eq!(summary.metadata.authors, ["GraphForge"]);
    assert_eq!(summary.metadata.languages, ["en"]);
    assert_eq!(summary.metadata.subjects, ["scholarly-communication"]);
    assert_eq!(summary.metadata.source_types, ["synthetic-example"]);
    assert_eq!(
        summary.metadata.ontologies,
        ["https://graphforge.sh/ont_drqf2xw8w33cxaxj"]
    );
    let size = summary.metadata.corpus_size.as_ref().unwrap();
    assert_eq!(size.node_count, Some(5));
    assert_eq!(size.relationship_count, Some(4));
    assert_eq!(size.source_count, None);
    assert_eq!(size.artifact_count, None);
    // The fixture publishes no research lineage and the summary carries no access
    // fields, so it lists no people.
    assert!(summary.people.is_empty());
    assert!(
        !String::from_utf8(summary_bytes.clone())
            .unwrap()
            .contains("access")
    );
    assert_eq!(summary.facts.ontology_mode, "advisory");

    let composition = summary.facts.ontology_composition.as_ref().unwrap();
    assert_eq!(composition.modules.len(), 1);
    let module = &composition.modules[0];
    assert_eq!(module.id, "https://graphforge.sh/ont_drqf2xw8w33cxaxj");
    assert_eq!(module.version, "2026.02");

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
        expected_repository: &RepositoryIdentity::parse("opendata/openalex").unwrap(),
        module: &identity,
        package: &module_package,
        discovery_limits: DiscoveryLimits::default(),
        portable_limits: PortableLimits::default(),
        cancelled: None,
    })
    .unwrap();
    assert_eq!(resolved.module.ontology_id, module.id);
    assert_eq!(resolved.module.authored_version, module.version);
    let document: graphforge_api::OntologyDoc = serde_json::from_slice(&resolved.document).unwrap();
    assert_eq!(
        document,
        serde_json::from_value::<graphforge_api::OntologyDoc>(serde_json::json!({
            "ontology_id": "https://graphforge.sh/ont_drqf2xw8w33cxaxj",
            "version": "2026.02",
            "entity_types": [
                {"name": "Work", "abstract": false, "parent": null},
                {"name": "Author", "abstract": false, "parent": null},
                {"name": "Institution", "abstract": false, "parent": null}
            ],
            "relation_types": [{"name": "AUTHORED", "src": "Author", "dst": "Work"}],
            "properties": [
                {"owner": "Author", "name": "name", "type": "utf8", "nullable": true, "multivalued": false},
                {"owner": "Work", "name": "title", "type": "utf8", "nullable": true, "multivalued": false}
            ], "constraints": [], "migrations": []
        }))
        .unwrap()
    );
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
    let relative = "objects/opendata-openalex.summary.json";
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

fn rows(graph: &GraphForge, query: &str) -> Vec<Vec<String>> {
    graph
        .execute(query)
        .unwrap()
        .batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows())
                .map(|row| {
                    batch
                        .columns()
                        .iter()
                        .map(|column| {
                            arrow::util::display::array_value_to_string(column.as_ref(), row)
                                .unwrap()
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn rebuilt_example_survives_generation_import_and_reopen_with_useful_authorship() {
    let workspace = tempfile::tempdir().unwrap();
    let source = workspace.path().join("source");
    rebuild_source(&source).unwrap();
    let artifacts = workspace.path().join("artifacts");
    generate(&source, &artifacts, DEFAULT_LOCATION_BASE).unwrap();
    let destination = workspace.path().join("imported");
    GraphForge::import_portable(
        &destination,
        &PortableImportRequest {
            input: artifacts.join("objects/opendata-openalex.gfpb"),
            operation_id: OperationId(uuid::Uuid::from_u128(
                0x019b_76da_a800_7000_a000_0000_0000_0001,
            )),
            limits: PortableLimits::default(),
        },
        None,
    )
    .unwrap();
    // The import writer is gone; open, drop and reopen the durable Project.
    drop(GraphForge::new(destination.to_str()).unwrap());
    let graph = GraphForge::new(destination.to_str()).unwrap();
    let measured = graph.profile_gsi().unwrap();
    assert_eq!((measured.node_count, measured.edge_count), (5, 4));
    assert_eq!(
        rows(&graph, "MATCH (a:Author) RETURN a.name ORDER BY a.name"),
        [vec!["Ada Example"], vec!["Basil Sample"]]
    );
    assert_eq!(
        rows(&graph, "MATCH (w:Work) RETURN w.title ORDER BY w.title"),
        [
            vec!["A Synthetic Atlas of Ideas"],
            vec!["Invented Networks for Learning"],
            vec!["Shared Paths in Example Graphs"]
        ]
    );
    assert_eq!(
        rows(
            &graph,
            "MATCH (a:Author)-[r:AUTHORED]->(w:Work) RETURN a.name, w.title ORDER BY a.name, w.title"
        ),
        [
            vec!["Ada Example", "A Synthetic Atlas of Ideas"],
            vec!["Ada Example", "Shared Paths in Example Graphs"],
            vec!["Basil Sample", "Invented Networks for Learning"],
            vec!["Basil Sample", "Shared Paths in Example Graphs"]
        ]
    );
    assert_eq!(
        rows(
            &graph,
            "MATCH (a:Author)-[:AUTHORED]->(w:Work) RETURN a.name, count(w) AS works ORDER BY a.name"
        ),
        [vec!["Ada Example", "2"], vec!["Basil Sample", "2"]]
    );
    assert_eq!(
        rows(
            &graph,
            "MATCH (a:Author)-[:AUTHORED]->(w:Work)<-[:AUTHORED]-(b:Author) WHERE a.name < b.name RETURN a.name, b.name, w.title"
        ),
        [vec![
            "Ada Example",
            "Basil Sample",
            "Shared Paths in Example Graphs"
        ]]
    );
    assert_eq!(
        graph.labels().unwrap(),
        [
            "ont_drqf2xw8w33cxaxj:entity:Author",
            "ont_drqf2xw8w33cxaxj:entity:Work"
        ]
    );
    assert_eq!(
        graph
            .node_count("ont_drqf2xw8w33cxaxj:entity:Institution")
            .unwrap(),
        0
    );
    for (query, expected) in [
        ("MATCH (n) RETURN n.node_uuid", (1..=5).collect::<Vec<_>>()),
        ("MATCH ()-[r]->() RETURN r.edge_uuid", (6..=9).collect()),
    ] {
        let mut ids = Vec::new();
        for batch in graph.execute(query).unwrap().batches {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                ids.push(uuid::Uuid::from_slice(values.value(row)).unwrap());
            }
        }
        ids.sort();
        let expected: Vec<_> = expected
            .into_iter()
            .map(|sequence| {
                uuid::Uuid::from_u128(0x019b_76da_a800_7000_8000_0000_0000_0000 + sequence)
            })
            .collect();
        assert_eq!(ids, expected);
        assert!(
            ids.iter()
                .all(|id| id.get_version() == Some(uuid::Version::SortRand))
        );
    }
    let summary = ProjectSummary::from_json(
        &fs::read(artifacts.join("objects/opendata-openalex.summary.json")).unwrap(),
        DiscoveryLimits::default(),
    )
    .unwrap();
    let metadata = graph.research_project_metadata().unwrap();
    let size = metadata.corpus_size.as_ref().unwrap();
    assert_eq!(size.node_count, Some(measured.node_count));
    assert_eq!(size.relationship_count, Some(measured.edge_count));
    let summary_size = summary.metadata.corpus_size.unwrap();
    assert_eq!(summary_size.node_count, size.node_count);
    assert_eq!(summary_size.relationship_count, size.relationship_count);
    assert_eq!(
        (summary_size.source_count, summary_size.artifact_count),
        (None, None)
    );
    assert_eq!(summary.metadata.title, metadata.title);
    assert_eq!(summary.metadata.description, metadata.description);
    assert_eq!(summary.metadata.authors, metadata.authors);
    let composition = graph.workspace_ontology_composition().unwrap().unwrap();
    assert_eq!(
        composition.profile_default,
        graphforge_api::ActivationMode::Advisory
    );
    assert_eq!(composition.modules.len(), 1);
    let module = &summary.facts.ontology_composition.as_ref().unwrap().modules[0];
    assert_eq!(module.id, "https://graphforge.sh/ont_drqf2xw8w33cxaxj");
    assert_eq!(module.version, "2026.02");
    assert_eq!(
        module.content_digest.0,
        "sha256:58a58d8d03535addad7e28fb55c20ff9b3f06f5aa41c35184621402e90818a0e"
    );
}
