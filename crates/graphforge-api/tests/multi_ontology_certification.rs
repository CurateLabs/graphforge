//! Public Rust certification for the reproducible six-domain M9 fixture.

use std::collections::BTreeMap;

use graphforge_api::{
    ActivationMode, ActivationProfileChangeRequest, BridgeAdoptionRequest, BridgeDocument,
    BridgeExportFormat, BridgeSelector, CancellationToken, GraphForge, ModuleAdoptionRequest,
    ModuleMigrationRequest, ModuleSelector, OntologyAuthorityExpectation, OntologyModuleId,
    ResolutionExplainRequest, SemanticMigrationOperation, SymbolKind, WriteContext,
};
use graphforge_ontology::OntologyDoc;
use serde::Deserialize;
use uuid::Uuid;

const FIXTURE_ROOT: &str = "../../tests/fixtures/multi-ontology-v1/certification-v1";

#[derive(Debug, Deserialize)]
struct FixtureManifest {
    contract: String,
    modules: Vec<String>,
    shared_module: String,
    bridges: Vec<String>,
    migration_target: String,
    expected: FixtureExpected,
}

#[derive(Debug, Deserialize)]
struct FixtureExpected {
    module_count: usize,
    bridge_count: usize,
    bridge_ids: Vec<String>,
    ambiguous_symbol: String,
    ambiguous_candidates: Vec<String>,
    qualified_symbol: String,
    migration_steps: Vec<String>,
    migration_data_impacts: Vec<String>,
}

fn fixture_text(name: &str) -> String {
    std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(FIXTURE_ROOT)
            .join(name),
    )
    .unwrap_or_else(|error| panic!("read certification fixture {name}: {error}"))
}

fn fixture_document(name: &str) -> OntologyDoc {
    serde_json::from_str(&fixture_text(name))
        .unwrap_or_else(|error| panic!("parse certification fixture {name}: {error}"))
}

fn fixture_manifest() -> FixtureManifest {
    serde_json::from_str(&fixture_text("certification.json")).expect("parse fixture manifest")
}

fn expectation(graph: &GraphForge, operation: u128) -> OntologyAuthorityExpectation {
    let state = graph
        .ontology_authority_state()
        .expect("read ontology authority");
    OntologyAuthorityExpectation {
        context: WriteContext {
            operation_uuid: graphforge_api::OperationId(Uuid::from_u128(operation)),
            actor_uuid: Some(Uuid::from_u128(843)),
        },
        expected_project_generation_uuid: state.project_generation_uuid,
        expected_composition_fingerprint: state.composition_fingerprint,
    }
}

fn adopt_six_domains(graph: &mut GraphForge) -> BTreeMap<String, OntologyModuleId> {
    let manifest = fixture_manifest();
    let mut modules = BTreeMap::new();
    for (index, name) in manifest.modules.iter().enumerate() {
        let document = fixture_document(name);
        let ontology_id = document.ontology_id.clone();
        let candidate = graph
            .create_ontology_module(document, Vec::new(), None)
            .expect("validate fixture module");
        graph
            .adopt_ontology_module(
                &ModuleAdoptionRequest {
                    authority: expectation(graph, 843_000 + index as u128),
                    candidate: candidate.clone(),
                },
                None,
            )
            .expect("adopt fixture module");
        modules.insert(ontology_id, candidate.id);
    }
    modules
}

fn fixture_bridge(name: &str, modules: &BTreeMap<String, OntologyModuleId>) -> BridgeDocument {
    let replacements = [
        ("$research", "https://graphforge.dev/ontology/research"),
        ("$document", "https://graphforge.dev/ontology/document"),
        ("$genealogy", "https://graphforge.dev/ontology/genealogy"),
        ("$scientific", "https://graphforge.dev/ontology/scientific"),
        ("$provenance", "https://graphforge.dev/ontology/provenance"),
        ("$evidence", "https://graphforge.dev/ontology/evidence"),
    ];
    let mut encoded = fixture_text(name);
    for (placeholder, ontology_id) in replacements {
        let exact = modules
            .get(ontology_id)
            .unwrap_or_else(|| panic!("missing exact fixture identity {ontology_id}"));
        encoded = encoded.replace(
            &format!("\"{placeholder}\""),
            &serde_json::to_string(exact).expect("encode exact identity"),
        );
    }
    serde_json::from_str(&encoded)
        .unwrap_or_else(|error| panic!("parse bridge fixture {name}: {error}"))
}

fn adopt_fixture_bridges(
    graph: &mut GraphForge,
    manifest: &FixtureManifest,
    modules: &BTreeMap<String, OntologyModuleId>,
) {
    for (index, bridge_file) in manifest.bridges.iter().enumerate() {
        let bridge = graph
            .create_ontology_bridge(fixture_bridge(bridge_file, modules))
            .expect("validate exact bridge fixture");
        graph
            .adopt_ontology_bridge(
                &BridgeAdoptionRequest {
                    authority: expectation(graph, 843_100 + index as u128),
                    candidate: bridge,
                },
                None,
            )
            .expect("adopt exact bridge fixture");
    }
}

#[test]
fn fixture_is_reproducible_and_six_domain_authority_is_exact() {
    let manifest = fixture_manifest();
    assert_eq!(
        manifest.contract,
        "graphforge-multi-ontology-certification/1"
    );
    assert_eq!(manifest.modules.len(), manifest.expected.module_count);
    assert_eq!(manifest.shared_module, "evidence.json");
    assert_eq!(manifest.bridges.len(), manifest.expected.bridge_count);
    assert_eq!(manifest.migration_target, "genealogy-v2.json");
    assert_eq!(
        manifest.expected.migration_steps,
        [
            "rename_type:Person->Human",
            "rename_property:Human|full_name->display_name"
        ]
    );
    assert_eq!(
        manifest.expected.migration_data_impacts,
        [
            "entity_label:Person->Human",
            "property:Human.full_name->display_name"
        ]
    );

    let mut graph = GraphForge::new(None).expect("create certification graph");
    let adopted = adopt_six_domains(&mut graph);
    assert_eq!(adopted.len(), 6);
    let inventory = graph.ontology_modules().expect("list exact modules");
    assert_eq!(inventory.len(), manifest.expected.module_count);
    assert_eq!(
        inventory
            .iter()
            .map(|entry| entry.id.ontology_id.as_str())
            .collect::<Vec<_>>(),
        [
            "https://graphforge.dev/ontology/document",
            "https://graphforge.dev/ontology/evidence",
            "https://graphforge.dev/ontology/genealogy",
            "https://graphforge.dev/ontology/provenance",
            "https://graphforge.dev/ontology/research",
            "https://graphforge.dev/ontology/scientific",
        ]
    );
    for entry in inventory {
        let exact = graph
            .inspect_ontology_module(&ModuleSelector::Exact(entry.id.clone()))
            .expect("inspect exact module");
        assert_eq!(exact.entry.id, entry.id);
    }

    adopt_fixture_bridges(&mut graph, &manifest, &adopted);
    let bridges = graph.ontology_bridges().expect("list exact bridges");
    assert_eq!(bridges.len(), manifest.expected.bridge_count);
    assert_eq!(
        bridges
            .iter()
            .map(|entry| entry.id.bridge_id.clone())
            .collect::<Vec<_>>(),
        manifest.expected.bridge_ids
    );

    let genealogy = adopted
        .get("https://graphforge.dev/ontology/genealogy")
        .expect("genealogy identity")
        .clone();
    let qualified = graph
        .explain_ontology_resolution(&ResolutionExplainRequest {
            module: Some(genealogy),
            kind: SymbolKind::Entity,
            local_id: manifest.expected.ambiguous_symbol.clone(),
            max_candidates: 8,
        })
        .expect("qualified resolution");
    let outcome = qualified.outcome.expect("qualified symbol resolves");
    assert_eq!(
        format!(
            "{}:{}:{}",
            outcome.symbol.module.ontology_id,
            outcome.symbol.kind.as_str(),
            outcome.symbol.local_id
        ),
        manifest.expected.qualified_symbol
    );

    let ambiguous = graph
        .explain_ontology_resolution(&ResolutionExplainRequest {
            module: None,
            kind: SymbolKind::Entity,
            local_id: manifest.expected.ambiguous_symbol,
            max_candidates: 8,
        })
        .expect("bounded ambiguity explanation");
    assert!(ambiguous.outcome.is_none());
    let candidates = &ambiguous.diagnostics[0].candidates;
    assert_eq!(candidates, &manifest.expected.ambiguous_candidates);
}

#[test]
fn retained_data_migrates_atomically_and_exact_identity_reopens() {
    let manifest = fixture_manifest();
    let project = tempfile::tempdir().expect("durable certification project");
    let path = project.path().to_str().expect("UTF-8 project path");
    let mut graph = GraphForge::new(Some(path)).expect("open durable certification graph");
    let mut modules = BTreeMap::new();
    for (index, name) in manifest.modules.iter().enumerate() {
        let document = fixture_document(name);
        let ontology_id = document.ontology_id.clone();
        let candidate = graph
            .create_ontology_module(document, Vec::new(), None)
            .expect("validate fixture module");
        graph
            .adopt_ontology_module(
                &ModuleAdoptionRequest {
                    authority: expectation(&graph, 843_200 + index as u128),
                    candidate: candidate.clone(),
                },
                None,
            )
            .expect("adopt fixture module");
        modules.insert(ontology_id.clone(), candidate.id);
        if ontology_id == "https://graphforge.dev/ontology/genealogy" {
            graph
                .execute("CREATE (:Person {full_name: 'Ada Lovelace', birth_year: 1815})")
                .expect("create retained genealogy row through Rust engine");
        }
    }
    adopt_fixture_bridges(&mut graph, &manifest, &modules);
    for bridge in graph.ontology_bridges().expect("bridge inventory") {
        let exported = graph
            .export_ontology_bridge(
                &BridgeSelector::Exact(bridge.id.clone()),
                BridgeExportFormat::Json,
            )
            .expect("export exact bridge");
        let parsed: BridgeDocument = serde_json::from_str(&exported).expect("parse bridge export");
        let validation = graph
            .validate_ontology_bridge(&parsed)
            .expect("revalidate exported bridge");
        assert!(validation.valid, "{:?}", validation.diagnostics);
        assert_eq!(
            graph
                .export_ontology_bridge(
                    &BridgeSelector::Exact(bridge.id),
                    BridgeExportFormat::Json,
                )
                .expect("repeat exact bridge export"),
            exported
        );
    }

    let composition_before = graph
        .ontology_authority_state()
        .expect("authority before migration")
        .composition_fingerprint
        .expect("composition before migration");
    let request = ModuleMigrationRequest {
        authority: expectation(&graph, 843_300),
        selector: ModuleSelector::Exact(
            modules
                .get("https://graphforge.dev/ontology/genealogy")
                .expect("genealogy v1 identity")
                .clone(),
        ),
        document: fixture_document(&manifest.migration_target),
        dependencies: Vec::new(),
        enforcement: None,
    };
    let preview = graph
        .preview_migrate_ontology_module(&request)
        .expect("preview retained-data migration");
    assert!(preview.plan.retained_rows_scanned > 0);
    assert_eq!(preview.affected_bridges.len(), 1);
    assert!(
        preview
            .plan
            .operations
            .iter()
            .any(|operation| matches!(operation, SemanticMigrationOperation::RenameEntity { .. }))
    );
    assert!(
        preview.plan.operations.iter().any(|operation| matches!(
            operation,
            SemanticMigrationOperation::RenameProperty { .. }
        ))
    );
    let before_cancel = graph
        .ontology_authority_state()
        .expect("authority before cancellation");
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert_eq!(
        graph
            .migrate_ontology_module(&request, &preview, Some(&cancelled))
            .expect_err("cancelled migration must fail")
            .code(),
        "GF_CANCELLED"
    );
    assert_eq!(
        graph
            .ontology_authority_state()
            .expect("authority after cancellation"),
        before_cancel
    );
    let receipt = graph
        .migrate_ontology_module(&request, &preview, None)
        .expect("publish retained-data migration");
    let replay = graph
        .migrate_ontology_module(&request, &preview, None)
        .expect("exact idempotent replay");
    assert_eq!(replay, receipt);
    let mut forged = preview.clone();
    forged.plan.to_composition_fingerprint = "forged-composition".into();
    forged.plan.retained_rows_scanned = u64::MAX;
    assert_eq!(
        graph
            .migrate_ontology_module(&request, &forged, None)
            .expect_err("forged replay preview must fail")
            .code(),
        "GF_VALIDATION"
    );
    let mut changed = request.clone();
    changed.enforcement = Some(graphforge_api::ActivationMode::Advisory);
    let changed_preview = graph
        .preview_migrate_ontology_module(&changed)
        .expect("preview changed-content replay against exact parent");
    assert_eq!(
        graph
            .migrate_ontology_module(&changed, &changed_preview, None)
            .expect_err("changed content must conflict with published operation")
            .code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
    drop(graph);

    let reopened = GraphForge::new(Some(path)).expect("reopen migrated project");
    assert_eq!(
        reopened
            .ontology_authority_state()
            .expect("reopened authority")
            .composition_fingerprint
            .as_deref(),
        Some(receipt.composition_fingerprint.as_str())
    );
    let report = reopened
        .multi_ontology_certification_report(
            "rust",
            &composition_before,
            &receipt.plan_digest,
            receipt.retained_rows_scanned,
        )
        .expect("Rust-owned certification report");
    assert_eq!(report.retained_data.name, "Ada Lovelace");
    assert_eq!(report.retained_data.birth_year, 1815);
    assert_eq!(report.composition_after, receipt.composition_fingerprint);
    assert_ne!(report.composition_before, report.composition_after);
    if let Some(path) = std::env::var_os("GRAPHFORGE_MULTI_ONTOLOGY_CERTIFICATION_REPORT") {
        std::fs::write(path, serde_json::to_vec(&report).unwrap()).unwrap();
    }
}

#[test]
fn stale_parent_race_preserves_competing_current_and_retained_data() {
    let project = tempfile::tempdir().expect("durable stale-parent project");
    let path = project.path().to_str().expect("UTF-8 project path");
    let mut migration = GraphForge::new(Some(path)).expect("open migration facade");
    let candidate = migration
        .create_ontology_module(fixture_document("genealogy-v1.json"), Vec::new(), None)
        .expect("validate genealogy v1");
    migration
        .adopt_ontology_module(
            &ModuleAdoptionRequest {
                authority: expectation(&migration, 843_400),
                candidate: candidate.clone(),
            },
            None,
        )
        .expect("adopt genealogy v1");
    migration
        .execute("CREATE (:Person {full_name: 'Race Safe', birth_year: 1900})")
        .expect("create retained race row");
    let request = ModuleMigrationRequest {
        authority: expectation(&migration, 843_401),
        selector: ModuleSelector::Exact(candidate.id),
        document: fixture_document("genealogy-v2.json"),
        dependencies: Vec::new(),
        enforcement: None,
    };
    let preview = migration
        .preview_migrate_ontology_module(&request)
        .expect("preview migration before race");

    let mut competitor = GraphForge::new(Some(path)).expect("open competing facade");
    competitor
        .change_ontology_activation_profile(
            &ActivationProfileChangeRequest {
                authority: expectation(&competitor, 843_402),
                profile_default: ActivationMode::Advisory,
                activation: Vec::new(),
            },
            None,
        )
        .expect("publish competing authority");
    let competing = competitor
        .ontology_authority_state()
        .expect("competing authority");
    drop(competitor);

    assert!(
        migration
            .migrate_ontology_module(&request, &preview, None)
            .is_err()
    );
    drop(migration);
    let reopened = GraphForge::new(Some(path)).expect("reopen competing CURRENT");
    assert_eq!(
        reopened
            .ontology_authority_state()
            .expect("authority after stale race"),
        competing
    );
    let retained = reopened
        .execute("MATCH (person:Person) RETURN person.full_name AS name")
        .expect("query retained pre-migration row");
    assert_eq!(retained.batches[0].num_rows(), 1);
    assert_eq!(
        arrow::util::display::array_value_to_string(
            retained.batches[0].column_by_name("name").expect("name"),
            0,
        )
        .expect("render retained name"),
        "Race Safe"
    );
}
