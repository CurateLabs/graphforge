//! Fresh-root imports preserve reproducible generation and composition identities.
#![cfg(feature = "portable")]

use std::fs;
use std::path::{Path, PathBuf};

use graphforge_api::{
    GraphForge, ModuleAdoptionRequest, OntologyAuthorityExpectation, OntologyModuleId, OperationId,
    PortableSelection, PortableV2ExactIdentity, PortableV2ExportRequest, PortableV2ImportRequest,
    PortableV2Limits, PortableV2Output, PortableV2SelectionProfile, WriteContext,
};
use graphforge_ontology::OntologyDoc;
use uuid::Uuid;

fn source_package(root: &Path) -> (PathBuf, OntologyModuleId) {
    let source = root.join("source");
    let mut graph = GraphForge::new(source.to_str()).unwrap();
    let candidate = graph
        .create_ontology_module(
            OntologyDoc {
                ontology_id: "https://example.org/ontology/reproducible-import".into(),
                version: "2026.1".into(),
                entity_types: Vec::new(),
                relation_types: Vec::new(),
                properties: Vec::new(),
                constraints: Vec::new(),
                migrations: Vec::new(),
            },
            Vec::new(),
            None,
        )
        .unwrap();
    let state = graph.ontology_authority_state().unwrap();
    graph
        .adopt_ontology_module(
            &ModuleAdoptionRequest {
                authority: OntologyAuthorityExpectation {
                    context: WriteContext {
                        operation_uuid: OperationId(Uuid::from_u128(0x601)),
                        actor_uuid: None,
                    },
                    expected_project_generation_uuid: state.project_generation_uuid,
                    expected_composition_fingerprint: state.composition_fingerprint,
                },
                candidate: candidate.clone(),
            },
            None,
        )
        .unwrap();
    let package = root.join("source.gfpb");
    export(&graph, &package, PortableV2SelectionProfile::Complete);
    (package, candidate.id)
}

fn export(graph: &GraphForge, path: &Path, profile: PortableV2SelectionProfile) -> String {
    graph
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: path.to_path_buf(),
                representation: PortableV2Output::Bundle,
                profile,
                subset: None,
                limits: PortableV2Limits::default(),
            },
            None,
            |_| {},
        )
        .unwrap()
        .package_digest
}

fn import_request(package: &Path) -> PortableV2ImportRequest {
    PortableV2ImportRequest {
        input: package.to_path_buf(),
        operation_id: OperationId(Uuid::from_u128(0x602)),
        limits: PortableV2Limits::default(),
    }
}

fn manifest(generation: &graphforge_storage::ResolvedProjectGeneration) -> Vec<u8> {
    fs::read(generation.generation_root().join("manifest.json")).unwrap()
}

#[test]
fn fresh_root_imports_reopen_with_identical_manifests_and_composition_bundles() {
    let root = tempfile::tempdir().unwrap();
    let (package, module) = source_package(root.path());
    let request = import_request(&package);
    let targets = [root.path().join("first"), root.path().join("second")];
    // Both absent roots and existing empty directories are fresh import targets.
    fs::create_dir(&targets[1]).unwrap();
    let mut manifests = Vec::new();
    let mut hashes = Vec::new();
    let mut bootstrap_manifests = Vec::new();
    let mut packages = Vec::new();
    for (index, target) in targets.iter().enumerate() {
        let imported = GraphForge::import_portable_v2(target, &request, None).unwrap();
        assert!(!imported.idempotent_replay);
        let generation = graphforge_storage::resolve_project_generation(target).unwrap();
        assert_eq!(generation.generation_uuid(), imported.generation_uuid);
        let bytes = manifest(&generation);
        let hash = generation.manifest_sha256();
        let parent = generation.parent_generation_uuid().unwrap();
        let bootstrap = graphforge_storage::resolve_generation_by_uuid(target, parent).unwrap();
        assert_eq!(bootstrap.parent_generation_uuid(), None);
        bootstrap_manifests.push(manifest(&bootstrap));
        drop(bootstrap);
        drop(generation);

        let reopened = GraphForge::new(target.to_str()).unwrap();
        let reopened_generation = graphforge_storage::resolve_project_generation(target).unwrap();
        assert_eq!(reopened_generation.manifest_sha256(), hash);
        assert_eq!(manifest(&reopened_generation), bytes);
        let output = root.path().join(format!("composition-{index}.gfpb"));
        let digest = export(
            &reopened,
            &output,
            PortableV2SelectionProfile::OntologyComposition(vec![PortableV2ExactIdentity {
                id: module.ontology_id.clone(),
                version: module.authored_version.clone(),
                content_digest: format!("sha256:{}", module.canonical_digest),
            }]),
        );
        packages.push((digest, fs::read(output).unwrap()));
        drop(reopened_generation);
        drop(reopened);
        let replay = GraphForge::import_portable_v2(target, &request, None).unwrap();
        assert!(replay.idempotent_replay);
        assert_eq!(replay.generation_uuid, imported.generation_uuid);
        let replayed = graphforge_storage::resolve_project_generation(target).unwrap();
        assert_eq!(manifest(&replayed), bytes);
        assert_eq!(replayed.manifest_sha256(), hash);
        manifests.push(bytes);
        hashes.push(hash);
    }
    assert_eq!(hashes[0], hashes[1], "fresh import manifest hashes differ");
    assert_eq!(manifests[0], manifests[1], "fresh import manifests differ");
    assert_eq!(bootstrap_manifests[0], bootstrap_manifests[1]);
    assert_eq!(
        packages[0], packages[1],
        "exact-composition packages differ"
    );
}

#[test]
fn initialized_import_targets_retain_their_original_parent_lineage() {
    let root = tempfile::tempdir().unwrap();
    let (package, _) = source_package(root.path());
    let request = import_request(&package);
    let mut parents = Vec::new();
    let mut imported_hashes = Vec::new();
    for name in ["first", "second"] {
        let target = root.path().join(name);
        let graph = GraphForge::new(target.to_str()).unwrap();
        let original = graphforge_storage::resolve_project_generation(&target).unwrap();
        let parent = original.generation_uuid();
        let parent_manifest = manifest(&original);
        drop(original);
        drop(graph);
        GraphForge::import_portable_v2(&target, &request, None).unwrap();
        let reopened = GraphForge::new(target.to_str()).unwrap();
        let generation = graphforge_storage::resolve_project_generation(&target).unwrap();
        assert_eq!(generation.parent_generation_uuid(), Some(parent));
        let retained = graphforge_storage::resolve_generation_by_uuid(&target, parent).unwrap();
        assert_eq!(manifest(&retained), parent_manifest);
        parents.push(parent);
        imported_hashes.push(generation.manifest_sha256());
        drop(reopened);
    }
    assert_ne!(
        parents[0], parents[1],
        "ordinary initialization stays independent"
    );
    assert_ne!(imported_hashes[0], imported_hashes[1]);
}
