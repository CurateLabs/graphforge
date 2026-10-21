//! Genuine advertised package fetch into an independently advanced same-Branch owner.
mod incorporation;
use super::*;
use graphforge_api::{
    DiscoveryResearchVersionRequest, FetchResearchBranchRequest, FetchedResearchBranch,
};
use graphforge_api::{PortableLimits, PortableMode};

struct Package {
    manifest: Vec<u8>,
    refs: Vec<u8>,
    lineage: Vec<u8>,
    path: PathBuf,
    head: String,
}
impl Package {
    fn download(hub: &LoopbackHub, root: &Path, head: Uuid) -> Self {
        let (manifest, lineage) = served_lineage(hub, "curate/claims");
        let (_, object) = manifest
            .research_version_object(&lineage, &head.to_string())
            .unwrap();
        let response = hub.get(&object.locations[0]);
        assert_eq!(response.status, 200);
        assert_eq!(digest_bytes(&response.body), object.digest.0);
        let path = root.join(format!("remote-{head}.gfpb"));
        std::fs::write(&path, &response.body).unwrap();
        Self {
            manifest: manifest.to_canonical_json().unwrap(),
            refs: refs_document(hub, "curate/claims").body,
            lineage: lineage.to_canonical_json().unwrap(),
            path,
            head: head.to_string(),
        }
    }
    fn fetch(
        &self,
        graph: &mut GraphForge,
        branch: Uuid,
        operation: Uuid,
        expected: Uuid,
    ) -> Result<FetchedResearchBranch, GfError> {
        let repository = RepositoryIdentity::parse("curate/claims").unwrap();
        graph.fetch_research_branch(
            &FetchResearchBranchRequest {
                operation_uuid: operation,
                expected_generation_uuid: expected,
                branch_uuid: branch,
                repository_url: "https://hub.test/curate/claims",
                discovery: DiscoveryResearchVersionRequest {
                    manifest_json: &self.manifest,
                    refs_json: &self.refs,
                    lineage_json: &self.lineage,
                    expected_repository: &repository,
                    version_uuid: &self.head,
                    package: &self.path,
                    discovery_limits: DiscoveryLimits::default(),
                    portable_limits: PortableLimits::default(),
                    mode: PortableMode::Full,
                    cancelled: None,
                    scratch: None,
                },
            },
            &CancellationToken::new(),
        )
    }
}
fn score(result: graphforge_api::ExecutionResult) -> i64 {
    result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0)
}

#[test]
fn verified_same_branch_fetch_preserves_local_head_workspace_and_exact_receipt_replay() {
    let mut alice = history();
    let hub = LoopbackHub::start();
    publish_ok(&hub, &alice.graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let bob_path = root.path().join("bob");
    clone_into(&hub, "curate/claims", &bob_path, Some("main"), None);
    let mut bob = GraphForge::new(bob_path.to_str()).unwrap();
    let local = edit_by(&mut bob, alice.branch, 2, "Bob");
    let remote = edit_by(&mut alice.graph, alice.branch, 3, "Alice");
    publish_ok(&hub, &alice.graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let package = Package::download(&hub, root.path(), remote);
    let prior = bob.research_version_retention().unwrap();
    let workspace = score(bob.execute("MATCH (n:Item) RETURN sum(n.score)").unwrap());
    let expected = generation(&bob);
    let operation = Uuid::now_v7();
    let repository = RepositoryIdentity::parse("curate/claims").unwrap();
    let result = bob
        .fetch_research_branch(
            &FetchResearchBranchRequest {
                operation_uuid: operation,
                expected_generation_uuid: expected,
                branch_uuid: alice.branch,
                repository_url: "https://hub.test/curate/claims",
                discovery: DiscoveryResearchVersionRequest {
                    manifest_json: &package.manifest,
                    refs_json: &package.refs,
                    lineage_json: &package.lineage,
                    expected_repository: &repository,
                    version_uuid: &package.head,
                    package: &package.path,
                    discovery_limits: DiscoveryLimits::default(),
                    portable_limits: PortableLimits::default(),
                    mode: PortableMode::Full,
                    cancelled: None,
                    scratch: None,
                },
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let after = bob.research_version_retention().unwrap();
    assert_eq!(after.heads, prior.heads);
    assert_eq!(after.branches, prior.branches);
    assert!(after.materialized.contains(&remote));
    assert!(after.interchange.contains_key(&remote));
    assert_eq!(
        score(bob.execute("MATCH (n:Item) RETURN sum(n.score)").unwrap()),
        workspace
    );
    assert_eq!(
        score(
            bob.open_research_version(local)
                .unwrap()
                .execute("MATCH (n:Item) RETURN sum(n.score)")
                .unwrap()
        ),
        2
    );
    assert_eq!(
        result
            .binding
            .authenticate(&after)
            .unwrap()
            .authenticate_receipt(&after)
            .unwrap(),
        result.receipt
    );
    assert_binding_integrity(&result, &after);
    assert!(
        bob.commit_research_version_operation(
            result.binding.authenticate(&after).unwrap().operation(),
            &CancellationToken::new()
        )
        .is_err()
    );
    edit_by(&mut bob, alice.branch, 4, "Bob");
    let advanced = generation(&bob);
    assert_eq!(
        package
            .fetch(&mut bob, alice.branch, operation, expected)
            .unwrap(),
        result
    );
    assert_eq!(generation(&bob), advanced);
    verify_refresh_and_tamper(&mut bob, &mut alice, &hub, &root, &package, result);
}

fn verify_refresh_and_tamper(
    bob: &mut GraphForge,
    alice: &mut History,
    hub: &LoopbackHub,
    root: &tempfile::TempDir,
    original: &Package,
    result: FetchedResearchBranch,
) {
    let operation = result.binding.operation_uuid;
    let expected = result.binding.expected_generation_uuid;
    let newer = edit_by(&mut alice.graph, alice.branch, 5, "Alice");
    publish_ok(hub, &alice.graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let package = Package::download(hub, root.path(), newer);
    let current = generation(bob);
    package
        .fetch(bob, alice.branch, Uuid::now_v7(), current)
        .unwrap();
    let current = generation(bob);
    assert_eq!(
        original
            .fetch(bob, alice.branch, operation, expected)
            .unwrap(),
        result
    );
    assert_eq!(generation(bob), current);
    assert!(
        bob.research_version_retention()
            .unwrap()
            .remote_branches
            .values()
            .any(|reference| reference.head_version_uuid == newer)
    );
    let mut bytes = std::fs::read(&original.path).unwrap();
    bytes[0] ^= 1;
    std::fs::write(&original.path, &bytes).unwrap();
    assert!(
        original
            .fetch(bob, alice.branch, operation, expected)
            .is_err()
    );
    assert_eq!(generation(bob), current);
}

fn assert_binding_integrity(
    result: &FetchedResearchBranch,
    registry: &graphforge_storage::research_versions::ResearchRegistry,
) {
    for field in [
        "repository",
        "generation",
        "lineage",
        "package",
        "transport",
    ] {
        let mut forged = result.binding.clone();
        match field {
            "repository" => forged.reference.repository_url = "https://hub.test/other/repo".into(),
            "generation" => forged.expected_generation_uuid = Uuid::now_v7(),
            "lineage" => forged.lineage_sha256[0] ^= 1,
            "package" => forged.package_sha256[0] ^= 1,
            "transport" => forged.object_sha256[0] ^= 1,
            _ => unreachable!(),
        }
        assert!(forged.authenticate(registry).is_err(), "{field}");
    }
    let mut altered = result.binding.authenticate(registry).unwrap();
    altered.archive.producer.push_str("-altered");
    assert!(altered.authenticate_receipt(registry).is_err());
}
