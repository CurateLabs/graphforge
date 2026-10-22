//! `gf clone` of a Version written by another Core version warns before it
//! requests any graph object, over real loopback HTTP (#1936).
//!
//! The Version's package is relabelled by the shared helper: the declared
//! version changes and `package_digest` and the BagIt checksums are recomputed.
//! The server's request log is the order witness: the warning is observed with
//! only the two discovery documents served, and the graph object follows it.

use super::status_tests::{Routed, StatusServer};
use super::*;

#[allow(dead_code, reason = "shared with the CLI integration tests")]
#[path = "../../tests/support/core_version.rs"]
mod relabel;

/// Run a clone and observe the Core version warning at the moment it is raised.
fn run_clone_observing_warnings(
    transport: &dyn Transport,
    args: CloneArgs,
    json: bool,
    output: &mut dyn Write,
    warn: WarningSink<'_>,
) -> Result<(), graphforge_api::GfError> {
    let mut env = CloneEnv {
        warn: Some(warn),
        ..CloneEnv::quiet()
    };
    run_clone_profiled_with_delays(
        transport,
        args,
        json,
        output,
        &TelemetryRuntime::default(),
        &CloneDelays::default(),
        &mut env,
    )
}

const CORE_X: &str = "0.0.1";
const PACKAGE_PATH: &str = "/objects/project.gfpb";

/// A real complete portable bundle of a one-node project whose package states
/// `producing` as its Core version, and its semantic package digest.
fn bundle_from(producing: &str) -> (Vec<u8>, String) {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    std::fs::create_dir(&source).unwrap();
    let graph = GraphForge::new(source.to_str()).unwrap();
    graph.execute("CREATE (:Doc {id: 1})").unwrap();
    drop(graph);
    let generation = graphforge_storage::resolve_project_generation(&source).unwrap();
    let limits = PortableLimits::default();
    let plan = graphforge_storage::plan_complete_portable(&generation, limits).unwrap();
    let expanded = root.path().join("expanded");
    graphforge_storage::export_complete_portable(
        &plan,
        &expanded,
        graphforge_storage::PortableOutput::Expanded,
        limits,
        &AtomicBool::new(false),
        |_| {},
    )
    .unwrap();
    if producing != graphforge_api::GRAPHFORGE_VERSION {
        relabel::relabel_package(&expanded, producing);
    }
    let bundle_path = root.path().join("complete.gfpb");
    graphforge_storage::repack_verified_expanded_portable(
        &expanded,
        &bundle_path,
        limits,
        &AtomicBool::new(false),
    )
    .unwrap();
    let report =
        graphforge_storage::verify_portable(&bundle_path, PortableMode::Full, limits, None)
            .unwrap();
    assert_eq!(report.producing_core_version, producing);
    (std::fs::read(&bundle_path).unwrap(), report.package_digest)
}

/// Refs and manifest for `curate/claims` advertising `bundle`, which declare
/// `producing` as the Core version that wrote it.
fn documents(bundle: &[u8], package_digest: &str, producing: &str) -> (Vec<u8>, Vec<u8>) {
    let object_digest = hash_reader(&mut std::io::Cursor::new(bundle)).unwrap();
    let repository = serde_json::json!({"owner":"curate","repository":"claims"});
    let immutable = format!("sha256:{}", "a".repeat(64));
    let validator = format!("sha256:{}", "d".repeat(64));
    let refs = serde_json::to_vec(&serde_json::json!({
        "format":"graphforge-discovery/1","version":{"major":1,"minor":0},
        "repository":repository.clone(),"default_ref":"main",
        "refs":[{"name":"main","target":immutable.clone(),"validator":validator}]
    }))
    .unwrap();
    let manifest = serde_json::to_vec(&serde_json::json!({
        "format":"graphforge-discovery/1","version":{"major":1,"minor":0},
        "repository":repository,"default_ref":"main","resolved_ref":"main",
        "immutable_version":immutable,
        "package":{"format":"graphforge-project/2","package_digest":package_digest,"producing_core_version":producing,"object_digest":object_digest},
        "requirements":[{"capability":"portable","major":1}],"capabilities":[{"capability":"range-requests","major":1}],
        "objects":[{"digest":object_digest,"length":bundle.len(),"media_type":graphforge_discovery::PORTABLE_MEDIA_TYPE,"locations":[format!("https://hub.test{PACKAGE_PATH}")]}]
    })).unwrap();
    (refs, manifest)
}

/// Serve the Version over loopback HTTP and clone it, recording how many
/// requests the server had received each time a warning was raised.
fn clone_version(
    producing: &str,
    destination: &Path,
) -> (
    serde_json::Value,
    StatusServer,
    Vec<(usize, graphforge_api::CoreVersionWarning)>,
) {
    let (bundle, package_digest) = bundle_from(producing);
    let (refs, manifest) = documents(&bundle, &package_digest, producing);
    let server = StatusServer::start(vec![
        (200, String::new(), refs),
        (200, String::new(), manifest),
        (200, "ETag: \"fixture-1\"\r\n".into(), bundle),
    ]);
    let transport = Routed::new(server.address);
    let mut seen = Vec::new();
    let mut output = Vec::new();
    run_clone_observing_warnings(
        &transport,
        CloneArgs {
            repository: "https://hub.test/curate/claims".into(),
            hub: None,
            token_env: None,
            protection_bypass_env: None,
            destination: Some(destination.to_path_buf()),
            telemetry_endpoint: None,
            git_ref: None,
            version_uuid: None,
        },
        true,
        &mut output,
        &mut |warnings| {
            seen.extend(
                warnings
                    .iter()
                    .map(|warning| (server.paths().len(), warning.clone())),
            );
        },
    )
    .unwrap();
    (serde_json::from_slice(&output).unwrap(), server, seen)
}

#[test]
fn a_version_from_another_core_warns_before_the_first_graph_object_request() {
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("claims");
    let (result, server, seen) = clone_version(CORE_X, &destination);

    // One warning, raised when the server had served only refs and manifest.
    assert_eq!(seen.len(), 1, "{seen:?}");
    let (served_at_warning, warning) = &seen[0];
    assert_eq!(
        server.paths(),
        [
            "/curate/claims/.gf/refs",
            "/curate/claims/.gf/manifest",
            PACKAGE_PATH
        ]
    );
    assert_eq!(
        *served_at_warning, 2,
        "the graph object was already requested"
    );
    assert_eq!(warning.code, "GF_CORE_VERSION_MISMATCH");
    assert_eq!(warning.producing_core_version, CORE_X);
    assert_eq!(
        warning.running_core_version,
        graphforge_api::GRAPHFORGE_VERSION
    );
    assert!(warning.message.contains(CORE_X) && warning.message.contains("npm install"));

    // The clone still completed and carries the same warning in its report.
    let reported = result["warnings"].as_array().unwrap();
    assert_eq!(reported.len(), 1, "{result}");
    assert_eq!(reported[0]["code"], "GF_CORE_VERSION_MISMATCH");
    assert_eq!(reported[0]["producing_core_version"], CORE_X);
    assert_eq!(
        reported[0]["running_core_version"],
        graphforge_api::GRAPHFORGE_VERSION
    );
    assert!(GraphForge::new(destination.to_str()).is_ok());
}

#[test]
fn a_version_from_the_running_core_clones_without_a_warning() {
    let root = tempfile::tempdir().unwrap();
    let (result, server, seen) = clone_version(
        graphforge_api::GRAPHFORGE_VERSION,
        &root.path().join("claims"),
    );

    assert!(seen.is_empty(), "{seen:?}");
    assert!(result.get("warnings").is_none(), "{result}");
    assert_eq!(server.paths().len(), 3);
}
