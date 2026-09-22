//! Same-binary integration coverage for `graphforge verify` (#1384, S7).
//!
//! Builds a real project through the ordinary import-session CLI path, runs
//! `verify` against it, then proves corruption detection by flipping one
//! bit in a sealed, content-addressed graph payload object on disk and
//! observing the same command report it -- the shape of corruption #1269's
//! own regression test exercises against the shaping path.
//!
//! The primary test's corrupted object is deliberately one that is retained
//! but not reachable from the currently selected generation's manifest
//! (installed directly, the way a superseded or not-yet-garbage-collected
//! object would be retained). Ordinary project open (`GraphForge::new`'s
//! workspace hydration) already eagerly authenticates much of the reachable
//! tree, so corrupting a *reachable* object is often refused before any
//! subcommand runs, with a generic `GF_VALIDATION` error rather than this
//! command's structured report -- see the second test below, which accepts
//! either outcome because which mechanism catches a given reachable object
//! is not guaranteed. Either way is fail-closed: corrupt data is never
//! served. `verify`'s distinct, load-bearing value is the strictly larger
//! and unconditional set: every retained object, reachable from the
//! current generation or not, on demand, without needing a query that
//! happens to touch the damaged bytes.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;

use arrow::array::{FixedSizeBinaryArray, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tempfile::TempDir;

fn gf_bin() -> std::path::PathBuf {
    fs::canonicalize(env!("CARGO_BIN_EXE_gf")).expect("resolve same-build gf binary")
}

fn gf(project: &Path, args: &[&str]) -> Output {
    Command::new(gf_bin())
        .arg("--project")
        .arg(project)
        .args(args)
        .output()
        .expect("run same-build gf binary")
}

fn json(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "gf failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stable JSON result")
}

/// Ingest two `Person` nodes through the ordinary `import-session` path so
/// the project retains real, content-addressed graph payload objects.
fn build_populated_project(project: &Path) {
    let root = TempDir::new().unwrap();
    let input = root.path().join("nodes.parquet");
    let ids = [uuid::Uuid::now_v7(), uuid::Uuid::now_v7()];
    let batch = RecordBatch::try_new(
        graphforge_api::bulk_node_input_schema(Vec::new()).unwrap(),
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(ids.iter().map(|id| id.as_bytes().as_slice()))
                    .unwrap(),
            ),
            Arc::new(StringArray::from(vec!["Person", "Person"])),
        ],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(fs::File::create(&input).unwrap(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let operation = uuid::Uuid::now_v7().to_string();
    let begun = json(&gf(
        project,
        &[
            "--json",
            "import-session",
            "begin",
            "--operation-uuid",
            &operation,
        ],
    ));
    let session = begun["session_uuid"].as_str().unwrap().to_owned();
    json(&gf(
        project,
        &[
            "--json",
            "import-session",
            "register-parquet",
            "--session-uuid",
            &session,
            "--kind",
            "nodes",
            "--path",
            input.to_str().unwrap(),
        ],
    ));
    json(&gf(
        project,
        &[
            "--json",
            "import-session",
            "validate",
            "--session-uuid",
            &session,
        ],
    ));
    let committed = json(&gf(
        project,
        &[
            "--json",
            "import-session",
            "commit",
            "--session-uuid",
            &session,
        ],
    ));
    assert_eq!(committed["construction"]["publication_committed"], true);
}

/// Install one extra content-addressed object under the project's object
/// store, named by its own real SHA-256, that no generation manifest
/// references. This is the retained-but-unreachable shape (a superseded or
/// not-yet-collected object) that only a full-store sweep, not ordinary
/// project open, examines. Returns its hex digest.
fn install_orphan_content_addressed_object(project: &Path, bytes: &[u8]) -> String {
    let mut digest = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        use std::fmt::Write as _;
        write!(&mut digest, "{byte:02x}").unwrap();
    }
    let bucket = project
        .join("graph-objects")
        .join("sha256")
        .join(&digest[..2]);
    fs::create_dir_all(&bucket).unwrap();
    fs::write(bucket.join(&digest[2..]), bytes).unwrap();
    digest
}

/// Flip the first byte of the object named by `digest`, preserving its
/// length and file identity -- the same corruption shape #1269 guards
/// against.
fn corrupt_content_addressed_object(project: &Path, digest: &str) {
    let path = project
        .join("graph-objects")
        .join("sha256")
        .join(&digest[..2])
        .join(&digest[2..]);
    let mut bytes = fs::read(&path).unwrap();
    bytes[0] ^= 0xFF;
    fs::write(&path, &bytes).unwrap();
}

#[test]
fn verify_reports_clean_then_detects_a_bit_flip_in_a_real_project() {
    let root = TempDir::new().unwrap();
    let project = root.path().join("project");
    fs::create_dir(&project).unwrap();
    build_populated_project(&project);

    let baseline = json(&gf(&project, &["--json", "verify"]));
    assert_eq!(baseline["contract"], "graphforge-verify-command/1");
    assert_eq!(baseline["ok"], true);
    assert!(
        baseline["content_addressed_objects"]["objects_checked"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(baseline["content_addressed_objects"]["objects_failed"], 0);
    assert_eq!(baseline["catalog_and_participants"]["objects_failed"], 0);

    // Read-only: verifying must not have changed the store it just checked.
    let graph = graphforge_api::GraphForge::new(project.to_str()).unwrap();
    assert_eq!(graph.node_count("Person").unwrap(), 2);
    drop(graph);

    // Retain an extra object no generation manifest references -- the
    // shape of an object pending garbage collection. Ordinary project open
    // never reads it, so it stays invisible to every other command; a full
    // `verify` sweep is the only thing that looks at it.
    let orphan_digest =
        install_orphan_content_addressed_object(&project, b"retained-but-unreachable-payload");

    let clean = json(&gf(&project, &["--json", "verify"]));
    assert_eq!(clean["ok"], true);
    assert_eq!(
        clean["content_addressed_objects"]["objects_checked"],
        baseline["content_addressed_objects"]["objects_checked"]
            .as_u64()
            .unwrap()
            + 1,
        "the orphan object must be swept even though nothing references it"
    );

    corrupt_content_addressed_object(&project, &orphan_digest);

    // Ordinary project open must still succeed: the corrupted object is not
    // reachable from the current generation, so nothing on the read path
    // touches it before `verify` does.
    let graph = graphforge_api::GraphForge::new(project.to_str()).unwrap();
    assert_eq!(graph.node_count("Person").unwrap(), 2);
    drop(graph);

    let dirty_output = gf(&project, &["--json", "verify"]);
    assert_eq!(
        dirty_output.status.code(),
        Some(5),
        "verify must exit non-zero when the store is not intact; stderr={}",
        String::from_utf8_lossy(&dirty_output.stderr)
    );
    let dirty: Value = serde_json::from_slice(&dirty_output.stdout).unwrap();
    assert_eq!(dirty["ok"], false);
    assert_eq!(dirty["content_addressed_objects"]["objects_failed"], 1);
    assert_eq!(
        dirty["content_addressed_objects"]["objects_checked"],
        clean["content_addressed_objects"]["objects_checked"]
    );

    // Re-running verify again must report the same corruption, not silently
    // heal it -- verify never mutates, repairs, or publishes anything.
    let rechecked_output = gf(&project, &["--json", "verify"]);
    assert_eq!(rechecked_output.status.code(), Some(5));
    let rechecked: Value = serde_json::from_slice(&rechecked_output.stdout).unwrap();
    assert_eq!(rechecked, dirty);

    // Human-readable output names what failed, in the repository's
    // key=value evidence convention.
    let text_output = gf(&project, &["verify"]);
    assert_eq!(text_output.status.code(), Some(5));
    let text = String::from_utf8_lossy(&text_output.stdout);
    assert!(text.contains("ok=false"));
    assert!(text.contains("content_addressed_objects(checked=") && text.contains("failed=1"));
}

#[test]
fn a_corrupted_reachable_object_is_refused_one_way_or_another() {
    // Complementary to the orphan-object proof above. Corrupting an object
    // the *current* generation's manifest references is refused either way:
    // ordinary project open (`GraphForge::new`'s workspace hydration)
    // eagerly authenticates much, though not deterministically all, of the
    // reachable tree, so which mechanism catches a given reachable object
    // depends on what hydration happened to touch. Either outcome is
    // fail-closed -- corrupt data is never served -- so this test accepts
    // both: a generic `GF_VALIDATION` refusal at open (exit 2), or this
    // command's own structured, non-zero report (exit 5).
    let root = TempDir::new().unwrap();
    let project = root.path().join("project");
    fs::create_dir(&project).unwrap();
    build_populated_project(&project);

    let sha256_root = project.join("graph-objects").join("sha256");
    let mut corrupted = false;
    'outer: for prefix in fs::read_dir(&sha256_root).unwrap() {
        let prefix = prefix.unwrap().path();
        if !prefix.is_dir() {
            continue;
        }
        for object in fs::read_dir(&prefix).unwrap() {
            let object = object.unwrap().path();
            let mut bytes = fs::read(&object).unwrap();
            // Directory order is not deterministic and a populated project can
            // legitimately retain zero-byte objects; corrupt a non-empty one
            // so the bit flip is real.
            if bytes.is_empty() {
                continue;
            }
            bytes[0] ^= 0xFF;
            // Sealed CAS objects are mode 0444; reopen them writable first.
            let mut permissions = fs::metadata(&object).unwrap().permissions();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                permissions.set_mode(permissions.mode() | 0o200);
            }
            #[cfg(not(unix))]
            permissions.set_readonly(false);
            fs::set_permissions(&object, permissions).unwrap();
            fs::write(&object, &bytes).unwrap();
            corrupted = true;
            break 'outer;
        }
    }
    assert!(
        corrupted,
        "populated fixture must retain a reachable object"
    );

    let output = gf(&project, &["--json", "verify"]);
    match output.status.code() {
        Some(2) => {
            // Refused at project open, before `verify` ran at all.
            let error: Value = serde_json::from_slice(&output.stderr).unwrap();
            assert_eq!(error["error"]["code"], "GF_VALIDATION");
        }
        Some(5) => {
            // Refused by `verify`'s own sweep.
            let report: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(report["ok"], false);
            assert_eq!(report["content_addressed_objects"]["objects_failed"], 1);
        }
        other => panic!("expected exit code 2 or 5, got {other:?}"),
    }
}
