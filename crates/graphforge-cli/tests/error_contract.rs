//! The agent-facing JSON error contract (`docs/reference/cli-errors.md`).
//!
//! Each documented class is produced by at least one real code through the
//! `gf` binary or the in-process CLI runner, asserting the authoritative JSON
//! fields and the exit code. The publish code table in the doc is compared
//! against the code-owned `HubPublishErrorCode::ALL` list so it cannot drift.

use std::process::Command;

use graphforge_hub_publish::{HubPublishError, HubPublishErrorCode};
use serde_json::Value;
use tempfile::tempdir;

/// Held by tests that read or write the process environment of the in-process
/// runner; the `gf`-binary tests use `Command` environments instead.
static ENV_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

const PUBLISH_TOKEN_ENV: &str = "GRAPHFORGE_HUB_PUBLISH_TOKEN";
const SECRET: &str = "gf-error-contract-secret-2127";
/// Reserved non-routable name: HTTPS and name-shaped, so it passes URL
/// admission, and guaranteed by RFC 2606 never to resolve. `gf publish` takes
/// the Hub from the repository URL; `gf clone` takes `--hub`.
const DEAD_HUB_REPOSITORY: &str = "https://nonexistent.invalid/owner/repo";
const DEAD_HUB: &str = "https://nonexistent.invalid";

fn gf() -> Command {
    Command::new(env!("CARGO_BIN_EXE_gf"))
}

/// The one JSON error document a failed `gf` run writes to standard error.
fn error_document(output: &std::process::Output) -> Value {
    assert!(
        output.stdout.is_empty(),
        "errors never write to stdout: {:?}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        stderr.trim().lines().count(),
        1,
        "exactly one error document, got: {stderr}"
    );
    let document: Value = serde_json::from_str(stderr.trim())
        .unwrap_or_else(|error| panic!("stderr is one JSON document ({error}): {stderr}"));
    document
        .get("error")
        .unwrap_or_else(|| panic!("missing error member: {stderr}"))
        .clone()
}

fn semantic_code(document: &Value) -> Option<String> {
    document["details"]["semantic_code"]
        .as_str()
        .map(str::to_owned)
}

#[test]
fn argument_refusal_is_fix_input_with_the_parser_source() {
    let refused = gf()
        .args(["--json", "definitely-not-a-command"])
        .env_remove(PUBLISH_TOKEN_ENV)
        .output()
        .unwrap();
    assert_eq!(refused.status.code(), Some(2));
    let document = error_document(&refused);
    assert_eq!(document["code"], "GF_VALIDATION");
    assert_eq!(document["details"]["source"], "argument_parser");
    assert_eq!(document["details"]["kind"], "invalid_subcommand");
    assert_eq!(semantic_code(&document), None);
}

#[test]
fn invalid_publish_identity_is_a_fix_input_validation() {
    let refused = gf()
        .args(["--json", "publish", "Not An Identity", "--ref", "main"])
        .env_remove(PUBLISH_TOKEN_ENV)
        .output()
        .unwrap();
    assert_eq!(refused.status.code(), Some(2));
    let document = error_document(&refused);
    assert_eq!(document["code"], "GF_VALIDATION");
    assert_eq!(document["details"]["source"], "runtime");
    assert_eq!(document["details"]["kind"], "validation");
    assert_eq!(
        semantic_code(&document).as_deref(),
        Some("hub.invalid_identity")
    );
}

#[test]
fn missing_clone_token_env_is_a_fix_input_error_before_any_request() {
    let missing = gf()
        .args([
            "--json",
            "clone",
            "owner/repo",
            "--hub",
            DEAD_HUB,
            "--token-env",
            "GF_ERROR_CONTRACT_MISSING_TOKEN_ENV",
        ])
        .env_remove("GF_ERROR_CONTRACT_MISSING_TOKEN_ENV")
        .env_remove(PUBLISH_TOKEN_ENV)
        .output()
        .unwrap();
    assert_eq!(missing.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&missing.stderr);
    let document = error_document(&missing);
    assert_eq!(document["code"], "GF_VALIDATION");
    assert_eq!(document["details"]["semantic_code"], "hub.auth_denied");
    assert!(stderr.contains("missing"), "{stderr}");
}

#[test]
fn missing_publish_token_fails_fix_input_before_any_request() {
    let _guard = ENV_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // SAFETY: the environment is only read or written under `ENV_SERIAL`.
    unsafe {
        std::env::remove_var(PUBLISH_TOKEN_ENV);
    }
    // The captured runner has no terminal and no device flow, so the missing
    // credential is refused before any network use.
    let refused = graphforge_cli::execute([
        "gf",
        "--json",
        "publish",
        DEAD_HUB_REPOSITORY,
        "--ref",
        "main",
    ]);
    assert_eq!(refused.exit_code, 2);
    assert!(refused.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&refused.stderr);
    let document: Value = serde_json::from_str(stderr.trim()).expect("one JSON error document");
    let document = &document["error"];
    assert_eq!(document["code"], "GF_VALIDATION");
    assert_eq!(
        semantic_code(document).as_deref(),
        Some("hub.publish.auth_denied")
    );
    let message = document["message"].as_str().unwrap();
    assert!(message.contains(PUBLISH_TOKEN_ENV), "{message}");
}

#[test]
fn publish_env_token_passes_the_early_credential_check_and_is_never_logged() {
    let _guard = ENV_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // SAFETY: the environment is only read or written under `ENV_SERIAL`.
    unsafe {
        std::env::set_var(PUBLISH_TOKEN_ENV, SECRET);
    }
    // The environment token's proven role here is the early credential gate:
    // without the environment read this captured run fails `auth_denied`
    // (exit 2) before any request; with it, the run proceeds and only the
    // reserved Hub name's transport failure remains — the documented retry
    // class, where rerunning may succeed. The request that fails is the
    // anonymous discovery read, which runs before credential selection, so
    // this test does not observe the Authorization header; bearer delivery
    // to the control plane is covered by
    // `publish_token_never_leaves_the_control_plane` in
    // `crates/graphforge-cli/src/hub_publish/tests.rs`.
    let run = graphforge_cli::execute([
        "gf",
        "--json",
        "publish",
        DEAD_HUB_REPOSITORY,
        "--ref",
        "main",
    ]);
    assert_eq!(run.exit_code, 3, "transport failures exit 3");
    let stderr = String::from_utf8_lossy(&run.stderr);
    let document: Value = serde_json::from_str(stderr.trim()).expect("one JSON error document");
    let document = &document["error"];
    assert_eq!(document["code"], "GF_IO");
    assert_eq!(document["details"]["kind"], "storage");
    assert_eq!(document["details"]["semantic_code"], "hub.network");
    assert!(
        !run.stdout
            .windows(SECRET.len())
            .any(|window| window == SECRET.as_bytes()),
        "the token never reaches stdout"
    );
    assert!(
        !stderr.contains(SECRET),
        "the token never reaches stderr: {stderr}"
    );
}

#[test]
fn idempotency_conflict_asks_for_a_human_decision() {
    let root = tempdir().unwrap();
    assert!(
        Command::new("git")
            .arg("init")
            .arg("-q")
            .arg(root.path())
            .status()
            .unwrap()
            .success()
    );
    let init = gf()
        .args([
            "--project-dir",
            root.path().to_str().unwrap(),
            "--json",
            "init",
        ])
        .output()
        .unwrap();
    assert!(
        init.status.success(),
        "{}",
        String::from_utf8_lossy(&init.stderr)
    );

    let key = "41414141-4141-4141-4141-414141414141";
    let sync = |actor: &str| {
        gf().args([
            "--project-dir",
            root.path().to_str().unwrap(),
            "--json",
            "sync",
            "--idempotency-key",
            key,
            "--actor-uuid",
            actor,
        ])
        .output()
        .unwrap()
    };
    let first = sync("42424242-4242-4242-4242-424242424242");
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );

    // The same operation identity under a different actor is not something a
    // program may decide: replay or intentional change needs a human.
    let conflict = sync("43434343-4343-4343-4343-434343434343");
    assert_eq!(conflict.status.code(), Some(1));
    let document = error_document(&conflict);
    assert_eq!(document["code"], "GF_IDEMPOTENCY_CONFLICT");
    assert_eq!(document["details"]["kind"], "project");
    assert_eq!(semantic_code(&document), None);
}

#[test]
fn doc_publish_code_table_matches_the_code_owned_list() {
    let doc = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/reference/cli-errors.md"
    ))
    .unwrap();
    // Local preparation failures share the `hub.publish` prefix, but are not
    // additional wire codes. Keep the wire vocabulary comparison exact.
    let (wire_sections, _) = doc
        .split_once("### Local publication preparation (fix input)")
        .expect("local publication preparation section");
    let documented: Vec<String> = wire_sections
        .lines()
        .filter(|line| line.starts_with("| `hub.publish."))
        .map(|line| {
            line.strip_prefix("| `")
                .and_then(|rest| rest.split('`').next())
                .unwrap_or_else(|| panic!("table row without a code cell: {line}"))
                .to_owned()
        })
        .collect();

    let mut defined: Vec<String> = HubPublishErrorCode::ALL
        .map(|code| {
            if code == HubPublishErrorCode::IdempotencyConflict {
                // The CLI surfaces this code as the outer GF_IDEMPOTENCY_CONFLICT.
                "hub.publish.idempotency_conflict".to_owned()
            } else {
                format!("hub.publish.{}", code.as_str())
            }
        })
        .to_vec();
    defined.sort();

    let mut documented = documented;
    documented.sort();
    documented.dedup();
    assert_eq!(
        documented, defined,
        "docs/reference/cli-errors.md and HubPublishErrorCode::ALL disagree"
    );

    // The wire names the doc claims are the ones the code owns.
    for code in HubPublishErrorCode::ALL {
        if code == HubPublishErrorCode::IdempotencyConflict {
            continue;
        }
        let wire = HubPublishError::new(code, "").wire_code();
        assert!(
            doc.contains(&format!("| `{wire}` |")),
            "missing row: {wire}"
        );
    }
}
