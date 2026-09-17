//! `graphforge verify`: explicit, out-of-band integrity verification.
//!
//! Storage entry point: [`graphforge_api::GraphForge::verify_project_store`]
//! / `graphforge_storage::verify_project_store`. Read-only; never runs as
//! part of ingest (#1384, S7).

use std::io::Write;

use graphforge_api::GraphForge;

/// `verify` ran cleanly but found a store that is not intact.
const VERIFY_FAILED_EXIT_CODE: i32 = 5;

#[derive(serde::Serialize)]
#[serde(deny_unknown_fields)]
struct VerifyCommandReceipt {
    contract: &'static str,
    catalog_and_participants: graphforge_api::VerifyCategoryCounts,
    content_addressed_objects: graphforge_api::VerifyCategoryCounts,
    ok: bool,
}

pub(crate) fn run_verify(
    graph: &GraphForge,
    json: bool,
    output: &mut dyn Write,
) -> Result<i32, graphforge_api::GfError> {
    let report = graph.verify_project_store()?;
    let receipt = VerifyCommandReceipt {
        contract: "graphforge-verify-command/1",
        catalog_and_participants: report.catalog_and_participants,
        content_addressed_objects: report.content_addressed_objects,
        ok: report.ok,
    };
    if json {
        serde_json::to_writer(&mut *output, &receipt)
            .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
        writeln!(output).map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
    } else {
        writeln!(
            output,
            "ok={} catalog_and_participants(checked={} passed={} failed={}) content_addressed_objects(checked={} passed={} failed={})",
            receipt.ok,
            receipt.catalog_and_participants.objects_checked,
            receipt.catalog_and_participants.objects_passed,
            receipt.catalog_and_participants.objects_failed,
            receipt.content_addressed_objects.objects_checked,
            receipt.content_addressed_objects.objects_passed,
            receipt.content_addressed_objects.objects_failed,
        )
        .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
    }
    Ok(if receipt.ok {
        0
    } else {
        VERIFY_FAILED_EXIT_CODE
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use tempfile::tempdir;
    use uuid::Uuid;

    #[test]
    fn verify_json_is_closed_sanitized_and_clean_on_a_fresh_project() {
        let project = tempdir().unwrap();
        let path = project.path().join("state");
        fs::create_dir(&path).unwrap();
        let result = crate::execute([
            "graphforge".to_owned(),
            "--json".to_owned(),
            "--project".to_owned(),
            path.to_string_lossy().into_owned(),
            "verify".to_owned(),
        ]);
        assert_eq!(
            result.exit_code,
            0,
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let json: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
        assert_eq!(json["contract"], "graphforge-verify-command/1");
        assert_eq!(json["ok"], true);
        for category in ["catalog_and_participants", "content_addressed_objects"] {
            assert_eq!(json[category]["objects_failed"], 0);
        }
        fn assert_sanitized(value: &serde_json::Value) {
            match value {
                serde_json::Value::Object(object) => {
                    for (key, value) in object {
                        assert!(
                            !matches!(
                                key.as_str(),
                                "uuid"
                                    | "path"
                                    | "generation_uuid"
                                    | "generation_manifest_sha256"
                                    | "physical_identity_allocated_bytes"
                                    | "provider_id"
                                    | "resource_id"
                                    | "credential"
                                    | "credentials"
                                    | "secret"
                            ),
                            "sensitive evidence key: {key}"
                        );
                        assert_sanitized(value);
                    }
                }
                serde_json::Value::Array(values) => {
                    values.iter().for_each(assert_sanitized);
                }
                serde_json::Value::String(value) => {
                    assert!(Uuid::parse_str(value).is_err());
                    assert!(!Path::new(value).is_absolute());
                }
                _ => {}
            }
        }
        assert_sanitized(&json);
        let encoded = String::from_utf8(result.stdout).unwrap();
        assert!(!encoded.contains("generation_uuid"));
        assert!(!encoded.contains("sha256"));
        assert!(!encoded.contains(path.to_string_lossy().as_ref()));
    }
}
