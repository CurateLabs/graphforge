//! Bounded native process entry point for the shared Hub control contract.

use clap::Args;
use graphforge_discovery::hub::{
    HubControlRequest, HubControlResult, HubManagementState, MAX_HUB_REQUEST_BYTES,
};
use std::io::{Read as _, Write};
use std::path::PathBuf;

#[derive(Args)]
pub(crate) struct ControlArgs {
    /// Core-owned structured control request; editors construct this from controls.
    #[arg(long)]
    request_file: PathBuf,
    /// Bare HTTPS Hub origin. Saved sessions are isolated by this origin/host.
    #[arg(long, default_value = crate::hub_http::DEFAULT_HUB)]
    hub: String,
    /// Never consult the OS vault; supports anonymous public browsing.
    #[arg(long)]
    anonymous: bool,
    /// Read the separate preview deployment bypass from this environment variable.
    #[arg(long)]
    protection_bypass_env: Option<String>,
}

pub(crate) fn run(
    args: &ControlArgs,
    output: &mut dyn Write,
) -> Result<i32, graphforge_api::GfError> {
    if !std::fs::symlink_metadata(&args.request_file)
        .map_err(|_| invalid_file())?
        .is_file()
    {
        return Err(invalid_file());
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    // Prevent a race replacing the checked regular file with a blocking FIFO.
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW);
    }
    let file = options
        .open(&args.request_file)
        .map_err(|_| invalid_file())?;
    if !file.metadata().map_err(|_| invalid_file())?.is_file() {
        return Err(invalid_file());
    }
    let mut bytes = Vec::new();
    file.take(MAX_HUB_REQUEST_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid_file())?;
    let request = HubControlRequest::from_json(&bytes).map_err(|_| invalid_file())?;
    let response = crate::auth::exchange_control(
        &args.hub,
        args.protection_bypass_env.as_deref(),
        &request,
        args.anonymous,
    )?;
    write_response(&response, output)
}

fn write_response(
    response: &graphforge_discovery::hub::HubControlResponse,
    output: &mut dyn Write,
) -> Result<i32, graphforge_api::GfError> {
    let status = match &response.result {
        HubControlResult::Failure { .. } => 1,
        HubControlResult::Receipt { receipt } => match receipt.state {
            HubManagementState::Refused => 1,
            HubManagementState::Pending | HubManagementState::Prepared => 2,
            HubManagementState::Applied => 0,
        },
        _ => 0,
    };
    serde_json::to_writer(&mut *output, response).map_err(|_| invalid_output())?;
    writeln!(output).map_err(|_| invalid_output())?;
    Ok(status)
}
fn invalid_file() -> graphforge_api::GfError {
    crate::hub_http::validation(
        "hub.control_invalid_request",
        "control request must be a readable, bounded native JSON document",
    )
}
fn invalid_output() -> graphforge_api::GfError {
    crate::hub_http::validation(
        "hub.control_output_failed",
        "cannot write the control result; reconcile an applied operation before retrying",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use graphforge_discovery::hub::*;

    #[test]
    fn process_dispatch_refuses_captured_account_calls_and_invalid_files_without_leaking_paths() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("private-native-path.json");
        for bytes in [b"not-json".as_slice(), &vec![b' '; MAX_HUB_REQUEST_BYTES + 1], br#"{"format":"graphforge-hub-control/1","request_uuid":"00000000-0000-4000-8000-000000000001","operation":{"kind":"accounts","page":{"limit":1,"cursor":null}}}"#] {
            std::fs::write(&path, bytes).unwrap();
            let result = crate::execute(["gf", "--json", "hub", "control", "--request-file", path.to_str().unwrap()]);
            assert_ne!(result.exit_code, 0);
            assert!(result.stdout.is_empty());
            let diagnostics = String::from_utf8(result.stderr).unwrap();
            assert!(!diagnostics.contains("private-native-path"));
            assert!(!diagnostics.contains(root.path().to_str().unwrap()));
        }
    }
    #[test]
    fn process_receipts_distinguish_incomplete_refused_and_applied_and_detect_output_failure() {
        struct Broken;
        impl Write for Broken {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("private-path"))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        for (state, expected) in [
            (HubManagementState::Prepared, 2),
            (HubManagementState::Pending, 2),
            (HubManagementState::Refused, 1),
            (HubManagementState::Applied, 0),
        ] {
            let response = HubControlResponse {
                format: HUB_CONTROL_FORMAT.into(),
                request_uuid: "00000000-0000-4000-8000-000000000001".into(),
                result: HubControlResult::Receipt {
                    receipt: HubManagementReceipt {
                        operation_uuid: "00000000-0000-4000-8000-000000000002".into(),
                        intent_digest: graphforge_discovery::Sha256Digest(format!(
                            "sha256:{}",
                            "a".repeat(64)
                        )),
                        state,
                        refusal: (state == HubManagementState::Refused)
                            .then_some(HubControlErrorCode::StalePlan),
                        record: None,
                    },
                },
            };
            let mut output = Vec::new();
            assert_eq!(write_response(&response, &mut output).unwrap(), expected);
            assert_eq!(
                serde_json::from_slice::<HubControlResponse>(&output).unwrap(),
                response
            );
            let error = write_response(&response, &mut Broken)
                .unwrap_err()
                .to_string();
            assert!(error.contains("hub.control_output_failed"));
            assert!(!error.contains("private-path"));
        }
    }
}
