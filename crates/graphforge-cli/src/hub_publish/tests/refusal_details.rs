//! Parsed CLI publication through the reference HTTP Hub and normal error renderer.
use super::*;

const GUIDANCE: &str =
    "Free accounts must create the Project as Public on the Hub before first publish.";

fn invoke(
    hub: &LoopbackHub,
    project: &Path,
    json: bool,
    credential: PublishToken,
) -> crate::CliExecution {
    let mut argv = vec![
        "gf",
        "publish",
        "curate/claims",
        "--hub",
        HUB,
        "--ref",
        "main",
    ];
    if json {
        argv.push("--json");
    }
    let cli = crate::parse_cli(argv).unwrap();
    let Some(crate::Command::Publish(args)) = cli.command else {
        panic!("publish");
    };
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut diagnostics = crate::Diagnostics::new(cli.json, &mut stderr);
    let error = run_command_with_transport(
        &hub.transport(),
        || Ok(Some(credential)),
        &args,
        Some(project.into()),
        None,
        cli.json,
        &mut stdout,
        &mut diagnostics,
    )
    .unwrap_err();
    drop(diagnostics);
    let error = crate::CliRuntimeError::Core(error);
    crate::write_runtime_error(&error, cli.json, &mut stderr, &[]).unwrap();
    crate::CliExecution {
        exit_code: crate::error_exit_code(&error),
        stdout,
        stderr,
    }
}

fn response(
    status: u16,
    code: &str,
    reason: Option<PublishErrorReason>,
    message: &str,
) -> HubResponse {
    let mut body = json!({"code":code,"message":message});
    if let Some(reason) = reason {
        body["reason"] = json!(reason);
    }
    HubResponse {
        status,
        headers: Vec::new(),
        body: serde_json::to_vec(&body).unwrap(),
    }
}

#[test]
fn parsed_plain_and_json_show_hub_guidance_and_every_supported_reason() {
    let History { root, graph, .. } = history();
    drop(graph);
    let hub = LoopbackHub::start();
    for json in [false, true] {
        for reason in PublishErrorReason::ALL.into_iter().map(Some).chain([None]) {
            let code = reason.map_or(
                HubPublishErrorCode::EntitlementDenied,
                PublishErrorReason::code,
            );
            hub.inject(Fault::Refuse(response(
                code.http_status(),
                code.as_str(),
                reason,
                GUIDANCE,
            )));
            let run = invoke(
                &hub,
                &root.path().join("project"),
                json,
                token(&hub, "curate/claims"),
            );
            assert_eq!(run.exit_code, 2);
            if json {
                assert!(run.stdout.is_empty());
            } else {
                let output = String::from_utf8(run.stdout).unwrap();
                let operation = output.strip_prefix("Operation UUID: ").unwrap().trim_end();
                Uuid::parse_str(operation).unwrap();
            }
            let stderr = String::from_utf8(run.stderr).unwrap();
            let message = if json {
                let doc: serde_json::Value = serde_json::from_str(&stderr).unwrap();
                assert_eq!(doc["error"]["code"], "GF_VALIDATION");
                assert_eq!(
                    doc["error"]["details"]["semantic_code"],
                    format!("hub.publish.{}", code.as_str())
                );
                doc["error"]["message"].as_str().unwrap().to_owned()
            } else {
                stderr
            };
            assert!(
                message.contains(&format!(
                    "hub.publish.{}: {}\nHub message: {GUIDANCE}",
                    code.as_str(),
                    code_detail(code)
                )),
                "{message}"
            );
            if let Some(reason) = reason {
                assert!(
                    message.contains(&format!("\nHub reason: {}", reason.as_str())),
                    "{message}"
                );
            } else {
                assert!(!message.contains("Hub reason:"));
            }
        }
    }
    assert!(puts(&hub.log()).is_empty());
    assert!(
        !hub.log()
            .iter()
            .any(|request| request.path.ends_with("/commit"))
    );
}

#[test]
fn parsed_refusal_prose_cannot_inject_lines_terminal_controls_or_credentials() {
    let History { root, graph, .. } = history();
    drop(graph);
    let hub = LoopbackHub::start();
    for json in [false, true] {
        let credential = token(&hub, "curate/claims");
        let secret = credential.expose().to_owned();
        let message = format!(
            "Create the Project as Public.\r\n\x1b[31m forged\u{202e} {} bEaReR unknown-token https://user:password@hub.test/u/opaque-capability?token=query-secret https://upload.test/opaque-upload-capability",
            secret
        );
        hub.inject(Fault::Refuse(response(
            403,
            "entitlement_denied",
            Some(PublishErrorReason::VisibilityRequired),
            &message,
        )));
        let run = invoke(&hub, &root.path().join("project"), json, credential);
        assert_eq!(run.exit_code, 2);
        let stderr = String::from_utf8(run.stderr).unwrap();
        let message = if json {
            serde_json::from_str::<serde_json::Value>(&stderr).unwrap()["error"]["message"]
                .as_str()
                .unwrap()
                .to_owned()
        } else {
            stderr
        };
        assert!(
            message.contains("Hub message: Create the Project as Public."),
            "{message}"
        );
        for secret in [
            &secret,
            "unknown-token",
            "password",
            "query-secret",
            "opaque-upload-capability",
            "opaque-capability",
        ] {
            assert!(!message.contains(secret), "{message}");
        }
        let line = message
            .lines()
            .find(|line| line.starts_with("Hub message:"))
            .unwrap();
        assert!(line.contains("forged"), "{message}");
        assert!(!line.chars().any(char::is_control));
        assert!(!line.contains('\u{202e}'));
        assert!(message.contains("Hub reason: visibility_required"));
    }
}

#[test]
fn redirect_refusals_redact_request_credentials_in_plain_and_json() {
    let History { root, graph, .. } = history();
    drop(graph);
    let hub = LoopbackHub::start();
    for json in [false, true] {
        for status in [301, 302, 303, 307, 308] {
            let credential = token(&hub, "curate/claims");
            let secret = credential.expose().to_owned();
            hub.inject(Fault::Refuse(response(
                status,
                "auth_denied",
                None,
                &secret,
            )));
            let run = invoke(&hub, &root.path().join("project"), json, credential);
            assert_eq!(run.exit_code, 2);
            let stderr = String::from_utf8(run.stderr).unwrap();
            assert!(
                !stderr.contains(&secret),
                "status {status} leaked a credential"
            );
            assert!(stderr.contains("[redacted]"));
            if json {
                let value: serde_json::Value = serde_json::from_str(&stderr).unwrap();
                assert_eq!(
                    value["error"]["details"]["semantic_code"],
                    "hub.publish.auth_denied"
                );
            }
        }
    }
}

#[test]
fn invalid_bodies_and_contradictory_statuses_preserve_network_failure_contract() {
    let History { root, graph, .. } = history();
    drop(graph);
    let hub = LoopbackHub::start();
    let invalid = [
        br#"{"code":"entitlement_denied","message":"PRIVATE","message":"duplicate"}"#.to_vec(),
        br#"{"code":"entitlement_denied","message":"PRIVATE","reason":"unknown"}"#.to_vec(),
        br#"{"code":"entitlement_denied","message":"PRIVATE","reason":"repository_is_public"}"#
            .to_vec(),
        br#"{"code":"entitlement_denied","message":42}"#.to_vec(),
        b"PRIVATE non-json".to_vec(),
    ];
    for json in [false, true] {
        for refusal in invalid
            .iter()
            .map(|body| HubResponse {
                status: 403,
                headers: Vec::new(),
                body: body.clone(),
            })
            .chain([response(
                500,
                "entitlement_denied",
                Some(PublishErrorReason::VisibilityRequired),
                "PRIVATE",
            )])
        {
            hub.inject(Fault::Refuse(refusal));
            let run = invoke(
                &hub,
                &root.path().join("project"),
                json,
                token(&hub, "curate/claims"),
            );
            assert_eq!(run.exit_code, 3);
            let stderr = String::from_utf8(run.stderr).unwrap();
            assert!(!stderr.contains("PRIVATE"));
            assert!(stderr.contains("hub.network"), "{stderr}");
            if json {
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&stderr).unwrap()["error"]["code"],
                    "GF_IO"
                );
            }
        }
    }
}

#[test]
fn unexpected_status_refusal_bodies_redact_publish_and_preview_credentials() {
    let request = HubRequest::new(HubMethod::Post, "https://hub.test/control")
        .with_header("authorization", "Bearer private-token")
        .with_header("x-vercel-protection-bypass", "private-bypass");
    for status in [200, 201, 202, 204, 301, 302, 303, 307, 308, 401, 500] {
        let safe = refusals::sanitize_response(
            response(status, "auth_denied", None, "private-token private-bypass"),
            &request,
        )
        .unwrap();
        let text = hub_failure(&safe).to_string();
        assert!(!text.contains("private-token"), "status {status}");
        assert!(!text.contains("private-bypass"), "status {status}");
        assert!(text.contains("[redacted]"));
    }
}

#[test]
fn preview_secret_redaction_and_message_budget_use_only_existing_request_values() {
    let request = HubRequest::new(HubMethod::Post, "https://hub.test/control")
        .with_header("authorization", "Bearer private-token")
        .with_header("x-vercel-protection-bypass", "private-bypass");
    let safe = refusals::sanitize_response(
        response(
            403,
            "entitlement_denied",
            None,
            "Create Public first; private-token and private-bypass",
        ),
        &request,
    )
    .unwrap();
    let text = hub_failure(&safe).to_string();
    assert!(text.contains("Create Public first"));
    assert!(!text.contains("private-token"));
    assert!(!text.contains("private-bypass"));
    assert!(refusals::message(&"x ".repeat(2048), &["x"]).len() <= 4096);
    let long = refusals::sanitize_response(
        response(
            403,
            "entitlement_denied",
            None,
            &format!("Create Public first. {}", "long ".repeat(1000)),
        ),
        &request,
    )
    .unwrap();
    assert!(refusals::body(&long).unwrap().message.len() <= 4096);
    let error = hub_failure(&long);
    assert_eq!(error_code(&error), "hub.publish.entitlement_denied");
    assert!(error.to_string().contains("Create Public first."));
    assert_eq!(crate::error_exit_code(&error.into()), 2);
}
