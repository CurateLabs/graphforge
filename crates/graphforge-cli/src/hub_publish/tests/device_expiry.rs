//! Parsed publish commands through native device polling and the CLI renderer.
use super::*;
use clap::Parser as _;

#[test]
fn parsed_publish_expiry_has_its_own_exit_and_never_mutates_local_or_remote_state() {
    for json in [false, true] {
        for server_expiry in [false, true] {
            let mut history = history();
            let generation = history.graph.committed_generation_identity().unwrap();
            let hub = LoopbackHub::with_config(ReferenceHubConfig {
                base_url: HUB.into(),
                device_code_ttl_seconds: 300,
                ..ReferenceHubConfig::default()
            });
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
            let cli = crate::Cli::try_parse_from(argv).unwrap();
            let Some(crate::Command::Publish(args)) = cli.command else {
                panic!("expected publish");
            };
            let started = std::time::Instant::now();
            let elapsed = std::cell::Cell::new(0);
            let mut notices = Vec::new();
            let mut stdout = Vec::new();
            let error = publish_and_report(
                &hub.transport(),
                &mut history.graph,
                &args,
                &mut PublishEnvironment {
                    bypass: None,
                    credential: Credential::DeviceFlow,
                    clock: &|| started + Duration::from_secs(elapsed.get()),
                    wait: &|duration| {
                        elapsed.set(elapsed.get() + duration.as_secs());
                        hub.hub.advance_clock(if server_expiry {
                            300
                        } else {
                            duration.as_secs()
                        });
                    },
                    notices: &mut notices,
                    resolution: None,
                },
                cli.json,
                &mut stdout,
            )
            .unwrap_err();
            assert_eq!(crate::error_exit_code(&error), 5);
            assert_eq!(error_code(&error), "hub.auth_expired");
            let mut stderr = Vec::new();
            crate::write_runtime_error(&error, cli.json, &mut stderr, &[]).unwrap();
            let text = String::from_utf8(stderr).unwrap();
            if json {
                let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                assert_eq!(value["error"]["code"], "GF_AUTHENTICATION");
                assert_eq!(value["error"]["details"]["kind"], "authentication");
                assert!(stdout.is_empty());
            } else {
                assert!(text.starts_with("GF_AUTHENTICATION: device authorization expired"));
            }
            assert!(text.contains("code lifetime: 300 seconds"), "{text}");
            assert!(text.contains("run the same command again"), "{text}");
            assert!(
                String::from_utf8(notices)
                    .unwrap()
                    .contains("code lifetime: 300 seconds")
            );
            assert_eq!(elapsed.get(), if server_expiry { 5 } else { 300 });
            assert_eq!(
                history.graph.committed_generation_identity().unwrap(),
                generation
            );
            let log = hub.log();
            assert!(log.iter().all(|entry| entry.bearer.is_none()));
            assert!(log.iter().all(|entry| !entry.path.contains("/publish/")));
            assert_eq!(refs_document(&hub, "curate/claims").status, 404);
        }
    }
}
