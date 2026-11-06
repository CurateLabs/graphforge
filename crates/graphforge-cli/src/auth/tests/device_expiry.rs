use super::*;
use clap::Parser as _;

#[test]
fn parsed_login_reports_local_and_server_expiry_with_the_advertised_lifetime() {
    for json in [false, true] {
        for server_expiry in [false, true] {
            let mut argv = vec!["gf", "auth", "login", "--no-browser", "--hub", DEFAULT_HUB];
            if json {
                argv.push("--json");
            }
            let cli = crate::Cli::try_parse_from(argv).unwrap();
            let Some(crate::Command::Auth(args)) = cli.command else {
                panic!("expected auth");
            };
            let AuthCommand::Login { hub, no_browser } = args.command else {
                panic!("expected login");
            };
            assert!(no_browser);
            let mut grant = device();
            grant["expires_in"] = 300.into();
            grant["interval"] = 5.into();
            let mut replies = vec![(200, grant)];
            if server_expiry {
                replies.push((400, serde_json::json!({"error":"expired_token", "error_description":"secret-provider-detail"})));
            } else {
                replies.extend(
                    (0..59).map(|_| (400, serde_json::json!({"error":"authorization_pending"}))),
                );
            }
            let mock = Mock::new(replies);
            let store = MemoryStore::default();
            let clock = Cell::new(100);
            let mut notices = Vec::new();
            let error = login(
                &store,
                &mock,
                &origin(&hub).unwrap(),
                &mut notices,
                &|delay| clock.set(clock.get() + delay.as_secs()),
                &|| clock.get(),
                &|_| {},
            )
            .unwrap_err();
            assert!(matches!(error, CliRuntimeError::Authentication(_)));
            assert_eq!(crate::error_exit_code(&error), 5);
            let mut rendered = Vec::new();
            crate::write_runtime_error(&error, cli.json, &mut rendered, &[]).unwrap();
            let text = String::from_utf8(rendered).unwrap();
            if json {
                let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                assert_eq!(value["error"]["code"], "GF_AUTHENTICATION");
                assert_eq!(
                    value["error"]["details"],
                    serde_json::json!({"source":"runtime", "kind":"authentication", "semantic_code":"hub.auth_expired"})
                );
            } else {
                assert!(text.starts_with("GF_AUTHENTICATION: device authorization expired"));
            }
            assert!(text.contains("code lifetime: 300 seconds"), "{text}");
            assert!(text.contains("run the same command again"), "{text}");
            assert!(!text.contains("secret-provider-detail"));
            assert!(
                String::from_utf8(notices)
                    .unwrap()
                    .contains("code lifetime: 300 seconds")
            );
            assert_eq!(clock.get(), if server_expiry { 105 } else { 400 });
            assert_eq!(store.writes.get(), 0);
            assert!(store.value.borrow().is_none());
            assert!(store.last_login.borrow().is_none());
            assert!(mock.replies.borrow().is_empty());
        }
    }
}

#[test]
fn login_wait_limit_does_not_claim_the_server_code_has_expired() {
    let mut grant = device();
    grant["expires_in"] = u64::MAX.into();
    grant["interval"] = u64::MAX.into();
    let mut replies = vec![(200, grant)];
    replies.extend((0..14).map(|_| (400, serde_json::json!({"error":"authorization_pending"}))));
    let clock = Cell::new(0);
    let error = login(
        &MemoryStore::default(),
        &Mock::new(replies),
        &origin(DEFAULT_HUB).unwrap(),
        &mut Vec::new(),
        &|delay| {
            assert_eq!(delay.as_secs(), 60);
            clock.set(clock.get() + delay.as_secs());
        },
        &|| clock.get(),
        &|_| {},
    )
    .unwrap_err();
    assert_eq!(crate::error_exit_code(&error), 5);
    let CliRuntimeError::Authentication(outcome) = &error else {
        panic!("expected auth outcome");
    };
    assert_eq!(outcome.semantic_code(), "hub.auth_wait_limit");
    assert_eq!(clock.get(), 900);
    assert!(
        error
            .to_string()
            .contains(&format!("code lifetime: {} seconds", u64::MAX))
    );
    assert!(!error.to_string().contains("expired"));
}

#[test]
fn zero_lifetime_is_invalid_and_denial_is_not_expiry() {
    for zero in [false, true] {
        let mut grant = device();
        if zero {
            grant["expires_in"] = 0.into();
        }
        let mut replies = vec![(200, grant)];
        if !zero {
            replies.push((400, serde_json::json!({"error":"access_denied"})));
        }
        let mock = Mock::new(replies);
        let store = MemoryStore::default();
        let mut output = Vec::new();
        let error = login(
            &store,
            &mock,
            &origin(DEFAULT_HUB).unwrap(),
            &mut output,
            &|_| assert!(!zero),
            &|| 100,
            &|_| assert!(!zero),
        )
        .unwrap_err();
        assert!(matches!(error, CliRuntimeError::Core(_)));
        assert_eq!(crate::error_exit_code(&error), 2);
        assert!(!error.to_string().contains("expired"));
        assert_eq!(store.writes.get(), 0);
        assert_eq!(mock.requests.borrow().len(), if zero { 1 } else { 2 });
        if zero {
            assert!(output.is_empty());
        }
    }
}
