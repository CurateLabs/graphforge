//! Denials retain the actual selected origin across text and JSON diagnostics.
use super::*;

fn assert_rendered(error: GfError, expected: &str) {
    let error: CliRuntimeError = error.into();
    assert_eq!(crate::error_exit_code(&error), 2);
    for json in [false, true] {
        let mut output = Vec::new();
        crate::write_runtime_error(&error, json, &mut output, &[]).unwrap();
        let text = String::from_utf8(output).unwrap();
        assert!(text.contains(&format!("Hub origin: {expected}")), "{text}");
        assert!(!text.contains("secret"), "{text}");
        if json {
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(value["error"]["code"], "GF_VALIDATION");
            assert_eq!(
                value["error"]["details"]["semantic_code"],
                "hub.auth_denied"
            );
        }
    }
}

#[test]
fn repository_denial_names_selected_production_explicit_and_full_url_origins() {
    for (repository, selected, expected) in [
        ("owner/repo", None, DEFAULT_HUB),
        (
            "owner/repo",
            Some("https://preview.example:8443"),
            "https://preview.example:8443",
        ),
        (
            "https://preview.example/owner/repo",
            None,
            "https://preview.example",
        ),
        (
            "https://PREVIEW.example:443/owner/repo",
            None,
            "https://preview.example",
        ),
    ] {
        let (identity, base) = crate::hub_http::parse_input_at(repository, selected).unwrap();
        let hub = origin(&base.origin().ascii_serialization()).unwrap();
        for capability in ["read", "publish"] {
            for status in [401, 403] {
                let store = MemoryStore::default();
                *store.value.borrow_mut() = Some(
                    serde_json::to_string(&Session {
                        access_token: "selected-session-secret".into(),
                        refresh_token: "refresh-secret".into(),
                        expires_at: 1000,
                    })
                    .unwrap(),
                );
                *store.last_login.borrow_mut() = Some("https://other.example".into());
                let mock = Mock::new(vec![(
                    status,
                    serde_json::json!({"origin":"https://wrong.example", "error":"provider-secret"}),
                )]);
                let error = repository_token_from(
                    &store,
                    &mock,
                    &hub,
                    &identity.owner,
                    &identity.repository,
                    capability,
                    100,
                )
                .err()
                .unwrap();
                assert!(error.to_string().contains("repository access denied"));
                assert_rendered(error, expected);
                let requests = mock.requests.borrow();
                assert_eq!(
                    requests.len(),
                    1,
                    "formatting must not refresh or acquire another token"
                );
                assert_eq!(
                    requests[0].url,
                    format!("{expected}/api/cli/repository-token")
                );
                assert_eq!(
                    requests[0].header("authorization"),
                    Some("Bearer selected-session-secret")
                );
                assert_eq!(store.writes.get(), 0);
                assert!(
                    store
                        .value
                        .borrow()
                        .as_ref()
                        .unwrap()
                        .contains("selected-session-secret")
                );
            }
        }
    }
}

#[test]
fn refusal_origin_strips_userinfo_path_query_and_fragment() {
    for (url, expected) in [
        (
            "https://user:secret@HUB.example:443/path-secret?token=query-secret#fragment-secret",
            "https://hub.example",
        ),
        (
            "https://user:secret@[2001:4860:4860::8888]:8443/capability-secret",
            "https://[2001:4860:4860::8888]:8443",
        ),
    ] {
        assert_rendered(
            denied_at(&Url::parse(url).unwrap(), "repository access denied"),
            expected,
        );
    }
}

#[test]
fn login_refresh_status_and_logout_refusals_name_the_contacted_hub() {
    let hub = origin("https://preview.example:8443").unwrap();
    for poll in [false, true] {
        let mut replies = Vec::new();
        if poll {
            replies.push((200, device()));
        }
        replies.push((
            403,
            serde_json::json!({"error":"access_denied", "error_description":"provider-secret"}),
        ));
        let mock = Mock::new(replies);
        let store = MemoryStore::default();
        let error = login(
            &store,
            &mock,
            &hub,
            &mut Vec::new(),
            &|_| {},
            &|| 100,
            &|_| {},
        )
        .unwrap_err();
        let CliRuntimeError::Core(error) = error else {
            panic!("denial must not become expiry")
        };
        assert_rendered(error, "https://preview.example:8443");
        assert_eq!(mock.requests.borrow().len(), if poll { 2 } else { 1 });
        assert_eq!(store.writes.get(), 0);
    }
    let mock = Mock::new(vec![(401, serde_json::json!({"error":"provider-secret"}))]);
    let error = verify_status(&mock, &hub, "session-secret", 100)
        .err()
        .unwrap();
    assert_rendered(error, "https://preview.example:8443");
    for refresh in [false, true] {
        let store = MemoryStore::default();
        *store.value.borrow_mut() = Some(
            serde_json::to_string(&Session {
                access_token: "session-secret".into(),
                refresh_token: "refresh-secret".into(),
                expires_at: if refresh { 1 } else { 1000 },
            })
            .unwrap(),
        );
        let mock = Mock::new(vec![(403, serde_json::json!({"error":"provider-secret"}))]);
        let error = if refresh {
            session(&store, &mock, &hub, 100).err().unwrap()
        } else {
            logout(&store, &mock, &hub, 100).unwrap_err()
        };
        assert_rendered(error, "https://preview.example:8443");
        assert_eq!(mock.requests.borrow().len(), 1);
        assert_eq!(store.value.borrow().is_none(), !refresh);
    }
}
