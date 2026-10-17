use super::*;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;

#[derive(Default)]
struct MemoryStore {
    value: RefCell<Option<String>>,
    writes: Cell<usize>,
}
impl Store for MemoryStore {
    fn lock(&self, _: &str) -> Result<Box<dyn StoreLock + '_>, GfError> {
        Ok(Box::new(()))
    }
    fn read(&self, _: &str) -> Result<Option<String>, GfError> {
        Ok(self.value.borrow().clone())
    }
    fn write(&self, _: &str, value: &str) -> Result<(), GfError> {
        self.writes.set(self.writes.get() + 1);
        *self.value.borrow_mut() = Some(value.into());
        Ok(())
    }
    fn delete(&self, _: &str) -> Result<(), GfError> {
        *self.value.borrow_mut() = None;
        Ok(())
    }
}
struct Mock {
    replies: RefCell<VecDeque<HubResponse>>,
    requests: RefCell<Vec<HubRequest>>,
}
impl Mock {
    fn new(replies: Vec<(u16, serde_json::Value)>) -> Self {
        Self {
            replies: RefCell::new(
                replies
                    .into_iter()
                    .map(|(status, body)| HubResponse {
                        status,
                        headers: Default::default(),
                        body: serde_json::to_vec(&body).unwrap(),
                    })
                    .collect(),
            ),
            requests: RefCell::new(Vec::new()),
        }
    }
}
impl Transport for Mock {
    fn get(
        &self,
        _: &Url,
        _: Option<u64>,
        _: Option<&str>,
        _: u64,
    ) -> Result<crate::hub_http::HttpResponse, GfError> {
        panic!("unexpected read")
    }
    fn exchange(&self, request: &HubRequest, _: usize) -> Result<HubResponse, GfError> {
        self.requests.borrow_mut().push(request.clone());
        Ok(self
            .replies
            .borrow_mut()
            .pop_front()
            .expect("unexpected request"))
    }
}
fn tokens(access: &str, refresh: &str) -> serde_json::Value {
    serde_json::json!({"access_token":access,"refresh_token":refresh,"expires_in":300,"token_type":"Bearer"})
}
fn device() -> serde_json::Value {
    serde_json::json!({"device_code":"device-secret","user_code":"ABCD-EFGH","verification_uri":"https://api.workos.com/device","verification_uri_complete":"https://api.workos.com/device?user_code=ABCD-EFGH","expires_in":60,"interval":1})
}
#[test]
fn login_polls_pending_slow_down_and_saves_one_atomic_pair() {
    let store = MemoryStore::default();
    let mock = Mock::new(vec![
        (200, device()),
        (400, serde_json::json!({"error":"authorization_pending"})),
        (400, serde_json::json!({"error":"slow_down"})),
        (200, tokens("access-secret", "refresh-secret")),
    ]);
    let time = Cell::new(100);
    let waits = RefCell::new(Vec::new());
    let opened = RefCell::new(Vec::new());
    let mut output = Vec::new();
    login(
        &store,
        &mock,
        &origin(DEFAULT_HUB).unwrap(),
        &mut output,
        &|delay| {
            time.set(time.get() + delay.as_secs());
            waits.borrow_mut().push(delay.as_secs());
        },
        &|| time.get(),
        &|url| opened.borrow_mut().push(url.clone()),
    )
    .unwrap();
    assert_eq!(*waits.borrow(), [1, 1, 6]);
    assert_eq!(store.writes.get(), 1);
    assert_eq!(opened.borrow()[0].query(), Some("user_code=ABCD-EFGH"));
    let output = String::from_utf8(output).unwrap();
    for secret in ["device-secret", "access-secret", "refresh-secret"] {
        assert!(!output.contains(secret));
    }
    let saved: Session = serde_json::from_str(store.value.borrow().as_ref().unwrap()).unwrap();
    assert_eq!(saved.expires_at, 408);
    assert!(
        mock.requests
            .borrow()
            .iter()
            .all(|r| Url::parse(&r.url).unwrap().origin() == origin(DEFAULT_HUB).unwrap().origin())
    );
}
#[test]
fn expired_session_rotates_both_tokens_and_invalid_grant_removes_saved_pair() {
    let store = MemoryStore::default();
    let hub = origin(DEFAULT_HUB).unwrap();
    save(
        &store,
        &hub,
        Mock::new(vec![(200, tokens("old-access", "old-refresh"))])
            .replies
            .borrow_mut()
            .pop_front()
            .unwrap(),
        100,
    )
    .unwrap();
    let mock = Mock::new(vec![(200, tokens("new-access", "new-refresh"))]);
    let refreshed = session(&store, &mock, &hub, 390).unwrap().unwrap();
    assert_eq!(refreshed.refresh_token, "new-refresh");
    let requests = mock.requests.borrow();
    let body = String::from_utf8_lossy(&requests[0].body);
    assert!(body.contains("refresh_token=old-refresh"));
    drop(requests);
    assert!(
        session(
            &store,
            &Mock::new(vec![(400, serde_json::json!({"error":"invalid_grant"}))]),
            &hub,
            700
        )
        .is_err()
    );
    assert!(store.value.borrow().is_none());
}
#[test]
fn denied_expired_or_malicious_verification_never_saves() {
    for error in ["access_denied", "expired_token"] {
        let store = MemoryStore::default();
        let mock = Mock::new(vec![
            (200, device()),
            (
                400,
                serde_json::json!({"error":error,"error_description":"secret-provider-detail"}),
            ),
        ]);
        let result = login(
            &store,
            &mock,
            &origin(DEFAULT_HUB).unwrap(),
            &mut Vec::new(),
            &|_| {},
            &|| 100,
            &|_| {},
        );
        assert!(result.is_err());
        assert_eq!(store.writes.get(), 0);
        assert!(
            !result
                .err()
                .unwrap()
                .to_string()
                .contains("secret-provider-detail")
        );
    }
    for url in [
        "http://api.workos.com/device",
        "https://user:password@api.workos.com/device",
        "https://api.workos.com/device#fragment",
    ] {
        assert!(verification_url(url).is_err(), "{url}");
    }
    assert!(verification_url("https://auth.example.com/device?user_code=CODE").is_ok());
    assert!(verification_url("https://auth.example.com/\nmalicious").is_err());
    let mut response = device();
    response["verification_uri_complete"] = serde_json::json!("https://different.example/device");
    assert!(
        login(
            &MemoryStore::default(),
            &Mock::new(vec![(200, response)]),
            &origin(DEFAULT_HUB).unwrap(),
            &mut Vec::new(),
            &|_| {},
            &|| 100,
            &|_| panic!("must reject mismatched origin")
        )
        .is_err()
    );
    assert!(origin("https://example.com/path").is_err());
    assert!(origin("https://example.com?query=value").is_err());
}
#[test]
fn timeout_and_malformed_tokens_leave_vault_unchanged() {
    let store = MemoryStore::default();
    let mut response = device();
    response["expires_in"] = serde_json::json!(1);
    let mock = Mock::new(vec![(200, response)]);
    assert!(
        login(
            &store,
            &mock,
            &origin(DEFAULT_HUB).unwrap(),
            &mut Vec::new(),
            &|_| panic!("no wait after expiry"),
            &|| 100,
            &|_| {}
        )
        .is_err()
    );
    let mut malformed = tokens("access\nsecret", "refresh");
    malformed["token_type"] = serde_json::json!("Basic");
    assert!(
        save(
            &store,
            &origin(DEFAULT_HUB).unwrap(),
            Mock::new(vec![(200, malformed)])
                .replies
                .borrow_mut()
                .pop_front()
                .unwrap(),
            100
        )
        .is_err()
    );
    assert_eq!(store.writes.get(), 0);
}

struct Reads {
    status: Cell<u16>,
    authorized_status: Cell<u16>,
    calls: RefCell<Vec<(String, Option<u64>, Option<String>, bool)>>,
}
impl Reads {
    fn response(&self) -> crate::hub_http::HttpResponse {
        crate::hub_http::HttpResponse {
            status: self.status.get(),
            location: Some("https://evil.example/stolen".into()),
            content_range: Some("bytes 10-20/21".into()),
            etag: Some("digest".into()),
            body: Box::new(std::io::Cursor::new(b"payload".to_vec())),
        }
    }
}
impl Transport for Reads {
    fn get(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        _: u64,
    ) -> Result<crate::hub_http::HttpResponse, GfError> {
        self.calls
            .borrow_mut()
            .push((url.to_string(), range, if_range.map(str::to_owned), false));
        Ok(self.response())
    }
    fn get_discovery(
        &self,
        url: &Url,
        headers: &[(&str, ureq::http::HeaderValue)],
        _: u64,
    ) -> Result<crate::hub_http::HttpResponse, GfError> {
        let bearer = headers.iter().find(|(name, _)| *name == "authorization");
        assert!(
            headers
                .iter()
                .any(|(name, value)| *name == "x-vercel-protection-bypass"
                    && value.to_str().unwrap() == "preview-secret")
        );
        if let Some((_, bearer)) = bearer {
            assert_eq!(bearer.to_str().unwrap(), "Bearer workos-secret");
            assert!(bearer.is_sensitive());
        }
        self.calls
            .borrow_mut()
            .push((url.to_string(), None, None, bearer.is_some()));
        let mut response = self.response();
        if bearer.is_some() {
            response.status = self.authorized_status.get();
        }
        Ok(response)
    }
    fn get_authorized(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        _: u64,
        headers: &[(&str, ureq::http::HeaderValue)],
    ) -> Result<crate::hub_http::HttpResponse, GfError> {
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].0, "authorization");
        assert_eq!(
            headers[0].1.to_str().unwrap(),
            if url.path().contains("/.gf/") {
                "Bearer workos-secret"
            } else {
                "Bearer repository-secret"
            }
        );
        assert!(headers[0].1.is_sensitive());
        self.calls
            .borrow_mut()
            .push((url.to_string(), range, if_range.map(str::to_owned), true));
        let mut response = self.response();
        response.status = self.authorized_status.get();
        Ok(response)
    }
}
fn download_transport(
    status: u16,
    acquire: Box<dyn Fn() -> Result<Option<RepositoryToken>, GfError>>,
) -> CloneTransport<Reads> {
    CloneTransport {
        inner: Reads {
            status: Cell::new(status),
            authorized_status: Cell::new(200),
            calls: RefCell::new(Vec::new()),
        },
        hub: origin(DEFAULT_HUB).unwrap(),
        metadata: RefCell::new([
            Url::parse("https://graphforge.sh/owner/repo/.gf/refs").unwrap(),
            Url::parse("https://graphforge.sh/owner/repo/.gf/manifest").unwrap(),
        ]),
        identity: std::rc::Rc::new(RefCell::new(
            graphforge_discovery::RepositoryIdentity::parse("owner/repo").unwrap(),
        )),
        discovery_acquire: Box::new(|| panic!("unexpected discovery vault access")),
        discovery_token: RefCell::new(None),
        acquire,
        token: RefCell::new(None),
    }
}
#[test]
fn public_clone_never_reads_saved_credentials() {
    let transport = download_transport(
        200,
        Box::new(|| panic!("public clone must not consult vault")),
    );
    transport
        .get(
            &Url::parse("https://graphforge.sh/owner/repo/.gf/refs").unwrap(),
            None,
            None,
            1024,
        )
        .unwrap();
    assert_eq!(transport.inner.calls.borrow().len(), 1);
    assert!(!transport.inner.calls.borrow()[0].3);
}
#[test]
fn private_401_and_masked_404_retry_scoped_and_retain_range_validators() {
    for status in [401, 404] {
        let mut transport = download_transport(
            status,
            Box::new(|| {
                Ok(Some(RepositoryToken {
                    token: "repository-secret".into(),
                    origin: origin("https://objects.example").unwrap(),
                }))
            }),
        );
        transport.discovery_acquire = Box::new(|| Ok(Some("workos-secret".into())));
        let url = Url::parse("https://graphforge.sh/owner/repo/.gf/refs").unwrap();
        transport.get(&url, None, None, 1024).unwrap();
        transport.inner.authorized_status.set(206);
        transport
            .get(
                &Url::parse("https://objects.example/objects/data").unwrap(),
                Some(10),
                Some("digest"),
                1024,
            )
            .unwrap();
        let calls = transport.inner.calls.borrow();
        assert_eq!(calls.len(), 4);
        assert!(!calls[0].3);
        assert!(calls[1].3);
        assert!(!calls[2].3);
        assert!(calls[3].3);
        assert_eq!(calls[3].1, Some(10));
        assert_eq!(calls[3].2.as_deref(), Some("digest"));
    }
}
#[test]
fn scoped_objects_reject_origin_changes_and_all_redirects_before_following() {
    let transport = download_transport(200, Box::new(|| panic!("already authorized")));
    transport.token.replace(Some(RepositoryToken {
        token: "repository-secret".into(),
        origin: origin(DEFAULT_HUB).unwrap(),
    }));
    for url in [
        "https://evil.example/object",
        "https://graphforge.sh:444/object",
    ] {
        assert!(
            transport
                .get(&Url::parse(url).unwrap(), None, None, 1024)
                .is_err()
        );
    }
    assert!(transport.inner.calls.borrow().is_empty());
    for status in [301, 302, 303, 307, 308] {
        transport.inner.authorized_status.set(status);
        assert!(
            transport
                .get(
                    &Url::parse("https://graphforge.sh/object").unwrap(),
                    None,
                    None,
                    1024
                )
                .is_err()
        );
    }
    assert_eq!(transport.inner.calls.borrow().len(), 5);
}

#[test]
fn logout_revokes_provider_and_cleans_vault_even_if_remote_fails() {
    for status in [200, 204, 401, 500] {
        let store = MemoryStore::default();
        let hub = origin(DEFAULT_HUB).unwrap();
        save(
            &store,
            &hub,
            Mock::new(vec![(200, tokens("access-secret", "refresh-secret"))])
                .replies
                .borrow_mut()
                .pop_front()
                .unwrap(),
            100,
        )
        .unwrap();
        let mock = Mock::new(vec![(
            status,
            serde_json::json!({"provider_detail":"hidden-secret"}),
        )]);
        let result = logout(&store, &mock, &hub, 101);
        assert_eq!(result.is_ok(), status != 500);
        assert!(store.value.borrow().is_none());
        assert!(mock.requests.borrow()[0].url.ends_with("/api/cli/logout"));
        if let Err(error) = result {
            assert!(!error.to_string().contains("hidden-secret"));
        }
    }
}
#[test]
fn repository_exchange_keeps_workos_session_on_hub_and_returns_separate_scoped_origin() {
    let hub = origin(DEFAULT_HUB).unwrap();
    let mock = Mock::new(vec![(
        200,
        serde_json::json!({"access_token":"repository-secret","token_type":"Bearer","expires_in":3600,"origin":"https://objects.example","repository_id":"repo-id"}),
    )]);
    let grant =
        exchange_repository_token(&mock, &hub, "workos-secret", "owner", "repo", "read").unwrap();
    assert_eq!(
        grant.origin.origin().ascii_serialization(),
        "https://objects.example"
    );
    assert_eq!(grant.token, "repository-secret");
    let requests = mock.requests.borrow();
    assert_eq!(requests.len(), 1);
    assert_eq!(Url::parse(&requests[0].url).unwrap().origin(), hub.origin());
    assert_eq!(
        requests[0].header("authorization"),
        Some("Bearer workos-secret")
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&requests[0].body).unwrap(),
        serde_json::json!({"owner":"owner","name":"repo","capability":"read"})
    );
}

#[test]
fn transient_refresh_error_preserves_credentials() {
    for (status, error) in [
        (400, "invalid_request"),
        (429, "rate_limit_exceeded"),
        (500, "server_error"),
    ] {
        let store = MemoryStore::default();
        let hub = origin(DEFAULT_HUB).unwrap();
        save(
            &store,
            &hub,
            Mock::new(vec![(200, tokens("old-access", "old-refresh"))])
                .replies
                .borrow_mut()
                .pop_front()
                .unwrap(),
            100,
        )
        .unwrap();
        assert!(
            session(
                &store,
                &Mock::new(vec![(status, serde_json::json!({"error":error}))]),
                &hub,
                500
            )
            .is_err()
        );
        let saved: Session = serde_json::from_str(store.value.borrow().as_ref().unwrap()).unwrap();
        assert_eq!(saved.refresh_token, "old-refresh");
    }
}
struct FailedStore;
impl Store for FailedStore {
    fn lock(&self, _: &str) -> Result<Box<dyn StoreLock + '_>, GfError> {
        Err(vault_error())
    }
    fn read(&self, _: &str) -> Result<Option<String>, GfError> {
        Err(vault_error())
    }
    fn write(&self, _: &str, _: &str) -> Result<(), GfError> {
        Err(vault_error())
    }
    fn delete(&self, _: &str) -> Result<(), GfError> {
        Err(vault_error())
    }
}
#[test]
fn unavailable_vault_fails_without_plaintext_fallback_or_secret_errors() {
    let hub = origin(DEFAULT_HUB).unwrap();
    let mock = Mock::new(Vec::new());
    let error = session(&FailedStore, &mock, &hub, 100).err().unwrap();
    assert!(
        error
            .to_string()
            .contains("OS credential vault unavailable")
    );
    assert!(mock.requests.borrow().is_empty());
    let response = Mock::new(vec![(200, tokens("access-secret", "refresh-secret"))])
        .replies
        .borrow_mut()
        .pop_front()
        .unwrap();
    let error = save(&FailedStore, &hub, response, 100).err().unwrap();
    assert!(!error.to_string().contains("access-secret"));
    assert!(!error.to_string().contains("refresh-secret"));
}
#[test]
fn each_expired_or_denied_scoped_read_renews_once_preserving_range() {
    for status in [401, 403, 404] {
        let renewals = std::rc::Rc::new(Cell::new(0));
        let count = renewals.clone();
        let transport = download_transport(
            200,
            Box::new(move || {
                count.set(count.get() + 1);
                Ok(Some(RepositoryToken {
                    token: "repository-secret".into(),
                    origin: origin("https://objects.example").unwrap(),
                }))
            }),
        );
        transport.token.replace(Some(RepositoryToken {
            token: "repository-secret".into(),
            origin: origin("https://objects.example").unwrap(),
        }));
        transport.inner.authorized_status.set(status);
        for _ in 0..2 {
            transport
                .get(
                    &Url::parse("https://objects.example/data").unwrap(),
                    Some(10),
                    Some("digest"),
                    1024,
                )
                .unwrap();
        }
        assert_eq!(renewals.get(), 2);
        assert_eq!(transport.inner.calls.borrow().len(), 4);
        assert!(
            transport
                .inner
                .calls
                .borrow()
                .iter()
                .all(|call| call.1 == Some(10) && call.2.as_deref() == Some("digest"))
        );
    }
}

#[test]
fn auth_commands_parse_without_a_project_and_headless_login_keeps_hub() {
    use clap::Parser as _;
    for command in ["login", "status", "logout"] {
        let parsed = crate::Cli::try_parse_from([
            "gf",
            "auth",
            command,
            "--hub",
            "https://alternate.example",
        ])
        .unwrap();
        assert!(matches!(parsed.command, Some(crate::Command::Auth(_))));
        assert!(parsed.project.is_none());
    }
    let parsed = crate::Cli::try_parse_from(["gf", "auth", "login", "--no-browser"]).unwrap();
    assert!(matches!(
        parsed.command,
        Some(crate::Command::Auth(AuthArgs {
            command: AuthCommand::Login {
                no_browser: true,
                ..
            },
            ..
        }))
    ));
}

#[test]
fn preview_bypass_combines_saved_identity_only_at_hub_discovery() {
    let mut transport = download_transport(
        404,
        Box::new(|| panic!("metadata must not acquire object token")),
    );
    transport.discovery_acquire = Box::new(|| Ok(Some("workos-secret".into())));
    let mut bypass = ureq::http::HeaderValue::from_static("preview-secret");
    bypass.set_sensitive(true);
    let url = Url::parse("https://graphforge.sh/owner/repo/.gf/refs").unwrap();
    assert_eq!(
        transport
            .get_discovery(
                &url,
                &[("x-vercel-protection-bypass", bypass.clone())],
                1024
            )
            .unwrap()
            .status,
        200
    );
    let calls = transport.inner.calls.borrow();
    assert_eq!(calls.len(), 2);
    assert!(!calls[0].3);
    assert!(calls[1].3);
    drop(calls);
    assert!(
        transport
            .get_discovery(
                &Url::parse("https://objects.example/data").unwrap(),
                &[("x-vercel-protection-bypass", bypass)],
                1024
            )
            .is_err()
    );
    assert_eq!(transport.inner.calls.borrow().len(), 2);
}
#[test]
fn status_rejects_revocation_expiry_and_malformed_identity_without_secrets() {
    let hub = origin(DEFAULT_HUB).unwrap();
    let valid =
        serde_json::json!({"authenticated":true,"principal_id":"user_123","expires_at":200});
    assert_eq!(
        verify_status(&Mock::new(vec![(200, valid)]), &hub, "workos-secret", 100)
            .unwrap()
            .principal_id,
        "user_123"
    );
    for (code, body) in [
        (401, serde_json::json!({"secret":"hidden"})),
        (
            200,
            serde_json::json!({"authenticated":false,"principal_id":"user_123","expires_at":200}),
        ),
        (
            200,
            serde_json::json!({"authenticated":true,"principal_id":"user_123","expires_at":99}),
        ),
        (
            200,
            serde_json::json!({"authenticated":true,"principal_id":"user\n123","expires_at":200}),
        ),
    ] {
        let result = verify_status(&Mock::new(vec![(code, body)]), &hub, "workos-secret", 100);
        assert!(result.is_err());
        assert!(!result.err().unwrap().to_string().contains("hidden"));
    }
}

#[test]
fn auth_preview_bypass_is_origin_bound_and_available_after_subcommand() {
    use clap::Parser as _;
    for command in ["login", "status", "logout"] {
        let parsed = crate::Cli::try_parse_from([
            "gf",
            "auth",
            command,
            "--protection-bypass-env",
            "PREVIEW_SECRET",
        ])
        .unwrap();
        let Some(crate::Command::Auth(args)) = parsed.command else {
            panic!("expected auth")
        };
        assert_eq!(
            args.protection_bypass_env.as_deref(),
            Some("PREVIEW_SECRET")
        );
    }
    let transport = PreviewTransport {
        inner: Mock::new(vec![(200, serde_json::json!({}))]),
        origin: origin(DEFAULT_HUB).unwrap().origin(),
        bypass: Some("preview-secret".into()),
    };
    request(
        &transport,
        &origin(DEFAULT_HUB).unwrap(),
        "/api/cli/device",
        HubMethod::Post,
        None,
        &[("client_id", "graphforge-cli")],
        None,
    )
    .unwrap();
    assert_eq!(
        transport.inner.requests.borrow()[0].header("x-vercel-protection-bypass"),
        Some("preview-secret")
    );
    assert!(
        transport
            .exchange(
                &HubRequest::new(HubMethod::Post, "https://api.workos.com/device"),
                1024
            )
            .is_err()
    );
    assert!(
        transport
            .exchange(
                &HubRequest::new(HubMethod::Get, "https://objects.example/data"),
                1024
            )
            .is_err()
    );
    assert_eq!(transport.inner.requests.borrow().len(), 1);
}

struct PrivateFixture {
    calls: RefCell<Vec<(String, bool)>>,
    manifest: Vec<u8>,
    refs: Vec<u8>,
    project: Vec<u8>,
    object: Url,
}
impl PrivateFixture {
    fn response(status: u16, body: Vec<u8>) -> crate::hub_http::HttpResponse {
        crate::hub_http::HttpResponse {
            status,
            location: None,
            content_range: None,
            etag: Some("\"private-fixture\"".into()),
            body: Box::new(std::io::Cursor::new(body)),
        }
    }
}
impl Transport for PrivateFixture {
    fn get(
        &self,
        url: &Url,
        _: Option<u64>,
        _: Option<&str>,
        _: u64,
    ) -> Result<crate::hub_http::HttpResponse, GfError> {
        self.calls.borrow_mut().push((url.to_string(), false));
        Ok(Self::response(
            if url.origin() == self.object.origin() {
                401
            } else {
                404
            },
            Vec::new(),
        ))
    }
    fn get_authorized(
        &self,
        url: &Url,
        range: Option<u64>,
        _: Option<&str>,
        _: u64,
        headers: &[(&str, ureq::http::HeaderValue)],
    ) -> Result<crate::hub_http::HttpResponse, GfError> {
        self.calls.borrow_mut().push((url.to_string(), true));
        let bearer = headers
            .iter()
            .find(|(name, _)| *name == "authorization")
            .unwrap()
            .1
            .to_str()
            .unwrap();
        if url == &self.object {
            assert_eq!(bearer, "Bearer repository-secret");
            assert_eq!(range, None);
            Ok(Self::response(200, self.project.clone()))
        } else {
            assert_eq!(url.origin(), origin(DEFAULT_HUB).unwrap().origin());
            assert_eq!(bearer, "Bearer workos-secret");
            let bytes = if url.path().ends_with("/.gf/refs") {
                &self.refs
            } else {
                assert!(url.path().ends_with("/.gf/manifest"));
                &self.manifest
            };
            Ok(Self::response(200, bytes.clone()))
        }
    }
}
#[test]
fn private_clone_authenticates_metadata_and_objects_then_imports_and_reopens_fixture() {
    macro_rules! fixture {
        ($name:literal) => {
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../tests/fixtures/hub/generated/v1/",
                $name
            ))
        };
    }
    let manifest_bytes = fixture!("manifest.json");
    let manifest = graphforge_discovery::DiscoveryManifest::from_json(
        manifest_bytes,
        graphforge_discovery::DiscoveryLimits::default(),
    )
    .unwrap();
    let object = Url::parse(&manifest.package_object().unwrap().locations[0]).unwrap();
    assert_ne!(object.origin(), origin(DEFAULT_HUB).unwrap().origin());
    let object_origin = origin(&object.origin().ascii_serialization()).unwrap();
    let transport = CloneTransport {
        inner: PrivateFixture {
            calls: RefCell::new(Vec::new()),
            manifest: manifest_bytes.to_vec(),
            refs: fixture!("refs.json").to_vec(),
            project: fixture!("objects/openalex-openalex.gfpb").to_vec(),
            object,
        },
        hub: origin(DEFAULT_HUB).unwrap(),
        metadata: RefCell::new([
            Url::parse("https://graphforge.sh/openalex/openalex/.gf/refs").unwrap(),
            Url::parse("https://graphforge.sh/openalex/openalex/.gf/manifest").unwrap(),
        ]),
        identity: std::rc::Rc::new(RefCell::new(
            graphforge_discovery::RepositoryIdentity::parse("openalex/openalex").unwrap(),
        )),
        discovery_acquire: Box::new(|| Ok(Some("workos-secret".into()))),
        discovery_token: RefCell::new(None),
        acquire: Box::new(move || {
            Ok(Some(RepositoryToken {
                token: "repository-secret".into(),
                origin: object_origin.clone(),
            }))
        }),
        token: RefCell::new(None),
    };
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("private-clone");
    let mut output = Vec::new();
    crate::hub_clone::run_clone_with(
        &transport,
        crate::hub_clone::CloneArgs {
            repository: "openalex/openalex".into(),
            hub: None,
            token_env: None,
            protection_bypass_env: None,
            destination: Some(destination.clone()),
            telemetry_endpoint: None,
            git_ref: None,
            version_uuid: None,
        },
        true,
        &mut output,
    )
    .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(result["contract"], "graphforge-hub-clone/1");
    assert_eq!(result["package_digest"], manifest.package.package_digest.0);
    let cloned =
        graphforge_api::GraphForge::new(destination.to_str()).expect("authenticated clone reopens");
    assert_eq!(
        cloned.research_project_metadata().unwrap().title.as_deref(),
        Some("OpenAlex")
    );
    let calls = transport.inner.calls.borrow();
    assert_eq!(calls.len(), 5);
    assert_eq!(
        calls.iter().filter(|(_, authorized)| *authorized).count(),
        3
    );
    let text = String::from_utf8(output).unwrap();
    assert!(!text.contains("workos-secret"));
    assert!(!text.contains("repository-secret"));
}

#[test]
fn saved_publish_refresh_and_repository_exchange_keep_preview_bypass_on_hub() {
    let hub = origin(DEFAULT_HUB).unwrap();
    let store = MemoryStore::default();
    save(
        &store,
        &hub,
        Mock::new(vec![(200, tokens("old-access", "old-refresh"))])
            .replies
            .borrow_mut()
            .pop_front()
            .unwrap(),
        0,
    )
    .unwrap();
    let transport = PreviewTransport {
        inner: Mock::new(vec![
            (200, tokens("new-access", "new-refresh")),
            (
                200,
                serde_json::json!({"access_token":"publish-scope","token_type":"Bearer","expires_in":300,"origin":DEFAULT_HUB,"repository_id":"repo-id"}),
            ),
        ]),
        origin: hub.origin(),
        bypass: Some("preview-secret".into()),
    };
    let grant = repository_token_from(&store, &transport, &hub, "owner", "repo", "publish", 500)
        .unwrap()
        .unwrap();
    assert_eq!(grant.token, "publish-scope");
    let requests = transport.inner.requests.borrow();
    assert_eq!(requests.len(), 2);
    for request in requests.iter() {
        assert_eq!(Url::parse(&request.url).unwrap().origin(), hub.origin());
        assert_eq!(
            request.header("x-vercel-protection-bypass"),
            Some("preview-secret")
        );
    }
    assert!(requests[0].url.ends_with("/api/cli/token"));
    assert!(requests[0].header("authorization").is_none());
    assert!(requests[1].url.ends_with("/api/cli/repository-token"));
    assert_eq!(
        requests[1].header("authorization"),
        Some("Bearer new-access")
    );
}

#[derive(Clone, Copy)]
enum MoveFault {
    None,
    Foreign,
    Disagree,
    Chain,
}
struct MovedPrivateFixture {
    base: PrivateFixture,
    fault: MoveFault,
    protected: bool,
    marker_reads: Cell<usize>,
    calls: RefCell<Vec<(String, bool, bool)>>,
}
impl MovedPrivateFixture {
    fn reply(
        &self,
        url: &Url,
        headers: &[(&str, ureq::http::HeaderValue)],
    ) -> Result<crate::hub_http::HttpResponse, GfError> {
        let bearer = headers
            .iter()
            .find(|(name, _)| *name == "authorization")
            .map(|(_, value)| value.to_str().unwrap());
        let bypass = headers.iter().any(|(name, value)| {
            *name == "x-vercel-protection-bypass" && value == "preview-secret"
        });
        self.calls
            .borrow_mut()
            .push((url.to_string(), bearer.is_some(), bypass));
        if url == &self.base.object {
            assert!(
                !bypass,
                "object request must never carry protection credentials"
            );
            if bearer.is_none() {
                return Ok(PrivateFixture::response(401, Vec::new()));
            }
            assert_eq!(
                bearer,
                Some("Bearer new-repository-secret"),
                "old repository scope must never be reused"
            );
            return Ok(PrivateFixture::response(200, self.base.project.clone()));
        }
        assert_eq!(
            url.origin(),
            origin(DEFAULT_HUB).unwrap().origin(),
            "foreign target was contacted"
        );
        if self.protected && !bypass {
            return Ok(PrivateFixture::response(403, Vec::new()));
        }
        if bearer != Some("Bearer workos-secret") {
            return Ok(PrivateFixture::response(401, Vec::new()));
        }
        let old = url.path().starts_with("/openalex/openalex/.gf/");
        let chained = !old && matches!(self.fault, MoveFault::Chain);
        if old || chained {
            self.marker_reads
                .set(self.marker_reads.get() + usize::from(old));
            let from = if old {
                "openalex/openalex"
            } else {
                "research/renamed"
            };
            let to = if chained
                || matches!(self.fault, MoveFault::Disagree) && url.path().ends_with("manifest")
            {
                "other/third"
            } else {
                "research/renamed"
            };
            let marker = graphforge_discovery::RepositoryMoved {
                format: "graphforge-repository-moved/1".into(),
                moved_from: graphforge_discovery::RepositoryIdentity::parse(from).unwrap(),
                moved_to: graphforge_discovery::RepositoryIdentity::parse(to).unwrap(),
            };
            let host = if matches!(self.fault, MoveFault::Foreign) {
                "foreign.example"
            } else {
                "graphforge.sh"
            };
            let suffix = if url.path().ends_with("refs") {
                "refs"
            } else {
                "manifest"
            };
            let mut response = PrivateFixture::response(308, serde_json::to_vec(&marker).unwrap());
            response.location = Some(format!("https://{host}/{to}/.gf/{suffix}"));
            return Ok(response);
        }
        assert!(
            self.marker_reads.get() >= 2,
            "new metadata was contacted before both old markers"
        );
        assert!(url.path().starts_with("/research/renamed/.gf/"));
        Ok(PrivateFixture::response(
            200,
            if url.path().ends_with("refs") {
                self.base.refs.clone()
            } else {
                self.base.manifest.clone()
            },
        ))
    }
}
impl Transport for MovedPrivateFixture {
    fn get(
        &self,
        url: &Url,
        _: Option<u64>,
        _: Option<&str>,
        _: u64,
    ) -> Result<crate::hub_http::HttpResponse, GfError> {
        self.reply(url, &[])
    }
    fn get_authorized(
        &self,
        url: &Url,
        _: Option<u64>,
        _: Option<&str>,
        _: u64,
        headers: &[(&str, ureq::http::HeaderValue)],
    ) -> Result<crate::hub_http::HttpResponse, GfError> {
        self.reply(url, headers)
    }
    fn get_discovery(
        &self,
        url: &Url,
        headers: &[(&str, ureq::http::HeaderValue)],
        _: u64,
    ) -> Result<crate::hub_http::HttpResponse, GfError> {
        self.reply(url, headers)
    }
}
fn moved_private_transport(
    fault: MoveFault,
    protected: bool,
) -> (
    CloneTransport<MovedPrivateFixture>,
    std::rc::Rc<RefCell<Vec<String>>>,
) {
    let mut manifest = graphforge_discovery::DiscoveryManifest::from_json(
        include_bytes!("../../../../tests/fixtures/hub/generated/v1/manifest.json"),
        graphforge_discovery::DiscoveryLimits::default(),
    )
    .unwrap();
    let object = Url::parse(&manifest.package_object().unwrap().locations[0]).unwrap();
    manifest.repository =
        graphforge_discovery::RepositoryIdentity::parse("research/renamed").unwrap();
    let mut refs = graphforge_discovery::RefSet::from_json(
        include_bytes!("../../../../tests/fixtures/hub/generated/v1/refs.json"),
        graphforge_discovery::DiscoveryLimits::default(),
    )
    .unwrap();
    refs.repository = manifest.repository.clone();
    for reference in &mut refs.refs {
        reference.validator = manifest.canonical_digest().unwrap();
    }
    let identity = std::rc::Rc::new(RefCell::new(
        graphforge_discovery::RepositoryIdentity::parse("openalex/openalex").unwrap(),
    ));
    let current = identity.clone();
    let grants = std::rc::Rc::new(RefCell::new(Vec::new()));
    let acquired = grants.clone();
    let object_origin = origin(&object.origin().ascii_serialization()).unwrap();
    let old_object_origin = object_origin.clone();
    let transport = CloneTransport {
        inner: MovedPrivateFixture {
            base: PrivateFixture {
                calls: RefCell::new(Vec::new()),
                manifest: manifest.to_canonical_json().unwrap(),
                refs: refs.to_canonical_json().unwrap(),
                project: include_bytes!(
                    "../../../../tests/fixtures/hub/generated/v1/objects/openalex-openalex.gfpb"
                )
                .to_vec(),
                object,
            },
            fault,
            protected,
            marker_reads: Cell::new(0),
            calls: RefCell::new(Vec::new()),
        },
        hub: origin(DEFAULT_HUB).unwrap(),
        metadata: RefCell::new([
            Url::parse("https://graphforge.sh/openalex/openalex/.gf/refs").unwrap(),
            Url::parse("https://graphforge.sh/openalex/openalex/.gf/manifest").unwrap(),
        ]),
        identity,
        discovery_acquire: Box::new(|| Ok(Some("workos-secret".into()))),
        discovery_token: RefCell::new(Some("workos-secret".into())),
        acquire: Box::new(move || {
            acquired
                .borrow_mut()
                .push(current.borrow().canonical_name());
            assert_eq!(current.borrow().canonical_name(), "research/renamed");
            Ok(Some(RepositoryToken {
                token: "new-repository-secret".into(),
                origin: object_origin.clone(),
            }))
        }),
        token: RefCell::new(Some(RepositoryToken {
            token: "old-repository-secret".into(),
            origin: old_object_origin,
        })),
    };
    (transport, grants)
}
fn moved_clone_args(destination: &std::path::Path) -> crate::hub_clone::CloneArgs {
    crate::hub_clone::CloneArgs {
        repository: "openalex/openalex".into(),
        hub: None,
        token_env: None,
        protection_bypass_env: None,
        destination: Some(destination.into()),
        telemetry_endpoint: None,
        git_ref: None,
        version_uuid: None,
    }
}
#[test]
fn private_moved_clone_reacquires_current_scope_and_reopens_native_project() {
    let (transport, grants) = moved_private_transport(MoveFault::None, false);
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("clone");
    let mut output = Vec::new();
    crate::hub_clone::run_clone_with(
        &transport,
        moved_clone_args(&destination),
        true,
        &mut output,
    )
    .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(result["repository"], "research/renamed");
    assert_eq!(&*grants.borrow(), &["research/renamed"]);
    assert_eq!(
        transport.discovery_token.borrow().as_deref(),
        Some("workos-secret")
    );
    let reopened = graphforge_api::GraphForge::new(destination.to_str()).unwrap();
    assert_eq!(
        reopened
            .research_project_metadata()
            .unwrap()
            .title
            .as_deref(),
        Some("OpenAlex")
    );
}
#[test]
fn malformed_private_moves_never_acquire_a_current_grant_or_publish_output() {
    for fault in [MoveFault::Foreign, MoveFault::Disagree, MoveFault::Chain] {
        let (transport, grants) = moved_private_transport(fault, false);
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("clone");
        let mut output = Vec::new();
        assert!(
            crate::hub_clone::run_clone_with(
                &transport,
                moved_clone_args(&destination),
                true,
                &mut output
            )
            .is_err()
        );
        assert!(grants.borrow().is_empty());
        assert!(output.is_empty());
        assert!(!destination.exists());
        assert!(
            transport
                .inner
                .calls
                .borrow()
                .iter()
                .all(|(url, _, _)| !url.contains("foreign.example")
                    && !url.contains("/other/third/"))
        );
    }
}
#[test]
fn verified_current_metadata_reapplies_only_configured_same_origin_protection_policy() {
    let (transport, grants) = moved_private_transport(MoveFault::None, true);
    let base = Url::parse("https://graphforge.sh/openalex/openalex").unwrap();
    let identity = graphforge_discovery::RepositoryIdentity::parse("openalex/openalex").unwrap();
    let credentials =
        crate::hub_http::DiscoveryCredentials::load(&base, None, Some("BYPASS"), |_| {
            Ok("preview-secret".into())
        })
        .unwrap()
        .unwrap();
    let moved = crate::hub_http::RepositoryMoveTransport::new(&transport, &identity, &base)
        .with_credentials(Some(&credentials));
    let mut attempts = 0;
    let response = crate::hub_http::fetch_discovery_with_attempts(
        &moved,
        &crate::hub_http::endpoint(&base, "refs"),
        Some(&credentials),
        16 * 1024 * 1024,
        &mut attempts,
        &crate::hub_http::RETRY_POLICY,
    )
    .unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(
        moved.resolved(false).unwrap().0.canonical_name(),
        "research/renamed"
    );
    assert!(
        grants.borrow().is_empty(),
        "metadata does not request an object grant"
    );
    assert!(
        transport
            .inner
            .calls
            .borrow()
            .iter()
            .all(|(_, _, bypass)| *bypass)
    );
    assert_eq!(moved.extra_metadata_requests(), 1);
    let response = moved
        .get(&transport.inner.base.object, None, None, 1024 * 1024)
        .unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(&*grants.borrow(), &["research/renamed"]);
}
