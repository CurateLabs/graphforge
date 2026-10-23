//! Opportunistic discovery credentials never replace an anonymous not-found.
use super::*;

struct UnavailableReadStore;
impl Store for UnavailableReadStore {
    fn lock(&self, _: &str) -> Result<Box<dyn StoreLock + '_>, GfError> {
        Ok(Box::new(()))
    }
    fn read(&self, _: &str) -> Result<Option<String>, SavedCredentialError> {
        Err(SavedCredentialError::Unavailable)
    }
    fn write(&self, _: &str, _: &str) -> Result<(), GfError> {
        Err(vault_error())
    }
    fn delete(&self, _: &str) -> Result<(), GfError> {
        Err(vault_error())
    }
}

pub(crate) fn unavailable_vault_transport<T: Transport>(inner: T) -> CloneTransport<T> {
    let template = download_transport(404, Box::new(|| panic!("no object acquisition")));
    CloneTransport {
        inner,
        hub: template.hub,
        metadata: template.metadata,
        identity: template.identity,
        discovery_acquire: Box::new(|| {
            session_lookup(
                &UnavailableReadStore,
                &Mock::new(Vec::new()),
                &origin(DEFAULT_HUB).unwrap(),
                100,
            )
            .map(|session| session.map(|session| session.access_token))
        }),
        discovery_token: template.discovery_token,
        acquire: template.acquire,
        token: template.token,
    }
}

fn metadata(transport: &CloneTransport<Reads>, headers: bool) -> Result<u16, GfError> {
    let url = Url::parse("https://graphforge.sh/owner/repo/.gf/refs").unwrap();
    if headers {
        transport.get_discovery(
            &url,
            &[(
                "x-vercel-protection-bypass",
                ureq::http::HeaderValue::from_static("preview-secret"),
            )],
            1024,
        )
    } else {
        transport.get(&url, None, None, 1024)
    }
    .map(|response| response.status)
}

#[test]
fn only_backend_unavailability_is_optional_and_never_exposes_backend_details() {
    for error in [
        keyring::Error::PlatformFailure(Box::new(std::io::Error::other("backend-secret"))),
        keyring::Error::NoStorageAccess(Box::new(std::io::Error::other("backend-secret"))),
    ] {
        assert!(matches!(
            vault_read_error(&error),
            SavedCredentialError::Unavailable
        ));
    }
    for error in [
        keyring::Error::BadEncoding(b"credential-secret".to_vec()),
        keyring::Error::Invalid("attribute-secret".into(), "reason-secret".into()),
        keyring::Error::TooLong("attribute-secret".into(), 1),
        keyring::Error::Ambiguous(Vec::new()),
    ] {
        let error = vault_read_error(&error);
        assert!(matches!(error, SavedCredentialError::Other(_)));
        let error = GfError::from(error).to_string();
        assert!(error.contains("hub.auth_denied"));
        assert!(!error.contains("secret"));
    }
}

#[test]
fn public_success_does_not_consult_saved_credentials_in_either_metadata_path() {
    for headers in [false, true] {
        let transport = download_transport(200, Box::new(|| panic!("no object acquisition")));
        assert_eq!(metadata(&transport, headers).unwrap(), 200);
        assert_eq!(transport.inner.calls.borrow().len(), 1);
    }
}

#[test]
fn unavailable_lookup_is_strict_for_401_403_and_already_authenticated_discovery() {
    for headers in [false, true] {
        for (status, cached) in [(401, false), (403, false), (404, true)] {
            let transport = unavailable_vault_transport(
                download_transport(status, Box::new(|| panic!("no object acquisition"))).inner,
            );
            transport.inner.authorized_status.set(status);
            if cached {
                transport
                    .discovery_token
                    .replace(Some("workos-secret".into()));
            }
            let error = metadata(&transport, headers).unwrap_err().to_string();
            assert!(error.contains("hub.auth_denied"), "{error}");
            assert!(!error.contains("workos-secret"));
            assert_eq!(transport.inner.calls.borrow().len(), 1);
        }
    }
}

#[test]
fn explicit_bearer_denial_never_looks_up_or_replaces_credentials() {
    let transport = download_transport(404, Box::new(|| panic!("no object acquisition")));
    transport.inner.authorized_status.set(401);
    let url = Url::parse("https://graphforge.sh/owner/repo/.gf/refs").unwrap();
    let mut authorization = ureq::http::HeaderValue::from_static("Bearer workos-secret");
    authorization.set_sensitive(true);
    assert_eq!(
        transport
            .get_discovery(
                &url,
                &[
                    (
                        "x-vercel-protection-bypass",
                        ureq::http::HeaderValue::from_static("preview-secret")
                    ),
                    ("authorization", authorization),
                ],
                1024
            )
            .unwrap()
            .status,
        401
    );
    assert_eq!(transport.inner.calls.borrow().len(), 1);
    let missing = format!(
        "GRAPHFORGE_TEST_MISSING_TOKEN_{}",
        uuid::Uuid::now_v7().simple()
    );
    assert!(matches!(
        saved_bearer(&origin(DEFAULT_HUB).unwrap(), Some(&missing), None),
        Err(SavedCredentialError::Other(_))
    ));
}

#[test]
fn corruption_refresh_and_secure_lock_failures_are_not_optional() {
    for headers in [false, true] {
        for failure in ["corrupt", "refresh", "lock"] {
            let mut transport =
                download_transport(404, Box::new(|| panic!("no object acquisition")));
            transport.discovery_acquire = Box::new(move || {
                let hub = origin(DEFAULT_HUB).unwrap();
                if failure == "lock" {
                    return session_lookup(&FailedStore, &Mock::new(Vec::new()), &hub, 100)
                        .map(|session| session.map(|session| session.access_token));
                }
                let store = MemoryStore::default();
                *store.value.borrow_mut() = Some(if failure == "corrupt" {
                    "corrupt-credential-secret".into()
                } else {
                    serde_json::to_string(&Session {
                        access_token: "access-secret".into(),
                        refresh_token: "refresh-secret".into(),
                        expires_at: 1,
                    })
                    .unwrap()
                });
                let provider =
                    Mock::new(vec![(500, serde_json::json!({"error": "provider-secret"}))]);
                session_lookup(&store, &provider, &hub, 100)
                    .map(|session| session.map(|session| session.access_token))
            });
            let error = metadata(&transport, headers).unwrap_err().to_string();
            assert!(error.contains("hub.auth_denied"), "{failure}: {error}");
            assert!(!error.contains("secret"));
            assert_eq!(transport.inner.calls.borrow().len(), 1);
        }
    }
}

#[test]
fn required_sessions_logout_and_object_reads_keep_vault_failures() {
    let hub = origin(DEFAULT_HUB).unwrap();
    let provider = Mock::new(Vec::new());
    for error in [
        session(&UnavailableReadStore, &provider, &hub, 100)
            .err()
            .unwrap(),
        logout(&UnavailableReadStore, &provider, &hub, 100).unwrap_err(),
    ] {
        assert!(
            error
                .to_string()
                .contains("OS credential vault unavailable")
        );
    }
    assert!(provider.requests.borrow().is_empty());
    let transport = download_transport(404, Box::new(|| Err(vault_error())));
    let error = transport
        .get(
            &Url::parse("https://objects.example/missing").unwrap(),
            None,
            None,
            1024,
        )
        .err()
        .unwrap();
    assert!(error.to_string().contains("hub.auth_denied"));
    assert_eq!(transport.inner.calls.borrow().len(), 1);
}
