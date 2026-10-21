//! Observe one marked repository move without changing generic object redirects.
use super::*;
use graphforge_discovery::{DiscoveryLimits, RepositoryMoved};
use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::io::Cursor;

/// Request-local move evidence and one bounded old-metadata confirmation probe.
/// The inner transport still owns every DNS/HTTPS exchange; generic redirects
/// and object requests retain their credential isolation.
pub(crate) struct RepositoryMoveTransport<'a> {
    inner: &'a dyn Transport,
    requested: Url,
    identity: RepositoryIdentity,
    evidence: RefCell<Option<(RepositoryMoved, BTreeSet<String>)>>,
    credentials: Option<&'a DiscoveryCredentials>,
    rebound: Cell<bool>,
    extra_requests: Cell<u32>,
    pending_current: RefCell<Option<Url>>,
}
impl<'a> RepositoryMoveTransport<'a> {
    pub(crate) fn new(inner: &'a dyn Transport, identity: &RepositoryIdentity, base: &Url) -> Self {
        Self {
            inner,
            requested: base.clone(),
            identity: identity.clone(),
            evidence: RefCell::new(None),
            credentials: None,
            rebound: Cell::new(false),
            extra_requests: Cell::new(0),
            pending_current: RefCell::new(None),
        }
    }
    pub(crate) fn with_credentials(
        mut self,
        credentials: Option<&'a DiscoveryCredentials>,
    ) -> Self {
        self.credentials = credentials;
        self
    }
    pub(crate) fn extra_metadata_requests(&self) -> u32 {
        self.extra_requests.get()
    }
    fn current_metadata(&self, url: &Url) -> bool {
        self.pending_current.borrow().as_ref() == Some(url)
            && self.rebound.get()
            && self.evidence.borrow().as_ref().is_some_and(|(moved, _)| {
                ["refs", "manifest", "publish"].iter().any(|suffix| {
                    let mut expected = self.requested.clone();
                    expected.set_path(&format!(
                        "/{}/.gf/{suffix}",
                        moved.moved_to.canonical_name()
                    ));
                    url == &expected
                })
            })
    }
    fn confirm_metadata_move(&self, url: &Url) -> Result<(), graphforge_api::GfError> {
        if self.rebound.get()
            || !matches!(
                self.endpoint(url).as_deref(),
                Some("refs" | "manifest" | "publish")
            )
        {
            return Ok(());
        }
        for suffix in ["refs", "manifest"] {
            let evidence = self.evidence.borrow();
            let Some((_, seen)) = evidence.as_ref() else {
                return Ok(());
            };
            let already_seen = seen.contains(suffix);
            drop(evidence);
            if already_seen {
                continue;
            }
            let counterpart = endpoint(&self.requested, suffix);
            self.inner.validate(&counterpart)?;
            self.extra_requests.set(self.extra_requests.get() + 1);
            // A new request to the OLD identity, under the configured policy;
            // no destination from the move has been contacted yet.
            let response = if let Some(credentials) = self.credentials {
                self.inner.get_discovery(
                    &counterpart,
                    credentials.headers_for(&counterpart)?,
                    4096,
                )?
            } else {
                self.inner.get(&counterpart, None, None, 4096)?
            };
            // Any server-side or throttling status leaves the confirmation open to
            // a retry; the stable status classes of the read path do not apply.
            if matches!(response.status, 408 | 429 | 500..=599) {
                return Err(network(
                    "repository move confirmation temporarily unavailable",
                ));
            }
            if response.status != 308 {
                return Err(validation(
                    "hub.integrity_failure",
                    "repository move markers disagree",
                ));
            }
            let mut bytes = Vec::new();
            response
                .body
                .take(4097)
                .read_to_end(&mut bytes)
                .map_err(|_| network("move response read failed"))?;
            if bytes.len() > 4096 {
                return Err(limit_error("repository move marker exceeds bound"));
            }
            self.observe(
                &counterpart,
                response.status,
                response.location.as_deref(),
                &bytes,
            )?;
        }
        let (_, current, moved) = self.resolved(false)?;
        self.inner
            .repository_moved(moved.as_ref().expect("verified move"), &current)?;
        self.rebound.set(true);
        Ok(())
    }
    fn endpoint(&self, url: &Url) -> Option<String> {
        if url.origin() != self.requested.origin() {
            return None;
        }
        let evidence = self.evidence.borrow();
        let mut identities = vec![&self.identity];
        if let Some((moved, _)) = evidence.as_ref() {
            identities.push(&moved.moved_to);
        }
        for identity in identities {
            let prefix = format!("/{}/.gf/", identity.canonical_name());
            if let Some(suffix) = url.path().strip_prefix(&prefix)
                && matches!(suffix, "refs" | "manifest" | "publish")
            {
                return Some(suffix.into());
            }
        }
        None
    }
    fn observe(
        &self,
        url: &Url,
        status: u16,
        location: Option<&str>,
        body: &[u8],
    ) -> Result<(), graphforge_api::GfError> {
        let Some(endpoint) = self.endpoint(url) else {
            return Ok(());
        };
        if !matches!(status, 301 | 302 | 303 | 307 | 308) {
            return Ok(());
        }
        let refused = || {
            validation(
                "hub.integrity_failure",
                "repository move is not one matching same-origin 308",
            )
        };
        let location = location.ok_or_else(refused)?;
        let marker = RepositoryMoved::from_json(
            body,
            DiscoveryLimits {
                max_response_bytes: 4096,
                ..DiscoveryLimits::default()
            },
        );
        let Ok(marker) = marker else {
            // Preserve ordinary redirects only when they do not change the
            // repository endpoint path. Generic transport strips credentials.
            if url
                .join(location)
                .is_ok_and(|target| target.path() == url.path())
            {
                return Ok(());
            }
            return Err(refused());
        };
        if status != 308 || marker.moved_from != self.identity {
            return Err(refused());
        }
        marker.destination(url, location).map_err(|_| refused())?;
        let mut evidence = self.evidence.borrow_mut();
        match evidence.as_mut() {
            Some((prior, seen)) => {
                if prior != &marker {
                    return Err(refused());
                }
                seen.insert(endpoint);
            }
            None => {
                *evidence = Some((marker, BTreeSet::from([endpoint])));
            }
        }
        Ok(())
    }
    /// Complete only after old refs and manifest agree on exactly one move.
    pub(crate) fn resolved(
        &self,
        require_publish: bool,
    ) -> Result<(RepositoryIdentity, Url, Option<RepositoryMoved>), graphforge_api::GfError> {
        let evidence = self.evidence.borrow();
        let Some((moved, seen)) = evidence.as_ref() else {
            return Ok((self.identity.clone(), self.requested.clone(), None));
        };
        if !seen.contains("refs")
            || !seen.contains("manifest")
            || (require_publish && !seen.contains("publish"))
        {
            return Err(validation(
                "hub.integrity_failure",
                "repository control documents disagree on the move",
            ));
        }
        let mut base = self.requested.clone();
        base.set_path(&format!("/{}", moved.moved_to.canonical_name()));
        Ok((moved.moved_to.clone(), base, Some(moved.clone())))
    }
    fn observe_response(
        &self,
        url: &Url,
        status: u16,
        location: Option<&str>,
        body: &[u8],
    ) -> Result<(), graphforge_api::GfError> {
        self.observe(url, status, location, body)?;
        self.confirm_metadata_move(url)?;
        if self.rebound.get()
            && let Some((moved, _)) = self.evidence.borrow().as_ref()
            && let Some(location) = location
            && let Ok(current) = moved.destination(url, location)
        {
            self.pending_current.replace(Some(current));
        }
        Ok(())
    }
    fn response(
        &self,
        url: &Url,
        mut response: HttpResponse,
    ) -> Result<HttpResponse, graphforge_api::GfError> {
        if self.endpoint(url).is_some() && matches!(response.status, 301 | 302 | 303 | 307 | 308) {
            let mut body = Vec::new();
            response
                .body
                .take(4097)
                .read_to_end(&mut body)
                .map_err(|_| network("move response read failed"))?;
            if body.len() > 4096 {
                return Err(limit_error("repository move marker exceeds bound"));
            }
            self.observe_response(url, response.status, response.location.as_deref(), &body)?;
            response.body = Box::new(Cursor::new(body));
        }
        Ok(response)
    }
}
impl Transport for RepositoryMoveTransport<'_> {
    fn validate(&self, url: &Url) -> Result<(), graphforge_api::GfError> {
        self.inner.validate(url)
    }
    fn get(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
    ) -> Result<HttpResponse, graphforge_api::GfError> {
        let current_metadata = self.current_metadata(url);
        if current_metadata {
            self.pending_current.take();
        }
        let response = if current_metadata && let Some(credentials) = self.credentials {
            // Fresh authenticated metadata request after the two-marker trust
            // boundary. Generic redirects and object reads never use this.
            self.inner
                .get_discovery(url, credentials.headers_for(url)?, limit)?
        } else {
            self.inner.get(url, range, if_range, limit)?
        };
        self.response(url, response)
    }
    fn get_discovery(
        &self,
        url: &Url,
        headers: &[(&str, ureq::http::HeaderValue)],
        limit: u64,
    ) -> Result<HttpResponse, graphforge_api::GfError> {
        if self.current_metadata(url) {
            self.pending_current.take();
        }
        self.response(url, self.inner.get_discovery(url, headers, limit)?)
    }
    fn get_authorized(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
        headers: &[(&str, ureq::http::HeaderValue)],
    ) -> Result<HttpResponse, graphforge_api::GfError> {
        self.inner
            .get_authorized(url, range, if_range, limit, headers)
    }
    fn exchange(
        &self,
        request: &HubRequest,
        limit: usize,
    ) -> Result<HubResponse, graphforge_api::GfError> {
        if request.method != HubMethod::Get {
            return self.inner.exchange(request, limit);
        }
        let url = Url::parse(&request.url)
            .map_err(|_| validation("hub.integrity_failure", "repository URL is invalid"))?;
        let mut request = request.clone();
        if self.current_metadata(&url) {
            self.pending_current.take();
            if let Some(credentials) = self.credentials {
                // A fresh configured-policy request only after old markers agree.
                // Never restore a bearer or credentials on a generic redirect.
                for (name, value) in credentials
                    .headers_for(&url)?
                    .iter()
                    .filter(|(name, _)| *name == "x-vercel-protection-bypass")
                {
                    request = request.with_header(
                        name,
                        value.to_str().map_err(|_| {
                            validation("hub.auth_denied", "invalid preview credential")
                        })?,
                    );
                }
            }
        }
        let response = self.inner.exchange(&request, limit)?;
        if self.endpoint(&url).is_some() && matches!(response.status, 301 | 302 | 303 | 307 | 308) {
            self.observe_response(
                &url,
                response.status,
                response.header("location"),
                &response.body,
            )?;
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct NoNetwork;
    impl Transport for NoNetwork {
        fn get(
            &self,
            _: &Url,
            _: Option<u64>,
            _: Option<&str>,
            _: u64,
        ) -> Result<HttpResponse, graphforge_api::GfError> {
            panic!("move validation must not touch a target")
        }
    }
    fn marker(from: &str, to: &str) -> Vec<u8> {
        serde_json::to_vec(&RepositoryMoved {
            format: "graphforge-repository-moved/1".into(),
            moved_from: RepositoryIdentity::parse(from).unwrap(),
            moved_to: RepositoryIdentity::parse(to).unwrap(),
        })
        .unwrap()
    }
    #[test]
    fn unmarked_confirmation_redirect_is_refused_after_one_probe() {
        struct Unmarked(Cell<u32>);
        impl Transport for Unmarked {
            fn get(
                &self,
                url: &Url,
                _: Option<u64>,
                _: Option<&str>,
                _: u64,
            ) -> Result<HttpResponse, graphforge_api::GfError> {
                self.0.set(self.0.get() + 1);
                assert_eq!(self.0.get(), 1, "confirmation must stay bounded");
                assert_eq!(url.path(), "/a/x/.gf/manifest");
                Ok(HttpResponse {
                    status: 308,
                    location: Some(url.to_string()),
                    etag: None,
                    content_range: None,
                    retry_after: None,
                    body: Box::new(Cursor::new(Vec::new())),
                })
            }
        }
        let inner = Unmarked(Cell::new(0));
        let base = Url::parse("https://hub.example/a/x").unwrap();
        let tracker =
            RepositoryMoveTransport::new(&inner, &RepositoryIdentity::parse("a/x").unwrap(), &base);
        let refs = endpoint(&base, "refs");
        tracker
            .observe(
                &refs,
                308,
                Some("https://hub.example/b/y/.gf/refs"),
                &marker("a/x", "b/y"),
            )
            .unwrap();
        assert!(
            tracker
                .confirm_metadata_move(&refs)
                .unwrap_err()
                .to_string()
                .contains("hub.integrity_failure")
        );
        assert_eq!(inner.0.get(), 1);
        assert!(!tracker.rebound.get());
    }
    #[test]
    fn temporary_confirmation_failures_retry_before_current_metadata() {
        struct TransientConfirmation {
            status: u16,
            probes: Cell<u32>,
            rebound: Cell<bool>,
        }
        impl Transport for TransientConfirmation {
            fn get(
                &self,
                url: &Url,
                _: Option<u64>,
                _: Option<&str>,
                _: u64,
            ) -> Result<HttpResponse, graphforge_api::GfError> {
                let (status, location, bytes) = match url.path() {
                    "/a/x/.gf/refs" => (
                        308,
                        Some("https://hub.example/b/y/.gf/refs".into()),
                        marker("a/x", "b/y"),
                    ),
                    "/a/x/.gf/manifest" => {
                        self.probes.set(self.probes.get() + 1);
                        assert!(!self.rebound.get());
                        if self.probes.get() == 1 {
                            (self.status, None, Vec::new())
                        } else {
                            (
                                308,
                                Some("https://hub.example/b/y/.gf/manifest".into()),
                                marker("a/x", "b/y"),
                            )
                        }
                    }
                    "/b/y/.gf/refs" => {
                        assert!(self.rebound.get());
                        assert_eq!(self.probes.get(), 2);
                        (200, None, Vec::new())
                    }
                    _ => panic!("unexpected move request"),
                };
                Ok(HttpResponse {
                    status,
                    location,
                    etag: None,
                    content_range: None,
                    retry_after: None,
                    body: Box::new(Cursor::new(bytes)),
                })
            }
            fn repository_moved(
                &self,
                _: &RepositoryMoved,
                _: &Url,
            ) -> Result<(), graphforge_api::GfError> {
                assert_eq!(self.probes.get(), 2);
                assert!(!self.rebound.replace(true));
                Ok(())
            }
        }
        for status in [408, 429, 500, 503, 599] {
            let inner = TransientConfirmation {
                status,
                probes: Cell::new(0),
                rebound: Cell::new(false),
            };
            let base = Url::parse("https://hub.example/a/x").unwrap();
            let tracker = RepositoryMoveTransport::new(
                &inner,
                &RepositoryIdentity::parse("a/x").unwrap(),
                &base,
            );
            let mut attempts = 0;
            let response = fetch_with_attempts(
                &tracker,
                &endpoint(&base, "refs"),
                None,
                None,
                4096,
                &mut attempts,
                &RetryPolicy {
                    attempts: 2,
                    initial_backoff: Duration::ZERO,
                    max_backoff: Duration::ZERO,
                },
            )
            .unwrap();
            assert_eq!(response.status, 200);
            assert_eq!(attempts, 3);
        }
    }
    #[test]
    fn move_requires_agreement_and_refuses_chain_without_target_io() {
        let identity = RepositoryIdentity::parse("a/x").unwrap();
        let base = Url::parse("https://hub.example/a/x").unwrap();
        let transport = RepositoryMoveTransport::new(&NoNetwork, &identity, &base);
        let refs = base.join("/a/x/.gf/refs").unwrap();
        transport
            .observe(
                &refs,
                308,
                Some("https://hub.example/b/y/.gf/refs"),
                &marker("a/x", "b/y"),
            )
            .unwrap();
        assert!(transport.resolved(false).is_err());
        let manifest = base.join("/a/x/.gf/manifest").unwrap();
        assert!(
            transport
                .observe(
                    &manifest,
                    308,
                    Some("https://hub.example/c/z/.gf/manifest"),
                    &marker("a/x", "c/z")
                )
                .is_err()
        );
        transport
            .observe(
                &manifest,
                308,
                Some("https://hub.example/b/y/.gf/manifest"),
                &marker("a/x", "b/y"),
            )
            .unwrap();
        assert_eq!(transport.resolved(false).unwrap().0.canonical_name(), "b/y");
        let second = base.join("/b/y/.gf/refs").unwrap();
        assert!(
            transport
                .observe(
                    &second,
                    308,
                    Some("https://hub.example/c/z/.gf/refs"),
                    &marker("b/y", "c/z")
                )
                .is_err()
        );
    }
    #[test]
    fn marked_move_rejects_status_origin_and_missing_marker_before_target_io() {
        let identity = RepositoryIdentity::parse("a/x").unwrap();
        let base = Url::parse("https://hub.example/a/x").unwrap();
        let refs = base.join("/a/x/.gf/refs").unwrap();
        for (status, location, body) in [
            (
                307,
                "https://hub.example/b/y/.gf/refs",
                marker("a/x", "b/y"),
            ),
            (
                308,
                "https://other.example/b/y/.gf/refs",
                marker("a/x", "b/y"),
            ),
            (308, "https://hub.example/b/y/.gf/refs", Vec::new()),
        ] {
            let transport = RepositoryMoveTransport::new(&NoNetwork, &identity, &base);
            assert!(
                transport
                    .observe(&refs, status, Some(location), &body)
                    .is_err()
            );
        }
    }
}
