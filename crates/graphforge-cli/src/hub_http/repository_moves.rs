//! Observe one marked repository move without changing generic object redirects.
use super::*;
use graphforge_discovery::{DiscoveryLimits, RepositoryMoved};
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::io::Cursor;

/// Request-local move evidence. The inner transport still owns DNS/HTTPS and
/// generic redirect credential stripping. This layer never sends a request.
pub(crate) struct RepositoryMoveTransport<'a> {
    inner: &'a dyn Transport,
    requested: Url,
    identity: RepositoryIdentity,
    evidence: RefCell<Option<(RepositoryMoved, BTreeSet<String>)>>,
}
impl<'a> RepositoryMoveTransport<'a> {
    pub(crate) fn new(inner: &'a dyn Transport, identity: &RepositoryIdentity, base: &Url) -> Self {
        Self {
            inner,
            requested: base.clone(),
            identity: identity.clone(),
            evidence: RefCell::new(None),
        }
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
            if let Some(suffix) = url.path().strip_prefix(&prefix) {
                if matches!(suffix, "refs" | "manifest" | "publish") {
                    return Some(suffix.into());
                }
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
    pub(crate) fn has_move(&self) -> bool {
        self.evidence.borrow().is_some()
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
            self.observe(url, response.status, response.location.as_deref(), &body)?;
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
        self.response(url, self.inner.get(url, range, if_range, limit)?)
    }
    fn get_discovery(
        &self,
        url: &Url,
        headers: &[(&str, ureq::http::HeaderValue)],
        limit: u64,
    ) -> Result<HttpResponse, graphforge_api::GfError> {
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
        let response = self.inner.exchange(request, limit)?;
        if request.method == HubMethod::Get {
            let url = Url::parse(&request.url)
                .map_err(|_| validation("hub.integrity_failure", "repository URL is invalid"))?;
            self.observe(
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
