//! Origin-bound explicit credentials for discovery and publish control requests.

use super::*;
use ureq::http::HeaderValue;

/// Credentials are opt-in and bound to the explicitly selected Hub origin.
/// No Debug implementation: neither errors nor telemetry may print secrets.
pub(crate) struct DiscoveryCredentials {
    origin: url::Origin,
    headers: Vec<(&'static str, HeaderValue)>,
}

impl DiscoveryCredentials {
    pub(crate) fn load(
        base: &Url,
        token_env: Option<&str>,
        bypass_env: Option<&str>,
        lookup: impl Fn(&str) -> Result<String, std::env::VarError>,
    ) -> Result<Option<Self>, graphforge_api::GfError> {
        validate_url(base)?;
        let mut headers = Vec::new();
        for (source, header, prefix) in [
            (token_env, "authorization", "Bearer "),
            (bypass_env, "x-vercel-protection-bypass", ""),
        ] {
            let Some(source) = source else { continue };
            let secret = lookup(source).map_err(|_| {
                validation(
                    "hub.auth_denied",
                    "clone credential environment variable is missing or not Unicode",
                )
            })?;
            if secret.is_empty() || !secret.bytes().all(|byte| byte.is_ascii_graphic()) {
                return Err(validation(
                    "hub.auth_denied",
                    "clone credential must contain only visible ASCII without whitespace",
                ));
            }
            let mut value = HeaderValue::from_str(&format!("{prefix}{secret}"))
                .map_err(|_| validation("hub.auth_denied", "invalid clone credential header"))?;
            value.set_sensitive(true);
            headers.push((header, value));
        }
        Ok((!headers.is_empty()).then(|| Self {
            origin: base.origin(),
            headers,
        }))
    }

    pub(crate) fn headers_for(
        &self,
        url: &Url,
    ) -> Result<&[(&'static str, HeaderValue)], graphforge_api::GfError> {
        if url.origin() != self.origin {
            return Err(validation(
                "hub.auth_denied",
                "clone credentials require the configured Hub origin",
            ));
        }
        Ok(&self.headers)
    }
}

/// Authenticate discovery at hop zero only. Each retry starts a fresh request;
/// redirects retain normal URL/DNS validation and never receive credentials.
pub(crate) fn fetch_discovery_with_attempts(
    transport: &dyn Transport,
    start: &Url,
    credentials: Option<&DiscoveryCredentials>,
    limit: u64,
    attempts: &mut u32,
    policy: &RetryPolicy,
) -> Result<HttpResponse, graphforge_api::GfError> {
    retry_read(policy, || {
        fetch_once_inner(transport, start, None, None, limit, attempts, credentials)
    })
}
