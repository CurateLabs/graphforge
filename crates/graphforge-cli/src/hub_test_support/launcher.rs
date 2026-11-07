//! Explicit, compile-time-only loopback transport and remote in-memory vault.
//! The production URL policy still runs before re-targeting https://hub.test.
use crate::hub_http::{HttpResponse, network, send_with_agent, validate_url};
use graphforge_api::GfError;
use graphforge_hub_publish::{HubMethod, HubRequest, HubResponse};
use std::cell::Cell;
use std::io::Cursor;
use url::Url;

thread_local! {
    static PORT: Cell<Option<u16>> = const { Cell::new(None) };
}

/// Activate a launcher acceptance fixture on the calling thread only.
pub fn start(port: u16) {
    assert_ne!(port, 0);
    PORT.set(Some(port));
}

pub(crate) fn active() -> bool {
    PORT.get().is_some()
}

fn send(port: u16, request: &HubRequest, limit: usize) -> Result<HubResponse, GfError> {
    let url = Url::parse(&request.url).map_err(|_| network("invalid fixture URL"))?;
    validate_url(&url)?;
    if url.origin().ascii_serialization() != "https://hub.test" {
        return Err(network("launcher fixture accepts only https://hub.test"));
    }
    let mut routed = Url::parse(&format!("http://127.0.0.1:{port}")).unwrap();
    routed.set_path(url.path());
    routed.set_query(url.query());
    let mut request = request.clone();
    request.url = routed.to_string();
    let agent = ureq::Agent::config_builder()
        .https_only(false)
        .http_status_as_error(false)
        .max_redirects(0)
        .proxy(None)
        .build()
        .new_agent();
    send_with_agent(&agent, &request, limit)
}

pub(crate) fn exchange(request: &HubRequest, limit: usize) -> Option<Result<HubResponse, GfError>> {
    PORT.get().map(|port| send(port, request, limit))
}

pub(crate) fn read(
    url: &Url,
    range: Option<u64>,
    if_range: Option<&str>,
    limit: u64,
    headers: &[(&str, ureq::http::HeaderValue)],
) -> Option<Result<HttpResponse, GfError>> {
    let port = PORT.get()?;
    let mut request = HubRequest::new(HubMethod::Get, url.as_str());
    if let Some(offset) = range {
        request = request.with_header("range", format!("bytes={offset}-"));
    }
    if let Some(value) = if_range {
        request = request.with_header("if-range", value);
    }
    for (name, value) in headers {
        request = request.with_header(name, value.to_str().expect("fixture ASCII headers"));
    }
    Some(
        send(port, &request, usize::try_from(limit).unwrap_or(usize::MAX)).map(|response| {
            HttpResponse {
                status: response.status,
                location: response.header("location").map(str::to_owned),
                content_range: response.header("content-range").map(str::to_owned),
                etag: response.header("etag").map(str::to_owned),
                content_length: Some(response.body.len() as u64),
                retry_after: None,
                body: Box::new(Cursor::new(response.body)),
            }
        }),
    )
}

pub(crate) fn vault(origin: &str, value: Option<&str>) -> Result<Option<String>, GfError> {
    let request = HubRequest::new(HubMethod::Post, "https://hub.test/_fixture/vault").with_body(
        serde_json::to_vec(&serde_json::json!({"origin":origin,"value":value})).unwrap(),
    );
    let response = exchange(&request, 64 * 1024).expect("fixture active")?;
    serde_json::from_slice(&response.body).map_err(|_| network("invalid fixture vault response"))
}
