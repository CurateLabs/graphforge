//! Compile-time-only acceptance fixture for native Python and Node workflows.
//!
//! Explicit activation replaces Hub dispatch on the calling thread only. Without
//! activation the ordinary production dispatch, URL policy and transport run.
//! Distribution builds do not enable this feature or expose its binding hooks.

use crate::Command;
use crate::hub_http::{HttpResponse, Transport, network, validate_url};
use graphforge_api::GfError;
use graphforge_hub_publish::{
    HubMethod, HubRequest, HubResponse, ReferenceHub, ReferenceHubConfig,
};
use std::cell::RefCell;
use std::io::{Cursor, Write};
use std::ops::ControlFlow;
use std::path::Path;
use std::sync::Arc;
use url::Url;

thread_local! {
    static FIXTURE: RefCell<Option<Arc<ReferenceHub>>> = const { RefCell::new(None) };
}

/// Start a fresh reference Hub on this thread and return a scoped publish token.
/// The fixture is dropped on replacement or thread exit. No server is started.
#[must_use]
pub fn start() -> String {
    let hub = Arc::new(
        ReferenceHub::with_config(ReferenceHubConfig {
            base_url: "https://hub.test".into(),
            ..ReferenceHubConfig::default()
        })
        .expect("reference Hub configuration is valid"),
    );
    let token = hub.issue_token("publish:curate/claims", 900);
    FIXTURE.with(|fixture| *fixture.borrow_mut() = Some(hub));
    token
}

/// Drop the fixture and restore ordinary production dispatch on this thread.
pub fn stop() {
    FIXTURE.with(|fixture| *fixture.borrow_mut() = None);
}

struct ReferenceTransport(Arc<ReferenceHub>);

impl ReferenceTransport {
    fn response(&self, request: &HubRequest, limit: usize) -> Result<HubResponse, GfError> {
        let url = Url::parse(&request.url).map_err(|_| network("invalid fixture URL"))?;
        // Keep the product URL validation, and fail closed outside this fixture.
        validate_url(&url)?;
        if url.origin().ascii_serialization() != "https://hub.test" {
            return Err(network("the native fixture accepts only https://hub.test"));
        }
        let response = self.0.handle(request);
        if response.body.len() > limit {
            return Err(network("reference Hub response exceeds byte limit"));
        }
        Ok(response)
    }

    fn read(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
        headers: &[(&str, ureq::http::HeaderValue)],
    ) -> Result<HttpResponse, GfError> {
        let mut request = HubRequest::new(HubMethod::Get, url.as_str());
        if let Some(offset) = range {
            request = request.with_header("range", format!("bytes={offset}-"));
        }
        if let Some(validator) = if_range {
            request = request.with_header("if-range", validator);
        }
        for (name, value) in headers {
            request = request.with_header(
                name,
                value
                    .to_str()
                    .map_err(|_| network("invalid fixture header"))?,
            );
        }
        let response = self.response(&request, usize::try_from(limit).unwrap_or(usize::MAX))?;
        Ok(HttpResponse {
            status: response.status,
            location: response.header("location").map(str::to_owned),
            content_range: response.header("content-range").map(str::to_owned),
            etag: response.header("etag").map(str::to_owned),
            content_length: response
                .header("content-length")
                .and_then(|value| value.parse().ok()),
            retry_after: None,
            body: Box::new(Cursor::new(response.body)),
        })
    }
}

impl Transport for ReferenceTransport {
    fn get(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
    ) -> Result<HttpResponse, GfError> {
        self.read(url, range, if_range, limit, &[])
    }
    fn get_discovery(
        &self,
        url: &Url,
        headers: &[(&str, ureq::http::HeaderValue)],
        limit: u64,
    ) -> Result<HttpResponse, GfError> {
        self.read(url, None, None, limit, headers)
    }
    fn get_authorized(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
        headers: &[(&str, ureq::http::HeaderValue)],
    ) -> Result<HttpResponse, GfError> {
        self.read(url, range, if_range, limit, headers)
    }
    fn exchange(&self, request: &HubRequest, limit: usize) -> Result<HubResponse, GfError> {
        self.response(request, limit)
    }
}

pub(crate) fn dispatch(
    command: Command,
    project: Option<&Path>,
    project_dir: Option<&Path>,
    json: bool,
    output: &mut dyn Write,
    diagnostics: &mut crate::Diagnostics<'_>,
    allocation: Option<&graphforge_api::StorageAllocationDiagnostics>,
) -> Result<ControlFlow<(), Command>, GfError> {
    let hub = FIXTURE.with(|fixture| fixture.borrow().clone());
    let Some(hub) = hub else {
        return Ok(ControlFlow::Continue(command));
    };
    if !matches!(
        &command,
        Command::Clone(_) | Command::Push(_) | Command::Pull(_)
    ) && !matches!(&command, Command::Log(args) if args.remote.is_some())
    {
        return Ok(ControlFlow::Continue(command));
    }
    let transport = ReferenceTransport(hub);
    if let Command::Clone(args) = command {
        crate::hub_clone::run_clone_with(&transport, args, json, output)?;
    } else {
        let path = crate::resolve_project_path(
            project.map(Path::to_path_buf),
            project_dir.map(Path::to_path_buf),
        )?;
        let mut graph = crate::open_cli_graph_warning(&path, allocation, diagnostics)?;
        crate::porcelain::remote::run_remote_with(
            &transport,
            &|hub| crate::hub_publish::publish_token(hub.token_env.as_deref()),
            &mut graph,
            command,
            json,
            output,
        )?;
    }
    Ok(ControlFlow::Break(()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activation_is_explicit_thread_local_and_replacement_drops_old_state() {
        stop();
        assert!(FIXTURE.with(|fixture| fixture.borrow().is_none()));
        let _token = start();
        let first = FIXTURE.with(|fixture| Arc::downgrade(fixture.borrow().as_ref().unwrap()));
        std::thread::spawn(|| {
            assert!(FIXTURE.with(|fixture| fixture.borrow().is_none()));
        })
        .join()
        .unwrap();
        let _token = start();
        assert!(first.upgrade().is_none());
        let second = FIXTURE.with(|fixture| Arc::downgrade(fixture.borrow().as_ref().unwrap()));
        stop();
        assert!(second.upgrade().is_none());
    }

    #[test]
    fn fixture_keeps_product_url_policy_and_never_routes_other_origins() {
        let hub = Arc::new(ReferenceHub::default());
        let transport = ReferenceTransport(hub);
        for url in [
            "http://hub.test/path",
            "https://127.0.0.1/path",
            "https://elsewhere.test/path",
        ] {
            assert!(
                transport
                    .response(&HubRequest::new(HubMethod::Get, url), 1024)
                    .is_err()
            );
        }
    }
}
