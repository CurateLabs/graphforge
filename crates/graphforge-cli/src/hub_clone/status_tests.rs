//! Unsuccessful Hub statuses reach `gf clone` and `gf ontology module fetch`
//! as distinct semantic codes and exit classes, through the real HTTP read
//! transport, and `Retry-After` paces the bounded in-process retry.

use super::module_fetch::{ModuleFetchArgs, run_module_fetch_with};
use super::*;
use crate::hub_http::{ReadTimeouts, read_agent};
use std::io::{BufRead as _, BufReader};
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use ureq::unversioned::resolver::DefaultResolver;

/// Body every unsuccessful response carries; no error may echo it.
const HUB_BODY: &str = "untrusted hub body";

/// A loopback server answering each request with the next scripted response
/// (status, extra header lines, body) and recording the request paths.
struct StatusServer {
    address: SocketAddr,
    paths: Arc<Mutex<Vec<String>>>,
}

impl StatusServer {
    fn start(script: Vec<(u16, String, Vec<u8>)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let paths = Arc::new(Mutex::new(Vec::new()));
        let recorded = paths.clone();
        std::thread::spawn(move || {
            for (status, headers, body) in script {
                let (mut stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                recorded
                    .lock()
                    .unwrap()
                    .push(line.split_whitespace().nth(1).unwrap_or("").to_owned());
                loop {
                    let mut header = String::new();
                    reader.read_line(&mut header).unwrap();
                    if header == "\r\n" || header.is_empty() {
                        break;
                    }
                }
                write!(
                    stream,
                    "HTTP/1.1 {status} Scripted\r\nContent-Length: {}\r\n{headers}Connection: close\r\n\r\n",
                    body.len()
                )
                .unwrap();
                stream.write_all(&body).unwrap();
            }
        });
        Self { address, paths }
    }

    fn paths(&self) -> Vec<String> {
        self.paths.lock().unwrap().clone()
    }
}

/// The production read transport and URL validation; only the connection is
/// re-targeted from `https://hub.test` to the loopback server.
struct Routed {
    inner: HttpTransport,
    address: SocketAddr,
}

impl Routed {
    fn new(address: SocketAddr) -> Self {
        let bound = Duration::from_secs(5);
        Self {
            inner: HttpTransport {
                agent: read_agent(
                    false,
                    &ReadTimeouts {
                        connect: bound,
                        response: bound,
                        idle: bound,
                    },
                    DefaultResolver::default(),
                ),
            },
            address,
        }
    }
}

impl Transport for Routed {
    fn get(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
    ) -> Result<HttpResponse, graphforge_api::GfError> {
        assert_eq!(url.host_str(), Some("hub.test"), "{url}");
        let mut routed = Url::parse(&format!("http://{}", self.address)).unwrap();
        routed.set_path(url.path());
        self.inner.get(&routed, range, if_range, limit)
    }
}

/// Render `error` as the CLI does: the JSON error and the process exit code.
fn rendered(error: graphforge_api::GfError) -> (serde_json::Value, i32) {
    let mut stderr = Vec::new();
    crate::write_error(&error, true, &mut stderr).unwrap();
    let exit = crate::error_exit_code(&crate::CliRuntimeError::Core(error));
    (serde_json::from_slice(&stderr).unwrap(), exit)
}

fn failing(status: u16, headers: &str, requests: usize) -> Vec<(u16, String, Vec<u8>)> {
    (0..requests)
        .map(|_| (status, headers.to_owned(), HUB_BODY.as_bytes().to_vec()))
        .collect()
}

#[test]
fn each_hub_status_class_has_its_semantic_code_and_exit_class_over_real_http() {
    let retries = TEST_RETRY_POLICY.attempts as usize;
    // (status, headers, semantic code, public code, exit, requests made)
    let cases = [
        (404, "", "hub.not_found", "GF_VALIDATION", 2, 1),
        (410, "", "hub.not_found", "GF_VALIDATION", 2, 1),
        (401, "", "hub.access_denied", "GF_VALIDATION", 2, 1),
        (403, "", "hub.access_denied", "GF_VALIDATION", 2, 1),
        (
            429,
            "Retry-After: 120\r\n",
            "hub.rate_limited",
            "GF_IO",
            3,
            1,
        ),
        (429, "", "hub.rate_limited", "GF_IO", 3, retries),
        (503, "", "hub.unavailable", "GF_IO", 3, retries),
        (502, "", "hub.unavailable", "GF_IO", 3, retries),
        (500, "", "hub.unexpected_status", "GF_VALIDATION", 2, 1),
        (400, "", "hub.unexpected_status", "GF_VALIDATION", 2, 1),
    ];
    let root = tempfile::tempdir().unwrap();
    for (index, (status, headers, semantic, code, exit, requests)) in cases.into_iter().enumerate()
    {
        let server = StatusServer::start(failing(status, headers, requests));
        let destination = root.path().join(format!("clone-{index}"));
        let error = run_clone_with(
            &Routed::new(server.address),
            CloneArgs {
                repository: "https://hub.test/curate/claims".into(),
                hub: None,
                token_env: None,
                protection_bypass_env: None,
                destination: Some(destination.clone()),
                telemetry_endpoint: None,
                git_ref: None,
                version_uuid: None,
            },
            true,
            &mut Vec::new(),
        )
        .unwrap_err();
        let message = error.to_string();
        let (json, exit_code) = rendered(error);
        assert_eq!(
            json["error"]["details"]["semantic_code"], semantic,
            "{status}"
        );
        assert_eq!(json["error"]["code"], code, "{status}");
        assert_eq!(exit_code, exit, "{status}");
        assert!(
            message.contains(&format!("HTTP status {status}")),
            "{message}"
        );
        assert!(!message.contains(HUB_BODY), "{message}");
        assert_eq!(
            server.paths(),
            vec!["/curate/claims/.gf/refs".to_owned(); requests],
            "{status}"
        );
        assert!(!destination.exists());
        if headers.is_empty() {
            assert!(!message.contains("retry after"), "{message}");
        } else {
            assert!(message.contains("retry after 120 s"), "{message}");
        }
    }
    // Module fetch reads through the same path.
    let server = StatusServer::start(failing(403, "", 1));
    let error = run_module_fetch_with(
        &Routed::new(server.address),
        &ModuleFetchArgs {
            repository: "curate/claims".into(),
            ontology_id: "https://example.org/ontology".into(),
            version: "1".into(),
            digest: "a".repeat(64),
            output: root.path().join("module.json"),
            hub: Some("https://hub.test".into()),
        },
        true,
        &mut Vec::new(),
        &TEST_RETRY_POLICY,
    )
    .unwrap_err();
    let (json, exit_code) = rendered(error);
    assert_eq!(
        json["error"]["details"]["semantic_code"],
        "hub.access_denied"
    );
    assert_eq!(exit_code, 2);
}

/// A policy whose single wait may last two seconds and whose backoff is zero,
/// so any measured pause comes from `Retry-After`.
const PACED: RetryPolicy = RetryPolicy {
    attempts: 3,
    initial_backoff: Duration::ZERO,
    max_backoff: Duration::from_secs(2),
};

#[test]
fn retry_after_paces_the_bounded_retry_of_documents_and_objects() {
    // A discovery document: one 429 asking for a second, then the document.
    let server = StatusServer::start(vec![
        (429, "Retry-After: 1\r\n".into(), Vec::new()),
        (200, String::new(), b"refs".to_vec()),
    ]);
    let started = Instant::now();
    let mut attempts = 0;
    let response = fetch_with_attempts(
        &Routed::new(server.address),
        &Url::parse("https://hub.test/curate/claims/.gf/refs").unwrap(),
        None,
        None,
        1024,
        &mut attempts,
        &PACED,
    )
    .unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(attempts, 2);
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );

    // A package object: one 503 asking for a second, then the object.
    let bytes = b"verified portable bytes".to_vec();
    let mut descriptor = super::tests::object(&bytes);
    descriptor.locations = vec!["https://hub.test/objects/project.gfpb".into()];
    let server = StatusServer::start(vec![
        (503, "Retry-After: 1\r\n".into(), Vec::new()),
        (200, "ETag: \"fixture-1\"\r\n".into(), bytes.clone()),
    ]);
    let root = tempfile::tempdir().unwrap();
    let partial = root.path().join("package.part");
    let started = Instant::now();
    let report = download_with_progress(
        &Routed::new(server.address),
        &descriptor,
        &partial,
        &mut DownloadReport::default(),
        &mut DownloadControl::quiet(&PACED),
    )
    .unwrap();
    assert_eq!(report.attempts, 2);
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(std::fs::read(&partial).unwrap(), bytes);

    // A pause longer than one wait may last is reported, not waited out.
    let server = StatusServer::start(vec![(503, "Retry-After: 3\r\n".into(), Vec::new())]);
    let started = Instant::now();
    let error = download_with_progress(
        &Routed::new(server.address),
        &descriptor,
        &root.path().join("other.part"),
        &mut DownloadReport::default(),
        &mut DownloadControl::quiet(&PACED),
    )
    .unwrap_err();
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    assert!(
        error
            .to_string()
            .contains("hub.unavailable: Hub returned HTTP status 503; retry after 3 s"),
        "{error}"
    );
}
