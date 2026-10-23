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
pub(super) struct StatusServer {
    pub(super) address: SocketAddr,
    paths: Arc<Mutex<Vec<String>>>,
}

impl StatusServer {
    pub(super) fn start(script: Vec<(u16, String, Vec<u8>)>) -> Self {
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
                // A script that declares its own Content-Length can promise more
                // bytes than it sends, which cuts the body short.
                let length = if headers.contains("Content-Length:") {
                    String::new()
                } else {
                    format!("Content-Length: {}\r\n", body.len())
                };
                write!(
                    stream,
                    "HTTP/1.1 {status} Scripted\r\n{length}{headers}Connection: close\r\n\r\n"
                )
                .unwrap();
                stream.write_all(&body).unwrap();
            }
        });
        Self { address, paths }
    }

    pub(super) fn paths(&self) -> Vec<String> {
        self.paths.lock().unwrap().clone()
    }
}

/// The production read transport and URL validation; only the connection is
/// re-targeted from `https://hub.test` to the loopback server.
pub(super) struct Routed {
    inner: HttpTransport,
    address: SocketAddr,
}

impl Routed {
    pub(super) fn new(address: SocketAddr) -> Self {
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
    crate::write_error(&error, true, &mut stderr, &[]).unwrap();
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
        // The HTTP-date form, and a delta too large for any integer, ask for
        // a wait beyond the cap; a date already past asks for none.
        (
            429,
            "Retry-After: Fri, 01 Jan 2100 00:00:00 GMT\r\n",
            "hub.rate_limited",
            "GF_IO",
            3,
            1,
        ),
        (
            429,
            "Retry-After: Fri, 01 Jan 18446744073709551615 00:00:00 GMT\r\n",
            "hub.rate_limited",
            "GF_IO",
            3,
            retries,
        ),
        (
            429,
            "Retry-After: 99999999999999999999999\r\n",
            "hub.rate_limited",
            "GF_IO",
            3,
            1,
        ),
        (
            429,
            "Retry-After: Sun, 06 Nov 1994 08:49:37 GMT\r\n",
            "hub.rate_limited",
            "GF_IO",
            3,
            retries,
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
        // Only a header that asks for a wait is reported; a date already past
        // (1994) asks for none.
        let asked = !headers.is_empty()
            && !headers.contains("1994")
            && !headers.contains("18446744073709551615 00:00");
        assert_eq!(message.contains("retry after"), asked, "{message}");
        if headers.contains("120") {
            assert!(message.contains("retry after 120 s"), "{message}");
        }
    }
    // Module fetch reads through the same path.
    let server = StatusServer::start(failing(403, "", 1));
    let error = run_module_fetch_with(
        &Routed::new(server.address),
        &ModuleFetchArgs {
            repository: "curate/claims".into(),
            ontology_id: Some("https://example.org/ontology".into()),
            version: Some("1".into()),
            digest: Some("a".repeat(64)),
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
    let (report, digest) = download_with_progress(
        &Routed::new(server.address),
        &descriptor,
        &partial,
        &mut DownloadReport::default(),
        &mut DownloadControl::quiet(&PACED),
    )
    .unwrap();
    assert_eq!(report.attempts, 2);
    assert_eq!(digest, descriptor.digest.0);
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

#[test]
fn retry_after_reads_delta_seconds_and_every_http_date_form() {
    use crate::hub_http::retry_after_seconds;
    // 1994-11-06T08:49:37Z, the RFC 9110 example instant.
    let now = 784_111_777;
    for (value, expected) in [
        ("120", Some(120)),
        (" 7 ", Some(7)),
        ("12345678901", Some(12_345_678_901)),
        ("18446744073709551615", Some(u64::MAX)),
        ("18446744073709551616", Some(u64::MAX)),
        ("99999999999999999999999", Some(u64::MAX)),
        ("Sun, 06 Nov 1994 08:49:47 GMT", Some(10)),
        ("Sunday, 06-Nov-94 08:50:37 GMT", Some(60)),
        ("Sun Nov  6 08:49:57 1994", Some(20)),
        // A year no calendar holds must not overflow the day count.
        ("Fri, 01 Jan 18446744073709551615 00:00:00 GMT", None),
        ("Fri, 01 Jan 10000 00:00:00 GMT", None),
        ("Fri, 01 Jan 1969 00:00:00 GMT", None),
        ("Sun, 06 Nov 1994 08:49:37 GMT", None),
        ("Sat, 05 Nov 1994 08:49:37 GMT", None),
        ("Mon, 32 Nov 1994 08:49:47 GMT", None),
        ("Sun, 06 Foo 1994 08:49:47 GMT", None),
        // Invalid calendar dates, a missing zone and an unknown weekday.
        ("Wed, 31 Feb 2027 00:00:00 GMT", None),
        ("Wed, 30 Feb 2027 00:00:00 GMT", None),
        ("Mon, 29 Feb 2027 00:00:00 GMT", None),
        ("Tue, 31 Apr 2027 00:00:00 GMT", None),
        ("Mon, 06 Nov 2028 08:49:37 PST", None),
        ("Mon, 06 Nov 2028 08:49:37", None),
        ("Fun, 06 Nov 2028 08:49:37 GMT", None),
        ("Sun Nov  6 08:49:37 GMT 2028", None),
        // A leap day exists in a leap year.
        (
            "Tue, 29 Feb 2028 00:00:00 GMT",
            Some(1_835_395_200 - 784_111_777),
        ),
        ("soon", None),
        ("", None),
        ("-5", None),
    ] {
        assert_eq!(retry_after_seconds(value, now), expected, "{value:?}");
    }
}

#[test]
fn a_partial_download_that_meets_repeated_503s_keeps_its_retry_class() {
    let bytes = b"verified portable bytes".to_vec();
    let mut descriptor = super::tests::object(&bytes);
    descriptor.locations = vec!["https://hub.test/objects/project.gfpb".into()];
    let declared = format!("Content-Length: {}\r\nETag: \"fixture-1\"\r\n", bytes.len());
    // Ten bytes arrive, the connection ends, and the Hub then stays unavailable.
    let mut script = vec![(200, declared, bytes[..10].to_vec())];
    script.extend(failing(503, "", TEST_RETRY_POLICY.attempts as usize));
    let server = StatusServer::start(script);
    let root = tempfile::tempdir().unwrap();
    let partial = root.path().join("package.part");
    let error = download_with_progress(
        &Routed::new(server.address),
        &descriptor,
        &partial,
        &mut DownloadReport::default(),
        &mut DownloadControl::quiet(&TEST_RETRY_POLICY),
    )
    .unwrap_err();
    assert_eq!(std::fs::read(&partial).unwrap(), &bytes[..10]);
    let message = error.to_string();
    let (json, exit_code) = rendered(error);
    assert_eq!(json["error"]["details"]["semantic_code"], "hub.unavailable");
    assert_eq!(json["error"]["code"], "GF_IO");
    assert_eq!(exit_code, 3);
    assert!(message.contains("rerun to resume"), "{message}");
    assert!(!message.contains(HUB_BODY), "{message}");
}

/// RFC 9110 §5.6.7: a two-digit year more than 50 years ahead is read as the
/// most recent past year with the same last two digits.
#[test]
fn a_two_digit_rfc_850_year_follows_the_fifty_year_rule_from_now() {
    use crate::hub_http::retry_after_seconds;
    // 2026-10-20T00:00:00Z
    let now = 1_792_454_400;
    let at = |value: &str| retry_after_seconds(value, now);
    // 2070-11-06T08:50:37Z is within 50 years of now.
    assert_eq!(
        at("Thursday, 06-Nov-70 08:50:37 GMT"),
        Some(3_182_489_437 - now)
    );
    // The limit is now's civil date plus 50 years, 2076-10-20T00:00:00Z: that
    // instant is not "more than" 50 years ahead, so it is 2076.
    assert_eq!(
        at("Tuesday, 20-Oct-76 00:00:00 GMT"),
        Some(3370377600 - now)
    );
    // A second later, or a day later, is in 1976 and so in the past; so is
    // 2076-11-06.
    assert_eq!(at("Tuesday, 20-Oct-76 00:00:01 GMT"), None);
    assert_eq!(at("Wednesday, 21-Oct-76 00:00:00 GMT"), None);
    assert_eq!(at("Thursday, 06-Nov-76 08:50:37 GMT"), None);
    // 2077 would be 51 years ahead, so it is 1977: in the past, no wait.
    assert_eq!(at("Thursday, 06-Nov-77 08:50:37 GMT"), None);
    // 2069 and 2000 are both in range; 2025 is the past.
    assert_eq!(at("Thursday, 06-Nov-25 08:50:37 GMT"), None);
    assert!(at("Thursday, 06-Nov-69 08:50:37 GMT").is_some());
}
