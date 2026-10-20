use super::*;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Instant;

fn read_request(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut byte = [0];
    while !bytes.ends_with(b"\r\n\r\n") {
        assert_eq!(stream.read(&mut byte).unwrap(), 1);
        bytes.push(byte[0]);
        assert!(bytes.len() < 64 * 1024);
    }
    String::from_utf8(bytes).unwrap()
}

fn response_server(response: Vec<u8>) -> (String, thread::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let host = listener.local_addr().unwrap().to_string();
    let thread = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let request = read_request(&mut stream);
        // Limit tests may close the connection before the full body is sent.
        let _ = stream.write_all(&response);
        request
    });
    (host, thread)
}

#[test]
fn foreign_upload_location_never_receives_credentials() {
    let sentinel = TcpListener::bind("127.0.0.1:0").unwrap();
    let sentinel_host = sentinel.local_addr().unwrap().to_string();
    let sentinel_worker = thread::spawn(move || {
        let mut requests = Vec::new();
        loop {
            let (mut stream, _) = sentinel.accept().unwrap();
            let request = read_request(&mut stream);
            stream
                .write_all(
                    b"HTTP/1.1 201 Created\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            if request.starts_with("GET /stop ") {
                break;
            }
            requests.push(request);
        }
        requests
    });
    let origin_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let host = origin_listener.local_addr().unwrap().to_string();
    let location = format!("http://{sentinel_host}/upload");
    let origin = thread::spawn(move || {
        let (mut head, _) = origin_listener.accept().unwrap();
        assert!(read_request(&mut head).starts_with("HEAD "));
        head.write_all(b"HTTP/1.1 404 Missing\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .unwrap();
        drop(head);
        let (mut post, _) = origin_listener.accept().unwrap();
        let request = read_request(&mut post);
        assert!(request.starts_with("POST /v2/repo/blobs/uploads/ "));
        post.write_all(format!("HTTP/1.1 202 Accepted\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).unwrap();
        request
    });
    let client = HttpOciRegistry::new(&host, Some("private-token"), true).unwrap();
    let error = client
        .put_blob("repo", &digest_sha256(b"data"), b"data")
        .unwrap_err();
    assert_eq!(error.code, PortableV2ErrorCode::InvalidPath);
    assert!(!error.to_string().contains("private-token"));
    assert!(
        origin
            .join()
            .unwrap()
            .to_ascii_lowercase()
            .contains("authorization: bearer private-token")
    );
    let mut stop = TcpStream::connect(sentinel_host).unwrap();
    stop.write_all(b"GET /stop HTTP/1.1\r\nHost: local\r\n\r\n")
        .unwrap();
    assert!(sentinel_worker.join().unwrap().is_empty());
}

#[test]
fn bounded_manifest_and_blob_responses_keep_typed_errors() {
    for manifest in [true, false] {
        let body = vec![b'x'; MAX_RESPONSE_BYTES as usize + 1];
        let mut response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend(body);
        let (host, server) = response_server(response);
        let client = HttpOciRegistry::new(&host, None, true).unwrap();
        let result = if manifest {
            client
                .get_manifest("repo", "latest")
                .map(|(_, bytes)| bytes)
        } else {
            client.get_blob("repo", &digest_sha256(b"data"))
        };
        assert_eq!(result.unwrap_err().code, PortableV2ErrorCode::Io);
        server.join().unwrap();
    }
}

#[test]
fn redirects_and_error_bodies_are_not_returned_or_followed() {
    for status in [302, 401, 500] {
        let (host, server) = response_server(format!("HTTP/1.1 {status} Rejected\r\nLocation: http://invalid.example/secret\r\nContent-Length: 19\r\nConnection: close\r\n\r\nsecret server body!").into_bytes());
        let client = HttpOciRegistry::new(&host, Some("user:secret"), true).unwrap();
        let error = client.get_manifest("repo", "latest").unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::InvalidStructure);
        assert!(!format!("{error:?}").contains("secret"));
        server.join().unwrap();
    }
}

#[test]
fn malformed_truncated_and_digest_mismatched_downloads_are_typed() {
    for (body, length, expected) in [
        (b"bad".as_slice(), 3, PortableV2ErrorCode::DigestMismatch),
        (b"bad".as_slice(), 10, PortableV2ErrorCode::Io),
    ] {
        let mut response =
            format!("HTTP/1.1 200 OK\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n")
                .into_bytes();
        response.extend(body);
        let (host, server) = response_server(response);
        let client = HttpOciRegistry::new(&host, None, true).unwrap();
        assert_eq!(
            client
                .get_blob("repo", &digest_sha256(b"good"))
                .unwrap_err()
                .code,
            expected
        );
        server.join().unwrap();
    }
}

#[test]
fn credentials_map_to_the_expected_authorization_header() {
    for (credential, expected) in [
        (None, None),
        (Some(""), None),
        (Some("   "), None),
        (Some("\t\n "), None),
        (Some("plain-token"), Some("Bearer plain-token".to_owned())),
        (
            Some("user:secret"),
            Some("Basic dXNlcjpzZWNyZXQ=".to_owned()),
        ),
    ] {
        let client = HttpOciRegistry::new("registry.invalid", credential, true).unwrap();
        assert_eq!(client.auth_header(), expected);
    }
}

#[test]
fn an_empty_credential_sends_no_authorization_header() {
    let body = b"data";
    let mut response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(body);
    let (host, server) = response_server(response);
    let client = HttpOciRegistry::new(&host, Some(""), true).unwrap();
    assert_eq!(client.get_blob("repo", &digest_sha256(body)).unwrap(), body);
    let request = server.join().unwrap().to_ascii_lowercase();
    assert!(!request.contains("authorization"));
}

#[test]
fn control_character_credentials_are_refused_by_the_message_they_report() {
    for credential in [
        "token\nsecond",
        "token\rsecond",
        "token\tsecond",
        "token\u{7}",
    ] {
        let Err(error) = HttpOciRegistry::new("registry.invalid", Some(credential), true) else {
            panic!("a control character must be refused");
        };
        assert_eq!(error.code, PortableV2ErrorCode::InvalidPath);
        assert!(error.to_string().contains("control characters"));
    }
    assert!(HttpOciRegistry::new("registry.invalid", Some("token-second"), true).is_ok());
}

/// What the loopback registry should do to the next matching request.
#[derive(Clone, Copy, Default)]
struct Faults {
    /// Close the connection halfway through the n-th (1-based) PATCH body.
    drop_patch: usize,
    /// Close the connection halfway through the n-th (1-based) ranged GET body.
    drop_range: usize,
    /// Answer ranged GETs with the whole blob and 200.
    ignore_range: bool,
    /// Sleep before answering every PATCH and ranged GET.
    delay: Duration,
}

#[derive(Default)]
struct LoopbackState {
    blobs: BTreeMap<String, Vec<u8>>,
    uploads: BTreeMap<usize, Vec<u8>>,
    patches: Vec<(String, usize)>,
    ranges: Vec<String>,
    finalized: usize,
}

/// A loopback OCI registry implementing blob HEAD, upload sessions with
/// PATCH chunks, ranged GET and digest-verified finalize.
struct LoopbackRegistry {
    host: String,
    state: Arc<Mutex<LoopbackState>>,
    worker: Option<thread::JoinHandle<()>>,
}

impl LoopbackRegistry {
    fn start(faults: Faults) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let host = listener.local_addr().unwrap().to_string();
        let state = Arc::new(Mutex::new(LoopbackState::default()));
        let shared = Arc::clone(&state);
        let worker = thread::spawn(move || {
            let (mut patch_count, mut range_count) = (0, 0);
            loop {
                let (mut stream, _) = listener.accept().unwrap();
                let request = read_request(&mut stream);
                let mut first = request.lines().next().unwrap().split_whitespace();
                let (method, path) = (first.next().unwrap(), first.next().unwrap().to_owned());
                if path == "/stop" {
                    return;
                }
                let header = |name: &str| {
                    request.lines().find_map(|line| {
                        line.split_once(':')
                            .filter(|(key, _)| key.eq_ignore_ascii_case(name))
                            .map(|(_, value)| value.trim().to_owned())
                    })
                };
                let length: usize = header("content-length")
                    .map_or(0, |value| value.parse().unwrap());
                let respond = |stream: &mut TcpStream, status: &str, extra: &str, body: &[u8]| {
                    let _ = write!(
                        stream,
                        "HTTP/1.1 {status}\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(body);
                };
                let mut state = shared.lock().unwrap();
                if let Some(digest) = path.strip_prefix("/v2/repo/blobs/") {
                    if digest == "uploads/" {
                        let id = state.uploads.len();
                        state.uploads.insert(id, Vec::new());
                        respond(
                            &mut stream,
                            "202 Accepted",
                            &format!("Location: /upload/{id}\r\n"),
                            &[],
                        );
                    } else if method == "HEAD" {
                        let status = if state.blobs.contains_key(digest) {
                            "200 OK"
                        } else {
                            "404 Not Found"
                        };
                        respond(&mut stream, status, "", &[]);
                    } else if let Some(blob) = state.blobs.get(digest).cloned() {
                        let range = header("range");
                        let Some(range) = range.filter(|_| !faults.ignore_range) else {
                            respond(&mut stream, "200 OK", "", &blob);
                            continue;
                        };
                        range_count += 1;
                        state.ranges.push(range.clone());
                        thread::sleep(faults.delay);
                        let (first, last) = range
                            .strip_prefix("bytes=")
                            .unwrap()
                            .split_once('-')
                            .unwrap();
                        let (first, last): (usize, usize) =
                            (first.parse().unwrap(), last.parse().unwrap());
                        let part = &blob[first..=last.min(blob.len() - 1)];
                        let extra = format!(
                            "Content-Range: bytes {first}-{}/{}\r\n",
                            first + part.len() - 1,
                            blob.len()
                        );
                        if range_count == faults.drop_range {
                            let _ = write!(
                                stream,
                                "HTTP/1.1 206 Partial Content\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n",
                                part.len()
                            );
                            let _ = stream.write_all(&part[..part.len() / 2]);
                            continue;
                        }
                        respond(&mut stream, "206 Partial Content", &extra, part);
                    } else {
                        respond(&mut stream, "404 Not Found", "", &[]);
                    }
                } else if let Some(rest) = path.strip_prefix("/upload/") {
                    let (id, query) = rest.split_once('?').unwrap_or((rest, ""));
                    let id: usize = id.parse().unwrap();
                    let mut body = vec![0; length];
                    if method == "PATCH" {
                        patch_count += 1;
                        if patch_count == faults.drop_patch {
                            let _ = stream.read_exact(&mut body[..length / 2]);
                            continue;
                        }
                        if stream.read_exact(&mut body).is_err() {
                            continue;
                        }
                        thread::sleep(faults.delay);
                        let range = header("content-range").unwrap();
                        let offset = state.uploads[&id].len();
                        assert!(
                            range.starts_with(&format!("{offset}-")),
                            "chunk {range} is not contiguous at {offset}"
                        );
                        state.patches.push((range, length));
                        state.uploads.get_mut(&id).unwrap().extend(body);
                        respond(
                            &mut stream,
                            "202 Accepted",
                            &format!("Location: /upload/{id}\r\n"),
                            &[],
                        );
                    } else {
                        if stream.read_exact(&mut body).is_err() {
                            continue;
                        }
                        let mut blob = state.uploads.remove(&id).unwrap();
                        blob.extend(body);
                        let digest = query.strip_prefix("digest=").unwrap().to_owned();
                        if digest_sha256(&blob) == digest {
                            state.blobs.insert(digest, blob);
                            state.finalized += 1;
                            respond(&mut stream, "201 Created", "", &[]);
                        } else {
                            respond(&mut stream, "400 Bad Request", "", &[]);
                        }
                    }
                } else {
                    panic!("unexpected loopback route {method} {path}");
                }
            }
        });
        Self {
            host,
            state,
            worker: Some(worker),
        }
    }

    fn client(&self, transfer: HttpOciTransfer) -> HttpOciRegistry {
        HttpOciRegistry::with_transfer(&self.host, None, true, transfer).unwrap()
    }
}

impl Drop for LoopbackRegistry {
    fn drop(&mut self) {
        let mut stop = TcpStream::connect(&self.host).unwrap();
        stop.write_all(b"GET /stop HTTP/1.1\r\nHost: local\r\n\r\n")
            .unwrap();
        self.worker.take().unwrap().join().unwrap();
    }
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i.wrapping_mul(31) >> 3) as u8).collect()
}

fn drain(reader: &mut dyn Read) -> std::io::Result<(String, u64)> {
    digest_reader(&mut { reader }, 64 * 1024)
}

const SMALL_CHUNKS: HttpOciTransfer = HttpOciTransfer {
    chunk_bytes: 64 * 1024,
    request_timeout: Duration::from_secs(30),
};

#[test]
fn a_layer_larger_than_the_response_bound_streams_through_http() {
    let layer = pattern(MAX_RESPONSE_BYTES as usize + 3 * 1024 * 1024 + 17);
    let digest = digest_sha256(&layer);
    let server = LoopbackRegistry::start(Faults::default());
    let chunk = 1024 * 1024;
    let client = server.client(HttpOciTransfer {
        chunk_bytes: chunk,
        ..HttpOciTransfer::default()
    });
    client
        .put_blob_stream("repo", &digest, layer.len() as u64, &mut layer.as_slice())
        .unwrap();
    let chunks = layer.len().div_ceil(chunk as usize);
    assert_eq!(server.state.lock().unwrap().patches.len(), chunks);
    assert_eq!(server.state.lock().unwrap().blobs[&digest], layer);

    let mut stream = client
        .get_blob_stream("repo", &digest, layer.len() as u64)
        .unwrap();
    assert_eq!(
        drain(&mut stream).unwrap(),
        (digest.clone(), layer.len() as u64)
    );
    assert_eq!(server.state.lock().unwrap().ranges.len(), chunks);
    // The whole-document bound still guards small JSON documents.
    assert_eq!(
        client.get_blob("repo", &digest).unwrap_err().code,
        PortableV2ErrorCode::Io
    );
}

#[test]
fn a_transfer_outliving_the_request_timeout_completes_while_it_progresses() {
    let layer = pattern(5 * 64 * 1024);
    let digest = digest_sha256(&layer);
    let delay = Duration::from_millis(600);
    let server = LoopbackRegistry::start(Faults {
        delay,
        ..Faults::default()
    });
    let timeout = Duration::from_secs(2);
    let client = server.client(HttpOciTransfer {
        request_timeout: timeout,
        ..SMALL_CHUNKS
    });
    let started = Instant::now();
    client
        .put_blob_stream("repo", &digest, layer.len() as u64, &mut layer.as_slice())
        .unwrap();
    let mut stream = client
        .get_blob_stream("repo", &digest, layer.len() as u64)
        .unwrap();
    assert_eq!(drain(&mut stream).unwrap().0, digest);
    // Each request stays within the timeout; the transfer as a whole does not.
    assert!(started.elapsed() > timeout);
}

#[test]
fn a_single_request_slower_than_the_timeout_still_fails() {
    let layer = pattern(64 * 1024);
    let digest = digest_sha256(&layer);
    let server = LoopbackRegistry::start(Faults {
        delay: Duration::from_millis(1500),
        ..Faults::default()
    });
    let client = server.client(HttpOciTransfer {
        request_timeout: Duration::from_secs(1),
        ..SMALL_CHUNKS
    });
    assert_eq!(
        client
            .put_blob_stream("repo", &digest, layer.len() as u64, &mut layer.as_slice())
            .unwrap_err()
            .code,
        PortableV2ErrorCode::Io
    );
    assert!(!server.state.lock().unwrap().blobs.contains_key(&digest));
}

#[test]
fn an_interrupted_push_admits_no_blob_and_a_rerun_completes() {
    let layer = pattern(4 * 64 * 1024 + 5);
    let digest = digest_sha256(&layer);
    let server = LoopbackRegistry::start(Faults {
        drop_patch: 3,
        ..Faults::default()
    });
    let client = server.client(SMALL_CHUNKS);
    let size = layer.len() as u64;
    assert_eq!(
        client
            .put_blob_stream("repo", &digest, size, &mut layer.as_slice())
            .unwrap_err()
            .code,
        PortableV2ErrorCode::Io
    );
    assert!(!client.blob_exists("repo", &digest).unwrap());
    assert_eq!(server.state.lock().unwrap().finalized, 0);
    client
        .put_blob_stream("repo", &digest, size, &mut layer.as_slice())
        .unwrap();
    assert_eq!(server.state.lock().unwrap().blobs[&digest], layer);
}

#[test]
fn an_interrupted_pull_fails_typed_and_a_rerun_completes() {
    let layer = pattern(4 * 64 * 1024 + 5);
    let digest = digest_sha256(&layer);
    let server = LoopbackRegistry::start(Faults {
        drop_range: 3,
        ..Faults::default()
    });
    let client = server.client(SMALL_CHUNKS);
    let size = layer.len() as u64;
    client
        .put_blob_stream("repo", &digest, size, &mut layer.as_slice())
        .unwrap();
    let mut stream = client.get_blob_stream("repo", &digest, size).unwrap();
    assert!(drain(&mut stream).is_err());
    let mut stream = client.get_blob_stream("repo", &digest, size).unwrap();
    assert_eq!(drain(&mut stream).unwrap(), (digest, size));
}

#[test]
fn a_stream_that_does_not_match_its_digest_or_size_is_never_finalized() {
    let layer = pattern(3 * 64 * 1024);
    let digest = digest_sha256(&layer);
    let server = LoopbackRegistry::start(Faults::default());
    let client = server.client(SMALL_CHUNKS);
    let mut other = layer.clone();
    other[10] ^= 1;
    assert_eq!(
        client
            .put_blob_stream("repo", &digest, other.len() as u64, &mut other.as_slice())
            .unwrap_err()
            .code,
        PortableV2ErrorCode::DigestMismatch
    );
    assert_eq!(
        client
            .put_blob_stream("repo", &digest, layer.len() as u64 + 1, &mut layer.as_slice())
            .unwrap_err()
            .code,
        PortableV2ErrorCode::Io
    );
    assert_eq!(
        client
            .put_blob_stream("repo", &digest, layer.len() as u64 - 1, &mut layer.as_slice())
            .unwrap_err()
            .code,
        PortableV2ErrorCode::Io
    );
    assert_eq!(server.state.lock().unwrap().finalized, 0);
}

#[test]
fn a_registry_that_ignores_range_cannot_serve_a_multi_request_layer() {
    let layer = pattern(3 * 64 * 1024);
    let digest = digest_sha256(&layer);
    let server = LoopbackRegistry::start(Faults {
        ignore_range: true,
        ..Faults::default()
    });
    let client = server.client(SMALL_CHUNKS);
    let size = layer.len() as u64;
    client
        .put_blob_stream("repo", &digest, size, &mut layer.as_slice())
        .unwrap();
    assert_eq!(
        client
            .get_blob_stream("repo", &digest, size)
            .err()
            .map(|error| error.code),
        Some(PortableV2ErrorCode::Io)
    );
    // One request covers the whole layer, so a 200 answer is exact.
    let whole = server.client(HttpOciTransfer {
        chunk_bytes: size,
        ..SMALL_CHUNKS
    });
    let mut stream = whole.get_blob_stream("repo", &digest, size).unwrap();
    assert_eq!(drain(&mut stream).unwrap(), (digest, size));
}

#[test]
fn the_memory_registry_streams_layers_and_checks_size_and_digest() {
    let layer = pattern(200_000);
    let digest = digest_sha256(&layer);
    let registry = MemoryOciRegistry::default();
    let size = layer.len() as u64;
    registry
        .put_blob_stream("repo", &digest, size, &mut layer.as_slice())
        .unwrap();
    let mut stream = registry.get_blob_stream("repo", &digest, size).unwrap();
    assert_eq!(drain(&mut stream).unwrap(), (digest.clone(), size));
    assert_eq!(
        registry
            .put_blob_stream("repo", &digest, size - 1, &mut layer.as_slice())
            .unwrap_err()
            .code,
        PortableV2ErrorCode::Io
    );
    assert_eq!(
        registry
            .put_blob_stream("repo", &digest_sha256(b"x"), size, &mut layer.as_slice())
            .unwrap_err()
            .code,
        PortableV2ErrorCode::DigestMismatch
    );
}
