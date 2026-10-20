//! Real HTTP transport plus local durable facade parity.
use super::*;
use crate::portable::{PortableSelection, PortableV2ExportRequest, PortableV2ImportRequest};
use crate::{GraphForge, OperationId};
use arrow::array::Array;
use graphforge_storage::{PortableV2Output, PortableV2SelectionProfile};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

/// Faults for the loopback registry; `0` means never.
#[derive(Clone, Copy, Default)]
struct ServerFaults {
    /// Close the connection halfway through this (1-based) PATCH body.
    drop_patch: usize,
    /// Close the connection halfway through this (1-based) ranged GET body.
    drop_range: usize,
}

struct RegistryServer {
    host: String,
    worker: Option<thread::JoinHandle<()>>,
    registry: std::sync::Arc<MemoryOciRegistry>,
}
impl RegistryServer {
    fn start() -> Self {
        Self::start_with(ServerFaults::default())
    }

    fn start_with(faults: ServerFaults) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let host = listener.local_addr().unwrap().to_string();
        let registry = std::sync::Arc::new(MemoryOciRegistry::default());
        let served = std::sync::Arc::clone(&registry);
        let worker = thread::spawn(move || {
            let registry = served;
            let mut pending = Vec::new();
            let (mut patches, mut ranges) = (0, 0);
            loop {
                let (mut stream, _) = listener.accept().unwrap();
                let mut header = Vec::new();
                let mut byte = [0];
                while !header.ends_with(b"\r\n\r\n") {
                    assert_eq!(stream.read(&mut byte).unwrap(), 1);
                    header.push(byte[0]);
                    assert!(header.len() < 64 * 1024);
                }
                let header = String::from_utf8(header).unwrap();
                let mut line = header.lines().next().unwrap().split_whitespace();
                let method = line.next().unwrap();
                let path = line.next().unwrap();
                if path == "/stop" {
                    break;
                }
                let header_value = |name: &str| -> Option<String> {
                    header.lines().find_map(|line| {
                        line.split_once(':')
                            .filter(|(key, _)| key.eq_ignore_ascii_case(name))
                            .map(|(_, value)| value.trim().to_owned())
                    })
                };
                let length: usize = header_value("content-length")
                    .map(|value| value.parse().unwrap())
                    .unwrap_or(0);
                let mut body = vec![0; length];
                if method == "PATCH" {
                    patches += 1;
                    if patches == faults.drop_patch {
                        let _ = stream.read_exact(&mut body[..length / 2]);
                        continue;
                    }
                }
                if stream.read_exact(&mut body).is_err() {
                    continue;
                }
                let (status, extra, response) = if let Some(digest) =
                    path.strip_prefix("/v2/tests/portable/blobs/")
                {
                    if method == "HEAD" {
                        (
                            if registry.blob_exists("tests/portable", digest).unwrap() {
                                200
                            } else {
                                404
                            },
                            String::new(),
                            vec![],
                        )
                    } else if method == "POST" {
                        pending.clear();
                        (202, "Location: /upload\r\n".into(), vec![])
                    } else {
                        match registry.get_blob("tests/portable", digest) {
                            Ok(blob) => {
                                if let Some(range) = header_value("range") {
                                    ranges += 1;
                                    let (first, last) = range
                                        .strip_prefix("bytes=")
                                        .unwrap()
                                        .split_once('-')
                                        .unwrap();
                                    let first: usize = first.parse().unwrap();
                                    let last = last.parse::<usize>().unwrap().min(blob.len() - 1);
                                    let part = &blob[first..=last];
                                    let extra = format!(
                                        "Content-Range: bytes {first}-{last}/{}\r\n",
                                        blob.len()
                                    );
                                    if ranges == faults.drop_range {
                                        let _ = write!(
                                            stream,
                                            "HTTP/1.1 206 Partial\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n",
                                            part.len()
                                        );
                                        let _ = stream.write_all(&part[..part.len() / 2]);
                                        continue;
                                    }
                                    (206, extra, part.to_vec())
                                } else {
                                    (200, String::new(), blob)
                                }
                            }
                            Err(_) => (404, String::new(), vec![]),
                        }
                    }
                } else if path == "/upload" && method == "PATCH" {
                    pending.extend_from_slice(&body);
                    (202, "Location: /upload\r\n".into(), vec![])
                } else if let Some(digest) = path.strip_prefix("/upload?digest=") {
                    pending.extend_from_slice(&body);
                    registry
                        .put_blob("tests/portable", digest, &std::mem::take(&mut pending))
                        .unwrap();
                    (201, String::new(), vec![])
                } else if let Some(reference) = path.strip_prefix("/v2/tests/portable/manifests/") {
                    if method == "PUT" {
                        registry
                            .put_manifest(
                                "tests/portable",
                                reference,
                                OCI_MANIFEST_MEDIA_TYPE,
                                &body,
                            )
                            .unwrap();
                        (201, String::new(), vec![])
                    } else {
                        let (media, body) =
                            registry.get_manifest("tests/portable", reference).unwrap();
                        (200, format!("Content-Type: {media}\r\n"), body)
                    }
                } else {
                    panic!("unexpected fixture route {method} {path}")
                };
                write!(stream,"HTTP/1.1 {status} Response\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n",response.len()).unwrap();
                stream.write_all(&response).unwrap();
            }
        });
        Self {
            host,
            worker: Some(worker),
            registry,
        }
    }
}
impl Drop for RegistryServer {
    fn drop(&mut self) {
        let mut stream = TcpStream::connect(&self.host).unwrap();
        stream
            .write_all(b"GET /stop HTTP/1.1\r\nHost: local\r\n\r\n")
            .unwrap();
        self.worker.take().unwrap().join().unwrap();
    }
}

#[test]
fn http_and_memory_receipts_match_and_http_package_reopens_durably() {
    let dir = tempfile::tempdir().unwrap();
    let project = dir.path().join("source");
    let graph = GraphForge::new(project.to_str()).unwrap();
    graph.execute("CREATE (:Person {name:'Ada'})").unwrap();
    let package = dir.path().join("package.gfpb");
    graph
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: package.clone(),
                representation: PortableV2Output::Bundle,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits: PortableV2Limits::default(),
            },
            None,
            |_| {},
        )
        .unwrap();
    let server = RegistryServer::start();
    let memory = MemoryOciRegistry::default();
    let publish = PortableV2OciPublishRequest {
        package_path: &package,
        registry: &server.host,
        repository: "tests/portable",
        tag: Some("latest"),
        limits: PortableV2Limits::default(),
        authenticity: Default::default(),
        signature: None,
        credential: None,
    };
    let expected = crate::publish_portable_v2_oci_with_registry(&memory, &publish, None).unwrap();
    let actual = crate::publish_portable_v2_oci(
        &crate::PortableV2OciPublishFacadeRequest {
            package_path: package.clone(),
            registry: server.host.clone(),
            repository: "tests/portable".into(),
            tag: Some("latest".into()),
            limits: publish.limits,
            authenticity: publish.authenticity.clone(),
            signature: None,
            insecure_http: true,
            credential: None,
        },
        None,
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(&actual).unwrap(),
        serde_json::to_value(&expected).unwrap()
    );
    let destination = dir.path().join("pull.gfpb");
    let pull = PortableV2OciPullRequest {
        registry: &server.host,
        repository: "tests/portable",
        reference: &actual.oci_manifest_digest,
        expected_oci_digest: Some(&actual.oci_manifest_digest),
        destination: &destination,
        limits: PortableV2Limits::default(),
        authenticity: Default::default(),
        credential: None,
    };
    let expected = crate::pull_portable_v2_oci_with_registry(&memory, &pull, None).unwrap();
    fs::remove_file(&destination).unwrap();
    let actual = crate::pull_portable_v2_oci(
        &crate::PortableV2OciPullFacadeRequest {
            registry: server.host.clone(),
            repository: "tests/portable".into(),
            reference: actual.oci_manifest_digest.clone(),
            expected_oci_digest: Some(actual.oci_manifest_digest.clone()),
            destination: destination.clone(),
            limits: pull.limits,
            authenticity: pull.authenticity.clone(),
            insecure_http: true,
            credential: None,
        },
        None,
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
    let imported = dir.path().join("imported");
    GraphForge::import_portable_v2(
        &imported,
        &PortableV2ImportRequest {
            input: destination,
            operation_id: OperationId(uuid::Uuid::now_v7()),
            limits: PortableV2Limits::default(),
        },
        None,
    )
    .unwrap();
    let reopened = GraphForge::new(imported.to_str()).unwrap();
    let result = reopened
        .execute("MATCH (n:Person) RETURN n.name AS name")
        .unwrap();
    let values = result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values.value(0), "Ada");
}
