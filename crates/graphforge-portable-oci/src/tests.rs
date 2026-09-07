use super::*;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

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
