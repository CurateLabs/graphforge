use super::*;
use std::sync::Mutex;

struct Disconnect {
    bytes: Vec<u8>,
    remaining: usize,
}

struct InterruptOnce {
    bytes: std::io::Cursor<Vec<u8>>,
    interrupted: bool,
}
impl Read for InterruptOnce {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if !self.interrupted {
            self.interrupted = true;
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        }
        self.bytes.read(out)
    }
}
impl Read for Disconnect {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Err(io::Error::other("forced disconnect"));
        }
        let count = out.len().min(self.remaining).min(self.bytes.len());
        out[..count].copy_from_slice(&self.bytes[..count]);
        self.bytes.drain(..count);
        self.remaining -= count;
        Ok(count)
    }
}

struct Fake {
    bytes: Vec<u8>,
    calls: Mutex<Vec<(Option<u64>, Option<String>)>>,
    changed: bool,
    status_on_resume: u16,
    mismatch_range: bool,
    repeat_disconnect: bool,
    initial_content_length: bool,
    interrupt_once: bool,
    initial_clean_eof: bool,
    weak_etag: bool,
    weak_resume_etag: bool,
    resume_range_end: Option<u64>,
    omit_resume_content_length: bool,
    initial_length_override: Option<u64>,
}
impl Transport for Fake {
    fn get(
        &self,
        _: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        _: u64,
    ) -> Result<HttpResponse, graphforge_api::GfError> {
        let mut calls = self.calls.lock().unwrap();
        calls.push((range, if_range.map(str::to_owned)));
        let etag = if self.changed && range.is_some() {
            "\"new\""
        } else if self.weak_etag || (range.is_some() && self.weak_resume_etag) {
            "W/\"stable\""
        } else {
            "\"stable\""
        };
        if let Some(offset) = range {
            if offset as usize >= self.bytes.len() {
                return Ok(HttpResponse {
                    status: 416,
                    location: None,
                    content_range: None,
                    etag: Some(etag.into()),
                    content_length: None,
                    retry_after: None,
                    body: Box::new(std::io::Cursor::new(Vec::new())),
                });
            }
            if self.status_on_resume != 206 {
                return Ok(HttpResponse {
                    status: self.status_on_resume,
                    location: None,
                    content_range: None,
                    etag: Some(etag.into()),
                    content_length: None,
                    retry_after: None,
                    body: Box::new(std::io::Cursor::new(Vec::new())),
                });
            }
            let range_end = self
                .resume_range_end
                .filter(|end| *end >= offset)
                .unwrap_or_else(|| self.bytes.len().saturating_sub(1) as u64);
            let bytes = self.bytes[offset as usize..].to_vec();
            Ok(HttpResponse {
                status: 206,
                location: None,
                content_range: Some(if self.mismatch_range {
                    "bytes 0-1/2".into()
                } else {
                    format!("bytes {offset}-{range_end}/{}", self.bytes.len())
                }),
                etag: Some(etag.into()),
                content_length: (!self.omit_resume_content_length).then_some(bytes.len() as u64),
                retry_after: None,
                body: if self.repeat_disconnect {
                    Box::new(Disconnect {
                        remaining: 0,
                        bytes,
                    })
                } else {
                    Box::new(std::io::Cursor::new(bytes))
                },
            })
        } else {
            Ok(HttpResponse {
                status: 200,
                location: None,
                content_range: None,
                etag: Some(etag.into()),
                content_length: self.initial_length_override.or_else(|| {
                    self.initial_content_length
                        .then_some(self.bytes.len() as u64)
                }),
                retry_after: None,
                body: if self.interrupt_once {
                    Box::new(InterruptOnce {
                        bytes: std::io::Cursor::new(self.bytes.clone()),
                        interrupted: false,
                    })
                } else if self.initial_clean_eof {
                    Box::new(std::io::Cursor::new(self.bytes.clone()))
                } else {
                    Box::new(Disconnect {
                        bytes: self.bytes.clone(),
                        remaining: self.bytes.len() / 2,
                    })
                },
            })
        }
    }
}

#[test]
fn resume_carries_range_and_if_range_and_rejects_changed_etag() {
    let data = b"portable bytes".to_vec();
    let transport = Fake {
        bytes: data.clone(),
        calls: Mutex::new(Vec::new()),
        changed: false,
        status_on_resume: 206,
        mismatch_range: false,
        repeat_disconnect: false,
        initial_content_length: true,
        interrupt_once: false,
        initial_clean_eof: false,
        weak_etag: false,
        weak_resume_etag: false,
        resume_range_end: None,
        omit_resume_content_length: false,
        initial_length_override: None,
    };
    let mut reader = RangedObjectReader::open(
        &transport,
        Url::parse("https://example.com/object").unwrap(),
    )
    .unwrap();
    let mut actual = Vec::new();
    reader.read_to_end(&mut actual).unwrap();
    assert_eq!(actual, data);
    assert_eq!(
        transport.calls.lock().unwrap()[1],
        (Some(7), Some("\"stable\"".into()))
    );
}

#[test]
fn changed_etag_is_concurrent_mutation() {
    let changed = Fake {
        bytes: b"portable bytes".to_vec(),
        calls: Mutex::new(Vec::new()),
        changed: true,
        status_on_resume: 206,
        mismatch_range: false,
        repeat_disconnect: false,
        initial_content_length: true,
        interrupt_once: false,
        initial_clean_eof: false,
        weak_etag: false,
        weak_resume_etag: false,
        resume_range_end: None,
        omit_resume_content_length: false,
        initial_length_override: None,
    };
    let mut reader =
        RangedObjectReader::open(&changed, Url::parse("https://example.com/object").unwrap())
            .unwrap();
    assert!(
        reader
            .read_to_end(&mut Vec::new())
            .unwrap_err()
            .to_string()
            .contains("ConcurrentMutation")
    );
    assert!(reader.changed);
}

fn assert_cli_error(error: crate::CliRuntimeError, code: &str, exit_code: i32) -> String {
    assert_eq!(crate::error_exit_code(&error), exit_code);
    let mut output = Vec::new();
    crate::write_runtime_error(&error, true, &mut output).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(value["error"]["code"], code);
    value["error"]["message"].as_str().unwrap().to_owned()
}

fn fixture_bytes() -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/hub/generated/v1/objects/openalex-openalex.gfpb");
    std::fs::read(path).unwrap()
}

#[test]
fn url_verification_maps_changed_objects_to_conflict_exit_one() {
    for (changed, status_on_resume, mismatch_range) in
        [(true, 206, false), (false, 200, false), (false, 206, true)]
    {
        let transport = Fake {
            bytes: fixture_bytes(),
            calls: Mutex::new(Vec::new()),
            changed,
            status_on_resume,
            mismatch_range,
            repeat_disconnect: false,
            initial_content_length: true,
            interrupt_once: false,
            initial_clean_eof: false,
            weak_etag: false,
            weak_resume_etag: false,
            resume_range_end: None,
            omit_resume_content_length: false,
            initial_length_override: None,
        };
        let error = crate::portable_cli::verify_portable_url(
            &transport,
            Url::parse("https://example.com/object").unwrap(),
        )
        .unwrap_err();
        assert_cli_error(error, "GF_IDEMPOTENCY_CONFLICT", 1);
    }
}

#[test]
fn url_verification_maps_ninth_disconnect_to_io_exit_three() {
    let transport = Fake {
        bytes: fixture_bytes(),
        calls: Mutex::new(Vec::new()),
        changed: false,
        status_on_resume: 206,
        mismatch_range: false,
        repeat_disconnect: true,
        initial_content_length: true,
        interrupt_once: false,
        initial_clean_eof: false,
        weak_etag: false,
        weak_resume_etag: false,
        resume_range_end: None,
        omit_resume_content_length: false,
        initial_length_override: None,
    };
    let error = crate::portable_cli::verify_portable_url(
        &transport,
        Url::parse("https://example.com/object").unwrap(),
    )
    .unwrap_err();
    assert_cli_error(error, "GF_STORAGE", 3);
}

#[test]
fn stdin_io_failure_uses_the_portable_storage_exit_class() {
    struct FailedStdin(std::io::Cursor<Vec<u8>>);
    impl Read for FailedStdin {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            if self.0.position() == self.0.get_ref().len() as u64 {
                Err(io::Error::other("stdin read failed"))
            } else {
                self.0.read(output)
            }
        }
    }

    let error = crate::portable_cli::verify_portable_stdin(&mut FailedStdin(std::io::Cursor::new(
        fixture_bytes(),
    )))
    .unwrap_err();
    assert_cli_error(error, "GF_STORAGE", 3);
}

#[test]
fn url_without_content_length_fails_before_reading() {
    let transport = Fake {
        bytes: fixture_bytes(),
        calls: Mutex::new(Vec::new()),
        changed: false,
        status_on_resume: 206,
        mismatch_range: false,
        repeat_disconnect: false,
        initial_content_length: false,
        interrupt_once: false,
        initial_clean_eof: true,
        weak_etag: false,
        weak_resume_etag: false,
        resume_range_end: None,
        omit_resume_content_length: false,
        initial_length_override: None,
    };
    let error = crate::portable_cli::verify_portable_url(
        &transport,
        Url::parse("https://example.com/object").unwrap(),
    )
    .unwrap_err();
    let message = assert_cli_error(error, "GF_IO", 3);
    assert!(message.contains("object response has no Content-Length; cannot verify resumably"));
    assert_eq!(transport.calls.lock().unwrap().len(), 1);
}

#[test]
fn interrupted_body_read_retries_without_ranging() {
    let transport = Fake {
        bytes: fixture_bytes(),
        calls: Mutex::new(Vec::new()),
        changed: false,
        status_on_resume: 206,
        mismatch_range: false,
        repeat_disconnect: false,
        initial_content_length: true,
        interrupt_once: true,
        initial_clean_eof: false,
        weak_etag: false,
        weak_resume_etag: false,
        resume_range_end: None,
        omit_resume_content_length: false,
        initial_length_override: None,
    };
    let report = crate::portable_cli::verify_portable_url(
        &transport,
        Url::parse("https://example.com/object").unwrap(),
    );
    let report = match report {
        Ok(report) => report,
        Err(_) => panic!("interrupted body read should retry and verify"),
    };
    assert_eq!(report.source, graphforge_api::PortableSource::Stream);
    assert_eq!(transport.calls.lock().unwrap().len(), 1);
}

#[test]
fn weak_etag_cannot_authorize_a_resume() {
    let transport = Fake {
        bytes: fixture_bytes(),
        calls: Mutex::new(Vec::new()),
        changed: false,
        status_on_resume: 206,
        mismatch_range: false,
        repeat_disconnect: false,
        initial_content_length: true,
        interrupt_once: false,
        initial_clean_eof: false,
        weak_etag: true,
        weak_resume_etag: false,
        resume_range_end: None,
        omit_resume_content_length: false,
        initial_length_override: None,
    };
    let mut reader = RangedObjectReader::open(
        &transport,
        Url::parse("https://example.com/object").unwrap(),
    )
    .unwrap();
    let read_error = reader.read_to_end(&mut Vec::new()).unwrap_err();
    assert!(
        read_error
            .to_string()
            .contains("object has no strong validator")
    );
    let error = crate::portable_cli::verify_portable_url(
        &transport,
        Url::parse("https://example.com/object").unwrap(),
    )
    .unwrap_err();
    assert_cli_error(error, "GF_STORAGE", 3);
    assert_eq!(transport.calls.lock().unwrap().len(), 2);
}

#[test]
fn weak_etag_on_resumed_response_is_a_conflict() {
    let transport = Fake {
        bytes: b"portable bytes".to_vec(),
        calls: Mutex::new(Vec::new()),
        changed: false,
        status_on_resume: 206,
        mismatch_range: false,
        repeat_disconnect: false,
        initial_content_length: true,
        interrupt_once: false,
        initial_clean_eof: false,
        weak_etag: false,
        weak_resume_etag: true,
        resume_range_end: None,
        omit_resume_content_length: false,
        initial_length_override: None,
    };
    let mut reader = RangedObjectReader::open(
        &transport,
        Url::parse("https://example.com/object").unwrap(),
    )
    .unwrap();
    let error = reader.read_to_end(&mut Vec::new()).unwrap_err();
    assert!(error.to_string().contains("ConcurrentMutation"));
    assert!(reader.changed());
}

#[test]
fn short_declared_range_is_bounded_and_resumed_from_its_end() {
    let data = b"0123456789".to_vec();
    let transport = Fake {
        bytes: data.clone(),
        calls: Mutex::new(Vec::new()),
        changed: false,
        status_on_resume: 206,
        mismatch_range: false,
        repeat_disconnect: false,
        initial_content_length: true,
        interrupt_once: false,
        initial_clean_eof: false,
        weak_etag: false,
        weak_resume_etag: false,
        resume_range_end: Some(6),
        omit_resume_content_length: true,
        initial_length_override: None,
    };
    let mut reader = RangedObjectReader::open(
        &transport,
        Url::parse("https://example.com/object").unwrap(),
    )
    .unwrap();
    let mut actual = Vec::new();
    reader.read_to_end(&mut actual).unwrap();
    assert_eq!(actual, data);
    assert_eq!(
        *transport.calls.lock().unwrap(),
        vec![
            (None, None),
            (Some(5), Some("\"stable\"".into())),
            (Some(7), Some("\"stable\"".into()))
        ]
    );
}

#[test]
fn range_end_must_be_less_than_total() {
    let transport = Fake {
        bytes: b"0123456789".to_vec(),
        calls: Mutex::new(Vec::new()),
        changed: false,
        status_on_resume: 206,
        mismatch_range: false,
        repeat_disconnect: false,
        initial_content_length: true,
        interrupt_once: false,
        initial_clean_eof: false,
        weak_etag: false,
        weak_resume_etag: false,
        resume_range_end: Some(10),
        omit_resume_content_length: true,
        initial_length_override: None,
    };
    let mut reader = RangedObjectReader::open(
        &transport,
        Url::parse("https://example.com/object").unwrap(),
    )
    .unwrap();
    let error = reader.read_to_end(&mut Vec::new()).unwrap_err();
    assert!(error.to_string().contains("ConcurrentMutation"));
    assert!(reader.changed());
}

#[test]
fn initial_body_never_exposes_bytes_beyond_content_length() {
    let transport = Fake {
        bytes: b"0123456789".to_vec(),
        calls: Mutex::new(Vec::new()),
        changed: false,
        status_on_resume: 206,
        mismatch_range: false,
        repeat_disconnect: false,
        initial_content_length: true,
        interrupt_once: false,
        initial_clean_eof: true,
        weak_etag: false,
        weak_resume_etag: false,
        resume_range_end: None,
        omit_resume_content_length: false,
        initial_length_override: Some(5),
    };
    let mut reader = RangedObjectReader::open(
        &transport,
        Url::parse("https://example.com/object").unwrap(),
    )
    .unwrap();
    let mut actual = Vec::new();
    reader.read_to_end(&mut actual).unwrap();
    assert_eq!(actual, b"01234");
}

#[test]
fn resumed_hub_bundle_verifies_like_the_file() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/hub/generated/v1/objects/openalex-openalex.gfpb");
    let bytes = std::fs::read(&path).unwrap();
    let transport = Fake {
        bytes,
        calls: Mutex::new(Vec::new()),
        changed: false,
        status_on_resume: 206,
        mismatch_range: false,
        repeat_disconnect: false,
        initial_content_length: true,
        interrupt_once: false,
        initial_clean_eof: false,
        weak_etag: false,
        weak_resume_etag: false,
        resume_range_end: None,
        omit_resume_content_length: false,
        initial_length_override: None,
    };
    let mut reader = RangedObjectReader::open(
        &transport,
        Url::parse("https://example.com/object").unwrap(),
    )
    .unwrap();
    let streamed = graphforge_api::verify_portable_stream(
        &mut reader,
        None,
        graphforge_api::PortableLimits::default(),
        None,
    )
    .unwrap();
    let file = graphforge_api::verify_portable(
        &graphforge_api::PortableVerifyRequest {
            input: path,
            mode: graphforge_api::PortableMode::Full,
            limits: graphforge_api::PortableLimits::default(),
        },
        None,
    )
    .unwrap();
    let mut streamed = serde_json::to_value(streamed).unwrap();
    let mut file = serde_json::to_value(file).unwrap();
    assert_eq!(streamed["source"], "stream");
    streamed.as_object_mut().unwrap().remove("source");
    file.as_object_mut().unwrap().remove("source");
    assert_eq!(streamed, file);
    let calls = transport.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[1],
        (
            Some(transport.bytes.len() as u64 / 2),
            Some("\"stable\"".into())
        )
    );
}

#[test]
fn resume_rejects_200_and_mismatched_content_range() {
    for (status_on_resume, mismatch_range) in [(200, false), (206, true)] {
        let transport = Fake {
            bytes: b"portable bytes".to_vec(),
            calls: Mutex::new(Vec::new()),
            changed: false,
            status_on_resume,
            mismatch_range,
            repeat_disconnect: false,
            initial_content_length: true,
            interrupt_once: false,
            initial_clean_eof: false,
            weak_etag: false,
            weak_resume_etag: false,
            resume_range_end: None,
            omit_resume_content_length: false,
            initial_length_override: None,
        };
        let mut reader = RangedObjectReader::open(
            &transport,
            Url::parse("https://example.com/object").unwrap(),
        )
        .unwrap();
        assert!(
            reader
                .read_to_end(&mut Vec::new())
                .unwrap_err()
                .to_string()
                .contains("ConcurrentMutation")
        );
        assert!(reader.changed());
    }
}

#[test]
fn ninth_disconnect_fails_as_io() {
    let transport = Fake {
        bytes: b"portable bytes".to_vec(),
        calls: Mutex::new(Vec::new()),
        changed: false,
        status_on_resume: 206,
        mismatch_range: false,
        repeat_disconnect: true,
        initial_content_length: true,
        interrupt_once: false,
        initial_clean_eof: false,
        weak_etag: false,
        weak_resume_etag: false,
        resume_range_end: None,
        omit_resume_content_length: false,
        initial_length_override: None,
    };
    let mut reader = RangedObjectReader::open(
        &transport,
        Url::parse("https://example.com/object").unwrap(),
    )
    .unwrap();
    assert!(
        reader
            .read_to_end(&mut Vec::new())
            .unwrap_err()
            .to_string()
            .contains("maximum ranged resumes")
    );
    assert_eq!(transport.calls.lock().unwrap().len(), 9);
}

#[test]
fn ranged_url_validation_refuses_unsafe_urls_before_transport() {
    let transport = Fake {
        bytes: Vec::new(),
        calls: Mutex::new(Vec::new()),
        changed: false,
        status_on_resume: 206,
        mismatch_range: false,
        repeat_disconnect: false,
        initial_content_length: true,
        interrupt_once: false,
        initial_clean_eof: true,
        weak_etag: false,
        weak_resume_etag: false,
        resume_range_end: None,
        omit_resume_content_length: false,
        initial_length_override: None,
    };
    for raw in [
        "http://example.com/object",
        "https://127.0.0.1/object",
        "https://u:p@host/x",
        "https://example.com/object#fragment",
    ] {
        assert!(
            RangedObjectReader::open(&transport, Url::parse(raw).unwrap()).is_err(),
            "accepted {raw}"
        );
    }
    assert!(transport.calls.lock().unwrap().is_empty());
}
