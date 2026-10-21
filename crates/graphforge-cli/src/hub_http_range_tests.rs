use super::*;
use std::sync::Mutex;

struct Disconnect {
    bytes: Vec<u8>,
    remaining: usize,
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
        } else {
            "\"stable\""
        };
        if let Some(offset) = range {
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
            let bytes = self.bytes[offset as usize..].to_vec();
            Ok(HttpResponse {
                status: 206,
                location: None,
                content_range: Some(if self.mismatch_range {
                    "bytes 0-1/2".into()
                } else {
                    format!(
                        "bytes {offset}-{}/{}",
                        self.bytes.len() - 1,
                        self.bytes.len()
                    )
                }),
                etag: Some(etag.into()),
                content_length: Some(bytes.len() as u64),
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
                content_length: Some(self.bytes.len() as u64),
                retry_after: None,
                body: Box::new(Disconnect {
                    bytes: self.bytes.clone(),
                    remaining: self.bytes.len() / 2,
                }),
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
fn ranged_url_validation_rejects_http_and_private_ip() {
    assert!(validate_url(&Url::parse("http://example.com/object").unwrap()).is_err());
    assert!(validate_url(&Url::parse("https://127.0.0.1/object").unwrap()).is_err());
    assert!(
        validate_object_url(&Url::parse("https://example.com/object?X-Amz-Signature=abc").unwrap())
            .is_ok()
    );
}
