//! Transport-neutral HTTP request and response values.

use crate::error::HubPublishError;
use serde::{Deserialize, Serialize};
use std::fmt;

/// HTTP method used by the publish and read mappings.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HubMethod {
    /// `GET`
    Get,
    /// `HEAD`
    Head,
    /// `POST`
    Post,
    /// `PUT`
    Put,
}

/// One HTTP request to a Hub.
///
/// `Debug` omits the URL path, header values, and body, because an upload URL
/// is a capability and an `Authorization` header carries the token.
#[derive(Clone, Eq, PartialEq)]
pub struct HubRequest {
    /// Method.
    pub method: HubMethod,
    /// Absolute URL.
    pub url: String,
    /// Headers; names compare case-insensitively.
    pub headers: Vec<(String, String)>,
    /// Body bytes.
    pub body: Vec<u8>,
}

impl HubRequest {
    /// Request without headers or body.
    #[must_use]
    pub fn new(method: HubMethod, url: impl Into<String>) -> Self {
        Self {
            method,
            url: url.into(),
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    /// Add a header.
    #[must_use]
    pub fn with_header(mut self, name: &str, value: impl Into<String>) -> Self {
        self.headers.push((name.to_ascii_lowercase(), value.into()));
        self
    }

    /// Set raw body bytes.
    #[must_use]
    pub fn with_body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }

    /// Set a JSON body and `content-type: application/json`.
    #[must_use]
    pub fn with_json<T: Serialize>(self, value: &T) -> Self {
        self.with_header("content-type", "application/json")
            .with_body(serde_json::to_vec(value).expect("publish documents serialize"))
    }

    /// First value of header `name`.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        find_header(&self.headers, name)
    }
}

impl fmt::Debug for HubRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let origin = url::Url::parse(&self.url).map_or_else(
            |_| "<invalid>".to_owned(),
            |url| url.origin().ascii_serialization(),
        );
        formatter
            .debug_struct("HubRequest")
            .field("method", &self.method)
            .field("origin", &origin)
            .field(
                "headers",
                &self
                    .headers
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>(),
            )
            .field("body_len", &self.body.len())
            .finish()
    }
}

/// One HTTP response from a Hub.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubResponse {
    /// Status code.
    pub status: u16,
    /// Headers with lowercase names.
    pub headers: Vec<(String, String)>,
    /// Body bytes.
    pub body: Vec<u8>,
}

impl HubResponse {
    /// First value of header `name`.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        find_header(&self.headers, name)
    }
}

fn find_header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

/// Sends publish and read requests to a Hub.
///
/// `ReferenceHub` implements it in-process; the CLI implements it over HTTPS.
pub trait HubExchange {
    /// Send one request. `Err` means no HTTP response was obtained.
    fn exchange(&self, request: HubRequest) -> Result<HubResponse, HubPublishError>;
}
