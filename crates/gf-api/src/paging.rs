//! Frozen bounded pagination and cooperative-cancellation request primitives.

use std::fmt::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use gf_core::{ApiErrorCode, GfError};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Default page size.
pub const DEFAULT_PAGE_LIMIT: u32 = 100;
/// Maximum accepted page size.
pub const MAX_PAGE_LIMIT: u32 = 10_000;

/// Opaque generation-bound page cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageToken(String);

impl PageToken {
    /// Parse an opaque token returned in Arrow schema metadata.
    ///
    /// # Errors
    /// Returns `GF_PAGE_INVALID` for malformed token text.
    pub fn parse(value: &str) -> Result<Self, GfError> {
        let token = Self(value.to_owned());
        if value.starts_with("gf-page-v2:") {
            token.decode_bound_parts()?;
        } else {
            token.decode()?;
        }
        Ok(token)
    }

    /// Return the exact opaque text used by binding boundaries.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn new(generation_uuid: Uuid, offset: usize) -> Self {
        Self(format!("gf-page-v1:{generation_uuid}:{offset}"))
    }

    pub(crate) fn new_bound(
        method: &str,
        request: Uuid,
        snapshot: Uuid,
        limit: u32,
        offset: usize,
        cursor: [u8; 32],
    ) -> Self {
        let cursor = hex(&cursor);
        let payload = format!("{method}:{request}:{snapshot}:{limit}:{offset}:{cursor}");
        let integrity = digest_hex(payload.as_bytes());
        Self(format!("gf-page-v2:{payload}:{integrity}"))
    }

    pub(crate) fn decode_bound(
        &self,
        expected_method: &str,
        expected_request: Uuid,
        expected_snapshot: Uuid,
        expected_limit: u32,
    ) -> Result<(usize, [u8; 32]), GfError> {
        let (method, request, snapshot, limit, offset, cursor) = self.decode_bound_parts()?;
        if method != expected_method || request != expected_request || limit != expected_limit {
            return Err(page_error(
                ApiErrorCode::PageInvalid,
                "page token request mismatch",
            ));
        }
        if snapshot != expected_snapshot {
            return Err(page_error(
                ApiErrorCode::PageSnapshotGone,
                "page token endpoints are no longer available",
            ));
        }
        Ok((offset, cursor))
    }

    fn decode_bound_parts(&self) -> Result<BoundTokenParts<'_>, GfError> {
        let invalid = || page_error(ApiErrorCode::PageInvalid, "invalid page token");
        let mut parts = self.0.split(':');
        if parts.next() != Some("gf-page-v2") {
            return Err(invalid());
        }
        let method = parts.next().ok_or_else(invalid)?;
        let request = parts
            .next()
            .and_then(|value| Uuid::parse_str(value).ok())
            .ok_or_else(invalid)?;
        let snapshot = parts
            .next()
            .and_then(|value| Uuid::parse_str(value).ok())
            .ok_or_else(invalid)?;
        let limit = parts
            .next()
            .and_then(|value| value.parse::<u32>().ok())
            .ok_or_else(invalid)?;
        let offset = parts
            .next()
            .and_then(|value| value.parse::<usize>().ok())
            .ok_or_else(invalid)?;
        let cursor_text = parts.next().ok_or_else(invalid)?;
        if cursor_text.len() != 64 {
            return Err(invalid());
        }
        let mut cursor = [0_u8; 32];
        for (index, pair) in cursor_text.as_bytes().chunks_exact(2).enumerate() {
            cursor[index] =
                u8::from_str_radix(std::str::from_utf8(pair).map_err(|_| invalid())?, 16)
                    .map_err(|_| invalid())?;
        }
        let integrity = parts.next().ok_or_else(invalid)?;
        if parts.next().is_some() {
            return Err(invalid());
        }
        let payload = format!("{method}:{request}:{snapshot}:{limit}:{offset}:{cursor_text}");
        if integrity != digest_hex(payload.as_bytes()) {
            return Err(invalid());
        }
        Ok((method, request, snapshot, limit, offset, cursor))
    }

    pub(crate) fn decode(&self) -> Result<(Uuid, usize), GfError> {
        let mut parts = self.0.split(':');
        if parts.next() != Some("gf-page-v1") {
            return Err(page_error(ApiErrorCode::PageInvalid, "invalid page token"));
        }
        let generation_uuid = parts
            .next()
            .and_then(|value| Uuid::parse_str(value).ok())
            .ok_or_else(|| page_error(ApiErrorCode::PageInvalid, "invalid page token"))?;
        let offset = parts
            .next()
            .and_then(|value| value.parse::<usize>().ok())
            .ok_or_else(|| page_error(ApiErrorCode::PageInvalid, "invalid page token"))?;
        if parts.next().is_some() {
            return Err(page_error(ApiErrorCode::PageInvalid, "invalid page token"));
        }
        Ok((generation_uuid, offset))
    }
}

fn digest_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

type BoundTokenParts<'a> = (&'a str, Uuid, Uuid, u32, usize, [u8; 32]);

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(
        String::with_capacity(bytes.len() * 2),
        |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to a String cannot fail");
            output
        },
    )
}

/// Cloneable cooperative-cancellation state.
#[derive(Clone, Debug)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    /// Create an uncancelled token.
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// Cancel all clones. Repeated calls are idempotent.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    pub(crate) fn checkpoint(&self) -> Result<(), GfError> {
        if self.is_cancelled() {
            Err(page_error(
                ApiErrorCode::Cancelled,
                "operation was cancelled",
            ))
        } else {
            Ok(())
        }
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for CancellationToken {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for CancellationToken {}

/// Shared exact page request shape for M20/M21 list methods.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageRequest {
    /// Requested row bound in `1..=10_000`.
    pub limit: u32,
    /// Optional opaque cursor from the same method and generation.
    pub after: Option<PageToken>,
    /// Optional cooperative cancellation state.
    pub cancellation: Option<CancellationToken>,
}

impl Default for PageRequest {
    fn default() -> Self {
        Self {
            limit: DEFAULT_PAGE_LIMIT,
            after: None,
            cancellation: None,
        }
    }
}

pub(crate) fn validate_page(
    request: &PageRequest,
    generation_uuid: Uuid,
    row_count: usize,
) -> Result<(usize, usize), GfError> {
    if !(1..=MAX_PAGE_LIMIT).contains(&request.limit) {
        return Err(GfError::Validation(format!(
            "page limit must be in 1..={MAX_PAGE_LIMIT}"
        )));
    }
    if let Some(cancellation) = &request.cancellation {
        cancellation.checkpoint()?;
    }
    let offset = match &request.after {
        Some(token) => {
            let (token_generation, offset) = token.decode()?;
            if token_generation != generation_uuid {
                return Err(page_error(
                    ApiErrorCode::PageSnapshotGone,
                    "page token belongs to a different generation",
                ));
            }
            if offset > row_count {
                return Err(page_error(
                    ApiErrorCode::PageInvalid,
                    "page token offset exceeds result rows",
                ));
            }
            offset
        }
        None => 0,
    };
    let end = offset.saturating_add(request.limit as usize).min(row_count);
    Ok((offset, end))
}

fn page_error(code: ApiErrorCode, message: impl Into<String>) -> GfError {
    GfError::Api {
        code,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_is_generation_bound_and_cancellation_is_shared() {
        let generation = Uuid::from_u128(1);
        let token = PageToken::new(generation, 7);
        assert_eq!(PageToken::parse(token.as_str()).unwrap(), token);
        assert_eq!(token.decode().unwrap(), (generation, 7));

        let cancellation = CancellationToken::new();
        let clone = cancellation.clone();
        clone.cancel();
        assert!(cancellation.is_cancelled());
        assert_eq!(
            cancellation.checkpoint().unwrap_err().code(),
            "GF_CANCELLED"
        );

        let request = Uuid::from_u128(2);
        let cursor = [3; 32];
        let bound = PageToken::new_bound("checkpoint-diff", request, generation, 10, 4, cursor);
        assert_eq!(PageToken::parse(bound.as_str()).unwrap(), bound);
        assert_eq!(
            bound
                .decode_bound("checkpoint-diff", request, generation, 10)
                .unwrap(),
            (4, cursor)
        );
        assert_eq!(
            bound
                .decode_bound("checkpoint-list", request, generation, 10)
                .unwrap_err()
                .code(),
            "GF_PAGE_INVALID"
        );
        assert_eq!(
            bound
                .decode_bound("checkpoint-diff", request, Uuid::from_u128(3), 10)
                .unwrap_err()
                .code(),
            "GF_PAGE_SNAPSHOT_GONE"
        );
        let mut tampered = bound.as_str().to_owned();
        tampered.push('0');
        assert_eq!(
            PageToken::parse(&tampered).unwrap_err().code(),
            "GF_PAGE_INVALID"
        );
    }
}
