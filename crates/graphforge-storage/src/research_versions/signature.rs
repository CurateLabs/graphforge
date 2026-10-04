//! Free-form credited identities on research Versions; recorded, never authenticated.
use graphforge_core::GfError;
use serde::{Deserialize, Serialize};

/// Maximum UTF-8 bytes in a signature name.
pub const MAX_SIGNATURE_NAME_BYTES: usize = 256;
/// Maximum UTF-8 bytes in a signature email (the RFC 5321 path limit).
pub const MAX_SIGNATURE_EMAIL_BYTES: usize = 254;

/// The author or committer credited on a Version. Credit, not access control:
/// nothing here grants or proves permission, and the host never reads it for that.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchSignature {
    /// Display name: non-empty, no surrounding whitespace or control characters.
    pub name: String,
    /// Optional address, checked syntactically only (`local@domain`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    /// Optional ORCID iD in its bare `0000-0002-1825-0097` form, checksum verified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orcid: Option<String>,
}

impl ResearchSignature {
    /// Check bounds and syntax. Every Version records exactly the validated bytes,
    /// so incidental whitespace never changes a Version identity.
    pub fn validate(&self) -> Result<(), GfError> {
        let refuse = |message: &str| Err(GfError::Validation(message.into()));
        if self.name.is_empty()
            || self.name.trim() != self.name
            || self.name.len() > MAX_SIGNATURE_NAME_BYTES
            || self.name.chars().any(char::is_control)
        {
            return refuse(
                "signature name must be 1..=256 bytes without surrounding whitespace or control characters",
            );
        }
        if self
            .email
            .as_deref()
            .is_some_and(|email| !valid_email(email))
        {
            return refuse("signature email must be a bounded local@domain address");
        }
        if self
            .orcid
            .as_deref()
            .is_some_and(|orcid| !valid_orcid(orcid))
        {
            return refuse("signature ORCID must be a checksummed 0000-0000-0000-000X iD");
        }
        Ok(())
    }
}

fn valid_email(email: &str) -> bool {
    let Some((local, domain)) = email.split_once('@') else {
        return false;
    };
    email.len() <= MAX_SIGNATURE_EMAIL_BYTES
        && !local.is_empty()
        && local.len() <= 64
        && !domain.is_empty()
        && domain.len() <= 253
        && !domain.contains('@')
        && domain.split('.').all(|label| !label.is_empty())
        && !email
            .chars()
            .any(|c| c.is_whitespace() || c.is_control() || matches!(c, '<' | '>'))
}

/// ISO 7064 MOD 11-2 over the fifteen leading digits, as ORCID specifies.
fn valid_orcid(orcid: &str) -> bool {
    let bytes = orcid.as_bytes();
    if bytes.len() != 19 || [4, 9, 14].iter().any(|&i| bytes[i] != b'-') {
        return false;
    }
    let digits: Vec<u8> = bytes
        .iter()
        .enumerate()
        .filter(|(i, _)| ![4, 9, 14].contains(i))
        .map(|(_, b)| *b)
        .collect();
    if digits[..15].iter().any(|b| !b.is_ascii_digit()) {
        return false;
    }
    let total = digits[..15]
        .iter()
        .fold(0_u32, |total, digit| (total + u32::from(digit - b'0')) * 2);
    let check = (12 - total % 11) % 11;
    let expected = if check == 10 {
        b'X'
    } else {
        b'0' + u8::try_from(check).expect("check digit below 11")
    };
    digits[15] == expected
}

#[cfg(test)]
mod tests {
    use super::ResearchSignature;

    fn signature(name: &str, email: Option<&str>, orcid: Option<&str>) -> ResearchSignature {
        ResearchSignature {
            name: name.into(),
            email: email.map(Into::into),
            orcid: orcid.map(Into::into),
        }
    }

    #[test]
    fn signature_bounds_and_syntax_are_enforced() {
        for valid in [
            signature("Ada Lovelace", None, None),
            signature("Ada", Some("ada@example.org"), Some("0000-0002-1825-0097")),
            signature("Ä", Some("a@localhost"), Some("0000-0002-9079-593X")),
            signature(&"n".repeat(256), None, None),
        ] {
            valid.validate().unwrap();
        }
        let long_local = format!("{}@example.org", "l".repeat(65));
        let long_email = format!("a@{}.org", "d".repeat(250));
        for invalid in [
            signature("", None, None),
            signature(" Ada", None, None),
            signature("Ada\n", None, None),
            signature("A\u{7}da", None, None),
            signature(&"n".repeat(257), None, None),
            signature("Ada", Some("ada"), None),
            signature("Ada", Some("@example.org"), None),
            signature("Ada", Some("ada@"), None),
            signature("Ada", Some("ada@@example.org"), None),
            signature("Ada", Some("ada@example..org"), None),
            signature("Ada", Some("a da@example.org"), None),
            signature("Ada", Some("<ada@example.org>"), None),
            signature("Ada", Some(&long_local), None),
            signature("Ada", Some(&long_email), None),
            signature("Ada", None, Some("0000-0002-1825-0098")),
            signature("Ada", None, Some("0000000218250097")),
            signature("Ada", None, Some("https://orcid.org/0000-0002-1825-0097")),
            signature("Ada", None, Some("0000-0002-1825-009x")),
        ] {
            assert!(invalid.validate().is_err(), "{invalid:?}");
        }
    }
}
