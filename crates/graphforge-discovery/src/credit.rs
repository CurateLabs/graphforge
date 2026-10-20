//! Published credit: the free-form identities recorded on research Versions.
//!
//! Authors and committers recorded in research history are always published
//! (ADR 0055). They are credit, never authentication or access control: nothing
//! here grants or proves permission. The Project people list in a summary is
//! derived from the lineage's Versions, so a reader can recompute and compare it.

use crate::{DiscoveryError, DiscoveryErrorCode, DiscoveryLimits, limit};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Maximum UTF-8 bytes in a signature name.
const MAX_NAME_BYTES: usize = 256;
/// Maximum UTF-8 bytes in a signature email (the RFC 5321 path limit).
const MAX_EMAIL_BYTES: usize = 254;
/// Role a person is credited in.
pub const ROLE_AUTHOR: &str = "author";
/// Role a person is credited in.
pub const ROLE_COMMITTER: &str = "committer";

/// The author or committer credited on a Version, mirroring the native signature.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreditSignature {
    /// Display name: non-empty, no surrounding whitespace or control characters.
    pub name: String,
    /// Optional address, checked syntactically only (`local@domain`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    /// Optional ORCID iD in its bare `0000-0002-1825-0097` form, checksum verified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orcid: Option<String>,
}

impl CreditSignature {
    /// Check the same bounds and syntax the native signature enforces.
    pub fn validate(&self, field: &'static str) -> Result<(), DiscoveryError> {
        if self.name.is_empty()
            || self.name.trim() != self.name
            || self.name.len() > MAX_NAME_BYTES
            || self.name.chars().any(char::is_control)
        {
            return Err(malformed_field(field, "credit name is invalid"));
        }
        if self
            .email
            .as_deref()
            .is_some_and(|email| !valid_email(email))
        {
            return Err(malformed_field(field, "credit email is invalid"));
        }
        if self
            .orcid
            .as_deref()
            .is_some_and(|orcid| !valid_orcid(orcid))
        {
            return Err(malformed_field(field, "credit ORCID is invalid"));
        }
        Ok(())
    }
}

fn valid_email(email: &str) -> bool {
    let Some((local, domain)) = email.split_once('@') else {
        return false;
    };
    email.len() <= MAX_EMAIL_BYTES
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

/// One person credited in a Project's published history.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryPerson {
    /// Display name.
    pub name: String,
    /// Optional address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    /// Optional ORCID iD.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orcid: Option<String>,
    /// Roles held in published Versions: `author` and/or `committer`, ascending.
    pub roles: Vec<String>,
}

impl SummaryPerson {
    fn key(&self) -> (&str, Option<&str>, Option<&str>) {
        (&self.name, self.email.as_deref(), self.orcid.as_deref())
    }

    fn signature(&self) -> CreditSignature {
        CreditSignature {
            name: self.name.clone(),
            email: self.email.clone(),
            orcid: self.orcid.clone(),
        }
    }
}

/// Validate a summary's people list: bounded, canonically ordered, unique, with
/// valid signatures and a non-empty ascending subset of the two roles.
pub(crate) fn validate_people(
    people: &[SummaryPerson],
    limits: DiscoveryLimits,
) -> Result<(), DiscoveryError> {
    const FIELD: &str = "people";
    if people.len() > limits.max_lineage_entries {
        return Err(limit(FIELD));
    }
    let mut prior = None;
    for person in people {
        person.signature().validate(FIELD)?;
        let roles: Vec<&str> = person.roles.iter().map(String::as_str).collect();
        if !matches!(
            roles.as_slice(),
            [ROLE_AUTHOR] | [ROLE_COMMITTER] | [ROLE_AUTHOR, ROLE_COMMITTER]
        ) {
            return Err(malformed_field(FIELD, "person roles are invalid"));
        }
        if prior.is_some_and(|prior| prior >= person.key()) {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::Duplicate,
                Some(FIELD),
                "entries are duplicated or not canonically ordered",
            ));
        }
        prior = Some(person.key());
    }
    Ok(())
}

fn malformed_field(field: &'static str, detail: &'static str) -> DiscoveryError {
    DiscoveryError::new(DiscoveryErrorCode::MalformedResponse, Some(field), detail)
}

/// Derive the people list from Version credit: every distinct signature with the
/// roles it holds, in canonical order.
pub(crate) fn derive_people<'a>(
    credits: impl Iterator<Item = (&'a CreditSignature, &'static str)>,
) -> Vec<SummaryPerson> {
    let mut people: BTreeMap<&CreditSignature, BTreeSet<&'static str>> = BTreeMap::new();
    for (signature, role) in credits {
        people.entry(signature).or_default().insert(role);
    }
    people
        .into_iter()
        .map(|(signature, roles)| SummaryPerson {
            name: signature.name.clone(),
            email: signature.email.clone(),
            orcid: signature.orcid.clone(),
            roles: roles.into_iter().map(str::to_owned).collect(),
        })
        .collect()
}

pub(crate) fn people_mismatch() -> DiscoveryError {
    DiscoveryError::new(
        DiscoveryErrorCode::IntegrityFailure,
        Some("people"),
        "summary people differ from the research lineage's credited Versions",
    )
}
