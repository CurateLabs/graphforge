//! Deterministic three-way comparison; incorporated state is distinct from origin.
use super::{accepted::Accepted, state::State};
use crate::{CancellationToken, GfError, branches::fields::Key};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;
#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct Row {
    pub key: Key,
    pub change: &'static str,
    pub disposition: &'static str,
    pub baseline: Option<[u8; 32]>,
    pub left: Option<[u8; 32]>,
    pub right: Option<[u8; 32]>,
    pub origin: Option<Uuid>,
    pub incorporated: Option<Uuid>,
    pub contribution: Option<Uuid>,
    pub accepted_source: Option<Uuid>,
    pub accepted_destination: Option<Uuid>,
    pub detail: String,
}
fn decode(value: &str) -> Result<Option<[u8; 32]>, GfError> {
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() != 64 || !value.is_ascii() {
        return Err(super::invalid("invalid native baseline commitment"));
    }
    let mut bytes = [0; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| super::invalid("invalid native baseline commitment"))?;
    }
    Ok(Some(bytes))
}
pub(crate) fn compare(
    left: &State,
    right: &State,
    accepted: &BTreeMap<Key, Accepted>,
    cancel: &CancellationToken,
) -> Result<Vec<Row>, GfError> {
    let keys: BTreeSet<_> = left
        .fields
        .keys()
        .chain(right.fields.keys())
        .chain(left.baseline.keys())
        .cloned()
        .collect();
    let mut rows = Vec::new();
    for key in keys {
        cancel.checkpoint()?;
        let a = left.fields.get(&key).copied();
        let b = right.fields.get(&key).copied();
        let source = left.baseline.get(&key);
        let persistent = source
            .map(|row| decode(&row.baseline))
            .transpose()?
            .flatten();
        let prior = accepted.get(&key);
        let baseline = prior.map_or(persistent, |p| p.value);
        let already_accepted = prior.is_some_and(|p| p.value == a);
        let disposition = if already_accepted {
            "accepted"
        } else if a == baseline {
            "inherited"
        } else if a.is_none() {
            "suppressed"
        } else if baseline.is_none() {
            "local"
        } else {
            "modified"
        };
        let direct = left.branch.is_none() || key.2 == "$canonical";
        let change = if a == b {
            if already_accepted {
                "accepted"
            } else if !direct && a != baseline {
                "equivalent"
            } else {
                continue;
            }
        } else if direct {
            "changed"
        } else if left.suppressed.contains(&(key.0.clone(), key.1)) && b != baseline {
            "conflict"
        } else if a == baseline {
            "upstream"
        } else if b == baseline {
            "local"
        } else {
            "conflict"
        };
        rows.push(Row {
            key,
            change,
            disposition,
            baseline,
            left: a,
            right: b,
            origin: source.map(|r| r.origin),
            incorporated: source.and_then(|r| r.incorporated),
            contribution: prior
                .map(|p| p.contribution)
                .or_else(|| source.map(|r| r.contribution)),
            accepted_source: prior.map(|p| p.source),
            accepted_destination: prior.map(|p| p.destination),
            detail: String::new(),
        });
    }
    for (side, state) in [("left", left), ("right", right)] {
        for (kind, id, detail) in &state.missing {
            rows.push(Row {
                key: (kind.clone(), *id, format!("$dependency:{side}")),
                change: "dependency_unavailable",
                disposition: "unavailable",
                baseline: None,
                left: None,
                right: None,
                origin: None,
                incorporated: None,
                contribution: None,
                accepted_source: None,
                accepted_destination: None,
                detail: detail.clone(),
            });
        }
    }
    rows.sort_by(|a, b| (&a.key, &a.change, &a.detail).cmp(&(&b.key, &b.change, &b.detail)));
    Ok(rows)
}
