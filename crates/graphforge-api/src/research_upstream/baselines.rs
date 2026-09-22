//! Reviewed fields advance independently; immutable original provenance survives.
use super::{invalid, preview::hex};
use crate::{
    GfError,
    branches::{baseline, fields},
};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

pub(super) fn incorporate(
    branch: Uuid,
    operation: Uuid,
    upstream_version: Uuid,
    reviewed: &BTreeSet<fields::Key>,
    incoming: &fields::Fields,
    current: &fields::Fields,
    rows: &mut BTreeMap<fields::Key, baseline::Row>,
) -> Result<(), GfError> {
    if upstream_version.is_nil() {
        return Err(invalid(
            "upstream incorporation requires an exact immutable source Version",
        ));
    }
    // Preserve existing local divergence, including reviewed keep-local choices.
    for row in rows.values_mut() {
        row.current = current.get(&row.key).map(hex).unwrap_or_default();
    }
    for key in reviewed {
        let incoming_hash = incoming.get(key).map(hex).unwrap_or_default();
        let row = rows.entry(key.clone()).or_insert_with(|| baseline::Row {
            key: key.clone(),
            origin: upstream_version,
            incorporated: None,
            original: incoming_hash.clone(),
            baseline: String::new(),
            current: String::new(),
            contribution: baseline::contribution(branch, operation, key),
            role: "active".into(),
        });
        row.incorporated = Some(upstream_version);
        row.baseline = incoming_hash;
        row.current = current.get(key).map(hex).unwrap_or_default();
    }
    Ok(())
}

/// Imported fields inherit upstream provenance before local-only field synthesis.
pub(super) fn seed_incoming(
    local: &BTreeMap<fields::Key, baseline::Row>,
    incoming: &BTreeMap<fields::Key, baseline::Row>,
    reviewed: &BTreeSet<fields::Key>,
    incoming_values: &fields::Fields,
    upstream_version: Uuid,
    rows: &mut BTreeMap<fields::Key, baseline::Row>,
) {
    for key in reviewed {
        if local.contains_key(key) {
            continue;
        }
        if let Some(origin) = incoming.get(key) {
            rows.insert(key.clone(), origin.clone());
        } else if let Some(value) = incoming_values.get(key) {
            rows.insert(
                key.clone(),
                baseline::Row {
                    key: key.clone(),
                    origin: upstream_version,
                    incorporated: None,
                    original: hex(value),
                    baseline: String::new(),
                    current: String::new(),
                    contribution: baseline::contribution(upstream_version, upstream_version, key),
                    role: "active".into(),
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selected_baseline_advances_without_losing_origin_or_local_divergence() {
        let branch = Uuid::now_v7();
        let object = Uuid::now_v7();
        let origin = Uuid::now_v7();
        let next = Uuid::now_v7();
        let x = ("node".into(), object, "property:x".into());
        let y = ("node".into(), object, "property:y".into());
        let mut rows: BTreeMap<_, _> = [x.clone(), y.clone()]
            .into_iter()
            .map(|key| {
                let row = baseline::Row {
                    key: key.clone(),
                    origin,
                    incorporated: Some(origin),
                    original: hex(&[0; 32]),
                    baseline: hex(&[0; 32]),
                    current: hex(&[0; 32]),
                    contribution: Uuid::now_v7(),
                    role: "active".into(),
                };
                (key, row)
            })
            .collect();
        let contribution = rows[&x].contribution;
        let incoming = fields::Fields::from([(x.clone(), [1; 32]), (y.clone(), [2; 32])]);
        let local = fields::Fields::from([(x.clone(), [3; 32]), (y.clone(), [0; 32])]);
        incorporate(
            branch,
            Uuid::now_v7(),
            next,
            &BTreeSet::from([x.clone()]),
            &incoming,
            &local,
            &mut rows,
        )
        .unwrap();
        assert_eq!(rows[&x].origin, origin);
        assert_eq!(rows[&x].original, hex(&[0; 32]));
        assert_eq!(rows[&x].contribution, contribution);
        assert_eq!(rows[&x].incorporated, Some(next));
        assert_eq!(rows[&x].baseline, hex(&[1; 32]));
        assert_eq!(rows[&x].current, hex(&[3; 32]));
        assert_eq!(rows[&y].incorporated, Some(origin));
        assert_eq!(rows[&y].baseline, hex(&[0; 32]));
        assert_eq!(rows[&y].current, hex(&[0; 32]));
    }
}
