//! Native owner closure is visible in preview before a resolution can publish.
use crate::{
    CancellationToken, GfError, GraphForge,
    branches::fields,
    research_comparison::{delta, state},
};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

#[derive(Default, serde::Serialize)]
pub(super) struct Requirements {
    pub fields: BTreeSet<fields::Key>,
    pub evidence: BTreeSet<Uuid>,
}
pub(super) fn inspect(
    source: &GraphForge,
    local: &state::State,
    upstream: &state::State,
    rows: &[delta::Row],
    cancel: &CancellationToken,
) -> Result<BTreeMap<fields::Key, Requirements>, GfError> {
    let mut closures = BTreeMap::<(String, Uuid), fields::Objects>::new();
    let mut result = BTreeMap::new();
    let mut budget = 8 * 1024 * 1024usize;
    for row in rows {
        cancel.checkpoint()?;
        if row.change == "dependency_unavailable" || row.right.is_none() {
            continue;
        }
        let object = (row.key.0.clone(), row.key.1);
        if !closures.contains_key(&object) {
            let closure =
                if upstream
                    .fields
                    .contains_key(&(object.0.clone(), object.1, "$object".into()))
                {
                    crate::slices::branch::dependency_objects(
                        source,
                        &BTreeSet::from([object.clone()]),
                        cancel,
                    )?
                } else {
                    BTreeSet::from([object.clone()])
                };
            charge(&mut budget, closure.len().saturating_mul(128))?;
            closures.insert(object.clone(), closure);
        }
        let closure = &closures[&object];
        let mut requirements = Requirements::default();
        for key in upstream.fields.keys() {
            cancel.checkpoint()?;
            let own = (key.0.clone(), key.1) == object;
            let ontology = key.0.starts_with("ontology")
                && matches!(object.0.as_str(), "node" | "edge" | "assertion");
            if !(closure.contains(&(key.0.clone(), key.1)) || ontology)
                || key == &row.key
                || local.fields.get(key) == upstream.fields.get(key)
            {
                continue;
            }
            if key.0 == "source"
                && key.2 == "$preferred_artifact"
                && local
                    .fields
                    .contains_key(&("source".into(), key.1, "$object".into()))
            {
                // An Artifact depends on its immutable Source, not on changing
                // that Source's independently reviewed preferred representation.
                continue;
            }
            if matches!(key.0.as_str(), "node" | "edge") {
                if local
                    .fields
                    .contains_key(&(key.0.clone(), key.1, "$object".into()))
                    || !key.2.starts_with('$')
                {
                    continue;
                }
            } else if own
                && key.0 == "source"
                && key.2 != "$preferred_artifact"
                && local
                    .fields
                    .contains_key(&(key.0.clone(), key.1, "$object".into()))
            {
                // The immutable Source row and its mutable preference ledger have distinct owners.
                continue;
            }
            charge(&mut budget, 128 + key.0.len() + key.2.len())?;
            requirements.fields.insert(key.clone());
        }
        for (kind, id, reason) in &upstream.missing {
            if kind == "artifact"
                && matches!(reason.as_str(), "external_only" | "unverifiable")
                && closure.contains(&(kind.clone(), *id))
            {
                requirements.evidence.insert(*id);
                charge(&mut budget, 32)?;
            }
        }
        result.insert(row.key.clone(), requirements);
    }
    Ok(result)
}
fn charge(budget: &mut usize, bytes: usize) -> Result<(), GfError> {
    *budget = budget.checked_sub(bytes).ok_or_else(|| GfError::Api {
        code: graphforge_core::ApiErrorCode::ResourceLimit,
        message: "upstream dependency review exceeds 8 MiB; narrow the field scope".into(),
    })?;
    Ok(())
}
