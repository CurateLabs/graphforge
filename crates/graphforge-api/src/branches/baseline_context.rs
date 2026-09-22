//! Read legacy Branch context baselines from their retained exact selected base.
use super::{
    baseline::{self, Row},
    fields::{Fields, Key},
};
use crate::{CancellationToken, GfError, GraphForge};
use graphforge_storage::research_versions::ResearchRegistry;
use std::collections::BTreeMap;
use uuid::Uuid;

pub(crate) fn read(
    owner: &GraphForge,
    graph: &GraphForge,
    context: Uuid,
    observed_version: Uuid,
    registry: &ResearchRegistry,
    cancel: &CancellationToken,
) -> Result<BTreeMap<Key, Row>, GfError> {
    let mut rows = baseline::read(graph)?;
    // Every new baseline has the two workspace ontology fields, including an
    // empty ontology. Their presence distinguishes already migrated baselines.
    if rows.keys().any(|key| key.0 == "ontology") {
        return Ok(rows);
    }
    let Some(branch) = registry.branches.get(&context) else {
        return Ok(rows);
    };
    let base = registry
        .versions
        .get(&branch.base_version_uuid)
        .ok_or_else(|| GfError::Api {
            code: graphforge_core::ApiErrorCode::ResultNotRetained,
            message: "legacy Branch context baseline requires its retained selected base".into(),
        })?;
    let base_graph = crate::research_versions::materialize_version(owner, base)?;
    let mut inherited = Fields::new();
    super::context_fields::read(&base_graph, &mut inherited, &mut 0, cancel)?;
    let mut current = Fields::new();
    super::context_fields::read(graph, &mut current, &mut 0, cancel)?;
    let hex = |value: &[u8; 32]| -> String {
        use std::fmt::Write;
        value.iter().fold(String::new(), |mut s, b| {
            write!(s, "{b:02x}").expect("string");
            s
        })
    };
    for (key, value) in inherited {
        rows.entry(key.clone()).or_insert_with(|| Row {
            key: key.clone(),
            origin: branch.origin_version_uuid,
            incorporated: Some(branch.origin_version_uuid),
            original: hex(&value),
            baseline: hex(&value),
            current: current.get(&key).map(hex).unwrap_or_default(),
            contribution: baseline::contribution(context, branch.base_version_uuid, &key),
            role: "required".into(),
        });
    }
    for (key, value) in current {
        rows.entry(key.clone()).or_insert_with(|| Row {
            key: key.clone(),
            origin: observed_version,
            incorporated: None,
            original: hex(&value),
            baseline: String::new(),
            current: hex(&value),
            contribution: baseline::contribution(context, branch.base_version_uuid, &key),
            role: "active".into(),
        });
    }
    Ok(rows)
}

#[cfg(test)]
mod tests;
