//! Shared native selected-field preparation for review and interchange.
use super::{baseline, fields};
use crate::{CancellationToken, GfError, GraphForge, ResearchFieldIdentity};
use graphforge_storage::research_versions::{PreparedResearchContent, RegisterResearchVersion};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

pub(crate) struct SelectedFields {
    pub prepared: PreparedResearchContent,
    pub baseline: BTreeMap<fields::Key, baseline::Row>,
    pub values: fields::Fields,
}
fn invalid(message: &str) -> GfError {
    GfError::Validation(message.into())
}

pub(crate) fn freeze(
    owner: &GraphForge,
    root: &Path,
    selected: crate::slices::branch::BranchSelection,
    fields: &[ResearchFieldIdentity],
    spec: &mut RegisterResearchVersion,
    cancellation: &CancellationToken,
) -> Result<SelectedFields, GfError> {
    let keys: BTreeSet<_> = fields
        .iter()
        .map(|f| (f.object_kind.clone(), f.object_uuid, f.field.clone()))
        .collect();
    let baseline = baseline::read(&selected.view)?;
    let values = fields::read_selected(
        &selected.view,
        Some(&selected.active.union(&selected.required).cloned().collect()),
        cancellation,
    )?;
    for key in &keys {
        if !values.contains_key(key) && !baseline.contains_key(key) {
            return Err(invalid(
                "research field is absent from both source content and its baseline",
            ));
        }
        if !(key.0.starts_with("ontology")
            || selected.active.contains(&(key.0.clone(), key.1))
            || !values.contains_key(key) && baseline.contains_key(key))
        {
            return Err(invalid(
                "research field is not explicit active Slice membership",
            ));
        }
    }
    for object in &selected.active {
        if !keys
            .iter()
            .any(|key| (&key.0, key.1) == (&object.0, object.1))
        {
            return Err(invalid(
                "research Slice includes an object without selected fields",
            ));
        }
        // Immutable domain rows are atomic. Review may select whole records and
        // choose among records, but cannot rewrite a fraction of an assertion.
        if !matches!(object.0.as_str(), "node" | "edge")
            && values
                .keys()
                .any(|key| (&key.0, key.1) == (&object.0, object.1) && !keys.contains(key))
        {
            return Err(invalid(
                "immutable research records require all their native fields explicitly selected",
            ));
        }
    }
    let prepared = crate::branches::selection::prepare(root, selected, spec, cancellation)?;
    let mut view = crate::branches::private_view::open(owner, &prepared)?;
    view.read_only = false;
    super::field_application::redact_properties(&view, &keys, cancellation)?;
    let mut frozen = graphforge_storage::research_versions::prepare_branch_content(
        root,
        &view.generation_for_read()?,
        prepared.version.clone(),
        cancellation.flag(),
    )?;
    let baseline: BTreeMap<_, _> = baseline
        .into_iter()
        .filter(|(key, _)| keys.contains(key))
        .collect();
    baseline::install(root, &mut frozen, &baseline, cancellation)?;
    graphforge_storage::research_versions::canonicalize_prepared_research_projection(
        root,
        &mut frozen,
    )?;
    let proof = crate::branches::private_view::open(owner, &frozen)?;
    let frozen_values = fields::read(&proof, cancellation)?;
    for key in &keys {
        if frozen_values.get(key) != values.get(key) {
            return Err(invalid(
                "selected research field changed during private freezing",
            ));
        }
    }
    Ok(SelectedFields {
        prepared: frozen,
        baseline,
        values: values
            .into_iter()
            .filter(|(key, _)| keys.contains(key))
            .collect(),
    })
}
