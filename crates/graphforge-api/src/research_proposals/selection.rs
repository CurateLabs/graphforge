//! Retain selected owner records and redact graph fields before freezing payloads.
use super::{SubmitResearchProposalRequest, identity, invalid, key};
use crate::{
    CancellationToken, GfError, GraphForge,
    branches::{baseline, fields, publication},
};
use graphforge_storage::research_versions::{PreparedResearchContent, RegisterResearchVersion};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

pub(super) struct Selected {
    pub prepared: PreparedResearchContent,
    pub baseline: BTreeMap<fields::Key, baseline::Row>,
    pub values: fields::Fields,
}

pub(super) fn freeze(
    owner: &GraphForge,
    command: &publication::Command,
    request: &SubmitResearchProposalRequest,
    cancellation: &CancellationToken,
) -> Result<Selected, GfError> {
    let selected = crate::slices::branch::authenticate(owner, &request.frozen_ipc, cancellation)?;
    if selected.version.version_uuid != request.source_version_uuid
        || selected.version.context_uuid != request.source_branch_uuid
    {
        return Err(invalid(
            "Proposal Slice does not identify the exact source Branch Version",
        ));
    }
    let keys: BTreeSet<_> = request.fields.iter().map(key).collect();
    let baseline = baseline::read(&selected.view)?;
    let values = fields::read_selected(
        &selected.view,
        Some(&selected.active.union(&selected.required).cloned().collect()),
        cancellation,
    )?;
    for key in &keys {
        if !values.contains_key(key) && !baseline.contains_key(key) {
            return Err(invalid(
                "Proposal field is absent from both source content and its baseline",
            ));
        }
        if !(key.0.starts_with("ontology")
            || selected.active.contains(&(key.0.clone(), key.1))
            || !values.contains_key(key) && baseline.contains_key(key))
        {
            return Err(invalid(
                "Proposal field is not explicit active Slice membership",
            ));
        }
    }
    for object in &selected.active {
        if !keys
            .iter()
            .any(|key| (&key.0, key.1) == (&object.0, object.1))
        {
            return Err(invalid(
                "Proposal Slice includes an object without selected fields",
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
    let mut spec = RegisterResearchVersion {
        version_uuid: identity(request.operation_uuid, "payload"),
        context_uuid: identity(request.operation_uuid, "payload_context"),
        source_generation_uuid: selected.version.content.generation_uuid,
        selection: None,
        source_version: Some(request.source_version_uuid),
        required_versions: BTreeSet::new(),
        label: None,
        description: None,
        created_at: request.created_at,
        evidence: selected.evidence.clone(),
    };
    let prepared = crate::branches::selection::prepare(command, selected, &mut spec, cancellation)?;
    let mut view = crate::branches::private_view::open(owner, &prepared)?;
    view.read_only = false;
    crate::branches::field_application::redact_properties(&view, &keys, cancellation)?;
    let mut frozen = graphforge_storage::research_versions::prepare_branch_content(
        &command.root,
        &view.generation_for_read()?,
        prepared.version.clone(),
        cancellation.flag(),
    )?;
    let baseline: BTreeMap<_, _> = baseline
        .into_iter()
        .filter(|(key, _)| keys.contains(key))
        .collect();
    baseline::install(&command.root, &mut frozen, &baseline, cancellation)?;
    let proof = crate::branches::private_view::open(owner, &frozen)?;
    let frozen_values = fields::read(&proof, cancellation)?;
    for key in &keys {
        if frozen_values.get(key) != values.get(key) {
            return Err(invalid(
                "selected Proposal field changed during private freezing",
            ));
        }
    }
    Ok(Selected {
        prepared: frozen,
        baseline,
        values: values
            .into_iter()
            .filter(|(key, _)| keys.contains(key))
            .collect(),
    })
}

pub(super) fn item_identity(proposal: Uuid, unit: &fields::Key) -> Result<Uuid, GfError> {
    let encoded = serde_json::to_string(unit).map_err(|_| invalid("invalid Proposal unit"))?;
    Ok(identity(proposal, &encoded))
}

pub(super) fn digest(value: &str) -> Result<Option<[u8; 32]>, GfError> {
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() != 64 {
        return Err(invalid("invalid native Branch field commitment"));
    }
    let mut digest = [0; 32];
    for (index, bytes) in value.as_bytes().chunks_exact(2).enumerate() {
        let byte = std::str::from_utf8(bytes).map_err(|_| invalid("invalid field commitment"))?;
        digest[index] =
            u8::from_str_radix(byte, 16).map_err(|_| invalid("invalid field commitment"))?;
    }
    Ok(Some(digest))
}
