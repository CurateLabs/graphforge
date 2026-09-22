//! Selection validates the exact preview before any private mutation is prepared.
use super::{
    ResearchUpstreamResolution, ResearchUpstreamSelection, UpdateResearchBranchRequest, invalid,
    preview::{self, Preview},
};
use crate::{GfError, branches::fields};
use std::collections::BTreeMap;

pub(super) fn validate(
    preview: &Preview,
    request: &UpdateResearchBranchRequest,
) -> Result<BTreeMap<fields::Key, ResearchUpstreamResolution>, GfError> {
    if preview.current.generation_uuid() != request.expected_generation_uuid
        || preview.digest != request.preview_sha256
    {
        return Err(GfError::Project {
            code: graphforge_core::ProjectErrorCode::WriteConflict,
            message: "upstream preview is stale; review the current Branch and upstream state"
                .into(),
        });
    }
    if request.version_uuid.is_nil()
        || request.actor_uuid.is_nil()
        || request.explanation.len() > 4096
        || request.acknowledge_evidence.len() > 256
    {
        return Err(invalid(
            "upstream update requires non-nil Version and actor identities and bounded review metadata",
        ));
    }
    let available: BTreeMap<_, _> = preview
        .rows
        .iter()
        .filter(|row| row.change != "dependency_unavailable")
        .map(|row| (row.key.clone(), row))
        .collect();
    let mut selected = BTreeMap::new();
    match &request.selection {
        ResearchUpstreamSelection::AllCompatible => {
            for (key, row) in &available {
                if matches!(row.change, "upstream" | "equivalent") {
                    selected.insert(key.clone(), ResearchUpstreamResolution::AdoptUpstream);
                }
            }
        }
        ResearchUpstreamSelection::Selected { decisions } => {
            if decisions.is_empty() || decisions.len() > 256 {
                return Err(invalid(
                    "upstream selection requires 1..256 explicit decisions",
                ));
            }
            for decision in decisions {
                let key = preview::key(&decision.unit);
                if !available.contains_key(&key)
                    || selected.insert(key, decision.resolution.clone()).is_some()
                {
                    return Err(invalid(
                        "each upstream decision must identify a distinct exact preview field",
                    ));
                }
            }
        }
    }
    if matches!(request.selection, ResearchUpstreamSelection::AllCompatible) {
        retain_compatible(preview, &mut selected);
    }
    let mut needed_evidence = std::collections::BTreeSet::new();
    for (key, resolution) in &selected {
        if matches!(
            resolution,
            ResearchUpstreamResolution::KeepLocal | ResearchUpstreamResolution::Explain { .. }
        ) {
            continue;
        }
        if let Some(requirements) = preview.requirements.get(key) {
            for dependency in &requirements.fields {
                if !matches!(
                    selected.get(dependency),
                    Some(ResearchUpstreamResolution::AdoptUpstream)
                ) {
                    return Err(invalid(
                        "selected upstream value requires an unselected or locally retained dependency; review and adopt the exact required fields",
                    ));
                }
            }
            needed_evidence.extend(requirements.evidence.iter().copied());
        }
    }
    if needed_evidence != request.acknowledge_evidence {
        return Err(invalid(
            "upstream update must acknowledge exactly the evidence gaps of its adopted selection",
        ));
    }
    if selected.is_empty() || selected.len() > 256 {
        return Err(invalid(
            "upstream update requires 1..256 reviewed fields; narrow the preview scope",
        ));
    }
    Ok(selected)
}

fn retain_compatible(
    preview: &Preview,
    selected: &mut BTreeMap<fields::Key, ResearchUpstreamResolution>,
) {
    // A compatible item with a conflicting or unselected prerequisite is not compatible as a whole.
    loop {
        let blocked: Vec<_> = selected
            .keys()
            .filter(|key| {
                preview.requirements.get(*key).is_some_and(|requirements| {
                    requirements
                        .fields
                        .iter()
                        .any(|dependency| !selected.contains_key(dependency))
                })
            })
            .cloned()
            .collect();
        if blocked.is_empty() {
            break;
        }
        for key in blocked {
            selected.remove(&key);
        }
    }
}
