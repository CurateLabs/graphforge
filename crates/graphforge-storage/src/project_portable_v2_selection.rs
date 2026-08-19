//! Deterministic, content-safe portable-v2 component selection.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::Read as _;

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    PortableV2Error, PortableV2ErrorCode, PortableV2Limits, ResolvedProjectGeneration,
    WORKSPACE_CAPABILITY_ID, WORKSPACE_CONFIGURATION_FAMILY,
};

/// Stable semantic participant identity. Runtime catalog IDs and host paths are never selectors.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PortableV2ParticipantId {
    /// Owning portable capability contract.
    pub capability_id: String,
    /// Stable record-family contract.
    pub record_family_id: String,
}

/// Built-in deterministic selection profiles.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PortableV2SelectionProfile {
    /// Every committed participant and graph-tree payload.
    Complete,
    /// Authored/adopted ontology plus required schema participants.
    OntologyOnly,
    /// Whole graph/data components. Row or subgraph selection belongs to #786.
    DataComponents,
    /// Derived and repository artifact participants.
    Artifacts,
    /// Closed-schema portable settings only.
    Settings,
    /// Explicit stable identities.
    Custom(Vec<PortableV2ParticipantId>),
}

/// Selection planning request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortableV2SelectionRequest {
    /// Requested built-in or custom profile.
    pub profile: PortableV2SelectionProfile,
    /// Refuse any automatically required dependency instead of widening visibly.
    pub strict: bool,
}

/// Stable reason for inclusion/exclusion.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PortableV2SelectionReason {
    /// Directly requested by profile or exact identity.
    Requested,
    /// Required ontology/schema closure.
    RequiredSchemaAuthority,
    /// Not part of the requested profile.
    ProfileExcluded,
}

/// Content-free preview row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableV2SelectionEntry {
    /// Stable semantic identity.
    pub identity: PortableV2ParticipantId,
    /// Canonical component kind.
    pub kind: String,
    /// Stable selection reason.
    pub reason: PortableV2SelectionReason,
    /// Exact committed payload bytes.
    pub estimated_bytes: u64,
    /// Manifest row count.
    pub row_count: u64,
}

/// Immutable deterministic preview consumed by both export representations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableV2SelectionPlan {
    /// Pinned source generation identity.
    pub source_generation_uuid: String,
    /// Pinned source generation manifest identity.
    pub source_manifest_sha256: String,
    /// Portable package class token.
    pub package_class: String,
    /// Included participants in canonical identity order.
    pub included: Vec<PortableV2SelectionEntry>,
    /// Excluded participants in canonical identity order.
    pub excluded: Vec<PortableV2SelectionEntry>,
    /// Explicit redaction reason codes; values are never retained.
    pub redactions: Vec<String>,
    /// Required portable capability contracts.
    pub required_capabilities: Vec<String>,
    /// Exact known participant bytes, excluding bounded control metadata.
    pub estimated_payload_bytes: u64,
    /// Stable digest over canonical content-free plan metadata.
    pub selection_fingerprint: String,
    pub(crate) include_graph_tree: bool,
}

impl PortableV2SelectionPlan {
    pub(crate) fn includes(&self, capability: &str, family: &str) -> bool {
        self.included.iter().any(|entry| {
            entry.identity.capability_id == capability && entry.identity.record_family_id == family
        })
    }
}

/// Resolve one bounded deterministic selection without retaining payload values.
#[expect(
    clippy::too_many_lines,
    reason = "keeps identity matching, dependency closure, and fingerprinting in one audit path"
)]
pub fn preview_portable_v2_selection(
    generation: &ResolvedProjectGeneration,
    request: &PortableV2SelectionRequest,
    limits: PortableV2Limits,
) -> Result<PortableV2SelectionPlan, PortableV2Error> {
    let descriptors = generation.participant_descriptors().map_err(storage)?;
    if descriptors.len() as u64 > limits.max_components {
        return Err(limit("selection component count exceeds limit"));
    }
    let custom = match &request.profile {
        PortableV2SelectionProfile::Custom(values) => {
            let set = values.iter().cloned().collect::<BTreeSet<_>>();
            if set.len() != values.len() {
                return Err(incompatible("duplicate custom selector"));
            }
            Some(set)
        }
        _ => None,
    };
    let available = descriptors
        .iter()
        .map(|descriptor| PortableV2ParticipantId {
            capability_id: descriptor.capability_id.clone(),
            record_family_id: descriptor.record_family_id.clone(),
        })
        .collect::<BTreeSet<_>>();
    if custom.as_ref().is_some_and(|selectors| {
        selectors
            .iter()
            .any(|selector| !available.contains(selector))
    }) {
        return Err(incompatible("custom selector is missing or ambiguous"));
    }

    let mut requested = BTreeSet::new();
    for descriptor in &descriptors {
        let identity = PortableV2ParticipantId {
            capability_id: descriptor.capability_id.clone(),
            record_family_id: descriptor.record_family_id.clone(),
        };
        let kind = component_kind(&identity.capability_id, &identity.record_family_id);
        let selected = match &request.profile {
            PortableV2SelectionProfile::Complete => true,
            PortableV2SelectionProfile::OntologyOnly => kind == "ontology",
            PortableV2SelectionProfile::DataComponents => kind == "graph-data",
            PortableV2SelectionProfile::Artifacts => kind == "derived-artifact",
            PortableV2SelectionProfile::Settings => kind == "settings",
            PortableV2SelectionProfile::Custom(_) => custom.as_ref().unwrap().contains(&identity),
        };
        if selected {
            requested.insert(identity);
        }
    }
    if requested.is_empty() && matches!(request.profile, PortableV2SelectionProfile::Custom(_)) {
        return Err(incompatible("selection matched no portable components"));
    }

    let needs_schema = requested.iter().any(|identity| {
        matches!(
            component_kind(&identity.capability_id, &identity.record_family_id),
            "ontology" | "graph-data" | "derived-artifact"
        )
    });
    let schema = descriptors
        .iter()
        .filter(|descriptor| {
            component_kind(&descriptor.capability_id, &descriptor.record_family_id) == "schema"
        })
        .map(|descriptor| PortableV2ParticipantId {
            capability_id: descriptor.capability_id.clone(),
            record_family_id: descriptor.record_family_id.clone(),
        })
        .collect::<BTreeSet<_>>();
    let auto = if needs_schema {
        schema
            .difference(&requested)
            .cloned()
            .collect::<BTreeSet<_>>()
    } else {
        BTreeSet::new()
    };
    if request.strict && !auto.is_empty() {
        return Err(incompatible(
            "strict selection requires undeclared schema authority",
        ));
    }
    let selected = requested.union(&auto).cloned().collect::<BTreeSet<_>>();
    validate_settings(generation, &selected, limits)?;

    let mut included = Vec::new();
    let mut excluded = Vec::new();
    let mut total = 0_u64;
    for descriptor in descriptors {
        let identity = PortableV2ParticipantId {
            capability_id: descriptor.capability_id.clone(),
            record_family_id: descriptor.record_family_id.clone(),
        };
        let path = generation
            .participant_path(&identity.capability_id, &identity.record_family_id)
            .map_err(storage)?;
        let bytes = std::fs::metadata(path).map_err(storage_io)?.len();
        let is_selected = selected.contains(&identity);
        if is_selected {
            total = total
                .checked_add(bytes)
                .ok_or_else(|| limit("selection byte overflow"))?;
            if total > limits.max_total_bytes {
                return Err(limit("selection bytes exceed limit"));
            }
        }
        let entry = PortableV2SelectionEntry {
            kind: component_kind(&identity.capability_id, &identity.record_family_id).into(),
            reason: if auto.contains(&identity) {
                PortableV2SelectionReason::RequiredSchemaAuthority
            } else if is_selected {
                PortableV2SelectionReason::Requested
            } else {
                PortableV2SelectionReason::ProfileExcluded
            },
            identity,
            estimated_bytes: bytes,
            row_count: descriptor.row_count,
        };
        if is_selected {
            included.push(entry);
        } else {
            excluded.push(entry);
        }
    }
    included.sort_by(|a, b| a.identity.cmp(&b.identity));
    excluded.sort_by(|a, b| a.identity.cmp(&b.identity));
    let required_capabilities = if matches!(request.profile, PortableV2SelectionProfile::Complete) {
        generation
            .capabilities()
            .into_iter()
            .map(|capability| capability.capability_id)
            .collect()
    } else {
        included
            .iter()
            .map(|entry| entry.identity.capability_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    };
    let package_class = match request.profile {
        PortableV2SelectionProfile::Complete => "complete",
        PortableV2SelectionProfile::OntologyOnly => "ontology-only",
        PortableV2SelectionProfile::DataComponents
        | PortableV2SelectionProfile::Artifacts
        | PortableV2SelectionProfile::Settings
        | PortableV2SelectionProfile::Custom(_) => "component-selective",
    }
    .to_owned();
    let mut plan = PortableV2SelectionPlan {
        source_generation_uuid: generation.generation_uuid().hyphenated().to_string(),
        source_manifest_sha256: format!("sha256:{}", hex(generation.manifest_sha256())),
        package_class,
        include_graph_tree: included.iter().any(|entry| entry.kind == "graph-data"),
        included,
        excluded,
        redactions: Vec::new(),
        required_capabilities,
        estimated_payload_bytes: total,
        selection_fingerprint: String::new(),
    };
    plan.selection_fingerprint = fingerprint(&plan)?;
    Ok(plan)
}

pub(crate) fn validate_selection_plan(
    generation: &ResolvedProjectGeneration,
    plan: &PortableV2SelectionPlan,
) -> Result<(), PortableV2Error> {
    if plan.source_generation_uuid != generation.generation_uuid().hyphenated().to_string()
        || plan.source_manifest_sha256 != format!("sha256:{}", hex(generation.manifest_sha256()))
        || plan.selection_fingerprint != fingerprint(plan)?
    {
        return Err(incompatible("selection plan identity mismatch"));
    }
    Ok(())
}

fn fingerprint(plan: &PortableV2SelectionPlan) -> Result<String, PortableV2Error> {
    let mut unsigned = plan.clone();
    unsigned.selection_fingerprint.clear();
    let bytes = crate::project_portable_v2::canonical_json(
        &serde_json::to_value(unsigned).map_err(|_| incompatible("selection serialization"))?,
    )?;
    let mut digest = Sha256::new();
    digest.update(b"graphforge-portable-selection/1\0");
    digest.update(bytes);
    Ok(format!("sha256:{}", hex(digest.finalize().into())))
}

pub(crate) fn component_kind(capability: &str, family: &str) -> &'static str {
    if capability == crate::GRAPH_CAPABILITY_ID {
        "graph-data"
    } else if family.contains("ontology") {
        "ontology"
    } else if family.contains("schema") {
        "schema"
    } else if family.contains("artifact") || family.contains("repository_snapshot") {
        "derived-artifact"
    } else {
        "settings"
    }
}

fn validate_settings(
    generation: &ResolvedProjectGeneration,
    selected: &BTreeSet<PortableV2ParticipantId>,
    limits: PortableV2Limits,
) -> Result<(), PortableV2Error> {
    let identity = PortableV2ParticipantId {
        capability_id: WORKSPACE_CAPABILITY_ID.into(),
        record_family_id: WORKSPACE_CONFIGURATION_FAMILY.into(),
    };
    if !selected.contains(&identity) {
        return Ok(());
    }
    let path = generation
        .participant_path(&identity.capability_id, &identity.record_family_id)
        .map_err(storage)?;
    let mut file = File::open(path).map_err(storage_io)?;
    let cap = limits.max_manifest_bytes.min(limits.max_entry_bytes);
    let mut bytes = Vec::new();
    file.by_ref()
        .take(cap + 1)
        .read_to_end(&mut bytes)
        .map_err(storage_io)?;
    if bytes.len() as u64 > cap {
        return Err(limit("settings validation state exceeds limit"));
    }
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| incompatible("settings JSON"))?;
    validate_setting_value(None, &value)
}

fn validate_setting_value(key: Option<&str>, value: &Value) -> Result<(), PortableV2Error> {
    if key.is_some_and(|key| {
        let key = key.to_ascii_lowercase();
        ["secret", "password", "token", "credential", "private_key"]
            .iter()
            .any(|needle| key.contains(needle))
    }) {
        return Err(incompatible("secret-bearing setting is not portable"));
    }
    match value {
        Value::String(value)
            if value.starts_with('/')
                || value.starts_with("\\\\")
                || value.as_bytes().get(1) == Some(&b':') =>
        {
            Err(incompatible("absolute host path setting is not portable"))
        }
        Value::Array(values) => values
            .iter()
            .try_for_each(|value| validate_setting_value(key, value)),
        Value::Object(values) => values
            .iter()
            .try_for_each(|(key, value)| validate_setting_value(Some(key), value)),
        _ => Ok(()),
    }
}

fn storage(_: crate::GfError) -> PortableV2Error {
    PortableV2Error::new(PortableV2ErrorCode::Io, "selection inventory unavailable")
}
fn storage_io(_: std::io::Error) -> PortableV2Error {
    PortableV2Error::new(PortableV2ErrorCode::Io, "selection entry unavailable")
}
fn incompatible(detail: &'static str) -> PortableV2Error {
    PortableV2Error::new(PortableV2ErrorCode::Incompatible, detail)
}
fn limit(detail: &'static str) -> PortableV2Error {
    PortableV2Error::new(PortableV2ErrorCode::LimitExceeded, detail)
}
fn hex(digest: [u8; 32]) -> String {
    use std::fmt::Write as _;
    digest
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        })
}
