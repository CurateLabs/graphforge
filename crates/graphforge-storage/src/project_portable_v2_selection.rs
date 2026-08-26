//! Deterministic, content-safe portable-v2 component selection.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::Read as _;

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    PortableV2Error, PortableV2ErrorCode, PortableV2ExactIdentity, PortableV2Limits,
    ResolvedProjectGeneration, WORKSPACE_CAPABILITY_ID, WORKSPACE_CONFIGURATION_FAMILY,
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
    /// Exact projected ontology module or bridge identities. The immutable
    /// preview exposes the complete emitted composition closure.
    OntologyComposition(Vec<PortableV2ExactIdentity>),
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
    /// Required exact multi-ontology composition closure.
    RequiredOntologyComposition,
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

/// Exact ontology module or bridge emitted by composition projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableV2ProjectedSelectionEntry {
    /// Projected component kind (`ontology` or `schema`).
    pub kind: String,
    /// Exact semantic identity addressable by callers.
    pub identity: PortableV2ExactIdentity,
    /// Stable component identity used by the portable manifest.
    pub participant_id: String,
    /// Whether this exact identity was directly requested or closure-added.
    pub reason: PortableV2SelectionReason,
    /// Exact canonical projected payload bytes.
    pub estimated_bytes: u64,
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
    /// Exact projected ontology closure in canonical identity order.
    pub projected: Vec<PortableV2ProjectedSelectionEntry>,
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
            PortableV2SelectionProfile::OntologyComposition(_) => {
                identity.capability_id == crate::WORKSPACE_CAPABILITY_ID
                    && identity.record_family_id == crate::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY
            }
        };
        if selected {
            requested.insert(identity);
        }
    }
    if requested.is_empty()
        && matches!(
            request.profile,
            PortableV2SelectionProfile::Custom(_)
                | PortableV2SelectionProfile::OntologyComposition(_)
        )
    {
        return Err(incompatible("selection matched no portable components"));
    }

    let exact_composition = matches!(
        request.profile,
        PortableV2SelectionProfile::OntologyComposition(_)
    );
    let needs_schema = !exact_composition
        && requested.iter().any(|identity| {
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
    let mut auto = if needs_schema {
        schema
            .difference(&requested)
            .cloned()
            .collect::<BTreeSet<_>>()
    } else {
        BTreeSet::new()
    };
    let needs_composition = requested.iter().any(|identity| {
        matches!(
            component_kind(&identity.capability_id, &identity.record_family_id),
            "ontology" | "graph-data" | "derived-artifact"
        )
    });
    let composition = PortableV2ParticipantId {
        capability_id: crate::WORKSPACE_CAPABILITY_ID.to_owned(),
        record_family_id: crate::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY.to_owned(),
    };
    if needs_composition && available.contains(&composition) && !requested.contains(&composition) {
        auto.insert(composition.clone());
    }
    if request.strict && !auto.is_empty() {
        return Err(incompatible(if auto.contains(&composition) {
            "strict selection requires undeclared ontology composition closure"
        } else {
            "strict selection requires undeclared schema authority"
        }));
    }
    let selected = requested.union(&auto).cloned().collect::<BTreeSet<_>>();
    let semantic_bindings = PortableV2ParticipantId {
        capability_id: crate::GRAPH_CAPABILITY_ID.to_owned(),
        record_family_id: crate::GRAPH_SEMANTIC_BINDINGS_FAMILY.to_owned(),
    };
    if selected.contains(&semantic_bindings) {
        crate::semantic_storage_bindings(generation).map_err(storage)?;
    }
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
                if identity == composition {
                    PortableV2SelectionReason::RequiredOntologyComposition
                } else {
                    PortableV2SelectionReason::RequiredSchemaAuthority
                }
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
        | PortableV2SelectionProfile::Custom(_)
        | PortableV2SelectionProfile::OntologyComposition(_) => "component-selective",
    }
    .to_owned();
    let projected = projected_composition_entries(generation, request, &selected, limits)?;
    total = projected.iter().try_fold(total, |sum, entry| {
        sum.checked_add(entry.estimated_bytes)
            .ok_or_else(|| limit("selection byte overflow"))
    })?;
    if total > limits.max_total_bytes {
        return Err(limit("selection bytes exceed limit"));
    }
    let mut plan = PortableV2SelectionPlan {
        source_generation_uuid: generation.generation_uuid().hyphenated().to_string(),
        source_manifest_sha256: format!("sha256:{}", hex(generation.manifest_sha256())),
        package_class,
        include_graph_tree: included.iter().any(|entry| entry.kind == "graph-data"),
        included,
        excluded,
        projected,
        redactions: Vec::new(),
        required_capabilities,
        estimated_payload_bytes: total,
        selection_fingerprint: String::new(),
    };
    plan.selection_fingerprint = fingerprint(&plan)?;
    Ok(plan)
}

#[expect(
    clippy::too_many_lines,
    reason = "keeps exact identity validation and transitive module/bridge closure in one audit path"
)]
fn projected_composition_entries(
    generation: &ResolvedProjectGeneration,
    request: &PortableV2SelectionRequest,
    selected: &BTreeSet<PortableV2ParticipantId>,
    limits: PortableV2Limits,
) -> Result<Vec<PortableV2ProjectedSelectionEntry>, PortableV2Error> {
    let authority = PortableV2ParticipantId {
        capability_id: crate::WORKSPACE_CAPABILITY_ID.into(),
        record_family_id: crate::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY.into(),
    };
    if !selected.contains(&authority) {
        return Ok(Vec::new());
    }
    let path = generation
        .participant_path(&authority.capability_id, &authority.record_family_id)
        .map_err(storage)?;
    let mut file = File::open(path).map_err(storage_io)?;
    let cap = limits.max_manifest_bytes.min(limits.max_entry_bytes);
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take(cap.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(storage_io)?;
    if bytes.len() as u64 > cap {
        return Err(limit("composition preview exceeds limit"));
    }
    let composition =
        crate::WorkspaceOntologyComposition::from_canonical_json(&bytes).map_err(storage)?;
    let requested = match &request.profile {
        PortableV2SelectionProfile::OntologyComposition(values) => {
            let set = values.iter().cloned().collect::<BTreeSet<_>>();
            if set.len() != values.len() {
                return Err(incompatible("duplicate projected ontology selector"));
            }
            Some(set)
        }
        _ => None,
    };
    let module_identity = |module: &crate::WorkspaceCompositionModule| PortableV2ExactIdentity {
        id: module.id.ontology_id.clone(),
        version: module.id.authored_version.clone(),
        content_digest: format!("sha256:{}", module.id.canonical_digest),
    };
    let bridge_identity = |bridge: &graphforge_ontology::BridgeDocument| {
        let digest = graphforge_ontology::bridge_document_digest(bridge)
            .map_err(|_| incompatible("composition bridge digest"))?;
        Ok::<_, PortableV2Error>((
            PortableV2ExactIdentity {
                id: bridge.bridge_id.clone(),
                version: bridge.authored_version.clone(),
                content_digest: format!("sha256:{digest}"),
            },
            digest,
        ))
    };
    let module_by_identity = composition
        .modules
        .iter()
        .map(|module| (module_identity(module), module))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut bridge_by_identity = std::collections::BTreeMap::new();
    for bridge in &composition.bridges {
        let (identity, digest) = bridge_identity(bridge)?;
        bridge_by_identity.insert(identity, (bridge, digest));
    }
    let mut closure = requested.clone().unwrap_or_else(|| {
        module_by_identity
            .keys()
            .chain(bridge_by_identity.keys())
            .cloned()
            .collect()
    });
    if requested.as_ref().is_some_and(|roots| {
        roots.iter().any(|identity| {
            !module_by_identity.contains_key(identity) && !bridge_by_identity.contains_key(identity)
        })
    }) {
        return Err(incompatible("projected ontology selector is absent"));
    }
    loop {
        let before = closure.len();
        for identity in closure.clone() {
            if let Some(module) = module_by_identity.get(&identity) {
                closure.extend(module.dependencies.iter().map(|dependency| {
                    PortableV2ExactIdentity {
                        id: dependency.ontology_id.clone(),
                        version: dependency.authored_version.clone(),
                        content_digest: format!("sha256:{}", dependency.canonical_digest),
                    }
                }));
            } else if let Some((bridge, _)) = bridge_by_identity.get(&identity) {
                closure.extend(
                    bridge
                        .source_modules
                        .iter()
                        .chain(&bridge.target_modules)
                        .map(|module| PortableV2ExactIdentity {
                            id: module.ontology_id.clone(),
                            version: module.authored_version.clone(),
                            content_digest: format!("sha256:{}", module.canonical_digest),
                        }),
                );
                closure.extend(bridge.dependencies.iter().map(|dependency| {
                    PortableV2ExactIdentity {
                        id: dependency.bridge_id.clone(),
                        version: dependency.authored_version.clone(),
                        content_digest: format!("sha256:{}", dependency.canonical_digest),
                    }
                }));
            }
        }
        if closure.len() == before {
            break;
        }
    }
    if closure.iter().any(|identity| {
        !module_by_identity.contains_key(identity) && !bridge_by_identity.contains_key(identity)
    }) {
        return Err(incompatible(
            "projected ontology dependency closure is missing",
        ));
    }
    let mut entries = Vec::new();
    for module in &composition.modules {
        let identity = PortableV2ExactIdentity {
            id: module.id.ontology_id.clone(),
            version: module.id.authored_version.clone(),
            content_digest: format!("sha256:{}", module.id.canonical_digest),
        };
        if !closure.contains(&identity) {
            continue;
        }
        let payload = crate::project_portable_v2::canonical_json(
            &serde_json::to_value(&module.document)
                .map_err(|_| incompatible("composition preview serialization"))?,
        )?;
        entries.push(PortableV2ProjectedSelectionEntry {
            kind: "ontology".into(),
            participant_id: format!("ontology-module-{}", module.id.canonical_digest),
            reason: if requested
                .as_ref()
                .is_some_and(|set| set.contains(&identity))
            {
                PortableV2SelectionReason::Requested
            } else {
                PortableV2SelectionReason::RequiredOntologyComposition
            },
            identity,
            estimated_bytes: payload.len() as u64,
        });
    }
    for bridge in &composition.bridges {
        let digest = graphforge_ontology::bridge_document_digest(bridge)
            .map_err(|_| incompatible("composition bridge digest"))?;
        let identity = PortableV2ExactIdentity {
            id: bridge.bridge_id.clone(),
            version: bridge.authored_version.clone(),
            content_digest: format!("sha256:{digest}"),
        };
        if !closure.contains(&identity) {
            continue;
        }
        let payload = crate::project_portable_v2::canonical_json(
            &serde_json::to_value(bridge)
                .map_err(|_| incompatible("composition preview serialization"))?,
        )?;
        entries.push(PortableV2ProjectedSelectionEntry {
            kind: "schema".into(),
            participant_id: format!("ontology-bridge-{digest}"),
            reason: if requested
                .as_ref()
                .is_some_and(|set| set.contains(&identity))
            {
                PortableV2SelectionReason::Requested
            } else {
                PortableV2SelectionReason::RequiredOntologyComposition
            },
            identity,
            estimated_bytes: payload.len() as u64,
        });
    }
    entries.sort_by(|left, right| left.participant_id.cmp(&right.participant_id));
    Ok(entries)
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

pub(crate) fn fingerprint(plan: &PortableV2SelectionPlan) -> Result<String, PortableV2Error> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn semantic_generation(
        composition: Option<crate::WorkspaceOntologyComposition>,
        binding_fingerprint: String,
    ) -> (tempfile::TempDir, crate::ResolvedProjectGeneration) {
        let root = tempfile::tempdir().unwrap();
        let parent = crate::open_or_initialize_project(root.path()).unwrap();
        let mut participants = crate::empty_workspace_participants().unwrap();
        participants.push(
            crate::SemanticStorageBindings::new(binding_fingerprint, Vec::new())
                .unwrap()
                .to_project_participant()
                .unwrap(),
        );
        if let Some(composition) = composition {
            participants.push(composition.to_project_participant().unwrap());
        }
        participants.sort_by(|left, right| {
            (&left.capability_id, &left.record_family_id)
                .cmp(&(&right.capability_id, &right.record_family_id))
        });
        let request = crate::ProjectGenerationRequest {
            transaction_uuid: Uuid::new_v4(),
            generation_uuid: Uuid::new_v4(),
            capabilities: vec![
                crate::ProjectCapability {
                    capability_id: crate::GRAPH_CAPABILITY_ID.into(),
                    capability_version: crate::GRAPH_CAPABILITY_VERSION,
                },
                crate::ProjectCapability {
                    capability_id: crate::WORKSPACE_CAPABILITY_ID.into(),
                    capability_version: crate::WORKSPACE_CAPABILITY_VERSION,
                },
            ],
            participants,
        };
        let crate::ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation(root.path(), &request).unwrap()
        else {
            panic!("fresh semantic generation replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        drop(parent);
        let generation = crate::resolve_project_generation(root.path()).unwrap();
        (root, generation)
    }

    fn composition() -> crate::WorkspaceOntologyComposition {
        let legacy = crate::WorkspaceOntology {
            contract_version: 1,
            mode: crate::WorkspaceOntologyMode::Strict,
            source_format: Some(crate::WorkspaceOntologySourceFormat::Json),
            canonical_ontology_sha256: Some("a".repeat(64)),
            canonical_ontology: Some(
                serde_json::to_value(graphforge_ontology::OntologyDoc {
                    ontology_id: "https://graphforge.dev/ontology/selection-closure".into(),
                    version: "1".into(),
                    entity_types: Vec::new(),
                    relation_types: Vec::new(),
                    properties: Vec::new(),
                    constraints: Vec::new(),
                    migrations: Vec::new(),
                })
                .unwrap(),
            ),
        };
        crate::WorkspaceOntologyComposition::virtual_legacy(&legacy)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn semantic_selection_fails_closed_without_matching_composition_authority() {
        for (authority, fingerprint) in [
            (None, "b".repeat(64)),
            (Some(composition()), "c".repeat(64)),
        ] {
            let (_root, generation) = semantic_generation(authority, fingerprint);
            let error = preview_portable_v2_selection(
                &generation,
                &PortableV2SelectionRequest {
                    profile: PortableV2SelectionProfile::Complete,
                    strict: false,
                },
                PortableV2Limits::default(),
            )
            .unwrap_err();
            assert_eq!(error.code, PortableV2ErrorCode::Io);
        }
    }
}
