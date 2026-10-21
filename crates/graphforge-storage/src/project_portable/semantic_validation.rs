//! Portable manifest, runtime-map, and ontology semantic validation.

use super::{
    Entry, Manifest, ManifestComponent, ManifestFile, ONTOLOGY_COMPOSITION_PATH,
    PortableCompositionEntry, PortableError, PortableErrorCode, PortableExactIdentity,
    PortableLimits, PortableOntologyComposition, PortablePackageClass, PortableReport,
    RUNTIME_MAP_PATH, RuntimeGenerationMap, UniqueValue, canonical_json, check_cancel,
    constant_time_eq, hex, read_entry_bytes_from_map, sha, validate_path,
};
use graphforge_core::hash_observation::ControlSha256 as Sha256;
use serde::Deserialize;
use serde_json::Value;
use sha2::Digest;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::AtomicBool;
use unicode_normalization::UnicodeNormalization;

pub(super) fn admit_composition_features(bytes: &[u8]) -> Result<(), PortableError> {
    let control: PortableOntologyComposition = serde_json::from_slice(bytes).map_err(|_| {
        PortableError::at(
            PortableErrorCode::InvalidStructure,
            ONTOLOGY_COMPOSITION_PATH,
            "ontology composition schema",
        )
    })?;
    if control.required_features.iter().any(|feature| {
        !matches!(
            feature.as_str(),
            "provenance-bridges@1" | "qualified-symbols@1"
        )
    }) {
        return Err(PortableError::at(
            PortableErrorCode::UnsupportedFuture,
            ONTOLOGY_COMPOSITION_PATH,
            "required ontology composition feature",
        ));
    }
    Ok(())
}

pub(super) fn validate_with_reader(
    report: &PortableReport,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
    mut read: impl FnMut(&PortableCompositionEntry) -> Result<Value, PortableError>,
) -> Result<(), PortableError> {
    let Some(control) = &report.ontology_composition else {
        return Ok(());
    };
    validate_semantic_payload_budget(report, limits)?;
    for entry in &report.ontology_composition_entries {
        check_cancel(cancelled)?;
        let value = read(entry)?;
        if entry.kind == "ontology" {
            validate_materialized_ontology(entry, value)?;
        } else {
            validate_materialized_bridge(entry, value, control)?;
        }
    }
    Ok(())
}

fn validate_semantic_payload_budget(
    report: &PortableReport,
    limits: PortableLimits,
) -> Result<(), PortableError> {
    let mut aggregate = 0_u64;
    for entry in &report.ontology_composition_entries {
        aggregate = aggregate.checked_add(entry.length).ok_or_else(|| {
            PortableError::new(
                PortableErrorCode::LimitExceeded,
                "semantic payload overflow",
            )
        })?;
        if aggregate > limits.max_manifest_bytes {
            return Err(PortableError::new(
                PortableErrorCode::LimitExceeded,
                "semantic payload budget",
            ));
        }
    }
    Ok(())
}

fn validate_materialized_ontology(
    entry: &PortableCompositionEntry,
    value: Value,
) -> Result<(), PortableError> {
    let document: graphforge_ontology::OntologyDoc =
        serde_json::from_value(value).map_err(|_| {
            PortableError::at(
                PortableErrorCode::InvalidStructure,
                &entry.path,
                "ontology module schema",
            )
        })?;
    let digest = graphforge_ontology::module_document_digest(&document).map_err(|_| {
        PortableError::at(
            PortableErrorCode::Incompatible,
            &entry.path,
            "ontology module canonicalization",
        )
    })?;
    if (!entry.identity.id.starts_with("legacy:") && document.ontology_id != entry.identity.id)
        || (!entry.identity.id.starts_with("legacy:") && document.version != entry.identity.version)
        || entry.identity.content_digest != format!("sha256:{digest}")
    {
        return Err(PortableError::at(
            PortableErrorCode::DigestMismatch,
            &entry.path,
            "ontology module semantic identity",
        ));
    }
    Ok(())
}

fn validate_materialized_bridge(
    entry: &PortableCompositionEntry,
    value: Value,
    control: &PortableOntologyComposition,
) -> Result<(), PortableError> {
    let document: graphforge_ontology::BridgeDocument =
        serde_json::from_value(value).map_err(|_| {
            PortableError::at(
                PortableErrorCode::InvalidStructure,
                &entry.path,
                "ontology bridge schema",
            )
        })?;
    let digest = graphforge_ontology::bridge_document_digest(&document).map_err(|_| {
        PortableError::at(
            PortableErrorCode::Incompatible,
            &entry.path,
            "ontology bridge canonicalization",
        )
    })?;
    let expected = control
        .bridge_sets
        .iter()
        .find(|bridge| {
            bridge.bridge_id == entry.identity.id
                && bridge.version == entry.identity.version
                && bridge.content_digest == entry.identity.content_digest
        })
        .ok_or_else(|| {
            PortableError::new(PortableErrorCode::Incompatible, "bridge control closure")
        })?;
    let exact_identity = |module: &graphforge_ontology::OntologyModuleId| PortableExactIdentity {
        id: module.ontology_id.clone(),
        version: module.authored_version.clone(),
        content_digest: format!("sha256:{}", module.canonical_digest),
    };
    let sources = document
        .source_modules
        .iter()
        .map(exact_identity)
        .collect::<Vec<_>>();
    let targets = document
        .target_modules
        .iter()
        .map(exact_identity)
        .collect::<Vec<_>>();
    if document.bridge_id != entry.identity.id
        || document.authored_version != entry.identity.version
        || entry.identity.content_digest != format!("sha256:{digest}")
        || sources != expected.source_modules
        || targets != expected.target_modules
    {
        return Err(PortableError::at(
            PortableErrorCode::DigestMismatch,
            &entry.path,
            "ontology bridge semantic identity",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub(super) fn validate_semantics(
    m: &Manifest,
    limits: PortableLimits,
) -> Result<(), PortableError> {
    if m.format != "graphforge-project/2" {
        return Err(PortableError::new(
            PortableErrorCode::UnsupportedFuture,
            "unsupported format",
        ));
    }
    if m.components.len() as u64 > limits.max_components {
        return Err(PortableError::new(
            PortableErrorCode::LimitExceeded,
            "component count",
        ));
    }
    if m.requirements.dependency_rule != "required-transitive-closure/1" {
        return Err(PortableError::new(
            PortableErrorCode::UnsupportedFuture,
            "dependency rule",
        ));
    }
    let supported = [
        "ontology@1",
        "schema@1",
        "migration@1",
        "settings@1",
        "graph-data@1",
        "derived-artifact@1",
        "evidence@1",
        "provenance@1",
        "compatibility@1",
        "ontology-composition@1",
        "research@1",
    ];
    if m.requirements
        .capabilities
        .iter()
        .any(|c| !supported.contains(&c.as_str()))
    {
        return Err(PortableError::new(
            PortableErrorCode::UnsupportedFuture,
            "capability",
        ));
    }
    unique_sorted(&m.requirements.capabilities, "capabilities")?;
    let mut ids = BTreeSet::new();
    let mut paths = BTreeSet::new();
    let mut last = None;
    for c in &m.components {
        let key = (&c.kind, &c.participant_id);
        if last >= Some(key) {
            return Err(PortableError::new(
                PortableErrorCode::Incompatible,
                "component order",
            ));
        }
        last = Some(key);
        if !valid_kind(&c.kind)
            || !valid_id(&c.participant_id)
            || super::research::is_reserved_directory(&c.participant_id)
            || !ids.insert(c.participant_id.as_str())
        {
            return Err(PortableError::new(
                PortableErrorCode::Incompatible,
                "component identity",
            ));
        }
        unique_sorted(&c.required_dependencies, "dependencies")?;
        let mut previous = None;
        for f in &c.files {
            validate_path(&f.path, limits.max_path_bytes)?;
            if previous >= Some(&f.path) || !paths.insert(f.path.as_str()) {
                return Err(PortableError::at(
                    PortableErrorCode::DuplicateEntry,
                    &f.path,
                    "file descriptor order/duplicate",
                ));
            }
            previous = Some(&f.path);
            if f.length > limits.max_entry_bytes {
                return Err(PortableError::at(
                    PortableErrorCode::LimitExceeded,
                    &f.path,
                    "file descriptor size",
                ));
            }
            if !sha(&f.sha256) || !valid_media(&f.media_type) {
                return Err(PortableError::at(
                    PortableErrorCode::Incompatible,
                    &f.path,
                    "file descriptor",
                ));
            }
        }
    }
    for c in &m.components {
        for d in &c.required_dependencies {
            if d == &c.participant_id || !ids.contains(d.as_str()) {
                return Err(PortableError::new(
                    PortableErrorCode::Incompatible,
                    "dependency closure",
                ));
            }
        }
    }
    detect_cycles(&m.components)?;
    for values in [
        &m.selection.roots,
        &m.selection.omissions,
        &m.selection.redactions,
    ] {
        unique_sorted(values, "selection")?;
    }
    if m.selection
        .roots
        .iter()
        .any(|id| !ids.contains(id.as_str()))
    {
        return Err(PortableError::new(
            PortableErrorCode::Incompatible,
            "unknown selection root",
        ));
    }
    let generation = uuid::Uuid::parse_str(&m.source_generation.generation_uuid).map_err(|_| {
        PortableError::new(PortableErrorCode::Incompatible, "source generation UUID")
    })?;
    if generation.is_nil() || !sha(&m.source_generation.manifest_sha256) {
        return Err(PortableError::new(
            PortableErrorCode::Incompatible,
            "source generation identity",
        ));
    }
    if m.states.integrity != "verified"
        || m.states.compatibility != "supported"
        || !matches!(m.states.authenticity.as_str(), "unsigned" | "not_evaluated")
    {
        return Err(PortableError::new(
            PortableErrorCode::Incompatible,
            "manifest state declaration",
        ));
    }
    match (&m.package_class[..], &m.selection.graph_subset) {
        ("graph-data-subset", Some(s))
            if !s.selector.is_empty()
                && matches!(
                    s.closure.as_str(),
                    "selected-only" | "induced-edges" | "referential"
                ) => {}
        ("graph-data-subset", _) | (_, Some(_)) => {
            return Err(PortableError::new(
                PortableErrorCode::Incompatible,
                "graph subset/class mismatch",
            ));
        }
        _ => {}
    }
    match m.package_class.as_str() {
        "complete" if !m.selection.omissions.is_empty() || !m.selection.redactions.is_empty() => {
            return Err(PortableError::new(
                PortableErrorCode::Incompatible,
                "complete package has omissions/redactions",
            ));
        }
        "ontology-only"
            if m.components.iter().any(|component| {
                !matches!(
                    component.kind.as_str(),
                    "ontology" | "schema" | "compatibility"
                )
            }) =>
        {
            return Err(PortableError::new(
                PortableErrorCode::Incompatible,
                "ontology-only package contains another kind",
            ));
        }
        "graph-data-subset"
            if !m
                .components
                .iter()
                .any(|component| component.kind == "graph-data") =>
        {
            return Err(PortableError::new(
                PortableErrorCode::Incompatible,
                "graph subset has no graph-data component",
            ));
        }
        _ => {}
    }
    package_class(&m.package_class)?;
    Ok(())
}

pub(super) fn validate_runtime_map(
    map: &BTreeMap<&str, &Entry>,
    manifest: &Manifest,
    limits: PortableLimits,
) -> Result<(), PortableError> {
    let Some(descriptor) = runtime_map_descriptor(manifest)? else {
        return Ok(());
    };
    if descriptor.media_type != "application/vnd.graphforge.runtime-generation+json"
        || descriptor.length > limits.max_manifest_bytes
    {
        return Err(PortableError::at(
            PortableErrorCode::Incompatible,
            &descriptor.path,
            "runtime map descriptor",
        ));
    }
    let bytes = read_entry_bytes_from_map(map, RUNTIME_MAP_PATH)?;
    let (value, runtime) = decode_runtime_map(&bytes)?;
    if canonical_json(&value).map_err(|_| {
        PortableError::at(
            PortableErrorCode::Incompatible,
            RUNTIME_MAP_PATH,
            "runtime map canonicalization",
        )
    })? != bytes
        || runtime.contract != "graphforge-runtime-generation-map/1"
        || runtime.participants.len() as u64 > limits.max_components
        || runtime.capabilities.len() > 256
    {
        return Err(PortableError::at(
            PortableErrorCode::Incompatible,
            RUNTIME_MAP_PATH,
            "runtime map contract",
        ));
    }
    validate_runtime_map_contents(&runtime, manifest)
}

pub(super) fn validate_ontology_composition(
    map: &BTreeMap<&str, &Entry>,
    manifest: &Manifest,
    limits: PortableLimits,
) -> Result<
    (
        Option<PortableOntologyComposition>,
        Vec<PortableCompositionEntry>,
    ),
    PortableError,
> {
    let required = manifest
        .requirements
        .capabilities
        .iter()
        .any(|capability| capability == "ontology-composition@1");
    let component = manifest.components.iter().find(|component| {
        component.kind == "compatibility"
            && component.participant_id == "graphforge-ontology-composition"
    });
    if required != component.is_some() {
        return Err(PortableError::new(
            PortableErrorCode::Incompatible,
            "ontology composition capability/component mismatch",
        ));
    }
    let Some(component) = component else {
        return Ok((None, Vec::new()));
    };
    if component.files.len() != 1
        || component.files[0].path != ONTOLOGY_COMPOSITION_PATH
        || component.files[0].media_type != "application/vnd.graphforge.ontology-composition+json"
        || component.files[0].length > limits.max_manifest_bytes
    {
        return Err(PortableError::at(
            PortableErrorCode::Incompatible,
            ONTOLOGY_COMPOSITION_PATH,
            "ontology composition descriptor",
        ));
    }
    let bytes = read_entry_bytes_from_map(map, ONTOLOGY_COMPOSITION_PATH)?;
    let value = UniqueValue::deserialize(&mut serde_json::Deserializer::from_slice(&bytes))
        .map_err(|_| {
            PortableError::at(
                PortableErrorCode::InvalidStructure,
                ONTOLOGY_COMPOSITION_PATH,
                "ontology composition JSON",
            )
        })?
        .0;
    if canonical_json(&value).map_err(|_| {
        PortableError::at(
            PortableErrorCode::Incompatible,
            ONTOLOGY_COMPOSITION_PATH,
            "ontology composition canonicalization",
        )
    })? != bytes
    {
        return Err(PortableError::at(
            PortableErrorCode::Incompatible,
            ONTOLOGY_COMPOSITION_PATH,
            "ontology composition is noncanonical",
        ));
    }
    let control: PortableOntologyComposition =
        serde_json::from_value(value.clone()).map_err(|_| {
            PortableError::at(
                PortableErrorCode::InvalidStructure,
                ONTOLOGY_COMPOSITION_PATH,
                "ontology composition schema",
            )
        })?;
    validate_ontology_composition_contents(&control, component, limits)?;
    let mut unsigned = value;
    unsigned
        .as_object_mut()
        .ok_or_else(|| PortableError::new(PortableErrorCode::InvalidStructure, "composition"))?
        .remove("composition_digest");
    let mut digest = Sha256::new();
    digest.update(b"graphforge-ontology-composition/1\0");
    digest.update(canonical_json(&unsigned).map_err(|_| {
        PortableError::new(PortableErrorCode::Incompatible, "composition identity")
    })?);
    let expected = format!("sha256:{}", hex(&digest.finalize()));
    if !constant_time_eq(expected.as_bytes(), control.composition_digest.as_bytes()) {
        return Err(PortableError::at(
            PortableErrorCode::DigestMismatch,
            ONTOLOGY_COMPOSITION_PATH,
            "composition digest",
        ));
    }
    let entries = validate_ontology_component_descriptors(&control, manifest)?;
    Ok((Some(control), entries))
}

/// The manifest components whose single file is an authored composition input:
/// an ontology module or a bridge set. These are exactly the entries
/// [`validate_ontology_component_descriptors`] reports, which the import reads
/// back to build the staged composition candidate and the composition
/// authority, so a generated control re-encodes them and nothing else.
const ONTOLOGY_MODULE_COMPONENT: (&str, &str) = ("ontology", "ontology-module-");
const ONTOLOGY_BRIDGE_COMPONENT: (&str, &str) = ("schema", "ontology-bridge-");

pub(super) fn is_composition_input(kind: &str, participant_id: &str) -> bool {
    [ONTOLOGY_MODULE_COMPONENT, ONTOLOGY_BRIDGE_COMPONENT]
        .into_iter()
        .any(|(input_kind, prefix)| kind == input_kind && participant_id.starts_with(prefix))
}

fn validate_ontology_component_descriptors(
    control: &PortableOntologyComposition,
    manifest: &Manifest,
) -> Result<Vec<PortableCompositionEntry>, PortableError> {
    let mut entries = Vec::new();
    for (kind, identity, component_id, file_name, media_type) in control
        .modules
        .iter()
        .map(|module| {
            let identity = PortableExactIdentity {
                id: module.ontology_id.clone(),
                version: module.version.clone(),
                content_digest: module.content_digest.clone(),
            };
            (
                ONTOLOGY_MODULE_COMPONENT.0,
                identity,
                format!(
                    "{}{}",
                    ONTOLOGY_MODULE_COMPONENT.1,
                    module.content_digest.trim_start_matches("sha256:")
                ),
                "module.json",
                "application/vnd.graphforge.ontology+json",
            )
        })
        .chain(control.bridge_sets.iter().map(|bridge| {
            let identity = PortableExactIdentity {
                id: bridge.bridge_id.clone(),
                version: bridge.version.clone(),
                content_digest: bridge.content_digest.clone(),
            };
            (
                ONTOLOGY_BRIDGE_COMPONENT.0,
                identity,
                format!(
                    "{}{}",
                    ONTOLOGY_BRIDGE_COMPONENT.1,
                    bridge.content_digest.trim_start_matches("sha256:")
                ),
                "bridge.json",
                "application/vnd.graphforge.ontology-bridge+json",
            )
        }))
    {
        let component = manifest
            .components
            .iter()
            .find(|component| component.kind == kind && component.participant_id == component_id)
            .ok_or_else(|| {
                PortableError::new(
                    PortableErrorCode::Incompatible,
                    "missing ontology composition payload component",
                )
            })?;
        let expected_path = format!("data/components/{kind}/{component_id}/{file_name}");
        if component.files.len() != 1
            || component.files[0].path != expected_path
            || component.files[0].media_type != media_type
        {
            return Err(PortableError::at(
                PortableErrorCode::Incompatible,
                &expected_path,
                "ontology composition payload descriptor",
            ));
        }
        let file = &component.files[0];
        entries.push(PortableCompositionEntry {
            kind: kind.into(),
            identity,
            path: file.path.clone(),
            media_type: file.media_type.clone(),
            length: file.length,
            sha256: file.sha256.clone(),
            required_dependencies: component.required_dependencies.clone(),
        });
    }
    entries.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
    Ok(entries)
}

#[expect(
    clippy::too_many_lines,
    reason = "keeps the closed composition contract and its closure checks auditable together"
)]
fn validate_ontology_composition_contents(
    control: &PortableOntologyComposition,
    component: &ManifestComponent,
    limits: PortableLimits,
) -> Result<(), PortableError> {
    if control.contract != "graphforge-ontology-composition/1"
        || control.modules.len() as u64 > limits.max_components
        || control.bridge_sets.len() as u64 > limits.max_components
        || control.activation_profile.overrides.len() as u64
            > limits.max_components.saturating_mul(2)
        || !matches!(
            control.activation_profile.profile_default.as_str(),
            "exploratory" | "advisory" | "strict"
        )
    {
        return Err(PortableError::new(
            PortableErrorCode::LimitExceeded,
            "ontology composition contract or bound",
        ));
    }
    let modules = control
        .modules
        .iter()
        .map(|module| {
            validate_exact_fields(&module.ontology_id, &module.version, &module.content_digest)?;
            if module.dialect != "graphforge-ontology"
                || !matches!(
                    module.profile.as_str(),
                    "exploratory" | "advisory" | "strict"
                )
            {
                return Err(PortableError::new(
                    PortableErrorCode::Incompatible,
                    "ontology module metadata",
                ));
            }
            Ok((
                module.ontology_id.as_str(),
                module.version.as_str(),
                module.content_digest.as_str(),
            ))
        })
        .collect::<Result<Vec<_>, PortableError>>()?;
    if modules.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(PortableError::new(
            PortableErrorCode::Incompatible,
            "ontology module order",
        ));
    }
    let bridges = control
        .bridge_sets
        .iter()
        .map(|bridge| {
            validate_exact_fields(&bridge.bridge_id, &bridge.version, &bridge.content_digest)?;
            validate_identity_list(&bridge.source_modules, &modules)?;
            validate_identity_list(&bridge.target_modules, &modules)?;
            Ok((
                bridge.bridge_id.as_str(),
                bridge.version.as_str(),
                bridge.content_digest.as_str(),
            ))
        })
        .collect::<Result<Vec<_>, PortableError>>()?;
    if bridges.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(PortableError::new(
            PortableErrorCode::Incompatible,
            "bridge set order",
        ));
    }
    let overrides = &control.activation_profile.overrides;
    let mut prior = None;
    for activation in overrides {
        let identity = exact_tuple(&activation.subject)?;
        let scope = activation.scope.as_str();
        if !matches!(scope, "module" | "bridge")
            || !matches!(
                activation.mode.as_str(),
                "exploratory" | "advisory" | "strict"
            )
            || (scope == "module" && !modules.contains(&identity))
            || (scope == "bridge" && !bridges.contains(&identity))
        {
            return Err(PortableError::new(
                PortableErrorCode::Incompatible,
                "activation closure",
            ));
        }
        let key = (scope, identity, activation.mode.as_str());
        if prior >= Some(key) {
            return Err(PortableError::new(
                PortableErrorCode::Incompatible,
                "activation order",
            ));
        }
        prior = Some(key);
    }
    for features in [&control.required_features, &control.optional_features] {
        unique_sorted(features, "ontology composition features")?;
        if features.len() > 256 || features.iter().any(|value| !valid_feature(value)) {
            return Err(PortableError::new(
                PortableErrorCode::UnsupportedFuture,
                "ontology composition feature",
            ));
        }
    }
    if control.required_features.iter().any(|feature| {
        !matches!(
            feature.as_str(),
            "provenance-bridges@1" | "qualified-symbols@1"
        )
    }) {
        return Err(PortableError::new(
            PortableErrorCode::UnsupportedFuture,
            "required ontology composition feature",
        ));
    }
    let dependency_ids = component
        .required_dependencies
        .iter()
        .collect::<BTreeSet<_>>();
    let expected_dependencies = control
        .modules
        .iter()
        .map(|module| {
            format!(
                "ontology-module-{}",
                module.content_digest.trim_start_matches("sha256:")
            )
        })
        .chain(control.bridge_sets.iter().map(|bridge| {
            format!(
                "ontology-bridge-{}",
                bridge.content_digest.trim_start_matches("sha256:")
            )
        }))
        .collect::<BTreeSet<_>>();
    if dependency_ids.len() != component.required_dependencies.len()
        || dependency_ids != expected_dependencies.iter().collect::<BTreeSet<_>>()
    {
        return Err(PortableError::new(
            PortableErrorCode::Incompatible,
            "ontology composition dependency closure",
        ));
    }
    Ok(())
}

fn validate_identity_list<'a>(
    values: &'a [PortableExactIdentity],
    modules: &[(&'a str, &'a str, &'a str)],
) -> Result<(), PortableError> {
    if values.is_empty() {
        return Err(PortableError::new(
            PortableErrorCode::Incompatible,
            "empty bridge endpoint",
        ));
    }
    let identities = values
        .iter()
        .map(exact_tuple)
        .collect::<Result<Vec<_>, _>>()?;
    if identities.windows(2).any(|pair| pair[0] >= pair[1])
        || identities
            .iter()
            .any(|identity| !modules.contains(identity))
    {
        return Err(PortableError::new(
            PortableErrorCode::Incompatible,
            "bridge endpoint closure",
        ));
    }
    Ok(())
}

fn exact_tuple(value: &PortableExactIdentity) -> Result<(&str, &str, &str), PortableError> {
    validate_exact_fields(&value.id, &value.version, &value.content_digest)?;
    Ok((&value.id, &value.version, &value.content_digest))
}

fn validate_exact_fields(id: &str, version: &str, digest: &str) -> Result<(), PortableError> {
    let scheme = id.find(':').unwrap_or_default();
    if id.len() > 2048
        || scheme == 0
        || !id[..scheme].bytes().enumerate().all(|(index, byte)| {
            if index == 0 {
                byte.is_ascii_alphabetic()
            } else {
                byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'.' | b'-')
            }
        })
        || id[scheme + 1..].is_empty()
        || id.chars().any(char::is_whitespace)
        || id.nfc().ne(id.chars())
        || version.is_empty()
        || version.len() > 256
        || version.chars().any(char::is_control)
        || version.nfc().ne(version.chars())
        || !digest.strip_prefix("sha256:").is_some_and(sha)
    {
        return Err(PortableError::new(
            PortableErrorCode::Incompatible,
            "exact ontology identity",
        ));
    }
    Ok(())
}

fn valid_feature(value: &str) -> bool {
    let Some((name, version)) = value.rsplit_once('@') else {
        return false;
    };
    valid_id(name)
        && version
            .as_bytes()
            .first()
            .is_some_and(|byte| byte.is_ascii_digit() && *byte != b'0')
        && version.bytes().all(|byte| byte.is_ascii_digit())
}

fn runtime_map_descriptor(manifest: &Manifest) -> Result<Option<&ManifestFile>, PortableError> {
    let runtime_files = manifest
        .components
        .iter()
        .flat_map(|component| &component.files);
    let descriptor = runtime_files
        .clone()
        .find(|file| file.path == RUNTIME_MAP_PATH);
    if descriptor.is_none()
        && runtime_files
            .clone()
            .any(|file| file.media_type == "application/vnd.graphforge.runtime-generation+json")
    {
        return Err(PortableError::at(
            PortableErrorCode::Incompatible,
            RUNTIME_MAP_PATH,
            "runtime map is at the wrong path",
        ));
    }
    let Some(descriptor) = descriptor else {
        return Ok(None);
    };
    Ok(Some(descriptor))
}

fn validate_runtime_map_contents(
    runtime: &RuntimeGenerationMap,
    manifest: &Manifest,
) -> Result<(), PortableError> {
    let component_ids: BTreeSet<_> = manifest
        .components
        .iter()
        .map(|component| component.participant_id.as_str())
        .collect();
    validate_research_runtime(runtime, manifest)?;
    let mut prior = None;
    let mut runtime_ids = BTreeSet::new();
    for participant in &runtime.participants {
        if prior >= Some(participant.participant_id.as_str())
            || !runtime_ids.insert(participant.participant_id.as_str())
            || !component_ids.contains(participant.participant_id.as_str())
            || participant.capability_version == 0
            || participant.record_version == 0
            || !matches!(participant.encoding.as_str(), "json" | "parquet" | "arrow")
            || !valid_runtime_id(&participant.capability_id)
            || !valid_runtime_id(&participant.record_family_id)
            || participant.schema_fingerprint.len() != 64
            || !participant
                .schema_fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(PortableError::at(
                PortableErrorCode::Incompatible,
                RUNTIME_MAP_PATH,
                "runtime participant mapping",
            ));
        }
        let _ = participant.row_count;
        prior = Some(participant.participant_id.as_str());
    }
    let mut prior_capability = None;
    for capability in &runtime.capabilities {
        if prior_capability >= Some(capability.capability_id.as_str())
            || capability.capability_version == 0
            || !valid_runtime_id(&capability.capability_id)
        {
            return Err(PortableError::at(
                PortableErrorCode::Incompatible,
                RUNTIME_MAP_PATH,
                "runtime capability mapping",
            ));
        }
        prior_capability = Some(capability.capability_id.as_str());
    }
    if let Some(graph) = &runtime.graph_tree
        && (graph.component_id != "graph-tree"
            || !component_ids.contains(graph.component_id.as_str())
            || !runtime_ids.contains(graph.inventory_participant_id.as_str()))
    {
        return Err(PortableError::at(
            PortableErrorCode::Incompatible,
            RUNTIME_MAP_PATH,
            "runtime graph placement",
        ));
    }
    Ok(())
}

fn validate_research_runtime(
    runtime: &RuntimeGenerationMap,
    manifest: &Manifest,
) -> Result<(), PortableError> {
    let research_parts: Vec<_> = runtime
        .participants
        .iter()
        .filter(|p| p.capability_id == "research")
        .collect();
    let research_components: Vec<_> = manifest
        .components
        .iter()
        .filter(|c| c.kind == "research")
        .collect();
    let research_caps: Vec<_> = runtime
        .capabilities
        .iter()
        .filter(|c| c.capability_id == "research")
        .collect();
    let declares_research = manifest
        .requirements
        .capabilities
        .iter()
        .any(|c| c == "research@1");
    if !research_parts.is_empty()
        || !research_components.is_empty()
        || !research_caps.is_empty()
        || declares_research
    {
        if research_parts.len() != 1
            || research_components.len() != 2
            || research_caps.len() != 1
            || !declares_research
        {
            return Err(PortableError::new(
                PortableErrorCode::Incompatible,
                "research component/runtime mismatch",
            ));
        }
        let part = research_parts[0];
        if part.record_family_id != "registry"
            // Revision 6 packages (older clients' exports) are admitted as-is.
            || !crate::research_versions::research_revision_readable(part.capability_version)
            || part.record_version != part.capability_version
            || research_caps[0].capability_version != part.capability_version
            || !research_components
                .iter()
                .any(|c| c.participant_id == part.participant_id)
            || !research_components
                .iter()
                .any(|c| c.participant_id == "research-content")
        {
            return Err(PortableError::new(
                PortableErrorCode::UnsupportedFuture,
                "research runtime contract",
            ));
        }
    }
    Ok(())
}

pub(crate) fn decode_runtime_map(
    bytes: &[u8],
) -> Result<(Value, RuntimeGenerationMap), PortableError> {
    let value = UniqueValue::deserialize(&mut serde_json::Deserializer::from_slice(bytes))
        .map_err(|_| {
            PortableError::at(
                PortableErrorCode::Incompatible,
                RUNTIME_MAP_PATH,
                "runtime map JSON",
            )
        })?
        .0;
    let runtime = serde_json::from_value(value.clone()).map_err(|_| {
        PortableError::at(
            PortableErrorCode::Incompatible,
            RUNTIME_MAP_PATH,
            "runtime map schema",
        )
    })?;
    Ok((value, runtime))
}
fn valid_runtime_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.bytes().enumerate().all(|(index, byte)| {
            if index == 0 {
                byte.is_ascii_lowercase()
            } else {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'-' | b'_' | b'.')
            }
        })
}

pub(super) fn package_class(s: &str) -> Result<PortablePackageClass, PortableError> {
    match s {
        "complete" => Ok(PortablePackageClass::Complete),
        "ontology-only" => Ok(PortablePackageClass::OntologyOnly),
        "component-selective" => Ok(PortablePackageClass::ComponentSelective),
        "graph-data-subset" => Ok(PortablePackageClass::GraphDataSubset),
        _ => Err(PortableError::new(
            PortableErrorCode::UnsupportedFuture,
            "package class",
        )),
    }
}
fn valid_kind(s: &str) -> bool {
    matches!(
        s,
        "ontology"
            | "schema"
            | "migration"
            | "settings"
            | "graph-data"
            | "derived-artifact"
            | "evidence"
            | "provenance"
            | "compatibility"
            | "research"
    )
}
fn valid_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 256
        && s.bytes().next().is_some_and(|b| b.is_ascii_lowercase())
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b))
}

fn valid_media(s: &str) -> bool {
    s.len() <= 255
        && s.split_once('/').is_some()
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"!#$&^_.+-/".contains(&b))
}
fn unique_sorted(v: &[String], _: &'static str) -> Result<(), PortableError> {
    if v.windows(2).any(|w| w[0] >= w[1]) {
        Err(PortableError::new(
            PortableErrorCode::Incompatible,
            "unordered/duplicate list",
        ))
    } else {
        Ok(())
    }
}
fn detect_cycles(c: &[ManifestComponent]) -> Result<(), PortableError> {
    fn visit<'a>(
        id: &'a str,
        m: &BTreeMap<&'a str, &'a ManifestComponent>,
        vis: &mut BTreeSet<&'a str>,
        done: &mut BTreeSet<&'a str>,
    ) -> bool {
        if done.contains(id) {
            return false;
        }
        if !vis.insert(id) {
            return true;
        }
        let cycle = m.get(id).is_some_and(|x| {
            x.required_dependencies
                .iter()
                .any(|d| visit(d, m, vis, done))
        });
        vis.remove(id);
        done.insert(id);
        cycle
    }
    let m = c.iter().map(|x| (x.participant_id.as_str(), x)).collect();
    let mut v = BTreeSet::new();
    let mut d = BTreeSet::new();
    if c.iter()
        .any(|x| visit(&x.participant_id, &m, &mut v, &mut d))
    {
        Err(PortableError::new(
            PortableErrorCode::Incompatible,
            "dependency cycle",
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
