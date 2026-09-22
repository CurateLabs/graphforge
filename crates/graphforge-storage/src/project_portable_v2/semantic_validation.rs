//! Portable manifest, runtime-map, and ontology semantic validation.

use super::{
    Entry, Manifest, ManifestComponent, ManifestFile, ONTOLOGY_COMPOSITION_PATH,
    PortableV2CompositionEntry, PortableV2Error, PortableV2ErrorCode, PortableV2ExactIdentity,
    PortableV2Limits, PortableV2OntologyComposition, PortableV2PackageClass, PortableV2Report,
    RUNTIME_MAP_PATH, RuntimeGenerationMap, UniqueValue, canonical_json, check_cancel,
    constant_time_eq, hex, read_entry_bytes_from_map, sha, validate_path,
};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use unicode_normalization::UnicodeNormalization;

pub(super) fn admit_composition_features(bytes: &[u8]) -> Result<(), PortableV2Error> {
    let control: PortableV2OntologyComposition = serde_json::from_slice(bytes).map_err(|_| {
        PortableV2Error::at(
            PortableV2ErrorCode::InvalidStructure,
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
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::UnsupportedFuture,
            ONTOLOGY_COMPOSITION_PATH,
            "required ontology composition feature",
        ));
    }
    Ok(())
}

pub(crate) fn validate_materialized_ontology_composition(
    root: &Path,
    report: &PortableV2Report,
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
) -> Result<(), PortableV2Error> {
    let Some(control) = &report.ontology_composition else {
        return Ok(());
    };
    validate_semantic_payload_budget(report, limits)?;
    for entry in &report.ontology_composition_entries {
        check_cancel(cancelled)?;
        let value = read_canonical_semantic_payload(root, entry)?;
        if entry.kind == "ontology" {
            validate_materialized_ontology(entry, value)?;
        } else {
            validate_materialized_bridge(entry, value, control)?;
        }
    }
    Ok(())
}

fn validate_semantic_payload_budget(
    report: &PortableV2Report,
    limits: PortableV2Limits,
) -> Result<(), PortableV2Error> {
    let mut aggregate = 0_u64;
    for entry in &report.ontology_composition_entries {
        aggregate = aggregate.checked_add(entry.length).ok_or_else(|| {
            PortableV2Error::new(
                PortableV2ErrorCode::LimitExceeded,
                "semantic payload overflow",
            )
        })?;
        if aggregate > limits.max_manifest_bytes {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::LimitExceeded,
                "semantic payload budget",
            ));
        }
    }
    Ok(())
}

fn read_canonical_semantic_payload(
    root: &Path,
    entry: &PortableV2CompositionEntry,
) -> Result<Value, PortableV2Error> {
    let path = root.join(&entry.path);
    let metadata = fs::symlink_metadata(&path).map_err(|_| {
        PortableV2Error::at(
            PortableV2ErrorCode::InvalidStructure,
            &entry.path,
            "semantic payload unavailable",
        )
    })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() != entry.length {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::InvalidStructure,
            &entry.path,
            "semantic payload is not the authenticated regular file",
        ));
    }
    let length = usize::try_from(entry.length).map_err(|_| {
        PortableV2Error::new(PortableV2ErrorCode::LimitExceeded, "semantic payload size")
    })?;
    let mut file =
        crate::project_portable_v2_export::open_source_no_follow(&path).map_err(|_| {
            PortableV2Error::at(
                PortableV2ErrorCode::InvalidStructure,
                &entry.path,
                "cannot open semantic payload",
            )
        })?;
    let mut bytes = Vec::with_capacity(length);
    std::io::Read::by_ref(&mut file)
        .take(entry.length.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| {
            PortableV2Error::at(
                PortableV2ErrorCode::Io,
                &entry.path,
                "cannot read semantic payload",
            )
        })?;
    if bytes.len() != length {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::ConcurrentMutation,
            &entry.path,
            "semantic payload changed",
        ));
    }
    let value = serde_json::from_slice(&bytes).map_err(|_| {
        PortableV2Error::at(
            PortableV2ErrorCode::InvalidStructure,
            &entry.path,
            "semantic payload JSON",
        )
    })?;
    if canonical_json(&value)? != bytes {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::Incompatible,
            &entry.path,
            "semantic payload is noncanonical",
        ));
    }
    Ok(value)
}

fn validate_materialized_ontology(
    entry: &PortableV2CompositionEntry,
    value: Value,
) -> Result<(), PortableV2Error> {
    let document: graphforge_ontology::OntologyDoc =
        serde_json::from_value(value).map_err(|_| {
            PortableV2Error::at(
                PortableV2ErrorCode::InvalidStructure,
                &entry.path,
                "ontology module schema",
            )
        })?;
    let digest = graphforge_ontology::module_document_digest(&document).map_err(|_| {
        PortableV2Error::at(
            PortableV2ErrorCode::Incompatible,
            &entry.path,
            "ontology module canonicalization",
        )
    })?;
    if (!entry.identity.id.starts_with("legacy:") && document.ontology_id != entry.identity.id)
        || (!entry.identity.id.starts_with("legacy:") && document.version != entry.identity.version)
        || entry.identity.content_digest != format!("sha256:{digest}")
    {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::DigestMismatch,
            &entry.path,
            "ontology module semantic identity",
        ));
    }
    Ok(())
}

fn validate_materialized_bridge(
    entry: &PortableV2CompositionEntry,
    value: Value,
    control: &PortableV2OntologyComposition,
) -> Result<(), PortableV2Error> {
    let document: graphforge_ontology::BridgeDocument =
        serde_json::from_value(value).map_err(|_| {
            PortableV2Error::at(
                PortableV2ErrorCode::InvalidStructure,
                &entry.path,
                "ontology bridge schema",
            )
        })?;
    let digest = graphforge_ontology::bridge_document_digest(&document).map_err(|_| {
        PortableV2Error::at(
            PortableV2ErrorCode::Incompatible,
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
            PortableV2Error::new(PortableV2ErrorCode::Incompatible, "bridge control closure")
        })?;
    let exact_identity = |module: &graphforge_ontology::OntologyModuleId| PortableV2ExactIdentity {
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
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::DigestMismatch,
            &entry.path,
            "ontology bridge semantic identity",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub(super) fn validate_semantics(
    m: &Manifest,
    limits: PortableV2Limits,
) -> Result<(), PortableV2Error> {
    if m.format != "graphforge-project/2" {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::UnsupportedFuture,
            "unsupported format",
        ));
    }
    if m.components.len() as u64 > limits.max_components {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::LimitExceeded,
            "component count",
        ));
    }
    if m.requirements.dependency_rule != "required-transitive-closure/1" {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::UnsupportedFuture,
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
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::UnsupportedFuture,
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
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Incompatible,
                "component order",
            ));
        }
        last = Some(key);
        if !valid_kind(&c.kind)
            || !valid_id(&c.participant_id)
            || !ids.insert(c.participant_id.as_str())
        {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Incompatible,
                "component identity",
            ));
        }
        unique_sorted(&c.required_dependencies, "dependencies")?;
        let mut previous = None;
        for f in &c.files {
            validate_path(&f.path, limits.max_path_bytes)?;
            if previous >= Some(&f.path) || !paths.insert(f.path.as_str()) {
                return Err(PortableV2Error::at(
                    PortableV2ErrorCode::DuplicateEntry,
                    &f.path,
                    "file descriptor order/duplicate",
                ));
            }
            previous = Some(&f.path);
            if f.length > limits.max_entry_bytes {
                return Err(PortableV2Error::at(
                    PortableV2ErrorCode::LimitExceeded,
                    &f.path,
                    "file descriptor size",
                ));
            }
            if !sha(&f.sha256) || !valid_media(&f.media_type) {
                return Err(PortableV2Error::at(
                    PortableV2ErrorCode::Incompatible,
                    &f.path,
                    "file descriptor",
                ));
            }
        }
    }
    for c in &m.components {
        for d in &c.required_dependencies {
            if d == &c.participant_id || !ids.contains(d.as_str()) {
                return Err(PortableV2Error::new(
                    PortableV2ErrorCode::Incompatible,
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
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "unknown selection root",
        ));
    }
    let generation = uuid::Uuid::parse_str(&m.source_generation.generation_uuid).map_err(|_| {
        PortableV2Error::new(PortableV2ErrorCode::Incompatible, "source generation UUID")
    })?;
    if generation.is_nil() || !sha(&m.source_generation.manifest_sha256) {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "source generation identity",
        ));
    }
    if m.states.integrity != "verified"
        || m.states.compatibility != "supported"
        || !matches!(m.states.authenticity.as_str(), "unsigned" | "not_evaluated")
    {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
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
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Incompatible,
                "graph subset/class mismatch",
            ));
        }
        _ => {}
    }
    match m.package_class.as_str() {
        "complete" if !m.selection.omissions.is_empty() || !m.selection.redactions.is_empty() => {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Incompatible,
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
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Incompatible,
                "ontology-only package contains another kind",
            ));
        }
        "graph-data-subset"
            if !m
                .components
                .iter()
                .any(|component| component.kind == "graph-data") =>
        {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Incompatible,
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
    limits: PortableV2Limits,
) -> Result<(), PortableV2Error> {
    let Some(descriptor) = runtime_map_descriptor(manifest)? else {
        return Ok(());
    };
    if descriptor.media_type != "application/vnd.graphforge.runtime-generation+json"
        || descriptor.length > limits.max_manifest_bytes
    {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::Incompatible,
            &descriptor.path,
            "runtime map descriptor",
        ));
    }
    let bytes = read_entry_bytes_from_map(map, RUNTIME_MAP_PATH)?;
    let (value, runtime) = decode_runtime_map(&bytes)?;
    if canonical_json(&value).map_err(|_| {
        PortableV2Error::at(
            PortableV2ErrorCode::Incompatible,
            RUNTIME_MAP_PATH,
            "runtime map canonicalization",
        )
    })? != bytes
        || runtime.contract != "graphforge-runtime-generation-map/1"
        || runtime.participants.len() as u64 > limits.max_components
        || runtime.capabilities.len() > 256
    {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::Incompatible,
            RUNTIME_MAP_PATH,
            "runtime map contract",
        ));
    }
    validate_runtime_map_contents(&runtime, manifest)
}

pub(super) fn validate_ontology_composition(
    map: &BTreeMap<&str, &Entry>,
    manifest: &Manifest,
    limits: PortableV2Limits,
) -> Result<
    (
        Option<PortableV2OntologyComposition>,
        Vec<PortableV2CompositionEntry>,
    ),
    PortableV2Error,
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
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
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
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::Incompatible,
            ONTOLOGY_COMPOSITION_PATH,
            "ontology composition descriptor",
        ));
    }
    let bytes = read_entry_bytes_from_map(map, ONTOLOGY_COMPOSITION_PATH)?;
    let value = UniqueValue::deserialize(&mut serde_json::Deserializer::from_slice(&bytes))
        .map_err(|_| {
            PortableV2Error::at(
                PortableV2ErrorCode::InvalidStructure,
                ONTOLOGY_COMPOSITION_PATH,
                "ontology composition JSON",
            )
        })?
        .0;
    if canonical_json(&value).map_err(|_| {
        PortableV2Error::at(
            PortableV2ErrorCode::Incompatible,
            ONTOLOGY_COMPOSITION_PATH,
            "ontology composition canonicalization",
        )
    })? != bytes
    {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::Incompatible,
            ONTOLOGY_COMPOSITION_PATH,
            "ontology composition is noncanonical",
        ));
    }
    let control: PortableV2OntologyComposition =
        serde_json::from_value(value.clone()).map_err(|_| {
            PortableV2Error::at(
                PortableV2ErrorCode::InvalidStructure,
                ONTOLOGY_COMPOSITION_PATH,
                "ontology composition schema",
            )
        })?;
    validate_ontology_composition_contents(&control, component, limits)?;
    let mut unsigned = value;
    unsigned
        .as_object_mut()
        .ok_or_else(|| PortableV2Error::new(PortableV2ErrorCode::InvalidStructure, "composition"))?
        .remove("composition_digest");
    let mut digest = Sha256::new();
    digest.update(b"graphforge-ontology-composition/1\0");
    digest.update(canonical_json(&unsigned).map_err(|_| {
        PortableV2Error::new(PortableV2ErrorCode::Incompatible, "composition identity")
    })?);
    let expected = format!("sha256:{}", hex(&digest.finalize()));
    if !constant_time_eq(expected.as_bytes(), control.composition_digest.as_bytes()) {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::DigestMismatch,
            ONTOLOGY_COMPOSITION_PATH,
            "composition digest",
        ));
    }
    let entries = validate_ontology_component_descriptors(&control, manifest)?;
    Ok((Some(control), entries))
}

fn validate_ontology_component_descriptors(
    control: &PortableV2OntologyComposition,
    manifest: &Manifest,
) -> Result<Vec<PortableV2CompositionEntry>, PortableV2Error> {
    let mut entries = Vec::new();
    for (kind, identity, component_id, file_name, media_type) in control
        .modules
        .iter()
        .map(|module| {
            let identity = PortableV2ExactIdentity {
                id: module.ontology_id.clone(),
                version: module.version.clone(),
                content_digest: module.content_digest.clone(),
            };
            (
                "ontology",
                identity,
                format!(
                    "ontology-module-{}",
                    module.content_digest.trim_start_matches("sha256:")
                ),
                "module.json",
                "application/vnd.graphforge.ontology+json",
            )
        })
        .chain(control.bridge_sets.iter().map(|bridge| {
            let identity = PortableV2ExactIdentity {
                id: bridge.bridge_id.clone(),
                version: bridge.version.clone(),
                content_digest: bridge.content_digest.clone(),
            };
            (
                "schema",
                identity,
                format!(
                    "ontology-bridge-{}",
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
                PortableV2Error::new(
                    PortableV2ErrorCode::Incompatible,
                    "missing ontology composition payload component",
                )
            })?;
        let expected_path = format!("data/components/{kind}/{component_id}/{file_name}");
        if component.files.len() != 1
            || component.files[0].path != expected_path
            || component.files[0].media_type != media_type
        {
            return Err(PortableV2Error::at(
                PortableV2ErrorCode::Incompatible,
                &expected_path,
                "ontology composition payload descriptor",
            ));
        }
        let file = &component.files[0];
        entries.push(PortableV2CompositionEntry {
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
    control: &PortableV2OntologyComposition,
    component: &ManifestComponent,
    limits: PortableV2Limits,
) -> Result<(), PortableV2Error> {
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
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::LimitExceeded,
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
                return Err(PortableV2Error::new(
                    PortableV2ErrorCode::Incompatible,
                    "ontology module metadata",
                ));
            }
            Ok((
                module.ontology_id.as_str(),
                module.version.as_str(),
                module.content_digest.as_str(),
            ))
        })
        .collect::<Result<Vec<_>, PortableV2Error>>()?;
    if modules.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
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
        .collect::<Result<Vec<_>, PortableV2Error>>()?;
    if bridges.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
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
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Incompatible,
                "activation closure",
            ));
        }
        let key = (scope, identity, activation.mode.as_str());
        if prior >= Some(key) {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Incompatible,
                "activation order",
            ));
        }
        prior = Some(key);
    }
    for features in [&control.required_features, &control.optional_features] {
        unique_sorted(features, "ontology composition features")?;
        if features.len() > 256 || features.iter().any(|value| !valid_feature(value)) {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::UnsupportedFuture,
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
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::UnsupportedFuture,
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
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "ontology composition dependency closure",
        ));
    }
    Ok(())
}

fn validate_identity_list<'a>(
    values: &'a [PortableV2ExactIdentity],
    modules: &[(&'a str, &'a str, &'a str)],
) -> Result<(), PortableV2Error> {
    if values.is_empty() {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
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
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "bridge endpoint closure",
        ));
    }
    Ok(())
}

fn exact_tuple(value: &PortableV2ExactIdentity) -> Result<(&str, &str, &str), PortableV2Error> {
    validate_exact_fields(&value.id, &value.version, &value.content_digest)?;
    Ok((&value.id, &value.version, &value.content_digest))
}

fn validate_exact_fields(id: &str, version: &str, digest: &str) -> Result<(), PortableV2Error> {
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
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
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

fn runtime_map_descriptor(manifest: &Manifest) -> Result<Option<&ManifestFile>, PortableV2Error> {
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
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::Incompatible,
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
) -> Result<(), PortableV2Error> {
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
            return Err(PortableV2Error::at(
                PortableV2ErrorCode::Incompatible,
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
            return Err(PortableV2Error::at(
                PortableV2ErrorCode::Incompatible,
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
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::Incompatible,
            RUNTIME_MAP_PATH,
            "runtime graph placement",
        ));
    }
    Ok(())
}

fn validate_research_runtime(
    runtime: &RuntimeGenerationMap,
    manifest: &Manifest,
) -> Result<(), PortableV2Error> {
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
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Incompatible,
                "research component/runtime mismatch",
            ));
        }
        let part = research_parts[0];
        if part.record_family_id != "registry"
            || part.record_version != crate::research_versions::RESEARCH_VERSION
            || part.capability_version != crate::research_versions::RESEARCH_VERSION
            || research_caps[0].capability_version != crate::research_versions::RESEARCH_VERSION
            || !research_components
                .iter()
                .any(|c| c.participant_id == part.participant_id)
            || !research_components
                .iter()
                .any(|c| c.participant_id == "research-content")
        {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::UnsupportedFuture,
                "research runtime contract",
            ));
        }
    }
    Ok(())
}

pub(crate) fn decode_runtime_map(
    bytes: &[u8],
) -> Result<(Value, RuntimeGenerationMap), PortableV2Error> {
    let value = UniqueValue::deserialize(&mut serde_json::Deserializer::from_slice(bytes))
        .map_err(|_| {
            PortableV2Error::at(
                PortableV2ErrorCode::Incompatible,
                RUNTIME_MAP_PATH,
                "runtime map JSON",
            )
        })?
        .0;
    let runtime = serde_json::from_value(value.clone()).map_err(|_| {
        PortableV2Error::at(
            PortableV2ErrorCode::Incompatible,
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

pub(super) fn package_class(s: &str) -> Result<PortableV2PackageClass, PortableV2Error> {
    match s {
        "complete" => Ok(PortableV2PackageClass::Complete),
        "ontology-only" => Ok(PortableV2PackageClass::OntologyOnly),
        "component-selective" => Ok(PortableV2PackageClass::ComponentSelective),
        "graph-data-subset" => Ok(PortableV2PackageClass::GraphDataSubset),
        _ => Err(PortableV2Error::new(
            PortableV2ErrorCode::UnsupportedFuture,
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
fn unique_sorted(v: &[String], _: &'static str) -> Result<(), PortableV2Error> {
    if v.windows(2).any(|w| w[0] >= w[1]) {
        Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "unordered/duplicate list",
        ))
    } else {
        Ok(())
    }
}
fn detect_cycles(c: &[ManifestComponent]) -> Result<(), PortableV2Error> {
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
        Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "dependency cycle",
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
