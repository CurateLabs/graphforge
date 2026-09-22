//! Selection, authenticated source planning, and manifest assembly.
use super::{
    BTreeSet, ExportError, Path, PlannedFile, PlannedSource, PortableV2ExportLimits,
    PortableV2ExportPlan, PortableV2PackageClass, err, hex, identity, limit, open_source_no_follow,
    storage, validate_limits,
};
use crate::project_portable_v2::{
    PortableV2ActivationOverride, PortableV2ActivationProfile, PortableV2BridgeSet,
    PortableV2ExactIdentity, PortableV2OntologyComposition, PortableV2OntologyModule,
};
use crate::workspace_participants::MAX_WORKSPACE_ONTOLOGY_COMPOSITION_BYTES;
use crate::{
    PortableV2SelectionPlan, PortableV2SelectionProfile, PortableV2SelectionRequest,
    ResolvedProjectGeneration, preview_portable_v2_selection, project_portable_v2::canonical_json,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::io::Read;
use unicode_normalization::UnicodeNormalization;

impl PortableV2ExportPlan {
    /// Replace whole-graph tree payload with a projected subset staging tree.
    #[expect(
        clippy::too_many_arguments,
        reason = "subset replacement keeps staging inventory selector and limits explicit"
    )]
    #[expect(
        clippy::too_many_lines,
        reason = "subset tree replacement must rebuild inventory components and semantic identity together"
    )]
    pub(crate) fn replace_graph_tree_with_subset(
        &mut self,
        staging: tempfile::TempDir,
        inventory: &crate::GraphFilesInventory,
        inventory_bytes: Vec<u8>,
        selector: &str,
        closure: &str,
        selection_fingerprint: &str,
        limits: PortableV2ExportLimits,
    ) -> Result<(), ExportError> {
        let inventory_id =
            portable_participant_id(crate::GRAPH_CAPABILITY_ID, crate::GRAPH_FILES_FAMILY);
        let inventory_path = format!("data/components/graph-data/{inventory_id}/participant.json");
        self.files.retain(|file| {
            !file
                .path
                .starts_with("data/components/graph-data/graph-tree/")
                && file.path != inventory_path
        });
        let mut total = self
            .files
            .iter()
            .map(|file| file.length)
            .try_fold(0_u64, u64::checked_add)
            .ok_or_else(|| limit("subset byte overflow"))?;
        let inventory_file = inline_control(&inventory_path, inventory_bytes, limits, &mut total)?;
        self.files.push(inventory_file);

        let mut graph_files = Vec::new();
        for entry in &inventory.files {
            if matches!(
                entry.role,
                crate::GraphFileRole::Index | crate::GraphFileRole::Delta
            ) {
                continue;
            }
            let source = crate::graph_files::resolve_v1_inventory_entry(staging.path(), entry)?;
            let relative =
                crate::graph_files::canonical_inventory_relative_text(&entry.relative_path)?;
            let path = format!("data/components/graph-data/graph-tree/{relative}");
            let planned = inspect(&source, &path, limits, &mut total)?;
            if planned.length != entry.byte_length || hex(planned.digest) != entry.content_sha256 {
                return Err(err(
                    "GF_SOURCE_CHANGED",
                    "subset graph file differs from captured inventory",
                ));
            }
            graph_files.push(ComponentFile {
                media_type: graph_media(&entry.relative_path).into(),
                path: path.clone(),
                length: planned.length,
                sha256: hex(planned.digest),
            });
            self.files.push(planned);
        }
        graph_files.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
        self.files
            .sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
        collisions(&self.files)?;
        if self.files.len() as u64 > limits.max_entries {
            return Err(limit("entry count exceeds configured limit"));
        }

        let existing: serde_json::Value =
            serde_json::from_slice(&self.manifest).map_err(storage)?;
        let dependency_map = existing["components"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|component| {
                let id = component["participant_id"].as_str()?.to_owned();
                let dependencies = component["required_dependencies"]
                    .as_array()?
                    .iter()
                    .filter_map(|value| value.as_str().map(str::to_owned))
                    .collect::<Vec<_>>();
                Some((id, dependencies))
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let media_map = existing["components"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|component| component["files"].as_array().into_iter().flatten())
            .filter_map(|file| {
                Some((
                    file["path"].as_str()?.to_owned(),
                    file["media_type"].as_str()?.to_owned(),
                ))
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut components = Vec::new();
        let mut roots = Vec::new();
        let mut by_component: std::collections::BTreeMap<(String, String), Vec<&PlannedFile>> =
            std::collections::BTreeMap::new();
        for file in &self.files {
            let Some(rest) = file.path.strip_prefix("data/components/") else {
                continue;
            };
            let mut parts = rest.splitn(3, '/');
            let kind = parts.next().unwrap_or_default().to_owned();
            let participant = parts.next().unwrap_or_default().to_owned();
            by_component
                .entry((kind, participant))
                .or_default()
                .push(file);
        }
        for ((kind, participant_id), files) in by_component {
            roots.push(participant_id.clone());
            let component_files = if participant_id == "graph-tree" {
                graph_files.clone()
            } else {
                files
                    .iter()
                    .map(|file| ComponentFile {
                        media_type: media_map
                            .get(&file.path)
                            .cloned()
                            .unwrap_or_else(|| media_type_for_path(&file.path).into()),
                        path: file.path.clone(),
                        length: file.length,
                        sha256: hex(file.digest),
                    })
                    .collect()
            };
            components.push(Component {
                kind,
                required_dependencies: dependency_map
                    .get(&participant_id)
                    .cloned()
                    .unwrap_or_default(),
                participant_id,
                files: component_files,
            });
        }
        components.sort_by(|a, b| (&a.kind, &a.participant_id).cmp(&(&b.kind, &b.participant_id)));
        roots.sort();
        roots.dedup();

        let mut capabilities = components
            .iter()
            .map(|component| format!("{}@1", component.kind))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if components
            .iter()
            .any(|component| component.participant_id == "graphforge-ontology-composition")
        {
            capabilities.push("ontology-composition@1".to_owned());
            capabilities.sort();
        }
        let generation_uuid = self.generation_uuid.hyphenated().to_string();
        let source_manifest = existing["source_generation"]["manifest_sha256"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let omissions = existing["selection"]["omissions"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|value| value.as_str().map(str::to_owned))
            .collect::<Vec<_>>();
        let redactions = existing["selection"]["redactions"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|value| value.as_str().map(str::to_owned))
            .collect::<Vec<_>>();
        let draft = Manifest {
            format: "graphforge-project/2",
            package_digest: String::new(),
            package_class: "graph-data-subset",
            source_generation: Source {
                generation_uuid: generation_uuid.clone(),
                manifest_sha256: source_manifest.clone(),
            },
            selection: Selection {
                roots: &roots,
                omissions: omissions.clone(),
                redactions: redactions.clone(),
                graph_subset: Some(GraphSubsetRef { selector, closure }),
            },
            components: &components,
            requirements: Requirements {
                capabilities: capabilities.clone(),
                dependency_rule: "required-transitive-closure/1",
            },
            states: States {
                integrity: "verified",
                compatibility: "supported",
                authenticity: "unsigned",
            },
        };
        let mut value = serde_json::to_value(&draft).map_err(storage)?;
        value.as_object_mut().unwrap().remove("package_digest");
        let semantic = canonical_json(&value)?;
        let mut hasher = Sha256::new();
        hasher.update(b"graphforge-project/2\0");
        hasher.update(semantic);
        self.package_digest = hasher.finalize().into();
        let final_manifest = Manifest {
            format: "graphforge-project/2",
            package_digest: format!("sha256:{}", hex(self.package_digest)),
            package_class: "graph-data-subset",
            source_generation: Source {
                generation_uuid,
                manifest_sha256: source_manifest,
            },
            selection: Selection {
                roots: &roots,
                omissions,
                redactions,
                graph_subset: Some(GraphSubsetRef { selector, closure }),
            },
            components: &components,
            requirements: Requirements {
                capabilities,
                dependency_rule: "required-transitive-closure/1",
            },
            states: States {
                integrity: "verified",
                compatibility: "supported",
                authenticity: "unsigned",
            },
        };
        self.manifest = canonical_json(&serde_json::to_value(final_manifest).map_err(storage)?)?;
        if self.manifest.len() as u64 > limits.max_manifest_bytes {
            return Err(limit("semantic manifest exceeds configured limit"));
        }
        self.payload_bytes = total;
        selection_fingerprint.clone_into(&mut self.selection_fingerprint);
        self.package_class = PortableV2PackageClass::GraphDataSubset;
        self.retained_subset = Some(std::sync::Arc::new(staging));
        Ok(())
    }
}

#[derive(Serialize)]
struct Manifest<'a> {
    format: &'static str,
    package_digest: String,
    package_class: &'a str,
    source_generation: Source,
    selection: Selection<'a>,
    components: &'a [Component],
    requirements: Requirements,
    states: States,
}
#[derive(Serialize)]
struct Source {
    generation_uuid: String,
    manifest_sha256: String,
}
#[derive(Serialize)]
struct Selection<'a> {
    roots: &'a [String],
    omissions: Vec<String>,
    redactions: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    graph_subset: Option<GraphSubsetRef<'a>>,
}
#[derive(Serialize)]
struct GraphSubsetRef<'a> {
    selector: &'a str,
    closure: &'a str,
}
#[derive(Serialize)]
struct Component {
    kind: String,
    participant_id: String,
    required_dependencies: Vec<String>,
    files: Vec<ComponentFile>,
}
#[derive(Clone, Serialize)]
struct ComponentFile {
    media_type: String,
    path: String,
    length: u64,
    sha256: String,
}
#[derive(Serialize)]
struct Requirements {
    capabilities: Vec<String>,
    dependency_rule: &'static str,
}
#[derive(Serialize)]
struct States {
    integrity: &'static str,
    compatibility: &'static str,
    authenticity: &'static str,
}
#[derive(Serialize)]
struct RuntimeGenerationMap<'a> {
    contract: &'static str,
    capabilities: Vec<RuntimeCapability>,
    participants: &'a [RuntimeParticipant],
    graph_tree: Option<RuntimeGraphTree>,
}
#[derive(Serialize)]
struct RuntimeCapability {
    capability_id: String,
    capability_version: u32,
}
#[derive(Serialize)]
struct RuntimeParticipant {
    participant_id: String,
    capability_id: String,
    capability_version: u32,
    record_family_id: String,
    record_version: u32,
    encoding: String,
    schema_fingerprint: String,
    row_count: u64,
}
#[derive(Serialize)]
struct RuntimeGraphTree {
    component_id: &'static str,
    inventory_participant_id: String,
}

pub(super) fn exact_identity(id: &str, version: &str, digest: &str) -> PortableV2ExactIdentity {
    PortableV2ExactIdentity {
        id: id.to_owned(),
        version: version.to_owned(),
        content_digest: format!("sha256:{digest}"),
    }
}

fn module_component_id(digest: &str) -> String {
    format!("ontology-module-{digest}")
}

fn bridge_component_id(digest: &str) -> String {
    format!("ontology-bridge-{digest}")
}

#[expect(
    clippy::too_many_lines,
    reason = "projection appends one authenticated closure to the existing immutable plan"
)]
fn project_ontology_composition(
    source: &Path,
    projected: &[crate::PortableV2ProjectedSelectionEntry],
    limits: PortableV2ExportLimits,
    total: &mut u64,
    files: &mut Vec<PlannedFile>,
    components: &mut Vec<Component>,
    roots: &mut Vec<String>,
) -> Result<(), ExportError> {
    let mut input = open_source_no_follow(source)?;
    let before = identity(&input.metadata().map_err(storage)?)?;
    if before.len > MAX_WORKSPACE_ONTOLOGY_COMPOSITION_BYTES as u64 {
        return Err(limit(
            "ontology composition authority exceeds configured limit",
        ));
    }
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut input)
        .take(MAX_WORKSPACE_ONTOLOGY_COMPOSITION_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(storage)?;
    if bytes.len() as u64 != before.len || identity(&input.metadata().map_err(storage)?)? != before
    {
        return Err(err(
            "GF_SOURCE_CHANGED",
            "ontology composition changed during planning",
        ));
    }
    let composition =
        crate::WorkspaceOntologyComposition::from_canonical_json(&bytes).map_err(storage)?;

    let selected = projected
        .iter()
        .map(|entry| entry.identity.clone())
        .collect::<BTreeSet<_>>();
    let mut modules = Vec::with_capacity(projected.len());
    let mut all_dependencies = Vec::new();
    for module in &composition.modules {
        let exact = exact_identity(
            &module.id.ontology_id,
            &module.id.authored_version,
            &module.id.canonical_digest,
        );
        if !selected.contains(&exact) {
            continue;
        }
        let component_id = module_component_id(&module.id.canonical_digest);
        let path = format!("data/components/ontology/{component_id}/module.json");
        let payload = canonical_json(&serde_json::to_value(&module.document).map_err(storage)?)?;
        let file = inline_control(&path, payload, limits, total)?;
        let component_file = ComponentFile {
            media_type: "application/vnd.graphforge.ontology+json".into(),
            path,
            length: file.length,
            sha256: hex(file.digest),
        };
        files.push(file);
        roots.push(component_id.clone());
        all_dependencies.push(component_id.clone());
        let subject = module.id.display_ref();
        let profile = composition
            .activation
            .iter()
            .find(|activation| {
                activation.scope.as_str() == "module" && activation.subject == subject
            })
            .map_or(composition.profile_default, |activation| activation.mode);
        modules.push(PortableV2OntologyModule {
            ontology_id: module.id.ontology_id.clone(),
            version: module.id.authored_version.clone(),
            content_digest: format!("sha256:{}", module.id.canonical_digest),
            dialect: "graphforge-ontology".into(),
            profile: profile.as_str().into(),
        });
        components.push(Component {
            kind: "ontology".into(),
            participant_id: component_id,
            required_dependencies: module
                .dependencies
                .iter()
                .map(|dependency| module_component_id(&dependency.canonical_digest))
                .collect(),
            files: vec![component_file],
        });
    }

    let mut bridges = Vec::with_capacity(composition.bridges.len());
    let mut bridge_identities = std::collections::BTreeMap::new();
    for bridge in &composition.bridges {
        let digest = graphforge_ontology::bridge_document_digest(bridge).map_err(storage)?;
        let exact = exact_identity(&bridge.bridge_id, &bridge.authored_version, &digest);
        if !selected.contains(&exact) {
            continue;
        }
        bridge_identities.insert(
            format!("{}@{}#{digest}", bridge.bridge_id, bridge.authored_version),
            exact_identity(&bridge.bridge_id, &bridge.authored_version, &digest),
        );
        let component_id = bridge_component_id(&digest);
        let path = format!("data/components/schema/{component_id}/bridge.json");
        let payload = canonical_json(&serde_json::to_value(bridge).map_err(storage)?)?;
        let file = inline_control(&path, payload, limits, total)?;
        let component_file = ComponentFile {
            media_type: "application/vnd.graphforge.ontology-bridge+json".into(),
            path,
            length: file.length,
            sha256: hex(file.digest),
        };
        files.push(file);
        roots.push(component_id.clone());
        all_dependencies.push(component_id.clone());
        let mut dependencies = bridge
            .source_modules
            .iter()
            .chain(&bridge.target_modules)
            .map(|module| module_component_id(&module.canonical_digest))
            .chain(
                bridge
                    .dependencies
                    .iter()
                    .map(|dependency| bridge_component_id(&dependency.canonical_digest)),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        dependencies.sort();
        components.push(Component {
            kind: "schema".into(),
            participant_id: component_id,
            required_dependencies: dependencies,
            files: vec![component_file],
        });
        bridges.push(PortableV2BridgeSet {
            bridge_id: bridge.bridge_id.clone(),
            version: bridge.authored_version.clone(),
            content_digest: format!("sha256:{digest}"),
            source_modules: bridge
                .source_modules
                .iter()
                .map(|module| {
                    exact_identity(
                        &module.ontology_id,
                        &module.authored_version,
                        &module.canonical_digest,
                    )
                })
                .collect(),
            target_modules: bridge
                .target_modules
                .iter()
                .map(|module| {
                    exact_identity(
                        &module.ontology_id,
                        &module.authored_version,
                        &module.canonical_digest,
                    )
                })
                .collect(),
        });
    }
    modules.sort_by(|left, right| {
        (&left.ontology_id, &left.version, &left.content_digest).cmp(&(
            &right.ontology_id,
            &right.version,
            &right.content_digest,
        ))
    });
    bridges.sort_by(|left, right| {
        (&left.bridge_id, &left.version, &left.content_digest).cmp(&(
            &right.bridge_id,
            &right.version,
            &right.content_digest,
        ))
    });
    let module_identities = composition
        .modules
        .iter()
        .filter(|module| {
            selected.contains(&exact_identity(
                &module.id.ontology_id,
                &module.id.authored_version,
                &module.id.canonical_digest,
            ))
        })
        .map(|module| {
            (
                module.id.display_ref(),
                exact_identity(
                    &module.id.ontology_id,
                    &module.id.authored_version,
                    &module.id.canonical_digest,
                ),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut overrides = composition
        .activation
        .iter()
        .filter(|activation| {
            if activation.scope.as_str() == "module" {
                module_identities.contains_key(&activation.subject)
            } else {
                bridge_identities.contains_key(&activation.subject)
            }
        })
        .map(|activation| {
            let subject = if activation.scope.as_str() == "module" {
                module_identities.get(&activation.subject)
            } else {
                bridge_identities.get(&activation.subject)
            }
            .ok_or_else(|| err("GF_PROJECT_CORRUPT", "composition activation is dangling"))?;
            Ok(PortableV2ActivationOverride {
                scope: activation.scope.as_str().into(),
                subject: subject.clone(),
                mode: activation.mode.as_str().into(),
            })
        })
        .collect::<Result<Vec<_>, ExportError>>()?;
    overrides.sort_by(|left, right| {
        (
            &left.scope,
            &left.subject.id,
            &left.subject.version,
            &left.subject.content_digest,
            &left.mode,
        )
            .cmp(&(
                &right.scope,
                &right.subject.id,
                &right.subject.version,
                &right.subject.content_digest,
                &right.mode,
            ))
    });
    let mut control = PortableV2OntologyComposition {
        contract: "graphforge-ontology-composition/1".into(),
        activation_profile: PortableV2ActivationProfile {
            profile_default: composition.profile_default.as_str().into(),
            overrides,
        },
        modules,
        bridge_sets: bridges,
        required_features: vec!["provenance-bridges@1".into(), "qualified-symbols@1".into()],
        optional_features: Vec::new(),
        composition_digest: String::new(),
    };
    let mut unsigned = serde_json::to_value(&control).map_err(storage)?;
    unsigned
        .as_object_mut()
        .expect("composition is an object")
        .remove("composition_digest");
    let mut digest = Sha256::new();
    digest.update(b"graphforge-ontology-composition/1\0");
    digest.update(canonical_json(&unsigned)?);
    control.composition_digest = format!("sha256:{}", hex(digest.finalize().into()));
    let control_bytes = canonical_json(&serde_json::to_value(control).map_err(storage)?)?;
    let path = crate::project_portable_v2::ONTOLOGY_COMPOSITION_PATH;
    let file = inline_control(path, control_bytes, limits, total)?;
    let component_file = ComponentFile {
        media_type: "application/vnd.graphforge.ontology-composition+json".into(),
        path: path.into(),
        length: file.length,
        sha256: hex(file.digest),
    };
    files.push(file);
    roots.push("graphforge-ontology-composition".into());
    all_dependencies.sort();
    all_dependencies.dedup();
    components.push(Component {
        kind: "compatibility".into(),
        participant_id: "graphforge-ontology-composition".into(),
        required_dependencies: all_dependencies,
        files: vec![component_file],
    });
    Ok(())
}

/// Plan every canonical participant of one already-pinned generation.
pub fn plan_complete_portable_v2(
    g: &ResolvedProjectGeneration,
    limits: PortableV2ExportLimits,
) -> Result<PortableV2ExportPlan, ExportError> {
    let selection = preview_portable_v2_selection(
        g,
        &PortableV2SelectionRequest {
            profile: PortableV2SelectionProfile::Complete,
            strict: false,
        },
        limits,
    )?;
    plan_selected_portable_v2(g, &selection, limits)
}

/// Materialize one immutable selection preview into a representation-independent export plan.
#[expect(
    clippy::too_many_lines,
    reason = "keeps one auditable sequence from immutable selection through semantic identity"
)]
pub fn plan_selected_portable_v2(
    g: &ResolvedProjectGeneration,
    selection: &PortableV2SelectionPlan,
    limits: PortableV2ExportLimits,
) -> Result<PortableV2ExportPlan, ExportError> {
    validate_limits(limits)?;
    crate::project_portable_v2_selection::validate_selection_plan(g, selection)?;
    g.validate_complete_participant_inventory()?;
    let mut files = Vec::new();
    let mut components = Vec::new();
    let mut roots = Vec::new();
    let mut runtime_participants = Vec::new();
    let mut graph_inventory_participant = None;
    let mut ontology_composition_source = None;
    let mut total = 0;
    for d in g.participant_descriptors()? {
        if !selection.includes(&d.capability_id, &d.record_family_id) {
            continue;
        }
        if d.capability_id == crate::WORKSPACE_CAPABILITY_ID
            && d.record_family_id == crate::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY
        {
            ontology_composition_source =
                Some(g.participant_path(&d.capability_id, &d.record_family_id)?);
            continue;
        }
        let id = portable_participant_id(&d.capability_id, &d.record_family_id);
        let kind = crate::project_portable_v2_selection::component_kind(
            &d.capability_id,
            &d.record_family_id,
        );
        let source = g.participant_path(&d.capability_id, &d.record_family_id)?;
        let path = format!(
            "data/components/{kind}/{id}/participant.{}",
            extension(&d.encoding)
        );
        let f = inspect(&source, &path, limits, &mut total)?;
        let cf = ComponentFile {
            media_type: media_type(&d.encoding).into(),
            path: path.clone(),
            length: f.length,
            sha256: hex(f.digest),
        };
        files.push(f);
        roots.push(id.clone());
        if d.capability_id == crate::GRAPH_CAPABILITY_ID
            && d.record_family_id == crate::GRAPH_FILES_FAMILY
        {
            graph_inventory_participant = Some(id.clone());
        }
        runtime_participants.push(RuntimeParticipant {
            participant_id: id.clone(),
            capability_id: d.capability_id,
            capability_version: d.capability_version,
            record_family_id: d.record_family_id,
            record_version: d.record_version,
            encoding: d.encoding,
            schema_fingerprint: hex(d.schema_fingerprint),
            row_count: d.row_count,
        });
        components.push(Component {
            kind: kind.into(),
            participant_id: id,
            required_dependencies: vec![],
            files: vec![cf],
        });
    }
    if let Some(source) = ontology_composition_source {
        project_ontology_composition(
            &source,
            &selection.projected,
            limits,
            &mut total,
            &mut files,
            &mut components,
            &mut roots,
        )?;
    }
    if selection.includes_graph_tree()
        && let Some(inv) = g.graph_files_inventory()?
    {
        let graph_authority = g.declared_graph_files_participant()?;
        let graph_cas = if matches!(graph_authority, Some(crate::GraphFilesParticipant::V2(_))) {
            Some(crate::graph_object_store::begin_graph_object_read(
                g.container_root(),
            )?)
        } else {
            None
        };
        let id = "graph-tree".to_owned();
        let mut owned = Vec::new();
        for e in inv.files {
            let canonical =
                crate::graph_files::canonical_inventory_relative_text(&e.relative_path)?;
            let path = format!("data/components/graph-data/{id}/{canonical}");
            let f = match &graph_authority {
                Some(crate::GraphFilesParticipant::V1(_)) => inspect(
                    &crate::graph_files::resolve_v1_inventory_entry(&g.graph_tree_root(), &e)?,
                    &path,
                    limits,
                    &mut total,
                )?,
                Some(crate::GraphFilesParticipant::V2(_)) => inspect_cas(
                    graph_cas.as_ref().expect("compact authority has CAS lease"),
                    &e.content_sha256,
                    e.byte_length,
                    &path,
                    limits,
                    &mut total,
                )?,
                None => return Err(err("GF_SOURCE_CHANGED", "graph authority disappeared")),
            };
            if f.length != e.byte_length || hex(f.digest) != e.content_sha256 {
                return Err(err(
                    "GF_SOURCE_CHANGED",
                    "graph file differs from pinned inventory",
                ));
            }
            owned.push(ComponentFile {
                media_type: graph_media(&e.relative_path).into(),
                path: path.clone(),
                length: f.length,
                sha256: hex(f.digest),
            });
            files.push(f);
        }
        owned.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
        roots.push(id.clone());
        components.push(Component {
            kind: "graph-data".into(),
            participant_id: id,
            required_dependencies: vec![],
            files: owned,
        });
    }
    if selection.includes("research", "registry") {
        let objects = crate::research_versions::interchange::portable::object_inventory(g)?;
        let lease = crate::graph_object_store::begin_graph_object_read(g.container_root())?;
        let mut owned = Vec::new();
        for digest in objects {
            let length = std::fs::metadata(crate::graph_object_path(g.container_root(), &digest)?)
                .map_err(storage)?
                .len();
            let path = format!("data/components/research/research-content/{digest}");
            let file = inspect_cas(&lease, &digest, length, &path, limits, &mut total)?;
            owned.push(ComponentFile {
                media_type: "application/octet-stream".into(),
                path,
                length,
                sha256: digest,
            });
            files.push(file);
        }
        roots.push("research-content".into());
        components.push(Component {
            kind: "research".into(),
            participant_id: "research-content".into(),
            required_dependencies: vec![portable_participant_id("research", "registry")],
            files: owned,
        });
    }
    runtime_participants.sort_by(|left, right| {
        left.participant_id
            .as_bytes()
            .cmp(right.participant_id.as_bytes())
    });
    let runtime_map = RuntimeGenerationMap {
        contract: "graphforge-runtime-generation-map/1",
        capabilities: g
            .capabilities()
            .into_iter()
            .filter(|capability| {
                selection
                    .required_capabilities
                    .contains(&capability.capability_id)
            })
            .map(|capability| RuntimeCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect(),
        participants: &runtime_participants,
        graph_tree: graph_inventory_participant.map(|inventory_participant_id| RuntimeGraphTree {
            component_id: "graph-tree",
            inventory_participant_id,
        }),
    };
    let runtime_bytes = canonical_json(&serde_json::to_value(runtime_map).map_err(storage)?)?;
    if runtime_bytes.len() as u64 > limits.max_manifest_bytes {
        return Err(limit("runtime compatibility map exceeds configured limit"));
    }
    let runtime_path =
        "data/components/compatibility/graphforge-runtime-map/runtime-generation.json";
    let runtime_file = inline_control(runtime_path, runtime_bytes, limits, &mut total)?;
    let runtime_component_file = ComponentFile {
        media_type: "application/vnd.graphforge.runtime-generation+json".into(),
        path: runtime_path.into(),
        length: runtime_file.length,
        sha256: hex(runtime_file.digest),
    };
    files.push(runtime_file);
    roots.push("graphforge-runtime-map".into());
    components.push(Component {
        kind: "compatibility".into(),
        participant_id: "graphforge-runtime-map".into(),
        required_dependencies: runtime_participants
            .iter()
            .map(|participant| participant.participant_id.clone())
            .collect(),
        files: vec![runtime_component_file],
    });
    if files.len() as u64 > limits.max_entries {
        return Err(limit("entry count exceeds configured limit"));
    }
    if components.len() as u64 > limits.max_components {
        return Err(limit("component count exceeds configured limit"));
    }
    files.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
    components.sort_by(|a, b| (&a.kind, &a.participant_id).cmp(&(&b.kind, &b.participant_id)));
    roots.sort();
    collisions(&files)?;
    let capabilities = || {
        let mut capabilities = components
            .iter()
            .map(|component| format!("{}@1", component.kind))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if components.iter().any(|component| {
            component.kind == "compatibility"
                && component.participant_id == "graphforge-ontology-composition"
        }) {
            capabilities.push("ontology-composition@1".to_owned());
            capabilities.sort();
        }
        capabilities
    };
    let research = if selection.includes("research", "registry") {
        let snapshot = g
            .participant_snapshot("research", "registry")?
            .ok_or_else(|| err("GF_INCOMPATIBLE", "research registry is unavailable"))?;
        Some(crate::research_versions::interchange::portable::portable_registry(&snapshot.bytes)?)
    } else {
        None
    };
    let selected_research = research
        .as_ref()
        .and_then(|registry| registry.interchange.values().next())
        .map(|archive| &archive.versions[&archive.selected_version_uuid]);
    let source_generation =
        selected_research.map_or(g.generation_uuid(), |v| v.content.generation_uuid);
    let source_manifest =
        selected_research.map_or(g.manifest_sha256(), |v| v.content.manifest_sha256);
    let selection_fingerprint = if let Some(registry) = &research {
        format!(
            "sha256:{}",
            hex(Sha256::digest(serde_json::to_vec(&registry.interchange).map_err(storage)?).into())
        )
    } else {
        selection.selection_fingerprint.clone()
    };
    let source = || Source {
        generation_uuid: source_generation.hyphenated().to_string(),
        manifest_sha256: hex(source_manifest),
    };
    let draft = Manifest {
        format: "graphforge-project/2",
        package_digest: String::new(),
        package_class: &selection.package_class,
        source_generation: source(),
        selection: Selection {
            roots: &roots,
            omissions: selection
                .excluded
                .iter()
                .map(|entry| {
                    portable_participant_id(
                        &entry.identity.capability_id,
                        &entry.identity.record_family_id,
                    )
                })
                .collect(),
            redactions: selection.redactions.clone(),
            graph_subset: None,
        },
        components: &components,
        requirements: Requirements {
            capabilities: capabilities(),
            dependency_rule: "required-transitive-closure/1",
        },
        states: States {
            integrity: "verified",
            compatibility: "supported",
            authenticity: "unsigned",
        },
    };
    let mut value = serde_json::to_value(draft).map_err(storage)?;
    value.as_object_mut().unwrap().remove("package_digest");
    let semantic = canonical_json(&value)?;
    let mut h = Sha256::new();
    h.update(b"graphforge-project/2\0");
    h.update(semantic);
    let package_digest = h.finalize().into();
    let final_manifest = Manifest {
        format: "graphforge-project/2",
        package_digest: format!("sha256:{}", hex(package_digest)),
        package_class: &selection.package_class,
        source_generation: source(),
        selection: Selection {
            roots: &roots,
            omissions: selection
                .excluded
                .iter()
                .map(|entry| {
                    portable_participant_id(
                        &entry.identity.capability_id,
                        &entry.identity.record_family_id,
                    )
                })
                .collect(),
            redactions: selection.redactions.clone(),
            graph_subset: None,
        },
        components: &components,
        requirements: Requirements {
            capabilities: capabilities(),
            dependency_rule: "required-transitive-closure/1",
        },
        states: States {
            integrity: "verified",
            compatibility: "supported",
            authenticity: "unsigned",
        },
    };
    let manifest = canonical_json(&serde_json::to_value(final_manifest).map_err(storage)?)?;
    if manifest.len() as u64 > limits.max_manifest_bytes {
        return Err(limit("semantic manifest exceeds configured limit"));
    }
    Ok(PortableV2ExportPlan {
        generation_uuid: source_generation,
        files,
        manifest,
        package_digest,
        payload_bytes: total,
        selection_fingerprint,
        package_class: package_class(&selection.package_class)?,
        retained_subset: None,
    })
}

pub(super) fn package_class(value: &str) -> Result<PortableV2PackageClass, ExportError> {
    match value {
        "complete" => Ok(PortableV2PackageClass::Complete),
        "ontology-only" => Ok(PortableV2PackageClass::OntologyOnly),
        "component-selective" => Ok(PortableV2PackageClass::ComponentSelective),
        "graph-data-subset" => Ok(PortableV2PackageClass::GraphDataSubset),
        _ => Err(err(
            "GF_INCOMPATIBLE",
            "unsupported selection package class",
        )),
    }
}

pub(super) fn inspect(
    source: &Path,
    path: &str,
    limits: PortableV2ExportLimits,
    total: &mut u64,
) -> Result<PlannedFile, ExportError> {
    if path.len() > limits.max_path_bytes {
        return Err(limit("portable path exceeds configured limit"));
    }
    valid_path(path)?;
    let mut input = open_source_no_follow(source)?;
    let before = identity(&input.metadata().map_err(storage)?)?;
    if before.len > limits.max_entry_bytes {
        return Err(limit("entry too large"));
    }
    *total = total
        .checked_add(before.len)
        .ok_or_else(|| limit("size overflow"))?;
    if *total > limits.max_total_bytes {
        return Err(limit("total too large"));
    }
    let mut digest = Sha256::new();
    let mut buffer = vec![0; limits.copy_buffer_bytes];
    let mut bytes_read = 0;
    loop {
        let count = input.read(&mut buffer).map_err(storage)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
        bytes_read += count as u64;
    }
    if bytes_read != before.len || identity(&input.metadata().map_err(storage)?)? != before {
        return Err(err("GF_SOURCE_CHANGED", "source changed during planning"));
    }
    Ok(PlannedFile {
        source: PlannedSource::File {
            path: source.into(),
            identity: before,
        },
        path: path.into(),
        length: bytes_read,
        digest: digest.finalize().into(),
    })
}
fn inspect_cas(
    lease: &crate::graph_object_store::GraphObjectReadLease,
    digest: &str,
    expected_length: u64,
    path: &str,
    limits: PortableV2ExportLimits,
    total: &mut u64,
) -> Result<PlannedFile, ExportError> {
    if path.len() > limits.max_path_bytes || expected_length > limits.max_entry_bytes {
        return Err(limit("CAS entry exceeds configured limit"));
    }
    valid_path(path)?;
    *total = total
        .checked_add(expected_length)
        .ok_or_else(|| limit("size overflow"))?;
    if *total > limits.max_total_bytes {
        return Err(limit("total too large"));
    }
    let _authenticated = lease.open(digest, expected_length)?;
    let digest_bytes = parse_sha256(digest)?;
    Ok(PlannedFile {
        source: PlannedSource::Cas {
            lease: lease.clone(),
            digest: digest.to_owned(),
            length: expected_length,
        },
        path: path.into(),
        length: expected_length,
        digest: digest_bytes,
    })
}
fn inline_control(
    path: &str,
    bytes: Vec<u8>,
    limits: PortableV2ExportLimits,
    total: &mut u64,
) -> Result<PlannedFile, ExportError> {
    if path.len() > limits.max_path_bytes || bytes.len() as u64 > limits.max_entry_bytes {
        return Err(limit("control entry exceeds configured limit"));
    }
    *total = total
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| limit("size overflow"))?;
    if *total > limits.max_total_bytes {
        return Err(limit("total too large"));
    }
    Ok(PlannedFile {
        path: path.into(),
        length: bytes.len() as u64,
        digest: Sha256::digest(&bytes).into(),
        source: PlannedSource::Control(bytes),
    })
}

fn parse_sha256(value: &str) -> Result<[u8; 32], ExportError> {
    if value.len() != 64 {
        return Err(err("GF_INTEGRITY_FAILED", "invalid CAS digest length"));
    }
    let mut bytes = [0_u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| err("GF_INTEGRITY_FAILED", "invalid CAS digest"))?;
    }
    Ok(bytes)
}

fn valid_path(p: &str) -> Result<(), ExportError> {
    if p.len() > 4096
        || p.starts_with('/')
        || p.contains('\\')
        || p.bytes().any(|b| b < 32 || b == 127)
        || p.split('/').any(|x| x.is_empty() || x == "." || x == "..")
        || p.nfc().ne(p.chars())
    {
        return Err(err("GF_INVALID_PORTABLE_PATH", "invalid path"));
    }
    Ok(())
}
fn collisions(f: &[PlannedFile]) -> Result<(), ExportError> {
    let mut a = BTreeSet::new();
    let mut b = BTreeSet::new();
    for x in f {
        if !a.insert(&x.path) || !b.insert(x.path.to_lowercase()) {
            return Err(err("GF_DUPLICATE_PORTABLE_PATH", "path collision"));
        }
    }
    Ok(())
}
pub(super) fn portable_id(s: &str) -> String {
    let mut o = s
        .to_ascii_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect::<String>();
    if !o.starts_with(|c: char| c.is_ascii_lowercase()) {
        o.insert(0, 'p');
    }
    o
}
pub(crate) fn portable_participant_id(capability: &str, family: &str) -> String {
    let mut prefix = portable_id(&format!("{capability}-{family}"));
    prefix.truncate(220);
    let mut digest = Sha256::new();
    digest.update(capability.as_bytes());
    digest.update([0]);
    digest.update(family.as_bytes());
    format!("{prefix}-{}", &hex(digest.finalize().into())[..12])
}
fn extension(e: &str) -> &str {
    match e {
        "json" => "json",
        "parquet" => "parquet",
        "arrow" => "arrow",
        _ => "bin",
    }
}
fn media_type(e: &str) -> &str {
    match e {
        "json" => "application/json",
        "parquet" => "application/vnd.apache.parquet",
        "arrow" => "application/vnd.apache.arrow.file",
        _ => "application/octet-stream",
    }
}
fn media_type_for_path(path: &str) -> &'static str {
    if path.ends_with("runtime-generation.json") {
        return "application/vnd.graphforge.runtime-generation+json";
    }
    match std::path::Path::new(path)
        .extension()
        .and_then(|ext| ext.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("json") => "application/json",
        Some("parquet") => "application/vnd.apache.parquet",
        Some("yaml" | "yml") => "application/yaml",
        _ => "application/octet-stream",
    }
}
fn graph_media(p: &str) -> &str {
    let extension = Path::new(p).extension().and_then(|value| value.to_str());
    if extension.is_some_and(|value| value.eq_ignore_ascii_case("parquet")) {
        "application/vnd.apache.parquet"
    } else if extension.is_some_and(|value| value.eq_ignore_ascii_case("json")) {
        "application/json"
    } else {
        "application/octet-stream"
    }
}
#[cfg(test)]
mod tests;
