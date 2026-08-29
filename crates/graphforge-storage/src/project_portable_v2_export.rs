//! Bounded deterministic portable-project v2 complete-package export.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use serde::Serialize;
use sha2::{Digest, Sha256};
use unicode_normalization::UnicodeNormalization;
use uuid::Uuid;

use crate::project_portable_v2::{
    PortableV2ActivationOverride, PortableV2ActivationProfile, PortableV2BridgeSet,
    PortableV2ExactIdentity, PortableV2OntologyComposition, PortableV2OntologyModule,
};
use crate::workspace_participants::MAX_WORKSPACE_ONTOLOGY_COMPOSITION_BYTES;
use crate::{
    PortableV2Error, PortableV2ErrorCode, PortableV2Limits, PortableV2Mode, PortableV2PackageClass,
    PortableV2SelectionPlan, PortableV2SelectionProfile, PortableV2SelectionRequest,
    ResolvedProjectGeneration, preview_portable_v2_selection, project_portable_v2::canonical_json,
    verify_portable_v2,
};

type ExportError = PortableV2Error;

const BAGIT: &[u8] = b"BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n";
const BAG_INFO: &[u8] = b"Bag-Software-Agent: GraphForge portable-v2\nBagging-Date: 1970-01-01\n";
const USTAR_MAX_ENTRY_BYTES: u64 = 0o77_777_777_777;

/// Finite planner and streaming-writer budgets.
pub type PortableV2ExportLimits = PortableV2Limits;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Portable-v2 transport representation.
pub enum PortableV2Output {
    /// Closed BagIt-compatible directory.
    Expanded,
    /// Canonical uncompressed PAX/ustar stream.
    Bundle,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Aggregate content-free progress observation.
pub struct PortableV2ExportProgress {
    /// Fully emitted entries.
    pub entries_completed: usize,
    /// Emitted source payload bytes.
    pub bytes_completed: u64,
    /// Planned entry count.
    pub entries_total: usize,
    /// Planned source payload bytes.
    pub bytes_total: u64,
}
#[derive(Debug, Clone)]
struct PlannedFile {
    source: PlannedSource,
    path: String,
    length: u64,
    digest: [u8; 32],
}
#[derive(Debug, Clone)]
enum PlannedSource {
    File {
        path: PathBuf,
        identity: Identity,
    },
    Cas {
        lease: crate::graph_object_store::GraphObjectReadLease,
        digest: String,
        length: u64,
    },
    Control(Vec<u8>),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Identity {
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
    len: u64,
    modified: Option<std::time::SystemTime>,
}
#[derive(Clone)]
/// Immutable pinned-generation metadata plan; contains no payload buffers.
pub struct PortableV2ExportPlan {
    generation_uuid: Uuid,
    files: Vec<PlannedFile>,
    manifest: Vec<u8>,
    package_digest: [u8; 32],
    payload_bytes: u64,
    selection_fingerprint: String,
    package_class: PortableV2PackageClass,
    /// Keeps subset materialization alive for the plan lifetime.
    retained_subset: Option<std::sync::Arc<tempfile::TempDir>>,
}
impl std::fmt::Debug for PortableV2ExportPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PortableV2ExportPlan")
            .field("generation_uuid", &self.generation_uuid)
            .field("entry_count", &self.files.len())
            .field("package_digest", &hex(self.package_digest))
            .field("payload_bytes", &self.payload_bytes)
            .field("selection_fingerprint", &self.selection_fingerprint)
            .finish_non_exhaustive()
    }
}

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
#[derive(Debug, Clone)]
/// Durable publication receipt with separate semantic and transport identities.
pub struct PortableV2ExportReceipt {
    /// Pinned source generation.
    pub generation_uuid: Uuid,
    /// Semantic package identity shared by both representations.
    pub package_digest: [u8; 32],
    /// Representation-specific transport identity.
    pub transport_digest: [u8; 32],
    /// Verified physical package entry count, including tag records.
    pub entry_count: usize,
    /// Source payload bytes, excluding tags and manifest.
    pub payload_bytes: u64,
    /// Published representation.
    pub output: PortableV2Output,
    /// Fingerprint of the immutable content-free selection preview used by the writer.
    pub selection_fingerprint: String,
    /// Exact native allocation of the published package for lifecycle evidence.
    #[doc(hidden)]
    pub allocation_identity_allocated_bytes: BTreeMap<String, u64>,
    /// Logical EOF bytes of the exact published identity union.
    #[doc(hidden)]
    pub allocation_logical_bytes: u64,
    /// Distinct physical files in the exact published identity union.
    #[doc(hidden)]
    pub allocation_physical_objects: u64,
}

impl PartialEq for PortableV2ExportReceipt {
    fn eq(&self, other: &Self) -> bool {
        self.generation_uuid == other.generation_uuid
            && self.package_digest == other.package_digest
            && self.transport_digest == other.transport_digest
            && self.entry_count == other.entry_count
            && self.payload_bytes == other.payload_bytes
            && self.output == other.output
            && self.selection_fingerprint == other.selection_fingerprint
            && self.allocation_logical_bytes == other.allocation_logical_bytes
            && self.allocation_physical_objects == other.allocation_physical_objects
    }
}

impl Eq for PortableV2ExportReceipt {}

#[derive(Default)]
struct ExportAllocationObserver {
    allocated: BTreeMap<String, u64>,
    logical: BTreeMap<String, u64>,
}

impl ExportAllocationObserver {
    fn observe(&mut self, file: &File) -> Result<(), ExportError> {
        let identity = graphforge_filesystem::file_identity(file).map_err(storage)?;
        let usage = graphforge_filesystem::file_space_usage(file).map_err(storage)?;
        let mut file_id = String::with_capacity(32);
        for byte in identity.file_id {
            use std::fmt::Write as _;
            write!(&mut file_id, "{byte:02x}").expect("writing to String cannot fail");
        }
        let key = format!("{:016x}:{file_id}", identity.volume_serial);
        self.allocated.insert(key.clone(), usage.allocated_bytes);
        self.logical.insert(key, usage.logical_bytes);
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

fn exact_identity(id: &str, version: &str, digest: &str) -> PortableV2ExactIdentity {
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
    if selection.include_graph_tree
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
    let source = || Source {
        generation_uuid: g.generation_uuid().hyphenated().to_string(),
        manifest_sha256: hex(g.manifest_sha256()),
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
        generation_uuid: g.generation_uuid(),
        files,
        manifest,
        package_digest,
        payload_bytes: total,
        selection_fingerprint: selection.selection_fingerprint.clone(),
        package_class: package_class(&selection.package_class)?,
        retained_subset: None,
    })
}

/// Stream and durably publish one representation, cleaning private staging on failure.
pub fn export_complete_portable_v2(
    plan: &PortableV2ExportPlan,
    destination: impl AsRef<Path>,
    output: PortableV2Output,
    limits: PortableV2ExportLimits,
    cancelled: &AtomicBool,
    mut progress: impl FnMut(PortableV2ExportProgress),
) -> Result<PortableV2ExportReceipt, ExportError> {
    validate_limits(limits)?;
    if output == PortableV2Output::Bundle
        && entries(plan, limits.max_tag_manifest_bytes)?
            .iter()
            .any(|(_, source)| source.len() > USTAR_MAX_ENTRY_BYTES)
    {
        return Err(limit("bundle entry exceeds ustar size field"));
    }
    let dst = destination.as_ref();
    reject_destination(dst)?;
    let parent = dst
        .parent()
        .ok_or_else(|| err("GF_INVALID_DESTINATION", "destination has no parent"))?;
    let name = dst
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| err("GF_INVALID_DESTINATION", "invalid destination name"))?;
    let stage = parent.join(format!(".{name}.{}.partial", Uuid::new_v4()));
    let is_cancelled = || cancelled.load(Ordering::Relaxed);
    let mut allocation = ExportAllocationObserver::default();
    let result = match output {
        PortableV2Output::Expanded => expanded(
            plan,
            &stage,
            limits,
            &is_cancelled,
            &mut progress,
            &mut allocation,
        ),
        PortableV2Output::Bundle => bundle(
            plan,
            &stage,
            limits,
            &is_cancelled,
            &mut progress,
            &mut allocation,
        ),
    };
    let digest = match result {
        Ok(d) => d,
        Err(e) => {
            remove(&stage);
            return Err(e.with_allocation_identities(allocation.allocated));
        }
    };
    let allocation_logical_bytes = allocation.logical.values().copied().sum();
    let allocation_physical_objects = allocation.logical.len() as u64;
    let staged_allocation = allocation.allocated;
    if is_cancelled() {
        remove(&stage);
        return Err(err("GF_CANCELLED", "portable export cancelled")
            .with_allocation_identities(staged_allocation));
    }
    let verified = verify_portable_v2(&stage, PortableV2Mode::Full, limits, Some(cancelled))
        .map_err(|error| {
            remove(&stage);
            error.with_allocation_identities(staged_allocation.clone())
        })?;
    let expected_transport = format!("sha256:{}", hex(digest));
    if verified.package_class != plan.package_class
        || verified.package_digest != format!("sha256:{}", hex(plan.package_digest))
    {
        remove(&stage);
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "writer and verifier semantic receipts disagree",
        )
        .with_allocation_identities(staged_allocation.clone()));
    }
    if verified.transport_digest.as_deref() != Some(expected_transport.as_str()) {
        remove(&stage);
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "writer and verifier transport receipts disagree",
        )
        .with_allocation_identities(staged_allocation.clone()));
    }
    publish_no_replace(&stage, dst).map_err(|error| {
        remove(&stage);
        storage(error).with_allocation_identities(staged_allocation.clone())
    })?;
    if let Err(error) = sync_dir(parent) {
        remove(dst);
        return Err(error.with_allocation_identities(staged_allocation));
    }
    Ok(PortableV2ExportReceipt {
        generation_uuid: plan.generation_uuid,
        package_digest: plan.package_digest,
        transport_digest: digest,
        entry_count: usize::try_from(verified.entry_count)
            .map_err(|_| limit("verified entry count exceeds platform capacity"))?,
        payload_bytes: plan.payload_bytes,
        output,
        selection_fingerprint: plan.selection_fingerprint.clone(),
        allocation_identity_allocated_bytes: staged_allocation,
        allocation_logical_bytes,
        allocation_physical_objects,
    })
}

/// Repack a fully verified expanded portable-v2 package into canonical bundle bytes.
///
/// This preserves the semantic manifest byte-for-byte and exists for deterministic
/// checked-in contract artifacts; normal project export should use
/// [`export_complete_portable_v2`].
pub fn repack_verified_expanded_portable_v2(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    limits: PortableV2ExportLimits,
    cancelled: &AtomicBool,
) -> Result<PortableV2ExportReceipt, ExportError> {
    let snapshot = tempfile::tempdir().map_err(storage)?;
    let snapshot_root = snapshot.path().join("verified");
    copy_expanded_snapshot(source.as_ref(), &snapshot_root, cancelled)?;
    let report = crate::verify_portable_v2(
        &snapshot_root,
        crate::PortableV2Mode::Full,
        limits,
        Some(cancelled),
    )?;
    let source = snapshot_root.as_path();
    let manifest = fs::read(source.join("data/graphforge-project.json")).map_err(storage)?;
    let value: serde_json::Value = serde_json::from_slice(&manifest).map_err(storage)?;
    let generation_uuid = value
        .pointer("/source_generation/generation_uuid")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| err("GF_INVALID_MANIFEST", "source generation is missing"))?
        .parse()
        .map_err(|_| err("GF_INVALID_MANIFEST", "source generation is invalid"))?;
    let package_class_name = value
        .get("package_class")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| err("GF_INVALID_MANIFEST", "package class is missing"))?;
    let mut package_digest = [0_u8; 32];
    let digest = report
        .package_digest
        .strip_prefix("sha256:")
        .ok_or_else(|| err("GF_INVALID_MANIFEST", "package digest is invalid"))?;
    if digest.len() != 64 {
        return Err(err("GF_INVALID_MANIFEST", "package digest is invalid"));
    }
    for (index, byte) in package_digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&digest[index * 2..index * 2 + 2], 16)
            .map_err(|_| err("GF_INVALID_MANIFEST", "package digest is invalid"))?;
    }
    let mut files = Vec::new();
    let mut total = 0_u64;
    for component in value
        .get("components")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| err("GF_INVALID_MANIFEST", "components are missing"))?
    {
        for file in component
            .get("files")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| err("GF_INVALID_MANIFEST", "component files are missing"))?
        {
            let path = file
                .get("path")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| err("GF_INVALID_MANIFEST", "component path is missing"))?;
            files.push(inspect(&source.join(path), path, limits, &mut total)?);
        }
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    let plan = PortableV2ExportPlan {
        generation_uuid,
        files,
        manifest,
        package_digest,
        payload_bytes: total,
        selection_fingerprint: report.package_digest.clone(),
        package_class: package_class(package_class_name)?,
        retained_subset: None,
    };
    export_complete_portable_v2(
        &plan,
        destination,
        PortableV2Output::Bundle,
        limits,
        cancelled,
        |_| {},
    )
}

fn copy_expanded_snapshot(
    source: &Path,
    destination: &Path,
    cancelled: &AtomicBool,
) -> Result<(), ExportError> {
    fs::create_dir(destination).map_err(storage)?;
    let mut pending = vec![(source.to_path_buf(), destination.to_path_buf())];
    while let Some((input, output)) = pending.pop() {
        for entry in fs::read_dir(&input).map_err(storage)? {
            if cancelled.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(err("GF_CANCELLED", "portable export cancelled"));
            }
            let entry = entry.map_err(storage)?;
            let metadata = fs::symlink_metadata(entry.path()).map_err(storage)?;
            let target = output.join(entry.file_name());
            if metadata.file_type().is_symlink() {
                return Err(err(
                    "GF_UNSUPPORTED_ENTRY_TYPE",
                    "expanded portable source contains a symlink",
                ));
            }
            if metadata.is_dir() {
                fs::create_dir(&target).map_err(storage)?;
                pending.push((entry.path(), target));
            } else if metadata.is_file() {
                fs::copy(entry.path(), target).map_err(storage)?;
            } else {
                return Err(err(
                    "GF_UNSUPPORTED_ENTRY_TYPE",
                    "expanded portable source contains a non-file entry",
                ));
            }
        }
    }
    Ok(())
}

fn package_class(value: &str) -> Result<PortableV2PackageClass, ExportError> {
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

fn expanded(
    plan: &PortableV2ExportPlan,
    stage: &Path,
    l: PortableV2ExportLimits,
    cancelled: &impl Fn() -> bool,
    progress: &mut impl FnMut(PortableV2ExportProgress),
    allocation: &mut ExportAllocationObserver,
) -> Result<[u8; 32], ExportError> {
    fs::create_dir(stage).map_err(storage)?;
    write_bytes(
        stage,
        "data/graphforge-project.json",
        &plan.manifest,
        allocation,
    )?;
    let mut payload = vec![(
        "data/graphforge-project.json".into(),
        plan.manifest.len() as u64,
        Sha256::digest(&plan.manifest).into(),
    )];
    let mut done = 0;
    for (i, f) in plan.files.iter().enumerate() {
        let target = stage.join(&f.path);
        parent(&target)?;
        copy(
            f,
            &target,
            l.copy_buffer_bytes,
            cancelled,
            allocation,
            |n| {
                done += n;
                progress(PortableV2ExportProgress {
                    entries_completed: i + 1,
                    bytes_completed: done,
                    entries_total: plan.files.len() + 5,
                    bytes_total: plan.payload_bytes,
                });
            },
        )?;
        progress(PortableV2ExportProgress {
            entries_completed: i + 2,
            bytes_completed: done,
            entries_total: plan.files.len() + 5,
            bytes_total: plan.payload_bytes,
        });
        payload.push((f.path.clone(), f.length, f.digest));
    }
    payload.sort_by(|a, b| a.0.cmp(&b.0));
    let inv = inventory(&payload, l.max_tag_manifest_bytes)?;
    write_bytes(stage, "manifest-sha256.txt", &inv, allocation)?;
    write_bytes(stage, "bagit.txt", BAGIT, allocation)?;
    write_bytes(stage, "bag-info.txt", BAG_INFO, allocation)?;
    let tags = [
        ("bag-info.txt", BAG_INFO),
        ("bagit.txt", BAGIT),
        ("manifest-sha256.txt", inv.as_slice()),
    ];
    let tag_rows = tags
        .iter()
        .map(|(p, b)| (p.to_string(), b.len() as u64, Sha256::digest(b).into()))
        .collect::<Vec<_>>();
    let tag = inventory(&tag_rows, l.max_tag_manifest_bytes)?;
    write_bytes(stage, "tagmanifest-sha256.txt", &tag, allocation)?;
    progress(PortableV2ExportProgress {
        entries_completed: plan.files.len() + 5,
        bytes_completed: done,
        entries_total: plan.files.len() + 5,
        bytes_total: plan.payload_bytes,
    });
    sync_tree(stage)?;
    let mut all = payload;
    all.extend(tag_rows);
    all.sort_by(|a, b| a.0.cmp(&b.0));
    let mut h = Sha256::new();
    h.update(b"graphforge-expanded/2\0");
    for (p, n, d) in all {
        h.update((p.len() as u64).to_be_bytes());
        h.update(p);
        h.update(n.to_be_bytes());
        h.update(d);
    }
    h.update(tag);
    Ok(h.finalize().into())
}

enum Src<'a> {
    Bytes(Vec<u8>),
    File(&'a PlannedFile),
}
impl Src<'_> {
    fn len(&self) -> u64 {
        match self {
            Self::Bytes(b) => b.len() as u64,
            Self::File(f) => f.length,
        }
    }
}
fn entries(
    plan: &PortableV2ExportPlan,
    max_tag_manifest_bytes: u64,
) -> Result<Vec<(String, Src<'_>)>, ExportError> {
    let mut v = vec![(
        "data/graphforge-project.json".into(),
        Src::Bytes(plan.manifest.clone()),
    )];
    v.extend(plan.files.iter().map(|f| (f.path.clone(), Src::File(f))));
    v.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let inv = inventory(
        &v.iter()
            .map(|(p, s)| {
                (
                    p.clone(),
                    s.len(),
                    match s {
                        Src::Bytes(b) => Sha256::digest(b).into(),
                        Src::File(f) => f.digest,
                    },
                )
            })
            .collect::<Vec<_>>(),
        max_tag_manifest_bytes,
    )?;
    let tags = [
        ("bag-info.txt", BAG_INFO.to_vec()),
        ("bagit.txt", BAGIT.to_vec()),
        ("manifest-sha256.txt", inv),
    ];
    let tag = inventory(
        &tags
            .iter()
            .map(|(p, b)| (p.to_string(), b.len() as u64, Sha256::digest(b).into()))
            .collect::<Vec<_>>(),
        max_tag_manifest_bytes,
    )?;
    v.extend(tags.into_iter().map(|(p, b)| (p.into(), Src::Bytes(b))));
    v.push(("tagmanifest-sha256.txt".into(), Src::Bytes(tag)));
    Ok(v)
}
fn bundle(
    plan: &PortableV2ExportPlan,
    stage: &Path,
    l: PortableV2ExportLimits,
    cancelled: &impl Fn() -> bool,
    progress: &mut impl FnMut(PortableV2ExportProgress),
    allocation: &mut ExportAllocationObserver,
) -> Result<[u8; 32], ExportError> {
    let mut items = entries(plan, l.max_tag_manifest_bytes)?;
    items.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(stage)
        .map_err(storage)?;
    let mut h = Sha256::new();
    let mut done = 0;
    for (i, (path, src)) in items.iter().enumerate() {
        if cancelled() {
            return Err(err("GF_CANCELLED", "portable export cancelled"));
        }
        header(&mut out, &mut h, path, src.len())?;
        allocation.observe(&out)?;
        match src {
            Src::Bytes(b) => {
                emit(&mut out, &mut h, b)?;
                allocation.observe(&out)?;
            }
            Src::File(f) => stream(
                &mut out,
                &mut h,
                f,
                l.copy_buffer_bytes,
                cancelled,
                allocation,
                |n| {
                    done += n;
                    progress(PortableV2ExportProgress {
                        entries_completed: i,
                        bytes_completed: done,
                        entries_total: items.len(),
                        bytes_total: plan.payload_bytes,
                    });
                },
            )?,
        }
        pad(&mut out, &mut h, src.len())?;
        allocation.observe(&out)?;
        progress(PortableV2ExportProgress {
            entries_completed: i + 1,
            bytes_completed: done,
            entries_total: items.len(),
            bytes_total: plan.payload_bytes,
        });
    }
    let end = [0u8; 1024];
    out.write_all(&end).map_err(storage)?;
    allocation.observe(&out)?;
    h.update(end);
    out.sync_all().map_err(storage)?;
    allocation.observe(&out)?;
    Ok(h.finalize().into())
}

fn inspect(
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
fn copy(
    planned: &PlannedFile,
    target: &Path,
    size: usize,
    cancelled: &impl Fn() -> bool,
    allocation: &mut ExportAllocationObserver,
    mut tick: impl FnMut(u64),
) -> Result<(), ExportError> {
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(target)
        .map_err(storage)?;
    if let PlannedSource::Control(bytes) = &planned.source {
        if cancelled() {
            return Err(err("GF_CANCELLED", "portable export cancelled"));
        }
        output.write_all(bytes).map_err(storage)?;
        allocation.observe(&output)?;
        output.sync_all().map_err(storage)?;
        allocation.observe(&output)?;
        tick(bytes.len() as u64);
        return Ok(());
    }
    let (mut input, planned_identity) = open_planned_source(planned)?;
    let mut buffer = vec![0; size];
    let mut digest = Sha256::new();
    let mut bytes_read = 0;
    loop {
        if cancelled() {
            return Err(err("GF_CANCELLED", "portable export cancelled"));
        }
        let count = input.read(&mut buffer).map_err(storage)?;
        if count == 0 {
            break;
        }
        output.write_all(&buffer[..count]).map_err(storage)?;
        digest.update(&buffer[..count]);
        bytes_read += count as u64;
        tick(count as u64);
    }
    output.sync_all().map_err(storage)?;
    allocation.observe(&output)?;
    if bytes_read != planned.length || <[u8; 32]>::from(digest.finalize()) != planned.digest {
        return Err(err("GF_SOURCE_CHANGED", "source changed during export"));
    }
    if let Some(expected) = planned_identity
        && identity(&input.metadata().map_err(storage)?)? != expected
    {
        return Err(err("GF_SOURCE_CHANGED", "source changed during export"));
    }
    Ok(())
}
fn stream(
    out: &mut File,
    transport: &mut Sha256,
    planned: &PlannedFile,
    size: usize,
    cancelled: &impl Fn() -> bool,
    allocation: &mut ExportAllocationObserver,
    mut tick: impl FnMut(u64),
) -> Result<(), ExportError> {
    if let PlannedSource::Control(bytes) = &planned.source {
        if cancelled() {
            return Err(err("GF_CANCELLED", "portable export cancelled"));
        }
        out.write_all(bytes).map_err(storage)?;
        allocation.observe(out)?;
        transport.update(bytes);
        tick(bytes.len() as u64);
        return Ok(());
    }
    let (mut input, planned_identity) = open_planned_source(planned)?;
    let mut buffer = vec![0; size];
    let mut digest = Sha256::new();
    let mut bytes_read = 0;
    loop {
        if cancelled() {
            return Err(err("GF_CANCELLED", "portable export cancelled"));
        }
        let count = input.read(&mut buffer).map_err(storage)?;
        if count == 0 {
            break;
        }
        out.write_all(&buffer[..count]).map_err(storage)?;
        transport.update(&buffer[..count]);
        digest.update(&buffer[..count]);
        bytes_read += count as u64;
        tick(count as u64);
    }
    if bytes_read != planned.length || <[u8; 32]>::from(digest.finalize()) != planned.digest {
        return Err(err("GF_SOURCE_CHANGED", "source changed during export"));
    }
    if let Some(expected) = planned_identity
        && identity(&input.metadata().map_err(storage)?)? != expected
    {
        return Err(err("GF_SOURCE_CHANGED", "source changed during export"));
    }
    Ok(())
}
fn open_planned_source(planned: &PlannedFile) -> Result<(File, Option<Identity>), ExportError> {
    match &planned.source {
        PlannedSource::File {
            path,
            identity: expected,
        } => {
            let input = open_source_no_follow(path)?;
            if identity(&input.metadata().map_err(storage)?)? != *expected {
                return Err(err("GF_SOURCE_CHANGED", "source changed"));
            }
            Ok((input, Some(*expected)))
        }
        PlannedSource::Cas {
            lease,
            digest,
            length,
        } => {
            let source = lease
                .open(digest, *length)
                .map_err(|_| err("GF_SOURCE_CHANGED", "pinned CAS source changed"))?;
            let mut file = source.try_clone_file().map_err(storage)?;
            file.seek(SeekFrom::Start(0)).map_err(storage)?;
            Ok((file, None))
        }
        PlannedSource::Control(_) => unreachable!("control source returned above"),
    }
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
fn header(out: &mut File, h: &mut Sha256, path: &str, size: u64) -> Result<(), ExportError> {
    if let Ok((name, prefix)) = split(path) {
        return raw_header(out, h, name, prefix, size, b'0');
    }
    let suffix = &hex(Sha256::digest(path.as_bytes()).into())[..16];
    let body = pax_path_record(path);
    raw_header(
        out,
        h,
        &format!("PaxHeaders/{suffix}"),
        "",
        body.len() as u64,
        b'x',
    )?;
    emit(out, h, body.as_bytes())?;
    pad(out, h, body.len() as u64)?;
    raw_header(out, h, &format!("PaxFiles/{suffix}"), "", size, b'0')
}
fn raw_header(
    out: &mut File,
    h: &mut Sha256,
    name: &str,
    prefix: &str,
    size: u64,
    kind: u8,
) -> Result<(), ExportError> {
    let mut b = [0u8; 512];
    put(&mut b[..100], name.as_bytes());
    oct(&mut b[100..108], 0o644)?;
    oct(&mut b[108..116], 0)?;
    oct(&mut b[116..124], 0)?;
    oct(&mut b[124..136], size)?;
    oct(&mut b[136..148], 0)?;
    b[148..156].fill(b' ');
    b[156] = kind;
    put(&mut b[257..263], b"ustar\0");
    put(&mut b[263..265], b"00");
    put(&mut b[345..500], prefix.as_bytes());
    let sum: u64 = b.iter().map(|x| u64::from(*x)).sum();
    b[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
    out.write_all(&b).map_err(storage)?;
    h.update(b);
    Ok(())
}
fn pax_path_record(path: &str) -> String {
    let value = format!(" path={path}\n");
    let mut digits = 1;
    loop {
        let length = digits + value.len();
        let actual_digits = length.to_string().len();
        if actual_digits == digits {
            return format!("{length}{value}");
        }
        digits = actual_digits;
    }
}
fn split(p: &str) -> Result<(&str, &str), ExportError> {
    if p.len() <= 100 {
        return Ok((p, ""));
    }
    for (i, _) in p.match_indices('/').rev() {
        let (pre, name) = p.split_at(i);
        if pre.len() <= 155 && name.len() - 1 <= 100 {
            return Ok((&name[1..], pre));
        }
    }
    Err(err("GF_INVALID_PORTABLE_PATH", "path cannot fit ustar"))
}
fn oct(dst: &mut [u8], n: u64) -> Result<(), ExportError> {
    let w = dst.len() - 1;
    let s = format!("{n:0w$o}");
    if s.len() > w {
        return Err(limit("tar field overflow"));
    }
    dst[..w].copy_from_slice(s.as_bytes());
    dst[w] = 0;
    Ok(())
}
fn put(d: &mut [u8], s: &[u8]) {
    d[..s.len()].copy_from_slice(s);
}
fn emit(o: &mut File, h: &mut Sha256, b: &[u8]) -> Result<(), ExportError> {
    o.write_all(b).map_err(storage)?;
    h.update(b);
    Ok(())
}
fn pad(output: &mut File, digest: &mut Sha256, length: u64) -> Result<(), ExportError> {
    let padding = ((512 - length % 512) % 512) as usize;
    let zeroes = [0u8; 512];
    emit(output, digest, &zeroes[..padding])
}
fn inventory(rows: &[(String, u64, [u8; 32])], limit_bytes: u64) -> Result<Vec<u8>, ExportError> {
    let mut o = Vec::new();
    for (p, _, d) in rows {
        let row_bytes = 64_u64
            .checked_add(2)
            .and_then(|value| value.checked_add(p.len() as u64))
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| limit("tag inventory size overflow"))?;
        if (o.len() as u64).saturating_add(row_bytes) > limit_bytes {
            return Err(limit("tag inventory exceeds configured limit"));
        }
        o.extend(hex(*d).bytes());
        o.extend(b"  ");
        o.extend(p.bytes());
        o.push(b'\n');
    }
    Ok(o)
}
fn identity(m: &fs::Metadata) -> Result<Identity, ExportError> {
    if !m.is_file() || m.file_type().is_symlink() {
        return Err(err("GF_UNSUPPORTED_ENTRY_TYPE", "not a regular file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if m.nlink() != 1 {
            return Err(err("GF_UNSUPPORTED_ENTRY_TYPE", "hard-linked source"));
        }
        Ok(Identity {
            dev: m.dev(),
            ino: m.ino(),
            len: m.len(),
            modified: m.modified().ok(),
        })
    }
    #[cfg(not(unix))]
    {
        Ok(Identity {
            len: m.len(),
            modified: m.modified().ok(),
        })
    }
}
#[cfg(unix)]
pub(crate) fn open_source_no_follow(path: &Path) -> Result<File, ExportError> {
    use std::path::Component;
    if fs::symlink_metadata(path)
        .map_err(storage)?
        .file_type()
        .is_symlink()
    {
        return Err(err("GF_UNSUPPORTED_ENTRY_TYPE", "source is a link"));
    }
    // Resolve fixed platform aliases (for example macOS /var -> /private/var),
    // then pin every component of that canonical path with openat+NOFOLLOW.
    let canonical = path.canonicalize().map_err(storage)?;
    let components = canonical
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(Ok(value)),
            Component::RootDir | Component::CurDir => None,
            Component::ParentDir | Component::Prefix(_) => Some(Err(err(
                "GF_UNSUPPORTED_ENTRY_TYPE",
                "source path contains an unsafe component",
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let Some((last, parents)) = components.split_last() else {
        return Err(err("GF_UNSUPPORTED_ENTRY_TYPE", "source is not a file"));
    };
    let mut directory = rustix::fs::open(
        if canonical.is_absolute() {
            Path::new("/")
        } else {
            Path::new(".")
        },
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(storage)?;
    for component in parents {
        directory = rustix::fs::openat(
            &directory,
            *component,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(storage)?;
    }
    let descriptor = rustix::fs::openat(
        &directory,
        *last,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::NONBLOCK
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(storage)?;
    Ok(descriptor.into())
}
#[cfg(windows)]
pub(crate) fn open_source_no_follow(path: &Path) -> Result<File, ExportError> {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    reject_windows_reparse_components(path)?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
        .map_err(storage)?;
    reject_windows_reparse_components(path)?;
    Ok(file)
}
#[cfg(windows)]
fn reject_windows_reparse_components(path: &Path) -> Result<(), ExportError> {
    use std::os::windows::fs::MetadataExt as _;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        if current.as_os_str().is_empty() {
            continue;
        }
        let metadata = fs::symlink_metadata(&current).map_err(storage)?;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(err(
                "GF_UNSUPPORTED_ENTRY_TYPE",
                "source path contains a reparse point",
            ));
        }
    }
    Ok(())
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
fn portable_id(s: &str) -> String {
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
fn portable_participant_id(capability: &str, family: &str) -> String {
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
fn validate_limits(l: PortableV2ExportLimits) -> Result<(), ExportError> {
    if l.max_components == 0
        || l.max_entries == 0
        || l.max_manifest_bytes == 0
        || l.max_tag_manifest_bytes == 0
        || l.max_path_bytes == 0
        || l.copy_buffer_bytes == 0
        || l.copy_buffer_bytes > 64 * 1024 * 1024
    {
        return Err(limit("invalid limits"));
    }
    Ok(())
}
fn reject_destination(p: &Path) -> Result<(), ExportError> {
    if p.exists() {
        return Err(err("GF_DESTINATION_EXISTS", "destination exists"));
    }
    let q = p
        .parent()
        .ok_or_else(|| err("GF_INVALID_DESTINATION", "missing parent"))?;
    let m = fs::symlink_metadata(q).map_err(storage)?;
    if !m.is_dir() || m.file_type().is_symlink() {
        return Err(err("GF_INVALID_DESTINATION", "unsafe parent"));
    }
    Ok(())
}
#[cfg(any(target_vendor = "apple", target_os = "linux", target_os = "redox"))]
fn publish_no_replace(stage: &Path, destination: &Path) -> std::io::Result<()> {
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        stage,
        rustix::fs::CWD,
        destination,
        rustix::fs::RenameFlags::NOREPLACE,
    )?;
    Ok(())
}
#[cfg(windows)]
fn publish_no_replace(stage: &Path, destination: &Path) -> std::io::Result<()> {
    // Windows rename is non-replacing. The destination was also checked before
    // staging; any intervening creation makes this operation fail closed.
    fs::rename(stage, destination)
}
#[cfg(not(any(
    target_vendor = "apple",
    target_os = "linux",
    target_os = "redox",
    windows
)))]
fn publish_no_replace(_: &Path, _: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "atomic no-replace publication is unsupported on this platform",
    ))
}
fn parent(p: &Path) -> Result<(), ExportError> {
    fs::create_dir_all(p.parent().unwrap()).map_err(storage)
}
fn write_bytes(
    root: &Path,
    p: &str,
    b: &[u8],
    allocation: &mut ExportAllocationObserver,
) -> Result<(), ExportError> {
    let p = root.join(p);
    parent(&p)?;
    let mut f = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(p)
        .map_err(storage)?;
    f.write_all(b).map_err(storage)?;
    allocation.observe(&f)?;
    f.sync_all().map_err(storage)?;
    allocation.observe(&f)
}
fn sync_tree(root: &Path) -> Result<(), ExportError> {
    let mut dirs = vec![root.into()];
    let mut i = 0;
    while i < dirs.len() {
        for e in fs::read_dir(&dirs[i]).map_err(storage)? {
            let e = e.map_err(storage)?;
            if e.file_type().map_err(storage)?.is_dir() {
                dirs.push(e.path());
            }
        }
        i += 1;
    }
    dirs.sort_by_key(|p: &PathBuf| std::cmp::Reverse(p.components().count()));
    for d in dirs {
        sync_dir(&d)?;
    }
    Ok(())
}
fn sync_dir(p: &Path) -> Result<(), ExportError> {
    sync_directory_handle(p).map_err(storage)
}
#[cfg(not(windows))]
fn sync_directory_handle(p: &Path) -> std::io::Result<()> {
    File::open(p)?.sync_all()
}
#[cfg(windows)]
fn sync_directory_handle(p: &Path) -> std::io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt as _;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(p)?
        .sync_all()
}
fn remove(p: &Path) {
    if p.is_dir() {
        let _ = fs::remove_dir_all(p);
    } else {
        let _ = fs::remove_file(p);
    }
}
fn hex(d: [u8; 32]) -> String {
    use std::fmt::Write as _;
    d.iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        })
}
fn limit(m: &str) -> ExportError {
    let _ = m;
    PortableV2Error::new(PortableV2ErrorCode::LimitExceeded, "export limit exceeded")
}
fn err(c: &str, m: &str) -> ExportError {
    let code = match c {
        "GF_CANCELLED" => PortableV2ErrorCode::Cancelled,
        "GF_LIMIT_EXCEEDED" => PortableV2ErrorCode::LimitExceeded,
        "GF_SOURCE_CHANGED" => PortableV2ErrorCode::ConcurrentMutation,
        "GF_INVALID_PORTABLE_PATH" => PortableV2ErrorCode::InvalidPath,
        "GF_UNSUPPORTED_ENTRY_TYPE" => PortableV2ErrorCode::InvalidStructure,
        "GF_DUPLICATE_PORTABLE_PATH" => PortableV2ErrorCode::DuplicateEntry,
        "GF_INTEGRITY_FAILED" => PortableV2ErrorCode::DigestMismatch,
        _ => PortableV2ErrorCode::Io,
    };
    let _ = m;
    PortableV2Error::new(code, "portable-v2 export failed")
}
fn storage(e: impl std::fmt::Display) -> ExportError {
    let _ = e;
    PortableV2Error::new(PortableV2ErrorCode::Io, "portable-v2 I/O failed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::open_or_initialize_project;

    fn compact_graph_generation() -> (tempfile::TempDir, ResolvedProjectGeneration) {
        let project = tempfile::tempdir().unwrap();
        open_or_initialize_project(project.path()).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let relative = PathBuf::from("topology/nodes/part-00000.parquet");
        fs::create_dir_all(workspace.path().join(relative.parent().unwrap())).unwrap();
        fs::write(workspace.path().join(&relative), b"compact graph payload").unwrap();
        let lease = crate::begin_graph_object_publication(project.path()).unwrap();
        let mut state = crate::GraphManifestState::empty();
        let (root, _) =
            crate::append_graph_files_v2(&lease, workspace.path(), &mut state, &[relative], &[])
                .unwrap();
        let request = crate::ProjectGenerationRequest {
            transaction_uuid: Uuid::new_v4(),
            generation_uuid: Uuid::new_v4(),
            capabilities: vec![crate::ProjectCapability {
                capability_id: crate::GRAPH_CAPABILITY_ID.into(),
                capability_version: 1,
            }],
            participants: vec![crate::graph_files_root_participant(&root).unwrap()],
        };
        let crate::ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation(project.path(), &request).unwrap()
        else {
            panic!("fresh compact generation replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish_with_graph_objects(&lease)
            .unwrap();
        drop(lease);
        let generation = crate::resolve_project_generation(project.path()).unwrap();
        (project, generation)
    }

    fn graph_generation() -> (tempfile::TempDir, ResolvedProjectGeneration) {
        graph_generation_with_composition(false)
    }

    fn graph_generation_with_composition(
        include_composition: bool,
    ) -> (tempfile::TempDir, ResolvedProjectGeneration) {
        let project = tempfile::tempdir().unwrap();
        let parent = open_or_initialize_project(project.path()).unwrap();
        let tree = tempfile::tempdir().unwrap();
        fs::write(tree.path().join("a.parquet"), b"graph-a").unwrap();
        fs::create_dir(tree.path().join("properties")).unwrap();
        fs::write(tree.path().join("properties/Person.parquet"), b"person").unwrap();
        let (_, inventory) = crate::capture_graph_files(tree.path()).unwrap();
        let mut participants = crate::empty_workspace_participants().unwrap();
        participants.insert(0, inventory);
        if include_composition {
            let document = graphforge_ontology::OntologyDoc {
                ontology_id: "https://graphforge.dev/ontology/portable".into(),
                version: "release-2026.08".into(),
                entity_types: vec![],
                relation_types: vec![],
                properties: vec![],
                constraints: vec![],
                migrations: vec![],
            };
            let legacy = crate::WorkspaceOntology {
                contract_version: 1,
                mode: crate::WorkspaceOntologyMode::Strict,
                source_format: Some(crate::WorkspaceOntologySourceFormat::Json),
                canonical_ontology_sha256: Some("a".repeat(64)),
                canonical_ontology: Some(serde_json::to_value(document).unwrap()),
            };
            let composition = crate::WorkspaceOntologyComposition::virtual_legacy(&legacy)
                .unwrap()
                .unwrap();
            participants.push(composition.to_project_participant().unwrap());
            participants.sort_by(|left, right| {
                (&left.capability_id, &left.record_family_id)
                    .cmp(&(&right.capability_id, &right.record_family_id))
            });
        }
        let request = crate::ProjectGenerationRequest {
            transaction_uuid: Uuid::new_v4(),
            generation_uuid: Uuid::new_v4(),
            capabilities: vec![
                crate::ProjectCapability {
                    capability_id: "graph".into(),
                    capability_version: 1,
                },
                crate::ProjectCapability {
                    capability_id: "workspace".into(),
                    capability_version: 1,
                },
            ],
            participants,
        };
        let crate::ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation_with_graph_tree(
                project.path(),
                &request,
                Some(tree.path()),
            )
            .unwrap()
        else {
            panic!("fresh graph generation replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        drop(parent);
        let generation = crate::resolve_project_generation(project.path()).unwrap();
        (project, generation)
    }

    fn graph_generation_with_bridge() -> (tempfile::TempDir, ResolvedProjectGeneration) {
        use graphforge_ontology::{
            ActivationMode, AuthoredModule, BridgeAssertion, BridgeDocument, BridgePredicate,
            BridgeProvenance, BridgeSetId, CompositionLimits, EntityTypeDef,
            InventoryCompileRequest, MappingMethod, OntologyDoc, OntologyModuleId, QualifiedSymbol,
            SymbolKind, bridge_document_digest, compile_inventory, module_document_digest,
        };

        let module = |ontology_id: &str| {
            let document = OntologyDoc {
                ontology_id: ontology_id.into(),
                version: "opaque-v1".into(),
                entity_types: vec![EntityTypeDef {
                    name: "Person".into(),
                    r#abstract: false,
                    parent: None,
                }],
                relation_types: Vec::new(),
                properties: Vec::new(),
                constraints: Vec::new(),
                migrations: Vec::new(),
            };
            AuthoredModule {
                id: OntologyModuleId {
                    ontology_id: document.ontology_id.clone(),
                    authored_version: document.version.clone(),
                    canonical_digest: module_document_digest(&document).unwrap(),
                },
                dependencies: Vec::new(),
                doc: document,
                allow_projected_identity: false,
            }
        };
        let source = module("https://graphforge.dev/ontology/source");
        let target = module("https://graphforge.dev/ontology/target");
        let qualified = |module: &AuthoredModule| QualifiedSymbol {
            module: module.id.clone(),
            kind: SymbolKind::Entity,
            local_id: "Person".into(),
        };
        let bridge = BridgeDocument {
            bridge_id: "https://graphforge.dev/bridge/person".into(),
            authored_version: "bridge-v1".into(),
            source_modules: vec![source.id.clone()],
            target_modules: vec![target.id.clone()],
            dependencies: Vec::new(),
            shared_surfaces: Vec::new(),
            assertions: vec![BridgeAssertion {
                source: qualified(&source),
                target: qualified(&target),
                predicate: BridgePredicate::Equivalent,
                directional: false,
                provenance: BridgeProvenance {
                    method: MappingMethod::Authored,
                    confidence: None,
                    justification: "portable fixture".into(),
                    evidence_refs: Vec::new(),
                },
                valid_from: None,
                valid_to: None,
            }],
            enforcement: Some(ActivationMode::Strict),
        };
        let bridge_id = BridgeSetId {
            bridge_id: bridge.bridge_id.clone(),
            authored_version: bridge.authored_version.clone(),
            canonical_digest: bridge_document_digest(&bridge).unwrap(),
        };
        let compiled = compile_inventory(InventoryCompileRequest {
            modules: &[source, target],
            bridges: &[bridge_id],
            activation: &[],
            profile_default: ActivationMode::Strict,
            limits: CompositionLimits::default(),
            cancelled: None,
        })
        .unwrap();
        let composition =
            crate::WorkspaceOntologyComposition::from_compiled(&compiled, vec![bridge]);

        let project = tempfile::tempdir().unwrap();
        let parent = open_or_initialize_project(project.path()).unwrap();
        let mut participants = crate::empty_workspace_participants().unwrap();
        participants.push(composition.to_project_participant().unwrap());
        participants.sort_by(|left, right| {
            (&left.capability_id, &left.record_family_id)
                .cmp(&(&right.capability_id, &right.record_family_id))
        });
        let request = crate::ProjectGenerationRequest {
            transaction_uuid: Uuid::new_v4(),
            generation_uuid: Uuid::new_v4(),
            capabilities: vec![
                crate::ProjectCapability {
                    capability_id: "graph".into(),
                    capability_version: 1,
                },
                crate::ProjectCapability {
                    capability_id: "workspace".into(),
                    capability_version: 1,
                },
            ],
            participants,
        };
        let crate::ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation(project.path(), &request).unwrap()
        else {
            panic!("fresh composition generation replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        drop(parent);
        let generation = crate::resolve_project_generation(project.path()).unwrap();
        (project, generation)
    }

    fn graph_generation_with_transitive_bridge_chain() -> (
        tempfile::TempDir,
        ResolvedProjectGeneration,
        PortableV2ExactIdentity,
        String,
    ) {
        use graphforge_ontology::{
            ActivationMode, AuthoredModule, BridgeAssertion, BridgeDocument, BridgePredicate,
            BridgeProvenance, BridgeSetId, CompositionLimits, EntityTypeDef,
            InventoryCompileRequest, MappingMethod, OntologyDoc, OntologyModuleId, QualifiedSymbol,
            SymbolKind, bridge_document_digest, compile_inventory, module_document_digest,
        };
        let module = |name: &str, dependencies: Vec<OntologyModuleId>| {
            let document = OntologyDoc {
                ontology_id: format!("https://graphforge.dev/ontology/{name}"),
                version: "v1".into(),
                entity_types: vec![EntityTypeDef {
                    name: "Person".into(),
                    r#abstract: false,
                    parent: None,
                }],
                relation_types: vec![],
                properties: vec![],
                constraints: vec![],
                migrations: vec![],
            };
            AuthoredModule {
                id: OntologyModuleId {
                    ontology_id: document.ontology_id.clone(),
                    authored_version: document.version.clone(),
                    canonical_digest: module_document_digest(&document).unwrap(),
                },
                dependencies,
                doc: document,
                allow_projected_identity: false,
            }
        };
        let base = module("base", vec![]);
        let source = module("source-chain", vec![base.id.clone()]);
        let target = module("target-chain", vec![]);
        let unrelated = module("unrelated", vec![]);
        let assertion = |from: &AuthoredModule, to: &AuthoredModule| BridgeAssertion {
            source: QualifiedSymbol {
                module: from.id.clone(),
                kind: SymbolKind::Entity,
                local_id: "Person".into(),
            },
            target: QualifiedSymbol {
                module: to.id.clone(),
                kind: SymbolKind::Entity,
                local_id: "Person".into(),
            },
            predicate: BridgePredicate::Equivalent,
            directional: false,
            provenance: BridgeProvenance {
                method: MappingMethod::Authored,
                confidence: None,
                justification: "transitive portable closure fixture".into(),
                evidence_refs: vec![],
            },
            valid_from: None,
            valid_to: None,
        };
        let bridge_a = BridgeDocument {
            bridge_id: "https://graphforge.dev/bridge/a".into(),
            authored_version: "v1".into(),
            source_modules: vec![base.id.clone()],
            target_modules: vec![target.id.clone()],
            dependencies: vec![],
            shared_surfaces: vec![],
            assertions: vec![assertion(&base, &target)],
            enforcement: Some(ActivationMode::Advisory),
        };
        let bridge_a_id = BridgeSetId {
            bridge_id: bridge_a.bridge_id.clone(),
            authored_version: bridge_a.authored_version.clone(),
            canonical_digest: bridge_document_digest(&bridge_a).unwrap(),
        };
        let bridge_b = BridgeDocument {
            bridge_id: "https://graphforge.dev/bridge/b".into(),
            authored_version: "v1".into(),
            source_modules: vec![source.id.clone()],
            target_modules: vec![target.id.clone()],
            dependencies: vec![bridge_a_id.clone()],
            shared_surfaces: vec![],
            assertions: vec![assertion(&source, &target)],
            enforcement: Some(ActivationMode::Strict),
        };
        let bridge_b_id = BridgeSetId {
            bridge_id: bridge_b.bridge_id.clone(),
            authored_version: bridge_b.authored_version.clone(),
            canonical_digest: bridge_document_digest(&bridge_b).unwrap(),
        };
        let modules = [base, source, target, unrelated];
        let compiled = compile_inventory(InventoryCompileRequest {
            modules: &modules,
            bridges: &[bridge_a_id, bridge_b_id.clone()],
            activation: &[],
            profile_default: ActivationMode::Strict,
            limits: CompositionLimits::default(),
            cancelled: None,
        })
        .unwrap();
        let composition =
            crate::WorkspaceOntologyComposition::from_compiled(&compiled, vec![bridge_a, bridge_b]);
        let root_identity = exact_identity(
            &bridge_b_id.bridge_id,
            &bridge_b_id.authored_version,
            &bridge_b_id.canonical_digest,
        );
        let unrelated_digest = modules[3].id.canonical_digest.clone();
        let project = tempfile::tempdir().unwrap();
        open_or_initialize_project(project.path()).unwrap();
        let mut participants = crate::empty_workspace_participants().unwrap();
        participants.push(composition.to_project_participant().unwrap());
        participants.sort_by(|left, right| {
            (&left.capability_id, &left.record_family_id)
                .cmp(&(&right.capability_id, &right.record_family_id))
        });
        let request = crate::ProjectGenerationRequest {
            transaction_uuid: Uuid::new_v4(),
            generation_uuid: Uuid::new_v4(),
            capabilities: vec![
                crate::ProjectCapability {
                    capability_id: "graph".into(),
                    capability_version: 1,
                },
                crate::ProjectCapability {
                    capability_id: "workspace".into(),
                    capability_version: 1,
                },
            ],
            participants,
        };
        let crate::ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation(project.path(), &request).unwrap()
        else {
            panic!("fresh transitive composition replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        let generation = crate::resolve_project_generation(project.path()).unwrap();
        (project, generation, root_identity, unrelated_digest)
    }

    fn resign_test_manifest(plan: &mut PortableV2ExportPlan) {
        let mut manifest: serde_json::Value = serde_json::from_slice(&plan.manifest).unwrap();
        manifest.as_object_mut().unwrap().remove("package_digest");
        let semantic = canonical_json(&manifest).unwrap();
        let mut digest = Sha256::new();
        digest.update(b"graphforge-project/2\0");
        digest.update(semantic);
        plan.package_digest = digest.finalize().into();
        manifest.as_object_mut().unwrap().insert(
            "package_digest".into(),
            serde_json::Value::String(format!("sha256:{}", hex(plan.package_digest))),
        );
        plan.manifest = canonical_json(&manifest).unwrap();
    }

    fn replace_test_control(plan: &mut PortableV2ExportPlan, path: &str, bytes: Vec<u8>) {
        let file = plan
            .files
            .iter_mut()
            .find(|file| file.path == path)
            .unwrap();
        let old_length = file.length;
        file.length = bytes.len() as u64;
        file.digest = Sha256::digest(&bytes).into();
        file.source = PlannedSource::Control(bytes);
        plan.payload_bytes = plan
            .payload_bytes
            .checked_sub(old_length)
            .unwrap()
            .checked_add(file.length)
            .unwrap();

        let mut manifest: serde_json::Value = serde_json::from_slice(&plan.manifest).unwrap();
        let descriptor = manifest["components"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .flat_map(|component| component["files"].as_array_mut().unwrap())
            .find(|descriptor| descriptor["path"] == path)
            .unwrap();
        descriptor["length"] = file.length.into();
        descriptor["sha256"] = hex(file.digest).into();
        plan.manifest = canonical_json(&manifest).unwrap();
        resign_test_manifest(plan);
    }

    fn write_test_representations(plan: &PortableV2ExportPlan, root: &Path) -> (PathBuf, PathBuf) {
        let expanded_path = root.join("hostile.gfproject");
        let bundle_path = root.join("hostile.gfpb");
        let limits = PortableV2ExportLimits::default();
        let mut allocation = ExportAllocationObserver::default();
        expanded(
            plan,
            &expanded_path,
            limits,
            &|| false,
            &mut |_| {},
            &mut allocation,
        )
        .unwrap();
        bundle(
            plan,
            &bundle_path,
            limits,
            &|| false,
            &mut |_| {},
            &mut allocation,
        )
        .unwrap();
        (expanded_path, bundle_path)
    }

    #[test]
    fn composition_projection_has_one_identity_in_both_forms_and_is_not_runtime_authority() {
        let (_project, generation) = graph_generation_with_composition(true);
        let limits = PortableV2ExportLimits::default();
        let plan = plan_complete_portable_v2(&generation, limits).unwrap();
        let output = tempfile::tempdir().unwrap();
        let expanded = output.path().join("composition.gfproject");
        let bundle = output.path().join("composition.gfpb");
        let cancelled = AtomicBool::new(false);
        let expanded_receipt = export_complete_portable_v2(
            &plan,
            &expanded,
            PortableV2Output::Expanded,
            limits,
            &cancelled,
            |_| {},
        )
        .unwrap();
        let bundle_receipt = export_complete_portable_v2(
            &plan,
            &bundle,
            PortableV2Output::Bundle,
            limits,
            &cancelled,
            |_| {},
        )
        .unwrap();
        assert_eq!(
            expanded_receipt.package_digest,
            bundle_receipt.package_digest
        );
        assert!(
            expanded_receipt.allocation_identity_allocated_bytes.len() > 1,
            "expanded writer must report each exact published identity"
        );
        assert_eq!(
            bundle_receipt.allocation_identity_allocated_bytes.len(),
            1,
            "bundle writer must report its one exact published identity"
        );
        let expanded_report =
            verify_portable_v2(&expanded, PortableV2Mode::Full, limits, Some(&cancelled)).unwrap();
        let bundle_report =
            verify_portable_v2(&bundle, PortableV2Mode::Full, limits, Some(&cancelled)).unwrap();
        assert_eq!(
            expanded_report.ontology_composition,
            bundle_report.ontology_composition
        );
        assert!(expanded_report.ontology_composition.is_some());

        let runtime: serde_json::Value = serde_json::from_slice(
            &fs::read(expanded.join(
                "data/components/compatibility/graphforge-runtime-map/runtime-generation.json",
            ))
            .unwrap(),
        )
        .unwrap();
        assert!(
            runtime["participants"]
                .as_array()
                .unwrap()
                .iter()
                .all(|participant| {
                    participant["record_family_id"] != crate::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY
                })
        );

        let supported = generation
            .capabilities()
            .into_iter()
            .map(|capability| crate::ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect::<Vec<_>>();
        let imported = output.path().join("imported-project");
        let import_receipt = crate::import_complete_portable_v2(
            &expanded,
            &imported,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported,
            limits,
            Some(&cancelled),
        )
        .unwrap();
        assert!(import_receipt.staged_composition.is_some());
        let reopened = crate::resolve_project_generation(&imported).unwrap();
        let staged = crate::load_portable_ontology_staging(&reopened, limits)
            .unwrap()
            .expect("verified composition remains durably staged");
        assert_eq!(
            staged.package_digest,
            format!("sha256:{}", hex(expanded_receipt.package_digest))
        );
        assert!(
            reopened
                .participant_snapshots()
                .unwrap()
                .iter()
                .all(|participant| participant.record_family_id
                    != crate::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY)
        );

        let cancelled_before_import = AtomicBool::new(true);
        let cancelled_target = output.path().join("cancelled-project");
        let error = crate::import_complete_portable_v2(
            &expanded,
            &cancelled_target,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported,
            limits,
            Some(&cancelled_before_import),
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Cancelled);
        assert!(!cancelled_target.exists());
    }

    #[test]
    fn tck_evidence_changes_package_identity_without_changing_composition_identity() {
        let (_project, generation) = graph_generation_with_composition(true);
        let limits = PortableV2ExportLimits::default();
        let base = plan_complete_portable_v2(&generation, limits).unwrap();
        let base_manifest: serde_json::Value = serde_json::from_slice(&base.manifest).unwrap();
        let composition = base
            .files
            .iter()
            .find(|file| file.path == crate::project_portable_v2::ONTOLOGY_COMPOSITION_PATH);
        let PlannedSource::Control(composition_bytes) = &composition.unwrap().source else {
            panic!("composition must be inline")
        };
        let composition: PortableV2OntologyComposition =
            serde_json::from_slice(composition_bytes).unwrap();

        let evidence_bytes = canonical_json(&serde_json::json!({
            "contract": "graphforge-tck-evidence/1",
            "passed": 3897,
            "total": 3897
        }))
        .unwrap();
        let evidence_digest: [u8; 32] = Sha256::digest(&evidence_bytes).into();
        let evidence_id = "tck-certification-evidence";
        let evidence_path = "data/components/evidence/tck-certification-evidence/report.json";
        let mut with_evidence = base.clone();
        with_evidence.payload_bytes += evidence_bytes.len() as u64;
        with_evidence.files.push(PlannedFile {
            source: PlannedSource::Control(evidence_bytes.clone()),
            path: evidence_path.into(),
            length: evidence_bytes.len() as u64,
            digest: evidence_digest,
        });
        with_evidence
            .files
            .sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));

        let mut manifest = base_manifest.clone();
        manifest["selection"]["roots"]
            .as_array_mut()
            .unwrap()
            .push(evidence_id.into());
        manifest["selection"]["roots"]
            .as_array_mut()
            .unwrap()
            .sort_by(|left, right| left.as_str().cmp(&right.as_str()));
        manifest["components"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "kind": "evidence",
                "participant_id": evidence_id,
                "required_dependencies": [],
                "files": [{
                    "media_type": "application/json",
                    "path": evidence_path,
                    "length": evidence_bytes.len(),
                    "sha256": hex(evidence_digest)
                }]
            }));
        manifest["components"]
            .as_array_mut()
            .unwrap()
            .sort_by(|left, right| {
                (left["kind"].as_str(), left["participant_id"].as_str())
                    .cmp(&(right["kind"].as_str(), right["participant_id"].as_str()))
            });
        with_evidence.manifest = canonical_json(&manifest).unwrap();
        resign_test_manifest(&mut with_evidence);
        assert_ne!(base.package_digest, with_evidence.package_digest);

        let output = tempfile::tempdir().unwrap();
        let (expanded, bundle) = write_test_representations(&with_evidence, output.path());
        let expanded_report =
            verify_portable_v2(&expanded, PortableV2Mode::Full, limits, None).unwrap();
        let bundle_report =
            verify_portable_v2(&bundle, PortableV2Mode::Full, limits, None).unwrap();
        assert_eq!(expanded_report.package_digest, bundle_report.package_digest);
        assert_eq!(
            expanded_report.ontology_composition,
            Some(composition.clone())
        );
        assert_eq!(
            bundle_report.ontology_composition,
            Some(composition.clone())
        );
        assert!(
            !serde_json::to_string(&composition)
                .unwrap()
                .contains("tck-certification-evidence")
        );
    }

    #[test]
    fn semantic_tamper_and_future_feature_precede_payload() {
        let (_project, generation) = graph_generation_with_composition(true);
        let limits = PortableV2ExportLimits::default();
        let original = plan_complete_portable_v2(&generation, limits).unwrap();
        let module_path = original
            .files
            .iter()
            .find(|file| file.path.ends_with("/module.json"))
            .unwrap()
            .path
            .clone();
        let module_bytes = match &original
            .files
            .iter()
            .find(|file| file.path == module_path)
            .unwrap()
            .source
        {
            PlannedSource::Control(bytes) => bytes.clone(),
            PlannedSource::File { .. } | PlannedSource::Cas { .. } => {
                panic!("projected module must be an inline control")
            }
        };
        let mut module: serde_json::Value = serde_json::from_slice(&module_bytes).unwrap();
        module["ontology_id"] = "https://graphforge.dev/ontology/tampered".into();
        let tampered_module = canonical_json(&module).unwrap();

        let mut semantic_tamper = original.clone();
        replace_test_control(&mut semantic_tamper, &module_path, tampered_module.clone());
        let outputs = tempfile::tempdir().unwrap();
        let (expanded, bundle) = write_test_representations(&semantic_tamper, outputs.path());
        for package in [&expanded, &bundle] {
            let error =
                verify_portable_v2(package, PortableV2Mode::Full, limits, None).unwrap_err();
            assert!(matches!(
                error.code,
                PortableV2ErrorCode::InvalidStructure | PortableV2ErrorCode::DigestMismatch
            ));
            assert_eq!(error.entry.as_deref(), Some(module_path.as_str()));
        }

        let mut future = semantic_tamper;
        let control_path = crate::project_portable_v2::ONTOLOGY_COMPOSITION_PATH;
        let control_bytes = match &future
            .files
            .iter()
            .find(|file| file.path == control_path)
            .unwrap()
            .source
        {
            PlannedSource::Control(bytes) => bytes.clone(),
            PlannedSource::File { .. } | PlannedSource::Cas { .. } => {
                panic!("composition must be an inline control")
            }
        };
        let mut control: serde_json::Value = serde_json::from_slice(&control_bytes).unwrap();
        control["required_features"]
            .as_array_mut()
            .unwrap()
            .push("future-contract@2".into());
        control["required_features"]
            .as_array_mut()
            .unwrap()
            .sort_by(|left, right| left.as_str().cmp(&right.as_str()));
        control
            .as_object_mut()
            .unwrap()
            .remove("composition_digest");
        let mut digest = Sha256::new();
        digest.update(b"graphforge-ontology-composition/1\0");
        digest.update(canonical_json(&control).unwrap());
        control["composition_digest"] = format!("sha256:{}", hex(digest.finalize().into())).into();
        replace_test_control(&mut future, control_path, canonical_json(&control).unwrap());
        let outputs = tempfile::tempdir().unwrap();
        let (expanded, bundle) = write_test_representations(&future, outputs.path());
        for package in [&expanded, &bundle] {
            let error =
                verify_portable_v2(package, PortableV2Mode::Full, limits, None).unwrap_err();
            assert_eq!(error.code, PortableV2ErrorCode::UnsupportedFuture);
            assert_eq!(error.entry.as_deref(), Some(control_path));
        }
    }

    #[test]
    fn bridge_semantic_tamper_fails_in_both_representations() {
        let (_project, generation) = graph_generation_with_bridge();
        let limits = PortableV2ExportLimits::default();
        let mut plan = plan_complete_portable_v2(&generation, limits).unwrap();
        let bridge_path = plan
            .files
            .iter()
            .find(|file| file.path.ends_with("/bridge.json"))
            .unwrap()
            .path
            .clone();
        let bytes = match &plan
            .files
            .iter()
            .find(|file| file.path == bridge_path)
            .unwrap()
            .source
        {
            PlannedSource::Control(bytes) => bytes.clone(),
            PlannedSource::File { .. } | PlannedSource::Cas { .. } => {
                panic!("projected bridge must be an inline control")
            }
        };
        let mut bridge: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        bridge["authored_version"] = "tampered-v2".into();
        replace_test_control(&mut plan, &bridge_path, canonical_json(&bridge).unwrap());
        let outputs = tempfile::tempdir().unwrap();
        let (expanded, bundle) = write_test_representations(&plan, outputs.path());
        for package in [&expanded, &bundle] {
            let error =
                verify_portable_v2(package, PortableV2Mode::Full, limits, None).unwrap_err();
            assert_eq!(error.code, PortableV2ErrorCode::DigestMismatch);
            assert_eq!(error.entry.as_deref(), Some(bridge_path.as_str()));
        }
    }

    #[test]
    fn semantic_descriptor_kind_path_and_media_mismatches_fail_in_both_forms() {
        let (_project, generation) = graph_generation_with_composition(true);
        let limits = PortableV2ExportLimits::default();
        let original = plan_complete_portable_v2(&generation, limits).unwrap();
        let module_path = original
            .files
            .iter()
            .find(|file| file.path.ends_with("/module.json"))
            .unwrap()
            .path
            .clone();
        for mutation in ["kind", "path", "media"] {
            let mut plan = original.clone();
            let mut manifest: serde_json::Value = serde_json::from_slice(&plan.manifest).unwrap();
            let component = manifest["components"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|component| {
                    component["files"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|file| file["path"] == module_path)
                })
                .unwrap();
            match mutation {
                "kind" => component["kind"] = "schema".into(),
                "path" => {
                    component["files"][0]["path"] =
                        "data/components/ontology/wrong/module.json".into()
                }
                "media" => component["files"][0]["media_type"] = "application/octet-stream".into(),
                _ => unreachable!(),
            }
            plan.manifest = canonical_json(&manifest).unwrap();
            resign_test_manifest(&mut plan);
            let outputs = tempfile::tempdir().unwrap();
            let (expanded, bundle) = write_test_representations(&plan, outputs.path());
            for package in [&expanded, &bundle] {
                let error =
                    verify_portable_v2(package, PortableV2Mode::Full, limits, None).unwrap_err();
                assert!(
                    matches!(
                        error.code,
                        PortableV2ErrorCode::Incompatible
                            | PortableV2ErrorCode::InvalidStructure
                            | PortableV2ErrorCode::DigestMismatch
                    ),
                    "{mutation}: {error:?}"
                );
            }
        }
    }

    #[test]
    fn versioned_m9_interchange_ledger_covers_required_matrix() {
        let ledger: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/portable-v2/m9-interchange-cases.json"
        ))
        .unwrap();
        assert_eq!(
            ledger["contract"],
            "graphforge-portable-v2-m9-interchange-cases/1"
        );
        assert_eq!(
            ledger["representations"],
            serde_json::json!(["expanded", "bundle"])
        );
        assert_eq!(
            ledger["positive_package_classes"],
            serde_json::json!([
                "complete",
                "ontology-only",
                "component-selective",
                "graph-data-subset"
            ])
        );
        let cases = ledger["negative_cases"]
            .as_array()
            .unwrap()
            .iter()
            .map(|case| case["name"].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        for required in [
            "future-manifest-capability-before-payload",
            "future-composition-feature-before-semantic-payload",
            "module-semantic-digest-tamper",
            "bridge-semantic-digest-tamper",
            "semantic-descriptor-kind-path-media-mismatch",
            "semantic-payload-budget",
            "cancelled-private-materialization",
            "durable-staging-replay",
            "durable-staging-transaction-conflict",
            "durable-staging-publication-failure",
        ] {
            assert!(cases.contains(required), "missing {required}");
        }
    }

    #[test]
    fn complete_ontology_data_and_custom_profiles_keep_composition_closure() {
        let (_project, generation) = graph_generation_with_composition(true);
        let limits = PortableV2ExportLimits::default();
        let requests = [
            PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::Complete,
                strict: false,
            },
            PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::OntologyOnly,
                strict: false,
            },
            PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::DataComponents,
                strict: false,
            },
            PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::Custom(vec![crate::PortableV2ParticipantId {
                    capability_id: crate::GRAPH_CAPABILITY_ID.into(),
                    record_family_id: crate::GRAPH_FILES_FAMILY.into(),
                }]),
                strict: false,
            },
        ];
        for (index, request) in requests.iter().enumerate() {
            let selection = preview_portable_v2_selection(&generation, request, limits).unwrap();
            assert!(selection.includes(
                crate::WORKSPACE_CAPABILITY_ID,
                crate::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY
            ));
            assert!(!selection.projected.is_empty());
            let mut reports = Vec::new();
            for representation in [PortableV2Output::Expanded, PortableV2Output::Bundle] {
                let plan = plan_selected_portable_v2(&generation, &selection, limits).unwrap();
                let output = tempfile::tempdir().unwrap();
                let extension = if representation == PortableV2Output::Expanded {
                    "gfproject"
                } else {
                    "gfpb"
                };
                let destination = output.path().join(format!("class-{index}.{extension}"));
                export_complete_portable_v2(
                    &plan,
                    &destination,
                    representation,
                    limits,
                    &AtomicBool::new(false),
                    |_| {},
                )
                .unwrap();
                let report =
                    verify_portable_v2(&destination, PortableV2Mode::Full, limits, None).unwrap();
                assert!(report.ontology_composition.is_some());
                reports.push(report);
            }
            assert_eq!(reports[0].package_digest, reports[1].package_digest);
            assert_eq!(
                reports[0].ontology_composition,
                reports[1].ontology_composition
            );
            assert_eq!(
                reports[0].ontology_composition_entries,
                reports[1].ontology_composition_entries
            );
        }

        let complete = preview_portable_v2_selection(&generation, &requests[0], limits).unwrap();
        let exact = complete.projected[0].identity.clone();
        let projected = preview_portable_v2_selection(
            &generation,
            &PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::OntologyComposition(vec![exact.clone()]),
                strict: false,
            },
            limits,
        )
        .unwrap();
        assert!(projected.projected.iter().any(|entry| {
            entry.identity == exact && entry.reason == crate::PortableV2SelectionReason::Requested
        }));
        assert!(projected.estimated_payload_bytes > 0);
    }

    #[test]
    fn exact_composition_selection_closes_bridges_without_widening() {
        let (_project, generation) = graph_generation_with_bridge();
        let limits = PortableV2ExportLimits::default();
        let all = preview_portable_v2_selection(
            &generation,
            &PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::Complete,
                strict: false,
            },
            limits,
        )
        .unwrap();
        let source = all
            .projected
            .iter()
            .find(|entry| entry.identity.id.ends_with("/source"))
            .unwrap()
            .identity
            .clone();
        let bridge = all
            .projected
            .iter()
            .find(|entry| entry.identity.id.contains("/bridge/"))
            .unwrap()
            .identity
            .clone();

        let module_only = preview_portable_v2_selection(
            &generation,
            &PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::OntologyComposition(vec![source.clone()]),
                strict: true,
            },
            limits,
        )
        .unwrap();
        assert_eq!(module_only.projected.len(), 1);
        assert_eq!(module_only.projected[0].identity, source);
        let module_plan = plan_selected_portable_v2(&generation, &module_only, limits).unwrap();
        let module_manifest: serde_json::Value =
            serde_json::from_slice(&module_plan.manifest).unwrap();
        let control = module_plan
            .files
            .iter()
            .find(|file| file.path == crate::project_portable_v2::ONTOLOGY_COMPOSITION_PATH)
            .unwrap();
        let PlannedSource::Control(bytes) = &control.source else {
            panic!("composition must be inline")
        };
        let control: PortableV2OntologyComposition = serde_json::from_slice(bytes).unwrap();
        assert_eq!(control.modules.len(), 1);
        assert!(control.bridge_sets.is_empty());
        assert_eq!(module_manifest["package_class"], "component-selective");

        let bridge_closed = preview_portable_v2_selection(
            &generation,
            &PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::OntologyComposition(vec![bridge.clone()]),
                strict: true,
            },
            limits,
        )
        .unwrap();
        assert_eq!(bridge_closed.projected.len(), 3);
        assert_eq!(
            bridge_closed
                .projected
                .iter()
                .filter(|entry| entry.reason == crate::PortableV2SelectionReason::Requested)
                .count(),
            1
        );
        assert!(
            bridge_closed
                .projected
                .iter()
                .any(|entry| entry.identity == bridge)
        );
        assert!(
            bridge_closed
                .projected
                .iter()
                .filter(|entry| entry.kind == "ontology")
                .all(|entry| entry.reason
                    == crate::PortableV2SelectionReason::RequiredOntologyComposition)
        );

        let missing = PortableV2ExactIdentity {
            id: "https://graphforge.dev/ontology/absent".into(),
            version: "v1".into(),
            content_digest: format!("sha256:{}", "f".repeat(64)),
        };
        let error = preview_portable_v2_selection(
            &generation,
            &PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::OntologyComposition(vec![missing]),
                strict: true,
            },
            limits,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Incompatible);
    }

    #[test]
    fn transitive_module_and_bridge_closure_is_exact_in_both_forms() {
        let (_project, generation, root_bridge, unrelated_digest) =
            graph_generation_with_transitive_bridge_chain();
        let limits = PortableV2ExportLimits::default();
        let selection = preview_portable_v2_selection(
            &generation,
            &PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::OntologyComposition(vec![root_bridge.clone()]),
                strict: true,
            },
            limits,
        )
        .unwrap();
        assert_eq!(selection.projected.len(), 5);
        assert_eq!(
            selection
                .projected
                .iter()
                .filter(|entry| entry.reason == crate::PortableV2SelectionReason::Requested)
                .count(),
            1
        );
        assert!(
            selection
                .projected
                .iter()
                .any(|entry| entry.identity == root_bridge)
        );
        assert!(
            !selection
                .projected
                .iter()
                .any(|entry| entry.identity.content_digest.ends_with(&unrelated_digest))
        );

        let plan = plan_selected_portable_v2(&generation, &selection, limits).unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(&plan.manifest).unwrap();
        let components = manifest["components"].as_array().unwrap();
        assert_eq!(
            components
                .iter()
                .filter(|component| matches!(
                    component["kind"].as_str(),
                    Some("ontology" | "schema")
                ))
                .count(),
            5
        );
        assert!(
            !serde_json::to_string(&manifest)
                .unwrap()
                .contains(&unrelated_digest)
        );
        let control_file = plan
            .files
            .iter()
            .find(|file| file.path == crate::project_portable_v2::ONTOLOGY_COMPOSITION_PATH)
            .unwrap();
        let PlannedSource::Control(control_bytes) = &control_file.source else {
            panic!("composition must be inline")
        };
        let control: PortableV2OntologyComposition = serde_json::from_slice(control_bytes).unwrap();
        assert_eq!(control.modules.len(), 3);
        assert_eq!(control.bridge_sets.len(), 2);
        assert!(
            !control
                .modules
                .iter()
                .any(|module| module.content_digest.ends_with(&unrelated_digest))
        );

        let output = tempfile::tempdir().unwrap();
        let mut reports = Vec::new();
        for representation in [PortableV2Output::Expanded, PortableV2Output::Bundle] {
            let path = output
                .path()
                .join(if representation == PortableV2Output::Expanded {
                    "chain.gfproject"
                } else {
                    "chain.gfpb"
                });
            export_complete_portable_v2(
                &plan,
                &path,
                representation,
                limits,
                &AtomicBool::new(false),
                |_| {},
            )
            .unwrap();
            reports.push(verify_portable_v2(&path, PortableV2Mode::Full, limits, None).unwrap());
        }
        assert_eq!(reports[0].package_digest, reports[1].package_digest);
        assert_eq!(
            reports[0].ontology_composition,
            reports[1].ontology_composition
        );
        assert_eq!(reports[0].ontology_composition_entries.len(), 5);
        assert_eq!(
            reports[0].ontology_composition_entries,
            reports[1].ontology_composition_entries
        );
    }

    #[test]
    fn graph_tree_round_trips_equivalently_from_both_representations() {
        let (_project, generation) = graph_generation();
        let limits = PortableV2ExportLimits::default();
        let plan = plan_complete_portable_v2(&generation, limits).unwrap();
        let output = tempfile::tempdir().unwrap();
        let expanded = output.path().join("graph.gfproject");
        let bundle = output.path().join("graph.gfpb");
        let cancelled = AtomicBool::new(false);
        export_complete_portable_v2(
            &plan,
            &expanded,
            PortableV2Output::Expanded,
            limits,
            &cancelled,
            |_| {},
        )
        .unwrap();
        export_complete_portable_v2(
            &plan,
            &bundle,
            PortableV2Output::Bundle,
            limits,
            &cancelled,
            |_| {},
        )
        .unwrap();
        let supported = generation
            .capabilities()
            .into_iter()
            .map(|capability| crate::ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect::<Vec<_>>();
        let expanded_target = output.path().join("expanded");
        let bundle_target = output.path().join("bundle");
        for (source, target) in [(&expanded, &expanded_target), (&bundle, &bundle_target)] {
            crate::import_complete_portable_v2(
                source,
                target,
                Uuid::new_v4(),
                Uuid::new_v4(),
                &supported,
                limits,
                None,
            )
            .unwrap();
        }
        let expanded = crate::resolve_project_generation(&expanded_target).unwrap();
        let bundle = crate::resolve_project_generation(&bundle_target).unwrap();
        assert_eq!(
            expanded.graph_files_inventory().unwrap(),
            bundle.graph_files_inventory().unwrap()
        );
        assert_eq!(
            tree_bytes(&expanded.graph_tree_root()),
            tree_bytes(&bundle.graph_tree_root())
        );
    }

    #[test]
    fn selection_preview_is_stable_exact_and_consumed_by_export_plan() {
        let project = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(project.path()).unwrap();
        let limits = PortableV2ExportLimits::default();
        let request = PortableV2SelectionRequest {
            profile: PortableV2SelectionProfile::Settings,
            strict: true,
        };
        let first = preview_portable_v2_selection(&generation, &request, limits).unwrap();
        let second = preview_portable_v2_selection(&generation, &request, limits).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.package_class, "component-selective");
        assert!(first.selection_fingerprint.starts_with("sha256:"));
        assert!(first.included.iter().all(|entry| entry.kind == "settings"));

        let plan = plan_selected_portable_v2(&generation, &first, limits).unwrap();
        assert_eq!(plan.selection_fingerprint, first.selection_fingerprint);
        assert_eq!(
            plan.package_class,
            PortableV2PackageClass::ComponentSelective
        );
        let out = tempfile::tempdir().unwrap();
        let receipt = export_complete_portable_v2(
            &plan,
            out.path().join("settings.gfpb"),
            PortableV2Output::Bundle,
            limits,
            &AtomicBool::new(false),
            |_| {},
        )
        .unwrap();
        assert_eq!(receipt.selection_fingerprint, first.selection_fingerprint);
        let mut tampered = first.clone();
        tampered.redactions.push("invented-after-preview".into());
        let error = plan_selected_portable_v2(&generation, &tampered, limits).unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Incompatible);

        let exact = PortableV2SelectionRequest {
            profile: PortableV2SelectionProfile::Custom(
                first
                    .included
                    .iter()
                    .map(|entry| entry.identity.clone())
                    .collect(),
            ),
            strict: true,
        };
        let exact = preview_portable_v2_selection(&generation, &exact, limits).unwrap();
        assert_eq!(
            first
                .included
                .iter()
                .map(|entry| &entry.identity)
                .collect::<Vec<_>>(),
            exact
                .included
                .iter()
                .map(|entry| &entry.identity)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn selection_rejects_ambiguous_identity_and_resource_overflow() {
        let project = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(project.path()).unwrap();
        let limits = PortableV2ExportLimits::default();
        let complete = preview_portable_v2_selection(
            &generation,
            &PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::Complete,
                strict: false,
            },
            limits,
        )
        .unwrap();
        let identity = complete.included[0].identity.clone();
        let error = preview_portable_v2_selection(
            &generation,
            &PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::Custom(vec![identity.clone(), identity]),
                strict: false,
            },
            limits,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Incompatible);
        let error = preview_portable_v2_selection(
            &generation,
            &PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::Complete,
                strict: false,
            },
            PortableV2ExportLimits {
                max_components: 1,
                ..limits
            },
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::LimitExceeded);
    }

    #[test]
    fn selection_rejects_secret_and_host_path_settings_without_leaking_values() {
        for unsafe_settings in [
            serde_json::json!({"api_token": "do-not-export"}),
            serde_json::json!({"cache": {"directory": "/Users/example/private"}}),
        ] {
            let project = tempfile::tempdir().unwrap();
            let generation = open_or_initialize_project(project.path()).unwrap();
            let path = generation
                .participant_path(
                    crate::WORKSPACE_CAPABILITY_ID,
                    crate::WORKSPACE_CONFIGURATION_FAMILY,
                )
                .unwrap();
            fs::write(path, serde_json::to_vec(&unsafe_settings).unwrap()).unwrap();
            let error = preview_portable_v2_selection(
                &generation,
                &PortableV2SelectionRequest {
                    profile: PortableV2SelectionProfile::Settings,
                    strict: true,
                },
                PortableV2ExportLimits::default(),
            )
            .unwrap_err();
            assert_eq!(error.code, PortableV2ErrorCode::Incompatible);
            assert!(!error.to_string().contains("do-not-export"));
            assert!(!error.to_string().contains("/Users/example"));
        }
    }

    #[test]
    fn built_in_profiles_select_only_their_canonical_component_classes() {
        let project = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(project.path()).unwrap();
        let limits = PortableV2ExportLimits::default();
        for (profile, allowed) in [
            (
                PortableV2SelectionProfile::OntologyOnly,
                &["ontology", "schema"][..],
            ),
            (
                PortableV2SelectionProfile::DataComponents,
                &["graph-data", "schema"][..],
            ),
            (
                PortableV2SelectionProfile::Artifacts,
                &["derived-artifact", "schema"][..],
            ),
            (PortableV2SelectionProfile::Settings, &["settings"][..]),
        ] {
            let plan = preview_portable_v2_selection(
                &generation,
                &PortableV2SelectionRequest {
                    profile,
                    strict: false,
                },
                limits,
            )
            .unwrap();
            assert!(
                plan.included
                    .iter()
                    .all(|entry| allowed.contains(&entry.kind.as_str()))
            );
            plan_selected_portable_v2(&generation, &plan, limits).unwrap();
        }
    }

    #[test]
    fn expanded_and_bundle_share_semantic_identity_and_are_deterministic() {
        let project = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(project.path()).unwrap();
        let limits = PortableV2ExportLimits {
            copy_buffer_bytes: 7,
            ..Default::default()
        };
        let plan = plan_complete_portable_v2(&generation, limits).unwrap();
        let out = tempfile::tempdir().unwrap();
        let expanded = out.path().join("complete.gfproject");
        let first = out.path().join("first.gfpb");
        let second = out.path().join("second.gfpb");
        let cancelled = AtomicBool::new(false);
        let mut expanded_progress = Vec::new();
        let a = export_complete_portable_v2(
            &plan,
            &expanded,
            PortableV2Output::Expanded,
            limits,
            &cancelled,
            |progress| expanded_progress.push(progress),
        )
        .unwrap();
        let final_progress = expanded_progress.last().unwrap();
        assert_eq!(
            final_progress.entries_completed,
            final_progress.entries_total
        );
        assert_eq!(final_progress.bytes_completed, final_progress.bytes_total);
        let b = export_complete_portable_v2(
            &plan,
            &first,
            PortableV2Output::Bundle,
            limits,
            &cancelled,
            |_| {},
        )
        .unwrap();
        let c = export_complete_portable_v2(
            &plan,
            &second,
            PortableV2Output::Bundle,
            limits,
            &cancelled,
            |_| {},
        )
        .unwrap();
        assert_eq!(a.package_digest, b.package_digest);
        assert_eq!(b, c);
        assert_eq!(fs::read(&first).unwrap(), fs::read(second).unwrap());
        assert_eq!(&fs::read(expanded.join("bagit.txt")).unwrap(), BAGIT);
        assert!(!expanded.join("CURRENT").exists());
        assert!(!expanded.join("lease.lock").exists());
        let expanded_stage = out.path().join("expanded-stage");
        let bundle_stage = out.path().join("bundle-stage");
        let expanded_report = crate::materialize_verified_portable_v2(
            &expanded,
            &expanded_stage,
            limits,
            Some(&cancelled),
        )
        .unwrap();
        let bundle_report = crate::materialize_verified_portable_v2(
            &first,
            &bundle_stage,
            limits,
            Some(&cancelled),
        )
        .unwrap();
        assert_eq!(expanded_report.package_digest, bundle_report.package_digest);
        assert_eq!(tree_bytes(&expanded_stage), tree_bytes(&bundle_stage));
        let runtime =
            fs::read(expanded_stage.join(
                "data/components/compatibility/graphforge-runtime-map/runtime-generation.json",
            ))
            .unwrap();
        let runtime: serde_json::Value = serde_json::from_slice(&runtime).unwrap();
        assert_eq!(runtime["contract"], "graphforge-runtime-generation-map/1");
        assert!(runtime.get("host_path").is_none());
        assert!(runtime.get("secret").is_none());

        let supported = generation
            .capabilities()
            .into_iter()
            .map(|capability| crate::ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect::<Vec<_>>();
        let expanded_target = out.path().join("expanded-project");
        let bundle_target = out.path().join("bundle-project");
        let mut progress = Vec::new();
        let expanded_transaction = Uuid::new_v4();
        let expanded_generation = Uuid::new_v4();
        let expanded_import = crate::import_complete_portable_v2_with_progress(
            &expanded,
            &expanded_target,
            expanded_transaction,
            expanded_generation,
            &supported,
            limits,
            Some(&cancelled),
            |event| progress.push(event),
        )
        .unwrap();
        assert_eq!(
            progress.iter().map(|event| event.phase).collect::<Vec<_>>(),
            vec![
                crate::PortableV2ImportPhase::Verifying,
                crate::PortableV2ImportPhase::Materialized,
                crate::PortableV2ImportPhase::Published,
            ]
        );
        let expected_package_digest = format!("sha256:{}", hex(a.package_digest));
        assert!(progress.iter().skip(1).all(|event| {
            event.entries > 0
                && event.bytes > 0
                && event.package_digest.as_deref() == Some(expected_package_digest.as_str())
        }));
        let bundle_import = crate::import_complete_portable_v2(
            &first,
            &bundle_target,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported,
            limits,
            Some(&cancelled),
        )
        .unwrap();
        assert_eq!(expanded_import.package_digest, bundle_import.package_digest);
        let expanded_reopened = crate::resolve_project_generation(&expanded_target).unwrap();
        let bundle_reopened = crate::resolve_project_generation(&bundle_target).unwrap();
        assert_eq!(
            expanded_reopened.capabilities(),
            bundle_reopened.capabilities()
        );
        assert_eq!(
            expanded_reopened.participant_snapshots().unwrap(),
            bundle_reopened.participant_snapshots().unwrap()
        );
        let protected_generation = expanded_reopened.generation_uuid();
        let replay = crate::import_complete_portable_v2(
            &expanded,
            &expanded_target,
            expanded_transaction,
            expanded_generation,
            &supported,
            limits,
            Some(&cancelled),
        )
        .unwrap();
        assert!(replay.publication.idempotent_replay);
        assert_eq!(replay.publication.generation_uuid, protected_generation);
        let overwrite = crate::import_complete_portable_v2(
            &expanded,
            &expanded_target,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported,
            limits,
            Some(&cancelled),
        )
        .unwrap_err();
        assert_eq!(overwrite.code, PortableV2ErrorCode::Io);
        assert_eq!(
            crate::resolve_project_generation(&expanded_target)
                .unwrap()
                .generation_uuid(),
            protected_generation
        );
        let cancelled = AtomicBool::new(true);
        let cancelled_target = out.path().join("cancelled-project");
        let cancelled_error = crate::import_complete_portable_v2(
            &expanded,
            &cancelled_target,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported,
            limits,
            Some(&cancelled),
        )
        .unwrap_err();
        assert_eq!(cancelled_error.code, PortableV2ErrorCode::Cancelled);
        assert!(!cancelled_target.exists());
        fs::write(
            expanded.join(
                "data/components/compatibility/graphforge-runtime-map/runtime-generation.json",
            ),
            b"{}",
        )
        .unwrap();
        let corrupt_target = out.path().join("corrupt-project");
        let corrupt = crate::import_complete_portable_v2(
            &expanded,
            &corrupt_target,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported,
            limits,
            None,
        )
        .unwrap_err();
        assert_eq!(corrupt.code, PortableV2ErrorCode::DigestMismatch);
        assert!(!corrupt_target.exists());
    }

    #[test]
    fn cancellation_and_limits_never_publish_a_destination() {
        let project = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(project.path()).unwrap();
        let limits = PortableV2ExportLimits::default();
        let plan = plan_complete_portable_v2(&generation, limits).unwrap();
        let out = tempfile::tempdir().unwrap();
        let destination = out.path().join("cancelled.gfpb");
        let cancelled = AtomicBool::new(true);
        let error = export_complete_portable_v2(
            &plan,
            &destination,
            PortableV2Output::Bundle,
            limits,
            &cancelled,
            |_| {},
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Cancelled);
        assert!(!destination.exists());
        assert!(fs::read_dir(out.path()).unwrap().next().is_none());
        let limited = PortableV2ExportLimits {
            max_entries: 1,
            ..limits
        };
        let error = plan_complete_portable_v2(&generation, limited).unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::LimitExceeded);
    }

    #[test]
    fn compact_plan_is_reusable_across_representations_cancelled_retry_and_cas_tamper() {
        let (project, generation) = compact_graph_generation();
        let limits = PortableV2ExportLimits::default();
        let plan = plan_complete_portable_v2(&generation, limits).unwrap();
        assert!(
            plan.files
                .iter()
                .any(|file| matches!(file.source, PlannedSource::Cas { .. }))
        );
        let out = tempfile::tempdir().unwrap();
        let expanded = out.path().join("compact.gfproject");
        let bundle = out.path().join("compact.gfpb");
        let cancelled_bundle = out.path().join("cancelled.gfpb");
        let active = AtomicBool::new(false);
        let expanded_receipt = export_complete_portable_v2(
            &plan,
            &expanded,
            PortableV2Output::Expanded,
            limits,
            &active,
            |_| {},
        )
        .unwrap();
        let cancelled = AtomicBool::new(true);
        assert_eq!(
            export_complete_portable_v2(
                &plan,
                &cancelled_bundle,
                PortableV2Output::Bundle,
                limits,
                &cancelled,
                |_| {},
            )
            .unwrap_err()
            .code,
            PortableV2ErrorCode::Cancelled
        );
        assert!(!cancelled_bundle.exists());
        let bundle_receipt = export_complete_portable_v2(
            &plan,
            &bundle,
            PortableV2Output::Bundle,
            limits,
            &active,
            |_| {},
        )
        .unwrap();
        assert_eq!(
            expanded_receipt.package_digest,
            bundle_receipt.package_digest
        );
        verify_portable_v2(&expanded, PortableV2Mode::Full, limits, Some(&active)).unwrap();
        verify_portable_v2(&bundle, PortableV2Mode::Full, limits, Some(&active)).unwrap();

        let payload_digest = hex(Sha256::digest(b"compact graph payload").into());
        let (digest, length) = plan
            .files
            .iter()
            .find_map(|file| match &file.source {
                PlannedSource::Cas { digest, length, .. } if digest == &payload_digest => {
                    Some((digest.clone(), *length))
                }
                PlannedSource::File { .. } | PlannedSource::Control(_) => None,
                PlannedSource::Cas { .. } => None,
            })
            .unwrap();
        let object = crate::graph_object_store::graph_object_path(project.path(), &digest).unwrap();
        let mut permissions = fs::metadata(&object).unwrap().permissions();
        permissions.set_readonly(false);
        fs::set_permissions(&object, permissions).unwrap();
        fs::write(&object, vec![b'x'; usize::try_from(length).unwrap()]).unwrap();
        let tampered = out.path().join("tampered.gfpb");
        let error = export_complete_portable_v2(
            &plan,
            &tampered,
            PortableV2Output::Bundle,
            limits,
            &active,
            |_| {},
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::ConcurrentMutation);
        assert!(!tampered.exists());
    }

    #[test]
    fn destination_replacement_and_source_mutation_fail_closed() {
        let project = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(project.path()).unwrap();
        let limits = PortableV2ExportLimits {
            copy_buffer_bytes: 3,
            ..Default::default()
        };
        let plan = plan_complete_portable_v2(&generation, limits).unwrap();
        let out = tempfile::tempdir().unwrap();
        let destination = out.path().join("raced.gfpb");
        let mut replaced = false;
        let cancelled = AtomicBool::new(false);
        let error = export_complete_portable_v2(
            &plan,
            &destination,
            PortableV2Output::Bundle,
            limits,
            &cancelled,
            |_| {
                if !replaced {
                    fs::write(&destination, b"attacker").unwrap();
                    replaced = true;
                }
            },
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Io);
        assert_eq!(fs::read(&destination).unwrap(), b"attacker");
        assert_eq!(fs::read_dir(out.path()).unwrap().count(), 1);

        let source = plan
            .files
            .iter()
            .find_map(|file| match &file.source {
                PlannedSource::File { path, .. } => Some(path),
                PlannedSource::Control(_) | PlannedSource::Cas { .. } => None,
            })
            .unwrap();
        let original = fs::read(source).unwrap();
        fs::write(source, vec![b'x'; original.len()]).unwrap();
        let mutated = out.path().join("mutated.gfpb");
        let error = export_complete_portable_v2(
            &plan,
            &mutated,
            PortableV2Output::Bundle,
            limits,
            &cancelled,
            |_| {},
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::ConcurrentMutation);
        assert!(
            !error.allocation_identity_allocated_bytes.is_empty(),
            "partial bundle allocation must survive typed failure"
        );
        assert!(!mutated.exists());
    }

    #[test]
    fn pax_and_structural_budgets_are_canonical_without_payload_allocation() {
        assert_eq!(portable_id("graph-files"), "graph-files");
        assert_ne!(portable_id("graph-files"), "graph-tree");
        assert_ne!(
            portable_participant_id("a-b", "c"),
            portable_participant_id("a", "b-c")
        );
        let long = format!(
            "data/components/graph-data/graph-files/{}/nodes.parquet",
            "segment".repeat(40)
        );
        let record = pax_path_record(&long);
        let declared: usize = record.split_once(' ').unwrap().0.parse().unwrap();
        assert_eq!(declared, record.len());
        assert_eq!(
            PortableV2ExportLimits::default().max_entry_bytes,
            16 * 1024 * 1024 * 1024 * 1024
        );
        assert!(PortableV2ExportLimits::default().copy_buffer_bytes <= 8 * 1024 * 1024);
        let out = tempfile::NamedTempFile::new().unwrap();
        let mut file = out.reopen().unwrap();
        let mut digest = Sha256::new();
        // The >16 GiB structural case is represented by multiple bounded
        // shards; no individual ustar size field requires a base-256 escape.
        header(&mut file, &mut digest, &long, 1_073_741_824).unwrap();
        let bytes = fs::read(out.path()).unwrap();
        assert_eq!(&bytes[..11], b"PaxHeaders/");
        assert_eq!(bytes[156], b'x');
        assert_eq!(bytes[1024..1033].as_ref(), b"PaxFiles/");
        assert_eq!(bytes[1024 + 156], b'0');
        assert!(oct(&mut [0; 12], USTAR_MAX_ENTRY_BYTES).is_ok());
        assert!(oct(&mut [0; 12], USTAR_MAX_ENTRY_BYTES + 1).is_err());
    }

    #[test]
    fn plan_debug_is_content_and_host_path_free() {
        let project = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(project.path()).unwrap();
        let plan =
            plan_complete_portable_v2(&generation, PortableV2ExportLimits::default()).unwrap();
        let debug = format!("{plan:?}");
        assert!(!debug.contains(project.path().to_string_lossy().as_ref()));
        assert!(!debug.contains("manifest"));
        assert!(debug.contains("entry_count"));
    }

    #[test]
    fn large_sparse_source_streams_densely_with_a_tiny_buffer() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("large.parquet");
        let file = File::create(&source).unwrap();
        file.set_len(32 * 1024 * 1024).unwrap();
        drop(file);
        let limits = PortableV2ExportLimits {
            copy_buffer_bytes: 11,
            ..Default::default()
        };
        let mut total = 0;
        let planned = inspect(
            &source,
            "data/components/graph-data/graph-files/large.parquet",
            limits,
            &mut total,
        )
        .unwrap();
        assert_eq!(total, 32 * 1024 * 1024);
        let destination = root.path().join("dense.parquet");
        let mut observed = 0;
        let mut allocation = ExportAllocationObserver::default();
        copy(
            &planned,
            &destination,
            limits.copy_buffer_bytes,
            &|| false,
            &mut allocation,
            |bytes| {
                observed += bytes;
            },
        )
        .unwrap();
        assert_eq!(observed, total);
        assert_eq!(fs::metadata(destination).unwrap().len(), total);
    }

    #[cfg(unix)]
    #[test]
    fn source_symlink_is_rejected_without_following_it() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let real = root.path().join("real");
        fs::write(&real, b"secret").unwrap();
        let linked = root.path().join("linked");
        symlink(&real, &linked).unwrap();
        let mut total = 0;
        let error = inspect(
            &linked,
            "data/components/settings/settings/secret.bin",
            PortableV2ExportLimits::default(),
            &mut total,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::InvalidStructure);
        assert_eq!(total, 0);
    }

    fn tree_bytes(root: &Path) -> Vec<(String, Vec<u8>)> {
        fn walk(root: &Path, directory: &Path, output: &mut Vec<(String, Vec<u8>)>) {
            for entry in fs::read_dir(directory).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    walk(root, &entry.path(), output);
                } else {
                    output.push((
                        entry
                            .path()
                            .strip_prefix(root)
                            .unwrap()
                            .to_string_lossy()
                            .replace(std::path::MAIN_SEPARATOR, "/"),
                        fs::read(entry.path()).unwrap(),
                    ));
                }
            }
        }
        let mut output = Vec::new();
        walk(root, root, &mut output);
        output.sort_by(|left, right| left.0.cmp(&right.0));
        output
    }
}
