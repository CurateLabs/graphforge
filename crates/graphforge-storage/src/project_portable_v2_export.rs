//! Bounded deterministic portable-project v2 complete-package export.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use unicode_normalization::UnicodeNormalization;
use uuid::Uuid;

use crate::{
    PortableV2Error, PortableV2ErrorCode, PortableV2Limits, PortableV2Mode, PortableV2PackageClass,
    ResolvedProjectGeneration, verify_portable_v2,
};

type GfError = PortableV2Error;

const BAGIT: &[u8] = b"BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n";
const BAG_INFO: &[u8] = b"Bag-Software-Agent: GraphForge portable-v2\nBagging-Date: 1970-01-01\n";

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
    File { path: PathBuf, identity: Identity },
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
#[derive(Debug, Clone)]
/// Immutable pinned-generation metadata plan; contains no payload buffers.
pub struct PortableV2ExportPlan {
    generation_uuid: Uuid,
    files: Vec<PlannedFile>,
    manifest: Vec<u8>,
    package_digest: [u8; 32],
    payload_bytes: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
/// Durable publication receipt with separate semantic and transport identities.
pub struct PortableV2ExportReceipt {
    /// Pinned source generation.
    pub generation_uuid: Uuid,
    /// Semantic package identity shared by both representations.
    pub package_digest: [u8; 32],
    /// Representation-specific transport identity.
    pub transport_digest: [u8; 32],
    /// Semantic manifest plus payload entry count.
    pub entry_count: usize,
    /// Source payload bytes, excluding tags and manifest.
    pub payload_bytes: u64,
    /// Published representation.
    pub output: PortableV2Output,
}

#[derive(Serialize)]
struct Manifest<'a> {
    format: &'static str,
    package_digest: String,
    package_class: &'static str,
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
}
#[derive(Serialize)]
struct Component {
    kind: String,
    participant_id: String,
    required_dependencies: Vec<String>,
    files: Vec<ComponentFile>,
}
#[derive(Serialize)]
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

/// Plan every canonical participant of one already-pinned generation.
#[expect(
    clippy::too_many_lines,
    reason = "keeps one auditable sequence from pinned inventory through semantic identity"
)]
pub fn plan_complete_portable_v2(
    g: &ResolvedProjectGeneration,
    limits: PortableV2ExportLimits,
) -> Result<PortableV2ExportPlan, GfError> {
    validate_limits(limits)?;
    g.validate_complete_participant_inventory()?;
    let mut files = Vec::new();
    let mut components = Vec::new();
    let mut roots = Vec::new();
    let mut runtime_participants = Vec::new();
    let mut graph_inventory_participant = None;
    let mut total = 0;
    for d in g.participant_descriptors()? {
        let id = portable_id(&format!("{}-{}", d.capability_id, d.record_family_id));
        let kind = kind(&d.capability_id, &d.record_family_id);
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
    if let Some(inv) = g.graph_files_inventory()? {
        let id = "graph-tree".to_owned();
        let mut owned = Vec::new();
        for e in inv.files {
            let source = g.graph_tree_root().join(&e.relative_path);
            let path = format!("data/components/graph-data/{id}/{}", e.relative_path);
            let f = inspect(&source, &path, limits, &mut total)?;
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
    let runtime_bytes = canonical(&serde_json::to_value(runtime_map).map_err(storage)?)?;
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
        components
            .iter()
            .map(|component| format!("{}@1", component.kind))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    };
    let source = || Source {
        generation_uuid: g.generation_uuid().hyphenated().to_string(),
        manifest_sha256: hex(g.manifest_sha256()),
    };
    let draft = Manifest {
        format: "graphforge-project/2",
        package_digest: String::new(),
        package_class: "complete",
        source_generation: source(),
        selection: Selection {
            roots: &roots,
            omissions: vec![],
            redactions: vec![],
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
    let semantic = canonical(&value)?;
    let mut h = Sha256::new();
    h.update(b"graphforge-project/2\0");
    h.update(semantic);
    let package_digest = h.finalize().into();
    let final_manifest = Manifest {
        format: "graphforge-project/2",
        package_digest: format!("sha256:{}", hex(package_digest)),
        package_class: "complete",
        source_generation: source(),
        selection: Selection {
            roots: &roots,
            omissions: vec![],
            redactions: vec![],
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
    let manifest = canonical(&serde_json::to_value(final_manifest).map_err(storage)?)?;
    if manifest.len() as u64 > limits.max_manifest_bytes {
        return Err(limit("semantic manifest exceeds configured limit"));
    }
    Ok(PortableV2ExportPlan {
        generation_uuid: g.generation_uuid(),
        files,
        manifest,
        package_digest,
        payload_bytes: total,
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
) -> Result<PortableV2ExportReceipt, GfError> {
    validate_limits(limits)?;
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
    let result = match output {
        PortableV2Output::Expanded => expanded(plan, &stage, limits, &is_cancelled, &mut progress),
        PortableV2Output::Bundle => bundle(plan, &stage, limits, &is_cancelled, &mut progress),
    };
    let digest = match result {
        Ok(d) => d,
        Err(e) => {
            remove(&stage);
            return Err(e);
        }
    };
    if is_cancelled() {
        remove(&stage);
        return Err(err("GF_CANCELLED", "portable export cancelled"));
    }
    let verified = verify_portable_v2(&stage, PortableV2Mode::Full, limits, Some(cancelled))
        .inspect_err(|_| remove(&stage))?;
    let expected_transport = format!("sha256:{}", hex(digest));
    if verified.package_class != PortableV2PackageClass::Complete
        || verified.package_digest != format!("sha256:{}", hex(plan.package_digest))
    {
        remove(&stage);
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "writer and verifier semantic receipts disagree",
        ));
    }
    if verified.transport_digest.as_deref() != Some(expected_transport.as_str()) {
        remove(&stage);
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "writer and verifier transport receipts disagree",
        ));
    }
    publish_no_replace(&stage, dst).map_err(|error| {
        remove(&stage);
        storage(error)
    })?;
    sync_dir(parent)?;
    Ok(PortableV2ExportReceipt {
        generation_uuid: plan.generation_uuid,
        package_digest: plan.package_digest,
        transport_digest: digest,
        entry_count: plan.files.len() + 1,
        payload_bytes: plan.payload_bytes,
        output,
    })
}

fn expanded(
    plan: &PortableV2ExportPlan,
    stage: &Path,
    l: PortableV2ExportLimits,
    cancelled: &impl Fn() -> bool,
    progress: &mut impl FnMut(PortableV2ExportProgress),
) -> Result<[u8; 32], GfError> {
    fs::create_dir(stage).map_err(storage)?;
    write_bytes(stage, "data/graphforge-project.json", &plan.manifest)?;
    let mut payload = vec![(
        "data/graphforge-project.json".into(),
        plan.manifest.len() as u64,
        Sha256::digest(&plan.manifest).into(),
    )];
    let mut done = 0;
    for (i, f) in plan.files.iter().enumerate() {
        let target = stage.join(&f.path);
        parent(&target)?;
        copy(f, &target, l.copy_buffer_bytes, cancelled, |n| {
            done += n;
            progress(PortableV2ExportProgress {
                entries_completed: i,
                bytes_completed: done,
                entries_total: plan.files.len(),
                bytes_total: plan.payload_bytes,
            });
        })?;
        payload.push((f.path.clone(), f.length, f.digest));
    }
    payload.sort_by(|a, b| a.0.cmp(&b.0));
    let inv = inventory(&payload, l.max_tag_manifest_bytes)?;
    write_bytes(stage, "manifest-sha256.txt", &inv)?;
    write_bytes(stage, "bagit.txt", BAGIT)?;
    write_bytes(stage, "bag-info.txt", BAG_INFO)?;
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
    write_bytes(stage, "tagmanifest-sha256.txt", &tag)?;
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
) -> Result<Vec<(String, Src<'_>)>, GfError> {
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
) -> Result<[u8; 32], GfError> {
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
        match src {
            Src::Bytes(b) => emit(&mut out, &mut h, b)?,
            Src::File(f) => stream(&mut out, &mut h, f, l.copy_buffer_bytes, cancelled, |n| {
                done += n;
                progress(PortableV2ExportProgress {
                    entries_completed: i,
                    bytes_completed: done,
                    entries_total: items.len(),
                    bytes_total: plan.payload_bytes,
                });
            })?,
        }
        pad(&mut out, &mut h, src.len())?;
    }
    let end = [0u8; 1024];
    out.write_all(&end).map_err(storage)?;
    h.update(end);
    out.sync_all().map_err(storage)?;
    Ok(h.finalize().into())
}

fn inspect(
    source: &Path,
    path: &str,
    limits: PortableV2ExportLimits,
    total: &mut u64,
) -> Result<PlannedFile, GfError> {
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
fn inline_control(
    path: &str,
    bytes: Vec<u8>,
    limits: PortableV2ExportLimits,
    total: &mut u64,
) -> Result<PlannedFile, GfError> {
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
    mut tick: impl FnMut(u64),
) -> Result<(), GfError> {
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
        output.sync_all().map_err(storage)?;
        tick(bytes.len() as u64);
        return Ok(());
    }
    let PlannedSource::File {
        path,
        identity: planned_identity,
    } = &planned.source
    else {
        unreachable!("control source returned above")
    };
    let mut input = open_source_no_follow(path)?;
    if identity(&input.metadata().map_err(storage)?)? != *planned_identity {
        return Err(err("GF_SOURCE_CHANGED", "source changed"));
    }
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
    if bytes_read != planned.length
        || <[u8; 32]>::from(digest.finalize()) != planned.digest
        || identity(&input.metadata().map_err(storage)?)? != *planned_identity
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
    mut tick: impl FnMut(u64),
) -> Result<(), GfError> {
    if let PlannedSource::Control(bytes) = &planned.source {
        if cancelled() {
            return Err(err("GF_CANCELLED", "portable export cancelled"));
        }
        out.write_all(bytes).map_err(storage)?;
        transport.update(bytes);
        tick(bytes.len() as u64);
        return Ok(());
    }
    let PlannedSource::File {
        path,
        identity: planned_identity,
    } = &planned.source
    else {
        unreachable!("control source returned above")
    };
    let mut input = open_source_no_follow(path)?;
    if identity(&input.metadata().map_err(storage)?)? != *planned_identity {
        return Err(err("GF_SOURCE_CHANGED", "source changed"));
    }
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
    if bytes_read != planned.length
        || <[u8; 32]>::from(digest.finalize()) != planned.digest
        || identity(&input.metadata().map_err(storage)?)? != *planned_identity
    {
        return Err(err("GF_SOURCE_CHANGED", "source changed during export"));
    }
    Ok(())
}
fn header(out: &mut File, h: &mut Sha256, path: &str, size: u64) -> Result<(), GfError> {
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
) -> Result<(), GfError> {
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
fn split(p: &str) -> Result<(&str, &str), GfError> {
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
fn oct(dst: &mut [u8], n: u64) -> Result<(), GfError> {
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
fn emit(o: &mut File, h: &mut Sha256, b: &[u8]) -> Result<(), GfError> {
    o.write_all(b).map_err(storage)?;
    h.update(b);
    Ok(())
}
fn pad(output: &mut File, digest: &mut Sha256, length: u64) -> Result<(), GfError> {
    let padding = ((512 - length % 512) % 512) as usize;
    let zeroes = [0u8; 512];
    emit(output, digest, &zeroes[..padding])
}
fn canonical(v: &Value) -> Result<Vec<u8>, GfError> {
    fn w(v: &Value, o: &mut Vec<u8>) -> Result<(), GfError> {
        match v {
            Value::Null => o.extend(b"null"),
            Value::Bool(x) => o.extend(if *x { &b"true"[..] } else { &b"false"[..] }),
            Value::Number(x) => o.extend(x.to_string().bytes()),
            Value::String(x) => o.extend(serde_json::to_vec(x).map_err(storage)?),
            Value::Array(a) => {
                o.push(b'[');
                for (i, x) in a.iter().enumerate() {
                    if i > 0 {
                        o.push(b',');
                    }
                    w(x, o)?;
                }
                o.push(b']');
            }
            Value::Object(m) => {
                o.push(b'{');
                let mut k = m.keys().collect::<Vec<_>>();
                k.sort();
                for (i, k) in k.into_iter().enumerate() {
                    if i > 0 {
                        o.push(b',');
                    }
                    o.extend(serde_json::to_vec(k).map_err(storage)?);
                    o.push(b':');
                    w(&m[k], o)?;
                }
                o.push(b'}');
            }
        }
        Ok(())
    }
    let mut o = Vec::new();
    w(v, &mut o)?;
    Ok(o)
}
fn inventory(rows: &[(String, u64, [u8; 32])], limit_bytes: u64) -> Result<Vec<u8>, GfError> {
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
fn identity(m: &fs::Metadata) -> Result<Identity, GfError> {
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
fn open_source_no_follow(path: &Path) -> Result<File, GfError> {
    if fs::symlink_metadata(path)
        .map_err(storage)?
        .file_type()
        .is_symlink()
    {
        return Err(err("GF_UNSUPPORTED_ENTRY_TYPE", "source is a link"));
    }
    let descriptor = rustix::fs::open(
        path,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW,
        rustix::fs::Mode::empty(),
    )
    .map_err(storage)?;
    Ok(descriptor.into())
}
#[cfg(windows)]
fn open_source_no_follow(path: &Path) -> Result<File, GfError> {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    if fs::symlink_metadata(path)
        .map_err(storage)?
        .file_type()
        .is_symlink()
    {
        return Err(err("GF_UNSUPPORTED_ENTRY_TYPE", "source is a link"));
    }
    OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
        .map_err(storage)
}
fn valid_path(p: &str) -> Result<(), GfError> {
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
fn collisions(f: &[PlannedFile]) -> Result<(), GfError> {
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
fn kind(c: &str, f: &str) -> &'static str {
    if c == "graph" {
        "graph-data"
    } else if f.contains("ontology") {
        "ontology"
    } else if f.contains("schema") {
        "schema"
    } else {
        "settings"
    }
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
fn validate_limits(l: PortableV2ExportLimits) -> Result<(), GfError> {
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
fn reject_destination(p: &Path) -> Result<(), GfError> {
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
#[cfg(not(any(target_vendor = "apple", target_os = "linux", target_os = "redox")))]
fn publish_no_replace(stage: &Path, destination: &Path) -> std::io::Result<()> {
    // Windows rename is non-replacing. The destination was also checked before
    // staging; any intervening creation makes this operation fail closed.
    fs::rename(stage, destination)
}
fn parent(p: &Path) -> Result<(), GfError> {
    fs::create_dir_all(p.parent().unwrap()).map_err(storage)
}
fn write_bytes(root: &Path, p: &str, b: &[u8]) -> Result<(), GfError> {
    let p = root.join(p);
    parent(&p)?;
    let mut f = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(p)
        .map_err(storage)?;
    f.write_all(b).map_err(storage)?;
    f.sync_all().map_err(storage)
}
fn sync_tree(root: &Path) -> Result<(), GfError> {
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
fn sync_dir(p: &Path) -> Result<(), GfError> {
    File::open(p).and_then(|f| f.sync_all()).map_err(storage)
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
fn limit(m: &str) -> GfError {
    let _ = m;
    PortableV2Error::new(PortableV2ErrorCode::LimitExceeded, "export limit exceeded")
}
fn err(c: &str, m: &str) -> GfError {
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
fn storage(e: impl std::fmt::Display) -> GfError {
    let _ = e;
    PortableV2Error::new(PortableV2ErrorCode::Io, "portable-v2 I/O failed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::open_or_initialize_project;

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
        let a = export_complete_portable_v2(
            &plan,
            &expanded,
            PortableV2Output::Expanded,
            limits,
            &cancelled,
            |_| {},
        )
        .unwrap();
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
        let runtime: Value = serde_json::from_slice(&runtime).unwrap();
        assert_eq!(runtime["contract"], "graphforge-runtime-generation-map/1");
        assert!(runtime.get("host_path").is_none());
        assert!(runtime.get("secret").is_none());
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
                PlannedSource::Control(_) => None,
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
        assert!(!mutated.exists());
    }

    #[test]
    fn pax_and_structural_budgets_are_canonical_without_payload_allocation() {
        assert_eq!(portable_id("graph-files"), "graph-files");
        assert_ne!(portable_id("graph-files"), "graph-tree");
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
        copy(
            &planned,
            &destination,
            limits.copy_buffer_bytes,
            &|| false,
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
