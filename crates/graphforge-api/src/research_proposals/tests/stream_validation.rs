//! Native research validation runs one Version at a time, the same way from a
//! file and from a stream.
use super::interchange::{
    export_selected, fixture, native_failure_is_refused_alike_from_a_file_and_a_stream,
    reauthenticated,
};
use super::*;
use graphforge_core::portable::{
    PortableError, PortableErrorCode, PortableLimits, PortableMode, PortableReport,
};
use graphforge_storage::research_versions::{ResearchRegistry, ResearchVersionRecord};
use std::{collections::BTreeMap, path::Path};

const STAGE_PREFIX: &str = ".graphforge-research-validation-";

/// A bundled selected export, with the bytes a stream reads.
fn bundle(directory: &Path) -> (std::path::PathBuf, Vec<u8>) {
    let (graph, proposal) = fixture(&directory.join("source"));
    let package = directory.join("selected.gfpb");
    graph
        .export_research(
            &ExportResearchRequest {
                version_uuid: proposal.source_version_uuid,
                output: package.clone(),
                bundled: true,
                projection: Some(projection(&proposal)),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let bytes = std::fs::read(&package).unwrap();
    (package, bytes)
}

fn projection(proposal: &SubmitResearchProposalRequest) -> ResearchExportProjection {
    ResearchExportProjection {
        version_uuid: Uuid::now_v7(),
        frozen_ipc: proposal.frozen_ipc.clone(),
        fields: ["$object", "$labels", "property:score", "property:deferred"]
            .into_iter()
            .map(|field| ResearchFieldIdentity {
                object_kind: "node".into(),
                object_uuid: proposal.fields[0].object_uuid,
                field: field.into(),
            })
            .collect(),
        created_at: 9,
    }
}

fn by_file(path: &Path, limits: PortableLimits) -> Result<PortableReport, PortableError> {
    crate::verify_portable(
        &PortableVerifyRequest {
            input: path.to_path_buf(),
            mode: PortableMode::Full,
            limits,
        },
        None,
    )
}

fn by_stream(
    bytes: &[u8],
    scratch: &Path,
    limits: PortableLimits,
) -> Result<PortableReport, PortableError> {
    crate::verify_portable_stream(
        &mut std::io::Cursor::new(bytes),
        Some(scratch),
        limits,
        None,
    )
}

fn strip_source(report: &PortableReport) -> serde_json::Value {
    let mut value = serde_json::to_value(report).unwrap();
    value.as_object_mut().unwrap().remove("source").unwrap();
    value
}

/// The bytes each Version's retained objects occupy in the package.
fn version_bytes(report: &PortableReport) -> BTreeMap<String, u64> {
    let mut bytes = BTreeMap::new();
    for entry in &report.research_entries {
        if let Some(rest) = entry.path.split("/research-retained/").nth(1) {
            let version = rest.split('/').next().unwrap();
            *bytes.entry(version.to_owned()).or_insert(0) += entry.length;
        }
    }
    bytes
}

fn stage_directories(scratch: &Path) -> usize {
    std::fs::read_dir(scratch)
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(STAGE_PREFIX)
        })
        .count()
}

#[test]
fn a_file_and_a_stream_validate_every_version_once_and_agree() {
    let directory = tempfile::tempdir().unwrap();
    let (package, bytes) = bundle(directory.path());
    let limits = PortableLimits::default();
    let versions = version_bytes(&by_file(&package, limits).unwrap()).len();
    assert!(versions >= 2, "the package carries several Versions");
    let mut owned = Vec::new();
    let mut seen = |generation: &graphforge_storage::ResolvedProjectGeneration,
                    version: &ResearchVersionRecord,
                    registry: &ResearchRegistry| {
        owned.push(version.version_uuid);
        crate::research_interchange::validation::validate(generation, version, registry)
    };
    let scratch = tempfile::tempdir().unwrap();
    let file = graphforge_storage::validate_research_package(
        &package,
        Some(scratch.path()),
        limits,
        None,
        &mut seen,
    )
    .unwrap();
    let from_file = std::mem::take(&mut owned);
    let mut seen = |generation: &graphforge_storage::ResolvedProjectGeneration,
                    version: &ResearchVersionRecord,
                    registry: &ResearchRegistry| {
        owned.push(version.version_uuid);
        crate::research_interchange::validation::validate(generation, version, registry)
    };
    let stream = graphforge_storage::validate_research_package_stream(
        &mut std::io::Cursor::new(&bytes),
        Some(scratch.path()),
        limits,
        None,
        &mut seen,
    )
    .unwrap();
    assert_eq!(from_file.len(), versions);
    assert_eq!(from_file, owned, "the same Versions in the same order");
    assert_eq!(strip_source(&file), strip_source(&stream));
    assert_eq!(stage_directories(scratch.path()), 0);
}

#[test]
fn refusals_are_the_same_from_a_file_and_a_stream() {
    let directory = tempfile::tempdir().unwrap();
    let (package, bytes) = bundle(directory.path());
    let limits = PortableLimits::default();
    // An accepted mapping commits to a value the Version does not hold.
    native_failure_is_refused_alike_from_a_file_and_a_stream(directory.path(), &package, limits);
    // The baseline names an origin the registry has no identity for.
    let unknown_origin = reauthenticated(&bytes, |registry| {
        let origin = registry
            .branches
            .values()
            .next()
            .map(|branch| branch.origin_version_uuid)
            .or_else(|| registry.identities.keys().next().copied())
            .expect("an origin");
        registry.identities.remove(&origin);
    });
    let path = directory.path().join("unknown-origin.gfpb");
    std::fs::write(&path, &unknown_origin).unwrap();
    let file = by_file(&path, limits).unwrap_err();
    let stream = by_stream(&unknown_origin, directory.path(), limits).unwrap_err();
    assert_eq!(file.code, PortableErrorCode::Incompatible);
    assert_eq!((stream.code, &stream.entry), (file.code, &file.entry));
    // One byte of a retained object differs from the digest in its path.
    let mut corrupt = bytes.clone();
    let name = b"research-retained/";
    let at = corrupt
        .windows(name.len())
        .position(|window| window == name)
        .unwrap();
    let header = at - at % 512;
    corrupt[header + 512] ^= 1;
    let path = directory.path().join("corrupt.gfpb");
    std::fs::write(&path, &corrupt).unwrap();
    let file = by_file(&path, limits).unwrap_err();
    let stream = by_stream(&corrupt, directory.path(), limits).unwrap_err();
    assert_eq!(file.code, PortableErrorCode::DigestMismatch);
    assert_eq!((stream.code, &stream.entry), (file.code, &file.entry));
}

/// One Version at a time is on disk, and nothing is left behind. A test thread
/// lists the scratch directory while the stream is verified.
#[test]
fn at_most_one_version_is_staged_and_nothing_remains() {
    let directory = tempfile::tempdir().unwrap();
    let (_, bytes) = bundle(directory.path());
    let scratch = tempfile::tempdir().unwrap();
    let done = std::sync::atomic::AtomicBool::new(false);
    let (most, validated) = std::thread::scope(|scope| {
        let observer = scope.spawn(|| {
            let mut most = 0;
            while !done.load(std::sync::atomic::Ordering::Acquire) {
                most = most.max(stage_directories(scratch.path()));
            }
            most
        });
        // The owner callback runs while its Version is staged, so it sees the
        // stage a polling thread can miss.
        let mut staged_at_validation = Vec::new();
        let mut check = |generation: &graphforge_storage::ResolvedProjectGeneration,
                         version: &ResearchVersionRecord,
                         registry: &ResearchRegistry| {
            staged_at_validation.push(stage_directories(scratch.path()));
            crate::research_interchange::validation::validate(generation, version, registry)
        };
        graphforge_storage::validate_research_package_stream(
            &mut std::io::Cursor::new(&bytes),
            Some(scratch.path()),
            PortableLimits::default(),
            None,
            &mut check,
        )
        .unwrap();
        done.store(true, std::sync::atomic::Ordering::Release);
        (observer.join().unwrap(), staged_at_validation)
    });
    assert!(validated.len() >= 2);
    assert!(validated.iter().all(|staged| *staged == 1));
    assert!(most <= 1, "{most} Versions were staged at once");
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
}

#[test]
fn a_version_over_the_limit_is_refused_alike_and_the_default_passes() {
    let directory = tempfile::tempdir().unwrap();
    let (package, bytes) = bundle(directory.path());
    let default = PortableLimits::default();
    let scratch = tempfile::tempdir().unwrap();
    let report = by_file(&package, default).unwrap();
    assert_eq!(
        strip_source(&report),
        strip_source(&by_stream(&bytes, scratch.path(), default).unwrap())
    );
    let largest = *version_bytes(&report).values().max().unwrap();
    let exact = PortableLimits {
        max_research_version_bytes: largest,
        ..default
    };
    by_file(&package, exact).unwrap();
    by_stream(&bytes, scratch.path(), exact).unwrap();
    let below = PortableLimits {
        max_research_version_bytes: largest - 1,
        ..default
    };
    let file = by_file(&package, below).unwrap_err();
    let stream = by_stream(&bytes, scratch.path(), below).unwrap_err();
    assert_eq!(file.code, PortableErrorCode::LimitExceeded);
    assert_eq!((stream.code, &stream.entry), (file.code, &file.entry));
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
}

/// The same selected export, expanded, through the directory source of the one driver.
#[test]
fn an_expanded_package_gets_the_verdict_of_its_bundle() {
    let directory = tempfile::tempdir().unwrap();
    let (graph, proposal) = fixture(&directory.path().join("source"));
    let node = proposal.fields[0].object_uuid;
    let expanded = directory.path().join("expanded");
    export_selected(&graph, &proposal, node, Uuid::now_v7(), &expanded);
    let limits = PortableLimits::default();
    let report = by_file(&expanded, limits).unwrap();
    assert!(report.research_interchange);
    assert!(version_bytes(&report).len() >= 2);
}
