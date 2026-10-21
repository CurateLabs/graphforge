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

/// Authentication outranks native validation on every path: the first Version
/// is refused natively, but the scan goes on to authenticate the rest of the
/// package, so a changed byte in a later Version is what is reported, from a
/// bundle file, an expanded directory and a stream alike.
#[test]
fn a_changed_byte_after_a_refused_version_is_reported_first_on_every_path() {
    use graphforge_core::GfError;
    let directory = tempfile::tempdir().unwrap();
    let limits = PortableLimits::default();
    let scratch = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("bundled")).unwrap();
    let (package, bytes) = bundle(&directory.path().join("bundled"));
    let (graph, proposal) = fixture(&directory.path().join("expanded-source"));
    let expanded = directory.path().join("expanded");
    export_selected(
        &graph,
        &proposal,
        proposal.fields[0].object_uuid,
        Uuid::now_v7(),
        &expanded,
    );
    let versions_of = |path: &Path| -> Vec<String> {
        version_bytes(&by_file(path, limits).unwrap())
            .into_keys()
            .collect()
    };
    let (bundled_versions, expanded_versions) = (versions_of(&package), versions_of(&expanded));
    assert!(bundled_versions.len() >= 2 && expanded_versions.len() >= 2);
    // The first Version of each package is refused by its native owner.
    let validate = |refused: String| {
        move |generation: &graphforge_storage::ResolvedProjectGeneration,
              version: &ResearchVersionRecord,
              registry: &ResearchRegistry| {
            if version.version_uuid.to_string() == refused {
                Err(GfError::Validation("refused".into()))
            } else {
                crate::research_interchange::validation::validate(generation, version, registry)
            }
        }
    };
    // One byte of the second Version's first object changes.
    let needle = format!("research-retained/{}", bundled_versions[1]);
    let at = bytes
        .windows(needle.len())
        .position(|window| window == needle.as_bytes())
        .unwrap();
    let mut changed = bytes.clone();
    changed[at - at % 512 + 512] ^= 1;
    let changed_package = directory.path().join("changed.gfpb");
    std::fs::write(&changed_package, &changed).unwrap();
    let object_directory = expanded.join(format!(
        "data/components/research/research-retained/{}",
        expanded_versions[1]
    ));
    let object = std::fs::read_dir(&object_directory)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let original = std::fs::read(&object).unwrap();
    let run_file = |path: &Path, refused: &str| {
        graphforge_storage::validate_research_package(
            path,
            Some(scratch.path()),
            limits,
            None,
            &mut validate(refused.to_owned()),
        )
    };
    let run_stream = |bytes: &[u8]| {
        graphforge_storage::validate_research_package_stream(
            &mut std::io::Cursor::new(bytes),
            Some(scratch.path()),
            limits,
            None,
            &mut validate(bundled_versions[0].clone()),
        )
    };
    // Only the first Version is damaged: the same refusal on every path.
    let only = [
        run_file(&package, &bundled_versions[0]),
        run_file(&expanded, &expanded_versions[0]),
        run_stream(&bytes),
    ];
    for result in &only {
        assert_eq!(
            result.as_ref().unwrap_err().code,
            PortableErrorCode::Incompatible
        );
    }
    assert_eq!(
        only[0].as_ref().unwrap_err().entry,
        only[2].as_ref().unwrap_err().entry
    );
    // A changed byte in the second Version is authentication, and it wins.
    let mut flipped = original.clone();
    flipped[0] ^= 1;
    std::fs::set_permissions(&object, {
        let mut permissions = std::fs::metadata(&object).unwrap().permissions();
        permissions.set_readonly(false);
        permissions
    })
    .unwrap();
    std::fs::write(&object, &flipped).unwrap();
    let combined = [
        run_file(&changed_package, &bundled_versions[0]),
        run_file(&expanded, &expanded_versions[0]),
        run_stream(&changed),
        // The facade checks authenticity first, with the real native owner.
        by_file(&changed_package, limits),
    ];
    for result in &combined {
        let error = result.as_ref().unwrap_err();
        assert_eq!(error.code, PortableErrorCode::DigestMismatch, "{error:?}");
    }
    assert_eq!(
        combined[0].as_ref().unwrap_err().entry,
        combined[2].as_ref().unwrap_err().entry
    );
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
}

/// One tar member of a canonical bundle: its header run, path and payload.
struct Member {
    run: Vec<u8>,
    path: String,
    payload: Vec<u8>,
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn members(bundle: &[u8]) -> Vec<Member> {
    let mut members = Vec::new();
    let mut at = 0;
    let mut pax = Vec::new();
    while bundle[at..at + 512].iter().any(|byte| *byte != 0) {
        let header = &bundle[at..at + 512];
        let size = usize::from_str_radix(
            std::str::from_utf8(&header[124..136])
                .unwrap()
                .trim_matches(['\0', ' ']),
            8,
        )
        .unwrap();
        let end = at + 512 + size.div_ceil(512) * 512;
        if header[156] == b'x' {
            pax.extend_from_slice(&bundle[at..end]);
        } else {
            let field = |range: std::ops::Range<usize>| {
                std::str::from_utf8(&header[range])
                    .unwrap()
                    .trim_end_matches('\0')
                    .to_owned()
            };
            let (name, prefix) = (field(0..100), field(345..500));
            let path = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            let mut run = std::mem::take(&mut pax);
            run.extend_from_slice(header);
            members.push(Member {
                run,
                path,
                payload: bundle[at + 512..at + 512 + size].to_vec(),
            });
        }
        at = end;
    }
    members
}

fn bundle_bytes(members: &[Member]) -> Vec<u8> {
    let mut out = Vec::new();
    for member in members {
        let mut run = member.run.clone();
        let header = run.len() - 512;
        let header = &mut run[header..];
        header[124..136].copy_from_slice(format!("{:011o}\0", member.payload.len()).as_bytes());
        header[148..156].fill(b' ');
        let sum: u64 = header.iter().map(|byte| u64::from(*byte)).sum();
        header[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
        out.extend(run);
        out.extend(&member.payload);
        out.resize(out.len().div_ceil(512) * 512, 0);
    }
    out.extend([0_u8; 1024]);
    out
}

/// The first staged core object of the bundle that is a Parquet file is
/// replaced by `replacement` under its own content address, and every digest
/// that covers it in the package is recomputed: the manifest's file record,
/// the package digest, the payload inventory and the tag manifest. Returns the
/// new bundle and the replaced object's new path.
fn with_core_parquet_replaced(bundle: &[u8], replacement: &[u8]) -> (Vec<u8>, String) {
    const MANIFEST: &str = "data/graphforge-project.json";
    let mut members = members(bundle);
    let target = members
        .iter()
        .position(|member| {
            member.path.contains("/research-retained/") && member.payload.ends_with(b"PAR1")
        })
        .expect("a staged core Parquet object");
    let old = members[target].path.clone();
    let directory = old.rsplit_once('/').unwrap().0.to_owned();
    let new = format!("{directory}/{}", sha256_hex(replacement));
    {
        let member = &mut members[target];
        let header = member.run.len() - 512;
        let header = &mut member.run[header..];
        header[..100].fill(0);
        let name = new.rsplit_once('/').unwrap().1;
        header[..name.len()].copy_from_slice(name.as_bytes());
        member.path = new.clone();
        member.payload = replacement.to_vec();
    }
    members.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
    let digest = |members: &[Member], path: &str| -> (String, usize) {
        let member = members.iter().find(|member| member.path == path).unwrap();
        (sha256_hex(&member.payload), member.payload.len())
    };
    let (new_digest, new_length) = digest(&members, &new);
    for member in &mut members {
        if member.path != MANIFEST {
            continue;
        }
        let mut manifest: serde_json::Value = serde_json::from_slice(&member.payload).unwrap();
        for component in manifest["components"].as_array_mut().unwrap() {
            let files = component["files"].as_array_mut().unwrap();
            for file in files.iter_mut() {
                if file["path"] == old.as_str() {
                    file["path"] = new.clone().into();
                    file["sha256"] = new_digest.clone().into();
                    file["length"] = new_length.into();
                }
            }
            files.sort_by(|a, b| {
                a["path"]
                    .as_str()
                    .unwrap()
                    .as_bytes()
                    .cmp(b["path"].as_str().unwrap().as_bytes())
            });
        }
        manifest.as_object_mut().unwrap().remove("package_digest");
        let canonical = serde_json::to_vec(&manifest).unwrap();
        let package = sha256_hex(&[b"graphforge-project/2\0".as_slice(), &canonical].concat());
        manifest["package_digest"] = format!("sha256:{package}").into();
        member.payload = serde_json::to_vec(&manifest).unwrap();
    }
    let manifest_digest = digest(&members, MANIFEST).0;
    for member in &mut members {
        if member.path == "manifest-sha256.txt" {
            let mut rows: Vec<(String, String)> = std::str::from_utf8(&member.payload)
                .unwrap()
                .lines()
                .map(|line| {
                    let (hash, path) = line.split_once("  ").unwrap();
                    match path {
                        p if p == old => (new.clone(), new_digest.clone()),
                        MANIFEST => (path.to_owned(), manifest_digest.clone()),
                        _ => (path.to_owned(), hash.to_owned()),
                    }
                })
                .collect();
            rows.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            member.payload = rows
                .iter()
                .map(|(path, hash)| format!("{hash}  {path}\n"))
                .collect::<String>()
                .into_bytes();
        }
    }
    let inventory_digest = digest(&members, "manifest-sha256.txt").0;
    for member in &mut members {
        if member.path == "tagmanifest-sha256.txt" {
            member.payload = std::str::from_utf8(&member.payload)
                .unwrap()
                .lines()
                .map(|line| {
                    let (hash, path) = line.split_once("  ").unwrap();
                    let hash = if path == "manifest-sha256.txt" {
                        &inventory_digest
                    } else {
                        hash
                    };
                    format!("{hash}  {path}\n")
                })
                .collect::<String>()
                .into_bytes();
        }
    }
    (bundle_bytes(&members), new)
}

/// A staged research core object with an unfunded Parquet encoding is refused
/// by the page profile before any native owner runs, from a bundle file, an
/// expanded directory and a stream. The object is staged and walked from its
/// staged copy, so nothing but that walk catches it.
#[test]
fn a_core_parquet_object_with_an_unfunded_encoding_is_refused_on_every_path() {
    let directory = tempfile::tempdir().unwrap();
    let (_, bytes) = bundle(directory.path());
    let (hostile, path) = with_core_parquet_replaced(
        &bytes,
        &crate::research_interchange::stream_parity::unfunded_parquet(),
    );
    let limits = PortableLimits::default();
    let scratch = tempfile::tempdir().unwrap();
    let bundle_path = directory.path().join("hostile.gfpb");
    std::fs::write(&bundle_path, &hostile).unwrap();
    let expanded = directory.path().join("hostile");
    for member in members(&hostile) {
        let target = expanded.join(&member.path);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(target, &member.payload).unwrap();
    }
    let mut native_runs = 0;
    let mut validate = |generation: &graphforge_storage::ResolvedProjectGeneration,
                        version: &ResearchVersionRecord,
                        registry: &ResearchRegistry| {
        native_runs += 1;
        crate::research_interchange::validation::validate(generation, version, registry)
    };
    let outcomes = [
        graphforge_storage::validate_research_package(
            &bundle_path,
            Some(scratch.path()),
            limits,
            None,
            &mut validate,
        ),
        graphforge_storage::validate_research_package(
            &expanded,
            Some(scratch.path()),
            limits,
            None,
            &mut validate,
        ),
        graphforge_storage::validate_research_package_stream(
            &mut std::io::Cursor::new(&hostile),
            Some(scratch.path()),
            limits,
            None,
            &mut validate,
        ),
    ];
    for outcome in outcomes {
        let error = outcome.unwrap_err();
        assert_eq!(error.code, PortableErrorCode::LimitExceeded, "{error:?}");
        assert_eq!(error.entry.as_deref(), Some(path.as_str()));
    }
    assert_eq!(native_runs, 0, "refused before any native owner ran");
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
}
