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

/// One member of the bundle moves to `new_path` (and takes `replacement` as its
/// payload, if given), and every digest that covers it in the package is
/// recomputed: the manifest's file record, the package digest, the payload
/// inventory and the tag manifest. Returns the new bundle and the member's new
/// path. The semantic chain that refers to the member (participants, registry)
/// is left alone: the tests that use this are refused before it is read.
fn with_member_moved(
    bundle: &[u8],
    select: impl Fn(&Member) -> bool,
    new_path: impl FnOnce(&str, &[u8]) -> String,
    replacement: Option<&[u8]>,
) -> (Vec<u8>, String) {
    const MANIFEST: &str = "data/graphforge-project.json";
    let mut members = members(bundle);
    let target = members.iter().position(select).expect("a member to move");
    let old = members[target].path.clone();
    let payload = replacement.map_or_else(|| members[target].payload.clone(), <[u8]>::to_vec);
    let new = new_path(&old, &payload);
    {
        let member = &mut members[target];
        let header = member.run.len() - 512;
        let header = &mut member.run[header..];
        let (prefix, name) = new.rsplit_once('/').unwrap();
        assert!(
            name.len() <= 100 && prefix.len() <= 155,
            "{new} needs a PAX header"
        );
        header[..100].fill(0);
        header[345..500].fill(0);
        header[..name.len()].copy_from_slice(name.as_bytes());
        header[345..345 + prefix.len()].copy_from_slice(prefix.as_bytes());
        member.path = new.clone();
        member.payload = payload;
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

/// The Versions of a research bundle, in order.
fn bundle_versions(bundle: &[u8]) -> Vec<String> {
    let mut versions: Vec<String> = members(bundle)
        .iter()
        .filter_map(|member| {
            member
                .path
                .split("/research-retained/")
                .nth(1)?
                .split('/')
                .next()
                .map(str::to_owned)
        })
        .collect();
    versions.dedup();
    versions
}

/// The first staged core Parquet object of Version number `version` is replaced
/// by `replacement` under its own content address.
fn with_core_parquet_replaced(
    bundle: &[u8],
    replacement: &[u8],
    version: usize,
) -> (Vec<u8>, String) {
    let directory = format!("/research-retained/{}/", bundle_versions(bundle)[version]);
    with_member_moved(
        bundle,
        |member| member.path.contains(&directory) && member.payload.ends_with(b"PAR1"),
        |old, payload| {
            format!(
                "{}/{}",
                old.rsplit_once('/').unwrap().0,
                sha256_hex(payload)
            )
        },
        Some(replacement),
    )
}

/// Run the research validation on a bundle file, an expanded directory and a
/// stream of the same bytes. `refused` names a Version whose native owner
/// refuses; the count is how many Versions reached a native owner.
fn on_every_path(
    directory: &Path,
    scratch: &Path,
    name: &str,
    bytes: &[u8],
    refused: Option<&str>,
) -> ([Result<PortableReport, PortableError>; 3], usize) {
    use graphforge_core::GfError;
    let limits = PortableLimits::default();
    let bundle_path = directory.join(format!("{name}.gfpb"));
    std::fs::write(&bundle_path, bytes).unwrap();
    let expanded = directory.join(name);
    for member in members(bytes) {
        let target = expanded.join(&member.path);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(target, &member.payload).unwrap();
    }
    let runs = std::cell::Cell::new(0);
    let mut validate = |generation: &graphforge_storage::ResolvedProjectGeneration,
                        version: &ResearchVersionRecord,
                        registry: &ResearchRegistry| {
        runs.set(runs.get() + 1);
        if refused.is_some_and(|refused| version.version_uuid.to_string() == refused) {
            Err(GfError::Validation("refused".into()))
        } else {
            crate::research_interchange::validation::validate(generation, version, registry)
        }
    };
    let outcomes = [
        graphforge_storage::validate_research_package(
            &bundle_path,
            Some(scratch),
            limits,
            None,
            &mut validate,
        ),
        graphforge_storage::validate_research_package(
            &expanded,
            Some(scratch),
            limits,
            None,
            &mut validate,
        ),
        graphforge_storage::validate_research_package_stream(
            &mut std::io::Cursor::new(bytes),
            Some(scratch),
            limits,
            None,
            &mut validate,
        ),
    ];
    (outcomes, runs.get())
}

fn assert_refused_alike(
    outcomes: &[Result<PortableReport, PortableError>; 3],
    code: PortableErrorCode,
    entry: Option<&str>,
) {
    for outcome in outcomes {
        let error = outcome.as_ref().unwrap_err();
        assert_eq!(error.code, code, "{error:?}");
        if let Some(entry) = entry {
            assert_eq!(error.entry.as_deref(), Some(entry), "{error:?}");
        }
    }
}

/// A staged research core object with an unfunded Parquet encoding is refused
/// by the page profile before any native owner runs, from a bundle file, an
/// expanded directory and a stream. The object is staged and walked from its
/// staged copy, so nothing but that walk catches it.
#[test]
fn a_core_parquet_object_with_an_unfunded_encoding_is_refused_on_every_path() {
    let directory = tempfile::tempdir().unwrap();
    let (_, bytes) = bundle(directory.path());
    let unfunded = crate::research_interchange::stream_parity::unfunded_parquet();
    let (hostile, path) = with_core_parquet_replaced(&bytes, &unfunded, 0);
    let scratch = tempfile::tempdir().unwrap();
    let (outcomes, native_runs) =
        on_every_path(directory.path(), scratch.path(), "hostile", &hostile, None);
    assert_refused_alike(&outcomes, PortableErrorCode::LimitExceeded, Some(&path));
    assert_eq!(native_runs, 0, "refused before any native owner ran");
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
}

/// A profile refusal is a refusal of its Version, deferred like a native one:
/// the first refused Version in order answers, and authentication outranks both.
#[test]
fn a_profile_refusal_is_deferred_like_a_native_refusal() {
    let directory = tempfile::tempdir().unwrap();
    let (_, bytes) = bundle(directory.path());
    let versions = bundle_versions(&bytes);
    assert!(versions.len() >= 2);
    let unfunded = crate::research_interchange::stream_parity::unfunded_parquet();
    let scratch = tempfile::tempdir().unwrap();
    let corrupt = |bytes: &[u8], version: &str| {
        let needle = format!("research-retained/{version}");
        let at = bytes
            .windows(needle.len())
            .position(|window| window == needle.as_bytes())
            .unwrap();
        let mut changed = bytes.to_vec();
        changed[at - at % 512 + 512] ^= 1;
        changed
    };
    // (i) An unfunded object in the first Version, a changed byte in the second.
    let (early, _) = with_core_parquet_replaced(&bytes, &unfunded, 0);
    let (outcomes, _) = on_every_path(
        directory.path(),
        scratch.path(),
        "authentication-first",
        &corrupt(&early, &versions[1]),
        None,
    );
    assert_refused_alike(&outcomes, PortableErrorCode::DigestMismatch, None);
    // (ii) A native refusal in the first Version, an unfunded object in the
    // second: the first Version answers.
    let (late, _) = with_core_parquet_replaced(&bytes, &unfunded, 1);
    let (outcomes, _) = on_every_path(
        directory.path(),
        scratch.path(),
        "native-first",
        &late,
        Some(&versions[0]),
    );
    assert_refused_alike(&outcomes, PortableErrorCode::Incompatible, None);
    // The reverse: an unfunded object in the first Version, a native refusal in the second.
    let (outcomes, native_runs) = on_every_path(
        directory.path(),
        scratch.path(),
        "profile-first",
        &early,
        Some(&versions[1]),
    );
    assert_refused_alike(&outcomes, PortableErrorCode::LimitExceeded, None);
    assert_eq!(native_runs, 0, "no native work after the first refusal");
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
}

/// Nothing but a canonical evidence object may sit below the evidence
/// directory, whose files are exempt from the page profile: a Parquet payload
/// moved there is refused at its header on every path, never decoded.
#[test]
fn a_parquet_payload_below_the_evidence_directory_is_refused_on_every_path() {
    let directory = tempfile::tempdir().unwrap();
    let (_, bytes) = bundle(directory.path());
    let (hostile, path) = with_member_moved(
        &bytes,
        |member| {
            member.path.starts_with("data/components/")
                && !member.path.contains("/research/")
                && member.payload.ends_with(b"PAR1")
        },
        |_, _| "data/components/research/research-retained-evidence/payload.parquet".to_owned(),
        None,
    );
    let scratch = tempfile::tempdir().unwrap();
    let (outcomes, native_runs) =
        on_every_path(directory.path(), scratch.path(), "reserved", &hostile, None);
    assert_refused_alike(&outcomes, PortableErrorCode::InvalidStructure, Some(&path));
    assert_eq!(native_runs, 0);
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
}

/// Once a Version is refused for any reason its stage is reclaimed and its
/// remaining entries are only hashed: a large entry that follows the refusal
/// is neither staged nor copied for the page walk, and the result is the
/// deferred refusal.
#[cfg(target_os = "linux")]
#[test]
fn a_refused_version_stages_and_spills_nothing_more() {
    let directory = tempfile::tempdir().unwrap();
    let (_, bytes) = bundle(directory.path());
    let first = bundle_versions(&bytes)[0].clone();
    let version_directory = format!("/research-retained/{first}/");
    let digests: Vec<String> = members(&bytes)
        .iter()
        .filter(|member| member.path.contains(&version_directory))
        .map(|member| member.path.rsplit_once('/').unwrap().1.to_owned())
        .collect();
    assert!(digests.len() >= 2);
    // A large object ending in the Parquet magic that sorts after the rest of
    // its Version, so the refusal of the first object comes before it.
    let mut payload = vec![0x5a_u8; 9 * 1024 * 1024];
    let length = payload.len();
    payload[length - 4..].copy_from_slice(b"PAR1");
    let last = digests.iter().max().unwrap();
    let mut salt = 0_u32;
    while sha256_hex(&payload) <= *last {
        salt += 1;
        payload[..4].copy_from_slice(&salt.to_le_bytes());
    }
    let (large, _) = with_core_parquet_replaced(&bytes, &payload, 0);
    let refused = members(&large)
        .iter()
        .find(|member| member.path.contains(&version_directory))
        .unwrap()
        .path
        .clone();
    let limits = PortableLimits {
        max_research_version_bytes: 1,
        max_parquet_walk_bytes: 1024 * 1024,
        ..PortableLimits::default()
    };
    let scratch = tempfile::tempdir().unwrap();
    let done = std::sync::atomic::AtomicBool::new(false);
    let (result, peak) = std::thread::scope(|scope| {
        let sampler = scope.spawn(|| {
            let mut peak = 0;
            while !done.load(std::sync::atomic::Ordering::Acquire) {
                peak = peak.max(crate::research_interchange::stream_parity::allocated_under(
                    scratch.path(),
                ));
            }
            peak
        });
        let result = by_stream_paced(&large, scratch.path(), limits);
        done.store(true, std::sync::atomic::Ordering::Release);
        (result, sampler.join().unwrap())
    });
    let error = result.unwrap_err();
    assert_eq!(error.code, PortableErrorCode::LimitExceeded, "{error:?}");
    assert_eq!(error.entry.as_deref(), Some(refused.as_str()));
    assert!(
        peak < payload.len() as u64,
        "{peak} bytes were allocated in scratch for a {} byte entry after the refusal",
        payload.len()
    );
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
}

fn by_stream_paced(
    bytes: &[u8],
    scratch: &Path,
    limits: PortableLimits,
) -> Result<PortableReport, PortableError> {
    crate::verify_portable_stream(
        &mut crate::research_interchange::stream_parity::Paced(std::io::Cursor::new(bytes)),
        Some(scratch),
        limits,
        None,
    )
}

/// The bundle with a second local evidence reference added to every Version
/// that has one: the same sha256 under a new artifact, `length` bytes long. The
/// registry, its archive copy and their identity commitments are rewritten and
/// every digest that covers them recomputed.
fn with_duplicate_evidence(bytes: &[u8], length: impl Fn(u64) -> u64) -> Vec<u8> {
    use graphforge_storage::research_versions::ResearchEvidenceReference::Local;
    use sha2::{Digest, Sha256};
    let duplicate = |version: &mut ResearchVersionRecord| {
        let Some(mut copy) = version
            .content
            .evidence
            .iter()
            .find(|evidence| matches!(evidence, Local { .. }))
            .cloned()
        else {
            return None;
        };
        if let Local {
            artifact_uuid,
            byte_length,
            ..
        } = &mut copy
        {
            *artifact_uuid = Uuid::now_v7();
            *byte_length = length(*byte_length);
        }
        // First, so a map that keeps the last length keeps the true one.
        version.content.evidence.insert(0, copy);
        let mut logical = version.clone();
        logical.content.generation_uuid = Uuid::nil();
        logical.content.manifest_sha256 = [0; 32];
        Some(<[u8; 32]>::from(Sha256::digest(
            serde_json::to_vec(&logical).unwrap(),
        )))
    };
    reauthenticated(bytes, |registry| {
        for (id, version) in &mut registry.versions {
            if let Some(identity) = duplicate(version) {
                registry.identities.insert(*id, identity);
                for archive in registry.interchange.values_mut() {
                    if let Some(archived) = archive.versions.get_mut(id) {
                        *archived = version.clone();
                        archive.identities.insert(*id, identity);
                    }
                }
            }
        }
    })
}

/// Two references to one object must agree on its length. One that does not is
/// refused as it was before the driver, alike from every source; one that does
/// still verifies.
#[test]
fn conflicting_evidence_lengths_are_refused_alike_and_equal_ones_verify() {
    let directory = tempfile::tempdir().unwrap();
    let payload = vec![0x5a_u8; 4096];
    let (_, bytes) =
        crate::research_interchange::closure_tests::evidence_bundle(directory.path(), |_| {
            payload.clone()
        });
    let scratch = tempfile::tempdir().unwrap();
    let equal = with_duplicate_evidence(&bytes, |length| length);
    let (outcomes, _) = on_every_path(directory.path(), scratch.path(), "equal", &equal, None);
    for outcome in &outcomes {
        outcome.as_ref().expect("equal lengths verify");
    }
    let conflicting = with_duplicate_evidence(&bytes, |length| length + 1);
    let (outcomes, _) = on_every_path(
        directory.path(),
        scratch.path(),
        "conflicting",
        &conflicting,
        None,
    );
    assert_refused_alike(&outcomes, PortableErrorCode::Incompatible, None);
    let message = outcomes[0].as_ref().unwrap_err().to_string();
    assert!(message.contains("selected closure is invalid"), "{message}");
    for outcome in &outcomes {
        assert_eq!(outcome.as_ref().unwrap_err().to_string(), message);
    }
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
}

/// A saved query and an ontology are components the scan of an expanded package
/// does not retain, so the checks that read them reopen the directory. The same
/// research package, expanded and bundled, verifies alike from the directory,
/// the bundle file and a stream.
#[test]
fn an_expanded_package_with_saved_queries_and_an_ontology_verifies_like_its_bundle() {
    use crate::research_interchange::closure_tests::{generation, install_ontology};
    let directory = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(directory.path().join("source").to_str()).unwrap();
    graph.execute("CREATE (:Character), (:Character)").unwrap();
    graph
        .create_saved_query(SavedQuery {
            query_uuid: Uuid::now_v7(),
            name: "Characters".into(),
            description: None,
            query: "MATCH (n:Character) RETURN count(n) AS people".into(),
            parameters: BTreeMap::new(),
        })
        .unwrap();
    let branch = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                author: None,
                committer: None,
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: generation(&graph),
                branch_uuid: branch,
                version_uuid: Uuid::now_v7(),
                source: BranchSource::Current {
                    origin_version_uuid: Uuid::now_v7(),
                    context_uuid: Uuid::now_v7(),
                },
                creator_uuid: Uuid::now_v7(),
                created_at: 1,
                label: "Queries-and-ontology".into(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let version = install_ontology(&mut graph, branch);
    let export = |name: &str, bundled: bool| {
        let output = directory.path().join(name);
        graph
            .export_research(
                &ExportResearchRequest {
                    version_uuid: version,
                    output: output.clone(),
                    bundled,
                    projection: None,
                },
                &CancellationToken::new(),
            )
            .unwrap();
        output
    };
    let expanded = export("expanded", false);
    let bundle = export("bundle.gfpb", true);
    let kinds: Vec<_> = std::fs::read_dir(expanded.join("data/components"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    for kind in ["ontology", "settings"] {
        assert!(kinds.iter().any(|name| name == kind), "{kind} in {kinds:?}");
    }
    let limits = PortableLimits::default();
    let from_directory = by_file(&expanded, limits).unwrap();
    let from_bundle = by_file(&bundle, limits).unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let from_stream = by_stream(&std::fs::read(&bundle).unwrap(), scratch.path(), limits).unwrap();
    assert!(from_directory.research_interchange);
    assert_eq!(from_directory.package_digest, from_bundle.package_digest);
    assert_eq!(
        from_directory.research_entries,
        from_bundle.research_entries
    );
    assert_eq!(strip_source(&from_bundle), strip_source(&from_stream));
}
