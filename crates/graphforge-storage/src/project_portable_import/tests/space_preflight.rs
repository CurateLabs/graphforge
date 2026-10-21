//! Disk and filesystem admission before an import extracts anything (#1770).

use super::super::adjacency::tests::{construction_published_project_sized, export_complete};
use super::*;
use std::collections::BTreeSet;
#[cfg(unix)]
use std::sync::Arc;
use std::sync::atomic::Ordering;

fn names(directory: &Path) -> BTreeSet<String> {
    fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect()
}

/// Run an import of `package` into `parent/imported` with a free-space probe,
/// returning the result and every progress phase the import reported.
fn import_with_space(
    package: &Path,
    parent: &Path,
    transaction: Uuid,
    available: space::AvailableSpace,
) -> (
    Result<PortableImportReceipt, PortableError>,
    Vec<PortableImportPhase>,
) {
    let mut phases = Vec::new();
    let result = import_complete_portable_native(
        package,
        parent.join("imported"),
        transaction,
        // A retry of one transaction publishes one generation.
        Uuid::from_u128(transaction.as_u128() ^ 1),
        &supported(),
        PortableLimits::default(),
        None,
        |progress| phases.push(progress.phase),
        None,
        None,
        available,
    );
    (result, phases)
}

/// Nothing of the import exists: no stage, no ownership marker, no target.
fn assert_nothing_staged(parent: &Path) {
    let staged: Vec<_> = names(parent)
        .into_iter()
        .filter(|name| name == "imported" || name.contains(".portable-"))
        .collect();
    assert!(staged.is_empty(), "the refused import staged {staged:?}");
}

static AVAILABLE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn available_from_static(_: &Path) -> std::io::Result<u64> {
    Ok(AVAILABLE.load(Ordering::SeqCst))
}

#[test]
fn an_import_that_cannot_fit_is_refused_before_anything_is_extracted() {
    let source = construction_published_project_sized(8, 24);
    let (_package_owner, package) = export_complete(source.path());
    let footprint =
        crate::portable_import_footprint(&package, PortableLimits::default(), None).unwrap();
    let needed = footprint.peak_bytes();

    let parent = tempfile::tempdir().unwrap();
    let (result, phases) = import_with_space(&package, parent.path(), Uuid::new_v4(), |_| Ok(1024));
    let error = result.unwrap_err();
    assert_eq!(error.code, PortableErrorCode::InsufficientSpace);
    assert!(
        error
            .cause
            .as_deref()
            .unwrap_or_default()
            .contains("1024 bytes available"),
        "{error:?}"
    );
    assert!(
        !phases.contains(&PortableImportPhase::Materialized),
        "{phases:?}"
    );
    assert_nothing_staged(parent.path());

    // The probe is the whole difference: a volume that holds the footprint
    // imports the same package, and a byte less refuses it again.
    let (result, _) = import_with_space(&package, parent.path(), Uuid::new_v4(), |_| Ok(u64::MAX));
    result.unwrap();
    let other = tempfile::tempdir().unwrap();
    AVAILABLE.store(needed - 1, Ordering::SeqCst);
    let (result, phases) = import_with_space(
        &package,
        other.path(),
        Uuid::new_v4(),
        available_from_static,
    );
    assert_eq!(
        result.unwrap_err().code,
        PortableErrorCode::InsufficientSpace
    );
    assert!(
        !phases.contains(&PortableImportPhase::Materialized),
        "{phases:?}"
    );
    assert_nothing_staged(other.path());
    AVAILABLE.store(needed, Ordering::SeqCst);
    let (result, _) = import_with_space(
        &package,
        other.path(),
        Uuid::new_v4(),
        available_from_static,
    );
    result.unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn an_unsupported_filesystem_is_refused_before_anything_is_extracted() {
    let source = construction_published_project_sized(8, 24);
    let (_package_owner, package) = export_complete(source.path());
    // `/dev/shm` is tmpfs, which the durable-publication contract does not admit.
    let parent = tempfile::tempdir_in("/dev/shm").unwrap();
    let (result, phases) =
        import_with_space(&package, parent.path(), Uuid::new_v4(), |_| Ok(u64::MAX));
    let error = result.unwrap_err();
    assert_eq!(error.code, PortableErrorCode::UnsupportedFilesystem);
    assert!(
        error
            .cause
            .as_deref()
            .unwrap_or_default()
            .contains("GF_UNSUPPORTED_FILESYSTEM"),
        "{error:?}"
    );
    assert!(
        !phases.contains(&PortableImportPhase::Materialized),
        "{phases:?}"
    );
    assert_nothing_staged(parent.path());
}

/// A stage an interrupted attempt of the import left, owned by `transaction`.
#[cfg(unix)]
fn interrupted_stage(parent: &Path, transaction: Uuid) -> (PathBuf, PathBuf, PathBuf) {
    let target = parent.join("imported");
    let (stage, owner) = space::stage_paths(&target, transaction).unwrap();
    fs::create_dir_all(stage.join("data")).unwrap();
    fs::write(&owner, transaction.hyphenated().to_string()).unwrap();
    (target, stage, owner)
}

#[cfg(unix)]
fn held(target: &Path, transaction: Uuid) -> u64 {
    space::held_bytes(target, transaction, PortableLimits::default()).unwrap()
}

#[cfg(unix)]
fn allocated(path: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt as _;
    fs::metadata(path).unwrap().blocks() * 512
}

#[cfg(unix)]
#[test]
fn a_retry_is_credited_only_for_the_allocation_its_own_stage_would_free() {
    let parent = tempfile::tempdir().unwrap();
    let transaction = Uuid::new_v4();
    let (target, stage, owner) = interrupted_stage(parent.path(), transaction);
    fs::write(stage.join("data/payload"), vec![1_u8; 3000]).unwrap();
    // The target is not removed by the retry, so it is not credited.
    fs::create_dir(&target).unwrap();
    fs::write(target.join("object"), vec![1_u8; 5000]).unwrap();
    let payload = allocated(&stage.join("data/payload"));
    assert!(payload >= 3000);
    assert_eq!(held(&target, transaction), payload);

    // A sparse file allocates nothing, whatever its length: removing it frees
    // no space.
    let sparse = fs::File::create(stage.join("data/sparse")).unwrap();
    sparse.set_len(1 << 30).unwrap();
    assert_eq!(
        held(&target, transaction),
        payload + allocated(&stage.join("data/sparse"))
    );
    assert!(allocated(&stage.join("data/sparse")) < 1 << 20);

    // A file the target also links frees nothing by leaving the stage.
    fs::hard_link(target.join("object"), stage.join("data/linked")).unwrap();
    assert_eq!(
        held(&target, transaction),
        payload + allocated(&stage.join("data/sparse"))
    );

    // A symlink is neither followed nor credited.
    std::os::unix::fs::symlink(target.join("object"), stage.join("data/symlink")).unwrap();
    assert_eq!(
        held(&target, transaction),
        payload + allocated(&stage.join("data/sparse"))
    );

    // The stage is the one `claim_stage` would reuse only under a marker that
    // is a regular file naming this transaction.
    fs::write(&owner, Uuid::new_v4().hyphenated().to_string()).unwrap();
    assert_eq!(held(&target, transaction), 0);
    fs::remove_file(&owner).unwrap();
    let elsewhere = parent.path().join("marker");
    fs::write(&elsewhere, transaction.hyphenated().to_string()).unwrap();
    std::os::unix::fs::symlink(&elsewhere, &owner).unwrap();
    assert_eq!(held(&target, transaction), 0);
}

#[cfg(unix)]
#[test]
fn a_sparse_stage_does_not_let_a_retry_past_the_space_check() {
    let source = construction_published_project_sized(8, 24);
    let (_package_owner, package) = export_complete(source.path());
    let needed = crate::portable_import_footprint(&package, PortableLimits::default(), None)
        .unwrap()
        .peak_bytes();
    let parent = tempfile::tempdir().unwrap();
    let transaction = Uuid::new_v4();
    let (_, stage, _) = interrupted_stage(parent.path(), transaction);
    fs::File::create(stage.join("data/sparse"))
        .unwrap()
        .set_len(1 << 30)
        .unwrap();
    AVAILABLE.store(needed - 1, Ordering::SeqCst);
    let (result, phases) =
        import_with_space(&package, parent.path(), transaction, available_from_static);
    assert_eq!(
        result.unwrap_err().code,
        PortableErrorCode::InsufficientSpace
    );
    assert!(
        !phases.contains(&PortableImportPhase::Materialized),
        "{phases:?}"
    );
}

/// Allocated bytes (blocks, a hard-linked file once) under `root`.
#[cfg(unix)]
fn allocated_bytes(root: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt as _;
    fn visit(directory: &Path, seen: &mut BTreeSet<(u64, u64)>, total: &mut u64) {
        let Ok(entries) = fs::read_dir(directory) else {
            return;
        };
        for entry in entries.flatten() {
            let Ok(metadata) = fs::symlink_metadata(entry.path()) else {
                continue;
            };
            if metadata.is_dir() {
                visit(&entry.path(), seen, total);
            } else if metadata.is_file() && seen.insert((metadata.dev(), metadata.ino())) {
                *total += metadata.blocks() * 512;
            }
        }
    }
    let mut total = 0;
    visit(root, &mut BTreeSet::new(), &mut total);
    total
}

/// Samples the allocated bytes under a directory every millisecond.
#[cfg(unix)]
struct PeakSampler {
    stop: Arc<AtomicBool>,
    handle: std::thread::JoinHandle<u64>,
}

#[cfg(unix)]
impl PeakSampler {
    fn start(root: PathBuf) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            let mut peak = 0;
            while !flag.load(Ordering::Relaxed) {
                peak = peak.max(allocated_bytes(&root));
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            peak
        });
        Self { stop, handle }
    }

    fn finish(self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.join().unwrap()
    }
}

/// The footprint is the peak a complete import measures. The package carries
/// its adjacency index, as every package this engine exports does.
///
/// `observed` is the import's own allocation accounting, the peak of every
/// byte its stage and target hold; the sampler measures the same directory
/// from outside, block by block, so neither can be the artifact of the other.
/// The footprint must cover both, and must not exceed the accounted peak by
/// more than `SLACK_PERCENT`: an estimate that large would refuse imports
/// that fit.
#[cfg(unix)]
#[test]
fn the_footprint_bounds_the_measured_peak_of_a_complete_import_within_its_slack() {
    const SLACK_PERCENT: u64 = 125;
    let source = construction_published_project_sized(40_000, 400_000);
    let generation = crate::resolve_project_generation(source.path()).unwrap();
    let packages = tempfile::tempdir().unwrap();
    for (name, output) in [
        ("bundle.gfpb", crate::PortableOutput::Bundle),
        ("expanded.gfproject", crate::PortableOutput::Expanded),
    ] {
        let package = packages.path().join(name);
        let limits = crate::PortableExportLimits::default();
        let plan = crate::plan_complete_portable(&generation, limits).unwrap();
        crate::export_complete_portable(
            &plan,
            &package,
            output,
            limits,
            &AtomicBool::new(false),
            |_| {},
        )
        .unwrap();
        let footprint =
            crate::portable_import_footprint(&package, PortableLimits::default(), None).unwrap();
        assert!(
            footprint.payload_bytes > 8 * 1024 * 1024,
            "{name}: a package this small measures only fixed costs"
        );
        let estimate = footprint.peak_bytes();

        let parent = tempfile::tempdir().unwrap();
        let target = parent.path().join("imported");
        let allocation = crate::StorageAllocationOperation::from_paths(
            &crate::StorageAllocationOperation::project_paths(&target).unwrap(),
        )
        .unwrap();
        let sampler = PeakSampler::start(parent.path().to_path_buf());
        let receipt = import_complete_portable_with_allocation(
            &package,
            &target,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported(),
            PortableLimits::default(),
            None,
            |_| {},
            Some(&allocation),
        )
        .unwrap();
        let sampled = sampler.finish();
        let observed = receipt.transient_peak_allocated_bytes;
        println!(
            "{name}: payload={} entries={} observed={observed} sampled={sampled} estimate={estimate}",
            footprint.payload_bytes, footprint.entries
        );
        assert!(
            observed <= estimate,
            "{name}: observed {observed} exceeds estimate {estimate}"
        );
        assert!(
            sampled <= estimate,
            "{name}: sampled {sampled} exceeds estimate {estimate}"
        );
        assert!(
            estimate * 100 <= observed * SLACK_PERCENT,
            "{name}: estimate {estimate} exceeds observed {observed} by more than {SLACK_PERCENT}%"
        );
    }
}

/// An ontology-dominated package: 100,000 entity types make one authored
/// module of 5.3 MB, and the import generates two controls that each re-encode
/// it. Measured, that import holds five times its payload, not two.
#[cfg(unix)]
#[test]
fn the_footprint_bounds_an_ontology_dominated_import_within_its_slack() {
    const SLACK_PERCENT: u64 = 130;
    let (_owner, package) = composition_package_with_entity_types(100_000);
    let footprint =
        crate::portable_import_footprint(&package, PortableLimits::default(), None).unwrap();
    assert!(footprint.payload_bytes > 5_000_000, "{footprint:?}");
    assert!(footprint.generated_bytes > 10_000_000, "{footprint:?}");
    let estimate = footprint.peak_bytes();

    let parent = tempfile::tempdir().unwrap();
    let target = parent.path().join("imported");
    let allocation = crate::StorageAllocationOperation::from_paths(
        &crate::StorageAllocationOperation::project_paths(&target).unwrap(),
    )
    .unwrap();
    let sampler = PeakSampler::start(parent.path().to_path_buf());
    let receipt = import_complete_portable_with_allocation(
        &package,
        &target,
        Uuid::new_v4(),
        Uuid::new_v4(),
        &supported(),
        PortableLimits::default(),
        None,
        |_| {},
        Some(&allocation),
    )
    .unwrap();
    let sampled = sampler.finish();
    let observed = receipt.transient_peak_allocated_bytes;
    println!(
        "ontology: payload={} generated={} observed={observed} sampled={sampled} estimate={estimate}",
        footprint.payload_bytes, footprint.generated_bytes
    );
    assert!(
        observed <= estimate,
        "observed {observed} exceeds estimate {estimate}"
    );
    assert!(
        sampled <= estimate,
        "sampled {sampled} exceeds estimate {estimate}"
    );
    assert!(
        estimate * 100 <= observed * SLACK_PERCENT,
        "estimate {estimate} exceeds observed {observed} by more than {SLACK_PERCENT}%"
    );
}

/// A complete package whose composition bridges two small modules with a
/// bridge whose provenance justification is `justification_bytes` long, so
/// the authored bridge document, which lives in a `schema` component, grows
/// with it.
#[cfg(unix)]
fn composition_package_with_bridge(justification_bytes: usize) -> (tempfile::TempDir, PathBuf) {
    use graphforge_ontology::{
        ActivationMode, AuthoredModule, BridgeAssertion, BridgeDocument, BridgePredicate,
        BridgeProvenance, BridgeSetId, CompositionLimits, EntityTypeDef, InventoryCompileRequest,
        MappingMethod, OntologyDoc, OntologyModuleId, QualifiedSymbol, SymbolKind,
        bridge_document_digest, compile_inventory, module_document_digest,
    };
    let module = |name: &str| {
        let document = OntologyDoc {
            ontology_id: format!("https://graphforge.dev/ontology/{name}"),
            version: "v1".into(),
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
    let (left, right) = (module("left"), module("right"));
    let symbol = |module: &AuthoredModule| QualifiedSymbol {
        module: module.id.clone(),
        kind: SymbolKind::Entity,
        local_id: "Person".into(),
    };
    let bridge = BridgeDocument {
        bridge_id: "https://graphforge.dev/bridge/large".into(),
        authored_version: "v1".into(),
        source_modules: vec![left.id.clone()],
        target_modules: vec![right.id.clone()],
        dependencies: Vec::new(),
        shared_surfaces: Vec::new(),
        assertions: vec![BridgeAssertion {
            source: symbol(&left),
            target: symbol(&right),
            predicate: BridgePredicate::Equivalent,
            directional: false,
            provenance: BridgeProvenance {
                method: MappingMethod::Authored,
                confidence: None,
                justification: "j".repeat(justification_bytes),
                evidence_refs: Vec::new(),
            },
            valid_from: None,
            valid_to: None,
        }],
        enforcement: Some(ActivationMode::Advisory),
    };
    let bridge_id = BridgeSetId {
        bridge_id: bridge.bridge_id.clone(),
        authored_version: bridge.authored_version.clone(),
        canonical_digest: bridge_document_digest(&bridge).unwrap(),
    };
    let compiled = compile_inventory(InventoryCompileRequest {
        modules: &[left, right],
        bridges: &[bridge_id],
        activation: &[],
        profile_default: ActivationMode::Advisory,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .unwrap();
    let composition = crate::WorkspaceOntologyComposition::from_compiled(&compiled, vec![bridge]);
    let source = tempfile::tempdir().unwrap();
    let parent = crate::open_or_initialize_project(source.path()).unwrap();
    publish_composition_package(source, parent, composition)
}

/// A bridge-dominated package: a valid bridge document with a 3 MiB
/// justification lives in a `schema` component, not an `ontology` one, and
/// both generated controls re-encode it. Measured, the import held three times
/// its payload more than the ontology-only estimate allowed.
#[cfg(unix)]
#[test]
fn the_footprint_bounds_a_bridge_dominated_import_within_its_slack() {
    const SLACK_PERCENT: u64 = 130;
    let (_owner, package) = composition_package_with_bridge(3 << 20);
    let footprint =
        crate::portable_import_footprint(&package, PortableLimits::default(), None).unwrap();
    assert!(footprint.payload_bytes > 3 << 20, "{footprint:?}");
    assert!(footprint.generated_bytes > 6 << 20, "{footprint:?}");
    let estimate = footprint.peak_bytes();

    let parent = tempfile::tempdir().unwrap();
    let target = parent.path().join("imported");
    let allocation = crate::StorageAllocationOperation::from_paths(
        &crate::StorageAllocationOperation::project_paths(&target).unwrap(),
    )
    .unwrap();
    let sampler = PeakSampler::start(parent.path().to_path_buf());
    let receipt = import_complete_portable_with_allocation(
        &package,
        &target,
        Uuid::new_v4(),
        Uuid::new_v4(),
        &supported(),
        PortableLimits::default(),
        None,
        |_| {},
        Some(&allocation),
    )
    .unwrap();
    let sampled = sampler.finish();
    let observed = receipt.transient_peak_allocated_bytes;
    println!(
        "bridge: payload={} generated={} observed={observed} sampled={sampled} estimate={estimate}",
        footprint.payload_bytes, footprint.generated_bytes
    );
    assert!(
        observed <= estimate,
        "observed {observed} exceeds estimate {estimate}"
    );
    assert!(
        sampled <= estimate,
        "sampled {sampled} exceeds estimate {estimate}"
    );
    assert!(
        estimate * 100 <= observed * SLACK_PERCENT,
        "estimate {estimate} exceeds observed {observed} by more than {SLACK_PERCENT}%"
    );
}

/// A transaction that already committed is admitted against what replaying it
/// needs, not a fresh import's peak, through the import's own free-space probe.
#[test]
fn a_committed_retry_is_admitted_against_the_replay_peak() {
    let source = construction_published_project_sized(8, 24);
    let (_package_owner, package) = export_complete(source.path());
    let footprint =
        crate::portable_import_footprint(&package, PortableLimits::default(), None).unwrap();
    let (fresh, replay) = (footprint.peak_bytes(), footprint.replay_peak_bytes());
    assert!(replay < fresh);

    let parent = tempfile::tempdir().unwrap();
    let transaction = Uuid::new_v4();
    let (result, _) = import_with_space(&package, parent.path(), transaction, |_| Ok(u64::MAX));
    assert!(!result.unwrap().publication.idempotent_replay);

    // Just enough for the replay, less than a fresh import needs.
    AVAILABLE.store(replay, Ordering::SeqCst);
    let (result, _) =
        import_with_space(&package, parent.path(), transaction, available_from_static);
    assert!(result.unwrap().publication.idempotent_replay);
    // One byte less is still refused, before anything is extracted.
    AVAILABLE.store(replay - 1, Ordering::SeqCst);
    let (result, phases) =
        import_with_space(&package, parent.path(), transaction, available_from_static);
    assert_eq!(
        result.unwrap_err().code,
        PortableErrorCode::InsufficientSpace
    );
    assert!(
        !phases.contains(&PortableImportPhase::Materialized),
        "{phases:?}"
    );
    // Another transaction into the same target is not a replay.
    AVAILABLE.store(replay, Ordering::SeqCst);
    let (result, _) = import_with_space(
        &package,
        parent.path(),
        Uuid::new_v4(),
        available_from_static,
    );
    assert_eq!(
        result.unwrap_err().code,
        PortableErrorCode::InsufficientSpace
    );
}

/// The replay estimate bounds what a committed retry really holds on top of
/// the project it replays into.
#[cfg(unix)]
#[test]
fn the_replay_peak_bounds_what_a_committed_retry_holds() {
    let source = construction_published_project_sized(40_000, 400_000);
    let (_package_owner, package) = export_complete(source.path());
    let footprint =
        crate::portable_import_footprint(&package, PortableLimits::default(), None).unwrap();
    assert!(footprint.payload_bytes > 8 << 20);
    let parent = tempfile::tempdir().unwrap();
    let transaction = Uuid::new_v4();
    let (result, _) = import_with_space(&package, parent.path(), transaction, |_| Ok(u64::MAX));
    result.unwrap();
    let baseline = allocated_bytes(parent.path());
    let sampler = PeakSampler::start(parent.path().to_path_buf());
    let (result, _) = import_with_space(&package, parent.path(), transaction, |_| Ok(u64::MAX));
    assert!(result.unwrap().publication.idempotent_replay);
    let held = sampler.finish().saturating_sub(baseline);
    let replay = footprint.replay_peak_bytes();
    println!(
        "replay: held={held} replay={replay} fresh={}",
        footprint.peak_bytes()
    );
    assert!(
        held <= replay,
        "a replay held {held}, more than its estimate {replay}"
    );
}

/// Admission credited the stage as it was scanned; the reclaim must not remove
/// a stage that now frees less. The reclaim re-scans and refuses.
#[cfg(unix)]
#[test]
fn a_stage_that_frees_less_than_was_credited_is_not_reclaimed() {
    let parent = tempfile::tempdir().unwrap();
    let transaction = Uuid::new_v4();
    let (target, stage, _) = interrupted_stage(parent.path(), transaction);
    fs::write(stage.join("data/payload"), vec![1_u8; 1 << 20]).unwrap();
    let limits = PortableLimits::default();
    let credit = space::StageCredit(held(&target, transaction));
    assert!(credit.0 >= 1 << 20);
    credit.confirm_at_reclaim(&stage, limits).unwrap();

    // After the scan, the file gains a link from outside: removing the stage
    // would free nothing for it.
    fs::hard_link(stage.join("data/payload"), parent.path().join("elsewhere")).unwrap();
    let error = claim_stage(
        &stage,
        "imported",
        transaction,
        Uuid::new_v4(),
        None,
        (&credit, limits),
    )
    .unwrap_err();
    assert_eq!(error.code, PortableErrorCode::InsufficientSpace);
    assert!(stage.join("data/payload").exists(), "the stage was removed");
}
