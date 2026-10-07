//! The payload inventory, `manifest-sha256.txt`, streams on both sides (#900).
//!
//! It has one row per `data/` entry. The S26 Graph500 project's inventory is
//! 21,531 rows and 5.1 MB, over the 4 MiB `max_tag_manifest_bytes` default
//! that used to bound it, so export failed. These tests lower that limit far
//! below the fixture's inventory through the public limits, the seam the real
//! path reads, and drive export, verify and import end to end.

use super::*;
use crate::{PortableV2Report, PortableV2Representation};

/// Room for `bagit.txt` (55 bytes), `bag-info.txt` (69) and the three-row
/// `tagmanifest-sha256.txt` (about 260), but not for the payload inventory.
const TAG_FILE_LIMIT: u64 = 512;
/// Enough rows that the inventory spans several 64 KiB write batches.
const EXTRA_FILES: usize = 1000;

fn limits() -> PortableV2ExportLimits {
    PortableV2ExportLimits {
        max_tag_manifest_bytes: TAG_FILE_LIMIT,
        ..PortableV2ExportLimits::default()
    }
}

/// A committed generation whose graph tree holds `extra` long-path files.
fn generation_with_files(extra: usize) -> (tempfile::TempDir, ResolvedProjectGeneration) {
    let project = tempfile::tempdir().unwrap();
    let parent = open_or_initialize_project(project.path()).unwrap();
    let tree = tempfile::tempdir().unwrap();
    fs::write(tree.path().join("a.parquet"), b"graph-a").unwrap();
    let directory = tree
        .path()
        .join("other")
        .join(format!("shards-{}.d", "0123456789abcdef".repeat(4)));
    fs::create_dir_all(&directory).unwrap();
    for index in 0..extra {
        fs::write(
            directory.join(format!("{index:020}.bin")),
            format!("payload {index}"),
        )
        .unwrap();
    }
    let (_, inventory) = crate::capture_graph_files(tree.path()).unwrap();
    let mut participants = crate::empty_workspace_participants().unwrap();
    participants.insert(0, inventory);
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

fn export(
    plan: &PortableV2ExportPlan,
    destination: &Path,
    output: PortableV2Output,
    limits: PortableV2ExportLimits,
) -> Result<PortableV2ExportReceipt, PortableV2Error> {
    export_complete_portable_v2(
        plan,
        destination,
        output,
        limits,
        &AtomicBool::new(false),
        |_| {},
    )
}

fn verify(
    package: &Path,
    limits: PortableV2ExportLimits,
) -> Result<PortableV2Report, PortableV2Error> {
    crate::verify_portable_v2(package, crate::PortableV2Mode::Full, limits, None)
}

fn import(
    package: &Path,
    target: &Path,
    generation: &ResolvedProjectGeneration,
    limits: PortableV2ExportLimits,
) -> Result<(), PortableV2Error> {
    let supported = generation
        .capabilities()
        .into_iter()
        .map(|capability| crate::ProjectCapability {
            capability_id: capability.capability_id,
            capability_version: capability.capability_version,
        })
        .collect::<Vec<_>>();
    crate::import_complete_portable_v2(
        package,
        target,
        Uuid::new_v4(),
        Uuid::new_v4(),
        &supported,
        limits,
        None,
    )
    .map(|_| ())
}

/// The canonical inventory, rendered independently of the writer: every
/// `data/` file of an expanded package, ascending, `sha256  path` LF.
fn independent_inventory(expanded: &Path) -> Vec<u8> {
    fn walk(root: &Path, directory: &Path, rows: &mut Vec<(String, String)>) {
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, rows);
            } else {
                let relative = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .replace(std::path::MAIN_SEPARATOR, "/");
                let digest = sha2::Sha256::digest(fs::read(&path).unwrap());
                rows.push((relative, hex(digest.into())));
            }
        }
    }
    let mut rows = Vec::new();
    walk(expanded, &expanded.join("data"), &mut rows);
    rows.sort();
    rows.iter()
        .flat_map(|(path, digest)| format!("{digest}  {path}\n").into_bytes())
        .collect()
}

#[test]
fn a_payload_inventory_over_the_tag_file_limit_exports_verifies_and_imports() {
    let (_project, generation) = generation_with_files(EXTRA_FILES);
    let plan = plan_complete_portable_v2(&generation, limits()).unwrap();
    let output = tempfile::tempdir().unwrap();
    let expanded = output.path().join("graph.gfproject");
    let bundle = output.path().join("graph.gfpb");
    let reference = output.path().join("reference.gfpb");
    export(&plan, &expanded, PortableV2Output::Expanded, limits()).unwrap();
    export(&plan, &bundle, PortableV2Output::Bundle, limits()).unwrap();
    export(
        &plan,
        &reference,
        PortableV2Output::Bundle,
        PortableV2ExportLimits::default(),
    )
    .unwrap();

    // The inventory is far over the limit and spans several write batches,
    // and it is exactly the canonical rendering of the package's data files.
    let inventory = fs::read(expanded.join("manifest-sha256.txt")).unwrap();
    assert!(
        inventory.len() as u64 > 100 * TAG_FILE_LIMIT,
        "inventory is {} bytes",
        inventory.len()
    );
    assert!(inventory.len() > 2 * 64 * 1024, "spans write batches");
    assert_eq!(inventory, independent_inventory(&expanded));
    // The tag files the limit still bounds fit under it.
    for tag in ["bagit.txt", "bag-info.txt", "tagmanifest-sha256.txt"] {
        assert!(fs::metadata(expanded.join(tag)).unwrap().len() <= TAG_FILE_LIMIT);
    }
    // Limits are admission bounds, not format parameters: the bundle bytes do
    // not depend on them.
    assert_eq!(fs::read(&bundle).unwrap(), fs::read(&reference).unwrap());

    let bundle_report = verify(&bundle, limits()).unwrap();
    let expanded_report = verify(&expanded, limits()).unwrap();
    assert_eq!(
        bundle_report.representation,
        PortableV2Representation::Bundle
    );
    assert_eq!(bundle_report.package_digest, expanded_report.package_digest);
    assert_eq!(bundle_report.entry_count, expanded_report.entry_count);
    // Every planned file, the semantic manifest and four tag files.
    assert_eq!(
        bundle_report.entry_count,
        u64::try_from(plan.files.len() + 5).unwrap()
    );
    assert!(plan.files.len() > EXTRA_FILES);
    assert_eq!(
        inventory.iter().filter(|byte| **byte == b'\n').count(),
        plan.files.len() + 1
    );
    // A one-byte copy buffer splits every row across reads.
    let one_byte = PortableV2ExportLimits {
        copy_buffer_bytes: 1,
        ..limits()
    };
    verify(&expanded, one_byte).unwrap();
    verify(&bundle, one_byte).unwrap();

    for (name, package) in [("bundle", &bundle), ("expanded", &expanded)] {
        let target = output.path().join(format!("{name}-project"));
        import(package, &target, &generation, limits()).unwrap();
        let imported = crate::resolve_project_generation(&target).unwrap();
        assert_eq!(
            imported.graph_files_inventory().unwrap(),
            generation.graph_files_inventory().unwrap(),
            "{name}"
        );
    }
}

#[test]
fn the_tag_manifest_still_honours_the_tag_file_limit() {
    let (_project, generation) = generation_with_files(0);
    let plan = plan_complete_portable_v2(&generation, limits()).unwrap();
    let output = tempfile::tempdir().unwrap();
    let expanded = output.path().join("graph.gfproject");
    export(&plan, &expanded, PortableV2Output::Expanded, limits()).unwrap();
    let tag_manifest = fs::metadata(expanded.join("tagmanifest-sha256.txt"))
        .unwrap()
        .len();
    let tight = PortableV2ExportLimits {
        max_tag_manifest_bytes: tag_manifest - 1,
        ..limits()
    };
    let error = export(
        &plan,
        &output.path().join("tight.gfpb"),
        PortableV2Output::Bundle,
        tight,
    )
    .unwrap_err();
    assert_eq!(error.code, PortableV2ErrorCode::LimitExceeded);
    assert_eq!(
        error.to_string(),
        "portable-v2 LimitExceeded: tag manifest exceeds configured limit"
    );
    assert_eq!(
        verify(&expanded, tight).unwrap_err().code,
        PortableV2ErrorCode::LimitExceeded
    );
}

/// Replace the payload inventory of a copy of `expanded`, and verify it.
fn verify_with_inventory(expanded: &Path, inventory: &[u8]) -> PortableV2Error {
    let copy = tempfile::tempdir().unwrap();
    let root = copy.path().join("tampered.gfproject");
    copy_tree(expanded, &root);
    fs::write(root.join("manifest-sha256.txt"), inventory).unwrap();
    verify(&root, limits()).unwrap_err()
}

fn copy_tree(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

#[test]
fn a_streamed_inventory_refuses_every_deviation_from_the_canonical_rows() {
    let (_project, generation) = generation_with_files(3);
    let plan = plan_complete_portable_v2(&generation, limits()).unwrap();
    let output = tempfile::tempdir().unwrap();
    let expanded = output.path().join("graph.gfproject");
    export(&plan, &expanded, PortableV2Output::Expanded, limits()).unwrap();
    let canonical = fs::read(expanded.join("manifest-sha256.txt")).unwrap();
    let text = String::from_utf8(canonical.clone()).unwrap();
    let rows = text.lines().collect::<Vec<_>>();
    let join = |rows: &[&str]| {
        rows.iter()
            .map(|row| format!("{row}\n"))
            .collect::<String>()
    };
    let digest_flip = {
        let mut bytes = canonical.clone();
        bytes[0] = if bytes[0] == b'0' { b'1' } else { b'0' };
        bytes
    };
    let mut overlong = canonical.clone();
    overlong.extend(std::iter::repeat_n(b'a', 64 + 2 + 4096 + 1));
    let mut not_utf8 = canonical.clone();
    not_utf8[70] = 0xff;
    let cases: Vec<(&str, Vec<u8>, PortableV2ErrorCode, &str)> = vec![
        (
            "missing row",
            join(&rows[1..]).into_bytes(),
            PortableV2ErrorCode::DigestMismatch,
            "data inventory manifest",
        ),
        (
            "extra row",
            format!("{text}{}  data/zz\n", "0".repeat(64)).into_bytes(),
            PortableV2ErrorCode::DigestMismatch,
            "data inventory manifest",
        ),
        (
            "duplicate row",
            join(&[rows[0], rows[0]]).into_bytes(),
            PortableV2ErrorCode::InvalidStructure,
            "tag manifest order/duplicate",
        ),
        (
            "rows out of order",
            join(&[rows[1], rows[0]]).into_bytes(),
            PortableV2ErrorCode::DigestMismatch,
            "data inventory manifest",
        ),
        (
            "wrong digest",
            digest_flip,
            PortableV2ErrorCode::DigestMismatch,
            "data inventory manifest",
        ),
        (
            "upper-case digest",
            text.to_uppercase().into_bytes(),
            PortableV2ErrorCode::InvalidStructure,
            "tag manifest order/duplicate",
        ),
        (
            "single space",
            text.replacen("  ", " ", 1).into_bytes(),
            PortableV2ErrorCode::InvalidStructure,
            "tag manifest record",
        ),
        (
            "CRLF rows",
            text.replace('\n', "\r\n").into_bytes(),
            PortableV2ErrorCode::InvalidPath,
            "unsafe/non-canonical path",
        ),
        (
            "missing final LF",
            canonical[..canonical.len() - 1].to_vec(),
            PortableV2ErrorCode::InvalidStructure,
            "tag manifest termination",
        ),
        (
            "empty",
            Vec::new(),
            PortableV2ErrorCode::InvalidStructure,
            "tag manifest termination",
        ),
        (
            "overlong row",
            overlong,
            PortableV2ErrorCode::InvalidStructure,
            "tag manifest record length",
        ),
        (
            "not UTF-8",
            not_utf8,
            PortableV2ErrorCode::InvalidStructure,
            "tag manifest UTF-8",
        ),
    ];
    for (name, inventory, code, detail) in cases {
        let error = verify_with_inventory(&expanded, &inventory);
        assert_eq!(error.code, code, "{name}: {error}");
        assert!(error.to_string().ends_with(detail), "{name}: {error}");
    }
    // The untampered copy verifies, so each refusal is the inventory's.
    let copy = tempfile::tempdir().unwrap();
    let root = copy.path().join("copy.gfproject");
    copy_tree(&expanded, &root);
    verify(&root, limits()).unwrap();
}

#[test]
fn a_bundle_with_a_tampered_inventory_row_is_refused() {
    let (_project, generation) = generation_with_files(3);
    let plan = plan_complete_portable_v2(&generation, limits()).unwrap();
    let output = tempfile::tempdir().unwrap();
    let bundle = output.path().join("graph.gfpb");
    export(&plan, &bundle, PortableV2Output::Bundle, limits()).unwrap();
    let mut bytes = fs::read(&bundle).unwrap();
    // The first inventory row names a component file; flip one digit of its
    // digest. Payload bytes are outside the tar header checksum.
    let row = b"  data/components/";
    let at = bytes
        .windows(row.len())
        .position(|window| window == row)
        .expect("inventory row in bundle")
        - 64;
    bytes[at] = if bytes[at] == b'0' { b'1' } else { b'0' };
    let tampered = output.path().join("tampered.gfpb");
    fs::write(&tampered, bytes).unwrap();
    let error = verify(&tampered, limits()).unwrap_err();
    assert_eq!(error.code, PortableV2ErrorCode::DigestMismatch, "{error}");
    assert!(
        error.to_string().ends_with("data inventory manifest"),
        "{error}"
    );
}
