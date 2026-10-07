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

/// Replace the payload inventory of a copy of `expanded`, re-sign it in the
/// tag manifest so only the inventory check can refuse it, and verify.
fn verify_with_inventory(expanded: &Path, inventory: &[u8]) -> PortableV2Error {
    let copy = tempfile::tempdir().unwrap();
    let root = copy.path().join("tampered.gfproject");
    copy_tree(expanded, &root);
    fs::write(root.join("manifest-sha256.txt"), inventory).unwrap();
    rewrite_tag_manifest(&root);
    verify(&root, limits()).unwrap_err()
}

/// Rewrite `tagmanifest-sha256.txt` to match the package's current tag files.
fn rewrite_tag_manifest(root: &Path) {
    let tag_manifest = ["bag-info.txt", "bagit.txt", "manifest-sha256.txt"]
        .into_iter()
        .map(|tag| {
            let digest = sha2::Sha256::digest(fs::read(root.join(tag)).unwrap());
            format!("{}  {tag}\n", hex(digest.into()))
        })
        .collect::<String>();
    fs::write(root.join("tagmanifest-sha256.txt"), tag_manifest).unwrap();
}

#[test]
fn bag_manifests_refuse_extra_payload_and_unmanifested_entries() {
    let (_project, generation) = generation_with_files(3);
    let plan = plan_complete_portable_v2(&generation, limits()).unwrap();
    let output = tempfile::tempdir().unwrap();
    let expanded = output.path().join("graph.gfproject");
    export(&plan, &expanded, PortableV2Output::Expanded, limits()).unwrap();
    for (name, extra, detail) in [
        (
            "undeclared component file",
            "data/components/graph-data/graph-tree/undeclared.bin",
            "extra payload",
        ),
        (
            "unmanifested root file",
            "extra.txt",
            "unmanifested extra entry",
        ),
    ] {
        let copy = tempfile::tempdir().unwrap();
        let root = copy.path().join("extra.gfproject");
        copy_tree(&expanded, &root);
        fs::write(root.join(extra), b"extra").unwrap();
        let error = verify(&root, limits()).unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::InvalidStructure, "{name}");
        assert!(error.to_string().ends_with(detail), "{name}: {error}");
    }
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
    let bagit_row = format!(
        "{}  bagit.txt",
        hex(sha2::Sha256::digest(fs::read(expanded.join("bagit.txt")).unwrap()).into())
    );
    let cases: Vec<(&str, Vec<u8>, PortableV2ErrorCode, &str)> = vec![
        (
            "missing first row",
            join(&rows[1..]).into_bytes(),
            PortableV2ErrorCode::DigestMismatch,
            "data inventory manifest",
        ),
        (
            "missing last row",
            join(&rows[..rows.len() - 1]).into_bytes(),
            PortableV2ErrorCode::DigestMismatch,
            "data inventory manifest",
        ),
        (
            "missing last two rows",
            join(&rows[..rows.len() - 2]).into_bytes(),
            PortableV2ErrorCode::DigestMismatch,
            "data inventory manifest",
        ),
        (
            "a row naming a tag file before the payload rows",
            join(&[&[bagit_row.as_str()], rows.as_slice()].concat()).into_bytes(),
            PortableV2ErrorCode::DigestMismatch,
            "data inventory manifest",
        ),
        (
            "a row naming a tag file instead of a payload row",
            join(&[&[bagit_row.as_str()], &rows[1..]].concat()).into_bytes(),
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
            PortableV2ErrorCode::InvalidStructure,
            "tag manifest line ending",
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
    let expanded = output.path().join("graph.gfproject");
    export(&plan, &bundle, PortableV2Output::Bundle, limits()).unwrap();
    export(&plan, &expanded, PortableV2Output::Expanded, limits()).unwrap();
    let mut bytes = fs::read(&bundle).unwrap();
    let find = |bytes: &[u8], needle: &[u8]| {
        bytes
            .windows(needle.len())
            .position(|window| window == needle)
            .expect("row in bundle")
    };
    // The first inventory row names a component file; flip one digit of its
    // digest. Payload bytes are outside the tar header checksum.
    let at = find(&bytes, b"  data/components/") - 64;
    let flip = |byte: u8| if byte == b'0' { b'1' } else { b'0' };
    bytes[at] = flip(bytes[at]);
    // Re-sign the tampered inventory in the tag manifest, as an attacker
    // would, so only the row check against the entries can refuse it.
    let mut inventory = fs::read(expanded.join("manifest-sha256.txt")).unwrap();
    inventory[0] = flip(inventory[0]);
    let signed = find(&bytes, b"  manifest-sha256.txt\n") - 64;
    bytes[signed..signed + 64]
        .copy_from_slice(hex(sha2::Sha256::digest(&inventory).into()).as_bytes());
    let tampered = output.path().join("tampered.gfpb");
    fs::write(&tampered, bytes).unwrap();
    let error = verify(&tampered, limits()).unwrap_err();
    assert_eq!(error.code, PortableV2ErrorCode::DigestMismatch, "{error}");
    assert!(
        error.to_string().ends_with("data inventory manifest"),
        "{error}"
    );
}

/// Each bundle member as `(path, start, end)` byte offsets, its PAX header
/// included, read independently of the verifier.
fn bundle_members(bundle: &[u8]) -> Vec<(String, usize, usize)> {
    let octal = |field: &[u8]| {
        let text = std::str::from_utf8(field).unwrap();
        usize::from_str_radix(text.trim_matches(['\0', ' ']), 8).unwrap()
    };
    let padded = |length: usize| length.div_ceil(512) * 512;
    let mut members = Vec::new();
    let mut offset = 0;
    let mut start = None;
    let mut pax_path = None;
    loop {
        let header = &bundle[offset..offset + 512];
        if header.iter().all(|byte| *byte == 0) {
            return members;
        }
        let size = octal(&header[124..136]);
        let begin = *start.get_or_insert(offset);
        let body = offset + 512;
        offset = body + padded(size);
        if header[156] == b'x' {
            let records = std::str::from_utf8(&bundle[body..body + size]).unwrap();
            pax_path = records
                .lines()
                .find_map(|record| record.split_once("path=").map(|(_, path)| path.to_owned()));
            continue;
        }
        let name = |range: std::ops::Range<usize>| {
            let bytes = &header[range];
            let end = bytes.iter().position(|b| *b == 0).unwrap_or(bytes.len());
            String::from_utf8(bytes[..end].to_vec()).unwrap()
        };
        let path = pax_path.take().unwrap_or_else(|| match name(345..500) {
            prefix if prefix.is_empty() => name(0..100),
            prefix => format!("{prefix}/{}", name(0..100)),
        });
        members.push((path, begin, offset));
        start = None;
    }
}

/// Rebuild a bundle from `members` of `bundle` in the given order.
fn reassemble(bundle: &[u8], members: &[&(String, usize, usize)]) -> Vec<u8> {
    let mut output = Vec::new();
    for (_, start, end) in members {
        output.extend_from_slice(&bundle[*start..*end]);
    }
    output.extend_from_slice(&[0_u8; 1024]);
    output
}

#[test]
fn a_bundle_with_a_misplaced_or_repeated_inventory_is_refused() {
    let (_project, generation) = generation_with_files(3);
    let plan = plan_complete_portable_v2(&generation, limits()).unwrap();
    let output = tempfile::tempdir().unwrap();
    let bundle = output.path().join("graph.gfpb");
    export(&plan, &bundle, PortableV2Output::Bundle, limits()).unwrap();
    let bytes = fs::read(&bundle).unwrap();
    let members = bundle_members(&bytes);
    let inventory = members
        .iter()
        .position(|(path, _, _)| path == "manifest-sha256.txt")
        .unwrap();
    // Reassembling every member in order reproduces the bundle exactly, so
    // each refusal below is the reordering's.
    let all = members.iter().collect::<Vec<_>>();
    assert_eq!(reassemble(&bytes, &all), bytes);

    // The inventory moved before the payload: it has no preceding rows to
    // match, and the bundle is out of canonical order.
    let mut early = all.clone();
    let moved = early.remove(inventory);
    let first_data = early
        .iter()
        .position(|(path, _, _)| path.starts_with("data/"))
        .unwrap();
    early.insert(first_data, moved);
    // Two inventories, both correct.
    let mut repeated = all.clone();
    repeated.insert(inventory, &members[inventory]);
    for (name, order) in [
        ("inventory before payload", early),
        ("two inventories", repeated),
    ] {
        let package = output.path().join(format!("{name}.gfpb"));
        fs::write(&package, reassemble(&bytes, &order)).unwrap();
        let error = verify(&package, limits()).unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::InvalidStructure, "{name}");
        assert!(
            error
                .to_string()
                .ends_with("bundle entries are not canonical order"),
            "{name}: {error}"
        );
    }
}

#[test]
fn an_oversized_semantic_manifest_is_refused_before_it_is_parsed() {
    let (_project, generation) = generation_with_files(3);
    // A small fixed allowance keeps the oversized manifest small.
    let tight = PortableV2ExportLimits {
        max_manifest_bytes: 4096,
        ..limits()
    };
    let plan = plan_complete_portable_v2(&generation, tight).unwrap();
    let output = tempfile::tempdir().unwrap();
    let expanded = output.path().join("graph.gfproject");
    let bundle = output.path().join("graph.gfpb");
    export(&plan, &expanded, PortableV2Output::Expanded, tight).unwrap();
    export(&plan, &bundle, PortableV2Output::Bundle, tight).unwrap();
    let entries = verify(&expanded, tight).unwrap().entry_count;
    let bound = tight.semantic_manifest_bound(entries);
    assert_eq!(bound, 4096 + entries * 512);
    let refused = |error: PortableV2Error, name: &str| {
        assert_eq!(
            error.code,
            PortableV2ErrorCode::LimitExceeded,
            "{name}: {error}"
        );
        assert!(
            error
                .to_string()
                .ends_with("semantic manifest exceeds its entry-count bound"),
            "{name}: {error}"
        );
    };

    // Bytes that are not JSON at all: parsing them would report invalid
    // JSON, so the limit result shows the manifest was never parsed.
    for (length, parsed) in [(bound + 1, false), (bound, true)] {
        let copy = tempfile::tempdir().unwrap();
        let root = copy.path().join("oversized.gfproject");
        copy_tree(&expanded, &root);
        fs::write(
            root.join("data/graphforge-project.json"),
            vec![b'x'; usize::try_from(length).unwrap()],
        )
        .unwrap();
        let error = verify(&root, tight).unwrap_err();
        if parsed {
            assert_eq!(error.code, PortableV2ErrorCode::InvalidStructure, "{error}");
            assert!(error.to_string().ends_with("invalid JSON"), "{error}");
        } else {
            refused(error, "expanded");
        }
    }

    // The ceiling applies whatever the entry count, to writer and to both
    // readers.
    let manifest = fs::metadata(expanded.join("data/graphforge-project.json"))
        .unwrap()
        .len();
    let ceiling = PortableV2ExportLimits {
        max_semantic_manifest_bytes: manifest - 1,
        ..tight
    };
    refused(verify(&expanded, ceiling).unwrap_err(), "expanded ceiling");
    refused(verify(&bundle, ceiling).unwrap_err(), "bundle ceiling");
    let error = export(
        &plan,
        &output.path().join("ceiling.gfpb"),
        PortableV2Output::Bundle,
        ceiling,
    );
    let error = match error {
        Err(error) => error,
        Ok(_) => plan_complete_portable_v2(&generation, ceiling).unwrap_err(),
    };
    assert_eq!(error.code, PortableV2ErrorCode::LimitExceeded, "{error}");
}

#[test]
fn json_controls_that_do_not_grow_with_entries_keep_their_own_limit() {
    let (_project, generation) = generation_with_files(3);
    let plan = plan_complete_portable_v2(&generation, limits()).unwrap();
    let output = tempfile::tempdir().unwrap();
    let bundle = output.path().join("graph.gfpb");
    export(&plan, &bundle, PortableV2Output::Bundle, limits()).unwrap();
    // The runtime map is about 1 KB; a control limit below it refuses the
    // package even though the semantic manifest ceiling is untouched.
    let controls = PortableV2ExportLimits {
        max_manifest_bytes: 512,
        ..limits()
    };
    assert_eq!(
        controls.max_semantic_manifest_bytes,
        PortableV2ExportLimits::default().max_semantic_manifest_bytes
    );
    assert_eq!(
        verify(&bundle, controls).unwrap_err().code,
        PortableV2ErrorCode::LimitExceeded
    );
}
