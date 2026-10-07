//! Bundle entries longer than the ustar `size` field (#900).
//!
//! The S25 Graph500 project holds a 9.28 GiB graph object; the 11-digit octal
//! ustar `size` field stops at 8 GiB - 1. Writing gigabytes in a unit test is
//! not an option, so these tests lower the ustar limit on the test thread
//! through `pax::test_seam` and exercise the same writer and reader branches
//! a real oversized entry takes.

use super::*;
use crate::project_portable_v2::pax::{self, test_seam::lower_ustar_size_limit};
use crate::{PortableV2Report, PortableV2Representation};

/// One regular bundle entry as its headers encode it.
struct BundleEntry {
    path: String,
    ustar_size_field: u64,
    pax_size: Option<u64>,
    length: u64,
}

fn octal(field: &[u8]) -> u64 {
    let text = std::str::from_utf8(field).unwrap();
    u64::from_str_radix(text.trim_matches(['\0', ' ']), 8).unwrap()
}

fn padded(length: u64) -> usize {
    usize::try_from(length.div_ceil(512) * 512).unwrap()
}

/// Walk a canonical bundle independently of the verifier under test.
fn bundle_entries(bundle: &[u8]) -> Vec<BundleEntry> {
    let mut entries = Vec::new();
    let mut offset = 0;
    let mut pending: Option<pax::PaxHeader> = None;
    loop {
        let header = &bundle[offset..offset + 512];
        if header.iter().all(|byte| *byte == 0) {
            return entries;
        }
        let field = octal(&header[124..136]);
        offset += 512;
        if header[156] == b'x' {
            let data = &bundle[offset..offset + usize::try_from(field).unwrap()];
            pending = Some(pax::parse(std::str::from_utf8(data).unwrap()).unwrap());
            offset += padded(field);
            continue;
        }
        let name = |range: std::ops::Range<usize>| {
            let bytes = &header[range];
            let end = bytes.iter().position(|b| *b == 0).unwrap_or(bytes.len());
            String::from_utf8(bytes[..end].to_vec()).unwrap()
        };
        let records = pending.take();
        let path = records.as_ref().map_or_else(
            || match name(345..500) {
                prefix if prefix.is_empty() => name(0..100),
                prefix => format!("{prefix}/{}", name(0..100)),
            },
            |records| records.path.clone(),
        );
        let pax_size = records.and_then(|records| records.size);
        let length = pax_size.unwrap_or(field);
        entries.push(BundleEntry {
            path,
            ustar_size_field: field,
            pax_size,
            length,
        });
        offset += padded(length);
    }
}

fn export(plan: &PortableV2ExportPlan, destination: &Path, output: PortableV2Output) {
    export_complete_portable_v2(
        plan,
        destination,
        output,
        PortableV2ExportLimits::default(),
        &AtomicBool::new(false),
        |_| {},
    )
    .unwrap();
}

fn import(source: &Path, target: &Path, generation: &ResolvedProjectGeneration) {
    let supported = generation
        .capabilities()
        .into_iter()
        .map(|capability| crate::ProjectCapability {
            capability_id: capability.capability_id,
            capability_version: capability.capability_version,
        })
        .collect::<Vec<_>>();
    crate::import_complete_portable_v2(
        source,
        target,
        Uuid::new_v4(),
        Uuid::new_v4(),
        &supported,
        PortableV2ExportLimits::default(),
        None,
    )
    .unwrap();
}

fn verify(bundle: &Path) -> Result<PortableV2Report, PortableV2Error> {
    crate::verify_portable_v2(
        bundle,
        crate::PortableV2Mode::Full,
        PortableV2ExportLimits::default(),
        None,
    )
}

#[test]
fn entries_over_the_ustar_size_field_round_trip_through_pax_size_records() {
    const LIMIT: u64 = 64;
    let (_project, generation) = graph_generation();
    let plan = plan_complete_portable_v2(&generation, PortableV2ExportLimits::default()).unwrap();
    let output = tempfile::tempdir().unwrap();
    let expanded = output.path().join("graph.gfproject");
    let reference = output.path().join("reference.gfpb");
    let oversized = output.path().join("oversized.gfpb");
    export(&plan, &expanded, PortableV2Output::Expanded);
    export(&plan, &reference, PortableV2Output::Bundle);

    // At the production limit every entry fits the ustar field: no size record.
    let reference_entries = bundle_entries(&fs::read(&reference).unwrap());
    assert!(
        reference_entries
            .iter()
            .all(|entry| entry.pax_size.is_none() && entry.ustar_size_field == entry.length)
    );

    let limit = lower_ustar_size_limit(LIMIT);
    export(&plan, &oversized, PortableV2Output::Bundle);
    let entries = bundle_entries(&fs::read(&oversized).unwrap());
    let (large, small): (Vec<_>, Vec<_>) = entries.iter().partition(|e| e.length > LIMIT);
    assert!(large.len() >= 3, "the fixture has entries above the limit");
    assert!(!small.is_empty(), "the fixture has entries at or below it");
    for entry in &large {
        assert_eq!(entry.ustar_size_field, 0, "{}", entry.path);
        assert_eq!(entry.pax_size, Some(entry.length), "{}", entry.path);
    }
    for entry in &small {
        assert_eq!(entry.pax_size, None, "{}", entry.path);
        assert_eq!(entry.ustar_size_field, entry.length, "{}", entry.path);
    }
    assert!(
        large
            .iter()
            .any(|entry| entry.path.starts_with("data/components/graph-data/")),
        "graph payload entries take the PAX size path"
    );
    // Only the transport changes: the same paths and lengths, in order.
    assert_eq!(
        entries
            .iter()
            .map(|e| (e.path.as_str(), e.length))
            .collect::<Vec<_>>(),
        reference_entries
            .iter()
            .map(|e| (e.path.as_str(), e.length))
            .collect::<Vec<_>>()
    );

    // Verify, materialize and import the PAX-size bundle.
    let report = verify(&oversized).unwrap();
    let expanded_report = verify(&expanded).unwrap();
    assert_eq!(report.package_digest, expanded_report.package_digest);
    assert_eq!(report.representation, PortableV2Representation::Bundle);
    assert_eq!(report.entry_count, expanded_report.entry_count);
    assert_eq!(report.payload_bytes, expanded_report.payload_bytes);
    let oversized_stage = output.path().join("oversized-stage");
    let expanded_stage = output.path().join("expanded-stage");
    crate::materialize_verified_portable_v2(
        &oversized,
        &oversized_stage,
        PortableV2ExportLimits::default(),
        None,
    )
    .unwrap();
    crate::materialize_verified_portable_v2(
        &expanded,
        &expanded_stage,
        PortableV2ExportLimits::default(),
        None,
    )
    .unwrap();
    assert_eq!(tree_bytes(&oversized_stage), tree_bytes(&expanded_stage));
    let oversized_target = output.path().join("oversized-project");
    let expanded_target = output.path().join("expanded-project");
    import(&oversized, &oversized_target, &generation);
    import(&expanded, &expanded_target, &generation);

    // Readers refuse the other encoding at each limit rather than misread it:
    // a ustar size above the limit needs a size record, and a size record for
    // a length the ustar field can carry is not canonical.
    assert_eq!(
        verify(&reference).unwrap_err().code,
        PortableV2ErrorCode::InvalidStructure
    );
    drop(limit);
    assert_eq!(
        verify(&oversized).unwrap_err().code,
        PortableV2ErrorCode::InvalidStructure
    );
    assert_eq!(
        verify(&reference).unwrap().package_digest,
        report.package_digest
    );

    // Byte-identical content after reopening both imports.
    let imported = crate::resolve_project_generation(&oversized_target).unwrap();
    let baseline = crate::resolve_project_generation(&expanded_target).unwrap();
    assert_eq!(
        imported.graph_files_inventory().unwrap(),
        baseline.graph_files_inventory().unwrap()
    );
    assert_eq!(
        imported.participant_snapshots().unwrap(),
        baseline.participant_snapshots().unwrap()
    );
    assert_eq!(
        tree_bytes(&imported.graph_tree_root()),
        tree_bytes(&baseline.graph_tree_root())
    );
    assert!(!tree_bytes(&imported.graph_tree_root()).is_empty());
}
