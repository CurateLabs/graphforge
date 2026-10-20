//! What importing a package would write beside its target, read from its
//! admitted controls alone (#1770).

use super::{AtomicBool, Manifest, Path, PortableError, PortableLimits, preflight};

/// What importing a package would write beside its target, read from its
/// admitted controls alone: nothing is extracted or mutated.
///
/// # Errors
/// Returns the package error the controls' admission refuses with.
pub fn portable_import_footprint(
    source: impl AsRef<Path>,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
) -> Result<graphforge_core::portable::PortableImportFootprint, PortableError> {
    preflight(source.as_ref(), limits, cancelled)
}

pub(super) fn footprint_of_manifest(
    manifest: &Manifest,
    limits: PortableLimits,
) -> graphforge_core::portable::PortableImportFootprint {
    let mut footprint = graphforge_core::portable::PortableImportFootprint {
        payload_bytes: 0,
        research_bytes: 0,
        generated_bytes: 0,
        entries: 0,
    };
    let mut authored_bytes = 0_u64;
    for component in &manifest.components {
        for file in &component.files {
            footprint.payload_bytes = footprint.payload_bytes.saturating_add(file.length);
            footprint.entries = footprint.entries.saturating_add(1);
            if component.kind == "research" {
                footprint.research_bytes = footprint.research_bytes.saturating_add(file.length);
            }
            if super::semantic_validation::is_composition_input(
                &component.kind,
                &component.participant_id,
            ) {
                authored_bytes = authored_bytes.saturating_add(file.length);
            }
        }
    }
    // An ontology composition makes the import generate two controls, the
    // staged candidate and the composition authority. Each re-encodes every
    // authored module and bridge document (measured at one times their bytes)
    // and the writer
    // refuses one past the manifest bound.
    if manifest
        .requirements
        .capabilities
        .iter()
        .any(|capability| capability == "ontology-composition@1")
    {
        footprint.generated_bytes = 2 * authored_bytes.min(limits.max_manifest_bytes);
        footprint.entries = footprint.entries.saturating_add(2);
    }
    footprint
}
