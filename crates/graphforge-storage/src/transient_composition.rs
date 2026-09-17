//! Path-derived composition of the transient work-root high-water mark.
//!
//! # Why this exists
//! [`StorageAllocationLifecycle`](crate::StorageAllocationLifecycle) already
//! tracks an exact high-water mark over the identity union of every file a
//! lifecycle run has open. That mark is the number scale admission gates on,
//! and until now it was a bare scalar: nothing recorded *what* those bytes
//! were. The retained categories in [`crate::ArtifactCategory`] cannot answer
//! it either, because they describe the artifacts a committed project keeps,
//! not the working set an ingest holds while it runs — a single
//! [`crate::ArtifactCategory::ConstructionStaging`] row stands for the whole
//! construction root.
//!
//! This module supplies the missing axis: a total, disjoint classification of
//! every path a lifecycle run can allocate, so the composition recorded at the
//! instant of the peak sums to the peak exactly rather than explaining a
//! fraction of it.
//!
//! # Contract
//! [`classify_allocation_path`] is a pure function of the path. It is total:
//! every absolute path maps to exactly one [`TransientComponent`], with
//! [`TransientComponent::Unclassified`] reserved for names the grammar does
//! not recognise. Unclassified bytes are a defect, not a bucket, and the
//! composition assertions in the scale tests require them to stay at zero.
//!
//! # Identity sharing
//! One native identity can be reachable through several owners in different
//! components — a content-addressed object hardlinked out of the construction
//! root is the ordinary case. The union counts such an identity once, and this
//! module attributes it to the component of the owner that **first** installed
//! it. That answers "where did these bytes first become resident", which is
//! the question a peak-reduction argument needs.

use serde::{Deserialize, Serialize};
use std::path::Path;

/// One disjoint component of the work root, derived from an allocation path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransientComponent {
    /// Parquet the operator already had, outside any project root. The import
    /// reads these bytes; it never created them.
    SourceParquet,
    /// Any other file outside a project root: portable packages, exported
    /// query results, operator scratch.
    ExternalArtifact,
    /// The byte-for-byte copy `register-parquet` takes into
    /// `<project>/import-sessions/<id>/sources/`. Deliberate durability
    /// policy: the session owns its bytes from registration.
    RegisteredSourceCopy,
    /// Import-session manifests and controls.
    ImportSessionControl,
    /// Staged decode output, `chunk-<sequence>-<kind>.parquet`.
    StagedChunkParquet,
    /// Staged fixed-width runs beside each staged chunk.
    StagedChunkRun,
    /// Whole-run copies taken when shaping consumes a staged chunk
    /// (`merge-unified-*`, `merge-<kind>-source-*`).
    MergeSourceCopy,
    /// External merge-tree levels (`merge-<kind>-l<level>-g<group>.run`,
    /// `merge-rows-*.parquet`).
    MergeTreeLevel,
    /// Shaped canonical output (`shaped-*`).
    ShapedOutput,
    /// Canonical encoded artifacts staged in the construction root before
    /// publication installs them.
    EncodedWorkspace,
    /// Construction intents, receipts, checkpoints, pointers and temporaries.
    ConstructionControl,
    /// Installed content-addressed objects, `<project>/graph-objects/sha256/`.
    ContentAddressedStore,
    /// The staging copy publication takes into `graph-objects/tmp/` before it
    /// seals and links an object into its digest bucket.
    ContentAddressedStaging,
    /// Published generation manifests and participants.
    PublishedGeneration,
    /// Project-level control: `FORMAT`, `CURRENT`, locks, transactions.
    ProjectControl,
    /// A path the grammar does not recognise. Must stay at zero.
    Unclassified,
}

impl TransientComponent {
    /// Canonical component inventory, including components with zero bytes.
    pub const ALL: [Self; 16] = [
        Self::SourceParquet,
        Self::ExternalArtifact,
        Self::RegisteredSourceCopy,
        Self::ImportSessionControl,
        Self::StagedChunkParquet,
        Self::StagedChunkRun,
        Self::MergeSourceCopy,
        Self::MergeTreeLevel,
        Self::ShapedOutput,
        Self::EncodedWorkspace,
        Self::ConstructionControl,
        Self::ContentAddressedStore,
        Self::ContentAddressedStaging,
        Self::PublishedGeneration,
        Self::ProjectControl,
        Self::Unclassified,
    ];

    /// Stable lower-case tag used to carry the component inside an opaque
    /// allocation owner key. Identity-free by construction: a component name
    /// names a kind of file, never a path or a native identity.
    #[must_use]
    pub const fn tag(self) -> &'static str {
        match self {
            Self::SourceParquet => "source-parquet",
            Self::ExternalArtifact => "external",
            Self::RegisteredSourceCopy => "registered-source-copy",
            Self::ImportSessionControl => "import-session-control",
            Self::StagedChunkParquet => "staged-chunk-parquet",
            Self::StagedChunkRun => "staged-chunk-run",
            Self::MergeSourceCopy => "merge-source-copy",
            Self::MergeTreeLevel => "merge-tree-level",
            Self::ShapedOutput => "shaped-output",
            Self::EncodedWorkspace => "encoded-workspace",
            Self::ConstructionControl => "construction-control",
            Self::ContentAddressedStore => "content-addressed-store",
            Self::ContentAddressedStaging => "content-addressed-staging",
            Self::PublishedGeneration => "published-generation",
            Self::ProjectControl => "project-control",
            Self::Unclassified => "unclassified",
        }
    }

    /// Recover a component from [`Self::tag`].
    #[must_use]
    pub fn from_tag(tag: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|value| value.tag() == tag)
    }
}

const CONSTRUCTION_ROOT: &str = ".graphforge-construction";
const IMPORT_SESSIONS: &str = "import-sessions";

/// Classify one absolute allocation path into exactly one component.
///
/// The classification is structural: a marker directory selects the region of
/// the work root, and the file-name grammar selects the component within it.
#[must_use]
#[allow(
    clippy::case_sensitive_file_extension_comparisons,
    reason = "these names are produced by this crate's own format strings and are exactly lower case; an ASCII-insensitive match would accept names the writers never emit"
)]
pub fn classify_allocation_path(path: &Path) -> TransientComponent {
    let parts = path
        .iter()
        .map(|part| part.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let name = parts.last().map(String::as_str).unwrap_or_default();
    for (index, part) in parts.iter().enumerate().rev() {
        match part.as_str() {
            CONSTRUCTION_ROOT => {
                // <root>/.graphforge-construction/<session>/<tail...>
                return classify_construction(parts.get(index + 2..).unwrap_or_default());
            }
            IMPORT_SESSIONS => {
                // <root>/import-sessions/<session>/<tail...>
                let tail = parts.get(index + 2..).unwrap_or_default();
                return if tail.first().is_some_and(|first| first == "sources") {
                    TransientComponent::RegisteredSourceCopy
                } else {
                    TransientComponent::ImportSessionControl
                };
            }
            crate::graph_object_store::GRAPH_OBJECTS_DIR => {
                return if parts.get(index + 1).is_some_and(|next| next == "tmp") {
                    TransientComponent::ContentAddressedStaging
                } else {
                    TransientComponent::ContentAddressedStore
                };
            }
            "generations" => return TransientComponent::PublishedGeneration,
            "transactions" | "locks" => return TransientComponent::ProjectControl,
            _ => {}
        }
    }
    match name {
        "FORMAT" | "CURRENT" => TransientComponent::ProjectControl,
        _ if name.ends_with(".lock") => TransientComponent::ProjectControl,
        _ if name.ends_with(".parquet") => TransientComponent::SourceParquet,
        _ => TransientComponent::ExternalArtifact,
    }
}

/// Classify a path relative to one construction session root.
#[allow(
    clippy::case_sensitive_file_extension_comparisons,
    reason = "these names are produced by this crate's own format strings and are exactly lower case; an ASCII-insensitive match would accept names the writers never emit"
)]
fn classify_construction(tail: &[String]) -> TransientComponent {
    let Some(name) = tail.last() else {
        return TransientComponent::ConstructionControl;
    };
    // Canonical encoded artifacts are the only construction output written
    // into subdirectories (`topology/…`, `properties/…`, `adjacency/…`).
    if tail.len() > 1 {
        return TransientComponent::EncodedWorkspace;
    }
    // Replaceable temporaries are `.<target>-<nonce>.tmp` for controls and
    // `.artifact-<target>-<nonce>.tmp` for payloads. Attribute a temporary to
    // whatever it is staging, so a rename boundary never hides a payload.
    let name = name
        .strip_prefix('.')
        .and_then(|body| body.strip_suffix(".tmp"))
        .map_or(name.as_str(), |body| {
            let target = body.rsplit_once('-').map_or(body, |(target, _)| target);
            target.strip_prefix("artifact-").unwrap_or(target)
        });
    if name.starts_with("chunk-") {
        return if name.ends_with(".parquet") {
            TransientComponent::StagedChunkParquet
        } else if name.ends_with(".run") {
            TransientComponent::StagedChunkRun
        } else {
            TransientComponent::ConstructionControl
        };
    }
    if name.starts_with("shaped-") {
        return TransientComponent::ShapedOutput;
    }
    if let Some(body) = name.strip_prefix("merge-") {
        for prefix in [
            "unified-",
            "node-source-",
            "edge-source-",
            "endpoint-source-",
            "resolved-source-",
        ] {
            if body.starts_with(prefix) {
                return TransientComponent::MergeSourceCopy;
            }
        }
        return TransientComponent::MergeTreeLevel;
    }
    // `session.lock`, `checkpoint.json`, `intent.json`, `shape-intent.json`,
    // `publication-{intent,receipt}.json`, `receipt-<sequence>.json`,
    // `key-<digest>.json`, `shape-receipt-<digest>.json`.
    if name.ends_with(".json") || name.ends_with(".lock") {
        return TransientComponent::ConstructionControl;
    }
    TransientComponent::Unclassified
}

#[cfg(test)]
mod tests {
    use super::{TransientComponent as C, classify_allocation_path as classify};
    use std::path::Path;

    #[test]
    fn every_component_has_a_unique_round_tripping_tag() {
        let mut tags = std::collections::BTreeSet::new();
        for component in C::ALL {
            assert!(tags.insert(component.tag()), "duplicate component tag");
            assert_eq!(C::from_tag(component.tag()), Some(component));
        }
        assert_eq!(C::from_tag("not-a-component"), None);
    }

    #[test]
    fn construction_names_map_to_disjoint_components() {
        let root = "/w/project/.graphforge-construction/session";
        for (name, expected) in [
            (
                "chunk-00000000000000000001-nodes.parquet",
                C::StagedChunkParquet,
            ),
            (
                "chunk-00000000000000000001-nodes.identities.run",
                C::StagedChunkRun,
            ),
            (
                "chunk-00000000000000000001-nodes.endpoints.run",
                C::StagedChunkRun,
            ),
            (
                "chunk-00000000000000000001-nodes.edge-details.run",
                C::StagedChunkRun,
            ),
            ("merge-unified-00000000000000000001.run", C::MergeSourceCopy),
            (
                "merge-node-source-00000000000000000001.run",
                C::MergeSourceCopy,
            ),
            (
                "merge-endpoint-source-00000000000000000001.run",
                C::MergeSourceCopy,
            ),
            (
                "merge-resolved-source-00000000000000000001.run",
                C::MergeSourceCopy,
            ),
            ("merge-identities-l001-g00000002.run", C::MergeTreeLevel),
            ("merge-edge-details-l002-g00000003.run", C::MergeTreeLevel),
            (
                "merge-rows-0123456789abcdef-l001-g00000000000000000002.parquet",
                C::MergeTreeLevel,
            ),
            ("shaped-identities.run", C::ShapedOutput),
            ("shaped-runtime-catalog.parquet", C::ShapedOutput),
            ("shaped-rows-0-0123.parquet", C::ShapedOutput),
            ("receipt-00000000000000000001.json", C::ConstructionControl),
            ("shape-receipt-abc.json", C::ConstructionControl),
            ("intent.json", C::ConstructionControl),
            ("checkpoint.json", C::ConstructionControl),
            ("shape-intent.json", C::ConstructionControl),
            ("publication-intent.json", C::ConstructionControl),
            ("publication-receipt.json", C::ConstructionControl),
            (
                "key-0000000000000000000000000000000000000000000000000000000000000000.json",
                C::ConstructionControl,
            ),
            ("session.lock", C::ConstructionControl),
        ] {
            assert_eq!(classify(&Path::new(root).join(name)), expected, "{name}");
        }
    }

    #[test]
    fn a_temporary_is_attributed_to_the_artifact_it_stages() {
        let root = Path::new("/w/project/.graphforge-construction/session");
        assert_eq!(
            classify(&root.join(".merge-unified-00000000000000000001.run-9f2c.tmp")),
            C::MergeSourceCopy
        );
        assert_eq!(
            classify(&root.join(".shaped-identities.run-9f2c.tmp")),
            C::ShapedOutput
        );
        assert_eq!(
            classify(&root.join(".artifact-chunk-00000000000000000001-edge.parquet-9f2c.tmp")),
            C::StagedChunkParquet
        );
        assert_eq!(
            classify(&root.join(".artifact-merge-identities-l001-g00000002.run-9f2c.tmp")),
            C::MergeTreeLevel
        );
        assert_eq!(
            classify(&root.join(".checkpoint.json-9f2c.tmp")),
            C::ConstructionControl
        );
    }

    #[test]
    fn encoded_artifacts_are_separated_from_staging() {
        assert_eq!(
            classify(Path::new(
                "/w/p/.graphforge-construction/s/topology/nodes/00000000000000000000-00000000000000000063.parquet"
            )),
            C::EncodedWorkspace
        );
    }

    #[test]
    fn project_regions_are_separated_from_each_other() {
        for (path, expected) in [
            (
                "/w/p/import-sessions/abc/sources/edges.parquet",
                C::RegisteredSourceCopy,
            ),
            (
                "/w/p/import-sessions/abc/manifest.json",
                C::ImportSessionControl,
            ),
            ("/w/p/graph-objects/ab/cd/abcdef", C::ContentAddressedStore),
            ("/w/p/graph-objects/tmp/9f2c", C::ContentAddressedStaging),
            (
                "/w/p/graph-objects/active/9f2c.lock",
                C::ContentAddressedStore,
            ),
            (
                "/w/p/generations/0198/manifest.json",
                C::PublishedGeneration,
            ),
            ("/w/p/transactions/receipt.json", C::ProjectControl),
            ("/w/p/locks/write.lock", C::ProjectControl),
            ("/w/p/FORMAT", C::ProjectControl),
            ("/w/p/CURRENT", C::ProjectControl),
            ("/w/inputs/edges.parquet", C::SourceParquet),
            ("/w/exports/package.gfp", C::ExternalArtifact),
        ] {
            assert_eq!(classify(Path::new(path)), expected, "{path}");
        }
    }

    #[test]
    fn an_unknown_construction_name_is_refused_rather_than_absorbed() {
        assert_eq!(
            classify(Path::new("/w/p/.graphforge-construction/s/mystery")),
            C::Unclassified
        );
    }
}
