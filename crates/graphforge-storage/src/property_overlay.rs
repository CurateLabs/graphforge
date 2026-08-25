//! Authenticated immutable property snapshot overlays (#940).
//!
//! A fragment row is a complete property snapshot for one UUID. Fragment
//! authority is the numeric `(generation, ordinal)` encoded in its canonical
//! filename; directory order and mtimes never select a winner.

use std::fs;
use std::path::{Path, PathBuf};

use graphforge_core::GfError;

/// On-disk property overlay format marker.
pub const PROPERTY_OVERLAY_FORMAT: &str = "full-snapshot-v1";
/// Schema metadata key carrying [`PROPERTY_OVERLAY_FORMAT`].
pub const PROPERTY_OVERLAY_FORMAT_KEY: &str = "graphforge.property_overlay";
/// Reserved non-user column marking whole-row deletion.
pub const PROPERTY_TOMBSTONE_FIELD: &str = "__gf_property_tombstone";

/// Node and edge property namespaces are disjoint authorities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyRouteKind {
    /// `properties/<route>/...`
    Node,
    /// `edge_properties/<route>/...`
    Edge,
}

impl PropertyRouteKind {
    fn subdir(self) -> &'static str {
        match self {
            Self::Node => "properties",
            Self::Edge => "edge_properties",
        }
    }
}

/// Canonical newest-wins authority of one immutable fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PropertyFragmentId {
    /// Committed topology generation associated with the write window.
    pub generation: u64,
    /// Route-local ordinal within that generation.
    pub ordinal: u64,
}

impl PropertyFragmentId {
    /// Parse exactly `GGGGGGGGGGGGGGGGGGGG-OOOOOOOOOOOOOOOOOOOO.parquet`.
    pub fn parse(name: &str) -> Result<Self, GfError> {
        let body = name
            .strip_suffix(".parquet")
            .ok_or_else(|| corrupt("property fragment name lacks the canonical .parquet suffix"))?;
        let (generation, ordinal) = body.split_once('-').ok_or_else(|| {
            corrupt("property fragment name lacks canonical generation and ordinal")
        })?;
        if generation.len() != 20
            || ordinal.len() != 20
            || !generation.bytes().all(|byte| byte.is_ascii_digit())
            || !ordinal.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(corrupt("property fragment identity is not canonical"));
        }
        let id = Self {
            generation: generation
                .parse()
                .map_err(|_| corrupt("property fragment generation overflows u64"))?,
            ordinal: ordinal
                .parse()
                .map_err(|_| corrupt("property fragment ordinal overflows u64"))?,
        };
        if id.file_name() != name {
            return Err(corrupt("property fragment identity is not canonical"));
        }
        Ok(id)
    }

    /// Render the sole accepted filename representation.
    #[must_use]
    pub fn file_name(self) -> String {
        format!("{:020}-{:020}.parquet", self.generation, self.ordinal)
    }
}

/// Strictly admitted immutable property fragment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyFragment {
    /// Numeric authority parsed from the filename.
    pub id: PropertyFragmentId,
    /// Canonical fragment path.
    pub path: PathBuf,
}

/// Exact work performed by one property overlay operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PropertyOverlayMetrics {
    /// Physical snapshot rows decoded.
    pub physical_rows: u64,
    /// Authenticated fragment bytes read.
    pub physical_bytes: u64,
    /// Bounded input blocks read.
    pub blocks_read: u64,
    /// Canonical fragments considered for authority.
    pub fragments_considered: u64,
    /// Live newest snapshot rows emitted.
    pub logical_rows: u64,
    /// Older rows suppressed by newer snapshots or tombstones.
    pub shadowed_rows: u64,
    /// Newest tombstone rows observed.
    pub tombstones: u64,
    /// Bytes written to external merge runs.
    pub spill_bytes: u64,
    /// External merge runs written.
    pub spill_runs: u64,
    /// Bounded fan-in merge passes performed.
    pub merge_passes: u64,
    /// Maximum decoded rows retained at once.
    pub peak_buffered_rows: u64,
    /// Maximum charged decoded bytes retained at once.
    pub peak_buffered_bytes: u64,
    /// Random/per-record seeks are forbidden and remain zero.
    pub per_record_seeks: u64,
}

/// Enumerate a route's immutable fragments in oldest-to-newest authority order.
///
/// The legacy flat file is admitted only as `(0, 0)`. Every entry in the
/// immutable directory must be a canonical regular file; near misses fail
/// closed instead of disappearing from the authority set.
pub fn enumerate_property_fragments(
    project: &Path,
    kind: PropertyRouteKind,
    route: &str,
) -> Result<Vec<PropertyFragment>, GfError> {
    validate_route(route)?;
    let root = project.join(kind.subdir());
    let mut fragments = Vec::new();
    let legacy = root.join(format!("{route}.parquet"));
    match fs::symlink_metadata(&legacy) {
        Ok(metadata) if metadata.file_type().is_file() => fragments.push(PropertyFragment {
            id: PropertyFragmentId {
                generation: 0,
                ordinal: 0,
            },
            path: legacy,
        }),
        Ok(_) => return Err(corrupt("legacy property fragment is not a regular file")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(GfError::Storage(error.to_string())),
    }
    let directory = root.join(route);
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(fragments),
        Err(error) => return Err(GfError::Storage(error.to_string())),
    };
    for entry in entries {
        let entry = entry.map_err(|error| GfError::Storage(error.to_string()))?;
        let metadata = fs::symlink_metadata(entry.path())
            .map_err(|error| GfError::Storage(error.to_string()))?;
        if !metadata.file_type().is_file() {
            return Err(corrupt("property fragment is not a regular file"));
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| corrupt("property fragment identity is not canonical UTF-8"))?;
        fragments.push(PropertyFragment {
            id: PropertyFragmentId::parse(&name)?,
            path: entry.path(),
        });
    }
    fragments.sort_unstable_by_key(|fragment| fragment.id);
    if fragments.windows(2).any(|pair| pair[0].id == pair[1].id) {
        return Err(corrupt("duplicate property fragment identity"));
    }
    Ok(fragments)
}

fn validate_route(route: &str) -> Result<(), GfError> {
    if route.is_empty() || route == "." || route == ".." || route.contains(['/', '\0']) {
        return Err(corrupt("property route is not canonical"));
    }
    Ok(())
}

fn corrupt(message: &str) -> GfError {
    GfError::Storage(format!("property overlay: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn fragment_identity_is_numeric_canonical_and_total() {
        let id = PropertyFragmentId {
            generation: 2,
            ordinal: 10,
        };
        assert_eq!(PropertyFragmentId::parse(&id.file_name()).unwrap(), id);
        for invalid in [
            "2-10.parquet",
            "00000000000000000002-00000000000000000010.PARQUET",
            "00000000000000000002-00000000000000000010.parquet.tmp",
            "00000000000000000002-00000000000000000010-0.parquet",
        ] {
            assert!(PropertyFragmentId::parse(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn enumeration_uses_numeric_authority_and_rejects_near_misses() {
        let dir = TempDir::new().unwrap();
        let route = dir.path().join("properties/Person");
        fs::create_dir_all(&route).unwrap();
        for id in [
            PropertyFragmentId {
                generation: 10,
                ordinal: 0,
            },
            PropertyFragmentId {
                generation: 2,
                ordinal: 0,
            },
        ] {
            fs::write(route.join(id.file_name()), b"x").unwrap();
        }
        let found =
            enumerate_property_fragments(dir.path(), PropertyRouteKind::Node, "Person").unwrap();
        assert_eq!(found[0].id.generation, 2);
        assert_eq!(found[1].id.generation, 10);
        fs::write(route.join("junk.parquet"), b"x").unwrap();
        assert!(
            enumerate_property_fragments(dir.path(), PropertyRouteKind::Node, "Person").is_err()
        );
    }
}
