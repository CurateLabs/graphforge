//! Fixed-depth content-addressed radix manifest for graph files.

use crate::{GraphFileEntry, GraphFileRole};
use graphforge_core::GfError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

/// Canonical v2 compact-root format identifier.
pub const GRAPH_FILES_V2_FORMAT: &str = "graphforge-graph-files-root";
/// Supported compact-root format version.
pub const GRAPH_FILES_V2_VERSION: u32 = 2;
/// Canonical radix-node format identifier.
pub const GRAPH_MANIFEST_NODE_FORMAT: &str = "graphforge-graph-manifest-radix-node";
/// Supported radix-node format version.
pub const GRAPH_MANIFEST_NODE_VERSION: u32 = 1;
/// Number of SHA-256 nibbles consumed by the fixed-depth trie.
pub const GRAPH_RADIX_DEPTH: u8 = 64;

/// Generation participant root naming one immutable radix root and its totals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphFilesRootV2 {
    /// Contract format identifier.
    pub format: String,
    /// Contract format version.
    pub format_version: u32,
    /// SHA-256 address of the root radix node.
    pub root_node_sha256: String,
    /// Total live logical files.
    pub logical_file_count: u64,
    /// Total live logical payload bytes.
    pub logical_byte_length: u64,
}

/// One immutable, canonically encoded radix node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphManifestNode {
    /// Node contract identifier.
    pub format: String,
    /// Node contract version.
    pub format_version: u32,
    /// SHA-256 nibble depth represented by this node.
    pub depth: u8,
    #[serde(flatten)]
    /// Branch or collision-leaf payload.
    pub kind: GraphManifestNodeKind,
}

/// Canonical radix-node payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GraphManifestNodeKind {
    /// Hex-nibble child references.
    Branch {
        /// Canonically ordered nibble to child-object digest map.
        children: BTreeMap<String, String>,
    },
    /// Terminal exact-path collision bucket.
    Leaf {
        /// Full SHA-256 digest shared by every colliding exact path.
        path_sha256: String,
        /// Exact logical entries ordered by relative path.
        entries: Vec<GraphFileEntry>,
    },
}

/// Admission bounds for resolving an untrusted radix manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphManifestLimits {
    /// Maximum distinct radix objects examined.
    pub max_segments: usize,
    /// Maximum live logical entries resolved.
    pub max_entries: usize,
}
impl Default for GraphManifestLimits {
    fn default() -> Self {
        Self {
            max_segments: 6_500_000,
            max_entries: 100_000,
        }
    }
}

/// Deterministic work evidence from manifest resolution.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GraphManifestResolveEvidence {
    /// Distinct radix nodes examined.
    pub segments_examined: u64,
    /// Terminal logical entries examined.
    pub entries_examined: u64,
}

/// Encode one validated node as canonical JSON-line bytes.
pub fn encode_node(node: &GraphManifestNode) -> Result<Vec<u8>, GfError> {
    validate_node(node)?;
    canonical_line(node, "graph manifest radix node")
}
/// Decode and validate canonical JSON-line node bytes.
pub fn decode_node(bytes: &[u8]) -> Result<GraphManifestNode, GfError> {
    decode_canonical_line(bytes, "graph manifest radix node", validate_node)
}
/// Encode one validated compact root as canonical JSON-line bytes.
pub fn encode_root(root: &GraphFilesRootV2) -> Result<Vec<u8>, GfError> {
    validate_root(root)?;
    canonical_line(root, "graph files v2 root")
}
/// Decode and validate canonical JSON-line compact-root bytes.
pub fn decode_root(bytes: &[u8]) -> Result<GraphFilesRootV2, GfError> {
    decode_canonical_line(bytes, "graph files v2 root", validate_root)
}
#[must_use]
/// Return the lowercase SHA-256 address of exact object bytes.
pub fn object_digest(bytes: &[u8]) -> String {
    hex_digest(Sha256::digest(bytes).into())
}
#[must_use]
/// Hash canonical logical-path UTF-8 bytes for radix routing.
pub fn logical_path_digest(path: &str) -> [u8; 32] {
    Sha256::digest(path.as_bytes()).into()
}
#[must_use]
/// Select one high/low SHA-256 nibble at `depth`.
pub const fn radix_nibble(digest: &[u8; 32], depth: u8) -> u8 {
    let byte = digest[(depth / 2) as usize];
    if depth.is_multiple_of(2) {
        byte >> 4
    } else {
        byte & 15
    }
}
/// Verify declared object length and SHA-256 identity.
pub fn verify_object_bytes(expected: &str, length: u64, bytes: &[u8]) -> Result<(), GfError> {
    validate_digest(expected)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != length {
        return Err(validation("graph object length does not match manifest"));
    }
    if object_digest(bytes) != expected {
        return Err(validation("graph object digest does not match manifest"));
    }
    Ok(())
}

/// Resolve a compact radix root into its authenticated logical inventory.
pub fn resolve_manifest<F>(
    root: &GraphFilesRootV2,
    limits: GraphManifestLimits,
    mut load: F,
) -> Result<(Vec<GraphFileEntry>, GraphManifestResolveEvidence), GfError>
where
    F: FnMut(&str) -> Result<Vec<u8>, GfError>,
{
    validate_root(root)?;
    if limits.max_segments == 0 || limits.max_entries == 0 {
        return Err(validation("graph manifest limits must be positive"));
    }
    let mut stack = vec![(root.root_node_sha256.clone(), 0_u8)];
    let mut visited = BTreeSet::new();
    let mut files = BTreeMap::new();
    let mut evidence = GraphManifestResolveEvidence::default();
    while let Some((digest, expected_depth)) = stack.pop() {
        if visited.len() >= limits.max_segments {
            return Err(validation("graph manifest node limit exceeded"));
        }
        if !visited.insert(digest.clone()) {
            return Err(validation(
                "graph manifest node cycle or duplicate reference detected",
            ));
        }
        let bytes = load(&digest)?;
        if object_digest(&bytes) != digest {
            return Err(validation("graph manifest node digest mismatch"));
        }
        let node = decode_node(&bytes)?;
        if node.depth != expected_depth {
            return Err(validation("graph manifest radix depth mismatch"));
        }
        evidence.segments_examined = evidence.segments_examined.saturating_add(1);
        match node.kind {
            GraphManifestNodeKind::Branch { children } => {
                if expected_depth >= GRAPH_RADIX_DEPTH {
                    return Err(validation("graph manifest branch exceeds radix depth"));
                }
                for (nibble, child) in children.into_iter().rev() {
                    validate_nibble(&nibble)?;
                    stack.push((child, expected_depth + 1));
                }
            }
            GraphManifestNodeKind::Leaf {
                path_sha256,
                entries,
            } => {
                if expected_depth != GRAPH_RADIX_DEPTH {
                    return Err(validation("graph manifest leaf is not terminal"));
                }
                for entry in entries {
                    if hex_digest(logical_path_digest(&entry.relative_path)) != path_sha256 {
                        return Err(validation("graph manifest leaf path digest mismatch"));
                    }
                    evidence.entries_examined = evidence.entries_examined.saturating_add(1);
                    if files.insert(entry.relative_path.clone(), entry).is_some() {
                        return Err(validation("graph manifest contains duplicate logical path"));
                    }
                    if files.len() > limits.max_entries {
                        return Err(validation("graph manifest entry limit exceeded"));
                    }
                }
            }
        }
    }
    let files: Vec<_> = files.into_values().collect();
    let bytes = files.iter().try_fold(0_u64, |n, e| {
        n.checked_add(e.byte_length)
            .ok_or_else(|| validation("graph manifest byte total overflow"))
    })?;
    if u64::try_from(files.len()).unwrap_or(u64::MAX) != root.logical_file_count
        || bytes != root.logical_byte_length
    {
        return Err(validation(
            "graph files v2 root totals do not match resolved manifest",
        ));
    }
    Ok((files, evidence))
}

fn validate_root(root: &GraphFilesRootV2) -> Result<(), GfError> {
    if root.format != GRAPH_FILES_V2_FORMAT || root.format_version != GRAPH_FILES_V2_VERSION {
        return Err(validation("unsupported graph files v2 root contract"));
    }
    validate_digest(&root.root_node_sha256)
}
fn validate_node(node: &GraphManifestNode) -> Result<(), GfError> {
    if node.format != GRAPH_MANIFEST_NODE_FORMAT
        || node.format_version != GRAPH_MANIFEST_NODE_VERSION
    {
        return Err(validation("unsupported graph manifest radix node contract"));
    }
    if node.depth > GRAPH_RADIX_DEPTH {
        return Err(validation("graph manifest radix depth exceeds limit"));
    }
    match &node.kind {
        GraphManifestNodeKind::Branch { children } => {
            if node.depth >= GRAPH_RADIX_DEPTH {
                return Err(validation("terminal graph manifest node must be a leaf"));
            }
            for (nibble, digest) in children {
                validate_nibble(nibble)?;
                validate_digest(digest)?;
            }
        }
        GraphManifestNodeKind::Leaf {
            path_sha256,
            entries,
        } => {
            if node.depth != GRAPH_RADIX_DEPTH || entries.is_empty() {
                return Err(validation("graph manifest leaf shape is invalid"));
            }
            validate_digest(path_sha256)?;
            let mut previous: Option<&str> = None;
            for entry in entries {
                validate_entry(entry)?;
                if previous.is_some_and(|p| p >= entry.relative_path.as_str()) {
                    return Err(validation("graph manifest collision leaf is not canonical"));
                }
                if hex_digest(logical_path_digest(&entry.relative_path)) != *path_sha256 {
                    return Err(validation("graph manifest leaf path digest mismatch"));
                }
                previous = Some(&entry.relative_path);
            }
        }
    }
    Ok(())
}
fn validate_entry(entry: &GraphFileEntry) -> Result<(), GfError> {
    validate_path(&entry.relative_path)?;
    validate_digest(&entry.content_sha256)?;
    match entry.role {
        GraphFileRole::Topology
        | GraphFileRole::Properties
        | GraphFileRole::Index
        | GraphFileRole::Delta
        | GraphFileRole::Catalog
        | GraphFileRole::Other => {}
    }
    Ok(())
}
fn validate_path(value: &str) -> Result<(), GfError> {
    let path = std::path::Path::new(value);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|c| !matches!(c, std::path::Component::Normal(_)))
    {
        return Err(validation("invalid graph manifest relative path"));
    }
    if path
        .components()
        .next()
        .is_some_and(|component| component.as_os_str() == std::ffi::OsStr::new(".graphforge-cache"))
    {
        return Err(validation("derived cache path cannot be authoritative"));
    }
    Ok(())
}
fn validate_digest(value: &str) -> Result<(), GfError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(validation(
            "graph manifest digest must be 64 lowercase hex characters",
        ));
    }
    Ok(())
}
fn validate_nibble(value: &str) -> Result<(), GfError> {
    if value.len() != 1
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(validation(
            "graph manifest child nibble is not canonical lowercase hex",
        ));
    }
    Ok(())
}
fn canonical_line<T: Serialize>(value: &T, label: &str) -> Result<Vec<u8>, GfError> {
    let mut bytes = serde_json::to_vec(value)
        .map_err(|e| validation(format!("failed to encode {label}: {e}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}
fn decode_canonical_line<T, V>(bytes: &[u8], label: &str, validate: V) -> Result<T, GfError>
where
    T: for<'de> Deserialize<'de> + Serialize,
    V: FnOnce(&T) -> Result<(), GfError>,
{
    if !bytes.ends_with(b"\n") || bytes[..bytes.len().saturating_sub(1)].contains(&b'\n') {
        return Err(validation(format!(
            "{label} must be one canonical JSON line"
        )));
    }
    let value: T = serde_json::from_slice(bytes)
        .map_err(|e| validation(format!("invalid {label} JSON: {e}")))?;
    validate(&value)?;
    if canonical_line(&value, label)? != bytes {
        return Err(validation(format!("{label} is not canonical")));
    }
    Ok(value)
}
fn hex_digest(digest: [u8; 32]) -> String {
    use std::fmt::Write as _;
    digest.iter().fold(String::with_capacity(64), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}
fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn branch(depth: u8, children: BTreeMap<String, String>) -> GraphManifestNode {
        GraphManifestNode {
            format: GRAPH_MANIFEST_NODE_FORMAT.into(),
            format_version: GRAPH_MANIFEST_NODE_VERSION,
            depth,
            kind: GraphManifestNodeKind::Branch { children },
        }
    }

    #[test]
    fn unknown_versions_unsafe_paths_and_noncanonical_nibbles_fail_closed() {
        let future = b"{\"format\":\"graphforge-graph-files-root\",\"format_version\":3,\"root_node_sha256\":\"0000000000000000000000000000000000000000000000000000000000000000\",\"logical_file_count\":0,\"logical_byte_length\":0}\n";
        assert!(decode_root(future).is_err());

        let mut children = BTreeMap::new();
        children.insert("A".into(), "0".repeat(64));
        assert!(encode_node(&branch(0, children)).is_err());

        let digest = "0".repeat(64);
        for path in ["../escape", "/absolute", ".graphforge-cache/index"] {
            let node = GraphManifestNode {
                format: GRAPH_MANIFEST_NODE_FORMAT.into(),
                format_version: GRAPH_MANIFEST_NODE_VERSION,
                depth: GRAPH_RADIX_DEPTH,
                kind: GraphManifestNodeKind::Leaf {
                    path_sha256: digest.clone(),
                    entries: vec![GraphFileEntry {
                        relative_path: path.into(),
                        byte_length: 0,
                        content_sha256: digest.clone(),
                        role: GraphFileRole::Other,
                    }],
                },
            };
            assert!(encode_node(&node).is_err());
        }
    }

    #[test]
    fn duplicate_references_depth_mismatch_and_corruption_fail_closed() {
        let child_bytes = encode_node(&branch(1, BTreeMap::new())).unwrap();
        let child_digest = object_digest(&child_bytes);
        let mut children = BTreeMap::new();
        children.insert("0".into(), child_digest.clone());
        children.insert("1".into(), child_digest.clone());
        let root_bytes = encode_node(&branch(0, children)).unwrap();
        let root_digest = object_digest(&root_bytes);
        let root = GraphFilesRootV2 {
            format: GRAPH_FILES_V2_FORMAT.into(),
            format_version: GRAPH_FILES_V2_VERSION,
            root_node_sha256: root_digest.clone(),
            logical_file_count: 0,
            logical_byte_length: 0,
        };
        let load = |digest: &str| {
            if digest == root_digest {
                Ok(root_bytes.clone())
            } else if digest == child_digest {
                Ok(child_bytes.clone())
            } else {
                Err(validation("missing test object"))
            }
        };
        assert!(resolve_manifest(&root, GraphManifestLimits::default(), load).is_err());

        let wrong_depth = encode_node(&branch(1, BTreeMap::new())).unwrap();
        let wrong_digest = object_digest(&wrong_depth);
        let wrong_root = GraphFilesRootV2 {
            root_node_sha256: wrong_digest,
            ..root.clone()
        };
        assert!(
            resolve_manifest(&wrong_root, GraphManifestLimits::default(), |_| {
                Ok(wrong_depth.clone())
            })
            .is_err()
        );

        assert!(
            resolve_manifest(&root, GraphManifestLimits::default(), |_| {
                Ok(b"corrupt\n".to_vec())
            })
            .is_err()
        );
    }
}
