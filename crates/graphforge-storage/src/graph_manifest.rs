//! Canonical content-addressed Patricia manifest for graph files.

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
pub const GRAPH_MANIFEST_NODE_VERSION: u32 = 2;
/// Number of SHA-256 nibbles consumed by the Patricia trie.
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
    /// Lowercase hex path compressed between `depth` and this node's payload.
    pub prefix: String,
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
#[allow(clippy::struct_field_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphManifestLimits {
    /// Maximum distinct radix objects examined.
    pub max_segments: usize,
    /// Maximum live logical entries resolved.
    pub max_entries: usize,
    /// Maximum aggregate encoded bytes decoded across all radix objects.
    pub max_decoded_bytes: u64,
    /// Maximum aggregate node, child-reference, and entry work admitted.
    pub max_work_units: u64,
}
impl Default for GraphManifestLimits {
    fn default() -> Self {
        Self {
            // A non-empty canonical Patricia tree has at most 2F-1 nodes for
            // F distinct path digests. The empty inventory has one root node.
            max_segments: 200_000,
            max_entries: 100_000,
            max_decoded_bytes: 1024 * 1024 * 1024,
            max_work_units: 500_000,
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
    /// Aggregate encoded bytes admitted for decoding.
    pub decoded_bytes: u64,
    /// Aggregate node, child-reference, and entry work performed.
    pub work_units: u64,
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
#[allow(clippy::too_many_lines)]
pub fn resolve_manifest<F>(
    root: &GraphFilesRootV2,
    limits: GraphManifestLimits,
    mut load: F,
) -> Result<(Vec<GraphFileEntry>, GraphManifestResolveEvidence), GfError>
where
    F: FnMut(&str) -> Result<Vec<u8>, GfError>,
{
    validate_root(root)?;
    if limits.max_segments == 0
        || limits.max_entries == 0
        || limits.max_decoded_bytes == 0
        || limits.max_work_units == 0
    {
        return Err(validation("graph manifest limits must be positive"));
    }
    if root.logical_file_count > u64::try_from(limits.max_entries).unwrap_or(u64::MAX) {
        return Err(validation("graph manifest declared entry limit exceeded"));
    }
    let structural_node_bound = if root.logical_file_count == 0 {
        1
    } else {
        root.logical_file_count
            .checked_mul(2)
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| validation("graph manifest structural bound overflow"))?
    };
    let mut stack = vec![(root.root_node_sha256.clone(), 0_u8, String::new())];
    let mut visited = BTreeSet::new();
    let mut files = BTreeMap::new();
    let mut evidence = GraphManifestResolveEvidence::default();
    while let Some((digest, expected_depth, route)) = stack.pop() {
        if visited.len() >= limits.max_segments {
            return Err(validation("graph manifest node limit exceeded"));
        }
        if !visited.insert(digest.clone()) {
            return Err(validation(
                "graph manifest node cycle or duplicate reference detected",
            ));
        }
        if u64::try_from(visited.len()).unwrap_or(u64::MAX) > structural_node_bound {
            return Err(validation(
                "graph manifest exceeds canonical Patricia node bound",
            ));
        }
        let bytes = load(&digest)?;
        evidence.decoded_bytes = evidence
            .decoded_bytes
            .checked_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| validation("graph manifest decoded byte total overflow"))?;
        if evidence.decoded_bytes > limits.max_decoded_bytes {
            return Err(validation("graph manifest decoded byte limit exceeded"));
        }
        if object_digest(&bytes) != digest {
            return Err(validation("graph manifest node digest mismatch"));
        }
        let node = decode_node(&bytes)?;
        if node.depth != expected_depth {
            return Err(validation("graph manifest radix depth mismatch"));
        }
        let payload_depth = node
            .depth
            .checked_add(u8::try_from(node.prefix.len()).map_err(|_| {
                validation("graph manifest compressed prefix length exceeds radix depth")
            })?)
            .ok_or_else(|| validation("graph manifest compressed depth overflow"))?;
        let mut node_route = route;
        node_route.push_str(&node.prefix);
        evidence.segments_examined = evidence.segments_examined.saturating_add(1);
        admit_work(&mut evidence, limits, 1)?;
        match node.kind {
            GraphManifestNodeKind::Branch { children } => {
                if payload_depth >= GRAPH_RADIX_DEPTH {
                    return Err(validation("graph manifest branch exceeds radix depth"));
                }
                admit_work(
                    &mut evidence,
                    limits,
                    u64::try_from(children.len()).unwrap_or(u64::MAX),
                )?;
                for (nibble, child) in children.into_iter().rev() {
                    validate_nibble(&nibble)?;
                    let mut child_route = node_route.clone();
                    child_route.push_str(&nibble);
                    stack.push((child, payload_depth + 1, child_route));
                }
            }
            GraphManifestNodeKind::Leaf {
                path_sha256,
                entries,
            } => {
                if payload_depth != GRAPH_RADIX_DEPTH || node_route != path_sha256 {
                    return Err(validation("graph manifest leaf is not terminal"));
                }
                admit_work(
                    &mut evidence,
                    limits,
                    u64::try_from(entries.len()).unwrap_or(u64::MAX),
                )?;
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

fn admit_work(
    evidence: &mut GraphManifestResolveEvidence,
    limits: GraphManifestLimits,
    units: u64,
) -> Result<(), GfError> {
    evidence.work_units = evidence
        .work_units
        .checked_add(units)
        .ok_or_else(|| validation("graph manifest work total overflow"))?;
    if evidence.work_units > limits.max_work_units {
        return Err(validation("graph manifest work limit exceeded"));
    }
    Ok(())
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
    validate_prefix(&node.prefix)?;
    let payload_depth = node
        .depth
        .checked_add(u8::try_from(node.prefix.len()).map_err(|_| {
            validation("graph manifest compressed prefix length exceeds radix depth")
        })?)
        .ok_or_else(|| validation("graph manifest compressed depth overflow"))?;
    if payload_depth > GRAPH_RADIX_DEPTH {
        return Err(validation("graph manifest radix depth exceeds limit"));
    }
    match &node.kind {
        GraphManifestNodeKind::Branch { children } => {
            if payload_depth >= GRAPH_RADIX_DEPTH {
                return Err(validation("terminal graph manifest node must be a leaf"));
            }
            if children.len() == 1 {
                return Err(validation("graph manifest unary branch is not canonical"));
            }
            let mut child_digests = BTreeSet::new();
            if children.is_empty() && (node.depth != 0 || !node.prefix.is_empty()) {
                return Err(validation("only the root may be an empty branch"));
            }
            for (nibble, digest) in children {
                validate_nibble(nibble)?;
                validate_digest(digest)?;
                if !child_digests.insert(digest) {
                    return Err(validation(
                        "graph manifest branch contains duplicate child references",
                    ));
                }
            }
        }
        GraphManifestNodeKind::Leaf {
            path_sha256,
            entries,
        } => {
            if payload_depth != GRAPH_RADIX_DEPTH || entries.is_empty() {
                return Err(validation("graph manifest leaf shape is invalid"));
            }
            validate_digest(path_sha256)?;
            if node.prefix != path_sha256[usize::from(node.depth)..] {
                return Err(validation("graph manifest leaf compressed route mismatch"));
            }
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
fn validate_prefix(value: &str) -> Result<(), GfError> {
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(validation(
            "graph manifest compressed prefix is not canonical lowercase hex",
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
            prefix: String::new(),
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
                prefix: String::new(),
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
        let mut children = BTreeMap::new();
        children.insert("0".into(), "1".repeat(64));
        children.insert("1".into(), "1".repeat(64));
        assert!(encode_node(&branch(0, children)).is_err());

        let mut children = BTreeMap::new();
        children.insert("0".into(), "1".repeat(64));
        children.insert("1".into(), "2".repeat(64));
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
            } else {
                Err(validation("missing test object"))
            }
        };
        assert!(resolve_manifest(&root, GraphManifestLimits::default(), load).is_err());

        let wrong_depth = encode_node(&branch(
            1,
            BTreeMap::from([("0".into(), "1".repeat(64)), ("1".into(), "2".repeat(64))]),
        ))
        .unwrap();
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

    #[test]
    fn aggregate_decode_and_work_limits_reject_hostile_roots() {
        let empty_bytes = encode_node(&branch(0, BTreeMap::new())).unwrap();
        let empty_digest = object_digest(&empty_bytes);
        let empty_root = GraphFilesRootV2 {
            format: GRAPH_FILES_V2_FORMAT.into(),
            format_version: GRAPH_FILES_V2_VERSION,
            root_node_sha256: empty_digest.clone(),
            logical_file_count: 0,
            logical_byte_length: 0,
        };
        let byte_limits = GraphManifestLimits {
            max_decoded_bytes: u64::try_from(empty_bytes.len() - 1).unwrap(),
            ..GraphManifestLimits::default()
        };
        assert!(resolve_manifest(&empty_root, byte_limits, |_| Ok(empty_bytes.clone())).is_err());

        let children = (0_u8..16)
            .map(|nibble| (format!("{nibble:x}"), format!("{nibble:064x}")))
            .collect();
        let wide_bytes = encode_node(&branch(0, children)).unwrap();
        let wide_digest = object_digest(&wide_bytes);
        let wide_root = GraphFilesRootV2 {
            root_node_sha256: wide_digest,
            ..empty_root
        };
        let work_limits = GraphManifestLimits {
            max_work_units: 16,
            ..GraphManifestLimits::default()
        };
        assert!(resolve_manifest(&wide_root, work_limits, |_| Ok(wide_bytes.clone())).is_err());
    }

    #[test]
    fn successful_resolution_reports_aggregate_admission_evidence() {
        let bytes = encode_node(&branch(0, BTreeMap::new())).unwrap();
        let digest = object_digest(&bytes);
        let root = GraphFilesRootV2 {
            format: GRAPH_FILES_V2_FORMAT.into(),
            format_version: GRAPH_FILES_V2_VERSION,
            root_node_sha256: digest,
            logical_file_count: 0,
            logical_byte_length: 0,
        };
        let (_, evidence) =
            resolve_manifest(&root, GraphManifestLimits::default(), |_| Ok(bytes.clone())).unwrap();
        assert_eq!(evidence.decoded_bytes, bytes.len() as u64);
        assert_eq!(evidence.work_units, 1);
    }

    #[test]
    fn node_v1_unary_and_wrong_compressed_routes_fail_closed() {
        let old = b"{\"format\":\"graphforge-graph-manifest-radix-node\",\"format_version\":1,\"depth\":0,\"kind\":\"branch\",\"children\":{}}\n";
        assert!(decode_node(old).is_err());

        let unary = branch(0, BTreeMap::from([("0".into(), "1".repeat(64))]));
        assert!(encode_node(&unary).is_err());

        let path = "topology/nodes/a.parquet";
        let path_sha256 = hex_digest(logical_path_digest(path));
        let entry = GraphFileEntry {
            relative_path: path.into(),
            byte_length: 1,
            content_sha256: "a".repeat(64),
            role: GraphFileRole::Topology,
        };
        let node = GraphManifestNode {
            format: GRAPH_MANIFEST_NODE_FORMAT.into(),
            format_version: GRAPH_MANIFEST_NODE_VERSION,
            depth: 0,
            prefix: path_sha256.clone(),
            kind: GraphManifestNodeKind::Leaf {
                path_sha256,
                entries: vec![entry],
            },
        };
        let bytes = encode_node(&node).unwrap();
        let digest = object_digest(&bytes);
        let root = GraphFilesRootV2 {
            format: GRAPH_FILES_V2_FORMAT.into(),
            format_version: GRAPH_FILES_V2_VERSION,
            root_node_sha256: digest,
            logical_file_count: 1,
            logical_byte_length: 1,
        };
        let mut wrong = node;
        wrong
            .prefix
            .replace_range(..1, if &wrong.prefix[..1] == "0" { "1" } else { "0" });
        let wrong_bytes = encode_node(&wrong).unwrap_err();
        assert!(matches!(wrong_bytes, GfError::Validation(_)));
        // Correctly routed leaf resolves; changing its authenticated bytes or
        // route changes the address and cannot satisfy this root.
        let (entries, _) =
            resolve_manifest(&root, GraphManifestLimits::default(), |_| Ok(bytes.clone())).unwrap();
        assert_eq!(entries.len(), 1);
    }
}
