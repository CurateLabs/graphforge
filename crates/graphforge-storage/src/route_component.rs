//! Contextual reversible semantic routes. Authentication belongs to graph inventory admission.
//!
//! Components contain only lowercase ASCII. Exact UTF-8 semantics live in the
//! authenticated table; neither case folding nor Unicode normalization is applied.

use std::collections::BTreeMap;

use graphforge_core::{GfError, ProjectErrorCode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) mod materialize;
pub(crate) mod owned;

pub(crate) const TABLE_FILE: &str = "semantic-routes.json";
const FORMAT: &str = "graphforge-semantic-routes";
const VERSION: u32 = 1;
const PREFIX: &str = "r-";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    component: String,
    route: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireTable {
    format: String,
    format_version: u32,
    // A sequence preserves duplicate keys until validation; a JSON map would
    // silently overwrite them during deserialization.
    entries: Vec<Entry>,
}

#[derive(Clone, Default, PartialEq, Eq)]
pub(crate) struct RouteTable {
    entries: BTreeMap<String, String>,
    semantic_bytes: u64,
}

impl std::fmt::Debug for RouteTable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RouteTable")
            .field("route_count", &self.entries.len())
            .field("semantic_bytes", &self.semantic_bytes)
            .finish()
    }
}

impl RouteTable {
    pub(crate) fn check_insert(
        &self,
        route: &str,
        max_bytes: u64,
        max_routes: u64,
    ) -> Result<String, GfError> {
        validate_semantic_route(route)?;
        let component = component(route);
        if let Some(existing) = self.entries.get(&component) {
            if existing != route {
                return Err(invalid("semantic route component collision"));
            }
        } else {
            let next_bytes = self
                .semantic_bytes
                .checked_add(route.len() as u64)
                .ok_or_else(|| limit("semantic route byte count overflow"))?;
            if next_bytes > max_bytes || self.entries.len() as u64 >= max_routes {
                return Err(limit("semantic route writer budget exceeded"));
            }
        }
        Ok(component)
    }

    pub(crate) fn insert(
        &mut self,
        route: &str,
        max_bytes: u64,
        max_routes: u64,
    ) -> Result<String, GfError> {
        let component = self.check_insert(route, max_bytes, max_routes)?;
        if !self.entries.contains_key(&component) {
            self.semantic_bytes += route.len() as u64;
            self.entries.insert(component.clone(), route.to_owned());
        }
        Ok(component)
    }

    pub(crate) fn route(&self, component: &str) -> Result<&str, GfError> {
        self.entries
            .get(component)
            .map(String::as_str)
            .ok_or_else(|| invalid("semantic route component is absent from authenticated table"))
    }

    /// Decode only an authenticated route position; callers retain the original
    /// physical path for file access and use this spelling for semantic lookup.
    pub(crate) fn semantic_relative_path(&self, relative: &str) -> Result<String, GfError> {
        let Some(component) = route_position(relative)? else {
            return Ok(relative.to_owned());
        };
        let route = self.route(component)?;
        let mut parts = relative.split('/').collect::<Vec<_>>();
        let index = if parts[0] == "topology" { 2 } else { 1 };
        let filename = if parts.len() == index + 1 {
            format!("{route}.parquet")
        } else {
            route.to_owned()
        };
        parts[index] = &filename;
        Ok(parts.join("/"))
    }

    pub(crate) fn validate_paths<'a>(
        &self,
        paths: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), GfError> {
        let mut used = std::collections::BTreeSet::new();
        for path in paths {
            if let Some(component) = route_position(path)? {
                self.route(component)?;
                used.insert(component);
            }
        }
        if used.len() != self.entries.len() {
            return Err(invalid(
                "semantic route table contains unreferenced entries",
            ));
        }
        Ok(())
    }

    pub(crate) fn encode(&self, max_bytes: u64) -> Result<Vec<u8>, GfError> {
        use serde::ser::{SerializeSeq, SerializeStruct};
        struct Entries<'a>(&'a BTreeMap<String, String>);
        impl Serialize for Entries<'_> {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                #[derive(Serialize)]
                struct BorrowedEntry<'a> {
                    component: &'a str,
                    route: &'a str,
                }
                let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
                for (component, route) in self.0 {
                    sequence.serialize_element(&BorrowedEntry { component, route })?;
                }
                sequence.end()
            }
        }
        struct Table<'a>(&'a RouteTable);
        impl Serialize for Table<'_> {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let mut state = serializer.serialize_struct("RouteTable", 3)?;
                state.serialize_field("format", FORMAT)?;
                state.serialize_field("format_version", &VERSION)?;
                state.serialize_field("entries", &Entries(&self.0.entries))?;
                state.end()
            }
        }
        struct Output {
            bytes: Vec<u8>,
            limit: u64,
        }
        impl std::io::Write for Output {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                let total = self
                    .bytes
                    .len()
                    .checked_add(bytes.len())
                    .ok_or_else(|| std::io::Error::other("route table size overflow"))?;
                if total as u64 > self.limit {
                    return Err(std::io::Error::other("route table byte budget exceeded"));
                }
                self.bytes.extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut output = Output {
            bytes: Vec::new(),
            limit: max_bytes.saturating_sub(1),
        };
        serde_json::to_writer(&mut output, &Table(self))
            .map_err(|_| limit("semantic route serialized byte budget exceeded"))?;
        output.bytes.push(b'\n');
        Ok(output.bytes)
    }

    /// The caller admits the inventoried file's byte bound before allocating it.
    /// Requiring canonical bytes also rejects unknown fields and duplicate JSON
    /// fields instead of permitting multiple authenticated spellings.
    pub(crate) fn decode(bytes: &[u8], max_bytes: u64, max_routes: u64) -> Result<Self, GfError> {
        if bytes.len() as u64 > max_bytes {
            return Err(limit("semantic route table byte budget exceeded"));
        }
        let wire: WireTable =
            serde_json::from_slice(bytes).map_err(|_| invalid("invalid semantic route table"))?;
        if wire.format != FORMAT || wire.format_version != VERSION {
            return Err(GfError::Project {
                code: ProjectErrorCode::UnsupportedProjectFormat,
                message: "unsupported semantic route table format".into(),
            });
        }
        if wire.entries.len() as u64 > max_routes {
            return Err(limit("semantic route table entry budget exceeded"));
        }
        let mut table = Self::default();
        for entry in wire.entries {
            if table.entries.contains_key(&entry.component) {
                return Err(invalid("duplicate semantic route component"));
            }
            let expected = table.insert(&entry.route, max_bytes, max_routes)?;
            if expected != entry.component {
                return Err(invalid("semantic route component digest mismatch"));
            }
        }
        if table.encode(max_bytes)? != bytes {
            return Err(invalid("noncanonical semantic route table"));
        }
        Ok(table)
    }
}

pub(crate) fn authenticate_manifest_routes(
    version: u32,
    entries: &[crate::GraphFileEntry],
    read: impl FnOnce(&crate::GraphFileEntry) -> Result<Vec<u8>, GfError>,
) -> Result<Option<RouteTable>, GfError> {
    if matches!(version, 3 | 4) {
        for entry in entries {
            crate::graph_files::wire_relative_path(&entry.relative_path)?;
        }
    }
    let table_entry = entries
        .iter()
        .find(|entry| entry.relative_path == TABLE_FILE);
    match (version, table_entry) {
        (1 | 2, None) => Ok(None),
        (1 | 2, Some(_)) => Err(invalid(
            "raw graph layout contains reserved semantic route authority",
        )),
        (3 | 4, Some(entry)) => {
            const MAX_BYTES: u64 = 64 * 1024 * 1024;
            const HEX: &[u8; 16] = b"0123456789abcdef";
            if entry.byte_length > MAX_BYTES {
                return Err(limit("semantic route table byte budget exceeded"));
            }
            let bytes = read(entry)?;
            let digest = Sha256::digest(&bytes);
            let matches = entry.content_sha256.len() == 64
                && digest
                    .iter()
                    .zip(entry.content_sha256.as_bytes().chunks_exact(2))
                    .all(|(byte, pair)| {
                        pair[0] == HEX[usize::from(byte >> 4)]
                            && pair[1] == HEX[usize::from(byte & 15)]
                    });
            if bytes.len() as u64 != entry.byte_length || !matches {
                return Err(invalid("semantic route table authentication mismatch"));
            }
            let table = RouteTable::decode(&bytes, MAX_BYTES, 100_000)?;
            table.validate_paths(entries.iter().map(|entry| entry.relative_path.as_str()))?;
            Ok(Some(table))
        }
        (3 | 4, None) => Err(invalid(
            "mapped graph layout lacks semantic route authority",
        )),
        _ => Err(invalid("unsupported graph route layout")),
    }
}

/// Return only a recognized graph route component, never arbitrary path text.
pub(crate) fn route_position(path: &str) -> Result<Option<&str>, GfError> {
    let parts = path.split('/').collect::<Vec<_>>();
    let route = match parts.as_slice() {
        ["properties" | "edge_properties", flat] => Some(
            flat.strip_suffix(".parquet")
                .ok_or_else(|| invalid("invalid property route file"))?,
        ),
        ["properties" | "edge_properties", route, file] if file.ends_with(".parquet") => {
            Some(*route)
        }
        ["topology", "edges", flat] => Some(
            flat.strip_suffix(".parquet")
                .ok_or_else(|| invalid("invalid topology route file"))?,
        ),
        ["topology", "edges", route, file] if file.ends_with(".parquet") => Some(*route),
        ["properties" | "edge_properties", ..] | ["topology", "edges", ..] => {
            return Err(invalid("unsupported semantic route path shape"));
        }
        _ => None,
    };
    Ok(route)
}

/// Translate a legacy semantic route position without interpreting its spelling
/// as an encoded component. Non-route path components are left for the ordinary
/// portable inventory validator.
pub(crate) fn encode_relative_route(
    relative: &str,
    table: &mut RouteTable,
    max_bytes: u64,
    max_routes: u64,
) -> Result<String, GfError> {
    let Some(route) = route_position(relative)? else {
        return Ok(relative.to_owned());
    };
    let encoded = table.insert(route, max_bytes, max_routes)?;
    let mut parts = relative.split('/').collect::<Vec<_>>();
    let index = if parts[0] == "topology" { 2 } else { 1 };
    let flat = parts.len() == index + 1;
    let filename = if flat {
        format!("{encoded}.parquet")
    } else {
        encoded
    };
    parts[index] = &filename;
    Ok(parts.join("/"))
}

pub(crate) fn component(route: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut digest = Sha256::new();
    digest.update(b"graphforge-semantic-route/1\0");
    digest.update(route.as_bytes());
    let mut component = String::with_capacity(PREFIX.len() + 64);
    component.push_str(PREFIX);
    for byte in digest.finalize() {
        component.push(char::from(HEX[usize::from(byte >> 4)]));
        component.push(char::from(HEX[usize::from(byte & 15)]));
    }
    component
}

fn validate_semantic_route(route: &str) -> Result<(), GfError> {
    if route.is_empty() || matches!(route, "." | "..") || route.contains(['/', '\0']) {
        return Err(invalid("semantic route is not canonical"));
    }
    Ok(())
}

fn invalid(message: &str) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::ProjectCorrupt,
        message: message.into(),
    }
}

fn limit(message: &str) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::ResourceLimit,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn exact_semantics_survive_portable_lookup_and_long_names() {
        let long = "é".repeat(4096);
        let routes = [
            "CON",
            "AUX",
            "COM1",
            "LPT9",
            "CON.foo",
            "a:b*?<>|\\x",
            "tail.",
            "tail ",
            "Name",
            "name",
            "é",
            "e\u{301}",
            "r-deadbeef",
            &long,
        ];
        let mut table = RouteTable::default();
        let mut components = BTreeSet::new();
        for route in routes {
            let component = table.insert(route, 65536, 100).unwrap();
            assert_eq!(component.len(), 66);
            assert!(
                component
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            );
            assert!(components.insert(component.clone()));
            assert_eq!(table.route(&component).unwrap(), route);
        }
        let bytes = table.encode(65536).unwrap();
        assert_eq!(
            RouteTable::decode(&bytes, bytes.len() as u64, 14).unwrap(),
            table
        );
        assert!(RouteTable::decode(&bytes, bytes.len() as u64 - 1, 14).is_err());
        assert!(RouteTable::decode(&bytes, bytes.len() as u64, 13).is_err());
    }

    #[test]
    fn legacy_prefixes_and_normalization_variants_remain_distinct_routes() {
        let mut table = RouteTable::default();
        let mut outputs = BTreeSet::new();
        let prefix_literal = component("CON");
        for route in ["CON", "con", "é", "e\u{301}", &prefix_literal] {
            let raw = format!("properties/{route}.parquet");
            let mapped = encode_relative_route(&raw, &mut table, 4096, 100).unwrap();
            assert!(outputs.insert(mapped.clone()));
            assert_eq!(
                table
                    .route(route_position(&mapped).unwrap().unwrap())
                    .unwrap(),
                route
            );
        }
        let edge = encode_relative_route(
            "topology/edges/AUX/00000000000000000001.parquet",
            &mut table,
            4096,
            100,
        )
        .unwrap();
        assert_eq!(
            table
                .route(route_position(&edge).unwrap().unwrap())
                .unwrap(),
            "AUX"
        );
        assert_eq!(
            encode_relative_route("runtime_catalog.parquet", &mut table, 4096, 100).unwrap(),
            "runtime_catalog.parquet"
        );
    }

    #[test]
    fn mapped_manifest_refuses_hidden_backslash_routes_before_table_read() {
        let entries = vec![crate::GraphFileEntry {
            relative_path: "properties\\r-hidden.parquet".into(),
            byte_length: 1,
            content_sha256: "0".repeat(64),
            role: crate::GraphFileRole::Properties,
        }];
        for version in [3, 4] {
            assert!(
                authenticate_manifest_routes(version, &entries, |_| {
                    panic!("invalid mapped paths must fail before table IO")
                })
                .is_err()
            );
        }
    }

    #[test]
    fn writer_budgets_refuse_before_mutation_with_typed_cause() {
        let mut table = RouteTable::default();
        table.insert("CON", 3, 1).unwrap();
        let before = table.clone();
        for result in [table.insert("AUX", 3, 2), table.insert("AUX", 6, 1)] {
            assert!(matches!(
                result,
                Err(GfError::Project {
                    code: ProjectErrorCode::ResourceLimit,
                    ..
                })
            ));
        }
        assert_eq!(table, before);
        assert!(matches!(
            table.encode(1),
            Err(GfError::Project {
                code: ProjectErrorCode::ResourceLimit,
                ..
            })
        ));
        let bytes = table.encode(4096).unwrap();
        assert!(matches!(
            RouteTable::decode(&bytes, 1, 1),
            Err(GfError::Project {
                code: ProjectErrorCode::ResourceLimit,
                ..
            })
        ));
        assert!(matches!(
            RouteTable::decode(&bytes, 4096, 0),
            Err(GfError::Project {
                code: ProjectErrorCode::ResourceLimit,
                ..
            })
        ));
        assert!(matches!(
            RouteTable::decode(b"{}", 4096, 1),
            Err(GfError::Project {
                code: ProjectErrorCode::ProjectCorrupt,
                ..
            })
        ));
    }

    #[test]
    fn malformed_duplicate_and_noncanonical_authority_is_rejected() {
        let mut table = RouteTable::default();
        let key = table.insert("CON", 65536, 100).unwrap();
        let bytes = table.encode(65536).unwrap();
        let mut wire: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let entry = wire["entries"][0].clone();
        wire["entries"].as_array_mut().unwrap().push(entry);
        assert!(RouteTable::decode(&serde_json::to_vec(&wire).unwrap(), 4096, 10).is_err());
        let mut altered = bytes.clone();
        let position = altered.windows(3).position(|part| part == b"CON").unwrap();
        altered[position] = b'c';
        assert!(RouteTable::decode(&altered, 4096, 10).is_err());
        assert!(RouteTable::decode(&bytes[..bytes.len() - 1], 4096, 10).is_err());
        assert!(table.route(&key.to_uppercase()).is_err());
        for path in [
            "properties",
            "properties/r-x/nested/part.parquet",
            "edge_properties/r-x/nested/part.parquet",
            "topology/edges/r-x/nested/part.parquet",
            "properties/not-parquet",
        ] {
            assert!(matches!(
                route_position(path),
                Err(GfError::Project {
                    code: ProjectErrorCode::ProjectCorrupt,
                    ..
                })
            ));
        }
        assert_eq!(route_position("indexes/some/nested/file").unwrap(), None);
        for route in ["", ".", "..", "a/b", "a\0b"] {
            assert!(table.insert(route, 65536, 100).is_err());
        }
    }
}
