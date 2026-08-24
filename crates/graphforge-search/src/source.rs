//! Graph-native Parquet projection for text indexing.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use arrow::array::{Array, FixedSizeBinaryArray, ListArray, StringArray, UInt32Array};
use arrow::datatypes::DataType;
use graphforge_storage::{SearchArtifactError, list_property_stems, read_nodes, read_properties};

use crate::TextSearchLimits;

type TextFieldsByUuid = BTreeMap<[u8; 16], BTreeMap<String, String>>;
type ProjectedProperties = (BTreeSet<String>, TextFieldsByUuid);

/// One UUID-only document supplied to the Tantivy backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextDocument {
    /// Stable graph identity.
    pub node_uuid: [u8; 16],
    /// Selected non-null string properties in canonical name order.
    pub fields: BTreeMap<String, String>,
}

/// Canonical label/property projection captured from committed graph Parquet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextSourceProjection {
    /// Sorted, deduplicated text fields represented by the index schema.
    pub properties: Vec<String>,
    /// UUID-sorted graph documents. Empty when the label has no indexable text.
    pub documents: Vec<TextDocument>,
    /// Physical committed source bytes inspected while projecting.
    pub source_bytes: u64,
}

/// Project one caller-resolved label from topology and property Parquet files.
///
/// `label_id` is resolved by the API/catalog layer. Full `type_ids` membership
/// is authoritative, so nodes carrying the label secondarily remain eligible.
/// Property stems are all scanned because properties stay with the immutable
/// primary-label routing stem.
///
/// `selected_properties=None` discovers every non-null string property on an
/// eligible node. An explicit list is canonicalized and fixes the schema even
/// when some selected properties are absent or null.
///
/// # Errors
/// Returns structured selector, source-corruption, resource, or cancellation
/// errors. No partial projection is returned.
pub fn project_text_source<C>(
    project_dir: &Path,
    label_id: u32,
    selected_properties: Option<&[String]>,
    limits: TextSearchLimits,
    mut checkpoint: C,
) -> Result<TextSourceProjection, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    checkpoint()?;
    let explicit = selected_properties
        .map(|properties| normalize_properties(properties, limits))
        .transpose()?;
    let explicit_set = explicit
        .as_ref()
        .map(|properties| properties.iter().cloned().collect::<BTreeSet<_>>());

    let mut source_bytes = 0_u64;
    let eligible = select_eligible_nodes(
        project_dir,
        label_id,
        limits,
        &mut checkpoint,
        &mut source_bytes,
    )?;
    if eligible.len() > limits.documents {
        return Err(exhausted("text_documents", limits.documents));
    }
    if eligible.is_empty() {
        return Ok(TextSourceProjection {
            properties: explicit.unwrap_or_default(),
            documents: Vec::new(),
            source_bytes,
        });
    }

    let (observed_properties, mut fields_by_uuid) = project_properties(
        project_dir,
        &eligible,
        explicit_set.as_ref(),
        limits,
        &mut checkpoint,
        &mut source_bytes,
    )?;
    let properties = explicit.unwrap_or_else(|| observed_properties.into_iter().collect());
    if properties.is_empty() {
        return Ok(TextSourceProjection {
            properties,
            documents: Vec::new(),
            source_bytes,
        });
    }
    let property_set = properties.iter().collect::<BTreeSet<_>>();
    let documents = eligible
        .into_iter()
        .map(|node_uuid| {
            let fields = fields_by_uuid
                .remove(&node_uuid)
                .unwrap_or_default()
                .into_iter()
                .filter(|(name, _)| property_set.contains(name))
                .collect();
            TextDocument { node_uuid, fields }
        })
        .collect();
    Ok(TextSourceProjection {
        properties,
        documents,
        source_bytes,
    })
}

fn select_eligible_nodes<C>(
    project_dir: &Path,
    label_id: u32,
    limits: TextSearchLimits,
    checkpoint: &mut C,
    source_bytes: &mut u64,
) -> Result<BTreeSet<[u8; 16]>, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    for path in graphforge_storage::topology_node_files(project_dir)
        .map_err(|error| source(error.to_string()))?
    {
        add_source_bytes(&path, source_bytes, limits)?;
    }
    let batches = read_nodes(project_dir).map_err(|error| source(error.to_string()))?;
    let mut eligible = BTreeSet::new();
    let mut seen_topology = BTreeSet::new();
    let mut topology_rows = 0_usize;
    for batch in batches {
        checkpoint()?;
        let uuids = batch
            .column_by_name("node_uuid")
            .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or_else(|| source("topology node_uuid is not FixedSizeBinary(16)"))?;
        let labels = batch
            .column_by_name("type_ids")
            .and_then(|column| column.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| source("topology type_ids is not List<UInt32>"))?;
        for row in 0..batch.num_rows() {
            checkpoint()?;
            topology_rows = topology_rows.saturating_add(1);
            if topology_rows > limits.topology_rows {
                return Err(exhausted("text_topology_rows", limits.topology_rows));
            }
            if uuids.is_null(row) || labels.is_null(row) {
                return Err(source("topology contains null node identity data"));
            }
            let node_uuid: [u8; 16] = uuids
                .value(row)
                .try_into()
                .map_err(|_| source("topology node_uuid is not 16 bytes"))?;
            if !seen_topology.insert(node_uuid) {
                return Err(source("topology contains duplicate node UUIDs"));
            }
            let label_values = labels.value(row);
            let label_values = label_values
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| source("topology type_ids child is not UInt32"))?;
            if label_values.null_count() != 0 {
                return Err(source("topology type_ids contains null labels"));
            }
            if label_values.values().contains(&label_id) {
                eligible.insert(node_uuid);
            }
        }
    }
    Ok(eligible)
}

fn project_properties<C>(
    project_dir: &Path,
    eligible: &BTreeSet<[u8; 16]>,
    explicit: Option<&BTreeSet<String>>,
    limits: TextSearchLimits,
    checkpoint: &mut C,
    source_bytes: &mut u64,
) -> Result<ProjectedProperties, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let mut property_rows = 0_usize;
    let mut seen_property_rows = BTreeSet::new();
    let mut observed_properties = BTreeSet::new();
    let mut fields_by_uuid = TextFieldsByUuid::new();
    for path in graphforge_storage::node_property_source_files(project_dir)
        .map_err(|error| source(error.to_string()))?
    {
        add_source_bytes(&path, source_bytes, limits)?;
    }
    for stem in list_property_stems(project_dir) {
        checkpoint()?;
        let batches = read_properties(project_dir, &stem)
            .map_err(|error| source(format!("read property route {stem}: {error}")))?;
        if batches.is_empty() {
            return Err(source(format!(
                "property route {stem} is not readable Parquet"
            )));
        }
        for batch in batches {
            checkpoint()?;
            let uuids = batch
                .column_by_name("node_uuid")
                .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .ok_or_else(|| source(format!("property route {stem} node_uuid is malformed")))?;
            for row in 0..batch.num_rows() {
                checkpoint()?;
                property_rows = property_rows.saturating_add(1);
                if property_rows > limits.property_rows {
                    return Err(exhausted("text_property_rows", limits.property_rows));
                }
                if uuids.is_null(row) {
                    return Err(source(format!(
                        "property route {stem} contains null node_uuid"
                    )));
                }
                let node_uuid: [u8; 16] = uuids.value(row).try_into().map_err(|_| {
                    source(format!("property route {stem} node_uuid is not 16 bytes"))
                })?;
                if !eligible.contains(&node_uuid) {
                    continue;
                }
                if !seen_property_rows.insert(node_uuid) {
                    return Err(source(format!(
                        "eligible UUID {node_uuid:02x?} has duplicate property rows"
                    )));
                }
                for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
                    let name = field.name();
                    if name == "node_uuid"
                        || explicit.is_some_and(|selected| !selected.contains(name))
                    {
                        continue;
                    }
                    if field.data_type() != &DataType::Utf8 {
                        continue;
                    }
                    let values =
                        column
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                source(format!("property route {stem} field {name:?} is malformed"))
                            })?;
                    if values.is_null(row) {
                        continue;
                    }
                    validate_property(name, limits)?;
                    observed_properties.insert(name.clone());
                    let replaced = fields_by_uuid
                        .entry(node_uuid)
                        .or_default()
                        .insert(name.clone(), values.value(row).to_owned());
                    if replaced.is_some() {
                        return Err(source(format!(
                            "eligible UUID {node_uuid:02x?} repeats property {name:?}"
                        )));
                    }
                }
            }
        }
    }
    Ok((observed_properties, fields_by_uuid))
}

pub(crate) fn normalize_properties(
    properties: &[String],
    limits: TextSearchLimits,
) -> Result<Vec<String>, SearchArtifactError> {
    if properties.is_empty() {
        return Err(invalid("properties", "at least one property is required"));
    }
    let mut normalized = properties
        .iter()
        .map(|property| property.trim().to_owned())
        .collect::<Vec<_>>();
    normalized.sort_unstable();
    normalized.dedup();
    if normalized.len() > limits.selected_properties {
        return Err(exhausted(
            "text_selected_properties",
            limits.selected_properties,
        ));
    }
    for property in &normalized {
        validate_property(property, limits)?;
    }
    Ok(normalized)
}

fn validate_property(property: &str, limits: TextSearchLimits) -> Result<(), SearchArtifactError> {
    if property.is_empty()
        || property.trim() != property
        || property.chars().any(char::is_control)
        || property == "node_uuid"
    {
        return Err(invalid("property", format!("invalid name {property:?}")));
    }
    if property.len() > limits.selector_bytes {
        return Err(exhausted("text_selector_bytes", limits.selector_bytes));
    }
    Ok(())
}

fn add_source_bytes(
    path: &Path,
    total: &mut u64,
    limits: TextSearchLimits,
) -> Result<(), SearchArtifactError> {
    match std::fs::metadata(path) {
        Ok(metadata) => {
            *total = total
                .checked_add(metadata.len())
                .ok_or_else(|| exhausted_u64("text_source_bytes", limits.source_bytes))?;
            if *total > limits.source_bytes {
                return Err(exhausted_u64("text_source_bytes", limits.source_bytes));
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(SearchArtifactError::Io {
            operation: "inspect text source",
            path: path.to_path_buf(),
            source: error,
        }),
    }
}

fn invalid(field: &'static str, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::InvalidSelector {
        field,
        reason: reason.into(),
    }
}

fn exhausted(resource: &'static str, limit: usize) -> SearchArtifactError {
    exhausted_u64(resource, u64::try_from(limit).unwrap_or(u64::MAX))
}

fn exhausted_u64(resource: &'static str, limit: u64) -> SearchArtifactError {
    SearchArtifactError::ResourceExhausted { resource, limit }
}

fn source(reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::SourceSnapshot {
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::HashMap;

    use graphforge_core::uuid::Uuid;
    use graphforge_ir::{IrLiteral, OntologyMode, TypeId};
    use graphforge_storage::GraphWriter;
    use tempfile::TempDir;

    use super::*;

    fn uuid(value: u8) -> Uuid {
        let mut bytes = [0_u8; 16];
        bytes[15] = value;
        Uuid::from_bytes(bytes)
    }

    #[test]
    fn property_normalization_and_source_byte_accounting_are_exact() {
        assert_eq!(
            normalize_properties(
                &[" title ".into(), "body".into(), "title".into()],
                TextSearchLimits::default()
            )
            .unwrap(),
            ["body", "title"]
        );
        for invalid_name in ["", "node_uuid", "line\nbreak"] {
            assert!(matches!(
                normalize_properties(&[invalid_name.into()], TextSearchLimits::default()),
                Err(SearchArtifactError::InvalidSelector { .. })
            ));
        }

        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("missing.parquet");
        let mut total = 7;
        add_source_bytes(&missing, &mut total, TextSearchLimits::default()).unwrap();
        assert_eq!(total, 7);
        let source = dir.path().join("source.parquet");
        std::fs::write(&source, b"four").unwrap();
        add_source_bytes(&source, &mut total, TextSearchLimits::default()).unwrap();
        assert_eq!(total, 11);
        let mut constrained = TextSearchLimits::default();
        constrained.source_bytes = 3;
        assert!(matches!(
            add_source_bytes(&source, &mut 0, constrained),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "text_source_bytes",
                ..
            })
        ));
    }

    #[test]
    fn source_byte_limit_counts_every_immutable_node_shard() {
        let dir = TempDir::new().unwrap();
        let mut first = GraphWriter::open_at(dir.path(), OntologyMode::Strict, 1).unwrap();
        first.create_node(uuid(1), TypeId(9)).unwrap();
        first.flush().unwrap();
        let mut second = GraphWriter::open_at(dir.path(), OntologyMode::Strict, 2).unwrap();
        second.create_node(uuid(2), TypeId(9)).unwrap();
        second.flush().unwrap();
        let paths = graphforge_storage::topology_node_files(dir.path()).unwrap();
        assert_eq!(paths.len(), 2);
        let mut limits = TextSearchLimits::default();
        limits.source_bytes = std::fs::metadata(&paths[0]).unwrap().len();
        assert!(matches!(
            project_text_source(dir.path(), 9, None, limits, || Ok(())),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "text_source_bytes",
                ..
            })
        ));
    }

    #[test]
    fn source_byte_limit_counts_every_immutable_property_shard() {
        let dir = TempDir::new().unwrap();
        for ordinal in 1_u8..=2 {
            let mut writer =
                GraphWriter::open_at(dir.path(), OntologyMode::Strict, i64::from(ordinal)).unwrap();
            let node = uuid(ordinal);
            writer.create_node(node, TypeId(9)).unwrap();
            writer
                .set_properties(
                    &node,
                    Some("Person"),
                    HashMap::from([(
                        "name".to_owned(),
                        IrLiteral::Str(format!("person-{ordinal}")),
                    )]),
                )
                .unwrap();
            writer.flush().unwrap();
        }
        let node_bytes = graphforge_storage::topology_node_files(dir.path())
            .unwrap()
            .into_iter()
            .map(|path| std::fs::metadata(path).unwrap().len())
            .sum::<u64>();
        let properties = graphforge_storage::node_property_files(dir.path(), "Person").unwrap();
        assert_eq!(properties.len(), 2);
        let mut limits = TextSearchLimits::default();
        limits.source_bytes = node_bytes + std::fs::metadata(&properties[0]).unwrap().len();
        assert!(matches!(
            project_text_source(dir.path(), 9, None, limits, || Ok(())),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "text_source_bytes",
                ..
            })
        ));
    }

    #[test]
    fn projection_honors_secondary_labels_and_primary_property_stems() {
        let dir = TempDir::new().unwrap();
        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, 1).unwrap();
        let secondary = uuid(1);
        let primary = uuid(2);
        let unrelated = uuid(3);
        writer
            .create_node_with_labels(secondary, &[TypeId(1), TypeId(9)])
            .unwrap();
        writer.create_node(primary, TypeId(9)).unwrap();
        writer.create_node(unrelated, TypeId(3)).unwrap();
        writer
            .set_properties(
                &secondary,
                Some("Primary"),
                HashMap::from([
                    ("name".to_owned(), IrLiteral::Str("Alice".to_owned())),
                    ("age".to_owned(), IrLiteral::Int(30)),
                ]),
            )
            .unwrap();
        writer
            .set_properties(
                &primary,
                Some("Secondary"),
                HashMap::from([(
                    "summary".to_owned(),
                    IrLiteral::Str("Graph search".to_owned()),
                )]),
            )
            .unwrap();
        writer
            .set_properties(
                &unrelated,
                Some("Other"),
                HashMap::from([(
                    "secret".to_owned(),
                    IrLiteral::Str("must not leak".to_owned()),
                )]),
            )
            .unwrap();
        writer.flush().unwrap();

        let projection =
            project_text_source(dir.path(), 9, None, TextSearchLimits::default(), || Ok(()))
                .unwrap();
        assert_eq!(projection.properties, ["name", "summary"]);
        assert_eq!(
            projection
                .documents
                .iter()
                .map(|document| document.node_uuid[15])
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(projection.documents[0].fields["name"], "Alice");
        assert!(!projection.documents[0].fields.contains_key("age"));
        assert!(!projection.properties.contains(&"secret".to_owned()));
    }

    #[test]
    fn explicit_projection_keeps_absent_fields_and_empty_labels_stable() {
        let dir = TempDir::new().unwrap();
        let properties = vec![" title ".to_owned(), "body".to_owned(), "title".to_owned()];
        let projection = project_text_source(
            dir.path(),
            7,
            Some(&properties),
            TextSearchLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert!(projection.documents.is_empty());
        assert_eq!(projection.properties, ["body", "title"]);
    }

    #[test]
    fn projection_limits_and_cancellation_return_no_partial_value() {
        let dir = TempDir::new().unwrap();
        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, 1).unwrap();
        writer.create_node(uuid(1), TypeId(1)).unwrap();
        writer.flush().unwrap();

        let mut limits = TextSearchLimits::default();
        limits.topology_rows = 0;
        assert!(matches!(
            project_text_source(dir.path(), 1, None, limits, || Ok(())),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "text_topology_rows",
                ..
            })
        ));
        assert!(matches!(
            project_text_source(dir.path(), 1, None, TextSearchLimits::default(), || Err(
                SearchArtifactError::Cancelled
            )),
            Err(SearchArtifactError::Cancelled)
        ));
    }

    #[test]
    fn malformed_property_parquet_is_a_source_error() {
        let dir = TempDir::new().unwrap();
        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, 1).unwrap();
        writer.create_node(uuid(1), TypeId(1)).unwrap();
        writer.flush().unwrap();
        let properties = dir.path().join("properties");
        std::fs::create_dir_all(&properties).unwrap();
        std::fs::write(properties.join("broken.parquet"), b"not parquet").unwrap();

        assert!(matches!(
            project_text_source(dir.path(), 1, None, TextSearchLimits::default(), || Ok(())),
            Err(SearchArtifactError::SourceSnapshot { .. })
        ));
    }

    #[test]
    fn explicit_selection_filters_fields_and_property_row_budget_is_enforced() {
        let dir = TempDir::new().unwrap();
        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, 1).unwrap();
        let selected = uuid(1);
        writer.create_node(selected, TypeId(7)).unwrap();
        writer
            .set_properties(
                &selected,
                Some("Person"),
                HashMap::from([
                    ("name".to_owned(), IrLiteral::Str("Alice".to_owned())),
                    ("bio".to_owned(), IrLiteral::Str("Engineer".to_owned())),
                    ("score".to_owned(), IrLiteral::Int(9)),
                ]),
            )
            .unwrap();
        writer.flush().unwrap();

        let projection = project_text_source(
            dir.path(),
            7,
            Some(&["name".to_owned(), "absent".to_owned()]),
            TextSearchLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(projection.properties, ["absent", "name"]);
        assert_eq!(projection.documents.len(), 1);
        assert_eq!(projection.documents[0].fields.len(), 1);
        assert_eq!(projection.documents[0].fields["name"], "Alice");

        let mut limits = TextSearchLimits::default();
        limits.property_rows = 0;
        assert!(matches!(
            project_text_source(dir.path(), 7, None, limits, || Ok(())),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "text_property_rows",
                limit: 0,
            })
        ));
    }

    #[test]
    fn selector_validation_canonicalizes_safe_names_and_rejects_unsafe_bounds() {
        let limits = TextSearchLimits::default();
        assert_eq!(
            normalize_properties(&[" z ".to_owned(), "a".to_owned(), "z".to_owned()], limits)
                .unwrap(),
            ["a", "z"]
        );
        for properties in [
            Vec::<String>::new(),
            vec![String::new()],
            vec!["node_uuid".to_owned()],
            vec!["line\nbreak".to_owned()],
        ] {
            assert!(matches!(
                normalize_properties(&properties, limits),
                Err(SearchArtifactError::InvalidSelector { .. })
            ));
        }

        let mut bounded = limits;
        bounded.selected_properties = 1;
        assert!(matches!(
            normalize_properties(&["a".to_owned(), "b".to_owned()], bounded),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "text_selected_properties",
                limit: 1,
            })
        ));
        bounded = limits;
        bounded.selector_bytes = 2;
        assert!(matches!(
            normalize_properties(&["long".to_owned()], bounded),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "text_selector_bytes",
                limit: 2,
            })
        ));
    }

    #[test]
    fn source_byte_accounting_handles_missing_files_limits_and_overflow() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("missing");
        let mut total = 7;
        add_source_bytes(&missing, &mut total, TextSearchLimits::default()).unwrap();
        assert_eq!(total, 7);

        let file = dir.path().join("source");
        std::fs::write(&file, b"abc").unwrap();
        let mut limits = TextSearchLimits::default();
        limits.source_bytes = 2;
        total = 0;
        assert!(matches!(
            add_source_bytes(&file, &mut total, limits),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "text_source_bytes",
                limit: 2,
            })
        ));

        total = u64::MAX;
        assert!(matches!(
            add_source_bytes(&file, &mut total, TextSearchLimits::default()),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "text_source_bytes",
                ..
            })
        ));
    }

    #[test]
    fn checkpoint_is_observed_during_topology_and_property_projection() {
        let dir = TempDir::new().unwrap();
        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, 1).unwrap();
        let node = uuid(1);
        writer.create_node(node, TypeId(1)).unwrap();
        writer
            .set_properties(
                &node,
                Some("Person"),
                HashMap::from([("name".to_owned(), IrLiteral::Str("Alice".to_owned()))]),
            )
            .unwrap();
        writer.flush().unwrap();

        let calls = Cell::new(0_usize);
        let result = project_text_source(dir.path(), 1, None, TextSearchLimits::default(), || {
            calls.set(calls.get() + 1);
            if calls.get() == 6 {
                Err(SearchArtifactError::Cancelled)
            } else {
                Ok(())
            }
        });
        assert!(matches!(result, Err(SearchArtifactError::Cancelled)));
        assert_eq!(calls.get(), 6);
    }
}
