//! Deterministic Tantivy build, reopen, validation, and BM25 search primitives.

use std::collections::BTreeSet;
use std::path::Path;

use graphforge_storage::SearchArtifactError;
use tantivy::collector::TopDocs;
use tantivy::merge_policy::NoMergePolicy;
use tantivy::query::{AllQuery, BooleanQuery, Query, TermQuery};
use tantivy::schema::{
    BytesOptions, Field, IndexRecordOption, Schema, TantivyDocument, TextFieldIndexing,
    TextOptions, Value,
};
use tantivy::{Index, ReloadPolicy, Term};

use crate::TextSearchLimits;
use crate::analyzer::{TEXT_ANALYZER_NAME, analyze_query, register_text_analyzer};
use crate::source::{TextSourceProjection, normalize_properties};

const NODE_UUID_FIELD: &str = "node_uuid";
const TANTIVY_MIN_WRITER_MEMORY_BYTES: usize = 15_000_000;
type TextSchema = (Schema, Field, Vec<(String, Field)>);

/// Pinned Tantivy storage/index-format release used by this backend.
pub const TEXT_BACKEND_VERSION: &str = "tantivy-0.26.1";

/// Result of building a graph label projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextIndexBuildOutcome {
    /// The label had no documents or no observed string properties; no index
    /// directory was created.
    Empty,
    /// A complete immutable index was built in the caller-supplied directory.
    Built {
        /// Number of UUID documents committed.
        documents: usize,
        /// Physical bytes in the completed index directory.
        index_bytes: u64,
    },
}

/// One deterministic BM25 hit before public Arrow shaping.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TextSearchHit {
    /// Stable graph UUID identity.
    pub node_uuid: [u8; 16],
    /// Finite Tantivy BM25 score widened to Float64.
    pub score: f64,
}

/// Build one immutable Tantivy index from a canonical graph projection.
///
/// Documents are sorted by UUID before insertion, a single bounded writer
/// thread is used, and background segment merging is disabled. The directory
/// must be absent or empty and is expected to be a private publication build
/// directory; callers expose it only after this function succeeds.
///
/// # Errors
/// Returns structured validation, build, resource, filesystem, or cancellation
/// errors. An empty projection returns [`TextIndexBuildOutcome::Empty`] without
/// creating an index.
pub fn build_text_index<C>(
    index_dir: &Path,
    projection: &TextSourceProjection,
    limits: TextSearchLimits,
    mut checkpoint: C,
) -> Result<TextIndexBuildOutcome, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    checkpoint()?;
    if projection.source_bytes > limits.source_bytes {
        return Err(exhausted_u64("text_source_bytes", limits.source_bytes));
    }
    if projection.documents.len() > limits.documents {
        return Err(exhausted("text_documents", limits.documents));
    }
    if projection.properties.is_empty() {
        return Ok(TextIndexBuildOutcome::Empty);
    }
    let properties = normalize_properties(&projection.properties, limits)?;
    if projection.documents.is_empty() {
        return Ok(TextIndexBuildOutcome::Empty);
    }
    let build_work = projection
        .documents
        .len()
        .checked_mul(properties.len())
        .ok_or_else(|| exhausted("text_build_work", limits.build_work))?;
    if build_work > limits.build_work {
        return Err(exhausted("text_build_work", limits.build_work));
    }
    if limits.writer_memory_bytes < TANTIVY_MIN_WRITER_MEMORY_BYTES {
        return Err(exhausted(
            "text_writer_memory_bytes",
            limits.writer_memory_bytes,
        ));
    }

    let property_set = properties.iter().collect::<BTreeSet<_>>();
    let mut documents = projection.documents.clone();
    documents.sort_unstable_by_key(|document| document.node_uuid);
    for pair in documents.windows(2) {
        if pair[0].node_uuid == pair[1].node_uuid {
            return Err(build("text projection contains duplicate node UUIDs"));
        }
    }
    for document in &documents {
        checkpoint()?;
        if document
            .fields
            .keys()
            .any(|field| !property_set.contains(field))
        {
            return Err(build(format!(
                "UUID {:02x?} contains an unselected text property",
                document.node_uuid
            )));
        }
    }

    prepare_empty_directory(index_dir)?;
    let (schema, uuid_field, text_fields) = text_schema(&properties, limits)?;
    let index = Index::create_in_dir(index_dir, schema)
        .map_err(|error| build(format!("create Tantivy index: {error}")))?;
    register_text_analyzer(&index);
    let mut writer = index
        .writer_with_num_threads::<TantivyDocument>(1, limits.writer_memory_bytes)
        .map_err(|error| build(format!("create Tantivy writer: {error}")))?;
    writer.set_merge_policy(Box::new(NoMergePolicy));
    for source in &documents {
        checkpoint()?;
        let mut document = TantivyDocument::new();
        document.add_bytes(uuid_field, &source.node_uuid);
        for (name, field) in &text_fields {
            if let Some(value) = source.fields.get(name) {
                document.add_text(*field, value);
            }
        }
        writer
            .add_document(document)
            .map_err(|error| build(format!("add Tantivy document: {error}")))?;
    }
    checkpoint()?;
    writer
        .commit()
        .map_err(|error| build(format!("commit Tantivy index: {error}")))?;
    writer
        .wait_merging_threads()
        .map_err(|error| build(format!("finish Tantivy writer: {error}")))?;
    checkpoint()?;
    let index_bytes = bounded_directory_bytes(index_dir, limits, &mut checkpoint, false)?;
    Ok(TextIndexBuildOutcome::Built {
        documents: documents.len(),
        index_bytes,
    })
}

/// Reopen and fully validate one derived text index.
///
/// Validation checks directory size/traversal, exact schema and tokenizer
/// contract, document count, stored UUID shape, and UUID uniqueness.
///
/// # Errors
/// Any backend/schema/store failure is returned as `CorruptDerivedIndex`;
/// configured limits and cancellation remain distinct.
pub fn validate_text_index<C>(
    index_dir: &Path,
    expected_properties: &[String],
    limits: TextSearchLimits,
    mut checkpoint: C,
) -> Result<(), SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    open_validated(index_dir, expected_properties, limits, &mut checkpoint).map(|_| ())
}

/// Search a fully validated text index using plain OR terms across all fields.
///
/// Every matching document is collected under the configured document bound
/// before canonical sorting. This ensures the final limit cannot cut an
/// arbitrary Tantivy document-address subset of a score tie.
///
/// # Errors
/// Returns structured query, schema/corruption, resource, search, or
/// cancellation errors. No partial hit vector is returned.
pub fn search_text_index<C>(
    index_dir: &Path,
    expected_properties: &[String],
    query: &str,
    limit: usize,
    limits: TextSearchLimits,
    mut checkpoint: C,
) -> Result<Vec<TextSearchHit>, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    checkpoint()?;
    if limit == 0 {
        return Err(invalid("limit", "must be greater than zero"));
    }
    if limit > limits.results {
        return Err(exhausted("text_results", limits.results));
    }
    let tokens = analyze_query(query, limits)?;
    let validated = open_validated(index_dir, expected_properties, limits, &mut checkpoint)?;
    let work = validated
        .documents
        .checked_mul(tokens.len())
        .and_then(|value| value.checked_mul(validated.text_fields.len()))
        .ok_or_else(|| exhausted("text_search_work", limits.search_work))?;
    if work > limits.search_work {
        return Err(exhausted("text_search_work", limits.search_work));
    }

    let queries = validated
        .text_fields
        .iter()
        .flat_map(|(_, field)| {
            tokens.iter().map(move |token| {
                Box::new(TermQuery::new(
                    Term::from_field_text(*field, token),
                    IndexRecordOption::WithFreqs,
                )) as Box<dyn Query>
            })
        })
        .collect();
    let query = BooleanQuery::union(queries);
    checkpoint()?;
    let top_docs = validated
        .searcher
        .search(
            &query,
            &TopDocs::with_limit(validated.documents).order_by_score(),
        )
        .map_err(|error| corrupt(index_dir, format!("search Tantivy index: {error}")))?;
    checkpoint()?;

    let mut hits = Vec::with_capacity(top_docs.len());
    let mut seen = BTreeSet::new();
    for (score, address) in top_docs {
        checkpoint()?;
        if !score.is_finite() {
            return Err(corrupt(index_dir, "Tantivy returned a non-finite score"));
        }
        let document = validated
            .searcher
            .doc::<TantivyDocument>(address)
            .map_err(|error| corrupt(index_dir, format!("read stored UUID: {error}")))?;
        let node_uuid = stored_uuid(index_dir, &document, validated.uuid_field)?;
        if !seen.insert(node_uuid) {
            return Err(corrupt(index_dir, "duplicate UUID search hit"));
        }
        hits.push(TextSearchHit {
            node_uuid,
            score: f64::from(score),
        });
    }
    hits.sort_unstable_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.node_uuid.cmp(&right.node_uuid))
    });
    hits.truncate(limit);
    Ok(hits)
}

struct ValidatedIndex {
    searcher: tantivy::Searcher,
    uuid_field: Field,
    text_fields: Vec<(String, Field)>,
    documents: usize,
}

fn open_validated<C>(
    index_dir: &Path,
    expected_properties: &[String],
    limits: TextSearchLimits,
    checkpoint: &mut C,
) -> Result<ValidatedIndex, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    checkpoint()?;
    bounded_directory_bytes(index_dir, limits, checkpoint, true)?;
    let properties = normalize_properties(expected_properties, limits)?;
    let (expected_schema, uuid_field, text_fields) = text_schema(&properties, limits)?;
    let index = Index::open_in_dir(index_dir)
        .map_err(|error| corrupt(index_dir, format!("open Tantivy index: {error}")))?;
    if index.schema() != expected_schema {
        return Err(corrupt(
            index_dir,
            "Tantivy schema or analyzer contract mismatch",
        ));
    }
    register_text_analyzer(&index);
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .map_err(|error| corrupt(index_dir, format!("open Tantivy reader: {error}")))?;
    let searcher = reader.searcher();
    let documents = usize::try_from(searcher.num_docs())
        .map_err(|_| exhausted("text_documents", limits.documents))?;
    if documents > limits.documents {
        return Err(exhausted("text_documents", limits.documents));
    }
    if documents == 0 {
        return Err(corrupt(index_dir, "Tantivy index contains no documents"));
    }
    checkpoint()?;
    let all_docs = searcher
        .search(&AllQuery, &TopDocs::with_limit(documents).order_by_score())
        .map_err(|error| corrupt(index_dir, format!("validate Tantivy documents: {error}")))?;
    if all_docs.len() != documents {
        return Err(corrupt(index_dir, "Tantivy document count is inconsistent"));
    }
    let mut seen = BTreeSet::new();
    for (_, address) in all_docs {
        checkpoint()?;
        let document = searcher
            .doc::<TantivyDocument>(address)
            .map_err(|error| corrupt(index_dir, format!("read stored document: {error}")))?;
        let node_uuid = stored_uuid(index_dir, &document, uuid_field)?;
        if !seen.insert(node_uuid) {
            return Err(corrupt(index_dir, "Tantivy index contains duplicate UUIDs"));
        }
    }
    Ok(ValidatedIndex {
        searcher,
        uuid_field,
        text_fields,
        documents,
    })
}

fn text_schema(
    properties: &[String],
    limits: TextSearchLimits,
) -> Result<TextSchema, SearchArtifactError> {
    let properties = normalize_properties(properties, limits)?;
    let mut builder = Schema::builder();
    let uuid_field = builder.add_bytes_field(
        NODE_UUID_FIELD,
        BytesOptions::default().set_indexed().set_stored(),
    );
    let options = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer(TEXT_ANALYZER_NAME)
            .set_index_option(IndexRecordOption::WithFreqs),
    );
    let text_fields = properties
        .into_iter()
        .map(|name| {
            let field = builder.add_text_field(&name, options.clone());
            (name, field)
        })
        .collect();
    Ok((builder.build(), uuid_field, text_fields))
}

fn stored_uuid(
    index_dir: &Path,
    document: &TantivyDocument,
    uuid_field: Field,
) -> Result<[u8; 16], SearchArtifactError> {
    let mut values = document.get_all(uuid_field);
    let bytes = values
        .next()
        .and_then(|value| value.as_bytes())
        .ok_or_else(|| corrupt(index_dir, "stored document omits UUID bytes"))?;
    if values.next().is_some() {
        return Err(corrupt(index_dir, "stored document repeats UUID field"));
    }
    bytes
        .try_into()
        .map_err(|_| corrupt(index_dir, "stored UUID is not 16 bytes"))
}

fn prepare_empty_directory(path: &Path) -> Result<(), SearchArtifactError> {
    std::fs::create_dir_all(path).map_err(|source| SearchArtifactError::Io {
        operation: "create text index directory",
        path: path.to_path_buf(),
        source,
    })?;
    let mut entries = std::fs::read_dir(path).map_err(|source| SearchArtifactError::Io {
        operation: "inspect text index directory",
        path: path.to_path_buf(),
        source,
    })?;
    if entries
        .next()
        .transpose()
        .map_err(|source| SearchArtifactError::Io {
            operation: "inspect text index directory",
            path: path.to_path_buf(),
            source,
        })?
        .is_some()
    {
        return Err(build("text index build directory is not empty"));
    }
    Ok(())
}

fn bounded_directory_bytes<C>(
    root: &Path,
    limits: TextSearchLimits,
    checkpoint: &mut C,
    corrupt_on_io: bool,
) -> Result<u64, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    fn visit<C>(
        root: &Path,
        path: &Path,
        total: &mut u64,
        limits: TextSearchLimits,
        checkpoint: &mut C,
        corrupt_on_io: bool,
    ) -> Result<(), SearchArtifactError>
    where
        C: FnMut() -> Result<(), SearchArtifactError>,
    {
        checkpoint()?;
        let entries = std::fs::read_dir(path).map_err(|error| {
            if corrupt_on_io {
                corrupt(
                    root,
                    format!("read index directory {}: {error}", path.display()),
                )
            } else {
                SearchArtifactError::Io {
                    operation: "measure text index",
                    path: path.to_path_buf(),
                    source: error,
                }
            }
        })?;
        for entry in entries {
            checkpoint()?;
            let entry = entry.map_err(|error| {
                if corrupt_on_io {
                    corrupt(root, format!("read index entry: {error}"))
                } else {
                    SearchArtifactError::Io {
                        operation: "measure text index",
                        path: path.to_path_buf(),
                        source: error,
                    }
                }
            })?;
            let entry_path = entry.path();
            let metadata = std::fs::symlink_metadata(&entry_path).map_err(|error| {
                if corrupt_on_io {
                    corrupt(
                        root,
                        format!("inspect index entry {}: {error}", entry_path.display()),
                    )
                } else {
                    SearchArtifactError::Io {
                        operation: "measure text index",
                        path: entry_path.clone(),
                        source: error,
                    }
                }
            })?;
            if metadata.file_type().is_symlink() {
                return Err(corrupt(root, "Tantivy index contains a symbolic link"));
            }
            if metadata.is_dir() {
                visit(root, &entry_path, total, limits, checkpoint, corrupt_on_io)?;
            } else if metadata.is_file() {
                *total = total
                    .checked_add(metadata.len())
                    .ok_or_else(|| exhausted_u64("text_index_bytes", limits.index_bytes))?;
                if *total > limits.index_bytes {
                    return Err(exhausted_u64("text_index_bytes", limits.index_bytes));
                }
            } else {
                return Err(corrupt(root, "Tantivy index contains an unsupported entry"));
            }
        }
        Ok(())
    }

    let mut total = 0_u64;
    visit(root, root, &mut total, limits, checkpoint, corrupt_on_io)?;
    Ok(total)
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

fn build(reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::Build(reason.into())
}

fn corrupt(path: &Path, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::CorruptDerivedIndex {
        path: path.to_path_buf(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::BTreeMap;

    use tempfile::TempDir;

    use super::*;
    use crate::source::TextDocument;

    fn uuid(value: u8) -> [u8; 16] {
        let mut bytes = [0_u8; 16];
        bytes[15] = value;
        bytes
    }

    fn projection(documents: Vec<TextDocument>) -> TextSourceProjection {
        TextSourceProjection {
            properties: vec!["body".to_owned(), "title".to_owned()],
            documents,
            source_bytes: 0,
        }
    }

    fn document(value: u8, title: &str, body: &str) -> TextDocument {
        TextDocument {
            node_uuid: uuid(value),
            fields: BTreeMap::from([
                ("body".to_owned(), body.to_owned()),
                ("title".to_owned(), title.to_owned()),
            ]),
        }
    }

    #[test]
    fn build_search_reopen_or_semantics_and_uuid_ties_are_deterministic() {
        let dir = TempDir::new().unwrap();
        let index_dir = dir.path().join("index");
        let source = projection(vec![
            document(2, "Alpha", "shared term"),
            document(3, "Gamma", "other"),
            document(1, "Alpha", "shared term"),
        ]);
        assert!(matches!(
            build_text_index(&index_dir, &source, TextSearchLimits::default(), || Ok(())).unwrap(),
            TextIndexBuildOutcome::Built { documents: 3, .. }
        ));
        validate_text_index(
            &index_dir,
            &source.properties,
            TextSearchLimits::default(),
            || Ok(()),
        )
        .unwrap();
        let hits = search_text_index(
            &index_dir,
            &source.properties,
            "title:ALPHA | gamma",
            3,
            TextSearchLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(
            hits.iter().map(|hit| hit.node_uuid[15]).collect::<Vec<_>>(),
            [3, 1, 2]
        );
        assert!(hits.iter().all(|hit| hit.score.is_finite()));

        let tie_hits = search_text_index(
            &index_dir,
            &source.properties,
            "shared",
            1,
            TextSearchLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(tie_hits[0].node_uuid, uuid(1));
    }

    #[test]
    fn empty_projection_does_not_create_an_ambiguous_index() {
        let dir = TempDir::new().unwrap();
        let index_dir = dir.path().join("index");
        let source = TextSourceProjection {
            properties: vec!["title".to_owned()],
            documents: Vec::new(),
            source_bytes: 0,
        };
        assert_eq!(
            build_text_index(&index_dir, &source, TextSearchLimits::default(), || Ok(())).unwrap(),
            TextIndexBuildOutcome::Empty
        );
        assert!(!index_dir.exists());
    }

    #[test]
    fn schema_mismatch_corruption_limits_and_cancellation_are_structured() {
        let dir = TempDir::new().unwrap();
        let index_dir = dir.path().join("index");
        let source = projection(vec![document(1, "Alpha", "body")]);
        build_text_index(&index_dir, &source, TextSearchLimits::default(), || Ok(())).unwrap();

        assert!(matches!(
            validate_text_index(
                &index_dir,
                &["other".to_owned()],
                TextSearchLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::CorruptDerivedIndex { .. })
        ));
        let mut limits = TextSearchLimits::default();
        limits.search_work = 0;
        assert!(matches!(
            search_text_index(
                &index_dir,
                &source.properties,
                "alpha",
                1,
                limits,
                || Ok(())
            ),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "text_search_work",
                ..
            })
        ));
        assert!(matches!(
            search_text_index(
                &index_dir,
                &source.properties,
                "alpha",
                1,
                TextSearchLimits::default(),
                || Err(SearchArtifactError::Cancelled)
            ),
            Err(SearchArtifactError::Cancelled)
        ));

        std::fs::remove_file(index_dir.join("meta.json")).unwrap();
        assert!(matches!(
            validate_text_index(
                &index_dir,
                &source.properties,
                TextSearchLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::CorruptDerivedIndex { .. })
        ));
    }

    #[test]
    fn build_checks_work_writer_memory_and_midstream_cancellation() {
        let dir = TempDir::new().unwrap();
        let source = projection(vec![
            document(1, "Alpha", "one"),
            document(2, "Beta", "two"),
        ]);
        let mut limits = TextSearchLimits::default();
        limits.writer_memory_bytes = TANTIVY_MIN_WRITER_MEMORY_BYTES - 1;
        assert!(matches!(
            build_text_index(&dir.path().join("small"), &source, limits, || Ok(())),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "text_writer_memory_bytes",
                ..
            })
        ));

        let mut limits = TextSearchLimits::default();
        limits.build_work = 3;
        assert!(matches!(
            build_text_index(&dir.path().join("work"), &source, limits, || Ok(())),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "text_build_work",
                ..
            })
        ));

        let mut oversized_source = source.clone();
        oversized_source.source_bytes = 1;
        let mut limits = TextSearchLimits::default();
        limits.source_bytes = 0;
        assert!(matches!(
            build_text_index(
                &dir.path().join("source-bytes"),
                &oversized_source,
                limits,
                || Ok(())
            ),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "text_source_bytes",
                ..
            })
        ));

        let calls = Cell::new(0_usize);
        assert!(matches!(
            build_text_index(
                &dir.path().join("cancelled"),
                &source,
                TextSearchLimits::default(),
                || {
                    calls.set(calls.get() + 1);
                    if calls.get() >= 4 {
                        Err(SearchArtifactError::Cancelled)
                    } else {
                        Ok(())
                    }
                }
            ),
            Err(SearchArtifactError::Cancelled)
        ));
    }

    #[test]
    fn build_rejects_duplicate_identities_and_unselected_fields_before_writing() {
        let dir = TempDir::new().unwrap();
        let duplicate = projection(vec![
            document(1, "Alpha", "one"),
            document(1, "Beta", "two"),
        ]);
        let duplicate_dir = dir.path().join("duplicate");
        assert!(matches!(
            build_text_index(
                &duplicate_dir,
                &duplicate,
                TextSearchLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::Build(_))
        ));
        assert!(!duplicate_dir.exists());

        let mut unselected = projection(vec![document(2, "Gamma", "three")]);
        unselected.documents[0]
            .fields
            .insert("private".to_owned(), "must not index".to_owned());
        let unselected_dir = dir.path().join("unselected");
        assert!(matches!(
            build_text_index(
                &unselected_dir,
                &unselected,
                TextSearchLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::Build(_))
        ));
        assert!(!unselected_dir.exists());
    }

    #[test]
    fn search_validates_result_bounds_query_and_document_budget() {
        let dir = TempDir::new().unwrap();
        let index_dir = dir.path().join("index");
        let source = projection(vec![document(1, "Alpha", "body")]);
        build_text_index(&index_dir, &source, TextSearchLimits::default(), || Ok(())).unwrap();

        assert!(matches!(
            search_text_index(
                &index_dir,
                &source.properties,
                "alpha",
                0,
                TextSearchLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::InvalidSelector { field: "limit", .. })
        ));
        let mut limits = TextSearchLimits::default();
        limits.results = 1;
        assert!(matches!(
            search_text_index(
                &index_dir,
                &source.properties,
                "alpha",
                2,
                limits,
                || Ok(())
            ),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "text_results",
                limit: 1,
            })
        ));
        assert!(matches!(
            search_text_index(
                &index_dir,
                &source.properties,
                "!!!",
                1,
                TextSearchLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::InvalidSelector { field: "query", .. })
        ));

        limits = TextSearchLimits::default();
        limits.documents = 0;
        assert!(matches!(
            validate_text_index(&index_dir, &source.properties, limits, || Ok(())),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "text_documents",
                limit: 0,
            })
        ));
    }

    #[test]
    fn stored_uuid_requires_one_exact_sixteen_byte_value() {
        let dir = TempDir::new().unwrap();
        let mut builder = Schema::builder();
        let uuid_field =
            builder.add_bytes_field(NODE_UUID_FIELD, BytesOptions::default().set_stored());

        let omitted = TantivyDocument::new();
        assert!(matches!(
            stored_uuid(dir.path(), &omitted, uuid_field),
            Err(SearchArtifactError::CorruptDerivedIndex { .. })
        ));

        let mut short = TantivyDocument::new();
        short.add_bytes(uuid_field, &[1_u8; 15]);
        assert!(matches!(
            stored_uuid(dir.path(), &short, uuid_field),
            Err(SearchArtifactError::CorruptDerivedIndex { .. })
        ));

        let mut repeated = TantivyDocument::new();
        repeated.add_bytes(uuid_field, &[1_u8; 16]);
        repeated.add_bytes(uuid_field, &[2_u8; 16]);
        assert!(matches!(
            stored_uuid(dir.path(), &repeated, uuid_field),
            Err(SearchArtifactError::CorruptDerivedIndex { .. })
        ));
    }

    #[test]
    fn missing_index_directory_preserves_build_vs_corruption_error_boundary() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("missing");
        for corrupt_on_io in [false, true] {
            let error = bounded_directory_bytes(
                &missing,
                TextSearchLimits::default(),
                &mut || Ok(()),
                corrupt_on_io,
            )
            .unwrap_err();
            if corrupt_on_io {
                assert!(matches!(
                    error,
                    SearchArtifactError::CorruptDerivedIndex { .. }
                ));
            } else {
                assert!(matches!(error, SearchArtifactError::Io { .. }));
            }
        }
    }

    #[test]
    fn index_directory_measurement_is_recursive_bounded_and_non_mutating() {
        let root = TempDir::new().unwrap();
        let nested = root.path().join("nested");
        std::fs::create_dir(&nested).unwrap();
        std::fs::write(root.path().join("a"), b"12").unwrap();
        std::fs::write(nested.join("b"), b"345").unwrap();
        assert_eq!(
            bounded_directory_bytes(
                root.path(),
                TextSearchLimits::default(),
                &mut || Ok(()),
                false,
            )
            .unwrap(),
            5
        );
        let mut limits = TextSearchLimits::default();
        limits.index_bytes = 4;
        assert!(matches!(
            bounded_directory_bytes(root.path(), limits, &mut || Ok(()), false),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "text_index_bytes",
                limit: 4,
            })
        ));
        assert_eq!(std::fs::read(nested.join("b")).unwrap(), b"345");
        assert!(matches!(
            bounded_directory_bytes(
                root.path(),
                TextSearchLimits::default(),
                &mut || Err(SearchArtifactError::Cancelled),
                false,
            ),
            Err(SearchArtifactError::Cancelled)
        ));
    }

    #[test]
    fn build_directory_and_symlink_entries_fail_closed_without_cleanup() {
        let root = TempDir::new().unwrap();
        let nonempty = root.path().join("nonempty");
        std::fs::create_dir(&nonempty).unwrap();
        std::fs::write(nonempty.join("keep"), b"caller").unwrap();
        assert!(matches!(
            prepare_empty_directory(&nonempty),
            Err(SearchArtifactError::Build(_))
        ));
        assert_eq!(std::fs::read(nonempty.join("keep")).unwrap(), b"caller");

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let linked = root.path().join("linked");
            std::fs::create_dir(&linked).unwrap();
            symlink(root.path(), linked.join("escape")).unwrap();
            for corrupt_on_io in [false, true] {
                assert!(matches!(
                    bounded_directory_bytes(
                        &linked,
                        TextSearchLimits::default(),
                        &mut || Ok(()),
                        corrupt_on_io,
                    ),
                    Err(SearchArtifactError::CorruptDerivedIndex { .. })
                ));
                assert!(
                    linked
                        .join("escape")
                        .symlink_metadata()
                        .unwrap()
                        .file_type()
                        .is_symlink()
                );
            }
        }
    }
}
