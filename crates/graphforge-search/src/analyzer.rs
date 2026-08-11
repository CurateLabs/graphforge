//! Canonical search text analysis and plain-query validation.

use std::collections::BTreeSet;

use graphforge_storage::SearchArtifactError;
use tantivy::Index;
use tantivy::tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer, TokenStream};

use crate::TextSearchLimits;

/// Registered Tantivy tokenizer name for the canonical v0.5 contract.
pub const TEXT_ANALYZER_NAME: &str = "graphforge_text_v1";
/// Persisted analyzer, query, BM25, and ordering contract version.
pub const TEXT_CONTRACT_VERSION: &str = "graphforge_text_v1";

/// Construct the canonical Unicode-alphanumeric, lowercase-only analyzer.
#[must_use]
pub fn graphforge_text_analyzer() -> TextAnalyzer {
    TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(LowerCaser)
        .build()
}

/// Register the canonical analyzer on a newly created or reopened index.
pub fn register_text_analyzer(index: &Index) {
    index
        .tokenizers()
        .register(TEXT_ANALYZER_NAME, graphforge_text_analyzer());
}

/// Analyze a public plain-text query into unique canonical OR terms.
///
/// Tantivy query syntax is never parsed: punctuation only delimits tokens.
/// The resource token count is checked before duplicate terms are collapsed.
///
/// # Errors
/// Rejects oversized or zero-token queries and token-limit exhaustion.
pub fn analyze_query(
    query: &str,
    limits: TextSearchLimits,
) -> Result<Vec<String>, SearchArtifactError> {
    if query.len() > limits.query_bytes {
        return Err(exhausted("text_query_bytes", limits.query_bytes));
    }
    let mut analyzer = graphforge_text_analyzer();
    let mut stream = analyzer.token_stream(query);
    let mut count = 0_usize;
    let mut tokens = BTreeSet::new();
    while stream.advance() {
        count = count.saturating_add(1);
        if count > limits.query_tokens {
            return Err(exhausted("text_query_tokens", limits.query_tokens));
        }
        tokens.insert(stream.token().text.clone());
    }
    if tokens.is_empty() {
        return Err(SearchArtifactError::InvalidSelector {
            field: "query",
            reason: "analysis produced no alphanumeric tokens".to_owned(),
        });
    }
    Ok(tokens.into_iter().collect())
}

fn exhausted(resource: &'static str, limit: usize) -> SearchArtifactError {
    SearchArtifactError::ResourceExhausted {
        resource,
        limit: u64::try_from(limit).unwrap_or(u64::MAX),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_case_and_punctuation_follow_v1_contract() {
        let tokens = analyze_query(
            "ÉCOLE—Straße_東京 title:ALICE OR *",
            TextSearchLimits::default(),
        )
        .unwrap();
        assert_eq!(tokens, ["alice", "or", "straße", "title", "école", "東京"]);
    }

    #[test]
    fn repeated_terms_deduplicate_after_resource_accounting() {
        let mut limits = TextSearchLimits::default();
        limits.query_tokens = 3;
        assert_eq!(analyze_query("A a B", limits).unwrap(), ["a", "b"]);
        assert!(matches!(
            analyze_query("A a B b", limits),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "text_query_tokens",
                ..
            })
        ));
    }

    #[test]
    fn malformed_and_oversized_queries_are_structured() {
        assert!(matches!(
            analyze_query("---", TextSearchLimits::default()),
            Err(SearchArtifactError::InvalidSelector { field: "query", .. })
        ));
        let mut limits = TextSearchLimits::default();
        limits.query_bytes = 2;
        assert!(matches!(
            analyze_query("abc", limits),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "text_query_bytes",
                ..
            })
        ));
    }
}
