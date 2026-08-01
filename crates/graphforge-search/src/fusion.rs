//! Deterministic reciprocal-rank fusion over canonical UUID search hits.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use graphforge_storage::SearchArtifactError;

/// Fixed v0.5 reciprocal-rank-fusion constant.
pub const RRF_RANK_CONSTANT: u32 = 60;
/// Maximum public search result and per-channel candidate depth.
pub const MAX_FUSION_RESULTS: usize = 10_000;

/// One already-ranked hit from a text or vector backend.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SearchChannelHit {
    /// Stable graph UUID identity.
    pub node_uuid: [u8; 16],
    /// Finite backend score used only to validate canonical channel order.
    pub score: f64,
}

/// Canonical channel membership exposed by unified search.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchedOn {
    /// Tantivy text channel only.
    Text,
    /// Exact cosine vector channel only.
    Vector,
    /// UUID appeared in both channels.
    TextAndVector,
}

impl MatchedOn {
    /// Stable public Arrow/string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Vector => "vector",
            Self::TextAndVector => "text+vector",
        }
    }
}

/// One fused UUID result before graph-property projection and Arrow shaping.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FusedSearchHit {
    /// Stable graph UUID identity.
    pub node_uuid: [u8; 16],
    /// Finite `rrf@1` score.
    pub score: f64,
    /// Channel or overlap that contributed to this result.
    pub matched_on: MatchedOn,
}

#[derive(Clone, Copy, Debug, Default)]
struct Contributions {
    score: f64,
    text: bool,
    vector: bool,
}

/// Fuse canonical text and vector candidate lists using v0.5 `rrf@1`.
///
/// Each input must already be ordered by descending finite backend score then
/// ascending UUID bytes. Ranks are one-based and each channel may contain at
/// most `limit` unique UUIDs. The fused union is ordered by descending RRF
/// score then ascending UUID bytes and truncated to `limit`.
///
/// # Errors
/// Returns structured validation or resource errors for an invalid limit,
/// duplicate/non-finite/non-canonical channel input, or excess channel depth.
pub fn reciprocal_rank_fusion(
    text_hits: &[SearchChannelHit],
    vector_hits: &[SearchChannelHit],
    limit: usize,
) -> Result<Vec<FusedSearchHit>, SearchArtifactError> {
    validate_limit(limit)?;
    validate_channel(text_hits, "text_hits", limit)?;
    validate_channel(vector_hits, "vector_hits", limit)?;

    let mut fused = BTreeMap::<[u8; 16], Contributions>::new();
    add_channel(&mut fused, text_hits, true);
    add_channel(&mut fused, vector_hits, false);

    let mut hits = fused
        .into_iter()
        .map(|(node_uuid, contribution)| FusedSearchHit {
            node_uuid,
            score: contribution.score,
            matched_on: match (contribution.text, contribution.vector) {
                (true, false) => MatchedOn::Text,
                (false, true) => MatchedOn::Vector,
                (true, true) => MatchedOn::TextAndVector,
                (false, false) => unreachable!("stored contribution has a channel"),
            },
        })
        .collect::<Vec<_>>();
    hits.sort_unstable_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.node_uuid.cmp(&right.node_uuid))
    });
    hits.truncate(limit);
    Ok(hits)
}

fn validate_limit(limit: usize) -> Result<(), SearchArtifactError> {
    if limit == 0 {
        return Err(invalid("limit", "must be greater than zero"));
    }
    if limit > MAX_FUSION_RESULTS {
        return Err(SearchArtifactError::ResourceExhausted {
            resource: "search_results",
            limit: MAX_FUSION_RESULTS as u64,
        });
    }
    Ok(())
}

fn validate_channel(
    hits: &[SearchChannelHit],
    field: &'static str,
    limit: usize,
) -> Result<(), SearchArtifactError> {
    if hits.len() > limit {
        return Err(SearchArtifactError::ResourceExhausted {
            resource: "search_channel_candidates",
            limit: limit as u64,
        });
    }
    let mut seen = BTreeSet::new();
    for hit in hits {
        if !hit.score.is_finite() {
            return Err(invalid(field, "scores must be finite"));
        }
        if !seen.insert(hit.node_uuid) {
            return Err(invalid(field, "UUIDs must be unique within a channel"));
        }
    }
    if hits
        .windows(2)
        .any(|pair| compare_channel_hits(&pair[0], &pair[1]) == Ordering::Greater)
    {
        return Err(invalid(
            field,
            "hits must be ordered by descending score then ascending UUID bytes",
        ));
    }
    Ok(())
}

fn compare_channel_hits(left: &SearchChannelHit, right: &SearchChannelHit) -> Ordering {
    right
        .score
        .total_cmp(&left.score)
        .then_with(|| left.node_uuid.cmp(&right.node_uuid))
}

fn add_channel(
    fused: &mut BTreeMap<[u8; 16], Contributions>,
    hits: &[SearchChannelHit],
    text: bool,
) {
    for (index, hit) in hits.iter().enumerate() {
        let rank = u32::try_from(index + 1).expect("validated candidate depth fits u32");
        let contribution = 1.0 / f64::from(RRF_RANK_CONSTANT + rank);
        let entry = fused.entry(hit.node_uuid).or_default();
        entry.score += contribution;
        if text {
            entry.text = true;
        } else {
            entry.vector = true;
        }
    }
}

fn invalid(field: &'static str, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::InvalidSelector {
        field,
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uuid(last: u8) -> [u8; 16] {
        let mut value = [0_u8; 16];
        value[15] = last;
        value
    }

    fn hit(last: u8, score: f64) -> SearchChannelHit {
        SearchChannelHit {
            node_uuid: uuid(last),
            score,
        }
    }

    #[test]
    fn fuses_exact_ranks_overlap_and_negative_channel_scores() {
        let hits = reciprocal_rank_fusion(
            &[hit(1, 9.0), hit(2, 8.0)],
            &[hit(2, 0.25), hit(3, -0.5)],
            3,
        )
        .unwrap();

        assert_eq!(
            hits,
            vec![
                FusedSearchHit {
                    node_uuid: uuid(2),
                    score: 1.0 / 62.0 + 1.0 / 61.0,
                    matched_on: MatchedOn::TextAndVector,
                },
                FusedSearchHit {
                    node_uuid: uuid(1),
                    score: 1.0 / 61.0,
                    matched_on: MatchedOn::Text,
                },
                FusedSearchHit {
                    node_uuid: uuid(3),
                    score: 1.0 / 62.0,
                    matched_on: MatchedOn::Vector,
                },
            ]
        );
    }

    #[test]
    fn equal_fused_scores_break_by_uuid_and_empty_inputs_are_stable() {
        let hits = reciprocal_rank_fusion(&[hit(2, 1.0)], &[hit(1, -1.0)], 2).unwrap();
        assert_eq!(
            hits.iter().map(|hit| hit.node_uuid).collect::<Vec<_>>(),
            [uuid(1), uuid(2)]
        );
        assert_eq!(hits[0].matched_on.as_str(), "vector");
        assert_eq!(hits[1].matched_on.as_str(), "text");
        assert!(reciprocal_rank_fusion(&[], &[], 10).unwrap().is_empty());
    }

    #[test]
    fn truncates_a_disjoint_channel_union_to_the_requested_limit() {
        let hits =
            reciprocal_rank_fusion(&[hit(1, 4.0), hit(2, 3.0)], &[hit(3, 2.0), hit(4, 1.0)], 2)
                .unwrap();

        assert_eq!(
            hits.iter().map(|hit| hit.node_uuid).collect::<Vec<_>>(),
            [uuid(1), uuid(3)]
        );
    }

    #[test]
    fn validates_limits_depth_scores_duplicates_and_channel_order() {
        assert!(matches!(
            reciprocal_rank_fusion(&[], &[], 0),
            Err(SearchArtifactError::InvalidSelector { field: "limit", .. })
        ));
        assert!(matches!(
            reciprocal_rank_fusion(&[], &[], MAX_FUSION_RESULTS + 1),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "search_results",
                ..
            })
        ));
        assert!(matches!(
            reciprocal_rank_fusion(&[hit(1, 2.0), hit(2, 1.0)], &[], 1),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "search_channel_candidates",
                ..
            })
        ));
        for invalid_hits in [
            vec![hit(1, f64::NAN)],
            vec![hit(1, 2.0), hit(1, 1.0)],
            vec![hit(1, 1.0), hit(2, 2.0)],
            vec![hit(2, 1.0), hit(1, 1.0)],
        ] {
            assert!(matches!(
                reciprocal_rank_fusion(&invalid_hits, &[], invalid_hits.len()),
                Err(SearchArtifactError::InvalidSelector { .. })
            ));
        }
    }
}
