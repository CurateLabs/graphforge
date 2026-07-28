//! Central normalization and validation for embedding-v1 invocations.

use std::collections::HashSet;

use gf_core::{
    GfError,
    algorithms::AnalyzeAlgorithm,
    embedding_options::{
        EmbeddingAnalyzeOptions, EmbeddingOptions, FastRpOptions, GraphSageOptions, HashGnnOptions,
        MAX_EMBEDDING_DIMENSIONS, MAX_HASHGNN_DIMENSIONS, Node2VecOptions,
    },
};

/// A deterministic graph-native embedding invocation descriptor.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NormalizedEmbeddingOptions {
    pub(crate) algorithm_version: &'static str,
    pub(crate) via: Option<String>,
    pub(crate) directed: bool,
    pub(crate) weight: Option<String>,
    pub(crate) options: EmbeddingOptions,
}

impl NormalizedEmbeddingOptions {
    pub(crate) fn dimensions(&self) -> usize {
        match &self.options {
            EmbeddingOptions::Node2Vec(value) => value.dimensions,
            EmbeddingOptions::GraphSage(value) => value.dimensions,
            EmbeddingOptions::FastRandomProjection(value) => value.dimensions,
            EmbeddingOptions::HashGnn(value) => value.dimensions,
        }
    }

    pub(crate) fn seed(&self) -> u64 {
        match &self.options {
            EmbeddingOptions::Node2Vec(value) => value.seed,
            EmbeddingOptions::GraphSage(value) => value.seed,
            EmbeddingOptions::FastRandomProjection(value) => value.seed,
            EmbeddingOptions::HashGnn(value) => value.seed,
        }
    }
}

pub(crate) fn normalize_embedding_options(
    invocation: &EmbeddingAnalyzeOptions,
) -> Result<NormalizedEmbeddingOptions, GfError> {
    let version = match invocation.by {
        AnalyzeAlgorithm::Node2Vec => "node2vec-v1",
        AnalyzeAlgorithm::GraphSage => "graphsage-unsupervised-v1",
        AnalyzeAlgorithm::FastRandomProjection => "fastrp-v1",
        AnalyzeAlgorithm::HashGnn => "hashgnn-v1",
        _ => return validation(format!("{} is not an embedding algorithm", invocation.by)),
    };
    let options = &invocation.options;
    validate_variant(invocation.by, options)?;
    validate_selector("relationship type", invocation.via.as_deref())?;
    validate_selector("edge weight property", invocation.weight.as_deref())?;
    validate_projection(invocation, options)?;
    validate_options(options)?;
    Ok(NormalizedEmbeddingOptions {
        algorithm_version: version,
        via: invocation.via.clone(),
        directed: invocation.directed,
        weight: invocation.weight.clone(),
        options: options.clone(),
    })
}

/// Validate one typed embedding invocation without activating a kernel.
///
/// This is the language-binding boundary: it delegates to the same
/// normalization path used by future execution handlers, but deliberately
/// discards the internal descriptor.
pub fn validate_embedding_options(invocation: &EmbeddingAnalyzeOptions) -> Result<(), GfError> {
    normalize_embedding_options(invocation).map(|_| ())
}

fn validate_variant(by: AnalyzeAlgorithm, options: &EmbeddingOptions) -> Result<(), GfError> {
    let matches = matches!(
        (by, options),
        (AnalyzeAlgorithm::Node2Vec, EmbeddingOptions::Node2Vec(_))
            | (AnalyzeAlgorithm::GraphSage, EmbeddingOptions::GraphSage(_))
            | (
                AnalyzeAlgorithm::FastRandomProjection,
                EmbeddingOptions::FastRandomProjection(_)
            )
            | (AnalyzeAlgorithm::HashGnn, EmbeddingOptions::HashGnn(_))
    );
    if matches {
        Ok(())
    } else {
        validation(format!(
            "{by} received options for another embedding algorithm"
        ))
    }
}

fn validate_projection(
    invocation: &EmbeddingAnalyzeOptions,
    options: &EmbeddingOptions,
) -> Result<(), GfError> {
    match options {
        EmbeddingOptions::GraphSage(_) if invocation.directed => {
            validation("graphsage requires directed=false")
        }
        EmbeddingOptions::GraphSage(_) if invocation.weight.is_some() => {
            validation("graphsage does not accept an edge weight property")
        }
        EmbeddingOptions::HashGnn(_) if invocation.weight.is_some() => {
            validation("hashgnn does not accept an edge weight property")
        }
        _ => Ok(()),
    }
}

fn validate_options(options: &EmbeddingOptions) -> Result<(), GfError> {
    match options {
        EmbeddingOptions::Node2Vec(value) => validate_node2vec(value),
        EmbeddingOptions::GraphSage(value) => validate_graphsage(value),
        EmbeddingOptions::FastRandomProjection(value) => validate_fastrp(value),
        EmbeddingOptions::HashGnn(value) => validate_hashgnn(value),
    }
}

fn validate_node2vec(options: &Node2VecOptions) -> Result<(), GfError> {
    validate_dimensions(options.dimensions, MAX_EMBEDDING_DIMENSIONS)?;
    for (name, value) in [
        ("walk_length", options.walk_length),
        ("walks_per_node", options.walks_per_node),
        ("window_size", options.window_size),
        ("negative_samples", options.negative_samples),
        ("epochs", options.epochs),
    ] {
        positive_count("node2vec", name, value)?;
    }
    positive_finite("node2vec", "p", options.p)?;
    positive_finite("node2vec", "q", options.q)?;
    positive_finite("node2vec", "learning_rate", options.learning_rate)
}

fn validate_graphsage(options: &GraphSageOptions) -> Result<(), GfError> {
    validate_dimensions(options.dimensions, MAX_EMBEDDING_DIMENSIONS)?;
    validate_dimensions(options.hidden_dimensions, MAX_EMBEDDING_DIMENSIONS)?;
    positive_count("graphsage", "layers", options.layers)?;
    positive_count("graphsage", "epochs", options.epochs)?;
    positive_count("graphsage", "negative_samples", options.negative_samples)?;
    positive_finite("graphsage", "learning_rate", options.learning_rate)?;
    if options.sample_sizes.len() != options.layers {
        return validation("graphsage sample_sizes length must equal layers");
    }
    if options.sample_sizes.contains(&0) {
        return validation("graphsage sample_sizes must all be greater than zero");
    }
    validate_properties(
        "graphsage feature_properties",
        &options.feature_properties,
        true,
    )
}

fn validate_fastrp(options: &FastRpOptions) -> Result<(), GfError> {
    validate_dimensions(options.dimensions, MAX_EMBEDDING_DIMENSIONS)?;
    if options.iteration_weights.is_empty() || options.iteration_weights.len() > 65 {
        return validation("fast_random_projection requires 1 to 65 iteration_weights");
    }
    if options
        .iteration_weights
        .iter()
        .any(|value| !value.is_finite())
    {
        return validation("fast_random_projection iteration_weights must be finite");
    }
    if options.iteration_weights.iter().all(|value| *value == 0.0) {
        return validation("fast_random_projection iteration_weights cannot all be zero");
    }
    if !options.normalization_strength.is_finite() {
        return validation("fast_random_projection normalization_strength must be finite");
    }
    if !options.feature_weight.is_finite() || options.feature_weight < 0.0 {
        return validation("fast_random_projection feature_weight must be finite and nonnegative");
    }
    validate_properties(
        "fast_random_projection feature_properties",
        &options.feature_properties,
        false,
    )?;
    if options.feature_weight > 0.0 && options.feature_properties.is_empty() {
        return validation(
            "fast_random_projection positive feature_weight requires feature_properties",
        );
    }
    Ok(())
}

fn validate_hashgnn(options: &HashGnnOptions) -> Result<(), GfError> {
    validate_dimensions(options.dimensions, MAX_HASHGNN_DIMENSIONS)?;
    if options.iterations > 64 {
        return validation("hashgnn iterations must be at most 64");
    }
    if !options.embedding_density.is_finite()
        || options.embedding_density <= 0.0
        || options.embedding_density > 1.0
    {
        return validation("hashgnn embedding_density must be finite and in (0, 1]");
    }
    if options.heterogeneous {
        validate_selector(
            "hashgnn node_type_property",
            options.node_type_property.as_deref(),
        )?;
        validate_selector(
            "hashgnn relationship_type_property",
            options.relationship_type_property.as_deref(),
        )?;
        if options.node_type_property.is_none() || options.relationship_type_property.is_none() {
            return validation(
                "heterogeneous hashgnn requires node_type_property and relationship_type_property",
            );
        }
    } else if options.node_type_property.is_some() || options.relationship_type_property.is_some() {
        return validation("homogeneous hashgnn does not accept type properties");
    }
    Ok(())
}

fn validate_dimensions(dimensions: usize, maximum: usize) -> Result<(), GfError> {
    if dimensions == 0 || dimensions > maximum {
        validation(format!(
            "embedding dimensions must be between 1 and {maximum}"
        ))
    } else {
        Ok(())
    }
}

fn positive_count(algorithm: &str, name: &str, value: usize) -> Result<(), GfError> {
    if value == 0 {
        validation(format!("{algorithm} {name} must be greater than zero"))
    } else {
        Ok(())
    }
}

fn positive_finite(algorithm: &str, name: &str, value: f64) -> Result<(), GfError> {
    if value.is_finite() && value > 0.0 {
        Ok(())
    } else {
        validation(format!("{algorithm} {name} must be finite and positive"))
    }
}

fn validate_properties(name: &str, properties: &[String], required: bool) -> Result<(), GfError> {
    if required && properties.is_empty() {
        return validation(format!("{name} must be a non-empty ordered list"));
    }
    let mut unique = HashSet::with_capacity(properties.len());
    for property in properties {
        if property.trim().is_empty() {
            return validation(format!("{name} cannot contain blank names"));
        }
        if is_knowledge_selector(property) {
            return validation(format!(
                "{name} cannot select knowledge-layer field {property}"
            ));
        }
        if !unique.insert(property.as_str()) {
            return validation(format!("{name} cannot contain duplicate names"));
        }
    }
    Ok(())
}

fn validate_selector(name: &str, selector: Option<&str>) -> Result<(), GfError> {
    if let Some(value) = selector {
        if value.trim().is_empty() {
            return validation(format!("{name} must be nonblank"));
        }
        if is_knowledge_selector(value) {
            return validation(format!(
                "{name} cannot select knowledge-layer field {value}"
            ));
        }
    }
    Ok(())
}

fn is_knowledge_selector(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "provenance" | "evidence" | "belief" | "hypothesis" | "valid_time" | "as_of"
    )
}

fn validation<T>(message: impl Into<String>) -> Result<T, GfError> {
    Err(GfError::Validation(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use gf_core::embedding_options::{GraphSageAggregator, HashGnnOptions};

    fn invocation(by: AnalyzeAlgorithm, options: EmbeddingOptions) -> EmbeddingAnalyzeOptions {
        EmbeddingAnalyzeOptions {
            by,
            via: Some("LINK".into()),
            directed: false,
            weight: None,
            options,
        }
    }

    #[test]
    fn all_defaults_are_exact_and_normalize_deterministically() {
        let node2vec = Node2VecOptions::default();
        assert_eq!(
            (
                node2vec.dimensions,
                node2vec.walk_length,
                node2vec.walks_per_node,
                node2vec.p,
                node2vec.q,
                node2vec.window_size,
                node2vec.negative_samples,
                node2vec.epochs,
                node2vec.learning_rate,
                node2vec.seed,
            ),
            (128, 80, 10, 1.0, 1.0, 10, 5, 1, 0.025, 0)
        );
        let fastrp = FastRpOptions::default();
        assert_eq!(fastrp.iteration_weights, [0.0, 1.0, 1.0]);
        assert_eq!(fastrp.normalization_strength, 0.0);
        assert_eq!(fastrp.feature_weight, 0.0);
        assert!(fastrp.feature_properties.is_empty());
        let hashgnn = HashGnnOptions::default();
        assert_eq!(hashgnn.iterations, 2);
        assert_eq!(hashgnn.embedding_density, 0.25);
        assert!(!hashgnn.heterogeneous);
        assert_eq!(hashgnn.node_type_property, None);
        assert_eq!(hashgnn.relationship_type_property, None);

        let cases = [
            (
                AnalyzeAlgorithm::Node2Vec,
                EmbeddingOptions::Node2Vec(Node2VecOptions::default()),
                "node2vec-v1",
                128,
            ),
            (
                AnalyzeAlgorithm::FastRandomProjection,
                EmbeddingOptions::FastRandomProjection(FastRpOptions::default()),
                "fastrp-v1",
                128,
            ),
            (
                AnalyzeAlgorithm::HashGnn,
                EmbeddingOptions::HashGnn(HashGnnOptions::default()),
                "hashgnn-v1",
                256,
            ),
        ];
        for (by, options, version, dimensions) in cases {
            let invocation = invocation(by, options);
            let first = normalize_embedding_options(&invocation).unwrap();
            assert_eq!(first, normalize_embedding_options(&invocation).unwrap());
            assert_eq!(first.algorithm_version, version);
            assert_eq!(first.dimensions(), dimensions);
            assert_eq!(first.seed(), 0);
            assert_eq!(first.via.as_deref(), Some("LINK"));
        }
        let graphsage = GraphSageOptions {
            feature_properties: vec!["age".into(), "score".into()],
            ..GraphSageOptions::default()
        };
        assert_eq!(graphsage.hidden_dimensions, 256);
        assert_eq!(graphsage.layers, 2);
        assert_eq!(graphsage.sample_sizes, [25, 10]);
        assert_eq!(graphsage.aggregator, GraphSageAggregator::Mean);
        assert_eq!(graphsage.learning_rate, 0.000_002);
        assert_eq!(
            normalize_embedding_options(&invocation(
                AnalyzeAlgorithm::GraphSage,
                EmbeddingOptions::GraphSage(graphsage),
            ))
            .unwrap()
            .algorithm_version,
            "graphsage-unsupervised-v1"
        );
    }

    #[test]
    fn every_family_rejects_invalid_closed_fields() {
        let invalid = [
            invocation(
                AnalyzeAlgorithm::Node2Vec,
                EmbeddingOptions::Node2Vec(Node2VecOptions {
                    dimensions: 0,
                    ..Node2VecOptions::default()
                }),
            ),
            invocation(
                AnalyzeAlgorithm::Node2Vec,
                EmbeddingOptions::Node2Vec(Node2VecOptions {
                    p: f64::NAN,
                    ..Node2VecOptions::default()
                }),
            ),
            invocation(
                AnalyzeAlgorithm::GraphSage,
                EmbeddingOptions::GraphSage(GraphSageOptions::default()),
            ),
            invocation(
                AnalyzeAlgorithm::GraphSage,
                EmbeddingOptions::GraphSage(GraphSageOptions {
                    layers: 1,
                    feature_properties: vec!["age".into()],
                    ..GraphSageOptions::default()
                }),
            ),
            invocation(
                AnalyzeAlgorithm::FastRandomProjection,
                EmbeddingOptions::FastRandomProjection(FastRpOptions {
                    iteration_weights: vec![0.0, 0.0],
                    ..FastRpOptions::default()
                }),
            ),
            invocation(
                AnalyzeAlgorithm::FastRandomProjection,
                EmbeddingOptions::FastRandomProjection(FastRpOptions {
                    feature_weight: 1.0,
                    ..FastRpOptions::default()
                }),
            ),
            invocation(
                AnalyzeAlgorithm::HashGnn,
                EmbeddingOptions::HashGnn(HashGnnOptions {
                    heterogeneous: true,
                    ..HashGnnOptions::default()
                }),
            ),
            invocation(
                AnalyzeAlgorithm::HashGnn,
                EmbeddingOptions::HashGnn(HashGnnOptions {
                    embedding_density: 0.0,
                    ..HashGnnOptions::default()
                }),
            ),
        ];
        for value in invalid {
            assert!(matches!(
                normalize_embedding_options(&value),
                Err(GfError::Validation(_))
            ));
        }
    }

    #[test]
    fn selectors_variants_and_knowledge_fields_fail_closed() {
        let mut wrong = invocation(
            AnalyzeAlgorithm::Node2Vec,
            EmbeddingOptions::HashGnn(HashGnnOptions::default()),
        );
        assert!(normalize_embedding_options(&wrong).is_err());
        wrong.by = AnalyzeAlgorithm::IsDag;
        assert!(normalize_embedding_options(&wrong).is_err());

        let duplicate_features = invocation(
            AnalyzeAlgorithm::FastRandomProjection,
            EmbeddingOptions::FastRandomProjection(FastRpOptions {
                feature_properties: vec!["confidence".into(), "confidence".into()],
                ..FastRpOptions::default()
            }),
        );
        assert!(normalize_embedding_options(&duplicate_features).is_err());

        for forbidden in [
            "provenance",
            "evidence",
            "belief",
            "hypothesis",
            "valid_time",
            "as_of",
        ] {
            let mut selector = invocation(
                AnalyzeAlgorithm::Node2Vec,
                EmbeddingOptions::Node2Vec(Node2VecOptions::default()),
            );
            selector.via = Some(forbidden.into());
            assert!(normalize_embedding_options(&selector).is_err());

            let features = invocation(
                AnalyzeAlgorithm::GraphSage,
                EmbeddingOptions::GraphSage(GraphSageOptions {
                    feature_properties: vec![forbidden.into()],
                    ..GraphSageOptions::default()
                }),
            );
            assert!(normalize_embedding_options(&features).is_err());
        }

        let domain_confidence = invocation(
            AnalyzeAlgorithm::GraphSage,
            EmbeddingOptions::GraphSage(GraphSageOptions {
                feature_properties: vec!["confidence".into()],
                ..GraphSageOptions::default()
            }),
        );
        assert!(normalize_embedding_options(&domain_confidence).is_ok());

        let heterogeneous = invocation(
            AnalyzeAlgorithm::HashGnn,
            EmbeddingOptions::HashGnn(HashGnnOptions {
                heterogeneous: true,
                node_type_property: Some("kind".into()),
                relationship_type_property: Some("relation_kind".into()),
                ..HashGnnOptions::default()
            }),
        );
        assert!(normalize_embedding_options(&heterogeneous).is_ok());
        let descriptor = format!("{:?}", heterogeneous.options);
        assert!(!descriptor.contains("provenance"));
    }
}
