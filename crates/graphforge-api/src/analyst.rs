//! Analyst invocation preparation and dispatch on the public facade.

use crate::{
    Algorithm, AnalyzeAlgorithm, AnalyzeOptions, ClusterAlgorithm, ClusterOptions,
    EmbeddingAnalyzeOptions, EmbeddingOptions, FastRpOptions, GfError, GraphForge,
    GraphSageAggregator, GraphSageOptions, HashGnnOptions, InvocationDescriptor,
    InvocationDescriptorError, InvocationError, InvocationParameter, Node2VecOptions, NodeSelector,
    PathAlgorithm, PathsOptions, RankOptions, SimilarAlgorithm, SimilarOptions, insert_usize,
    invocation_descriptor,
};

impl GraphForge {
    /// Prepare a canonical, knowledge-neutral rank descriptor without running it.
    ///
    /// # Errors
    /// Returns a structured graph or descriptor failure. Write-back is rejected
    /// because it is a separate mutation, not part of neutral invocation state.
    pub fn prepare_rank_invocation(
        &self,
        label: &str,
        options: &RankOptions,
    ) -> Result<InvocationDescriptor, InvocationError> {
        if options.write_property.is_some() {
            return Err(InvocationDescriptorError::Invalid(
                "rank write_property is not part of a neutral invocation".into(),
            )
            .into());
        }
        let (label_id, _) = self.algorithm_label(label, "rank")?;
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        let adjacency_provider = self.adjacency_provider_for_session();
        adjacency_provider.revalidate();
        let projection = graphforge_exec::rank_projection_fingerprint(
            &graphforge_exec::AdmittedAdjacencyProvider::new(
                adjacency_provider.as_ref(),
                self.property_inventory_for_session(),
            ),
            &self.dir,
            self.ontology_mode,
            label_id,
            options,
        )?;
        InvocationDescriptor::new(
            Algorithm::Rank(options.by),
            projection,
            std::collections::BTreeMap::from([
                (
                    "directed".into(),
                    InvocationParameter::Bool(options.directed),
                ),
                ("label".into(), InvocationParameter::Utf8(label.to_owned())),
                (
                    "via".into(),
                    InvocationParameter::Utf8(options.via.clone().unwrap_or_else(|| "*".into())),
                ),
            ]),
        )
        .map_err(Into::into)
    }

    /// Dispatch a prepared rank descriptor through the same executor as [`Self::rank`].
    ///
    /// # Errors
    /// Returns `GF_PROJECTION_CHANGED` if graph input changed after preparation,
    /// or the structured descriptor/algorithm failure.
    pub fn invoke_rank_descriptor(
        &self,
        descriptor: &InvocationDescriptor,
    ) -> Result<arrow::record_batch::RecordBatch, InvocationError> {
        self.graph_visibility.health.check()?;
        let _graph_visibility = self.graph_visibility.lock()?;
        let Algorithm::Rank(by) = descriptor.algorithm() else {
            return Err(InvocationDescriptorError::Invalid(
                "rank dispatch requires a rank descriptor".into(),
            )
            .into());
        };
        let parameters = descriptor.parameters();
        let label = invocation_descriptor::required_utf8(parameters, "label")?;
        let via = invocation_descriptor::required_utf8(parameters, "via")?;
        let options = RankOptions {
            by,
            via: (via != "*").then(|| via.to_owned()),
            directed: invocation_descriptor::required_bool(parameters, "directed")?,
            write_property: None,
        };
        let current = self.prepare_rank_invocation(label, &options)?;
        if current.projection_fingerprint() != descriptor.projection_fingerprint() {
            return Err(InvocationError::ProjectionChanged);
        }
        invocation_descriptor::validate_result(descriptor, self.rank(label, options)?)
    }

    /// Prepare a canonical, knowledge-neutral clustering descriptor.
    ///
    /// # Errors
    /// Returns a structured graph or descriptor failure; write-back is rejected.
    pub fn prepare_cluster_invocation(
        &self,
        label: &str,
        options: &ClusterOptions,
    ) -> Result<InvocationDescriptor, InvocationError> {
        if options.write_property.is_some() {
            return Err(InvocationDescriptorError::Invalid(
                "cluster write_property is not part of a neutral invocation".into(),
            )
            .into());
        }
        let (label_id, stem) = self.algorithm_label(label, "cluster")?;
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        let adjacency_provider = self.adjacency_provider_for_session();
        adjacency_provider.revalidate();
        let projection = graphforge_exec::cluster_projection_fingerprint(
            &graphforge_exec::AdmittedAdjacencyProvider::new(
                adjacency_provider.as_ref(),
                self.property_inventory_for_session(),
            ),
            &self.dir,
            self.ontology_mode,
            label_id,
            std::slice::from_ref(&stem),
            options,
        )?;
        let mut parameters = std::collections::BTreeMap::from([
            (
                "directed".into(),
                InvocationParameter::Bool(options.directed && options.by.respects_direction()),
            ),
            ("label".into(), InvocationParameter::Utf8(label.to_owned())),
        ]);
        if matches!(
            options.by,
            ClusterAlgorithm::Hdbscan | ClusterAlgorithm::KMeans
        ) {
            let property = options.vector_property.as_ref().ok_or_else(|| {
                InvocationDescriptorError::Invalid(format!(
                    "cluster.{} requires vector_property",
                    options.by
                ))
            })?;
            parameters.insert(
                "vector_property".into(),
                InvocationParameter::Utf8(property.clone()),
            );
        } else {
            parameters.insert(
                "via".into(),
                InvocationParameter::Utf8(options.via.clone().unwrap_or_else(|| "*".into())),
            );
        }
        InvocationDescriptor::new(Algorithm::Cluster(options.by), projection, parameters)
            .map_err(Into::into)
    }

    /// Dispatch a prepared clustering descriptor through [`Self::cluster`].
    ///
    /// # Errors
    /// Returns a structured descriptor, projection, graph, or execution failure.
    pub fn invoke_cluster_descriptor(
        &self,
        descriptor: &InvocationDescriptor,
    ) -> Result<arrow::record_batch::RecordBatch, InvocationError> {
        self.graph_visibility.health.check()?;
        let _graph_visibility = self.graph_visibility.lock()?;
        let Algorithm::Cluster(by) = descriptor.algorithm() else {
            return Err(InvocationDescriptorError::Invalid(
                "cluster dispatch requires a cluster descriptor".into(),
            )
            .into());
        };
        let parameters = descriptor.parameters();
        let label = invocation_descriptor::required_utf8(parameters, "label")?;
        let via = invocation_descriptor::optional_utf8(parameters, "via")?;
        let options = ClusterOptions {
            by,
            vector_property: invocation_descriptor::optional_utf8(parameters, "vector_property")?,
            via: via.filter(|value| value != "*"),
            directed: invocation_descriptor::required_bool(parameters, "directed")?,
            write_property: None,
        };
        let current = self.prepare_cluster_invocation(label, &options)?;
        if current.projection_fingerprint() != descriptor.projection_fingerprint() {
            return Err(InvocationError::ProjectionChanged);
        }
        invocation_descriptor::validate_result(descriptor, self.cluster(label, options)?)
    }

    /// Prepare a canonical, knowledge-neutral similarity descriptor.
    ///
    /// # Errors
    /// Returns a structured graph or descriptor failure.
    pub fn prepare_similar_invocation(
        &self,
        label: &str,
        options: &SimilarOptions,
    ) -> Result<InvocationDescriptor, InvocationError> {
        let (label_id, stem) = self.algorithm_label(label, "similar")?;
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        let adjacency_provider = self.adjacency_provider_for_session();
        adjacency_provider.revalidate();
        let projection = graphforge_exec::similar_projection_fingerprint(
            &graphforge_exec::AdmittedAdjacencyProvider::new(
                adjacency_provider.as_ref(),
                self.property_inventory_for_session(),
            ),
            &self.dir,
            self.ontology_mode,
            label_id,
            std::slice::from_ref(&stem),
            options,
        )?;
        let mut parameters = std::collections::BTreeMap::from([
            (
                "k".into(),
                InvocationParameter::U64(u64::try_from(options.k).map_err(|_| {
                    InvocationDescriptorError::Invalid("similar k exceeds UInt64".into())
                })?),
            ),
            ("label".into(), InvocationParameter::Utf8(label.to_owned())),
        ]);
        if matches!(
            options.by,
            SimilarAlgorithm::Knn | SimilarAlgorithm::FilteredKnn | SimilarAlgorithm::Cosine
        ) {
            let property = options.vector_property.as_ref().ok_or_else(|| {
                InvocationDescriptorError::Invalid(format!(
                    "similar.{} requires vector_property",
                    options.by
                ))
            })?;
            parameters.insert(
                "vector_property".into(),
                InvocationParameter::Utf8(property.clone()),
            );
        }
        if !matches!(options.by, SimilarAlgorithm::Knn | SimilarAlgorithm::Cosine) {
            parameters.insert(
                "via".into(),
                InvocationParameter::Utf8(options.via.clone().unwrap_or_else(|| "*".into())),
            );
        }
        InvocationDescriptor::new(Algorithm::Similar(options.by), projection, parameters)
            .map_err(Into::into)
    }

    /// Dispatch a prepared similarity descriptor through [`Self::similar`].
    ///
    /// # Errors
    /// Returns a structured descriptor, projection, graph, or execution failure.
    pub fn invoke_similar_descriptor(
        &self,
        descriptor: &InvocationDescriptor,
    ) -> Result<arrow::record_batch::RecordBatch, InvocationError> {
        self.graph_visibility.health.check()?;
        let _graph_visibility = self.graph_visibility.lock()?;
        let Algorithm::Similar(by) = descriptor.algorithm() else {
            return Err(InvocationDescriptorError::Invalid(
                "similar dispatch requires a similarity descriptor".into(),
            )
            .into());
        };
        let parameters = descriptor.parameters();
        let label = invocation_descriptor::required_utf8(parameters, "label")?;
        let via = invocation_descriptor::optional_utf8(parameters, "via")?;
        let options = SimilarOptions {
            by,
            k: usize::try_from(invocation_descriptor::required_u64(parameters, "k")?).map_err(
                |_| InvocationDescriptorError::Invalid("similar k exceeds usize".into()),
            )?,
            vector_property: invocation_descriptor::optional_utf8(parameters, "vector_property")?,
            via: via.filter(|value| value != "*"),
        };
        let current = self.prepare_similar_invocation(label, &options)?;
        if current.projection_fingerprint() != descriptor.projection_fingerprint() {
            return Err(InvocationError::ProjectionChanged);
        }
        invocation_descriptor::validate_result(descriptor, self.similar(label, options)?)
    }

    /// Prepare a canonical, knowledge-neutral embedding descriptor.
    ///
    /// # Errors
    /// Returns the same normalization, projection, property, and resource
    /// failures as embedding execution, without starting the kernel.
    #[allow(
        clippy::too_many_lines,
        reason = "the closed four-variant embedding registry is encoded in one exhaustive match"
    )]
    pub fn prepare_embedding_invocation(
        &self,
        label: Option<&str>,
        options: &EmbeddingAnalyzeOptions,
    ) -> Result<InvocationDescriptor, InvocationError> {
        let label_id = label
            .map(|value| self.algorithm_label(value, "analyze").map(|(id, _)| id))
            .transpose()?
            .unwrap_or(graphforge_value::EntityTypeSelection::All);
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        let adjacency_provider = self.adjacency_provider_for_session();
        adjacency_provider.revalidate();
        let prepared = graphforge_exec::prepare_embedding_invocation_descriptor_with_compute(
            &graphforge_exec::AdmittedAdjacencyProvider::new(
                adjacency_provider.as_ref(),
                self.property_inventory_for_session(),
            ),
            &self.dir,
            self.ontology_mode,
            label_id,
            label,
            options,
            graphforge_exec::AlgorithmLimits::default()
                .with_batch_size(self.resource_policy.batch_size)
                .with_compute_threads(self.resource_policy.compute_threads),
            Some(self.compute_pool.clone()),
        )?;
        let mut parameters = std::collections::BTreeMap::from([
            (
                "directed".into(),
                InvocationParameter::Bool(prepared.selector.directed),
            ),
            (
                "label".into(),
                InvocationParameter::Utf8(prepared.selector.label.unwrap_or_default()),
            ),
            ("seed".into(), InvocationParameter::U64(prepared.rng.seed)),
            (
                "via".into(),
                InvocationParameter::Utf8(prepared.selector.via.unwrap_or_else(|| "*".into())),
            ),
            (
                "weight".into(),
                InvocationParameter::Utf8(prepared.selector.weight.unwrap_or_default()),
            ),
        ]);
        match prepared.options {
            EmbeddingOptions::Node2Vec(value) => {
                insert_usize(&mut parameters, "dimensions", value.dimensions)?;
                insert_usize(&mut parameters, "walk_length", value.walk_length)?;
                insert_usize(&mut parameters, "walks_per_node", value.walks_per_node)?;
                parameters.insert("p".into(), InvocationParameter::F64(value.p));
                parameters.insert("q".into(), InvocationParameter::F64(value.q));
                insert_usize(&mut parameters, "window_size", value.window_size)?;
                insert_usize(&mut parameters, "negative_samples", value.negative_samples)?;
                insert_usize(&mut parameters, "epochs", value.epochs)?;
                parameters.insert(
                    "learning_rate".into(),
                    InvocationParameter::F64(value.learning_rate),
                );
            }
            EmbeddingOptions::GraphSage(value) => {
                insert_usize(&mut parameters, "dimensions", value.dimensions)?;
                insert_usize(
                    &mut parameters,
                    "hidden_dimensions",
                    value.hidden_dimensions,
                )?;
                insert_usize(&mut parameters, "layers", value.layers)?;
                parameters.insert(
                    "sample_sizes".into(),
                    InvocationParameter::U64List(
                        value
                            .sample_sizes
                            .into_iter()
                            .map(|item| {
                                u64::try_from(item).map_err(|_| {
                                    InvocationDescriptorError::Invalid(
                                        "GraphSAGE sample size exceeds UInt64".into(),
                                    )
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    ),
                );
                parameters.insert(
                    "aggregator".into(),
                    InvocationParameter::Utf8(match value.aggregator {
                        GraphSageAggregator::Mean => "mean".into(),
                    }),
                );
                insert_usize(&mut parameters, "epochs", value.epochs)?;
                insert_usize(&mut parameters, "negative_samples", value.negative_samples)?;
                parameters.insert(
                    "learning_rate".into(),
                    InvocationParameter::F64(value.learning_rate),
                );
                parameters.insert(
                    "feature_properties".into(),
                    InvocationParameter::Utf8List(value.feature_properties),
                );
            }
            EmbeddingOptions::FastRandomProjection(value) => {
                insert_usize(&mut parameters, "dimensions", value.dimensions)?;
                parameters.insert(
                    "iteration_weights".into(),
                    InvocationParameter::F64List(value.iteration_weights),
                );
                parameters.insert(
                    "normalization_strength".into(),
                    InvocationParameter::F64(value.normalization_strength),
                );
                parameters.insert(
                    "feature_weight".into(),
                    InvocationParameter::F64(value.feature_weight),
                );
                parameters.insert(
                    "feature_properties".into(),
                    InvocationParameter::Utf8List(value.feature_properties),
                );
            }
            EmbeddingOptions::HashGnn(value) => {
                insert_usize(&mut parameters, "dimensions", value.dimensions)?;
                insert_usize(&mut parameters, "iterations", value.iterations)?;
                parameters.insert(
                    "embedding_density".into(),
                    InvocationParameter::F64(value.embedding_density),
                );
                parameters.insert(
                    "heterogeneous".into(),
                    InvocationParameter::Bool(value.heterogeneous),
                );
                parameters.insert(
                    "node_type_property".into(),
                    InvocationParameter::Utf8(value.node_type_property.unwrap_or_default()),
                );
                parameters.insert(
                    "relationship_type_property".into(),
                    InvocationParameter::Utf8(value.relationship_type_property.unwrap_or_default()),
                );
            }
        }
        InvocationDescriptor::new(
            Algorithm::Analyze(options.by),
            prepared.projection_fingerprint,
            parameters,
        )
        .map_err(Into::into)
    }

    /// Dispatch a prepared embedding descriptor through [`Self::analyze_embedding`].
    ///
    /// # Errors
    /// Returns a structured descriptor, projection, graph, or execution failure.
    #[allow(
        clippy::too_many_lines,
        reason = "the closed four-variant embedding registry is decoded in one exhaustive match"
    )]
    pub fn invoke_embedding_descriptor(
        &self,
        descriptor: &InvocationDescriptor,
    ) -> Result<arrow::record_batch::RecordBatch, InvocationError> {
        self.graph_visibility.health.check()?;
        let _graph_visibility = self.graph_visibility.lock()?;
        let Algorithm::Analyze(by) = descriptor.algorithm() else {
            return Err(InvocationDescriptorError::Invalid(
                "embedding dispatch requires an analyze descriptor".into(),
            )
            .into());
        };
        if !matches!(
            by,
            AnalyzeAlgorithm::Node2Vec
                | AnalyzeAlgorithm::GraphSage
                | AnalyzeAlgorithm::FastRandomProjection
                | AnalyzeAlgorithm::HashGnn
        ) {
            return Err(InvocationDescriptorError::Invalid(
                "descriptor is not an embedding algorithm".into(),
            )
            .into());
        }
        let parameters = descriptor.parameters();
        let usize_value = |name| {
            usize::try_from(invocation_descriptor::required_u64(parameters, name)?)
                .map_err(|_| InvocationDescriptorError::Invalid(format!("{name} exceeds usize")))
        };
        let seed = invocation_descriptor::required_u64(parameters, "seed")?;
        let embedding_options = match by {
            AnalyzeAlgorithm::Node2Vec => EmbeddingOptions::Node2Vec(Node2VecOptions {
                dimensions: usize_value("dimensions")?,
                walk_length: usize_value("walk_length")?,
                walks_per_node: usize_value("walks_per_node")?,
                p: invocation_descriptor::required_f64(parameters, "p")?,
                q: invocation_descriptor::required_f64(parameters, "q")?,
                window_size: usize_value("window_size")?,
                negative_samples: usize_value("negative_samples")?,
                epochs: usize_value("epochs")?,
                learning_rate: invocation_descriptor::required_f64(parameters, "learning_rate")?,
                seed,
            }),
            AnalyzeAlgorithm::GraphSage => {
                let aggregator =
                    match invocation_descriptor::required_utf8(parameters, "aggregator")? {
                        "mean" => GraphSageAggregator::Mean,
                        value => {
                            return Err(InvocationDescriptorError::Invalid(format!(
                                "unsupported GraphSAGE aggregator {value:?}"
                            ))
                            .into());
                        }
                    };
                EmbeddingOptions::GraphSage(GraphSageOptions {
                    dimensions: usize_value("dimensions")?,
                    hidden_dimensions: usize_value("hidden_dimensions")?,
                    layers: usize_value("layers")?,
                    sample_sizes: invocation_descriptor::required_u64_list(
                        parameters,
                        "sample_sizes",
                    )?
                    .into_iter()
                    .map(|value| {
                        usize::try_from(value).map_err(|_| {
                            InvocationDescriptorError::Invalid(
                                "GraphSAGE sample size exceeds usize".into(),
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                    aggregator,
                    epochs: usize_value("epochs")?,
                    negative_samples: usize_value("negative_samples")?,
                    learning_rate: invocation_descriptor::required_f64(
                        parameters,
                        "learning_rate",
                    )?,
                    feature_properties: invocation_descriptor::required_utf8_list(
                        parameters,
                        "feature_properties",
                    )?,
                    seed,
                })
            }
            AnalyzeAlgorithm::FastRandomProjection => {
                EmbeddingOptions::FastRandomProjection(FastRpOptions {
                    dimensions: usize_value("dimensions")?,
                    iteration_weights: invocation_descriptor::required_f64_list(
                        parameters,
                        "iteration_weights",
                    )?,
                    normalization_strength: invocation_descriptor::required_f64(
                        parameters,
                        "normalization_strength",
                    )?,
                    feature_weight: invocation_descriptor::required_f64(
                        parameters,
                        "feature_weight",
                    )?,
                    feature_properties: invocation_descriptor::required_utf8_list(
                        parameters,
                        "feature_properties",
                    )?,
                    seed,
                })
            }
            AnalyzeAlgorithm::HashGnn => EmbeddingOptions::HashGnn(HashGnnOptions {
                dimensions: usize_value("dimensions")?,
                iterations: usize_value("iterations")?,
                embedding_density: invocation_descriptor::required_f64(
                    parameters,
                    "embedding_density",
                )?,
                heterogeneous: invocation_descriptor::required_bool(parameters, "heterogeneous")?,
                node_type_property: {
                    let value =
                        invocation_descriptor::required_utf8(parameters, "node_type_property")?;
                    (!value.is_empty()).then(|| value.to_owned())
                },
                relationship_type_property: {
                    let value = invocation_descriptor::required_utf8(
                        parameters,
                        "relationship_type_property",
                    )?;
                    (!value.is_empty()).then(|| value.to_owned())
                },
                seed,
            }),
            _ => unreachable!("non-embedding analyze algorithms were rejected above"),
        };
        let empty_to_none = |value: &str| (!value.is_empty()).then(|| value.to_owned());
        let label = invocation_descriptor::required_utf8(parameters, "label")?;
        let via = invocation_descriptor::required_utf8(parameters, "via")?;
        let weight = invocation_descriptor::required_utf8(parameters, "weight")?;
        let options = EmbeddingAnalyzeOptions {
            by,
            via: (via != "*").then(|| via.to_owned()),
            directed: invocation_descriptor::required_bool(parameters, "directed")?,
            weight: empty_to_none(weight),
            options: embedding_options,
        };
        let current =
            self.prepare_embedding_invocation(empty_to_none(label).as_deref(), &options)?;
        if current.projection_fingerprint() != descriptor.projection_fingerprint() {
            return Err(InvocationError::ProjectionChanged);
        }
        invocation_descriptor::validate_result(
            descriptor,
            self.analyze_embedding(empty_to_none(label).as_deref(), &options)?,
        )
    }

    /// Prepare a canonical, knowledge-neutral structural-analysis descriptor.
    ///
    /// # Errors
    /// Returns a structured graph or descriptor failure.
    pub fn prepare_analyze_invocation(
        &self,
        label: Option<&str>,
        options: &AnalyzeOptions,
    ) -> Result<InvocationDescriptor, InvocationError> {
        let label_id = label
            .map(|value| self.algorithm_label(value, "analyze").map(|(id, _)| id))
            .transpose()?
            .unwrap_or(graphforge_value::EntityTypeSelection::All);
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        let adjacency_provider = self.adjacency_provider_for_session();
        adjacency_provider.revalidate();
        let projection = graphforge_exec::analyze_projection_fingerprint(
            &graphforge_exec::AdmittedAdjacencyProvider::new(
                adjacency_provider.as_ref(),
                self.property_inventory_for_session(),
            ),
            &self.dir,
            self.ontology_mode,
            label_id,
            options,
        )?;
        let mut parameters = std::collections::BTreeMap::from([
            (
                "directed".into(),
                InvocationParameter::Bool(options.directed),
            ),
            (
                "via".into(),
                InvocationParameter::Utf8(options.via.clone().unwrap_or_else(|| "*".into())),
            ),
        ]);
        if let Some(value) = label {
            parameters.insert("label".into(), InvocationParameter::Utf8(value.to_owned()));
        }
        if let Some(value) = &options.weight {
            parameters.insert("weight".into(), InvocationParameter::Utf8(value.clone()));
        }
        if let Some(value) = options.k {
            parameters.insert(
                "k".into(),
                InvocationParameter::U64(u64::try_from(value).map_err(|_| {
                    InvocationDescriptorError::Invalid("analyze k exceeds UInt64".into())
                })?),
            );
        }
        if let Some(value) = &options.partition_property {
            parameters.insert(
                "partition_property".into(),
                InvocationParameter::Utf8(value.clone()),
            );
        }
        InvocationDescriptor::new(Algorithm::Analyze(options.by), projection, parameters)
            .map_err(Into::into)
    }

    /// Dispatch a prepared analysis descriptor through [`Self::analyze`].
    ///
    /// # Errors
    /// Returns a structured descriptor, projection, graph, or execution failure.
    pub fn invoke_analyze_descriptor(
        &self,
        descriptor: &InvocationDescriptor,
    ) -> Result<arrow::record_batch::RecordBatch, InvocationError> {
        self.graph_visibility.health.check()?;
        let _graph_visibility = self.graph_visibility.lock()?;
        let Algorithm::Analyze(by) = descriptor.algorithm() else {
            return Err(InvocationDescriptorError::Invalid(
                "analyze dispatch requires an analyze descriptor".into(),
            )
            .into());
        };
        let parameters = descriptor.parameters();
        let label = invocation_descriptor::optional_utf8(parameters, "label")?;
        let via = invocation_descriptor::required_utf8(parameters, "via")?;
        let options = AnalyzeOptions {
            by,
            via: (via != "*").then(|| via.to_owned()),
            directed: invocation_descriptor::required_bool(parameters, "directed")?,
            weight: invocation_descriptor::optional_utf8(parameters, "weight")?,
            k: invocation_descriptor::optional_u64(parameters, "k")?
                .map(|value| {
                    usize::try_from(value).map_err(|_| {
                        InvocationDescriptorError::Invalid("analyze k exceeds usize".into())
                    })
                })
                .transpose()?,
            partition_property: invocation_descriptor::optional_utf8(
                parameters,
                "partition_property",
            )?,
        };
        let current = self.prepare_analyze_invocation(label.as_deref(), &options)?;
        if current.projection_fingerprint() != descriptor.projection_fingerprint() {
            return Err(InvocationError::ProjectionChanged);
        }
        invocation_descriptor::validate_result(descriptor, self.analyze(label.as_deref(), options)?)
    }

    /// Prepare a canonical, knowledge-neutral paths descriptor.
    ///
    /// Node selectors are resolved to stable UUIDs before canonicalization.
    ///
    /// # Errors
    /// Returns a structured selector, projection, option, or descriptor failure.
    pub fn prepare_paths_invocation(
        &self,
        source: Option<&NodeSelector>,
        target: Option<&NodeSelector>,
        options: &PathsOptions,
    ) -> Result<InvocationDescriptor, InvocationError> {
        if matches!(
            options.by,
            PathAlgorithm::MinSteinerTree
                | PathAlgorithm::PrizeCollectingSteinerTree
                | PathAlgorithm::GomoryHuTree
        ) && (source.is_some() || target.is_some())
        {
            return Err(GfError::Validation(format!(
                "{} does not accept positional source or target selectors",
                options.by
            ))
            .into());
        }
        let source = source
            .map(|selector| self.resolve_node_selector(selector))
            .transpose()?
            .map(|uuid| *uuid.as_bytes());
        let target = target
            .map(|selector| self.resolve_node_selector(selector))
            .transpose()?
            .map(|uuid| *uuid.as_bytes());
        let mut normalized = options.clone();
        normalized.terminal_uuids.sort_unstable();
        normalized.terminal_uuids.dedup();
        if normalized.by == PathAlgorithm::RandomWalk {
            normalized.walk_length = Some(normalized.walk_length.unwrap_or(10));
            normalized.seed = Some(normalized.seed.unwrap_or(0));
        }
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        let adjacency_provider = self.adjacency_provider_for_session();
        adjacency_provider.revalidate();
        let projection = graphforge_exec::paths_projection_fingerprint(
            &graphforge_exec::AdmittedAdjacencyProvider::new(
                adjacency_provider.as_ref(),
                self.property_inventory_for_session(),
            ),
            &self.dir,
            self.ontology_mode,
            source,
            target,
            &normalized,
        )?;
        let mut parameters = std::collections::BTreeMap::from([
            (
                "directed".into(),
                InvocationParameter::Bool(normalized.directed),
            ),
            (
                "k".into(),
                InvocationParameter::U64(u64::try_from(normalized.k).map_err(|_| {
                    InvocationDescriptorError::Invalid("paths k exceeds UInt64".into())
                })?),
            ),
            (
                "via".into(),
                InvocationParameter::Utf8(normalized.via.clone().unwrap_or_else(|| "*".into())),
            ),
        ]);
        if let Some(value) = source {
            parameters.insert("source_uuid".into(), InvocationParameter::Uuid(value));
        }
        if let Some(value) = target {
            parameters.insert("target_uuid".into(), InvocationParameter::Uuid(value));
        }
        for (name, value) in [
            ("weight", normalized.weight.as_ref()),
            ("capacity_property", normalized.capacity_property.as_ref()),
            ("cost_property", normalized.cost_property.as_ref()),
            ("heuristic", normalized.heuristic.as_ref()),
            ("prize_property", normalized.prize_property.as_ref()),
        ] {
            if let Some(value) = value {
                parameters.insert(name.into(), InvocationParameter::Utf8(value.clone()));
            }
        }
        if let Some(value) = normalized.walk_length {
            parameters.insert(
                "walk_length".into(),
                InvocationParameter::U64(u64::try_from(value).map_err(|_| {
                    InvocationDescriptorError::Invalid("walk length exceeds UInt64".into())
                })?),
            );
        }
        if let Some(value) = normalized.seed {
            parameters.insert("seed".into(), InvocationParameter::U64(value));
        }
        if !normalized.terminal_uuids.is_empty() {
            parameters.insert(
                "terminal_uuids".into(),
                InvocationParameter::UuidList(normalized.terminal_uuids),
            );
        }
        InvocationDescriptor::new(Algorithm::Paths(normalized.by), projection, parameters)
            .map_err(Into::into)
    }

    /// Dispatch a prepared paths descriptor through [`Self::paths`].
    ///
    /// # Errors
    /// Returns a structured descriptor, projection, selector, graph, or execution failure.
    pub fn invoke_paths_descriptor(
        &self,
        descriptor: &InvocationDescriptor,
    ) -> Result<arrow::record_batch::RecordBatch, InvocationError> {
        self.graph_visibility.health.check()?;
        let _graph_visibility = self.graph_visibility.lock()?;
        let Algorithm::Paths(by) = descriptor.algorithm() else {
            return Err(InvocationDescriptorError::Invalid(
                "paths dispatch requires a paths descriptor".into(),
            )
            .into());
        };
        let parameters = descriptor.parameters();
        let source = invocation_descriptor::optional_uuid(parameters, "source_uuid")?
            .map(|value| NodeSelector::Uuid(uuid::Uuid::from_bytes(value)));
        let target = invocation_descriptor::optional_uuid(parameters, "target_uuid")?
            .map(|value| NodeSelector::Uuid(uuid::Uuid::from_bytes(value)));
        let via = invocation_descriptor::required_utf8(parameters, "via")?;
        let options = PathsOptions {
            by,
            via: (via != "*").then(|| via.to_owned()),
            directed: invocation_descriptor::required_bool(parameters, "directed")?,
            k: usize::try_from(invocation_descriptor::required_u64(parameters, "k")?)
                .map_err(|_| InvocationDescriptorError::Invalid("paths k exceeds usize".into()))?,
            weight: invocation_descriptor::optional_utf8(parameters, "weight")?,
            capacity_property: invocation_descriptor::optional_utf8(
                parameters,
                "capacity_property",
            )?,
            cost_property: invocation_descriptor::optional_utf8(parameters, "cost_property")?,
            heuristic: invocation_descriptor::optional_utf8(parameters, "heuristic")?,
            walk_length: invocation_descriptor::optional_u64(parameters, "walk_length")?
                .map(|value| {
                    usize::try_from(value).map_err(|_| {
                        InvocationDescriptorError::Invalid("walk length exceeds usize".into())
                    })
                })
                .transpose()?,
            seed: invocation_descriptor::optional_u64(parameters, "seed")?,
            terminal_uuids: invocation_descriptor::optional_uuid_list(
                parameters,
                "terminal_uuids",
            )?
            .unwrap_or_default(),
            prize_property: invocation_descriptor::optional_utf8(parameters, "prize_property")?,
        };
        let current = self.prepare_paths_invocation(source.as_ref(), target.as_ref(), &options)?;
        if current.projection_fingerprint() != descriptor.projection_fingerprint() {
            return Err(InvocationError::ProjectionChanged);
        }
        invocation_descriptor::validate_result(
            descriptor,
            self.paths(source.as_ref(), target.as_ref(), options)?,
        )
    }

    /// Dispatch any prepared neutral algorithm descriptor through its owning verb.
    ///
    /// # Errors
    /// Returns the same structured descriptor, projection, graph, or execution
    /// failure as the typed verb-specific dispatch method.
    pub fn invoke_descriptor(
        &self,
        descriptor: &InvocationDescriptor,
    ) -> Result<arrow::record_batch::RecordBatch, InvocationError> {
        match descriptor.algorithm() {
            Algorithm::Rank(_) => self.invoke_rank_descriptor(descriptor),
            Algorithm::Cluster(_) => self.invoke_cluster_descriptor(descriptor),
            Algorithm::Paths(_) => self.invoke_paths_descriptor(descriptor),
            Algorithm::Analyze(
                AnalyzeAlgorithm::Node2Vec
                | AnalyzeAlgorithm::GraphSage
                | AnalyzeAlgorithm::FastRandomProjection
                | AnalyzeAlgorithm::HashGnn,
            ) => self.invoke_embedding_descriptor(descriptor),
            Algorithm::Analyze(_) => self.invoke_analyze_descriptor(descriptor),
            Algorithm::Similar(_) => self.invoke_similar_descriptor(descriptor),
        }
    }

    /// Decode canonical bytes and dispatch the resulting neutral descriptor.
    ///
    /// # Errors
    /// Rejects malformed, non-canonical, unknown-version, or changed-projection
    /// descriptors before kernel execution.
    pub fn invoke_descriptor_bytes(
        &self,
        bytes: &[u8],
    ) -> Result<arrow::record_batch::RecordBatch, InvocationError> {
        let descriptor = InvocationDescriptor::from_canonical_bytes(bytes)?;
        self.invoke_descriptor(&descriptor)
    }

    /// Rank nodes by a structural algorithm through the Rust-only registry.
    ///
    /// `degree` normalizes adjacency-entry counts by `max(selected_nodes - 1,
    /// 1)`. It counts outgoing entries when `directed` is true (the default)
    /// and both endpoints when false. Parallel edges count separately; an
    /// undirected self-loop contributes two. Rows follow stable topology order
    /// and expose only UUID identity. `via=None` selects every relation.
    ///
    /// # Errors
    /// Returns [`GfError::Validation`] for malformed selectors or a rank
    /// algorithm without a registered Rust implementation, and structured
    /// execution/storage failures from adjacency, limits, shaping, or atomic
    /// opt-in write-back.
    pub fn rank(
        &self,
        label: &str,
        options: RankOptions,
    ) -> Result<arrow::record_batch::RecordBatch, GfError> {
        let _admission = self.admit_heavy_query()?;
        let RankOptions {
            by,
            via,
            directed,
            write_property,
        } = options;
        let dispatch_options = RankOptions {
            by,
            via,
            directed,
            write_property: None,
        };
        let _graph_visibility = write_property
            .as_ref()
            .map(|_| self.graph_visibility.lock())
            .transpose()?;
        let (label_id, stem) = self.algorithm_label(label, "rank")?;
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        let adjacency_provider = self.adjacency_provider_for_session();
        adjacency_provider.revalidate();
        let batch = graphforge_exec::rank_algorithm_with_compute(
            &graphforge_exec::AdmittedAdjacencyProvider::new(
                adjacency_provider.as_ref(),
                self.property_inventory_for_session(),
            ),
            &self.dir,
            self.ontology_mode,
            label_id,
            std::slice::from_ref(&stem),
            &dispatch_options,
            graphforge_exec::AlgorithmLimits::default()
                .with_batch_size(self.resource_policy.batch_size)
                .with_compute_threads(self.resource_policy.compute_threads),
            Some(self.compute_pool.clone()),
        )?;
        self.write_algorithm_property(
            label,
            &stem,
            Algorithm::Rank(by),
            write_property.as_deref(),
            &batch,
        )?;
        Ok(batch)
    }

    /// Cluster nodes through the Rust-only algorithm registry.
    ///
    /// # Errors
    /// Returns [`GfError::Validation`] for malformed labels/selectors or a
    /// cluster algorithm without a registered Rust implementation, and
    /// structured execution/storage failures from adjacency, limits, shaping,
    /// or atomic opt-in write-back.
    pub fn cluster(
        &self,
        label: &str,
        options: ClusterOptions,
    ) -> Result<arrow::record_batch::RecordBatch, GfError> {
        let ClusterOptions {
            by,
            vector_property,
            via,
            directed,
            write_property,
        } = options;
        let dispatch_options = ClusterOptions {
            by,
            vector_property,
            via,
            directed,
            write_property: None,
        };
        let _graph_visibility = write_property
            .as_ref()
            .map(|_| self.graph_visibility.lock())
            .transpose()?;
        let (label_id, stem) = self.algorithm_label(label, "cluster")?;
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        let adjacency_provider = self.adjacency_provider_for_session();
        adjacency_provider.revalidate();
        let batch = graphforge_exec::cluster_algorithm_with_compute(
            &graphforge_exec::AdmittedAdjacencyProvider::new(
                adjacency_provider.as_ref(),
                self.property_inventory_for_session(),
            ),
            &self.dir,
            self.ontology_mode,
            label_id,
            std::slice::from_ref(&stem),
            &dispatch_options,
            graphforge_exec::AlgorithmLimits::default()
                .with_batch_size(self.resource_policy.batch_size)
                .with_compute_threads(self.resource_policy.compute_threads),
            Some(self.compute_pool.clone()),
        )?;
        self.write_algorithm_property(
            label,
            &stem,
            Algorithm::Cluster(by),
            write_property.as_deref(),
            &batch,
        )?;
        Ok(batch)
    }

    /// Find paths / flows between nodes selected by UUID, handle, or property.
    ///
    /// # Errors
    /// Returns [`GfError::Validation`] for a missing, ambiguous, malformed, or
    /// cross-graph selector, then [`GfError::NotImplemented`] until the selected
    /// path algorithm ships.
    pub fn paths<'a>(
        &self,
        source: impl Into<Option<&'a NodeSelector>>,
        target: Option<&NodeSelector>,
        options: PathsOptions,
    ) -> Result<arrow::record_batch::RecordBatch, GfError> {
        let source = source.into();
        if matches!(
            options.by,
            PathAlgorithm::MinSteinerTree
                | PathAlgorithm::PrizeCollectingSteinerTree
                | PathAlgorithm::GomoryHuTree
        ) && (source.is_some() || target.is_some())
        {
            return Err(GfError::Validation(format!(
                "{} does not accept positional source or target selectors",
                options.by
            )));
        }
        let source = source
            .map(|selector| self.resolve_node_selector(selector))
            .transpose()?;
        let target = target
            .map(|selector| self.resolve_node_selector(selector))
            .transpose()?;
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        let adjacency_provider = self.adjacency_provider_for_session();
        adjacency_provider.revalidate();
        graphforge_exec::paths_algorithm_with_compute(
            &graphforge_exec::AdmittedAdjacencyProvider::new(
                adjacency_provider.as_ref(),
                self.property_inventory_for_session(),
            ),
            &self.dir,
            self.ontology_mode,
            source.map(|uuid| *uuid.as_bytes()),
            target.map(|uuid| *uuid.as_bytes()),
            options,
            graphforge_exec::AlgorithmLimits::default()
                .with_batch_size(self.resource_policy.batch_size)
                .with_compute_threads(self.resource_policy.compute_threads),
            Some(self.compute_pool.clone()),
        )
    }

    /// Compute a graph-level structural metric (spanning trees, DAG checks,
    /// coloring, embeddings, …).
    ///
    /// # Errors
    /// Returns [`GfError::Validation`] for malformed labels/options or an
    /// analysis algorithm without a registered Rust implementation, and
    /// structured execution/storage failures from adjacency, limits, or shaping.
    pub fn analyze(
        &self,
        label: Option<&str>,
        options: AnalyzeOptions,
    ) -> Result<arrow::record_batch::RecordBatch, GfError> {
        let dispatch_options = options;
        let label_id = label
            .map(|value| self.algorithm_label(value, "analyze").map(|(id, _)| id))
            .transpose()?
            .unwrap_or(graphforge_value::EntityTypeSelection::All);
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        let adjacency_provider = self.adjacency_provider_for_session();
        adjacency_provider.revalidate();
        graphforge_exec::analyze_algorithm_with_compute(
            &graphforge_exec::AdmittedAdjacencyProvider::new(
                adjacency_provider.as_ref(),
                self.property_inventory_for_session(),
            ),
            &self.dir,
            self.ontology_mode,
            label_id,
            &dispatch_options,
            graphforge_exec::AlgorithmLimits::default()
                .with_batch_size(self.resource_policy.batch_size)
                .with_compute_threads(self.resource_policy.compute_threads),
            Some(self.compute_pool.clone()),
        )
    }

    /// Compute one graph-native node embedding through an activated Rust kernel.
    ///
    /// # Errors
    /// Returns [`GfError::Validation`] for malformed labels or typed options,
    /// [`GfError::NotImplemented`] for embedding values whose native kernel has
    /// not shipped, and structured projection, resource, execution, or shaping
    /// failures.
    pub fn analyze_embedding(
        &self,
        label: Option<&str>,
        options: &EmbeddingAnalyzeOptions,
    ) -> Result<arrow::record_batch::RecordBatch, GfError> {
        let _admission = self.admit_heavy_query()?;
        let label_id = label
            .map(|value| self.algorithm_label(value, "analyze").map(|(id, _)| id))
            .transpose()?
            .unwrap_or(graphforge_value::EntityTypeSelection::All);
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        let adjacency_provider = self.adjacency_provider_for_session();
        adjacency_provider.revalidate();
        graphforge_exec::embedding_algorithm_execution_with_compute(
            &graphforge_exec::AdmittedAdjacencyProvider::new(
                adjacency_provider.as_ref(),
                self.property_inventory_for_session(),
            ),
            &self.dir,
            self.ontology_mode,
            label_id,
            label,
            options,
            graphforge_exec::AlgorithmLimits::default()
                .with_batch_size(self.resource_policy.batch_size)
                .with_compute_threads(self.resource_policy.compute_threads),
            Some(self.compute_pool.clone()),
        )
        .map(|execution| execution.result)
    }

    /// Compute pairwise node similarity through the Rust-only algorithm registry.
    ///
    /// # Errors
    /// Returns [`GfError::Validation`] for malformed options or a similarity
    /// algorithm without a registered Rust implementation, and structured
    /// execution/storage failures from adjacency, limits, or result shaping.
    pub fn similar(
        &self,
        label: &str,
        options: SimilarOptions,
    ) -> Result<arrow::record_batch::RecordBatch, GfError> {
        let _admission = self.admit_heavy_query()?;
        let (label_id, stem) = self.algorithm_label(label, "similar")?;
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        let adjacency_provider = self.adjacency_provider_for_session();
        adjacency_provider.revalidate();
        graphforge_exec::similar_algorithm_with_compute(
            &graphforge_exec::AdmittedAdjacencyProvider::new(
                adjacency_provider.as_ref(),
                self.property_inventory_for_session(),
            ),
            &self.dir,
            self.ontology_mode,
            label_id,
            std::slice::from_ref(&stem),
            options,
            graphforge_exec::AlgorithmLimits::default()
                .with_batch_size(self.resource_policy.batch_size)
                .with_compute_threads(self.resource_policy.compute_threads),
            Some(self.compute_pool.clone()),
        )
    }
}

#[cfg(test)]
mod tests;
