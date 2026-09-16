//! Analyst bindings and native task ownership.

use crate::AnalyzeAlgorithm;
use crate::AnalyzeOptions;
use crate::BigInt;
use crate::Buffer;
use crate::ClassInstance;
use crate::EmbeddingInput;
use crate::GfError;
use crate::GraphForge;
use crate::InvocationDescriptor;
use crate::NodeSelector;
use crate::NodeSelectorInput;
use crate::PathsOptions;
use crate::Result;
use crate::SimilarOptions;
use crate::algorithm_descriptor_contracts;
use crate::embedding_error;
use crate::embedding_options_from_input;
use crate::hex_bytes;
use crate::napi;
use crate::napi_validation;
use crate::node_selector_from_input;
use crate::record_batch_to_ipc;
use crate::to_napi_err;
use crate::to_napi_invocation_err;
use crate::validate_embedding_options;

#[napi(object)]
/// Structured Graph Scale Index grade for one opened workspace.
pub struct GraphScaleIndexProfileOutput {
    /// Full GSI identifier.
    pub gsi: String,
    /// `directed`, `undirected`, or `unknown`.
    pub directedness: String,
    /// Live node count `V`.
    pub node_count: BigInt,
    /// Live edge count `E`.
    pub edge_count: BigInt,
    /// Raw clamped density in `[0.0, 1.0]`.
    pub density: f64,
    /// Two-character Scale Code.
    pub scale_code: String,
    /// Size Tag for the Scale Code band.
    pub size_tag: String,
    /// Integer density percent in `0..=100`.
    pub density_integer: u32,
}

pub(super) fn parse_terminal_uuids(values: &[String]) -> Result<Vec<[u8; 16]>> {
    let mut terminals = Vec::new();
    terminals.try_reserve_exact(values.len()).map_err(|_| {
        to_napi_err(&GfError::Execution(
            "Steiner terminal allocation exceeds available memory".into(),
        ))
    })?;
    for value in values {
        if value.len() != 36 {
            return Err(to_napi_err(&GfError::Validation(format!(
                "invalid Steiner terminal UUID {value:?}"
            ))));
        }
        let NodeSelector::Uuid(uuid) =
            NodeSelector::uuid(value).map_err(|error| to_napi_err(&error))?
        else {
            unreachable!("UUID parser always constructs a UUID selector")
        };
        if uuid.hyphenated().to_string() != *value {
            return Err(to_napi_err(&GfError::Validation(format!(
                "invalid Steiner terminal UUID {value:?}"
            ))));
        }
        terminals.push(*uuid.as_bytes());
    }
    Ok(terminals)
}

pub(super) fn parse_algorithm_id(value: &str) -> Result<graphforge_api::Algorithm> {
    let (verb, name) = value
        .split_once('.')
        .ok_or_else(|| napi_validation("algorithm must be verb.name"))?;
    let verb = match verb {
        "rank" => graphforge_api::AlgorithmVerb::Rank,
        "cluster" => graphforge_api::AlgorithmVerb::Cluster,
        "paths" => graphforge_api::AlgorithmVerb::Paths,
        "analyze" => graphforge_api::AlgorithmVerb::Analyze,
        "similar" => graphforge_api::AlgorithmVerb::Similar,
        _ => return Err(napi_validation("unknown algorithm verb")),
    };
    graphforge_api::Algorithm::parse(verb, name)
        .map_err(|_| napi_validation("unknown algorithm ID"))
}

/// Opaque Rust-owned neutral algorithm invocation descriptor.
#[napi(js_name = "InvocationDescriptor")]
pub struct InvocationDescriptorHandle {
    pub(super) inner: InvocationDescriptor,
}

/// Thin Node projection of one live Rust algorithm descriptor contract.
#[napi(object)]
pub struct AlgorithmDescriptorContractJs {
    /// Owning analyst verb.
    pub verb: String,
    /// Canonical algorithm catalog value.
    pub algorithm: String,
    /// Mathematical/dispatch contract version.
    pub algorithm_version: u32,
    /// Result schema version.
    pub result_schema_version: u32,
}

#[napi]
impl InvocationDescriptorHandle {
    /// Canonical language-neutral descriptor bytes.
    #[napi(getter)]
    #[must_use]
    pub fn canonical_bytes(&self) -> Buffer {
        Buffer::from(self.inner.canonical_bytes().to_vec())
    }

    /// Full descriptor fingerprint as lowercase hex.
    #[napi(getter)]
    #[must_use]
    pub fn fingerprint(&self) -> String {
        hex_bytes(self.inner.fingerprint())
    }

    /// Exact logical projection fingerprint as lowercase hex.
    #[napi(getter)]
    #[must_use]
    pub fn projection_fingerprint(&self) -> String {
        hex_bytes(self.inner.projection_fingerprint())
    }

    /// Owning analyst verb.
    #[napi(getter)]
    #[must_use]
    pub fn verb(&self) -> String {
        self.inner.algorithm().verb().as_str().to_owned()
    }

    /// Canonical algorithm catalog value.
    #[napi(getter)]
    #[must_use]
    pub fn algorithm(&self) -> String {
        self.inner.algorithm().as_str().to_owned()
    }
}

pub(super) fn parse_seed(value: BigInt) -> Result<u64> {
    let (negative, seed, lossless) = value.get_u64();
    if !negative && lossless {
        Ok(seed)
    } else {
        Err(napi_validation("seed must be an unsigned 64-bit integer"))
    }
}

#[napi]
impl GraphForge {
    /// Rank nodes through the Rust registry. Returns an Arrow IPC `Buffer`.
    #[napi]
    pub fn rank(
        &self,
        label: String,
        by: String,
        via: Option<String>,
        directed: Option<bool>,
        write_property: Option<String>,
    ) -> Result<Buffer> {
        let g = self.open_guard()?;
        let options = graphforge_api::RankOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            via,
            directed: directed.unwrap_or(true),
            write_property,
        };
        let batch = g
            .rank(&label, options)
            .map_err(|error| to_napi_err(&error))?;
        Ok(Buffer::from(
            record_batch_to_ipc(&batch).map_err(|error| to_napi_err(&error))?,
        ))
    }

    /// Prepare a Rust-owned neutral rank invocation without executing it.
    #[napi]
    pub fn prepare_rank_invocation(
        &self,
        label: String,
        by: String,
        via: Option<String>,
        directed: Option<bool>,
    ) -> Result<InvocationDescriptorHandle> {
        let graph = self.open_guard()?;
        let options = graphforge_api::RankOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            via,
            directed: directed.unwrap_or(true),
            write_property: None,
        };
        graph
            .prepare_rank_invocation(&label, &options)
            .map(|inner| InvocationDescriptorHandle { inner })
            .map_err(|error| to_napi_invocation_err(&error))
    }

    /// Prepare clustering without executing it.
    #[napi]
    pub fn prepare_cluster_invocation(
        &self,
        label: String,
        by: String,
        via: Option<String>,
        directed: Option<bool>,
        vector_property: Option<String>,
    ) -> Result<InvocationDescriptorHandle> {
        let graph = self.open_guard()?;
        let options = graphforge_api::ClusterOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            vector_property,
            via,
            directed: directed.unwrap_or(false),
            write_property: None,
        };
        graph
            .prepare_cluster_invocation(&label, &options)
            .map(|inner| InvocationDescriptorHandle { inner })
            .map_err(|error| to_napi_invocation_err(&error))
    }

    /// Prepare paths without executing it.
    #[napi(
        ts_args_type = "source: string | NodeHandle | { label: string; property: string; value: any } | null | undefined, target: string | NodeHandle | { label: string; property: string; value: any } | null | undefined, by: string, via?: string | null, directed?: boolean | null, k?: number | null, weight?: string | null, heuristic?: string | null, walkLength?: number | null, seed?: bigint | null, terminalUuids?: string[] | null, prizeProperty?: string | null, capacityProperty?: string | null, costProperty?: string | null"
    )]
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_paths_invocation(
        &self,
        source: Option<NodeSelectorInput<'_>>,
        target: Option<NodeSelectorInput<'_>>,
        by: String,
        via: Option<String>,
        directed: Option<bool>,
        k: Option<u32>,
        weight: Option<String>,
        heuristic: Option<String>,
        walk_length: Option<u32>,
        seed: Option<BigInt>,
        terminal_uuids: Option<Vec<String>>,
        prize_property: Option<String>,
        capacity_property: Option<String>,
        cost_property: Option<String>,
    ) -> Result<InvocationDescriptorHandle> {
        let graph = self.open_guard()?;
        let seed = seed.map(parse_seed).transpose()?;
        let options = PathsOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            via,
            directed: directed.unwrap_or(true),
            k: k.unwrap_or(1) as usize,
            weight,
            capacity_property,
            cost_property,
            heuristic,
            walk_length: walk_length.map(|value| value as usize),
            seed,
            terminal_uuids: parse_terminal_uuids(terminal_uuids.as_deref().unwrap_or_default())?,
            prize_property,
        };
        let source = source.map(node_selector_from_input).transpose()?;
        let target = target.map(node_selector_from_input).transpose()?;
        graph
            .prepare_paths_invocation(source.as_ref(), target.as_ref(), &options)
            .map(|inner| InvocationDescriptorHandle { inner })
            .map_err(|error| to_napi_invocation_err(&error))
    }

    /// Prepare analysis without executing it.
    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_analyze_invocation(
        &self,
        by: String,
        label: Option<String>,
        via: Option<String>,
        directed: Option<bool>,
        weight: Option<String>,
        partition_property: Option<String>,
        k: Option<u32>,
    ) -> Result<InvocationDescriptorHandle> {
        let graph = self.open_guard()?;
        let options = AnalyzeOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            via,
            directed: directed.unwrap_or(true),
            weight,
            k: k.map(|value| value as usize),
            partition_property,
        };
        graph
            .prepare_analyze_invocation(label.as_deref(), &options)
            .map(|inner| InvocationDescriptorHandle { inner })
            .map_err(|error| to_napi_invocation_err(&error))
    }

    /// Prepare similarity without executing it.
    #[napi]
    pub fn prepare_similar_invocation(
        &self,
        label: String,
        by: String,
        k: Option<u32>,
        vector_property: Option<String>,
        via: Option<String>,
    ) -> Result<InvocationDescriptorHandle> {
        let graph = self.open_guard()?;
        let options = SimilarOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            k: k.unwrap_or(10) as usize,
            vector_property,
            via,
        };
        graph
            .prepare_similar_invocation(&label, &options)
            .map(|inner| InvocationDescriptorHandle { inner })
            .map_err(|error| to_napi_invocation_err(&error))
    }

    /// Return every live algorithm descriptor contract in deterministic catalog order.
    ///
    /// This projects the Rust-owned registry without opening knowledge or
    /// epistemic storage. Callers still prepare and invoke descriptors through
    /// the neutral facade below.
    #[napi]
    pub fn algorithm_descriptor_contracts(&self) -> Result<Vec<AlgorithmDescriptorContractJs>> {
        self.ensure_open()?;
        Ok(algorithm_descriptor_contracts()
            .into_iter()
            .map(|contract| AlgorithmDescriptorContractJs {
                algorithm: contract.algorithm.as_str().to_owned(),
                algorithm_version: contract.algorithm_version,
                result_schema_version: contract.result_schema_version,
                verb: contract.algorithm.verb().as_str().to_owned(),
            })
            .collect())
    }

    /// Dispatch an opaque descriptor through its Rust-owned analyst verb.
    #[napi]
    pub fn invoke_descriptor(
        &self,
        descriptor: ClassInstance<'_, InvocationDescriptorHandle>,
    ) -> Result<Buffer> {
        let graph = self.open_guard()?;
        let batch = graph
            .invoke_descriptor(&descriptor.inner)
            .map_err(|error| to_napi_invocation_err(&error))?;
        record_batch_to_ipc(&batch)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

    /// Decode canonical descriptor bytes in Rust and dispatch them.
    #[napi]
    pub fn invoke_descriptor_bytes(&self, descriptor: Buffer) -> Result<Buffer> {
        let graph = self.open_guard()?;
        let batch = graph
            .invoke_descriptor_bytes(descriptor.as_ref())
            .map_err(|error| to_napi_invocation_err(&error))?;
        record_batch_to_ipc(&batch)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

    /// Detect communities/components. Returns an Arrow IPC `Buffer`.
    #[napi]
    pub fn cluster(
        &self,
        label: String,
        by: String,
        via: Option<String>,
        directed: Option<bool>,
        write_property: Option<String>,
        vector_property: Option<String>,
    ) -> Result<Buffer> {
        let g = self.open_guard()?;
        let options = graphforge_api::ClusterOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            vector_property,
            via,
            directed: directed.unwrap_or(false),
            write_property,
        };
        let batch = g
            .cluster(&label, options)
            .map_err(|error| to_napi_err(&error))?;
        Ok(Buffer::from(
            record_batch_to_ipc(&batch).map_err(|error| to_napi_err(&error))?,
        ))
    }

    /// Path-finding / flow between typed node selectors. Returns Arrow IPC.
    #[napi(
        ts_args_type = "source: string | NodeHandle | { label: string; property: string; value: any } | null | undefined, target: string | NodeHandle | { label: string; property: string; value: any } | null | undefined, by: string, via?: string | null, directed?: boolean | null, k?: number | null, weight?: string | null, heuristic?: string | null, walkLength?: number | null, seed?: bigint | null, terminalUuids?: string[] | null, prizeProperty?: string | null, capacityProperty?: string | null, costProperty?: string | null"
    )]
    #[allow(clippy::too_many_arguments)] // kwarg-rich v0.5 paths() signature
    pub fn paths(
        &self,
        source: Option<NodeSelectorInput<'_>>,
        target: Option<NodeSelectorInput<'_>>,
        by: String,
        via: Option<String>,
        directed: Option<bool>,
        k: Option<u32>,
        weight: Option<String>,
        heuristic: Option<String>,
        walk_length: Option<u32>,
        seed: Option<BigInt>,
        terminal_uuids: Option<Vec<String>>,
        prize_property: Option<String>,
        capacity_property: Option<String>,
        cost_property: Option<String>,
    ) -> Result<Buffer> {
        self.ensure_open()?;
        let seed = seed
            .map(|value| {
                let (negative, seed, lossless) = value.get_u64();
                if !negative && lossless {
                    Ok(seed)
                } else {
                    Err(to_napi_err(&GfError::Validation(
                        "seed must be an unsigned 64-bit integer".into(),
                    )))
                }
            })
            .transpose()?;
        let options = PathsOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            via,
            directed: directed.unwrap_or(true),
            k: k.unwrap_or(1) as usize,
            weight,
            capacity_property,
            cost_property,
            heuristic,
            walk_length: walk_length.map(|value| value as usize),
            seed,
            terminal_uuids: parse_terminal_uuids(terminal_uuids.as_deref().unwrap_or_default())?,
            prize_property,
        };
        let source = source.map(node_selector_from_input).transpose()?;
        let target = target.map(node_selector_from_input).transpose()?;
        let graph = self.open_guard()?;
        let batch = graph
            .paths(source.as_ref(), target.as_ref(), options)
            .map_err(|error| to_napi_err(&error))?;
        record_batch_to_ipc(&batch)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

    /// Graph-level structural metric. Returns an Arrow IPC `Buffer`.
    #[allow(clippy::too_many_arguments)] // positional v0.5 analyze() signature
    #[napi]
    pub fn analyze(
        &self,
        by: String,
        label: Option<String>,
        via: Option<String>,
        directed: Option<bool>,
        weight: Option<String>,
        partition_property: Option<String>,
        k: Option<u32>,
        embedding_options: Option<EmbeddingInput>,
    ) -> Result<Buffer> {
        self.ensure_open()?;
        let algorithm = by.parse().map_err(|error| to_napi_err(&error))?;
        if matches!(
            algorithm,
            AnalyzeAlgorithm::Node2Vec
                | AnalyzeAlgorithm::GraphSage
                | AnalyzeAlgorithm::FastRandomProjection
                | AnalyzeAlgorithm::HashGnn
        ) {
            if partition_property.is_some() || k.is_some() {
                return Err(embedding_error(
                    "embedding algorithms do not accept partition_property or k",
                ));
            }
            let directed = directed.unwrap_or(!matches!(algorithm, AnalyzeAlgorithm::GraphSage));
            let options = embedding_options_from_input(
                algorithm,
                via,
                directed,
                weight,
                embedding_options.unwrap_or_default(),
            )?;
            validate_embedding_options(&options).map_err(|error| to_napi_err(&error))?;
            let graph = self.open_guard()?;
            let batch = graph
                .analyze_embedding(label.as_deref(), &options)
                .map_err(|error| to_napi_err(&error))?;
            return record_batch_to_ipc(&batch)
                .map(Buffer::from)
                .map_err(|error| to_napi_err(&error));
        }
        if embedding_options.is_some() {
            return Err(embedding_error(format!(
                "{by} does not accept embedding options"
            )));
        }
        // Keep binding construction extension-safe as AnalyzeOptions gains fields.
        #[allow(clippy::needless_update)]
        let options = AnalyzeOptions {
            by: algorithm,
            via,
            directed: directed.unwrap_or(true),
            weight,
            k: k.map(|value| value as usize),
            partition_property,
            ..AnalyzeOptions::default()
        };
        let graph = self.open_guard()?;
        let batch = graph
            .analyze(label.as_deref(), options)
            .map_err(|error| to_napi_err(&error))?;
        record_batch_to_ipc(&batch)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

    /// Pairwise node similarity. Returns an Arrow IPC `Buffer`.
    #[napi]
    pub fn similar(
        &self,
        label: String,
        by: String,
        k: Option<u32>,
        vector_property: Option<String>,
        via: Option<String>,
    ) -> Result<Buffer> {
        let options = SimilarOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            k: k.unwrap_or(10) as usize,
            vector_property,
            via,
        };
        let graph = self.open_guard()?;
        let batch = graph
            .similar(&label, options)
            .map_err(|error| to_napi_err(&error))?;
        record_batch_to_ipc(&batch)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }
}

#[cfg(test)]
mod tests;
