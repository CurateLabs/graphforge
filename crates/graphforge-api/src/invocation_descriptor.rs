//! Canonical, knowledge-neutral descriptors for the complete algorithm catalog.

use std::collections::BTreeMap;

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use graphforge_core::algorithms::{
    Algorithm, AlgorithmFieldType, AlgorithmVerb, AnalyzeAlgorithm, ClusterAlgorithm,
    PathAlgorithm, RankAlgorithm, SimilarAlgorithm,
};
use graphforge_core::canonical::{
    CANONICAL_CONTRACT_VERSION, CanonicalDomain, CanonicalError, CanonicalReader, CanonicalWriter,
    fingerprint,
};

/// Frozen neutral invocation descriptor version.
pub const DESCRIPTOR_CONTRACT_VERSION: u32 = 1;
const ALGORITHM_CONTRACT_VERSION: u32 = 1;
const RESULT_SCHEMA_VERSION: u32 = 1;

/// One normalized descriptor parameter.
#[derive(Clone, Debug, PartialEq)]
pub enum InvocationParameter {
    /// Boolean value.
    Bool(bool),
    /// Unsigned integer value.
    U64(u64),
    /// Ordered unsigned integer list.
    U64List(Vec<u64>),
    /// Finite IEEE-754 value.
    F64(f64),
    /// UTF-8 value.
    Utf8(String),
    /// Stable UUID value.
    Uuid([u8; 16]),
    /// Stable UUID list.
    UuidList(Vec<[u8; 16]>),
    /// Ordered UTF-8 list.
    Utf8List(Vec<String>),
    /// Ordered finite IEEE-754 list.
    F64List(Vec<f64>),
}

impl InvocationParameter {
    fn kind(&self) -> &'static str {
        match self {
            Self::Bool(_) => "bool",
            Self::U64(_) => "u64",
            Self::U64List(_) => "u64_list",
            Self::F64(_) => "f64",
            Self::Utf8(_) => "utf8",
            Self::Uuid(_) => "uuid",
            Self::UuidList(_) => "uuid_list",
            Self::Utf8List(_) => "utf8_list",
            Self::F64List(_) => "f64_list",
        }
    }

    fn encode(&self, writer: &mut CanonicalWriter) -> Result<(), InvocationDescriptorError> {
        writer.text(self.kind())?;
        match self {
            Self::Bool(value) => writer.u8(u8::from(*value))?,
            Self::U64(value) => writer.u64(*value)?,
            Self::U64List(values) => {
                writer.u64(exact_len(values.len())?)?;
                for value in values {
                    writer.u64(*value)?;
                }
            }
            Self::F64(value) => {
                finite(*value)?;
                writer.u64(value.to_bits())?;
            }
            Self::Utf8(value) => writer.text(value)?,
            Self::Uuid(value) => writer.raw(value)?,
            Self::UuidList(values) => {
                writer.u64(exact_len(values.len())?)?;
                for value in values {
                    writer.raw(value)?;
                }
            }
            Self::Utf8List(values) => {
                writer.u64(exact_len(values.len())?)?;
                for value in values {
                    writer.text(value)?;
                }
            }
            Self::F64List(values) => {
                writer.u64(exact_len(values.len())?)?;
                for value in values {
                    finite(*value)?;
                    writer.u64(value.to_bits())?;
                }
            }
        }
        Ok(())
    }
}

/// Stable metadata required for every public algorithm catalog entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AlgorithmDescriptorContract {
    /// Typed public algorithm.
    pub algorithm: Algorithm,
    /// Mathematical/dispatch contract version.
    pub algorithm_version: u32,
    /// Result schema version.
    pub result_schema_version: u32,
}

/// Return every descriptor contract in deterministic catalog order.
#[must_use]
pub fn algorithm_descriptor_contracts() -> Vec<AlgorithmDescriptorContract> {
    RankAlgorithm::ALL
        .iter()
        .copied()
        .map(Algorithm::Rank)
        .chain(
            ClusterAlgorithm::ALL
                .iter()
                .copied()
                .map(Algorithm::Cluster),
        )
        .chain(PathAlgorithm::ALL.iter().copied().map(Algorithm::Paths))
        .chain(
            AnalyzeAlgorithm::ALL
                .iter()
                .copied()
                .map(Algorithm::Analyze),
        )
        .chain(
            SimilarAlgorithm::ALL
                .iter()
                .copied()
                .map(Algorithm::Similar),
        )
        .map(|algorithm| AlgorithmDescriptorContract {
            algorithm,
            algorithm_version: ALGORITHM_CONTRACT_VERSION,
            result_schema_version: RESULT_SCHEMA_VERSION,
        })
        .collect()
}

/// Canonical neutral invocation descriptor.
#[derive(Clone, Debug, PartialEq)]
pub struct InvocationDescriptor {
    descriptor_version: u32,
    algorithm: Algorithm,
    projection_fingerprint: [u8; 32],
    parameters: BTreeMap<String, InvocationParameter>,
    result_schema_fingerprint: [u8; 32],
    canonical_bytes: Vec<u8>,
    fingerprint: [u8; 32],
}

impl InvocationDescriptor {
    /// Construct a version-1 descriptor from normalized effective parameters.
    pub fn new(
        algorithm: Algorithm,
        projection_fingerprint: [u8; 32],
        parameters: BTreeMap<String, InvocationParameter>,
    ) -> Result<Self, InvocationDescriptorError> {
        validate_parameters(algorithm, &parameters)?;
        let result_schema_fingerprint = result_schema_fingerprint(algorithm)?;
        let canonical_bytes = encode_descriptor(
            algorithm,
            projection_fingerprint,
            &parameters,
            result_schema_fingerprint,
        )?;
        let descriptor_fingerprint = fingerprint(
            CanonicalDomain::InvocationDescriptor,
            CANONICAL_CONTRACT_VERSION,
            &canonical_bytes,
        )?;
        Ok(Self {
            descriptor_version: DESCRIPTOR_CONTRACT_VERSION,
            algorithm,
            projection_fingerprint,
            parameters,
            result_schema_fingerprint,
            canonical_bytes,
            fingerprint: descriptor_fingerprint,
        })
    }

    /// Decode and fully validate canonical descriptor bytes.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, InvocationDescriptorError> {
        let mut reader = CanonicalReader::new(bytes)?;
        if reader.raw(4)? != b"GFID" {
            return Err(InvocationDescriptorError::Invalid(
                "descriptor magic is not GFID".into(),
            ));
        }
        let descriptor_version = reader.u32()?;
        if descriptor_version != DESCRIPTOR_CONTRACT_VERSION {
            return Err(InvocationDescriptorError::UnsupportedVersion {
                version: descriptor_version,
            });
        }
        let verb = match reader.text()? {
            "rank" => AlgorithmVerb::Rank,
            "cluster" => AlgorithmVerb::Cluster,
            "paths" => AlgorithmVerb::Paths,
            "analyze" => AlgorithmVerb::Analyze,
            "similar" => AlgorithmVerb::Similar,
            _ => {
                return Err(InvocationDescriptorError::Invalid(
                    "descriptor verb is not registered".into(),
                ));
            }
        };
        let algorithm_name = reader.text()?;
        let algorithm = Algorithm::parse(verb, algorithm_name).map_err(|_| {
            InvocationDescriptorError::Invalid(format!(
                "algorithm {verb:?}.{algorithm_name} is not registered"
            ))
        })?;
        let algorithm_version = reader.u32()?;
        if algorithm_version != ALGORITHM_CONTRACT_VERSION {
            return Err(InvocationDescriptorError::UnsupportedVersion {
                version: algorithm_version,
            });
        }
        let projection_fingerprint = reader
            .raw(32)?
            .try_into()
            .expect("reader returned 32 fingerprint bytes");
        let count = reader.u64()?;
        if count > 64 {
            return Err(InvocationDescriptorError::Invalid(
                "descriptor has more than 64 parameters".into(),
            ));
        }
        let mut parameters = BTreeMap::new();
        for _ in 0..count {
            let name = reader.text()?.to_owned();
            let value = decode_parameter(&mut reader)?;
            if parameters.insert(name.clone(), value).is_some() {
                return Err(InvocationDescriptorError::Invalid(format!(
                    "duplicate parameter {name:?}"
                )));
            }
        }
        let result_schema_version = reader.u32()?;
        if result_schema_version != RESULT_SCHEMA_VERSION {
            return Err(InvocationDescriptorError::UnsupportedVersion {
                version: result_schema_version,
            });
        }
        let encoded_schema_fingerprint: [u8; 32] = reader
            .raw(32)?
            .try_into()
            .expect("reader returned 32 fingerprint bytes");
        reader.finish()?;

        let descriptor = Self::new(algorithm, projection_fingerprint, parameters)?;
        if descriptor.result_schema_fingerprint != encoded_schema_fingerprint {
            return Err(InvocationDescriptorError::Invalid(
                "result schema fingerprint does not match the registry".into(),
            ));
        }
        if descriptor.canonical_bytes != bytes {
            return Err(InvocationDescriptorError::Invalid(
                "descriptor bytes are not in canonical order".into(),
            ));
        }
        Ok(descriptor)
    }

    /// Descriptor contract version.
    #[must_use]
    pub const fn descriptor_version(&self) -> u32 {
        self.descriptor_version
    }

    /// Typed catalog entry.
    #[must_use]
    pub const fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    /// Exact logical graph projection fingerprint.
    #[must_use]
    pub const fn projection_fingerprint(&self) -> &[u8; 32] {
        &self.projection_fingerprint
    }

    /// Normalized parameters in raw UTF-8 key order.
    #[must_use]
    pub const fn parameters(&self) -> &BTreeMap<String, InvocationParameter> {
        &self.parameters
    }

    /// Registered result schema fingerprint.
    #[must_use]
    pub const fn result_schema_fingerprint(&self) -> &[u8; 32] {
        &self.result_schema_fingerprint
    }

    /// Canonical language-neutral descriptor bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Full domain-separated descriptor fingerprint.
    #[must_use]
    pub const fn fingerprint(&self) -> &[u8; 32] {
        &self.fingerprint
    }
}

fn decode_parameter(
    reader: &mut CanonicalReader<'_>,
) -> Result<InvocationParameter, InvocationDescriptorError> {
    let kind = reader.text()?;
    Ok(match kind {
        "bool" => match reader.u8()? {
            0 => InvocationParameter::Bool(false),
            1 => InvocationParameter::Bool(true),
            _ => {
                return Err(InvocationDescriptorError::Invalid(
                    "boolean parameter is not 0 or 1".into(),
                ));
            }
        },
        "u64" => InvocationParameter::U64(reader.u64()?),
        "u64_list" => {
            let count = bounded_list_count(reader.u64()?)?;
            let mut values = Vec::new();
            for _ in 0..count {
                values.push(reader.u64()?);
            }
            InvocationParameter::U64List(values)
        }
        "f64" => {
            let value = f64::from_bits(reader.u64()?);
            finite(value)?;
            InvocationParameter::F64(value)
        }
        "utf8" => InvocationParameter::Utf8(reader.text()?.to_owned()),
        "uuid" => InvocationParameter::Uuid(
            reader
                .raw(16)?
                .try_into()
                .expect("reader returned 16 UUID bytes"),
        ),
        "uuid_list" => {
            let count = bounded_list_count(reader.u64()?)?;
            let mut values = Vec::new();
            for _ in 0..count {
                values.push(
                    reader
                        .raw(16)?
                        .try_into()
                        .expect("reader returned 16 UUID bytes"),
                );
            }
            InvocationParameter::UuidList(values)
        }
        "utf8_list" => {
            let count = bounded_list_count(reader.u64()?)?;
            let mut values = Vec::new();
            for _ in 0..count {
                values.push(reader.text()?.to_owned());
            }
            InvocationParameter::Utf8List(values)
        }
        "f64_list" => {
            let count = bounded_list_count(reader.u64()?)?;
            let mut values = Vec::new();
            for _ in 0..count {
                let value = f64::from_bits(reader.u64()?);
                finite(value)?;
                values.push(value);
            }
            InvocationParameter::F64List(values)
        }
        _ => {
            return Err(InvocationDescriptorError::Invalid(format!(
                "parameter kind {kind:?} is not registered"
            )));
        }
    })
}

fn bounded_list_count(value: u64) -> Result<usize, InvocationDescriptorError> {
    if value > 1_000_000 {
        return Err(InvocationDescriptorError::Invalid(
            "descriptor list has more than 1000000 items".into(),
        ));
    }
    usize::try_from(value)
        .map_err(|_| InvocationDescriptorError::Invalid("list count exceeds usize".into()))
}

/// Structured descriptor construction/decoding failure.
#[derive(thiserror::Error, Debug)]
pub enum InvocationDescriptorError {
    /// Unsupported descriptor or canonical version.
    #[error("unsupported invocation descriptor contract version {version}")]
    UnsupportedVersion {
        /// Rejected version.
        version: u32,
    },
    /// Descriptor shape or parameter contract is invalid.
    #[error("invalid invocation descriptor: {0}")]
    Invalid(String),
    /// Canonical encoding failed.
    #[error(transparent)]
    Canonical(#[from] CanonicalError),
}

impl InvocationDescriptorError {
    /// Stable binding-facing error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::UnsupportedVersion { .. } => "GF_UNSUPPORTED_CONTRACT_VERSION",
            Self::Invalid(_) | Self::Canonical(_) => "GF_DESCRIPTOR_INVALID",
        }
    }
}

/// Failure while preparing or dispatching a neutral invocation.
#[derive(thiserror::Error, Debug)]
pub enum InvocationError {
    /// Descriptor bytes or normalized parameters are invalid.
    #[error(transparent)]
    Descriptor(#[from] InvocationDescriptorError),
    /// The graph projection no longer matches the prepared invocation.
    #[error("the graph projection changed after descriptor preparation")]
    ProjectionChanged,
    /// Executor output no longer matches the registered Arrow result contract.
    #[error("algorithm result schema does not match the invocation descriptor")]
    SchemaMismatch,
    /// Graph selection or algorithm execution failed.
    #[error(transparent)]
    Graph(#[from] GfError),
}

impl InvocationError {
    /// Stable binding-facing error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Descriptor(error) => error.code(),
            Self::ProjectionChanged => "GF_PROJECTION_CHANGED",
            Self::SchemaMismatch => "GF_SCHEMA_MISMATCH",
            Self::Graph(error) => error.code(),
        }
    }
}

pub(crate) fn validate_result(
    descriptor: &InvocationDescriptor,
    result: RecordBatch,
) -> Result<RecordBatch, InvocationError> {
    let contract = descriptor.algorithm().result_schema();
    let schema = result.schema();
    if contract.fields.iter().all(|expected| {
        schema.field_with_name(expected.name).is_ok_and(|actual| {
            actual.is_nullable() == expected.nullable
                && arrow_type_matches(expected.data_type, actual.data_type())
        })
    }) {
        Ok(result)
    } else {
        Err(InvocationError::SchemaMismatch)
    }
}

fn arrow_type_matches(expected: AlgorithmFieldType, actual: &DataType) -> bool {
    match expected {
        AlgorithmFieldType::Uuid => actual == &DataType::FixedSizeBinary(16),
        AlgorithmFieldType::UuidList => matches!(
            actual,
            DataType::List(field) if field.data_type() == &DataType::FixedSizeBinary(16)
        ),
        AlgorithmFieldType::Float32List => matches!(
            actual,
            DataType::List(field) | DataType::FixedSizeList(field, _)
                if field.data_type() == &DataType::Float32
        ),
        AlgorithmFieldType::Utf8 => actual == &DataType::Utf8,
        AlgorithmFieldType::Boolean => actual == &DataType::Boolean,
        AlgorithmFieldType::UInt64 => actual == &DataType::UInt64,
        AlgorithmFieldType::Int64 => actual == &DataType::Int64,
        AlgorithmFieldType::Float64 => actual == &DataType::Float64,
    }
}

pub(crate) fn required_utf8<'a>(
    parameters: &'a BTreeMap<String, InvocationParameter>,
    name: &str,
) -> Result<&'a str, InvocationDescriptorError> {
    match parameters.get(name) {
        Some(InvocationParameter::Utf8(value)) => Ok(value),
        _ => Err(InvocationDescriptorError::Invalid(format!(
            "parameter {name:?} must be utf8"
        ))),
    }
}

pub(crate) fn optional_utf8(
    parameters: &BTreeMap<String, InvocationParameter>,
    name: &str,
) -> Result<Option<String>, InvocationDescriptorError> {
    parameters
        .get(name)
        .map(|value| match value {
            InvocationParameter::Utf8(value) => Ok(value.clone()),
            _ => Err(InvocationDescriptorError::Invalid(format!(
                "parameter {name:?} must be utf8"
            ))),
        })
        .transpose()
}

pub(crate) fn required_bool(
    parameters: &BTreeMap<String, InvocationParameter>,
    name: &str,
) -> Result<bool, InvocationDescriptorError> {
    match parameters.get(name) {
        Some(InvocationParameter::Bool(value)) => Ok(*value),
        _ => Err(InvocationDescriptorError::Invalid(format!(
            "parameter {name:?} must be bool"
        ))),
    }
}

pub(crate) fn required_u64(
    parameters: &BTreeMap<String, InvocationParameter>,
    name: &str,
) -> Result<u64, InvocationDescriptorError> {
    match parameters.get(name) {
        Some(InvocationParameter::U64(value)) => Ok(*value),
        _ => Err(InvocationDescriptorError::Invalid(format!(
            "parameter {name:?} must be u64"
        ))),
    }
}

pub(crate) fn optional_u64(
    parameters: &BTreeMap<String, InvocationParameter>,
    name: &str,
) -> Result<Option<u64>, InvocationDescriptorError> {
    parameters
        .get(name)
        .map(|value| match value {
            InvocationParameter::U64(value) => Ok(*value),
            _ => Err(InvocationDescriptorError::Invalid(format!(
                "parameter {name:?} must be u64"
            ))),
        })
        .transpose()
}

pub(crate) fn required_f64(
    parameters: &BTreeMap<String, InvocationParameter>,
    name: &str,
) -> Result<f64, InvocationDescriptorError> {
    match parameters.get(name) {
        Some(InvocationParameter::F64(value)) => Ok(*value),
        _ => Err(InvocationDescriptorError::Invalid(format!(
            "parameter {name:?} must be f64"
        ))),
    }
}

pub(crate) fn required_utf8_list(
    parameters: &BTreeMap<String, InvocationParameter>,
    name: &str,
) -> Result<Vec<String>, InvocationDescriptorError> {
    match parameters.get(name) {
        Some(InvocationParameter::Utf8List(value)) => Ok(value.clone()),
        _ => Err(InvocationDescriptorError::Invalid(format!(
            "parameter {name:?} must be utf8_list"
        ))),
    }
}

pub(crate) fn required_u64_list(
    parameters: &BTreeMap<String, InvocationParameter>,
    name: &str,
) -> Result<Vec<u64>, InvocationDescriptorError> {
    match parameters.get(name) {
        Some(InvocationParameter::U64List(value)) => Ok(value.clone()),
        _ => Err(InvocationDescriptorError::Invalid(format!(
            "parameter {name:?} must be u64_list"
        ))),
    }
}

pub(crate) fn required_f64_list(
    parameters: &BTreeMap<String, InvocationParameter>,
    name: &str,
) -> Result<Vec<f64>, InvocationDescriptorError> {
    match parameters.get(name) {
        Some(InvocationParameter::F64List(value)) => Ok(value.clone()),
        _ => Err(InvocationDescriptorError::Invalid(format!(
            "parameter {name:?} must be f64_list"
        ))),
    }
}

pub(crate) fn optional_uuid(
    parameters: &BTreeMap<String, InvocationParameter>,
    name: &str,
) -> Result<Option<[u8; 16]>, InvocationDescriptorError> {
    parameters
        .get(name)
        .map(|value| match value {
            InvocationParameter::Uuid(value) => Ok(*value),
            _ => Err(InvocationDescriptorError::Invalid(format!(
                "parameter {name:?} must be uuid"
            ))),
        })
        .transpose()
}

pub(crate) fn optional_uuid_list(
    parameters: &BTreeMap<String, InvocationParameter>,
    name: &str,
) -> Result<Option<Vec<[u8; 16]>>, InvocationDescriptorError> {
    parameters
        .get(name)
        .map(|value| match value {
            InvocationParameter::UuidList(value) => Ok(value.clone()),
            _ => Err(InvocationDescriptorError::Invalid(format!(
                "parameter {name:?} must be uuid_list"
            ))),
        })
        .transpose()
}

fn encode_descriptor(
    algorithm: Algorithm,
    projection_fingerprint: [u8; 32],
    parameters: &BTreeMap<String, InvocationParameter>,
    result_schema_fingerprint: [u8; 32],
) -> Result<Vec<u8>, InvocationDescriptorError> {
    let mut writer = CanonicalWriter::new();
    writer.raw(b"GFID")?;
    writer.u32(DESCRIPTOR_CONTRACT_VERSION)?;
    writer.text(algorithm.verb().as_str())?;
    writer.text(algorithm.as_str())?;
    writer.u32(ALGORITHM_CONTRACT_VERSION)?;
    writer.raw(&projection_fingerprint)?;
    writer.u64(exact_len(parameters.len())?)?;
    for (name, value) in parameters {
        writer.text(name)?;
        value.encode(&mut writer)?;
    }
    writer.u32(RESULT_SCHEMA_VERSION)?;
    writer.raw(&result_schema_fingerprint)?;
    Ok(writer.finish())
}

fn result_schema_fingerprint(algorithm: Algorithm) -> Result<[u8; 32], InvocationDescriptorError> {
    let schema = algorithm.result_schema();
    let mut writer = CanonicalWriter::new();
    writer.raw(b"GFAS")?;
    writer.u32(RESULT_SCHEMA_VERSION)?;
    writer.u64(exact_len(schema.fields.len())?)?;
    for field in schema.fields {
        writer.text(field.name)?;
        writer.text(field_type_name(field.data_type))?;
        writer.u8(u8::from(field.nullable))?;
    }
    writer.u8(u8::from(schema.includes_node_properties))?;
    Ok(fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        &writer.finish(),
    )?)
}

const fn field_type_name(value: AlgorithmFieldType) -> &'static str {
    match value {
        AlgorithmFieldType::Uuid => "fixed_size_binary_16",
        AlgorithmFieldType::UuidList => "list_fixed_size_binary_16",
        AlgorithmFieldType::Float32List => "list_float32",
        AlgorithmFieldType::Utf8 => "utf8",
        AlgorithmFieldType::Boolean => "bool",
        AlgorithmFieldType::UInt64 => "u64",
        AlgorithmFieldType::Int64 => "i64",
        AlgorithmFieldType::Float64 => "f64",
    }
}

fn validate_parameters(
    algorithm: Algorithm,
    parameters: &BTreeMap<String, InvocationParameter>,
) -> Result<(), InvocationDescriptorError> {
    for name in parameters.keys() {
        if name.is_empty()
            || name.trim() != name
            || name.chars().any(char::is_control)
            || !allowed_parameter(algorithm, name)
        {
            return Err(InvocationDescriptorError::Invalid(format!(
                "parameter {name:?} is not registered for {}.{}",
                algorithm.verb().as_str(),
                algorithm.as_str()
            )));
        }
    }
    let names = parameters.keys().map(String::as_str).collect::<Vec<_>>();
    match algorithm {
        Algorithm::Rank(_) => {
            require_exact_names(algorithm, &names, &["directed", "label", "via"])?;
        }
        Algorithm::Cluster(value) => {
            let expected = if matches!(value, ClusterAlgorithm::Hdbscan | ClusterAlgorithm::KMeans)
            {
                &["directed", "label", "vector_property"][..]
            } else {
                &["directed", "label", "via"][..]
            };
            require_exact_names(algorithm, &names, expected)?;
        }
        Algorithm::Similar(value) => {
            let expected = match value {
                SimilarAlgorithm::Knn | SimilarAlgorithm::Cosine => {
                    &["k", "label", "vector_property"][..]
                }
                SimilarAlgorithm::FilteredKnn => &["k", "label", "vector_property", "via"][..],
                SimilarAlgorithm::NodeSimilarity | SimilarAlgorithm::FilteredNodeSimilarity => {
                    &["k", "label", "via"][..]
                }
            };
            require_exact_names(algorithm, &names, expected)?;
        }
        Algorithm::Paths(_) => {}
        Algorithm::Analyze(value) => {
            if let Some(expected) = embedding_parameter_names(value) {
                require_exact_names(algorithm, &names, expected)?;
            }
        }
    }
    Ok(())
}

fn embedding_parameter_names(value: AnalyzeAlgorithm) -> Option<&'static [&'static str]> {
    match value {
        AnalyzeAlgorithm::Node2Vec => Some(&[
            "dimensions",
            "directed",
            "epochs",
            "label",
            "learning_rate",
            "negative_samples",
            "p",
            "q",
            "seed",
            "via",
            "walk_length",
            "walks_per_node",
            "weight",
            "window_size",
        ]),
        AnalyzeAlgorithm::GraphSage => Some(&[
            "aggregator",
            "dimensions",
            "directed",
            "epochs",
            "feature_properties",
            "hidden_dimensions",
            "label",
            "layers",
            "learning_rate",
            "negative_samples",
            "sample_sizes",
            "seed",
            "via",
            "weight",
        ]),
        AnalyzeAlgorithm::FastRandomProjection => Some(&[
            "dimensions",
            "directed",
            "feature_properties",
            "feature_weight",
            "iteration_weights",
            "label",
            "normalization_strength",
            "seed",
            "via",
            "weight",
        ]),
        AnalyzeAlgorithm::HashGnn => Some(&[
            "dimensions",
            "directed",
            "embedding_density",
            "heterogeneous",
            "iterations",
            "label",
            "node_type_property",
            "relationship_type_property",
            "seed",
            "via",
            "weight",
        ]),
        _ => None,
    }
}

fn require_exact_names(
    algorithm: Algorithm,
    observed: &[&str],
    expected: &[&str],
) -> Result<(), InvocationDescriptorError> {
    debug_assert!(
        expected.windows(2).all(|pair| pair[0] < pair[1]),
        "expected parameter names must be sorted to match BTreeMap key order"
    );
    if observed == expected {
        Ok(())
    } else {
        Err(InvocationDescriptorError::Invalid(format!(
            "{}.{} requires parameters {expected:?}; observed {observed:?}",
            algorithm.verb().as_str(),
            algorithm.as_str()
        )))
    }
}

fn allowed_parameter(algorithm: Algorithm, name: &str) -> bool {
    match algorithm {
        Algorithm::Rank(_) => matches!(name, "label" | "via" | "directed"),
        Algorithm::Cluster(_) => {
            matches!(name, "label" | "via" | "directed" | "vector_property")
        }
        Algorithm::Similar(_) => {
            matches!(name, "label" | "via" | "k" | "vector_property")
        }
        Algorithm::Paths(value) => allowed_path_parameter(value, name),
        Algorithm::Analyze(value) => allowed_analyze_parameter(value, name),
    }
}

fn allowed_path_parameter(algorithm: PathAlgorithm, name: &str) -> bool {
    if matches!(name, "via" | "directed" | "k") {
        return true;
    }
    match name {
        "source_uuid" => !matches!(
            algorithm,
            PathAlgorithm::GomoryHuTree
                | PathAlgorithm::MinSteinerTree
                | PathAlgorithm::PrizeCollectingSteinerTree
        ),
        "target_uuid" => !matches!(
            algorithm,
            PathAlgorithm::DijkstraAllPairs
                | PathAlgorithm::FloydWarshall
                | PathAlgorithm::Dfs
                | PathAlgorithm::RandomWalk
                | PathAlgorithm::TransitiveClosure
                | PathAlgorithm::GomoryHuTree
                | PathAlgorithm::MinSteinerTree
                | PathAlgorithm::PrizeCollectingSteinerTree
        ),
        "weight" => !matches!(
            algorithm,
            PathAlgorithm::Bfs
                | PathAlgorithm::Dfs
                | PathAlgorithm::TransitiveClosure
                | PathAlgorithm::MinCostMaxFlow
                | PathAlgorithm::MinCostMaxFlowEdges
        ),
        "capacity_property" | "cost_property" => matches!(
            algorithm,
            PathAlgorithm::MinCostMaxFlow | PathAlgorithm::MinCostMaxFlowEdges
        ),
        "heuristic" => algorithm == PathAlgorithm::AStar,
        "walk_length" | "seed" => algorithm == PathAlgorithm::RandomWalk,
        "terminal_uuids" => matches!(
            algorithm,
            PathAlgorithm::MinSteinerTree | PathAlgorithm::PrizeCollectingSteinerTree
        ),
        "prize_property" => algorithm == PathAlgorithm::PrizeCollectingSteinerTree,
        _ => false,
    }
}

fn allowed_analyze_parameter(algorithm: AnalyzeAlgorithm, name: &str) -> bool {
    if let Some(expected) = embedding_parameter_names(algorithm) {
        return expected.contains(&name);
    }
    if matches!(name, "label" | "via" | "directed") {
        return true;
    }
    match name {
        "weight" => matches!(
            algorithm,
            AnalyzeAlgorithm::MinimumSpanningTree
                | AnalyzeAlgorithm::MaximumSpanningTree
                | AnalyzeAlgorithm::MinimumKSpanningTree
                | AnalyzeAlgorithm::DagLongestPathWeighted
                | AnalyzeAlgorithm::Conductance
                | AnalyzeAlgorithm::Modularity
                | AnalyzeAlgorithm::MaxWeightMatching
        ),
        "k" => algorithm == AnalyzeAlgorithm::MinimumKSpanningTree,
        "partition_property" => matches!(
            algorithm,
            AnalyzeAlgorithm::MaxBipartiteMatching
                | AnalyzeAlgorithm::Conductance
                | AnalyzeAlgorithm::Modularity
        ),
        _ => false,
    }
}

fn finite(value: f64) -> Result<(), InvocationDescriptorError> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(InvocationDescriptorError::Invalid(
            "floating-point parameters must be finite".into(),
        ))
    }
}

fn exact_len(value: usize) -> Result<u64, InvocationDescriptorError> {
    u64::try_from(value)
        .map_err(|_| InvocationDescriptorError::Invalid("item count exceeds UInt64".into()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::{
        AnalyzeOptions, ClusterOptions, EmbeddingAnalyzeOptions, EmbeddingOptions, GraphForge,
        Node2VecOptions, NodeSelector, PathsOptions, PropValue, RankOptions, SimilarOptions,
    };

    fn raw_descriptor(
        magic: &[u8; 4],
        descriptor_version: u32,
        verb: &str,
        algorithm: &str,
        algorithm_version: u32,
        parameter_count: u64,
        result_schema_version: u32,
        schema_fingerprint: [u8; 32],
    ) -> Vec<u8> {
        let mut writer = CanonicalWriter::new();
        writer.raw(magic).unwrap();
        writer.u32(descriptor_version).unwrap();
        writer.text(verb).unwrap();
        writer.text(algorithm).unwrap();
        writer.u32(algorithm_version).unwrap();
        writer.raw(&[0; 32]).unwrap();
        writer.u64(parameter_count).unwrap();
        writer.u32(result_schema_version).unwrap();
        writer.raw(&schema_fingerprint).unwrap();
        writer.finish()
    }

    #[test]
    fn wave10_canonical_decoder_rejects_each_frozen_envelope_violation() {
        let schema = result_schema_fingerprint(Algorithm::Paths(PathAlgorithm::Bfs)).unwrap();
        let cases = [
            raw_descriptor(b"NOPE", 1, "paths", "bfs", 1, 0, 1, schema),
            raw_descriptor(b"GFID", 2, "paths", "bfs", 1, 0, 1, schema),
            raw_descriptor(b"GFID", 1, "unknown", "bfs", 1, 0, 1, schema),
            raw_descriptor(b"GFID", 1, "paths", "unknown", 1, 0, 1, schema),
            raw_descriptor(b"GFID", 1, "paths", "bfs", 2, 0, 1, schema),
            raw_descriptor(b"GFID", 1, "paths", "bfs", 1, 65, 1, schema),
            raw_descriptor(b"GFID", 1, "paths", "bfs", 1, 0, 2, schema),
            raw_descriptor(b"GFID", 1, "paths", "bfs", 1, 0, 1, [9; 32]),
        ];
        for bytes in cases {
            assert!(InvocationDescriptor::from_canonical_bytes(&bytes).is_err());
        }

        let descriptor = InvocationDescriptor::new(
            Algorithm::Paths(PathAlgorithm::Bfs),
            [0; 32],
            BTreeMap::new(),
        )
        .unwrap();
        let mut trailing = descriptor.canonical_bytes().to_vec();
        trailing.push(0);
        assert_eq!(
            InvocationDescriptor::from_canonical_bytes(&trailing)
                .unwrap_err()
                .code(),
            "GF_DESCRIPTOR_INVALID"
        );
    }

    #[test]
    fn registry_is_exhaustive_for_all_94_catalog_entries() {
        let contracts = algorithm_descriptor_contracts();
        assert_eq!(contracts.len(), 94);
        let identities = contracts
            .iter()
            .map(|contract| {
                (
                    contract.algorithm.verb().as_str(),
                    contract.algorithm.as_str(),
                )
            })
            .collect::<HashSet<_>>();
        assert_eq!(identities.len(), contracts.len());
        assert!(contracts.iter().all(|contract| {
            contract.algorithm_version == 1 && contract.result_schema_version == 1
        }));
    }

    #[test]
    fn insertion_order_cannot_change_canonical_bytes_or_fingerprint() {
        let algorithm = Algorithm::Rank(RankAlgorithm::PageRank);
        let first = BTreeMap::from([
            ("via".into(), InvocationParameter::Utf8("*".into())),
            ("label".into(), InvocationParameter::Utf8("Person".into())),
            ("directed".into(), InvocationParameter::Bool(true)),
        ]);
        let second = BTreeMap::from([
            ("directed".into(), InvocationParameter::Bool(true)),
            ("label".into(), InvocationParameter::Utf8("Person".into())),
            ("via".into(), InvocationParameter::Utf8("*".into())),
        ]);
        let first = InvocationDescriptor::new(algorithm, [7; 32], first).unwrap();
        let second = InvocationDescriptor::new(algorithm, [7; 32], second).unwrap();
        assert_eq!(first.canonical_bytes(), second.canonical_bytes());
        assert_eq!(first.fingerprint(), second.fingerprint());
    }

    #[test]
    fn knowledge_fields_are_rejected_by_the_closed_registry() {
        let error = InvocationDescriptor::new(
            Algorithm::Rank(RankAlgorithm::Degree),
            [0; 32],
            BTreeMap::from([("confidence".into(), InvocationParameter::F64(0.9))]),
        )
        .unwrap_err();
        assert_eq!(error.code(), "GF_DESCRIPTOR_INVALID");
    }

    #[test]
    fn every_parameter_encoding_and_typed_accessor_has_an_exact_contract() {
        let values = [
            InvocationParameter::Bool(true),
            InvocationParameter::U64(42),
            InvocationParameter::U64List(vec![1, 2]),
            InvocationParameter::F64(1.25),
            InvocationParameter::Utf8("value".into()),
            InvocationParameter::Uuid([7; 16]),
            InvocationParameter::UuidList(vec![[1; 16], [2; 16]]),
            InvocationParameter::Utf8List(vec!["a".into(), "b".into()]),
            InvocationParameter::F64List(vec![-1.0, 2.5]),
        ];
        for value in &values {
            let mut writer = CanonicalWriter::new();
            value.encode(&mut writer).unwrap();
            let bytes = writer.finish();
            let mut reader = CanonicalReader::new(&bytes).unwrap();
            assert_eq!(&decode_parameter(&mut reader).unwrap(), value);
            reader.finish().unwrap();
        }

        let parameters = BTreeMap::from([
            ("bool".into(), values[0].clone()),
            ("u64".into(), values[1].clone()),
            ("u64_list".into(), values[2].clone()),
            ("f64".into(), values[3].clone()),
            ("utf8".into(), values[4].clone()),
            ("uuid".into(), values[5].clone()),
            ("uuid_list".into(), values[6].clone()),
            ("utf8_list".into(), values[7].clone()),
            ("f64_list".into(), values[8].clone()),
        ]);
        assert!(required_bool(&parameters, "bool").unwrap());
        assert_eq!(required_u64(&parameters, "u64").unwrap(), 42);
        assert_eq!(optional_u64(&parameters, "u64").unwrap(), Some(42));
        assert_eq!(required_u64_list(&parameters, "u64_list").unwrap(), [1, 2]);
        assert_eq!(required_f64(&parameters, "f64").unwrap(), 1.25);
        assert_eq!(
            required_f64_list(&parameters, "f64_list").unwrap(),
            [-1.0, 2.5]
        );
        assert_eq!(required_utf8(&parameters, "utf8").unwrap(), "value");
        assert_eq!(
            optional_utf8(&parameters, "utf8").unwrap().as_deref(),
            Some("value")
        );
        assert_eq!(
            required_utf8_list(&parameters, "utf8_list").unwrap(),
            ["a", "b"]
        );
        assert_eq!(optional_uuid(&parameters, "uuid").unwrap(), Some([7; 16]));
        assert_eq!(
            optional_uuid_list(&parameters, "uuid_list").unwrap(),
            Some(vec![[1; 16], [2; 16]])
        );
        assert_eq!(optional_u64(&parameters, "missing").unwrap(), None);
        assert_eq!(optional_utf8(&parameters, "missing").unwrap(), None);
        assert_eq!(optional_uuid(&parameters, "missing").unwrap(), None);
        assert_eq!(optional_uuid_list(&parameters, "missing").unwrap(), None);

        for error in [
            required_bool(&parameters, "utf8").unwrap_err(),
            required_u64(&parameters, "utf8").unwrap_err(),
            optional_u64(&parameters, "utf8").unwrap_err(),
            required_u64_list(&parameters, "utf8").unwrap_err(),
            required_f64(&parameters, "utf8").unwrap_err(),
            required_f64_list(&parameters, "utf8").unwrap_err(),
            required_utf8(&parameters, "bool").unwrap_err(),
            optional_utf8(&parameters, "bool").unwrap_err(),
            required_utf8_list(&parameters, "utf8").unwrap_err(),
            optional_uuid(&parameters, "utf8").unwrap_err(),
            optional_uuid_list(&parameters, "utf8").unwrap_err(),
        ] {
            assert_eq!(error.code(), "GF_DESCRIPTOR_INVALID");
        }
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut writer = CanonicalWriter::new();
            assert_eq!(
                InvocationParameter::F64(value)
                    .encode(&mut writer)
                    .unwrap_err()
                    .code(),
                "GF_DESCRIPTOR_INVALID"
            );
        }
    }

    #[test]
    fn specialized_path_and_analysis_parameter_allowlists_are_closed() {
        for (algorithm, name) in [
            (PathAlgorithm::AStar, "heuristic"),
            (PathAlgorithm::RandomWalk, "walk_length"),
            (PathAlgorithm::RandomWalk, "seed"),
            (PathAlgorithm::MinCostMaxFlow, "capacity_property"),
            (PathAlgorithm::MinCostMaxFlowEdges, "cost_property"),
            (PathAlgorithm::MinSteinerTree, "terminal_uuids"),
            (PathAlgorithm::PrizeCollectingSteinerTree, "prize_property"),
        ] {
            assert!(allowed_path_parameter(algorithm, name));
        }
        for (algorithm, name) in [
            (PathAlgorithm::Bfs, "weight"),
            (PathAlgorithm::GomoryHuTree, "source_uuid"),
            (PathAlgorithm::Dfs, "target_uuid"),
            (PathAlgorithm::Bfs, "unknown"),
        ] {
            assert!(!allowed_path_parameter(algorithm, name));
        }
        for algorithm in [
            AnalyzeAlgorithm::Node2Vec,
            AnalyzeAlgorithm::GraphSage,
            AnalyzeAlgorithm::FastRandomProjection,
            AnalyzeAlgorithm::HashGnn,
        ] {
            let names = embedding_parameter_names(algorithm).unwrap();
            assert!(names.windows(2).all(|pair| pair[0] < pair[1]));
            assert!(
                names
                    .iter()
                    .all(|name| allowed_analyze_parameter(algorithm, name))
            );
        }
        assert!(allowed_analyze_parameter(
            AnalyzeAlgorithm::MinimumKSpanningTree,
            "k"
        ));
        assert!(allowed_analyze_parameter(
            AnalyzeAlgorithm::MaxWeightMatching,
            "weight"
        ));
        assert!(allowed_analyze_parameter(
            AnalyzeAlgorithm::Modularity,
            "partition_property"
        ));
        assert!(!allowed_analyze_parameter(
            AnalyzeAlgorithm::IsDag,
            "weight"
        ));
    }

    #[test]
    fn algorithm_specific_registry_rejects_irrelevant_parameters() {
        let analyze_error = InvocationDescriptor::new(
            Algorithm::Analyze(AnalyzeAlgorithm::IsDag),
            [0; 32],
            BTreeMap::from([
                ("dimensions".into(), InvocationParameter::U64(8)),
                ("directed".into(), InvocationParameter::Bool(true)),
                ("via".into(), InvocationParameter::Utf8("*".into())),
            ]),
        )
        .unwrap_err();
        assert_eq!(analyze_error.code(), "GF_DESCRIPTOR_INVALID");

        let paths_error = InvocationDescriptor::new(
            Algorithm::Paths(PathAlgorithm::Bfs),
            [0; 32],
            BTreeMap::from([
                ("directed".into(), InvocationParameter::Bool(true)),
                ("k".into(), InvocationParameter::U64(1)),
                ("seed".into(), InvocationParameter::U64(7)),
                ("source_uuid".into(), InvocationParameter::Uuid([1; 16])),
                ("via".into(), InvocationParameter::Utf8("*".into())),
            ]),
        )
        .unwrap_err();
        assert_eq!(paths_error.code(), "GF_DESCRIPTOR_INVALID");
    }

    #[test]
    fn golden_rank_descriptor_bytes_are_stable() {
        let descriptor = InvocationDescriptor::new(
            Algorithm::Rank(RankAlgorithm::Degree),
            [0x11; 32],
            BTreeMap::from([
                ("directed".into(), InvocationParameter::Bool(false)),
                ("label".into(), InvocationParameter::Utf8("Person".into())),
                ("via".into(), InvocationParameter::Utf8("KNOWS".into())),
            ]),
        )
        .unwrap();
        assert_eq!(
            hex(descriptor.fingerprint()),
            "fd8e8e50f2022dc0ed31c93f19420c6722ec9b575a9a36edd7fdd5d978c5730d"
        );
    }

    #[test]
    fn prepared_rank_dispatch_is_arrow_equivalent_and_detects_projection_change() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person)-[:KNOWS]->(b:Person), \
                 (b)-[:KNOWS]->(c:Person)",
            )
            .unwrap();
        let options = RankOptions {
            by: RankAlgorithm::Degree,
            via: Some("KNOWS".into()),
            directed: true,
            write_property: None,
        };
        let descriptor = graph.prepare_rank_invocation("Person", &options).unwrap();
        let direct = graph.rank("Person", options.clone()).unwrap();
        let dispatched = graph.invoke_rank_descriptor(&descriptor).unwrap();
        assert_eq!(direct, dispatched);

        graph.execute("CREATE (:Other)").unwrap();
        assert_eq!(
            direct,
            graph.invoke_rank_descriptor(&descriptor).unwrap(),
            "unselected graph data must not change the logical projection"
        );
        graph.execute("CREATE (:Person)").unwrap();
        let error = graph.invoke_rank_descriptor(&descriptor).unwrap_err();
        assert_eq!(error.code(), "GF_PROJECTION_CHANGED");
    }

    #[test]
    fn every_verb_prepares_and_dispatches_through_the_direct_executor() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'A'})-[:KNOWS]->(b:Person {name:'B'}), \
                 (b)-[:KNOWS]->(c:Person {name:'C'})",
            )
            .unwrap();

        let cluster = ClusterOptions {
            by: ClusterAlgorithm::Components,
            vector_property: None,
            via: Some("KNOWS".into()),
            directed: false,
            write_property: None,
        };
        let descriptor = graph
            .prepare_cluster_invocation("Person", &cluster)
            .unwrap();
        assert_eq!(
            graph.cluster("Person", cluster).unwrap(),
            graph.invoke_cluster_descriptor(&descriptor).unwrap()
        );

        let similar = SimilarOptions {
            by: SimilarAlgorithm::NodeSimilarity,
            k: 2,
            vector_property: None,
            via: Some("KNOWS".into()),
        };
        let descriptor = graph
            .prepare_similar_invocation("Person", &similar)
            .unwrap();
        assert_eq!(
            graph.similar("Person", similar).unwrap(),
            graph.invoke_similar_descriptor(&descriptor).unwrap()
        );

        let analyze = AnalyzeOptions {
            by: AnalyzeAlgorithm::IsDag,
            via: Some("KNOWS".into()),
            directed: true,
            ..AnalyzeOptions::default()
        };
        let descriptor = graph
            .prepare_analyze_invocation(Some("Person"), &analyze)
            .unwrap();
        assert_eq!(
            graph.analyze(Some("Person"), analyze).unwrap(),
            graph.invoke_analyze_descriptor(&descriptor).unwrap()
        );

        let embedding = EmbeddingAnalyzeOptions {
            by: AnalyzeAlgorithm::Node2Vec,
            via: Some("KNOWS".into()),
            directed: true,
            weight: None,
            options: EmbeddingOptions::Node2Vec(Node2VecOptions {
                dimensions: 4,
                walk_length: 3,
                walks_per_node: 2,
                window_size: 2,
                negative_samples: 1,
                ..Node2VecOptions::default()
            }),
        };
        let descriptor = graph
            .prepare_embedding_invocation(Some("Person"), &embedding)
            .unwrap();
        assert_eq!(
            graph.analyze_embedding(Some("Person"), &embedding).unwrap(),
            graph.invoke_descriptor(&descriptor).unwrap()
        );
        assert_eq!(
            InvocationDescriptor::from_canonical_bytes(descriptor.canonical_bytes()).unwrap(),
            descriptor
        );

        let source = NodeSelector::Match {
            label: "Person".into(),
            property: "name".into(),
            value: PropValue::Str("A".into()),
        };
        let target = NodeSelector::Match {
            label: "Person".into(),
            property: "name".into(),
            value: PropValue::Str("C".into()),
        };
        let paths = PathsOptions {
            by: PathAlgorithm::Bfs,
            via: Some("KNOWS".into()),
            ..PathsOptions::default()
        };
        let descriptor = graph
            .prepare_paths_invocation(Some(&source), Some(&target), &paths)
            .unwrap();
        assert_eq!(
            graph.paths(&source, Some(&target), paths).unwrap(),
            graph.invoke_paths_descriptor(&descriptor).unwrap()
        );
    }

    #[test]
    fn empty_graph_rank_vector_is_frozen_for_all_bindings() {
        let graph = GraphForge::new(None).unwrap();
        let descriptor = graph
            .prepare_rank_invocation(
                "Person",
                &RankOptions {
                    by: RankAlgorithm::Degree,
                    via: Some("KNOWS".into()),
                    directed: true,
                    write_property: None,
                },
            )
            .unwrap();
        assert_eq!(
            hex(descriptor.fingerprint()),
            "61be156b4aea627fd2cdbf75e18bcc5d0cfc1df53de51ceec5ab9c98f5e19992"
        );
        let decoded =
            InvocationDescriptor::from_canonical_bytes(descriptor.canonical_bytes()).unwrap();
        assert_eq!(decoded, descriptor);

        let mut future = descriptor.canonical_bytes().to_vec();
        future[4..8].copy_from_slice(&2_u32.to_be_bytes());
        let error = InvocationDescriptor::from_canonical_bytes(&future).unwrap_err();
        assert_eq!(error.code(), "GF_UNSUPPORTED_CONTRACT_VERSION");

        let truncated = &descriptor.canonical_bytes()[..descriptor.canonical_bytes().len() - 1];
        let error = InvocationDescriptor::from_canonical_bytes(truncated).unwrap_err();
        assert_eq!(error.code(), "GF_DESCRIPTOR_INVALID");
    }

    #[test]
    fn canonical_decoder_rejects_invalid_magic_kinds_values_and_bounds() {
        let descriptor = InvocationDescriptor::new(
            Algorithm::Rank(RankAlgorithm::Degree),
            [9; 32],
            BTreeMap::from([
                ("directed".into(), InvocationParameter::Bool(false)),
                ("label".into(), InvocationParameter::Utf8("Person".into())),
                ("via".into(), InvocationParameter::Utf8("*".into())),
            ]),
        )
        .unwrap();
        let mut corrupt = descriptor.canonical_bytes().to_vec();
        corrupt[..4].copy_from_slice(b"NOPE");
        assert_eq!(
            InvocationDescriptor::from_canonical_bytes(&corrupt)
                .unwrap_err()
                .code(),
            "GF_DESCRIPTOR_INVALID"
        );
        let mut trailing = descriptor.canonical_bytes().to_vec();
        trailing.push(0);
        assert_eq!(
            InvocationDescriptor::from_canonical_bytes(&trailing)
                .unwrap_err()
                .code(),
            "GF_DESCRIPTOR_INVALID"
        );
        let decode = |kind: &str, payload: Option<u8>| {
            let mut writer = CanonicalWriter::new();
            writer.text(kind).unwrap();
            if let Some(value) = payload {
                writer.u8(value).unwrap();
            }
            let bytes = writer.finish();
            let mut reader = CanonicalReader::new(&bytes).unwrap();
            decode_parameter(&mut reader).unwrap_err()
        };
        assert_eq!(decode("bool", Some(2)).code(), "GF_DESCRIPTOR_INVALID");
        assert_eq!(decode("unknown", None).code(), "GF_DESCRIPTOR_INVALID");
        assert_eq!(
            bounded_list_count(1_000_001).unwrap_err().code(),
            "GF_DESCRIPTOR_INVALID"
        );
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}
