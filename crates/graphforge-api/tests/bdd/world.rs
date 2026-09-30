//! The shared cucumber [`World`] for the GraphForge BDD suites.
//!
//! Shared by the Cucumber runner (`tests/bdd/main.rs`) and the in-process Divan
//! TCK benchmark (`benches/tck_scenarios/`), which both execute scenarios
//! through the step functions registered against this type.

use cucumber::World;

/// Shared cucumber [`World`] for the GraphForge BDD suites (public API + TCK).
#[derive(Debug, Default, World)]
pub struct GraphForgeWorld {
    /// The forge instance under test (None until a Given step creates it).
    pub forge: Option<graphforge_api::GraphForge>,
    /// Owns a persistent-project fixture directory for lifecycle scenarios.
    pub persistent_fixture: Option<tempfile::TempDir>,
    /// Owns an ontology fixture directory for load scenarios.
    pub ontology_fixture: Option<tempfile::TempDir>,
    /// Ontology fixture path selected by the Given step.
    pub ontology_path: Option<std::path::PathBuf>,
    /// Last error returned by a When step.
    pub last_error: Option<String>,
    /// Stable public code for the last typed Rust facade error.
    pub last_error_code: Option<&'static str>,
    /// Typed planning InvalidType rejection; its public code is GF_VALIDATION.
    pub last_compile_type_error: bool,
    /// Last metadata collection returned by labels or relationship_types.
    pub last_names: Option<Vec<String>>,
    /// Last scalar returned by node_count.
    pub last_count: Option<u64>,
    /// Last explanation returned by explain.
    pub last_explanation: Option<String>,
    /// Last Arrow-backed result returned by `execute()`.
    pub last_exec: Option<graphforge_api::ExecutionResult>,
    /// Most recent Arrow result returned by an analyst verb.
    pub last_algorithm_result: Option<arrow::record_batch::RecordBatch>,
    /// Previous analyst result, retained for comparison scenarios.
    pub previous_algorithm_result: Option<arrow::record_batch::RecordBatch>,
    /// Query parameters bound by openCypher TCK `And parameters are:` steps.
    pub params: std::collections::HashMap<String, graphforge_api::IrLiteral>,
    /// Node handles by name.
    pub nodes: std::collections::HashMap<String, graphforge_api::NodeHandle>,
    /// Most recently created node handle for result-focused assertions.
    pub last_node_handle: Option<graphforge_api::NodeHandle>,
    /// Most recently created edge handle for result-focused assertions.
    pub last_edge_handle: Option<graphforge_api::EdgeHandle>,
    /// Number of explicit public index calls made in this scenario.
    pub index_calls: usize,
    /// Stored query/index vector for find/index scenarios.
    pub stored_vector: Option<Vec<f32>>,
    /// Caller-defined vector space used by find/index fixtures.
    pub stored_space: Option<String>,
    /// Stored node UUID (hex or hyphenated) for index upsert scenarios.
    pub stored_paper_id: Option<String>,
}
